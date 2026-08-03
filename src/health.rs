use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures::stream::{self, Stream, StreamExt};
use hickory_client::{
    client::{Client, ClientHandle},
    proto::{
        ProtoError,
        rr::{DNSClass, Name, RecordType},
        runtime::TokioRuntimeProvider,
        runtime::TokioTime,
        tcp::TcpClientStream,
        udp::{UdpClientStream, UdpStream},
        xfer::{DnsClientStream, DnsResponse, SerialMessage},
    },
};
use serde::Serialize;
use std::future::Future;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tracing::debug;

use crate::{
    config::BlastDNSConfig,
    error::BlastDNSError,
    limiter::{RateLimiter, UNLIMITED_QPS},
};

/// The background driver for a resolver's client. The per-query and persistent
/// transports produce different future types, so they are boxed to one shape.
type ClientBackground = std::pin::Pin<Box<dyn Future<Output = Result<(), ProtoError>> + Send>>;

/// Adapts hickory's raw [`UdpStream`] into a client stream, so one long-lived
/// UDP socket can back a multiplexed client.
///
/// hickory implements `DnsClientStream` only for TCP, because it treats UDP as
/// one socket per query. The stream is already the right shape; all it lacks is
/// the remote address the client layer needs to report.
struct PersistentUdpStream {
    inner: UdpStream<TokioRuntimeProvider>,
    name_server: SocketAddr,
}

impl Stream for PersistentUdpStream {
    type Item = Result<SerialMessage, ProtoError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.get_mut().inner)
            .poll_next(cx)
            .map(|item| item.map(|result| result.map_err(ProtoError::from)))
    }
}

impl std::fmt::Display for PersistentUdpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UDP({})", self.name_server)
    }
}

impl DnsClientStream for PersistentUdpStream {
    type Time = TokioTime;

    fn name_server_addr(&self) -> SocketAddr {
        self.name_server
    }
}

/// How long a minimum-RTT observation stays authoritative before the window
/// resets. Without this a single lucky sample would pin the baseline forever.
const RTT_MIN_WINDOW: Duration = Duration::from_secs(30);

/// Weight of the newest sample in the RTT moving average (1/8, as in TCP).
const RTT_EWMA_SHIFT: u64 = 3;

/// Shortest deadline the startup probe will use, whatever the request timeout is.
const PROBE_DEADLINE_FLOOR: Duration = Duration::from_secs(2);

/// Counter snapshot for one resolver.
///
/// `attempted` equals `answered + empty + timeout + error`, so a caller can
/// account for every dispatched query.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ResolverStats {
    pub resolver: String,
    pub attempted: u64,
    pub answered: u64,
    pub empty: u64,
    pub timeout: u64,
    pub error: u64,
    pub rtt_mean_us: u64,
    pub rtt_min_us: u64,
    pub purgatory_entries: u64,
    /// Responses that arrived truncated and were refetched over TCP.
    pub truncated: u64,
    /// Current pacing rate in queries per second, or `None` when unthrottled.
    /// Set by the adaptive controller; a value here means this resolver is being
    /// held below the rate at which it started losing queries.
    pub rate_qps: Option<f64>,
}

/// Health, capacity, and connection state for a single resolver.
///
/// One `Client` is created lazily per resolver and shared by every worker that
/// selects it. hickory's client is a handle to a background task, so cloning it
/// costs nothing and does not open another socket.
pub(crate) struct ResolverHealth {
    resolver: SocketAddr,
    request_timeout: Duration,
    persistent_socket: bool,
    purgatory_threshold: usize,
    purgatory_sentence: Duration,
    client: OnceCell<Client>,
    /// Built only if a response arrives truncated. Most resolvers never need it.
    tcp_client: OnceCell<Client>,
    inflight: Arc<Semaphore>,
    /// Per-resolver dispatch pacing. Starts effectively unlimited so the
    /// in-flight permits are what bind until the controller lowers it.
    limiter: RateLimiter,
    start: tokio::time::Instant,
    active: AtomicBool,
    attempted: AtomicU64,
    answered: AtomicU64,
    empty: AtomicU64,
    timeout: AtomicU64,
    error: AtomicU64,
    consecutive_errors: AtomicU64,
    benched_until_ns: AtomicU64,
    purgatory_entries: AtomicU64,
    truncated: AtomicU64,
    rtt_ewma_us: AtomicU64,
    rtt_min_us: AtomicU64,
    rtt_min_window_ns: AtomicU64,
}

impl ResolverHealth {
    pub(crate) fn new(
        resolver: SocketAddr,
        config: &BlastDNSConfig,
        start: tokio::time::Instant,
    ) -> Self {
        Self {
            resolver,
            request_timeout: config.request_timeout,
            persistent_socket: config.persistent_socket,
            purgatory_threshold: config.purgatory_threshold,
            purgatory_sentence: config.purgatory_sentence,
            client: OnceCell::new(),
            tcp_client: OnceCell::new(),
            inflight: Arc::new(Semaphore::new(config.max_inflight_per_resolver.max(1))),
            limiter: RateLimiter::new(UNLIMITED_QPS),
            start,
            active: AtomicBool::new(true),
            attempted: AtomicU64::new(0),
            answered: AtomicU64::new(0),
            empty: AtomicU64::new(0),
            timeout: AtomicU64::new(0),
            error: AtomicU64::new(0),
            consecutive_errors: AtomicU64::new(0),
            benched_until_ns: AtomicU64::new(0),
            purgatory_entries: AtomicU64::new(0),
            truncated: AtomicU64::new(0),
            rtt_ewma_us: AtomicU64::new(0),
            rtt_min_us: AtomicU64::new(0),
            rtt_min_window_ns: AtomicU64::new(0),
        }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.resolver
    }

    fn now_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    /// Take an in-flight slot without waiting, if one is free.
    pub(crate) fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.inflight.clone().try_acquire_owned().ok()
    }

    /// Wait for an in-flight slot.
    pub(crate) async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.inflight.clone().acquire_owned().await.ok()
    }

    /// Whether this resolver is eligible for selection at all. A resolver that
    /// fails the startup probe is deactivated for the life of the client.
    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Wait for this resolver's own dispatch slot.
    ///
    /// Called while holding an in-flight permit, so pacing a resolver also makes
    /// the pool route around it: its permits stay busy and selection skips it.
    pub(crate) async fn acquire_rate(&self) {
        self.limiter.acquire().await;
    }

    /// This resolver's current pacing rate in queries per second.
    pub(crate) fn rate(&self) -> f64 {
        self.limiter.rate()
    }

    /// Set this resolver's pacing rate.
    pub(crate) fn set_rate(&self, queries_per_second: f64) {
        self.limiter.set_rate(queries_per_second);
    }

    /// Send one query to this resolver, giving up after `deadline`.
    ///
    /// The bound is applied here rather than at the call sites because only the
    /// per-query transport can carry one on its stream; a persistent socket has
    /// no per-request deadline of its own, so an unanswered query would otherwise
    /// wait indefinitely.
    pub(crate) async fn query_within(
        &self,
        name: Name,
        record_type: RecordType,
        deadline: Duration,
    ) -> Result<DnsResponse, BlastDNSError> {
        let mut client = self.client().await?;
        let query = client.query(name, DNSClass::IN, record_type);
        match tokio::time::timeout(deadline, query).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(source)) => Err(BlastDNSError::ResolverRequestFailed {
                resolver: self.resolver,
                source,
            }),
            Err(_) => Err(BlastDNSError::QueryTimedOut {
                resolver: self.resolver,
                timeout: deadline,
            }),
        }
    }

    /// Send one query to this resolver under the configured request timeout.
    pub(crate) async fn query_bounded(
        &self,
        name: Name,
        record_type: RecordType,
    ) -> Result<DnsResponse, BlastDNSError> {
        self.query_within(name, record_type, self.request_timeout)
            .await
    }

    /// Confirm this resolver answers queries, deactivating it if not.
    ///
    /// Queries the root nameservers, which every working recursive resolver
    /// answers, so liveness does not depend on any external zone.
    ///
    /// Judged on a looser deadline than a normal query. Deactivation lasts for the
    /// life of the client, and a root-NS query to a cold resolver is slower than
    /// the cached lookups a scan mostly makes, so a brute-force timeout of a few
    /// hundred milliseconds would evict resolvers that serve those lookups fine --
    /// and a passing latency spike would take out much of the pool at once.
    pub(crate) async fn probe(&self) -> bool {
        let deadline = self.request_timeout.max(PROBE_DEADLINE_FLOOR);
        let alive = self
            .query_within(Name::root(), RecordType::NS, deadline)
            .await
            .is_ok();
        if !alive {
            self.active.store(false, Ordering::Relaxed);
            debug!(resolver = %self.resolver, "resolver failed startup probe");
        }
        alive
    }

    /// Whether this resolver is currently serving a purgatory sentence.
    pub(crate) fn is_benched(&self) -> bool {
        self.benched_until_ns.load(Ordering::Relaxed) > self.now_ns()
    }

    /// Nanoseconds from the pool epoch until this resolver is eligible again.
    pub(crate) fn benched_until_ns(&self) -> u64 {
        self.benched_until_ns.load(Ordering::Relaxed)
    }

    /// The shared client for this resolver, connecting on first use.
    pub(crate) async fn client(&self) -> Result<Client, BlastDNSError> {
        let client = self
            .client
            .get_or_try_init(|| async { self.connect().await })
            .await?;
        Ok(client.clone())
    }

    pub(crate) fn record_truncated(&self) {
        self.truncated.fetch_add(1, Ordering::Relaxed);
    }

    /// A TCP client for this resolver, built on first truncated response.
    ///
    /// UDP caps a response at the negotiated payload size; when the server sets
    /// TC it has dropped records to fit. RFC 1035 says to re-ask over TCP, which
    /// has no such limit.
    pub(crate) async fn tcp_client(&self) -> Result<Client, BlastDNSError> {
        let client = self
            .tcp_client
            .get_or_try_init(|| async {
                let (stream, handle) = TcpClientStream::new(
                    self.resolver,
                    None,
                    Some(self.request_timeout),
                    TokioRuntimeProvider::new(),
                );
                let (client, bg) = Client::new(stream, handle, None).await.map_err(|source| {
                    BlastDNSError::ResolverSetupFailed {
                        resolver: self.resolver,
                        source,
                    }
                })?;
                let resolver = self.resolver;
                tokio::spawn(async move {
                    if let Err(err) = bg.await {
                        debug!(%resolver, %err, "resolver TCP background task exited");
                    }
                });
                Ok::<_, BlastDNSError>(client)
            })
            .await?;
        Ok(client.clone())
    }

    async fn connect(&self) -> Result<Client, BlastDNSError> {
        let (client, bg) = if self.persistent_socket {
            self.connect_persistent().await?
        } else {
            self.connect_per_query().await?
        };

        let resolver = self.resolver;
        tokio::spawn(async move {
            if let Err(err) = bg.await {
                debug!(%resolver, %err, "resolver background task exited");
            }
        });

        Ok(client)
    }

    /// hickory's default: a fresh randomly-bound socket for every query.
    ///
    /// Maximum source-port entropy, but it creates one NAT/conntrack entry per
    /// query, which churns state on every device in the path.
    async fn connect_per_query(&self) -> Result<(Client, ClientBackground), BlastDNSError> {
        let provider = TokioRuntimeProvider::new();
        let stream = UdpClientStream::builder(self.resolver, provider)
            .with_timeout(Some(self.request_timeout))
            .build();

        Client::connect(stream)
            .await
            .map(|(client, bg)| (client, Box::pin(bg) as ClientBackground))
            .map_err(|source| BlastDNSError::ResolverSetupFailed {
                resolver: self.resolver,
                source,
            })
    }

    /// One long-lived socket per resolver, with queries multiplexed over it by
    /// transaction ID.
    ///
    /// Trades per-query port randomization for stable path state: conntrack sees
    /// one reused entry per resolver instead of one per query. Matches hickory's
    /// unconnected-socket model, so the source address check and transaction ID
    /// remain the response validation.
    async fn connect_persistent(&self) -> Result<(Client, ClientBackground), BlastDNSError> {
        let bind: SocketAddr = if self.resolver.is_ipv4() {
            (Ipv4Addr::UNSPECIFIED, 0).into()
        } else {
            (Ipv6Addr::UNSPECIFIED, 0).into()
        };
        let socket = tokio::net::UdpSocket::bind(bind).await.map_err(|e| {
            BlastDNSError::Configuration(format!(
                "failed to bind socket for {}: {e}",
                self.resolver
            ))
        })?;

        let (inner, handle) = UdpStream::<TokioRuntimeProvider>::with_bound(socket, self.resolver);
        let stream = PersistentUdpStream {
            inner,
            name_server: self.resolver,
        };
        Client::new(Box::pin(futures::future::ready(Ok(stream))), handle, None)
            .await
            .map(|(client, bg)| (client, Box::pin(bg) as ClientBackground))
            .map_err(|source| BlastDNSError::ResolverSetupFailed {
                resolver: self.resolver,
                source,
            })
    }

    pub(crate) fn record_dispatch(&self) {
        self.attempted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a response from this resolver. `had_answers` distinguishes a real
    /// answer from an empty one (NXDOMAIN, NODATA); both mean the resolver did
    /// its job, so both clear error pressure.
    pub(crate) fn record_response(&self, rtt: Duration, had_answers: bool) {
        if had_answers {
            self.answered.fetch_add(1, Ordering::Relaxed);
        } else {
            self.empty.fetch_add(1, Ordering::Relaxed);
        }
        self.decay_errors();
        self.record_rtt(rtt);
    }

    pub(crate) fn record_error(&self, is_timeout: bool) {
        if is_timeout {
            self.timeout.fetch_add(1, Ordering::Relaxed);
        } else {
            self.error.fetch_add(1, Ordering::Relaxed);
        }

        let consecutive = self.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
        if self.purgatory_threshold == 0 || (consecutive as usize) < self.purgatory_threshold {
            return;
        }
        if self.purgatory_sentence.is_zero() {
            return;
        }

        // The sentence discharges the failures that earned it, so clear the count.
        // Decaying it instead buys a fresh sentence on the very next failure.
        let until = self.now_ns() + self.purgatory_sentence.as_nanos() as u64;
        self.benched_until_ns.store(until, Ordering::Relaxed);
        self.purgatory_entries.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.store(0, Ordering::Relaxed);
        debug!(
            resolver = %self.resolver,
            sentence = ?self.purgatory_sentence,
            consecutive,
            "entering purgatory"
        );
    }

    fn decay_errors(&self) {
        let _ = self
            .consecutive_errors
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    fn record_rtt(&self, rtt: Duration) {
        let sample = (rtt.as_micros() as u64).max(1);

        let previous = self.rtt_ewma_us.load(Ordering::Relaxed);
        let ewma = if previous == 0 {
            sample
        } else {
            previous - (previous >> RTT_EWMA_SHIFT) + (sample >> RTT_EWMA_SHIFT)
        };
        self.rtt_ewma_us.store(ewma.max(1), Ordering::Relaxed);

        // Reset the baseline once the window lapses so a stale minimum cannot
        // pin it below what the resolver currently delivers.
        let now = self.now_ns();
        let window_start = self.rtt_min_window_ns.load(Ordering::Relaxed);
        let current_min = self.rtt_min_us.load(Ordering::Relaxed);
        let lapsed = now.saturating_sub(window_start) > RTT_MIN_WINDOW.as_nanos() as u64;

        if current_min == 0 || lapsed {
            self.rtt_min_us.store(sample, Ordering::Relaxed);
            self.rtt_min_window_ns.store(now, Ordering::Relaxed);
        } else if sample < current_min {
            self.rtt_min_us.store(sample, Ordering::Relaxed);
        }
    }

    pub(crate) fn stats(&self) -> ResolverStats {
        ResolverStats {
            resolver: self.resolver.to_string(),
            attempted: self.attempted.load(Ordering::Relaxed),
            answered: self.answered.load(Ordering::Relaxed),
            empty: self.empty.load(Ordering::Relaxed),
            timeout: self.timeout.load(Ordering::Relaxed),
            error: self.error.load(Ordering::Relaxed),
            rtt_mean_us: self.rtt_ewma_us.load(Ordering::Relaxed),
            rtt_min_us: self.rtt_min_us.load(Ordering::Relaxed),
            purgatory_entries: self.purgatory_entries.load(Ordering::Relaxed),
            truncated: self.truncated.load(Ordering::Relaxed),
            rate_qps: {
                let rate = self.limiter.rate();
                (rate < UNLIMITED_QPS).then_some(rate)
            },
        }
    }
}

/// The set of resolvers a client dispatches across, plus the selection policy.
pub(crate) struct ResolverPool {
    resolvers: Vec<Arc<ResolverHealth>>,
    cursor: AtomicU64,
    start: tokio::time::Instant,
}

/// A resolver reserved for one query. Dropping it returns the in-flight slot.
pub(crate) struct Reservation {
    pub(crate) resolver: Arc<ResolverHealth>,
    _permit: OwnedSemaphorePermit,
}

impl ResolverPool {
    pub(crate) fn new(resolvers: &[SocketAddr], config: &BlastDNSConfig) -> Self {
        let start = tokio::time::Instant::now();
        Self {
            resolvers: resolvers
                .iter()
                .map(|addr| Arc::new(ResolverHealth::new(*addr, config, start)))
                .collect(),
            cursor: AtomicU64::new(0),
            start,
        }
    }

    /// Probe every resolver concurrently, deactivating those that do not answer.
    ///
    /// Returns how many remain active. Queries go straight to each resolver
    /// rather than through the work queue, so this is safe to run before any
    /// workers exist.
    pub(crate) async fn probe(&self, concurrency: usize) -> usize {
        let alive = stream::iter(self.resolvers.iter().cloned())
            .map(|resolver| async move { resolver.probe().await })
            .buffer_unordered(concurrency.max(1))
            .filter(|alive| {
                let alive = *alive;
                async move { alive }
            })
            .count()
            .await;
        debug!(
            total = self.resolvers.len(),
            alive, "resolver probe complete"
        );
        alive
    }

    /// Reserve a resolver for one query.
    ///
    /// Rotates through the pool taking the first free, active, non-benched
    /// resolver. Because a busy resolver holds its permits until its query
    /// completes, rotation naturally sends less work to slow resolvers with no
    /// explicit weighting. Returns `None` when no resolver is active.
    pub(crate) async fn reserve(&self) -> Option<Reservation> {
        if self.resolvers.is_empty() {
            return None;
        }

        loop {
            let offset = self.cursor.fetch_add(1, Ordering::Relaxed) as usize;
            let mut active = 0usize;
            let mut benched = 0usize;
            let mut waitable: Option<&Arc<ResolverHealth>> = None;

            for i in 0..self.resolvers.len() {
                let resolver = &self.resolvers[(offset + i) % self.resolvers.len()];
                if !resolver.is_active() {
                    continue;
                }
                active += 1;
                if resolver.is_benched() {
                    benched += 1;
                    continue;
                }
                if let Some(permit) = resolver.try_acquire() {
                    return Some(Reservation {
                        resolver: resolver.clone(),
                        _permit: permit,
                    });
                }
                if waitable.is_none() {
                    waitable = Some(resolver);
                }
            }

            if active == 0 {
                return None;
            }

            // Every active resolver is benched: wait for the first sentence to
            // lapse rather than spinning.
            if benched == active {
                self.sleep_until_first_eligible().await;
                continue;
            }

            // All eligible resolvers are saturated. Wait on one rather than
            // polling, then re-check bench state on the next pass.
            if let Some(resolver) = waitable
                && let Some(permit) = resolver.acquire().await
                && resolver.is_active()
                && !resolver.is_benched()
            {
                return Some(Reservation {
                    resolver: resolver.clone(),
                    _permit: permit,
                });
            }
        }
    }

    async fn sleep_until_first_eligible(&self) {
        let now = self.start.elapsed().as_nanos() as u64;
        let soonest = self
            .resolvers
            .iter()
            .filter(|r| r.is_active())
            .map(|r| r.benched_until_ns())
            .min()
            .unwrap_or(now);
        let wait = soonest.saturating_sub(now);
        if wait > 0 {
            tokio::time::sleep(Duration::from_nanos(wait)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    pub(crate) fn stats(&self) -> Vec<ResolverStats> {
        self.resolvers.iter().map(|r| r.stats()).collect()
    }

    pub(crate) fn resolvers(&self) -> &[Arc<ResolverHealth>] {
        &self.resolvers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, n], 53))
    }

    fn config(max_inflight: usize, purgatory_threshold: usize) -> BlastDNSConfig {
        BlastDNSConfig {
            max_inflight_per_resolver: max_inflight,
            purgatory_threshold,
            purgatory_sentence: Duration::from_millis(50),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn inflight_permits_cap_per_resolver_concurrency() {
        let health = ResolverHealth::new(addr(1), &config(2, 10), tokio::time::Instant::now());

        let first = health.try_acquire();
        let second = health.try_acquire();
        assert!(first.is_some());
        assert!(second.is_some());
        assert!(
            health.try_acquire().is_none(),
            "a resolver must never exceed max_inflight_per_resolver"
        );

        drop(first);
        assert!(health.try_acquire().is_some(), "permit should be reusable");
    }

    #[tokio::test]
    async fn purgatory_engages_at_the_threshold() {
        let health = ResolverHealth::new(addr(1), &config(1, 3), tokio::time::Instant::now());

        health.record_error(true);
        health.record_error(true);
        assert!(
            !health.is_benched(),
            "benched before reaching the threshold"
        );

        health.record_error(true);
        assert!(health.is_benched(), "threshold reached but not benched");
        assert_eq!(health.stats().purgatory_entries, 1);

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!health.is_benched(), "sentence should lapse");
    }

    #[tokio::test]
    async fn a_second_sentence_costs_another_full_threshold() {
        let health = ResolverHealth::new(addr(1), &config(1, 3), tokio::time::Instant::now());

        for _ in 0..3 {
            health.record_error(true);
        }
        assert!(health.is_benched(), "threshold reached but not benched");
        tokio::time::sleep(Duration::from_millis(80)).await;

        // One more failure must not re-bench: a sentence per failure would pace
        // an all-benched pool at one query per sentence.
        health.record_error(true);
        assert!(!health.is_benched(), "single failure re-armed the sentence");
        assert_eq!(health.stats().purgatory_entries, 1);

        health.record_error(true);
        health.record_error(true);
        assert!(
            health.is_benched(),
            "threshold reached again but not benched"
        );
        assert_eq!(health.stats().purgatory_entries, 2);
    }

    #[tokio::test]
    async fn responses_relieve_error_pressure() {
        let health = ResolverHealth::new(addr(1), &config(1, 3), tokio::time::Instant::now());

        health.record_error(true);
        health.record_error(true);
        health.record_response(Duration::from_millis(10), true);
        // Back down to one accumulated error, so the next two do not bench it.
        health.record_error(true);
        assert!(!health.is_benched());

        let stats = health.stats();
        assert_eq!(stats.answered, 1);
        assert_eq!(stats.timeout, 3);
    }

    #[tokio::test]
    async fn stats_account_for_every_dispatch() {
        let health = ResolverHealth::new(addr(1), &config(1, 0), tokio::time::Instant::now());

        for _ in 0..4 {
            health.record_dispatch();
        }
        health.record_response(Duration::from_millis(5), true);
        health.record_response(Duration::from_millis(5), false);
        health.record_error(true);
        health.record_error(false);

        let s = health.stats();
        assert_eq!(s.attempted, 4);
        assert_eq!(s.answered + s.empty + s.timeout + s.error, s.attempted);
    }

    #[tokio::test]
    async fn rtt_tracks_mean_and_minimum() {
        let health = ResolverHealth::new(addr(1), &config(1, 0), tokio::time::Instant::now());

        health.record_response(Duration::from_millis(100), true);
        assert_eq!(health.stats().rtt_min_us, 100_000);
        assert_eq!(health.stats().rtt_mean_us, 100_000);

        health.record_response(Duration::from_millis(20), true);
        assert_eq!(health.stats().rtt_min_us, 20_000, "minimum should drop");
        let mean = health.stats().rtt_mean_us;
        assert!(
            mean < 100_000 && mean > 20_000,
            "mean {mean} should move toward the new sample without jumping to it"
        );
    }

    #[tokio::test]
    async fn reserve_rotates_across_resolvers() {
        let pool = ResolverPool::new(&[addr(1), addr(2), addr(3)], &config(1, 10));

        let a = pool.reserve().await.unwrap();
        let b = pool.reserve().await.unwrap();
        let c = pool.reserve().await.unwrap();

        let mut picked = [a.resolver.addr(), b.resolver.addr(), c.resolver.addr()];
        picked.sort();
        assert_eq!(
            picked,
            [addr(1), addr(2), addr(3)],
            "with one permit each, three reservations must use three resolvers"
        );
    }

    #[tokio::test]
    async fn reserve_skips_benched_resolvers() {
        let pool = ResolverPool::new(&[addr(1), addr(2)], &config(1, 1));

        // Bench the first resolver by driving it past the threshold.
        let first = pool.reserve().await.unwrap();
        let benched = first.resolver.addr();
        first.resolver.record_error(true);
        drop(first);

        for _ in 0..4 {
            let reservation = pool.reserve().await.unwrap();
            assert_ne!(
                reservation.resolver.addr(),
                benched,
                "a benched resolver must not be selected"
            );
        }
    }

    #[tokio::test]
    async fn reserve_returns_none_for_an_empty_pool() {
        let pool = ResolverPool::new(&[], &config(1, 10));
        assert!(pool.reserve().await.is_none());
    }
}
