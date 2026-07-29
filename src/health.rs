use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures::stream::{self, StreamExt};
use hickory_client::{
    client::{Client, ClientHandle},
    proto::{
        rr::{DNSClass, Name, RecordType},
        runtime::TokioRuntimeProvider,
        udp::UdpClientStream,
    },
};
use serde::Serialize;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tracing::debug;

use crate::{
    config::BlastDNSConfig,
    error::BlastDNSError,
    limiter::{RateLimiter, UNLIMITED_QPS},
};

/// How long a minimum-RTT observation stays authoritative before the window
/// resets. Without this a single lucky sample would pin the baseline forever.
const RTT_MIN_WINDOW: Duration = Duration::from_secs(30);

/// Weight of the newest sample in the RTT moving average (1/8, as in TCP).
const RTT_EWMA_SHIFT: u64 = 3;

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
    purgatory_threshold: usize,
    purgatory_sentence: Duration,
    client: OnceCell<Client>,
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
            purgatory_threshold: config.purgatory_threshold,
            purgatory_sentence: config.purgatory_sentence,
            client: OnceCell::new(),
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

    /// Confirm this resolver answers queries, deactivating it if not.
    ///
    /// Queries the root nameservers, which every working recursive resolver
    /// answers, so liveness does not depend on any external zone.
    pub(crate) async fn probe(&self) -> bool {
        let alive = match self.client().await {
            Ok(mut client) => client
                .query(Name::root(), DNSClass::IN, RecordType::NS)
                .await
                .is_ok(),
            Err(_) => false,
        };
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

    async fn connect(&self) -> Result<Client, BlastDNSError> {
        let provider = TokioRuntimeProvider::new();
        let stream = UdpClientStream::builder(self.resolver, provider)
            .with_timeout(Some(self.request_timeout))
            .build();

        let (client, bg) =
            Client::connect(stream)
                .await
                .map_err(|source| BlastDNSError::ResolverSetupFailed {
                    resolver: self.resolver,
                    source,
                })?;

        let resolver = self.resolver;
        tokio::spawn(async move {
            if let Err(err) = bg.await {
                debug!(%resolver, %err, "resolver background task exited");
            }
        });

        Ok(client)
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

        // Bench the resolver, then relieve one unit of error pressure so a
        // resolver that recovers is not stuck at the threshold.
        let until = self.now_ns() + self.purgatory_sentence.as_nanos() as u64;
        self.benched_until_ns.store(until, Ordering::Relaxed);
        self.purgatory_entries.fetch_add(1, Ordering::Relaxed);
        self.decay_errors();
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
