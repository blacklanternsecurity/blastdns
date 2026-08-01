//! In-process DNS server for testing behavior that depends on how a resolver
//! responds under load.
//!
//! Queries traverse the real worker and hickory client path over a real UDP
//! socket; only the server side is synthetic. Loss is deterministic rather than
//! random so convergence assertions are reproducible.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hickory_client::proto::{
    op::{Message, MessageType, ResponseCode},
    rr::{RData, Record, RecordType, rdata::A},
    serialize::binary::BinDecodable,
};
use tokio::{net::UdpSocket, task::JoinHandle};

/// How much backlog the simulated resolver tolerates before dropping. Models a
/// server with a small queue rather than one that drops the instant it is busy.
const BURST_ALLOWANCE: Duration = Duration::from_millis(50);

#[derive(Clone, Debug)]
pub(crate) struct SimConfig {
    /// Delay before answering, simulating distance.
    pub latency: Duration,
    /// Queries per second the resolver can absorb. Arrivals beyond this (plus
    /// [`BURST_ALLOWANCE`]) are dropped, which is the edge a controller should
    /// discover. `None` means unlimited.
    pub capacity_qps: Option<f64>,
    /// Drop one in every N queries regardless of rate. `None` means never.
    pub drop_one_in: Option<u64>,
    /// Answer REFUSED for one in every N queries, simulating a resolver that
    /// declines rather than one that is merely slow.
    pub refuse_one_in: Option<u64>,
    /// Answer over UDP with the TC bit set and a reduced answer set, as a server
    /// does when the full response will not fit. A TCP listener serves the whole
    /// thing, so a correct client recovers the dropped records.
    pub truncate_udp: bool,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            latency: Duration::from_millis(1),
            capacity_qps: None,
            drop_one_in: None,
            refuse_one_in: None,
            truncate_udp: false,
        }
    }
}

struct Counters {
    received: AtomicU64,
    answered: AtomicU64,
    dropped: AtomicU64,
    tcp_served: AtomicU64,
    budget_ns: AtomicU64,
}

/// A running simulated resolver. Dropping it stops the server.
pub(crate) struct SimResolver {
    addr: SocketAddr,
    counters: Arc<Counters>,
    handle: JoinHandle<()>,
    tcp_handle: Option<JoinHandle<()>>,
}

impl Drop for SimResolver {
    fn drop(&mut self) {
        self.handle.abort();
        if let Some(h) = &self.tcp_handle {
            h.abort();
        }
    }
}

impl SimResolver {
    pub(crate) async fn start(config: SimConfig) -> Self {
        let socket = Arc::new(
            UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
                .await
                .expect("bind simulated resolver"),
        );
        let addr = socket.local_addr().expect("simulated resolver address");
        let counters = Arc::new(Counters {
            received: AtomicU64::new(0),
            answered: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            tcp_served: AtomicU64::new(0),
            budget_ns: AtomicU64::new(0),
        });

        let handle = tokio::spawn(serve(socket, config.clone(), counters.clone()));

        // A truncating resolver must also answer over TCP, or a client doing the
        // right thing has nowhere to go.
        let tcp_handle = if config.truncate_udp {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .expect("bind simulated resolver TCP");
            Some(tokio::spawn(serve_tcp(listener, counters.clone())))
        } else {
            None
        };

        Self {
            addr,
            counters,
            handle,
            tcp_handle,
        }
    }

    pub(crate) fn addr(&self) -> String {
        self.addr.to_string()
    }

    pub(crate) fn received(&self) -> u64 {
        self.counters.received.load(Ordering::Relaxed)
    }

    pub(crate) fn answered(&self) -> u64 {
        self.counters.answered.load(Ordering::Relaxed)
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.counters.dropped.load(Ordering::Relaxed)
    }

    /// Queries answered over TCP, i.e. how often a client recovered from TC.
    pub(crate) fn tcp_served(&self) -> u64 {
        self.counters.tcp_served.load(Ordering::Relaxed)
    }
}

async fn serve(socket: Arc<UdpSocket>, config: SimConfig, counters: Arc<Counters>) {
    let start = tokio::time::Instant::now();
    let interval_ns = config
        .capacity_qps
        .map(|qps| (1_000_000_000.0 / qps).round().max(1.0) as u64);
    let mut buf = vec![0u8; 1024];

    loop {
        let (len, src) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(_) => continue,
        };

        let seq = counters.received.fetch_add(1, Ordering::Relaxed) + 1;

        if should_drop(&config, &counters, interval_ns, start, seq) {
            counters.dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let Ok(request) = Message::from_bytes(&buf[..len]) else {
            counters.dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };

        let socket = socket.clone();
        let counters = counters.clone();
        let latency = config.latency;
        let refuse = config
            .refuse_one_in
            .is_some_and(|n| n > 0 && seq.is_multiple_of(n));
        let truncate_udp = config.truncate_udp;
        tokio::spawn(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            let response = if truncate_udp {
                build_response_sized(&request, refuse, TRUNCATED_ANSWER_COUNT, true)
            } else {
                build_response(&request, refuse)
            };
            if let Ok(bytes) = response.to_vec()
                && socket.send_to(&bytes, src).await.is_ok()
            {
                counters.answered.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
}

/// Serves complete responses over TCP, using the two-byte length prefix DNS
/// requires there.
async fn serve_tcp(listener: tokio::net::TcpListener, counters: Arc<Counters>) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let counters = counters.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let mut len_buf = [0u8; 2];
                if stream.read_exact(&mut len_buf).await.is_err() {
                    return;
                }
                let len = u16::from_be_bytes(len_buf) as usize;
                let mut buf = vec![0u8; len];
                if stream.read_exact(&mut buf).await.is_err() {
                    return;
                }
                let Ok(request) = Message::from_bytes(&buf) else {
                    return;
                };
                let Ok(bytes) = build_response(&request, false).to_vec() else {
                    return;
                };
                let mut framed = (bytes.len() as u16).to_be_bytes().to_vec();
                framed.extend_from_slice(&bytes);
                if stream.write_all(&framed).await.is_err() {
                    return;
                }
                counters.tcp_served.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
}

fn should_drop(
    config: &SimConfig,
    counters: &Counters,
    interval_ns: Option<u64>,
    start: tokio::time::Instant,
    seq: u64,
) -> bool {
    if let Some(n) = config.drop_one_in
        && n > 0
        && seq.is_multiple_of(n)
    {
        return true;
    }

    // Capacity is a budget cursor: each answered query consumes one interval. A
    // query arriving after the cursor has run further ahead than the burst
    // allowance is beyond what this resolver can absorb.
    let Some(interval_ns) = interval_ns else {
        return false;
    };
    let now_ns = start.elapsed().as_nanos() as u64;
    let allowance_ns = BURST_ALLOWANCE.as_nanos() as u64;

    loop {
        let current = counters.budget_ns.load(Ordering::Relaxed);
        if current > now_ns.saturating_add(allowance_ns) {
            return true;
        }
        let base = current.max(now_ns);
        let next = base.saturating_add(interval_ns);
        if counters
            .budget_ns
            .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return false;
        }
    }
}

/// How many A records the simulated zone holds for a truncating name. UDP gets a
/// subset with TC set; TCP gets all of them.
const TRUNCATED_ANSWER_COUNT: usize = 2;
const FULL_ANSWER_COUNT: usize = 8;

fn build_response(request: &Message, refuse: bool) -> Message {
    build_response_sized(request, refuse, FULL_ANSWER_COUNT, false)
}

fn build_response_sized(
    request: &Message,
    refuse: bool,
    answer_count: usize,
    truncated: bool,
) -> Message {
    let mut response = Message::new();
    response.set_id(request.id());
    response.set_message_type(MessageType::Response);
    response.set_op_code(request.op_code());
    response.set_recursion_desired(request.recursion_desired());
    response.set_recursion_available(true);

    for query in request.queries() {
        response.add_query(query.clone());
    }

    // A refusing resolver returns no answers, but REFUSED says nothing about
    // whether the name exists.
    if refuse {
        response.set_response_code(ResponseCode::Refused);
        return response;
    }

    // Answer A queries. Anything else gets an empty NOERROR, which is a valid
    // response and enough for liveness probes.
    if let Some(query) = request.queries().first()
        && query.query_type() == RecordType::A
    {
        for i in 0..answer_count {
            response.add_answer(Record::from_rdata(
                query.name().clone(),
                60,
                RData::A(A::new(127, 0, 0, (i + 1) as u8)),
            ));
        }
    }
    response.set_truncated(truncated);

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlastDNSClient, BlastDNSConfig, DnsResolver};

    /// Client config with caching, retries, and purgatory off, so tests observe
    /// exactly the queries they send.
    fn client_config() -> BlastDNSConfig {
        BlastDNSConfig {
            max_concurrency: 16,
            max_inflight_per_resolver: 4,
            request_timeout: Duration::from_millis(300),
            max_retries: 0,
            cache_capacity: 0,
            purgatory_threshold: 0,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn answers_queries_through_the_real_client_path() {
        let sim = SimResolver::start(SimConfig::default()).await;
        let client = BlastDNSClient::with_config(vec![sim.addr()], client_config()).unwrap();

        let answers = client
            .resolve("example.com.".to_string(), RecordType::A)
            .await
            .expect("simulated resolver should answer");

        // The simulated zone holds several records per name so truncation has
        // something to drop.
        assert_eq!(answers.len(), FULL_ANSWER_COUNT);
        assert_eq!(answers[0], "127.0.0.1");
        assert_eq!(sim.received(), 1);
        assert_eq!(sim.answered(), 1);
    }

    /// A query holds its resolver's in-flight permit until it returns, so an
    /// unanswered one has to give that permit back on a deadline. Both transports
    /// must honour the timeout rather than leaving it to whichever one happens to
    /// set it on its stream: otherwise lost UDP responses park permits until the
    /// pool starves.
    #[tokio::test]
    async fn an_unanswered_query_times_out_on_every_transport() {
        for persistent_socket in [false, true] {
            let sim = SimResolver::start(SimConfig {
                drop_one_in: Some(1),
                ..Default::default()
            })
            .await;
            let config = BlastDNSConfig {
                persistent_socket,
                ..client_config()
            };
            let timeout = config.request_timeout;
            let client = BlastDNSClient::with_config(vec![sim.addr()], config).unwrap();

            let started = tokio::time::Instant::now();
            let result = client
                .resolve("example.com.".to_string(), RecordType::A)
                .await;
            let elapsed = started.elapsed();

            assert!(
                result.is_err(),
                "persistent_socket={persistent_socket}: a dropped response must not resolve"
            );
            assert!(
                elapsed < timeout * 4,
                "persistent_socket={persistent_socket}: waited {elapsed:?} for a {timeout:?} timeout"
            );
            assert_eq!(sim.dropped(), 1);
        }
    }

    #[tokio::test]
    async fn deterministic_loss_drops_the_expected_share() {
        let sim = SimResolver::start(SimConfig {
            drop_one_in: Some(2),
            ..Default::default()
        })
        .await;
        let client = BlastDNSClient::with_config(vec![sim.addr()], client_config()).unwrap();

        for i in 0..10 {
            let _ = client
                .resolve(format!("h{i}.example.com."), RecordType::A)
                .await;
        }

        assert_eq!(sim.received(), 10);
        assert_eq!(sim.dropped(), 5, "every second query should be dropped");
        assert_eq!(sim.answered(), 5);
    }

    #[tokio::test]
    async fn capacity_causes_loss_only_above_the_threshold() {
        // 50 QPS capacity. Sending well under it should see no drops.
        let sim = SimResolver::start(SimConfig {
            latency: Duration::from_millis(1),
            capacity_qps: Some(50.0),
            ..Default::default()
        })
        .await;
        let client = BlastDNSClient::with_config(vec![sim.addr()], client_config()).unwrap();

        for i in 0..5 {
            let _ = client
                .resolve(format!("slow{i}.example.com."), RecordType::A)
                .await;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(sim.dropped(), 0, "under capacity should not drop");

        // Now flood it well past capacity and confirm it starts dropping.
        let hosts: Vec<String> = (0..400).map(|i| format!("fast{i}.example.com.")).collect();
        let mut stream = Arc::new(client).resolve_batch_full(
            hosts.into_iter().map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            false,
        );
        while futures::StreamExt::next(&mut stream).await.is_some() {}

        assert!(
            sim.dropped() > 0,
            "flooding past capacity should cause drops, got {} received / {} dropped",
            sim.received(),
            sim.dropped()
        );
    }
}
