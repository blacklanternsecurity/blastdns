use std::time::Duration;

use serde::Deserialize;

/// Default cap on queries in flight to any single resolver.
pub const DEFAULT_MAX_INFLIGHT_PER_RESOLVER: usize = 2;
/// Default cap on queries in flight across the whole client.
pub const DEFAULT_MAX_CONCURRENCY: usize = 256;
/// Default timeout in milliseconds used for each resolver request.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(1000);
/// Default number of retry attempts per hostname.
pub const DEFAULT_MAX_RETRIES: usize = 10;
/// Default consecutive error count needed to bench a resolver.
pub const DEFAULT_PURGATORY_THRESHOLD: usize = 10;
/// Default purgatory sentence duration.
pub const DEFAULT_PURGATORY_SENTENCE: Duration = Duration::from_millis(1000);
/// Default cache capacity (0 = disabled).
pub const DEFAULT_CACHE_CAPACITY: usize = 10000;
/// Default minimum TTL for cached entries.
pub const DEFAULT_CACHE_MIN_TTL: Duration = Duration::from_secs(10);
/// Default maximum TTL for cached entries.
pub const DEFAULT_CACHE_MAX_TTL: Duration = Duration::from_secs(86400);

/// Configuration knobs for [`crate::BlastDNSClient`].
#[derive(Clone, Debug)]
pub struct BlastDNSConfig {
    /// Cap on queries in flight to any one resolver. This is the politeness
    /// bound: no resolver can be sent more than this at once, however large the
    /// pool or the workload.
    pub max_inflight_per_resolver: usize,
    /// Cap on queries in flight across all resolvers.
    pub max_concurrency: usize,
    /// Ceiling on dispatch rate in queries per second. `None` is unlimited,
    /// in which case throughput is bounded only by concurrency and latency.
    ///
    /// This is a hard cap, independent of `adaptive`: the controller can lower
    /// the effective rate below it but never above it.
    pub rate_limit: Option<f64>,
    /// Watch loss and back off automatically. On by default. When a resolver
    /// starts dropping queries, pacing retreats below the rate at which that
    /// happened, whether or not `rate_limit` is set.
    pub adaptive: bool,
    /// Probe each resolver once at startup and keep only those that answer.
    pub resolver_probe: bool,
    /// Keep one long-lived socket per resolver, multiplexing queries over it by
    /// transaction ID, instead of binding a fresh socket per query.
    ///
    /// Cuts NAT/conntrack state from one entry per query to one reused entry per
    /// resolver, at the cost of a fixed source port per resolver.
    pub persistent_socket: bool,
    /// Per-request timeout while talking to a resolver.
    pub request_timeout: Duration,
    /// How many times to retry a failed lookup.
    pub max_retries: usize,
    /// Consecutive errors before a resolver is benched.
    pub purgatory_threshold: usize,
    /// How long a resolver stays benched after hitting the threshold.
    pub purgatory_sentence: Duration,
    /// Maximum number of entries in the DNS cache (0 = disabled).
    pub cache_capacity: usize,
    /// Minimum TTL for cached entries.
    pub cache_min_ttl: Duration,
    /// Maximum TTL for cached entries.
    pub cache_max_ttl: Duration,
}

/// JSON-serializable config shape used at the Python FFI boundary.
#[derive(Debug, Deserialize)]
pub struct BlastDNSConfigWire {
    pub max_inflight_per_resolver: usize,
    pub max_concurrency: usize,
    pub rate_limit: Option<f64>,
    pub adaptive: bool,
    pub resolver_probe: bool,
    pub persistent_socket: bool,
    pub request_timeout_ms: u64,
    pub max_retries: usize,
    pub purgatory_threshold: usize,
    pub purgatory_sentence_ms: u64,
    pub cache_capacity: usize,
    pub cache_min_ttl_secs: u64,
    pub cache_max_ttl_secs: u64,
}

impl From<BlastDNSConfigWire> for BlastDNSConfig {
    fn from(w: BlastDNSConfigWire) -> Self {
        Self {
            max_inflight_per_resolver: w.max_inflight_per_resolver.max(1),
            max_concurrency: w.max_concurrency.max(1),
            rate_limit: w.rate_limit.filter(|r| *r > 0.0),
            adaptive: w.adaptive,
            resolver_probe: w.resolver_probe,
            persistent_socket: w.persistent_socket,
            request_timeout: Duration::from_millis(w.request_timeout_ms.max(1)),
            max_retries: w.max_retries,
            purgatory_threshold: w.purgatory_threshold,
            purgatory_sentence: Duration::from_millis(w.purgatory_sentence_ms.max(1)),
            cache_capacity: w.cache_capacity,
            cache_min_ttl: Duration::from_secs(w.cache_min_ttl_secs),
            cache_max_ttl: Duration::from_secs(w.cache_max_ttl_secs),
        }
    }
}

impl Default for BlastDNSConfig {
    fn default() -> Self {
        Self {
            max_inflight_per_resolver: DEFAULT_MAX_INFLIGHT_PER_RESOLVER,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            rate_limit: None,
            adaptive: true,
            resolver_probe: false,
            persistent_socket: false,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            purgatory_threshold: DEFAULT_PURGATORY_THRESHOLD,
            purgatory_sentence: DEFAULT_PURGATORY_SENTENCE,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            cache_min_ttl: DEFAULT_CACHE_MIN_TTL,
            cache_max_ttl: DEFAULT_CACHE_MAX_TTL,
        }
    }
}
