use std::time::Duration;

use serde::Deserialize;

/// Default number of worker tasks spawned per resolver.
pub const DEFAULT_THREADS_PER_RESOLVER: usize = 2;
/// Default timeout in milliseconds used for each resolver request.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(1000);
/// Default number of retry attempts per hostname.
pub const DEFAULT_MAX_RETRIES: usize = 10;
/// Default consecutive error count needed to send a worker to purgatory.
pub const DEFAULT_PURGATORY_THRESHOLD: usize = 10;
/// Default purgatory sentence duration.
pub const DEFAULT_PURGATORY_SENTENCE: Duration = Duration::from_millis(1000);
/// Default cache capacity (0 = disabled).
pub const DEFAULT_CACHE_CAPACITY: usize = 10000;
/// Default minimum TTL for cached entries.
pub const DEFAULT_CACHE_MIN_TTL: Duration = Duration::from_secs(10); // 10 minutes
/// Default maximum TTL for cached entries.
pub const DEFAULT_CACHE_MAX_TTL: Duration = Duration::from_secs(86400); // 1 day

/// Configuration knobs for [`BlastDNSClient`].
#[derive(Clone, Debug)]
pub struct BlastDNSConfig {
    /// How many worker tasks are attached to each resolver endpoint.
    pub threads_per_resolver: usize,
    /// Per-request timeout while talking to a resolver.
    pub request_timeout: Duration,
    /// How many times to retry a failed lookup.
    pub max_retries: usize,
    /// Consecutive errors before a worker rests.
    pub purgatory_threshold: usize,
    /// How long a worker must rest after hitting the threshold.
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
    pub threads_per_resolver: usize,
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
            threads_per_resolver: w.threads_per_resolver.max(1),
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
            threads_per_resolver: DEFAULT_THREADS_PER_RESOLVER,
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
