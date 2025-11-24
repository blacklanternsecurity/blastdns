use std::time::Duration;

/// Default number of worker tasks spawned per resolver.
pub const DEFAULT_THREADS_PER_RESOLVER: usize = 2;
/// Default timeout in milliseconds used for each resolver request.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(1000);
/// Default number of retry attempts per hostname.
pub const DEFAULT_MAX_RETRIES: usize = 10;
/// Default consecutive error count needed to send a worker to purgatory.
pub const DEFAULT_PURGATORY_THRESHOLD: usize = 5;
/// Default purgatory sentence duration.
pub const DEFAULT_PURGATORY_SENTENCE: Duration = Duration::from_millis(1000);

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
}

impl Default for BlastDNSConfig {
    fn default() -> Self {
        Self {
            threads_per_resolver: DEFAULT_THREADS_PER_RESOLVER,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            purgatory_threshold: DEFAULT_PURGATORY_THRESHOLD,
            purgatory_sentence: DEFAULT_PURGATORY_SENTENCE,
        }
    }
}
