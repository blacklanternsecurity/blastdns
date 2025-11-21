use std::time::Duration;

/// Default number of worker tasks spawned per resolver.
pub const DEFAULT_THREADS_PER_RESOLVER: usize = 1;
/// Default timeout in milliseconds used for each resolver request.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(1000);
/// Default number of retry attempts per hostname.
pub const DEFAULT_MAX_RETRIES: usize = 3;

/// Configuration knobs for [`BlastDNSClient`].
#[derive(Clone, Debug)]
pub struct BlastDNSConfig {
    /// How many worker tasks are attached to each resolver endpoint.
    pub threads_per_resolver: usize,
    /// Per-request timeout while talking to a resolver.
    pub request_timeout: Duration,
    /// Enable debug logging for DNS lookups.
    pub debug: bool,
    /// How many times to retry a failed lookup.
    pub max_retries: usize,
}

impl Default for BlastDNSConfig {
    fn default() -> Self {
        Self {
            threads_per_resolver: DEFAULT_THREADS_PER_RESOLVER,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            debug: false,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}
