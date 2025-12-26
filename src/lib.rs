mod cache;
mod client;
mod config;
mod error;
mod mock;
// Only compile Python bindings when "python" feature is enabled or running tests
#[cfg(any(feature = "python", test))]
mod python;
mod resolver;
mod utils;
mod worker;

pub use client::{BatchResult, BatchResultBasic, BlastDNSClient};
pub use config::{
    BlastDNSConfig, DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_MAX_TTL, DEFAULT_CACHE_MIN_TTL,
    DEFAULT_MAX_RETRIES, DEFAULT_PURGATORY_SENTENCE, DEFAULT_PURGATORY_THRESHOLD,
    DEFAULT_REQUEST_TIMEOUT, DEFAULT_THREADS_PER_RESOLVER,
};
pub use error::BlastDNSError;
pub use mock::MockBlastDNSClient;
pub use resolver::DnsResolver;
pub use utils::{check_ulimits, get_system_resolvers};
