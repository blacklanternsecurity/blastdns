mod cache;
mod client;
mod config;
mod controller;
mod error;
mod health;
mod limiter;
mod mock;
// Only compile Python bindings when "python" feature is enabled or running tests
#[cfg(any(feature = "python", test))]
mod python;
mod resolver;
#[cfg(test)]
mod sim;
mod utils;
mod worker;
pub mod zone_transfer;

pub use client::{BatchResult, BatchResultBasic, BlastDNSClient};
pub use config::{
    BlastDNSConfig, DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_MAX_TTL, DEFAULT_CACHE_MIN_TTL,
    DEFAULT_MAX_CONCURRENCY, DEFAULT_MAX_INFLIGHT_PER_RESOLVER, DEFAULT_MAX_RETRIES,
    DEFAULT_PURGATORY_SENTENCE, DEFAULT_PURGATORY_THRESHOLD, DEFAULT_REQUEST_TIMEOUT,
};
pub use error::BlastDNSError;
pub use health::ResolverStats;
pub use limiter::RateLimiter;
pub use mock::MockBlastDNSClient;
pub use resolver::DnsResolver;
pub use utils::{check_ulimits, get_system_resolvers};
pub use zone_transfer::{ZoneTransferResult, zone_transfer};
