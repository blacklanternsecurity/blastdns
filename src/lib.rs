mod client;
mod config;
mod error;
// Only compile Python bindings when "python" feature is enabled or running tests
#[cfg(any(feature = "python", test))]
mod python;
mod utils;
mod worker;

pub use client::{BatchResult, BlastDNSClient};
pub use config::{
    BlastDNSConfig, DEFAULT_MAX_RETRIES, DEFAULT_PURGATORY_SENTENCE, DEFAULT_PURGATORY_THRESHOLD,
    DEFAULT_REQUEST_TIMEOUT, DEFAULT_THREADS_PER_RESOLVER,
};
pub use error::BlastDNSError;
pub use utils::check_ulimits;
