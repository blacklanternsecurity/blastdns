mod client;
mod config;
mod error;
mod utils;
mod worker;

pub use client::{BatchResult, BlastDNSClient};
pub use config::{
    BlastDNSConfig, DEFAULT_MAX_RETRIES, DEFAULT_REQUEST_TIMEOUT, DEFAULT_THREADS_PER_RESOLVER,
};
pub use error::BlastDNSError;
