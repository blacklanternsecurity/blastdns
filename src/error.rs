use std::net::{AddrParseError, SocketAddr};

use hickory_client::{ClientError, proto::ProtoError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlastDNSError {
    #[error("no resolvers provided")]
    NoResolvers,
    #[error("invalid resolver `{resolver}`")]
    InvalidResolver {
        resolver: String,
        #[source]
        source: AddrParseError,
    },
    #[error("invalid hostname `{name}`")]
    InvalidHostname {
        name: String,
        #[source]
        source: ProtoError,
    },
    #[error("request queue closed")]
    QueueClosed,
    #[error("resolver workers dropped before delivering a response")]
    WorkerDropped,
    #[error("failed to initialize resolver {resolver}")]
    ResolverSetupFailed {
        resolver: SocketAddr,
        #[source]
        source: ProtoError,
    },
    #[error("resolver {resolver} query failed")]
    ResolverRequestFailed {
        resolver: SocketAddr,
        #[source]
        source: ClientError,
    },
    #[error("configuration error: {0}")]
    Configuration(String),
}

impl BlastDNSError {
    /// Returns `true` when the error is transient and worth retrying.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            BlastDNSError::ResolverRequestFailed { .. } | BlastDNSError::WorkerDropped
        )
    }
}

#[cfg(test)]
mod tests {
    use super::BlastDNSError;

    #[test]
    fn retryable_errors_flagged() {
        assert!(BlastDNSError::WorkerDropped.is_retryable());
    }

    #[test]
    fn non_retryable_errors_rejected() {
        assert!(!BlastDNSError::QueueClosed.is_retryable());
    }
}
