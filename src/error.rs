use std::net::{AddrParseError, SocketAddr};

use hickory_client::{
    ClientError, ClientErrorKind,
    proto::{ProtoError, ProtoErrorKind},
};
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
    #[error("resolver {resolver} did not answer within {timeout:?}")]
    QueryTimedOut {
        resolver: SocketAddr,
        timeout: std::time::Duration,
    },
    #[error("configuration error: {0}")]
    Configuration(String),
}

impl BlastDNSError {
    /// Returns `true` when the error is transient and worth retrying.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            BlastDNSError::ResolverRequestFailed { .. }
                | BlastDNSError::QueryTimedOut { .. }
                | BlastDNSError::WorkerDropped
        )
    }

    /// Returns `true` when the failure says something about how hard the path is
    /// being driven, rather than about the input.
    ///
    /// A name that does not parse fails identically at one query per second and at
    /// fifty thousand, so counting it as loss lets a wordlist's junk entries
    /// throttle a scan that was never in trouble. A query that went out and got
    /// nothing back, or one that could not find a live resolver because the pool
    /// has been driven into purgatory, is evidence about the path and counts.
    pub fn is_congestion_evidence(&self) -> bool {
        !matches!(
            self,
            BlastDNSError::InvalidHostname { .. }
                | BlastDNSError::InvalidResolver { .. }
                | BlastDNSError::QueueClosed
                | BlastDNSError::Configuration(_)
        )
    }

    /// Returns `true` when the query got no response at all, as opposed to
    /// failing for some other reason. Over UDP this is the loss signal.
    pub fn is_timeout(&self) -> bool {
        if matches!(self, BlastDNSError::QueryTimedOut { .. }) {
            return true;
        }
        let BlastDNSError::ResolverRequestFailed { source, .. } = self else {
            return false;
        };
        match source.kind() {
            ClientErrorKind::Timeout => true,
            ClientErrorKind::Io(e) => e.kind() == std::io::ErrorKind::TimedOut,
            ClientErrorKind::Proto(e) => matches!(e.kind(), ProtoErrorKind::Timeout),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BlastDNSError;
    use hickory_client::proto::ProtoError;

    #[test]
    fn retryable_errors_flagged() {
        assert!(BlastDNSError::WorkerDropped.is_retryable());
    }

    #[test]
    fn non_retryable_errors_rejected() {
        assert!(!BlastDNSError::QueueClosed.is_retryable());
    }

    #[test]
    fn unusable_input_is_not_congestion() {
        // The adaptive controller reads failed queries as a sign the path is
        // saturated. A hostname that never parsed never reached the path.
        let err = BlastDNSError::InvalidHostname {
            name: "not a hostname".into(),
            source: ProtoError::from("bad name"),
        };
        assert!(!err.is_congestion_evidence());
    }

    #[test]
    fn queries_that_went_out_and_got_nothing_are_congestion() {
        assert!(
            BlastDNSError::QueryTimedOut {
                resolver: "127.0.0.1:53".parse().unwrap(),
                timeout: std::time::Duration::from_secs(5),
            }
            .is_congestion_evidence()
        );
        assert!(BlastDNSError::NoResolvers.is_congestion_evidence());
    }
}
