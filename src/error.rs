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
}

