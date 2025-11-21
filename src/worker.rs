use std::net::SocketAddr;

use crossfire::MAsyncRx;
use hickory_client::{
    client::{Client, ClientHandle},
    proto::{
        rr::{DNSClass, Name, RecordType},
        runtime::TokioRuntimeProvider,
        udp::UdpClientStream,
        xfer::DnsResponse,
    },
};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::{BlastDNSConfig, error::BlastDNSError};

/// DNS query specification containing the hostname and record type to query.
#[derive(Debug)]
pub(crate) struct QuerySpec {
    pub(crate) host: String,
    pub(crate) record_type: RecordType,
}

/// Work item containing a query and a channel to send the response back.
pub(crate) struct WorkItem {
    pub(crate) query: QuerySpec,
    pub(crate) responder: oneshot::Sender<Result<DnsResponse, BlastDNSError>>,
}

impl WorkItem {
    /// Creates a new work item with the given query and response channel.
    pub(crate) fn new(
        query: QuerySpec,
        responder: oneshot::Sender<Result<DnsResponse, BlastDNSError>>,
    ) -> Self {
        Self { query, responder }
    }

    /// Sends the query result back through the response channel.
    pub(crate) fn respond(self, result: Result<DnsResponse, BlastDNSError>) {
        let _ = self.responder.send(result);
    }
}

/// Worker that processes DNS queries by forwarding them to a resolver.
pub(crate) struct ResolverWorker {
    resolver: SocketAddr,
    config: BlastDNSConfig,
    work_rx: MAsyncRx<WorkItem>,
}

impl ResolverWorker {
    /// Spawns a new resolver worker task.
    pub fn spawn(
        resolver: SocketAddr,
        work_rx: MAsyncRx<WorkItem>,
        config: BlastDNSConfig,
        worker_idx: usize,
    ) {
        tokio::spawn(async move {
            let resolver_addr = resolver;
            let worker = Self {
                resolver: resolver_addr,
                config,
                work_rx,
            };

            match worker.run().await {
                Ok(()) => debug!("resolver worker {resolver_addr} (#{worker_idx}) shutting down"),
                Err(err) => {
                    warn!("resolver worker {resolver_addr} (#{worker_idx}) exited: {err:?}")
                }
            }
        });
    }

    /// Main worker loop that receives and processes queries until the channel closes.
    async fn run(self) -> Result<(), BlastDNSError> {
        let mut client = self.init_client().await?;

        while let Ok(work_item) = self.work_rx.recv().await {
            let WorkItem { query, responder } = work_item;
            let result = self.handle_query(&mut client, query).await;
            let _ = responder.send(result);
        }

        Ok(())
    }

    /// Initializes a DNS client connected to the configured resolver.
    async fn init_client(&self) -> Result<Client, BlastDNSError> {
        let provider = TokioRuntimeProvider::new();
        let stream = UdpClientStream::builder(self.resolver, provider)
            .with_timeout(Some(self.config.request_timeout))
            .build();

        let (client, bg) =
            Client::connect(stream)
                .await
                .map_err(|source| BlastDNSError::ResolverSetupFailed {
                    resolver: self.resolver,
                    source,
                })?;

        let resolver = self.resolver;
        tokio::spawn(async move {
            if let Err(err) = bg.await {
                warn!("resolver {resolver} background task exited: {err}");
            }
        });

        Ok(client)
    }

    /// Executes a DNS query using the client and returns the response.
    async fn handle_query(
        &self,
        client: &mut Client,
        query: QuerySpec,
    ) -> Result<DnsResponse, BlastDNSError> {
        let QuerySpec { host, record_type } = query;

        if self.config.debug {
            eprintln!("[{}] Querying {} {}", self.resolver, host, record_type);
        }

        let name = Name::from_ascii(&host)
            .map_err(|source| BlastDNSError::InvalidHostname { name: host, source })?;

        client
            .query(name, DNSClass::IN, record_type)
            .await
            .map_err(|source| BlastDNSError::ResolverRequestFailed {
                resolver: self.resolver,
                source,
            })
    }
}
