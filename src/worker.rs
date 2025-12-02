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
use tokio::{sync::oneshot, time::sleep};
use tracing::debug;

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
    client: Option<Client>,
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
                client: None,
            };

            match worker.run().await {
                Ok(()) => debug!("resolver worker {resolver_addr} (#{worker_idx}) shutting down"),
                Err(err) => {
                    eprintln!("resolver worker {resolver_addr} (#{worker_idx}) exited: {err:?}")
                }
            }
        });
    }

    /// Main worker loop that receives and processes queries until the channel closes.
    async fn run(mut self) -> Result<(), BlastDNSError> {
        let mut consecutive_errors = 0usize;

        loop {
            if self.config.purgatory_threshold > 0
                && consecutive_errors >= self.config.purgatory_threshold
            {
                let sentence = self.config.purgatory_sentence;
                if !sentence.is_zero() {
                    debug!(
                        resolver = %self.resolver,
                        sentence = ?sentence,
                        consecutive_errors,
                        "entering purgatory"
                    );
                    sleep(sentence).await;
                }
                consecutive_errors = consecutive_errors.saturating_sub(1);
            }

            let work_item = match self.work_rx.recv().await {
                Ok(item) => item,
                Err(_) => break,
            };

            // Lazy initialization: create client on first use
            if self.client.is_none() {
                self.client = Some(self.init_client().await?);
            }

            let WorkItem { query, responder } = work_item;
            match self.handle_query(query).await {
                Ok(response) => {
                    consecutive_errors = consecutive_errors.saturating_sub(1);
                    let _ = responder.send(Ok(response));
                }
                Err(err) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    let _ = responder.send(Err(err));
                }
            }
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
                eprintln!("resolver {resolver} background task exited: {err}");
            }
        });

        Ok(client)
    }

    /// Executes a DNS query using the client and returns the response.
    async fn handle_query(&mut self, query: QuerySpec) -> Result<DnsResponse, BlastDNSError> {
        let QuerySpec { host, record_type } = query;

        debug!(
            resolver = %self.resolver,
            host,
            %record_type,
            "querying DNS resolver"
        );

        let name = Name::from_ascii(&host)
            .map_err(|source| BlastDNSError::InvalidHostname { name: host, source })?;

        self.client
            .as_mut()
            .unwrap()
            .query(name, DNSClass::IN, record_type)
            .await
            .map_err(|source| BlastDNSError::ResolverRequestFailed {
                resolver: self.resolver,
                source,
            })
    }
}
