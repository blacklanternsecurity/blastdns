use std::sync::Arc;

use crossfire::MAsyncRx;
use hickory_client::{
    client::ClientHandle,
    proto::{
        rr::{DNSClass, Name, RecordType},
        xfer::DnsResponse,
    },
};
use tokio::{sync::oneshot, time::Instant};
use tracing::debug;

use crate::{
    error::BlastDNSError,
    health::{ResolverHealth, ResolverPool},
    limiter::RateLimiter,
};

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

/// Worker that pulls queries off the shared queue and dispatches each one to a
/// resolver chosen from the pool.
///
/// Workers are not bound to a resolver. Capacity per resolver is enforced by the
/// pool's per-resolver permits, so the worker count sets total concurrency
/// independently of how many resolvers are configured.
pub(crate) struct ResolverWorker {
    pool: Arc<ResolverPool>,
    limiter: Arc<RateLimiter>,
    work_rx: MAsyncRx<WorkItem>,
}

impl ResolverWorker {
    /// Spawns a new worker task.
    pub fn spawn(
        pool: Arc<ResolverPool>,
        limiter: Arc<RateLimiter>,
        work_rx: MAsyncRx<WorkItem>,
        worker_idx: usize,
    ) {
        tokio::spawn(async move {
            let worker = Self {
                pool,
                limiter,
                work_rx,
            };
            worker.run().await;
            debug!(worker_idx, "resolver worker shutting down");
        });
    }

    /// Main worker loop that receives and dispatches queries until the channel
    /// closes.
    async fn run(self) {
        while let Ok(work_item) = self.work_rx.recv().await {
            let WorkItem { query, responder } = work_item;

            // Reject unusable hostnames before spending a dispatch slot.
            let name = match Name::from_ascii(&query.host) {
                Ok(name) => name,
                Err(source) => {
                    let _ = responder.send(Err(BlastDNSError::InvalidHostname {
                        name: query.host,
                        source,
                    }));
                    continue;
                }
            };

            // Pace dispatch before taking capacity, so the configured rate
            // governs sends instead of being distorted by completion timing.
            self.limiter.acquire().await;

            let Some(reservation) = self.pool.reserve().await else {
                let _ = responder.send(Err(BlastDNSError::NoResolvers));
                continue;
            };

            // Respect the chosen resolver's own pacing on top of the global rate.
            reservation.resolver.acquire_rate().await;

            let result = Self::query(&reservation.resolver, name, query.record_type).await;
            let _ = responder.send(result);
        }
    }

    /// Re-ask a truncated query over TCP. Returns `None` if TCP is unavailable,
    /// leaving the caller with the partial UDP answer, which still beats nothing.
    async fn refetch_over_tcp(
        health: &ResolverHealth,
        name: Name,
        record_type: RecordType,
    ) -> Option<DnsResponse> {
        let mut tcp = match health.tcp_client().await {
            Ok(tcp) => tcp,
            Err(err) => {
                debug!(resolver = %health.addr(), %err, "TCP unavailable for truncated response");
                return None;
            }
        };
        match tcp.query(name, DNSClass::IN, record_type).await {
            Ok(response) => Some(response),
            Err(err) => {
                debug!(resolver = %health.addr(), %err, "TCP refetch failed");
                None
            }
        }
    }

    /// Executes a single query against one resolver and records the outcome.
    async fn query(
        health: &ResolverHealth,
        name: Name,
        record_type: RecordType,
    ) -> Result<DnsResponse, BlastDNSError> {
        health.record_dispatch();

        let mut client = match health.client().await {
            Ok(client) => client,
            Err(err) => {
                health.record_error(false);
                return Err(err);
            }
        };

        debug!(
            resolver = %health.addr(),
            %name,
            %record_type,
            "querying DNS resolver"
        );

        let started = Instant::now();
        match client.query(name.clone(), DNSClass::IN, record_type).await {
            Ok(response) if response.truncated() => {
                // TC means the server dropped records to fit the UDP payload, so
                // what arrived is an arbitrary subset. Re-ask over TCP, which has
                // no size limit, rather than reporting a partial answer as whole.
                health.record_truncated();
                let full = Self::refetch_over_tcp(health, name, record_type).await;
                let response = full.unwrap_or(response);
                health.record_response(started.elapsed(), !response.answers().is_empty());
                Ok(response)
            }
            Ok(response) => {
                health.record_response(started.elapsed(), !response.answers().is_empty());
                Ok(response)
            }
            Err(source) => {
                let err = BlastDNSError::ResolverRequestFailed {
                    resolver: health.addr(),
                    source,
                };
                health.record_error(err.is_timeout());
                Err(err)
            }
        }
    }
}
