use std::net::SocketAddr;

mod config;
mod error;
mod utils;
mod worker;

use crossfire::{MAsyncRx, MAsyncTx, mpmc};
use hickory_client::proto::{rr::RecordType, xfer::DnsResponse};
use tokio::sync::oneshot;
use utils::parse_resolver;
use worker::{QuerySpec, ResolverWorker, WorkItem};

pub use config::{BlastDNSConfig, DEFAULT_THREADS_PER_RESOLVER, DEFAULT_REQUEST_TIMEOUT};
pub use error::BlastDNSError;

/// Primary API surface for performing DNS lookups concurrently.
#[derive(Debug)]
pub struct BlastDNSClient {
    resolvers: Vec<SocketAddr>,
    work_tx: MAsyncTx<WorkItem>,
    config: BlastDNSConfig,
}

impl BlastDNSClient {
    /// Build a client using the default configuration.
    pub async fn new(resolvers: Vec<String>) -> Result<Self, BlastDNSError> {
        Self::with_config(resolvers, BlastDNSConfig::default()).await
    }

    /// Build a client with an explicit configuration.
    pub async fn with_config(
        resolvers: Vec<String>,
        config: BlastDNSConfig,
    ) -> Result<Self, BlastDNSError> {
        if resolvers.is_empty() {
            return Err(BlastDNSError::NoResolvers);
        }

        let parsed: Vec<SocketAddr> = resolvers
            .into_iter()
            .map(|input| parse_resolver(&input))
            .collect::<Result<_, _>>()?;

        let resolver_count = parsed.len();
        let queue_capacity = (resolver_count * config.threads_per_resolver).max(1);

        let (work_tx, work_rx) = mpmc::bounded_async::<WorkItem>(queue_capacity);

        let client = Self {
            resolvers: parsed,
            work_tx,
            config,
        };
        client.spawn_workers(work_rx);

        Ok(client)
    }

    /// Enqueue a DNS lookup and await the resolver result.
    pub async fn resolve<S: Into<String>>(
        &self,
        host: S,
        record_type: RecordType,
    ) -> Result<DnsResponse, BlastDNSError> {
        let query = QuerySpec {
            host: host.into(),
            record_type,
        };

        let (tx, rx) = oneshot::channel();
        let work_item = WorkItem::new(query, tx);

        match self.work_tx.send(work_item).await {
            Ok(_) => match rx.await {
                Ok(result) => result,
                Err(_) => Err(BlastDNSError::WorkerDropped),
            },
            Err(err) => {
                let work_item = err.0;
                work_item.respond(Err(BlastDNSError::QueueClosed));
                Err(BlastDNSError::QueueClosed)
            }
        }
    }

    fn spawn_workers(&self, work_rx: MAsyncRx<WorkItem>) {
        let threads = self.config.threads_per_resolver.max(1);

        for &resolver in &self.resolvers {
            for worker_idx in 0..threads {
                ResolverWorker::spawn(resolver, work_rx.clone(), self.config.clone(), worker_idx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hickory_client::proto::rr::RecordType;

    use super::*;

    #[tokio::test]
    async fn rejects_empty_resolvers() {
        let err = BlastDNSClient::new(Vec::new())
            .await
            .expect_err("expected failure");
        assert!(matches!(err, BlastDNSError::NoResolvers));
    }


    #[test]
    fn parse_resolver_accepts_portless_ip() {
        let addr = parse_resolver("203.0.113.10").expect("should parse");
        assert_eq!(addr, SocketAddr::from(([203, 0, 113, 10], 53)));
    }

    #[test]
    fn parse_resolver_rejects_garbage() {
        let err = parse_resolver("not-an-ip").expect_err("should fail");
        assert!(matches!(err, BlastDNSError::InvalidResolver { .. }));
    }

    #[tokio::test]
    async fn resolver_worker_handles_real_resolver() {
        let resolver: SocketAddr = "127.0.0.1:53".parse().unwrap();
        let mut config = BlastDNSConfig::default();
        config.request_timeout = Duration::from_secs(1);
        config.threads_per_resolver = 1;

        let (tx, rx) = mpmc::bounded_async::<WorkItem>(1);
        ResolverWorker::spawn(resolver, rx, config.clone(), 0);

        let query = QuerySpec {
            host: "example.com.".into(),
            record_type: RecordType::A,
        };
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(WorkItem::new(query, resp_tx)).await.unwrap();

        let response = resp_rx
            .await
            .expect("oneshot dropped")
            .expect("worker resolution");
        assert!(
            !response.answers().is_empty(),
            "resolver returned no answers"
        );
    }

    #[test]
    fn parse_resolver_accepts_ipv6() {
        let addr = parse_resolver("[::1]:53").expect("should parse");
        assert_eq!(addr.ip().to_string(), "::1");
        assert_eq!(addr.port(), 53);
    }

    #[test]
    fn parse_resolver_accepts_portless_ipv6() {
        let addr = parse_resolver("::1").expect("should parse");
        assert_eq!(addr.ip().to_string(), "::1");
        assert_eq!(addr.port(), 53);
    }

    #[tokio::test]
    async fn resolver_worker_handles_ipv6_resolver() {
        let resolver: SocketAddr = "[::1]:53".parse().unwrap();
        let mut config = BlastDNSConfig::default();
        config.request_timeout = Duration::from_secs(1);
        config.threads_per_resolver = 1;

        let (tx, rx) = mpmc::bounded_async::<WorkItem>(1);
        ResolverWorker::spawn(resolver, rx, config.clone(), 0);

        let query = QuerySpec {
            host: "example.com.".into(),
            record_type: RecordType::A,
        };
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(WorkItem::new(query, resp_tx)).await.unwrap();

        let response = resp_rx
            .await
            .expect("oneshot dropped")
            .expect("worker resolution");
        assert!(
            !response.answers().is_empty(),
            "resolver returned no answers"
        );
    }
}
