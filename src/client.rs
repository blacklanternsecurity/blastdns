use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crossfire::{MAsyncRx, MAsyncTx, mpmc};
use futures::stream::{self, Stream, StreamExt};
use hickory_client::proto::{rr::RecordType, xfer::DnsResponse};
use tokio::sync::{OnceCell, oneshot};
use tokio::task::JoinHandle;
use tracing::debug;

use crate::{
    config::BlastDNSConfig,
    error::BlastDNSError,
    utils::{check_ulimits, parse_resolver},
    worker::{QuerySpec, ResolverWorker, WorkItem},
};

/// Primary API surface for performing DNS lookups concurrently.
pub struct BlastDNSClient {
    resolvers: Vec<SocketAddr>,
    work_tx: MAsyncTx<WorkItem>,
    work_rx: MAsyncRx<WorkItem>,
    config: BlastDNSConfig,
    queue_capacity: usize,
    workers_spawned: OnceCell<()>,
}

impl std::fmt::Debug for BlastDNSClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlastDNSClient")
            .field("resolvers", &self.resolvers)
            .field("config", &self.config)
            .field("queue_capacity", &self.queue_capacity)
            .finish_non_exhaustive()
    }
}

/// Result item produced by [`BlastDNSClient::resolve_batch`].
pub type BatchResult = (String, Result<DnsResponse, BlastDNSError>);

impl BlastDNSClient {
    /// Build a client using the default configuration.
    pub fn new(resolvers: Vec<String>) -> Result<Self, BlastDNSError> {
        Self::with_config(resolvers, BlastDNSConfig::default())
    }

    /// Build a client with an explicit configuration.
    pub fn with_config(
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

        // Check system ulimits before spawning workers
        check_ulimits(resolver_count, config.threads_per_resolver)
            .map_err(|e| BlastDNSError::Configuration(e.to_string()))?;

        let queue_capacity = (resolver_count * config.threads_per_resolver).max(1);

        let (work_tx, work_rx) = mpmc::bounded_async::<WorkItem>(queue_capacity);

        Ok(Self {
            resolvers: parsed,
            work_tx,
            work_rx,
            config,
            queue_capacity,
            workers_spawned: OnceCell::new(),
        })
    }

    /// Ensure workers are spawned (called lazily on first use).
    async fn ensure_workers(&self) {
        self.workers_spawned
            .get_or_init(|| async {
                self.spawn_workers(self.work_rx.clone());
            })
            .await;
    }

    /// Enqueue a DNS lookup and await the resolver result.
    pub async fn resolve<S: Into<String>>(
        &self,
        host: S,
        record_type: RecordType,
    ) -> Result<DnsResponse, BlastDNSError> {
        self.ensure_workers().await;

        let host = host.into();
        let attempts = self.config.max_retries.saturating_add(1);

        for attempt in 0..attempts {
            debug!(
                attempt = attempt + 1,
                attempts,
                host,
                %record_type,
                "attempting DNS resolution"
            );

            let query = QuerySpec {
                host: host.clone(),
                record_type,
            };

            let (tx, rx) = oneshot::channel();
            let work_item = WorkItem::new(query, tx);

            let response = match self.work_tx.send(work_item).await {
                Ok(_) => match rx.await {
                    Ok(result) => result,
                    Err(_) => Err(BlastDNSError::WorkerDropped),
                },
                Err(err) => {
                    let work_item = err.0;
                    work_item.respond(Err(BlastDNSError::QueueClosed));
                    debug!(host, "failed to enqueue: queue closed");
                    return Err(BlastDNSError::QueueClosed);
                }
            };

            match response {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    debug!(
                        attempt = attempt + 1,
                        attempts,
                        host,
                        error = %err,
                        "DNS resolution attempt failed"
                    );
                    if attempt + 1 == attempts || !err.is_retryable() {
                        return Err(err);
                    }
                }
            }
        }

        Err(BlastDNSError::WorkerDropped)
    }

    /// Resolve a batch of hostnames with bounded concurrency and stream the results as they complete.
    pub fn resolve_batch<I>(
        self: &Arc<Self>,
        hosts: I,
        record_type: RecordType,
    ) -> impl stream::Stream<Item = BatchResult> + Unpin + Send + 'static
    where
        I: Iterator<Item = String> + Send + 'static,
    {
        let client = Arc::clone(self);
        let concurrency = client.queue_capacity.max(1);

        // Convert iterator to stream using spawn_blocking to avoid blocking Tokio
        let host_stream = BlockingIteratorStream::new(hosts);

        Box::pin(
            host_stream
                .map(move |host| {
                    let client = Arc::clone(&client);
                    let label = host.clone();
                    async move {
                        let result = client.resolve(host, record_type).await;
                        (label, result)
                    }
                })
                .buffer_unordered(concurrency * 2),
        )
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

/// Stream adapter that wraps an iterator and polls it via spawn_blocking
struct BlockingIteratorStream<I> {
    iterator: Arc<Mutex<I>>,
    pending: Option<JoinHandle<Option<String>>>,
}

impl<I> BlockingIteratorStream<I>
where
    I: Iterator<Item = String> + Send + 'static,
{
    fn new(iterator: I) -> Self {
        Self {
            iterator: Arc::new(Mutex::new(iterator)),
            pending: None,
        }
    }
}

impl<I> Stream for BlockingIteratorStream<I>
where
    I: Iterator<Item = String> + Send + 'static,
{
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If no pending task, spawn one
        if self.pending.is_none() {
            let iterator = Arc::clone(&self.iterator);
            let handle = tokio::task::spawn_blocking(move || {
                let mut iter = iterator.lock().unwrap();
                iter.next()
            });
            self.pending = Some(handle);
        }

        // Poll the pending task
        let handle = self.pending.as_mut().unwrap();
        match Pin::new(handle).poll(cx) {
            Poll::Ready(Ok(result)) => {
                self.pending = None;
                Poll::Ready(result)
            }
            Poll::Ready(Err(_)) => {
                self.pending = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use crossfire::mpmc;
    use futures::StreamExt;
    use hickory_client::proto::rr::RecordType;
    use tokio::sync::oneshot;

    use crate::utils::parse_resolver;

    use super::*;

    #[test]
    fn rejects_empty_resolvers() {
        let err = BlastDNSClient::new(Vec::new()).expect_err("expected failure");
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
        let resolver: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(1),
            threads_per_resolver: 1,
            ..Default::default()
        };

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
        let resolver: SocketAddr = "[::1]:5353".parse().unwrap();
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(1),
            threads_per_resolver: 1,
            ..Default::default()
        };

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

    #[tokio::test]
    async fn resolve_batch_streams_results() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(1),
            threads_per_resolver: 1,
            ..Default::default()
        };

        let client = Arc::new(BlastDNSClient::with_config(resolvers, config).expect("client init"));

        let inputs = vec!["example.com".to_string(), "example.net".to_string()];
        let expected = inputs.clone();
        let mut stream = client.resolve_batch(inputs.into_iter(), RecordType::A);

        let mut seen = Vec::new();
        while let Some((host, result)) = stream.next().await {
            let response = result.expect("resolution failed");
            assert!(
                !response.answers().is_empty(),
                "resolver returned no answers for {host}"
            );
            seen.push(host);
        }

        let mut seen_sorted = seen;
        seen_sorted.sort();
        let mut expected_sorted = expected;
        expected_sorted.sort();
        assert_eq!(seen_sorted, expected_sorted);
    }
}
