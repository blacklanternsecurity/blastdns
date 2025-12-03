use std::collections::HashMap;
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

    /// Resolve multiple record types for a single hostname in parallel.
    pub async fn resolve_multi<S: Into<String>>(
        &self,
        host: S,
        record_types: Vec<RecordType>,
    ) -> Result<HashMap<RecordType, Result<DnsResponse, BlastDNSError>>, BlastDNSError> {
        if record_types.is_empty() {
            return Err(BlastDNSError::Configuration(
                "at least one record type is required".into(),
            ));
        }

        let host = host.into();
        let futures: Vec<_> = record_types
            .iter()
            .map(|&record_type| {
                let host = host.clone();
                async move {
                    let result = self.resolve(host, record_type).await;
                    (record_type, result)
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        Ok(results.into_iter().collect())
    }

    /// Resolve a batch of hostnames with bounded concurrency and stream the results as they complete.
    pub fn resolve_batch<I, E>(
        self: &Arc<Self>,
        hosts: I,
        record_type: RecordType,
        skip_empty: bool,
        skip_errors: bool,
    ) -> impl stream::Stream<Item = BatchResult> + Unpin + Send + 'static
    where
        I: Iterator<Item = Result<String, E>> + Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        let client = Arc::clone(self);
        let concurrency = client.queue_capacity.max(1);

        // Convert iterator to stream using spawn_blocking to avoid blocking Tokio
        let host_stream = BlockingIteratorStream::new(hosts);

        Box::pin(
            host_stream
                .filter_map(|result| async move {
                    match result {
                        Ok(host) => Some(host),
                        Err(e) => {
                            eprintln!("Iterator error: {}", e);
                            None
                        }
                    }
                })
                .map(move |host| {
                    let client = Arc::clone(&client);
                    let label = host.clone();
                    async move {
                        let result = client.resolve(host, record_type).await;
                        (label, result)
                    }
                })
                .buffer_unordered(concurrency * 2)
                .filter_map(move |(host, result)| async move {
                    // Filter empty responses if skip_empty is true
                    if skip_empty {
                        match &result {
                            Ok(response) if response.answers().is_empty() => return None,
                            _ => {}
                        }
                    }

                    // Filter errors if skip_errors is true
                    if skip_errors && result.is_err() {
                        return None;
                    }

                    Some((host, result))
                }),
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
struct BlockingIteratorStream<I, T> {
    iterator: Arc<Mutex<I>>,
    pending: Option<JoinHandle<Option<T>>>,
}

impl<I, T> BlockingIteratorStream<I, T>
where
    I: Iterator<Item = T> + Send + 'static,
    T: Send + 'static,
{
    fn new(iterator: I) -> Self {
        Self {
            iterator: Arc::new(Mutex::new(iterator)),
            pending: None,
        }
    }
}

impl<I, T> Stream for BlockingIteratorStream<I, T>
where
    I: Iterator<Item = T> + Send + 'static,
    T: Send + 'static,
{
    type Item = T;

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
        let mut stream = client.resolve_batch(
            inputs.into_iter().map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            false,
        );

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

    #[tokio::test]
    async fn resolve_batch_skip_empty_filters_empty_responses() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 1,
            max_retries: 0,
            ..Default::default()
        };

        let client = Arc::new(BlastDNSClient::with_config(resolvers, config).expect("client init"));

        // example.com will return A records, garbage subdomain won't
        let inputs = vec![
            "example.com".to_string(),
            "lkgdjasldkjsdgsdgsdfahwejhori.example.com".to_string(),
        ];

        // First, collect results with skip_empty = false
        let mut stream_all = client.resolve_batch(
            inputs
                .clone()
                .into_iter()
                .map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            false,
        );

        let mut all_results = Vec::new();
        while let Some((host, result)) = stream_all.next().await {
            all_results.push((host, result));
        }

        assert_eq!(
            all_results.len(),
            2,
            "should get both results with skip_empty=false"
        );

        // Find which one has answers and which doesn't
        let (has_answers, empty_or_error): (Vec<_>, Vec<_>) = all_results.iter().partition(
            |(_, result)| matches!(result, Ok(response) if !response.answers().is_empty()),
        );

        assert_eq!(
            has_answers.len(),
            1,
            "should have one result with answers (example.com)"
        );
        assert_eq!(
            empty_or_error.len(),
            1,
            "should have one result without answers"
        );

        // Now test with skip_empty = true
        let mut stream_filtered = client.resolve_batch(
            inputs.into_iter().map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            true,
            false,
        );

        let mut filtered_results = Vec::new();
        while let Some((host, result)) = stream_filtered.next().await {
            filtered_results.push((host, result));
        }

        // With skip_empty=true, should only get example.com (the garbage domain's empty response is filtered)
        assert_eq!(
            filtered_results.len(),
            1,
            "should only get one result with skip_empty=true"
        );
        assert_eq!(filtered_results[0].0, "example.com");

        if let Ok(response) = &filtered_results[0].1 {
            assert!(
                !response.answers().is_empty(),
                "filtered result should have answers"
            );
        } else {
            panic!("example.com should return Ok, not Err");
        }

        // Test that errors still pass through with skip_empty=true
        let bad_resolver_config = BlastDNSConfig {
            request_timeout: Duration::from_millis(100),
            threads_per_resolver: 1,
            max_retries: 0,
            ..Default::default()
        };
        let bad_client = Arc::new(
            BlastDNSClient::with_config(vec!["127.0.0.1:5354".to_string()], bad_resolver_config)
                .expect("client init"),
        );

        let error_inputs = vec!["example.com".to_string()];
        let mut error_stream = bad_client.resolve_batch(
            error_inputs
                .into_iter()
                .map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            true,
            false,
        );

        let mut error_count = 0;
        while let Some((_host, result)) = error_stream.next().await {
            error_count += 1;
            assert!(
                result.is_err(),
                "should get error from non-responsive resolver"
            );
        }

        assert_eq!(
            error_count, 1,
            "errors should pass through even with skip_empty=true"
        );
    }

    #[tokio::test]
    async fn resolve_batch_skip_errors_filters_error_responses() {
        let bad_resolver_config = BlastDNSConfig {
            request_timeout: Duration::from_millis(100),
            threads_per_resolver: 1,
            max_retries: 0,
            ..Default::default()
        };
        let bad_client = Arc::new(
            BlastDNSClient::with_config(vec!["127.0.0.1:5354".to_string()], bad_resolver_config)
                .expect("client init"),
        );

        let error_inputs = vec!["example.com".to_string()];

        // With skip_errors=false, should get error
        let mut stream_with_errors = bad_client.resolve_batch(
            error_inputs
                .clone()
                .into_iter()
                .map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            false,
        );

        let mut error_count = 0;
        while let Some((_host, result)) = stream_with_errors.next().await {
            error_count += 1;
            assert!(
                result.is_err(),
                "should get error from non-responsive resolver"
            );
        }
        assert_eq!(error_count, 1, "should get error with skip_errors=false");

        // With skip_errors=true, should get nothing
        let mut stream_no_errors = bad_client.resolve_batch(
            error_inputs
                .into_iter()
                .map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            true,
        );

        let mut filtered_count = 0;
        while stream_no_errors.next().await.is_some() {
            filtered_count += 1;
        }
        assert_eq!(
            filtered_count, 0,
            "errors should be filtered with skip_errors=true"
        );
    }

    #[tokio::test]
    async fn resolve_multi_rejects_empty_record_types() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let client = BlastDNSClient::new(resolvers).expect("client init");

        let result = client.resolve_multi("example.com", vec![]).await;
        assert!(result.is_err());
        match result {
            Err(BlastDNSError::Configuration(msg)) => {
                assert!(msg.contains("at least one record type"));
            }
            _ => panic!("expected Configuration error"),
        }
    }

    #[tokio::test]
    async fn resolve_multi_resolves_multiple_types() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 2,
            ..Default::default()
        };

        let client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        let record_types = vec![RecordType::A, RecordType::AAAA, RecordType::MX];
        let results = client
            .resolve_multi("example.com", record_types.clone())
            .await
            .expect("resolve_multi failed");

        // Verify all requested record types are in the result
        assert_eq!(results.len(), record_types.len());
        for record_type in record_types {
            assert!(
                results.contains_key(&record_type),
                "missing result for {record_type}"
            );
        }
    }

    #[tokio::test]
    async fn resolve_multi_handles_mixed_success_failure() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 2,
            ..Default::default()
        };

        let client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        // A and AAAA should succeed for example.com, but some exotic types might not have records
        let record_types = vec![RecordType::A, RecordType::AAAA, RecordType::CAA];
        let results = client
            .resolve_multi("example.com", record_types.clone())
            .await
            .expect("resolve_multi failed");

        // All record types should be present in results, even if some failed
        assert_eq!(results.len(), record_types.len());

        // A should succeed
        if let Some(Ok(response)) = results.get(&RecordType::A) {
            assert!(
                !response.answers().is_empty(),
                "A record should have answers"
            );
        } else {
            panic!("A record query should succeed");
        }
    }
}
