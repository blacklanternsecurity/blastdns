use std::{net::SocketAddr, sync::Arc, time::Instant};

use crossfire::{MAsyncRx, MAsyncTx, mpmc};
use hickory_client::proto::{op::Query, rr::RecordType, xfer::DnsResponse};
use tokio::sync::{OnceCell, oneshot};
use tracing::debug;

use crate::{
    cache::SimpleCache,
    config::BlastDNSConfig,
    error::BlastDNSError,
    resolver::DnsResolver,
    utils::{check_ulimits, format_ptr_query, get_system_resolvers, parse_resolver},
    worker::{QuerySpec, ResolverWorker, WorkItem},
};

/// Primary API surface for performing DNS lookups concurrently.
#[derive(Clone)]
pub struct BlastDNSClient {
    resolvers: Vec<SocketAddr>,
    work_tx: MAsyncTx<WorkItem>,
    work_rx: MAsyncRx<WorkItem>,
    config: BlastDNSConfig,
    queue_capacity: usize,
    workers_spawned: OnceCell<()>,
    cache: Option<Arc<SimpleCache>>,
}

impl std::fmt::Debug for BlastDNSClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlastDNSClient")
            .field("resolvers", &self.resolvers)
            .field("config", &self.config)
            .field("queue_capacity", &self.queue_capacity)
            .field("cache_enabled", &self.cache.is_some())
            .finish_non_exhaustive()
    }
}

/// Result item produced by [`BlastDNSClient::resolve_batch_full`].
pub type BatchResult = (String, Result<DnsResponse, BlastDNSError>);

/// Result item produced by [`BlastDNSClient::resolve_batch`].
pub type BatchResultBasic = (String, String, Vec<String>);

impl BlastDNSClient {
    /// Build a client using the default configuration.
    /// If resolvers is empty, system resolvers will be used.
    pub fn new(resolvers: Vec<String>) -> Result<Self, BlastDNSError> {
        Self::with_config(resolvers, BlastDNSConfig::default())
    }

    /// Build a client with an explicit configuration.
    /// If resolvers is empty, system resolvers will be used.
    pub fn with_config(
        resolvers: Vec<String>,
        config: BlastDNSConfig,
    ) -> Result<Self, BlastDNSError> {
        let resolvers = if resolvers.is_empty() {
            // Get system resolvers and format them as strings
            let system_ips = get_system_resolvers()?;
            system_ips
                .into_iter()
                .map(|ip| format!("{}:53", ip))
                .collect()
        } else {
            resolvers
        };

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

        // Initialize cache if capacity > 0
        let cache = (config.cache_capacity > 0).then(|| {
            Arc::new(SimpleCache::new(
                config.cache_capacity,
                config.cache_min_ttl,
                config.cache_max_ttl,
            ))
        });

        Ok(Self {
            resolvers: parsed,
            work_tx,
            work_rx,
            config,
            queue_capacity,
            workers_spawned: OnceCell::new(),
            cache,
        })
    }

    /// Get the list of resolvers being used by this client.
    pub fn resolvers(&self) -> Vec<String> {
        self.resolvers.iter().map(|addr| addr.to_string()).collect()
    }

    /// Ensure workers are spawned (called lazily on first use).
    async fn ensure_workers(&self) {
        self.workers_spawned
            .get_or_init(|| async {
                self.spawn_workers(self.work_rx.clone());
            })
            .await;
    }

    /// Enqueue a DNS lookup and await the full resolver result.
    pub async fn resolve_full(
        &self,
        mut host: String,
        record_type: RecordType,
    ) -> Result<DnsResponse, BlastDNSError> {
        self.ensure_workers().await;

        // Auto-format PTR queries if an IP address is provided
        if record_type == RecordType::PTR {
            host = format_ptr_query(&host);
        }

        // Check cache first
        if let Some(cache) = &self.cache {
            // Try to parse hostname into a Name
            if let Ok(name) = host.parse() {
                let query = Query::query(name, record_type);

                if let Some(response) = cache.get(&query, Instant::now()) {
                    debug!(host, %record_type, "cache hit");
                    return Ok(response.as_ref().clone());
                }
            }
        }

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
                Ok(resp) => {
                    // Cache successful responses with answers
                    if let Some(cache) = &self.cache
                        && !resp.answers().is_empty()
                        && let Ok(name) = host.parse()
                    {
                        let query = Query::query(name, record_type);
                        cache.insert(query, resp.clone(), Instant::now());
                        debug!(host, %record_type, "cached response");
                    }
                    return Ok(resp);
                }
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

    fn spawn_workers(&self, work_rx: MAsyncRx<WorkItem>) {
        let threads = self.config.threads_per_resolver.max(1);

        for &resolver in &self.resolvers {
            for worker_idx in 0..threads {
                ResolverWorker::spawn(resolver, work_rx.clone(), self.config.clone(), worker_idx);
            }
        }
    }
}

// Implement the DnsResolver trait
impl DnsResolver for BlastDNSClient {
    fn resolve_full(
        &self,
        host: String,
        record_type: RecordType,
    ) -> impl std::future::Future<Output = Result<DnsResponse, BlastDNSError>> + Send {
        // Delegate to the existing method
        self.resolve_full(host, record_type)
    }

    fn get_concurrency(&self) -> usize {
        self.queue_capacity.max(1)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use crossfire::mpmc;
    use futures::StreamExt;
    use hickory_client::proto::op::Query;
    use hickory_client::proto::rr::{Name, RecordType};
    use tokio::sync::oneshot;

    use crate::utils::parse_resolver;

    use super::*;

    #[test]
    fn empty_resolvers_uses_system() {
        // This test verifies that empty resolvers fall back to system resolvers
        // We don't test the actual behavior as it requires system DNS config
        let result = BlastDNSClient::new(Vec::new());
        // Should succeed if system resolvers are available, fail otherwise
        // The exact behavior depends on the system, so we just verify it doesn't panic
        match result {
            Ok(_) => {
                // System resolvers found
            }
            Err(BlastDNSError::Configuration(_)) => {
                // Expected if no system resolvers configured
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
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
    async fn resolve_batch_full_streams_results() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(1),
            threads_per_resolver: 1,
            ..Default::default()
        };

        let client = Arc::new(BlastDNSClient::with_config(resolvers, config).expect("client init"));

        let inputs = vec!["example.com".to_string(), "example.net".to_string()];
        let expected = inputs.clone();
        let mut stream = client.resolve_batch_full(
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
    async fn resolve_batch_full_skip_empty_filters_empty_responses() {
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
        let mut stream_all = client.clone().resolve_batch_full(
            inputs
                .clone()
                .into_iter()
                .map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            false,
        );

        let mut all_results: Vec<BatchResult> = Vec::new();
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
        let mut stream_filtered = client.resolve_batch_full(
            inputs.into_iter().map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            true,
            false,
        );

        let mut filtered_results: Vec<BatchResult> = Vec::new();
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
        let mut error_stream = bad_client.resolve_batch_full(
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
    async fn resolve_batch_full_skip_errors_filters_error_responses() {
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
        let mut stream_with_errors = bad_client.clone().resolve_batch_full(
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
        let mut stream_no_errors = bad_client.resolve_batch_full(
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
    async fn resolve_multi_full_rejects_empty_record_types() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let client = BlastDNSClient::new(resolvers).expect("client init");

        let result = client
            .resolve_multi_full("example.com".to_string(), vec![])
            .await;
        assert!(result.is_err());
        match result {
            Err(BlastDNSError::Configuration(msg)) => {
                assert!(msg.contains("at least one record type"));
            }
            _ => panic!("expected Configuration error"),
        }
    }

    #[tokio::test]
    async fn resolve_multi_full_resolves_multiple_types() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 2,
            ..Default::default()
        };

        let client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        let record_types = vec![RecordType::A, RecordType::AAAA, RecordType::MX];
        let results = client
            .resolve_multi_full("example.com".to_string(), record_types.clone())
            .await
            .expect("resolve_multi_full failed");

        // Verify all requested record types are in the result and succeeded
        assert_eq!(results.len(), record_types.len());
        for record_type in &record_types {
            let result = results
                .get(record_type)
                .unwrap_or_else(|| panic!("missing result for {record_type}"));
            match result {
                Ok(response) => {
                    assert!(
                        !response.answers().is_empty(),
                        "{record_type} query should have answers"
                    );
                }
                Err(e) => panic!("{record_type} query should succeed, got error: {e:?}"),
            }
        }
    }

    #[tokio::test]
    async fn resolve_multi_full_handles_mixed_success_failure() {
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
            .resolve_multi_full("example.com".to_string(), record_types.clone())
            .await
            .expect("resolve_multi_full failed");

        // All record types should be present in results, even if some failed
        assert_eq!(results.len(), record_types.len());

        // A should succeed
        let a_result = results
            .get(&RecordType::A)
            .expect("A record must be present in results");
        match a_result {
            Ok(response) => {
                assert!(
                    !response.answers().is_empty(),
                    "A record should have answers"
                );
            }
            Err(e) => panic!("A record query should succeed, got error: {e:?}"),
        }

        // AAAA should also succeed
        let aaaa_result = results
            .get(&RecordType::AAAA)
            .expect("AAAA record must be present in results");
        match aaaa_result {
            Ok(response) => {
                assert!(
                    !response.answers().is_empty(),
                    "AAAA record should have answers"
                );
            }
            Err(e) => panic!("AAAA record query should succeed, got error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_returns_answer_strings() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 1,
            ..Default::default()
        };

        let client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        let answers = client
            .resolve("example.com".to_string(), RecordType::A)
            .await
            .expect("resolve failed");

        assert!(
            answers.len() > 1,
            "should have multiple answers, got {}",
            answers.len()
        );

        // Verify answer format (should be just IP addresses)
        for answer in &answers {
            assert!(
                answer.parse::<std::net::IpAddr>().is_ok(),
                "should be a valid IP address: {}",
                answer
            );
        }
    }

    #[tokio::test]
    async fn resolve_multi_filters_successful_queries() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 2,
            ..Default::default()
        };

        let client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        let record_types = vec![RecordType::A, RecordType::AAAA];
        let results = client
            .resolve_multi("example.com".to_string(), record_types.clone())
            .await
            .expect("resolve_multi failed");

        // Should have results for A and AAAA (both should succeed for example.com)
        assert_eq!(
            results.len(),
            2,
            "should have exactly 2 record types (A and AAAA)"
        );

        // Verify A record is present
        let a_answers = results
            .get(&RecordType::A)
            .expect("A record must be present in results");
        assert!(
            a_answers.len() > 1,
            "A record should have multiple answers, got {}",
            a_answers.len()
        );
        // Verify A record format
        for answer in a_answers {
            assert!(
                answer.parse::<std::net::IpAddr>().is_ok(),
                "should be a valid IP address: {}",
                answer
            );
        }

        // Verify AAAA record is present
        let aaaa_answers = results
            .get(&RecordType::AAAA)
            .expect("AAAA record must be present in results");
        assert!(
            aaaa_answers.len() > 1,
            "AAAA record should have multiple answers, got {}",
            aaaa_answers.len()
        );
        // Verify AAAA record format
        for answer in aaaa_answers {
            assert!(
                answer.parse::<std::net::IpAddr>().is_ok(),
                "should be a valid IPv6 address: {}",
                answer
            );
        }
    }

    #[tokio::test]
    async fn resolve_auto_formats_ptr_with_ipv4() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 1,
            ..Default::default()
        };

        let client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        // Pass raw IP address for PTR query
        let answers = client
            .resolve("8.8.8.8".to_string(), RecordType::PTR)
            .await
            .expect("resolve failed");

        assert!(!answers.is_empty(), "should have PTR answers");
        // PTR results should be domain names
        for answer in &answers {
            assert!(answer.contains('.'), "PTR should return domain names");
        }
    }

    #[tokio::test]
    async fn resolve_batch_streams_simplified_tuples() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 1,
            ..Default::default()
        };

        let client = Arc::new(BlastDNSClient::with_config(resolvers, config).expect("client init"));

        let inputs = vec!["example.com".to_string(), "example.net".to_string()];
        let expected = inputs.clone();
        let mut stream = client.resolve_batch(
            inputs.into_iter().map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
        );

        let mut seen = Vec::new();
        while let Some((host, record_type, answers)) = stream.next().await {
            assert_eq!(record_type, "A", "record type should be A");
            assert!(
                answers.len() > 1,
                "should have multiple answers, got {}",
                answers.len()
            );

            // Verify answer format
            for answer in &answers {
                assert!(
                    answer.parse::<std::net::IpAddr>().is_ok(),
                    "should be a valid IP address: {}",
                    answer
                );
            }

            seen.push(host);
        }

        let mut seen_sorted = seen;
        seen_sorted.sort();
        let mut expected_sorted = expected;
        expected_sorted.sort();
        assert_eq!(seen_sorted, expected_sorted);
    }

    #[tokio::test]
    async fn nxdomain_behavior_matches_mock() {
        use crate::mock::MockBlastDNSClient;
        use crate::resolver::DnsResolver;
        use rand::{Rng, distributions::Alphanumeric};

        // Create real client
        let resolvers = vec!["8.8.8.8:53".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 1,
            max_retries: 0,
            ..Default::default()
        };
        let real_client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        // Generate random 32-character domain name
        let random_subdomain: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect::<String>()
            .to_lowercase();
        let fake_domain = format!("{}.com", random_subdomain);

        // Create mock client with NXDOMAIN configured
        let mut mock_client = MockBlastDNSClient::new();
        let nxdomains = vec![fake_domain.clone()];
        mock_client.mock_dns(HashMap::new(), nxdomains);

        // Real client behavior
        let real_result = real_client
            .resolve(fake_domain.clone(), RecordType::A)
            .await;

        // Mock client behavior
        let mock_result = mock_client
            .resolve(fake_domain.clone(), RecordType::A)
            .await;

        // Both should return Ok with empty list
        assert!(
            real_result.is_ok(),
            "real client should return Ok for NXDOMAIN"
        );
        assert!(
            mock_result.is_ok(),
            "mock client should return Ok for NXDOMAIN"
        );

        let real_answers = real_result.unwrap();
        let mock_answers = mock_result.unwrap();

        assert_eq!(
            real_answers.len(),
            0,
            "real client should return empty list for NXDOMAIN"
        );
        assert_eq!(
            mock_answers.len(),
            0,
            "mock client should return empty list for NXDOMAIN"
        );
    }

    #[tokio::test]
    async fn cache_stores_and_retrieves_responses() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 1,
            cache_capacity: 100,
            ..Default::default()
        };

        let client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        let host = "example.com".to_string();
        let name: Name = host.parse().unwrap();
        let query = Query::query(name.clone(), RecordType::A);
        let cache = client
            .cache
            .as_ref()
            .expect("cache should be present")
            .clone();
        assert!(
            !cache.contains(&query, Instant::now()),
            "cache should start empty"
        );

        // First request - should hit DNS
        let start = std::time::Instant::now();
        let first_result = client
            .resolve_full(host.clone(), RecordType::A)
            .await
            .expect("first resolve failed");
        let first_duration = start.elapsed();

        assert!(!first_result.answers().is_empty(), "should have answers");

        // Second request - should hit cache (much faster)
        let start = std::time::Instant::now();
        let second_result = client
            .resolve_full("example.com".to_string(), RecordType::A)
            .await
            .expect("second resolve failed");
        let second_duration = start.elapsed();

        assert!(
            !second_result.answers().is_empty(),
            "should have cached answers"
        );

        // Cache should contain the entry after the first request and still on the second.
        assert!(
            cache.contains(&query, Instant::now()),
            "cache should hold entry"
        );
        assert!(
            second_duration < first_duration,
            "cached lookup should be faster"
        );
    }

    #[tokio::test]
    async fn cache_disabled_when_capacity_zero() {
        let resolvers = vec!["127.0.0.1:5353".to_string()];
        let config = BlastDNSConfig {
            request_timeout: Duration::from_secs(2),
            threads_per_resolver: 1,
            cache_capacity: 0, // Disable cache
            ..Default::default()
        };

        let client = BlastDNSClient::with_config(resolvers, config).expect("client init");

        // Verify cache is None
        assert!(client.cache.is_none(), "cache should be disabled");

        // Should still work without cache
        let result = client
            .resolve_full("example.com".to_string(), RecordType::A)
            .await
            .expect("resolve failed");

        assert!(
            !result.answers().is_empty(),
            "should have answers without cache"
        );
    }
}
