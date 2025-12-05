use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::{
    future,
    stream::{self, Stream, StreamExt},
};
use hickory_client::proto::{rr::RecordType, xfer::DnsResponse};
use tokio::task::JoinHandle;

use crate::{error::BlastDNSError, utils::format_ptr_query};

/// Core trait for DNS resolution.
/// Types only need to implement `resolve_full`, all other methods have default implementations.
pub trait DnsResolver: Send + Sync + Clone {
    /// Resolve a hostname and return the full DNS response.
    /// This is the only method that needs to be implemented by types.
    fn resolve_full(
        &self,
        host: String,
        record_type: RecordType,
    ) -> impl std::future::Future<Output = Result<DnsResponse, BlastDNSError>> + Send;

    /// Get the concurrency limit for batch operations.
    /// Default implementation returns 32.
    fn get_concurrency(&self) -> usize {
        32
    }

    /// Resolve a hostname and return only the record data strings.
    /// Default implementation extracts strings from the full response.
    fn resolve(
        &self,
        host: String,
        record_type: RecordType,
    ) -> impl std::future::Future<Output = Result<Vec<String>, BlastDNSError>> + Send {
        async move {
            let response = self.resolve_full(host, record_type).await?;
            let answers: Vec<String> = response
                .answers()
                .iter()
                .map(|record| record.data().to_string())
                .collect();
            Ok(answers)
        }
    }

    /// Resolve multiple record types for a single hostname in parallel, returning only successful rdata strings.
    /// Default implementation calls resolve_multi_full and filters successful results.
    fn resolve_multi(
        &self,
        host: String,
        record_types: Vec<RecordType>,
    ) -> impl std::future::Future<Output = Result<HashMap<RecordType, Vec<String>>, BlastDNSError>> + Send
    {
        async move {
            let full_results = self.resolve_multi_full(host, record_types).await?;
            let simplified: HashMap<RecordType, Vec<String>> = full_results
                .into_iter()
                .filter_map(|(record_type, result)| {
                    result.ok().and_then(|response| {
                        let answers: Vec<String> = response
                            .answers()
                            .iter()
                            .map(|record| record.data().to_string())
                            .collect();
                        // Only include if there are actual answers
                        if answers.is_empty() {
                            None
                        } else {
                            Some((record_type, answers))
                        }
                    })
                })
                .collect();
            Ok(simplified)
        }
    }

    /// Resolve multiple record types for a single hostname in parallel, returning full responses.
    /// Default implementation calls resolve_full for each record type.
    fn resolve_multi_full(
        &self,
        host: String,
        record_types: Vec<RecordType>,
    ) -> impl std::future::Future<
        Output = Result<HashMap<RecordType, Result<DnsResponse, BlastDNSError>>, BlastDNSError>,
    > + Send {
        async move {
            if record_types.is_empty() {
                return Err(BlastDNSError::Configuration(
                    "at least one record type is required".into(),
                ));
            }

            let futures: Vec<_> = record_types
                .iter()
                .map(|&record_type| {
                    let host = host.clone();
                    async move {
                        let mut query_host = host;

                        // Auto-format PTR queries if an IP address is provided
                        if record_type == RecordType::PTR {
                            query_host = format_ptr_query(&query_host);
                        }

                        let result = self.resolve_full(query_host, record_type).await;
                        (record_type, result)
                    }
                })
                .collect();

            let results = future::join_all(futures).await;
            Ok(results.into_iter().collect())
        }
    }

    /// Resolve a batch of hostnames with bounded concurrency, returning simplified tuples.
    ///
    /// Returns (hostname, record_type, [rdata_strings]) where rdata_strings contain only
    /// the record data (e.g., "93.184.216.34" for A records, "10 aspmx.l.google.com." for MX).
    /// Only successful resolutions with non-empty answers are returned.
    fn resolve_batch<I, E>(
        self: Arc<Self>,
        hosts: I,
        record_type: RecordType,
    ) -> impl stream::Stream<Item = (String, String, Vec<String>)> + Unpin + Send + 'static
    where
        Self: Sized + 'static,
        I: Iterator<Item = Result<String, E>> + Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        let record_type_string = record_type.to_string();

        Box::pin(
            self.resolve_batch_full(hosts, record_type, true, true)
                .filter_map(move |(host, result)| {
                    let record_type_str = record_type_string.clone();
                    async move {
                        match result {
                            Ok(response) => {
                                let answers: Vec<String> = response
                                    .answers()
                                    .iter()
                                    .map(|record| record.data().to_string())
                                    .collect();

                                if answers.is_empty() {
                                    None
                                } else {
                                    Some((host, record_type_str, answers))
                                }
                            }
                            Err(_) => None,
                        }
                    }
                }),
        )
    }

    /// Resolve a batch of hostnames with bounded concurrency and stream the full results as they complete.
    fn resolve_batch_full<I, E>(
        self: Arc<Self>,
        hosts: I,
        record_type: RecordType,
        skip_empty: bool,
        skip_errors: bool,
    ) -> impl stream::Stream<Item = (String, Result<DnsResponse, BlastDNSError>)> + Unpin + Send + 'static
    where
        Self: Sized + 'static,
        I: Iterator<Item = Result<String, E>> + Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        let client = Arc::clone(&self);
        let concurrency = self.get_concurrency();

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
                        let result = client.resolve_full(host, record_type).await;
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
}

/// Stream adapter that wraps an iterator and polls it via spawn_blocking
pub struct BlockingIteratorStream<I, T> {
    iterator: Arc<Mutex<I>>,
    pending: Option<JoinHandle<Option<T>>>,
}

impl<I, T> BlockingIteratorStream<I, T>
where
    I: Iterator<Item = T> + Send + 'static,
    T: Send + 'static,
{
    pub fn new(iterator: I) -> Self {
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
