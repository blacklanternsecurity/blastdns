use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{Stream, StreamExt};
use hickory_proto::op::Message;
use hickory_proto::rr::{Name, RData, Record, RecordType};
use hickory_proto::xfer::DnsResponse;
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyIterator, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use serde_json::Value;
use std::sync::OnceLock;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::Instant;

use crate::client::{BatchResult, BatchResultBasic, BlastDNSClient};
use crate::config::{BlastDNSConfig, BlastDNSConfigWire};
use crate::error::BlastDNSError;
use crate::mock::MockBlastDNSClient;
use crate::resolver::DnsResolver;
use crate::utils::get_system_resolvers;

#[pyclass(name = "Client")]
pub struct PyBlastDNSClient {
    inner: Arc<BlastDNSClient>,
}

#[pymethods]
impl PyBlastDNSClient {
    #[new]
    #[pyo3(signature = (resolvers, config_json = None))]
    fn new(resolvers: Vec<String>, config_json: Option<String>) -> PyResult<Self> {
        let config = match config_json {
            Some(json) => {
                let wire: BlastDNSConfigWire = serde_json::from_str(&json)
                    .map_err(|e| PyValueError::new_err(format!("invalid config JSON: {e}")))?;
                BlastDNSConfig::from(wire)
            }
            None => BlastDNSConfig::default(),
        };

        let client = BlastDNSClient::with_config(resolvers, config).map_err(PyErr::from)?;

        Ok(PyBlastDNSClient {
            inner: Arc::new(client),
        })
    }

    /// Get the list of resolvers being used by this client.
    #[getter]
    fn resolvers(&self) -> Vec<String> {
        self.inner.resolvers()
    }

    #[pyo3(signature = (host, record_type = None))]
    fn resolve<'py>(
        &self,
        py: Python<'py>,
        host: String,
        record_type: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let record_type = parse_record_type(record_type)?;

        future_into_py(py, async move {
            let answers = client
                .resolve(host, record_type)
                .await
                .map_err(PyErr::from)?;
            Ok(answers)
        })
    }

    #[pyo3(signature = (host, record_type = None))]
    fn resolve_full<'py>(
        &self,
        py: Python<'py>,
        host: String,
        record_type: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let record_type = parse_record_type(record_type)?;

        future_into_py(py, async move {
            let response = client
                .resolve_full(host, record_type)
                .await
                .map_err(PyErr::from)?;
            dns_response_to_bytes(response)
        })
    }

    fn resolve_multi<'py>(
        &self,
        py: Python<'py>,
        host: String,
        record_types: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();

        let parsed_types: Result<Vec<RecordType>, PyErr> = record_types
            .iter()
            .map(|rt| parse_record_type(Some(rt.as_str())))
            .collect();
        let parsed_types = parsed_types?;

        future_into_py(py, async move {
            let results = client
                .resolve_multi(host, parsed_types.clone())
                .await
                .map_err(PyErr::from)?;

            // Convert HashMap<RecordType, Vec<String>> to Python dict
            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                for (record_type, answers) in results {
                    let key = record_type.to_string();
                    dict.set_item(key, answers)?;
                }
                Ok(dict.unbind())
            })
        })
    }

    fn resolve_multi_full<'py>(
        &self,
        py: Python<'py>,
        host: String,
        record_types: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();

        let parsed_types: Result<Vec<RecordType>, PyErr> = record_types
            .iter()
            .map(|rt| parse_record_type(Some(rt.as_str())))
            .collect();
        let parsed_types = parsed_types?;

        future_into_py(py, async move {
            let results = client
                .resolve_multi_full(host, parsed_types.clone())
                .await
                .map_err(PyErr::from)?;

            // Convert HashMap<RecordType, Result<DnsResponse, BlastDNSError>> to Python dict
            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                for (record_type, result) in results {
                    let key = record_type.to_string();
                    let value = match result {
                        Ok(response) => dns_response_to_bytes(response)?,
                        Err(err) => error_to_bytes(err)?,
                    };
                    dict.set_item(key, value)?;
                }
                Ok(dict.unbind())
            })
        })
    }

    #[pyo3(signature = (hosts, record_type = None))]
    fn resolve_batch(
        &self,
        hosts: Py<PyAny>,
        record_type: Option<&str>,
    ) -> PyResult<PyBatchBasicIterator> {
        let record_type = parse_record_type(record_type)?;

        // Convert Python iterable to Rust iterator
        let py_iter = Python::attach(|py| {
            let bound = hosts.bind(py);
            bound.try_iter().map(|i| i.unbind())
        })?;

        let rust_iter = PythonHostIterator::new(py_iter);

        // Call Rust resolve_batch (returns simplified results)
        let result_stream = self.inner.clone().resolve_batch(rust_iter, record_type);

        Ok(PyBatchBasicIterator {
            inner: Arc::new(TokioMutex::new(Box::pin(result_stream))),
        })
    }

    #[pyo3(signature = (hosts, record_type = None, skip_empty = false, skip_errors = false))]
    fn resolve_batch_full(
        &self,
        hosts: Py<PyAny>,
        record_type: Option<&str>,
        skip_empty: bool,
        skip_errors: bool,
    ) -> PyResult<PyBatchIterator> {
        let record_type = parse_record_type(record_type)?;

        // Convert Python iterable to Rust iterator
        let py_iter = Python::attach(|py| {
            let bound = hosts.bind(py);
            bound.try_iter().map(|i| i.unbind())
        })?;

        let rust_iter = PythonHostIterator::new(py_iter);

        // Call Rust resolve_batch_full (it handles spawn_blocking internally)
        let result_stream =
            self.inner
                .clone()
                .resolve_batch_full(rust_iter, record_type, skip_empty, skip_errors);

        Ok(PyBatchIterator {
            inner: Arc::new(TokioMutex::new(Box::pin(result_stream))),
        })
    }
}

#[pyclass]
pub struct PyBatchIterator {
    inner: Arc<TokioMutex<Pin<Box<dyn Stream<Item = BatchResult> + Send>>>>,
}

#[pymethods]
impl PyBatchIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let mut stream = inner.lock().await;
            let mut batch: Vec<(String, Vec<u8>)> = Vec::new();
            let start = Instant::now();
            let timeout = Duration::from_millis(200);

            loop {
                // Check if we should send the batch
                if batch.len() >= 1000 || (!batch.is_empty() && start.elapsed() >= timeout) {
                    return Ok(batch);
                }

                match stream.next().await {
                    Some((host, result)) => {
                        let payload = match result {
                            Ok(response) => dns_response_to_bytes(response)?,
                            Err(err) => error_to_bytes(err)?,
                        };
                        batch.push((host, payload));
                    }
                    None => {
                        if batch.is_empty() {
                            return Err(PyStopAsyncIteration::new_err("end of stream"));
                        } else {
                            return Ok(batch);
                        }
                    }
                }
            }
        })
    }
}

#[pyclass]
pub struct PyBatchBasicIterator {
    inner: Arc<TokioMutex<Pin<Box<dyn Stream<Item = BatchResultBasic> + Send>>>>,
}

#[pymethods]
impl PyBatchBasicIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let mut stream = inner.lock().await;
            let mut batch: Vec<(String, String, Vec<String>)> = Vec::new();
            let start = Instant::now();
            let timeout = Duration::from_millis(200);

            loop {
                // Check if we should send the batch
                if batch.len() >= 1000 || (!batch.is_empty() && start.elapsed() >= timeout) {
                    return Ok(batch);
                }

                match stream.next().await {
                    Some((host, record_type, answers)) => {
                        batch.push((host, record_type, answers));
                    }
                    None => {
                        if batch.is_empty() {
                            return Err(PyStopAsyncIteration::new_err("end of stream"));
                        } else {
                            return Ok(batch);
                        }
                    }
                }
            }
        })
    }
}

struct PythonHostIterator {
    iterator: Py<PyIterator>,
}

impl PythonHostIterator {
    fn new(iterator: Py<PyIterator>) -> Self {
        Self { iterator }
    }
}

impl Iterator for PythonHostIterator {
    type Item = Result<String, PyErr>;

    fn next(&mut self) -> Option<Self::Item> {
        Python::attach(|py| {
            let iter = self.iterator.bind(py);
            iter.into_iter()
                .next()
                .map(|result| result.and_then(|item| item.extract()))
        })
    }
}

fn parse_record_type(input: Option<&str>) -> PyResult<RecordType> {
    match input {
        None => Ok(RecordType::A),
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(RecordType::A);
            }
            let upper = trimmed.to_ascii_uppercase();
            RecordType::from_str(&upper)
                .map_err(|_| PyValueError::new_err(format!("invalid record type `{value}`")))
        }
    }
}

fn dns_response_to_bytes(response: DnsResponse) -> PyResult<Vec<u8>> {
    let message = response.into_message();
    let value = message_to_extended_value(&message);
    serde_json::to_vec(&value)
        .map_err(|err| PyValueError::new_err(format!("failed to serialize response: {err}")))
}

/// Serialize a Message in the same shape Pydantic's Response model expects, but
/// with each Record carrying pre-computed ``text`` (presentation format via
/// hickory's ``Display``) and ``targets`` (host strings worth following).
fn message_to_extended_value(message: &Message) -> Value {
    let to_records = |records: &[Record]| -> Vec<Value> {
        records.iter().map(record_to_extended_value).collect()
    };
    serde_json::json!({
        "header": serde_json::to_value(message.header()).unwrap_or(Value::Null),
        "queries": serde_json::to_value(message.queries()).unwrap_or(Value::Null),
        "answers": to_records(message.answers()),
        "name_servers": to_records(message.name_servers()),
        "additionals": to_records(message.additionals()),
        "signature": serde_json::to_value(message.signature()).unwrap_or(Value::Null),
        "edns": serde_json::to_value(message.extensions()).unwrap_or(Value::Null),
    })
}

fn record_to_extended_value(record: &Record) -> Value {
    let data = record.data();
    serde_json::json!({
        "name_labels": record.name().to_string(),
        "ttl": record.ttl(),
        "dns_class": record.dns_class().to_string(),
        "rdata": serde_json::to_value(data).unwrap_or(Value::Null),
        "text": data.to_string(),
        "targets": targets_for_rdata(data),
    })
}

/// For rdata variants whose payload includes a hostname BBOT-style consumers
/// want to follow (A/AAAA literal IPs, CNAME chain target, MX exchange, etc),
/// return the list of ``(rdtype, host)`` pairs. Names are normalized: trailing
/// dot stripped, lowercased -- matching the historical BBOT shape.
///
/// TXT is intentionally omitted: extracting hostnames from free-form TXT
/// content (SPF/DKIM/etc) is consumer-specific and stays in BBOT.
fn targets_for_rdata(data: &RData) -> Vec<(String, String)> {
    let rdtype = data.record_type().to_string();
    let normalize = |n: &Name| n.to_string().trim_end_matches('.').to_ascii_lowercase();
    match data {
        RData::A(addr) => vec![(rdtype, addr.to_string())],
        RData::AAAA(addr) => vec![(rdtype, addr.to_string())],
        RData::CNAME(c) => vec![(rdtype, normalize(&c.0))],
        RData::NS(n) => vec![(rdtype, normalize(&n.0))],
        RData::PTR(p) => vec![(rdtype, normalize(&p.0))],
        RData::ANAME(a) => vec![(rdtype, normalize(&a.0))],
        RData::MX(mx) => vec![(rdtype, normalize(mx.exchange()))],
        RData::SRV(srv) => vec![(rdtype, normalize(srv.target()))],
        RData::SOA(soa) => vec![(rdtype, normalize(soa.mname()))],
        RData::NAPTR(naptr) => vec![(rdtype, normalize(naptr.replacement()))],
        _ => Vec::new(),
    }
}

fn error_to_bytes(err: BlastDNSError) -> PyResult<Vec<u8>> {
    let payload = serde_json::json!({ "error": err.to_string() });
    serde_json::to_vec(&payload)
        .map_err(|e| PyValueError::new_err(format!("failed to serialize error payload: {e}")))
}

// Python exception classes imported from blastdns.exceptions
static PY_CONFIGURATION_ERROR: OnceLock<Py<PyAny>> = OnceLock::new();
static PY_NO_RESOLVERS_ERROR: OnceLock<Py<PyAny>> = OnceLock::new();
static PY_RESOLVER_ERROR: OnceLock<Py<PyAny>> = OnceLock::new();

fn init_exception_types(py: Python<'_>) -> PyResult<()> {
    let module = py.import("blastdns.exceptions")?;
    PY_CONFIGURATION_ERROR.get_or_init(|| module.getattr("ConfigurationError").unwrap().unbind());
    PY_NO_RESOLVERS_ERROR.get_or_init(|| module.getattr("NoResolversError").unwrap().unbind());
    PY_RESOLVER_ERROR.get_or_init(|| module.getattr("ResolverError").unwrap().unbind());
    Ok(())
}

fn make_pyerr(py: Python<'_>, exc_type: &Py<PyAny>, msg: String) -> Option<PyErr> {
    exc_type
        .bind(py)
        .clone()
        .cast_exact::<PyType>()
        .ok()
        .map(|t| PyErr::from_type(t.clone(), (msg,)))
}

fn blastdns_error_to_pyerr(err: BlastDNSError) -> PyErr {
    Python::attach(|py| {
        // Try to initialize exception types if not already done
        let _ = init_exception_types(py);

        let msg = err.to_string();
        let result = match &err {
            BlastDNSError::NoResolvers => PY_NO_RESOLVERS_ERROR
                .get()
                .and_then(|t| make_pyerr(py, t, msg.clone())),
            BlastDNSError::InvalidResolver { .. }
            | BlastDNSError::InvalidHostname { .. }
            | BlastDNSError::Configuration(_) => PY_CONFIGURATION_ERROR
                .get()
                .and_then(|t| make_pyerr(py, t, msg.clone())),
            BlastDNSError::ResolverRequestFailed { .. }
            | BlastDNSError::ResolverSetupFailed { .. }
            | BlastDNSError::WorkerDropped
            | BlastDNSError::QueueClosed => PY_RESOLVER_ERROR
                .get()
                .and_then(|t| make_pyerr(py, t, msg.clone())),
        };
        result.unwrap_or_else(|| {
            // If exception types aren't initialized, something is wrong with the package
            PyRuntimeError::new_err(format!(
                "blastdns exception types not initialized (packaging bug?): {msg}"
            ))
        })
    })
}

impl From<BlastDNSError> for PyErr {
    fn from(err: BlastDNSError) -> Self {
        blastdns_error_to_pyerr(err)
    }
}

#[pyclass(name = "MockClient")]
pub struct PyMockBlastDNSClient {
    inner: Arc<MockBlastDNSClient>,
}

#[pymethods]
impl PyMockBlastDNSClient {
    #[new]
    fn new() -> Self {
        PyMockBlastDNSClient {
            inner: Arc::new(MockBlastDNSClient::new()),
        }
    }

    fn mock_dns(&mut self, data: Bound<'_, PyAny>) -> PyResult<()> {
        use std::collections::HashMap;

        let client = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            PyRuntimeError::new_err("Cannot modify mock client with outstanding references")
        })?;

        let dict = data
            .cast::<pyo3::types::PyDict>()
            .map_err(|_| PyValueError::new_err("expected dict"))?;

        let mut responses: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        let mut nxdomains: Vec<String> = Vec::new();

        for (key, value) in dict.iter() {
            let key_str: String = key.extract()?;

            if key_str == "_NXDOMAIN" {
                nxdomains = value.extract()?;
            } else {
                let host_records: HashMap<String, Vec<String>> = value.extract()?;
                responses.insert(key_str, host_records);
            }
        }

        client.mock_dns(responses, nxdomains);
        Ok(())
    }

    #[pyo3(signature = (host, record_type = None))]
    fn resolve<'py>(
        &self,
        py: Python<'py>,
        host: String,
        record_type: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let record_type = parse_record_type(record_type)?;

        future_into_py(py, async move {
            let answers = inner
                .resolve(host, record_type)
                .await
                .map_err(PyErr::from)?;
            Ok(answers)
        })
    }

    #[pyo3(signature = (host, record_type = None))]
    fn resolve_full<'py>(
        &self,
        py: Python<'py>,
        host: String,
        record_type: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let record_type = parse_record_type(record_type)?;

        future_into_py(py, async move {
            let response = inner
                .resolve_full(host, record_type)
                .await
                .map_err(PyErr::from)?;
            dns_response_to_bytes(response)
        })
    }

    fn resolve_multi<'py>(
        &self,
        py: Python<'py>,
        host: String,
        record_types: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        let parsed_types: Result<Vec<RecordType>, PyErr> = record_types
            .iter()
            .map(|rt| parse_record_type(Some(rt.as_str())))
            .collect();
        let parsed_types = parsed_types?;

        future_into_py(py, async move {
            let results = inner
                .resolve_multi(host, parsed_types.clone())
                .await
                .map_err(PyErr::from)?;

            // Convert HashMap<RecordType, Vec<String>> to Python dict
            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                for (record_type, answers) in results {
                    let key = record_type.to_string();
                    dict.set_item(key, answers)?;
                }
                Ok(dict.unbind())
            })
        })
    }

    fn resolve_multi_full<'py>(
        &self,
        py: Python<'py>,
        host: String,
        record_types: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        let parsed_types: Result<Vec<RecordType>, PyErr> = record_types
            .iter()
            .map(|rt| parse_record_type(Some(rt.as_str())))
            .collect();
        let parsed_types = parsed_types?;

        future_into_py(py, async move {
            let results = inner
                .resolve_multi_full(host, parsed_types.clone())
                .await
                .map_err(PyErr::from)?;

            // Convert HashMap<RecordType, Result<DnsResponse, BlastDNSError>> to Python dict
            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                for (record_type, result) in results {
                    let key = record_type.to_string();
                    let value = match result {
                        Ok(response) => dns_response_to_bytes(response)?,
                        Err(err) => error_to_bytes(err)?,
                    };
                    dict.set_item(key, value)?;
                }
                Ok(dict.unbind())
            })
        })
    }

    #[pyo3(signature = (hosts, record_type = None))]
    fn resolve_batch(
        &self,
        hosts: Py<PyAny>,
        record_type: Option<&str>,
    ) -> PyResult<PyBatchBasicIterator> {
        let record_type = parse_record_type(record_type)?;

        // Convert Python iterable to Rust iterator
        let py_iter = Python::attach(|py| {
            let bound = hosts.bind(py);
            bound.try_iter().map(|i| i.unbind())
        })?;

        let rust_iter = PythonHostIterator::new(py_iter);

        // Call Rust resolve_batch (returns simplified results)
        let result_stream = self.inner.clone().resolve_batch(rust_iter, record_type);

        Ok(PyBatchBasicIterator {
            inner: Arc::new(TokioMutex::new(Box::pin(result_stream))),
        })
    }

    #[pyo3(signature = (hosts, record_type = None, skip_empty = false, skip_errors = false))]
    fn resolve_batch_full(
        &self,
        hosts: Py<PyAny>,
        record_type: Option<&str>,
        skip_empty: bool,
        skip_errors: bool,
    ) -> PyResult<PyBatchIterator> {
        let record_type = parse_record_type(record_type)?;

        // Convert Python iterable to Rust iterator
        let py_iter = Python::attach(|py| {
            let bound = hosts.bind(py);
            bound.try_iter().map(|i| i.unbind())
        })?;

        let rust_iter = PythonHostIterator::new(py_iter);

        // Call Rust resolve_batch_full (it handles spawn_blocking internally)
        let result_stream =
            self.inner
                .clone()
                .resolve_batch_full(rust_iter, record_type, skip_empty, skip_errors);

        Ok(PyBatchIterator {
            inner: Arc::new(TokioMutex::new(Box::pin(result_stream))),
        })
    }
}

/// Get system DNS resolver IP addresses from OS configuration.
/// Works on Unix, Windows, macOS, and Android.
#[pyfunction]
fn get_system_resolvers_py() -> PyResult<Vec<String>> {
    let resolver_ips = get_system_resolvers()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to get system resolvers: {}", e)))?;

    Ok(resolver_ips.iter().map(|ip| ip.to_string()).collect())
}

/// Perform an AXFR (full zone transfer) against a specific nameserver.
/// Returns a list of (name, record_type, rdata) tuples.
#[pyfunction]
#[pyo3(signature = (nameserver, zone, timeout_secs = 6.0))]
fn zone_transfer_py<'py>(
    py: Python<'py>,
    nameserver: String,
    zone: String,
    timeout_secs: f64,
) -> PyResult<Bound<'py, PyAny>> {
    let timeout = std::time::Duration::from_secs_f64(timeout_secs);
    future_into_py(py, async move {
        let result = crate::zone_transfer::zone_transfer(&nameserver, &zone, timeout)
            .await
            .map_err(PyErr::from)?;

        // Convert records to JSON-serializable tuples: (name, type, rdata_string)
        let records: Vec<(String, String, String)> = result
            .records
            .iter()
            .map(|record| {
                let name = record.name().to_string();
                let rtype = record.record_type().to_string();
                let rdata = record.data().to_string();
                (name, rtype, rdata)
            })
            .collect();

        Ok(records)
    })
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBlastDNSClient>()?;
    m.add_class::<PyMockBlastDNSClient>()?;
    m.add_function(wrap_pyfunction!(get_system_resolvers_py, m)?)?;
    m.add_function(wrap_pyfunction!(zone_transfer_py, m)?)?;
    // Eagerly initialize exception types so they're ready before any errors occur
    let _ = init_exception_types(py);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::{PyList, PyModule};

    #[test]
    fn python_iterator_error_handling() {
        pyo3::append_to_inittab!(_native);
        Python::initialize();

        Python::attach(|py| {
            // Normal iteration with StopIteration
            let list = PyList::new(py, ["a", "b", "c"]).unwrap();
            let py_iter = list.try_iter().unwrap().unbind();
            let mut rust_iter = PythonHostIterator::new(py_iter);

            assert!(matches!(rust_iter.next(), Some(Ok(s)) if s == "a"));
            assert!(matches!(rust_iter.next(), Some(Ok(s)) if s == "b"));
            assert!(matches!(rust_iter.next(), Some(Ok(s)) if s == "c"));
            assert!(rust_iter.next().is_none());

            // Iterator yielding non-string returns error
            let list = PyList::new(py, [1, 2, 3]).unwrap();
            let py_iter = list.try_iter().unwrap().unbind();
            let mut rust_iter = PythonHostIterator::new(py_iter);

            assert!(matches!(rust_iter.next(), Some(Err(_))));

            // Iterator whose __next__ raises a Python exception returns Err(...)
            let code = c"class FailingIter:\n    def __iter__(self): return self\n    def __next__(self): raise RuntimeError('failure')";
            let module = PyModule::from_code(py, code, c"test.py", c"test").unwrap();
            let cls = module.getattr("FailingIter").unwrap();
            let failing_iter = cls.call0().unwrap();
            let py_iter = failing_iter.try_iter().unwrap().unbind();
            let mut rust_iter = PythonHostIterator::new(py_iter);

            assert!(matches!(rust_iter.next(), Some(Err(_))));
        });
    }
}
