use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use futures::stream::{Stream, StreamExt};
use hickory_client::proto::{rr::RecordType, xfer::DnsResponse};
use pyo3::exceptions::{PyRuntimeError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyIterator};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex as TokioMutex;

use crate::client::{BatchResult, BlastDNSClient};
use crate::config::{BlastDNSConfig, BlastDNSConfigWire};
use crate::error::BlastDNSError;

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
            let response = client
                .resolve(host, record_type)
                .await
                .map_err(PyErr::from)?;
            dns_response_to_bytes(response)
        })
    }

    #[pyo3(signature = (hosts, record_type = None))]
    fn resolve_batch(
        &self,
        hosts: Py<PyAny>,
        record_type: Option<&str>,
    ) -> PyResult<PyBatchIterator> {
        let record_type = parse_record_type(record_type)?;

        // Convert Python iterable to Rust iterator
        let py_iter = Python::attach(|py| {
            let bound = hosts.bind(py);
            bound.try_iter().map(|i| i.unbind())
        })?;

        let rust_iter = PythonHostIterator::new(py_iter);

        // Call Rust resolve_batch (it handles spawn_blocking internally)
        let result_stream = self.inner.resolve_batch(rust_iter, record_type);

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
            match stream.next().await {
                Some((host, result)) => {
                    let payload = match result {
                        Ok(response) => dns_response_to_bytes(response)?,
                        Err(err) => error_to_bytes(err)?,
                    };
                    Ok((host, payload))
                }
                None => Err(PyStopIteration::new_err("end of stream")),
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
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        Python::attach(|py| {
            let iter = self.iterator.bind(py);
            match iter.call_method0("__next__") {
                Ok(item) => item.extract::<String>().ok(),
                Err(e) if e.is_instance_of::<PyStopIteration>(py) => None,
                Err(_) => None,
            }
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
    let serialized = serde_json::to_vec(&message)
        .map_err(|err| PyValueError::new_err(format!("failed to serialize response: {err}")))?;
    Ok(serialized)
}

fn error_to_bytes(err: BlastDNSError) -> PyResult<Vec<u8>> {
    let payload = serde_json::json!({ "error": err.to_string() });
    serde_json::to_vec(&payload)
        .map_err(|e| PyValueError::new_err(format!("failed to serialize error payload: {e}")))
}

impl From<BlastDNSError> for PyErr {
    fn from(err: BlastDNSError) -> Self {
        PyRuntimeError::new_err(err.to_string())
    }
}

#[pymodule]
fn _native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBlastDNSClient>()?;
    Ok(())
}
