use std::str::FromStr;
use std::sync::{Arc, Mutex};

use futures::stream::StreamExt;
use hickory_client::proto::{rr::RecordType, xfer::DnsResponse};
use pyo3::exceptions::{PyRuntimeError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::{PyBytes, PyIterator};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::client::BlastDNSClient;
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

    #[pyo3(signature = (hosts, queue, sentinel, record_type = None))]
    fn resolve_batch<'py>(
        &self,
        py: Python<'py>,
        hosts: Bound<'py, PyAny>,
        queue: Bound<'py, PyAny>,
        sentinel: Bound<'py, PyAny>,
        record_type: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let record_type = parse_record_type(record_type)?;
        let hosts_iter = PythonHosts::new(hosts.unbind())?;
        let iterator_error = hosts_iter.error_handle();
        let queue = queue.unbind();
        let sentinel = sentinel.unbind();

        future_into_py(py, async move {
            let mut stream = client.resolve_batch(futures::stream::iter(hosts_iter), record_type);
            let stream_result = async {
                while let Some((host, result)) = stream.next().await {
                    let payload = match result {
                        Ok(response) => dns_response_to_bytes(response)?,
                        Err(err) => error_to_bytes(err)?,
                    };
                    enqueue_result(&queue, &host, &payload)?;
                }

                if let Some(err) = iterator_error.lock().unwrap().take() {
                    return Err(err);
                }

                Ok(())
            }
            .await;

            send_sentinel(&queue, &sentinel)?;

            stream_result
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

fn enqueue_result(queue: &Py<PyAny>, host: &str, payload: &[u8]) -> PyResult<()> {
    Python::attach(|py| {
        let queue = queue.bind(py);
        queue.call_method1("put_nowait", ((host, PyBytes::new(py, payload)),))?;
        Ok(())
    })
}

fn send_sentinel(queue: &Py<PyAny>, sentinel: &Py<PyAny>) -> PyResult<()> {
    Python::attach(|py| {
        let queue = queue.bind(py);
        let sentinel = sentinel.bind(py);
        queue.call_method1("put_nowait", (sentinel,))?;
        Ok(())
    })
}

struct PythonHosts {
    iterator: Py<PyIterator>,
    error: Arc<Mutex<Option<PyErr>>>,
}

impl PythonHosts {
    fn new(obj: Py<PyAny>) -> PyResult<Self> {
        Python::attach(|py| {
            let iterator = obj.bind(py).try_iter()?.unbind();
            Ok(Self {
                iterator,
                error: Arc::new(Mutex::new(None)),
            })
        })
    }

    fn error_handle(&self) -> Arc<Mutex<Option<PyErr>>> {
        Arc::clone(&self.error)
    }
}

impl IntoIterator for PythonHosts {
    type Item = String;
    type IntoIter = PythonHostsIter;

    fn into_iter(self) -> Self::IntoIter {
        PythonHostsIter {
            iterator: self.iterator,
            error: self.error,
        }
    }
}

struct PythonHostsIter {
    iterator: Py<PyIterator>,
    error: Arc<Mutex<Option<PyErr>>>,
}

impl Iterator for PythonHostsIter {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        Python::attach(|py| {
            let iterator_obj = self.iterator.clone_ref(py);
            let iterator = iterator_obj.bind(py);
            match iterator.call_method0("__next__") {
                Ok(obj) => match obj.extract::<String>() {
                    Ok(host) => Some(host),
                    Err(err) => {
                        *self.error.lock().unwrap() = Some(err);
                        None
                    }
                },
                Err(err) => {
                    if err.is_instance_of::<PyStopIteration>(py) {
                        None
                    } else {
                        *self.error.lock().unwrap() = Some(err);
                        None
                    }
                }
            }
        })
    }
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
