use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use futures::stream::{Stream, StreamExt};
use hickory_client::proto::{rr::RecordType, xfer::DnsResponse};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
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

    #[pyo3(signature = (hosts, record_type = None, skip_empty = false, skip_errors = false))]
    fn resolve_batch(
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

        // Call Rust resolve_batch (it handles spawn_blocking internally)
        let result_stream =
            self.inner
                .resolve_batch(rust_iter, record_type, skip_empty, skip_errors);

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
                None => Err(PyStopAsyncIteration::new_err("end of stream")),
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
