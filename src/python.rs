use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use hickory_client::proto::{rr::RecordType, xfer::DnsResponse};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyAnyMethods, PyDict, PyDictMethods, PyModule, PyModuleMethods};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::client::BlastDNSClient;
use crate::config::BlastDNSConfig;
use crate::error::BlastDNSError;

#[pyclass(name = "Client")]
pub struct PyBlastDNSClient {
    inner: Arc<BlastDNSClient>,
}

#[pymethods]
impl PyBlastDNSClient {
    #[new]
    #[pyo3(signature = (resolvers, config = None))]
    fn new(resolvers: Vec<String>, config: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        let config = config
            .as_ref()
            .map(config_from_py)
            .transpose()?
            .unwrap_or_else(BlastDNSConfig::default);

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
            Python::attach(|py| dns_response_to_py(py, response))
        })
    }
}

fn config_from_py(obj: &Bound<'_, PyAny>) -> PyResult<BlastDNSConfig> {
    if let Ok(mapping) = obj.cast::<PyDict>() {
        return config_from_dict(mapping);
    }

    if obj.hasattr("model_dump")? {
        let dumped = obj.call_method0("model_dump")?;
        let dict = dumped
            .cast_into::<PyDict>()
            .map_err(|_| PyTypeError::new_err("model_dump() must return a dict"))?;
        return config_from_dict(&dict);
    }

    if obj.hasattr("dict")? {
        let dumped = obj.call_method0("dict")?;
        let dict = dumped
            .cast_into::<PyDict>()
            .map_err(|_| PyTypeError::new_err("dict() must return a dict"))?;
        return config_from_dict(&dict);
    }

    Err(PyTypeError::new_err(
        "config must be a mapping or expose model_dump()/dict()",
    ))
}

fn config_from_dict(dict: &Bound<'_, PyDict>) -> PyResult<BlastDNSConfig> {
    let threads: usize = dict_get(dict, "threads_per_resolver")?;
    let timeout_ms: u64 = dict_get(dict, "request_timeout_ms")?;
    let max_retries: usize = dict_get(dict, "max_retries")?;
    let purgatory_threshold: usize = dict_get(dict, "purgatory_threshold")?;
    let purgatory_sentence_ms: u64 = dict_get(dict, "purgatory_sentence_ms")?;

    Ok(BlastDNSConfig {
        threads_per_resolver: threads.max(1),
        request_timeout: Duration::from_millis(timeout_ms.max(1)),
        max_retries,
        purgatory_threshold,
        purgatory_sentence: Duration::from_millis(purgatory_sentence_ms.max(1)),
    })
}

fn dict_get<'py, T>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<T>
where
    T: for<'a> FromPyObject<'a, 'py>,
{
    match dict.get_item(key)? {
        Some(value) => value.extract().map_err(Into::into),
        None => Err(PyRuntimeError::new_err(format!("config missing `{key}`"))),
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

fn dns_response_to_py(py: Python<'_>, response: DnsResponse) -> PyResult<Py<PyAny>> {
    let message = response.into_message();
    let serialized = serde_json::to_string(&message)
        .map_err(|err| PyValueError::new_err(format!("failed to serialize response: {err}")))?;
    let json_mod = py
        .import("json")
        .map_err(|err| PyRuntimeError::new_err(format!("failed to import json: {err}")))?;
    let obj = json_mod
        .call_method1("loads", (serialized,))
        .map_err(|err| PyRuntimeError::new_err(format!("failed to decode JSON: {err}")))?;
    Ok(obj.into())
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
