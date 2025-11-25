use std::str::FromStr;
use std::sync::Arc;

use hickory_client::proto::{rr::RecordType, xfer::DnsResponse};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
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
