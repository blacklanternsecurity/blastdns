use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use futures::StreamExt;
use hickory_client::{
    client::{Client, ClientHandle},
    proto::{
        rr::{Name, Record},
        tcp::TcpClientStream,
        xfer::DnsResponse,
    },
};
use tracing::debug;

use crate::error::BlastDNSError;
use crate::utils::parse_resolver;

/// Result of a zone transfer operation.
#[derive(Debug)]
pub struct ZoneTransferResult {
    /// The zone origin (e.g., "example.com.")
    pub zone: String,
    /// The nameserver that was queried
    pub nameserver: SocketAddr,
    /// All records returned by the zone transfer
    pub records: Vec<Record>,
}

/// Perform an AXFR (full zone transfer) against a specific nameserver.
///
/// Zone transfers are TCP-based operations that download all records from a DNS zone.
/// This bypasses the normal worker pool since it requires a dedicated TCP connection.
///
/// # Arguments
/// * `nameserver` - The nameserver to query (IP:port string, e.g., "1.2.3.4:53")
/// * `zone` - The zone name to transfer (e.g., "example.com")
/// * `timeout` - Connection/query timeout
pub async fn zone_transfer(
    nameserver: &str,
    zone: &str,
    timeout: Duration,
) -> Result<ZoneTransferResult, BlastDNSError> {
    let addr = parse_resolver(nameserver)?;

    let zone_fqdn = if zone.ends_with('.') {
        zone.to_string()
    } else {
        format!("{zone}.")
    };

    let zone_name = Name::from_str(&zone_fqdn).map_err(|e| BlastDNSError::InvalidHostname {
        name: zone.to_string(),
        source: e,
    })?;

    debug!("Starting AXFR for zone {zone} from {addr}");

    // Create a TCP connection to the nameserver
    let provider = hickory_client::proto::runtime::TokioRuntimeProvider::new();
    let (stream_future, sender) = TcpClientStream::new(addr, None, Some(timeout), provider);

    // Create the client (awaits TCP connection internally)
    let (mut client, bg) = Client::new(stream_future, sender, None)
        .await
        .map_err(|e| BlastDNSError::ResolverSetupFailed {
            resolver: addr,
            source: e,
        })?;

    // Spawn the background task
    let bg_handle = tokio::spawn(bg);

    // Perform the zone transfer (AXFR, no last_soa = full transfer)
    let mut xfr_stream = client.zone_transfer(zone_name, None);

    // Collect all records from the transfer
    let mut all_records: Vec<Record> = Vec::new();
    while let Some(result) = xfr_stream.next().await {
        let response: DnsResponse = result.map_err(|e| BlastDNSError::ResolverRequestFailed {
            resolver: addr,
            source: e,
        })?;
        all_records.extend(response.answers().iter().cloned());
    }

    debug!(
        "AXFR complete for zone {zone} from {addr}: {} records",
        all_records.len()
    );

    // Clean up the background task
    bg_handle.abort();

    Ok(ZoneTransferResult {
        zone: zone.to_string(),
        nameserver: addr,
        records: all_records,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_client::proto::rr::RecordType;

    #[tokio::test]
    async fn test_invalid_nameserver() {
        let result = zone_transfer("not-an-ip", "example.com", Duration::from_secs(5)).await;
        assert!(result.is_err());
    }

    /// Integration test: AXFR against local BIND9 test server.
    /// Requires: ./scripts/start-test-axfr.sh
    /// Run with: cargo test -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_local_zone_transfer() {
        let result = zone_transfer(
            "127.0.0.1:5354",
            "zonetransfer.test",
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        assert!(
            result.records.len() > 10,
            "Expected many records, got {}",
            result.records.len()
        );
        assert_eq!(result.zone, "zonetransfer.test");

        // Verify expected record types
        let has_soa = result
            .records
            .iter()
            .any(|r| r.record_type() == RecordType::SOA);
        assert!(has_soa, "Expected SOA record in zone transfer");

        let has_a = result
            .records
            .iter()
            .any(|r| r.record_type() == RecordType::A);
        assert!(has_a, "Expected A record in zone transfer");

        let has_mx = result
            .records
            .iter()
            .any(|r| r.record_type() == RecordType::MX);
        assert!(has_mx, "Expected MX record in zone transfer");
    }
}
