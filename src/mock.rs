use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use hickory_client::proto::op::{Header, Message, MessageType, OpCode, Query, ResponseCode};
use hickory_client::proto::rr::rdata::{CNAME, MX, NS, PTR, TXT};
use hickory_client::proto::rr::{Name, RData, Record, RecordType};
use hickory_client::proto::xfer::DnsResponse;
use regex::Regex;

use crate::error::BlastDNSError;
use crate::resolver::DnsResolver;
use crate::utils::format_ptr_query;

/// Mock DNS client for testing purposes.
#[derive(Clone, Debug)]
pub struct MockBlastDNSClient {
    mock_data: HashMap<String, HashMap<RecordType, Vec<String>>>,
    regex_patterns: Vec<(Regex, HashMap<RecordType, Vec<String>>)>,
    nxdomain_hosts: HashSet<String>,
    nxdomain_patterns: Vec<Regex>,
}

impl MockBlastDNSClient {
    /// Create a new mock client.
    pub fn new() -> Self {
        Self {
            mock_data: HashMap::new(),
            regex_patterns: Vec::new(),
            nxdomain_hosts: HashSet::new(),
            nxdomain_patterns: Vec::new(),
        }
    }

    /// Configure mock DNS responses.
    /// Takes responses (hostname -> record type -> answers) and a list of NXDOMAIN hosts.
    ///
    /// Hostnames prefixed with "regex:" will be treated as regex patterns.
    /// Examples:
    /// - "example.com" - exact match
    /// - "regex:.*\.example\.com" - matches any subdomain of example.com
    /// - "regex:test-\d+" - matches test-1, test-2, etc.
    pub fn mock_dns(
        &mut self,
        responses: HashMap<String, HashMap<String, Vec<String>>>,
        nxdomains: Vec<String>,
    ) {
        self.clear();

        for (host, records) in responses {
            if let Some(pattern) = host.strip_prefix("regex:") {
                // Regex pattern
                if let Ok(regex) = Regex::new(pattern) {
                    let mut record_map = HashMap::new();
                    for (record_type_str, answers) in records {
                        if let Ok(record_type) = RecordType::from_str(&record_type_str) {
                            record_map.insert(record_type, answers);
                        }
                    }
                    self.regex_patterns.push((regex, record_map));
                }
            } else {
                // Exact match
                for (record_type_str, answers) in records {
                    if let Ok(record_type) = RecordType::from_str(&record_type_str) {
                        self.mock_data
                            .entry(host.clone())
                            .or_default()
                            .insert(record_type, answers);
                    }
                }
            }
        }

        for host in nxdomains {
            if let Some(pattern) = host.strip_prefix("regex:") {
                // Regex pattern for NXDOMAIN
                if let Ok(regex) = Regex::new(pattern) {
                    self.nxdomain_patterns.push(regex);
                }
            } else {
                // Exact match for NXDOMAIN
                self.nxdomain_hosts.insert(host);
            }
        }
    }

    fn clear(&mut self) {
        self.mock_data.clear();
        self.regex_patterns.clear();
        self.nxdomain_hosts.clear();
        self.nxdomain_patterns.clear();
    }

    /// Resolve a hostname (mocked), returning full DNS response.
    /// Note: PTR formatting is handled by the trait implementation.
    async fn resolve_full_impl(
        &self,
        host: String,
        record_type: RecordType,
    ) -> Result<DnsResponse, BlastDNSError> {
        // Check if this host should return NXDOMAIN (exact match)
        if self.nxdomain_hosts.contains(&host) {
            return self.fabricate_response(&host, record_type, &[]);
        }

        // Check if this host matches any NXDOMAIN regex pattern
        for pattern in &self.nxdomain_patterns {
            if pattern.is_match(&host) {
                return self.fabricate_response(&host, record_type, &[]);
            }
        }

        // Check if we have exact match mock data for this host
        if let Some(host_data) = self.mock_data.get(&host)
            && let Some(answers_data) = host_data.get(&record_type)
        {
            return self.fabricate_response(&host, record_type, answers_data);
        }

        // Check if host matches any regex pattern
        for (pattern, record_map) in &self.regex_patterns {
            if pattern.is_match(&host)
                && let Some(answers_data) = record_map.get(&record_type)
            {
                return self.fabricate_response(&host, record_type, answers_data);
            }
        }

        // No mock data, return empty response
        self.fabricate_response(&host, record_type, &[])
    }

    fn fabricate_response(
        &self,
        host: &str,
        record_type: RecordType,
        answers_data: &[String],
    ) -> Result<DnsResponse, BlastDNSError> {
        // Ensure host has trailing dot (FQDN format)
        let fqdn = if host.ends_with('.') {
            host.to_string()
        } else {
            format!("{host}.")
        };

        let name = Name::from_str(&fqdn)
            .map_err(|e| BlastDNSError::Configuration(format!("invalid name: {e}")))?;

        // Create answer records
        let mut answers = Vec::new();
        for rdata_str in answers_data {
            if let Some(rdata) = self.parse_rdata(record_type, rdata_str)? {
                let record = Record::from_rdata(name.clone(), 300, rdata);
                answers.push(record);
            }
        }

        // Fabricate header
        let mut header = Header::new();
        header.set_id(12345);
        header.set_message_type(MessageType::Response);
        header.set_op_code(OpCode::Query);
        header.set_authoritative(false);
        header.set_truncated(false);
        header.set_recursion_desired(true);
        header.set_recursion_available(true);
        header.set_authentic_data(false);
        header.set_checking_disabled(false);
        header.set_response_code(ResponseCode::NoError);

        // Fabricate query
        let query = Query::query(name, record_type);

        // Build message
        let mut message = Message::new();
        message.set_header(header);
        message.add_query(query);
        for answer in answers {
            message.add_answer(answer);
        }

        DnsResponse::from_message(message)
            .map_err(|e| BlastDNSError::Configuration(format!("failed to create response: {e}")))
    }

    fn parse_rdata(
        &self,
        record_type: RecordType,
        rdata_str: &str,
    ) -> Result<Option<RData>, BlastDNSError> {
        let rdata = match record_type {
            RecordType::A => {
                let addr = rdata_str
                    .parse()
                    .map_err(|e| BlastDNSError::Configuration(format!("invalid A record: {e}")))?;
                RData::A(addr)
            }
            RecordType::AAAA => {
                let addr = rdata_str.parse().map_err(|e| {
                    BlastDNSError::Configuration(format!("invalid AAAA record: {e}"))
                })?;
                RData::AAAA(addr)
            }
            RecordType::CNAME => {
                let name = Name::from_str(rdata_str)
                    .map_err(|e| BlastDNSError::Configuration(format!("invalid CNAME: {e}")))?;
                RData::CNAME(CNAME(name))
            }
            RecordType::MX => {
                // MX records like "10 aspmx.l.google.com."
                let parts: Vec<&str> = rdata_str.split_whitespace().collect();
                if parts.len() == 2 {
                    let preference = parts[0].parse().map_err(|e| {
                        BlastDNSError::Configuration(format!("invalid MX preference: {e}"))
                    })?;
                    let exchange = Name::from_str(parts[1])
                        .map_err(|e| BlastDNSError::Configuration(format!("invalid MX: {e}")))?;
                    RData::MX(MX::new(preference, exchange))
                } else {
                    let exchange = Name::from_str(rdata_str)
                        .map_err(|e| BlastDNSError::Configuration(format!("invalid MX: {e}")))?;
                    RData::MX(MX::new(0, exchange))
                }
            }
            RecordType::TXT => RData::TXT(TXT::new(vec![rdata_str.to_string()])),
            RecordType::NS => {
                let name = Name::from_str(rdata_str)
                    .map_err(|e| BlastDNSError::Configuration(format!("invalid NS: {e}")))?;
                RData::NS(NS(name))
            }
            RecordType::PTR => {
                let name = Name::from_str(rdata_str)
                    .map_err(|e| BlastDNSError::Configuration(format!("invalid PTR: {e}")))?;
                RData::PTR(PTR(name))
            }
            RecordType::SOA => {
                // For simplicity, parse as minimal SOA or return None
                return Ok(None);
            }
            RecordType::SRV => {
                // For simplicity, parse as minimal SRV or return None
                return Ok(None);
            }
            _ => {
                // Unsupported record type
                return Ok(None);
            }
        };

        Ok(Some(rdata))
    }
}

impl Default for MockBlastDNSClient {
    fn default() -> Self {
        Self::new()
    }
}

// Implement the DnsResolver trait
impl DnsResolver for MockBlastDNSClient {
    fn resolve_full(
        &self,
        mut host: String,
        record_type: RecordType,
    ) -> impl std::future::Future<Output = Result<DnsResponse, BlastDNSError>> + Send {
        // Auto-format PTR queries if an IP address is provided
        if record_type == RecordType::PTR {
            host = format_ptr_query(&host);
        }

        self.resolve_full_impl(host, record_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_client::proto::rr::RecordType;

    fn create_test_mock_client() -> MockBlastDNSClient {
        let mut client = MockBlastDNSClient::new();

        let responses = HashMap::from([
            (
                "example.com".to_string(),
                HashMap::from([
                    ("A".to_string(), vec!["93.184.216.34".to_string()]),
                    (
                        "AAAA".to_string(),
                        vec!["2606:2800:220:1:248:1893:25c8:1946".to_string()],
                    ),
                    (
                        "MX".to_string(),
                        vec![
                            "10 aspmx.l.google.com.".to_string(),
                            "20 alt1.aspmx.l.google.com.".to_string(),
                        ],
                    ),
                ]),
            ),
            (
                "cname.example.com".to_string(),
                HashMap::from([("CNAME".to_string(), vec!["example.com.".to_string()])]),
            ),
        ]);

        let nxdomains = vec!["notfound.example.com".to_string()];

        client.mock_dns(responses, nxdomains);
        client
    }

    #[tokio::test]
    async fn test_resolve_a_record() {
        let client = create_test_mock_client();
        let result = client
            .resolve("example.com".to_string(), RecordType::A)
            .await;

        assert!(result.is_ok());
        let answers = result.unwrap();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0], "93.184.216.34");
    }

    #[tokio::test]
    async fn test_resolve_mx_records() {
        let client = create_test_mock_client();
        let result = client
            .resolve("example.com".to_string(), RecordType::MX)
            .await;

        assert!(result.is_ok());
        let answers = result.unwrap();
        assert_eq!(answers.len(), 2);
        assert!(answers.contains(&"10 aspmx.l.google.com.".to_string()));
        assert!(answers.contains(&"20 alt1.aspmx.l.google.com.".to_string()));
    }

    #[tokio::test]
    async fn test_resolve_nxdomain() {
        let client = create_test_mock_client();
        let result = client
            .resolve("notfound.example.com".to_string(), RecordType::A)
            .await;

        assert!(result.is_ok());
        let answers = result.unwrap();
        assert_eq!(answers.len(), 0, "NXDOMAIN should return empty list");
    }

    #[tokio::test]
    async fn test_resolve_unknown_host() {
        let client = create_test_mock_client();
        let result = client
            .resolve("unknown.example.com".to_string(), RecordType::A)
            .await;

        assert!(result.is_ok());
        let answers = result.unwrap();
        assert_eq!(answers.len(), 0, "Unknown host should return empty list");
    }

    #[tokio::test]
    async fn test_resolve_full_with_answers() {
        let client = create_test_mock_client();
        let result = client
            .resolve_full("example.com".to_string(), RecordType::A)
            .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.answers().len(), 1);
        assert_eq!(response.answers()[0].data().to_string(), "93.184.216.34");
    }

    #[tokio::test]
    async fn test_resolve_full_nxdomain() {
        let client = create_test_mock_client();
        let result = client
            .resolve_full("notfound.example.com".to_string(), RecordType::A)
            .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(
            response.answers().len(),
            0,
            "NXDOMAIN should return empty response"
        );
    }

    #[tokio::test]
    async fn test_resolve_multi() {
        let client = create_test_mock_client();
        let record_types = vec![RecordType::A, RecordType::AAAA, RecordType::MX];
        let result = client
            .resolve_multi("example.com".to_string(), record_types)
            .await;

        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.contains_key(&RecordType::A));
        assert!(results.contains_key(&RecordType::AAAA));
        assert!(results.contains_key(&RecordType::MX));
        assert_eq!(results[&RecordType::MX].len(), 2);
    }

    #[tokio::test]
    async fn test_resolve_multi_partial_mocking() {
        let client = create_test_mock_client();
        let record_types = vec![RecordType::A, RecordType::TXT];
        let result = client
            .resolve_multi("example.com".to_string(), record_types)
            .await;

        assert!(result.is_ok());
        let results = result.unwrap();
        // Only A should be in results (TXT has no mock data)
        assert_eq!(results.len(), 1);
        assert!(results.contains_key(&RecordType::A));
        assert!(!results.contains_key(&RecordType::TXT));
    }

    #[tokio::test]
    async fn test_resolve_multi_nxdomain() {
        let client = create_test_mock_client();
        let record_types = vec![RecordType::A, RecordType::AAAA];
        let result = client
            .resolve_multi("notfound.example.com".to_string(), record_types)
            .await;

        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 0, "NXDOMAIN should return empty results");
    }

    #[tokio::test]
    async fn test_resolve_multi_full() {
        let client = create_test_mock_client();
        let record_types = vec![RecordType::A, RecordType::AAAA, RecordType::MX];
        let result = client
            .resolve_multi_full("example.com".to_string(), record_types)
            .await;

        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 3);

        // All should be successful
        for result in results.values() {
            assert!(result.is_ok());
        }

        // Check MX has multiple answers
        let mx_result = &results[&RecordType::MX];
        assert!(mx_result.is_ok());
        let mx_response = mx_result.as_ref().unwrap();
        assert_eq!(mx_response.answers().len(), 2);
    }

    #[tokio::test]
    async fn test_resolve_multi_full_with_nxdomain() {
        let client = create_test_mock_client();
        let record_types = vec![RecordType::A];
        let result = client
            .resolve_multi_full("notfound.example.com".to_string(), record_types)
            .await;

        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 1);

        let a_result = &results[&RecordType::A];
        assert!(
            a_result.is_ok(),
            "NXDOMAIN should return Ok with empty response"
        );
        let response = a_result.as_ref().unwrap();
        assert_eq!(response.answers().len(), 0);
    }

    #[tokio::test]
    async fn test_ptr_auto_format_ipv4() {
        let mut client = MockBlastDNSClient::new();

        let responses = HashMap::from([(
            "8.8.8.8.in-addr.arpa".to_string(),
            HashMap::from([("PTR".to_string(), vec!["dns.google.".to_string()])]),
        )]);
        client.mock_dns(responses, vec![]);

        // Query with raw IP - should be auto-formatted
        let result = client.resolve("8.8.8.8".to_string(), RecordType::PTR).await;
        assert!(result.is_ok());
        let answers = result.unwrap();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0], "dns.google.");

        // Query with already-formatted string should also work
        let result2 = client
            .resolve("8.8.8.8.in-addr.arpa".to_string(), RecordType::PTR)
            .await;
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), answers);
    }

    #[tokio::test]
    async fn test_ptr_auto_format_ipv6() {
        let mut client = MockBlastDNSClient::new();

        let responses = HashMap::from([(
            "8.8.8.8.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.6.8.4.0.6.8.4.1.0.0.2.ip6.arpa".to_string(),
            HashMap::from([("PTR".to_string(), vec!["dns.google.".to_string()])]),
        )]);
        client.mock_dns(responses, vec![]);

        // Query with IPv6 address - should be auto-formatted
        let result = client
            .resolve("2001:4860:4860::8888".to_string(), RecordType::PTR)
            .await;
        assert!(result.is_ok());
        let answers = result.unwrap();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0], "dns.google.");
    }

    #[test]
    fn test_mock_dns() {
        let mut client = MockBlastDNSClient::new();

        let responses = HashMap::from([(
            "test.com".to_string(),
            HashMap::from([
                (
                    "A".to_string(),
                    vec!["1.2.3.4".to_string(), "5.6.7.8".to_string()],
                ),
                ("AAAA".to_string(), vec!["2001:db8::1".to_string()]),
            ]),
        )]);

        let nxdomains = vec!["bad.com".to_string(), "notfound.com".to_string()];

        client.mock_dns(responses, nxdomains);

        // Verify the data was loaded
        assert!(client.mock_data.contains_key("test.com"));
        assert_eq!(client.nxdomain_hosts.len(), 2);
        assert!(client.nxdomain_hosts.contains("bad.com"));
        assert!(client.nxdomain_hosts.contains("notfound.com"));
    }

    #[tokio::test]
    async fn test_cname_record() {
        let client = create_test_mock_client();
        let result = client
            .resolve("cname.example.com".to_string(), RecordType::CNAME)
            .await;

        assert!(result.is_ok());
        let answers = result.unwrap();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0], "example.com.");
    }

    #[tokio::test]
    async fn test_empty_response_structure() {
        let client = create_test_mock_client();
        let result = client
            .resolve_full("unknown.example.com".to_string(), RecordType::A)
            .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.answers().len(), 0);
        assert_eq!(response.response_code().to_string(), "No Error");
        assert_eq!(response.queries().len(), 1);
    }

    #[tokio::test]
    async fn test_regex_wildcard_subdomain() {
        let mut client = MockBlastDNSClient::new();

        let responses = HashMap::from([(
            "regex:.*\\.example\\.com".to_string(),
            HashMap::from([("A".to_string(), vec!["192.168.1.1".to_string()])]),
        )]);

        client.mock_dns(responses, vec![]);

        // Should match any subdomain
        let result = client
            .resolve("api.example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["192.168.1.1"]);

        let result = client
            .resolve("cdn.example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["192.168.1.1"]);

        let result = client
            .resolve("sub.domain.example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["192.168.1.1"]);

        // Should not match the base domain
        let result = client
            .resolve("example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_regex_numeric_pattern() {
        let mut client = MockBlastDNSClient::new();

        let responses = HashMap::from([(
            "regex:^server-\\d+\\.test\\.com$".to_string(),
            HashMap::from([("A".to_string(), vec!["10.0.0.1".to_string()])]),
        )]);

        client.mock_dns(responses, vec![]);

        // Should match numbered servers
        let result = client
            .resolve("server-1.test.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["10.0.0.1"]);

        let result = client
            .resolve("server-42.test.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["10.0.0.1"]);

        // Should not match non-numeric
        let result = client
            .resolve("server-abc.test.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_regex_nxdomain_pattern() {
        let mut client = MockBlastDNSClient::new();

        let responses = HashMap::from([(
            "good.example.com".to_string(),
            HashMap::from([("A".to_string(), vec!["1.2.3.4".to_string()])]),
        )]);

        let nxdomains = vec!["regex:^bad-.*\\.example\\.com$".to_string()];

        client.mock_dns(responses, nxdomains);

        // Should return NXDOMAIN for matching pattern
        let result = client
            .resolve("bad-host.example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        let result = client
            .resolve("bad-server.example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        // Should return normal result for non-matching
        let result = client
            .resolve("good.example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["1.2.3.4"]);
    }

    #[tokio::test]
    async fn test_regex_exact_match_priority() {
        let mut client = MockBlastDNSClient::new();

        let responses = HashMap::from([
            (
                "specific.example.com".to_string(),
                HashMap::from([("A".to_string(), vec!["10.0.0.1".to_string()])]),
            ),
            (
                "regex:.*\\.example\\.com".to_string(),
                HashMap::from([("A".to_string(), vec!["192.168.1.1".to_string()])]),
            ),
        ]);

        client.mock_dns(responses, vec![]);

        // Exact match should take priority
        let result = client
            .resolve("specific.example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["10.0.0.1"]);

        // Regex should match others
        let result = client
            .resolve("other.example.com".to_string(), RecordType::A)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["192.168.1.1"]);
    }
}
