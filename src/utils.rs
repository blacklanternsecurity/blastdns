use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use crate::error::BlastDNSError;

pub(crate) fn parse_resolver(input: &str) -> Result<SocketAddr, BlastDNSError> {
    match SocketAddr::from_str(input) {
        Ok(addr) => Ok(addr),
        Err(original) => {
            let trimmed = input.trim();
            let stripped = trimmed.trim_matches(|c| c == '[' || c == ']');
            if let Ok(ip) = IpAddr::from_str(stripped) {
                return Ok(SocketAddr::new(ip, 53));
            }

            Err(BlastDNSError::InvalidResolver {
                resolver: input.to_string(),
                source: original,
            })
        }
    }
}

