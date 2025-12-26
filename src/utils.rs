use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use anyhow::Result;
#[cfg(unix)]
use anyhow::bail;
use hickory_resolver::system_conf;

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

/// Checks if the system's NOFILE limit is sufficient for the given configuration.
/// Each worker needs file descriptors for UDP sockets, plus overhead.
pub fn check_ulimits(
    #[cfg_attr(not(unix), allow(unused_variables))] num_resolvers: usize,
    #[cfg_attr(not(unix), allow(unused_variables))] threads_per_resolver: usize,
) -> Result<()> {
    #[cfg(unix)]
    {
        let mut rlimit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };

        unsafe {
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlimit) != 0 {
                bail!("failed to read RLIMIT_NOFILE");
            }
        }

        let hard_limit = rlimit.rlim_max;

        if rlimit.rlim_cur < hard_limit {
            let desired = libc::rlimit {
                rlim_cur: hard_limit,
                rlim_max: hard_limit,
            };

            unsafe {
                if libc::setrlimit(libc::RLIMIT_NOFILE, &desired) != 0 {
                    bail!(
                        "failed to raise RLIMIT_NOFILE to hard limit (soft={}, hard={}): {}",
                        rlimit.rlim_cur,
                        hard_limit,
                        std::io::Error::last_os_error()
                    );
                }

                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlimit) != 0 {
                    bail!("failed to re-read RLIMIT_NOFILE after raising it");
                }
            }
        }

        let current_limit = rlimit.rlim_cur;
        let total_workers = num_resolvers * threads_per_resolver;

        // Each worker needs at least 1 FD for the UDP socket,
        // plus hickory spawns background tasks that may use additional FDs.
        // Add overhead for stdin/stdout/stderr and other system needs.
        let required = (total_workers * 3) + 100;

        // rlim_cur is u64 on most platforms but u32 on armv7, so convert for portability
        #[allow(clippy::useless_conversion)]
        if u64::from(current_limit) < required as u64 {
            bail!(
                "NOFILE limit too low even after raising soft limit: current={}, required={}\n\
                 {} resolvers × {} threads/resolver = {} workers (need ~{} FDs)\n\
                 Increase with: ulimit -n {} (or higher)",
                current_limit,
                required,
                num_resolvers,
                threads_per_resolver,
                total_workers,
                required,
                required
            );
        }

        tracing::debug!(
            "ulimit check: NOFILE={} (need ~{} for {} workers)",
            current_limit,
            required,
            total_workers
        );
    }

    Ok(())
}

/// Get system DNS resolver IP addresses from OS configuration.
/// Works on Unix, Windows, macOS, and Android.
pub fn get_system_resolvers() -> Result<Vec<IpAddr>, BlastDNSError> {
    use std::collections::HashSet;

    let (config, _options) = system_conf::read_system_conf().map_err(|e| {
        BlastDNSError::Configuration(format!("Failed to read system DNS configuration: {}", e))
    })?;

    let resolver_ips: Vec<IpAddr> = config
        .name_servers()
        .iter()
        .map(|ns| ns.socket_addr.ip())
        .collect::<HashSet<_>>() // Deduplicate
        .into_iter()
        .collect();

    if resolver_ips.is_empty() {
        return Err(BlastDNSError::Configuration(
            "No system resolvers found".to_string(),
        ));
    }

    Ok(resolver_ips)
}

/// Format an IP address for PTR lookup.
/// IPv4: "8.8.8.8" -> "8.8.8.8.in-addr.arpa"
/// IPv6: "2001:4860:4860::8888" -> (expanded, reversed nibbles).ip6.arpa
pub fn format_ptr_query(host: &str) -> String {
    // Try to parse as IP address
    if let Ok(ip) = host.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(ipv4) => {
                let octets = ipv4.octets();
                format!(
                    "{}.{}.{}.{}.in-addr.arpa",
                    octets[3], octets[2], octets[1], octets[0]
                )
            }
            IpAddr::V6(ipv6) => {
                let segments = ipv6.segments();
                let mut nibbles = Vec::new();

                // Convert each segment to nibbles (4 hex digits)
                for segment in segments.iter() {
                    nibbles.push((segment >> 12) & 0xF);
                    nibbles.push((segment >> 8) & 0xF);
                    nibbles.push((segment >> 4) & 0xF);
                    nibbles.push(segment & 0xF);
                }

                // Reverse and join with dots
                nibbles.reverse();
                let reversed: Vec<String> = nibbles.iter().map(|n| format!("{:x}", n)).collect();
                format!("{}.ip6.arpa", reversed.join("."))
            }
        }
    } else {
        // Not an IP address, return as-is
        host.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_ptr_query_handles_ipv4() {
        assert_eq!(format_ptr_query("8.8.8.8"), "8.8.8.8.in-addr.arpa");
        assert_eq!(format_ptr_query("192.168.1.1"), "1.1.168.192.in-addr.arpa");
    }

    #[test]
    fn format_ptr_query_handles_ipv6() {
        // Short form
        assert_eq!(
            format_ptr_query("::1"),
            "1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa"
        );

        // Full form
        assert_eq!(
            format_ptr_query("2001:4860:4860::8888"),
            "8.8.8.8.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.6.8.4.0.6.8.4.1.0.0.2.ip6.arpa"
        );
    }

    #[test]
    fn format_ptr_query_leaves_formatted_queries_unchanged() {
        assert_eq!(
            format_ptr_query("8.8.8.8.in-addr.arpa"),
            "8.8.8.8.in-addr.arpa"
        );
        assert_eq!(format_ptr_query("example.com"), "example.com");
    }
}
