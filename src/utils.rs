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

/// File descriptors needed to run this configuration.
///
/// The UDP socket count depends on the transport: per-query sockets exist only
/// while a query is in flight, so at most `max_concurrency` are open at once,
/// while persistent sockets are held for the client's life, one per resolver.
/// Truncated responses can add a TCP socket per in-flight query on top.
fn required_fds(max_concurrency: usize, resolvers: usize, persistent_socket: bool) -> usize {
    let udp = if persistent_socket {
        resolvers
    } else {
        max_concurrency
    };
    udp + max_concurrency + 100
}

/// Checks if the system's NOFILE limit is sufficient for the given configuration.
pub fn check_ulimits(
    #[cfg_attr(not(unix), allow(unused_variables))] max_concurrency: usize,
    #[cfg_attr(not(unix), allow(unused_variables))] resolvers: usize,
    #[cfg_attr(not(unix), allow(unused_variables))] persistent_socket: bool,
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

        let required = required_fds(max_concurrency, resolvers, persistent_socket);

        // rlim_cur is u64 on most platforms but u32 on armv7, so convert for portability
        #[allow(clippy::useless_conversion)]
        if u64::from(current_limit) < required as u64 {
            let driver = if persistent_socket {
                format!(
                    "persistent sockets for {resolvers} resolvers plus max_concurrency {max_concurrency}"
                )
            } else {
                format!("max_concurrency of {max_concurrency}")
            };
            bail!(
                "NOFILE limit too low even after raising soft limit: current={}, required={}\n\
                 {} needs ~{} FDs\n\
                 Increase with: ulimit -n {} (or higher), or lower max_concurrency",
                current_limit,
                required,
                driver,
                required,
                required
            );
        }

        tracing::debug!(
            "ulimit check: NOFILE={} (need ~{} for max_concurrency {}, {} resolvers, persistent_socket={})",
            current_limit,
            required,
            max_concurrency,
            resolvers,
            persistent_socket
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
    fn per_query_sockets_scale_with_concurrency_not_resolvers() {
        // A UDP socket is bound per query in flight, so a small resolver list
        // with high concurrency still needs a high limit. Basing the check on
        // the resolver count would let it pass and then run out of FDs mid-run.
        let modest = required_fds(100, 5, false);
        let heavy = required_fds(5000, 5, false);
        assert!(
            heavy > modest * 10,
            "concurrency must dominate the requirement: {modest} vs {heavy}"
        );
        assert!(
            heavy >= 5000,
            "5000 concurrent queries need at least that many FDs, got {heavy}"
        );
    }

    #[test]
    fn persistent_sockets_scale_with_the_resolver_list() {
        // Persistent sockets are held for the client's life, one per resolver, so
        // a large pool outgrows the concurrency setting. Sizing this off
        // concurrency alone passes a config that then runs out of FDs.
        let concurrency = 1000;
        let resolvers = 6000;
        let persistent = required_fds(concurrency, resolvers, true);
        let per_query = required_fds(concurrency, resolvers, false);
        assert!(
            persistent >= resolvers,
            "{resolvers} persistent sockets need at least that many FDs, got {persistent}"
        );
        assert!(
            persistent > per_query,
            "a resolver list larger than concurrency must raise the requirement: \
             {per_query} per-query vs {persistent} persistent"
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
