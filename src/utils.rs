use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use anyhow::{Result, bail};

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
pub fn check_ulimits(num_resolvers: usize, threads_per_resolver: usize) -> Result<()> {
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

        let current_limit = rlimit.rlim_cur;
        let total_workers = num_resolvers * threads_per_resolver;

        // Each worker needs at least 1 FD for the UDP socket,
        // plus hickory spawns background tasks that may use additional FDs.
        // Add overhead for stdin/stdout/stderr and other system needs.
        let required = (total_workers * 3) + 100;

        if current_limit < required as u64 {
            bail!(
                "NOFILE limit too low: current={}, required={}\n\
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

        eprintln!(
            "ulimit check: NOFILE={} (need ~{} for {} workers)",
            current_limit, required, total_workers
        );
    }

    Ok(())
}
