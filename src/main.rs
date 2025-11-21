use std::{path::PathBuf, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use blastdns::{
    BlastDNSClient, BlastDNSConfig, DEFAULT_MAX_RETRIES, DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_THREADS_PER_RESOLVER,
};
use clap::Parser;
use futures::StreamExt;
use hickory_client::proto::rr::RecordType;
use serde_json::{json, to_string};

#[derive(Parser, Debug)]
#[command(author, version, about = "Async DNS spray client", long_about = None)]
struct Args {
    /// File containing hostnames to resolve (one per line).
    #[arg(value_name = "HOSTS_FILE")]
    hosts: PathBuf,
    /// Record type to query (A, AAAA, MX, ...).
    #[arg(long = "rdtype", default_value = "A", value_parser = parse_record_type)]
    record_type: RecordType,
    /// File containing resolver endpoints (one per line).
    #[arg(long, value_name = "FILE")]
    resolvers: PathBuf,
    /// Worker threads per resolver.
    #[arg(long, default_value_t = DEFAULT_THREADS_PER_RESOLVER)]
    threads_per_resolver: usize,
    /// Per-request timeout in milliseconds.
    #[arg(long, default_value_t = DEFAULT_REQUEST_TIMEOUT.as_millis() as u64)]
    timeout_ms: u64,
    /// Retry attempts after a resolver failure.
    #[arg(long, default_value_t = DEFAULT_MAX_RETRIES)]
    retries: usize,
    /// Enable debug logging to show which resolver handles each query.
    #[arg(long)]
    debug: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let resolvers = load_resolvers(&args.resolvers)
        .with_context(|| format!("failed to load resolvers from {}", args.resolvers.display()))?;
    let hosts = load_hosts(&args.hosts)
        .with_context(|| format!("failed to load hostnames from {}", args.hosts.display()))?;

    let timeout = Duration::from_millis(args.timeout_ms.max(1));
    let config = BlastDNSConfig {
        threads_per_resolver: args.threads_per_resolver.max(1),
        request_timeout: timeout,
        debug: args.debug,
        max_retries: args.retries,
    };

    let client = BlastDNSClient::with_config(resolvers, config).await?;
    let mut stream = client.resolve_batch(hosts, args.record_type);

    while let Some((host, outcome)) = stream.next().await {
        match outcome {
            Ok(response) => {
                let message = response.into_message();
                let payload = json!({ "host": host, "response": message });
                println!("{}", to_string(&payload)?);
            }
            Err(err) => {
                let payload = json!({ "host": host, "error": err.to_string() });
                println!("{}", to_string(&payload)?);
            }
        }
    }

    Ok(())
}

fn parse_record_type(value: &str) -> std::result::Result<RecordType, String> {
    let upper = value.trim().to_ascii_uppercase();
    RecordType::from_str(&upper).map_err(|_| format!("invalid record type `{value}`"))
}

fn load_resolvers(path: &PathBuf) -> Result<Vec<String>> {
    let buf = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in buf.lines() {
        let trimmed = line.split('#').next().unwrap_or("").trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(trimmed.to_string());
    }

    if out.is_empty() {
        anyhow::bail!("resolver list `{}` is empty", path.display());
    }

    Ok(out)
}

fn load_hosts(path: &PathBuf) -> Result<Vec<String>> {
    let buf = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in buf.lines() {
        let trimmed = line.split('#').next().unwrap_or("").trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(trimmed.to_string());
    }

    if out.is_empty() {
        anyhow::bail!("host list `{}` is empty", path.display());
    }

    Ok(out)
}
