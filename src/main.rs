use std::{path::PathBuf, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use blastdns::{BlastDNSClient, BlastDNSConfig, DEFAULT_THREADS_PER_RESOLVER, DEFAULT_REQUEST_TIMEOUT};
use clap::Parser;
use hickory_client::proto::rr::RecordType;
use serde_json::to_string_pretty;

#[derive(Parser, Debug)]
#[command(author, version, about = "Async DNS spray client", long_about = None)]
struct Args {
    /// Domain name to resolve.
    #[arg(value_name = "HOSTNAME")]
    host: String,
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
    /// Enable debug logging to show which resolver handles each query.
    #[arg(long)]
    debug: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let resolvers = load_resolvers(&args.resolvers)
        .with_context(|| format!("failed to load resolvers from {}", args.resolvers.display()))?;

    let timeout = Duration::from_millis(args.timeout_ms.max(1));
    let mut config = BlastDNSConfig::default();
    config.threads_per_resolver = args.threads_per_resolver.max(1);
    config.request_timeout = timeout;
    config.debug = args.debug;

    let client = BlastDNSClient::with_config(resolvers, config).await?;
    let response = client.resolve(&args.host, args.record_type).await?;

    println!("{}", to_string_pretty(&*response)?);
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
