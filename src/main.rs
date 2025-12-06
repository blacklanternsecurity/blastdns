use std::{
    fs::File,
    io::{BufRead, BufReader, stdin},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use blastdns::{
    BlastDNSClient, BlastDNSConfig, DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_MAX_TTL,
    DEFAULT_CACHE_MIN_TTL, DEFAULT_MAX_RETRIES, DEFAULT_PURGATORY_SENTENCE,
    DEFAULT_PURGATORY_THRESHOLD, DEFAULT_REQUEST_TIMEOUT, DEFAULT_THREADS_PER_RESOLVER,
    DnsResolver,
};
use clap::Parser;
use futures::StreamExt;
use hickory_client::proto::rr::RecordType;
use serde_json::{json, to_string};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(author, version, about = "BlastDNS - Ultra-fast DNS Resolver written in Rust", long_about = None)]
struct Args {
    /// File containing hostnames to resolve (one per line). Reads from stdin if not specified.
    #[arg(value_name = "HOSTS_TO_RESOLVE")]
    hosts: Option<String>,
    /// Record type to query (A, AAAA, MX, ...).
    #[arg(long = "rdtype", default_value = "A", value_parser = parse_record_type)]
    record_type: RecordType,
    /// File containing DNS nameservers (one per line).
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
    /// Consecutive errors before a worker is put into timeout.
    #[arg(long, default_value_t = DEFAULT_PURGATORY_THRESHOLD)]
    purgatory_threshold: usize,
    /// How many milliseconds a worker stays in timeout.
    #[arg(long, default_value_t = DEFAULT_PURGATORY_SENTENCE.as_millis() as u64)]
    purgatory_sentence_ms: u64,
    /// Don't show responses with no answers.
    #[arg(long)]
    skip_empty: bool,
    /// Don't show error responses.
    #[arg(long)]
    skip_errors: bool,
    /// Output brief format (hostname, record type, answers only).
    #[arg(long)]
    brief: bool,
    /// DNS cache capacity (0 = disabled).
    #[arg(long, default_value_t = DEFAULT_CACHE_CAPACITY)]
    cache_capacity: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let resolvers = load_resolvers(&args.resolvers)
        .with_context(|| format!("failed to load resolvers from {}", args.resolvers.display()))?;
    let hosts = load_hosts(args.hosts.clone()).with_context(|| "failed to load hostnames")?;

    let timeout = Duration::from_millis(args.timeout_ms.max(1));
    let config = BlastDNSConfig {
        threads_per_resolver: args.threads_per_resolver.max(1),
        request_timeout: timeout,
        max_retries: args.retries,
        purgatory_threshold: args.purgatory_threshold,
        purgatory_sentence: Duration::from_millis(args.purgatory_sentence_ms),
        cache_capacity: args.cache_capacity,
        cache_min_ttl: DEFAULT_CACHE_MIN_TTL,
        cache_max_ttl: DEFAULT_CACHE_MAX_TTL,
    };

    let client = Arc::new(BlastDNSClient::with_config(resolvers, config)?);

    if args.brief {
        let mut stream = client.resolve_batch(
            hosts.map(Ok::<_, std::convert::Infallible>),
            args.record_type,
        );

        while let Some((host, record_type, answers)) = stream.next().await {
            let payload = json!({
                "host": host,
                "record_type": record_type,
                "answers": answers,
            });
            println!("{}", to_string(&payload)?);
        }
    } else {
        let mut stream = client.resolve_batch_full(
            hosts.map(Ok::<_, std::convert::Infallible>),
            args.record_type,
            args.skip_empty,
            args.skip_errors,
        );

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
        bail!("resolver list `{}` is empty", path.display());
    }

    Ok(out)
}

fn load_hosts(path: Option<String>) -> Result<impl Iterator<Item = String> + Send> {
    let reader: Box<dyn BufRead + Send> = match path {
        None => Box::new(BufReader::new(stdin())),
        Some(p) => Box::new(BufReader::new(File::open(p)?)),
    };

    Ok(reader.lines().filter_map(|line| {
        line.ok().and_then(|l| {
            let trimmed = l.split('#').next().unwrap_or("").trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
    }))
}
