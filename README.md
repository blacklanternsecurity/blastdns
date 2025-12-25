# BlastDNS

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-black.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Rust 2024](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/blastdns.svg?color=orange)](https://crates.io/crates/blastdns)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://img.shields.io/pypi/v/blastdns.svg?color=blue)](https://pypi.org/project/blastdns/)
[![Rust Tests](https://github.com/blacklanternsecurity/blastdns/actions/workflows/rust-tests.yml/badge.svg)](https://github.com/blacklanternsecurity/blastdns/actions/workflows/rust-tests.yml)
[![Python Tests](https://github.com/blacklanternsecurity/blastdns/actions/workflows/python-tests.yml/badge.svg)](https://github.com/blacklanternsecurity/blastdns/actions/workflows/python-tests.yml)

[BlastDNS](https://github.com/blacklanternsecurity/blastdns) is an ultra-fast DNS resolver written in Rust. Like [massdns](https://github.com/blechschmidt/massdns), it's designed to be faster the more resolvers you give it. Features include built-in caching, and high accuracy even with unreliable DNS servers. For details, see [Architecture](#architecture). BlastDNS is the main DNS library used by [BBOT](https://github.com/blacklanternsecurity/bbot).

There are three ways to use it:

- [Rust CLI tool](#cli)
- [Rust library](#rust-api)
- [Python library](#python-api)

## Benchmark

100K DNS lookups against local `dnsmasq`, with 100 workers:

| Library         | Language | Time    | QPS    | Success | Failed | vs dnspython |
|-----------------|----------|---------|--------|---------|--------|--------------|
| massdns         | C        | 1.370s  | 72,998 | 100,000 | 0      | 28.63x       |
| blastdns-cli    | Rust     | 1.654s  | 60,470 | 100,000 | 0      | 23.72x       |
| blastdns-python | Python   | 2.485s  | 40,249 | 100,000 | 0      | 15.79x       |
| dnspython       | Python   | 39.223s | 2,550  | 100,000 | 0      | 1.00x        |

### CLI

The CLI mass-resolves hosts using a specified list of resolvers. It outputs to JSON.

```bash
# send all results to jq
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt | jq

# print only the raw IPv4 addresses
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt | jq '.response.answers[].rdata.A'

# load from stdin
$ cat hosts.txt | blastdns --rdtype A --resolvers resolvers.txt

# skip empty responses (e.g., NXDOMAIN with no answers)
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt --skip-empty | jq

# skip error responses (e.g., timeouts, connection failures)
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt --skip-errors | jq
```

#### CLI Help

```
$ blastdns --help
BlastDNS - Ultra-fast DNS Resolver written in Rust

Usage: blastdns [OPTIONS] --resolvers <FILE> [HOSTS_TO_RESOLVE]

Arguments:
  [HOSTS_TO_RESOLVE]  File containing hostnames to resolve (one per line). Reads from stdin if not specified

Options:
      --rdtype <RECORD_TYPE>
          Record type to query (A, AAAA, MX, ...) [default: A]
      --resolvers <FILE>
          File containing DNS nameservers (one per line)
      --threads-per-resolver <THREADS_PER_RESOLVER>
          Worker threads per resolver [default: 2]
      --timeout-ms <TIMEOUT_MS>
          Per-request timeout in milliseconds [default: 1000]
      --retries <RETRIES>
          Retry attempts after a resolver failure [default: 10]
      --purgatory-threshold <PURGATORY_THRESHOLD>
          Consecutive errors before a worker is put into timeout [default: 10]
      --purgatory-sentence-ms <PURGATORY_SENTENCE_MS>
          How many milliseconds a worker stays in timeout [default: 1000]
      --skip-empty
          Don't show responses with no answers
      --skip-errors
          Don't show error responses
      --brief
          Output brief format (hostname, record type, answers only)
      --cache-capacity <CACHE_CAPACITY>
          DNS cache capacity (0 = disabled) [default: 10000]
  -h, --help
          Print help
  -V, --version
          Print version
```

#### Example JSON output

BlastDNS outputs to JSON by default:

```json
{
  "host": "microsoft.com",
  "response": {
    "additionals": [],
    "answers": [
      {
        "dns_class": "IN",
        "name_labels": "microsoft.com.",
        "rdata": {
          "A": "13.107.213.41"
        },
        "ttl": 1968
      },
      {
        "dns_class": "IN",
        "name_labels": "microsoft.com.",
        "rdata": {
          "A": "13.107.246.41"
        },
        "ttl": 1968
      }
    ],
    "edns": {
      "flags": {
        "dnssec_ok": false,
        "z": 0
      },
      "max_payload": 1232,
      "options": {
        "options": []
      },
      "rcode_high": 0,
      "version": 0
    },
    "header": {
      "additional_count": 1,
      "answer_count": 2,
      "authentic_data": false,
      "authoritative": false,
      "checking_disabled": false,
      "id": 62150,
      "message_type": "Response",
      "name_server_count": 0,
      "op_code": "Query",
      "query_count": 1,
      "recursion_available": true,
      "recursion_desired": true,
      "response_code": "NoError",
      "truncation": false
    },
    "name_servers": [],
    "queries": [
      {
        "name": "microsoft.com.",
        "query_class": "IN",
        "query_type": "A"
      }
    ],
    "signature": []
  }
}
```

#### Debug Logging

BlastDNS uses the standard Rust `tracing` ecosystem. Enable debug logging by setting the `RUST_LOG` environment variable:

```bash
# Show debug logs from blastdns only
RUST_LOG=blastdns=debug blastdns hosts.txt --rdtype A --resolvers resolvers.txt

# Show debug logs from everything
RUST_LOG=debug blastdns hosts.txt --rdtype A --resolvers resolvers.txt

# Show trace-level logs for detailed internal behavior
RUST_LOG=blastdns=trace blastdns hosts.txt --rdtype A --resolvers resolvers.txt
```

Valid log levels (from least to most verbose): `error`, `warn`, `info`, `debug`, `trace`

### Rust API

#### Installation

```bash
# Install CLI tool
cargo install blastdns

# Add library to your project
cargo add blastdns
```

#### Usage

BlastDNS can either use system resolvers (detected automatically from OS configuration) or custom resolvers:

```rust
use blastdns::{BlastDNSClient, BlastDNSConfig};
use futures::StreamExt;
use hickory_client::proto::rr::RecordType;
use std::time::Duration;

// Option 1: Use system DNS resolvers (default)
let client = BlastDNSClient::new(vec![]).await?;

// Check what resolvers are being used
println!("Using resolvers: {:?}", client.resolvers());

// Option 2: Read DNS resolvers from a file (one per line -> vector of strings)
let resolvers = std::fs::read_to_string("resolvers.txt")
    .expect("Failed to read resolvers file")
    .lines()
    .map(str::to_string)
    .collect::<Vec<String>>();

// create a new blastdns client with default config
let client = BlastDNSClient::new(resolvers).await?;

// or with custom config
let mut config = BlastDNSConfig::default();
config.threads_per_resolver = 5;
config.request_timeout = Duration::from_secs(2);
let client = BlastDNSClient::with_config(resolvers, config).await?;

// resolve: lookup a domain, returns only the rdata strings
let answers = client.resolve("example.com", RecordType::A).await?;
for answer in answers {
    println!("{}", answer);  // e.g., "93.184.216.34"
}

// resolve_full: lookup a domain, returns the full DNS response
let result = client.resolve_full("example.com", RecordType::A).await?;
println!("{}", serde_json::to_string_pretty(&result).unwrap());

// resolve_batch: process many hosts in parallel, returns simplified output
// streams back (host, record_type, Vec<rdata>) tuples as they complete
// automatically filters out errors and empty responses
let wordlist = ["one.example", "two.example", "three.example"];
let mut stream = client.resolve_batch(
    wordlist.into_iter().map(Ok::<_, std::convert::Infallible>),
    RecordType::A,
);
while let Some((host, record_type, answers)) = stream.next().await {
    println!("{} ({}):", host, record_type);
    for answer in answers {
        println!("  {}", answer);  // e.g., "93.184.216.34" for A records
    }
}

// resolve_batch_full: process many hosts with full DNS response structures
// streams back (host, Result<response>) tuples with configurable filtering
let wordlist = ["one.example", "two.example", "three.example"];
let mut stream = client.resolve_batch_full(
    wordlist.into_iter().map(Ok::<_, std::convert::Infallible>),
    RecordType::A,
    false,  // skip_empty: don't filter out empty responses
    false,  // skip_errors: don't filter out errors
);
while let Some((host, outcome)) = stream.next().await {
    match outcome {
        Ok(response) => println!("{}: {} answers", host, response.answers().len()),
        Err(err) => eprintln!("{} failed: {err}", host),
    }
}

// resolve_multi: resolve multiple record types for a single host
// returns only successful results with answers as dict[record_type, Vec<rdata>]
let record_types = vec![RecordType::A, RecordType::AAAA, RecordType::MX];
let results = client.resolve_multi("example.com", record_types).await?;
for (record_type, answers) in results {
    println!("{}: {} answers", record_type, answers.len());
    for answer in answers {
        println!("  {}", answer);
    }
}

// resolve_multi_full: resolve multiple record types with full responses
// returns all results (success and failure) as dict[record_type, Result<response>]
let record_types = vec![RecordType::A, RecordType::AAAA, RecordType::MX];
let results = client.resolve_multi_full("example.com", record_types).await?;
for (record_type, result) in results {
    match result {
        Ok(response) => println!("{}: {} answers", record_type, response.answers().len()),
        Err(err) => eprintln!("{} failed: {err}", record_type),
    }
}
```

#### System Resolvers

You can retrieve the system's configured DNS resolvers programmatically:

```rust
use blastdns::get_system_resolvers;

// Get system resolver IPs (works on Unix, Windows, macOS, Android)
let resolver_ips = get_system_resolvers()?;
for ip in resolver_ips {
    println!("System resolver: {}", ip);
}
```

#### MockBlastDNSClient for Testing

`MockBlastDNSClient` implements the `DnsResolver` trait and provides a drop-in replacement that returns fabricated DNS responses without making real network requests.

```rust
use blastdns::{MockBlastDNSClient, DnsResolver};
use hickory_client::proto::rr::RecordType;
use std::collections::HashMap;

// Create a mock client
let mut mock_client = MockBlastDNSClient::new();

// Configure mock responses
let responses = HashMap::from([
    (
        "example.com".to_string(),
        HashMap::from([
            ("A".to_string(), vec!["93.184.216.34".to_string()]),
            ("AAAA".to_string(), vec!["2606:2800:220:1:248:1893:25c8:1946".to_string()]),
        ]),
    ),
]);

// Hosts that should return NXDOMAIN
let nxdomains = vec!["notfound.example.com".to_string()];

mock_client.mock_dns(responses, nxdomains);

// Use like any DnsResolver
let answers = mock_client.resolve("example.com".to_string(), RecordType::A).await?;
assert_eq!(answers, vec!["93.184.216.34"]);

// NXDOMAIN hosts return empty responses
let answers = mock_client.resolve("notfound.example.com".to_string(), RecordType::A).await?;
assert_eq!(answers.len(), 0);
```

`MockBlastDNSClient` supports all `DnsResolver` methods including `resolve`, `resolve_full`, `resolve_batch`, `resolve_batch_full`, `resolve_multi`, and `resolve_multi_full`.

### Python API

The `blastdns` Python package is a thin wrapper around the Rust library.

#### Installation

```bash
# Using pip
pip install blastdns

# Using uv
uv add blastdns

# Using poetry
poetry add blastdns
```

#### Development Setup

```bash
# install python dependencies
uv sync
# build and install the rust->python bindings
uv run maturin develop
# run tests
uv run pytest
```

#### Usage

To use it in Python, you can use the `Client` class:

```python
import asyncio
from blastdns import Client, ClientConfig, DNSResult, DNSError, get_system_resolvers


async def main():
    # Option 1: Use system resolvers (pass empty list)
    client = Client([], ClientConfig(threads_per_resolver=4, request_timeout_ms=1500))
    
    # Check what resolvers are being used
    print(f"Using resolvers: {client.resolvers}")
    
    # Option 2: Manually get system resolvers
    system_resolvers = get_system_resolvers()
    print(f"System resolvers: {system_resolvers}")
    
    # Option 3: Use custom resolvers
    resolvers = ["1.1.1.1:53", "8.8.8.8:53"]
    client = Client(resolvers, ClientConfig(threads_per_resolver=4, request_timeout_ms=1500))

    # resolve: lookup a single host, returns only rdata strings
    answers = await client.resolve("example.com", "A")
    for answer in answers:
        print(f"  {answer}")  # e.g., "93.184.216.34"

    # resolve_full: lookup a single host, returns full DNS response as Pydantic model
    result = await client.resolve_full("example.com", "AAAA")
    print(f"Host: {result.host}")
    print(f"Response code: {result.response.header.response_code}")
    for answer in result.response.answers:
        print(f"  {answer.name_labels}: {answer.rdata}")

    # resolve_batch: simplified batch resolution with minimal output
    # returns only (host, record_type, list[rdata]) - no full DNS response structures
    # automatically filters out errors and empty responses
    hosts = ["example.com", "google.com", "github.com"]
    async for host, rdtype, answers in client.resolve_batch(hosts, "A"):
        print(f"{host} ({rdtype}):")
        for answer in answers:
            print(f"  {answer}")  # e.g., "93.184.216.34" for A records

    # resolve_batch_full: process many hosts in parallel with full responses
    # streams results back as they complete
    hosts = ["one.example.com", "two.example.com", "three.example.com"]
    async for host, result in client.resolve_batch_full(hosts, "A"):
        if isinstance(result, DNSError):
            print(f"{host} failed: {result.error}")
        else:
            print(f"{host}: {len(result.response.answers)} answers")

    # resolve_multi: resolve multiple record types for a single host in parallel
    # returns only successful results with answers
    record_types = ["A", "AAAA", "MX"]
    results = await client.resolve_multi("example.com", record_types)
    for record_type, answers in results.items():
        print(f"{record_type}: {answers}")

    # resolve_multi_full: resolve multiple record types with full response data
    record_types = ["A", "AAAA", "MX"]
    results = await client.resolve_multi_full("example.com", record_types)
    for record_type, result in results.items():
        if isinstance(result, DNSError):
            print(f"{record_type} failed: {result.error}")
        else:
            print(f"{record_type}: {len(result.response.answers)} answers")


asyncio.run(main())
```

#### Python API Methods

- **`Client.resolvers`** (property): Get the list of resolver addresses being used by this client. Returns a list of strings (e.g., `["8.8.8.8:53", "1.1.1.1:53"]`).

- **`get_system_resolvers() -> list[str]`**: Get system DNS resolver IP addresses from OS configuration. Works on Unix, Windows, macOS, and Android. Returns a list of IP addresses without ports (e.g., `["8.8.8.8", "1.1.1.1"]`). Useful for inspecting what resolvers the OS is configured to use.

- **`Client.resolve(host, record_type=None) -> list[str]`**: Lookup a single hostname, returning only rdata strings. Defaults to `A` records. Returns a list of strings (e.g., `["93.184.216.34"]` for A records). Perfect for simple use cases where you just need the record data without the full DNS response structure.

- **`Client.resolve_full(host, record_type=None) -> DNSResult`**: Lookup a single hostname, returning the full DNS response. Defaults to `A` records. Returns a Pydantic `DNSResult` model with typed fields for easy access to headers, queries, answers, etc.

- **`Client.resolve_batch(hosts, record_type=None)`**: Simplified batch resolution that returns only the essential data. Takes an iterable of hostnames and streams back `(host, record_type, answers)` tuples where `answers` is a list of rdata strings (e.g., `["93.184.216.34"]` for A records, `["10 aspmx.l.google.com."]` for MX records). Automatically filters out errors and empty responses. Perfect for processing large lists of hosts efficiently.

- **`Client.resolve_batch_full(hosts, record_type=None, skip_empty=False, skip_errors=False)`**: Resolve many hosts in parallel with full DNS responses. Takes an iterable of hostnames and streams back `(host, result)` tuples as results complete. Each result is either a `DNSResult` or `DNSError` Pydantic model. Set `skip_empty=True` to filter out successful responses with no answers. Set `skip_errors=True` to filter out error responses.

- **`Client.resolve_multi(host, record_types) -> dict[str, list[str]]`**: Resolve multiple record types for a single hostname in parallel, returning only successful results with answers. Takes a list of record type strings (e.g., `["A", "AAAA", "MX"]`) and returns a dictionary mapping record types to lists of rdata strings. Only includes record types that resolved successfully and have answers.

- **`Client.resolve_multi_full(host, record_types) -> dict[str, DNSResultOrError]`**: Resolve multiple record types for a single hostname in parallel, returning full DNS responses. Takes a list of record type strings and returns a dictionary keyed by record type. Each value is either a `DNSResult` (success) or `DNSError` (failure) Pydantic model. Includes all record types, even those that failed or had no answers.

#### MockClient for Testing

`MockClient` provides a drop-in replacement for `Client` that returns fabricated DNS responses without making real network requests. It implements the same interface as `Client` and is useful for testing code that depends on DNS lookups.

```python
import pytest
from blastdns import MockClient, DNSResult


@pytest.fixture
def mock_client():
    """Create a mock client with pre-configured test data."""
    client = MockClient()
    client.mock_dns({
        "example.com": {
            "A": ["93.184.216.34"],
            "AAAA": ["2606:2800:220:1:248:1893:25c8:1946"],
            "MX": ["10 aspmx.l.google.com.", "20 alt1.aspmx.l.google.com."],
        },
        "cname.example.com": {
            "CNAME": ["example.com."]
        },
        "_NXDOMAIN": ["notfound.example.com"],  # hosts that return NXDOMAIN
    })
    return client


@pytest.mark.asyncio
async def test_my_function(mock_client):
    # resolve() returns simple rdata strings
    answers = await mock_client.resolve("example.com", "A")
    assert answers == ["93.184.216.34"]

    # resolve_full() returns full DNS response structure
    result = await mock_client.resolve_full("example.com", "A")
    assert isinstance(result, DNSResult)
    assert len(result.response.answers) == 1

    # NXDOMAIN hosts return empty responses (not errors)
    answers = await mock_client.resolve("notfound.example.com", "A")
    assert len(answers) == 0

    # resolve_batch() works with all mocked hosts
    async for host, rdtype, answers in mock_client.resolve_batch(["example.com"], "A"):
        print(f"{host}: {answers}")  # ["93.184.216.34"]

    # resolve_multi() resolves multiple record types in parallel
    results = await mock_client.resolve_multi("example.com", ["A", "AAAA", "MX"])
    assert len(results) == 3
    assert results["MX"] == ["10 aspmx.l.google.com.", "20 alt1.aspmx.l.google.com."]
```

**Key Features:**
- Supports all `Client` methods: `resolve`, `resolve_full`, `resolve_batch`, `resolve_batch_full`, `resolve_multi`, `resolve_multi_full`
- Returns the same data structures as `Client` for drop-in compatibility
- NXDOMAIN hosts (specified in `_NXDOMAIN` list) return empty responses, not errors
- Unmocked hosts also return empty responses
- Auto-formats PTR queries (IP addresses → reverse DNS format) just like the real client

#### Response Models

The `*_full()` methods return Pydantic V2 models for type safety and IDE autocomplete:

- **`DNSResult`**: Successful DNS response with `host` and `response` fields
- **`DNSError`**: Failed DNS lookup with an `error` field
- **`Response`**: DNS message with `header`, `queries`, `answers`, `name_servers`, etc.

The base methods (`resolve`, `resolve_batch`, `resolve_multi`) return simple Python types (lists, dicts, strings) for convenience when you don't need the full response structure.

`ClientConfig` exposes the knobs shown above (`threads_per_resolver`, `request_timeout_ms`, `max_retries`, `purgatory_threshold`, `purgatory_sentence_ms`) and validates them before handing them to the Rust core.

## Architecture

BlastDNS is built on top of [`hickory-dns`](https://github.com/hickory-dns/hickory-dns), but only makes use of the low-level Client API, not the Resolver API.

Beneath the hood of the `BlastDNSClient`, each resolver gets its own `ResolverWorker` tasks, with a configurable number of workers per resolver (default: 2, configurable via `BlastDNSConfig.threads_per_resolver`).

When a user calls `BlastDNSClient::resolve`, a new `WorkItem` is created which contains the request (host + rdtype) and a oneshot channel to hold the result. This `WorkItem` is put into a [crossfire](https://github.com/frostyplanet/crossfire-rs) MPMC queue, to be picked up by the first available `ResolverWorker`. Workers are spawned lazily when the first request is made.

### Caching

BlastDNS includes an optional TTL-aware cache using an LRU eviction policy. The cache is enabled by default with a capacity of 10,000 entries and can be configured or disabled entirely:

- Only **positive responses with answers** are cached (no errors, NXDOMAIN, or empty responses)
- Cache entries automatically expire based on DNS record TTLs (clamped to configurable min/max bounds)
- Expired entries are removed when accessed; unaccessed expired entries remain until evicted by LRU policy
- Cache has a hard capacity limit (prevents unbounded growth even with expired entries)
- Thread-safe with minimal lock contention

Configure via `BlastDNSConfig`:
- `cache_capacity`: Number of entries (default: 10000, set to 0 to disable)
- `cache_min_ttl`: Minimum TTL (default: 10 seconds)
- `cache_max_ttl`: Maximum TTL (default: 1 day)

### Retry Logic and Fault Tolerance

BlastDNS handles unreliable resolvers through a multi-layered retry system:

**Client-Level Retries**: When a query fails with a retryable error (network timeouts, connection failures), the client automatically retries up to `max_retries` times (default: 10). Each retry creates a fresh `WorkItem` and sends it back to the shared queue, where it can be picked up by **any available worker**—not necessarily the same resolver. This means retries naturally route around problematic resolvers.

**Purgatory System**: Each worker tracks consecutive errors. After hitting `purgatory_threshold` failures (default: 10), the worker enters "purgatory"—it sleeps for `purgatory_sentence` milliseconds (default: 1000ms) before resuming work. This temporarily sidelines struggling resolvers without removing them entirely, allowing the system to self-heal if resolver issues are transient.

**Non-Retryable Errors**: Configuration errors (invalid hostnames) and system errors (queue closed) fail immediately without retry, preventing wasted work on queries that can't succeed.

This architecture ensures maximum accuracy even with a mixed pool of reliable and unreliable DNS servers, as queries naturally migrate toward responsive resolvers while problematic ones throttle themselves.

## Testing

BlastDNS has two types of tests:

### Unit Tests (No DNS Server Required)

Unit tests use `MockBlastDNSClient` (Rust) or `MockClient` (Python) and run without any external dependencies:

```bash
# Rust unit tests
cargo test

# Python unit tests
uv run pytest
```

### Integration Tests (Require DNS Server)

Integration tests verify real DNS resolution against a local `dnsmasq` server running on `127.0.0.1:5353` and `[::1]:5353`.

Install `dnsmasq`:

```bash
sudo apt install dnsmasq
```

Start the test DNS server:

```bash
sudo ./scripts/start-test-dns.sh
```

Run integration tests:

```bash
# Rust integration tests (marked with #[ignore])
cargo test -- --ignored

# Python integration tests with real DNS
uv run pytest -k "not mock"
```

When done, stop the test DNS server:

```bash
./scripts/stop-test-dns.sh
```

## Linting

### Rust

```bash
# Run clippy for lints
cargo clippy --all-targets --all-features

# Run rustfmt for formatting
cargo fmt --all
```

### Python

```bash
# Run ruff for lints
uv run ruff check --fix

# Run ruff for formatting
uv run ruff format
```
