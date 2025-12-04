# BlastDNS

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-black.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Rust 2024](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/blastdns.svg?color=orange)](https://crates.io/crates/blastdns)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://img.shields.io/pypi/v/blastdns.svg?color=blue)](https://pypi.org/project/blastdns/)
[![Rust Tests](https://github.com/blacklanternsecurity/blastdns/actions/workflows/rust-tests.yml/badge.svg)](https://github.com/blacklanternsecurity/blastdns/actions/workflows/rust-tests.yml)
[![Python Tests](https://github.com/blacklanternsecurity/blastdns/actions/workflows/python-tests.yml/badge.svg)](https://github.com/blacklanternsecurity/blastdns/actions/workflows/python-tests.yml)

[BlastDNS](https://github.com/blacklanternsecurity/blastdns) is an ultra-fast DNS resolver written in Rust. Like [massdns](https://github.com/blechschmidt/massdns), it's designed to be faster the more resolvers you give it. It's both highly efficient and reliable, even if you have shoddy DNS servers. For details, see [Architecture](#architecture).

There are three ways to use it:

- [Rust CLI tool](#cli)
- [Rust library](#rust-api)
- [Python library](#python-api)

BlastDNS is the primary DNS library used by [BBOT](https://github.com/blacklanternsecurity/bbot).

## Benchmark

100K DNS lookups against local `dnsmasq`, with 100 workers:

| Library         | Language | Time    | QPS    | Success  | Failed | vs dnspython |
|-----------------|----------|---------|--------|----------|--------|--------------|
| massdns         | C        | 1.687s  | 71,898 | 100,000  | 0      | 28.87x       |
| blastdns-cli    | Rust     | 1.732s  | 64,942 | 100,000  | 0      | 26.07x       |
| blastdns-python | Python   | 3.903s  | 25,623 | 100,000  | 0      | 10.29x       |
| dnspython       | Python   | 40.149s | 2,491  | 100,000  | 0      | 1.00x        |

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
BlastDNS - Async DNS spray client

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

Or add to `Cargo.toml`:

```toml
[dependencies]
blastdns = "0.1"
```

#### Usage

```rust
use blastdns::{BlastDNSClient, BlastDNSConfig};
use futures::StreamExt;
use hickory_client::proto::rr::RecordType;
use std::time::Duration;

// read DNS resolvers from a file (one per line -> vector of strings)
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

// lookup a domain
let result = client.resolve("example.com", RecordType::A).await?;

// print the result as serde JSON
println!("{}", serde_json::to_string_pretty(&result).unwrap());

// resolve_batch: process many hosts in parallel with bounded concurrency
// streams results back as they complete
let wordlist = ["one.example", "two.example", "three.example"];
let mut stream = client.resolve_batch(
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

// resolve_batch_basic: simplified batch resolution with minimal output
// returns only (host, record_type, Vec<rdata>) - no full DNS response structures
// automatically filters out errors and empty responses
let wordlist = ["one.example", "two.example", "three.example"];
let mut stream = client.resolve_batch_basic(
    wordlist.into_iter().map(Ok::<_, std::convert::Infallible>),
    RecordType::A,
);
while let Some((host, record_type, answers)) = stream.next().await {
    println!("{} ({}):", host, record_type);
    for answer in answers {
        println!("  {}", answer);  // e.g., "93.184.216.34" for A records
    }
}

// resolve_multi: resolve multiple record types for a single host in parallel
let record_types = vec![RecordType::A, RecordType::AAAA, RecordType::MX];
let results = client.resolve_multi("example.com", record_types).await?;
for (record_type, result) in results {
    match result {
        Ok(response) => println!("{}: {} answers", record_type, response.answers().len()),
        Err(err) => eprintln!("{} failed: {err}", record_type),
    }
}
```

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
from blastdns import Client, ClientConfig, DNSResult, DNSError


async def main():
    resolvers = ["1.1.1.1:53"]
    client = Client(resolvers, ClientConfig(threads_per_resolver=4, request_timeout_ms=1500))

    # resolve: lookup a single host, returns a Pydantic model
    result = await client.resolve("example.com", "AAAA")
    print(f"Host: {result.host}")
    print(f"Response code: {result.response.header.response_code}")
    for answer in result.response.answers:
        print(f"  {answer.name_labels}: {answer.rdata}")

    # resolve_batch: process many hosts in parallel with bounded concurrency
    # streams results back as they complete
    hosts = ["one.example.com", "two.example.com", "three.example.com"]
    async for host, result in client.resolve_batch(hosts, "A"):
        if isinstance(result, DNSError):
            print(f"{host} failed: {result.error}")
        else:
            print(f"{host}: {len(result.response.answers)} answers")

    # resolve_batch_basic: simplified batch resolution with minimal output
    # returns only (host, record_type, list[rdata]) - no full DNS response structures
    # automatically filters out errors and empty responses
    hosts = ["example.com", "google.com", "github.com"]
    async for host, rdtype, answers in client.resolve_batch_basic(hosts, "A"):
        print(f"{host} ({rdtype}):")
        for answer in answers:
            print(f"  {answer}")  # e.g., "93.184.216.34" for A records

    # resolve_multi: resolve multiple record types for a single host in parallel
    record_types = ["A", "AAAA", "MX"]
    results = await client.resolve_multi("example.com", record_types)
    for record_type, result in results.items():
        if isinstance(result, DNSError):
            print(f"{record_type} failed: {result.error}")
        else:
            print(f"{record_type}: {len(result.response.answers)} answers")


asyncio.run(main())
```

#### Python API Methods

- **`Client.resolve(host, record_type=None) -> DNSResult`**: Lookup a single hostname. Defaults to `A` records. Returns a Pydantic `DNSResult` model with typed fields for easy access to the response data.

- **`Client.resolve_batch(hosts, record_type=None, skip_empty=False, skip_errors=False)`**: Resolve many hosts in parallel. Takes an iterable of hostnames and streams back `(host, result)` tuples as results complete. Each result is either a `DNSResult` or `DNSError` Pydantic model. Set `skip_empty=True` to filter out successful responses with no answers. Set `skip_errors=True` to filter out error responses. Useful for processing large lists of hosts.

- **`Client.resolve_batch_basic(hosts, record_type=None)`**: Simplified batch resolution that returns only the essential data. Takes an iterable of hostnames and streams back `(host, record_type, answers)` tuples where `answers` is a list of rdata strings (e.g., `["93.184.216.34"]` for A records, `["10 aspmx.l.google.com."]` for MX records). Automatically filters out errors and empty responses. Perfect for simple use cases where you just need the IP addresses or other record data without the full DNS response structure.

- **`Client.resolve_multi(host, record_types) -> dict[str, DNSResultOrError]`**: Resolve multiple record types for a single hostname in parallel. Takes a list of record type strings (e.g., `["A", "AAAA", "MX"]`) and returns a dictionary keyed by record type. Each value is either a `DNSResult` (success) or `DNSError` (failure) Pydantic model.

#### MockClient for Testing

`MockClient` provides a drop-in replacement for `Client` that returns fabricated DNS responses without making real network requests. This is useful for testing code that depends on DNS lookups.

```python
import pytest
from blastdns import MockClient, DNSResult, DNSError


@pytest.fixture
def mock_client():
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
        "_NXDOMAIN": ["notfound.example.com"],  # hosts that return NXDOMAIN errors
    })
    return client


@pytest.mark.asyncio
async def test_my_function(mock_client):
    # MockClient implements the same interface as Client
    result = await mock_client.resolve("example.com", "A")
    assert isinstance(result, DNSResult)
    assert len(result.response.answers) == 1

    # Test error cases
    result = await mock_client.resolve("notfound.example.com", "A")
    assert result.response.header.response_code == "NXDomain"

    # Works with all Client methods
    async for host, rdtype, answers in mock_client.resolve_batch_basic(["example.com"], "A"):
        print(f"{host}: {answers}")  # ["93.184.216.34"]
```

`MockClient` supports all the same methods as `Client` (`resolve`, `resolve_batch`, `resolve_batch_basic`, `resolve_multi`) and returns the same Pydantic models.

#### Response Models

All methods return Pydantic V2 models for type safety and IDE autocomplete:

- **`DNSResult`**: Successful DNS response with `host` and `response` fields
- **`DNSError`**: Failed DNS lookup with an `error` field
- **`Response`**: DNS message with `header`, `queries`, `answers`, `name_servers`, etc.

`ClientConfig` exposes the knobs shown above (`threads_per_resolver`, `request_timeout_ms`, `max_retries`, `purgatory_threshold`, `purgatory_sentence_ms`) and validates them before handing them to the Rust core.

## Architecture

BlastDNS is built on top of [`hickory-dns`](https://github.com/hickory-dns/hickory-dns), but only makes use of the low-level Client API, not the Resolver API.

Beneath the hood of the `BlastDNSClient`, each resolver gets its own `ResolverWorker` tasks, with a configurable number of workers per resolver (default: 2, configurable via `BlastDNSConfig.threads_per_resolver`).

When a user calls `BlastDNSClient::resolve`, a new `WorkItem` is created which contains the request (host + rdtype) and a oneshot channel to hold the result. This `WorkItem` is put into a [crossfire](https://github.com/frostyplanet/crossfire-rs) MPMC queue, to be picked up by the first available `ResolverWorker`. Workers are spawned lazily when the first request is made.

### Retry Logic and Fault Tolerance

BlastDNS handles unreliable resolvers through a multi-layered retry system:

**Client-Level Retries**: When a query fails with a retryable error (network timeouts, connection failures), the client automatically retries up to `max_retries` times (default: 10). Each retry creates a fresh `WorkItem` and sends it back to the shared queue, where it can be picked up by **any available worker**—not necessarily the same resolver. This means retries naturally route around problematic resolvers.

**Purgatory System**: Each worker tracks consecutive errors. After hitting `purgatory_threshold` failures (default: 10), the worker enters "purgatory"—it sleeps for `purgatory_sentence` milliseconds (default: 1000ms) before resuming work. This temporarily sidelines struggling resolvers without removing them entirely, allowing the system to self-heal if resolver issues are transient.

**Non-Retryable Errors**: Configuration errors (invalid hostnames) and system errors (queue closed) fail immediately without retry, preventing wasted work on queries that can't succeed.

This architecture ensures maximum accuracy even with a mixed pool of reliable and unreliable DNS servers, as queries naturally migrate toward responsive resolvers while problematic ones throttle themselves.

## Testing

To run the full test suite including integration tests, you'll need a local DNS server running on `127.0.0.1:5353` and `[::1]:5353`.

Install `dnsmasq`:

```bash
sudo apt install dnsmasq
```

Start the test DNS server:

```bash
sudo ./scripts/start-test-dns.sh
```

Then run tests with:

```bash
# rust tests
cargo test -- --ignored

# python tests
uv run pytest
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
