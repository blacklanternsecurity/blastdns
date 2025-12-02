# BlastDNS

An async rust library for DNS lookups. Can be used to perform simple, one-off lookups or bulk lookups in parallel with many resolvers, similar to [`massdns`](https://github.com/blechschmidt/massdns).

## Features

BlastDNS is simultaneously a:

- [Rust CLI tool](#cli)
- [Rust library](#rust-api)
- [Python library](#python-api)

## Benchmark

20K DNS lookups against local `dnsmasq`, with 100 workers:

| Library         | Language    | Time   | QPS    | Success Rate | vs dnspython   |
|-----------------|-------------|--------|--------|--------------|----------------|
| massdns         | C           | 0.308s | 65,019 | 100%         | 31.52x         |
| blastdns-cli    | Rust        | 0.336s | 59,548 | 100%         | 28.86x         |
| blastdns-python | Python+Rust | 1.564s | 12,791 | 100%         | 6.20x          |
| dnspython       | Python      | 9.695s | 2,063  | 100%         | 1.00x          |

### CLI

The CLI mass-resolves hosts using a specified list of resolvers. It outputs to JSON.

```bash
# send all results to jq
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt | jq

# print only the raw IPv4 addresses
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt | jq '.response.answers[].rdata.A'

# load from stdin
$ cat hosts.txt | blastdns --rdtype A --resolvers resolvers.txt
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

// bulk lookups stream back as soon as each resolver answers
let wordlist = ["one.example", "two.example", "three.example"];
let mut stream = client.resolve_batch(wordlist, RecordType::A);
while let Some((host, outcome)) = stream.next().await {
    match outcome {
        Ok(response) => println!("{}: {} answers", host, response.answers().len()),
        Err(err) => eprintln!("{} failed: {err}", host),
    }
}
```

### Python API

The `blastdns` Python package is a thin wrapper around the Rust library.

```bash
# install python dependencies
uv sync
# build and install the rust->python bindings
uv run maturin develop
# run tests
uv run pytest
```

To use it in Python, you can use the `Client` class:

```python
import json
import asyncio
from blastdns import Client, ClientConfig


async def main():
    resolvers = ["1.1.1.1:53"]
    client = Client(resolvers, ClientConfig(threads_per_resolver=4, request_timeout_ms=1500))

    response = await client.resolve("example.com", "AAAA")
    print(json.dumps(response, indent=2))


asyncio.run(main())
```

`Client.resolve(host, record_type=None)` defaults to `A` records and returns the same JSON-shaped dictionaries the CLI prints, so you can reuse downstream tooling. `ClientConfig` exposes the knobs shown above (`threads_per_resolver`, `request_timeout_ms`, `max_retries`, `purgatory_threshold`, `purgatory_sentence_ms`) and validates them before handing them to the Rust core.

## Architecture

BlastDNS is built on top of [`hickory-dns`](https://github.com/hickory-dns/hickory-dns), but only makes use of the low-level Client API, not the Resolver API.

BlastDNS is designed to be faster the more resolvers you give it.

Beneath the hood of the `BlastDNSClient`, each resolver gets its own `ResolverWorker` tasks, with a configurable number of workers per resolver (default: 2, configurable via `BlastDNSConfig.threads_per_resolver`).

When a user calls `BlastDNSClient::resolve`, a new `WorkItem` is created which contains the request (host + rdtype) and a oneshot channel to hold the result. This `WorkItem` is put into a [crossfire](https://github.com/frostyplanet/crossfire-rs) MPMC queue, to be picked up by the first available `ResolverWorker`. Workers are spawned immediately during client instantiation.

## Testing

To run the full test suite including integration tests, you'll need a local DNS server running on `127.0.0.1:5353` and `[::1]:5353`.

Install `dnsmasq`:

```bash
sudo apt install dnsmasq
```

Start the test DNS server:

```bash
./scripts/start-test-dns.sh
```

Then run tests with:

```bash
cargo test -- --ignored
```

When done, stop the test DNS server:

```bash
./scripts/stop-test-dns.sh
```

## Linting

Run clippy for lints:

```bash
cargo clippy --all-targets --all-features
```

Run rustfmt for formatting:

```bash
cargo fmt --all
```
