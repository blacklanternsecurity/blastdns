# BlastDNS

An async rust library for DNS lookups. Can be used to perform simple, one-off lookups or bulk lookups in parallel with many resolvers, similar to [`massdns`](https://github.com/blechschmidt/massdns).

## Features

BlastDNS is a:
- [CLI tool](#cli)
- [Rust library](#rust-api)

### CLI

The CLI streams JSON records for each hostname in an input file, resolving them with the same worker pool used by the library:

```bash
# send all results to jq
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt | jq

# print only the raw IPv4 addresses
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt | jq '.response.answers[].rdata.A'
```

#### CLI Help

```
$ blastdns --help
BlastDNS - Async DNS spray client

Usage: blastdns [OPTIONS] --resolvers <FILE> <HOSTS_TO_RESOLVE>

Arguments:
  <HOSTS_TO_RESOLVE>  File containing hostnames to resolve (one per line)

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
          Consecutive errors before a worker is put into timeout [default: 5]
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

## Architecture

BlastDNS is built on top of [`hickory-dns`](https://github.com/hickory-dns/hickory-dns), but only makes use of the low-level Client API, not the Resolver API.

BlastDNS is designed to be faster the more resolvers you give it.

Beneath the hood of the `BlastDNSClient`, each resolver gets its own `ResolverWorker` tasks, with a configurable number of workers per resolver (default: 2, configurable via `BlastDNSConfig.threads_per_resolver`).

When a user calls `BlastDNSClient::resolve`, a new `WorkItem` is created which contains the request (host + rdtype) and a oneshot channel to hold the result. This `WorkItem` is put into a [crossfire](https://github.com/frostyplanet/crossfire-rs) MPMC queue, to be picked up by the first available `ResolverWorker`. Workers are spawned immediately during client instantiation.

## Testing

To run the full test suite including integration tests, you'll need a local DNS server running on `127.0.0.1:5353` and `[::1]:5353`.

Install `dnsmasq`:

```bash
# Arch Linux
sudo pacman -S dnsmasq

# Debian/Ubuntu
sudo apt install dnsmasq

# macOS
brew install dnsmasq
```

Start a simple DNS server using `dnsmasq`:

```bash
dnsmasq --no-daemon --no-hosts --no-resolv --port=5353 --server=1.1.1.1
```

Then run tests with:

```bash
cargo test -- --ignored
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
