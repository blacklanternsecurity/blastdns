# BlastDNS

An async rust library for DNS lookups. Can be used to perform simple, one-off lookups or bulk lookups in parallel with many resolvers, similar to [`massdns`](https://github.com/blechschmidt/massdns).

## Features

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

### CLI

The CLI streams JSON records for each hostname in an input file, resolving them with the same worker pool used by the library:

```bash
$ blastdns hosts.txt --rdtype A --resolvers resolvers.txt
```

`hosts.txt` and the resolver file both accept one entry per line (comments via `#` are ignored).

Additional CLI options:
- `--threads-per-resolver N`: Number of worker tasks per resolver (default: 1)
- `--timeout-ms N`: Per-request timeout in milliseconds (default: 3000)
- `--purgatory-threshold N`: Consecutive worker errors before it rests (default: 10)
- `--purgatory-sentence-ms N`: How long a resting worker stays idle (default: 1000)

## Architecture

BlastDNS is built on top of [`hickory-dns`](https://github.com/hickory-dns/hickory-dns), but only makes use of the low-level Client API, not the Resolver API.

BlastDNS is designed to be faster the more resolvers you give it.

Beneath the hood of the `BlastDNSClient`, each resolver gets its own `ResolverWorker` tasks, with a configurable number of workers per resolver (default: 1, configurable via `BlastDNSConfig.threads_per_resolver`).

When a user calls `BlastDNSClient::resolve`, a new `WorkItem` is created which contains the request (host + rdtype) and a oneshot channel to hold the result. This `WorkItem` is put into a [crossfire](https://github.com/frostyplanet/crossfire-rs) MPMC queue, to be picked up by the first available `ResolverWorker`. Workers are spawned immediately during client instantiation.

### Work queue layout

We use crossfire's lockless `mpmc::bounded_async` channel so that every API call (producer) can push a `WorkItem` and every `ResolverWorker` (consumer) can pop whichever request is next without additional locking overhead. The bounded flavor gives us built-in backpressure when callers enqueue faster than workers can drain.

1. Size the queue based on how many lookups you want to have in-flight (a good default is `resolvers.len() * threads_per_resolver`).
2. Build the queue once in `BlastDNSClient::with_config` and hand a cloned `MAsyncRx` handle to each worker when spawned.
3. On `resolve`, send the `WorkItem` through the channel and await the result via the oneshot receiver.
4. Inside each worker task, loop on `work_rx.recv().await`, fire the query against its resolver, and fulfill the responder stored in the `WorkItem`.

```rust
use crossfire::mpmc;

// Internal implementation (not public API)
let queue_depth = resolvers.len() * threads_per_resolver;
let (work_tx, work_rx) = mpmc::bounded_async::<WorkItem>(queue_depth);

for resolver in resolvers.iter() {
    for worker_idx in 0..threads_per_resolver {
        ResolverWorker::spawn(*resolver, work_rx.clone(), config.clone(), worker_idx);
    }
}

// later inside BlastDNSClient::resolve
let (tx, rx) = oneshot::channel();
let work_item = WorkItem::new(query, tx);
work_tx.send(work_item).await?;
rx.await?
```

Because both endpoints are async, everything runs on the same runtime as the client, and workers can still be simple blocking tasks by calling `recv()` from a blocking context with the `From` conversion to `MRx` if we ever need to bridge runtimes.

### Hickory client wiring

Each `ResolverWorker` wraps a dedicated `hickory_client::client::Client` instance, which Hickory documents as a handle to an unbounded request queue backed by a background task (`bg`) that multiplexes I/O to the resolver socket. That background future **must** be spawned before we issue queries, otherwise the queue never drains.

1. Create a UDP stream using `UdpClientStream::builder` and configure the timeout.
2. Call `Client::connect(stream)` to obtain `(client, bg)`.
3. Spawn `bg` immediately on the worker runtime so Hickory can pump the socket.
4. Hold onto the `Client` and service each `WorkItem` via `client.query(name, DNSClass::IN, record_type).await`.

```rust
use hickory_client::client::{Client, ClientHandle};
use hickory_client::proto::{
    runtime::TokioRuntimeProvider,
    rr::{DNSClass, Name, RecordType},
    udp::UdpClientStream,
    xfer::DnsResponse,
};

async fn init_hickory_client(resolver: SocketAddr, timeout: Duration) -> Result<Client, BlastDNSError> {
    let provider = TokioRuntimeProvider::new();
    let stream = UdpClientStream::builder(resolver, provider)
        .with_timeout(Some(timeout))
        .build();

    let (client, bg) = Client::connect(stream).await?;
    tokio::spawn(bg); // runs the multiplexed exchange loop
    Ok(client)
}

async fn resolve_once(
    client: &mut Client,
    host: &str,
    record_type: RecordType,
) -> Result<DnsResponse, BlastDNSError> {
    let name = Name::from_ascii(host)?;
    Ok(client.query(name, DNSClass::IN, record_type).await?)
}
```

Because the I/O path already multiplexes requests internally, we can share a single client per resolver worker and still issue multiple inflight queries—Hickory's internal exchange mechanism coalesces them and the background future handles I/O.

### Future additions (not to be implemented yet)

Later we will implement error handling, retries, caching, and penalties/timeouts for badly behaving resolvers. But for now, just a simple client with parallel resolution.

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
