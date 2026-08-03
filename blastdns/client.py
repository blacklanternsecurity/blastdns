from typing import Optional

import orjson
from pydantic import BaseModel, ConfigDict, Field

from . import _native  # type: ignore
from .models import DNSError, DNSResult, DNSResultOrError, ResolverStats

__all__ = [
    "ClientConfig",
    "Client",
    "MockClient",
    "get_system_resolvers",
    "init_logging",
]


def init_logging(filter: Optional[str] = None) -> bool:
    """Send blastdns's internal logs to stderr.

    Without this, nothing the engine logs is visible from Python: resolver
    diagnostics, purgatory sentences, TCP refetches on truncation, and adaptive
    rate changes are all discarded. Call it once, early.

    Args:
        filter: A ``RUST_LOG``-style directive, e.g. ``"blastdns=debug"``.
                Defaults to reading ``RUST_LOG`` from the environment.

    Returns:
        bool: False if a log subscriber is already installed, in which case that
              one stays in charge. Not an error.

    Example:
        blastdns.init_logging("blastdns=debug")
    """
    return _native.init_logging(filter)


def get_system_resolvers() -> list[str]:
    """Get system DNS resolver IP addresses from OS configuration.

    Works on Unix, Windows, macOS, and Android.

    Returns:
        list[str]: List of system resolver IP addresses (e.g., ["8.8.8.8", "1.1.1.1"])

    Example:
        resolvers = get_system_resolvers()
        for ip in resolvers:
            print(f"System resolver: {ip}")
    """
    return _native.get_system_resolvers_py()


class ClientConfig(BaseModel):
    """Configuration for a :class:`Client`.

    Throughput is governed by three independent limits: ``max_concurrency`` caps
    total queries in flight, ``max_inflight_per_resolver`` caps how many any one
    resolver receives at once (the politeness bound), and ``rate_limit`` caps the
    dispatch rate outright. The tightest one binds.

    With ``adaptive`` on (the default), pacing also backs off on its own: when a
    resolver starts losing queries, its rate retreats below the rate at which that
    began. ``rate_limit`` is an additional hard cap, not a replacement for it, so
    adaptation happens whether or not one is set.

    Unknown keys are rejected rather than ignored, so a stale or misspelled
    option fails loudly instead of silently falling back to a default.
    """

    model_config = ConfigDict(extra="forbid")

    max_inflight_per_resolver: int = Field(default=2, ge=1)
    max_concurrency: int = Field(default=256, ge=1)
    rate_limit: Optional[float] = Field(default=None, gt=0)
    adaptive: bool = Field(default=True)
    resolver_probe: bool = Field(default=False)
    persistent_socket: bool = Field(default=False)
    request_timeout_ms: int = Field(default=1000, ge=1)
    max_retries: int = Field(default=10, ge=0)
    purgatory_threshold: int = Field(default=10, ge=1)
    purgatory_sentence_ms: int = Field(default=1000, ge=0)
    cache_capacity: int = Field(default=10000, ge=0)
    cache_min_ttl_secs: int = Field(default=10, ge=0)
    cache_max_ttl_secs: int = Field(default=86400, ge=0)


class Client:
    """Async DNS client backed by the Rust BlastDNS engine.

    This is a thin, ergonomic wrapper around the native Rust client. It accepts a
    list of DNS resolvers and an optional `ClientConfig`, and exposes a single
    async `resolve` method that returns JSON-shaped Python dictionaries matching
    the CLI output shown in the README.
    """

    def __init__(self, resolvers, config=None):
        if _native is None:
            raise RuntimeError(
                "blastdns native module is unavailable. "
                "Build it via `maturin develop --features python` "
                "or `cargo build --features python` before using Client."
            )
        config_json = (config or ClientConfig()).model_dump_json()
        self._inner = _native.Client(list(resolvers), config_json)

    @property
    def resolvers(self) -> list[str]:
        """Get the list of resolvers being used by this client.

        Returns:
            list[str]: List of resolver addresses (e.g., ["8.8.8.8:53", "1.1.1.1:53"])

        Example:
            client = Client(["8.8.8.8"])
            print(client.resolvers)  # ["8.8.8.8:53"]
        """
        return self._inner.resolvers

    def stats(self) -> list[ResolverStats]:
        """Cumulative per-resolver counters, one entry per configured resolver.

        Every dispatched query lands in exactly one of ``answered``, ``empty``,
        ``timeout``, or ``error``, so diffing two snapshots accounts for a batch
        in full and reveals queries that never came back.

        Example:
            before = {s.resolver: s for s in client.stats()}
            async for host, rdtype, answers in client.resolve_batch(hosts, "A"):
                ...
            after = client.stats()
        """
        return [ResolverStats.model_validate(s) for s in orjson.loads(self._inner.stats())]

    async def resolve(self, host, record_type=None) -> list[str]:
        """Resolve a hostname to DNS records, returning simplified rdata strings.

        Args:
            host: Hostname to resolve (IP addresses are auto-formatted for PTR queries)
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"

        Returns:
            list[str]: List of rdata strings (e.g., ["93.184.216.34"] for A records).

        Example:
            ips = await client.resolve("example.com", "A")
            for ip in ips:
                print(ip)
        """
        return await self._inner.resolve(host, record_type)

    async def resolve_full(self, host, record_type=None) -> DNSResult:
        """Resolve a hostname to DNS records, returning full DNS response.

        Args:
            host: Hostname to resolve (IP addresses are auto-formatted for PTR queries)
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"

        Returns:
            DNSResult: A Pydantic model containing the host and DNS response with
                      typed fields for header, queries, answers, etc.

        Example:
            result = await client.resolve_full("example.com", "A")
            print(result.host)
            for answer in result.response.answers:
                print(answer.rdata)
        """
        raw = await self._inner.resolve_full(host, record_type)
        response_data = orjson.loads(raw)
        return DNSResult.model_validate({"host": host, "response": response_data})

    async def resolve_multi(self, host, record_types) -> dict[str, list[str]]:
        """Resolve multiple record types for a single hostname in parallel, returning simplified results.

        Args:
            host: Hostname to resolve
            record_types: List of record type strings (e.g. ["A", "AAAA", "MX"])

        Returns:
            dict[str, list[str]]: Dictionary mapping record type to list of rdata strings.
                                  Only successful queries with answers are included.

        Example:
            results = await client.resolve_multi("example.com", ["A", "AAAA", "MX"])
            if "A" in results:
                for ip in results["A"]:
                    print(ip)
        """
        return await self._inner.resolve_multi(host, record_types)

    async def resolve_multi_full(self, host, record_types) -> dict[str, DNSResultOrError]:
        """Resolve multiple record types for a single hostname in parallel, returning full results.

        Args:
            host: Hostname to resolve
            record_types: List of record type strings (e.g. ["A", "AAAA", "MX"])

        Returns:
            dict[str, DNSResultOrError]: Dictionary mapping record type to result.
                                         Successful resolutions return DNSResult,
                                         failures return DNSError.

        Example:
            results = await client.resolve_multi_full("example.com", ["A", "AAAA", "MX"])
            a_result = results["A"]
            if isinstance(a_result, DNSResult):
                print(f"A records: {a_result.response.answers}")
            else:
                print(f"Error: {a_result.error}")
        """
        raw_dict = await self._inner.resolve_multi_full(host, record_types)
        result = {}
        for key, value in raw_dict.items():
            data = orjson.loads(value)
            if "error" in data:
                result[key] = DNSError.model_validate(data)
            else:
                result[key] = DNSResult.model_validate({"host": host, "response": data})
        return result

    async def resolve_batch(self, hosts, record_type=None):
        """Resolve multiple hostnames concurrently, yielding simplified tuples.

        Args:
            hosts: Iterable of hostname strings
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"

        Yields:
            tuple[str, str, list[str]]: (hostname, record_type, rdata) tuples.
                                        Only successful, non-empty results are returned.
                                        Results are unordered (faster hosts first).

        Example:
            async for host, rdtype, answers in client.resolve_batch(["example.com", "google.com"], "A"):
                print(f"{host} ({rdtype}): {', '.join(answers)}")
        """
        async for batch in self._inner.resolve_batch(hosts, record_type):
            for host, rdtype, answers in batch:
                yield (host, rdtype, answers)

    async def resolve_batch_full(self, hosts, record_type=None, skip_empty=False, skip_errors=False):
        """Resolve multiple hostnames concurrently, yielding full results as they complete.

        Args:
            hosts: Iterable of hostname strings
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"
            skip_empty: Skip empty responses (default: False)
            skip_errors: Skip error responses (default: False)

        Yields:
            tuple[str, DNSResultOrError]: (hostname, result) pairs. Successful resolutions
                                          return DNSResult, failures return DNSError.
                                          Results are unordered (faster hosts first).

        Example:
            async for host, result in client.resolve_batch_full(["example.com", "google.com"], "A"):
                if isinstance(result, DNSError):
                    print(f"{host} failed: {result.error}")
                else:
                    print(f"{host}: {len(result.response.answers)} answers")
        """
        async for batch in self._inner.resolve_batch_full(hosts, record_type, skip_empty, skip_errors):
            for host, raw in batch:
                data = orjson.loads(raw)
                if "error" in data:
                    yield (host, DNSError.model_validate(data))
                else:
                    yield (host, DNSResult.model_validate({"host": host, "response": data}))


class MockClient(Client):
    """Mock DNS client for testing purposes.

    This client inherits from Client but uses a mocked Rust backend that returns
    fabricated responses based on pre-configured mock data. Use `mock_dns()` to
    configure the responses.
    """

    def __init__(self, resolvers=None, config=None):
        """Initialize mock client (resolvers and config are ignored)."""
        if _native is None:
            raise RuntimeError(
                "blastdns native module is unavailable. "
                "Build it via `maturin develop --features python` "
                "or `cargo build --features python` before using MockClient."
            )
        # Skip parent __init__, directly set _inner to mock client
        self._inner = _native.MockClient()

    @property
    def resolvers(self) -> list[str]:
        """Mock client has no real resolvers."""
        return ["mock:53"]

    def stats(self) -> list[ResolverStats]:
        """Mock client dispatches to no real resolvers, so there are no counters."""
        return []

    def mock_dns(self, data):
        """Configure mock DNS responses.

        Args:
            data: Dictionary mapping hosts to their DNS records, with optional
                  "_NXDOMAIN" key for hosts that should return NXDOMAIN.

        Example:
            mock_client.mock_dns({
                "example.com": {"A": ["93.184.216.34"], "AAAA": ["2606:2800:220:1:248:1893:25c8:1946"]},
                "bad.dns": {"CNAME": ["baddns.azurewebsites.net."]},
                "_NXDOMAIN": ["notfound.example.com", "baddns.azurewebsites.net"]
            })
        """
        self._inner.mock_dns(data)
