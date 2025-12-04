import orjson
from pydantic import BaseModel, Field

from . import _native  # type: ignore
from .models import DNSError, DNSResult, DNSResultOrError

__all__ = [
    "ClientConfig",
    "Client",
    "DNSResult",
    "DNSError",
    "DNSResultOrError",
]


class ClientConfig(BaseModel):
    threads_per_resolver: int = Field(default=2, ge=1)
    request_timeout_ms: int = Field(default=1000, ge=1)
    max_retries: int = Field(default=10, ge=0)
    purgatory_threshold: int = Field(default=10, ge=1)
    purgatory_sentence_ms: int = Field(default=1000, ge=0)


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

    async def resolve(self, host, record_type=None) -> DNSResult:
        """Resolve a hostname to DNS records.

        Args:
            host: Hostname to resolve
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"

        Returns:
            DNSResult: A Pydantic model containing the host and DNS response with
                      typed fields for header, queries, answers, etc.

        Example:
            result = await client.resolve("example.com", "A")
            print(result.host)
            for answer in result.response.answers:
                print(answer.rdata)
        """
        raw = await self._inner.resolve(host, record_type)
        response_data = orjson.loads(raw)
        return DNSResult.model_validate({"host": host, "response": response_data})

    async def resolve_multi(self, host, record_types) -> dict[str, DNSResultOrError]:
        """Resolve multiple record types for a single hostname in parallel.

        Args:
            host: Hostname to resolve
            record_types: List of record type strings (e.g. ["A", "AAAA", "MX"])

        Returns:
            dict[str, DNSResultOrError]: Dictionary mapping record type to result.
                                         Successful resolutions return DNSResult,
                                         failures return DNSError.

        Example:
            results = await client.resolve_multi("example.com", ["A", "AAAA", "MX"])
            a_result = results["A"]
            if isinstance(a_result, DNSResult):
                print(f"A records: {a_result.response.answers}")
            else:
                print(f"Error: {a_result.error}")
        """
        raw_dict = await self._inner.resolve_multi(host, record_types)
        result = {}
        for key, value in raw_dict.items():
            data = orjson.loads(value)
            if "error" in data:
                result[key] = DNSError.model_validate(data)
            else:
                result[key] = DNSResult.model_validate({"host": host, "response": data})
        return result

    async def resolve_batch(self, hosts, record_type=None, skip_empty=False, skip_errors=False):
        """Resolve multiple hostnames concurrently, yielding results as they complete.

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
            async for host, result in client.resolve_batch(["example.com", "google.com"], "A"):
                if isinstance(result, DNSError):
                    print(f"{host} failed: {result.error}")
                else:
                    print(f"{host}: {len(result.response.answers)} answers")
        """
        async for host, raw in self._inner.resolve_batch(hosts, record_type, skip_empty, skip_errors):
            data = orjson.loads(raw)
            if "error" in data:
                yield (host, DNSError.model_validate(data))
            else:
                yield (host, DNSResult.model_validate({"host": host, "response": data}))
