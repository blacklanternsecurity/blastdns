import orjson
from pydantic import BaseModel, Field

from . import _native  # type: ignore

__all__ = ["ClientConfig", "Client"]


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

    async def resolve(self, host, record_type=None):
        """Resolve a hostname to DNS records.

        `record_type` is a string such as `"A"`, `"AAAA"`, `"MX"`, etc. If
        omitted or `None`, it defaults to `"A"`. The return value is a dict in
        the same shape as the CLI JSON output. For example:

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
        """
        raw = await self._inner.resolve(host, record_type)
        return orjson.loads(raw)

    async def resolve_multi(self, host, record_types):
        """Resolve multiple record types for a single hostname in parallel.

        `host` is a hostname string. `record_types` is a list of record type strings
        such as `["A", "AAAA", "MX"]`. At least one record type is required.

        Returns a dict mapping each record type string to its result. For successful
        resolutions, the value is a dict matching the format from `resolve()`. For
        failures, the value is `{"error": "error message"}`.

        Example:
            results = await client.resolve_multi("example.com", ["A", "AAAA", "MX"])
            print(results["A"])  # A record result
            print(results["AAAA"])  # AAAA record result
            print(results["MX"])  # MX record result
        """
        raw_dict = await self._inner.resolve_multi(host, record_types)
        return {key: orjson.loads(value) for key, value in raw_dict.items()}

    async def resolve_batch(self, hosts, record_type=None, skip_empty=False, skip_errors=False):
        """Resolve multiple hostnames concurrently, yielding results as they complete.

        `hosts` is an iterable of hostname strings. `record_type` is a string such
        as `"A"`, `"AAAA"`, `"MX"`, etc. If omitted or `None`, it defaults to `"A"`.
        `skip_empty` is a boolean that, when True, filters out successful responses
        with no answers (errors still pass through).
        `skip_errors` is a boolean that, when True, filters out error responses
        (successful responses still pass through).

        This method is an async generator that yields `(host, result)` tuples as
        resolutions complete. Results are unordered (faster hosts complete first).

        For successful resolutions, `result` is a dict matching the format from
        `resolve()`. For failures, `result` is `{"error": "error message"}`.

        Example:
            async for host, result in client.resolve_batch(["example.com", "google.com"], "A"):
                if "error" in result:
                    print(f"{host} failed: {result['error']}")
                else:
                    print(f"{host} resolved: {result}")
        """
        async for host, raw in self._inner.resolve_batch(hosts, record_type, skip_empty, skip_errors):
            yield (host, orjson.loads(raw))
