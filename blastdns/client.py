import orjson
from pydantic import BaseModel, Field

from . import _native  # type: ignore
from .models import DNSError, DNSResult, DNSResultOrError

__all__ = [
    "ClientConfig",
    "Client",
    "MockClient",
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
        async for host, rdtype, answers in self._inner.resolve_batch(hosts, record_type):
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
        async for host, raw in self._inner.resolve_batch_full(hosts, record_type, skip_empty, skip_errors):
            data = orjson.loads(raw)
            if "error" in data:
                yield (host, DNSError.model_validate(data))
            else:
                yield (host, DNSResult.model_validate({"host": host, "response": data}))


class MockClient:
    """Mock DNS client for testing purposes.

    This client mimics the interface of the real Client but returns fabricated
    responses based on pre-configured mock data. Use `mock_dns()` to configure
    the responses.
    """

    def __init__(self, resolvers=None, config=None):
        """Initialize mock client (resolvers and config are ignored)."""
        self._mock_data = {}
        self._nxdomain_hosts = set()

    @staticmethod
    def _format_ptr_query(host: str) -> str:
        """Format an IP address for PTR lookup."""
        import ipaddress

        try:
            ip = ipaddress.ip_address(host)
            if isinstance(ip, ipaddress.IPv4Address):
                octets = host.split(".")
                return f"{octets[3]}.{octets[2]}.{octets[1]}.{octets[0]}.in-addr.arpa"
            elif isinstance(ip, ipaddress.IPv6Address):
                # Expand to full form and reverse nibbles
                expanded = ip.exploded.replace(":", "")
                nibbles = list(reversed(expanded))
                return ".".join(nibbles) + ".ip6.arpa"
        except ValueError:
            # Not an IP address, return as-is
            pass

        return host

    def mock_dns(self, data):
        """Configure mock DNS responses.

        Args:
            data: Dictionary mapping hosts to their DNS records, with optional
                  "_NXDOMAIN" key for hosts that should return NXDOMAIN errors.

        Example:
            mock_client.mock_dns({
                "example.com": {"A": ["93.184.216.34"], "AAAA": ["2606:2800:220:1:248:1893:25c8:1946"]},
                "bad.dns": {"CNAME": ["baddns.azurewebsites.net."]},
                "_NXDOMAIN": ["baddns.azurewebsites.net", "notfound.example.com"]
            })
        """
        for key, value in data.items():
            if key == "_NXDOMAIN":
                self._nxdomain_hosts.update(value)
            else:
                self._mock_data[key] = value

    def _fabricate_response(self, host, record_type, answers_data):
        """Fabricate a complete DNS response structure."""
        from .models import Response, Header, Query, Record

        # Ensure host has trailing dot (FQDN format)
        fqdn = host if host.endswith(".") else f"{host}."

        # Create answer records
        answers = []
        for rdata_str in answers_data:
            # Parse the rdata based on record type
            if record_type == "A":
                rdata = {"A": rdata_str}
            elif record_type == "AAAA":
                rdata = {"AAAA": rdata_str}
            elif record_type == "CNAME":
                rdata = {"CNAME": rdata_str}
            elif record_type == "MX":
                # MX records like "10 aspmx.l.google.com."
                parts = rdata_str.split(None, 1)
                if len(parts) == 2:
                    rdata = {"MX": {"preference": int(parts[0]), "exchange": parts[1]}}
                else:
                    rdata = {"MX": {"preference": 0, "exchange": rdata_str}}
            elif record_type == "TXT":
                rdata = {"TXT": rdata_str}
            elif record_type == "NS":
                rdata = {"NS": rdata_str}
            elif record_type == "PTR":
                rdata = {"PTR": rdata_str}
            elif record_type == "SOA":
                rdata = {"SOA": rdata_str}
            elif record_type == "SRV":
                rdata = {"SRV": rdata_str}
            else:
                rdata = {record_type: rdata_str}

            answers.append(Record(name_labels=fqdn, ttl=300, dns_class="IN", rdata=rdata))

        # Fabricate header
        header = Header(
            id=12345,
            message_type="Response",
            op_code="Query",
            authoritative=False,
            truncation=False,
            recursion_desired=True,
            recursion_available=True,
            authentic_data=False,
            checking_disabled=False,
            response_code="NoError",
            query_count=1,
            answer_count=len(answers),
            name_server_count=0,
            additional_count=0,
        )

        # Fabricate query
        queries = [Query(name=fqdn, query_type=record_type, query_class="IN")]

        return Response(
            header=header, queries=queries, answers=answers, name_servers=[], additionals=[], signature=[], edns=None
        )

    def _fabricate_nxdomain_response(self, host, record_type):
        """Fabricate an NXDOMAIN error response."""
        from .models import Response, Header, Query

        # Ensure host has trailing dot (FQDN format)
        fqdn = host if host.endswith(".") else f"{host}."

        header = Header(
            id=12345,
            message_type="Response",
            op_code="Query",
            authoritative=False,
            truncation=False,
            recursion_desired=True,
            recursion_available=True,
            authentic_data=False,
            checking_disabled=False,
            response_code="NXDomain",
            query_count=1,
            answer_count=0,
            name_server_count=0,
            additional_count=0,
        )

        queries = [Query(name=fqdn, query_type=record_type, query_class="IN")]

        return Response(
            header=header, queries=queries, answers=[], name_servers=[], additionals=[], signature=[], edns=None
        )

    async def resolve(self, host, record_type=None) -> list[str]:
        """Resolve a hostname to DNS records (mocked), returning simplified rdata strings.

        Args:
            host: Hostname to resolve
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"

        Returns:
            list[str]: List of rdata strings. Empty list for NXDOMAIN or no data.
        """
        record_type = record_type or "A"

        # Auto-format PTR queries if an IP address is provided
        if record_type == "PTR":
            host = self._format_ptr_query(host)

        # Check if this host should return NXDOMAIN
        if host in self._nxdomain_hosts:
            return []

        # Check if we have mock data for this host
        if host in self._mock_data and record_type in self._mock_data[host]:
            return self._mock_data[host][record_type]

        # No mock data, return empty list
        return []

    async def resolve_full(self, host, record_type=None) -> DNSResult:
        """Resolve a hostname to DNS records (mocked), returning full DNSResult.

        Args:
            host: Hostname to resolve
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"

        Returns:
            DNSResult: A Pydantic model containing the host and fabricated DNS response.
        """
        record_type = record_type or "A"

        # Auto-format PTR queries if an IP address is provided
        if record_type == "PTR":
            host = self._format_ptr_query(host)

        # Check if this host should return NXDOMAIN
        if host in self._nxdomain_hosts:
            response = self._fabricate_nxdomain_response(host, record_type)
            return DNSResult(host=host, response=response)

        # Check if we have mock data for this host
        if host in self._mock_data and record_type in self._mock_data[host]:
            answers_data = self._mock_data[host][record_type]
            response = self._fabricate_response(host, record_type, answers_data)
            return DNSResult(host=host, response=response)

        # No mock data, return empty response
        response = self._fabricate_response(host, record_type, [])
        return DNSResult(host=host, response=response)

    async def resolve_multi(self, host, record_types) -> dict[str, list[str]]:
        """Resolve multiple record types for a single hostname in parallel (mocked), returning simplified results.

        Args:
            host: Hostname to resolve
            record_types: List of record type strings (e.g. ["A", "AAAA", "MX"])

        Returns:
            dict[str, list[str]]: Dictionary mapping record type to list of rdata strings.
                                  Only successful queries with answers are included.
        """
        result = {}
        for record_type in record_types:
            query_host = host

            # Auto-format PTR queries if an IP address is provided
            if record_type == "PTR":
                query_host = self._format_ptr_query(host)

            # Skip NXDOMAIN hosts
            if query_host in self._nxdomain_hosts:
                continue

            # Check if we have mock data for this host/type
            if query_host in self._mock_data and record_type in self._mock_data[query_host]:
                answers_data = self._mock_data[query_host][record_type]
                if answers_data:  # Only include if there are answers
                    result[record_type] = answers_data

        return result

    async def resolve_multi_full(self, host, record_types) -> dict[str, DNSResultOrError]:
        """Resolve multiple record types for a single hostname in parallel (mocked), returning full results.

        Args:
            host: Hostname to resolve
            record_types: List of record type strings (e.g. ["A", "AAAA", "MX"])

        Returns:
            dict[str, DNSResultOrError]: Dictionary mapping record type to result.
        """
        result = {}
        for record_type in record_types:
            query_host = host

            # Auto-format PTR queries if an IP address is provided
            if record_type == "PTR":
                query_host = self._format_ptr_query(host)

            # Check if this host should return NXDOMAIN
            if query_host in self._nxdomain_hosts:
                result[record_type] = DNSError(error="NXDomain")
            # Check if we have mock data for this host/type
            elif query_host in self._mock_data and record_type in self._mock_data[query_host]:
                answers_data = self._mock_data[query_host][record_type]
                response = self._fabricate_response(query_host, record_type, answers_data)
                result[record_type] = DNSResult(host=query_host, response=response)
            else:
                # No mock data, return empty response
                response = self._fabricate_response(query_host, record_type, [])
                result[record_type] = DNSResult(host=query_host, response=response)

        return result

    async def resolve_batch(self, hosts, record_type=None):
        """Resolve multiple hostnames concurrently (mocked), returning simplified tuples.

        Args:
            hosts: Iterable of hostname strings
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"

        Yields:
            tuple[str, str, list[str]]: (hostname, record_type, rdata) tuples.
                                        Only successful, non-empty results are returned.
        """
        record_type = record_type or "A"

        for host in hosts:
            query_host = host

            # Auto-format PTR queries if an IP address is provided
            if record_type == "PTR":
                query_host = self._format_ptr_query(host)

            # Skip NXDOMAIN hosts
            if query_host in self._nxdomain_hosts:
                continue

            # Check if we have mock data for this host
            if query_host in self._mock_data and record_type in self._mock_data[query_host]:
                answers_data = self._mock_data[query_host][record_type]
                if answers_data:  # Only yield non-empty results
                    yield (host, record_type, answers_data)

    async def resolve_batch_full(self, hosts, record_type=None, skip_empty=False, skip_errors=False):
        """Resolve multiple hostnames concurrently (mocked), returning full results.

        Args:
            hosts: Iterable of hostname strings
            record_type: Record type string ("A", "AAAA", "MX", etc.). Defaults to "A"
            skip_empty: Skip empty responses (default: False)
            skip_errors: Skip error responses (default: False)

        Yields:
            tuple[str, DNSResultOrError]: (hostname, result) pairs.
        """
        record_type = record_type or "A"

        for host in hosts:
            query_host = host

            # Auto-format PTR queries if an IP address is provided
            if record_type == "PTR":
                query_host = self._format_ptr_query(host)

            # Check if this host should return NXDOMAIN
            if query_host in self._nxdomain_hosts:
                if not skip_errors:
                    yield (host, DNSError(error="NXDomain"))
            # Check if we have mock data for this host
            elif query_host in self._mock_data and record_type in self._mock_data[query_host]:
                answers_data = self._mock_data[query_host][record_type]
                response = self._fabricate_response(query_host, record_type, answers_data)
                result = DNSResult(host=host, response=response)
                if not skip_empty or len(response.answers) > 0:
                    yield (host, result)
            else:
                # No mock data, return empty response
                response = self._fabricate_response(query_host, record_type, [])
                result = DNSResult(host=host, response=response)
                if not skip_empty:
                    yield (host, result)
