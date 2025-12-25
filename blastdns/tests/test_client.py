import pytest

from blastdns import Client, ClientConfig, DNSError, DNSResult, get_system_resolvers


def test_get_system_resolvers():
    """Test that get_system_resolvers returns valid IP addresses."""
    resolvers = get_system_resolvers()

    assert isinstance(resolvers, list), "should return a list"
    assert len(resolvers) > 0, "should have at least one system resolver"

    # Validate each resolver is a valid IP address
    import ipaddress

    for resolver in resolvers:
        assert isinstance(resolver, str), f"resolver should be string, got {type(resolver)}"
        # Should be just IP, no port
        assert ":" not in resolver, f"resolver should not have port: {resolver}"
        # Should be valid IP
        try:
            ipaddress.ip_address(resolver)
        except ValueError:
            pytest.fail(f"Invalid IP address: {resolver}")


def test_client_config_defaults():
    cfg = ClientConfig()
    assert cfg.model_dump() == {
        "threads_per_resolver": 2,
        "request_timeout_ms": 1000,
        "max_retries": 10,
        "purgatory_threshold": 10,
        "purgatory_sentence_ms": 1000,
        "cache_capacity": 10000,
        "cache_min_ttl_secs": 10,
        "cache_max_ttl_secs": 86400,
    }


def test_client_config_custom_values():
    cfg = ClientConfig(
        threads_per_resolver=4,
        request_timeout_ms=2500,
        max_retries=3,
        purgatory_threshold=7,
        purgatory_sentence_ms=2000,
    )
    data = cfg.model_dump()
    assert data["threads_per_resolver"] == 4
    assert data["request_timeout_ms"] == 2500
    assert data["max_retries"] == 3
    assert data["purgatory_threshold"] == 7
    assert data["purgatory_sentence_ms"] == 2000


@pytest.mark.asyncio
async def test_client_resolve_hits_real_resolver():
    client = Client(["127.0.0.1:5353"])
    result = await client.resolve("example.com", "A")
    assert isinstance(result, list)
    assert len(result) > 0
    # Results should be IP addresses
    import ipaddress

    for ip in result:
        ipaddress.IPv4Address(ip)  # Will raise if invalid


@pytest.mark.asyncio
async def test_client_resolve_ptr():
    client = Client(["127.0.0.1:5353"])

    # Test with pre-formatted PTR query
    result = await client.resolve("8.8.8.8.in-addr.arpa", "PTR")
    assert isinstance(result, list)
    assert len(result) > 0
    # PTR results should be domain names
    assert any("dns.google." in ptr for ptr in result)


@pytest.mark.asyncio
async def test_client_resolve_ptr_auto_formats_ipv4():
    client = Client(["127.0.0.1:5353"])

    # Test with raw IPv4 address - should be auto-formatted to in-addr.arpa
    result = await client.resolve("8.8.8.8", "PTR")
    assert isinstance(result, list)
    assert len(result) > 0
    # PTR results should be domain names
    assert any("dns.google." in ptr for ptr in result)


@pytest.mark.asyncio
async def test_client_resolve_ptr_auto_formats_ipv6():
    client = Client(["127.0.0.1:5353"])

    # Test with raw IPv6 address - should be auto-formatted to ip6.arpa
    # Using Google's public DNS IPv6: 2001:4860:4860::8888
    result = await client.resolve("2001:4860:4860::8888", "PTR")
    assert isinstance(result, list)
    assert len(result) > 0
    # Should get dns.google response
    assert any("dns.google" in ptr for ptr in result)


@pytest.mark.asyncio
async def test_client_resolve_supports_default_record_type():
    client = Client(["127.0.0.1:5353"])
    result = await client.resolve("example.com")
    assert isinstance(result, list)
    assert len(result) > 0
    # Default should be A records (IPv4 addresses)
    import ipaddress

    for ip in result:
        ipaddress.IPv4Address(ip)


@pytest.mark.asyncio
async def test_client_resolve_batch_streams_results():
    import ipaddress

    client = Client(["127.0.0.1:5353"])

    hosts_list = ["example.com", "example.net", "example.org"]
    seen_hosts = []

    async for host, record_type, answers in client.resolve_batch(hosts_list, "A"):
        seen_hosts.append(host)
        assert record_type == "A"
        assert isinstance(answers, list)
        assert len(answers) > 1, f"should have multiple answers, got {len(answers)}"

        # Verify answers are valid IPv4 addresses
        for answer in answers:
            assert isinstance(answer, str), "each answer should be a string"
            try:
                ip = ipaddress.IPv4Address(answer)
                assert str(ip) == answer, f"IP address should be normalized: {answer}"
            except ipaddress.AddressValueError:
                pytest.fail(f"Invalid IPv4 address: {answer}")

    assert sorted(seen_hosts) == sorted(hosts_list)


@pytest.mark.asyncio
async def test_client_resolve_batch_accepts_generators():
    import ipaddress

    client = Client(["127.0.0.1:5353"])

    def host_gen():
        for domain in ["com", "net", "org"]:
            yield f"example.{domain}"

    count = 0
    async for host, record_type, answers in client.resolve_batch(host_gen(), "A"):
        assert host.startswith("example.")
        assert record_type == "A"
        assert len(answers) > 1, f"should have multiple answers, got {len(answers)}"

        # Validate each answer is a valid IPv4 address
        for answer in answers:
            try:
                ipaddress.IPv4Address(answer)
            except ipaddress.AddressValueError:
                pytest.fail(f"Invalid IPv4 address: {answer}")

        count += 1

    assert count == 3


@pytest.mark.asyncio
async def test_client_resolve_batch_filters_errors_and_empty():
    import ipaddress

    client = Client(["127.0.0.1:5353"])

    # Mix valid and invalid hosts
    hosts = ["example.com", "invalid-host-that-does-not-exist-12345.com"]
    results = {}

    async for host, record_type, answers in client.resolve_batch(hosts, "A"):
        results[host] = answers

    # Only successful results with answers should be returned (resolve_batch filters automatically)
    # Should only have example.com (invalid host filtered out)
    assert len(results) == 1
    assert "example.com" in results
    assert len(results["example.com"]) > 1, f"should have multiple answers, got {len(results['example.com'])}"
    assert "invalid-host-that-does-not-exist-12345.com" not in results

    # Validate all answers are valid IPv4 addresses
    for answer in results["example.com"]:
        try:
            ipaddress.IPv4Address(answer)
        except ipaddress.AddressValueError:
            pytest.fail(f"Invalid IPv4 address: {answer}")


@pytest.mark.asyncio
async def test_client_resolve_multi_requires_at_least_one_record_type():
    client = Client(["127.0.0.1:5353"])

    with pytest.raises(RuntimeError, match="at least one record type"):
        await client.resolve_multi_full("example.com", [])


@pytest.mark.asyncio
async def test_client_resolve_multi_resolves_multiple_types():
    client = Client(["127.0.0.1:5353"])

    results = await client.resolve_multi("example.com", ["A", "AAAA", "MX"])

    # Should return a dict with all successful record types
    assert isinstance(results, dict)
    # Should have all three types (example.com has all of them)
    assert len(results) == 3
    assert set(results.keys()) == {"A", "AAAA", "MX"}

    # All should be lists
    for record_type, answers in results.items():
        assert isinstance(answers, list)
        assert len(answers) > 0


@pytest.mark.asyncio
async def test_client_resolve_multi_filters_successful_queries():
    client = Client(["127.0.0.1:5353"])

    # Request common types that should succeed and potentially one that might not have records
    results = await client.resolve_multi("example.com", ["A", "AAAA", "CAA"])

    assert set(results) == {"A", "AAAA"}

    assert results["A"] and all(isinstance(answer, str) for answer in results["A"]), "A should have answers"
    assert results["AAAA"] and all(isinstance(answer, str) for answer in results["AAAA"]), "AAAA should have answers"


@pytest.mark.asyncio
async def test_client_resolve_batch_full_skip_empty_filters_empty_responses():
    client = Client(["127.0.0.1:5353"])

    # example.com will return A records, garbage subdomain won't
    hosts = ["example.com", "lkgdjasldkjsdgsdgsdfahwejhori.example.com"]

    # With skip_empty=False, should get both results
    all_results = {}
    async for host, result in client.resolve_batch_full(hosts, "A", skip_empty=False):
        all_results[host] = result

    assert len(all_results) == 2, "should get both results with skip_empty=False"

    # example.com should have answers
    example_result = all_results["example.com"]
    assert isinstance(example_result, DNSResult)
    assert len(example_result.response.answers) > 0

    # garbage domain should have empty answers (or error)
    garbage_result = all_results["lkgdjasldkjsdgsdgsdfahwejhori.example.com"]
    if isinstance(garbage_result, DNSResult):
        assert len(garbage_result.response.answers) == 0, "garbage domain should have no answers"

    # With skip_empty=True, should only get example.com
    filtered_results = {}
    async for host, result in client.resolve_batch_full(hosts, "A", skip_empty=True):
        filtered_results[host] = result

    assert len(filtered_results) == 1, "should only get one result with skip_empty=True"
    assert "example.com" in filtered_results
    example_filtered = filtered_results["example.com"]
    assert isinstance(example_filtered, DNSResult)
    assert len(example_filtered.response.answers) > 0


@pytest.mark.asyncio
async def test_client_resolve_batch_full_skip_empty_allows_errors():
    # Use a non-responsive resolver to generate errors
    bad_config = ClientConfig(
        request_timeout_ms=100,
        max_retries=0,
    )
    bad_client = Client(["127.0.0.1:5354"], bad_config)

    error_count = 0
    async for host, result in bad_client.resolve_batch_full(["example.com"], "A", skip_empty=True):
        error_count += 1
        assert isinstance(result, DNSError), "should get error from non-responsive resolver"
        assert host == "example.com"

    assert error_count == 1, "errors should pass through even with skip_empty=True"


@pytest.mark.asyncio
async def test_client_resolve_batch_with_mx_records():
    client = Client(["127.0.0.1:5353"])

    # Test with MX records
    hosts = ["gmail.com"]
    results = []

    async for host, rdtype, answers in client.resolve_batch(hosts, "MX"):
        results.append((host, rdtype, answers))

    # Should get MX results for gmail.com
    assert len(results) == 1, "should get exactly one result for gmail.com"
    assert results[0][0] == "gmail.com"
    assert results[0][1] == "MX"
    assert len(results[0][2]) > 0, "gmail.com should have MX records"

    # MX answers should be just the rdata (e.g., "10 aspmx.l.google.com.")
    for answer in results[0][2]:
        assert isinstance(answer, str), "answer should be a string"
        # MX rdata format is "preference mailserver"
        parts = answer.split(None, 1)
        assert len(parts) == 2, "MX should have preference and server"
        assert parts[0].isdigit(), "first part should be preference number"
        assert "." in parts[1], "second part should be mail server domain"


@pytest.mark.asyncio
async def test_client_resolve_batch_full_skip_errors_filters_error_responses():
    # Use a non-responsive resolver to generate errors
    bad_config = ClientConfig(
        request_timeout_ms=100,
        max_retries=0,
    )
    bad_client = Client(["127.0.0.1:5354"], bad_config)

    # With skip_errors=False, should get errors
    error_count = 0
    async for host, result in bad_client.resolve_batch_full(["example.com"], "A", skip_errors=False):
        error_count += 1
        assert isinstance(result, DNSError), "should get error from non-responsive resolver"
        assert host == "example.com"

    assert error_count == 1, "should get error with skip_errors=False"

    # With skip_errors=True, should get nothing
    filtered_count = 0
    async for host, result in bad_client.resolve_batch_full(["example.com"], "A", skip_errors=True):
        filtered_count += 1

    assert filtered_count == 0, "errors should be filtered with skip_errors=True"
