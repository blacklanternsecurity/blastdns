import pytest

from blastdns import Client, ClientConfig, DNSError, DNSResult


def test_client_config_defaults():
    cfg = ClientConfig()
    assert cfg.model_dump() == {
        "threads_per_resolver": 2,
        "request_timeout_ms": 1000,
        "max_retries": 10,
        "purgatory_threshold": 10,
        "purgatory_sentence_ms": 1000,
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
    assert isinstance(result, DNSResult)
    assert result.host == "example.com"
    assert any(answer.name_labels == "example.com." for answer in result.response.answers)


@pytest.mark.asyncio
async def test_client_resolve_ptr():
    client = Client(["127.0.0.1:5353"])
    result = await client.resolve("8.8.8.8.in-addr.arpa", "PTR")
    assert isinstance(result, DNSResult)
    assert any(answer.rdata.get("PTR", "") == "dns.google." for answer in result.response.answers)


@pytest.mark.asyncio
async def test_client_resolve_supports_default_record_type():
    client = Client(["127.0.0.1:5353"])
    result = await client.resolve("example.com")
    assert isinstance(result, DNSResult)
    assert result.response.queries[0].query_type == "A"


@pytest.mark.asyncio
async def test_client_resolve_batch_streams_results():
    client = Client(["127.0.0.1:5353"])

    hosts_list = ["example.com", "example.net", "example.org"]
    seen_hosts = []

    async for host, result in client.resolve_batch(hosts_list, "A"):
        seen_hosts.append(host)
        # Check for either success or error format
        if isinstance(result, DNSError):
            assert isinstance(result.error, str)
        else:
            assert isinstance(result, DNSResult)
            assert len(result.response.queries) > 0
            assert len(result.response.answers) >= 0
            assert result.response.queries[0].query_type == "A"

    assert sorted(seen_hosts) == sorted(hosts_list)


@pytest.mark.asyncio
async def test_client_resolve_batch_accepts_generators():
    client = Client(["127.0.0.1:5353"])

    def host_gen():
        for domain in ["com", "net", "org"]:
            yield f"example.{domain}"

    count = 0
    async for host, result in client.resolve_batch(host_gen(), "A"):
        assert host.startswith("example.")
        if isinstance(result, DNSResult):
            assert len(result.response.queries) > 0
        count += 1

    assert count == 3


@pytest.mark.asyncio
async def test_client_resolve_batch_handles_mixed_success_and_failure():
    client = Client(["127.0.0.1:5353"])

    # Mix valid and invalid hosts
    hosts = ["example.com", "invalid-host-that-does-not-exist-12345.com"]
    results = {}

    async for host, result in client.resolve_batch(hosts, "A"):
        results[host] = result

    assert len(results) == 2
    # At least one should succeed
    assert any(isinstance(r, DNSResult) for r in results.values())


@pytest.mark.asyncio
async def test_client_resolve_multi_requires_at_least_one_record_type():
    client = Client(["127.0.0.1:5353"])

    with pytest.raises(RuntimeError, match="at least one record type"):
        await client.resolve_multi("example.com", [])


@pytest.mark.asyncio
async def test_client_resolve_multi_resolves_multiple_types():
    client = Client(["127.0.0.1:5353"])

    results = await client.resolve_multi("example.com", ["A", "AAAA", "MX"])

    # Should return a dict with all requested record types
    assert isinstance(results, dict)
    assert set(results.keys()) == {"A", "AAAA", "MX"}

    # A record should have answers
    a_result = results["A"]
    assert isinstance(a_result, (DNSResult, DNSError))
    if isinstance(a_result, DNSResult):
        assert len(a_result.response.answers) > 0


@pytest.mark.asyncio
async def test_client_resolve_multi_handles_mixed_success_failure():
    client = Client(["127.0.0.1:5353"])

    # Request common types that should succeed and potentially one that might not have records
    results = await client.resolve_multi("example.com", ["A", "AAAA", "CAA"])

    # All record types should be present in results
    assert len(results) == 3
    assert "A" in results
    assert "AAAA" in results
    assert "CAA" in results

    # A should succeed
    a_result = results["A"]
    if isinstance(a_result, DNSResult):
        assert len(a_result.response.answers) >= 0

    # Individual results can succeed or fail
    for record_type, result in results.items():
        assert isinstance(result, (DNSResult, DNSError))
        # Each result should have either response or error
        assert hasattr(result, "response") or hasattr(result, "error")


@pytest.mark.asyncio
async def test_client_resolve_batch_skip_empty_filters_empty_responses():
    client = Client(["127.0.0.1:5353"])

    # example.com will return A records, garbage subdomain won't
    hosts = ["example.com", "lkgdjasldkjsdgsdgsdfahwejhori.example.com"]

    # With skip_empty=False, should get both results
    all_results = {}
    async for host, result in client.resolve_batch(hosts, "A", skip_empty=False):
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
    async for host, result in client.resolve_batch(hosts, "A", skip_empty=True):
        filtered_results[host] = result

    assert len(filtered_results) == 1, "should only get one result with skip_empty=True"
    assert "example.com" in filtered_results
    example_filtered = filtered_results["example.com"]
    assert isinstance(example_filtered, DNSResult)
    assert len(example_filtered.response.answers) > 0


@pytest.mark.asyncio
async def test_client_resolve_batch_skip_empty_allows_errors():
    # Use a non-responsive resolver to generate errors
    bad_config = ClientConfig(
        request_timeout_ms=100,
        max_retries=0,
    )
    bad_client = Client(["127.0.0.1:5354"], bad_config)

    error_count = 0
    async for host, result in bad_client.resolve_batch(["example.com"], "A", skip_empty=True):
        error_count += 1
        assert isinstance(result, DNSError), "should get error from non-responsive resolver"
        assert host == "example.com"

    assert error_count == 1, "errors should pass through even with skip_empty=True"


@pytest.mark.asyncio
async def test_client_resolve_batch_skip_errors_filters_error_responses():
    # Use a non-responsive resolver to generate errors
    bad_config = ClientConfig(
        request_timeout_ms=100,
        max_retries=0,
    )
    bad_client = Client(["127.0.0.1:5354"], bad_config)

    # With skip_errors=False, should get errors
    error_count = 0
    async for host, result in bad_client.resolve_batch(["example.com"], "A", skip_errors=False):
        error_count += 1
        assert isinstance(result, DNSError), "should get error from non-responsive resolver"
        assert host == "example.com"

    assert error_count == 1, "should get error with skip_errors=False"

    # With skip_errors=True, should get nothing
    filtered_count = 0
    async for host, result in bad_client.resolve_batch(["example.com"], "A", skip_errors=True):
        filtered_count += 1

    assert filtered_count == 0, "errors should be filtered with skip_errors=True"


@pytest.mark.asyncio
async def test_client_resolve_batch_basic_returns_simplified_tuples():
    import ipaddress

    client = Client(["127.0.0.1:5353"])

    hosts_list = ["example.com", "example.net", "example.org"]
    seen_hosts = []

    async for host, rdtype, answers in client.resolve_batch_basic(hosts_list, "A"):
        seen_hosts.append(host)
        assert rdtype == "A", "record type should be A"
        assert isinstance(answers, list), "answers should be a list"
        assert len(answers) > 1, f"should have multiple answers, got {len(answers)}"

        # Verify answers are valid IPv4 addresses
        for answer in answers:
            assert isinstance(answer, str), "each answer should be a string"
            # Should not contain DNS record metadata
            assert "IN" not in answer, "answer should not contain IN class"
            assert " " not in answer, "IP address should not contain spaces"
            # Validate it's a proper IPv4 address
            try:
                ip = ipaddress.IPv4Address(answer)
                assert str(ip) == answer, f"IP address should be normalized: {answer}"
            except ipaddress.AddressValueError:
                pytest.fail(f"Invalid IPv4 address: {answer}")

    assert sorted(seen_hosts) == sorted(hosts_list)


@pytest.mark.asyncio
async def test_client_resolve_batch_basic_filters_errors_and_empty():
    import ipaddress

    client = Client(["127.0.0.1:5353"])

    # example.com will return A records, garbage subdomain won't
    hosts = ["example.com", "lkgdjasldkjsdgsdgsdfahwejhori.example.com"]

    results = []
    async for host, rdtype, answers in client.resolve_batch_basic(hosts, "A"):
        results.append((host, rdtype, answers))

    # Should only get example.com (garbage domain and errors filtered out)
    assert len(results) == 1, "should only get valid, non-empty results"
    assert results[0][0] == "example.com"
    assert results[0][1] == "A"
    assert len(results[0][2]) > 1, f"should have multiple answers, got {len(results[0][2])}"

    # Validate all answers are valid IPv4 addresses
    for answer in results[0][2]:
        try:
            ipaddress.IPv4Address(answer)
        except ipaddress.AddressValueError:
            pytest.fail(f"Invalid IPv4 address: {answer}")


@pytest.mark.asyncio
async def test_client_resolve_batch_basic_with_mx_records():
    client = Client(["127.0.0.1:5353"])

    # Test with MX records
    hosts = ["gmail.com"]
    results = []

    async for host, rdtype, answers in client.resolve_batch_basic(hosts, "MX"):
        results.append((host, rdtype, answers))

    # Should get results if MX records exist
    if len(results) > 0:
        assert results[0][0] == "gmail.com"
        assert results[0][1] == "MX"
        # MX answers should be just the rdata (e.g., "10 aspmx.l.google.com.")
        for answer in results[0][2]:
            assert isinstance(answer, str), "answer should be a string"
            # MX rdata format is "preference mailserver"
            parts = answer.split(None, 1)
            assert len(parts) == 2, "MX should have preference and server"
            assert parts[0].isdigit(), "first part should be preference number"
            assert "." in parts[1], "second part should be mail server domain"


@pytest.mark.asyncio
async def test_client_resolve_batch_basic_accepts_generators():
    import ipaddress

    client = Client(["127.0.0.1:5353"])

    def host_gen():
        for domain in ["com", "net", "org"]:
            yield f"example.{domain}"

    count = 0
    async for host, rdtype, answers in client.resolve_batch_basic(host_gen(), "A"):
        assert host.startswith("example."), "host should start with example."
        assert rdtype == "A", "record type should be A"
        assert len(answers) > 1, f"should have multiple answers, got {len(answers)}"

        # Validate each answer is a valid IPv4 address
        for answer in answers:
            try:
                ipaddress.IPv4Address(answer)
            except ipaddress.AddressValueError:
                pytest.fail(f"Invalid IPv4 address: {answer}")

        count += 1

    assert count == 3, "should process all three hosts"
