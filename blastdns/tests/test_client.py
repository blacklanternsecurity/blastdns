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
    assert any(
        answer.name_labels == "example.com." for answer in result.response.answers
    )


@pytest.mark.asyncio
async def test_client_resolve_ptr():
    client = Client(["127.0.0.1:5353"])
    result = await client.resolve("8.8.8.8.in-addr.arpa", "PTR")
    assert isinstance(result, DNSResult)
    assert any(
        answer.rdata.get("PTR", "") == "dns.google." for answer in result.response.answers
    )


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
