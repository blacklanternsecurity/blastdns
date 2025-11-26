import pytest

from blastdns import Client, ClientConfig


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
    assert "answers" in result
    assert any(
        answer.get("name_labels") == "example.com." for answer in result["answers"]
    )


@pytest.mark.asyncio
async def test_client_resolve_ptr():
    client = Client(["127.0.0.1:5353"])
    result = await client.resolve("8.8.8.8.in-addr.arpa", "PTR")
    assert "answers" in result
    assert any(
        answer.get("rdata", {}).get("PTR", "") == "dns.google." for answer in result["answers"]
    )


@pytest.mark.asyncio
async def test_client_resolve_supports_default_record_type():
    client = Client(["127.0.0.1:5353"])
    result = await client.resolve("example.com")
    assert "queries" in result
    assert result["queries"][0]["query_type"] == "A"


@pytest.mark.asyncio
async def test_client_resolve_batch_streams_results():
    client = Client(["127.0.0.1:5353"])

    hosts_list = ["example.com", "example.net", "example.org"]
    seen_hosts = []

    async for host, result in client.resolve_batch(hosts_list, "A"):
        seen_hosts.append(host)
        # Check for either success or error format
        if "error" in result:
            assert isinstance(result["error"], str)
        else:
            assert "queries" in result
            assert "answers" in result
            assert result["queries"][0]["query_type"] == "A"

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
        if "error" not in result:
            assert "queries" in result
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
    assert any("answers" in r for r in results.values())
