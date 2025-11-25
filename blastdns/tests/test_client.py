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


# @pytest.mark.asyncio
# async def test_client_resolve_batch_streams_results_and_accepts_generators():
#     client = Client(["127.0.0.1:5353"])

#     hosts_list = ["example.com", "example.net"]
#     seen_hosts = []

#     async for host, result in client.resolve_batch(hosts_list, "A"):
#         assert "queries" in result
#         seen_hosts.append(host)

#     assert sorted(seen_hosts) == sorted(hosts_list)

#     # Now verify that we can pass a generator and that it is consumed lazily.
#     def host_gen():
#         for i in range(5):
#             yield f"example{i}.com"

#     gen_hosts = host_gen()
#     count = 0
#     async for host, result in client.resolve_batch(gen_hosts, "A"):
#         assert "queries" in result
#         assert host.startswith("example")
#         count += 1

#     assert count == 5
