import pytest

from blastdns.client import Client, MockClient
from blastdns.models import DNSResult


@pytest.fixture
def mock_client():
    """Create a mock client with pre-configured test data."""
    client = MockClient()
    client.mock_dns(
        {
            "example.com": {
                "A": ["93.184.216.34"],
                "AAAA": ["2606:2800:220:1:248:1893:25c8:1946"],
                "MX": ["10 aspmx.l.google.com.", "20 alt1.aspmx.l.google.com."],
            },
            "cname.example.com": {"CNAME": ["example.com."]},
            "_NXDOMAIN": ["notfound.example.com"],
        }
    )
    return client


@pytest.mark.asyncio
async def test_resolve(mock_client):
    """Test basic resolve functionality."""
    # Test A record
    result = await mock_client.resolve("example.com", "A")
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == "93.184.216.34"

    # Test MX record with multiple answers
    result = await mock_client.resolve("example.com", "MX")
    assert len(result) == 2
    assert "10 aspmx.l.google.com." in result
    assert "20 alt1.aspmx.l.google.com." in result

    # Test NXDOMAIN - should return empty list for simplified API
    result = await mock_client.resolve("notfound.example.com", "A")
    assert isinstance(result, list)
    assert len(result) == 0

    # Test empty response for unmocked host
    result = await mock_client.resolve("unknown.example.com", "A")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_resolve_multi(mock_client):
    """Test resolve_multi with multiple record types."""
    # Test successful multi-resolution
    results = await mock_client.resolve_multi("example.com", ["A", "AAAA", "MX"])
    assert len(results) == 3
    assert set(results.keys()) == {"A", "AAAA", "MX"}
    assert all(isinstance(r, list) for r in results.values())
    assert len(results["MX"]) == 2

    # Test NXDOMAIN - returns empty dict for simplified API (no successful results)
    results = await mock_client.resolve_multi("notfound.example.com", ["A", "AAAA"])
    assert isinstance(results, dict)
    assert len(results) == 0

    # Test partial mocking (TXT not mocked, should not be in results)
    results = await mock_client.resolve_multi("example.com", ["A", "TXT"])
    # Only A should be in results (TXT has no mock data)
    assert len(results) == 1
    assert "A" in results
    assert isinstance(results["A"], list)
    assert len(results["A"]) == 1
    assert "TXT" not in results


@pytest.mark.asyncio
async def test_resolve_batch(mock_client):
    """Test resolve_batch with simplified output."""
    # Basic batch resolution - now returns simplified tuples
    results = [r async for r in mock_client.resolve_batch(["example.com", "notfound.example.com"], "A")]
    host_map = {host: answers for host, record_type, answers in results}
    # Only successful results with answers should be returned
    assert "example.com" in host_map
    assert isinstance(host_map["example.com"], list)
    # notfound.example.com should be filtered out


@pytest.mark.asyncio
async def test_resolve_batch_full(mock_client):
    """Test resolve_batch_full with multiple hosts."""
    # Basic batch resolution with full results
    results = [r async for r in mock_client.resolve_batch_full(["example.com", "notfound.example.com"], "A")]
    host_map = {host: result for host, result in results}
    assert isinstance(host_map["example.com"], DNSResult)
    # NXDOMAIN returns empty response, not error (matches real client behavior)
    assert isinstance(host_map["notfound.example.com"], DNSResult)
    assert len(host_map["notfound.example.com"].response.answers) == 0


@pytest.mark.asyncio
async def test_resolve_batch_full_skip_empty_filters_empty_responses(mock_client):
    """Test that skip_empty filters out empty responses (like NXDOMAIN)."""
    hosts = ["example.com", "notfound.example.com"]

    # With skip_empty=False, should get both results
    all_results = {}
    async for host, result in mock_client.resolve_batch_full(hosts, "A", skip_empty=False):
        all_results[host] = result

    assert len(all_results) == 2, "should get both results with skip_empty=False"

    # example.com should have answers
    example_result = all_results["example.com"]
    assert isinstance(example_result, DNSResult)
    assert len(example_result.response.answers) > 0

    # notfound.example.com should have empty answers (NXDOMAIN)
    notfound_result = all_results["notfound.example.com"]
    assert isinstance(notfound_result, DNSResult)
    assert len(notfound_result.response.answers) == 0, "NXDOMAIN should have no answers"

    # With skip_empty=True, should only get example.com
    filtered_results = {}
    async for host, result in mock_client.resolve_batch_full(hosts, "A", skip_empty=True):
        filtered_results[host] = result

    assert len(filtered_results) == 1, "should only get one result with skip_empty=True"
    assert "example.com" in filtered_results
    example_filtered = filtered_results["example.com"]
    assert isinstance(example_filtered, DNSResult)
    assert len(example_filtered.response.answers) > 0


@pytest.mark.asyncio
async def test_resolve_ptr_auto_formats_ip(mock_client):
    """Test that PTR queries auto-format IP addresses."""
    # Mock PTR data using the formatted query
    mock_client.mock_dns(
        {
            "8.8.8.8.in-addr.arpa": {"PTR": ["dns.google."]},
        }
    )

    # Query with raw IP - should be auto-formatted
    result = await mock_client.resolve("8.8.8.8", "PTR")
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == "dns.google."

    # Query with already-formatted string should also work
    result2 = await mock_client.resolve("8.8.8.8.in-addr.arpa", "PTR")
    assert result2 == result


@pytest.mark.asyncio
async def test_mock_matches_real_client():
    """Test that MockClient produces output matching the real Client."""
    # Query example.com with real client
    real_client = Client(["8.8.8.8"])
    real_ips = await real_client.resolve("example.com", "A")

    # Should get a list of IP strings
    assert isinstance(real_ips, list)
    assert len(real_ips) > 0, "example.com should have A records"

    # Create mock client with the same data
    mock_client = MockClient()
    mock_client.mock_dns({"example.com": {"A": real_ips}})
    mock_ips = await mock_client.resolve("example.com", "A")

    # Compare the IP lists
    assert sorted(mock_ips) == sorted(real_ips)
