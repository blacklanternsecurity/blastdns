"""Tests for batching functionality in resolve_batch and resolve_batch_full."""

import pytest

from blastdns import MockClient


@pytest.mark.asyncio
async def test_batch_timeout_triggers():
    """Test that 200ms timeout triggers batch send with slow iterator."""
    client = MockClient()

    # Mock hosts
    mock_data = {}
    for i in range(60):
        mock_data[f"host{i}.example.com"] = {"A": [f"192.0.2.{i % 255}"]}

    client.mock_dns(mock_data)

    # Generator: 20 fast, delay 250ms, 20 fast, delay 250ms, 20 fast
    # Expected batches:
    #   Batch 1: 21 items (first 20 + 1 after delay, then timeout)
    #   Batch 2: 20 items (next 19 + 1 after delay, then timeout)
    #   Batch 3: 19 items (remaining items, stream end)
    def slow_hosts():
        import time

        for i in range(60):
            yield f"host{i}.example.com"
            if i == 19 or i == 39:
                time.sleep(0.25)

    batch_sizes = []
    total_results = 0

    async for batch in client._inner.resolve_batch(slow_hosts(), "A"):
        batch_sizes.append(len(batch))
        total_results += len(batch)

    assert total_results == 60, f"Expected 60 total results, got {total_results}"
    assert len(batch_sizes) == 3, f"Expected 3 batches, got {len(batch_sizes)}: {batch_sizes}"
    assert batch_sizes[0] == 21, f"Batch 1 should have 21 items, got {batch_sizes[0]}"
    assert batch_sizes[1] == 20, f"Batch 2 should have 20 items, got {batch_sizes[1]}"
    assert batch_sizes[2] == 19, f"Batch 3 should have 19 items, got {batch_sizes[2]}"


@pytest.mark.asyncio
async def test_batch_size_1000():
    """Test that batches are sent at 1000 items."""
    client = MockClient()

    # Mock 2500 hosts - should create 3 batches: 1000, 1000, 500
    mock_data = {}
    for i in range(2500):
        mock_data[f"host{i}.example.com"] = {"A": [f"192.0.2.{i % 255}"]}

    client.mock_dns(mock_data)
    hosts = [f"host{i}.example.com" for i in range(2500)]

    # Call _inner directly to count batches
    batch_count = 0
    total_results = 0
    batch_sizes = []

    async for batch in client._inner.resolve_batch(hosts, "A"):
        batch_count += 1
        batch_size = len(batch)
        batch_sizes.append(batch_size)
        total_results += batch_size

    assert batch_count == 3, f"Expected 3 batches, got {batch_count}"
    assert total_results == 2500, f"Expected 2500 total results, got {total_results}"
    assert batch_sizes[0] == 1000, f"First batch should be 1000, got {batch_sizes[0]}"
    assert batch_sizes[1] == 1000, f"Second batch should be 1000, got {batch_sizes[1]}"
    assert batch_sizes[2] == 500, f"Third batch should be 500, got {batch_sizes[2]}"


@pytest.mark.asyncio
async def test_batch_full_with_size_limit():
    """Test resolve_batch_full respects batch size limit."""
    client = MockClient()

    # Mock 2200 hosts - should create 3 batches: 1000, 1000, 200
    mock_data = {}
    for i in range(2200):
        mock_data[f"host{i}.example.com"] = {"A": [f"192.0.2.{i % 255}"]}

    client.mock_dns(mock_data)
    hosts = [f"host{i}.example.com" for i in range(2200)]

    batch_count = 0
    total_results = 0
    batch_sizes = []

    async for batch in client._inner.resolve_batch_full(hosts, "A", False, False):
        batch_count += 1
        batch_size = len(batch)
        batch_sizes.append(batch_size)
        total_results += batch_size

    assert batch_count == 3, f"Expected 3 batches, got {batch_count}"
    assert total_results == 2200, f"Expected 2200 total results, got {total_results}"
    assert batch_sizes[0] == 1000, f"First batch should be 1000, got {batch_sizes[0]}"
    assert batch_sizes[1] == 1000, f"Second batch should be 1000, got {batch_sizes[1]}"
    assert batch_sizes[2] == 200, f"Third batch should be 200, got {batch_sizes[2]}"


@pytest.mark.asyncio
async def test_empty_stream():
    """Test that empty stream returns StopAsyncIteration."""
    client = MockClient()
    client.mock_dns({})  # No hosts

    batch_count = 0
    async for batch in client._inner.resolve_batch([], "A"):
        batch_count += 1

    assert batch_count == 0, f"Expected 0 batches, got {batch_count}"


@pytest.mark.asyncio
async def test_python_wrapper_unwraps_batches():
    """Test that the Python wrapper properly unwraps batches."""
    client = MockClient()

    # Mock 2500 hosts
    mock_data = {}
    for i in range(2500):
        mock_data[f"host{i}.example.com"] = {"A": [f"192.0.2.{i % 255}"]}

    client.mock_dns(mock_data)
    hosts = [f"host{i}.example.com" for i in range(2500)]

    # Use the high-level API that should unwrap batches
    results = []
    async for host, record_type, answers in client.resolve_batch(hosts, "A"):
        results.append((host, record_type, answers))

    assert len(results) == 2500, f"Expected 2500 results, got {len(results)}"
