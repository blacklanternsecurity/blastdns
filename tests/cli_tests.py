#!/usr/bin/env python3
import subprocess
import tempfile
import json
import select
import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def binary_path():
    """Build the release binary once for all tests."""
    print("Building release binary...")
    subprocess.run(["cargo", "build", "--release"], check=True)
    return Path("target/release/blastdns")


@pytest.fixture
def resolver_file():
    """Create a temporary resolver file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("127.0.0.1:5353\n")
        f.flush()
        path = f.name
    yield path
    Path(path).unlink(missing_ok=True)


@pytest.fixture
def hosts_file():
    """Create a temporary hosts file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("example.com\n")
        f.write("example.net\n")
        f.flush()
        path = f.name
    yield path
    Path(path).unlink(missing_ok=True)


def test_streaming_stdin_first_host(binary_path, resolver_file):
    """Test that results stream immediately as hosts are provided via stdin."""
    proc = subprocess.Popen(
        [str(binary_path), "--resolvers", resolver_file],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    try:
        # Write first host
        print("google.com", file=proc.stdin, flush=True)

        # Read the result with timeout
        assert select.select([proc.stdout], [], [], 5.0)[0], "Timeout waiting for result"
        result_line = proc.stdout.readline()

        assert result_line, "No output received"
        result = json.loads(result_line)
        assert result.get("host") == "google.com", f"Expected host 'google.com', got {result.get('host')}"

        # Verify it has the full response format
        assert "response" in result or "error" in result, "Result should have response or error"

    finally:
        proc.stdin.close()
        proc.terminate()
        proc.wait(timeout=2)


def test_streaming_stdin_multiple_hosts(binary_path, resolver_file):
    """Test streaming multiple hosts via stdin."""
    proc = subprocess.Popen(
        [str(binary_path), "--resolvers", resolver_file],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    try:
        hosts = ["google.com", "example.com"]
        results = []

        for host in hosts:
            print(host, file=proc.stdin, flush=True)

            # Read result with timeout
            assert select.select([proc.stdout], [], [], 5.0)[0], f"Timeout waiting for {host}"
            result_line = proc.stdout.readline()
            assert result_line, f"No output for {host}"

            result = json.loads(result_line)
            results.append(result)

        # Verify we got both results
        assert len(results) == 2
        result_hosts = [r.get("host") for r in results]
        assert set(result_hosts) == set(hosts)

    finally:
        proc.stdin.close()
        proc.terminate()
        proc.wait(timeout=2)


def test_brief_mode_simple_json(binary_path, resolver_file, hosts_file):
    """Test that --brief flag outputs simplified JSON format."""
    result = subprocess.run(
        [str(binary_path), hosts_file, "--resolvers", resolver_file, "--rdtype", "A", "--brief"],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, f"Command failed: {result.stderr}"

    lines = [line for line in result.stdout.strip().split("\n") if line]
    assert len(lines) > 0, "No output from brief mode"

    for line in lines:
        data = json.loads(line)

        # Brief mode should have: host, record_type, answers
        assert "host" in data, "Missing 'host' field"
        assert "record_type" in data, "Missing 'record_type' field"
        assert "answers" in data, "Missing 'answers' field"

        # Brief mode should NOT have these fields
        assert "response" not in data, "Brief mode should not have 'response' field"
        assert "error" not in data, "Brief mode should not have 'error' field"

        # Verify types
        assert isinstance(data["host"], str)
        assert isinstance(data["record_type"], str)
        assert isinstance(data["answers"], list)
        assert len(data["answers"]) > 0, "Answers should not be empty"

        # For A records, answers should be IP addresses
        assert data["record_type"] == "A"
        for answer in data["answers"]:
            assert isinstance(answer, str)
            # Verify it looks like an IP address
            parts = answer.split(".")
            assert len(parts) == 4, f"Invalid IP format: {answer}"


def test_full_mode_complex_json(binary_path, resolver_file, hosts_file):
    """Test that default mode outputs full DNS response."""
    result = subprocess.run(
        [str(binary_path), hosts_file, "--resolvers", resolver_file, "--rdtype", "A"],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, f"Command failed: {result.stderr}"

    lines = [line for line in result.stdout.strip().split("\n") if line]
    assert len(lines) > 0, "No output from full mode"

    for line in lines:
        data = json.loads(line)

        # Full mode should have: host, response (or error)
        assert "host" in data, "Missing 'host' field"
        assert "response" in data or "error" in data, "Should have response or error"

        if "response" in data:
            response = data["response"]
            assert isinstance(response, dict), "Response should be an object"
            # Full response should have DNS message structure
            assert "header" in response, "Missing 'header' in response"
            assert "queries" in response, "Missing 'queries' in response"


def test_brief_mode_mx_records(binary_path, resolver_file):
    """Test brief mode with MX records."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("google.com\n")
        hosts_path = f.name

    try:
        result = subprocess.run(
            [str(binary_path), hosts_path, "--resolvers", resolver_file, "--rdtype", "MX", "--brief"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0, f"Command failed: {result.stderr}"

        lines = [line for line in result.stdout.strip().split("\n") if line]
        assert len(lines) > 0, "No output for MX query"

        data = json.loads(lines[0])
        assert data["record_type"] == "MX"
        assert len(data["answers"]) > 0, "Should have MX records"

        # MX records should contain priority and domain
        for answer in data["answers"]:
            assert " " in answer, f"MX record should have priority and domain: {answer}"

    finally:
        Path(hosts_path).unlink(missing_ok=True)


def test_skip_empty_flag(binary_path, resolver_file):
    """Test that --skip-empty filters out empty responses."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("example.com\n")
        f.write("example.net\n")
        hosts_path = f.name

    try:
        # Query for a record type that might not exist
        result_with_empty = subprocess.run(
            [str(binary_path), hosts_path, "--resolvers", resolver_file, "--rdtype", "AAAA"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        result_skip_empty = subprocess.run(
            [str(binary_path), hosts_path, "--resolvers", resolver_file, "--rdtype", "AAAA", "--skip-empty"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        lines_with_empty = [line for line in result_with_empty.stdout.strip().split("\n") if line]
        lines_skip_empty = [line for line in result_skip_empty.stdout.strip().split("\n") if line]

        # With --skip-empty, we should have fewer or equal results
        assert len(lines_skip_empty) <= len(lines_with_empty)

    finally:
        Path(hosts_path).unlink(missing_ok=True)


def test_skip_errors_flag(binary_path):
    """Test that --skip-errors filters out actual errors (timeouts, unreachable resolvers)."""
    # Use an unreachable resolver to generate actual errors
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("192.0.2.1\n")  # TEST-NET-1 address, should be unreachable
        bad_resolver_path = f.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("example.com\n")
        f.write("google.com\n")
        hosts_path = f.name

    try:
        # With very short timeout, these should error
        result_with_errors = subprocess.run(
            [str(binary_path), hosts_path, "--resolvers", bad_resolver_path, "--rdtype", "A", "--timeout-ms", "10"],
            capture_output=True,
            text=True,
            timeout=5,
        )

        result_skip_errors = subprocess.run(
            [
                str(binary_path),
                hosts_path,
                "--resolvers",
                bad_resolver_path,
                "--rdtype",
                "A",
                "--timeout-ms",
                "10",
                "--skip-errors",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        lines_with_errors = [line for line in result_with_errors.stdout.strip().split("\n") if line]
        lines_skip_errors = [line for line in result_skip_errors.stdout.strip().split("\n") if line]

        # With --skip-errors, we should have fewer or no results
        assert len(lines_skip_errors) <= len(lines_with_errors)

        # Count error entries in result_with_errors
        error_count = sum(1 for line in lines_with_errors if "error" in json.loads(line))
        assert error_count > 0, "Should have at least one error"

        # All remaining results should be successful (there probably won't be any)
        for line in lines_skip_errors:
            data = json.loads(line)
            assert "error" not in data, "Should not have error field with --skip-errors"
            assert "response" in data, "Should have response field"

    finally:
        Path(hosts_path).unlink(missing_ok=True)
        Path(bad_resolver_path).unlink(missing_ok=True)
