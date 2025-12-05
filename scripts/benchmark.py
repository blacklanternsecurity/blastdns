#!/usr/bin/env python3
"""
Benchmark comparing blastdns vs dnspython resolver performance.

Resolves a hostname repeatedly using concurrent async tasks.
"""

import argparse
import asyncio
import json
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import dns.asyncresolver
import dns.message
import uvloop
from tabulate import tabulate

from blastdns import Client


# =============================================================================
# dnspython benchmark
# =============================================================================


@dataclass
class WorkItem:
    """A DNS query work item."""

    hostname: str
    index: int
    result = None
    error = None


async def dnspython_worker(worker_id, queue, resolver):
    """Worker task that consumes WorkItems from the queue and resolves them."""
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        try:
            answer = await resolver.resolve(item.hostname, "A")
            item.result = answer.response
        except Exception as e:
            item.error = e

        queue.task_done()


async def benchmark_dnspython(hostnames, num_workers, nameserver):
    """Benchmark dnspython with concurrent workers."""
    if ":" in nameserver:
        ns_ip, ns_port = nameserver.rsplit(":", 1)
        ns_port = int(ns_port)
    else:
        ns_ip = nameserver
        ns_port = 53

    resolver = dns.asyncresolver.Resolver()
    resolver.nameservers = [ns_ip]
    resolver.port = ns_port
    resolver.timeout = 2.0
    resolver.lifetime = 2.0

    queue = asyncio.Queue(maxsize=num_workers * 2)
    work_items = [WorkItem(hostname=h, index=i) for i, h in enumerate(hostnames)]

    start_time = time.perf_counter()

    workers = [asyncio.create_task(dnspython_worker(i, queue, resolver)) for i in range(num_workers)]

    for item in work_items:
        await queue.put(item)

    await queue.join()

    for _ in range(num_workers):
        await queue.put(None)
    await asyncio.gather(*workers)

    total_time = time.perf_counter() - start_time

    success_count = sum(1 for item in work_items if item.result is not None)
    error_count = sum(1 for item in work_items if item.error is not None)
    qps = len(hostnames) / total_time

    return total_time, qps, success_count, error_count


# =============================================================================
# blastdns Python library benchmark
# =============================================================================


async def benchmark_blastdns(hostnames, num_workers, nameserver):
    """Benchmark blastdns with resolve_batch."""
    client = Client([nameserver], config=None)

    start_time = time.perf_counter()

    success_count = 0
    # resolve_batch automatically filters errors and empty responses,
    # so we count total queries vs what we got back
    async for host, rdtype, answers in client.resolve_batch(hostnames, "A"):
        success_count += 1

    total_time = time.perf_counter() - start_time
    qps = len(hostnames) / total_time

    # Error count is implicit: total requested minus successful
    error_count = len(hostnames) - success_count

    return total_time, qps, success_count, error_count


# =============================================================================
# blastdns native CLI benchmark
# =============================================================================


def benchmark_blastdns_native(hostnames, num_workers, nameserver):
    """Benchmark blastdns CLI binary."""
    # Find the binary
    binary = Path(__file__).parent.parent / "target" / "release" / "blastdns"
    if not binary.exists():
        raise RuntimeError(f"Binary not found at {binary}. Run: cargo build --release")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as hosts_file:
        for hostname in hostnames:
            hosts_file.write(f"{hostname}\n")
        hosts_path = hosts_file.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as resolver_file:
        resolver_file.write(f"{nameserver}\n")
        resolver_path = resolver_file.name

    try:
        start_time = time.perf_counter()

        result = subprocess.run(
            [
                str(binary),
                hosts_path,
                "--resolvers",
                resolver_path,
                "--threads-per-resolver",
                str(num_workers),
            ],
            capture_output=True,
            text=True,
        )

        total_time = time.perf_counter() - start_time

        if result.returncode != 0:
            raise RuntimeError(f"blastdns failed: {result.stderr}")

        # Count results from stdout
        success_count = 0
        error_count = 0
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            data = json.loads(line)
            if "error" in data:
                error_count += 1
            else:
                success_count += 1

        qps = len(hostnames) / total_time
        return total_time, qps, success_count, error_count

    finally:
        Path(hosts_path).unlink(missing_ok=True)
        Path(resolver_path).unlink(missing_ok=True)


# =============================================================================
# MassDNS benchmark
# =============================================================================


def benchmark_massdns(hostnames, num_workers, nameserver):
    """Benchmark MassDNS CLI."""
    import shutil

    binary = shutil.which("massdns")
    if not binary:
        raise RuntimeError("massdns not found in PATH")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as hosts_file:
        for hostname in hostnames:
            hosts_file.write(f"{hostname}\n")
        hosts_path = hosts_file.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as resolver_file:
        resolver_file.write(f"{nameserver}\n")
        resolver_path = resolver_file.name

    try:
        start_time = time.perf_counter()

        # MassDNS is single-threaded but uses -s for concurrent in-flight queries
        # Set to match our worker count for fair comparison
        result = subprocess.run(
            [
                binary,
                "-r",
                resolver_path,
                "-t",
                "A",
                "-o",
                "J",  # JSON output
                "-s",
                str(num_workers),  # concurrent queries
                hosts_path,
            ],
            capture_output=True,
            text=True,
        )

        total_time = time.perf_counter() - start_time

        if result.returncode != 0:
            raise RuntimeError(f"massdns failed: {result.stderr}")

        # Count results from stdout (JSON lines)
        success_count = 0
        error_count = 0
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            try:
                data = json.loads(line)
                if data.get("status") == "NOERROR":
                    success_count += 1
                else:
                    error_count += 1
            except json.JSONDecodeError:
                continue

        qps = len(hostnames) / total_time
        return total_time, qps, success_count, error_count

    finally:
        Path(hosts_path).unlink(missing_ok=True)
        Path(resolver_path).unlink(missing_ok=True)


# =============================================================================
# Main
# =============================================================================


def print_table(results, baseline="dnspython"):
    """Print results as a markdown table with performance relative to baseline."""
    baseline_qps = results.get(baseline, (0, 1, 0, 0))[1]

    rows = []
    for name, (total_time, qps, success, errors) in sorted(results.items(), key=lambda x: -x[1][1]):
        multiplier = (qps / baseline_qps) if baseline_qps > 0 else 0
        rows.append(
            [
                name,
                f"{total_time:.3f}s",
                f"{qps:,.0f}",
                f"{success:,}",
                f"{errors:,}",
                f"{multiplier:.2f}x",
            ]
        )

    headers = ["Library", "Time", "QPS", "Success", "Failed", "vs dnspython"]
    print(tabulate(rows, headers=headers, tablefmt="github"))


def generate_hostnames(num_queries, pattern):
    """Generate unique hostnames for benchmarking."""
    if "{n}" in pattern:
        return [pattern.format(n=i) for i in range(num_queries)]
    else:
        return [pattern] * num_queries


async def main():
    parser = argparse.ArgumentParser(description="Benchmark blastdns vs dnspython")
    parser.add_argument("-n", "--num-queries", type=int, default=20_000, help="Number of queries")
    parser.add_argument("-w", "--num-workers", type=int, default=100, help="Number of concurrent workers")
    parser.add_argument("-s", "--nameserver", default="127.0.0.1:5353", help="DNS server (IP:port)")
    parser.add_argument("--hostname", default="{n}.bench.local", help="Hostname pattern ({n} = query number)")
    parser.add_argument(
        "--only", choices=["blastdns-cli", "blastdns-python", "massdns", "dnspython"], help="Run only one benchmark"
    )
    args = parser.parse_args()

    print("## DNS Resolver Benchmark")
    print()
    print(f"- **Queries:** {args.num_queries:,}")
    print(f"- **Workers:** {args.num_workers}")
    print(f"- **Target:** {args.hostname}")
    print(f"- **Nameserver:** {args.nameserver}")
    print()

    results = {}

    import sys

    hostnames = generate_hostnames(args.num_queries, args.hostname)

    if args.only in (None, "blastdns-cli"):
        print("Running blastdns-cli...", file=sys.stderr, flush=True)
        results["blastdns-cli"] = benchmark_blastdns_native(hostnames, args.num_workers, args.nameserver)

    if args.only in (None, "blastdns-python"):
        print("Running blastdns-python...", file=sys.stderr, flush=True)
        results["blastdns-python"] = await benchmark_blastdns(hostnames, args.num_workers, args.nameserver)

    if args.only in (None, "massdns"):
        print("Running massdns...", file=sys.stderr, flush=True)
        try:
            results["massdns"] = benchmark_massdns(hostnames, args.num_workers, args.nameserver)
        except RuntimeError as e:
            print(f"Skipping massdns: {e}", file=sys.stderr)

    if args.only in (None, "dnspython"):
        print("Running dnspython...", file=sys.stderr, flush=True)
        results["dnspython"] = await benchmark_dnspython(hostnames, args.num_workers, args.nameserver)

    print("### Results")
    print()
    print_table(results)


if __name__ == "__main__":
    # Install uvloop as the default event loop for better performance
    uvloop.install()
    asyncio.run(main())
