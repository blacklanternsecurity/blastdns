#!/usr/bin/env python3
"""Benchmark blastdns across the regimes a caller actually runs in.

The existing benchmark reports one number -- queries per second against a single
local resolver with no impairment. That number moves 36% between identical
commits on a shared CI runner, and no real workload resembles the regime, so it
neither detects regressions nor predicts behavior.

This measures several regimes and reports, per regime:

  throughput          queries per second (report only -- not stable across hardware)
  vs dnspython        same-run ratio, which is stable across hardware
  unanswered          fraction of requested names that never came back at all
  attempts/query      retry amplification, i.e. what the delivery cost
  p50 / p99 / p100    completion percentiles; the tail is what sets wall clock
  sockets             peak file descriptors held
  conntrack           peak connection-tracking entries added

Only the last five are worth gating on. Throughput belongs in the report so a
human can see it, and nowhere near a pass/fail threshold.

Impairment is generated in-process so it is deterministic and needs no root:
`--impair loss=N` drops every Nth response, `latency=MS` delays every response.
High-throughput regimes point at a real resolver instead, since a Python server
would itself become the ceiling.
"""

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import sys

import dns.message
import dns.rcode
import dns.rdatatype
import dns.rrset

CONNTRACK_COUNT = Path("/proc/sys/net/netfilter/nf_conntrack_count")


# ---------------------------------------------------------------- impaired server


class ImpairedResolver(asyncio.DatagramProtocol):
    """A DNS server that answers everything, with optional loss and latency.

    Deterministic by construction: the Nth query is the one dropped, so a run is
    reproducible and a delivery number means something.
    """

    def __init__(self, loss_one_in=0, latency_ms=0, refuse_one_in=0):
        self.loss_one_in = loss_one_in
        self.latency_ms = latency_ms
        self.refuse_one_in = refuse_one_in
        self.received = 0
        self.dropped = 0
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.received += 1
        n = self.received
        if self.loss_one_in and n % self.loss_one_in == 0:
            self.dropped += 1
            return
        try:
            query = dns.message.from_wire(data)
        except Exception:
            return
        response = dns.message.make_response(query)
        if self.refuse_one_in and n % self.refuse_one_in == 0:
            response.set_rcode(dns.rcode.REFUSED)
        else:
            for question in query.question:
                if question.rdtype == dns.rdatatype.A:
                    response.answer.append(dns.rrset.from_text(question.name, 60, "IN", "A", "127.0.0.1"))
        wire = response.to_wire()
        if self.latency_ms:
            asyncio.get_running_loop().call_later(self.latency_ms / 1000, self.transport.sendto, wire, addr)
        else:
            self.transport.sendto(wire, addr)


class ThreadedImpaired:
    """Runs an impaired resolver on its own event loop in its own thread.

    It cannot share the caller's loop: the client holds that loop while awaiting a
    batch, so a server on it could never answer the queries being awaited.
    """

    def __init__(self, port, **kwargs):
        self.port = port
        self.kwargs = kwargs
        self.protocol = None
        self._loop = None
        self._thread = None
        self._ready = threading.Event()

    def _serve(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        async def boot():
            self.protocol = ImpairedResolver(**self.kwargs)
            await self._loop.create_datagram_endpoint(lambda: self.protocol, local_addr=("127.0.0.1", self.port))
            self._ready.set()

        self._loop.run_until_complete(boot())
        self._loop.run_forever()

    def __enter__(self):
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError(f"impaired resolver on :{self.port} did not start")
        return self

    def __exit__(self, *exc):
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2)


# ---------------------------------------------------------------- footprint


class Footprint:
    """Samples what the process is holding while a run is in flight."""

    def __init__(self, pid=None, interval=0.002):
        self.pid = pid or os.getpid()
        self.interval = interval
        self.peak_fds = 0
        self.peak_conntrack = 0
        self.base_conntrack = self._conntrack()
        self._stop = threading.Event()
        self._thread = None

    def _conntrack(self):
        try:
            return int(CONNTRACK_COUNT.read_text().strip())
        except OSError:
            return 0

    def _fds(self):
        try:
            return len(os.listdir(f"/proc/{self.pid}/fd"))
        except OSError:
            return 0

    def _run(self):
        while not self._stop.is_set():
            self.peak_fds = max(self.peak_fds, self._fds())
            self.peak_conntrack = max(self.peak_conntrack, self._conntrack())
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=2)

    @property
    def conntrack_added(self):
        return max(0, self.peak_conntrack - self.base_conntrack)


# ---------------------------------------------------------------- results


@dataclass
class Result:
    engine: str
    regime: str
    requested: int
    delivered: int = 0
    elapsed: float = 0.0
    attempts: int = 0
    marks: list = field(default_factory=list)
    peak_fds: int = 0
    conntrack: int = 0
    resolvers_used: int = 0
    concurrency: int = 0
    note: str = ""

    @property
    def qps(self):
        return self.requested / self.elapsed if self.elapsed else 0

    @property
    def unanswered_pct(self):
        missing = self.requested - self.delivered
        return 100 * missing / self.requested if self.requested else 0

    @property
    def attempts_per_query(self):
        return self.attempts / self.requested if self.requested and self.attempts else 0

    def pct(self, p):
        if not self.marks:
            return None
        ordered = sorted(self.marks)
        return ordered[min(int(len(ordered) * p / 100), len(ordered) - 1)]


# ---------------------------------------------------------------- engines


async def run_blastdns(hosts, resolvers, regime, persistent=False, inflight=2, concurrency=1000):
    from blastdns import Client, ClientConfig, DNSError

    client = Client(
        resolvers,
        ClientConfig(
            max_concurrency=concurrency,
            max_inflight_per_resolver=inflight,
            persistent_socket=persistent,
            cache_capacity=0,
            request_timeout_ms=1000,
            max_retries=10,
            adaptive=True,
        ),
    )
    result = Result("blastdns", regime, len(hosts))
    # What the engine could actually hold in flight: the per-resolver cap binds
    # before max_concurrency when the pool is small.
    result.concurrency = min(concurrency, inflight * len(resolvers))
    with Footprint() as fp:
        start = time.monotonic()
        async for _host, item in client.resolve_batch_full(hosts, "A"):
            result.marks.append(time.monotonic() - start)
            if not isinstance(item, DNSError):
                result.delivered += 1
        result.elapsed = time.monotonic() - start
    stats = client.stats()
    result.attempts = sum(s.attempted for s in stats)
    result.resolvers_used = sum(1 for s in stats if s.attempted > 0)
    result.peak_fds, result.conntrack = fp.peak_fds, fp.conntrack_added
    return result


async def run_dnspython(hosts, resolvers, regime, workers=100):
    import dns.asyncresolver

    resolver = dns.asyncresolver.Resolver(configure=False)
    host, _, port = resolvers[0].partition(":")
    resolver.nameservers = [host]
    resolver.port = int(port or 53)
    resolver.lifetime = 2.0

    result = Result("dnspython", regime, len(hosts))
    result.concurrency = workers
    queue = asyncio.Queue()
    for name in hosts:
        queue.put_nowait(name)

    async def worker():
        while True:
            try:
                name = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await resolver.resolve(name, "A")
                result.delivered += 1
            except Exception:
                pass
            result.marks.append(time.monotonic() - start)

    with Footprint() as fp:
        start = time.monotonic()
        await asyncio.gather(*(worker() for _ in range(workers)))
        result.elapsed = time.monotonic() - start
    result.peak_fds, result.conntrack = fp.peak_fds, fp.conntrack_added
    return result


def run_massdns(hosts, resolvers, regime):
    binary = shutil.which("massdns")
    if not binary:
        r = Result("massdns", regime, len(hosts))
        r.note = "not installed"
        return r
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as hf:
        hf.write("\n".join(hosts))
        hosts_path = hf.name
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as rf:
        rf.write("\n".join(resolvers))
        resolvers_path = rf.name

    result = Result("massdns", regime, len(hosts))
    # massdns is a batch tool: it reports no per-query timing, so percentiles are
    # left unset rather than invented.
    cmd = [binary, "-r", resolvers_path, "-t", "A", "-o", "J", "-q", hosts_path]
    start = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    with Footprint(pid=proc.pid) as fp:
        try:
            out, _ = proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
            result.note = "timed out after 120s"
    result.elapsed = time.monotonic() - start
    for line in out.splitlines():
        try:
            if json.loads(line).get("data", {}).get("answers"):
                result.delivered += 1
        except Exception:
            pass
    result.peak_fds, result.conntrack = fp.peak_fds, fp.conntrack_added
    os.unlink(hosts_path)
    os.unlink(resolvers_path)
    return result


# ---------------------------------------------------------------- regimes


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("-n", "--num-queries", type=int, default=20000)
    p.add_argument("--real-resolver", default="127.0.0.1:5355", help="unimpaired server for ceiling regimes")
    p.add_argument("--json-out")
    args = p.parse_args()

    hosts = [f"{i}.bench.local" for i in range(args.num_queries)]
    small = hosts[: max(2000, args.num_queries // 10)]
    real = args.real_resolver
    # One server answers on every loopback alias, so a pool of N distinct resolver
    # entries needs no extra servers.
    host, _, port = real.partition(":")
    pool = [f"127.0.0.{i}:{port}" for i in range(1, 33)]

    results = []

    def step(msg):
        print(f"  .. {msg}", file=sys.stderr, flush=True)

    async def bounded(coro, engine, regime, requested, limit=45):
        """Cap every regime. Not finishing is itself a result worth reporting."""
        try:
            return await asyncio.wait_for(coro, timeout=limit)
        except asyncio.TimeoutError:
            r = Result(engine, regime, requested)
            r.note = f"did not finish within {limit}s"
            print(f"     ! {engine}/{regime} did not finish", file=sys.stderr, flush=True)
            return r

    # Regimes against the real resolver: dispatch ceiling and pool behavior.
    step("blastdns ceiling")
    results.append(
        await bounded(
            run_blastdns(hosts, [real], "ceiling/1-resolver", inflight=32),
            "blastdns",
            "ceiling/1-resolver",
            len(hosts),
        )
    )
    step("dnspython ceiling")
    results.append(
        await bounded(
            run_dnspython(small, [real], "ceiling/1-resolver"), "dnspython", "ceiling/1-resolver", len(small)
        )
    )
    step("massdns ceiling")
    results.append(
        await bounded(
            asyncio.to_thread(run_massdns, hosts, [real], "ceiling/1-resolver"),
            "massdns",
            "ceiling/1-resolver",
            len(hosts),
        )
    )

    step("blastdns pool")
    results.append(
        await bounded(run_blastdns(hosts, pool, "pool/32-resolvers"), "blastdns", "pool/32-resolvers", len(hosts))
    )
    step("massdns pool")
    results.append(
        await bounded(
            asyncio.to_thread(run_massdns, hosts, pool, "pool/32-resolvers"),
            "massdns",
            "pool/32-resolvers",
            len(hosts),
        )
    )
    step("blastdns pool persistent")
    results.append(
        await bounded(
            run_blastdns(hosts, pool, "pool/32-persistent", persistent=True, inflight=8),
            "blastdns",
            "pool/32-persistent",
            len(hosts),
        )
    )

    # Impaired regimes: server is in-process, so throughput is capped by it. The
    # point here is delivery and tail, not speed.
    imp_port = 15399
    with ThreadedImpaired(imp_port, loss_one_in=10):
        imp = [f"127.0.0.1:{imp_port}"]
        step("blastdns loss")
        results.append(
            await bounded(
                run_blastdns(small, imp, "impaired/10%-loss", inflight=32), "blastdns", "impaired/10%-loss", len(small)
            )
        )
        step("massdns loss")
        results.append(
            await bounded(
                asyncio.to_thread(run_massdns, small, imp, "impaired/10%-loss"),
                "massdns",
                "impaired/10%-loss",
                len(small),
            )
        )

    with ThreadedImpaired(imp_port + 1, refuse_one_in=5):
        imp = [f"127.0.0.1:{imp_port + 1}"]
        step("blastdns refused")
        results.append(
            await bounded(
                run_blastdns(small, imp, "impaired/20%-REFUSED", inflight=32),
                "blastdns",
                "impaired/20%-REFUSED",
                len(small),
            )
        )
        step("massdns refused")
        results.append(
            await bounded(
                asyncio.to_thread(run_massdns, small, imp, "impaired/20%-REFUSED"),
                "massdns",
                "impaired/20%-REFUSED",
                len(small),
            )
        )

    with ThreadedImpaired(imp_port + 2, latency_ms=50):
        imp = [f"127.0.0.1:{imp_port + 2}"]
        step("blastdns latency")
        results.append(
            await bounded(
                run_blastdns(small, imp, "impaired/50ms-latency", inflight=32),
                "blastdns",
                "impaired/50ms-latency",
                len(small),
            )
        )

    report(results, args.json_out)


def report(results, json_out=None):
    by_regime = {}
    for r in results:
        by_regime.setdefault(r.regime, []).append(r)

    print("## blastdns benchmark matrix\n")
    print("Throughput is reported, never gated: it moves with hardware. The stable")
    print("columns are unanswered, attempts/query, sockets, and conntrack.\n")

    for regime, group in by_regime.items():
        baseline = next((r.qps for r in group if r.engine == "dnspython"), None)
        print(f"### {regime}\n")
        header = (
            "| engine | in flight | qps | vs dnspython | unanswered | atts/query | "
            "p50 | p99 | p100 | sockets | conntrack |"
        )
        print(header)
        print("|" + "---|" * 11)
        for r in group:
            if r.note:
                print(f"| {r.engine} | | _{r.note}_ | | | | | | | | |")
                continue
            ratio = f"{r.qps / baseline:.2f}x" if baseline else "-"
            p50 = f"{r.pct(50):.3f}s" if r.pct(50) is not None else "n/a"
            p99 = f"{r.pct(99):.3f}s" if r.pct(99) is not None else "n/a"
            p100 = f"{r.pct(100):.3f}s" if r.pct(100) is not None else "n/a"
            atts = f"{r.attempts_per_query:.2f}" if r.attempts_per_query else "n/a"
            print(
                f"| {r.engine} | {r.concurrency or '-'} | {r.qps:,.0f} | {ratio} | {r.unanswered_pct:.3f}% | {atts} "
                f"| {p50} | {p99} | {p100} | {r.peak_fds:,} | {r.conntrack:,} |"
            )
        print()

    if json_out:
        payload = [
            {
                "engine": r.engine,
                "regime": r.regime,
                "qps": round(r.qps),
                "unanswered_pct": round(r.unanswered_pct, 4),
                "attempts_per_query": round(r.attempts_per_query, 3),
                "p99": r.pct(99),
                "p100": r.pct(100),
                "peak_fds": r.peak_fds,
                "conntrack": r.conntrack,
                "resolvers_used": r.resolvers_used,
                "concurrency": r.concurrency,
            }
            for r in results
            if not r.note
        ]
        Path(json_out).write_text(json.dumps(payload, indent=2))
        print(f"_machine-readable results written to {json_out}_")


if __name__ == "__main__":
    asyncio.run(main())
