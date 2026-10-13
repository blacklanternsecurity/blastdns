#!/usr/bin/env python3
"""Compare two blastdns builds across the regimes a caller actually runs in.

    python scripts/benchmark.py --base BASE_PYTHON --head HEAD_PYTHON

Each build is an interpreter whose environment has that build of blastdns
installed. Every regime runs in a fresh process under each build, alternating
which goes first each round so runner drift lands on both sides. The report shows
medians side by side, and bolds a change only when the two builds' ranges across
rounds don't overlap.

Impairment is generated in-process and needs no root. Which queries it drops or
refuses is random, seeded per round and shared by both builds, so chance shows up
as spread across rounds instead of as a difference. Regimes that need throughput
point at a real resolver (`--real-resolver`), since a Python server would itself
become the ceiling.
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

CONNTRACK_COUNT = Path("/proc/sys/net/netfilter/nf_conntrack_count")
IMPAIRED_PORT = 15399


# ---------------------------------------------------------------- regimes


@dataclass
class Regime:
    name: str
    # "real" is one real resolver, "pool" is 32 loopback aliases of it, and
    # "impaired" is an in-process server shaped by `impair`.
    resolvers: str
    small: bool = False
    inflight: int = 2
    persistent: bool = False
    impair: dict = field(default_factory=dict)
    timeout_ms: int = 1000
    retries: int = 10
    # (names, one_in) segments, in order: every one_in-th name in a segment is one
    # the impaired server never answers, at any rate.
    dead: tuple = ()


REGIMES = [
    Regime("ceiling/1-resolver", "real", inflight=32),
    Regime("pool/32-resolvers", "pool"),
    Regime("pool/32-persistent", "pool", inflight=8, persistent=True),
    Regime("impaired/10%-loss", "impaired", small=True, inflight=32, impair={"loss_one_in": 10}),
    Regime("impaired/20%-REFUSED", "impaired", small=True, inflight=32, impair={"refuse_one_in": 5}),
    Regime("impaired/50ms-latency", "impaired", small=True, inflight=32, impair={"latency_ms": 50}),
    # Shaped like bbot's main client: a bad patch where a fifth of names never
    # answer, then a steady 2.5%. Pacing can't fix either, so once the patch
    # passes the rate has to come back.
    Regime(
        "impaired/dead-names-after-bad-patch",
        "impaired",
        inflight=25,
        timeout_ms=500,
        retries=1,
        dead=((500, 5), (2500, 40)),
    ),
]


# ---------------------------------------------------------------- impaired server


class ImpairedResolver(asyncio.DatagramProtocol):
    """A DNS server that answers everything, with optional loss, latency, refusals,
    and names it never answers. Loss and refusals are random, from `seed`.
    """

    def __init__(self, loss_one_in=0, latency_ms=0, refuse_one_in=0, seed=0):
        self.loss_one_in = loss_one_in
        self.latency_ms = latency_ms
        self.refuse_one_in = refuse_one_in
        self.rng = random.Random(seed)
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        import dns.message
        import dns.rcode
        import dns.rdatatype
        import dns.rrset

        if self.loss_one_in and self.rng.random() < 1 / self.loss_one_in:
            return
        try:
            query = dns.message.from_wire(data)
        except Exception:
            return
        if any(q.name.labels and q.name.labels[0].startswith(b"dead") for q in query.question):
            return
        response = dns.message.make_response(query)
        if self.refuse_one_in and self.rng.random() < 1 / self.refuse_one_in:
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
        self._loop = None
        self._thread = None
        self._ready = threading.Event()

    def _serve(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        async def boot():
            protocol = ImpairedResolver(**self.kwargs)
            await self._loop.create_datagram_endpoint(lambda: protocol, local_addr=("127.0.0.1", self.port))
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

    def __init__(self, interval=0.002):
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
            return len(os.listdir(f"/proc/{os.getpid()}/fd"))
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


# ---------------------------------------------------------------- worker


async def run_blastdns(hosts, resolvers, regime):
    from blastdns import Client, ClientConfig, DNSError

    client = Client(
        resolvers,
        ClientConfig(
            max_concurrency=1000,
            max_inflight_per_resolver=regime.inflight,
            persistent_socket=regime.persistent,
            cache_capacity=0,
            request_timeout_ms=regime.timeout_ms,
            max_retries=regime.retries,
            adaptive=True,
        ),
    )
    # Warm up before timing: building sockets costs the same at any query count.
    async for _ in client.resolve_batch_full(["warmup.invalid"], "A"):
        pass

    marks, delivered = [], 0
    with Footprint() as fp:
        start = time.monotonic()
        async for _host, item in client.resolve_batch_full(hosts, "A"):
            marks.append(time.monotonic() - start)
            if not isinstance(item, DNSError):
                delivered += 1
        elapsed = time.monotonic() - start

    marks.sort()

    def pct(p):
        return marks[min(int(len(marks) * p / 100), len(marks) - 1)] if marks else None

    attempts = sum(s.attempted for s in client.stats())
    return {
        "qps": len(hosts) / elapsed if elapsed else 0,
        "unanswered_pct": 100 * (len(hosts) - delivered) / len(hosts),
        "attempts_per_query": attempts / len(hosts),
        "p50": pct(50),
        "p99": pct(99),
        "p100": pct(100),
        "peak_fds": fp.peak_fds,
        "conntrack": fp.conntrack_added,
    }


async def worker(regime, args):
    if regime.dead:
        hosts = []
        for names, one_in in regime.dead:
            for j in range(names):
                i = len(hosts)
                hosts.append(f"dead{i}.bench.local" if j % one_in == 0 else f"{i}.bench.local")
    else:
        count = max(2000, args.num_queries // 10) if regime.small else args.num_queries
        hosts = [f"{i}.bench.local" for i in range(count)]
    # One server answers on every loopback alias, so a pool needs no extra servers.
    host, _, port = args.real_resolver.partition(":")
    if regime.resolvers == "real":
        return await run_blastdns(hosts, [args.real_resolver], regime)
    if regime.resolvers == "pool":
        return await run_blastdns(hosts, [f"127.0.0.{i}:{port}" for i in range(1, 33)], regime)
    with ThreadedImpaired(IMPAIRED_PORT, seed=args.seed, **regime.impair):
        return await run_blastdns(hosts, [f"127.0.0.1:{IMPAIRED_PORT}"], regime)


# ---------------------------------------------------------------- comparison


def run_one(python, regime, args, seed):
    cmd = [
        python,
        __file__,
        "--worker",
        regime.name,
        "-n",
        str(args.num_queries),
        "--real-resolver",
        args.real_resolver,
        "--seed",
        str(seed),
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=args.limit)
    except subprocess.TimeoutExpired:
        return {"note": f"did not finish within {args.limit}s"}
    if out.returncode != 0:
        lines = (out.stderr or "").strip().splitlines()
        return {"note": f"failed: {lines[-1] if lines else 'exit ' + str(out.returncode)}"}
    return json.loads(out.stdout.strip().splitlines()[-1])


def compare(args):
    builds = {"base": args.base, "head": args.head}
    regimes = [r for r in REGIMES if not args.only or args.only in r.name]
    results = {r.name: {"base": [], "head": []} for r in regimes}
    for rnd in range(args.rounds):
        order = ["base", "head"] if rnd % 2 == 0 else ["head", "base"]
        for regime in regimes:
            for build in order:
                print(f"  .. round {rnd + 1}/{args.rounds} {regime.name} {build}", file=sys.stderr, flush=True)
                results[regime.name][build].append(run_one(builds[build], regime, args, seed=rnd))
    report(results, args)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(results, indent=2))


# ---------------------------------------------------------------- report

# metric -> (label, formatter)
METRICS = {
    "qps": ("qps", lambda v: f"{v:,.0f}"),
    "unanswered_pct": ("unanswered", lambda v: f"{v:.2f}%"),
    "attempts_per_query": ("attempts/query", lambda v: f"{v:.2f}"),
    "p50": ("p50", lambda v: f"{v:.3f}s"),
    "p99": ("p99", lambda v: f"{v:.3f}s"),
    "p100": ("p100", lambda v: f"{v:.3f}s"),
    "peak_fds": ("sockets", lambda v: f"{v:,.0f}"),
    "conntrack": ("conntrack", lambda v: f"{v:,.0f}"),
}


def cell(base_runs, head_runs, metric):
    fmt = METRICS[metric][1]
    b = [r[metric] for r in base_runs if r.get(metric) is not None]
    h = [r[metric] for r in head_runs if r.get(metric) is not None]
    if not b or not h:
        notes = {r["note"] for r in base_runs + head_runs if "note" in r}
        return "_" + "; ".join(sorted(notes)) + "_" if notes else "n/a"
    bm, hm = statistics.median(b), statistics.median(h)
    text = f"{fmt(bm)} → {fmt(hm)}"
    visible = fmt(bm) != fmt(hm)
    if bm and metric not in ("unanswered_pct", "attempts_per_query"):
        change = round((hm - bm) / bm, 2)
        text += f" ({change:+.0%})" if change else " (0%)"
        visible = visible and change != 0
    # Bold only a visible change the run-to-run spread can't explain, which takes
    # a spread to judge.
    apart = min(h) > max(b) or max(h) < min(b)
    if apart and visible and len(b) > 1 and len(h) > 1:
        text = f"**{text}**"
    return text


def report(results, args):
    print("## DNS Resolver Benchmark\n")
    print(
        f"`{args.head_label}` against `{args.base_label}`, base → head, median of "
        f"{args.rounds} interleaved round{'s' if args.rounds != 1 else ''}. **Bold** is "
        "outside both builds' run-to-run range; the rest is noise.\n"
    )
    headline = ["qps", "unanswered_pct", "p99", "attempts_per_query"]
    print("| regime | " + " | ".join(METRICS[m][0] for m in headline) + " |")
    print("|---" * (len(headline) + 1) + "|")
    for name, runs in results.items():
        cells = [cell(runs["base"], runs["head"], m) for m in headline]
        print(f"| {name} | " + " | ".join(cells) + " |")
    print("\n<details><summary>All metrics</summary>\n")
    rest = [m for m in METRICS if m not in headline]
    print("| regime | " + " | ".join(METRICS[m][0] for m in rest) + " |")
    print("|---" * (len(rest) + 1) + "|")
    for name, runs in results.items():
        cells = [cell(runs["base"], runs["head"], m) for m in rest]
        print(f"| {name} | " + " | ".join(cells) + " |")
    print("\n</details>")


# ---------------------------------------------------------------- entry


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", help="interpreter with the base build of blastdns")
    p.add_argument("--head", help="interpreter with the head build of blastdns")
    p.add_argument("--base-label", default="base")
    p.add_argument("--head-label", default="head")
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--limit", type=int, default=120, help="seconds before a run counts as not finishing")
    p.add_argument("-n", "--num-queries", type=int, default=20000)
    p.add_argument("--real-resolver", default="127.0.0.1:5353", help="unimpaired server for throughput regimes")
    p.add_argument("--only", help="run only regimes whose name contains this")
    p.add_argument("--json-out")
    p.add_argument("--worker", help=argparse.SUPPRESS)
    p.add_argument("--seed", type=int, default=0, help=argparse.SUPPRESS)
    args = p.parse_args()

    if args.worker:
        regime = next(r for r in REGIMES if r.name == args.worker)
        print(json.dumps(asyncio.run(worker(regime, args))))
        return
    if not (args.base and args.head):
        p.error("--base and --head are required")
    compare(args)


if __name__ == "__main__":
    main()
