#!/usr/bin/env python3
"""Compare two blastdns builds across the regimes a caller actually runs in.

    python scripts/benchmark.py --base BASE_PYTHON --head HEAD_PYTHON

Each build is an interpreter whose environment has that build of blastdns
installed. Every regime runs in a fresh process under each build, alternating
which goes first each round so runner drift lands on both sides. The report shows
medians side by side and bolds a change only when a t-test finds it significant
after a Benjamini-Hochberg correction over every cell in the report, so a build
compared against itself shows any bold at all only 1% of the time.

Impairment is generated in-process and needs no root. Which queries it drops or
refuses is random, seeded per round and shared by both builds. Regimes that need
throughput point at a real resolver (`--real-resolver`), since a Python server
would itself become the ceiling.
"""

import argparse
import asyncio
import enum
import json
import math
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
# Loopback aliases one real resolver answers on, so a pool needs no extra servers.
POOL_SIZE = 32
# A Python server caps throughput, so impaired regimes are sized to finish quickly.
IMPAIRED_QUERIES = 2000
# Names starting with this are never answered by the impaired server.
DEAD_PREFIX = "dead"
FOOTPRINT_INTERVAL_S = 0.002
# Every query goes to the network, and the global cap stays out of the way of the
# per-resolver limit each regime sets.
COMMON_CONFIG = {"cache_capacity": 0, "max_concurrency": 1000}
# False discovery rate; also the chance of any bold when nothing changed.
FALSE_DISCOVERY_RATE = 0.01
# Smallest relative change worth bolding, however significant.
MIN_EFFECT = 0.01


# ---------------------------------------------------------------- regimes


class Resolvers(enum.Enum):
    REAL = "real"
    POOL = "pool"
    IMPAIRED = "impaired"


@dataclass
class Regime:
    resolvers: Resolvers
    # ClientConfig overrides on top of COMMON_CONFIG.
    config: dict = field(default_factory=dict)
    # ImpairedResolver options.
    impair: dict = field(default_factory=dict)
    # (names, one_in) segments, in order: every one_in-th name is never answered.
    dead: tuple = ()

    @property
    def name(self):
        if self.resolvers is Resolvers.REAL:
            return "ceiling/1-resolver"
        if self.resolvers is Resolvers.POOL:
            kind = "persistent" if self.config.get("persistent_socket") else "resolvers"
            return f"pool/{POOL_SIZE}-{kind}"
        parts = []
        if "loss_one_in" in self.impair:
            parts.append(f"{100 / self.impair['loss_one_in']:g}%-loss")
        if "refuse_one_in" in self.impair:
            parts.append(f"{100 / self.impair['refuse_one_in']:g}%-REFUSED")
        if "latency_ms" in self.impair:
            parts.append(f"{self.impair['latency_ms']}ms-latency")
        if self.dead:
            parts.append("-then-".join(f"{100 / one_in:g}%" for _, one_in in self.dead) + "-dead-names")
        return "impaired/" + "-".join(parts)

    def hosts(self, num_queries):
        if not self.dead:
            count = IMPAIRED_QUERIES if self.resolvers is Resolvers.IMPAIRED else num_queries
            return [f"{i}.bench.local" for i in range(count)]
        hosts = []
        for names, one_in in self.dead:
            for j in range(names):
                prefix = DEAD_PREFIX if j % one_in == 0 else ""
                hosts.append(f"{prefix}{len(hosts)}.bench.local")
        return hosts


REGIMES = [
    Regime(Resolvers.REAL, config={"max_inflight_per_resolver": 32}),
    Regime(Resolvers.POOL),
    Regime(Resolvers.POOL, config={"persistent_socket": True, "max_inflight_per_resolver": 8}),
    Regime(Resolvers.IMPAIRED, config={"max_inflight_per_resolver": 32}, impair={"loss_one_in": 10}),
    Regime(Resolvers.IMPAIRED, config={"max_inflight_per_resolver": 32}, impair={"refuse_one_in": 5}),
    Regime(Resolvers.IMPAIRED, config={"max_inflight_per_resolver": 32}, impair={"latency_ms": 50}),
    # Shaped like bbot's main client: a bad patch where a fifth of names never
    # answer, then a steady 2.5%. Pacing can't fix either, so once the patch
    # passes the rate has to come back.
    Regime(
        Resolvers.IMPAIRED,
        config={"max_inflight_per_resolver": 25, "request_timeout_ms": 500, "max_retries": 1},
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
        if any(q.name.labels and q.name.labels[0].startswith(DEAD_PREFIX.encode()) for q in query.question):
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
    """Runs an impaired resolver on its own event loop in its own thread, on a port
    the OS picks.

    It cannot share the caller's loop: the client holds that loop while awaiting a
    batch, so a server on it could never answer the queries being awaited.
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.port = None
        self._loop = None
        self._thread = None
        self._ready = threading.Event()

    def _serve(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        async def boot():
            protocol = ImpairedResolver(**self.kwargs)
            transport, _ = await self._loop.create_datagram_endpoint(lambda: protocol, local_addr=("127.0.0.1", 0))
            self.port = transport.get_extra_info("sockname")[1]
            self._ready.set()

        self._loop.run_until_complete(boot())
        self._loop.run_forever()

    def __enter__(self):
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("impaired resolver did not start")
        return self

    def __exit__(self, *exc):
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2)


# ---------------------------------------------------------------- footprint


class Footprint:
    """Samples what the process is holding while a run is in flight."""

    def __init__(self):
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
            self._stop.wait(FOOTPRINT_INTERVAL_S)

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

    client = Client(resolvers, ClientConfig(**COMMON_CONFIG, **regime.config))
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
    hosts = regime.hosts(args.num_queries)
    if regime.resolvers is Resolvers.REAL:
        return await run_blastdns(hosts, [args.real_resolver], regime)
    if regime.resolvers is Resolvers.POOL:
        _, _, port = args.real_resolver.partition(":")
        pool = [f"127.0.0.{i}:{port}" for i in range(1, POOL_SIZE + 1)]
        return await run_blastdns(hosts, pool, regime)
    with ThreadedImpaired(seed=args.seed, **regime.impair) as server:
        return await run_blastdns(hosts, [f"127.0.0.1:{server.port}"], regime)


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
    """Run every regime under both builds and print the report. Returns how many
    runs measured nothing."""
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
    return sum("note" in r for runs in results.values() for side in runs.values() for r in side)


# ---------------------------------------------------------------- statistics


def _beta_continued_fraction(a, b, x):
    """Lentz's method for the continued fraction of the incomplete beta function."""
    tiny = 1e-300
    c, d = 1.0, 1 - (a + b) * x / (a + 1)
    d = 1 / (d if abs(d) > tiny else tiny)
    h = d
    for m in range(1, 300):
        for numerator in (
            m * (b - m) * x / ((a + 2 * m - 1) * (a + 2 * m)),
            -(a + m) * (a + b + m) * x / ((a + 2 * m) * (a + 2 * m + 1)),
        ):
            d = 1 + numerator * d
            d = 1 / (d if abs(d) > tiny else tiny)
            c = 1 + numerator / c
            c = c if abs(c) > tiny else tiny
            h *= d * c
        if abs(d * c - 1) < 1e-14:
            break
    return h


def regularized_beta(a, b, x):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    log_front = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x)
    if x < (a + 1) / (a + b + 2):
        return math.exp(log_front) * _beta_continued_fraction(a, b, x) / a
    return 1 - math.exp(log_front) * _beta_continued_fraction(b, a, 1 - x) / b


def t_test_p(base, head):
    """Two-sided p-value of a pooled two-sample t-test that the samples share a mean.

    Pooled rather than Welch: both builds get the same number of rounds, which keeps
    it robust to unequal spread, and it keeps its degrees of freedom when one build
    is noisier, which Welch at a handful of rounds does not.
    """
    nb, nh = len(base), len(head)
    df = nb + nh - 2
    pooled = ((nb - 1) * statistics.variance(base) + (nh - 1) * statistics.variance(head)) / df
    diff = statistics.fmean(head) - statistics.fmean(base)
    if pooled == 0:
        return 1.0 if diff == 0 else 0.0
    t = diff / math.sqrt(pooled * (1 / nb + 1 / nh))
    return regularized_beta(df / 2, 0.5, df / (df + t * t))


def discoveries(p_values, rate):
    """Benjamini-Hochberg: the largest p-value still counted as a discovery, or None."""
    ordered = sorted(p_values)
    passing = [p for k, p in enumerate(ordered, 1) if p <= k * rate / len(ordered)]
    return passing[-1] if passing else None


# ---------------------------------------------------------------- report


@dataclass
class Metric:
    key: str
    label: str
    fmt: object
    # In the headline table rather than the details.
    headline: bool = False
    # Shown and judged as a relative change.
    relative: bool = True


METRICS = [
    Metric("qps", "qps", lambda v: f"{v:,.0f}", headline=True),
    Metric("unanswered_pct", "unanswered", lambda v: f"{v:.2f}%", headline=True, relative=False),
    Metric("p99", "p99", lambda v: f"{v:.3f}s", headline=True),
    Metric("attempts_per_query", "attempts/query", lambda v: f"{v:.2f}", headline=True, relative=False),
    Metric("p50", "p50", lambda v: f"{v:.3f}s"),
    Metric("p100", "p100", lambda v: f"{v:.3f}s"),
    Metric("peak_fds", "sockets", lambda v: f"{v:,.0f}"),
    Metric("conntrack", "conntrack", lambda v: f"{v:,.0f}"),
]


def change_text(change):
    return f"{change:+.1%}" if abs(change) < 0.1 else f"{change:+.0%}"


@dataclass
class Cell:
    text: str
    p: float = None
    meaningful: bool = False


def cell(base_runs, head_runs, metric):
    b = [r[metric.key] for r in base_runs if r.get(metric.key) is not None]
    h = [r[metric.key] for r in head_runs if r.get(metric.key) is not None]
    if not b or not h:
        notes = {r["note"] for r in base_runs + head_runs if "note" in r}
        return Cell("_" + "; ".join(sorted(notes)) + "_" if notes else "n/a")
    bm, hm = statistics.median(b), statistics.median(h)
    text = f"{metric.fmt(bm)} → {metric.fmt(hm)}"
    meaningful = metric.fmt(bm) != metric.fmt(hm)
    if metric.relative and bm:
        change = (hm - bm) / bm
        text += f" ({change_text(change)})"
        meaningful = abs(change) >= MIN_EFFECT
    p = None
    if len(b) > 1 and len(h) > 1:
        # Rates and times vary in proportion to their size, so compare them as logs.
        if metric.relative and min(b + h) > 0:
            b, h = [math.log(v) for v in b], [math.log(v) for v in h]
        p = t_test_p(b, h)
    return Cell(text, p, meaningful)


def report(results, args):
    cells = {name: {m.key: cell(runs["base"], runs["head"], m) for m in METRICS} for name, runs in results.items()}
    p_values = [c.p for row in cells.values() for c in row.values() if c.p is not None]
    cutoff = discoveries(p_values, FALSE_DISCOVERY_RATE) if p_values else None

    def render(c):
        bold = cutoff is not None and c.p is not None and c.p <= cutoff and c.meaningful
        return f"**{c.text}**" if bold else c.text

    def table(metrics):
        print("| regime | " + " | ".join(m.label for m in metrics) + " |")
        print("|---" * (len(metrics) + 1) + "|")
        for name, row in cells.items():
            print(f"| {name} | " + " | ".join(render(row[m.key]) for m in metrics) + " |")

    rounds = f"{args.rounds} interleaved round{'s' if args.rounds != 1 else ''}"
    print("## DNS Resolver Benchmark\n")
    print(
        f"`{args.head_label}` against `{args.base_label}`, base → head, median of {rounds}. "
        f"**Bold** is significant across all {len(p_values)} comparisons "
        f"(Benjamini-Hochberg at {FALSE_DISCOVERY_RATE:.0%}); the rest is noise.\n"
    )
    table([m for m in METRICS if m.headline])
    print("\n<details><summary>All metrics</summary>\n")
    table([m for m in METRICS if not m.headline])
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
    failed = compare(args)
    if failed:
        print(f"{failed} run(s) measured nothing; see the report", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
