//! Adaptive dispatch control.
//!
//! Rather than being told how fast to go, the controller finds out. It watches
//! loss per resolver, and when a resolver starts dropping queries it records the
//! rate at which that happened as an "edge" and settles below it. A configured
//! rate limit, if any, is a separate hard cap applied on top: adaptation happens
//! either way and can only ever lower the effective rate.
//!
//! Loss on one resolver only throttles that resolver. Cutting the global rate
//! requires loss in the aggregate across resolvers that have answered at least
//! once, so a large public list full of dead entries cannot drag the rate down.
//!
//! Retries are never given up, only paced. Abandoning them under load was tried
//! and measured: it took unanswered queries from 0% to 3.5% on a 5,000-name
//! brute-force, losing real results to save load that was never shown to be a
//! problem.
//!
//! The two signals need different amounts of evidence. A per-resolver loss ratio
//! needs enough queries against that one resolver, and the per-resolver in-flight
//! cap means a few queries per tick at most, so those samples accumulate across
//! ticks until the ratio means something. The aggregate is well sampled every tick.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::Instant;
use tracing::debug;

use crate::{
    client::Outcomes,
    health::{ResolverHealth, ResolverPool},
    limiter::{RateLimiter, UNLIMITED_QPS},
};

/// Fraction of a discovered edge to operate at.
const RETREAT: f64 = 0.85;
/// Per-tick multiplier when climbing back toward the target.
const GROWTH: f64 = 1.25;
/// Loss fraction above which a resolver counts as degraded.
const LOSS_THRESHOLD: f64 = 0.02;
/// Queries against one resolver before its own loss ratio is trusted. Accumulated
/// across ticks: a resolver held to a couple of queries in flight cannot produce
/// this many within one tick, so demanding it per tick never judges anything.
const MIN_SAMPLES: u64 = 20;
/// Queries finished in a tick before the global failure rate is trusted.
const MIN_AGGREGATE_SAMPLES: u64 = 100;
/// How long a sampling window may stay open. Past this the samples span too many
/// conditions to be one measurement, and a resolver drawing this little traffic is
/// not one we could be overloading anyway.
const WINDOW_TTL: Duration = Duration::from_secs(10);
/// How often limits are recomputed.
const TICK: Duration = Duration::from_millis(500);
/// How long a discovered edge is honored. Without expiry, one transient blip
/// would cap the rest of a long scan.
const EDGE_TTL: Duration = Duration::from_secs(30);
/// Floor on any pacing rate, so a bad patch cannot stall a scan outright.
const MIN_RATE_QPS: f64 = 1.0;

#[derive(Default, Clone, Copy)]
struct Observation {
    attempted: u64,
    lost: u64,
}

/// Sampling state for one resolver.
///
/// `last` gives the per-tick delta, which is what the global rate is measured
/// against. `window` accumulates those deltas until there are enough to trust a
/// loss ratio for this one resolver, which usually takes several ticks.
///
/// The window's span is accumulated from the elapsed time each tick reports rather
/// than read off the clock, so the caller's notion of time is the only one in play.
#[derive(Default)]
struct Sampler {
    last: Observation,
    window: Observation,
    window_seconds: f64,
}

impl Sampler {
    fn reopen(&mut self) {
        self.window = Observation::default();
        self.window_seconds = 0.0;
    }
}

/// A rate at which loss was observed, and when it was seen.
struct Edge {
    qps: f64,
    seen_at: Instant,
}

/// Watches per-resolver loss and adjusts pacing to stay below the loss edge.
pub(crate) struct AdaptiveController {
    global: Arc<RateLimiter>,
    /// Configured hard cap. `UNLIMITED_QPS` when none was set.
    global_ceiling: f64,
    per_resolver: Vec<Sampler>,
    edges: Vec<Option<Edge>>,
    /// Queries finished and lost, as of the previous tick.
    outcomes: Arc<Outcomes>,
    previous_outcomes: Observation,
    /// Current global ceiling, or `None` while unthrottled. Retreat is relative to
    /// this rather than to observed throughput: observed can sit far below the
    /// limit simply because the workload is small, and retreating from it would
    /// clamp the limit for no reason and then compound on every tick.
    global_limit: Option<f64>,
}

impl AdaptiveController {
    pub(crate) fn new(
        resolver_count: usize,
        global: Arc<RateLimiter>,
        global_ceiling: f64,
        outcomes: Arc<Outcomes>,
    ) -> Self {
        Self {
            global,
            global_ceiling,
            per_resolver: (0..resolver_count).map(|_| Sampler::default()).collect(),
            edges: (0..resolver_count).map(|_| None).collect(),
            outcomes,
            previous_outcomes: Observation::default(),
            global_limit: None,
        }
    }

    /// Run the control loop until the pool is dropped.
    pub(crate) fn spawn(mut self, pool: &Arc<ResolverPool>) {
        let weak = Arc::downgrade(pool);
        tokio::spawn(async move {
            let mut previous = Instant::now();
            loop {
                tokio::time::sleep(TICK).await;
                let Some(pool) = weak.upgrade() else {
                    debug!("adaptive controller stopping: pool dropped");
                    break;
                };
                let now = Instant::now();
                self.tick(&pool, now.duration_since(previous), now);
                previous = now;
            }
        });
    }

    /// Recompute limits from the loss observed since the previous tick.
    ///
    /// Split out from the timer so it can be driven directly in tests.
    pub(crate) fn tick(&mut self, pool: &ResolverPool, elapsed: Duration, now: Instant) {
        let seconds = elapsed.as_secs_f64().max(1e-6);
        let resolvers = pool.resolvers();

        let mut attempted_total = 0u64;

        for (i, resolver) in resolvers.iter().enumerate() {
            let stats = resolver.stats();
            let current = Observation {
                attempted: stats.attempted,
                lost: stats.timeout + stats.error,
            };

            let sampler = &mut self.per_resolver[i];
            let tick_attempted = current.attempted.saturating_sub(sampler.last.attempted);
            let tick_lost = current.lost.saturating_sub(sampler.last.lost);
            sampler.last = current;
            sampler.window.attempted += tick_attempted;
            sampler.window.lost += tick_lost;
            sampler.window_seconds += seconds;

            // The global rate is measured per tick; only the per-resolver ratio
            // accumulates.
            attempted_total += tick_attempted;

            let window = sampler.window;
            let window_seconds = sampler.window_seconds;

            if window.attempted < MIN_SAMPLES {
                // Not enough queries against this resolver to judge it yet. Drop a
                // window that has gone stale rather than deciding on it, and let any
                // existing edge expire so an idle resolver is not throttled forever.
                if window_seconds >= WINDOW_TTL.as_secs_f64() {
                    sampler.reopen();
                }
                self.relax(i, now, resolver);
                continue;
            }

            self.per_resolver[i].reopen();

            let loss = window.lost as f64 / window.attempted as f64;
            if loss > LOSS_THRESHOLD {
                // The rate sustained over the window is the rate at which loss
                // appeared, so it is the edge. Operate below it.
                let observed_qps = window.attempted as f64 / window_seconds.max(1e-6);
                let target = (RETREAT * observed_qps).max(MIN_RATE_QPS);
                self.edges[i] = Some(Edge {
                    qps: observed_qps,
                    seen_at: now,
                });
                resolver.set_rate(target);
                debug!(
                    resolver = %resolver.addr(),
                    loss, observed_qps, target,
                    "resolver losing queries, retreating below the edge"
                );
            } else {
                self.relax(i, now, resolver);
            }
        }

        self.adjust_global(seconds, attempted_total);
    }

    /// Ease a resolver's pacing back up: expire a stale edge outright, otherwise
    /// climb toward the retreat target.
    fn relax(&mut self, i: usize, now: Instant, resolver: &Arc<ResolverHealth>) {
        let Some(edge) = &self.edges[i] else {
            return;
        };

        if now.duration_since(edge.seen_at) >= EDGE_TTL {
            self.edges[i] = None;
            resolver.set_rate(UNLIMITED_QPS);
            debug!(
                resolver = %resolver.addr(),
                "edge expired, probing upward again"
            );
            return;
        }

        let target = (RETREAT * edge.qps).max(MIN_RATE_QPS);
        let current = resolver.rate();
        if current < target {
            resolver.set_rate((current * GROWTH).min(target));
        }
    }

    /// Adjust the global rate from queries that ultimately failed.
    ///
    /// Deliberately not the per-attempt loss used for individual resolvers. A
    /// public pool refuses a few percent of attempts as a matter of course and a
    /// retry elsewhere answers them; counting those makes a healthy pool look
    /// permanently congested, and a permanent congestion signal drives the rate
    /// to a standstill. A query that no amount of retrying could answer is real
    /// evidence about the path.
    fn adjust_global(&mut self, seconds: f64, attempted: u64) {
        let current = Observation {
            attempted: self.outcomes.completed.load(Ordering::Relaxed),
            lost: self.outcomes.failed.load(Ordering::Relaxed),
        };
        let previous = std::mem::replace(&mut self.previous_outcomes, current);
        let completed = current.attempted.saturating_sub(previous.attempted);
        let failed = current.lost.saturating_sub(previous.lost);

        let loss = if completed > 0 {
            failed as f64 / completed as f64
        } else {
            0.0
        };
        let congested = completed >= MIN_AGGREGATE_SAMPLES && loss > LOSS_THRESHOLD;

        if congested {
            // Retreat from the standing limit, not from observed throughput. The
            // first event has no limit yet, so observed is the only number
            // available; after that, compounding on observed would ratchet toward
            // a standstill whether or not the retreat helped.
            let basis = self.global_limit.unwrap_or(attempted as f64 / seconds);
            let target = (basis * RETREAT).max(MIN_RATE_QPS).min(self.global_ceiling);
            self.global_limit = Some(target);
            self.global.set_rate(target);
            debug!(
                loss,
                completed, failed, target, "queries failing outright, lowering global rate"
            );
            return;
        }

        // No unrecoverable loss: ease back up, and stop limiting once the ceiling
        // is reached so a healthy path is not paced at all.
        let Some(limit) = self.global_limit else {
            return;
        };
        let grown = limit * GROWTH;
        if grown >= self.global_ceiling {
            self.global_limit = None;
            self.global.set_rate(self.global_ceiling);
            debug!("global rate recovered to the configured ceiling");
        } else {
            self.global_limit = Some(grown);
            self.global.set_rate(grown);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BlastDNSConfig;
    use std::net::SocketAddr;

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, n], 53))
    }

    fn pool_of(count: usize) -> Arc<ResolverPool> {
        let config = BlastDNSConfig {
            purgatory_threshold: 0,
            ..Default::default()
        };
        let addrs: Vec<SocketAddr> = (0..count)
            .map(|i| SocketAddr::from(([127, 0, (i / 256) as u8, (i % 256) as u8], 53)))
            .collect();
        Arc::new(ResolverPool::new(&addrs, &config))
    }

    fn pool(count: u8) -> Arc<ResolverPool> {
        let config = BlastDNSConfig {
            purgatory_threshold: 0,
            ..Default::default()
        };
        let addrs: Vec<SocketAddr> = (1..=count).map(addr).collect();
        Arc::new(ResolverPool::new(&addrs, &config))
    }

    fn controller(
        count: usize,
        ceiling: f64,
    ) -> (AdaptiveController, Arc<RateLimiter>, Arc<Outcomes>) {
        let global = Arc::new(RateLimiter::new(ceiling));
        let outcomes = Arc::new(Outcomes::default());
        let c = AdaptiveController::new(count, global.clone(), ceiling, outcomes.clone());
        (c, global, outcomes)
    }

    /// Record `completed` finished queries, `failed` of which returned nothing
    /// even after retries.
    fn finish(outcomes: &Outcomes, completed: u64, failed: u64) {
        outcomes.completed.fetch_add(completed, Ordering::Relaxed);
        outcomes.failed.fetch_add(failed, Ordering::Relaxed);
    }

    /// Drive `attempted` queries through a resolver, `lost` of them failing.
    fn drive(resolver: &Arc<ResolverHealth>, attempted: u64, lost: u64) {
        for i in 0..attempted {
            resolver.record_dispatch();
            if i < lost {
                resolver.record_error(true);
            } else {
                resolver.record_response(Duration::from_millis(10), true);
            }
        }
    }

    #[tokio::test]
    async fn clean_traffic_leaves_pacing_unlimited() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);

        drive(&pool.resolvers()[0], 100, 0);
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert_eq!(
            pool.resolvers()[0].rate(),
            UNLIMITED_QPS,
            "no loss means no throttling"
        );
    }

    #[tokio::test]
    async fn loss_retreats_to_the_configured_fraction_of_the_edge() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);

        // 200 queries in one second with 10% loss: the edge is 200 QPS.
        drive(&pool.resolvers()[0], 200, 20);
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        let rate = pool.resolvers()[0].rate();
        let expected = RETREAT * 200.0;
        assert!(
            (rate - expected).abs() / expected < 0.05,
            "expected ~{expected} QPS (85% of the 200 QPS edge), got {rate}"
        );
    }

    #[tokio::test]
    async fn sustained_loss_ratchets_down() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];

        drive(resolver, 200, 20);
        c.tick(&pool, Duration::from_secs(1), Instant::now());
        let first = resolver.rate();

        // Loss continues at a lower delivered rate, so the edge moves down too.
        drive(resolver, 100, 20);
        c.tick(&pool, Duration::from_secs(1), Instant::now());
        let second = resolver.rate();

        assert!(
            second < first,
            "continued loss should lower the rate further: {first} then {second}"
        );
    }

    #[tokio::test]
    async fn a_single_bad_resolver_does_not_cut_the_global_rate() {
        let pool = pool(5);
        let (mut c, global, _outcomes) = controller(5, UNLIMITED_QPS);

        // One resolver is losing badly, four are clean.
        drive(&pool.resolvers()[0], 100, 50);
        for r in &pool.resolvers()[1..] {
            drive(r, 100, 0);
        }
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert!(
            pool.resolvers()[0].rate() < UNLIMITED_QPS,
            "the bad resolver should be throttled"
        );
        assert_eq!(
            global.rate(),
            UNLIMITED_QPS,
            "one bad resolver in five must not cut the global rate"
        );
    }

    #[tokio::test]
    async fn a_mostly_dead_pool_does_not_read_as_congestion() {
        // The common case for a large public resolver list: most entries never
        // worked. That is many broken resolvers, not us sending too fast, and
        // retries are exactly what finds the live ones.
        let pool = pool(10);
        let (mut c, global, _outcomes) = controller(10, UNLIMITED_QPS);

        for _ in 0..3 {
            for r in &pool.resolvers()[..8] {
                drive(r, 100, 100);
            }
            for r in &pool.resolvers()[8..] {
                drive(r, 100, 0);
            }
            c.tick(&pool, Duration::from_secs(1), Instant::now());
        }

        assert_eq!(
            global.rate(),
            UNLIMITED_QPS,
            "resolvers that never worked must not cut the global rate"
        );
    }

    #[tokio::test]
    async fn recovered_failures_do_not_touch_the_global_rate() {
        // The regression that matters most. A public resolver pool refuses a few
        // percent of attempts as a matter of course, and a retry elsewhere answers
        // them. Counting those as congestion throttled a healthy path down to a
        // sixth of its throughput while losing nothing.
        let pool = pool(5);
        let (mut c, global, outcomes) = controller(5, UNLIMITED_QPS);

        for _ in 0..10 {
            // Heavy per-attempt loss, but every query ultimately answered.
            for r in pool.resolvers() {
                drive(r, 100, 30);
            }
            finish(&outcomes, 500, 0);
            c.tick(&pool, Duration::from_millis(500), Instant::now());
        }

        assert_eq!(
            global.rate(),
            UNLIMITED_QPS,
            "attempt failures that retries recovered from must not pace the pool"
        );
    }

    #[tokio::test]
    async fn queries_failing_outright_lower_the_global_rate() {
        let pool = pool(5);
        let (mut c, global, outcomes) = controller(5, UNLIMITED_QPS);

        for r in pool.resolvers() {
            drive(r, 100, 0);
        }
        finish(&outcomes, 500, 40); // 8% could not be answered at all
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert!(
            global.rate() < UNLIMITED_QPS,
            "unrecoverable loss is real evidence about the path"
        );
    }

    #[tokio::test]
    async fn the_rate_recovers_once_loss_clears() {
        // Sustained congestion lowers the rate, but it has to come back, or one
        // bad patch would hobble the rest of a long scan.
        let pool = pool(5);
        let (mut c, global, outcomes) = controller(5, UNLIMITED_QPS);

        for _ in 0..3 {
            for r in pool.resolvers() {
                drive(r, 100, 0);
            }
            finish(&outcomes, 500, 40);
            c.tick(&pool, Duration::from_secs(1), Instant::now());
        }
        let throttled = global.rate();
        assert!(throttled < UNLIMITED_QPS, "should have been paced");

        // Clean ticks: the limit climbs 25% each tick and eventually stops
        // limiting. Reaching a real-world rate takes a few ticks; dropping the
        // limiter entirely takes ~33s, which is why this loops a while.
        for _ in 0..80 {
            for r in pool.resolvers() {
                drive(r, 100, 0);
            }
            finish(&outcomes, 500, 0);
            c.tick(&pool, Duration::from_secs(1), Instant::now());
        }

        assert_eq!(
            global.rate(),
            UNLIMITED_QPS,
            "a healthy path must end up unpaced, not merely faster"
        );
    }

    #[tokio::test]
    async fn brute_force_scale_is_still_judged() {
        // The case the controller previously could not see at all: thousands of
        // resolvers each carrying a trickle, so no per-resolver ratio exists. The
        // global signal comes from query outcomes, which do not depend on any one
        // resolver being individually measurable.
        let pool = pool_of(2000);
        let (mut c, global, outcomes) = controller(2000, UNLIMITED_QPS);
        let tick = Duration::from_millis(500);

        for r in pool.resolvers() {
            drive(r, 1, 0);
        }
        finish(&outcomes, 2000, 0);
        c.tick(&pool, tick, Instant::now());
        assert_eq!(global.rate(), UNLIMITED_QPS, "clean trickle, no throttling");

        for (i, r) in pool.resolvers().iter().enumerate() {
            drive(r, 1, if i % 10 == 0 { 1 } else { 0 });
        }
        finish(&outcomes, 2000, 200);
        c.tick(&pool, tick, Instant::now());

        assert!(
            global.rate() < UNLIMITED_QPS,
            "unrecoverable loss must be actionable even when no resolver is individually measurable"
        );
    }

    #[tokio::test]
    async fn a_configured_ceiling_is_never_exceeded() {
        let pool = pool(5);
        let ceiling = 50.0;
        let (mut c, global, _outcomes) = controller(5, ceiling);

        // Correlated loss at a delivered rate far above the ceiling.
        for r in pool.resolvers() {
            drive(r, 400, 100);
        }
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert!(
            global.rate() <= ceiling,
            "controller must never raise the rate above the configured cap, got {}",
            global.rate()
        );
    }

    #[tokio::test]
    async fn an_expired_edge_releases_the_throttle() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];
        let start = Instant::now();

        drive(resolver, 200, 20);
        c.tick(&pool, Duration::from_secs(1), start);
        assert!(resolver.rate() < UNLIMITED_QPS, "should be throttled");

        // Quiet period longer than the edge lifetime.
        let later = start + EDGE_TTL + Duration::from_secs(1);
        c.tick(&pool, Duration::from_secs(1), later);

        assert_eq!(
            resolver.rate(),
            UNLIMITED_QPS,
            "a stale edge must be forgotten so the controller probes upward again"
        );
    }

    #[tokio::test]
    async fn recovery_climbs_back_toward_the_target() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];
        let now = Instant::now();

        drive(resolver, 200, 20);
        c.tick(&pool, Duration::from_secs(1), now);
        let target = resolver.rate();

        // Push it below the target, then let clean ticks bring it back.
        resolver.set_rate(target / 4.0);
        for _ in 0..5 {
            drive(resolver, 100, 0);
            c.tick(&pool, Duration::from_secs(1), now);
        }

        let recovered = resolver.rate();
        assert!(
            recovered > target / 4.0 && recovered <= target * 1.01,
            "should climb back toward {target} without overshooting, got {recovered}"
        );
    }

    /// End-to-end: run real queries at a resolver that drops above a known
    /// capacity and confirm the controller finds that edge and settles below it,
    /// with loss falling as a result.
    #[tokio::test]
    async fn converges_below_a_real_resolver_capacity() {
        use crate::sim::{SimConfig, SimResolver};
        use crate::{BlastDNSClient, DnsResolver};
        use futures::StreamExt;
        use hickory_client::proto::rr::RecordType;

        const CAPACITY: f64 = 100.0;

        let sim = SimResolver::start(SimConfig {
            latency: Duration::from_millis(2),
            capacity_qps: Some(CAPACITY),
            drop_one_in: None,
            refuse_one_in: None,
            truncate_udp: false,
        })
        .await;

        let client = Arc::new(
            BlastDNSClient::with_config(
                vec![sim.addr()],
                BlastDNSConfig {
                    max_concurrency: 64,
                    max_inflight_per_resolver: 32,
                    request_timeout: Duration::from_millis(200),
                    max_retries: 0,
                    cache_capacity: 0,
                    purgatory_threshold: 0,
                    adaptive: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        );

        // Push hard for long enough that the controller sees several ticks.
        let hosts: Vec<String> = (0..4000).map(|i| format!("h{i}.example.com.")).collect();
        let mut stream = client.clone().resolve_batch_full(
            hosts.into_iter().map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            false,
        );

        let deadline = Instant::now() + Duration::from_secs(4);
        let mut early_loss = None;
        while Instant::now() < deadline {
            if stream.next().await.is_none() {
                break;
            }
            if early_loss.is_none() && sim.received() > 400 {
                early_loss = Some(loss_ratio(&sim));
            }
        }

        let rate = client.stats()[0]
            .rate_qps
            .expect("controller should have set a pacing rate");
        assert!(
            rate < CAPACITY,
            "controller should hold below the {CAPACITY} QPS edge, got {rate}"
        );
        assert!(
            rate > CAPACITY * 0.3,
            "controller should not collapse the rate, got {rate}"
        );

        // Adaptation should have reduced loss relative to the unthrottled start.
        if let Some(early) = early_loss {
            let overall = loss_ratio(&sim);
            assert!(
                overall <= early + 0.02,
                "loss should not grow after adapting: {early} early vs {overall} overall"
            );
        }
    }

    fn loss_ratio(sim: &crate::sim::SimResolver) -> f64 {
        let received = sim.received().max(1) as f64;
        sim.dropped() as f64 / received
    }

    #[tokio::test]
    async fn small_samples_are_not_treated_as_signal() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);

        // Below MIN_SAMPLES, even total loss must not move the rate.
        drive(&pool.resolvers()[0], 5, 5);
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert_eq!(
            pool.resolvers()[0].rate(),
            UNLIMITED_QPS,
            "a handful of failures is not enough to conclude anything"
        );
    }

    /// The per-resolver in-flight cap holds a resolver to a few queries per tick,
    /// so requiring MIN_SAMPLES within a single tick never judges one at all. The
    /// samples have to carry across ticks.
    #[tokio::test]
    async fn a_trickle_accumulates_until_it_is_judged() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];
        let tick = Duration::from_millis(500);

        // 5 queries per tick with one failing: 20% loss, far above the threshold,
        // but never 20 samples in any single tick.
        for _ in 0..3 {
            drive(resolver, 5, 1);
            c.tick(&pool, tick, Instant::now());
            assert_eq!(
                resolver.rate(),
                UNLIMITED_QPS,
                "must not act before there are enough samples to trust the ratio"
            );
        }

        drive(resolver, 5, 1);
        c.tick(&pool, tick, Instant::now());

        // 20 queries over four 500ms ticks is 10 QPS, so that is the edge.
        let expected = RETREAT * 10.0;
        let rate = resolver.rate();
        assert!(
            (rate - expected).abs() / expected < 0.05,
            "expected ~{expected} QPS (85% of the 10 QPS edge), got {rate}"
        );
    }

    /// A resolver too lightly loaded to reach MIN_SAMPLES inside the window is left
    /// alone rather than judged on samples spanning many seconds of conditions.
    #[tokio::test]
    async fn a_window_that_never_fills_is_discarded_rather_than_judged() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];

        // One failing query per second, so the window expires before it ever fills.
        for _ in 0..40 {
            drive(resolver, 1, 1);
            c.tick(&pool, Duration::from_secs(1), Instant::now());
        }

        assert_eq!(
            resolver.rate(),
            UNLIMITED_QPS,
            "traffic this light is not evidence we are overloading anything"
        );
    }
}
