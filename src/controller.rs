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
//! The two signals need different amounts of evidence. A per-resolver loss ratio
//! needs enough queries against that one resolver, which only happens with a
//! small pool; spread a few hundred queries per second over thousands of
//! resolvers and no single one is ever measurable. The aggregate is well sampled
//! either way, so it is what makes this useful at brute-force scale.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::time::Instant;
use tracing::debug;

use crate::{
    health::{ResolverHealth, ResolverPool},
    limiter::{RateLimiter, UNLIMITED_QPS},
};

/// Fraction of a discovered edge to operate at.
const RETREAT: f64 = 0.85;
/// Per-tick multiplier when climbing back toward the target.
const GROWTH: f64 = 1.25;
/// Loss fraction above which a resolver counts as degraded.
const LOSS_THRESHOLD: f64 = 0.02;
/// Queries against one resolver in a tick before its own loss ratio is trusted.
const MIN_SAMPLES: u64 = 20;
/// Queries across all proven resolvers in a tick before the aggregate is trusted.
const MIN_AGGREGATE_SAMPLES: u64 = 100;
/// Largest share of the tick's losses one resolver may account for and still have
/// the loss treated as a path problem. Above this it is that resolver's problem,
/// and its own throttle is the right response.
const LOSS_CONCENTRATION_LIMIT: f64 = 0.5;
/// Aggregate loss above which retries are suppressed as well as the rate cut.
///
/// Deliberately far above [`LOSS_THRESHOLD`]: retries are what keep a lossy path
/// from silently dropping results, so giving them up needs stronger evidence than
/// merely slowing down does.
const SUPPRESS_RETRIES_LOSS: f64 = 0.25;
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
    suppress_retries: Arc<AtomicBool>,
    per_resolver: Vec<Observation>,
    edges: Vec<Option<Edge>>,
    global_edge: Option<Edge>,
}

impl AdaptiveController {
    pub(crate) fn new(
        resolver_count: usize,
        global: Arc<RateLimiter>,
        global_ceiling: f64,
        suppress_retries: Arc<AtomicBool>,
    ) -> Self {
        Self {
            global,
            global_ceiling,
            suppress_retries,
            per_resolver: vec![Observation::default(); resolver_count],
            edges: (0..resolver_count).map(|_| None).collect(),
            global_edge: None,
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
        // Aggregate only over resolvers that have ever returned a response.
        // "Ever" rather than "recently": a dead list entry never qualifies, and a
        // working one does not need to re-prove itself every tick to be counted.
        let mut proven_attempted = 0u64;
        let mut proven_lost = 0u64;
        let mut worst_resolver_lost = 0u64;

        for (i, resolver) in resolvers.iter().enumerate() {
            let stats = resolver.stats();
            let current = Observation {
                attempted: stats.attempted,
                lost: stats.timeout + stats.error,
            };
            let previous = std::mem::replace(&mut self.per_resolver[i], current);

            let attempted = current.attempted.saturating_sub(previous.attempted);
            let lost = current.lost.saturating_sub(previous.lost);
            attempted_total += attempted;

            if stats.answered + stats.empty > 0 {
                proven_attempted += attempted;
                proven_lost += lost;
                worst_resolver_lost = worst_resolver_lost.max(lost);
            }

            // Throttling one resolver needs that resolver's own loss ratio, which
            // needs enough queries against it. Let any stale edge expire
            // regardless, so an idle resolver does not stay throttled forever.
            if attempted < MIN_SAMPLES {
                self.relax(i, now, resolver);
                continue;
            }

            let loss = lost as f64 / attempted as f64;
            if loss > LOSS_THRESHOLD {
                let observed_qps = attempted as f64 / seconds;
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

        self.adjust_global(
            now,
            seconds,
            attempted_total,
            proven_attempted,
            proven_lost,
            worst_resolver_lost,
        );
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

    fn adjust_global(
        &mut self,
        now: Instant,
        seconds: f64,
        attempted: u64,
        proven_attempted: u64,
        proven_lost: u64,
        worst_resolver_lost: u64,
    ) {
        let aggregate_loss = if proven_attempted > 0 {
            proven_lost as f64 / proven_attempted as f64
        } else {
            0.0
        };
        // Loss piled up on one resolver is that resolver misbehaving, and its own
        // throttle already answers it. Only loss spread across the pool is
        // evidence about the path we share.
        let concentration = if proven_lost > 0 {
            worst_resolver_lost as f64 / proven_lost as f64
        } else {
            0.0
        };
        let congested = proven_attempted >= MIN_AGGREGATE_SAMPLES
            && aggregate_loss > LOSS_THRESHOLD
            && concentration < LOSS_CONCENTRATION_LIMIT;

        if congested {
            let observed_qps = attempted as f64 / seconds;
            let target = (RETREAT * observed_qps)
                .max(MIN_RATE_QPS)
                .min(self.global_ceiling);
            self.global_edge = Some(Edge {
                qps: observed_qps,
                seen_at: now,
            });
            self.global.set_rate(target);
            // Only give up retries when the path is bad enough that retrying
            // cannot plausibly help; below that, losing results is the worse
            // outcome.
            let starved = aggregate_loss > SUPPRESS_RETRIES_LOSS;
            self.suppress_retries.store(starved, Ordering::Relaxed);
            debug!(
                aggregate_loss,
                concentration,
                proven_attempted,
                observed_qps,
                target,
                starved,
                "aggregate loss across proven resolvers, cutting global rate"
            );
            return;
        }

        self.suppress_retries.store(false, Ordering::Relaxed);

        let Some(edge) = &self.global_edge else {
            return;
        };
        if now.duration_since(edge.seen_at) >= EDGE_TTL {
            self.global_edge = None;
            self.global.set_rate(self.global_ceiling);
            debug!("global edge expired, restoring configured ceiling");
            return;
        }
        let target = (RETREAT * edge.qps)
            .max(MIN_RATE_QPS)
            .min(self.global_ceiling);
        let current = self.global.rate();
        if current < target {
            self.global.set_rate((current * GROWTH).min(target));
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
    ) -> (AdaptiveController, Arc<RateLimiter>, Arc<AtomicBool>) {
        let global = Arc::new(RateLimiter::new(ceiling));
        let suppress = Arc::new(AtomicBool::new(false));
        let c = AdaptiveController::new(count, global.clone(), ceiling, suppress.clone());
        (c, global, suppress)
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
        let (mut c, _global, _s) = controller(1, UNLIMITED_QPS);

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
        let (mut c, _global, _s) = controller(1, UNLIMITED_QPS);

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
        let (mut c, _global, _s) = controller(1, UNLIMITED_QPS);
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
        let (mut c, global, suppress) = controller(5, UNLIMITED_QPS);

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
        assert!(!suppress.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn a_mostly_dead_pool_does_not_read_as_congestion() {
        // The common case for a large public resolver list: most entries never
        // worked. That is many broken resolvers, not us sending too fast, and
        // retries are exactly what finds the live ones.
        let pool = pool(10);
        let (mut c, global, suppress) = controller(10, UNLIMITED_QPS);

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
        assert!(
            !suppress.load(Ordering::Relaxed),
            "retries must stay enabled when the pool is mostly dead"
        );
    }

    #[tokio::test]
    async fn loss_spread_across_proven_resolvers_cuts_the_global_rate() {
        // Loss spread across resolvers that have all answered before is evidence
        // about the shared path rather than about any one of them.
        let pool = pool(5);
        let (mut c, global, suppress) = controller(5, UNLIMITED_QPS);

        for r in pool.resolvers() {
            drive(r, 100, 0);
        }
        c.tick(&pool, Duration::from_secs(1), Instant::now());
        assert_eq!(global.rate(), UNLIMITED_QPS, "clean start, no throttling");

        // ~8% aggregate: enough to slow down, not enough to stop retrying.
        for r in pool.resolvers() {
            drive(r, 100, 10);
        }
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert!(
            global.rate() < UNLIMITED_QPS,
            "broad loss should cut the global rate"
        );
        assert!(
            !suppress.load(Ordering::Relaxed),
            "retries must survive moderate loss: dropping them loses results, \
             which is worse than being slow"
        );
    }

    #[tokio::test]
    async fn retries_are_only_abandoned_when_the_path_is_hopeless() {
        // Retries are what keep a lossy path from silently dropping results, so
        // they are given up only when loss is severe enough that retrying cannot
        // plausibly help.
        let pool = pool(5);
        let (mut c, global, suppress) = controller(5, UNLIMITED_QPS);

        for r in pool.resolvers() {
            drive(r, 100, 0);
        }
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        // ~48% aggregate, spread evenly.
        for r in pool.resolvers() {
            drive(r, 100, 60);
        }
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert!(global.rate() < UNLIMITED_QPS);
        assert!(
            suppress.load(Ordering::Relaxed),
            "at this loss level retrying only adds load"
        );
    }

    #[tokio::test]
    async fn brute_force_scale_is_still_judged() {
        // The case this controller previously could not see at all: a large pool
        // where every resolver carries a trickle. Spread ~290 qps over 2,000
        // resolvers and a 500ms tick gives each one well under one query, so no
        // per-resolver ratio exists. The aggregate still does.
        let pool = pool_of(2000);
        let (mut c, global, _s) = controller(2000, UNLIMITED_QPS);
        let tick = Duration::from_millis(500);

        // Establish that every resolver has answered at some point.
        for r in pool.resolvers() {
            drive(r, 1, 0);
        }
        c.tick(&pool, tick, Instant::now());
        assert_eq!(global.rate(), UNLIMITED_QPS, "clean trickle, no throttling");

        // Now a trickle with broad loss: one query each, 10% of them lost, so no
        // single resolver comes anywhere near MIN_SAMPLES.
        for (i, r) in pool.resolvers().iter().enumerate() {
            drive(r, 1, if i % 10 == 0 { 1 } else { 0 });
        }
        c.tick(&pool, tick, Instant::now());

        assert!(
            global.rate() < UNLIMITED_QPS,
            "aggregate loss must be actionable even when no resolver is individually measurable"
        );
    }

    #[tokio::test]
    async fn a_configured_ceiling_is_never_exceeded() {
        let pool = pool(5);
        let ceiling = 50.0;
        let (mut c, global, _s) = controller(5, ceiling);

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
        let (mut c, _global, _s) = controller(1, UNLIMITED_QPS);
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
        let (mut c, _global, _s) = controller(1, UNLIMITED_QPS);
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
        let (mut c, _global, _s) = controller(1, UNLIMITED_QPS);

        // Below MIN_SAMPLES, even total loss must not move the rate.
        drive(&pool.resolvers()[0], 5, 5);
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert_eq!(
            pool.resolvers()[0].rate(),
            UNLIMITED_QPS,
            "a handful of failures is not enough to conclude anything"
        );
    }
}
