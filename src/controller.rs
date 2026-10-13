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
//! Not all loss is the rate's doing: some names fail at any rate. A retreat is
//! only repeated while it visibly helps, and loss it cannot move becomes a
//! baseline that later loss is judged against.
//!
//! Retries are never given up, only paced. Abandoning them under load was tried
//! and measured: it took unanswered queries from 0% to 3.5% on a 5,000-name
//! brute-force, losing real results to save load that was never shown to be a
//! problem.
//!
//! The two signals need different amounts of evidence. A per-resolver loss ratio
//! needs enough queries against that one resolver, which only happens with a
//! small pool; spread a few hundred queries per second over thousands of
//! resolvers and no single one is ever measurable. The aggregate is well sampled
//! either way, so it is what makes this useful at brute-force scale.

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
/// Loss above the baseline at which a path counts as congested.
const LOSS_THRESHOLD: f64 = 0.02;
/// Queries against one resolver before its own loss ratio is trusted. Fewer is
/// mostly noise, and noise reads as a resolver that keeps starting to fail.
const MIN_SAMPLES: u64 = 100;
/// Request timeouts a resolver's window may stay open gathering `MIN_SAMPLES`.
/// A resolver that can't fill one is left to the aggregate, which keeps
/// brute-force scale (a trickle per resolver) out of per-resolver pacing.
const MAX_WINDOW_TIMEOUTS: f64 = 5.0;
/// Queries that must finish before the global failure rate is trusted. Gathered
/// over as many ticks as it takes, so a low rate is judged late rather than never.
const MIN_AGGREGATE_SAMPLES: u64 = 100;
/// How often limits are recomputed.
const TICK: Duration = Duration::from_millis(500);
/// How long a discovered edge is honored. Without expiry, one transient blip
/// would cap the rest of a long scan.
const EDGE_TTL: Duration = Duration::from_secs(30);
/// Floor on any pacing rate, so a bad patch cannot stall a scan outright.
const MIN_RATE_QPS: f64 = 1.0;
/// Loss must fall to this fraction of the best seen so far to justify retreating
/// again.
///
/// Backing off only helps when the rate is what caused the loss. A resolver
/// dropping a fixed share of queries loses just as much at any rate, and retreating
/// every tick would walk it to the floor without recovering a query. The margin
/// stays small because real convergence toward a capacity limit improves gradually.
const RETREAT_MUST_IMPROVE: f64 = 0.99;
/// Share of an episode's best answered rate a retreat must keep to count as
/// working. Falling loss alone can be noise or a changing workload; a saturated
/// path keeps answering at capacity when sent less, a fixed-share loss doesn't.
const RETREAT_MUST_DELIVER: f64 = 0.95;
/// Outright-failure share above which the global rate holds rather than grows:
/// a path answering almost nothing has no throughput to win back.
const MAX_BASELINE_LOSS: f64 = 0.5;
/// Consecutive windows without improvement before the loss becomes the baseline.
/// A retreat that cost answers needs only one.
///
/// One flat window is not proof: loss is a sampled ratio, and a noisy reading
/// during a genuine convergence would otherwise stop it short of the path's real
/// capacity. Requiring a couple in a row still bounds how far a rate can walk when
/// the loss truly is not ours to fix.
const STALL_TICKS: u8 = 2;

/// What a window of loss is worth doing about.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Verdict {
    /// Worth retreating from. `converging`: the retreats so far are working
    /// (never set on an episode's first window).
    Congested { converging: bool },
    /// Nothing to act on.
    Clear,
    /// Retreating stopped helping; this loss is now the baseline.
    Baseline,
}

/// Decides whether a window's loss is the rate's fault: congestion is loss above
/// a learned baseline, and a retreat continues only while loss falls and answers
/// hold up.
#[derive(Default, Clone)]
struct LossJudge {
    /// Lowest loss this episode.
    best: Option<f64>,
    /// Highest answered rate this episode.
    best_delivered: Option<f64>,
    /// Consecutive windows a retreat hasn't earned its keep.
    stalled: u8,
    /// Loss the path shows at any rate.
    baseline: Option<f64>,
}

impl LossJudge {
    /// `delivered` is answered queries per second over the same window as `loss`.
    fn judge(&mut self, loss: f64, delivered: f64) -> Verdict {
        // The baseline follows the loss down, so a bar set in a bad patch can't hide
        // later congestion; only a fresh stall raises it.
        if let Some(baseline) = self.baseline {
            self.baseline = Some(baseline.min(loss));
        }

        // Congestion is loss above the baseline, not above zero.
        if loss <= self.baseline.unwrap_or(0.0) + LOSS_THRESHOLD {
            self.end_episode();
            return Verdict::Clear;
        }

        // The first sight of loss earns a retreat; after that, loss must keep
        // falling and answers must hold up.
        let improving = match self.best {
            Some(best) => loss < best * RETREAT_MUST_IMPROVE,
            None => true,
        };
        let paying = match self.best_delivered {
            Some(best) => delivered >= best * RETREAT_MUST_DELIVER,
            None => true,
        };
        let converging = self.best.is_some() && improving && paying;
        if improving && paying {
            self.stalled = 0;
        } else {
            self.stalled = self.stalled.saturating_add(1);
            // A retreat that cost answers is settled at once; loss that merely
            // failed to improve gets another window.
            if !paying || self.stalled >= STALL_TICKS {
                self.baseline = Some(loss);
                self.end_episode();
                return Verdict::Baseline;
            }
        }

        self.best = Some(match self.best {
            Some(best) => best.min(loss),
            None => loss,
        });
        self.best_delivered = Some(match self.best_delivered {
            Some(best) => best.max(delivered),
            None => delivered,
        });
        Verdict::Congested { converging }
    }

    /// Start the next episode fresh. The baseline stays: it belongs to the path.
    fn end_episode(&mut self) {
        self.best = None;
        self.best_delivered = None;
        self.stalled = 0;
    }
}

#[derive(Default, Clone, Copy)]
struct Observation {
    attempted: u64,
    lost: u64,
    /// Counted when the answer arrives; `attempted - lost` lags, since a loss lands
    /// a timeout after its dispatch.
    responded: u64,
}

/// A rate at which loss was observed, and when it was seen.
#[derive(Clone, Copy)]
struct Edge {
    qps: f64,
    seen_at: Instant,
}

/// Watches per-resolver loss and adjusts pacing to stay below the loss edge.
pub(crate) struct AdaptiveController {
    global: Arc<RateLimiter>,
    /// Configured hard cap. `UNLIMITED_QPS` when none was set.
    global_ceiling: f64,
    per_resolver: Vec<Observation>,
    edges: Vec<Option<Edge>>,
    /// Whether a resolver's last retreat actually lowered its pacing.
    stepped: Vec<bool>,
    /// Edge and rate from before a resolver's last retreat, to undo it if it fails.
    undo: Vec<Option<(Option<Edge>, f64)>>,
    /// Per-resolver verdict on the loss it is showing.
    judge: Vec<LossJudge>,
    /// Loss accumulated per resolver since its window opened, and how long that
    /// window has been open. The window has to outlast the request timeout for the
    /// ratio to compare the same period's dispatches and losses.
    window: Vec<Observation>,
    window_seconds: Vec<f64>,
    /// Queries finished and lost, as of the previous tick.
    outcomes: Arc<Outcomes>,
    previous_outcomes: Observation,
    /// Queries finished and lost since the global window opened, the attempts
    /// dispatched over the same period, and how long it has been open. A tick too
    /// small to judge is carried into the next one rather than thrown away.
    global_window: Observation,
    global_window_dispatched: u64,
    global_window_seconds: f64,
    /// Verdict on the aggregate loss across every resolver that answered.
    global_judge: LossJudge,
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
            per_resolver: vec![Observation::default(); resolver_count],
            edges: (0..resolver_count).map(|_| None).collect(),
            stepped: vec![false; resolver_count],
            undo: vec![None; resolver_count],
            judge: vec![LossJudge::default(); resolver_count],
            window: vec![Observation::default(); resolver_count],
            window_seconds: vec![0.0; resolver_count],
            outcomes,
            previous_outcomes: Observation::default(),
            global_window: Observation::default(),
            global_window_dispatched: 0,
            global_window_seconds: 0.0,
            global_judge: LossJudge::default(),
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
                responded: stats.answered + stats.empty,
            };
            let previous = std::mem::replace(&mut self.per_resolver[i], current);

            let tick_attempted = current.attempted.saturating_sub(previous.attempted);
            let tick_lost = current.lost.saturating_sub(previous.lost);
            attempted_total += tick_attempted;

            // A query becomes "lost" only when its timeout expires, so losses land
            // later than the dispatches that produced them. Measuring over a window
            // shorter than the timeout puts the two in different windows: cut the
            // rate and the next window counts the old rate's losses against the new
            // rate's dispatches, which has been seen to read over 100% loss and to
            // turn one retreat into a slide. Accumulate until the window is at least
            // as long as the timeout, then judge once and start over.
            self.window[i].attempted += tick_attempted;
            self.window[i].lost += tick_lost;
            self.window[i].responded += current.responded.saturating_sub(previous.responded);
            self.window_seconds[i] += seconds;

            // Gather until the window is long and full enough to judge, or shows
            // the resolver can't be measured on its own. Relax meanwhile so an idle
            // resolver doesn't stay throttled.
            let timeout = resolver.request_timeout().as_secs_f64();
            if self.window_seconds[i] < timeout
                || (self.window[i].attempted < MIN_SAMPLES
                    && self.window_seconds[i] < timeout * MAX_WINDOW_TIMEOUTS)
            {
                self.relax(i, now, resolver);
                continue;
            }
            let dispatched = self.window[i].attempted;
            let lost = self.window[i].lost;
            let responded = self.window[i].responded;
            let window_seconds = self.window_seconds[i];
            self.window[i] = Observation::default();
            self.window_seconds[i] = 0.0;

            // Too few queries to judge this resolver alone: leave it to the
            // aggregate.
            if dispatched < MIN_SAMPLES {
                self.relax(i, now, resolver);
                continue;
            }

            // Clamped because a window whose rate was just cut can still be
            // absorbing losses from the busier period before it, which reads as more
            // than everything it sent.
            let loss = (lost as f64 / dispatched as f64).min(1.0);
            let delivered = responded as f64 / window_seconds.max(1e-6);
            let converging = match self.judge[i].judge(loss, delivered) {
                Verdict::Clear => {
                    self.stepped[i] = false;
                    self.relax(i, now, resolver);
                    continue;
                }
                Verdict::Baseline => {
                    // The retreat bought nothing: undo it. Earlier retreats passed
                    // their own tests and stand.
                    if std::mem::take(&mut self.stepped[i])
                        && let Some((edge, rate)) = self.undo[i].take()
                    {
                        self.edges[i] = edge;
                        resolver.set_rate(rate);
                    }
                    debug!(
                        resolver = %resolver.addr(),
                        loss, "loss is not responding to retreat, taking it as the resolver's cost"
                    );
                    continue;
                }
                Verdict::Congested { converging } => converging,
            };

            // Step down from this resolver's own rate, never to observed
            // throughput, which every other limit on the path also caps. Only the
            // first edge comes from observation.
            let observed_qps = dispatched as f64 / window_seconds.max(1e-6);
            let rate = resolver.rate();
            // A window only shows whether this resolver's pacing helps if that
            // pacing shaped it: the last retreat really lowered the limit and
            // traffic hit it.
            let shaped = self.stepped[i] && observed_qps >= RETREAT * rate;
            let edge = match self.edges[i] {
                None => Edge {
                    qps: observed_qps,
                    seen_at: now,
                },
                // Working: the current rate still loses, so it becomes the edge.
                Some(edge) if converging && shaped => Edge {
                    qps: edge.qps.min(rate),
                    seen_at: now,
                },
                // Otherwise hold the edge, still aging toward expiry.
                Some(edge) => edge,
            };
            let edge_qps = edge.qps;
            let target = (RETREAT * edge_qps).max(MIN_RATE_QPS);
            // A real step, not the limiter's nanosecond rounding of the same rate.
            self.stepped[i] = target < rate * (1.0 + RETREAT) / 2.0;
            if self.stepped[i] {
                self.undo[i] = Some((self.edges[i], rate));
            }
            self.edges[i] = Some(edge);
            resolver.set_rate(target);
            debug!(
                resolver = %resolver.addr(),
                loss, observed_qps, edge_qps, target,
                "resolver losing queries, retreating below the edge"
            );
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
            // Forget what the episode achieved too: the next one gets judged on its
            // own, not against a reading from 30 seconds ago. The baseline stays.
            self.judge[i].end_episode();
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
            // Answered is `attempted - lost` here: both move as a query finishes.
            ..Observation::default()
        };
        let previous = std::mem::replace(&mut self.previous_outcomes, current);
        self.global_window.attempted += current.attempted.saturating_sub(previous.attempted);
        self.global_window.lost += current.lost.saturating_sub(previous.lost);
        self.global_window_dispatched += attempted;
        self.global_window_seconds += seconds;

        // Accumulate until enough queries have finished to trust the ratio, however
        // many ticks that takes. Discarding an under-sampled tick instead would
        // make the signal vanish exactly where it is needed most: the limit gates
        // throughput and throughput gates the sample count, so once the limit is
        // low enough that a tick finishes fewer than MIN_AGGREGATE_SAMPLES queries,
        // neither congestion nor recovery can ever be seen again and whatever rate
        // the retreat happened to land on becomes permanent. Judging over a longer
        // window only makes a slow path slower to react, which is the right trade.
        //
        // Unlike the per-resolver window this needs no minimum duration. Both
        // counters are incremented at the point a query finishes, so a shrinking
        // window cannot count an earlier rate's losses against a later rate's
        // dispatches the way per-resolver dispatch-versus-timeout counting can.
        if self.global_window.attempted < MIN_AGGREGATE_SAMPLES {
            return;
        }

        let completed = self.global_window.attempted;
        let failed = self.global_window.lost;
        let dispatched = self.global_window_dispatched;
        let window_seconds = self.global_window_seconds;
        self.global_window = Observation::default();
        self.global_window_dispatched = 0;
        self.global_window_seconds = 0.0;

        let loss = failed as f64 / completed as f64;

        let delivered = completed.saturating_sub(failed) as f64 / window_seconds.max(1e-6);
        let verdict = self.global_judge.judge(loss, delivered);
        if verdict == Verdict::Baseline {
            debug!(
                loss,
                "global loss is not responding to retreat, taking it as the path's cost"
            );
        }

        if matches!(verdict, Verdict::Congested { .. }) {
            // Retreat from the standing limit, not from observed throughput. The
            // first event has no limit yet, so observed is the only number
            // available; after that, compounding on observed would ratchet toward
            // a standstill whether or not the retreat helped.
            let basis = self
                .global_limit
                .unwrap_or(dispatched as f64 / window_seconds.max(1e-6));
            let target = (basis * RETREAT).max(MIN_RATE_QPS).min(self.global_ceiling);
            self.global_limit = Some(target);
            self.global.set_rate(target);
            debug!(
                loss,
                completed, failed, target, "queries failing outright, lowering global rate"
            );
            return;
        }

        // Almost nothing is getting through: hold rather than grow into it. The
        // first window after recovery releases it.
        if loss > MAX_BASELINE_LOSS {
            return;
        }

        let Some(limit) = self.global_limit else {
            return;
        };

        // Queries are finishing and not failing: ease back up, and stop limiting
        // once the ceiling is reached so a healthy path is not paced at all.
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

    /// Retreating is only worth repeating while the loss is responding to it.
    #[tokio::test]
    async fn retreat_continues_only_while_loss_is_falling() {
        let pool = pool(2);
        let (mut c, _global, _outcomes) = controller(2, UNLIMITED_QPS);
        let falling = &pool.resolvers()[0];
        let stuck = &pool.resolvers()[1];

        // Both start at 25% loss and earn their first retreat.
        for r in pool.resolvers() {
            drive(r, 200, 50);
        }
        c.tick(&pool, Duration::from_secs(1), Instant::now());
        let (falling_first, stuck_first) = (falling.rate(), stuck.rate());
        assert!(falling_first < UNLIMITED_QPS && stuck_first < UNLIMITED_QPS);

        // One resolver is saturated at 150 answers a second: sent less, it loses
        // less but answers as much. The other loses the same share of anything it
        // sends.
        for (attempted, lost) in [(170, 20), (150, 6), (140, 2)] {
            drive(falling, attempted, lost);
            drive(stuck, 200, 50);
            c.tick(&pool, Duration::from_secs(1), Instant::now());
        }

        assert!(
            falling.rate() < falling_first,
            "loss that responds to retreat should draw further retreat: \
             {falling_first} then {}",
            falling.rate()
        );
        assert_eq!(
            stuck.rate(),
            stuck_first,
            "loss that ignores retreat must hold, not walk the rate to the floor"
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

    /// A tick that finished too few queries to judge must hold the limit. Growing
    /// on it treats a quiet moment as proof of health, so bursty traffic over a
    /// failing path drifts the limit upward between congested ticks instead of
    /// converging down.
    #[tokio::test]
    async fn quiet_ticks_do_not_raise_the_limit_on_a_failing_path() {
        let pool = pool(1);
        let (mut c, global, outcomes) = controller(1, UNLIMITED_QPS);
        let tick = Duration::from_secs(1);

        drive(&pool.resolvers()[0], 500, 0);
        finish(&outcomes, 500, 500);
        c.tick(&pool, tick, Instant::now());
        let first = global.rate();
        assert!(
            first < UNLIMITED_QPS,
            "total loss should have paced the path"
        );

        // Quiet ticks: a trickle finishes, still all failing, but too few to judge.
        for _ in 0..5 {
            drive(&pool.resolvers()[0], 10, 0);
            finish(&outcomes, 10, 10);
            c.tick(&pool, tick, Instant::now());
        }

        assert!(
            global.rate() <= first,
            "a failing path must not drift upward on under-sampled ticks: \
             {first} then {}",
            global.rate()
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
    async fn an_under_sampled_tick_is_carried_rather_than_discarded() {
        // No single tick finishes MIN_AGGREGATE_SAMPLES queries, so none can be
        // judged on its own. The same queries accumulated across ticks are plenty,
        // and a client paced this slowly is exactly the one that needs judging.
        let pool = pool(1);
        let (mut c, global, outcomes) = controller(1, UNLIMITED_QPS);

        for _ in 0..4 {
            drive(&pool.resolvers()[0], 30, 0);
            finish(&outcomes, 30, 3);
            c.tick(&pool, Duration::from_millis(500), Instant::now());
        }

        assert!(
            global.rate() < UNLIMITED_QPS,
            "120 finished queries at 10% loss must be judged, even 30 ticks at a time"
        );
    }

    #[tokio::test]
    async fn a_throttled_path_is_not_latched_there() {
        // Drives the loop the way it actually runs: the queries that finish in a
        // tick are bounded by the rate the controller set on the previous one.
        // Under that feedback the retreat used to compound until a tick finished
        // too few queries to sample, at which point congestion and recovery both
        // became invisible and the rate stayed pinned for the life of the client.
        //
        // Capacity congestion, because only loss retreating can fix drives it that
        // deep; at this capacity a tick finishes well under MIN_AGGREGATE_SAMPLES.
        const CAPACITY: f64 = 40.0;
        let pool = pool(5);
        let ceiling = 5_000.0;
        let (mut c, global, outcomes) = controller(5, ceiling);
        let tick = Duration::from_millis(500);
        let seconds = tick.as_secs_f64();

        let step = |c: &mut AdaptiveController, capacity: Option<f64>| {
            let finished = (global.rate().min(ceiling) * seconds) as u64;
            for r in pool.resolvers() {
                drive(r, finished / 5, 0);
            }
            let failed = match capacity {
                Some(capacity) => finished.saturating_sub((capacity * seconds) as u64),
                None => 0,
            };
            finish(&outcomes, finished, failed);
            c.tick(&pool, tick, Instant::now());
        };

        // Sixty ticks over capacity: more than the twenty it takes to compound 15%
        // retreats below the old sampling floor.
        for _ in 0..60 {
            step(&mut c, Some(CAPACITY));
        }
        let throttled = global.rate();
        assert!(
            throttled < ceiling,
            "a path losing everything over {CAPACITY} qps should have paced the client down"
        );

        // The path is clean from here on, and the client has to find that out.
        for _ in 0..400 {
            step(&mut c, None);
        }

        assert_eq!(
            global.rate(),
            ceiling,
            "a healthy path must climb back to the ceiling, not stay pinned at {throttled}"
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

    /// End-to-end against loss that no rate can fix: the resolver drops a fixed
    /// share of queries regardless of how fast it is asked.
    ///
    /// Retreating cannot recover those, so the controller has to stop retreating.
    /// Without that, the rate walks to the floor and a workload that would finish
    /// in seconds never finishes at all.
    #[tokio::test]
    async fn rate_independent_loss_does_not_walk_the_rate_to_the_floor() {
        use crate::sim::{SimConfig, SimResolver};
        use crate::{BlastDNSClient, DnsResolver};
        use futures::StreamExt;
        use hickory_client::proto::rr::RecordType;

        let sim = SimResolver::start(SimConfig {
            // Enough latency that the run spans many control ticks, which is what
            // gives a decaying rate the chance to matter.
            latency: Duration::from_millis(20),
            // One in eight is dropped no matter the rate.
            drop_one_in: Some(8),
            ..Default::default()
        })
        .await;

        let client = Arc::new(
            BlastDNSClient::with_config(
                vec![sim.addr()],
                BlastDNSConfig {
                    max_concurrency: 8,
                    max_inflight_per_resolver: 8,
                    request_timeout: Duration::from_millis(200),
                    max_retries: 3,
                    cache_capacity: 0,
                    purgatory_threshold: 0,
                    adaptive: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        );

        let hosts: Vec<String> = (0..3000).map(|i| format!("h{i}.example.com.")).collect();
        let mut stream = client.clone().resolve_batch_full(
            hosts.into_iter().map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            false,
        );

        let deadline = Instant::now() + Duration::from_secs(25);
        let mut delivered = 0usize;
        while Instant::now() < deadline {
            match stream.next().await {
                Some(_) => delivered += 1,
                None => break,
            }
        }

        assert!(
            delivered >= 3000,
            "should have finished inside the deadline, delivered {delivered}/3000"
        );
        if let Some(rate) = client.stats()[0].rate_qps {
            assert!(
                rate > MIN_RATE_QPS * 5.0,
                "rate should hold well above the floor, got {rate}"
            );
        }
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

        // Push hard for long enough that the controller sees several windows (about
        // a second each at this capacity).
        let hosts: Vec<String> = (0..12000).map(|i| format!("h{i}.example.com.")).collect();
        let mut stream = client.clone().resolve_batch_full(
            hosts.into_iter().map(Ok::<_, std::convert::Infallible>),
            RecordType::A,
            false,
            false,
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut early_loss = None;
        while Instant::now() < deadline {
            if stream.next().await.is_none() {
                break;
            }
            if early_loss.is_none() && sim.received() > 400 {
                early_loss = Some(loss_ratio(&sim));
            }
        }

        // With one resolver, whichever limit pulls ahead shapes traffic and the
        // other stops; check the pace actually in force.
        let rate = client.stats()[0]
            .rate_qps
            .into_iter()
            .chain(client.global_rate_qps())
            .reduce(f64::min)
            .expect("controller should have set a pacing rate");
        // Retreat stops once loss falls under the threshold, so the rate settles at
        // the capacity rather than somewhere well beneath it. Landing just above,
        // with a fraction of a percent of loss that retries absorb, is the intended
        // outcome: undershooting would leave capacity unused.
        assert!(
            rate < CAPACITY * 1.05,
            "controller should settle at the {CAPACITY} QPS edge, got {rate}"
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

    /// Real congestion is still followed down to the path's capacity.
    #[tokio::test]
    async fn global_retreat_continues_while_the_loss_is_falling() {
        const CAPACITY: f64 = 400.0;
        let pool = pool(1);
        let ceiling = 5_000.0;
        let (mut c, global, outcomes) = controller(1, ceiling);
        let tick = Duration::from_millis(500);
        let seconds = tick.as_secs_f64();

        for _ in 0..40 {
            // Everything over capacity is lost, so loss falls with the rate.
            let dispatched = global.rate().min(ceiling) * seconds;
            let lost = (dispatched - CAPACITY * seconds).max(0.0);
            drive(&pool.resolvers()[0], dispatched as u64, 0);
            finish(&outcomes, dispatched as u64, lost as u64);
            c.tick(&pool, tick, Instant::now());
        }

        let rate = global.rate();
        assert!(
            rate < CAPACITY * 2.0,
            "loss that answers to the rate must be followed down to the \
             {CAPACITY} qps capacity, got {rate}"
        );
        assert!(
            rate > CAPACITY * 0.5,
            "and not overshot into a collapse, got {rate}"
        );
    }

    /// Loss that reads the same at any rate becomes the baseline, and the rate
    /// returns.
    #[tokio::test]
    async fn rate_insensitive_loss_is_learned_as_a_baseline_and_the_rate_returns() {
        let pool = pool(1);
        let ceiling = 5_000.0;
        let (mut c, global, outcomes) = controller(1, ceiling);
        let tick = Duration::from_millis(500);
        let seconds = tick.as_secs_f64();
        let mut now = Instant::now();

        // A tenth of everything fails, at whatever rate it is sent.
        for _ in 0..40 {
            let finished = (global.rate().min(ceiling) * seconds) as u64;
            drive(&pool.resolvers()[0], finished, 0);
            finish(&outcomes, finished, finished / 10);
            now += tick;
            c.tick(&pool, tick, now);
        }

        assert_eq!(
            global.rate(),
            ceiling,
            "loss no rate can fix must not cost the client its rate"
        );
    }

    /// A path answering nothing is held, not grown back into.
    #[tokio::test]
    async fn a_broken_path_is_not_grown_into() {
        let pool = pool(1);
        let ceiling = 5_000.0;
        let (mut c, global, outcomes) = controller(1, ceiling);
        let tick = Duration::from_millis(500);
        let seconds = tick.as_secs_f64();
        let mut now = Instant::now();

        // Every query fails.
        for _ in 0..80 {
            let finished = (global.rate().min(ceiling) * seconds).max(1.0) as u64;
            drive(&pool.resolvers()[0], finished, 0);
            finish(&outcomes, finished, finished);
            now += tick;
            c.tick(&pool, tick, now);
        }

        assert!(
            global.rate() < ceiling,
            "a path answering nothing must not be driven at the ceiling, got {}",
            global.rate()
        );
    }

    /// The baseline follows loss down, so a stale one can't hide later congestion.
    #[tokio::test]
    async fn a_baseline_follows_the_loss_down() {
        let pool = pool(1);
        let ceiling = 5_000.0;
        let (mut c, global, outcomes) = controller(1, ceiling);
        let tick = Duration::from_millis(500);
        let seconds = tick.as_secs_f64();

        // `loss` maps the current rate to a loss fraction.
        let step = |c: &mut AdaptiveController, loss: &dyn Fn(f64) -> f64| {
            let rate = global.rate().min(ceiling);
            let finished = (rate * seconds) as u64;
            drive(&pool.resolvers()[0], finished, 0);
            finish(&outcomes, finished, (finished as f64 * loss(rate)) as u64);
            c.tick(&pool, tick, Instant::now());
        };

        // A bad patch no rate can fix sets the bar high.
        for _ in 0..40 {
            step(&mut c, &|_| 0.30);
        }
        assert_eq!(
            global.rate(),
            ceiling,
            "the baseline should have been learned"
        );

        // It passes, and the path settles at a far smaller standing cost.
        for _ in 0..20 {
            step(&mut c, &|_| 0.05);
        }
        assert_eq!(
            global.rate(),
            ceiling,
            "a smaller cost is still not congestion"
        );

        // Now real congestion: 25% loss at full rate, which a stale 30% bar would
        // never act on.
        let capacity = ceiling * 0.75;
        let mut lowest = f64::INFINITY;
        for _ in 0..20 {
            step(&mut c, &|rate| (1.0 - capacity / rate).max(0.0));
            lowest = lowest.min(global.rate());
        }

        assert!(
            lowest < ceiling,
            "congestion above the path's real cost must still be acted on"
        );
        assert!(
            lowest > ceiling * 0.5,
            "and followed down to the capacity, not past it, got {lowest}"
        );
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

    /// Names that fail at any rate must not ratchet the global rate down.
    #[tokio::test]
    async fn rate_insensitive_failures_do_not_collapse_the_global_rate() {
        let pool = pool(3);
        let (mut c, global, outcomes) = controller(3, UNLIMITED_QPS);
        let tick = Duration::from_millis(500);
        let demand = 2000.0_f64; // what the workload would dispatch if unpaced
        let (mut finished_total, mut failed_total) = (0u64, 0u64);
        for _ in 0..240 {
            // 120 seconds
            let finished = (global.rate().min(demand) * tick.as_secs_f64()).max(1.0) as u64;
            finished_total += finished;
            // a fixed 3% of names can never be answered, whatever the rate
            let failed = (finished_total * 3 / 100) - failed_total;
            failed_total += failed;
            for r in pool.resolvers() {
                drive(r, finished / 3, 0);
            }
            finish(&outcomes, finished, failed);
            c.tick(&pool, tick, Instant::now());
        }
        let rate = global.rate().min(demand);
        assert!(
            rate >= demand * 0.5,
            "loss that does not respond to retreat drove the global rate from {demand} to {rate:.1} qps"
        );
    }

    /// The same at brute-force scale, where the aggregate is the only signal and no
    /// resolver may be paced individually.
    #[tokio::test]
    async fn rate_insensitive_failures_do_not_collapse_the_rate_at_brute_force_scale() {
        const RESOLVERS: usize = 2000;
        const RETRIES: u64 = 20;
        let pool = pool_of(RESOLVERS);
        let (mut c, global, outcomes) = controller(RESOLVERS, UNLIMITED_QPS);
        let tick = Duration::from_millis(500);
        let demand = 4000.0_f64;
        let (mut finished_total, mut failed_total) = (0u64, 0u64);
        let mut cursor = 0usize;

        for _ in 0..240 {
            // 120 seconds
            let finished = (global.rate().min(demand) * tick.as_secs_f64()).max(1.0) as u64;
            finished_total += finished;
            // a fixed 3% of names can never be answered, whatever the rate
            let failed = (finished_total * 3 / 100) - failed_total;
            failed_total += failed;

            // Round-robin: one attempt per answered name, every retry timing out
            // for the rest.
            let timeouts = failed * (1 + RETRIES);
            for n in 0..(finished - failed + timeouts) {
                let r = &pool.resolvers()[cursor % RESOLVERS];
                cursor += 1;
                r.record_dispatch();
                if n < timeouts {
                    r.record_error(true);
                } else {
                    r.record_response(Duration::from_millis(10), true);
                }
            }
            finish(&outcomes, finished, failed);
            c.tick(&pool, tick, Instant::now());
        }

        let rate = global.rate().min(demand);
        assert!(
            rate >= demand * 0.5,
            "loss that does not respond to retreat drove the global rate from {demand} to {rate:.1} qps"
        );
        assert!(
            pool.resolvers().iter().all(|r| r.rate() == UNLIMITED_QPS),
            "a resolver carrying a trickle is not individually measurable and must not be paced"
        );
    }

    /// A resolver's edge steps from its own rate, not to a rate another limit
    /// imposed.
    #[tokio::test]
    async fn a_resolver_edge_steps_down_from_its_own_rate_not_another_limit() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];

        // Driven hard, the resolver answers a hundred a second and loses the rest.
        drive(resolver, 400, 300);
        c.tick(&pool, Duration::from_secs(1), Instant::now());
        let standing = resolver.rate();
        assert!(standing < UNLIMITED_QPS, "heavy loss should earn a retreat");

        // Another limit now caps its traffic far below its own pacing; loss drops,
        // so the retreat looks like it worked.
        drive(resolver, 110, 10);
        c.tick(&pool, Duration::from_secs(1), Instant::now());

        assert!(
            resolver.rate() >= standing * RETREAT * 0.99,
            "one step down from {standing:.0} qps, not a fall to what another limit allowed: {:.0}",
            resolver.rate()
        );
    }

    /// A retreat that bought nothing is undone.
    #[tokio::test]
    async fn a_retreat_that_buys_nothing_is_taken_back() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];

        // A tenth of everything lost, at whatever rate.
        let step = |c: &mut AdaptiveController| {
            let sent = resolver.rate().min(1000.0) as u64;
            drive(resolver, sent, sent / 10);
            c.tick(&pool, Duration::from_secs(1), Instant::now());
        };
        step(&mut c);
        assert!(
            resolver.rate() < UNLIMITED_QPS,
            "the first sight of loss earns a retreat"
        );
        step(&mut c);
        assert_eq!(
            resolver.rate(),
            UNLIMITED_QPS,
            "a retreat that bought nothing must be taken back"
        );
    }

    /// A held edge still expires, even while noise keeps reopening episodes.
    #[tokio::test]
    async fn a_held_edge_still_expires() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];
        let start = Instant::now();
        let at = |s: u64| start + Duration::from_secs(s);

        // A resolver saturated at 600 answers a second, followed down to it.
        // Bounded, so a regression fails below instead of looping.
        let mut t = 0;
        while t < 20 {
            t += 1;
            let sent = resolver.rate().min(1000.0) as u64;
            drive(resolver, sent, sent.saturating_sub(600));
            c.tick(&pool, Duration::from_secs(1), at(t));
            if sent <= 600 {
                break;
            }
        }
        let held = resolver.rate();
        assert!(held < UNLIMITED_QPS, "the capacity should have been found");

        // Noise: every other window reads a few percent of loss.
        let mut released = false;
        for _ in 0..EDGE_TTL.as_secs() + 10 {
            t += 1;
            let sent = resolver.rate().min(1000.0) as u64;
            let lost = if t % 2 == 0 { 0 } else { sent / 20 };
            drive(resolver, sent, lost);
            c.tick(&pool, Duration::from_secs(1), at(t));
            released |= resolver.rate() == UNLIMITED_QPS;
        }
        assert!(
            released,
            "an edge held at {held:.0} qps must still expire on schedule"
        );
    }

    /// Chance improvement on an unchanged rate must not step a resolver down.
    #[tokio::test]
    async fn noise_on_a_held_edge_does_not_step_a_resolver_down() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];
        let start = Instant::now();
        let at = |s: u64| start + Duration::from_secs(s);

        // A resolver saturated at 600 answers a second, followed down to it.
        // Bounded, so a regression fails below instead of looping.
        let mut t = 0;
        while t < 20 {
            t += 1;
            let sent = resolver.rate().min(1000.0) as u64;
            drive(resolver, sent, sent.saturating_sub(600));
            c.tick(&pool, Duration::from_secs(1), at(t));
            if sent <= 600 {
                break;
            }
        }
        let held = resolver.rate();
        assert!(held < UNLIMITED_QPS, "the capacity should have been found");

        // 6% then 4% loss at the same rate: looks like progress, but nothing
        // changed.
        for percent in [6, 4] {
            t += 1;
            let sent = resolver.rate().min(1000.0) as u64;
            drive(resolver, sent, sent * percent / 100);
            c.tick(&pool, Duration::from_secs(1), at(t));
        }

        assert_eq!(
            resolver.rate(),
            held,
            "chance improvement on an unchanged rate must not step the resolver down"
        );
    }

    /// Loss crossing the threshold back and forth must not ratchet a resolver down.
    #[tokio::test]
    async fn loss_crossing_the_threshold_does_not_ratchet_a_resolver_down() {
        let pool = pool(1);
        let (mut c, _global, _outcomes) = controller(1, UNLIMITED_QPS);
        let resolver = &pool.resolvers()[0];
        let demand = 1000.0_f64;
        let start = Instant::now();

        let mut first = None;
        // Within one edge lifetime, so only a ratchet could move the rate.
        for i in 0..20u64 {
            let sent = resolver.rate().min(demand) as u64;
            // A fixed share no rate can fix, read as 6% and 1% by turns.
            let lost = if i % 2 == 0 {
                sent * 6 / 100
            } else {
                sent / 100
            };
            drive(resolver, sent, lost);
            c.tick(
                &pool,
                Duration::from_secs(1),
                start + Duration::from_secs(i + 1),
            );
            if first.is_none() && resolver.rate() < UNLIMITED_QPS {
                first = Some(resolver.rate());
            }
        }

        let first = first.expect("the first lossy window should have earned a retreat");
        assert!(
            resolver.rate() >= first * 0.99,
            "loss that ignores the rate stepped the resolver from {first:.0} down to {:.0} qps",
            resolver.rate()
        );
    }
}
