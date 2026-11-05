use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;

/// Async rate limiter backed by a single atomic cursor.
///
/// `next_ns` is a monotonic-clock offset (nanoseconds since construction) at
/// which the NEXT permit becomes available. `acquire` atomically bumps the
/// cursor by one interval and treats the OLD value as its dispatch slot,
/// running immediately if that slot is already past and sleeping until it
/// otherwise.
///
/// Dispatch is interval-paced rather than token-bucketed: there is no burst
/// allowance, so a resolver never sees a thundering herd on the first tick.
///
/// The rate is adjustable at runtime via [`RateLimiter::set_rate`]. Each acquire
/// reads the current interval, so a change takes effect on the next dispatch
/// without disturbing slots already handed out.
pub struct RateLimiter {
    interval_ns: AtomicU64,
    start: tokio::time::Instant,
    next_ns: AtomicU64,
}

/// Rate high enough that pacing never sleeps, used to mean "no limit" without
/// needing a separate unlimited state on the hot path.
pub(crate) const UNLIMITED_QPS: f64 = 1_000_000_000.0;

impl RateLimiter {
    /// Build a limiter pacing dispatch at `queries_per_second`.
    ///
    /// # Panics
    /// Panics if `queries_per_second` is not positive.
    pub fn new(queries_per_second: f64) -> Self {
        RateLimiter {
            interval_ns: AtomicU64::new(Self::interval_for(queries_per_second)),
            start: tokio::time::Instant::now(),
            next_ns: AtomicU64::new(0),
        }
    }

    fn interval_for(queries_per_second: f64) -> u64 {
        assert!(
            queries_per_second > 0.0,
            "RateLimiter requires queries_per_second > 0, got {queries_per_second}",
        );
        // Clamp to >=1ns so the cursor always makes positive progress, even at
        // absurd rates. u64 nanoseconds gives ~584y of runtime headroom.
        (1_000_000_000.0 / queries_per_second).round().max(1.0) as u64
    }

    /// Change the pacing rate. Takes effect on the next acquire.
    ///
    /// # Panics
    /// Panics if `queries_per_second` is not positive.
    pub fn set_rate(&self, queries_per_second: f64) {
        self.interval_ns
            .store(Self::interval_for(queries_per_second), Ordering::Relaxed);
    }

    /// The current pacing rate in queries per second.
    pub fn rate(&self) -> f64 {
        1_000_000_000.0 / self.interval_ns.load(Ordering::Relaxed) as f64
    }

    /// The current spacing between consecutive permits.
    pub fn interval(&self) -> Duration {
        Duration::from_nanos(self.interval_ns.load(Ordering::Relaxed))
    }

    /// Wait until this caller's dispatch slot arrives.
    pub async fn acquire(&self) {
        let interval_ns = self.interval_ns.load(Ordering::Relaxed);

        // `base` is max(cursor, now) so an idle limiter resets to "now" instead
        // of letting a stockpile of back-dated slots leak out as a burst.
        let slot_ns = loop {
            let current = self.next_ns.load(Ordering::Relaxed);
            let now_ns = self.start.elapsed().as_nanos() as u64;
            let base = current.max(now_ns);
            let next = base.saturating_add(interval_ns);
            if self
                .next_ns
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break base;
            }
        };

        // Re-read now that the CAS resolved: if we lost a few rounds our slot
        // may already be in the past, and no sleep is needed.
        let now_ns = self.start.elapsed().as_nanos() as u64;
        if slot_ns <= now_ns {
            return;
        }
        let deficit_ns = slot_ns - now_ns;

        // Tokio's timer has ~1ms granularity, so a 10us sleep actually returns
        // in ~1ms and would floor throughput at ~1k QPS. Skip sub-millisecond
        // sleeps: the cursor has already advanced, so subsequent acquires
        // accumulate the debt until it crosses 1ms and a real sleep lands. The
        // aggregate rate is still capped correctly.
        const SUB_MS_SKIP_THRESHOLD_NS: u64 = 1_000_000;
        if deficit_ns >= SUB_MS_SKIP_THRESHOLD_NS {
            tokio::time::sleep(Duration::from_nanos(deficit_ns)).await;
        }
    }
}

/// Paces attempts at a rate the adaptive controller can move while traffic is
/// running.
///
/// A ticker adds tokens at the current rate and the bucket holds at most a short
/// burst. Callers wait on a shared semaphore instead of reserving their own future
/// slot, so a rate change takes effect on the next tick in either direction: nothing
/// is banked ahead of a cut, and no queue of reservations is left behind a raise.
/// Unlimited until a rate is set, and then free to callers again once it is cleared.
pub(crate) struct AttemptPacer {
    /// `None` while unlimited.
    rate: Mutex<Option<f64>>,
    tokens: Semaphore,
    /// Fractional tokens carried between ticks.
    carry: Mutex<f64>,
    waiting: AtomicU64,
}

const PACER_TICK: Duration = Duration::from_millis(2);
/// Refill interval while nothing is paced; a newly set rate starts within it.
const PACER_IDLE_TICK: Duration = Duration::from_millis(100);
/// Seconds of tokens the bucket may hold, so a quiet spell cannot bank a burst.
const PACER_BURST_SECONDS: f64 = 0.01;

/// Counts a caller as waiting until it gets a token or is cancelled.
struct Waiting<'a>(&'a AtomicU64);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AttemptPacer {
    pub(crate) fn new() -> Self {
        AttemptPacer {
            rate: Mutex::new(None),
            tokens: Semaphore::new(0),
            carry: Mutex::new(0.0),
            waiting: AtomicU64::new(0),
        }
    }

    pub(crate) fn rate(&self) -> Option<f64> {
        *self.rate.lock().unwrap()
    }

    pub(crate) fn set_rate(&self, rate: Option<f64>) {
        *self.rate.lock().unwrap() = rate;
        *self.carry.lock().unwrap() = 0.0;
    }

    /// Callers waiting for a token: non-zero means the rate is what holds sends back.
    pub(crate) fn waiting(&self) -> u64 {
        self.waiting.load(Ordering::Relaxed)
    }

    /// Wait for a token to send one attempt; immediate while unlimited.
    pub(crate) async fn acquire(&self) {
        if self.rate().is_none() {
            return;
        }
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let _waiting = Waiting(&self.waiting);
        if let Ok(permit) = self.tokens.acquire().await {
            permit.forget();
        }
    }

    fn refill(&self, seconds: f64) {
        let Some(rate) = self.rate() else {
            // Unlimited again: let through anyone still waiting from before.
            let waiting = self.waiting() as usize;
            self.tokens
                .add_permits(waiting.saturating_sub(self.tokens.available_permits()));
            return;
        };
        let mut carry = self.carry.lock().unwrap();
        *carry += rate * seconds;
        let whole = carry.floor();
        *carry -= whole;
        let burst = (rate * PACER_BURST_SECONDS).max(1.0) as usize;
        let room = burst.saturating_sub(self.tokens.available_permits());
        self.tokens.add_permits((whole as usize).min(room));
    }

    /// Refill tokens until the pacer is dropped. While there is nothing to pace the
    /// task checks in only occasionally, so an idle client costs next to nothing.
    pub(crate) fn spawn(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut previous = tokio::time::Instant::now();
            let mut pause = PACER_TICK;
            loop {
                tokio::time::sleep(pause).await;
                let Some(pacer) = weak.upgrade() else {
                    break;
                };
                let now = tokio::time::Instant::now();
                pacer.refill(now.duration_since(previous).as_secs_f64());
                previous = now;
                let idle = pacer.rate().is_none() && pacer.waiting() == 0;
                pause = if idle { PACER_IDLE_TICK } else { PACER_TICK };
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[tokio::test]
    async fn paces_acquires_at_configured_rate() {
        // 10 QPS = 100ms spacing; 5 acquires span 4 intervals.
        let limiter = RateLimiter::new(10.0);
        let start = Instant::now();
        for _ in 0..5 {
            limiter.acquire().await;
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(350),
            "5 acquires at 10 QPS took {elapsed:?}, expected >= 350ms"
        );
        assert!(
            elapsed < Duration::from_millis(700),
            "5 acquires at 10 QPS took {elapsed:?}, expected < 700ms"
        );
    }

    #[tokio::test]
    async fn high_rps_does_not_collapse_to_timer_tick() {
        // At 100k QPS the interval is 10us. The limiter must not round that up
        // to the ~1ms timer tick and serialize every acquire at that pace.
        let limiter = RateLimiter::new(100_000.0);
        let start = Instant::now();
        for _ in 0..1000 {
            limiter.acquire().await;
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(250),
            "1000 acquires at 100k QPS took {elapsed:?}, expected < 250ms"
        );
    }

    #[tokio::test]
    async fn idle_period_does_not_stockpile_a_burst() {
        // 10 QPS. Acquire once, idle 5 intervals, then take 3 more. The cursor
        // must reset to "now" rather than releasing back-dated slots at once.
        let limiter = RateLimiter::new(10.0);
        limiter.acquire().await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let start = Instant::now();
        for _ in 0..3 {
            limiter.acquire().await;
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(150),
            "3 acquires after idling took {elapsed:?}, expected >= 150ms \
             (back-dated slots must not accumulate)"
        );
    }

    #[tokio::test]
    async fn interval_matches_the_configured_rate() {
        assert_eq!(
            RateLimiter::new(10.0).interval(),
            Duration::from_millis(100)
        );
        assert_eq!(RateLimiter::new(25.0).interval(), Duration::from_millis(40));
        assert_eq!(
            RateLimiter::new(1000.0).interval(),
            Duration::from_millis(1)
        );
    }

    async fn sends_per_second(pacer: &Arc<AttemptPacer>, seconds: f64) -> f64 {
        let sent = Arc::new(AtomicU64::new(0));
        let senders: Vec<_> = (0..200)
            .map(|_| {
                let (pacer, sent) = (pacer.clone(), sent.clone());
                tokio::spawn(async move {
                    loop {
                        pacer.acquire().await;
                        sent.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
        let rate = sent.load(Ordering::Relaxed) as f64 / seconds;
        for sender in senders {
            sender.abort();
        }
        rate
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pacer_holds_the_rate_it_is_given() {
        let pacer = Arc::new(AttemptPacer::new());
        pacer.spawn();
        pacer.set_rate(Some(500.0));
        let rate = sends_per_second(&pacer, 1.0).await;
        assert!(
            (400.0..=600.0).contains(&rate),
            "expected about 500/s with many waiters, got {rate}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_raised_pacer_rate_applies_at_once() {
        // a rate limiter that reserves slots ahead keeps a slow backlog after a raise
        let pacer = Arc::new(AttemptPacer::new());
        pacer.spawn();
        pacer.set_rate(Some(50.0));
        let slow = sends_per_second(&pacer, 0.5).await;
        pacer.set_rate(Some(2000.0));
        let fast = sends_per_second(&pacer, 0.2).await;
        assert!(slow < 100.0, "expected about 50/s, got {slow}");
        assert!(
            fast > 1200.0,
            "a raise must not wait out a backlog, got {fast}"
        );
    }

    #[tokio::test]
    async fn a_cleared_pacer_releases_its_waiters() {
        let pacer = Arc::new(AttemptPacer::new());
        pacer.spawn();
        pacer.set_rate(Some(1.0));
        let waiter = {
            let pacer = pacer.clone();
            tokio::spawn(async move { pacer.acquire().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        pacer.set_rate(None);
        tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter should be released once the rate is cleared")
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_idle_pacer_paces_promptly_once_given_a_rate() {
        // idle, the refill task checks in only occasionally; a new rate must still apply
        let pacer = Arc::new(AttemptPacer::new());
        pacer.spawn();
        tokio::time::sleep(Duration::from_millis(300)).await;
        pacer.set_rate(Some(1000.0));
        let rate = sends_per_second(&pacer, 0.3).await;
        assert!(
            rate > 500.0,
            "expected pacing near 1000/s soon after the rate was set, got {rate}"
        );
    }
}
