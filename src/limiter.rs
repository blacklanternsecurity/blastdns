use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
}
