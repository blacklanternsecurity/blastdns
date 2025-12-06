use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hickory_client::proto::op::Query;
use hickory_client::proto::xfer::DnsResponse;
use lru::LruCache;

#[derive(Clone)]
pub struct SimpleCache {
    inner: Arc<Mutex<LruCache<Query, CacheEntry>>>,
    min_ttl: Duration,
    max_ttl: Duration,
}

#[derive(Clone)]
struct CacheEntry {
    response: Arc<DnsResponse>,
    expire_at: Instant,
}

impl SimpleCache {
    pub fn new(capacity: usize, min_ttl: Duration, max_ttl: Duration) -> Self {
        let size = NonZeroUsize::new(capacity.max(1)).expect("non-zero cache capacity");
        let inner = LruCache::new(size);

        Self {
            inner: Arc::new(Mutex::new(inner)),
            min_ttl,
            max_ttl,
        }
    }

    pub fn get(&self, query: &Query, now: Instant) -> Option<Arc<DnsResponse>> {
        let mut guard = self.inner.lock().ok()?;

        if let Some(entry) = guard.get(query) {
            if entry.expire_at > now {
                return Some(entry.response.clone());
            }

            // Drop expired entries on the way out; eviction of other expired
            // items is left to the LRU policy.
            guard.pop(query);
        }

        None
    }

    // This is not used in the library, but is useful for testing.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn contains(&self, query: &Query, now: Instant) -> bool {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };

        if let Some(entry) = guard.get(query) {
            if entry.expire_at > now {
                return true;
            }
            guard.pop(query);
        }

        false
    }

    pub fn insert(&self, query: Query, response: DnsResponse, now: Instant) {
        let ttl_secs = response
            .answers()
            .iter()
            .map(|r| r.ttl())
            .min()
            .unwrap_or(0);
        if ttl_secs == 0 {
            return;
        }

        let ttl = Duration::from_secs(ttl_secs as u64).clamp(self.min_ttl, self.max_ttl);
        let expire_at = now + ttl;

        let entry = CacheEntry {
            response: Arc::new(response),
            expire_at,
        };

        if let Ok(mut guard) = self.inner.lock() {
            guard.put(query, entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_client::proto::rr::{Name, RData, Record, RecordType};
    use std::net::Ipv4Addr;
    use std::thread;

    fn make_response(name: &str, ttl: u32) -> (Query, DnsResponse) {
        let name = Name::from_ascii(name).unwrap();
        let query = Query::query(name.clone(), RecordType::A);

        let record = Record::from_rdata(name, ttl, RData::A(Ipv4Addr::new(1, 2, 3, 4).into()));

        let mut msg = hickory_client::proto::op::Message::new();
        msg.add_query(query.clone());
        msg.add_answer(record);

        let response = DnsResponse::from_message(msg).unwrap();
        (query, response)
    }

    #[test]
    fn hit_returns_cached_response() {
        let cache = SimpleCache::new(8, Duration::from_secs(1), Duration::from_secs(3600));
        let (query, response) = make_response("example.com.", 30);

        cache.insert(query.clone(), response.clone(), Instant::now());

        let hit = cache.get(&query, Instant::now()).expect("cache hit");
        assert_eq!(hit.answers()[0].ttl(), 30);
        assert_eq!(hit.answers()[0].data(), response.answers()[0].data());
    }

    #[test]
    fn expired_entry_is_miss() {
        let cache = SimpleCache::new(4, Duration::from_millis(0), Duration::from_millis(2));
        let (query, response) = make_response("expired.test.", 1);

        cache.insert(query.clone(), response, Instant::now());
        thread::sleep(Duration::from_millis(5));

        assert!(cache.get(&query, Instant::now()).is_none());
    }

    #[test]
    fn zero_ttl_is_not_cached() {
        let cache = SimpleCache::new(4, Duration::from_millis(1), Duration::from_secs(60));
        let (query, response) = make_response("zero-ttl.test.", 0);

        cache.insert(query.clone(), response, Instant::now());
        assert!(cache.get(&query, Instant::now()).is_none());
    }

    #[test]
    fn lru_eviction_respects_capacity() {
        let cache = SimpleCache::new(1, Duration::from_secs(1), Duration::from_secs(60));
        let (q1, r1) = make_response("first.test.", 10);
        let (q2, r2) = make_response("second.test.", 10);

        cache.insert(q1.clone(), r1, Instant::now());
        cache.insert(q2.clone(), r2.clone(), Instant::now());

        assert!(
            cache.get(&q1, Instant::now()).is_none(),
            "oldest should be evicted"
        );
        let hit = cache
            .get(&q2, Instant::now())
            .expect("newest should remain");
        assert_eq!(hit.answers()[0].data(), r2.answers()[0].data());
    }

    #[test]
    fn contains_checks_expiry() {
        let cache = SimpleCache::new(2, Duration::from_secs(0), Duration::from_millis(2));
        let (query, response) = make_response("contains.test.", 1);

        assert!(!cache.contains(&query, Instant::now()), "empty cache");

        cache.insert(query.clone(), response, Instant::now());
        assert!(
            cache.contains(&query, Instant::now()),
            "should see fresh entry"
        );

        thread::sleep(Duration::from_millis(5));
        assert!(
            !cache.contains(&query, Instant::now()),
            "expired entry should be treated as miss"
        );
    }
}
