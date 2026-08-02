use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::key::CacheKey;
use crate::limits::CacheLimits;
use crate::metrics::CacheMetrics;
use crate::s3fifo::S3Fifo;
use crate::single_flight::{Role, SingleFlight, SingleFlightError};

/// XOR mask applied to every byte on a hit in corruption mode. Never
/// 0x00: XORing with 0x00 would leave a zero-valued byte unchanged, and a
/// real segment page containing at least one 0x00 byte is common enough
/// that "unchanged" would not reliably mean "not corrupted".
const CORRUPTION_XOR: u8 = 0xA5;

/// The RAM tier of ADR-0046's read cache: a bounded, content-addressed,
/// single-flighted `CacheKey -> Bytes` map. `E` is the caller's upstream
/// fetch error type; this crate defines no error type of its own and has
/// no opinion on what a miss's upstream call looks like.
pub struct Cache<E> {
    fifo: Mutex<S3Fifo<Bytes>>,
    single_flight: SingleFlight<CacheKey, Bytes, E>,
    metrics: Arc<CacheMetrics>,
    corrupt_hits: bool,
}

impl<E> Cache<E>
where
    E: Clone + Send + Sync + 'static,
{
    pub fn new(limits: CacheLimits) -> Self {
        Cache {
            fifo: Mutex::new(S3Fifo::new(limits)),
            single_flight: SingleFlight::new(),
            metrics: Arc::new(CacheMetrics::default()),
            corrupt_hits: false,
        }
    }

    /// A cache that returns deliberately corrupted bytes on every hit.
    ///
    /// This is not a test fixture: it is a supported mode, and the
    /// acceptance gate for ADR-0046's whole read-cache epic. A later task
    /// runs the entire query test suite against a cache built this way,
    /// and every test must either return the identical result it returns
    /// against an uncached store, or fail with a typed error -- proving
    /// that query correctness never depends on what the cache happens to
    /// hold. Do not use this constructor for anything other than that
    /// suite.
    pub fn with_corruption(limits: CacheLimits) -> Self {
        Cache {
            corrupt_hits: true,
            ..Cache::new(limits)
        }
    }

    /// A cloneable handle to this cache's counters, independent of the
    /// cache's own lifetime.
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        self.metrics.clone()
    }

    /// Current number of resident entries.
    pub fn len(&self) -> usize {
        self.fifo.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current total bytes across every resident entry.
    pub fn total_bytes(&self) -> u64 {
        self.fifo.lock().total_bytes()
    }

    /// Look up `key` without fetching. Records a hit or a miss either
    /// way. In corruption mode, a hit returns deliberately corrupted
    /// bytes rather than what was inserted.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let hit = self.fifo.lock().get(key);
        match hit {
            Some(bytes) => {
                self.metrics.record_hit(bytes.len() as u64);
                Some(self.maybe_corrupt(bytes))
            }
            None => {
                self.metrics.record_miss();
                None
            }
        }
    }

    /// Admit `value` under `key`. Not an error, and a no-op on the
    /// eviction state, if `value` is larger than the configured maximum
    /// single-entry size: the caller still has its own copy of the bytes.
    pub fn insert(&self, key: CacheKey, value: Bytes) {
        let size = value.len() as u64;
        self.fifo.lock().insert(key, value, size, &self.metrics);
    }

    fn maybe_corrupt(&self, bytes: Bytes) -> Bytes {
        if !self.corrupt_hits {
            return bytes;
        }
        Bytes::from(
            bytes
                .iter()
                .map(|b| b ^ CORRUPTION_XOR)
                .collect::<Vec<u8>>(),
        )
    }

    /// Collapses concurrent misses on the same key into one call to `fetch`
    /// (ADR-0046 decision 5) and, on a leader miss that succeeds, admits the
    /// result before returning it.
    ///
    /// Does not consult the cache itself first: both call sites in
    /// `ravel-query` already call `get` to decide their own hit/miss
    /// accounting (ADR-0044's `QueryAccounting` needs that branch either
    /// way) and call this only on that miss. An earlier version re-checked
    /// here too, so the *same* logical miss recorded two `CacheMetrics`
    /// misses -- one from the caller's `get`, one from this method's own --
    /// corrupting the request-hit-rate SLI ADR-0046 lists. Call `get`
    /// yourself first if you need the hit path; this is the miss-only half.
    pub async fn get_or_fetch<F, Fut>(
        &self,
        key: CacheKey,
        fetch: F,
    ) -> Result<Bytes, SingleFlightError<E>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<Bytes, E>> + Send,
    {
        let (result, role) = self.single_flight.run(key, fetch).await;
        match (&result, role) {
            (Ok(bytes), Role::Leader) => self.insert(key, bytes.clone()),
            (Ok(_), Role::Follower) => self.metrics.record_collapse(),
            (Err(_), _) => {}
        }
        result
    }
}
