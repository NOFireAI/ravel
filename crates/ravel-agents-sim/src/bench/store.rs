//! The store the benchmarks run over: `MemoryStore` under an empty-plan
//! `FaultStore`, under [`BenchStore`], which injects latency on tokio's
//! paused clock and tallies what no agents counter sees (HEAD reads by key,
//! telemetry requests, requests to watched agents, deletes).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_agents_store::Clock;
use ravel_object_store::fault::{FaultPlan, FaultStore};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};

use crate::drive::T0_NS;

/// Prefix of every telemetry key the ingest loop writes.
pub const TELEMETRY_PREFIX: &str = "telemetry/";

/// Injected latency per request kind, in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Latency {
    pub get_ms: u64,
    /// A metadata-only HEAD request.
    pub head_ms: u64,
    /// A PUT, conditional or not.
    pub put_ms: u64,
    pub list_ms: u64,
    pub delete_ms: u64,
}

/// The nominal store the prereg assumes.
pub const NOMINAL: Latency = Latency {
    get_ms: 10,
    head_ms: 5,
    put_ms: 20,
    list_ms: 15,
    delete_ms: 10,
};

/// How a [`BenchStore`] turns a request kind's nominal latency into a delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Speed {
    Nominal,
    /// Ten times nominal, plus a jitter drawn uniformly from zero to another
    /// ten times nominal, from `seed` and the request's position.
    Slow {
        seed: u64,
    },
}

impl Speed {
    pub fn name(self) -> &'static str {
        match self {
            Speed::Nominal => "nominal",
            Speed::Slow { .. } => "slow",
        }
    }
}

/// SplitMix64, for the slow store's jitter.
pub fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// What [`BenchStore`] counted, at a point in time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    /// GETs of an agent HEAD key.
    pub head_gets: u64,
    pub deletes: u64,
    pub telemetry_puts: u64,
    pub telemetry_gets: u64,
    /// Object-body bytes the telemetry PUTs sent.
    pub telemetry_out: u64,
    /// Object-body bytes the telemetry GETs returned.
    pub telemetry_in: u64,
    /// Requests to any key under the agents prefix `t/<tenant>/ag/`.
    pub agents_requests: u64,
    /// Requests to a key naming a watched agent.
    pub watched_requests: u64,
}

impl Tally {
    pub fn since(&self, earlier: &Tally) -> Tally {
        Tally {
            head_gets: self.head_gets.saturating_sub(earlier.head_gets),
            deletes: self.deletes.saturating_sub(earlier.deletes),
            telemetry_puts: self.telemetry_puts.saturating_sub(earlier.telemetry_puts),
            telemetry_gets: self.telemetry_gets.saturating_sub(earlier.telemetry_gets),
            telemetry_out: self.telemetry_out.saturating_sub(earlier.telemetry_out),
            telemetry_in: self.telemetry_in.saturating_sub(earlier.telemetry_in),
            agents_requests: self.agents_requests.saturating_sub(earlier.agents_requests),
            watched_requests: self
                .watched_requests
                .saturating_sub(earlier.watched_requests),
        }
    }
}

#[derive(Debug, Default)]
struct Counts {
    head_gets: AtomicU64,
    deletes: AtomicU64,
    telemetry_puts: AtomicU64,
    telemetry_gets: AtomicU64,
    telemetry_out: AtomicU64,
    telemetry_in: AtomicU64,
    agents_requests: AtomicU64,
    watched_requests: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Get,
    Head,
    Put,
    List,
    Delete,
}

/// The benchmark store. See the module docs.
pub struct BenchStore {
    inner: Arc<dyn ObjectStoreBackend>,
    speed: Speed,
    latency: AtomicBool,
    draws: AtomicU64,
    counts: Counts,
    agents_prefix: String,
    watched: Mutex<HashSet<String>>,
}

fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::SeqCst);
}

fn usize_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

impl BenchStore {
    /// `memory` under an empty-plan `FaultStore`, latency off until
    /// [`BenchStore::set_latency`] turns it on.
    pub fn new(memory: MemoryStore, speed: Speed, agents_prefix: String) -> BenchStore {
        let fault = FaultStore::new(memory, FaultPlan::empty());
        BenchStore {
            inner: Arc::new(fault),
            speed,
            latency: AtomicBool::new(false),
            draws: AtomicU64::new(0),
            counts: Counts::default(),
            agents_prefix,
            watched: Mutex::new(HashSet::new()),
        }
    }

    /// Latency applies only while on, so setup runs at no virtual cost.
    pub fn set_latency(&self, on: bool) {
        self.latency.store(on, Ordering::SeqCst);
    }

    pub fn speed(&self) -> Speed {
        self.speed
    }

    /// Count every later request to a key with a path segment equal to one
    /// of `agents` (agent ids in hex).
    pub fn watch(&self, agents: impl IntoIterator<Item = String>) {
        let mut set = self.watched.lock().unwrap_or_else(PoisonError::into_inner);
        set.extend(agents);
    }

    pub fn tally(&self) -> Tally {
        let c = &self.counts;
        let read = |a: &AtomicU64| a.load(Ordering::SeqCst);
        Tally {
            head_gets: read(&c.head_gets),
            deletes: read(&c.deletes),
            telemetry_puts: read(&c.telemetry_puts),
            telemetry_gets: read(&c.telemetry_gets),
            telemetry_out: read(&c.telemetry_out),
            telemetry_in: read(&c.telemetry_in),
            agents_requests: read(&c.agents_requests),
            watched_requests: read(&c.watched_requests),
        }
    }

    fn delay_ms(&self, kind: Kind) -> u64 {
        let base = match kind {
            Kind::Get => NOMINAL.get_ms,
            Kind::Head => NOMINAL.head_ms,
            Kind::Put => NOMINAL.put_ms,
            Kind::List => NOMINAL.list_ms,
            Kind::Delete => NOMINAL.delete_ms,
        };
        match self.speed {
            Speed::Nominal => base,
            Speed::Slow { seed } => {
                let n = self.draws.fetch_add(1, Ordering::SeqCst);
                let span = base * 10;
                span + splitmix(seed ^ splitmix(n)) % (span + 1)
            }
        }
    }

    async fn enter(&self, kind: Kind, key: &str) {
        heap::sample();
        if key.starts_with(&self.agents_prefix) {
            add(&self.counts.agents_requests, 1);
        }
        let watched = {
            let set = self.watched.lock().unwrap_or_else(PoisonError::into_inner);
            !set.is_empty() && key.split('/').any(|segment| set.contains(segment))
        };
        if watched {
            add(&self.counts.watched_requests, 1);
        }
        if kind == Kind::Delete {
            add(&self.counts.deletes, 1);
        }
        if self.latency.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(self.delay_ms(kind))).await;
        }
    }
}

#[async_trait]
impl ObjectStoreBackend for BenchStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.enter(Kind::Put, key).await;
        if key.starts_with(TELEMETRY_PREFIX) {
            add(&self.counts.telemetry_puts, 1);
            add(&self.counts.telemetry_out, usize_u64(data.len()));
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.enter(Kind::Get, key).await;
        if key.ends_with("/head") {
            add(&self.counts.head_gets, 1);
        }
        let out = self.inner.get(key, range).await;
        if key.starts_with(TELEMETRY_PREFIX) {
            add(&self.counts.telemetry_gets, 1);
            if let Ok(got) = &out {
                add(&self.counts.telemetry_in, usize_u64(got.data.len()));
            }
        }
        out
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.enter(Kind::Get, key).await;
        self.inner.get_pinned(key, range, pin).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.enter(Kind::Get, key).await;
        self.inner.get_with_pin(key, range).await
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.enter(Kind::Head, key).await;
        self.inner.pin_of(key).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.enter(Kind::Put, key).await;
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.enter(Kind::Head, key).await;
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.enter(Kind::List, prefix).await;
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.enter(Kind::List, prefix).await;
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.enter(Kind::List, prefix).await;
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.enter(Kind::Delete, key).await;
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

/// The agents clock on tokio's virtual time: [`T0_NS`] plus the time since
/// the clock was made. Make it inside the paused runtime.
#[derive(Debug)]
pub struct TokioClock {
    start: tokio::time::Instant,
}

impl TokioClock {
    pub fn new() -> TokioClock {
        TokioClock {
            start: tokio::time::Instant::now(),
        }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        TokioClock::new()
    }
}

impl Clock for TokioClock {
    fn now_unix_ns(&self) -> i64 {
        let elapsed = i64::try_from(self.start.elapsed().as_nanos()).unwrap_or(i64::MAX);
        T0_NS.saturating_add(elapsed)
    }
}

/// Peak live heap, from a probe the bench binary installs over its
/// counting global allocator. Without a probe nothing is sampled and no
/// heap figure is printed.
pub mod heap {
    use super::*;

    static PROBE: OnceLock<fn() -> u64> = OnceLock::new();
    static BASE: AtomicU64 = AtomicU64::new(0);
    static PEAK: AtomicU64 = AtomicU64::new(0);

    /// Install `probe`, which answers the live heap bytes. Only the first
    /// call takes effect.
    pub fn install(probe: fn() -> u64) {
        let _ = PROBE.set(probe);
    }

    pub fn installed() -> bool {
        PROBE.get().is_some()
    }

    /// Start a measurement at the current live heap.
    pub fn reset() {
        if let Some(probe) = PROBE.get() {
            let now = probe();
            BASE.store(now, Ordering::SeqCst);
            PEAK.store(now, Ordering::SeqCst);
        }
    }

    /// Fold the current live heap into the peak.
    pub fn sample() {
        if let Some(probe) = PROBE.get() {
            PEAK.fetch_max(probe(), Ordering::SeqCst);
        }
    }

    /// The largest sampled rise over the live heap at [`reset`], or `None`
    /// without a probe.
    pub fn peak_rise() -> Option<u64> {
        PROBE.get()?;
        sample();
        Some(
            PEAK.load(Ordering::SeqCst)
                .saturating_sub(BASE.load(Ordering::SeqCst)),
        )
    }
}
