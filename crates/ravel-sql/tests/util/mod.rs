//! Shared fixtures for the integration tests.
//!
//! Everything here builds *real* artifacts: real RSEG segments written with
//! `ravel_segment`, published with `ravel_commit` onto a `MemoryStore`, and
//! resolved through a real `Catalog`. No hand-built `Snapshot`s, because the
//! endpoint's contract includes the resolve step and a fake snapshot would
//! skip exactly the code the retry contract exercises.
//!
//! [`reference_rows`] is the independent row source the layer-2 differential
//! gate evaluates against. It reimplements the normative greatest-wins total
//! order (`is_greater` in crates/ravel-query/src/engine.rs,
//! docs/catalog-and-mvcc.md) over `SegmentFetcher::fetch` -- the AoS surface
//! -- while the path under test decodes with `fetch_soa` and dedups in
//! `RsegDedupExec`. The two sides share fetched *bytes* and share no merge
//! or dedup code, which the reference requires: the
//! plan names `merge_segments` as the reference window, but that function is
//! private to ravel-query and making it public is outside this crate's
//! scope, so the gate uses a separate implementation of the same
//! normative order (the same choice, for the same reason, that B1's layer-1
//! oracle in tests/pipeline.rs made).

#![allow(dead_code, clippy::expect_used, clippy::unwrap_used)]

/// The shared v1 SQL differential grammar (query shapes, dataset strategies,
/// reference folds, comparable rows), used by both the HTTP-path oracle gate
/// and the Flight-vs-HTTP transport parity gate.
pub mod gate;

/// The in-process Flight SQL harness, shared across the Flight test suites.
/// Only compiled under `flight-sql`, which is what links arrow-flight/tonic.
#[cfg(feature = "flight-sql")]
pub mod flight_harness;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{
    CommitToken, Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange,
};
use uuid::Uuid;

pub const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// The injected clock reading every fixture query uses. Small on purpose:
/// `Catalog::resolve` issues one LIST per (shard, ingest-hour) pair between
/// the window start and `now_ns`, so a realistic wall-clock value would fan
/// out to hundreds of thousands of LISTs in a test.
pub const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// Wide enough to cover every fixture sample; segment pruning is exercised
/// by the pushdown tests, not here.
pub fn full_window() -> TimeRange {
    TimeRange {
        start_ns: 0,
        end_ns: 3 * NS_PER_HOUR,
    }
}

pub fn tenant_id(name: &str) -> TenantId {
    TenantId::new(name.to_string())
}

/// A logical series inside a segment: metric name plus (ts, value) samples
/// in write order. The writer stable-sorts by ts, so equal-ts insertion
/// order is preserved and becomes the in-page-index tiebreak.
#[derive(Clone, Debug)]
pub struct SeriesSpec {
    pub metric: String,
    pub samples: Vec<(i64, f64)>,
}

impl SeriesSpec {
    pub fn new(metric: &str, samples: Vec<(i64, f64)>) -> Self {
        SeriesSpec {
            metric: metric.to_string(),
            samples,
        }
    }
}

/// A segment's provenance plus its series. Distinct provenance across
/// segments exercises every field of the dedup total order.
#[derive(Clone, Debug)]
pub struct SegSpec {
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub series: Vec<SeriesSpec>,
}

impl SegSpec {
    pub fn new(
        created_unix_ns: i64,
        writer_epoch: u64,
        writer_seq: u64,
        series: Vec<SeriesSpec>,
    ) -> Self {
        SegSpec {
            created_unix_ns,
            writer_epoch,
            writer_seq,
            series,
        }
    }
}

pub fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

pub fn series_id_for(tenant: &TenantId, metric: &str) -> [u8; 16] {
    SeriesId::compute(tenant, metric, &labels_for(metric))
        .expect("series id")
        .0
}

/// Write one real RSEG segment for `spec` and publish its commit record.
/// Returns the commit token, so a caller can exercise `min_commit_token`.
pub async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    spec: &SegSpec,
) -> (CommitToken, String) {
    let tenant_hash = tenant.hash();
    let inputs: Vec<SeriesInput> = spec
        .series
        .iter()
        .filter(|s| !s.samples.is_empty())
        .map(|s| SeriesInput {
            series_id: SeriesId(series_id_for(tenant, &s.metric)),
            labels: labels_for(&s.metric),
            samples: s
                .samples
                .iter()
                .map(|(ts_ns, value)| Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        })
        .collect();

    let writer_id = Uuid::from_u128(1_000 + index as u128);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: spec.writer_epoch,
        writer_seq: spec.writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: spec.writer_epoch,
        writer_seq: spec.writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: spec.created_unix_ns,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    let token = publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (token, data_key)
}

/// A running stack: store, catalog, fetcher, and executor.
pub struct Fixture {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub catalog: Arc<Catalog>,
    pub fetcher: SegmentFetcher,
    pub executor: SqlExecutor,
    /// Data object keys in publish order, per tenant name.
    pub data_keys: HashMap<String, Vec<String>>,
}

impl Fixture {
    pub async fn build(
        store: Arc<dyn ObjectStoreBackend>,
        tenants: &[(&TenantId, &[SegSpec])],
        config: SqlConfig,
        max_tenant_bytes: usize,
    ) -> Self {
        let mut data_keys: HashMap<String, Vec<String>> = HashMap::new();
        for (tenant, specs) in tenants {
            for (i, spec) in specs.iter().enumerate() {
                let (_token, key) = publish_segment(store.as_ref(), tenant, i, spec).await;
                data_keys
                    .entry(tenant.as_str().to_string())
                    .or_default()
                    .push(key);
            }
        }
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let fetcher = SegmentFetcher::new(Arc::clone(&store));
        let executor = SqlExecutor::new(
            Arc::clone(&catalog),
            fetcher.clone(),
            LogSegmentFetcher::new(Arc::clone(&store)),
            config,
            max_tenant_bytes,
        );
        Fixture {
            store,
            catalog,
            fetcher,
            executor,
            data_keys,
        }
    }

    /// A fixture on a plain `MemoryStore` with default budgets.
    pub async fn memory(tenants: &[(&TenantId, &[SegSpec])]) -> Self {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        Fixture::build(store, tenants, SqlConfig::default(), 1 << 30).await
    }

    pub async fn snapshot(&self, tenant: &TenantId) -> Snapshot {
        self.catalog
            .resolve(&tenant.hash(), Signal::Metrics, full_window(), &[], NOW_NS)
            .await
            .expect("resolve")
    }
}

/// A `SqlRequest` over the full window at [`NOW_NS`].
pub fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: full_window(),
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
    }
}

/// The default engine budgets, for tests that need to name them.
pub fn engine_config() -> EngineConfig {
    EngineConfig::default()
}

// ---------------------------------------------------------------------------
// Operation-counting store
// ---------------------------------------------------------------------------

/// A pass-through backend that counts every operation.
///
/// `FaultStore` counts only faults it *injected*, which cannot answer "did
/// this request touch the store at all". The security tests need exactly
/// that: proving a rejected statement was refused *before planning* means
/// proving zero catalog LISTs and zero GETs happened, not merely that an
/// error came back.
pub struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    puts: AtomicU64,
    gets: AtomicU64,
    heads: AtomicU64,
    lists: AtomicU64,
    deletes: AtomicU64,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        Arc::new(CountingStore {
            inner,
            puts: AtomicU64::new(0),
            gets: AtomicU64::new(0),
            heads: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
        })
    }

    /// Total operations of every kind since construction.
    pub fn total_ops(&self) -> u64 {
        [
            &self.puts,
            &self.gets,
            &self.heads,
            &self.lists,
            &self.deletes,
        ]
        .into_iter()
        .map(|c| c.load(Ordering::Acquire))
        .sum()
    }

    pub fn gets(&self) -> u64 {
        self.gets.load(Ordering::Acquire)
    }

    pub fn lists(&self) -> u64 {
        self.lists.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        self.puts.fetch_add(1, Ordering::AcqRel);
        self.inner.put(key, data, opts).await
    }

    async fn get(
        &self,
        key: &str,
        range: ravel_object_store::GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        self.gets.fetch_add(1, Ordering::AcqRel);
        self.inner.get(key, range).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        self.heads.fetch_add(1, Ordering::AcqRel);
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        self.lists.fetch_add(1, Ordering::AcqRel);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        self.lists.fetch_add(1, Ordering::AcqRel);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.deletes.fetch_add(1, Ordering::AcqRel);
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits.
        ravel_object_store::Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

// ---------------------------------------------------------------------------
// Stalling store
// ---------------------------------------------------------------------------

/// A pass-through backend whose segment reads never complete.
///
/// `FaultStore` has no slow/hanging fault: every scripted fault either
/// returns an error immediately or passes through. Testing the wall deadline
/// needs a read that genuinely does not finish, otherwise the assertion turns
/// into a race between the timer and an in-memory fetch. `get` on any key
/// containing `.rseg` awaits forever; commit-record reads and every other
/// operation pass straight through, so snapshot resolution still works and
/// only the scan hangs.
pub struct StallingStore {
    inner: Arc<dyn ObjectStoreBackend>,
}

impl StallingStore {
    pub fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        Arc::new(StallingStore { inner })
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for StallingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(
        &self,
        key: &str,
        range: ravel_object_store::GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        if key.contains(".rseg") {
            std::future::pending::<()>().await;
        }
        self.inner.get(key, range).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits.
        ravel_object_store::Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

// ---------------------------------------------------------------------------
// The independent reference row source
// ---------------------------------------------------------------------------

/// One post-dedup sample, the unit the layer-2 reference executor consumes.
#[derive(Clone, Debug)]
pub struct RefRow {
    pub series_id: [u8; 16],
    pub ts: i64,
    pub value: f64,
    pub labels: LabelSet,
}

/// Materialize the deduplicated window for `snapshot` using an independent
/// greatest-wins implementation over `SegmentFetcher::fetch`.
///
/// Ranks candidates by `(created_unix_ns, writer_epoch, writer_seq,
/// in_page_index)` then `value.to_bits()`, greatest wins -- byte-for-byte
/// the order `is_greater` implements. Output is sorted by `(series_id, ts)`,
/// the canonical order the SQL pipeline emits, so a comparison against the
/// pipeline never depends on either side's internal ordering.
pub async fn reference_rows(
    fetcher: &SegmentFetcher,
    tenant_hash: TenantHash,
    snapshot: &Snapshot,
) -> Vec<RefRow> {
    type Cand = ((i64, u64, u64, u32), u64);

    let mut best: HashMap<[u8; 16], HashMap<i64, Cand>> = HashMap::new();
    let mut labels: HashMap<[u8; 16], LabelSet> = HashMap::new();

    for seg in &snapshot.segments {
        let fetched = fetcher
            .fetch(tenant_hash, seg, &[])
            .await
            .expect("reference fetch");
        for fs in fetched {
            let sid = fs.series_id.0;
            labels.entry(sid).or_insert_with(|| fs.labels.clone());
            for (idx, sample) in fs.samples.iter().enumerate() {
                let cand: Cand = (
                    (
                        fs.created_unix_ns,
                        fs.writer_epoch,
                        fs.writer_seq,
                        idx as u32,
                    ),
                    sample.value.to_bits(),
                );
                best.entry(sid)
                    .or_default()
                    .entry(sample.ts_ns)
                    .and_modify(|existing| {
                        if cand > *existing {
                            *existing = cand;
                        }
                    })
                    .or_insert(cand);
            }
        }
    }

    let mut rows: Vec<RefRow> = Vec::new();
    for (sid, by_ts) in best {
        let series_labels = labels.get(&sid).cloned().unwrap_or_default();
        for (ts, (_priority, bits)) in by_ts {
            rows.push(RefRow {
                series_id: sid,
                ts,
                value: f64::from_bits(bits),
                labels: series_labels.clone(),
            });
        }
    }
    // The canonical dedup ordering: (series_id, ts). Both sides compare
    // under it, so an unordered SQL result cannot flake the gate.
    rows.sort_by(|a, b| a.series_id.cmp(&b.series_id).then(a.ts.cmp(&b.ts)));
    rows
}
