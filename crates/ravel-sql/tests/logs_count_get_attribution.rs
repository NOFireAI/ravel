//! ADR-3492 decision D0: attribute the object-store GETs the benchmark's
//! stored-rows poll issues against an unfolded logs tenant (issue #3497).
//!
//! The harness polls `SELECT count(*) AS n FROM logs` every 15 s over
//! `[now - 7 days, now]` through `POST /api/v1/sql`, which runs it through
//! `SqlExecutor::execute_accounted_with`. This test runs that statement
//! through [`SqlExecutor::execute`], a wrapper over the same call, over a
//! `MemoryStore` holding N published, never-folded logs commit records, twice
//! in a row on one executor (one process, so its catalog caches carry over
//! between the two runs), and records the GETs per phase from the outcome's
//! phase accounting and per key prefix from a counting store underneath
//! everything.
//!
//! The catalog here differs from the server's in two ways that change the
//! figures, not the attribution. The server builds its catalog with
//! provisioning enforcement, which adds one GET of the tenant's provisioning
//! record to a cold query, so the server's cold poll is N + 2 where this test
//! pins N + 1. And the test freezes the catalog's monotonic clock, so its HEAD
//! cache (30 s TTL) never expires between the two queries, however long the
//! first one takes; in production, at the harness's 15 s interval, about every
//! other poll re-reads the HEAD, about half a GET per poll.
//!
//! Arms:
//! - N = 100, 1,000 and 5,000 at the shipped commit-record cache bound
//!   (`derive_cache_capacity_per_tenant(1, 2 s)`, 25,000 entries), so every
//!   record fits;
//! - N = 5,000 with the cache bound at 1,000 entries: the benchmark's end
//!   state in miniature, where the unfolded tail (about 179k data objects,
//!   one L0 commit record each) is larger than the 25,000-entry bound;
//! - N = 1,000 and 5,000 folded before the first query.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use datafusion::arrow::array::Int64Array;
use ravel_catalog::{Catalog, CatalogConfig, derive_cache_capacity_per_tenant};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlOutcome, SqlRequest};
use ravel_types::accounting::AccountedOp;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;
/// Records per data object, so the count (2N) cannot pass by counting objects.
const ROWS_PER_OBJECT: usize = 2;
/// The commits are spread over this many ingest hours, as a 3 h run spreads its
/// flushes.
const INGEST_HOURS: u64 = 3;
/// First ingest hour bucket; any hour well after the epoch works.
const BASE_HOUR: u32 = 500_000;
/// The harness's statement, verbatim.
const SQL: &str = "SELECT count(*) AS n FROM logs";

// ---------------------------------------------------------------------------
// GET counting by key prefix
// ---------------------------------------------------------------------------

/// The object kinds a GET can address, from the key layout in
/// docs/catalog-and-mvcc.md.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    /// `t/<th>/<signal>/c/<shard>/<hour>/<...>.cmt`.
    CommitRecord,
    /// `t/<th>/<signal>/l0/...` and `.../l1/...` objects.
    Data,
    /// `t/<th>/catalog/<signal>/HEAD` and `.../snap/<...>.csnap`.
    CatalogSnapshot,
    /// `t/<th>/catalog/<signal>/idx/...`.
    Index,
    Other,
}

const KINDS: [KeyKind; 5] = [
    KeyKind::CommitRecord,
    KeyKind::Data,
    KeyKind::CatalogSnapshot,
    KeyKind::Index,
    KeyKind::Other,
];

impl KeyKind {
    fn of(key: &str) -> Self {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.first() != Some(&"t") || parts.len() < 4 {
            return KeyKind::Other;
        }
        match (parts[2], parts[3]) {
            ("catalog", _) if key.ends_with("/HEAD") || parts.get(4) == Some(&"snap") => {
                KeyKind::CatalogSnapshot
            }
            ("catalog", _) if parts.get(4) == Some(&"idx") => KeyKind::Index,
            (_, "c") if key.ends_with(".cmt") => KeyKind::CommitRecord,
            (_, "l0" | "l1") => KeyKind::Data,
            _ => KeyKind::Other,
        }
    }

    fn index(self) -> usize {
        KINDS.iter().position(|k| *k == self).expect("listed kind")
    }

    fn name(self) -> &'static str {
        match self {
            KeyKind::CommitRecord => "commit_record",
            KeyKind::Data => "data",
            KeyKind::CatalogSnapshot => "catalog_snapshot",
            KeyKind::Index => "index",
            KeyKind::Other => "other",
        }
    }
}

/// GETs by [`KeyKind`], plus LISTs and HEADs, counted below every cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StoreCounts {
    gets: [u64; KINDS.len()],
    lists: u64,
    heads: u64,
}

impl StoreCounts {
    fn get(&self, kind: KeyKind) -> u64 {
        self.gets[kind.index()]
    }

    fn total_gets(&self) -> u64 {
        self.gets.iter().sum()
    }

    fn minus(&self, before: &StoreCounts) -> StoreCounts {
        let mut gets = [0u64; KINDS.len()];
        for (i, g) in gets.iter_mut().enumerate() {
            *g = self.gets[i] - before.gets[i];
        }
        StoreCounts {
            gets,
            lists: self.lists - before.lists,
            heads: self.heads - before.heads,
        }
    }
}

struct PrefixCountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    gets: [AtomicU64; KINDS.len()],
    lists: AtomicU64,
    heads: AtomicU64,
}

impl PrefixCountingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        Arc::new(PrefixCountingStore {
            inner,
            gets: Default::default(),
            lists: AtomicU64::new(0),
            heads: AtomicU64::new(0),
        })
    }

    fn counts(&self) -> StoreCounts {
        let mut gets = [0u64; KINDS.len()];
        for (g, c) in gets.iter_mut().zip(&self.gets) {
            *g = c.load(Ordering::Acquire);
        }
        StoreCounts {
            gets,
            lists: self.lists.load(Ordering::Acquire),
            heads: self.heads.load(Ordering::Acquire),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for PrefixCountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets[KeyKind::of(key).index()].fetch_add(1, Ordering::AcqRel);
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.heads.fetch_add(1, Ordering::AcqRel);
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.lists.fetch_add(1, Ordering::AcqRel);
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.lists.fetch_add(1, Ordering::AcqRel);
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.lists.fetch_add(1, Ordering::AcqRel);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // The refusing default `put_multipart` is inherited, so say so.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

// ---------------------------------------------------------------------------
// Tenant fixture
// ---------------------------------------------------------------------------

fn tenant() -> TenantId {
    TenantId::new("get-attribution-3497".to_string())
}

fn log_record(ts: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("line at {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Ingest hour of commit `seq` (1-based) of `n`: the commits fill
/// [`INGEST_HOURS`] consecutive hours in publish order.
fn hour_of(seq: u64, n: u64) -> u32 {
    BASE_HOUR + u32::try_from((seq - 1) * INGEST_HOURS / n).expect("small hour offset")
}

/// Write data object `seq` and publish its commit record through the real
/// commit API, as an ingest flush does.
async fn publish_object(store: &dyn ObjectStoreBackend, tenant: &TenantId, seq: u64, n: u64) {
    let hour = hour_of(seq, n);
    // Inside the hour, strictly increasing with `seq`.
    let ts0 = i64::from(hour) * NS_PER_HOUR + (seq as i64 % 3_000) * NS_PER_SEC;
    let records: Vec<LogRecord> = (0..ROWS_PER_OBJECT)
        .map(|i| log_record(ts0 + i as i64))
        .collect();
    let mut w = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: [9u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        },
    );
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(3_497),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: max,
        ingest_hour_bucket: hour,
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// The instant every arm queries at: past the seal horizon of every ingest
/// hour written (`max_flush_lifetime + clock_skew_allowance +
/// fold_safety_margin` after the last hour ends), so a fold at this instant
/// seals all of them and the unfolded and folded arms query the same window.
fn now_ns() -> i64 {
    let cfg = CatalogConfig::default();
    let last_hour_end = (i64::from(BASE_HOUR) + INGEST_HOURS as i64) * NS_PER_HOUR;
    last_hour_end
        + cfg.max_flush_lifetime_ns
        + cfg.clock_skew_allowance_ns
        + cfg.fold_safety_margin_ns
        + NS_PER_SEC
}

fn request() -> SqlRequest {
    let now = now_ns();
    SqlRequest {
        sql: SQL.to_string(),
        window: TimeRange {
            start_ns: now - 7 * NS_PER_DAY,
            end_ns: now,
        },
        min_tokens: Vec::new(),
        now_ns: now,
        deadline: Duration::from_secs(120),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// A monotonic clock stopped at zero, so no catalog HEAD cache entry expires
/// between the two queries of an arm.
struct FrozenMono;

impl ravel_cpu_gate::MonotonicClock for FrozenMono {
    fn now_nanos(&self) -> u64 {
        0
    }
}

/// A catalog over `store` whose HEAD cache ages on [`FrozenMono`].
fn frozen_catalog(store: &Arc<dyn ObjectStoreBackend>, config: CatalogConfig) -> Catalog {
    Catalog::new(Arc::clone(store), config)
        .expect("catalog")
        .with_monotonic_clock(Arc::new(FrozenMono))
}

fn catalog_config(cache_entries: usize) -> CatalogConfig {
    CatalogConfig {
        cache_capacity_per_tenant: cache_entries,
        ..CatalogConfig::default()
    }
}

/// The shipped commit-record cache bound: the server derives it from
/// `--shards` and `--max-flush-delay` (default 2 s), and at that cadence the
/// 25,000-entry cap decides it.
fn shipped_cache_entries() -> usize {
    derive_cache_capacity_per_tenant(1, Duration::from_secs(2))
}

/// `SqlConfig::default()` with the server's derived `max_segments`
/// (`DERIVED_MAX_SEGMENTS` in ravel-server, 1,000,000). The library default of
/// 1,024 refuses the folded arm: a sealed segment counts against the cap, a
/// recent unfolded one does not.
fn sql_config() -> SqlConfig {
    let mut config = SqlConfig::default();
    config.engine.max_segments = 1_000_000;
    config
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

/// One query's GETs, from the executor's phase accounting and from the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RunGets {
    resolve: u64,
    plan: u64,
    probe: u64,
    scan: u64,
    key_index: u64,
    commit_record_cache_hits: u64,
    store: StoreCounts,
}

impl RunGets {
    fn accounted_total(&self) -> u64 {
        self.resolve + self.plan + self.probe + self.scan + self.key_index
    }
}

struct Arm {
    label: &'static str,
    n: u64,
    cache_entries: usize,
    fold: bool,
}

fn count_of(outcome: &SqlOutcome) -> i64 {
    let batches = outcome.output.batches();
    assert_eq!(outcome.output.num_rows(), 1, "count(*) returns one row");
    let batch = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 count column")
        .value(0)
}

async fn run_arm(arm: &Arm) -> [RunGets; 2] {
    let tid = tenant();
    let counting = PrefixCountingStore::new(Arc::new(MemoryStore::new()));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();
    for seq in 1..=arm.n {
        publish_object(store.as_ref(), &tid, seq, arm.n).await;
    }
    if arm.fold {
        // A separate catalog, as the maintain role's fold is a separate
        // process: the query executor below starts with cold caches.
        let folder = frozen_catalog(&store, CatalogConfig::default());
        let report = folder
            .fold(
                &tid.hash(),
                Signal::Logs,
                Uuid::from_u128(1),
                now_ns(),
                &[],
                None,
            )
            .await
            .expect("fold");
        assert_eq!(
            report.watermark_hour,
            Some(hour_of(arm.n, arm.n)),
            "{}: the fold seals every written hour",
            arm.label
        );
        assert_eq!(report.entry_count, arm.n, "{}: folded entries", arm.label);
    }

    let catalog = Arc::new(frozen_catalog(&store, catalog_config(arm.cache_entries)));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        sql_config(),
        1 << 30,
    );

    let req = request();
    let mut runs = Vec::with_capacity(2);
    for _ in 0..2 {
        let before = counting.counts();
        let outcome = executor
            .execute(tid.hash(), &req)
            .await
            .expect("count(*) executes");
        let store_delta = counting.counts().minus(&before);
        let expected_rows = i64::try_from(arm.n).unwrap() * ROWS_PER_OBJECT as i64;
        assert_eq!(count_of(&outcome), expected_rows, "{}: count", arm.label);
        let pa = &outcome.phase_accounting;
        let run = RunGets {
            resolve: pa.resolve.s3_requests(AccountedOp::Get),
            plan: pa.plan.s3_requests(AccountedOp::Get),
            probe: pa.probe.s3_requests(AccountedOp::Get),
            scan: pa.scan.s3_requests(AccountedOp::Get),
            key_index: pa.key_index.s3_requests(AccountedOp::Get),
            commit_record_cache_hits: outcome.accounting.commit_record_cache_hits,
            store: store_delta,
        };
        // The accounting must explain every GET the store saw, or the phase
        // split below is attributing only part of the traffic.
        assert_eq!(
            run.accounted_total(),
            run.store.total_gets(),
            "{}: accounted GETs equal store GETs: {run:?}",
            arm.label
        );
        runs.push(run);
    }
    let runs: [RunGets; 2] = runs.try_into().expect("two runs");
    for (i, r) in runs.iter().enumerate() {
        let by_kind: Vec<String> = KINDS
            .iter()
            .map(|k| format!("{}={}", k.name(), r.store.get(*k)))
            .collect();
        println!(
            "D0 arm={} n={} cache_entries={} fold={} run={} gets: resolve={} plan={} probe={} \
             scan={} key_index={} | by key: {} | lists={} heads={} commit_record_cache_hits={}",
            arm.label,
            arm.n,
            arm.cache_entries,
            arm.fold,
            i + 1,
            r.resolve,
            r.plan,
            r.probe,
            r.scan,
            r.key_index,
            by_kind.join(" "),
            r.store.lists,
            r.store.heads,
            r.commit_record_cache_hits,
        );
    }
    runs
}

/// The figures a run is expected to show: GETs only in the resolve phase,
/// `commit_records` of them on commit records and `snapshot` on the catalog
/// HEAD/snapshot keys, none on data or index objects.
fn resolve_only(commit_records: u64, snapshot: u64, lists: u64, cache_hits: u64) -> RunGets {
    let mut gets = [0u64; KINDS.len()];
    gets[KeyKind::CommitRecord.index()] = commit_records;
    gets[KeyKind::CatalogSnapshot.index()] = snapshot;
    RunGets {
        resolve: commit_records + snapshot,
        plan: 0,
        probe: 0,
        scan: 0,
        key_index: 0,
        commit_record_cache_hits: cache_hits,
        store: StoreCounts {
            gets,
            lists,
            heads: 0,
        },
    }
}

/// Below the cache bound, the first query on a process issues one resolve GET
/// per unfolded commit record plus one catalog HEAD GET (N + 1, exactly linear
/// with slope 1), and nothing in plan, probe or scan: the count is answered
/// from the records' `sample_count`. The repeat query issues no GET at all,
/// only the commit-prefix LISTs, because every record is served from the
/// catalog's per-process commit-record cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unfolded_count_issues_one_get_per_commit_record_once_per_process() {
    let shipped = shipped_cache_entries();
    assert_eq!(shipped, 25_000, "the shipped commit-record cache bound");

    // (N, commit-prefix LISTs per query).
    let cases = [(100u64, 2u64), (1_000, 3), (5_000, 7)];
    let mut first_run_gets = Vec::new();
    for (n, lists) in cases {
        let arm = Arm {
            label: "unfolded",
            n,
            cache_entries: shipped,
            fold: false,
        };
        let [first, second] = run_arm(&arm).await;
        assert_eq!(first, resolve_only(n, 1, lists, 0), "N={n}: first query");
        assert_eq!(second, resolve_only(0, 0, lists, n), "N={n}: repeat query");
        first_run_gets.push((n, first.store.total_gets()));
    }
    // GETs per cold query = N + 1 at every N: slope 1, intercept 1.
    for (n, gets) in &first_run_gets {
        assert_eq!(*gets, n + 1, "N={n}: cold GETs are N + 1");
    }
}

/// Past the cache bound the repeat query is no longer free: it refetches every
/// record the bound could not hold, N - bound GETs per query, every query. At
/// the benchmark's end state (about 179k unfolded records against the shipped
/// 25,000) the same arithmetic would give at least about 154k GETs per poll.
/// It is a lower bound: these records declare no column statistics, so the
/// 25,000-entry cap binds before the cache's byte budget, and records that
/// carry statistics fill the byte budget with fewer entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unfolded_count_past_the_cache_bound_refetches_the_overflow_every_query() {
    let (n, bound) = (5_000u64, 1_000usize);
    let arm = Arm {
        label: "over-cache-bound",
        n,
        cache_entries: bound,
        fold: false,
    };
    let [first, second] = run_arm(&arm).await;
    let bound = bound as u64;
    assert_eq!(first, resolve_only(n, 1, 7, 0), "first query");
    assert_eq!(second, resolve_only(n - bound, 0, 7, bound), "repeat query");
}

/// Folding first takes the commit records out of the query path: the cold
/// query reads the catalog HEAD and one snapshot part (2 GETs, whatever N),
/// the repeat query reads nothing, and neither lists more than 2 pages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn folded_count_reads_the_snapshot_not_the_commit_records() {
    for n in [1_000u64, 5_000] {
        let arm = Arm {
            label: "folded",
            n,
            cache_entries: shipped_cache_entries(),
            fold: true,
        };
        let [first, second] = run_arm(&arm).await;
        assert_eq!(first, resolve_only(0, 2, 2, 0), "N={n}: first query");
        assert_eq!(second, resolve_only(0, 0, 2, 0), "N={n}: repeat query");
    }
}
