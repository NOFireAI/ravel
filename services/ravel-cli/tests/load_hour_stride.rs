//! `ravel-cli load --ingest-hour-stride/--ingest-hour-start` and `ravel-cli
//! catalog fold --snapshot-part-max-entries` (ADR-3692 decision 8, issue
//! #3700).
//!
//! The load tests drive `ravel_cli::load::run_load_reporting`, the function
//! `load::run` (what `main.rs` dispatches `load` to) calls with the system
//! clock, in-process against a shared `MemoryStore` and a pinned clock. The
//! fold tests drive `ravel_cli::catalog::fold_with_part_cap`, what `main.rs`
//! dispatches `catalog fold` to. A subprocess against `--store memory` gets an
//! empty store of its own, so only the refusals that need no prior state run
//! through the built binary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::{ArrayRef, BinaryArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load::{
    self, DEFAULT_DECODE_QUEUE_BATCHES, HourStrideBatchError, INGEST_HOUR_STRIDE_REFUSED_EXIT,
    IngestHourStride, IngestHourStrideRefused, LoadError, LoadLayoutReport, LoadMemoryRequest,
};
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_ingest::{Clock, RlogChunkOrder, RlogGroupTargetBlocks, RlogZstdLevel};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{TenantId, TimeRange};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_SECOND: i64 = 1_000_000_000;

/// The test's "real" clock: 2023-11-14T22:13:20Z, in unix hour 472_222.
const CLOCK_NS: i64 = 1_700_000_000_000_000_000;
const NOW_HOUR: u32 = (CLOCK_NS / NS_PER_HOUR) as u32;

const TENANT: &str = "acme";
const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

const MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"
"#;

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// A store that counts PUTs (single and multipart) and, when `observed` is
/// set, reports that reading as the store's own clock, as S3's `Date` header
/// does.
struct ObservingStore {
    inner: MemoryStore,
    puts: AtomicU64,
    observed: Option<i64>,
}

impl ObservingStore {
    fn new(observed: Option<i64>) -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryStore::new(),
            puts: AtomicU64::new(0),
            observed,
        })
    }

    fn puts(&self) -> u64 {
        self.puts.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for ObservingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.observed
    }
}

/// `batches * batch_rows` rows; batch `i`'s rows carry event times inside
/// `first_hour + i * stride` and `svc` values cycling through `services`
/// names.
fn write_fixture(
    path: &Path,
    batches: usize,
    batch_rows: usize,
    first_hour: u32,
    stride: u32,
    services: usize,
) {
    let rows = batches * batch_rows;
    let ts: Vec<i64> = (0..rows)
        .map(|j| {
            let hour = i64::from(first_hour) + (j / batch_rows) as i64 * i64::from(stride);
            hour * NS_PER_HOUR + ((j % batch_rows) as i64 + 1) * NS_PER_SECOND
        })
        .collect();
    let body: Vec<String> = (0..rows).map(|j| format!("row {j}")).collect();
    let svc: Vec<String> = (0..rows).map(|j| format!("svc-{}", j % services)).collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", Arc::new(Int64Array::from(ts)) as ArrayRef),
        ("body", Arc::new(StringArray::from(body)) as ArrayRef),
        ("svc", Arc::new(StringArray::from(svc)) as ArrayRef),
    ])
    .expect("batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

struct Fixture {
    _dir: tempfile::TempDir,
    parquet: std::path::PathBuf,
    mapping: std::path::PathBuf,
}

fn fixture(batches: usize, batch_rows: usize, first_hour: u32, stride: u32) -> Fixture {
    fixture_with_services(batches, batch_rows, first_hour, stride, 1)
}

fn fixture_with_services(
    batches: usize,
    batch_rows: usize,
    first_hour: u32,
    stride: u32,
    services: usize,
) -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = dir.path().join("logs.parquet");
    let mapping = dir.path().join("mapping.toml");
    std::fs::write(&mapping, MAPPING).expect("write mapping");
    write_fixture(&parquet, batches, batch_rows, first_hour, stride, services);
    Fixture {
        _dir: dir,
        parquet,
        mapping,
    }
}

/// One logs `load` through the entry point `load::run` calls, with every
/// lever at its CLI default and the clock pinned at `CLOCK_NS`.
async fn load_with(
    store: Arc<dyn ObjectStoreBackend>,
    fx: &Fixture,
    shards: u32,
    batch_rows: usize,
    stride: Option<IngestHourStride>,
) -> anyhow::Result<LoadLayoutReport> {
    load_signal(store, fx, SignalArg::Logs, shards, batch_rows, 0, stride).await
}

/// [`load_with`] for any signal and `--skip-rows`.
async fn load_signal(
    store: Arc<dyn ObjectStoreBackend>,
    fx: &Fixture,
    signal: SignalArg,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
    stride: Option<IngestHourStride>,
) -> anyhow::Result<LoadLayoutReport> {
    let mut warnings = Vec::new();
    load::run_load_reporting(
        store,
        &fx.parquet,
        TENANT,
        &fx.mapping,
        signal,
        shards,
        batch_rows,
        skip_rows,
        None,
        None,
        1,
        DEFAULT_DECODE_QUEUE_BATCHES,
        None,
        None,
        None,
        RlogZstdLevel::DEFAULT,
        RlogGroupTargetBlocks::DEFAULT,
        RlogChunkOrder::DEFAULT,
        LoadMemoryRequest::Flag(256 << 20),
        false,
        &ravel_cli::ipcrypt::IpcryptPolicy::empty(),
        stride,
        CLOCK_NS,
        Arc::new(FixedClock(CLOCK_NS)),
        &mut warnings,
    )
    .await
}

fn stride(stride_hours: u32, start_hour: u32) -> Option<IngestHourStride> {
    Some(IngestHourStride {
        stride_hours,
        start_hour,
    })
}

fn tenant_hex() -> String {
    TenantId::new(TENANT).hash().to_hex()
}

/// Every logs commit record of the tenant, decoded.
async fn commit_records(
    store: &dyn ObjectStoreBackend,
) -> Vec<ravel_proto::commit::v1::CommitRecord> {
    signal_commit_records(store, "l").await
}

/// Every commit record of the tenant under signal key prefix `signal`.
async fn signal_commit_records(
    store: &dyn ObjectStoreBackend,
    signal: &str,
) -> Vec<ravel_proto::commit::v1::CommitRecord> {
    let prefix = format!("t/{}/{signal}/c/", tenant_hex());
    let mut records = Vec::new();
    for meta in list_all(store, &prefix).await.expect("list commits") {
        let data = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get commit")
            .data;
        records.push(ravel_commit::record::decode(&data).expect("commit record decodes"));
    }
    records
}

fn hour_of(ns: i64) -> u32 {
    u32::try_from(ns.div_euclid(NS_PER_HOUR)).expect("hour fits u32")
}

async fn fold(
    store: &Arc<dyn ObjectStoreBackend>,
    part_cap: Option<usize>,
    json: bool,
) -> anyhow::Result<(ravel_catalog::FoldReport, String)> {
    ravel_cli::catalog::fold_with_part_cap(
        Arc::clone(store),
        MEMORY,
        TENANT,
        1,
        SignalArg::Logs,
        None,
        true,
        false,
        part_cap,
        CLOCK_NS,
        json,
    )
    .await
}

async fn query_rows(store: &Arc<dyn ObjectStoreBackend>, window: TimeRange) -> usize {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig::default(),
        1 << 30,
    );
    let req = SqlRequest {
        sql: "SELECT ts, body FROM logs".to_string(),
        window,
        min_tokens: Vec::new(),
        now_ns: CLOCK_NS + 1_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    };
    executor
        .execute(TenantId::new(TENANT).hash(), &req)
        .await
        .expect("logs query executes")
        .output
        .num_rows()
}

/// The lane's typed stop, out of a load error.
fn lane_stop(err: &anyhow::Error) -> (&HourStrideBatchError, usize) {
    match err.downcast_ref::<LoadError>() {
        Some(LoadError::HourStride { error, durable, .. }) => (error, durable.len()),
        _ => panic!("expected LoadError::HourStride, got: {err:#}"),
    }
}

fn refusal(err: &anyhow::Error) -> String {
    err.downcast_ref::<IngestHourStrideRefused>()
        .unwrap_or_else(|| panic!("expected an IngestHourStrideRefused, got: {err:#}"))
        .0
        .clone()
}

/// 24 one-hour batches from 48 hours back fold, at a two-entry part cap, into
/// exactly 12 parts tiling the 24 hours in order; every commit record's
/// ingest hour bucket and created_unix_ns hour are its batch's hour, and a
/// whole-range query returns every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stride_load_folds_into_hour_ranged_parts() {
    let start = NOW_HOUR - 48;
    let fx = fixture(24, 2, start, 1);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load_with(Arc::clone(&store), &fx, 1, 2, stride(1, start))
        .await
        .expect("strided load succeeds");
    assert_eq!(report.objects_written, 24);

    let records = commit_records(store.as_ref()).await;
    assert_eq!(records.len(), 24, "one flush per batch on one shard");
    let mut buckets: Vec<u32> = Vec::new();
    for record in &records {
        let batch_hour = hour_of(record.min_event_ts_ns);
        assert_eq!(
            hour_of(record.max_event_ts_ns),
            batch_hour,
            "one record holds one batch"
        );
        assert_eq!(
            record.ingest_hour_bucket, batch_hour,
            "bucket is the batch's hour"
        );
        assert!(record.ingest_hour_bucket <= hour_of(record.created_unix_ns));
        assert_eq!(
            hour_of(record.created_unix_ns),
            batch_hour,
            "created_unix_ns is read off the shifted clock too"
        );
        buckets.push(record.ingest_hour_bucket);
    }
    buckets.sort_unstable();
    assert_eq!(buckets, (start..start + 24).collect::<Vec<_>>());

    let (fold_report, _) = fold(&store, Some(2), true).await.expect("fold succeeds");
    // `--writers-stopped` seals through the fold's own current hour.
    assert_eq!(fold_report.watermark_hour, Some(NOW_HOUR));
    let head_key = format!("t/{}/catalog/l/HEAD", tenant_hex());
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("HEAD exists")
        .data;
    let head = ravel_catalog::decode_head(&head_bytes).expect("HEAD decodes");
    let tiling: Vec<(u32, u32, u64)> = head
        .parts
        .iter()
        .map(|p| (p.min_hour, p.watermark_hour, p.entry_count))
        .collect();
    // Each part holds two consecutive loaded hours. The last part's watermark
    // is the HEAD's: the empty sealed hours after the load belong to it.
    let mut expected: Vec<(u32, u32, u64)> = (0..12)
        .map(|k| (start + 2 * k, start + 2 * k + 1, 2))
        .collect();
    expected[11].1 = NOW_HOUR;
    assert_eq!(tiling, expected, "12 two-hour parts, in order, no gap");

    let window = TimeRange {
        start_ns: i64::from(start) * NS_PER_HOUR,
        end_ns: i64::from(start + 24) * NS_PER_HOUR,
    };
    assert_eq!(query_rows(&store, window).await, 48, "every row is visible");
}

/// With two shards and eight services per batch, each batch flushes on both
/// shards, and both flushes land in the batch's hour: the clock moves per
/// batch, not per flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_flush_of_a_batch_shares_its_hour() {
    let start = NOW_HOUR - 30;
    let fx = fixture_with_services(6, 8, start, 3, 8);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load_with(Arc::clone(&store), &fx, 2, 8, stride(3, start))
        .await
        .expect("strided load succeeds");

    let mut per_hour: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for record in commit_records(store.as_ref()).await {
        assert_eq!(
            record.ingest_hour_bucket,
            hour_of(record.min_event_ts_ns),
            "shard {}: bucket is the batch's hour",
            record.shard
        );
        assert_eq!(hour_of(record.max_event_ts_ns), record.ingest_hour_bucket);
        assert_eq!(hour_of(record.created_unix_ns), record.ingest_hour_bucket);
        per_hour
            .entry(record.ingest_hour_bucket)
            .or_default()
            .push(record.shard);
    }
    let expected: BTreeMap<u32, Vec<u32>> = (0..6).map(|i| (start + 3 * i, vec![0, 1])).collect();
    for shards in per_hour.values_mut() {
        shards.sort_unstable();
    }
    assert_eq!(
        per_hour, expected,
        "each batch's hour holds one flush per shard"
    );
}

/// A store whose own clock reads the real `CLOCK_NS` would refuse a flush
/// whose writer clock reads 40 hours earlier as skewed; the lane shifts the
/// store's observed clock by the same offset, so the strided load succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stride_load_passes_the_store_clock_check() {
    let start = NOW_HOUR - 40;
    let fx = fixture(3, 2, start, 1);
    let store = ObservingStore::new(Some(CLOCK_NS));
    let report = load_with(store.clone(), &fx, 1, 2, stride(1, start))
        .await
        .expect("strided load succeeds against a store reporting the real time");
    assert_eq!(report.objects_written, 3);
    let buckets: Vec<u32> = commit_records(store.as_ref())
        .await
        .iter()
        .map(|r| r.ingest_hour_bucket)
        .collect();
    assert_eq!(buckets.len(), 3);
    assert!(buckets.iter().all(|b| (start..start + 3).contains(b)));
}

/// A tenant with a logs HEAD is refused before any PUT, and the refusal names
/// the HEAD's watermark hour.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stride_is_refused_on_a_tenant_with_a_head() {
    let start = NOW_HOUR - 48;
    let fx = fixture(4, 2, start, 1);
    let store = ObservingStore::new(None);
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    load_with(Arc::clone(&dyn_store), &fx, 1, 2, stride(1, start))
        .await
        .expect("first strided load succeeds");
    fold(&dyn_store, None, true).await.expect("fold succeeds");

    let puts = store.puts();
    let err = load_with(Arc::clone(&dyn_store), &fx, 1, 2, stride(1, start))
        .await
        .expect_err("second strided load is refused");
    assert_eq!(
        refusal(&err),
        format!(
            "ingest-hour-stride is only honored on a tenant whose logs HEAD does not exist \
             (found watermark hour {NOW_HOUR})"
        )
    );
    assert_eq!(store.puts(), puts, "refused before any PUT");
}

/// A tenant with logs commit records and no HEAD is refused before any PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stride_is_refused_on_a_tenant_with_commit_records() {
    let start = NOW_HOUR - 48;
    let fx = fixture(2, 2, start, 1);
    let store = ObservingStore::new(None);
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    load_with(Arc::clone(&dyn_store), &fx, 1, 2, None)
        .await
        .expect("plain load succeeds");
    let head_key = format!("t/{}/catalog/l/HEAD", tenant_hex());
    assert!(
        matches!(dyn_store.head(&head_key).await, Err(StoreError::NotFound)),
        "a plain load writes no HEAD"
    );

    let puts = store.puts();
    let err = load_with(Arc::clone(&dyn_store), &fx, 1, 2, stride(1, start))
        .await
        .expect_err("strided load is refused");
    let reason = refusal(&err);
    let prefix = format!("t/{}/l/c/", tenant_hex());
    assert!(
        reason.starts_with(
            "ingest-hour-stride is only honored on a tenant with no logs commit records (found "
        ) && reason.ends_with(&format!(" under {prefix})")),
        "{reason}"
    );
    assert_eq!(store.puts(), puts, "refused before any PUT");
}

/// A start in the current hour, and a range that would reach the current
/// hour, are each refused before any PUT, naming both figures.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stride_into_the_future_is_refused() {
    let store = ObservingStore::new(None);
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();

    let fx = fixture(2, 2, NOW_HOUR, 1);
    let err = load_with(Arc::clone(&dyn_store), &fx, 1, 2, stride(1, NOW_HOUR))
        .await
        .expect_err("a start in the current hour is refused");
    assert_eq!(
        refusal(&err),
        format!(
            "--ingest-hour-start {NOW_HOUR} is not strictly in the past: the current ingest \
             hour is {NOW_HOUR}"
        )
    );

    let start = NOW_HOUR - 10;
    let fx = fixture(5, 2, start, 2);
    let err = load_with(Arc::clone(&dyn_store), &fx, 1, 2, stride(2, start))
        .await
        .expect_err("a range reaching the current hour is refused");
    assert_eq!(
        refusal(&err),
        format!(
            "--ingest-hour-start {start} plus 5 batches times --ingest-hour-stride 2 reaches \
             hour {NOW_HOUR}, which is not below the current ingest hour {NOW_HOUR}; move the \
             start back or load fewer batches"
        )
    );

    // One hour further back fits: the last batch lands in NOW_HOUR - 2.
    let start = NOW_HOUR - 11;
    let fx = fixture(5, 2, start, 2);
    assert_eq!(store.puts(), 0, "both refusals came before any PUT");
    load_with(Arc::clone(&dyn_store), &fx, 1, 2, stride(2, start))
        .await
        .expect("a range ending below the current hour loads");
}

/// The load report names the synthetic hours; a plain load's report has none
/// of the four fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn report_names_the_synthetic_hours() {
    let start = NOW_HOUR - 20;
    let fx = fixture(3, 2, start, 2);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load_with(Arc::clone(&store), &fx, 1, 2, stride(2, start))
        .await
        .expect("strided load succeeds");
    let json = serde_json::to_value(&report).expect("report serializes");
    assert_eq!(json["ingest_hour_stride"], serde_json::json!(2));
    assert_eq!(json["ingest_hour_start"], serde_json::json!(start));
    assert_eq!(json["ingest_hour_last"], serde_json::json!(start + 4));
    assert_eq!(json["synthetic_ingest_hours"], serde_json::json!(true));
    assert_eq!(
        json["ingest_hour_flush"],
        serde_json::json!("per_batch_target_bytes")
    );

    let plain_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let plain = load_with(plain_store, &fx, 1, 2, None)
        .await
        .expect("plain load succeeds");
    let plain = serde_json::to_value(&plain).expect("report serializes");
    for key in [
        "ingest_hour_stride",
        "ingest_hour_start",
        "ingest_hour_last",
        "synthetic_ingest_hours",
        "ingest_hour_flush",
    ] {
        assert!(plain.get(key).is_none(), "a plain load reports no {key}");
    }
}

/// `--snapshot-part-max-entries 7` is named in both fold reports, the default
/// is 250,000, and 0 is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fold_report_names_the_part_cap() {
    let start = NOW_HOUR - 48;
    let fx = fixture(2, 2, start, 1);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load_with(Arc::clone(&store), &fx, 1, 2, stride(1, start))
        .await
        .expect("strided load succeeds");

    let err = fold(&store, Some(0), true)
        .await
        .expect_err("a zero part cap is refused");
    assert!(
        err.to_string()
            .contains("--snapshot-part-max-entries must be at least 1"),
        "{err:#}"
    );

    let (_, text) = fold(&store, Some(7), true).await.expect("fold succeeds");
    let json: serde_json::Value = serde_json::from_str(&text).expect("fold JSON");
    assert_eq!(json["snapshot_part_max_entries"], serde_json::json!(7));

    let (_, human) = fold(&store, Some(7), false).await.expect("fold succeeds");
    assert!(
        human
            .lines()
            .any(|l| l.trim() == "snapshot_part_max_entries: 7"),
        "{human}"
    );

    let (_, text) = fold(&store, None, true).await.expect("fold succeeds");
    let json: serde_json::Value = serde_json::from_str(&text).expect("fold JSON");
    assert_eq!(
        json["snapshot_part_max_entries"],
        serde_json::json!(250_000)
    );
}

const SPANS_MAPPING: &str = r#"
[spans]
trace_id_column = "trace_id"
span_id_column  = "span_id"
name_column     = "name"
start_ts_column = "start_ns"
start_ts_unit   = "nanos"
end_ts_column   = "end_ns"
end_ts_unit     = "nanos"
"#;

/// `batches * batch_rows` spans in row groups of `row_group_rows`; batch `i`'s
/// spans start and end inside hour `first_hour + i * stride`, and every span
/// has its own trace id.
fn spans_fixture(
    batches: usize,
    batch_rows: usize,
    row_group_rows: usize,
    first_hour: u32,
    stride: u32,
) -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = dir.path().join("spans.parquet");
    let mapping = dir.path().join("mapping.toml");
    std::fs::write(&mapping, SPANS_MAPPING).expect("write mapping");
    let rows = batches * batch_rows;
    let ts: Vec<i64> = (0..rows)
        .map(|j| {
            let hour = i64::from(first_hour) + (j / batch_rows) as i64 * i64::from(stride);
            hour * NS_PER_HOUR + ((j % batch_rows) as i64 + 1) * NS_PER_SECOND
        })
        .collect();
    // Row j's trace id routes to shard j % 2 of two, so every batch reaches
    // both shards.
    let mut candidates = (1u64..).map(|n| {
        let mut id = [0xa5; 16];
        id[8..].copy_from_slice(&n.to_be_bytes());
        id
    });
    let trace_ids: Vec<Vec<u8>> = (0..rows)
        .map(|j| {
            let shard = (j % 2) as u32;
            candidates
                .find(|id| ravel_ingest::shard_for_span(id, 2) == shard)
                .expect("a trace id for the shard")
                .to_vec()
        })
        .collect();
    let span_ids: Vec<Vec<u8>> = (0..rows)
        .map(|j| (j as u64 + 1).to_be_bytes().to_vec())
        .collect();
    let names: Vec<String> = (0..rows).map(|j| format!("span {j}")).collect();
    let bin = |vals: &[Vec<u8>]| {
        Arc::new(BinaryArray::from(
            vals.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        )) as ArrayRef
    };
    let batch = RecordBatch::try_from_iter(vec![
        ("trace_id", bin(&trace_ids)),
        ("span_id", bin(&span_ids)),
        ("name", Arc::new(StringArray::from(names)) as ArrayRef),
        ("start_ns", Arc::new(Int64Array::from(ts.clone())) as ArrayRef),
        ("end_ns", Arc::new(Int64Array::from(ts)) as ArrayRef),
    ])
    .expect("batch");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let file = std::fs::File::create(&parquet).expect("create parquet");
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), Some(props)).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    Fixture {
        _dir: dir,
        parquet,
        mapping,
    }
}

/// A strided spans load over 5-row row groups at `--batch-rows 8`: each of the
/// six batches lands in its planned hour on both shards, once each, and the
/// report names the last hour.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strided_spans_land_one_record_per_shard_in_each_planned_hour() {
    let start = NOW_HOUR - 30;
    let fx = spans_fixture(6, 8, 5, start, 3);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load_signal(
        Arc::clone(&store),
        &fx,
        SignalArg::Spans,
        2,
        8,
        0,
        stride(3, start),
    )
    .await
    .expect("strided spans load succeeds");
    let json = serde_json::to_value(&report).expect("report serializes");
    assert_eq!(json["ingest_hour_last"], serde_json::json!(start + 15));
    assert_eq!(json["synthetic_ingest_hours"], serde_json::json!(true));

    let mut per_hour: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for record in signal_commit_records(store.as_ref(), "s").await {
        let planned = hour_of(record.min_event_ts_ns);
        assert_eq!(hour_of(record.max_event_ts_ns), planned, "one batch per record");
        assert_eq!(record.ingest_hour_bucket, planned, "bucket is the planned hour");
        assert_eq!(
            hour_of(record.created_unix_ns),
            planned,
            "created hour is the planned hour"
        );
        per_hour.entry(planned).or_default().push(record.shard);
    }
    for shards in per_hour.values_mut() {
        shards.sort_unstable();
    }
    let expected: BTreeMap<u32, Vec<u32>> = (0..6).map(|i| (start + 3 * i, vec![0, 1])).collect();
    assert_eq!(per_hour, expected, "exactly one record per shard per hour");
}

/// A HEAD request that fails with a store error, rather than NotFound, fails
/// the load (a plain error, exit 1) before any PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_head_store_error_fails_the_load_before_any_put() {
    let start = NOW_HOUR - 48;
    let fx = fixture(2, 2, start, 1);
    let observing = ObservingStore::new(None);
    let faults = Arc::new(FaultStore::new(
        Arc::clone(&observing),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Head, ScriptedFault::Permanent("head refused".into()))
                .with_key_contains("catalog/l/HEAD"),
        ),
    ));
    let err = load_with(faults.clone(), &fx, 1, 2, stride(1, start))
        .await
        .expect_err("a HEAD store error fails the load");
    assert!(
        err.downcast_ref::<IngestHourStrideRefused>().is_none(),
        "a store error is not a refusal: {err:#}"
    );
    assert!(
        err.to_string()
            .contains("--ingest-hour-stride: could not check for the logs catalog HEAD"),
        "{err:#}"
    );
    assert!(faults.fault_count(Op::Head, FaultKind::Permanent) >= 1);
    assert_eq!(observing.puts(), 0, "failed before any PUT");
}

/// A LIST of the commit prefix that fails with a store error fails the load
/// (a plain error, exit 1) before any PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_list_store_error_fails_the_load_before_any_put() {
    let start = NOW_HOUR - 48;
    let fx = fixture(2, 2, start, 1);
    let observing = ObservingStore::new(None);
    let faults = Arc::new(FaultStore::new(
        Arc::clone(&observing),
        FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Permanent("list refused".into()))
                .with_key_contains(format!("t/{}/l/c/", tenant_hex())),
        ),
    ));
    let err = load_with(faults.clone(), &fx, 1, 2, stride(1, start))
        .await
        .expect_err("a LIST store error fails the load");
    assert!(
        err.downcast_ref::<IngestHourStrideRefused>().is_none(),
        "a store error is not a refusal: {err:#}"
    );
    assert!(
        err.to_string()
            .contains(&format!("--ingest-hour-stride: could not list t/{}/l/c/", tenant_hex())),
        "{err:#}"
    );
    assert!(faults.fault_count(Op::List, FaultKind::Permanent) >= 1);
    assert_eq!(observing.puts(), 0, "failed before any PUT");
}

/// A store that runs a real `catalog fold --writers-stopped` the first time
/// the logs HEAD is asked for once `fold_after` commit records have landed,
/// as a fold process racing the load would. Counts the load's PUTs only.
struct FoldingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    commit_puts: AtomicU64,
    puts: AtomicU64,
    heads: AtomicU64,
    fold_after: u64,
    puts_at_fold: std::sync::Mutex<Option<u64>>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for FoldingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        if key.contains("/l/c/") {
            self.commit_puts.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        if key.ends_with("/catalog/l/HEAD") {
            self.heads.fetch_add(1, Ordering::SeqCst);
            let due = self.commit_puts.load(Ordering::SeqCst) >= self.fold_after;
            let first = self.puts_at_fold.lock().expect("lock").is_none();
            if due && first {
                *self.puts_at_fold.lock().expect("lock") = Some(self.puts.load(Ordering::SeqCst));
                fold(&self.inner, None, true)
                    .await
                    .expect("the racing fold succeeds");
            }
        }
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// A HEAD written by a fold between batch 2 and batch 3 stops the load at
/// batch 3, which writes nothing; batches 0 to 2 stay durable in their hours.
/// The lane asks for the HEAD once in its guard and once per batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_head_that_appears_mid_load_stops_the_next_batch() {
    let start = NOW_HOUR - 48;
    let fx = fixture(6, 2, start, 1);
    let store = Arc::new(FoldingStore {
        inner: Arc::new(MemoryStore::new()),
        commit_puts: AtomicU64::new(0),
        puts: AtomicU64::new(0),
        heads: AtomicU64::new(0),
        fold_after: 3,
        puts_at_fold: std::sync::Mutex::new(None),
    });
    let err = load_with(store.clone(), &fx, 1, 2, stride(1, start))
        .await
        .expect_err("the load stops once the HEAD appears");
    let (error, durable) = lane_stop(&err);
    assert_eq!(
        error,
        &HourStrideBatchError::HeadAppeared {
            batch: 3,
            signal: "logs",
            watermark: NOW_HOUR.to_string(),
        }
    );
    assert_eq!(durable, 3, "batches 0 to 2 are reported durable");
    let puts_at_fold = store
        .puts_at_fold
        .lock()
        .expect("lock")
        .expect("the fold ran");
    assert_eq!(
        store.puts.load(Ordering::SeqCst),
        puts_at_fold,
        "batch 3 made zero PUTs"
    );
    assert_eq!(
        store.heads.load(Ordering::SeqCst),
        5,
        "one HEAD in the guard and one before each of batches 0 to 3"
    );
    let mut buckets: Vec<u32> = commit_records(store.inner.as_ref())
        .await
        .iter()
        .map(|r| {
            assert_eq!(r.ingest_hour_bucket, hour_of(r.min_event_ts_ns));
            r.ingest_hour_bucket
        })
        .collect();
    buckets.sort_unstable();
    assert_eq!(buckets, vec![start, start + 1, start + 2], "batches 0 to 2 intact");
}

/// A start below the write path's plausible-clock floor is refused before any
/// PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_below_the_clock_floor_is_refused() {
    let fx = fixture(2, 2, 0, 1);
    let store = ObservingStore::new(None);
    let err = load_with(store.clone(), &fx, 1, 2, stride(1, 0))
        .await
        .expect_err("start 0 is refused");
    assert_eq!(
        refusal(&err),
        "--ingest-hour-start 0 is below hour 438288, the earliest ingest clock the write path \
         accepts (MIN_PLAUSIBLE_INGEST_CLOCK_NS, 2020-01-01T00:00:00Z)"
    );
    assert_eq!(store.puts(), 0, "refused before any PUT");
}

/// A batch whose latest event time reaches its hour's end plus the catalog's
/// two-hour max_ingest_lag is refused before it is submitted; the batch before
/// it is durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_past_its_hour_plus_the_lag_is_refused() {
    let start = NOW_HOUR - 48;
    // Event hours start, start + 4, start + 8 against load hours start,
    // start + 1, start + 2: batch 1's events begin at its bound.
    let fx = fixture(3, 2, start, 4);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let err = load_with(Arc::clone(&store), &fx, 1, 2, stride(1, start))
        .await
        .expect_err("batch 1 is refused");
    let (error, durable) = lane_stop(&err);
    let bound_ns = i64::from(start + 2) * NS_PER_HOUR + 2 * NS_PER_HOUR;
    assert_eq!(
        error,
        &HourStrideBatchError::EventTimePastLag {
            batch: 1,
            hour: start + 1,
            max_event_ns: i64::from(start + 4) * NS_PER_HOUR + 2 * NS_PER_SECOND,
            bound_ns,
            lag_ns: 2 * NS_PER_HOUR,
        }
    );
    assert_eq!(durable, 1);
    let records = commit_records(store.as_ref()).await;
    assert_eq!(records.len(), 1, "batch 1 wrote no commit record");
    assert_eq!(records[0].ingest_hour_bucket, start);
    assert_eq!(hour_of(records[0].max_event_ts_ns), start);
}

/// The commit-prefix check walks past a listing page that holds only
/// unaddressable keys to the commit record on the next page.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_commit_check_reads_past_a_page_of_unaddressable_keys() {
    let start = NOW_HOUR - 48;
    let fx = fixture(2, 2, start, 1);
    let memory = Arc::new(MemoryStore::with_page_size(2));
    let store: Arc<dyn ObjectStoreBackend> = memory.clone();
    load_with(Arc::clone(&store), &fx, 1, 2, None)
        .await
        .expect("plain load succeeds");
    let prefix = format!("t/{}/l/c/", tenant_hex());
    let commits = list_all(store.as_ref(), &prefix).await.expect("list");
    assert!(!commits.is_empty());
    for name in ["!a*", "!b*"] {
        memory.insert_foreign(&format!("{prefix}{name}"), bytes::Bytes::from_static(b"x"));
    }
    let first = store.list(&prefix, None).await.expect("first page");
    assert!(
        first.objects.is_empty() && first.unaddressable.len() == 2 && first.next.is_some(),
        "the first page holds only the two unaddressable keys: {first:?}"
    );

    let err = load_with(Arc::clone(&store), &fx, 1, 2, stride(1, start))
        .await
        .expect_err("strided load is refused");
    assert_eq!(
        refusal(&err),
        format!(
            "ingest-hour-stride is only honored on a tenant with no logs commit records (found \
             {} under {prefix})",
            commits[0].key
        )
    );
}

/// A strided logs load whose `--skip-rows` covers the file writes nothing and
/// still reports its lane, with no last hour.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_row_strided_logs_load_reports_its_lane() {
    let start = NOW_HOUR - 20;
    let fx = fixture(3, 2, start, 1);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load_signal(
        Arc::clone(&store),
        &fx,
        SignalArg::Logs,
        1,
        2,
        6,
        stride(1, start),
    )
    .await
    .expect("a load that skips every row succeeds");
    assert_eq!(report.objects_written, 0);
    let json = serde_json::to_value(&report).expect("report serializes");
    assert_eq!(json["ingest_hour_stride"], serde_json::json!(1));
    assert_eq!(json["ingest_hour_start"], serde_json::json!(start));
    assert_eq!(json["ingest_hour_last"], serde_json::Value::Null);
    assert_eq!(json["synthetic_ingest_hours"], serde_json::json!(true));
    assert_eq!(
        json["ingest_hour_flush"],
        serde_json::json!("per_batch_target_bytes")
    );
}

fn cli() -> std::process::Command {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("RAVEL_") {
            cmd.env_remove(key);
        }
    }
    cmd.args(["--store", "memory", "--tenant-hash-unkeyed", "load"]);
    cmd
}

/// The built binary exits 2 when one of the two flags is given alone (clap's
/// `requires`) and when the guard refuses a stride.
#[test]
fn the_binary_exits_two_on_a_lone_flag_and_on_a_refusal() {
    let fx = fixture(2, 2, 400_000, 1);
    for (given, missing) in [
        ("--ingest-hour-stride", "--ingest-hour-start"),
        ("--ingest-hour-start", "--ingest-hour-stride"),
    ] {
        let out = cli()
            .args(["--signal", "logs", "--tenant", TENANT, "--parquet"])
            .arg(&fx.parquet)
            .arg("--mapping")
            .arg(&fx.mapping)
            .args([given, "1"])
            .output()
            .expect("ravel-cli runs");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{stderr}");
        assert!(stderr.contains(missing), "{stderr}");
    }

    // An hour far past any real clock's current hour.
    let out = cli()
        .args(["--signal", "logs", "--tenant", TENANT, "--parquet"])
        .arg(&fx.parquet)
        .arg("--mapping")
        .arg(&fx.mapping)
        .args([
            "--ingest-hour-stride",
            "1",
            "--ingest-hour-start",
            "4000000000",
        ])
        .output()
        .expect("ravel-cli runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(INGEST_HOUR_STRIDE_REFUSED_EXIT),
        "{stderr}"
    );
    assert_eq!(INGEST_HOUR_STRIDE_REFUSED_EXIT, 2);
    assert!(
        stderr.contains("--ingest-hour-start 4000000000 is not strictly in the past"),
        "{stderr}"
    );
}
