//! ADR-2677 decision 1, issue #2679: `ravel-cli load --fold-after-load` seals
//! the hours the load wrote, so the first query after the load resolves them
//! from the folded snapshot instead of listing them.
//!
//! The load runs through `ravel_cli::load::load_with_fold_after_load` (the
//! logs entry point `--fold-after-load` dispatches to) with an injected clock
//! fixed inside hour `H`, ten-odd minutes in, where the seal margin alone seals
//! only `H - 3`. The query runs afterwards through the real `/api/v1/sql`
//! handler with its own clock in hour `H + 1`, behind a store that records
//! every LIST, and is compared against a control load of the same file without
//! the flag.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load::{self, LoadReport, Mapping};
use ravel_commit::keys;
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_types::TenantId;
use serde_json::Value;
use tower::ServiceExt;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// The loader's clock: 2023-11-14T22:13:20Z, thirteen minutes into its hour.
const CLOCK_NS: i64 = 1_700_000_000_000_000_000;
const HOUR: u32 = (CLOCK_NS / NS_PER_HOUR) as u32;

/// The load's start-of-run `now_ns`, one nanosecond before the clock's
/// reading, so a fold that used it instead of a fresh clock reading is told
/// apart by HEAD's `created_unix_ns`.
const LOAD_NOW_NS: i64 = CLOCK_NS - 1;

/// The query's clock: a minute into hour `H + 1`, so the default window (the
/// hour ending at now) covers the loaded rows and the resolve's listing runs
/// to `H + 1`.
const QUERY_NOW_NS: i64 = (HOUR as i64 + 1) * NS_PER_HOUR + 60_000_000_000;

/// How long every PUT of the logs HEAD is held. Only a fold writes it, so the
/// fold takes at least this long and an `elapsed` that left the fold out
/// would come in under it.
const HEAD_PUT_DELAY: Duration = Duration::from_millis(500);

/// How long every GET of the logs HEAD is held once a HEAD PUT has landed.
/// The `--fold-after-load` coverage check reads the HEAD its fold just wrote,
/// so a fold `elapsed` that left the check out would come in under
/// `HEAD_PUT_DELAY + HEAD_GET_DELAY`.
const HEAD_GET_DELAY: Duration = Duration::from_millis(500);

const TENANT: &str = "acme";
const TOKEN: &str = "acme-token";
const SHARDS: u32 = 2;
const ROWS: usize = 40;

struct FixedClock(Arc<AtomicI64>);

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Seals the logs snapshot through hour `HOUR` (a writers-stopped fold by
/// another process) when the first commit record is PUT, before that PUT
/// lands, then moves the load's clock to `advance_to`.
struct SealMidLoad {
    fired: AtomicBool,
    clock: Arc<AtomicI64>,
    advance_to: i64,
}

/// Seals the logs snapshot through hour `HOUR` (a writers-stopped fold by
/// another process) at the first GET of the logs HEAD once `commits` commit
/// records have landed: after the load's last commit, before its own fold
/// reads HEAD.
struct SealBeforeFold {
    commits: usize,
    fired: AtomicBool,
    report: Mutex<Option<ravel_catalog::FoldReport>>,
}

/// One step of a [`CommitScript`], run when the `at_commit`-th commit record
/// (1-based) is PUT, before that PUT lands: first a fold by another process
/// at the clock's current reading when `fold` is `Some` (its value is the
/// seal-through hour, `None` for the margin alone), then the clock moves to
/// `clock_after`.
struct ScriptStep {
    at_commit: usize,
    fold: Option<Option<u32>>,
    clock_after: i64,
}

/// Drives the load's clock and other processes' folds from the order of its
/// commit record PUTs.
struct CommitScript {
    clock: Arc<AtomicI64>,
    steps: Vec<ScriptStep>,
    /// The ingest hour of every commit record PUT, in PUT order.
    commit_hours: Mutex<Vec<u32>>,
    /// The watermark each scripted fold left, in step order.
    fold_watermarks: Mutex<Vec<Option<u32>>>,
}

impl CommitScript {
    async fn on_commit_put(&self, inner: &Arc<dyn ObjectStoreBackend>, hour: u32) {
        let index = {
            let mut hours = self.commit_hours.lock().unwrap();
            hours.push(hour);
            hours.len()
        };
        for step in self.steps.iter().filter(|step| step.at_commit == index) {
            if let Some(seal_through) = step.fold {
                let catalog = Catalog::new(
                    Arc::clone(inner),
                    CatalogConfig {
                        shard_count: SHARDS,
                        ..CatalogConfig::default()
                    },
                )
                .expect("catalog");
                let fold = catalog
                    .fold_with_seal_through(
                        &TenantId::new(TENANT).hash(),
                        ravel_types::Signal::Logs,
                        uuid::Uuid::new_v4(),
                        self.clock.load(Ordering::SeqCst),
                        &[],
                        None,
                        &ravel_catalog::RefoldRequest::new(),
                        seal_through,
                    )
                    .await
                    .expect("the scripted fold");
                self.fold_watermarks
                    .lock()
                    .unwrap()
                    .push(fold.watermark_hour);
            }
            self.clock.store(step.clock_after, Ordering::SeqCst);
        }
    }
}

/// Records every LIST prefix, every key a LIST returned and every PUT key,
/// and holds each PUT of a key ending in `head_suffix` for
/// [`HEAD_PUT_DELAY`] and each later GET of one for [`HEAD_GET_DELAY`].
struct RecordingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    head_suffix: String,
    head_written: AtomicBool,
    /// When set, every GET of the HEAD after one is written is refused.
    refuse_written_head_reads: AtomicBool,
    prefixes: Mutex<Vec<String>>,
    listed_keys: Mutex<Vec<String>>,
    put_keys: Mutex<Vec<String>>,
    seal_mid_load: Option<SealMidLoad>,
    seal_before_fold: Option<SealBeforeFold>,
    script: Option<CommitScript>,
}

impl RecordingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        Arc::new(Self::build(inner))
    }

    fn sealing_mid_load(
        inner: Arc<dyn ObjectStoreBackend>,
        clock: Arc<AtomicI64>,
        advance_to: i64,
    ) -> Arc<Self> {
        let mut store = Self::build(inner);
        store.seal_mid_load = Some(SealMidLoad {
            fired: AtomicBool::new(false),
            clock,
            advance_to,
        });
        Arc::new(store)
    }

    fn sealing_before_fold(inner: Arc<dyn ObjectStoreBackend>, commits: usize) -> Arc<Self> {
        let mut store = Self::build(inner);
        store.seal_before_fold = Some(SealBeforeFold {
            commits,
            fired: AtomicBool::new(false),
            report: Mutex::new(None),
        });
        Arc::new(store)
    }

    fn scripted(
        inner: Arc<dyn ObjectStoreBackend>,
        clock: Arc<AtomicI64>,
        steps: Vec<ScriptStep>,
    ) -> Arc<Self> {
        let mut store = Self::build(inner);
        store.script = Some(CommitScript {
            clock,
            steps,
            commit_hours: Mutex::new(Vec::new()),
            fold_watermarks: Mutex::new(Vec::new()),
        });
        Arc::new(store)
    }

    fn build(inner: Arc<dyn ObjectStoreBackend>) -> Self {
        let head_suffix = format!("/catalog/{}/HEAD", ravel_types::Signal::Logs.key_prefix());
        Self {
            inner,
            head_suffix,
            head_written: AtomicBool::new(false),
            refuse_written_head_reads: AtomicBool::new(false),
            prefixes: Mutex::new(Vec::new()),
            listed_keys: Mutex::new(Vec::new()),
            put_keys: Mutex::new(Vec::new()),
            seal_mid_load: None,
            seal_before_fold: None,
            script: None,
        }
    }

    /// PUT keys of data objects and commit records, in PUT order.
    fn data_and_commit_puts(&self) -> Vec<String> {
        self.put_keys
            .lock()
            .unwrap()
            .iter()
            .filter(|key| is_data_key(key) || keys::parse_commit_key(key).is_ok())
            .cloned()
            .collect()
    }

    fn commit_put_count(&self) -> usize {
        self.put_keys
            .lock()
            .unwrap()
            .iter()
            .filter(|key| keys::parse_commit_key(key).is_ok())
            .count()
    }

    fn put_count(&self) -> usize {
        self.put_keys.lock().unwrap().len()
    }

    fn reset(&self) {
        self.prefixes.lock().unwrap().clear();
        self.listed_keys.lock().unwrap().clear();
    }

    fn list_count(&self) -> usize {
        self.prefixes.lock().unwrap().len()
    }

    fn listed_keys(&self) -> Vec<String> {
        self.listed_keys.lock().unwrap().clone()
    }

    fn note(&self, prefix: &str, page: &ListPage) {
        self.prefixes.lock().unwrap().push(prefix.to_string());
        let mut keys = self.listed_keys.lock().unwrap();
        keys.extend(page.objects.iter().map(|meta| meta.key.clone()));
    }
}

#[async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(&self, k: &str, d: Bytes, o: PutOptions) -> Result<PutOutcome, StoreError> {
        self.put_keys.lock().unwrap().push(k.to_string());
        if let Some(hook) = &self.seal_mid_load
            && keys::parse_commit_key(k).is_ok()
            && !hook.fired.swap(true, Ordering::SeqCst)
        {
            let catalog = Catalog::new(
                Arc::clone(&self.inner),
                CatalogConfig {
                    shard_count: SHARDS,
                    ..CatalogConfig::default()
                },
            )
            .expect("catalog");
            let fold = catalog
                .fold_with_seal_through(
                    &TenantId::new(TENANT).hash(),
                    ravel_types::Signal::Logs,
                    uuid::Uuid::new_v4(),
                    hook.clock.load(Ordering::SeqCst),
                    &[],
                    None,
                    &ravel_catalog::RefoldRequest::new(),
                    Some(HOUR),
                )
                .await
                .expect("the mid-load seal");
            assert_eq!(fold.watermark_hour, Some(HOUR), "{fold:?}");
            hook.clock.store(hook.advance_to, Ordering::SeqCst);
        }
        if let Some(script) = &self.script
            && let Ok(parsed) = keys::parse_commit_key(k)
        {
            script
                .on_commit_put(&self.inner, parsed.ingest_hour_bucket)
                .await;
        }
        if !k.ends_with(&self.head_suffix) {
            return self.inner.put(k, d, o).await;
        }
        tokio::time::sleep(HEAD_PUT_DELAY).await;
        let outcome = self.inner.put(k, d, o).await;
        if outcome.is_ok() {
            self.head_written.store(true, Ordering::SeqCst);
        }
        outcome
    }
    async fn get(&self, k: &str, r: GetRange) -> Result<GetOutcome, StoreError> {
        if let Some(hook) = &self.seal_before_fold
            && k.ends_with(&self.head_suffix)
            && self.commit_put_count() == hook.commits
            && !hook.fired.swap(true, Ordering::SeqCst)
        {
            let catalog = Catalog::new(
                Arc::clone(&self.inner),
                CatalogConfig {
                    shard_count: SHARDS,
                    ..CatalogConfig::default()
                },
            )
            .expect("catalog");
            let fold = catalog
                .fold_with_seal_through(
                    &TenantId::new(TENANT).hash(),
                    ravel_types::Signal::Logs,
                    uuid::Uuid::new_v4(),
                    CLOCK_NS,
                    &[],
                    None,
                    &ravel_catalog::RefoldRequest::new(),
                    Some(HOUR),
                )
                .await
                .expect("the seal before the load's fold");
            *hook.report.lock().unwrap() = Some(fold);
        }
        if k.ends_with(&self.head_suffix) && self.head_written.load(Ordering::SeqCst) {
            if self.refuse_written_head_reads.load(Ordering::SeqCst) {
                return Err(StoreError::AccessDenied("HEAD read refused".to_string()));
            }
            tokio::time::sleep(HEAD_GET_DELAY).await;
        }
        self.inner.get(k, r).await
    }
    async fn head(&self, k: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(k).await
    }
    async fn list(&self, p: &str, t: Option<PageToken>) -> Result<ListPage, StoreError> {
        let page = self.inner.list(p, t).await?;
        self.note(p, &page);
        Ok(page)
    }
    async fn list_after(
        &self,
        p: &str,
        start_after: Option<&str>,
        t: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let page = self.inner.list_after(p, start_after, t).await?;
        self.note(p, &page);
        Ok(page)
    }
    async fn list_delimited(&self, p: &str) -> Result<DelimitedList, StoreError> {
        self.prefixes.lock().unwrap().push(p.to_string());
        self.inner.list_delimited(p).await
    }
    async fn delete(&self, k: &str) -> Result<(), StoreError> {
        self.inner.delete(k).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// A logs data object: `t/<tenant>/<signal>/l0/...`.
fn is_data_key(key: &str) -> bool {
    key.contains(&format!("/{}/l0/", ravel_types::Signal::Logs.key_prefix()))
}

const MAPPING: &str = "ts_column = \"ts\"\n\
                       ts_unit = \"nanos\"\n\
                       body_column = \"body\"\n\
                       \n\
                       [[resource_attribute]]\n\
                       key = \"service.name\"\n\
                       column = \"svc\"\n\
                       type = \"str\"\n";

/// The fixture Parquet file, with `mapping.toml` written beside it.
fn write_fixture() -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("logs.parquet");
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "ts",
            Arc::new(Int64Array::from(
                (0..ROWS).map(|r| CLOCK_NS + r as i64).collect::<Vec<_>>(),
            )),
        ),
        (
            "body",
            Arc::new(StringArray::from(
                (0..ROWS).map(|r| format!("row{r}")).collect::<Vec<_>>(),
            )),
        ),
        (
            "svc",
            Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|r| format!("svc{}", r % 4))
                    .collect::<Vec<_>>(),
            )),
        ),
    ];
    let batch = RecordBatch::try_from_iter(columns).expect("record batch");
    let file = std::fs::File::create(&parquet_path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    std::fs::write(dir.path().join("mapping.toml"), MAPPING).expect("write mapping");
    let mapping = load::parse_mapping(MAPPING).expect("valid mapping");
    (dir, parquet_path, mapping)
}

async fn run_load(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    mapping: &Mapping,
    fold_after_load: bool,
) -> LoadReport {
    let clock = Arc::new(AtomicI64::new(CLOCK_NS));
    try_run_load(store, parquet_path, mapping, fold_after_load, clock)
        .await
        .expect("load succeeds")
}

async fn try_run_load(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    mapping: &Mapping,
    fold_after_load: bool,
    clock: Arc<AtomicI64>,
) -> Result<LoadReport, load::LoadError> {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock(clock));
    if fold_after_load {
        load::load_with_fold_after_load(
            store,
            parquet_path,
            TENANT,
            mapping,
            SHARDS,
            10,
            None,
            1,
            LOAD_NOW_NS,
            clock,
        )
        .await
    } else {
        load::load(
            store,
            parquet_path,
            TENANT,
            mapping,
            SHARDS,
            10,
            None,
            1,
            LOAD_NOW_NS,
            clock,
        )
        .await
    }
}

fn build_app(store: Arc<dyn ObjectStoreBackend>) -> Router {
    let config = CatalogConfig {
        shard_count: SHARDS,
        ..CatalogConfig::default()
    };
    let catalog = Arc::new(Catalog::new(Arc::clone(&store), config).expect("catalog"));
    let tokens = HashMap::from([(TOKEN.to_string(), TenantId::new(TENANT))]);
    let mut state = ravel_server::query::build_sql_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        None,
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    )
    .expect("build_sql_state");
    state.clock = Arc::new(FixedClock(Arc::new(AtomicI64::new(QUERY_NOW_NS))));
    ravel_server::sql::router(state)
}

async fn count_rows(app: &Router) -> Value {
    let payload =
        serde_json::json!({ "query": "SELECT COUNT(*) FROM logs", "timeout": 60.0 }).to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::from(payload))
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).expect("JSON body");
    assert_eq!(status, StatusCode::OK, "query failed: {value}");
    value
}

fn count_of(value: &Value) -> i64 {
    value["data"]["rows"][0][0]
        .as_i64()
        .unwrap_or_else(|| panic!("no count in {value}"))
}

fn resolve_lists(value: &Value) -> u64 {
    value["stats"]["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|phase| phase["phase"] == "resolve")
        .unwrap_or_else(|| panic!("no resolve phase in {value}"))["s3ListRequests"]
        .as_u64()
        .unwrap_or_else(|| panic!("no resolve s3ListRequests in {value}"))
}

fn unfolded_segments(value: &Value) -> u64 {
    value["stats"]["io"]["unfoldedSegmentsResolved"]
        .as_u64()
        .unwrap_or_else(|| panic!("no io.unfoldedSegmentsResolved in {value}"))
}

/// Listed keys that carry the loaded hour `H` in their path: commit records of
/// an hour the fold sealed, which a resolve from the snapshot never lists.
fn sealed_hour_keys(store: &RecordingStore) -> Vec<String> {
    let marker = format!("/{}/", keys::ingest_hour_string(HOUR));
    store
        .listed_keys()
        .into_iter()
        .filter(|key| key.contains(&marker))
        .collect()
}

#[tokio::test]
async fn load_with_fold_after_load_resolves_from_the_snapshot_through_sql() {
    let (_dir, parquet_path, mapping) = write_fixture();

    // The control: the same file without the flag, queried at the same now.
    let control_store = RecordingStore::new(Arc::new(MemoryStore::new()));
    let control_report = run_load(
        Arc::clone(&control_store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        false,
    )
    .await;
    assert_eq!(control_report.rows_processed, ROWS as u64);
    assert_eq!(control_report.fold, None, "no flag, no fold figures");
    control_store.reset();
    let control = count_rows(&build_app(
        Arc::clone(&control_store) as Arc<dyn ObjectStoreBackend>
    ))
    .await;
    assert_eq!(count_of(&control), ROWS as i64, "control count: {control}");
    assert!(
        unfolded_segments(&control) > 0,
        "the control resolves its segments from the listing: {control}"
    );
    assert!(
        !sealed_hour_keys(&control_store).is_empty(),
        "the control lists hour {HOUR}'s commit records"
    );

    // The load under test.
    let store = RecordingStore::new(Arc::new(MemoryStore::new()));
    let report = run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
    )
    .await;
    assert_eq!(report.rows_processed, ROWS as u64);
    assert!(
        report.tokens.iter().all(|t| t.ingest_hour_bucket == HOUR),
        "every object lands in the clock's hour {HOUR}"
    );
    let fold = report
        .fold
        .clone()
        .expect("--fold-after-load reports a fold");
    assert!(!fold.no_op, "the fold sealed the loaded hour: {fold:?}");
    assert_eq!(fold.seal_through_hour, Some(HOUR), "{fold:?}");
    assert_eq!(fold.watermark_hour, Some(HOUR), "{fold:?}");
    assert_eq!(
        fold.entry_count,
        report.objects_written() as u64,
        "every loaded object is a snapshot entry: {fold:?}"
    );
    assert!(
        fold.elapsed >= HEAD_PUT_DELAY + HEAD_GET_DELAY,
        "the fold's time covers its HEAD write and the coverage check's HEAD read: {fold:?}"
    );
    assert!(
        report.elapsed >= fold.elapsed,
        "the load's elapsed {:?} includes the fold's {:?}",
        report.elapsed,
        fold.elapsed
    );

    // HEAD was written at the clock's fresh reading: the fold did not move
    // the clock to make the margin seal hour H, and did not reuse the
    // load's start-of-run now.
    let tenant_hash = TenantId::new(TENANT).hash();
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash.to_hex(),
        ravel_types::Signal::Logs.key_prefix()
    );
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("the fold wrote HEAD")
        .data;
    let head = ravel_catalog::decode_head(&head_bytes).expect("HEAD decodes");
    assert_eq!(head.watermark_hour, HOUR);
    assert_eq!(head.created_unix_ns, CLOCK_NS);
    assert_eq!(
        fold.parts_read,
        head.parts.len(),
        "the coverage check read every part once: {fold:?}"
    );
    assert_eq!(fold.buckets_listed, 0, "every commit is a level-0 entry");
    assert_eq!(fold.records_read, 0, "{fold:?}");

    // Only now query, and only the query's LISTs are counted.
    store.reset();
    let folded = count_rows(&build_app(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)).await;
    assert_eq!(count_of(&folded), ROWS as i64, "folded count: {folded}");
    assert_eq!(count_of(&folded), count_of(&control));
    assert_eq!(
        unfolded_segments(&folded),
        0,
        "every segment resolves from the snapshot: {folded}"
    );
    assert_eq!(
        resolve_lists(&folded),
        u64::from(SHARDS) + 1,
        "one bounded LIST per shard plus the erasure LIST: {folded}"
    );
    assert_eq!(
        store.list_count(),
        SHARDS as usize + 1,
        "the store saw exactly the resolve's LISTs"
    );
    assert_eq!(
        sealed_hour_keys(&store),
        Vec::<String>::new(),
        "no commit record of the sealed hour {HOUR} is listed"
    );
}

/// A second `--fold-after-load` in an hour the first one sealed is refused by
/// the preflight before any row is read: every object it wrote would sit in
/// a sealed hour, invisible to queries without a commit token. Nothing is
/// written, not even the provisioning record, and the query still counts the
/// first load's rows. The same load once the next hour begins is accepted.
#[tokio::test]
async fn a_second_load_into_a_sealed_hour_is_refused_before_anything_is_written() {
    let (_dir, parquet_path, mapping) = write_fixture();
    let store = RecordingStore::new(Arc::new(MemoryStore::new()));
    let first = run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
    )
    .await;
    assert!(!first.fold.as_ref().expect("first fold").no_op);
    let data_and_commits = store.data_and_commit_puts();
    assert_eq!(
        data_and_commits.len(),
        2 * first.objects_written(),
        "one data object and one commit record per object: {data_and_commits:?}"
    );
    let puts = store.put_count();

    let err = try_run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
        Arc::new(AtomicI64::new(CLOCK_NS)),
    )
    .await
    .expect_err("the second load is refused");
    match &err {
        load::LoadError::HourAlreadySealed {
            tenant,
            hour,
            watermark_hour,
            first_open_hour,
        } => {
            assert_eq!(tenant, TENANT);
            assert_eq!(*hour, HOUR);
            assert_eq!(*watermark_hour, HOUR);
            assert_eq!(*first_open_hour, u64::from(HOUR) + 1);
        }
        other => panic!("expected HourAlreadySealed, got {other:?}"),
    }
    let message = err.to_string();
    for needle in [
        "already sealed",
        "commit token",
        "wait until hour",
        "Rebuild the snapshot",
    ] {
        assert!(message.contains(needle), "{needle:?} in {message}");
    }
    assert!(err.durable_tokens().is_empty());
    assert_eq!(
        store.data_and_commit_puts(),
        data_and_commits,
        "the refused load PUT no data object and no commit record"
    );
    assert_eq!(store.put_count(), puts, "the refused load PUT nothing");

    let value = count_rows(&build_app(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)).await;
    assert_eq!(count_of(&value), ROWS as i64, "{value}");

    // The first way out the refusal names: once hour H + 1 begins the same
    // file loads, though its rows' timestamps are still in hour H, and its
    // fold seals H + 1.
    let third = try_run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
        Arc::new(AtomicI64::new(
            (i64::from(HOUR) + 1) * NS_PER_HOUR + 30_000_000_000,
        )),
    )
    .await
    .expect("a load in the next hour is not refused");
    let fold = third.fold.expect("third fold");
    assert!(!fold.no_op, "{fold:?}");
    assert_eq!(fold.watermark_hour, Some(HOUR + 1), "{fold:?}");
    let value = count_rows(&build_app(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)).await;
    assert_eq!(
        count_of(&value),
        2 * ROWS as i64,
        "both loads are visible: {value}"
    );
}

/// The crossing case: another writers-stopped fold seals hour `H` after the
/// preflight passed, just before this load's first commit record lands, and
/// the load's clock then moves into `H + 1`. The load's own fold runs at
/// `H + 1:00:30`, before `H`'s natural seal, so `H` is still held open to
/// late commits: the fold re-lists it and picks up the commits published
/// after the other seal. The load succeeds, every token is in the snapshot,
/// and a token-less query counts every row.
#[tokio::test]
async fn a_seal_landing_mid_load_is_folded_by_the_loads_own_fold() {
    let (_dir, parquet_path, mapping) = write_fixture();
    // Half a minute either side of the H + 1 boundary, so no flush open
    // across the move outlives its flush lifetime.
    let next_hour_ns = (i64::from(HOUR) + 1) * NS_PER_HOUR;
    let clock = Arc::new(AtomicI64::new(next_hour_ns - 30_000_000_000));
    let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let store = RecordingStore::sealing_mid_load(
        Arc::clone(&inner),
        Arc::clone(&clock),
        next_hour_ns + 30_000_000_000,
    );
    let report = try_run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
        clock,
    )
    .await
    .expect("the load's fold picks up the commits after the other seal");
    assert!(
        store
            .seal_mid_load
            .as_ref()
            .expect("hook")
            .fired
            .load(Ordering::SeqCst),
        "the mid-load seal ran"
    );
    assert_eq!(report.rows_processed, ROWS as u64, "every row was written");
    assert_eq!(report.tokens.len(), report.objects_written());
    let token_hours: std::collections::BTreeSet<u32> =
        report.tokens.iter().map(|t| t.ingest_hour_bucket).collect();
    assert_eq!(
        token_hours,
        [HOUR, HOUR + 1].into_iter().collect(),
        "the load wrote into both hours"
    );
    let fold = report.fold.clone().expect("the fold ran");
    assert!(!fold.no_op, "this fold sealed something: {fold:?}");
    assert_eq!(fold.seal_through_hour, Some(HOUR + 1), "{fold:?}");
    assert_eq!(fold.watermark_hour, Some(HOUR + 1), "{fold:?}");
    assert_eq!(fold.entry_count, report.tokens.len() as u64, "{fold:?}");

    let coverage = ravel_catalog::snapshot_coverage(
        inner.as_ref(),
        &TenantId::new(TENANT).hash(),
        ravel_types::Signal::Logs,
        &identities(&report.tokens),
    )
    .await
    .expect("readable");
    assert_eq!(coverage.watermark_hour, Some(HOUR + 1));
    assert!(coverage.missing.is_empty(), "{coverage:?}");

    let value = count_rows(&build_app(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)).await;
    assert_eq!(
        count_of(&value),
        ROWS as i64,
        "every row is visible: {value}"
    );
    assert_eq!(
        unfolded_segments(&value),
        0,
        "every segment resolves from the snapshot: {value}"
    );
}

/// Another process's writers-stopped fold seals the current hour `H` between
/// the load's first and second commit, and the load stays inside `H`. The
/// load's own fold seals through `H` as well, so it does not advance the
/// watermark; `H` is held open, so the fold re-lists it, folds the seven
/// commits the other seal missed, and the load succeeds.
#[tokio::test]
async fn a_seal_landing_mid_load_inside_one_hour_is_folded_by_the_loads_own_fold() {
    let (_dir, parquet_path, mapping) = write_fixture();
    let clock = Arc::new(AtomicI64::new(CLOCK_NS));
    let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let store = RecordingStore::scripted(
        Arc::clone(&inner),
        Arc::clone(&clock),
        vec![ScriptStep {
            at_commit: 2,
            fold: Some(Some(HOUR)),
            clock_after: CLOCK_NS,
        }],
    );
    let report = try_run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
        clock,
    )
    .await
    .expect("the load's fold picks up the commits after the other seal");
    let hours = commit_hours(&store);
    assert_eq!(hours, vec![HOUR; 8], "eight commits into H");
    assert_eq!(fold_watermarks(&store), vec![Some(HOUR)], "the other seal");
    let fold = report.fold.clone().expect("the fold ran");
    assert!(!fold.no_op, "the fold found the late commits: {fold:?}");
    assert_eq!(fold.seal_through_hour, Some(HOUR), "{fold:?}");
    assert_eq!(fold.watermark_hour, Some(HOUR), "{fold:?}");
    assert_eq!(fold.entry_count, 8, "{fold:?}");

    let coverage = ravel_catalog::snapshot_coverage(
        inner.as_ref(),
        &TenantId::new(TENANT).hash(),
        ravel_types::Signal::Logs,
        &identities(&report.tokens),
    )
    .await
    .expect("readable");
    assert!(coverage.missing.is_empty(), "{coverage:?}");

    let value = count_rows(&build_app(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)).await;
    assert_eq!(
        count_of(&value),
        ROWS as i64,
        "every row is visible: {value}"
    );
    assert_eq!(
        unfolded_segments(&value),
        0,
        "every segment resolves from the snapshot: {value}"
    );
}

/// The token identities of `tokens` as snapshot entry identities.
fn identities(tokens: &[ravel_types::CommitToken]) -> Vec<ravel_catalog::EntryIdentity> {
    tokens
        .iter()
        .map(|t| {
            (
                t.shard,
                t.ingest_hour_bucket,
                *t.writer_id.as_bytes(),
                t.epoch,
                t.seq,
            )
        })
        .collect()
}

/// Another writers-stopped fold seals hour `H` after the load's last commit
/// and before the load's own fold, so that fold has nothing left to seal. The
/// snapshot the other fold left holds every commit the load wrote, so the
/// load succeeds with a no-op fold and a token-less query counts every row.
#[tokio::test]
async fn a_seal_after_the_last_commit_leaves_a_no_op_fold_that_succeeds() {
    const COMMITS: usize = 8;
    let (_dir, parquet_path, mapping) = write_fixture();
    let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let store = RecordingStore::sealing_before_fold(Arc::clone(&inner), COMMITS);
    let report = try_run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
        Arc::new(AtomicI64::new(CLOCK_NS)),
    )
    .await
    .expect("every commit is in the snapshot the other fold left");
    assert_eq!(report.tokens.len(), COMMITS, "{:?}", report.tokens);
    assert!(report.tokens.iter().all(|t| t.ingest_hour_bucket == HOUR));
    let hook = store.seal_before_fold.as_ref().expect("hook");
    assert!(hook.fired.load(Ordering::SeqCst), "the other seal ran");
    let other = hook.report.lock().unwrap().clone().expect("the other fold");
    assert!(!other.no_op, "the other fold sealed hour {HOUR}: {other:?}");
    assert_eq!(other.watermark_hour, Some(HOUR), "{other:?}");
    assert_eq!(other.entry_count, COMMITS as u64, "{other:?}");

    let fold = report.fold.clone().expect("the fold ran");
    assert!(fold.no_op, "the load's fold had nothing to seal: {fold:?}");
    assert_eq!(fold.seal_through_hour, Some(HOUR), "{fold:?}");
    assert_eq!(fold.watermark_hour, Some(HOUR), "{fold:?}");
    assert_eq!(fold.buckets_listed, 0, "every commit is a level-0 entry");

    let coverage = ravel_catalog::snapshot_coverage(
        inner.as_ref(),
        &TenantId::new(TENANT).hash(),
        ravel_types::Signal::Logs,
        &identities(&report.tokens),
    )
    .await
    .expect("readable");
    assert_eq!(coverage.watermark_hour, Some(HOUR));
    assert!(coverage.missing.is_empty(), "{coverage:?}");

    let value = count_rows(&build_app(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)).await;
    assert_eq!(count_of(&value), ROWS as i64, "{value}");
    assert_eq!(
        unfolded_segments(&value),
        0,
        "every segment resolves from the snapshot: {value}"
    );
}

/// The value after `label` up to the next `,` or ` `, parsed.
fn field<T: std::str::FromStr>(line: &str, label: &str) -> T {
    let rest = line
        .split_once(label)
        .unwrap_or_else(|| panic!("no {label:?} in {line:?}"))
        .1;
    let end = rest.find([',', ' ']).unwrap_or(rest.len());
    rest[..end]
        .trim_end_matches('s')
        .parse()
        .unwrap_or_else(|_| panic!("{label:?} in {line:?} does not parse"))
}

/// The real binary's summary carries the fold's figures on its own line, and
/// the summary's `elapsed` is at least the fold's. The binary's clock is the
/// system clock, so the hour is read back from the line rather than pinned:
/// seal-through and watermark must agree, and every object is an entry.
#[test]
fn the_load_summary_prints_the_fold_figures() {
    let (dir, parquet_path, _mapping) = write_fixture();
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("RAVEL_") {
            cmd.env_remove(key);
        }
    }
    let output = cmd
        .args(["--store", "memory", "--tenant-hash-unkeyed", "load"])
        .arg("--parquet")
        .arg(&parquet_path)
        .arg("--mapping")
        .arg(dir.path().join("mapping.toml"))
        .args(["--tenant", TENANT, "--shards", "2", "--batch-rows", "10"])
        .arg("--fold-after-load")
        .output()
        .expect("ravel-cli runs");
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf-8");
    assert!(output.status.success(), "load fails:\n{stdout}\n{stderr}");

    let line = |prefix: &str| -> String {
        let found: Vec<&str> = stdout.lines().filter(|l| l.starts_with(prefix)).collect();
        assert_eq!(found.len(), 1, "one {prefix:?} line in:\n{stdout}");
        found[0].to_string()
    };
    let fold = line("  fold after load  : ");
    let objects: u64 = field(&line("  objects written  : "), ": ");
    let elapsed: f64 = field(&line("  elapsed          : "), ": ");
    assert!(
        fold.starts_with("  fold after load  : sealed, "),
        "the fold sealed something: {fold}"
    );
    let seal_through: u32 = field(&fold, "seal_through_hour ");
    let watermark: u32 = field(&fold, "watermark_hour ");
    assert_eq!(seal_through, watermark, "{fold}");
    assert_eq!(field::<u64>(&fold, "entries "), objects, "{fold}");
    assert_eq!(field::<usize>(&fold, "parts read "), 1, "{fold}");
    assert_eq!(field::<usize>(&fold, "buckets listed "), 0, "{fold}");
    assert_eq!(field::<usize>(&fold, "records read "), 0, "{fold}");
    let fold_elapsed: f64 = field(&fold, "elapsed ");
    assert!(
        fold.ends_with("(included in elapsed)"),
        "the line says where its time is counted: {fold}"
    );
    assert!(
        elapsed >= fold_elapsed,
        "summary elapsed {elapsed} includes the fold's {fold_elapsed}"
    );
}

/// `minutes:seconds` into hour `HOUR + hour_offset`, in nanoseconds.
fn at(hour_offset: i64, minutes: i64, seconds: i64) -> i64 {
    (i64::from(HOUR) + hour_offset) * NS_PER_HOUR + (minutes * 60 + seconds) * 1_000_000_000
}

/// The ingest hour of each commit record the load PUT, in PUT order: shard 0
/// then shard 1 for each of its four batches.
fn commit_hours(store: &RecordingStore) -> Vec<u32> {
    store
        .script
        .as_ref()
        .expect("script")
        .commit_hours
        .lock()
        .unwrap()
        .clone()
}

fn fold_watermarks(store: &RecordingStore) -> Vec<Option<u32>> {
    store
        .script
        .as_ref()
        .expect("script")
        .fold_watermarks
        .lock()
        .unwrap()
        .clone()
}

/// A load long enough to commit into `H`, `H + 1` and `H + 2`. Another
/// process's writers-stopped fold seals `H` between the load's first and
/// second commit into `H`, and the load's own fold runs at `H + 2:45`, past
/// the full seal margin for `H`, so the margin alone would seal `H` at that
/// time as well. `H` was held open to late commits only until its natural
/// seal at `H + 2:20`, and no fold ran before then, so the second commit into
/// `H` is outside the snapshot and the load fails naming `H`.
#[tokio::test]
async fn a_seal_landing_mid_load_is_caught_after_the_margin_has_passed() {
    let (_dir, parquet_path, mapping) = write_fixture();
    let clock = Arc::new(AtomicI64::new(CLOCK_NS));
    let fold_at = at(2, 45, 0);
    let store = RecordingStore::scripted(
        Arc::new(MemoryStore::new()),
        Arc::clone(&clock),
        vec![
            ScriptStep {
                at_commit: 2,
                fold: Some(Some(HOUR)),
                clock_after: at(1, 5, 0),
            },
            ScriptStep {
                at_commit: 4,
                fold: None,
                clock_after: at(2, 0, 30),
            },
            ScriptStep {
                at_commit: 8,
                fold: None,
                clock_after: fold_at,
            },
        ],
    );
    let result = try_run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
        Arc::clone(&clock),
    )
    .await;
    let hours = commit_hours(&store);
    assert_eq!(
        hours
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<u32>>(),
        [HOUR, HOUR + 1, HOUR + 2].into_iter().collect(),
        "the load committed into three hours: {hours:?}"
    );
    assert_eq!(&hours[..2], &[HOUR, HOUR], "two commits into H: {hours:?}");
    assert_eq!(fold_watermarks(&store), vec![Some(HOUR)], "the other seal");
    assert_eq!(clock.load(Ordering::SeqCst), fold_at);
    let err = result.expect_err("the load fails naming hour H");
    let load::LoadError::FoldLeftCommitsUncovered {
        durable,
        hours: uncovered,
        finding,
        report,
        ..
    } = &err
    else {
        panic!("expected FoldLeftCommitsUncovered, got {err:?}");
    };
    assert_eq!(durable.len(), hours.len(), "every commit is durable");
    let fold = report.fold.clone().expect("the fold ran");
    assert!(!fold.no_op, "{fold:?}");
    assert_eq!(fold.watermark_hour, Some(HOUR + 2), "{fold:?}");
    assert_eq!(fold.entry_count, hours.len() as u64 - 1, "{fold:?}");
    assert_eq!(uncovered, &HOUR.to_string());
    // The second commit into H, shard 1's first, landed after the other seal.
    let late = durable
        .iter()
        .find(|t| t.shard == 1 && t.seq == 0)
        .expect("shard 1's first commit");
    assert_eq!(late.ingest_hour_bucket, HOUR);
    let named = format!(
        "1 of the 8 commits it published are not in the snapshot of the catalog HEAD its fold \
         left, in ingest hour(s) {HOUR}: shard 1 hour {HOUR} writer {} epoch {} seq 0",
        late.writer_id, late.epoch
    );
    assert_eq!(finding, &named);
    assert!(err.to_string().contains(&named), "{err}");
}

/// The same long load with no asserted seal: another process's margin-only
/// fold at `H + 2:25`, past the margin for `H` and after the load's last
/// commit into `H`, seals `H`. Every commit is in the snapshot the load's
/// own fold leaves, and the load succeeds.
#[tokio::test]
async fn a_margin_seal_after_the_last_commit_into_an_hour_is_not_reported() {
    let (_dir, parquet_path, mapping) = write_fixture();
    let clock = Arc::new(AtomicI64::new(CLOCK_NS));
    let store = RecordingStore::scripted(
        Arc::new(MemoryStore::new()),
        Arc::clone(&clock),
        vec![
            ScriptStep {
                at_commit: 2,
                fold: None,
                clock_after: at(1, 5, 0),
            },
            ScriptStep {
                at_commit: 4,
                fold: None,
                clock_after: at(2, 0, 30),
            },
            ScriptStep {
                at_commit: 5,
                fold: None,
                clock_after: at(2, 25, 0),
            },
            ScriptStep {
                at_commit: 6,
                fold: Some(None),
                clock_after: at(2, 45, 0),
            },
        ],
    );
    let report = try_run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
        clock,
    )
    .await
    .expect("every commit is covered");
    let hours = commit_hours(&store);
    assert_eq!(
        hours
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<u32>>(),
        [HOUR, HOUR + 1, HOUR + 2].into_iter().collect(),
        "the load committed into three hours: {hours:?}"
    );
    assert_eq!(
        hours.iter().rposition(|&h| h == HOUR),
        Some(1),
        "no commit into H after the second: {hours:?}"
    );
    assert_eq!(fold_watermarks(&store), vec![Some(HOUR)], "the margin seal");
    let fold = report.fold.expect("the fold ran");
    assert_eq!(fold.watermark_hour, Some(HOUR + 2), "{fold:?}");
    assert_eq!(
        fold.entry_count,
        hours.len() as u64,
        "every commit is an entry"
    );
}

/// The fold succeeds but the HEAD it wrote cannot be read back: coverage
/// could not be checked, so the load fails in the uncovered class and says
/// so, naming every hour it wrote.
#[tokio::test]
async fn an_unreadable_head_after_the_fold_fails_the_load_as_unchecked() {
    let (_dir, parquet_path, mapping) = write_fixture();
    let store = RecordingStore::new(Arc::new(MemoryStore::new()));
    store
        .refuse_written_head_reads
        .store(true, Ordering::SeqCst);
    let err = try_run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
        Arc::new(AtomicI64::new(CLOCK_NS)),
    )
    .await
    .expect_err("coverage could not be checked");
    let load::LoadError::FoldLeftCommitsUncovered {
        durable,
        hours,
        finding,
        report,
        ..
    } = &err
    else {
        panic!("expected FoldLeftCommitsUncovered, got {err:?}");
    };
    assert_eq!(durable.len(), report.objects_written());
    let fold = report.fold.clone().expect("the fold ran");
    assert!(!fold.no_op, "{fold:?}");
    assert_eq!(fold.entry_count, durable.len() as u64, "{fold:?}");
    assert_eq!(hours, &HOUR.to_string());
    assert!(
        finding.starts_with(&format!(
            "whether the snapshot of the catalog HEAD its fold left covers the {} commits it \
             published into ingest hour(s) {HOUR} could not be checked: ",
            durable.len()
        )),
        "{finding}"
    );
    assert!(finding.contains("HEAD read refused"), "{finding}");
}
