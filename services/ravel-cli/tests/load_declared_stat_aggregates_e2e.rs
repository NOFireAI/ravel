//! ADR-4628 test plan items 9 and 12 for the loader, and the
//! `--no-declared-stat-aggregates` flag through the built binary.
//!
//! Test 9: a tenant declared first and then loaded by `ravel_cli::load::load`
//! (the default, which emits the stamp's sum and zero count) answers Q1, Q2,
//! Q3 and Q29 through a real `POST /api/v1/sql` on the unfolded catalog from
//! the commit-record stamps alone, cold and hot, with zero data-object GETs
//! and exact answers.
//!
//! Test 12, the statement half for the loader producer: a second tenant
//! loaded from the same file through `run_load_reporting` with the opt-out
//! carries min, max and null count and none of fields 6 to 8 on every commit
//! record; the four statements scan and answer what the emitting tenant
//! answers, while MIN and MAX still answer from its stamps.
//!
//! GETs are counted below every cache by a key-classifying wrapper, the shape
//! of `KeyCountingStore` in `crates/ravel-sql/tests/logs_stamp_readers.rs`.
//! The test asserts that the keys it counts as data objects are the keys the
//! commit records name.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load::{
    self, DEFAULT_DECODE_QUEUE_BATCHES, DeclaredStatAggregates, LOAD_REPORT_JSON_PREFIX,
    LoadMemoryRequest, LoadPageChoice, PER_BATCH_TARGET_BYTES,
};
use ravel_cli::maintain::SignalArg;
use ravel_ingest::{Clock, RlogChunkOrder, RlogGroupTargetBlocks, RlogZstdLevel};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_proto::commit::v1::CommitRecord;
use ravel_query::http::StaticBearerTokenResolver;
use ravel_server::declared_columns::TenantConfigDeclaredColumns;
use ravel_server::typed_attr_config::TypedAttrColumnConfig;
use ravel_types::TenantId;
use serde_json::Value;
use tower::ServiceExt;

/// The injected ingest and query clock (2023-11-14T22:13:20Z).
const CLOCK_NS: i64 = 1_700_000_000_000_000_000;
const TEST_HORIZON_NS: i64 = 1_000;
const TEST_BACKOFF_NS: i64 = 1_000;

const EMIT_TENANT: &str = "stamp-emit";
const EMIT_TOKEN: &str = "stamp-emit-token";
const WITHHOLD_TENANT: &str = "stamp-withhold";
const WITHHOLD_TOKEN: &str = "stamp-withhold-token";

/// Rows in the file, and rows per loader batch. One shard, one read cursor,
/// a pipeline depth of 1 and the per-batch flush target write one RLOG object
/// per batch, so the file loads as `ROWS / BATCH_ROWS` objects.
const ROWS: usize = 30;
const BATCH_ROWS: usize = 10;
const OBJECTS: u64 = 3;

/// `UserID` of the first object's rows: ten of them sum past `i64::MAX`, so
/// that object's stamp carries the sum in field 7.
const BIG_USER: i64 = 1 << 62;

const MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "AdvEngineID"
column = "AdvEngineID"
type = "i64"

[[attribute]]
key = "ResolutionWidth"
column = "ResolutionWidth"
type = "i64"

[[attribute]]
key = "UserID"
column = "UserID"
type = "i64"
"#;

// The four statements of ADR-4628, copied from
// crates/ravel-sql/tests/logs_stamp_readers.rs.
const Q1: &str = "SELECT COUNT(*) FROM logs WHERE \"AdvEngineID\" <> 0";
const Q2: &str = "SELECT SUM(\"AdvEngineID\"), COUNT(*), AVG(\"ResolutionWidth\") FROM logs";
const Q3: &str = "SELECT AVG(\"UserID\") FROM logs";

fn clickbench_q29() -> String {
    let terms: Vec<String> = (0..90)
        .map(|k| {
            if k == 0 {
                "SUM(\"ResolutionWidth\")".to_string()
            } else {
                format!("SUM(\"ResolutionWidth\" + {k})")
            }
        })
        .collect();
    format!("SELECT {} FROM logs", terms.join(", "))
}

fn ts(row: usize) -> i64 {
    CLOCK_NS - 60_000_000_000 + row as i64 * 1_000
}

/// 0, 2, 5 repeating: every run of ten rows holds both zero and nonzero
/// values, with 0 strictly between nothing and the max, so no object's min
/// and max alone answer Q1.
fn adv_engine_id(row: usize) -> i64 {
    [0, 2, 5][row % 3]
}

fn resolution_width(row: usize) -> i64 {
    1_000 + row as i64
}

fn user_id(row: usize) -> i64 {
    if row < BATCH_ROWS {
        BIG_USER + row as i64
    } else {
        1_000_000 + row as i64
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    parquet: PathBuf,
    mapping: PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = dir.path().join("hits.parquet");
    let mapping = dir.path().join("mapping.toml");
    std::fs::write(&mapping, MAPPING).expect("write mapping");
    let col = |f: fn(usize) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from((0..ROWS).map(f).collect::<Vec<_>>()))
    };
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", col(ts)),
        (
            "body",
            Arc::new(StringArray::from(
                (0..ROWS).map(|r| format!("row{r}")).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "svc",
            Arc::new(StringArray::from(vec!["web"; ROWS])) as ArrayRef,
        ),
        ("AdvEngineID", col(adv_engine_id)),
        ("ResolutionWidth", col(resolution_width)),
        ("UserID", col(user_id)),
    ])
    .expect("batch");
    let file = std::fs::File::create(&parquet).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    Fixture {
        _dir: dir,
        parquet,
        mapping,
    }
}

/// GETs by key kind, counted below every cache. A data object is a key whose
/// fourth segment is a level (`t/<tenant>/<signal>/l0/...`).
#[derive(Default)]
struct KeyCountingStore {
    inner: MemoryStore,
    data_gets: AtomicU64,
}

fn is_data_key(key: &str) -> bool {
    let parts: Vec<&str> = key.split('/').collect();
    parts.len() > 3 && matches!(parts[3], "l0" | "l1" | "rw")
}

#[async_trait::async_trait]
impl ObjectStoreBackend for KeyCountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if is_data_key(key) {
            self.data_gets.fetch_add(1, Ordering::AcqRel);
        }
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

struct AdvancingClock(Arc<AtomicI64>);

impl Clock for AdvancingClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// The declaration the CLI derives from the mapping: `service.name` and the
/// three I64 columns.
async fn declare(store: &Arc<KeyCountingStore>, fx: &Fixture, tenant: &str) {
    ravel_cli::typed_attr_column::set_from_mapping(
        Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
        tenant,
        &fx.mapping,
        &ravel_cli::typed_attr_column::KeyIndexSpec::default(),
        CLOCK_NS,
    )
    .await
    .expect("the mapping-derived declaration is written");
}

/// The emitting tenant: `load::load`, which passes the default.
async fn load_emitting(store: &Arc<KeyCountingStore>, fx: &Fixture) -> load::LoadReport {
    let mapping = load::parse_mapping(MAPPING).expect("valid mapping");
    load::load(
        Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
        &fx.parquet,
        EMIT_TENANT,
        &mapping,
        1,
        BATCH_ROWS,
        Some(1),
        1,
        CLOCK_NS,
        Arc::new(AdvancingClock(Arc::new(AtomicI64::new(CLOCK_NS)))),
    )
    .await
    .expect("the emitting load succeeds")
}

/// The withholding tenant: `run_load_reporting`, what `load::run` delegates
/// to, with the flag's value, on an injected clock and with no fold.
async fn load_withholding(store: &Arc<KeyCountingStore>, fx: &Fixture) -> load::LoadLayoutReport {
    let mut warnings = Vec::new();
    let report = load::run_load_reporting(
        Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
        &fx.parquet,
        WITHHOLD_TENANT,
        &fx.mapping,
        SignalArg::Logs,
        1,
        BATCH_ROWS,
        0,
        Some(1),
        Some(1),
        1,
        DEFAULT_DECODE_QUEUE_BATCHES,
        None,
        Some(PER_BATCH_TARGET_BYTES),
        None,
        None,
        RlogZstdLevel::DEFAULT,
        RlogGroupTargetBlocks::DEFAULT,
        RlogChunkOrder::DEFAULT,
        LoadPageChoice::default(),
        DeclaredStatAggregates::Withhold,
        LoadMemoryRequest::Flag(256 << 20),
        false,
        &ravel_cli::ipcrypt::IpcryptPolicy::empty(),
        None,
        CLOCK_NS,
        Arc::new(AdvancingClock(Arc::new(AtomicI64::new(CLOCK_NS)))),
        &mut warnings,
    )
    .await
    .expect("the withholding load succeeds");
    let warnings = String::from_utf8(warnings).expect("utf8 warnings");
    assert!(
        !warnings.contains("--no-declared-stat-aggregates"),
        "a logs load does not warn about the flag: {warnings}"
    );
    report
}

/// The real `/api/v1/sql` router over `store`, both tenants' tokens wired.
fn build_app(store: Arc<KeyCountingStore>, clock: Arc<AtomicI64>) -> Router {
    let store = store as Arc<dyn ObjectStoreBackend>;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let tokens = HashMap::from([
        (EMIT_TOKEN.to_string(), TenantId::new(EMIT_TENANT)),
        (WITHHOLD_TOKEN.to_string(), TenantId::new(WITHHOLD_TENANT)),
    ]);
    let declared = Arc::new(TenantConfigDeclaredColumns::with_intervals(
        TypedAttrColumnConfig::default(),
        Arc::clone(&store),
        TEST_HORIZON_NS,
        TEST_BACKOFF_NS,
    ));
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
        Some(declared),
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    )
    .expect("build_sql_state");
    state.clock = Arc::new(AdvancingClock(clock));
    ravel_server::sql::router(state)
}

/// One statement's response and the data-object GETs it caused.
struct Run {
    row: Vec<Value>,
    stats: Value,
    data_gets: u64,
}

async fn run(app: &Router, store: &KeyCountingStore, token: &str, sql: &str) -> Run {
    let payload = serde_json::json!({ "query": sql, "timeout": 60.0 }).to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(payload))
        .expect("build request");
    let before = store.data_gets.load(Ordering::Acquire);
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let data_gets = store.data_gets.load(Ordering::Acquire) - before;
    let body: Value = serde_json::from_slice(&bytes).expect("a JSON body");
    assert_eq!(status, StatusCode::OK, "{sql}: {body}");
    let rows = body["data"]["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "{sql}: one row: {body}");
    Run {
        row: rows[0].as_array().expect("a row array").clone(),
        stats: body["stats"].clone(),
        data_gets,
    }
}

/// An expected answer cell: integers exactly, floats by bit pattern.
#[derive(Debug, Clone, Copy)]
enum Cell {
    Int(i64),
    Float(f64),
}

fn assert_cells(sql: &str, got: &[Value], want: &[Cell]) {
    assert_eq!(got.len(), want.len(), "{sql}: {got:?}");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        match w {
            Cell::Int(v) => assert_eq!(g.as_i64(), Some(*v), "{sql}: column {i}: {got:?}"),
            Cell::Float(v) => assert_eq!(
                g.as_f64().map(f64::to_bits),
                Some(v.to_bits()),
                "{sql}: column {i}: {g} against {v}"
            ),
        }
    }
}

/// Two answers equal cell for cell, floats by bit pattern.
fn assert_same_answer(sql: &str, got: &[Value], want: &[Value]) {
    assert_eq!(got.len(), want.len(), "{sql}");
    for (g, w) in got.iter().zip(want) {
        match (g.as_i64(), w.as_i64()) {
            (Some(a), Some(b)) => assert_eq!(a, b, "{sql}"),
            _ => assert_eq!(
                g.as_f64().map(f64::to_bits),
                w.as_f64().map(f64::to_bits),
                "{sql}: {g} against {w}"
            ),
        }
    }
}

/// The four statements with their exact answers, computed from the rows.
fn statements() -> Vec<(String, Vec<Cell>)> {
    let rows = 0..ROWS;
    let n = ROWS as i64;
    let q1 = rows.clone().filter(|&r| adv_engine_id(r) != 0).count() as i64;
    let adv_sum: i64 = rows.clone().map(adv_engine_id).sum();
    let rw_sum: i64 = rows.clone().map(resolution_width).sum();
    let user_sum: i128 = rows.map(|r| i128::from(user_id(r))).sum();
    vec![
        (Q1.to_string(), vec![Cell::Int(q1)]),
        (
            Q2.to_string(),
            vec![
                Cell::Int(adv_sum),
                Cell::Int(n),
                Cell::Float(rw_sum as f64 / n as f64),
            ],
        ),
        (
            Q3.to_string(),
            vec![Cell::Float(user_sum as f64 / n as f64)],
        ),
        (
            clickbench_q29(),
            (0..90).map(|k| Cell::Int(rw_sum + k * n)).collect(),
        ),
    ]
}

fn pruning(stats: &Value, key: &str) -> u64 {
    stats["pruning"][key]
        .as_u64()
        .unwrap_or_else(|| panic!("no stats.pruning.{key} in {stats}"))
}

/// `(phase, s3GetRequests)` for every `stats.phases` entry.
fn phase_gets(stats: &Value) -> Vec<(String, u64)> {
    stats["phases"]
        .as_array()
        .unwrap_or_else(|| panic!("no stats.phases in {stats}"))
        .iter()
        .map(|p| {
            (
                p["phase"].as_str().expect("phase name").to_string(),
                p["s3GetRequests"].as_u64().expect("s3GetRequests"),
            )
        })
        .collect()
}

/// Every commit record (not a compaction, rewrite or tombstone) under the
/// tenant's logs prefix.
async fn commit_records(store: &KeyCountingStore, tenant: &str) -> Vec<CommitRecord> {
    let prefix = format!("t/{}/l/c/", TenantId::new(tenant).hash().to_hex());
    let mut records = Vec::new();
    for meta in list_all(&store.inner, &prefix).await.expect("list") {
        let name = meta.key.rsplit('/').next().expect("a name");
        if !name.ends_with(".cmt") || name.starts_with("l1.") || name.starts_with("rw.") {
            continue;
        }
        let data = store
            .inner
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get record")
            .data;
        records.push(ravel_commit::record::decode(&data).expect("a commit record"));
    }
    records
}

/// The rows an object holds, read off its record's event-time range.
fn rows_of(record: &CommitRecord) -> Vec<usize> {
    (0..ROWS)
        .filter(|&r| (record.min_event_ts_ns..=record.max_event_ts_ns).contains(&ts(r)))
        .collect()
}

/// The stamp lines `commit decode` prints for `record`, through the shared
/// formatter, sorted by text.
fn printed_stamp(record: &CommitRecord) -> Vec<String> {
    let kept = ravel_commit::declared_stats::read_commit_record(record)
        .covered()
        .len();
    let mut lines =
        ravel_cli::catalog::declared_stamp_lines(&record.declared_column_stats, kept, "");
    lines.sort();
    lines
}

/// What the formatter must print for one I64 column of an emitting record,
/// computed from the object's rows.
fn expected_entry(name: &str, rows: &[usize], value: fn(usize) -> i64) -> String {
    let values: Vec<i64> = rows.iter().map(|&r| value(r)).collect();
    let sum: i128 = values.iter().map(|&v| i128::from(v)).sum();
    let (sum, sum_wide) = match i64::try_from(sum) {
        Ok(narrow) => (narrow.to_string(), "ABSENT".to_string()),
        Err(_) => ("ABSENT".to_string(), sum.to_string()),
    };
    format!(
        "name={name:?} declared_type=2 (I64) min={} max={} null_count=0 sum={sum} \
         sum_wide={sum_wide} zero_count={}",
        values.iter().min().expect("rows"),
        values.iter().max().expect("rows"),
        values.iter().filter(|&&v| v == 0).count(),
    )
}

/// A world: one store holding the loaded tenants, and the fixture.
struct World {
    _fx: Fixture,
    store: Arc<KeyCountingStore>,
}

impl World {
    /// A fresh router over the store, so its catalog, record and segment
    /// caches start empty: the first statement it runs is cold.
    fn app(&self) -> Router {
        build_app(
            Arc::clone(&self.store),
            Arc::new(AtomicI64::new(CLOCK_NS + 1_000_000)),
        )
    }
}

async fn world(with_withholding: bool) -> World {
    let fx = fixture();
    let store = Arc::new(KeyCountingStore::default());
    declare(&store, &fx, EMIT_TENANT).await;
    let report = load_emitting(&store, &fx).await;
    assert_eq!(report.rows_processed, ROWS as u64);
    assert_eq!(report.objects_written() as u64, OBJECTS);
    assert_eq!(
        report.declared_stat_aggregates,
        DeclaredStatAggregates::Emit,
        "load::load passes the default"
    );
    if with_withholding {
        declare(&store, &fx, WITHHOLD_TENANT).await;
        let layout = load_withholding(&store, &fx).await;
        assert_eq!(layout.objects_written as u64, OBJECTS);
        assert_eq!(layout.declared_stat_aggregates, Some(false));
    }
    World { _fx: fx, store }
}

/// Test 9: every statement, cold then hot, answers exactly from the stamps
/// of the unfolded records with no data-object GET, and the records carry
/// the sum (field 7 for the object past i64) and the zero count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loaded_tenant_answers_the_four_statements_from_its_stamps() {
    let w = world(false).await;

    let records = commit_records(&w.store, EMIT_TENANT).await;
    assert_eq!(records.len() as u64, OBJECTS);
    let mut wide = 0;
    for record in &records {
        assert!(is_data_key(&record.object_key), "{}", record.object_key);
        let rows = rows_of(record);
        assert_eq!(rows.len() as u64, record.sample_count);
        let mut entries = vec![
            expected_entry("AdvEngineID", &rows, adv_engine_id),
            expected_entry("ResolutionWidth", &rows, resolution_width),
            expected_entry("UserID", &rows, user_id),
        ];
        entries.sort();
        let printed = printed_stamp(record);
        let mut bodies: Vec<String> = printed
            .iter()
            .filter_map(|l| l.split_once("]: ").map(|(_, body)| body.to_string()))
            .collect();
        bodies.sort();
        assert_eq!(bodies, entries, "{printed:?}");
        assert_eq!(printed.len(), 4, "{printed:?}");
        assert!(
            printed.iter().any(|l| l == "stamp: entries=3 kept=3"),
            "{printed:?}"
        );
        if record
            .declared_column_stats
            .iter()
            .any(|e| !e.sum_wide.is_empty())
        {
            wide += 1;
            let user = printed
                .iter()
                .find(|l| l.contains("name=\"UserID\""))
                .expect("a UserID line");
            let sum: i128 = rows.iter().map(|&r| i128::from(user_id(r))).sum();
            assert!(sum > i128::from(i64::MAX));
            assert!(
                user.ends_with(&format!(" sum=ABSENT sum_wide={sum} zero_count=0")),
                "{user}"
            );
        }
    }
    assert_eq!(wide, 1, "exactly one object's UserID sum leaves i64");

    for (sql, want) in statements() {
        let app = w.app();
        for pass in ["cold", "hot"] {
            let r = run(&app, &w.store, EMIT_TOKEN, &sql).await;
            let label = format!("{pass} {}", &sql[..sql.len().min(60)]);
            assert_cells(&label, &r.row, &want);
            assert_eq!(pruning(&r.stats, "segments"), OBJECTS, "{label}");
            assert_eq!(
                pruning(&r.stats, "segmentsAnsweredFromStamp"),
                OBJECTS,
                "{label}"
            );
            assert_eq!(pruning(&r.stats, "segmentsAnsweredFromCstat"), 0, "{label}");
            let phases = phase_gets(&r.stats);
            for (phase, gets) in &phases {
                if phase != "resolve" {
                    assert_eq!(*gets, 0, "{label}: {phase}: {phases:?}");
                }
            }
            assert_eq!(r.data_gets, 0, "{label}");
            println!(
                "test 9 {label}: phases {phases:?} data-object GETs {}",
                r.data_gets
            );
        }
    }
}

/// Test 12, loader producer: with the opt-out, every record carries min, max
/// and null count and none of fields 6 to 8; the statements scan and answer
/// what the emitting tenant answers; MIN and MAX still answer from the
/// stamps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withholding_load_scans_and_answers_what_the_emitting_load_answers() {
    let w = world(true).await;

    let records = commit_records(&w.store, WITHHOLD_TENANT).await;
    assert_eq!(records.len() as u64, OBJECTS);
    for record in &records {
        let names: Vec<&str> = record
            .declared_column_stats
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(names.len(), 3, "{names:?}");
        for name in ["AdvEngineID", "ResolutionWidth", "UserID"] {
            assert!(names.contains(&name), "{name} in {names:?}");
        }
        for entry in &record.declared_column_stats {
            assert!(entry.min.is_some() && entry.max.is_some(), "{entry:?}");
            assert_eq!(entry.null_count, 0, "{entry:?}");
            assert_eq!(
                (entry.sum, entry.sum_wide.len(), entry.zero_count),
                (None, 0, None),
                "{entry:?}"
            );
        }
    }

    for (sql, want) in statements() {
        let label = &sql[..sql.len().min(60)];
        let app = w.app();
        let emitted = run(&app, &w.store, EMIT_TOKEN, &sql).await;
        let withheld = run(&app, &w.store, WITHHOLD_TOKEN, &sql).await;
        assert_cells(label, &withheld.row, &want);
        assert_same_answer(label, &withheld.row, &emitted.row);
        assert_eq!(
            pruning(&withheld.stats, "segmentsAnsweredFromStamp"),
            0,
            "{label}"
        );
        assert!(withheld.data_gets > 0, "{label}: the statement scans");
        println!(
            "test 12 {label}: phases {:?} data-object GETs {}",
            phase_gets(&withheld.stats),
            withheld.data_gets
        );
    }

    let min_max = "SELECT MIN(\"UserID\"), MAX(\"UserID\") FROM logs";
    let r = run(&w.app(), &w.store, WITHHOLD_TOKEN, min_max).await;
    let users: Vec<i64> = (0..ROWS).map(user_id).collect();
    assert_cells(
        min_max,
        &r.row,
        &[
            Cell::Int(*users.iter().min().expect("rows")),
            Cell::Int(*users.iter().max().expect("rows")),
        ],
    );
    assert_eq!(
        r.data_gets, 0,
        "MIN and MAX answer from the withheld stamps"
    );
}

/// Runs the built binary's logs or metrics load against `--store memory`.
fn run_binary(signal: &str, parquet: &Path, mapping: &Path, extra: &[&str]) -> (String, String) {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("RAVEL_") {
            cmd.env_remove(key);
        }
    }
    let out = cmd
        .args([
            "--store",
            "memory",
            "--tenant-hash-unkeyed",
            "load",
            "--signal",
            signal,
            "--parquet",
        ])
        .arg(parquet)
        .args(["--tenant", "acme", "--mapping"])
        .arg(mapping)
        .args(extra)
        .output()
        .expect("ravel-cli runs");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    assert!(out.status.success(), "{stderr}");
    (String::from_utf8(out.stdout).expect("utf8 stdout"), stderr)
}

fn report_json(stdout: &str) -> Value {
    let lines: Vec<&str> = stdout
        .lines()
        .filter_map(|l| l.strip_prefix(LOAD_REPORT_JSON_PREFIX))
        .collect();
    assert_eq!(lines.len(), 1, "{stdout}");
    serde_json::from_str(lines[0]).expect("report JSON")
}

fn summary_lines(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .filter(|l| l.contains("stamp aggregates"))
        .collect()
}

/// The built binary names the stamp aggregates on one summary line and in the
/// report JSON: true with no flag, false with the flag; a metrics load with
/// the flag warns and reports neither.
#[test]
fn the_binary_reports_the_stamp_aggregate_choice() {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos() as i64;
    let dir = tempfile::tempdir().expect("tempdir");
    let logs = dir.path().join("logs.parquet");
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from(vec![now_ns - 60_000_000_000; 4])) as ArrayRef,
        ),
        (
            "body",
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef,
        ),
        (
            "svc",
            Arc::new(StringArray::from(vec!["web"; 4])) as ArrayRef,
        ),
        (
            "AdvEngineID",
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
        (
            "ResolutionWidth",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ),
        (
            "UserID",
            Arc::new(Int64Array::from(vec![5, 6, 7, 8])) as ArrayRef,
        ),
    ])
    .expect("batch");
    let file = std::fs::File::create(&logs).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    let logs_mapping = dir.path().join("logs.toml");
    std::fs::write(&logs_mapping, MAPPING).expect("write mapping");

    let (stdout, stderr) = run_binary("logs", &logs, &logs_mapping, &[]);
    assert_eq!(
        summary_lines(&stdout),
        vec!["  stamp aggregates : emitted"],
        "{stdout}"
    );
    assert_eq!(
        report_json(&stdout)["declared_stat_aggregates"],
        Value::Bool(true)
    );
    assert!(
        !stderr.contains("--no-declared-stat-aggregates"),
        "{stderr}"
    );

    let (stdout, stderr) = run_binary(
        "logs",
        &logs,
        &logs_mapping,
        &["--no-declared-stat-aggregates"],
    );
    assert_eq!(
        summary_lines(&stdout),
        vec!["  stamp aggregates : withheld (--no-declared-stat-aggregates)"],
        "{stdout}"
    );
    assert_eq!(
        report_json(&stdout)["declared_stat_aggregates"],
        Value::Bool(false)
    );
    assert!(
        !stderr.contains("ignores --no-declared-stat-aggregates"),
        "{stderr}"
    );

    let metrics = dir.path().join("metrics.parquet");
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from(vec![now_ns - 60_000_000_000])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(arrow::array::Float64Array::from(vec![1.5])) as ArrayRef,
        ),
    ])
    .expect("batch");
    let file = std::fs::File::create(&metrics).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    let metrics_mapping = dir.path().join("metrics.toml");
    std::fs::write(
        &metrics_mapping,
        "[metrics]\nname = \"m\"\nvalue_column = \"value\"\nts_column = \"ts\"\n\
         ts_unit = \"nanos\"\nkind = \"gauge\"\n",
    )
    .expect("write mapping");
    let (stdout, stderr) = run_binary(
        "metrics",
        &metrics,
        &metrics_mapping,
        &["--no-declared-stat-aggregates"],
    );
    assert_eq!(
        stderr
            .lines()
            .filter(|l| l.contains("--no-declared-stat-aggregates"))
            .collect::<Vec<_>>(),
        vec![
            "warning: a metrics load ignores --no-declared-stat-aggregates. Only the commit \
             records a logs load writes carry a declared-column stamp."
        ],
        "{stderr}"
    );
    assert!(summary_lines(&stdout).is_empty(), "{stdout}");
    let json = report_json(&stdout);
    assert_eq!(json["declared_stat_aggregates"], Value::Null, "{json}");
    assert!(json.get("declared_stat_aggregates").is_none(), "{json}");
}
