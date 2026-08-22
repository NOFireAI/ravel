//! ADR-0100 decision 5: the epic's end-to-end acceptance test. A wide Parquet
//! export loaded by the shipping loader, declared by the shipping
//! control-plane command, and queried through the shipping HTTP handler.
//!
//! Every hop is the production one, and the two mechanisms ADR-0100's Context
//! calls independent are both exercised in the same object:
//!
//! 1. `ravel_cli::load::load` (what `ravel-cli load --parquet` dispatches to)
//!    ingests a 1013-column Parquet file through the real log ingest path, with
//!    the `--mapping` document read off disk. The mapping names 1010 record
//!    attributes, more than `RlogConfig::default().max_dynamic_columns`, so the
//!    single flushed object both fills its dynamic-column budget and overflows
//!    it into `attrs_raw`.
//! 2. `ravel_cli::typed_attr_column::set_from_mapping` (what `ravel-cli
//!    typed-attr-column set --from-mapping` dispatches to) derives the
//!    declared-column list from that same mapping file and writes it through the
//!    CAS whole-list replace. No `DeclaredTypedColumn` is hand-built here: the
//!    CLI's own derivation is what feeds the query.
//! 3. `ravel_server::query::build_sql_state` + `ravel_server::sql::router` wire
//!    the endpoint exactly the way `ravel_server::start` does, including the
//!    cache-aside `TenantConfigDeclaredColumns` source, and a real
//!    `POST /api/v1/sql` request reads the result.
//!
//! # Why `load::load` and not `load::run`
//!
//! `run` builds its router with `Arc::new(SystemClock)`, so the RLOG objects
//! would bucket by wall-clock time while this test's query window and
//! declared-column staleness horizon ride an injected clock. `load` is the
//! function `run` delegates to (its own body is the mapping read plus the
//! summary and warning printing, which `tests/load.rs` covers through
//! `run_warning_to`), and it is the only entry point that returns the
//! `LoadReport` whose `metrics` carry the dynamic-column counters this test
//! pins exactly.
//!
//! # Why the filler attributes are `f64`
//!
//! The budget has to be crossed by real attributes, so the mapping needs more
//! than 1000 of them. Declaring all 1010 would build a 1019-column `logs`
//! schema on every plan, an order of magnitude past the ~105 columns ADR-0100
//! decision 2 targets, and would measure planning cost rather than
//! reachability. `ColType::F64` has no `DeclaredColumnType` (ADR-0100 decision
//! 2), so the filler columns are the ones the derivation skips: the declaration
//! stays at the 36 typed columns the fixture actually queries, and the keys that
//! overflow the budget are keys `attrs` is the only route to. That is exactly
//! the pair of documented degradations ADR-0100 says an operator is left with.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float64Array, Int64Array,
    StringArray,
};
use arrow::record_batch::RecordBatch;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig, read_config};
use ravel_cli::load::{self, Mapping};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::StaticBearerTokenResolver;
use ravel_server::declared_columns::TenantConfigDeclaredColumns;
use ravel_server::typed_attr_config::TypedAttrColumnConfig;
use ravel_types::TenantId;
use serde_json::Value;
use tower::ServiceExt;

/// A fixed, plausible (post-2020) ingest clock, the same reading and reasoning
/// as `tests/load.rs`'s: the RLOG flush buckets by it, so a realistic value
/// keeps the query window's catalog fan-out small.
const CLOCK_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z

/// The declared-column staleness horizon for this test: 1 microsecond, so the
/// clock advance below crosses it without the test waiting on anything. The
/// production default is 60s.
const TEST_HORIZON_NS: i64 = 1_000;
/// The failed-read backoff, likewise tiny. No read fails here.
const TEST_BACKOFF_NS: i64 = 1_000;

const TENANT: &str = "acme";
const TOKEN: &str = "acme-token";

/// Rows in the fixture. Small on purpose: this is a reachability proof, and the
/// width is what the wide path needs, not the height.
const ROWS: usize = 6;

/// Typed (declarable) record attributes, by type. Together with the two date
/// columns these are the "few dozen typed columns" the declaration carries and
/// the queries below read.
const N_I64: usize = 10;
const N_STR: usize = 10;
const N_BOOL: usize = 8;
const N_BYTES: usize = 5;
/// `col_date32` and `col_date64`: the ADR-0100 date support, which lands as an
/// `i64` attribute in the column's native unit (days / milliseconds).
const N_DATE: usize = 2;
const N_TYPED: usize = N_I64 + N_STR + N_BOOL + N_BYTES + N_DATE;

/// `f64` filler attributes, sized so the object's distinct `(name, type)` count
/// crosses the writer's budget by exactly [`EXPECTED_OVERFLOW`].
const N_FILL: usize = 975;

/// The writer's per-object dynamic-column budget. Asserted against
/// `RlogConfig::default()` below rather than trusted: if the default moves, this
/// fixture stops overflowing, and that must fail loudly instead of quietly
/// proving nothing.
const BUDGET: usize = 1_000;

/// Distinct `(name, type)` pairs past the budget. The resource attribute
/// (`service.name`, a non-indexed `Str` at stream level) draws no column, so the
/// object's distinct count is exactly the record attributes:
/// `N_TYPED + N_FILL`.
const EXPECTED_OVERFLOW: usize = N_TYPED + N_FILL - BUDGET;

/// Index of the first filler key that overflows. Column assignment is
/// lexicographic by name bytes then type byte, every `col_*` name sorts before
/// every `fill_*` name, and the filler names are zero-padded so their
/// lexicographic order is their numeric order.
const FIRST_OVERFLOW_FILL: usize = BUDGET - N_TYPED;

/// Declared columns the derivation must produce: the resource attribute plus the
/// typed record attributes. Every `f64` filler is skipped.
const EXPECTED_DECLARED: usize = 1 + N_TYPED;

/// Every row carries all `N_TYPED + N_FILL` attributes, which has to stay inside
/// the loader's per-record cap: past it the load is rejected outright instead of
/// overflowing the object's dynamic-column budget, and the test would prove the
/// wrong thing.
const _: () = assert!(N_TYPED + N_FILL <= ravel_cli::load::LOADER_MAX_ATTRIBUTES_PER_RECORD);

/// A clock whose reading the test advances, so the declared-column staleness
/// horizon can be crossed deterministically.
struct AdvancingClock(Arc<AtomicI64>);

impl Clock for AdvancingClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn fill_name(k: usize) -> String {
    format!("fill_{k:04}")
}

/// The filler value for key `k` on row `row`. Exact halves, so `f64::to_string`
/// (which is how `attrs` renders an `F64`) is one unambiguous decimal.
fn fill_value(k: usize, row: usize) -> f64 {
    (k as f64) * 10.0 + (row as f64) + 0.5
}

fn i64_value(j: usize, row: usize) -> i64 {
    1_000 * (j as i64 + 1) + row as i64
}

fn str_value(j: usize, row: usize) -> String {
    format!("s{j}r{row}")
}

fn bool_value(j: usize, row: usize) -> bool {
    (j + row).is_multiple_of(2)
}

/// Days since the Unix epoch for `row`, as a `Date32` cell. 19_700 is
/// 2023-12-05; the exact day is immaterial, its arrival as a day count is not.
fn date32_value(row: usize) -> i32 {
    19_700 + row as i32
}

/// Milliseconds since the Unix epoch for `row`, as a `Date64` cell.
fn date64_value(row: usize) -> i64 {
    1_700_000_000_000 + (row as i64) * 86_400_000
}

fn body_value(row: usize) -> String {
    format!("row{row}")
}

/// Build the fixture Parquet file and the `--mapping` TOML beside it, and return
/// their paths (the `TempDir` is returned so it outlives them).
fn write_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("wide.parquet");
    let mapping_path = dir.path().join("mapping.toml");

    let mut columns: Vec<(String, ArrayRef)> = Vec::with_capacity(3 + N_TYPED + N_FILL);
    let mut attrs_toml = String::new();

    columns.push((
        "ts".to_string(),
        Arc::new(Int64Array::from(
            (0..ROWS).map(|r| CLOCK_NS + r as i64).collect::<Vec<_>>(),
        )),
    ));
    columns.push((
        "body".to_string(),
        Arc::new(StringArray::from(
            (0..ROWS).map(body_value).collect::<Vec<_>>(),
        )),
    ));
    columns.push((
        "svc".to_string(),
        Arc::new(StringArray::from(vec!["api"; ROWS])),
    ));

    // Typed record attributes. Emitted in the mapping in this order, which is
    // also the declaration's schema-append order.
    for j in 0..N_BOOL {
        let name = format!("col_bool_{j:02}");
        columns.push((
            name.clone(),
            Arc::new(BooleanArray::from(
                (0..ROWS).map(|r| bool_value(j, r)).collect::<Vec<_>>(),
            )),
        ));
        attrs_toml.push_str(&attr_entry(&name, &name, "bool"));
    }
    for j in 0..N_BYTES {
        let name = format!("col_bytes_{j:02}");
        let values: Vec<Vec<u8>> = (0..ROWS).map(|r| vec![j as u8, r as u8]).collect();
        columns.push((
            name.clone(),
            Arc::new(BinaryArray::from_iter_values(values.iter())),
        ));
        attrs_toml.push_str(&attr_entry(&name, &name, "bytes"));
    }
    columns.push((
        "col_date32".to_string(),
        Arc::new(Date32Array::from(
            (0..ROWS).map(date32_value).collect::<Vec<_>>(),
        )),
    ));
    attrs_toml.push_str(&attr_entry("col_date32", "col_date32", "i64"));
    columns.push((
        "col_date64".to_string(),
        Arc::new(Date64Array::from(
            (0..ROWS).map(date64_value).collect::<Vec<_>>(),
        )),
    ));
    attrs_toml.push_str(&attr_entry("col_date64", "col_date64", "i64"));
    for j in 0..N_I64 {
        let name = format!("col_i64_{j:02}");
        columns.push((
            name.clone(),
            Arc::new(Int64Array::from(
                (0..ROWS).map(|r| i64_value(j, r)).collect::<Vec<_>>(),
            )),
        ));
        attrs_toml.push_str(&attr_entry(&name, &name, "i64"));
    }
    for j in 0..N_STR {
        let name = format!("col_str_{j:02}");
        columns.push((
            name.clone(),
            Arc::new(StringArray::from(
                (0..ROWS).map(|r| str_value(j, r)).collect::<Vec<_>>(),
            )),
        ));
        attrs_toml.push_str(&attr_entry(&name, &name, "str"));
    }

    // Filler attributes: `f64`, so the derivation skips them and the declaration
    // stays narrow while the object's attribute set stays wide.
    for k in 0..N_FILL {
        let name = fill_name(k);
        columns.push((
            name.clone(),
            Arc::new(Float64Array::from(
                (0..ROWS).map(|r| fill_value(k, r)).collect::<Vec<_>>(),
            )),
        ));
        attrs_toml.push_str(&attr_entry(&name, &name, "f64"));
    }

    let batch = RecordBatch::try_from_iter(columns).expect("wide record batch");
    let file = std::fs::File::create(&parquet_path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let mapping_text = format!(
        "ts_column = \"ts\"\n\
         ts_unit = \"nanos\"\n\
         body_column = \"body\"\n\
         \n\
         [[resource_attribute]]\n\
         key = \"service.name\"\n\
         column = \"svc\"\n\
         type = \"str\"\n\
         {attrs_toml}"
    );
    std::fs::write(&mapping_path, &mapping_text).expect("write mapping");

    (dir, parquet_path, mapping_path)
}

fn attr_entry(key: &str, column: &str, ty: &str) -> String {
    format!("\n[[attribute]]\nkey = \"{key}\"\ncolumn = \"{column}\"\ntype = \"{ty}\"\n")
}

/// The real `/api/v1/sql` router, wired the way `ravel_server::start` wires it:
/// `build_sql_state` with a `TenantConfigDeclaredColumns` source over the same
/// store, and the test's advancing clock installed on the state.
///
/// The base declaration is `TypedAttrColumnConfig::default()`, i.e. empty: every
/// declared column this test observes came from the CLI write, not from a flag
/// default.
fn build_app(store: Arc<dyn ObjectStoreBackend>, clock: Arc<AtomicI64>) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let tokens = HashMap::from([(TOKEN.to_string(), TenantId::new(TENANT))]);
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
    )
    .expect("build_sql_state");
    state.clock = Arc::new(AdvancingClock(clock));
    ravel_server::sql::router(state)
}

/// POST one query. The window is left to the endpoint's default (the hour
/// ending at the injected `now_ns`), which covers the fixture's records.
async fn post_sql(app: &Router, query: &str) -> (StatusCode, Value) {
    let payload = serde_json::json!({ "query": query, "timeout": 60.0 }).to_string();
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
        .expect("read body")
        .to_vec();
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

/// The column's reported Arrow type in a successful response.
fn column_type(value: &Value, name: &str) -> String {
    value["data"]["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no columns in {value}"))
        .iter()
        .find(|c| c["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("no column {name} in {value}"))["type"]
        .as_str()
        .unwrap_or_else(|| panic!("column {name} has no type in {value}"))
        .to_string()
}

fn rows(value: &Value) -> &Vec<Value> {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {value}"))
}

fn cell_i64(row: &Value, col: usize) -> i64 {
    row[col]
        .as_i64()
        .unwrap_or_else(|| panic!("column {col} of {row} is not a JSON number"))
}

fn cell_str(row: &Value, col: usize) -> &str {
    row[col]
        .as_str()
        .unwrap_or_else(|| panic!("column {col} of {row} is not a JSON string"))
}

/// Load the fixture through the real loader and return its report.
async fn run_load(
    store: &Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    mapping_path: &Path,
) -> load::LoadReport {
    // The mapping the loader uses is the same document on disk that
    // `typed-attr-column set --from-mapping` reads below; nothing is
    // reconstructed in memory for one side and not the other.
    let mapping_text = std::fs::read_to_string(mapping_path).expect("read mapping");
    let mapping: Mapping = load::parse_mapping(&mapping_text).expect("valid mapping");

    load::load(
        Arc::clone(store),
        parquet_path,
        TENANT,
        &mapping,
        // One shard, matching `CatalogConfig::default()`'s `shard_count`: the
        // query side builds its catalog from that default, and a mismatch would
        // silently resolve over a subset of shards.
        1,
        10_000,
        CLOCK_NS,
        Arc::new(AdvancingClock(Arc::new(AtomicI64::new(CLOCK_NS)))),
    )
    .await
    .expect("the wide load succeeds")
}

/// ADR-0100 decision 5, both required properties over one loaded object:
///
/// - a typed predicate over a declared column returns the loaded rows, with the
///   values the loader wrote (including a `Date32` column arriving as its native
///   day count and a `Date64` column as its native millisecond count);
/// - an attribute that overflowed the writer's dynamic-column budget is still
///   queryable through `attrs`, with the value it was loaded with.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wide_load_is_queryable_through_declared_columns_and_the_attrs_overflow() {
    // The fixture only proves anything if it actually crosses the budget the
    // writer enforces. Pin that, rather than trusting the constant.
    assert_eq!(
        ravel_logseg::RlogConfig::default().max_dynamic_columns,
        BUDGET,
        "this fixture is sized to overflow a {BUDGET}-column budget by exactly \
         {EXPECTED_OVERFLOW} pairs; the writer default moved, so resize it"
    );

    let (_dir, parquet_path, mapping_path) = write_fixture();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = run_load(&store, &parquet_path, &mapping_path).await;

    assert_eq!(report.rows_processed, ROWS as u64);
    assert_eq!(
        report.objects_written(),
        1,
        "one shard and one batch: the whole fixture is one RLOG object, which is \
         what makes the per-object budget figures below exact"
    );
    // The budget counters, pinned exactly: 1000 pairs took a column and 10 did
    // not. A "> 0" assertion here would hold just as well if one pair
    // overflowed, or if 500 did.
    assert_eq!(
        report.metrics.dynamic_columns_used_max, BUDGET as u64,
        "the single object filled its dynamic-column budget: {:?}",
        report.metrics
    );
    assert_eq!(
        report.metrics.dynamic_columns_used_total, BUDGET as u64,
        "one object, so the cumulative used total equals its own: {:?}",
        report.metrics
    );
    assert_eq!(
        report.metrics.dynamic_columns_overflowed_total, EXPECTED_OVERFLOW as u64,
        "exactly the {EXPECTED_OVERFLOW} pairs past the budget folded into \
         attrs_raw: {:?}",
        report.metrics
    );
    // And the loader turns that into the operator-facing overflow warning
    // (ADR-0100 decision 1), naming the count it saw.
    let warnings = load::dynamic_column_warnings(&report.metrics, BUDGET);
    assert_eq!(warnings.len(), 1, "one overflow warning: {warnings:?}");
    assert!(
        warnings[0].contains(&format!("{EXPECTED_OVERFLOW} distinct")),
        "the warning names the overflowed pair count: {}",
        warnings[0]
    );

    let clock = Arc::new(AtomicI64::new(CLOCK_NS + 1_000_000));
    let app = build_app(Arc::clone(&store), Arc::clone(&clock));

    // The fixture is queryable before any declaration exists, so nothing below
    // can be explained by the declaration having made the data visible.
    let (status, body) = post_sql(&app, "SELECT body FROM logs ORDER BY body").await;
    assert_eq!(status, StatusCode::OK, "baseline query failed: {body}");
    let got_bodies: Vec<&str> = rows(&body).iter().map(|r| cell_str(r, 0)).collect();
    let want_bodies: Vec<String> = (0..ROWS).map(body_value).collect();
    assert_eq!(
        got_bodies, want_bodies,
        "every loaded row is queryable before the declaration: {body}"
    );

    // And the typed column does not exist yet.
    let (status, body) = post_sql(&app, "SELECT col_date32 FROM logs").await;
    assert_ne!(
        status,
        StatusCode::OK,
        "an undeclared column must not resolve: {body}"
    );

    // The control-plane write: the declaration derived by the CLI from the same
    // mapping file the loader read.
    ravel_cli::typed_attr_column::set_from_mapping(
        Arc::clone(&store),
        TENANT,
        &mapping_path,
        CLOCK_NS,
    )
    .await
    .expect("the mapping-derived declaration is written");

    // What the derivation produced, pinned by count: the resource attribute plus
    // every typed record attribute, and none of the 975 f64 filler keys.
    let declared = read_config(store.as_ref(), &TenantId::new(TENANT).hash())
        .await
        .expect("read tenant config")
        .expect("the CLI created the record")
        .0
        .typed_attr_columns
        .expect("the declaration is present");
    assert_eq!(
        declared.len(),
        EXPECTED_DECLARED,
        "the derivation declared the {EXPECTED_DECLARED} declarable mapping keys and \
         skipped every f64 one: {:?}",
        declared.iter().map(|c| &c.key).take(8).collect::<Vec<_>>()
    );

    // Cross the staleness horizon so the next query re-resolves (the failed
    // `SELECT col_date32` above cached the zero-declaration resolution).
    clock.fetch_add(TEST_HORIZON_NS * 4, Ordering::SeqCst);

    // ---- Required assertion 1: a typed predicate over a declared column.
    //
    // `col_date32` is a Date32 Parquet column loaded as an i64 attribute in its
    // native unit (days), declared through the derivation above, and compared
    // here as an integer with no CAST.
    let cutoff = date32_value(2);
    let (status, body) = post_sql(
        &app,
        &format!(
            "SELECT body, col_date32, col_date64, col_i64_00, col_str_00 FROM logs \
             WHERE col_date32 >= {cutoff} ORDER BY col_date32"
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the typed predicate must resolve after the CLI declaration: {body}"
    );
    assert_eq!(
        column_type(&body, "col_date32"),
        "Int64",
        "a declared Date32-sourced column is a native Arrow Int64, not a Utf8 map \
         read: {body}"
    );
    let want_rows: Vec<(String, i64, i64, i64, String)> = (2..ROWS)
        .map(|r| {
            (
                body_value(r),
                i64::from(date32_value(r)),
                date64_value(r),
                i64_value(0, r),
                str_value(0, r),
            )
        })
        .collect();
    let got_rows: Vec<(String, i64, i64, i64, String)> = rows(&body)
        .iter()
        .map(|r| {
            (
                cell_str(r, 0).to_string(),
                cell_i64(r, 1),
                cell_i64(r, 2),
                cell_i64(r, 3),
                cell_str(r, 4).to_string(),
            )
        })
        .collect();
    assert_eq!(
        got_rows, want_rows,
        "the typed predicate returns exactly the loaded rows at or past day \
         {cutoff}, with the values the loader wrote (Date32 as days, Date64 as \
         milliseconds): {body}"
    );

    // ---- Required assertion 2: an overflowed attribute is still in `attrs`.
    //
    // `FIRST_OVERFLOW_FILL` and the last filler key are the two ends of the
    // overflow set; the key one index below it is the last that did get a
    // column. All three must read back identically through the map, which is
    // what "the fold loses nothing" means.
    let over_first = fill_name(FIRST_OVERFLOW_FILL);
    let over_last = fill_name(N_FILL - 1);
    let under_last = fill_name(FIRST_OVERFLOW_FILL - 1);
    let (status, body) = post_sql(
        &app,
        &format!(
            "SELECT body, attrs['{over_first}'] AS a, attrs['{over_last}'] AS b, \
             attrs['{under_last}'] AS c FROM logs ORDER BY body"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "attrs query failed: {body}");
    // Compared as JSON values rather than through `cell_str`: a lost fold reads
    // back as `null`, and the diff has to show that rather than panic on the
    // accessor before the assertion runs.
    let want_attrs: Vec<Vec<Value>> = (0..ROWS)
        .map(|r| {
            vec![
                Value::from(body_value(r)),
                Value::from(fill_value(FIRST_OVERFLOW_FILL, r).to_string()),
                Value::from(fill_value(N_FILL - 1, r).to_string()),
                Value::from(fill_value(FIRST_OVERFLOW_FILL - 1, r).to_string()),
            ]
        })
        .collect();
    let got_attrs: Vec<Vec<Value>> = rows(&body)
        .iter()
        .map(|r| (0..4).map(|c| r[c].clone()).collect())
        .collect();
    assert_eq!(
        got_attrs, want_attrs,
        "both overflowed keys ({over_first}, {over_last}) and the last key that \
         kept its column ({under_last}) read back their loaded values through \
         attrs: the attrs_raw fold lost nothing: {body}"
    );
}
