//! ADR-0090 reachability gate: a declaration written by `ravel-cli
//! typed-attr-column set` becomes a native typed Arrow column in a real
//! `POST /api/v1/sql` response.
//!
//! Every hop is the production one:
//!
//! 1. `ravel_cli::load::load` (what `ravel-cli load --parquet` dispatches to)
//!    ingests records carrying a typed `i64` attribute through the real log
//!    ingest path, so the value on disk is a real RLOG typed column, not a
//!    fixture hand-written into a shape the reader wants.
//! 2. `ravel_cli::typed_attr_column::set` (what `ravel-cli typed-attr-column
//!    set` dispatches to) writes the durable `TenantConfig.typed_attr_columns`
//!    override through `ravel_catalog::set_tenant_config`.
//! 3. `ravel_server::query::build_sql_state` builds the `SqlState` exactly the
//!    way `ravel_server::start` does, including installing the cache-aside
//!    `TenantConfigDeclaredColumns` source on the executor, and
//!    `ravel_server::sql::router` mounts the real handler.
//! 4. A real HTTP request to `POST /api/v1/sql` returns the declared column.
//!
//! It lives in `ravel-cli`'s test suite (with a dev-dependency on
//! `ravel-server`) rather than the other way round because the CLI write is the
//! first hop: driving it in-process is the only way a single `MemoryStore` can
//! carry the write and the query, and a subprocess `ravel-cli --store memory`
//! would get its own empty store (the same reason `tests/load.rs` and
//! `tests/catalog.rs` drive library entry points in-process).
//!
//! The staleness horizon is a real part of the contract under test, not
//! something stubbed out: the source is built with a deliberately tiny horizon
//! (the seam `TenantConfigDeclaredColumns::with_intervals` exists for, mirroring
//! `IndexedFieldsOverlay::with_intervals`'s own tests) and the test's injected
//! clock is advanced past it between the pre-declaration query and the
//! post-declaration one. Nothing about the wiring differs from production; only
//! the horizon's magnitude does.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig};
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
/// The failed-read backoff, likewise tiny. No read fails here; it is set
/// explicitly so the test never depends on the production value.
const TEST_BACKOFF_NS: i64 = 1_000;

const TENANT: &str = "acme";
const TOKEN: &str = "acme-token";

/// A clock whose reading the test advances, so the declared-column staleness
/// horizon can be crossed deterministically. The SQL endpoint reads it once per
/// request and threads that `now_ns` into the declared-column resolution.
struct AdvancingClock(Arc<AtomicI64>);

impl Clock for AdvancingClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

fn i64_col(vals: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vals))
}

fn str_col(vals: Vec<&str>) -> ArrayRef {
    Arc::new(StringArray::from(vals))
}

/// Ingest three records carrying a typed `i64` attribute `dur` through the real
/// loader entry point, and return the durations in ingest order.
async fn ingest_fixture(store: &Arc<dyn ObjectStoreBackend>) -> Vec<i64> {
    let durations = vec![200i64, 500, 900];
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("logs.parquet");
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts".to_string(),
            i64_col(vec![CLOCK_NS, CLOCK_NS + 1, CLOCK_NS + 2]),
        ),
        (
            "body".to_string(),
            str_col(vec!["first", "second", "third"]),
        ),
        ("svc".to_string(), str_col(vec!["api", "api", "api"])),
        ("dur".to_string(), i64_col(durations.clone())),
    ])
    .expect("batch");
    write_parquet(&pq, &batch);

    // `dur` is declared `i64` in the mapping, so the loader writes a typed I64
    // RLOG column, which is what makes the declared-column read meaningful (a
    // Str-typed value would read NULL under ADR-0090 decision 7).
    let mapping: Mapping = load::parse_mapping(
        r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "dur"
column = "dur"
type = "i64"
"#,
    )
    .expect("valid mapping");

    let report = load::load(
        Arc::clone(store),
        &pq,
        TENANT,
        &mapping,
        // One shard, matching `CatalogConfig::default()`'s `shard_count`: the
        // query side below builds its catalog from that default, and a
        // mismatch would silently resolve over a subset of shards.
        1,
        10_000,
        CLOCK_NS,
        Arc::new(AdvancingClock(Arc::new(AtomicI64::new(CLOCK_NS)))),
    )
    .await
    .expect("load succeeds");
    assert_eq!(report.rows_processed, 3);
    assert!(
        report.objects_written() >= 1,
        "the loader flushed at least one RLOG object"
    );
    durations
}

/// The real `/api/v1/sql` router, wired the way `ravel_server::start` wires it:
/// `build_sql_state` with a `TenantConfigDeclaredColumns` source over the same
/// store, and the test's advancing clock installed on the state.
fn build_app(store: Arc<dyn ObjectStoreBackend>, clock: Arc<AtomicI64>) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let tokens = HashMap::from([(TOKEN.to_string(), TenantId::new(TENANT))]);
    // No CLI-derived declaration at all: everything this test observes comes
    // from the durable override the CLI writes, so a passing assertion cannot
    // be explained by a flag default.
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

/// The acceptance test: `ravel-cli typed-attr-column set acme dur:i64` makes
/// `dur` a native `Int64` column in a real `/api/v1/sql` response over that
/// tenant, once the declared-column staleness horizon has elapsed.
///
/// Three things are pinned, so a pass cannot come from something weaker than
/// the full path:
/// - before the CLI write, `SELECT dur` fails: the column does not exist, so
///   the later success is caused by the durable declaration, not by a column
///   that was there all along;
/// - the successful response reports `dur` as `Int64` and its values as JSON
///   numbers, while `attrs['dur']` in the same response is the stringified map
///   entry: a native typed column, not a restringified map read;
/// - `SUM(dur)` aggregates without any `CAST`, which is exactly what the
///   pre-ADR-0090 `Map(Utf8, Utf8)`-only table cannot do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cli_written_declaration_reaches_a_real_sql_query_as_a_typed_column() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let durations = ingest_fixture(&store).await;
    let clock = Arc::new(AtomicI64::new(CLOCK_NS + 1_000_000));
    let app = build_app(Arc::clone(&store), Arc::clone(&clock));

    // Sanity: the fixture is queryable at all, and the attribute is reachable
    // through the stringified map exactly as it was before ADR-0090.
    let (status, body) = post_sql(&app, "SELECT attrs['dur'] AS s FROM logs ORDER BY s").await;
    assert_eq!(status, StatusCode::OK, "baseline query failed: {body}");
    assert_eq!(rows(&body).len(), 3, "three fixture records: {body}");

    // Before the declaration exists, `dur` is not a column.
    let (status, body) = post_sql(&app, "SELECT dur FROM logs").await;
    assert_ne!(
        status,
        StatusCode::OK,
        "an undeclared column must not resolve: {body}"
    );

    // The CLI write: the durable per-tenant declaration.
    ravel_cli::typed_attr_column::set(
        Arc::clone(&store),
        TENANT,
        &["dur:i64".to_string()],
        CLOCK_NS,
    )
    .await
    .expect("typed-attr-column set writes the durable override");

    // Cross the staleness horizon so the next query re-resolves. (The failed
    // `SELECT dur` above cached the zero-declaration resolution; without this
    // advance the query below would legitimately still serve it.)
    clock.fetch_add(TEST_HORIZON_NS * 4, Ordering::SeqCst);

    let (status, body) =
        post_sql(&app, "SELECT dur, attrs['dur'] AS s FROM logs ORDER BY dur").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the declared column must resolve after the CLI write: {body}"
    );
    assert_eq!(
        column_type(&body, "dur"),
        "Int64",
        "the declared column is a native Arrow Int64, not a Utf8 map read: {body}"
    );
    let got: Vec<i64> = rows(&body)
        .iter()
        .map(|row| {
            row[0]
                .as_i64()
                .unwrap_or_else(|| panic!("declared column value is not a JSON number: {row}"))
        })
        .collect();
    assert_eq!(
        got, durations,
        "every declared value matches the ingested typed attribute: {body}"
    );
    // The same key read through the map is still the stringified value, which
    // is what makes the assertion above about typing rather than about
    // round-tripping some number twice.
    let as_strings: Vec<&str> = rows(&body)
        .iter()
        .map(|row| {
            row[1]
                .as_str()
                .unwrap_or_else(|| panic!("attrs['dur'] is not a JSON string: {row}"))
        })
        .collect();
    assert_eq!(as_strings, ["200", "500", "900"]);

    // A typed aggregate with no CAST: impossible over the map alone.
    let (status, body) = post_sql(&app, "SELECT SUM(dur) AS total FROM logs").await;
    assert_eq!(status, StatusCode::OK, "typed aggregate failed: {body}");
    assert_eq!(
        rows(&body)[0][0].as_i64(),
        Some(durations.iter().sum::<i64>()),
        "SUM over the declared i64 column: {body}"
    );

    // And `show` reports back exactly what was set (the operator-facing half of
    // the same durable record).
    ravel_cli::typed_attr_column::show(Arc::clone(&store), TENANT)
        .await
        .expect("show reads the declaration back");
}

/// The other half of the durable contract, through the same real query path: an
/// explicitly empty declaration (`typed-attr-column set acme` with no specs) is
/// zero declared columns for that tenant, and it overrides the CLI-derived
/// default rather than falling through to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_explicitly_empty_declaration_overrides_the_cli_default() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    ingest_fixture(&store).await;
    let clock = Arc::new(AtomicI64::new(CLOCK_NS + 1_000_000));

    // This app's CLI-derived base DOES declare `dur`, so a fall-through would
    // make the query below succeed.
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let base = TypedAttrColumnConfig::from_policy(
        ravel_server::typed_attr_config::TypedAttrColumnPolicy {
            default: vec![ravel_catalog::DeclaredTypedColumn {
                key: "dur".to_string(),
                ty: ravel_catalog::DeclaredColumnType::I64,
            }],
            tenants: Vec::new(),
        },
    )
    .expect("valid base declaration");
    let declared = Arc::new(TenantConfigDeclaredColumns::with_intervals(
        base,
        Arc::clone(&store),
        TEST_HORIZON_NS,
        TEST_BACKOFF_NS,
    ));
    let mut state = ravel_server::query::build_sql_state(
        catalog,
        Arc::clone(&store),
        Arc::new(StaticBearerTokenResolver::new(HashMap::from([(
            TOKEN.to_string(),
            TenantId::new(TENANT),
        )]))),
        None,
        ravel_query::EngineConfig::default(),
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        Some(declared),
    )
    .expect("build_sql_state");
    state.clock = Arc::new(AdvancingClock(Arc::clone(&clock)));
    let app = ravel_server::sql::router(state);

    // The CLI base declaration is in force to begin with.
    let (status, body) = post_sql(&app, "SELECT dur FROM logs").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the CLI-derived default declaration must apply with no durable override: {body}"
    );
    assert_eq!(
        rows(&body).len(),
        3,
        "and it really resolves the fixture's rows: {body}"
    );

    // An explicitly empty durable declaration takes the column away.
    ravel_cli::typed_attr_column::set(Arc::clone(&store), TENANT, &[], CLOCK_NS)
        .await
        .expect("an empty declaration is a valid write");
    clock.fetch_add(TEST_HORIZON_NS * 4, Ordering::SeqCst);

    let (status, body) = post_sql(&app, "SELECT dur FROM logs").await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a present-but-empty durable declaration means zero declared columns for \
         this tenant, not a fall-through to the CLI default: {body}"
    );
}
