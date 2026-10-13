//! Issue #2901: ClickBench q29's final aggregation runs partitioned again.
//!
//! A declared `Str` column reads as `Dictionary(Int32, Utf8)`, and under
//! DataFusion 55.2 `length(col)` and `octet_length(col)` over it can resolve to
//! `Dictionary(Int32, Int32)` rather than `Int32`. `crate::avg`'s
//! `coerce_types` admits an integer argument to `Int64` (and so to the exact
//! `i128` accumulator of ADR-0825 decision 2) only when it recognises the type
//! as an admitted integer. These tests drive the real
//! `SqlExecutor::plan_pinned` / `execute` path over a `logs` table with a
//! declared `Str` column `Referer` and a declared `Int64` column `n`, spread
//! over several objects so the scan has several partitions, and pin three
//! outcomes for each shape: the plan shape (ADR-0094's `FinalPartitioned`
//! above a hash repartition, or a single-partition `Final`), the `Partial`
//! aggregate's `avg_sum` state type (`Decimal128(38, 0)` for the exact integer
//! accumulator, `Float64` for the IEEE fold), and the returned rows with
//! `parallel_final_aggregation` on and off.
//!
//! None of the assertions names the type `length("Referer")` resolves to or
//! the text of the cast the analyzer inserts, so they hold whether that
//! function result reaches the aggregate dictionary-encoded or not.
//!
//! The integer column is named `n`, not `k`: q29 aliases its GROUP BY
//! expression `k`, and a column of the same name would take that name over.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::displayable;
use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// RLOG objects the fixture publishes; also the scan's fan-out at
/// `fetch_concurrency == OBJECTS`, so the plan has this many partitions.
const OBJECTS: usize = 4;

/// Log records per object.
const RECORDS_PER_OBJECT: usize = 30;

/// The `Referer` values the fixture cycles through. Every non-empty value
/// matches q29's pattern, the hosts repeat across values so a regexp group
/// folds more than one length, and the empty value is what q29's
/// `"Referer" <> ''` filter removes.
const REFERERS: [&str; 7] = [
    "https://www.alpha.example/a",
    "http://alpha.example/bb/cc",
    "https://beta.example/x",
    "https://beta.example/longer/path/here",
    "http://gamma.example/q?z=1",
    "https://www.delta.example/deep/er/path/than/the/rest",
    "",
];

/// The q29 statement, with `HAVING COUNT(*) > 0` in place of ClickBench's
/// `> 100000` so the fixture returns rows.
const Q29_SQL: &str = r#"SELECT REGEXP_REPLACE("Referer", '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length("Referer")) AS l, COUNT(*) AS c, MIN("Referer") FROM logs WHERE "Referer" <> '' GROUP BY k HAVING COUNT(*) > 0 ORDER BY l DESC LIMIT 25"#;

/// q29 grouped by the declared `Str` column itself.
const Q29_BY_REFERER_SQL: &str = r#"SELECT "Referer" AS k, AVG(length("Referer")) AS l, COUNT(*) AS c, MIN("Referer") FROM logs WHERE "Referer" <> '' GROUP BY "Referer" HAVING COUNT(*) > 0 ORDER BY l DESC LIMIT 25"#;

fn tenant() -> TenantId {
    TenantId::new("issue-2901-q29-exact-typed".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("Referer", DeclaredType::Str),
        DeclaredColumn::new("n", DeclaredType::I64),
    ]
}

/// The `(Referer, n)` pair record `index` of object `object` carries.
fn fixture_values(object: usize, index: usize) -> (&'static str, i64) {
    (
        REFERERS[(index + object) % REFERERS.len()],
        (index + 3 * object) as i64,
    )
}

/// Every `(Referer, n)` pair the fixture publishes, across all objects.
fn all_fixture_values() -> Vec<(&'static str, i64)> {
    (0..OBJECTS)
        .flat_map(|object| (0..RECORDS_PER_OBJECT).map(move |i| fixture_values(object, i)))
        .collect()
}

fn record(ts: i64, referer: &str, n: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("web".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: String::new(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("Referer".to_string(), AttrValue::Str(referer.to_string())),
            ("n".to_string(), AttrValue::I64(n)),
        ],
    }
}

async fn publish_object(store: &dyn ObjectStoreBackend, tenant: &TenantId, object: usize) {
    let base_ts = object as i64 * 100_000;
    let records: Vec<LogRecord> = (0..RECORDS_PER_OBJECT)
        .map(|i| {
            let (referer, n) = fixture_values(object, i);
            record(base_ts + i as i64, referer, n)
        })
        .collect();

    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: *Uuid::from_u128(0x2901_0000 + object as u128).as_bytes(),
        writer_epoch: 1,
        writer_seq: object as u64 + 1,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity);
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(0x2901_0000 + object as u128),
        writer_epoch: 1,
        writer_seq: object as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash: [object as u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

struct Fixture {
    executor: SqlExecutor,
    catalog: Arc<Catalog>,
}

impl Fixture {
    async fn build(parallel_final_aggregation: bool) -> Self {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let t = tenant();
        for object in 0..OBJECTS {
            publish_object(store.as_ref(), &t, object).await;
        }
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let config = SqlConfig {
            engine: EngineConfig {
                fetch_concurrency: OBJECTS,
                ..EngineConfig::default()
            },
            parallel_final_aggregation,
            ..SqlConfig::default()
        };
        let executor = SqlExecutor::new(
            Arc::clone(&catalog),
            SegmentFetcher::new(Arc::clone(&store)),
            LogSegmentFetcher::new(Arc::clone(&store)),
            SpanSegmentFetcher::new(Arc::clone(&store)),
            config,
            1 << 30,
        )
        .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())));
        Fixture { executor, catalog }
    }

    async fn snapshot(&self) -> Snapshot {
        self.catalog
            .resolve(
                &tenant().hash(),
                Signal::Logs,
                TimeRange {
                    start_ns: 0,
                    end_ns: 1_000_000,
                },
                &[],
                1_000_000,
            )
            .await
            .expect("resolve logs snapshot")
    }

    /// The indented physical plan for `sql` through `plan_pinned`, with each
    /// operator's output schema so the `Partial` state types are visible.
    async fn plan(&self, sql: &str) -> String {
        let accounting = QueryAccounting::new();
        let planned = self
            .executor
            .plan_pinned(
                tenant().hash(),
                self.snapshot().await,
                sql,
                &accounting,
                &declared(),
            )
            .await
            .expect("plan_pinned");
        let plan = planned
            .create_physical_plan()
            .await
            .expect("physical plan builds");
        format!(
            "{}",
            displayable(plan.as_ref())
                .set_show_schema(true)
                .indent(true)
        )
    }

    /// Every row `sql` returns, each cell rendered by [`render_column`], sorted.
    async fn rows(&self, sql: &str) -> Vec<Vec<String>> {
        let outcome = self
            .executor
            .execute(tenant().hash(), &request(sql))
            .await
            .expect("query executes");
        let mut rows = Vec::new();
        for batch in outcome.output.batches() {
            let columns: Vec<Vec<String>> = batch.columns().iter().map(render_column).collect();
            for row in 0..batch.num_rows() {
                rows.push(columns.iter().map(|column| column[row].clone()).collect());
            }
        }
        rows.sort();
        rows
    }
}

/// A Float64 cell as its raw bits, so row comparisons are bit-exact; any other
/// cell as its Utf8 cast.
fn render_column(array: &ArrayRef) -> Vec<String> {
    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        return (0..floats.len())
            .map(|i| {
                if floats.is_null(i) {
                    "null".to_string()
                } else {
                    float_cell(floats.value(i))
                }
            })
            .collect();
    }
    let text = cast(array, &DataType::Utf8).expect("result column casts to Utf8");
    let text = text
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a Utf8 cast yields a StringArray");
    (0..text.len())
        .map(|i| {
            if text.is_null(i) {
                "null".to_string()
            } else {
                text.value(i).to_string()
            }
        })
        .collect()
}

fn float_cell(value: f64) -> String {
    format!("f64:{:#018x}", value.to_bits())
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: 1_000_000,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// q29's grouping key for one `Referer`: the host between the scheme (and an
/// optional `www.`) and the first `/` after it. Every non-empty fixture value
/// matches the pattern, so this never needs the no-match branch.
fn host_of(referer: &str) -> String {
    let rest = referer
        .strip_prefix("https://")
        .or_else(|| referer.strip_prefix("http://"))
        .expect("every fixture Referer carries an http(s) scheme");
    let rest = rest.strip_prefix("www.").unwrap_or(rest);
    rest.split('/')
        .next()
        .expect("split yields a first piece")
        .to_string()
}

/// The exact average the integer accumulator returns: the exact sum converted
/// to f64 once, divided by the count in one IEEE division.
fn exact_avg(sum: i64, count: i64) -> f64 {
    sum as f64 / count as f64
}

/// The rows q29 must return over the fixture: `[host, avg length bits, count,
/// min Referer]` per host, sorted.
fn expected_q29_rows() -> Vec<Vec<String>> {
    let mut groups: BTreeMap<String, (i64, i64, &str)> = BTreeMap::new();
    for (referer, _) in all_fixture_values() {
        if referer.is_empty() {
            continue;
        }
        let entry = groups.entry(host_of(referer)).or_insert((0, 0, referer));
        entry.0 += referer.chars().count() as i64;
        entry.1 += 1;
        entry.2 = entry.2.min(referer);
    }
    let mut rows: Vec<Vec<String>> = groups
        .into_iter()
        .map(|(host, (sum, count, min))| {
            vec![
                host,
                float_cell(exact_avg(sum, count)),
                count.to_string(),
                min.to_string(),
            ]
        })
        .collect();
    rows.sort();
    rows
}

/// The rows `SELECT n % 3 AS g, avg(<length of Referer>) ... GROUP BY g` must
/// return: `[g, avg bits]`, sorted. Every fixture value is ASCII, so the
/// character length and the octet length agree.
fn expected_length_by_n_mod_3_rows() -> Vec<Vec<String>> {
    let mut groups: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    for (referer, n) in all_fixture_values() {
        let entry = groups.entry(n % 3).or_insert((0, 0));
        entry.0 += referer.len() as i64;
        entry.1 += 1;
    }
    let mut rows: Vec<Vec<String>> = groups
        .into_iter()
        .map(|(g, (sum, count))| vec![g.to_string(), float_cell(exact_avg(sum, count))])
        .collect();
    rows.sort();
    rows
}

/// The 0-based line index of the first plan line containing `needle`.
fn line_of(plan: &str, needle: &str) -> usize {
    plan.lines()
        .position(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("expected a plan line containing {needle:?}; got:\n{plan}"))
}

fn assert_final_partitioned(plan: &str, what: &str) {
    assert!(
        plan.contains("AggregateExec: mode=FinalPartitioned"),
        "{what}: an exact-typed plan must fan its final aggregation out \
         (FinalPartitioned); got:\n{plan}"
    );
    assert!(
        plan.contains("RepartitionExec: partitioning=Hash"),
        "{what}: the fanned-out final aggregate must be fed by a hash repartition; \
         got:\n{plan}"
    );
    assert!(
        line_of(plan, "AggregateExec: mode=FinalPartitioned")
            < line_of(plan, "RepartitionExec: partitioning=Hash"),
        "{what}: FinalPartitioned must sit above the hash repartition; got:\n{plan}"
    );
}

fn assert_single_partition_final(plan: &str, what: &str) {
    assert!(
        !plan.contains("AggregateExec: mode=FinalPartitioned"),
        "{what}: a plan that is not exact-typed must not fan its final aggregation \
         out; got:\n{plan}"
    );
    assert!(
        !plan.contains("RepartitionExec: partitioning=Hash"),
        "{what}: a plan that is not exact-typed must not get a hash repartition; \
         got:\n{plan}"
    );
    assert!(
        plan.contains("AggregateExec: mode=Final,"),
        "{what}: a plan that is not exact-typed keeps a single-partition Final \
         aggregate; got:\n{plan}"
    );
}

/// The `Partial` aggregate's plan line.
fn partial_line<'a>(plan: &'a str, what: &str) -> &'a str {
    plan.lines()
        .find(|line| line.contains("AggregateExec: mode=Partial"))
        .unwrap_or_else(|| panic!("{what}: expected a Partial AggregateExec; got:\n{plan}"))
}

fn assert_exact_avg_state(plan: &str, what: &str) {
    let partial = partial_line(plan, what);
    assert!(
        partial.contains("[avg_sum]:Decimal128(38, 0)"),
        "{what}: an integer avg argument must run the exact integer accumulator, whose \
         partial sum state is Decimal128(38, 0) (ADR-0825 decision 2); got:\n{partial}"
    );
    assert!(
        !partial.contains("[avg_sum]:Float64"),
        "{what}: no avg in this statement may fold its sum as Float64; got:\n{partial}"
    );
}

#[tokio::test]
async fn q29_plans_final_partitioned_with_an_exact_avg_state() {
    let on = Fixture::build(true).await;
    let off = Fixture::build(false).await;

    let plan = on.plan(Q29_SQL).await;
    assert_final_partitioned(&plan, "q29");
    assert_exact_avg_state(&plan, "q29");

    let on_rows = on.rows(Q29_SQL).await;
    let off_rows = off.rows(Q29_SQL).await;
    let expected = expected_q29_rows();
    assert_eq!(
        expected.len(),
        4,
        "the fixture spreads its non-empty Referer values over four hosts"
    );
    assert_eq!(
        on_rows, expected,
        "q29 must return the exact per-host avg, count and min"
    );
    assert_eq!(
        on_rows, off_rows,
        "q29 must return identical rows with parallel_final_aggregation on and off"
    );
}

#[tokio::test]
async fn avg_of_string_length_grouped_by_int_key_is_exact() {
    let on = Fixture::build(true).await;
    let off = Fixture::build(false).await;
    let expected = expected_length_by_n_mod_3_rows();
    assert_eq!(expected.len(), 3, "n % 3 has three groups over the fixture");

    for function in ["length", "octet_length"] {
        let sql =
            format!(r#"SELECT n % 3 AS g, avg({function}("Referer")) AS l FROM logs GROUP BY g"#);
        let plan = on.plan(&sql).await;
        assert_final_partitioned(&plan, function);
        assert_exact_avg_state(&plan, function);

        let on_rows = on.rows(&sql).await;
        let off_rows = off.rows(&sql).await;
        assert_eq!(
            on_rows, expected,
            "avg({function}(\"Referer\")) must return the exact per-group average"
        );
        assert_eq!(
            on_rows, off_rows,
            "avg({function}(\"Referer\")) must return identical rows with \
             parallel_final_aggregation on and off"
        );
    }
}

#[tokio::test]
async fn q29_grouped_by_the_string_column_plans_final_partitioned() {
    let on = Fixture::build(true).await;
    let plan = on.plan(Q29_BY_REFERER_SQL).await;
    assert_final_partitioned(&plan, "q29 grouped by Referer");
}

#[tokio::test]
async fn float_avg_argument_stays_single_partition() {
    let on = Fixture::build(true).await;

    let float_avg = r#"SELECT REGEXP_REPLACE("Referer", '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(CAST(length("Referer") AS DOUBLE)) AS l, COUNT(*) AS c FROM logs WHERE "Referer" <> '' GROUP BY k"#;
    let plan = on.plan(float_avg).await;
    assert_single_partition_final(&plan, "avg over a DOUBLE cast");
    let partial = partial_line(&plan, "avg over a DOUBLE cast");
    assert!(
        partial.contains("[avg_sum]:Float64"),
        "avg over a Float64 argument folds its sum as Float64; got:\n{partial}"
    );
}

#[tokio::test]
async fn float_group_key_stays_single_partition() {
    let on = Fixture::build(true).await;

    // Every aggregate here is exact, so the float key is the only thing that
    // keeps this plan single-partition.
    let float_key =
        "SELECT CAST(n AS DOUBLE) AS f, COUNT(*) AS c, SUM(n) AS s FROM logs GROUP BY f";
    let plan = on.plan(float_key).await;
    assert_single_partition_final(&plan, "a float GROUP BY key");
}

/// Float-valued expressions over `length("Referer")`. Each must keep the
/// final aggregation single-partition as a `sum` argument and as a GROUP BY
/// key.
const FLOAT_EXPRESSIONS: [&str; 5] = [
    r#"sqrt(length("Referer"))"#,
    r#"ln(length("Referer"))"#,
    r#"length("Referer") * 1.5"#,
    r#"length("Referer") / 2.0"#,
    r#"CAST(length("Referer") AS DOUBLE)"#,
];

#[tokio::test]
async fn float_functions_of_string_length_stay_single_partition() {
    let on = Fixture::build(true).await;
    for expression in FLOAT_EXPRESSIONS {
        let as_sum = format!("SELECT n % 3 AS g, SUM({expression}) AS s FROM logs GROUP BY g");
        let plan = on.plan(&as_sum).await;
        assert_single_partition_final(&plan, &format!("sum({expression})"));

        let as_key = format!("SELECT {expression} AS f, COUNT(*) AS c FROM logs GROUP BY f");
        let plan = on.plan(&as_key).await;
        assert_single_partition_final(&plan, &format!("GROUP BY {expression}"));
    }
}

#[tokio::test]
async fn integer_function_of_string_length_plans_final_partitioned() {
    let on = Fixture::build(true).await;
    let expression = r#"abs(length("Referer"))"#;

    let as_sum = format!("SELECT n % 3 AS g, SUM({expression}) AS s FROM logs GROUP BY g");
    let plan = on.plan(&as_sum).await;
    assert_final_partitioned(&plan, &format!("sum({expression})"));

    let as_key = format!("SELECT {expression} AS a, COUNT(*) AS c FROM logs GROUP BY a");
    let plan = on.plan(&as_key).await;
    assert_final_partitioned(&plan, &format!("GROUP BY {expression}"));
}
