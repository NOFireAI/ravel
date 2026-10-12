//! A scalar function over a declared `Str` column, which reaches Arrow as
//! `Dictionary(Int32, Utf8)` (ADR-0099 decision 5), is typed by the value it
//! computes, not by that wire type (issue #2918). DataFusion 55.2's encoding
//! preservation makes `length` and its relatives return
//! `Dictionary(Int32, Int32)`; the session plans each such call as
//! `CAST(<call> AS Int32)` instead, so unary minus plans and an integer `avg`
//! over it takes its exact, partitioned path.
//!
//! Everything here goes through the production session: `build_session` for
//! the registry walk, and `SqlExecutor::plan_pinned` / `explain` / `execute`
//! (the `/api/v1/sql` entry point) for plans and answers.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::empty::EmptyTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher, PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable,
    SpanSegmentFetcher, SpillDecision, SqlConfig, SqlExecutor, SqlRequest, StaticDeclaredColumns,
    TenantDelegatingPool, TenantMemoryAccountant, build_session, logs_schema_with_declared,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// RLOG objects the fixture publishes, and the scan fan-out, so a
/// repartitioned final aggregate really has several partitions.
const OBJECTS: usize = 4;

/// The `(k, Referer)` rows every object carries. `length("Referer")` is 1 and
/// 3 for `k = 0` and 2 and 5 for `k = 1`, so the per-`k` averages are 2.0 and
/// 3.5 whatever the object count.
const ROWS: [(i64, &str); 4] = [(0, "a"), (0, "abc"), (1, "ab"), (1, "abcde")];

fn tenant() -> TenantId {
    TenantId::new("issue-2918-declared-str-function-results".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("Referer", DeclaredType::Str),
        DeclaredColumn::new("k", DeclaredType::I64),
    ]
}

fn log_record(ts: i64, k: i64, referer: &str) -> LogRecord {
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
        body: String::new(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("Referer".to_string(), AttrValue::Str(referer.to_string())),
            ("k".to_string(), AttrValue::I64(k)),
        ],
    }
}

async fn publish_object(store: &dyn ObjectStoreBackend, tenant: &TenantId, object: usize) {
    let base_ts = object as i64 * 100_000;
    let records: Vec<LogRecord> = ROWS
        .iter()
        .enumerate()
        .map(|(i, (k, referer))| log_record(base_ts + i as i64, *k, referer))
        .collect();
    let writer_id = Uuid::from_u128(0x2918_0000 + object as u128);
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: *writer_id.as_bytes(),
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
        writer_id,
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
    async fn build() -> Self {
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
            .resolve(&tenant().hash(), Signal::Logs, window(), &[], 1_000_000)
            .await
            .expect("resolve logs snapshot")
    }

    /// The output `(name, type)` of `sql` as `plan_pinned` reports it, and
    /// the indented physical plan it builds.
    async fn plan(&self, sql: &str) -> (Vec<(String, DataType)>, String) {
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
            .unwrap_or_else(|e| panic!("{sql} plans: {e}"));
        let fields = fields_of(&planned.schema());
        let physical = planned
            .create_physical_plan()
            .await
            .expect("physical plan builds");
        (
            fields,
            format!("{}", displayable(physical.as_ref()).indent(true)),
        )
    }

    async fn execute(&self, sql: &str) -> Vec<datafusion::arrow::array::RecordBatch> {
        let outcome = self
            .executor
            .execute(tenant().hash(), &request(sql))
            .await
            .unwrap_or_else(|e| panic!("{sql} executes: {e}"));
        outcome
            .output
            .batches()
            .iter()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect()
    }
}

fn window() -> TimeRange {
    TimeRange {
        start_ns: 0,
        end_ns: 1_000_000,
    }
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: window(),
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

fn fields_of(schema: &datafusion::arrow::datatypes::Schema) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect()
}

fn empty_snapshot() -> Snapshot {
    Snapshot {
        segments: Vec::new(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// The session `build_session` gives a `logs` statement over the declared
/// columns: the production session, every scalar registration included.
fn production_session() -> SessionContext {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let table = SessionTable::Logs(Arc::new(
        LogsTableProvider::new(
            empty_snapshot(),
            tenant().hash(),
            LogSegmentFetcher::new(store),
            PhaseAccounting::new(),
        )
        .with_declared_columns(declared()),
    ));
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    build_session(
        &SqlConfig::default(),
        pool,
        table,
        false,
        SpillDecision::Disabled,
    )
    .expect("session builds")
}

/// A stock DataFusion session over the same `logs` schema: the types a call
/// has before the rule.
fn stock_session() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table(
        "logs",
        Arc::new(EmptyTable::new(logs_schema_with_declared(&declared()))),
    )
    .expect("register logs");
    ctx
}

/// The single output column's type of `sql` in `ctx`, unoptimized and
/// optimized, or the planning error.
async fn planned_types(ctx: &SessionContext, sql: &str) -> Result<(DataType, DataType), String> {
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let unoptimized = df.schema().field(0).data_type().clone();
    let optimized = ctx
        .state()
        .optimize(df.logical_plan())
        .map_err(|e| e.to_string())?;
    Ok((unoptimized, optimized.schema().field(0).data_type().clone()))
}

fn is_dictionary_of_non_strings(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(_, value)
            if !matches!(**value, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
    )
}

/// Argument lists tried in order for each function; the first that plans is
/// the one examined. `c` is the declared `Str` column and `n` is
/// `length(c)`, an integer computed from it, for the numeric functions.
const CALL_SHAPES: [&str; 12] = [
    "c",
    "c, 1",
    "c, 'a'",
    "c, 1, 2",
    "c, 'a', 'b'",
    "c, 1, 'a'",
    "c, 'a', 1",
    "'a', c",
    "c, c",
    "n",
    "n, 1",
    "n, n",
];

fn call_sql(function: &str, shape: &str) -> String {
    format!(
        "SELECT {function}({}) AS v FROM logs",
        shape
            .replace('c', "\"Referer\"")
            .replace('n', "length(\"Referer\")")
    )
}

#[tokio::test]
async fn avg_of_length_over_a_declared_str_is_exact_and_partitioned() {
    let fixture = Fixture::build().await;
    let sql = "SELECT k, avg(length(\"Referer\")) AS a FROM logs GROUP BY k ORDER BY k";

    let (_, plan) = fixture.plan(sql).await;
    assert!(
        plan.contains("AggregateExec: mode=FinalPartitioned"),
        "avg over an integer computed from a declared Str is exact, so its final \
         aggregation fans out; got:\n{plan}"
    );
    let partitions = plan
        .lines()
        .find_map(|l| {
            l.split("partitioning=Hash(")
                .nth(1)
                .and_then(|rest| rest.split("], ").nth(1))
                .and_then(|n| n.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|n| n.parse::<usize>().ok())
        })
        .unwrap_or_else(|| panic!("a hash repartition with a partition count; got:\n{plan}"));
    assert!(
        partitions > 1,
        "the final aggregate's target partitions; got {partitions} in:\n{plan}"
    );

    let mut rows = Vec::new();
    for batch in fixture.execute(sql).await {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("k is Int64");
        let avgs = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("avg is Float64");
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), avgs.value(row).to_bits()));
        }
    }
    assert_eq!(
        rows,
        vec![(0, 2.0f64.to_bits()), (1, 3.5f64.to_bits())],
        "avg(length) per k: (1 + 3) / 2 and (2 + 5) / 2"
    );
}

#[tokio::test]
async fn negated_length_over_a_declared_str_plans_and_answers() {
    let fixture = Fixture::build().await;
    let sql = "SELECT -length(\"Referer\") FROM logs ORDER BY 1";
    let batches = fixture.execute(sql).await;
    let mut values = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("-length is Int32");
        values.extend((0..column.len()).map(|row| column.value(row)));
    }
    let mut expected = Vec::new();
    for value in [-5, -3, -2, -1] {
        expected.extend(std::iter::repeat_n(value, OBJECTS));
    }
    assert_eq!(values, expected);
}

#[tokio::test]
async fn no_scalar_function_returns_a_dictionary_of_non_strings() {
    let production = production_session();
    let stock = stock_session();
    let names: Vec<String> = production
        .state()
        .scalar_functions()
        .values()
        .map(|udf| udf.name().to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    let mut examined: BTreeMap<String, (String, String, DataType, DataType)> = BTreeMap::new();
    let mut skipped = Vec::new();
    let mut offenders = Vec::new();
    for name in &names {
        let mut found = None;
        for shape in CALL_SHAPES {
            let sql = call_sql(name, shape);
            if let Ok((unoptimized, optimized)) = planned_types(&production, &sql).await {
                found = Some((shape, sql, unoptimized, optimized));
                break;
            }
        }
        let Some((shape, sql, unoptimized, optimized)) = found else {
            println!("{name:<24} skipped: no call shape plans");
            skipped.push(name.clone());
            continue;
        };
        let before = match planned_types(&stock, &sql).await {
            Ok((before, _)) => format!("{before}"),
            Err(_) => "(not in a stock session)".to_string(),
        };
        println!(
            "{name:<24} ({shape:<12}) before: {before:<28} after: {unoptimized} / {optimized}"
        );
        if is_dictionary_of_non_strings(&unoptimized) || is_dictionary_of_non_strings(&optimized) {
            offenders.push(format!("{name}({shape}): {unoptimized} / {optimized}"));
        }
        examined.insert(
            name.clone(),
            (shape.to_string(), before, unoptimized, optimized),
        );
    }
    println!(
        "examined {}, skipped {}: {skipped:?}",
        examined.len(),
        skipped.len()
    );

    assert!(
        offenders.is_empty(),
        "functions still typed as a dictionary of non-strings: {offenders:#?}"
    );
    assert!(
        examined.len() >= 20,
        "only {} functions planned with a declared Str argument",
        examined.len()
    );
    for name in ["character_length", "octet_length", "bit_length", "ascii"] {
        let (_, before, unoptimized, optimized) = examined
            .get(name)
            .unwrap_or_else(|| panic!("{name} examined"));
        assert_eq!(before, "Dictionary(Int32, Int32)", "{name} before the rule");
        assert_eq!(unoptimized, &DataType::Int32, "{name} after the rule");
        assert_eq!(optimized, &DataType::Int32, "{name} after optimization");
    }
    // `length` and `char_length` are aliases of `character_length`, which the
    // walk above examined under that name; each spelling is checked here.
    for alias in ["length", "char_length"] {
        let sql = call_sql(alias, "c");
        let (before, _) = planned_types(&stock, &sql).await.expect("stock plans");
        assert_eq!(
            before,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
            "{alias} before the rule"
        );
        let (unoptimized, optimized) = planned_types(&production, &sql)
            .await
            .expect("production plans");
        assert_eq!(
            (unoptimized, optimized),
            (DataType::Int32, DataType::Int32),
            "{alias} after the rule"
        );
    }
    let (unoptimized, optimized) = planned_types(
        &production,
        "SELECT abs(length(\"Referer\")) AS v FROM logs",
    )
    .await
    .expect("abs(length) plans");
    assert_eq!((unoptimized, optimized), (DataType::Int32, DataType::Int32));
}

#[tokio::test]
async fn the_cast_wraps_the_result_not_the_argument() {
    let fixture = Fixture::build().await;
    let sql = "SELECT length(\"Referer\") FROM logs";
    let (_, physical) = fixture.plan(sql).await;
    let explain = fixture
        .executor
        .explain(tenant().hash(), &request(sql))
        .await
        .expect("explain");
    for plan in [physical.as_str(), explain.plan_text.as_str()] {
        assert!(
            !plan.contains("character_length(CAST("),
            "the dictionary column reaches the function uncast; got:\n{plan}"
        );
        assert!(
            !plan.contains("AS Utf8)") && !plan.contains("AS Utf8View)"),
            "nothing is cast to a string type; got:\n{plan}"
        );
        assert!(
            plan.contains(
                "ProjectionExec: expr=[CAST(character_length(Referer@0) AS Int32) \
                 as length(logs.Referer)]"
            ),
            "the function runs on the dictionary column and its result is cast; got:\n{plan}"
        );
    }
}

#[tokio::test]
async fn length_keeps_its_output_name_and_type() {
    let fixture = Fixture::build().await;
    let stock = stock_session();
    for sql in [
        "SELECT length(\"Referer\") FROM logs",
        "SELECT length(\"Referer\"), count(*) FROM logs GROUP BY 1",
    ] {
        let stock_name = stock
            .sql(sql)
            .await
            .expect("stock plans")
            .schema()
            .field(0)
            .name()
            .clone();
        let (fields, _) = fixture.plan(sql).await;
        assert_eq!(
            fields[0],
            (stock_name.clone(), DataType::Int32),
            "{sql}: planned name and type"
        );
        assert!(!fields[0].0.contains("CAST("), "{sql}: {fields:?}");
        let batches = fixture.execute(sql).await;
        let executed = batches.first().expect("rows").schema();
        assert_eq!(
            (executed.field(0).name(), executed.field(0).data_type()),
            (&stock_name, &DataType::Int32),
            "{sql}: executed name and type"
        );
    }
}

/// Calls whose result is a string, or the column itself, keep the types they
/// had before the rule: the rule only touches dictionaries of non-strings.
/// The expected types are the ones these statements planned to on `main`
/// before this rule existed.
#[tokio::test]
async fn string_results_and_the_column_keep_their_types() {
    let production = production_session();
    let utf8_dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    for (sql, expected) in [
        (
            "SELECT regexp_replace(\"Referer\", 'a', 'b') AS v FROM logs",
            DataType::Utf8View,
        ),
        (
            "SELECT lower(\"Referer\") AS v FROM logs",
            utf8_dictionary.clone(),
        ),
        ("SELECT \"Referer\" AS v FROM logs", utf8_dictionary.clone()),
    ] {
        let (unoptimized, optimized) = planned_types(&production, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} plans: {e}"));
        assert_eq!((&unoptimized, &optimized), (&expected, &expected), "{sql}");
    }
}
