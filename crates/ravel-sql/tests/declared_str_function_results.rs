//! A scalar function over a declared `Str` column, which reaches Arrow as
//! `Dictionary(Int32, Utf8)` (ADR-0099 decision 5), is typed by the value it
//! computes, not by that wire type. DataFusion 55.2's encoding preservation
//! makes `length` and its relatives return `Dictionary(Int32, Int32)`; the
//! session plans each such call as `CAST(<call> AS Int32)` instead, so unary
//! minus plans and an integer `avg` over it takes its exact, partitioned
//! path. Only the functions that return such a dictionary are replaced, so a
//! call an expression planner builds and its by-name spelling stay one
//! expression.
//!
//! Everything here goes through the production session: `build_session` for
//! the registry walk, and `SqlExecutor::plan_pinned` / `explain` / `execute`
//! (the `/api/v1/sql` entry point) for plans and answers.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    Array, DictionaryArray, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::empty::EmptyTable;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::expressions::CastExpr;
use datafusion::physical_plan::{ExecutionPlan, displayable};
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
        let (fields, physical) = self.physical_plan(sql).await;
        (
            fields,
            format!("{}", displayable(physical.as_ref()).indent(true)),
        )
    }

    /// The output `(name, type)` of `sql` and the physical plan it builds.
    async fn physical_plan(&self, sql: &str) -> (Vec<(String, DataType)>, Arc<dyn ExecutionPlan>) {
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
        (fields, physical)
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

/// The type of the first argument of the first partial aggregate in `plan`,
/// and, when that argument is a cast, the type of the cast's input.
fn partial_aggregate_argument(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<(DataType, Option<DataType>)> {
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && *aggregate.mode() == AggregateMode::Partial
    {
        let schema = aggregate.input_schema();
        let argument = aggregate
            .aggr_expr()
            .first()?
            .expressions()
            .into_iter()
            .next()?;
        let under_cast = argument
            .downcast_ref::<CastExpr>()
            .and_then(|cast| cast.expr().data_type(&schema).ok());
        return Some((argument.data_type(&schema).ok()?, under_cast));
    }
    plan.children()
        .into_iter()
        .find_map(partial_aggregate_argument)
}

fn is_dictionary_of_non_strings(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(_, value)
            if !matches!(**value, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
    )
}

/// Argument lists tried for each function; every one that plans is checked.
/// `c` is the declared `Str` column and `n` is `length(c)`, an integer
/// computed from it, for the numeric functions.
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

    let (_, physical) = fixture.physical_plan(sql).await;
    let plan = format!("{}", displayable(physical.as_ref()).indent(true));
    let (argument, under_cast) = partial_aggregate_argument(&physical)
        .unwrap_or_else(|| panic!("a partial aggregate with an argument; got:\n{plan}"));
    assert!(
        argument == DataType::Int32
            || (argument == DataType::Int64 && under_cast == Some(DataType::Int32)),
        "avg's argument is Int32 or a cast of it to Int64, never a dictionary or a Float64; \
         got {argument} over {under_cast:?} in:\n{plan}"
    );
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

/// The functions no call shape plans for, and why: each needs an argument
/// none of [`CALL_SHAPES`] supplies.
const SKIPPED: [(&str, &str); 6] = [
    ("arrow_cast", "its second argument must name an Arrow type"),
    (
        "arrow_try_cast",
        "its second argument must name an Arrow type",
    ),
    ("get_field", "its first argument must be a struct or a map"),
    ("pi", "it takes no arguments"),
    ("to_local_time", "its argument must be a timestamp"),
    ("union_extract", "its first argument must be a union"),
];

/// The functions whose stock result is a dictionary of non-strings for a
/// call shape over the declared `Str` column, by canonical name.
const STOCK_NUMERIC_DICTIONARIES: [&str; 5] = [
    "ascii",
    "bit_length",
    "character_length",
    "coalesce",
    "octet_length",
];

#[tokio::test]
async fn no_scalar_function_returns_a_dictionary_of_non_strings() {
    let production = production_session();
    let stock = stock_session();
    let names: Vec<String> = production
        .state()
        .scalar_functions()
        .values()
        .map(|udf| udf.name().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let mut examined = BTreeSet::new();
    let mut skipped = Vec::new();
    let mut offenders = Vec::new();
    let mut stock_numeric = BTreeSet::new();
    for name in &names {
        for shape in CALL_SHAPES {
            let sql = call_sql(name, shape);
            let Ok((unoptimized, optimized)) = planned_types(&production, &sql).await else {
                continue;
            };
            let before = planned_types(&stock, &sql)
                .await
                .ok()
                .map(|(before, _)| before);
            if is_dictionary_of_non_strings(&unoptimized)
                || is_dictionary_of_non_strings(&optimized)
            {
                offenders.push(format!("{name}({shape}): {unoptimized} / {optimized}"));
            }
            if !shape.contains('n') && before.as_ref().is_some_and(is_dictionary_of_non_strings) {
                stock_numeric.insert(name.clone());
            }
            // The first shape that plans, and every shape whose result is a
            // dictionary before or after the rule.
            let dictionary = |t: &DataType| matches!(t, DataType::Dictionary(..));
            if examined.insert(name.clone())
                || before.as_ref().is_some_and(dictionary)
                || dictionary(&unoptimized)
            {
                let before = before
                    .map_or_else(|| "(not in a stock session)".to_string(), |t| t.to_string());
                println!(
                    "{name:<24} ({shape:<12}) before: {before:<28} after: {unoptimized} / {optimized}"
                );
            }
        }
        if !examined.contains(name) {
            println!("{name:<24} skipped: no call shape plans");
            skipped.push(name.clone());
        }
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
    assert_eq!(
        skipped,
        SKIPPED.map(|(name, _)| name.to_string()),
        "the functions no call shape plans for"
    );
    assert_eq!(examined.len(), names.len() - SKIPPED.len());
    assert_eq!(
        stock_numeric,
        BTreeSet::from(STOCK_NUMERIC_DICTIONARIES.map(String::from)),
        "the functions whose stock result is a dictionary of non-strings"
    );

    // Each spelling of each affected function, at the shape that reaches a
    // numeric dictionary in a stock session, plans as the value type.
    for (name, shape, value) in [
        ("character_length", "c", DataType::Int32),
        ("length", "c", DataType::Int32),
        ("char_length", "c", DataType::Int32),
        ("octet_length", "c", DataType::Int32),
        ("bit_length", "c", DataType::Int32),
        ("ascii", "c", DataType::Int32),
        ("coalesce", "c, 1", DataType::Int64),
    ] {
        let sql = call_sql(name, shape);
        let (before, _) = planned_types(&stock, &sql).await.expect("stock plans");
        assert_eq!(
            before,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(value.clone())),
            "{name}({shape}) before the rule"
        );
        let (unoptimized, optimized) = planned_types(&production, &sql)
            .await
            .expect("production plans");
        assert_eq!(
            (unoptimized, optimized),
            (value.clone(), value),
            "{name}({shape}) after the rule"
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
    for (sql, data_type) in [
        ("SELECT length(\"Referer\") FROM logs", DataType::Int32),
        (
            "SELECT length(\"Referer\"), count(*) FROM logs GROUP BY 1",
            DataType::Int32,
        ),
        (
            "SELECT length(\"Referer\") FROM logs ORDER BY length(\"Referer\")",
            DataType::Int32,
        ),
        (
            "SELECT count(*) OVER (PARTITION BY length(\"Referer\")) FROM logs",
            DataType::Int64,
        ),
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
            (stock_name.clone(), data_type.clone()),
            "{sql}: planned name and type"
        );
        assert!(!fields[0].0.contains("CAST("), "{sql}: {fields:?}");
        let batches = fixture.execute(sql).await;
        let executed = batches.first().expect("rows").schema();
        assert_eq!(
            (executed.field(0).name(), executed.field(0).data_type()),
            (&stock_name, &data_type),
            "{sql}: executed name and type"
        );
    }
}

/// Calls whose result is a string, and the column itself, keep their stock
/// DataFusion types: the rule only touches dictionaries of non-strings.
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

/// The rows of `sql`, rendered, so two spellings of one statement compare.
async fn rendered_rows(fixture: &Fixture, sql: &str) -> String {
    datafusion::arrow::util::pretty::pretty_format_batches(&fixture.execute(sql).await)
        .expect("render rows")
        .to_string()
}

/// A call an expression planner builds from SQL syntax and the same call
/// spelled by name are one expression, so a GROUP BY written in one spelling
/// matches a SELECT written in the other.
#[tokio::test]
async fn a_syntax_built_call_matches_its_by_name_spelling() {
    let fixture = Fixture::build().await;
    let mut failures = Vec::new();
    for (mixed, single) in [
        (
            "SELECT EXTRACT(HOUR FROM ts) AS g, count(*) AS n FROM logs \
             GROUP BY date_part('HOUR', ts) ORDER BY 1",
            "SELECT EXTRACT(HOUR FROM ts) AS g, count(*) AS n FROM logs \
             GROUP BY EXTRACT(HOUR FROM ts) ORDER BY 1",
        ),
        (
            "SELECT attrs['Referer'] AS g, count(*) AS n FROM logs \
             GROUP BY get_field(attrs, 'Referer') ORDER BY 1",
            "SELECT attrs['Referer'] AS g, count(*) AS n FROM logs \
             GROUP BY attrs['Referer'] ORDER BY 1",
        ),
        (
            "SELECT SUBSTRING(\"Referer\" FROM 1 FOR 3) AS g, count(*) AS n FROM logs \
             GROUP BY \"substr\"(\"Referer\", 1, 3) ORDER BY 1",
            "SELECT SUBSTRING(\"Referer\" FROM 1 FOR 3) AS g, count(*) AS n FROM logs \
             GROUP BY SUBSTRING(\"Referer\" FROM 1 FOR 3) ORDER BY 1",
        ),
        (
            "SELECT position('b' IN \"Referer\") AS g, count(*) AS n FROM logs \
             GROUP BY strpos(\"Referer\", 'b') ORDER BY 1",
            "SELECT position('b' IN \"Referer\") AS g, count(*) AS n FROM logs \
             GROUP BY position('b' IN \"Referer\") ORDER BY 1",
        ),
    ] {
        let accounting = QueryAccounting::new();
        if let Err(e) = fixture
            .executor
            .plan_pinned(
                tenant().hash(),
                fixture.snapshot().await,
                mixed,
                &accounting,
                &declared(),
            )
            .await
        {
            failures.push(format!("{mixed}\n  {e}"));
            continue;
        }
        assert_eq!(
            rendered_rows(&fixture, mixed).await,
            rendered_rows(&fixture, single).await,
            "{mixed}"
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A cast the caller names is not one of the wrapped functions, so a
/// dictionary of integers asked for by name keeps that type.
#[tokio::test]
async fn an_explicit_cast_to_a_numeric_dictionary_keeps_its_type() {
    let production = production_session();
    let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64));
    let (unoptimized, optimized) = planned_types(
        &production,
        "SELECT arrow_cast(1, 'Dictionary(Int32, Int64)') AS v",
    )
    .await
    .expect("arrow_cast plans");
    assert_eq!((unoptimized, optimized), (dictionary.clone(), dictionary));
    let batches = production
        .sql("SELECT arrow_typeof(arrow_cast(1, 'Dictionary(Int32, Int64)')) AS t")
        .await
        .expect("arrow_typeof plans")
        .collect()
        .await
        .expect("arrow_typeof runs");
    assert_eq!(
        array_value_to_string(batches[0].column(0), 0).expect("render"),
        "Dictionary(Int32, Int64)"
    );
}

/// The wrapper's own evaluation casts the inner function's dictionary result
/// to the value type: a constant-folded call, and a physical expression
/// built from the wrapped function without simplification, both answer
/// `Int32`.
#[tokio::test]
async fn a_folded_or_unsimplified_call_answers_the_value_type() {
    let production = production_session();
    let batches = production
        .sql("SELECT length(arrow_cast('abc', 'Dictionary(Int32, Utf8)')) AS v")
        .await
        .expect("plans")
        .collect()
        .await
        .expect("runs");
    let folded = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("a folded length is Int32");
    assert_eq!(folded.values().to_vec(), vec![3]);

    let length = production
        .state()
        .scalar_functions()
        .get("length")
        .cloned()
        .expect("length is registered");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "c",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]));
    let values: DictionaryArray<Int32Type> = vec!["abc", "a", "abc"].into_iter().collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).expect("batch");
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).expect("schema");
    let abc = ScalarValue::Dictionary(
        Box::new(DataType::Int32),
        Box::new(ScalarValue::from("abc")),
    );
    for (argument, expected) in [(col("c"), vec![3, 1, 3]), (lit(abc), vec![3, 3, 3])] {
        let call =
            Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(&length), vec![argument]));
        let physical = create_physical_expr(
            &call,
            &df_schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .expect("physical");
        let result = physical
            .evaluate(&batch)
            .expect("evaluates")
            .into_array(batch.num_rows())
            .expect("array");
        let lengths = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("{call} is Int32, got {}", result.data_type()));
        assert_eq!(lengths.values().to_vec(), expected, "{call}");
    }
}
