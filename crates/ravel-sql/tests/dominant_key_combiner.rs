//! Issue #4676 (ADR-0102, amendment of 2026-11-24): the
//! dominant-key combiner between a `Partial` aggregate and its hash exchange.
//!
//! The "off" side of every answer comparison is
//! `SqlConfig::dominant_key_combiner = false`, which installs no rule.
//!
//! Two fixtures. The logs fixture drives a real `SqlExecutor` over one RLOG
//! object with declared columns `s` (`Str`), `a` and `v` (`I64`), for plans,
//! answers and the `EXPLAIN ANALYZE` metrics. The direct fixture runs a
//! `DominantKeyCombinerExec` over a scripted input of state rows, for the
//! counter bound, the memory charge, errors and hash collisions, where the
//! row sequence the exec sees must be exact.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::logical_expr::GroupsAccumulator;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::aggregates::group_values::new_group_values;
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, displayable,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use proptest::prelude::*;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, DominantKeyCombiner, DominantKeyCombinerExec, KeyHasher,
    LogsTableProvider, SKIP_PARTIAL_AGGREGATION_PROBE_ROWS, SessionTable, SpanSegmentFetcher,
    SpillDecision, SqlConfig, SqlExecutor, SqlRequest, StaticDeclaredColumns, build_session,
    counters_for,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// The logs fixture
// ---------------------------------------------------------------------------

/// One logs row: the `s` and `a` keys (absent reads NULL) and the value `v`.
#[derive(Clone, Debug)]
struct Row {
    s: Option<String>,
    a: Option<i64>,
    v: i64,
}

fn tenant() -> TenantId {
    TenantId::new("dominant-key-combiner-4676".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("s", DeclaredType::Str),
        DeclaredColumn::new("a", DeclaredType::I64),
        DeclaredColumn::new("v", DeclaredType::I64),
    ]
}

fn log_record(i: usize, row: &Row) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let mut attrs = vec![("v".to_string(), AttrValue::I64(row.v))];
    if let Some(s) = &row.s {
        attrs.push(("s".to_string(), AttrValue::Str(s.clone())));
    }
    if let Some(a) = row.a {
        attrs.push(("a".to_string(), AttrValue::I64(a)));
    }
    let ts = i as i64 + 1;
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
        attrs,
    }
}

/// A store holding `rows` as one published RLOG object, in order.
async fn store_of(rows: &[Row]) -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = tenant();
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [6u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for (i, row) in rows.iter().enumerate() {
        writer.push(log_record(i, row)).expect("push record");
    }
    let bytes = writer.finish().expect("finish rlog");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(9_4676),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [9u8; 32],
        sample_count: rows.len() as u64,
        series_count: 1,
        min_event_ts_ns: 1,
        max_event_ts_ns: rows.len() as i64,
        min_ingest_ts_ns: 1,
        max_ingest_ts_ns: rows.len() as i64,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store.as_ref(), &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
    store
}

/// The default config at `partitions` SQL partitions, with the combiner as
/// given and no exchange floor, so the exchange has exactly `partitions`
/// outputs.
fn sql_config(partitions: usize, combiner: bool) -> SqlConfig {
    let mut config = SqlConfig {
        dominant_key_combiner: combiner,
        ..SqlConfig::default()
    };
    config.engine.sql_partition_count = Some(partitions);
    config.first_attempt_min_partitions = 0;
    config
}

fn executor_over(store: &Arc<dyn ObjectStoreBackend>, config: SqlConfig) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        config,
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())))
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: i64::MAX,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(120),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

async fn pinned(executor: &SqlExecutor, sql: &str) -> ravel_sql::PinnedQuery {
    let accounting = QueryAccounting::new();
    let declared = executor
        .resolve_declared_columns(tenant().hash(), request(sql).now_ns)
        .await;
    let (snapshot, _) = executor
        .resolve_snapshot(tenant().hash(), &request(sql), &accounting)
        .await
        .expect("snapshot resolves");
    executor
        .plan_pinned(tenant().hash(), snapshot, sql, &accounting, &declared)
        .await
        .expect("query plans")
}

/// The physical plan `sql` produces, rendered as `EXPLAIN` renders it.
async fn physical_plan(executor: &SqlExecutor, sql: &str) -> String {
    let plan = pinned(executor, sql)
        .await
        .create_physical_plan()
        .await
        .expect("physical plan builds");
    format!("{}", displayable(plan.as_ref()).indent(false))
}

/// Every row `sql` returns, each rendered column by column, sorted.
async fn sorted_rows(executor: &SqlExecutor, sql: &str) -> Vec<String> {
    let outcome = executor
        .execute(tenant().hash(), &request(sql))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for batch in outcome.output.batches() {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| array_value_to_string(column, row).expect("renders"))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

/// The plan lines, trimmed of their indentation.
fn plan_lines(plan: &str) -> Vec<&str> {
    plan.lines().map(str::trim).collect()
}

/// The line after the hash exchange's, and the one after that.
fn below_the_exchange(plan: &str) -> (&str, &str) {
    let lines = plan_lines(plan);
    let at = lines
        .iter()
        .position(|line| line.starts_with("RepartitionExec: partitioning=Hash"))
        .unwrap_or_else(|| panic!("no hash exchange:\n{plan}"));
    (
        lines.get(at + 1).copied().unwrap_or(""),
        lines.get(at + 2).copied().unwrap_or(""),
    )
}

/// The statements the answer comparison runs, each with whether the rule
/// fires on it when the plan has a hash exchange. These are the admitted
/// aggregates: none (`COUNT(DISTINCT ..)`, the line 6 shape), `count(*)`,
/// `count(v)`, `sum(v)`, `min(v)`, `max(v)` and `avg(v)` over an `I64`
/// column, under a `Utf8View` key, an `Int64` key and both.
const STATEMENTS: &[(&str, &str, bool)] = &[
    (
        "zero aggregates over a string key (line 6)",
        "SELECT count(DISTINCT s) FROM logs",
        true,
    ),
    (
        "zero aggregates over an Int64 key",
        "SELECT count(DISTINCT a) FROM logs",
        true,
    ),
    ("count(*)", "SELECT s, count(*) FROM logs GROUP BY s", true),
    ("count(v)", "SELECT s, count(v) FROM logs GROUP BY s", true),
    ("sum(v)", "SELECT s, sum(v) FROM logs GROUP BY s", true),
    ("min(v)", "SELECT s, min(v) FROM logs GROUP BY s", true),
    ("max(v)", "SELECT s, max(v) FROM logs GROUP BY s", true),
    ("avg(v)", "SELECT s, avg(v) FROM logs GROUP BY s", true),
    (
        "every admitted aggregate at once",
        "SELECT s, count(*), count(v), sum(v), min(v), max(v), avg(v) FROM logs GROUP BY s",
        true,
    ),
    (
        "a two-column key",
        "SELECT s, a, count(*), sum(v) FROM logs GROUP BY s, a",
        true,
    ),
    (
        "an Int64 key",
        "SELECT a, count(*), sum(v) FROM logs GROUP BY a",
        true,
    ),
];

/// The row sets the answer comparison draws from.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// One key on about 85% of rows, the rest distinct.
    OneDominant,
    /// Two keys on about 40% of rows each, the rest distinct.
    TwoDominant,
    /// The NULL key dominant.
    NullDominant,
    /// The empty-string key dominant.
    EmptyDominant,
    /// [`SKIP_PARTIAL_AGGREGATION_PROBE_ROWS`] distinct rows first, so the
    /// partial skips, and only then one dominant key.
    LateDominant,
    /// Every key distinct.
    Uniform,
}

const SHAPES: [Shape; 6] = [
    Shape::OneDominant,
    Shape::TwoDominant,
    Shape::NullDominant,
    Shape::EmptyDominant,
    Shape::LateDominant,
    Shape::Uniform,
];

/// A small deterministic generator, so a shape and a seed name one row set.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn rows_of(shape: Shape, n: usize, seed: u64) -> Vec<Row> {
    let mut rng = SplitMix(seed);
    let distinct = |i: usize| Some(format!("u{i:08}"));
    (0..n)
        .map(|i| {
            let roll = rng.below(100);
            let s = match shape {
                Shape::OneDominant if roll < 85 => Some("hot".to_string()),
                Shape::TwoDominant if roll < 40 => Some("hot".to_string()),
                Shape::TwoDominant if roll < 80 => Some("warm".to_string()),
                Shape::NullDominant if roll < 85 => None,
                Shape::EmptyDominant if roll < 85 => Some(String::new()),
                Shape::LateDominant if i >= SKIP_PARTIAL_AGGREGATION_PROBE_ROWS && roll < 90 => {
                    Some("late".to_string())
                }
                _ => distinct(i),
            };
            // A NULL `a` on one row in eleven, a dominant 7 on half the rows.
            let a = match rng.below(22) {
                0 | 1 => None,
                2..=12 => Some(7),
                _ => Some(rng.below(1_000) as i64),
            };
            Row {
                s,
                a,
                v: rng.below(2_000) as i64 - 1_000,
            }
        })
        .collect()
}

/// Every statement answers the same with the rule on and off, at 4 and 32
/// SQL partitions, and the rule fires where the plan has an exchange. At 1
/// partition the plan has no exchange and the rule has nothing to do; the
/// exec is driven directly at `P = 1` instead, and at 4 and 32 as well.
///
/// With `merges`, the rows are at least five blocks, so the round-robin
/// exchange deals partial partition 0 a second batch after its probe skips,
/// and at 4 partitions the exec merges rows (`merged_rows` is not 0) for
/// `GROUP BY s`.
async fn check_answers(rows: &[Row], what: &str, merges: bool) {
    let store = store_of(rows).await;
    for partitions in [1usize, 4, 32] {
        let on = executor_over(&store, sql_config(partitions, true));
        let off = executor_over(&store, sql_config(partitions, false));
        for (name, sql, admitted) in STATEMENTS {
            let plan = physical_plan(&on, sql).await;
            let fired = plan.contains("DominantKeyCombinerExec");
            if partitions == 1 {
                assert!(!fired, "{what}, {name} at 1 partition:\n{plan}");
            } else {
                assert_eq!(
                    fired, *admitted,
                    "{what}, {name} at {partitions} partitions:\n{plan}"
                );
            }
            let on_rows = sorted_rows(&on, sql).await;
            let off_rows = sorted_rows(&off, sql).await;
            assert!(!off_rows.is_empty(), "{what}, {name}: no rows");
            assert_eq!(
                on_rows, off_rows,
                "{what}, {name} at {partitions} partitions answers differently"
            );
        }
        if merges && partitions == 4 {
            let run = pinned(&on, "SELECT s, count(*) FROM logs GROUP BY s")
                .await
                .execute_analyzed()
                .await
                .expect("query runs");
            let line = run
                .plan_with_metrics
                .lines()
                .find(|line| line.contains("DominantKeyCombinerExec"))
                .unwrap_or_else(|| panic!("{what}: no combiner:\n{}", run.plan_with_metrics));
            assert!(
                !line.contains("merged_rows=0,") && !line.contains("merged_rows=0]"),
                "{what}: the exec merged nothing: {line}"
            );
        }
    }
    for partitions in [1usize, 4, 32] {
        check_direct(rows, what, partitions).await;
    }
}

/// The exec at `counters_for(partitions)` counters, over the `s` keys of
/// `rows` as one-row `count` states in batches of 1,000: the merged count
/// per key is the input's.
async fn check_direct(rows: &[Row], what: &str, partitions: usize) {
    let keys: Vec<Option<String>> = rows.iter().map(|row| row.s.clone()).collect();
    let batches: Vec<RecordBatch> = keys
        .chunks(1_000)
        .map(|chunk| string_count_batch(chunk))
        .collect();
    let exec = string_count_exec(
        vec![batches.into_iter().map(Ok).collect()],
        counters_for(partitions),
    );
    let out = run_partition(&exec, 0, task_ctx(Arc::new(GreedyMemoryPool::new(1 << 30))))
        .await
        .expect("runs");
    let mut want: BTreeMap<Option<String>, i64> = BTreeMap::new();
    for key in &keys {
        *want.entry(key.clone()).or_default() += 1;
    }
    assert_eq!(
        string_counts(&out),
        want,
        "{what}: direct at P = {partitions}"
    );
}

#[tokio::test]
async fn every_shape_answers_as_the_rule_off_at_1_4_and_32_partitions() {
    for shape in SHAPES {
        let rows = rows_of(shape, 5 * SKIP_PARTIAL_AGGREGATION_PROBE_ROWS + 100, 4676);
        let merges = !matches!(shape, Shape::Uniform);
        check_answers(&rows, &format!("{shape:?}"), merges).await;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(6))]

    /// The same comparison over random row counts, seeds and shapes.
    #[test]
    fn random_rows_answer_as_the_rule_off(
        shape in prop::sample::select(SHAPES.to_vec()),
        n in 1usize..(2 * SKIP_PARTIAL_AGGREGATION_PROBE_ROWS),
        seed in any::<u64>(),
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let rows = rows_of(shape, n, seed);
            check_answers(&rows, &format!("{shape:?}, n = {n}, seed = {seed}"), false).await;
        });
    }
}

// ---------------------------------------------------------------------------
// Plans
// ---------------------------------------------------------------------------

async fn plan_fixture() -> Arc<dyn ObjectStoreBackend> {
    store_of(&rows_of(Shape::OneDominant, 2_000, 1)).await
}

/// The exec sits directly between the partial aggregate and the hash
/// exchange, with `2P - 1` counters, for the line 6 shape and a plain
/// `GROUP BY s`.
#[tokio::test]
async fn the_exec_sits_between_the_partial_and_the_hash_exchange() {
    let store = plan_fixture().await;
    let on = executor_over(&store, sql_config(4, true));
    for (sql, exec) in [
        (
            "SELECT count(DISTINCT s) FROM logs",
            "DominantKeyCombinerExec: gby=[alias1], aggr=[], counters=7",
        ),
        (
            "SELECT s, count(*) FROM logs GROUP BY s",
            "DominantKeyCombinerExec: gby=[s], aggr=[count(Int64(1))], counters=7",
        ),
    ] {
        let plan = physical_plan(&on, sql).await;
        let (first, second) = below_the_exchange(&plan);
        assert_eq!(first, exec, "{sql}:\n{plan}");
        assert!(
            second.starts_with("AggregateExec: mode=Partial"),
            "{sql}:\n{plan}"
        );
    }
}

/// No exec with the field off, with no exchange, or in a repartition-free
/// spill session.
#[tokio::test]
async fn no_exec_without_the_field_an_exchange_or_with_spill() {
    let store = plan_fixture().await;
    let sql = "SELECT s, count(*) FROM logs GROUP BY s";
    let off = executor_over(&store, sql_config(4, false));
    let plan = physical_plan(&off, sql).await;
    assert!(!plan.contains("DominantKeyCombinerExec"), "{plan}");
    assert!(
        plan.contains("RepartitionExec: partitioning=Hash"),
        "the off side still has its exchange:\n{plan}"
    );

    let one = executor_over(&store, sql_config(1, true));
    let plan = physical_plan(&one, sql).await;
    assert!(!plan.contains("DominantKeyCombinerExec"), "{plan}");

    let config = sql_config(4, true);
    let executor = executor_over(&store, config.clone());
    let accounting = QueryAccounting::new();
    let (snapshot, _) = executor
        .resolve_snapshot(tenant().hash(), &request(sql), &accounting)
        .await
        .expect("snapshot resolves");
    let provider = Arc::new(
        LogsTableProvider::new(
            snapshot,
            tenant().hash(),
            LogSegmentFetcher::new(Arc::clone(&store)),
            PhaseAccounting::new(),
        )
        .with_declared_columns(declared()),
    );
    let scratch = std::env::temp_dir().join(format!("dominant-key-4676-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).expect("scratch dir");
    let (pool, _breach) = config.query_pool(
        ravel_sql::TenantMemoryAccountant::new(1 << 30),
        accounting.clone(),
    );
    let ctx = build_session(
        &config,
        pool,
        SessionTable::Logs(provider),
        true,
        SpillDecision::Enabled {
            dir: &scratch,
            max_bytes: 1 << 30,
        },
        &ravel_sql::SessionPrincipal::without_capabilities(tenant().hash()),
    )
    .expect("session");
    let plan = ctx
        .sql(sql)
        .await
        .expect("plans")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan = format!("{}", displayable(plan.as_ref()).indent(false));
    assert!(
        !plan.contains("RepartitionExec: partitioning=Hash"),
        "{plan}"
    );
    assert!(!plan.contains("DominantKeyCombinerExec"), "{plan}");
    let _ = std::fs::remove_dir_all(&scratch);
}

/// A bounded top-k partial carries a limit, which the rule refuses.
#[tokio::test]
async fn a_bounded_top_k_partial_is_refused() {
    let store = plan_fixture().await;
    let on = executor_over(&store, sql_config(4, true));
    let plan = physical_plan(
        &on,
        "SELECT a, max(ts) AS m FROM logs GROUP BY a ORDER BY m DESC LIMIT 3",
    )
    .await;
    let partial = plan_lines(&plan)
        .into_iter()
        .find(|line| line.starts_with("AggregateExec: mode=Partial"))
        .unwrap_or_else(|| panic!("no partial:\n{plan}"))
        .to_string();
    assert!(partial.contains("lim=[3]"), "{plan}");
    assert!(!plan.contains("DominantKeyCombinerExec"), "{plan}");
}

/// A plain DataFusion session over `partitions` partitions of `batch`, with
/// the rule installed after the default rules.
fn plain_session(batch: RecordBatch, partitions: usize) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(partitions);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DominantKeyCombiner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let rows = batch.num_rows();
    let per = rows.div_ceil(partitions);
    let parts: Vec<Vec<RecordBatch>> = (0..partitions)
        .map(|p| {
            let start = (p * per).min(rows);
            vec![batch.slice(start, per.min(rows - start))]
        })
        .collect();
    let table = MemTable::try_new(batch.schema(), parts).expect("table");
    ctx.register_table("t", Arc::new(table)).expect("register");
    ctx
}

/// A float sum, a `List` state and a float key are refused even where the
/// plan has a hash exchange; a count over an integer key is admitted.
#[tokio::test]
async fn a_float_sum_and_a_list_state_are_refused() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let n = 1_000i64;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values((0..n).map(|i| i % 10))),
            Arc::new(datafusion::arrow::array::Float64Array::from_iter_values(
                (0..n).map(|i| i as f64 / 3.0),
            )),
            Arc::new(Int64Array::from_iter_values(0..n)),
        ],
    )
    .expect("batch");
    let ctx = plain_session(batch, 4);
    for (sql, admitted) in [
        ("SELECT k, count(*) FROM t GROUP BY k", true),
        ("SELECT k, sum(f) FROM t GROUP BY k", false),
        ("SELECT k, array_agg(v) FROM t GROUP BY k", false),
        ("SELECT f, count(*) FROM t GROUP BY f", false),
    ] {
        let plan = ctx
            .sql(sql)
            .await
            .expect("plans")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let plan = format!("{}", displayable(plan.as_ref()).indent(false));
        assert!(
            plan.contains("RepartitionExec: partitioning=Hash"),
            "{sql}:\n{plan}"
        );
        assert_eq!(
            plan.contains("DominantKeyCombinerExec"),
            admitted,
            "{sql}:\n{plan}"
        );
    }
}

/// `EXPLAIN ANALYZE` carries the five counters on the exec's line, at the
/// values the fixture implies, each below 1,000 so the line prints it
/// exactly.
///
/// The fixture is one object of five blocks, which the one scan partition
/// reads as five batches and a round-robin exchange deals to 4 partial
/// partitions in turn: block 0, 8,192 rows cycling over 820 keys, and block
/// 4, 900 rows of `hot`, to partition 0; blocks 1 to 3, 8,192 rows of one
/// key each, to partitions 1 to 3.
///
/// Partition 0's probe sees 820 groups in 8,192 rows, a ratio above 0.1,
/// and skips: it emits its 820 groups, then one state row per `hot` row.
/// Partitions 1 to 3 aggregate their one key and emit one row each. At 4
/// partitions a table holds 7 counters. In partition 0 each run of 8
/// distinct state rows admits 7 and triggers one pass, 102 passes over the
/// first 816, and the last 4 are admitted. The first `hot` row takes a free
/// counter and is forwarded, the second creates its state, and every later
/// one merges. In all: 899 merged, 820 + 1 + 3 = 824 forwarded, one state
/// emitted at end of input, a peak of one group, 825 rows out.
#[tokio::test]
async fn explain_analyze_reports_the_five_metrics() {
    let block = SKIP_PARTIAL_AGGREGATION_PROBE_ROWS;
    let rows: Vec<Row> = (0..4 * block + 900)
        .map(|i| Row {
            s: Some(match i / block {
                0 => format!("u{:08}", i % 820),
                1..=3 => format!("u{:08}", i / block),
                _ => "hot".to_string(),
            }),
            a: Some(1),
            v: 1,
        })
        .collect();
    let store = store_of(&rows).await;
    let on = executor_over(&store, sql_config(4, true));
    let run = pinned(&on, "SELECT count(DISTINCT s) FROM logs")
        .await
        .execute_analyzed()
        .await
        .expect("query runs");
    let line = run
        .plan_with_metrics
        .lines()
        .find(|line| line.contains("DominantKeyCombinerExec"))
        .unwrap_or_else(|| panic!("no combiner:\n{}", run.plan_with_metrics));
    for metric in [
        "merged_rows=899",
        "forwarded_rows=824",
        "emitted_states=1",
        "held_groups_peak=1",
        "decrement_passes=102",
        "output_rows=825",
    ] {
        assert!(
            line.contains(&format!("{metric},")) || line.contains(&format!("{metric}]")),
            "{metric} missing: {line}\n{}",
            run.plan_with_metrics
        );
    }
}

// ---------------------------------------------------------------------------
// The direct fixture
// ---------------------------------------------------------------------------

/// A leaf that serves scripted batches, or an error, per partition.
struct Scripted {
    schema: SchemaRef,
    parts: Vec<Vec<Result<RecordBatch, String>>>,
    properties: Arc<PlanProperties>,
}

impl Scripted {
    fn new(schema: SchemaRef, parts: Vec<Vec<Result<RecordBatch, String>>>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(parts.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Scripted {
            schema,
            parts,
            properties,
        }
    }
}

impl fmt::Debug for Scripted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scripted")
    }
}

impl DisplayAs for Scripted {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scripted")
    }
}

impl ExecutionPlan for Scripted {
    fn name(&self) -> &str {
        "Scripted"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let items: Vec<DFResult<RecordBatch>> = self
            .parts
            .get(partition)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|item| item.map_err(DataFusionError::Execution))
            .collect();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(items),
        )))
    }
}

/// `(k, count)`: an `Int64` key and a `count` state column.
fn int_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("count(v)[count]", DataType::Int64, false),
    ]))
}

/// `(s, count)`: a `Utf8View` key and a `count` state column.
fn string_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8View, true),
        Field::new("count(v)[count]", DataType::Int64, false),
    ]))
}

/// A `count(v)` expression over an `(k, v)` input; its one state is `Int64`.
fn count_expr() -> Arc<AggregateFunctionExpr> {
    let input = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    Arc::new(
        AggregateExprBuilder::new(count_udaf(), vec![col("v", &input).expect("v")])
            .schema(Arc::clone(&input))
            .alias("count(v)")
            .build()
            .expect("count"),
    )
}

/// One-row `count` states, one per key.
fn int_count_batch(keys: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        int_count_schema(),
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(Int64Array::from(vec![1i64; keys.len()])),
        ],
    )
    .expect("batch")
}

fn string_count_batch(keys: &[Option<String>]) -> RecordBatch {
    RecordBatch::try_new(
        string_count_schema(),
        vec![
            Arc::new(StringViewArray::from_iter(keys.iter().cloned())),
            Arc::new(Int64Array::from(vec![1i64; keys.len()])),
        ],
    )
    .expect("batch")
}

/// Keys alone, for the zero-aggregate exec.
fn key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]))
}

fn key_batch(keys: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        key_schema(),
        vec![Arc::new(Int64Array::from_iter_values(keys.iter().copied()))],
    )
    .expect("batch")
}

fn int_count_exec(
    parts: Vec<Vec<Result<RecordBatch, String>>>,
    counters: usize,
) -> DominantKeyCombinerExec {
    DominantKeyCombinerExec::try_new(
        Arc::new(Scripted::new(int_count_schema(), parts)),
        1,
        vec![count_expr()],
        counters,
    )
    .expect("exec")
}

fn string_count_exec(
    parts: Vec<Vec<Result<RecordBatch, String>>>,
    counters: usize,
) -> DominantKeyCombinerExec {
    DominantKeyCombinerExec::try_new(
        Arc::new(Scripted::new(string_count_schema(), parts)),
        1,
        vec![count_expr()],
        counters,
    )
    .expect("exec")
}

fn key_exec(keys: &[i64], counters: usize) -> DominantKeyCombinerExec {
    let batches = keys.chunks(100).map(|chunk| Ok(key_batch(chunk))).collect();
    DominantKeyCombinerExec::try_new(
        Arc::new(Scripted::new(key_schema(), vec![batches])),
        1,
        Vec::new(),
        counters,
    )
    .expect("exec")
}

fn task_ctx(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .expect("runtime");
    Arc::new(TaskContext::default().with_runtime(runtime))
}

async fn run_partition(
    exec: &DominantKeyCombinerExec,
    partition: usize,
    ctx: Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    let mut stream = exec.execute(partition, ctx)?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

/// The exec's metric `name`, summed over its partitions.
fn metric(exec: &DominantKeyCombinerExec, name: &str) -> usize {
    exec.metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map_or(0, |value| value.as_usize())
}

/// The summed `count` state per `Int64` key of `batches`.
fn int_counts(batches: &[RecordBatch]) -> BTreeMap<Option<i64>, i64> {
    let mut out = BTreeMap::new();
    for batch in batches {
        let keys = batch.column(0).as_primitive::<Int64Type>();
        let counts = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let key = keys.is_valid(row).then(|| keys.value(row));
            *out.entry(key).or_default() += counts.value(row);
        }
    }
    out
}

fn string_counts(batches: &[RecordBatch]) -> BTreeMap<Option<String>, i64> {
    let mut out = BTreeMap::new();
    for batch in batches {
        let keys = batch.column(0).as_string_view();
        let counts = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let key = keys.is_valid(row).then(|| keys.value(row).to_string());
            *out.entry(key).or_default() += counts.value(row);
        }
    }
    out
}

fn key_values(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// T4: the bound
// ---------------------------------------------------------------------------

/// `n = 128P` rows of `block` new keys then the key 0, repeated; new keys
/// start at 1 and are never repeated.
fn blocks_then_x(partitions: usize, block: usize) -> Vec<i64> {
    let n = 128 * partitions;
    let mut keys = Vec::with_capacity(n);
    let mut next = 1i64;
    while keys.len() < n {
        for _ in 0..block {
            keys.push(next);
            next += 1;
        }
        keys.push(0);
    }
    keys.truncate(n);
    keys
}

/// The rows of key 0 the exec forwarded: every forwarded row is either key 0
/// or a new key, and every new key occurs once and is forwarded.
async fn forwarded_x(keys: &[i64], counters: usize) -> usize {
    let exec = key_exec(keys, counters);
    run_partition(&exec, 0, task_ctx(Arc::new(GreedyMemoryPool::new(1 << 30))))
        .await
        .expect("runs");
    let new_keys = keys.iter().filter(|&&key| key != 0).count();
    metric(&exec, "forwarded_rows") - new_keys
}

/// The bound's worst case: `2P - 1` new keys then X, repeated, forwards X
/// on every one of the `n / (2P)` passes.
#[tokio::test]
async fn the_worst_case_forwards_x_once_per_pass() {
    for partitions in [1usize, 4, 32] {
        let keys = blocks_then_x(partitions, 2 * partitions - 1);
        let n = keys.len();
        assert_eq!(
            forwarded_x(&keys, counters_for(partitions)).await,
            n / (2 * partitions),
            "at P = {partitions}"
        );
    }
}

/// `P - 1` new keys then X, repeated: `2P - 1` counters keep X after its
/// first admission, so it is forwarded exactly once. A table of `P - 1`
/// counters forwards it `n / P` times, and one of `P` forwards it `P` times.
#[tokio::test]
async fn a_key_on_one_row_in_p_is_forwarded_once() {
    for partitions in [4usize, 32] {
        let keys = blocks_then_x(partitions, partitions - 1);
        let n = keys.len();
        assert_eq!(
            forwarded_x(&keys, counters_for(partitions)).await,
            1,
            "2P - 1 counters at P = {partitions}"
        );
        assert_eq!(
            forwarded_x(&keys, partitions - 1).await,
            n / partitions,
            "P - 1 counters at P = {partitions}"
        );
        assert_eq!(
            forwarded_x(&keys, partitions).await,
            partitions,
            "P counters at P = {partitions}"
        );
    }
}

/// `4P` keys, each twice in a row, cycled: every key's second row creates a
/// group, so the table fills with groups, and the peak is the table size.
#[tokio::test]
async fn held_groups_peak_at_the_table_size() {
    for partitions in [1usize, 4, 32] {
        let n = 128 * partitions;
        let keys: Vec<i64> = (0..n)
            .map(|i| ((i / 2) % (4 * partitions)) as i64)
            .collect();
        for (counters, peak) in [
            (counters_for(partitions), 2 * partitions - 1),
            (partitions, partitions),
        ] {
            let exec = key_exec(&keys, counters);
            let out = run_partition(&exec, 0, task_ctx(Arc::new(GreedyMemoryPool::new(1 << 30))))
                .await
                .expect("runs");
            assert_eq!(
                metric(&exec, "held_groups_peak"),
                peak,
                "{counters} counters at P = {partitions}"
            );
            let mut got = key_values(&out);
            got.sort_unstable();
            got.dedup();
            assert_eq!(got.len(), 4 * partitions, "every key is in the output");
        }
    }
}

// ---------------------------------------------------------------------------
// T5: uniform keys
// ---------------------------------------------------------------------------

/// All-distinct keys merge nothing, hold nothing and pass every row through
/// unchanged and in order.
#[tokio::test]
async fn distinct_keys_pass_through_unchanged() {
    for partitions in [1usize, 4, 32] {
        let n = 128 * partitions;
        let keys: Vec<i64> = (0..n as i64).collect();
        let exec = key_exec(&keys, counters_for(partitions));
        let out = run_partition(&exec, 0, task_ctx(Arc::new(GreedyMemoryPool::new(1 << 30))))
            .await
            .expect("runs");
        assert_eq!(metric(&exec, "merged_rows"), 0, "at P = {partitions}");
        assert_eq!(metric(&exec, "emitted_states"), 0, "at P = {partitions}");
        assert_eq!(metric(&exec, "held_groups_peak"), 0, "at P = {partitions}");
        assert_eq!(metric(&exec, "forwarded_rows"), n, "at P = {partitions}");
        assert_eq!(key_values(&out), keys, "at P = {partitions}");
    }
}

// ---------------------------------------------------------------------------
// T6: memory
// ---------------------------------------------------------------------------

/// The charge of a table of `counters` counters holding `key_bytes` bytes of
/// encoded keys: 40 bytes per counter, and an index of
/// `(2 * counters).next_power_of_two()` four-byte entries.
fn counter_table_bytes(counters: usize, key_bytes: usize) -> usize {
    counters * 40 + (2 * counters).next_power_of_two() * 4 + key_bytes
}

/// The encoded bytes of the `Int64` key `key`.
fn encoded_int_key(key: i64) -> usize {
    let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("converter");
    let column: ArrayRef = Arc::new(Int64Array::from(vec![key]));
    converter
        .convert_columns(&[column])
        .expect("encodes")
        .row(0)
        .as_ref()
        .len()
}

/// One call the exec makes on its group table and `count` accumulator.
enum Op {
    /// Intern one key, as one row.
    Intern(i64),
    /// Merge `(group, count)` pairs, over every group interned so far.
    Merge(&'static [(usize, i64)]),
}

/// `GroupValues::size()` plus the `count` accumulator's `size()` after
/// replaying `ops` on a fresh table and accumulator, as the exec builds them.
fn table_bytes(ops: &[Op]) -> usize {
    let mut group_values = new_group_values(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
        &GroupOrdering::None,
    )
    .expect("group values");
    let mut accumulator: Box<dyn GroupsAccumulator> =
        count_expr().create_groups_accumulator().expect("acc");
    let mut groups = Vec::new();
    for op in ops {
        match op {
            Op::Intern(key) => {
                let column: ArrayRef = Arc::new(Int64Array::from(vec![Some(*key)]));
                group_values
                    .intern(&[column], &mut groups)
                    .expect("interns");
            }
            Op::Merge(merge) => {
                let values: ArrayRef = Arc::new(Int64Array::from_iter_values(
                    merge.iter().map(|(_, count)| *count),
                ));
                let indices: Vec<usize> = merge.iter().map(|(group, _)| *group).collect();
                accumulator
                    .merge_batch(&[values], &indices, group_values.len())
                    .expect("merges");
            }
        }
    }
    group_values.size() + accumulator.size()
}

const X: i64 = 1_000;
const W: i64 = 2_000;

/// The reservation at each checkpoint equals the formula: `GroupValues`
/// size, every accumulator's size, the counter table and the stored key
/// bytes, each computed here from a fresh table replaying the exec's calls.
/// `counters = 7`, `P = 4`. A checkpoint is the first output of an input
/// batch, which the exec yields only once the batch is processed.
///
/// 1. `[X, X, X, 11]`: X takes a counter, the second X creates X's group,
///    and the second and third X merge in one call. Held: X.
/// 2. `[X, 12]`: X merges. Counters: X at 4, 11 and 12.
/// 3. `[W, W, 21, 22, 23, 31, 32, 33, 34, 35]`: W takes a counter and its
///    second row creates W's group. 21 to 23 fill the seven counters, 31
///    runs a pass that frees 11, 12 and 21 to 23 (X at 3, W at 1), and 32
///    to 35 take four of the five free counters. Nothing is evicted.
/// 4. `[41, 42]`: 41 takes the last counter and 42 runs a pass that frees W,
///    32 to 35 and 41. W's state (count 1) is emitted and the table is
///    rebuilt from X alone: one intern, one merge of its state, 3.
/// 5. End of input: X's state is emitted after the release. A stream
///    dropped after checkpoint 1 also releases everything.
#[tokio::test]
async fn the_reservation_is_the_formula_at_every_checkpoint() {
    let counters = counters_for(4);
    let batches = vec![
        int_count_batch(&[Some(X), Some(X), Some(X), Some(11)]),
        int_count_batch(&[Some(X), Some(12)]),
        int_count_batch(&[
            Some(W),
            Some(W),
            Some(21),
            Some(22),
            Some(23),
            Some(31),
            Some(32),
            Some(33),
            Some(34),
            Some(35),
        ]),
        int_count_batch(&[Some(41), Some(42)]),
    ];
    let x_key = encoded_int_key(X);
    let w_key = encoded_int_key(W);
    let first = [Op::Intern(X), Op::Merge(&[(0, 1), (0, 1)])];
    let second = [
        Op::Intern(X),
        Op::Merge(&[(0, 1), (0, 1)]),
        Op::Merge(&[(0, 1)]),
    ];
    let third = [
        Op::Intern(X),
        Op::Merge(&[(0, 1), (0, 1)]),
        Op::Merge(&[(0, 1)]),
        Op::Intern(W),
        Op::Merge(&[(1, 1)]),
    ];
    let fourth = [Op::Intern(X), Op::Merge(&[(0, 3)])];
    let expected = [
        table_bytes(&first) + counter_table_bytes(counters, x_key),
        table_bytes(&second) + counter_table_bytes(counters, x_key),
        table_bytes(&third) + counter_table_bytes(counters, x_key + w_key),
        table_bytes(&fourth) + counter_table_bytes(counters, x_key),
    ];
    assert!(
        expected
            .iter()
            .all(|bytes| *bytes > counter_table_bytes(counters, x_key + w_key)),
        "every checkpoint holds table and accumulator bytes: {expected:?}"
    );

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
    let exec = int_count_exec(vec![batches.iter().cloned().map(Ok).collect()], counters);
    let mut stream = exec
        .execute(0, task_ctx(Arc::clone(&pool)))
        .expect("stream");
    let mut seen = Vec::new();
    let mut outputs = Vec::new();
    while let Some(batch) = stream.next().await {
        outputs.push(batch.expect("batch"));
        seen.push(pool.reserved());
    }
    // One output per input batch, two for the fourth (W's state, then the
    // forwarded 41 and 42), and X's state at the end.
    assert_eq!(outputs.len(), 6, "{outputs:?}");
    assert_eq!(
        seen,
        vec![
            expected[0],
            expected[1],
            expected[2],
            expected[3],
            expected[3],
            0
        ],
        "the reservation at each checkpoint"
    );
    assert_eq!(pool.reserved(), 0, "released at end of input");
    let counts = int_counts(&outputs);
    assert_eq!(counts.get(&Some(X)), Some(&4), "X's four rows survive");
    assert_eq!(counts.get(&Some(W)), Some(&2), "W's two rows survive");
    assert_eq!(
        metric(&exec, "emitted_states"),
        2,
        "W at eviction, X at end"
    );

    let exec = int_count_exec(vec![batches.into_iter().map(Ok).collect()], counters);
    let mut stream = exec
        .execute(0, task_ctx(Arc::clone(&pool)))
        .expect("stream");
    stream.next().await.expect("one batch").expect("ok");
    assert_eq!(pool.reserved(), expected[0], "held mid-input");
    drop(stream);
    assert_eq!(pool.reserved(), 0, "released on drop");
}

// ---------------------------------------------------------------------------
// T7: errors
// ---------------------------------------------------------------------------

/// An input that fails after two batches: its error is the exec's, nothing
/// follows it, the held state is never emitted, and the charge is released.
#[tokio::test]
async fn an_input_error_ends_the_stream_with_nothing_emitted_after_it() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
    let exec = int_count_exec(
        vec![vec![
            Ok(int_count_batch(&[Some(X), Some(X), Some(X), Some(1)])),
            Ok(int_count_batch(&[Some(X), Some(2)])),
            Err("planted input failure".to_string()),
            Ok(int_count_batch(&[Some(X), Some(3)])),
        ]],
        counters_for(4),
    );
    let mut stream = exec
        .execute(0, task_ctx(Arc::clone(&pool)))
        .expect("stream");
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item);
    }
    let errors: Vec<usize> = items
        .iter()
        .enumerate()
        .filter_map(|(at, item)| item.is_err().then_some(at))
        .collect();
    assert_eq!(errors, vec![items.len() - 1], "the error is the last item");
    let error = items.last().expect("items").as_ref().expect_err("an error");
    assert!(
        error.to_string().contains("planted input failure"),
        "{error}"
    );
    let ok: Vec<RecordBatch> = items.into_iter().filter_map(Result::ok).collect();
    assert_eq!(
        int_counts(&ok).get(&Some(X)),
        Some(&1),
        "only X's forwarded first row was emitted: {ok:?}"
    );
    assert_eq!(metric(&exec, "emitted_states"), 0);
    assert_eq!(pool.reserved(), 0, "released on error");
    assert!(stream.next().await.is_none(), "fused after the error");
}

// ---------------------------------------------------------------------------
// T8: hash collisions
// ---------------------------------------------------------------------------

/// Every key hashes to one value: the counter belongs to whichever key
/// created its state, every other key's rows are forwarded, and the merged
/// counts are the input's.
#[tokio::test]
async fn colliding_keys_answer_as_the_input() {
    let collide: KeyHasher = Arc::new(|_keys, hashes| {
        hashes.fill(42);
        Ok(())
    });
    let keys: Vec<Option<i64>> = (0..4_000)
        .map(|i| match i % 5 {
            0 | 1 => Some(X),
            2 => Some(W),
            3 => None,
            _ => Some(i64::from(i % 7)),
        })
        .collect();
    let batches: Vec<Result<RecordBatch, String>> = keys
        .chunks(512)
        .map(|chunk| Ok(int_count_batch(chunk)))
        .collect();
    for partitions in [1usize, 4, 32] {
        let exec = int_count_exec(vec![batches.clone()], counters_for(partitions))
            .with_hasher(Arc::clone(&collide));
        let out = run_partition(&exec, 0, task_ctx(Arc::new(GreedyMemoryPool::new(1 << 30))))
            .await
            .expect("runs");
        let mut want: BTreeMap<Option<i64>, i64> = BTreeMap::new();
        for key in &keys {
            *want.entry(*key).or_default() += 1;
        }
        assert_eq!(int_counts(&out), want, "at P = {partitions}");
        assert!(
            metric(&exec, "merged_rows") > 0,
            "the colliding counter still merges its own key"
        );
    }
}
