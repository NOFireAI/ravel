//! Issue #3608 (ADR-3565 decision 1): bucketed final aggregation.
//!
//! The "off" side of every comparison here is
//! `SqlConfig::bucketed_final_aggregation = None`, which does not install
//! `BucketedFinalAggregateRule` at all.
//!
//! Two fixtures. The logs fixture drives a real `SqlExecutor` over a real
//! RLOG object, for the shipped answers, plans and peak bytes. The samples
//! fixture builds a session through the production pool and runs the
//! `BucketedFinalAggregateExec` node of the planned statement directly, for
//! the per-partition metrics, the input-exhaustion order, a refused grow and
//! a dropped stream.
//!
//! Most tests use `engage_groups = 64` rather than the 65,536 default, so a
//! fixture of a few thousand rows engages the bucketed phase in every
//! partition, at 32 partitions too.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, collect, displayable,
};
use futures::{Stream, StreamExt};
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
    BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX, BucketedFinalAggregateExec, BucketedFinalAggregation,
    DeclaredColumn, DeclaredType, RavelTableProvider, SessionTable, SpanSegmentFetcher,
    SpillDecision, SqlConfig, SqlExecutor, SqlRequest, StaticDeclaredColumns, TenantDelegatingPool,
    TenantMemoryAccountant, build_session,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;
use ravel_types::{Signal, TenantId, TimeRange};
use util::{Fixture, SegSpec, SeriesSpec, tenant_id};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// The logs fixture
// ---------------------------------------------------------------------------

/// Rows in the answer and plan fixture.
const ROWS: i64 = 6_000;

/// The engage threshold every test uses unless it says otherwise: low enough
/// that [`ROWS`] engages the bucketed phase at 32 partitions.
const ENGAGE_GROUPS: usize = 64;

fn tenant() -> TenantId {
    TenantId::new("bucketed-final-3608".to_string())
}

/// Row `i`: `a = i` (unique), `n = i` except every seventh row, where `n` is
/// absent and reads NULL, `b = i % 50`, `v = i % 1000 - 500` (fits `Int16`).
/// `z` is declared and never present, so it is NULL on every row.
fn log_record(i: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let mut attrs = vec![
        ("a".to_string(), AttrValue::I64(i)),
        ("b".to_string(), AttrValue::I64(i % 50)),
        ("v".to_string(), AttrValue::I64(i % 1000 - 500)),
    ];
    if i % 7 != 0 {
        attrs.push(("n".to_string(), AttrValue::I64(i)));
    }
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: i + 1,
        observed_ts_ns: i + 1,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("req-{}", i % 13),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn declared() -> Vec<DeclaredColumn> {
    ["a", "b", "v", "n", "z"]
        .into_iter()
        .map(|name| DeclaredColumn::new(name, DeclaredType::I64))
        .collect()
}

async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [5u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for r in records {
        writer.push(r.clone()).expect("push record");
    }
    let bytes = writer.finish().expect("finish rlog");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(9_3608),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [8u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
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
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

fn bucketed(engage_groups: usize) -> BucketedFinalAggregation {
    BucketedFinalAggregation {
        engage_groups,
        ..BucketedFinalAggregation::default()
    }
}

fn sql_config(
    partitions: usize,
    bucketed: Option<BucketedFinalAggregation>,
    topk: Option<usize>,
) -> SqlConfig {
    let mut config = SqlConfig {
        bucketed_final_aggregation: bucketed,
        bounded_topk_max_limit: topk,
        ..SqlConfig::default()
    };
    config.engine.sql_partition_count = Some(partitions);
    // The tests read per-partition metrics and drive `partitions` streams by
    // index, and 16 bucketed streams at about 19 MB each exhaust the 256 MiB
    // pool, so the final stage runs at `partitions`.
    config.first_attempt_min_partitions = 0;
    config
}

async fn executor(rows: i64, config: SqlConfig) -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let records: Vec<LogRecord> = (0..rows).map(log_record).collect();
    publish_logs(store.as_ref(), &tenant(), &records).await;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
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

async fn physical_plan_tree(executor: &SqlExecutor, sql: &str) -> Arc<dyn ExecutionPlan> {
    pinned(executor, sql)
        .await
        .create_physical_plan()
        .await
        .expect("physical plan builds")
}

/// The physical plan `sql` produces, rendered as `EXPLAIN` renders it.
async fn physical_plan(executor: &SqlExecutor, sql: &str) -> String {
    let plan = physical_plan_tree(executor, sql).await;
    format!("{}", displayable(plan.as_ref()).indent(false))
}

/// Every row `sql` returns, each rendered column by column, sorted: the
/// answer up to row order.
async fn sorted_rows(executor: &SqlExecutor, sql: &str) -> Vec<String> {
    let outcome = executor
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("query runs");
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

/// Every `AggregateExec` limit in `plan`, in pre-order.
fn aggregate_limits(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    let mut out = Vec::new();
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && let Some(options) = aggregate.limit_options()
    {
        out.push(options.limit);
    }
    for child in plan.children() {
        out.extend(aggregate_limits(child));
    }
    out
}

/// The first `BucketedFinalAggregateExec` in `plan`, in pre-order.
fn find_bucketed(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<BucketedFinalAggregateExec>().is_some() {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_bucketed)
}

/// The name of the node directly above the first `BucketedFinalAggregateExec`.
fn parent_of_bucketed(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
    for child in plan.children() {
        if child.downcast_ref::<BucketedFinalAggregateExec>().is_some() {
            return Some(plan.name().to_string());
        }
        if let Some(name) = parent_of_bucketed(child) {
            return Some(name);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Answers
// ---------------------------------------------------------------------------

/// The admitted shapes, each with the reason it is here. `admitted` says
/// whether the rule must fire on it at 4 and 32 partitions.
const ANSWER_SHAPES: &[(&str, &str, bool)] = &[
    (
        "one Int64 key, COUNT",
        "SELECT a, count(*) FROM logs GROUP BY a",
        true,
    ),
    (
        "q33: Int64 and Int32 keys, COUNT, SUM(Int16), AVG(Int16)",
        "SELECT a, CAST(b AS INT) AS b32, count(*), sum(CAST(v AS SMALLINT)), \
         avg(CAST(v AS SMALLINT)) FROM logs GROUP BY a, b32",
        true,
    ),
    (
        "q36: four derived Int64 keys",
        "SELECT a, a - 1, a - 2, a - 3, count(*) FROM logs GROUP BY a, a - 1, a - 2, a - 3",
        true,
    ),
    (
        "q09: a single-distinct rewrite",
        "SELECT b, count(DISTINCT a) FROM logs GROUP BY b",
        true,
    ),
    (
        "NULL keys on every seventh row",
        "SELECT n, count(*), sum(v) FROM logs GROUP BY n",
        true,
    ),
    (
        "all-NULL keys",
        "SELECT z, count(*), sum(v) FROM logs GROUP BY z",
        true,
    ),
    (
        "one group",
        "SELECT severity_num, count(*), sum(v) FROM logs GROUP BY severity_num",
        true,
    ),
    (
        "90% of rows in one group",
        "SELECT CASE WHEN a % 10 = 0 THEN a ELSE 0 END AS k, count(*), sum(v) \
         FROM logs GROUP BY k",
        true,
    ),
    (
        "all-unique timestamp keys",
        "SELECT ts, count(*), min(v), max(v) FROM logs GROUP BY ts",
        true,
    ),
    (
        "a 16-byte Decimal128 state routed through the chunks",
        "SELECT a, sum(CAST(v AS DECIMAL(20, 2))) FROM logs GROUP BY a",
        true,
    ),
    (
        "a Date32 key",
        "SELECT arrow_cast(CAST(a AS INT), 'Date32') AS d, count(*), sum(v) FROM logs GROUP BY d",
        true,
    ),
];

/// The bucketed plan answers every admitted shape exactly as the stock final
/// does, at 1, 4 and 32 SQL partitions, with `engage_groups = 64` so every
/// partition of every fanned-out plan routes into buckets, and
/// `routed_chunk_rows = 64`: at the default 8192, 32 partitions each holding
/// one chunk per column in each of 64 buckets ask for more than the default
/// 256 MiB query ceiling before a single bucket is full. At one partition
/// the aggregate plans single-stage and the rule has nothing to replace; that
/// count is here to pin that it stays so.
#[tokio::test]
async fn answers_match_the_stock_final_at_1_4_and_32_partitions() {
    for partitions in [1usize, 4, 32] {
        let tuning = BucketedFinalAggregation {
            engage_groups: ENGAGE_GROUPS,
            bits: 6,
            routed_chunk_rows: 64,
        };
        let on = executor(ROWS, sql_config(partitions, Some(tuning), None)).await;
        let off = executor(ROWS, sql_config(partitions, None, None)).await;
        for (what, sql, admitted) in ANSWER_SHAPES {
            let plan = physical_plan(&on, sql).await;
            let fired = plan.contains("BucketedFinalAggregateExec");
            if partitions == 1 {
                assert!(!fired, "{what} at 1 partition:\n{plan}");
            } else {
                assert_eq!(
                    fired, *admitted,
                    "{what} at {partitions} partitions:\n{plan}"
                );
            }
            let on_rows = sorted_rows(&on, sql).await;
            let off_rows = sorted_rows(&off, sql).await;
            assert!(!off_rows.is_empty(), "{what}: no rows");
            assert_eq!(
                on_rows, off_rows,
                "{what} at {partitions} partitions answers differently"
            );
        }
    }
}

/// `None` installs no rule: the plan has no bucketed exec, and keeps the
/// stock `FinalPartitioned` aggregate the rule would otherwise replace.
#[tokio::test]
async fn the_plan_is_unchanged_when_the_field_is_none() {
    let off = executor(ROWS, sql_config(4, None, None)).await;
    for (what, sql, _) in ANSWER_SHAPES {
        let plan = physical_plan(&off, sql).await;
        assert!(
            !plan.contains("BucketedFinalAggregateExec"),
            "{what} with the field None:\n{plan}"
        );
    }
    let plan = physical_plan(&off, ANSWER_SHAPES[0].1).await;
    assert!(plan.contains("mode=FinalPartitioned"), "{plan}");
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

/// Every shape the rule must refuse, each a `FinalPartitioned` aggregate the
/// rule would otherwise see.
const REFUSED_SHAPES: &[(&str, &str)] = &[
    (
        "a string key",
        "SELECT body, count(*) FROM logs GROUP BY body",
    ),
    (
        "a dictionary key",
        "SELECT arrow_cast(body, 'Dictionary(Int32, Utf8)') AS k, count(*) FROM logs GROUP BY k",
    ),
    (
        "a view key",
        "SELECT arrow_cast(body, 'Utf8View') AS k, count(*) FROM logs GROUP BY k",
    ),
    (
        "a Boolean key",
        "SELECT a > 3000 AS k, count(*) FROM logs GROUP BY k",
    ),
    (
        "a Decimal128 key",
        "SELECT CAST(a AS DECIMAL(20, 2)) AS k, count(*) FROM logs GROUP BY k",
    ),
    (
        "a Duration key",
        "SELECT arrow_cast(a, 'Duration(Second)') AS k, count(*) FROM logs GROUP BY k",
    ),
    (
        "GROUPING SETS",
        "SELECT a, b, count(*) FROM logs GROUP BY GROUPING SETS ((a), (b))",
    ),
    (
        "q10: COUNT(DISTINCT) beside AVG",
        "SELECT b, sum(v), count(*), avg(v), count(DISTINCT a) FROM logs GROUP BY b",
    ),
];

/// The shapes the executor never hands the rule, planned through a session
/// directly ([`Planned`], built with `exact_typed_aggregates` on). The executor
/// fans out no aggregate that is not exact-typed (ADR-0094), so a float key or
/// float state never reaches a `FinalPartitioned` aggregate there, and its
/// validation refuses `first_value` outright. `first_value(.. ORDER BY ..)` is
/// refused on its ordering and on its `Boolean` `is_set` state field both.
const REFUSED_SESSION_SHAPES: &[(&str, &str)] = &[
    (
        "a Float64 key",
        "SELECT value, count(*) FROM samples GROUP BY value",
    ),
    (
        "a float accumulator state",
        "SELECT ts, sum(value) FROM samples GROUP BY ts",
    ),
    (
        "FIRST_VALUE ... ORDER BY",
        "SELECT ts, first_value(ts ORDER BY value) FROM samples GROUP BY ts",
    ),
];

/// Every refusal condition plans exactly as the field set to `None` does, and
/// each shape does reach a `FinalPartitioned` aggregate, so the refusal is the
/// gate's and not the absence of a target.
#[tokio::test]
async fn every_refusal_condition_leaves_the_plan_unchanged() {
    for (what, sql) in REFUSED_SESSION_SHAPES {
        let on = Planned::new(sql, sql_config(4, Some(bucketed(ENGAGE_GROUPS)), None)).await;
        let off = Planned::new(sql, sql_config(4, None, None)).await;
        let on_plan = format!("{}", displayable(on.physical.as_ref()).indent(false));
        let off_plan = format!("{}", displayable(off.physical.as_ref()).indent(false));
        assert!(
            off_plan.contains("mode=FinalPartitioned"),
            "{what} has no FinalPartitioned aggregate to refuse:\n{off_plan}"
        );
        assert_eq!(on_plan, off_plan, "the rule fired on {what}");
    }

    let on = executor(ROWS, sql_config(4, Some(bucketed(ENGAGE_GROUPS)), None)).await;
    let off = executor(ROWS, sql_config(4, None, None)).await;
    for (what, sql) in REFUSED_SHAPES {
        let on_plan = physical_plan(&on, sql).await;
        let off_plan = physical_plan(&off, sql).await;
        assert!(
            off_plan.contains("mode=FinalPartitioned"),
            "{what} has no FinalPartitioned aggregate to refuse:\n{off_plan}"
        );
        assert_eq!(on_plan, off_plan, "the rule fired on {what}");
    }
}

/// The rule's place in the optimizer: after `BoundedTopKAggregate`, before
/// `CompactSlices`.
///
/// The q33 shape under a `LIMIT` orders by `count(*)`, which bounded top-k
/// refuses, so no aggregate carries a limit and the bucketed exec replaces the
/// final; `CompactSlices` wraps only an `AggregateExec`, and the bucketed exec
/// already copies its output, so nothing wraps it. A `max`-ordered top-k is the
/// opposite case: bounded top-k sets `lim=[10]` on the final, the bucketed rule
/// refuses a limited aggregate, and the plan equals the plan with the field
/// `None`. Were the rule ordered before bounded top-k, it would replace that
/// final first and the limit would be gone.
#[tokio::test]
async fn the_rule_sits_after_bounded_topk_and_before_compact_slices() {
    let on = executor(ROWS, sql_config(4, Some(bucketed(ENGAGE_GROUPS)), Some(10))).await;
    let off = executor(ROWS, sql_config(4, None, Some(10))).await;

    let q33 = "SELECT a, CAST(b AS INT) AS b32, count(*) AS c, sum(CAST(v AS SMALLINT)) AS s \
               FROM logs GROUP BY a, b32 ORDER BY c DESC LIMIT 10";
    let on_tree = physical_plan_tree(&on, q33).await;
    let on_plan = format!("{}", displayable(on_tree.as_ref()).indent(false));
    let off_tree = physical_plan_tree(&off, q33).await;
    let off_plan = format!("{}", displayable(off_tree.as_ref()).indent(false));
    assert!(find_bucketed(&on_tree).is_some(), "{on_plan}");
    assert!(aggregate_limits(&on_tree).is_empty(), "{on_plan}");
    assert!(aggregate_limits(&off_tree).is_empty(), "{off_plan}");
    assert_ne!(
        parent_of_bucketed(&on_tree).as_deref(),
        Some("CompactSlicesExec"),
        "{on_plan}"
    );
    assert!(off_plan.contains("CompactSlicesExec"), "{off_plan}");
    assert_eq!(
        sorted_rows(&on, q33).await,
        sorted_rows(&off, q33).await,
        "q33 under a limit"
    );

    let max_topk = "SELECT a, max(ts) AS m FROM logs GROUP BY a ORDER BY m DESC LIMIT 10";
    let on_tree = physical_plan_tree(&on, max_topk).await;
    let on_plan = format!("{}", displayable(on_tree.as_ref()).indent(false));
    let off_plan = physical_plan(&off, max_topk).await;
    assert!(aggregate_limits(&on_tree).contains(&10), "{on_plan}");
    assert!(find_bucketed(&on_tree).is_none(), "{on_plan}");
    assert_eq!(on_plan, off_plan, "the rule fired on a bounded top-k");
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

/// `EXPLAIN ANALYZE` carries the exec's three counters on its line.
#[tokio::test]
async fn explain_analyze_reports_the_bucket_metrics() {
    let on = executor(ROWS, sql_config(4, Some(bucketed(ENGAGE_GROUPS)), None)).await;
    let run = pinned(&on, "SELECT a, count(*) FROM logs GROUP BY a")
        .await
        .execute_analyzed()
        .await
        .expect("query runs");
    assert_eq!(run.rows, ROWS as usize);
    let line = run
        .plan_with_metrics
        .lines()
        .find(|line| line.contains("BucketedFinalAggregateExec"))
        .unwrap_or_else(|| panic!("no bucketed exec:\n{}", run.plan_with_metrics));
    for metric in ["buckets_non_empty=", "routed_rows=", "engaged="] {
        assert!(line.contains(metric), "{metric} missing: {line}");
    }
    assert!(
        line.contains("engaged=4"),
        "every partition engages: {line}"
    );
    assert!(
        line.contains("output_rows=6.00 K") || line.contains("output_rows=6000"),
        "{line}"
    );
}

// ---------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------

/// Unique keys for the peak-asked comparison.
const PEAK_ROWS: i64 = 400_000;

/// With unique keys the bucketed final's peak asked bytes are at most three
/// quarters of the stock final's. Run at 4 partitions with `engage_groups =
/// 4096` and `routed_chunk_rows = 1024`: the chunk slack, at most one
/// partly filled chunk per column per bucket, is then about 1 MiB per
/// partition, where the default 8192 rows would make it about 8 MiB and the
/// comparison would need a fixture eight times this size.
/// `peak_asked_intermediate_bytes` is compared rather than the charged peak
/// so the stock final's reserve-ahead factor is not counted against it.
#[tokio::test]
async fn peak_asked_bytes_are_lower_than_the_stock_final_on_unique_keys() {
    let sql = "SELECT a, count(*) FROM logs GROUP BY a";
    let tuning = BucketedFinalAggregation {
        engage_groups: 4096,
        bits: 6,
        routed_chunk_rows: 1024,
    };
    let on = executor(PEAK_ROWS, sql_config(4, Some(tuning), None)).await;
    let off = executor(PEAK_ROWS, sql_config(4, None, None)).await;
    let on_outcome = on
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("bucketed query runs");
    let off_outcome = off
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("stock query runs");
    let on_peak = on_outcome.peak_asked_intermediate_bytes;
    let off_peak = off_outcome.peak_asked_intermediate_bytes;
    eprintln!("peak asked: bucketed {on_peak}, stock {off_peak}");
    assert!(
        on_peak * 4 <= off_peak * 3,
        "bucketed peak {on_peak} is above 0.75 of the stock peak {off_peak}"
    );
}

// ---------------------------------------------------------------------------
// The samples fixture: the exec node run directly
// ---------------------------------------------------------------------------

/// One group per sample: every `ts` is unique.
const SAMPLES: i64 = 64_000;

const SAMPLES_SQL: &str = "SELECT ts, count(*) FROM samples GROUP BY ts";

/// A statement planned through the production pool, with the fixture it reads.
struct Planned {
    _fixture: Fixture,
    _accountant: Arc<TenantMemoryAccountant>,
    pool: Arc<dyn MemoryPool>,
    ctx: datafusion::prelude::SessionContext,
    physical: Arc<dyn ExecutionPlan>,
}

impl Planned {
    async fn new(sql: &str, config: SqlConfig) -> Self {
        Self::with_samples(sql, config, SAMPLES).await
    }

    /// `sql` over `samples` samples, one per `ts` in `0..samples`.
    async fn with_samples(sql: &str, config: SqlConfig, samples: i64) -> Self {
        let tenant = tenant_id("acme");
        let specs = vec![SegSpec::new(
            10,
            1,
            1,
            vec![SeriesSpec::new(
                "m",
                (0..samples).map(|i| (i, i as f64)).collect(),
            )],
        )];
        let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
        let accountant = TenantMemoryAccountant::new(1 << 30);
        let (pool, _breach) = config.query_pool(Arc::clone(&accountant), QueryAccounting::new());
        let provider = Arc::new(RavelTableProvider::new(
            fixture.snapshot(&tenant).await,
            tenant.hash(),
            SegmentFetcher::new(Arc::clone(&fixture.store)),
            config.clone(),
            PhaseAccounting::new(),
        ));
        let ctx = build_session(
            &config,
            Arc::clone(&pool) as Arc<dyn MemoryPool>,
            SessionTable::Metrics(provider),
            true,
            SpillDecision::Disabled,
            &ravel_sql::SessionPrincipal::without_capabilities(ravel_types::TenantHash([0u8; 16])),
        )
        .expect("session");
        let physical = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        Planned {
            _fixture: fixture,
            _accountant: accountant,
            pool,
            ctx,
            physical,
        }
    }

    fn bucketed(&self) -> Arc<dyn ExecutionPlan> {
        find_bucketed(&self.physical).unwrap_or_else(|| {
            panic!(
                "no bucketed exec:\n{}",
                displayable(self.physical.as_ref()).indent(false)
            )
        })
    }

    fn reserved(&self) -> usize {
        self.pool.reserved()
    }

    fn held(&self) -> usize {
        self.pool
            .downcast_ref::<TenantDelegatingPool>()
            .expect("query_pool builds a TenantDelegatingPool")
            .held_bytes()
    }
}

/// Each partition's value of the counter `name` on `plan`, indexed by
/// partition.
fn counter_by_partition(
    plan: &Arc<dyn ExecutionPlan>,
    name: &str,
    partitions: usize,
) -> Vec<usize> {
    let mut out = vec![0usize; partitions];
    let metrics = plan.metrics().expect("the exec reports metrics");
    for metric in metrics.iter() {
        if let MetricValue::Count { name: n, count } = metric.value()
            && n.as_ref() == name
        {
            let partition = metric.partition().expect("a per-partition counter");
            out[partition] += count.value();
        }
    }
    out
}

/// The buckets are not correlated with the partitions: on unique keys at 32
/// partitions, every partition fills at least `2^bits - 1` of its 64 buckets.
///
/// The repartition exchange sends a row to `hash(seed 0) % 32`, so within one
/// partition the low five bits of that hash are fixed. Had the exec bucketed
/// on the exchange's seed and the hash's low bits, a partition's rows would
/// land in at most `64 / 32 = 2` buckets; the fixed non-zero seed and the top
/// bits are what make the buckets spread. `routed_chunk_rows = 256` keeps 32
/// partitions of 64 buckets under the default query ceiling.
#[tokio::test]
async fn every_partition_fills_its_buckets_on_unique_keys_at_32_partitions() {
    const PARTITIONS: usize = 32;
    let tuning = BucketedFinalAggregation {
        engage_groups: ENGAGE_GROUPS,
        bits: 6,
        routed_chunk_rows: 256,
    };
    let planned = Planned::new(SAMPLES_SQL, sql_config(PARTITIONS, Some(tuning), None)).await;
    let batches = collect(Arc::clone(&planned.physical), planned.ctx.task_ctx())
        .await
        .expect("query runs");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, SAMPLES as usize);

    let exec = planned.bucketed();
    let engaged = counter_by_partition(&exec, "engaged", PARTITIONS);
    assert_eq!(engaged, vec![1; PARTITIONS], "every partition engages");
    let routed: usize = counter_by_partition(&exec, "routed_rows", PARTITIONS)
        .iter()
        .sum();
    assert!(routed > 0, "nothing routed");
    let non_empty = counter_by_partition(&exec, "buckets_non_empty", PARTITIONS);
    for (partition, buckets) in non_empty.iter().enumerate() {
        assert!(
            *buckets >= 63,
            "partition {partition} filled {buckets} of 64 buckets: {non_empty:?}"
        );
    }
}

/// An input that records, per partition, how many batches it handed out and
/// whether it has returned end-of-stream.
#[derive(Debug)]
struct RecordingInput {
    input: Arc<dyn ExecutionPlan>,
    batches: Arc<Vec<AtomicUsize>>,
    exhausted: Arc<Vec<AtomicBool>>,
}

impl DisplayAs for RecordingInput {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingInput")
    }
}

impl ExecutionPlan for RecordingInput {
    fn name(&self) -> &'static str {
        "RecordingInput"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RecordingInput {
            input: children.swap_remove(0),
            batches: Arc::clone(&self.batches),
            exhausted: Arc::clone(&self.exhausted),
        }))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordingStream {
            inner: self.input.execute(partition, context)?,
            partition,
            batches: Arc::clone(&self.batches),
            exhausted: Arc::clone(&self.exhausted),
        }))
    }
}

struct RecordingStream {
    inner: SendableRecordBatchStream,
    partition: usize,
    batches: Arc<Vec<AtomicUsize>>,
    exhausted: Arc<Vec<AtomicBool>>,
}

impl Stream for RecordingStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = self.inner.poll_next_unpin(cx);
        match &polled {
            Poll::Ready(Some(Ok(_))) => {
                self.batches[self.partition].fetch_add(1, Ordering::SeqCst);
            }
            Poll::Ready(None) => self.exhausted[self.partition].store(true, Ordering::SeqCst),
            _ => {}
        }
        polled
    }
}

impl RecordBatchStream for RecordingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// No partition emits a row before its input has returned end-of-stream: a
/// bucket's groups are complete only once every input row has been routed.
/// Each partition must have read more than one input batch, so an exec that
/// emitted after its first batch would be caught.
#[tokio::test]
async fn no_output_before_the_input_is_exhausted() {
    const PARTITIONS: usize = 4;
    let planned = Planned::new(
        SAMPLES_SQL,
        sql_config(PARTITIONS, Some(bucketed(ENGAGE_GROUPS)), None),
    )
    .await;
    let exec = planned.bucketed();
    let batches: Arc<Vec<AtomicUsize>> =
        Arc::new((0..PARTITIONS).map(|_| AtomicUsize::new(0)).collect());
    let exhausted: Arc<Vec<AtomicBool>> =
        Arc::new((0..PARTITIONS).map(|_| AtomicBool::new(false)).collect());
    let recorder: Arc<dyn ExecutionPlan> = Arc::new(RecordingInput {
        input: Arc::clone(exec.children()[0]),
        batches: Arc::clone(&batches),
        exhausted: Arc::clone(&exhausted),
    });
    let exec = exec
        .replace_children(
            vec![recorder],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .expect("rewired");
    assert!(exec.downcast_ref::<BucketedFinalAggregateExec>().is_some());

    let task_ctx = planned.ctx.task_ctx();
    let runs = (0..PARTITIONS).map(|partition| {
        let exec = Arc::clone(&exec);
        let task_ctx = Arc::clone(&task_ctx);
        let batches = Arc::clone(&batches);
        let exhausted = Arc::clone(&exhausted);
        async move {
            let mut stream = exec.execute(partition, task_ctx).expect("executes");
            let mut rows = 0usize;
            while let Some(batch) = stream.next().await {
                let batch = batch.expect("batch");
                assert!(
                    exhausted[partition].load(Ordering::SeqCst),
                    "partition {partition} emitted {} rows after {} input batches, \
                     before its input ended",
                    batch.num_rows(),
                    batches[partition].load(Ordering::SeqCst),
                );
                rows += batch.num_rows();
            }
            rows
        }
    });
    let rows: usize = futures::future::join_all(runs).await.into_iter().sum();
    assert_eq!(rows, SAMPLES as usize);
    for partition in 0..PARTITIONS {
        let read = batches[partition].load(Ordering::SeqCst);
        assert!(read > 1, "partition {partition} read {read} input batches");
    }
}

/// A bucket chunk the pool refuses fails the query with `ResourcesExhausted`
/// naming the consumer, and leaves nothing charged. One chunk of 4,000,000
/// rows is about 32 MB per column; the ceiling is 16 MiB, which the scan and
/// the partial aggregate fit under.
#[tokio::test]
async fn a_refused_bucket_grow_fails_the_query_with_resources_exhausted() {
    let tuning = BucketedFinalAggregation {
        engage_groups: ENGAGE_GROUPS,
        bits: 6,
        routed_chunk_rows: 4_000_000,
    };
    let mut config = sql_config(4, Some(tuning), None);
    config.max_query_bytes = 16 << 20;
    let planned = Planned::new(SAMPLES_SQL, config).await;
    planned.bucketed();
    let err = collect(Arc::clone(&planned.physical), planned.ctx.task_ctx())
        .await
        .expect_err("the bucket chunk exceeds the ceiling");
    wait_for_release(&planned).await;
    let message = err.to_string();
    assert!(
        message.contains(BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX),
        "the error does not name the consumer: {message}"
    );
    assert!(
        message.contains("Resources exhausted"),
        "not ResourcesExhausted: {message}"
    );
    assert_eq!(
        planned.reserved(),
        0,
        "bytes left reserved after the failure"
    );
    assert_eq!(planned.held(), 0, "bytes left held after the failure");
}

/// Wait, up to five seconds, for the pool to drain: `collect` returns on the
/// first error and the other partitions' tasks, aborted when its stream drops,
/// release their reservations as the runtime cancels them.
async fn wait_for_release(planned: &Planned) {
    for _ in 0..500 {
        if planned.reserved() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Dropping the streams after phase 2 has started, with buckets still
/// unemitted, returns the pool to zero. Two bits, so four buckets; every
/// partition is polled to its first output batch, which exists only once its
/// input has ended and bucket 0 has been aggregated.
#[tokio::test]
async fn dropping_the_stream_releases_every_bucket() {
    const PARTITIONS: usize = 2;
    let tuning = BucketedFinalAggregation {
        engage_groups: ENGAGE_GROUPS,
        bits: 2,
        routed_chunk_rows: 1024,
    };
    let planned = Planned::new(SAMPLES_SQL, sql_config(PARTITIONS, Some(tuning), None)).await;
    let exec = planned.bucketed();
    let task_ctx = planned.ctx.task_ctx();
    let firsts = (0..PARTITIONS).map(|partition| {
        let exec = Arc::clone(&exec);
        let task_ctx = Arc::clone(&task_ctx);
        async move {
            let mut stream = exec.execute(partition, task_ctx).expect("executes");
            let first = stream.next().await.expect("a first batch").expect("batch");
            (stream, first.num_rows())
        }
    });
    let started = futures::future::join_all(firsts).await;
    let emitted: usize = started.iter().map(|(_, rows)| rows).sum();
    assert!(
        emitted < SAMPLES as usize,
        "every row already emitted: {emitted}"
    );
    let routed: usize = counter_by_partition(&exec, "routed_rows", PARTITIONS)
        .iter()
        .sum();
    assert!(routed > 0, "phase 1 routed nothing");
    let reserved_mid_drain = planned.reserved();
    assert!(reserved_mid_drain > 0, "nothing reserved mid-drain");

    drop(started);
    assert_eq!(planned.reserved(), 0, "bytes left reserved after the drop");
    assert_eq!(planned.held(), 0, "bytes left held after the drop");
}

// ---------------------------------------------------------------------------
// Repeated and NULL keys routed after engagement
// ---------------------------------------------------------------------------

/// Samples in the repeated-key fixture at `partitions` partitions.
///
/// The scan emits 8,192-row batches that a round-robin exchange deals to the
/// partial partitions. A partial skips only once a batch arrives after its
/// first `SKIP_PARTIAL_AGGREGATION_PROBE_ROWS` (8,192) rows, and the hash
/// exchange above it coalesces what reaches each final partition into
/// batches of up to 8,192 rows. A final partition reads more than one batch
/// only when more than 8,192 rows reach it, so the fixture is sized for that:
/// 300,000 rows at 4 partitions, and 800,000 (98 scan batches, about three
/// per partial) at 32.
fn repeated_samples(partitions: usize) -> i64 {
    if partitions >= 32 { 800_000 } else { 300_000 }
}

/// 1,000 non-NULL keys, every key on 1 row in 1,000, plus a NULL key on
/// every seventh row. 1,001 groups in the first 8,192 rows a partial reads
/// is a ratio above `SKIP_PARTIAL_AGGREGATION_PROBE_RATIO` (0.1), so the partial
/// stops aggregating and passes rows through.
const REPEATED_SQL: &str = "SELECT CASE WHEN arrow_cast(ts, 'Int64') % 7 = 0 THEN NULL \
                            ELSE arrow_cast(ts, 'Int64') % 1000 END AS k, count(*) AS c, \
                            sum(arrow_cast(ts, 'Int64') % 997) AS s FROM samples GROUP BY k";

/// Every row of `batches`, rendered column by column, sorted.
fn render_sorted(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
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

/// `plan` with the input of its bucketed exec wrapped in a [`RecordingInput`]
/// that counts each partition's input batches into `batches`.
fn recording_bucketed_input(
    plan: &Arc<dyn ExecutionPlan>,
    batches: &Arc<Vec<AtomicUsize>>,
) -> Arc<dyn ExecutionPlan> {
    if plan.downcast_ref::<BucketedFinalAggregateExec>().is_some() {
        let recorder: Arc<dyn ExecutionPlan> = Arc::new(RecordingInput {
            input: Arc::clone(plan.children()[0]),
            batches: Arc::clone(batches),
            exhausted: Arc::new((0..batches.len()).map(|_| AtomicBool::new(false)).collect()),
        });
        return Arc::clone(plan)
            .replace_children(
                vec![recorder],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .expect("rewired");
    }
    let children: Vec<Arc<dyn ExecutionPlan>> = plan
        .children()
        .into_iter()
        .map(|child| recording_bucketed_input(child, batches))
        .collect();
    if children.is_empty() {
        return Arc::clone(plan);
    }
    Arc::clone(plan)
        .replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .expect("rewired")
}

/// The first `Partial` `AggregateExec` in `plan`, in pre-order.
fn find_partial(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan
        .downcast_ref::<AggregateExec>()
        .is_some_and(|aggregate| *aggregate.mode() == AggregateMode::Partial)
    {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_partial)
}

/// Each partition's `output_rows` on `plan`, indexed by partition.
fn output_rows_by_partition(plan: &Arc<dyn ExecutionPlan>, partitions: usize) -> Vec<usize> {
    let mut out = vec![0usize; partitions];
    let metrics = plan.metrics().expect("the exec reports metrics");
    for metric in metrics.iter() {
        if let MetricValue::OutputRows(count) = metric.value() {
            let partition = metric.partition().expect("per-partition output rows");
            out[partition] += count.value();
        }
    }
    out
}

/// Repeated keys and NULL keys answer exactly as the stock final does when
/// the same key is in the table at engagement and arrives again in later
/// routed batches. `engage_groups = 16` engages a partition once its table
/// holds 16 of the 1,001 groups, at 32 partitions too, and the partial
/// aggregate passes raw rows through (its `skipped_aggregation_rows` is
/// asserted above zero), so keys keep arriving after their partition
/// engaged. Keys, counts and sums are compared, with the field set and
/// unset, at 1, 4 and 32 partitions.
///
/// The metrics show the routing really happened after engagement: every
/// engaged partition read more than one input batch, and its `routed_rows`
/// exceeds its groups (its `output_rows`, there being no TopK), which bounds
/// the rows engagement itself routes, since the table it routes holds each
/// group once.
#[tokio::test]
async fn repeated_and_null_keys_routed_after_engagement_answer_as_the_stock_final() {
    let tuning = BucketedFinalAggregation {
        engage_groups: 16,
        bits: 6,
        routed_chunk_rows: 64,
    };
    for partitions in [1usize, 4, 32] {
        let on = Planned::with_samples(
            REPEATED_SQL,
            sql_config(partitions, Some(tuning), None),
            repeated_samples(partitions),
        )
        .await;
        let off = Planned::with_samples(
            REPEATED_SQL,
            sql_config(partitions, None, None),
            repeated_samples(partitions),
        )
        .await;
        let input_batches: Arc<Vec<AtomicUsize>> =
            Arc::new((0..partitions).map(|_| AtomicUsize::new(0)).collect());
        let physical = recording_bucketed_input(&on.physical, &input_batches);
        let on_batches = collect(Arc::clone(&physical), on.ctx.task_ctx())
            .await
            .expect("bucketed query runs");
        let off_batches = collect(Arc::clone(&off.physical), off.ctx.task_ctx())
            .await
            .expect("stock query runs");
        let on_rows = render_sorted(&on_batches);
        let off_rows = render_sorted(&off_batches);
        assert_eq!(off_rows.len(), 1_001, "1,000 keys and NULL");
        assert!(
            off_rows.iter().any(|row| row.starts_with('|')),
            "no NULL key group"
        );
        assert_eq!(
            on_rows, off_rows,
            "repeated keys at {partitions} partitions answer differently"
        );
        if partitions == 1 {
            continue;
        }

        let exec = find_bucketed(&physical).expect("a bucketed exec");
        let engaged = counter_by_partition(&exec, "engaged", partitions);
        let routed = counter_by_partition(&exec, "routed_rows", partitions);
        let groups = output_rows_by_partition(&exec, partitions);
        let read: Vec<usize> = input_batches
            .iter()
            .map(|count| count.load(Ordering::SeqCst))
            .collect();
        assert_eq!(groups.iter().sum::<usize>(), 1_001);
        assert!(
            engaged.contains(&1),
            "no partition engaged at {partitions}: {engaged:?}"
        );
        for partition in 0..partitions {
            if engaged[partition] == 1 {
                assert!(
                    read[partition] > 1,
                    "partition {partition} at {partitions} read {} input batches",
                    read[partition]
                );
                assert!(
                    routed[partition] > groups[partition],
                    "partition {partition} at {partitions} routed {} rows for {} groups: \
                     nothing was routed after engagement",
                    routed[partition],
                    groups[partition],
                );
            }
        }
        let partial = find_partial(&on.physical).expect("a partial aggregate");
        let skipped: usize = counter_by_partition(&partial, "skipped_aggregation_rows", partitions)
            .iter()
            .sum();
        assert!(
            skipped > 0,
            "the partial aggregate never skipped at {partitions}: the final saw no raw rows"
        );
    }
}

// ---------------------------------------------------------------------------
// Per-bucket top-k (ADR-3565 decision 4)
// ---------------------------------------------------------------------------

/// Samples in the tie fixture: enough that each final partition reads
/// several input batches at 4 partitions (see [`repeated_samples`]).
const TIE_SAMPLES: i64 = 300_000;

/// A key over `t = 0..300,000` in 300 runs of 1,000: five keys with distinct
/// counts 1,500, 1,200, 900, 600 and 300, twenty keys tied at 75, and 4,900
/// keys of count 60. Every key recurs across the whole range, and 4,925
/// groups in a partial's first 8,192 rows make it skip, so a key's rows keep
/// reaching its final partition in later input batches, after engagement.
/// Under `LIMIT 10` the tenth count is 75, a tie five of the twenty fill.
const TIE_SQL: &str = "SELECT CASE WHEN t % 1000 < 5 THEN 1000 WHEN t % 1000 < 9 THEN 1001 \
                       WHEN t % 1000 < 12 THEN 1002 WHEN t % 1000 < 14 THEN 1003 \
                       WHEN t % 1000 = 14 THEN 1004 WHEN t % 1000 < 20 THEN 2000 + (t / 1000) % 20 \
                       ELSE 3000 + t % 5000 END AS k, count(*) AS c \
                       FROM (SELECT arrow_cast(ts, 'Int64') AS t FROM samples) \
                       GROUP BY k ORDER BY c DESC LIMIT 10";

/// `(key, count)` of every row of an answer rendered as `key|count`.
fn key_counts(rows: &[String]) -> Vec<(i64, i64)> {
    rows.iter()
        .map(|row| {
            let (key, count) = row.split_once('|').expect("two columns");
            (
                key.parse().expect("an integer key"),
                count.parse().expect("an integer count"),
            )
        })
        .collect()
}

/// The value of the metric `name` on an `EXPLAIN ANALYZE` line, with
/// DataFusion's `K`/`M`/`B` suffix applied.
fn metric_on_line(line: &str, name: &str) -> f64 {
    let needle = format!(" {name}=");
    let start = line
        .find(&needle)
        .or_else(|| line.find(&format!("[{name}=")))
        .unwrap_or_else(|| panic!("{name} missing: {line}"))
        + needle.len();
    let rest = &line[start..];
    let end = rest.find([',', ']']).unwrap_or(rest.len());
    let mut parts = rest[..end].split_whitespace();
    let number: f64 = parts.next().expect("a value").parse().expect("a number");
    let scale = match parts.next() {
        None => 1.0,
        Some("K") => 1e3,
        Some("M") => 1e6,
        Some("B") => 1e9,
        Some(unit) => panic!("unit {unit} on {line}"),
    };
    number * scale
}

/// The bucketed exec's line of `sql`'s `EXPLAIN ANALYZE` on `executor`.
async fn analyzed_bucketed_line(executor: &SqlExecutor, sql: &str) -> String {
    let run = pinned(executor, sql)
        .await
        .execute_analyzed()
        .await
        .expect("query runs");
    run.plan_with_metrics
        .lines()
        .find(|line| line.contains("BucketedFinalAggregateExec"))
        .unwrap_or_else(|| panic!("no bucketed exec:\n{}", run.plan_with_metrics))
        .to_string()
}

/// The per-bucket cut is exact when the tenth count ties across buckets: at
/// 1, 4 and 32 partitions the counts are the stock final's multiset, and
/// every key whose count is above the tenth is the stock final's. Which five
/// of the twenty tied keys survive may differ, as SQL allows. Both answers
/// are also checked against the fixture's known counts, and the run proves
/// the cut fired: `topk_rows_dropped` is above zero at 4 and 32 partitions.
///
/// At 4 partitions every engaged partition reads more than one input batch,
/// so a key's state is spread over several of its bucket's chunks: a cut
/// taken before the bucket's last chunk is merged splits that key's count
/// and fails here.
#[tokio::test]
async fn per_bucket_topk_is_exact_on_ties() {
    let expected_counts = vec![1500, 1200, 900, 600, 300, 75, 75, 75, 75, 75];
    for partitions in [1usize, 4, 32] {
        // About 150 groups reach each of 32 partitions: 4 buckets keep more
        // than 10 in a bucket, so the cut has rows to drop there too.
        let tuning = BucketedFinalAggregation {
            engage_groups: ENGAGE_GROUPS,
            bits: if partitions == 32 { 2 } else { 6 },
            routed_chunk_rows: 64,
        };
        let on = Planned::with_samples(
            TIE_SQL,
            sql_config(partitions, Some(tuning), None),
            TIE_SAMPLES,
        )
        .await;
        let off =
            Planned::with_samples(TIE_SQL, sql_config(partitions, None, None), TIE_SAMPLES).await;
        let input_batches: Arc<Vec<AtomicUsize>> =
            Arc::new((0..partitions).map(|_| AtomicUsize::new(0)).collect());
        let physical = recording_bucketed_input(&on.physical, &input_batches);
        let on_batches = collect(Arc::clone(&physical), on.ctx.task_ctx())
            .await
            .expect("bucketed query runs");
        let off_batches = collect(Arc::clone(&off.physical), off.ctx.task_ctx())
            .await
            .expect("stock query runs");
        let on_rows = key_counts(&render_sorted(&on_batches));
        let off_rows = key_counts(&render_sorted(&off_batches));
        let mut on_counts: Vec<i64> = on_rows.iter().map(|(_, count)| *count).collect();
        let mut off_counts: Vec<i64> = off_rows.iter().map(|(_, count)| *count).collect();
        on_counts.sort_unstable_by(|a, b| b.cmp(a));
        off_counts.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(off_counts, expected_counts, "the stock final's counts");
        assert_eq!(
            on_counts, off_counts,
            "count multiset at {partitions} partitions"
        );
        let above = |rows: &[(i64, i64)]| {
            let mut keys: Vec<i64> = rows
                .iter()
                .filter(|(_, count)| *count > 75)
                .map(|(key, _)| *key)
                .collect();
            keys.sort_unstable();
            keys
        };
        assert_eq!(above(&off_rows), vec![1000, 1001, 1002, 1003, 1004]);
        assert_eq!(
            above(&on_rows),
            above(&off_rows),
            "keys above the tie at {partitions} partitions"
        );
        for (key, count) in &on_rows {
            if *count == 75 {
                assert!((2000..2020).contains(key), "{key} is not a tied key");
            }
        }
        if partitions == 1 {
            continue;
        }
        let exec = find_bucketed(&physical).expect("a bucketed exec");
        let line = format!("{}", displayable(exec.as_ref()).one_line());
        assert!(line.contains(", k=10"), "no TopK on the exec: {line}");
        let dropped: usize = counter_by_partition(&exec, "topk_rows_dropped", partitions)
            .iter()
            .sum();
        assert!(
            dropped > 0,
            "the cut never fired at {partitions} partitions"
        );
        if partitions == 4 {
            let engaged = counter_by_partition(&exec, "engaged", partitions);
            assert_eq!(engaged, vec![1; 4]);
            for (partition, count) in input_batches.iter().enumerate() {
                let read = count.load(Ordering::SeqCst);
                assert!(read > 1, "partition {partition} read {read} input batches");
            }
        }
    }
}

/// `EXPLAIN` shows the TopK the exec cuts to, and `EXPLAIN ANALYZE` the rows
/// the cut dropped, on the q33 shape under `ORDER BY count DESC LIMIT 10`
/// through the executor: 6,000 unique `a` reach the exec, and with at most
/// ten per bucket in 64 buckets of 4 partitions, at most 2,560 leave it.
#[tokio::test]
async fn explain_analyze_reports_the_topk_and_the_rows_it_dropped() {
    let on = executor(ROWS, sql_config(4, Some(bucketed(ENGAGE_GROUPS)), None)).await;
    let q33 = "SELECT a, CAST(b AS INT) AS b32, count(*) AS c, sum(CAST(v AS SMALLINT)) AS s \
               FROM logs GROUP BY a, b32 ORDER BY c DESC LIMIT 10";
    let plan = physical_plan(&on, q33).await;
    let line = plan
        .lines()
        .find(|line| line.contains("BucketedFinalAggregateExec"))
        .unwrap_or_else(|| panic!("no bucketed exec:\n{plan}"));
    assert!(
        line.contains(", topk=[count(Int64(1))@2 DESC, "),
        "no TopK on the EXPLAIN line: {line}"
    );
    let line = analyzed_bucketed_line(&on, q33).await;
    assert!(line.contains(", k=10"), "{line}");
    let dropped = metric_on_line(&line, "topk_rows_dropped");
    let output = metric_on_line(&line, "output_rows");
    assert!(output <= 2_560.0, "{line}");
    assert_eq!(dropped + output, ROWS as f64, "{line}");
}

/// The q33 shape over the samples fixture: two fixed-width keys that are
/// unique together, `count(*)`, ordered by the count under `LIMIT 10`.
const Q33_SAMPLES_SQL: &str = "SELECT ts, CAST(value AS BIGINT) AS v, count(*) AS c \
                               FROM samples GROUP BY ts, v ORDER BY c DESC LIMIT 10";

/// With unique keys, `LIMIT 10` and 64 buckets, every partition emits at
/// most `64 x 10` rows to the sort above, and the rows it cut account for
/// every other group: per partition `output_rows <= 640`, and over the
/// partitions `output_rows + topk_rows_dropped` is exactly the 64,000
/// groups, so `topk_rows_dropped` is at least the groups minus 640 per
/// partition.
#[tokio::test]
async fn per_bucket_topk_feeds_the_sort_at_most_64_times_k_rows_per_partition() {
    const PARTITIONS: usize = 4;
    let tuning = BucketedFinalAggregation {
        engage_groups: ENGAGE_GROUPS,
        bits: 6,
        routed_chunk_rows: 256,
    };
    let planned = Planned::new(Q33_SAMPLES_SQL, sql_config(PARTITIONS, Some(tuning), None)).await;
    let exec = planned.bucketed();
    let topk = exec
        .downcast_ref::<BucketedFinalAggregateExec>()
        .and_then(BucketedFinalAggregateExec::topk)
        .expect("the rule reached the TopK");
    assert_eq!(topk.fetch(), 10);
    let batches = collect(Arc::clone(&planned.physical), planned.ctx.task_ctx())
        .await
        .expect("query runs");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);

    let engaged = counter_by_partition(&exec, "engaged", PARTITIONS);
    assert_eq!(engaged, vec![1; PARTITIONS], "every partition engages");
    let output = output_rows_by_partition(&exec, PARTITIONS);
    let dropped = counter_by_partition(&exec, "topk_rows_dropped", PARTITIONS);
    for partition in 0..PARTITIONS {
        assert!(
            output[partition] <= 64 * 10,
            "partition {partition} fed the sort {} rows: {output:?}",
            output[partition]
        );
        assert!(output[partition] > 0, "partition {partition}: {output:?}");
    }
    let total: usize = output.iter().sum::<usize>() + dropped.iter().sum::<usize>();
    assert_eq!(total, SAMPLES as usize, "{output:?} {dropped:?}");
    assert!(dropped.iter().sum::<usize>() >= SAMPLES as usize - PARTITIONS * 640);
}

/// Without a fetch the sort reaches, the exec keeps no TopK and cuts
/// nothing: a plain `ORDER BY` with no `LIMIT`, a `LIMIT` with no
/// `ORDER BY`, and an `ORDER BY ... LIMIT` whose sort key the projection
/// computes rather than passes as a bare column. Each exec is run directly
/// over every partition, so a `LIMIT` above cannot stop it early: every
/// partition engages, `topk_rows_dropped` is 0 and the rows are the 64,000
/// groups.
#[tokio::test]
async fn per_bucket_topk_does_not_engage_without_a_fetch_above() {
    const PARTITIONS: usize = 4;
    let statements = [
        "SELECT ts, count(*) AS c FROM samples GROUP BY ts ORDER BY c DESC",
        "SELECT ts, count(*) AS c FROM samples GROUP BY ts LIMIT 10",
        "SELECT ts, count(*) + 1 AS c FROM samples GROUP BY ts ORDER BY c DESC LIMIT 10",
    ];
    let tuning = BucketedFinalAggregation {
        engage_groups: ENGAGE_GROUPS,
        bits: 6,
        routed_chunk_rows: 256,
    };
    for sql in statements {
        // The first-groups bound (ADR-3985) would cut the `LIMIT` with no
        // `ORDER BY` to 10 groups per partition below this exec.
        let config = SqlConfig {
            bounded_groups_max_limit: 0,
            ..sql_config(PARTITIONS, Some(tuning), None)
        };
        let planned = Planned::new(sql, config).await;
        let exec = planned.bucketed();
        let line = format!("{}", displayable(exec.as_ref()).one_line());
        assert!(
            exec.downcast_ref::<BucketedFinalAggregateExec>()
                .expect("bucketed")
                .topk()
                .is_none(),
            "{sql}: a TopK was kept: {line}"
        );
        assert!(!line.contains("topk="), "{sql}: {line}");
        let task_ctx = planned.ctx.task_ctx();
        let runs = (0..PARTITIONS).map(|partition| {
            let exec = Arc::clone(&exec);
            let task_ctx = Arc::clone(&task_ctx);
            async move {
                let mut stream = exec.execute(partition, task_ctx).expect("executes");
                let mut rows = 0usize;
                while let Some(batch) = stream.next().await {
                    rows += batch.expect("batch").num_rows();
                }
                rows
            }
        });
        let rows: usize = futures::future::join_all(runs).await.into_iter().sum();
        assert_eq!(rows, SAMPLES as usize, "{sql}");
        assert_eq!(
            counter_by_partition(&exec, "engaged", PARTITIONS),
            vec![1; PARTITIONS],
            "{sql}"
        );
        assert_eq!(
            counter_by_partition(&exec, "topk_rows_dropped", PARTITIONS),
            vec![0; PARTITIONS],
            "{sql}"
        );
        assert_eq!(
            output_rows_by_partition(&exec, PARTITIONS)
                .iter()
                .sum::<usize>(),
            SAMPLES as usize,
            "{sql}"
        );
    }
}

/// Bytes of one output row of `exec`: the widths of its columns, every one of
/// them fixed-width.
fn output_row_bytes(exec: &Arc<dyn ExecutionPlan>) -> usize {
    exec.schema()
        .fields()
        .iter()
        .map(|field| {
            field
                .data_type()
                .primitive_width()
                .unwrap_or_else(|| panic!("{field} is not fixed-width"))
        })
        .sum()
}

/// The per-bucket cut releases a bucket's evaluated output beyond its top k
/// before that output drains. Pinned on a state that does not depend on
/// scheduling: both partitions at their first output batch, with their input
/// exhausted and bucket 0 evaluated.
///
/// `bits = 1` gives each of 2 partitions two buckets of about 16,000 groups,
/// above the 8,192-row `batch_size`, so without the cut bucket 0's output is
/// still pending after its first batch. With the cut, at most 10 rows remain
/// and the first batch hands them all out. The reserved bytes therefore fall
/// by at least the uncut bucket 0 output: its rows, which are the cut's
/// `topk_rows_dropped` plus the rows kept, at 24 bytes a row.
#[tokio::test]
async fn per_bucket_topk_releases_the_pending_bucket_output() {
    const PARTITIONS: usize = 2;
    let tuning = BucketedFinalAggregation {
        engage_groups: ENGAGE_GROUPS,
        bits: 1,
        routed_chunk_rows: 256,
    };
    let first_batches = |planned: &Planned, exec: Arc<dyn ExecutionPlan>| {
        let task_ctx = planned.ctx.task_ctx();
        let firsts = (0..PARTITIONS).map(move |partition| {
            let exec = Arc::clone(&exec);
            let task_ctx = Arc::clone(&task_ctx);
            async move {
                let mut stream = exec.execute(partition, task_ctx).expect("executes");
                let first = stream.next().await.expect("a first batch").expect("batch");
                (stream, first.num_rows())
            }
        });
        futures::future::join_all(firsts)
    };

    let cut = Planned::new(Q33_SAMPLES_SQL, sql_config(PARTITIONS, Some(tuning), None)).await;
    let cut_exec = cut.bucketed();
    let started_cut = first_batches(&cut, Arc::clone(&cut_exec)).await;
    let reserved_cut = cut.reserved();
    let dropped = counter_by_partition(&cut_exec, "topk_rows_dropped", PARTITIONS);
    for (partition, (_, rows)) in started_cut.iter().enumerate() {
        assert!(*rows <= 10, "partition {partition} emitted {rows} rows");
        assert!(
            dropped[partition] + rows > 8192,
            "partition {partition}: bucket 0 holds {} groups, within one batch",
            dropped[partition] + rows
        );
    }

    let uncut = Planned::new(Q33_SAMPLES_SQL, sql_config(PARTITIONS, Some(tuning), None)).await;
    let planned_exec = uncut.bucketed();
    let exec = planned_exec
        .downcast_ref::<BucketedFinalAggregateExec>()
        .expect("bucketed");
    assert!(exec.topk().is_some());
    let uncut_exec: Arc<dyn ExecutionPlan> = Arc::new(BucketedFinalAggregateExec::new(
        Arc::clone(exec.aggregate()),
        exec.config(),
    ));
    let started_uncut = first_batches(&uncut, Arc::clone(&uncut_exec)).await;
    let reserved_uncut = uncut.reserved();
    for (partition, (_, rows)) in started_uncut.iter().enumerate() {
        assert_eq!(*rows, 8192, "partition {partition}'s first uncut batch");
    }
    assert_eq!(
        counter_by_partition(&uncut_exec, "topk_rows_dropped", PARTITIONS),
        vec![0; PARTITIONS]
    );

    let bucket_0_rows: usize =
        dropped.iter().sum::<usize>() + started_cut.iter().map(|(_, rows)| rows).sum::<usize>();
    let row_bytes = output_row_bytes(&cut_exec);
    assert_eq!(row_bytes, 24, "ts, v and count(*)");
    let floor = bucket_0_rows * row_bytes;
    eprintln!(
        "reserved at the first batch: cut {reserved_cut}, uncut {reserved_uncut}, \
         floor {floor} ({bucket_0_rows} bucket 0 rows)"
    );
    assert!(
        reserved_uncut >= reserved_cut + floor,
        "the cut released {} bytes, below the {floor} bytes of bucket 0's output",
        reserved_uncut.saturating_sub(reserved_cut)
    );
    drop(started_cut);
    drop(started_uncut);
}
