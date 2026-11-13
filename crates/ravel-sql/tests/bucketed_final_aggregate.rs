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
use datafusion::common::config::SpillCompression;
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
use datafusion::prelude::SessionConfig;
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
    BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX, BucketSpillPlanShape, BucketedFinalAggregateExec,
    BucketedFinalAggregation, DeclaredColumn, DeclaredType, NoTuning, RavelTableProvider,
    SessionTable, SpanSegmentFetcher, SpillDecision, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns, TenantDelegatingPool, TenantMemoryAccountant, build_session,
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
    log_record_in(
        ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        i,
    )
}

/// [`log_record`] with its stream's identity computed by the caller.
fn log_record_in(
    stream_id: ravel_types::logstream::LogStreamId,
    stream_attrs: Vec<u8>,
    i: i64,
) -> LogRecord {
    let mut attrs = vec![
        ("a".to_string(), AttrValue::I64(i)),
        ("b".to_string(), AttrValue::I64(i % 50)),
        ("v".to_string(), AttrValue::I64(i % 1000 - 500)),
    ];
    if i % 7 != 0 {
        attrs.push(("n".to_string(), AttrValue::I64(i)));
    }
    LogRecord {
        stream_id,
        stream_attrs,
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

fn rlog_writer(tenant: &TenantId) -> RlogWriter {
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [5u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    RlogWriter::new(RlogConfig::default(), identity)
}

async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let mut writer = rlog_writer(tenant);
    for r in records {
        writer.push(r.clone()).expect("push record");
    }
    let bytes = writer.finish().expect("finish rlog");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    publish_rlog(
        store,
        tenant,
        bytes::Bytes::from(bytes),
        records.len() as u64,
        (min, max),
    )
    .await;
}

/// Put `bytes`, an RLOG object of `rows` rows whose timestamps span
/// `min..=max`, and publish its commit record.
async fn publish_rlog(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    bytes: bytes::Bytes,
    rows: u64,
    (min, max): (i64, i64),
) {
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(9_3608),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [8u8; 32],
        sample_count: rows,
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
        .put(&data_key, bytes, PutOptions::default())
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
    // The tests drive `partitions` output streams by index and read
    // per-partition metrics, so the final stage must have exactly that many.
    // At the floor they would drive 2 of 16: DataFusion keeps an undriven
    // partition's channel and reservation in its ConsumingInputStreamsState
    // until the plan drops, so a test holding the plan reads those bytes as
    // still reserved (1,837,696 at 16, and the same at 8 streams), and the
    // pool counts 16 bucketed streams at about 19 MB each against 256 MiB.
    config.first_attempt_min_partitions = 0;
    config
}

async fn executor(rows: i64, config: SqlConfig) -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let records: Vec<LogRecord> = (0..rows).map(log_record).collect();
    publish_logs(store.as_ref(), &tenant(), &records).await;
    executor_over(store, config, 1 << 30)
}

/// An executor over `store` with a tenant budget of `max_tenant_bytes`.
fn executor_over(
    store: Arc<dyn ObjectStoreBackend>,
    config: SqlConfig,
    max_tenant_bytes: usize,
) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        config,
        max_tenant_bytes,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())))
}

/// Rows in the retry's large logs fixture: unique keys enough that every
/// final partition holds more than the default engage threshold of 65,536
/// groups at 16 partitions, and that a first attempt refused at nine tenths
/// of its peak derives a tuning of at least one bit at 2, 4 and 16.
const BIG_ROWS: i64 = 3_000_000;

/// The large fixture's RLOG object, written once per test binary.
fn big_rlog() -> bytes::Bytes {
    static RLOG: std::sync::OnceLock<bytes::Bytes> = std::sync::OnceLock::new();
    RLOG.get_or_init(|| {
        let first = log_record(0);
        let mut writer = rlog_writer(&tenant());
        for i in 0..BIG_ROWS {
            writer
                .push(log_record_in(
                    first.stream_id,
                    first.stream_attrs.clone(),
                    i,
                ))
                .expect("push record");
        }
        bytes::Bytes::from(writer.finish().expect("finish rlog"))
    })
    .clone()
}

/// An executor over a fresh store holding the [`BIG_ROWS`] fixture.
async fn big_executor(config: SqlConfig, max_tenant_bytes: usize) -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_rlog(
        store.as_ref(),
        &tenant(),
        big_rlog(),
        BIG_ROWS as u64,
        (1, BIG_ROWS),
    )
    .await;
    executor_over(store, config, max_tenant_bytes)
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
            routed_chunk_bytes: 1 << 20,
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
        routed_chunk_bytes: 1 << 20,
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
        routed_chunk_bytes: 1 << 20,
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
        routed_chunk_bytes: 1 << 20,
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
        routed_chunk_bytes: 1 << 20,
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
        routed_chunk_bytes: 1 << 20,
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
            routed_chunk_bytes: 1 << 20,
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
        routed_chunk_bytes: 1 << 20,
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
        routed_chunk_bytes: 1 << 20,
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
        routed_chunk_bytes: 1 << 20,
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

// ---------------------------------------------------------------------------
// Bucket spill (ADR-3565 amendment 2)
// ---------------------------------------------------------------------------

/// `plan` with every bucketed exec rebuilt to spill.
fn spilling(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(exec) = plan.downcast_ref::<BucketedFinalAggregateExec>() {
        return Arc::new(
            BucketedFinalAggregateExec::new(Arc::clone(exec.aggregate()), exec.config())
                .with_topk(exec.topk().cloned())
                .with_spill(true),
        );
    }
    let children: Vec<Arc<dyn ExecutionPlan>> = plan.children().into_iter().map(spilling).collect();
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

/// A task context over `pool` whose disk manager writes into `dir`, up to
/// `quota` bytes, with the planned session's config as `configure` leaves it.
fn spill_task_ctx(
    planned: &Planned,
    pool: Arc<dyn MemoryPool>,
    dir: &std::path::Path,
    quota: u64,
    configure: impl FnOnce(&mut SessionConfig),
) -> Arc<TaskContext> {
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_disk_manager_builder(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![dir.to_path_buf()]))
                .with_max_temp_directory_size(quota),
        )
        .build_arc()
        .expect("runtime");
    let ctx = Arc::try_unwrap(planned.ctx.task_ctx()).unwrap_or_else(|_| panic!("unshared"));
    let mut config = ctx.session_config().clone();
    configure(&mut config);
    Arc::new(ctx.with_runtime(runtime).with_session_config(config))
}

/// What a spilled run of a plan's bucketed exec reported.
struct SpillRun {
    compression: SpillCompression,
    rows: Vec<String>,
    routed_rows: usize,
    spilled_rows: usize,
    spilled_bytes: usize,
    spill_files: usize,
    bytes_read: usize,
    rows_read: usize,
    batches_written: usize,
    refusals: usize,
    /// Each bucketed consumer's pool calls, in order.
    events: Vec<(String, PoolEvent)>,
}

impl fmt::Debug for SpillRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillRun")
            .field("compression", &self.compression)
            .field("rows", &self.rows.len())
            .field("routed_rows", &self.routed_rows)
            .field("spilled_rows", &self.spilled_rows)
            .field("spilled_bytes", &self.spilled_bytes)
            .field("spill_files", &self.spill_files)
            .field("bytes_read", &self.bytes_read)
            .field("rows_read", &self.rows_read)
            .field("batches_written", &self.batches_written)
            .field("refusals", &self.refusals)
            .field("events", &self.events.len())
            .finish()
    }
}

/// One pool call by a bucketed consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolEvent {
    /// A `try_grow` of this many bytes the pool admitted.
    Grown(usize),
    /// A `try_grow` of this many bytes the pool refused.
    Refused(usize),
    /// An infallible `grow`.
    Forced,
    Shrunk,
}

/// A pool that refuses a bucketed consumer's grow past `cap` bytes, as the
/// spill-mode pool refuses one past its fair share (or with the refusal
/// phrase `phrase` names), and records that consumer's pool calls.
#[derive(Debug)]
struct CappedPool {
    inner: datafusion::execution::memory_pool::UnboundedMemoryPool,
    cap: usize,
    phrase: &'static str,
    refusals: AtomicUsize,
    events: std::sync::Mutex<Vec<(String, PoolEvent)>>,
}

impl CappedPool {
    fn new(cap: usize) -> Self {
        Self::with_phrase(cap, "over its fair share")
    }

    fn with_phrase(cap: usize, phrase: &'static str) -> Self {
        CappedPool {
            inner: Default::default(),
            cap,
            phrase,
            refusals: AtomicUsize::new(0),
            events: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Record `event` when `reservation` is a bucketed consumer's.
    fn record(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        event: PoolEvent,
    ) -> bool {
        let name = reservation.consumer().name();
        if !name.starts_with(BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX) {
            return false;
        }
        self.events
            .lock()
            .expect("events")
            .push((name.to_string(), event));
        true
    }
}

impl fmt::Display for CappedPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "capped({})", self.cap)
    }
}

impl MemoryPool for CappedPool {
    fn name(&self) -> &str {
        "capped"
    }

    fn grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) {
        self.record(reservation, PoolEvent::Forced);
        self.inner.grow(reservation, additional);
    }

    fn shrink(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        shrink: usize,
    ) {
        self.record(reservation, PoolEvent::Shrunk);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) -> DFResult<()> {
        let after = reservation.size() + additional;
        let bucketed = reservation
            .consumer()
            .name()
            .starts_with(BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX);
        if bucketed && after > self.cap {
            self.record(reservation, PoolEvent::Refused(additional));
            self.refusals.fetch_add(1, Ordering::SeqCst);
            return Err(datafusion::error::DataFusionError::ResourcesExhausted(
                format!(
                    "{} {}: {after} > {}",
                    reservation.consumer().name(),
                    self.phrase,
                    self.cap
                ),
            ));
        }
        self.record(reservation, PoolEvent::Grown(additional));
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

async fn run_spilled(planned: &Planned, partitions: usize, cap: usize) -> DFResult<SpillRun> {
    run_spilled_within(planned, partitions, cap, 1 << 30).await
}

/// Run `planned` with spill on, under a pool that caps the bucketed
/// consumers at `cap` and a scratch quota of `quota` bytes, asserting the
/// pool and the scratch directory are back at zero whatever the outcome.
async fn run_spilled_within(
    planned: &Planned,
    partitions: usize,
    cap: usize,
    quota: u64,
) -> DFResult<SpillRun> {
    run_spilled_on(planned, partitions, CappedPool::new(cap), quota, |_| {}).await
}

/// [`run_spilled_within`] under `capped`, with `prepare` called on the task
/// context before the run starts.
async fn run_spilled_on(
    planned: &Planned,
    partitions: usize,
    capped: CappedPool,
    quota: u64,
    prepare: impl FnOnce(&TaskContext),
) -> DFResult<SpillRun> {
    run_spilled_with(planned, partitions, capped, quota, |_| {}, prepare).await
}

/// [`run_spilled_on`] with the session config as `configure` leaves it.
async fn run_spilled_with(
    planned: &Planned,
    partitions: usize,
    capped: CappedPool,
    quota: u64,
    configure: impl FnOnce(&mut SessionConfig),
    prepare: impl FnOnce(&TaskContext),
) -> DFResult<SpillRun> {
    let dir = tempfile::tempdir().expect("scratch dir");
    let capped = Arc::new(capped);
    let pool: Arc<dyn MemoryPool> = Arc::clone(&capped) as Arc<dyn MemoryPool>;
    let plan = spilling(&planned.physical);
    let task_ctx = spill_task_ctx(planned, Arc::clone(&pool), dir.path(), quota, configure);
    let compression = task_ctx.session_config().spill_compression();
    prepare(&task_ctx);
    let result = collect(Arc::clone(&plan), task_ctx).await;
    let exec = find_bucketed(&plan).expect("a bucketed exec");
    // After a failure the other partitions' tasks release the runtime, and
    // with it the disk manager's directory, as the runtime cancels them.
    let left = || std::fs::read_dir(dir.path()).expect("reads").count();
    for _ in 0..500 {
        if pool.reserved() == 0 && left() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(pool.reserved(), 0, "bytes left reserved");
    let left = left();
    assert_eq!(left, 0, "scratch files left under the scratch directory");
    let batches = result?;
    let metrics = exec.metrics().expect("metrics");
    Ok(SpillRun {
        compression,
        rows: render_sorted(&batches),
        routed_rows: counter_by_partition(&exec, "routed_rows", partitions)
            .iter()
            .sum(),
        spilled_rows: metrics.spilled_rows().unwrap_or(0),
        spilled_bytes: metrics.spilled_bytes().unwrap_or(0),
        spill_files: metrics.spill_count().unwrap_or(0),
        bytes_read: counter_by_partition(&exec, ravel_sql::BUCKET_SPILL_BYTES_READ, partitions)
            .iter()
            .sum(),
        rows_read: counter_by_partition(&exec, ravel_sql::BUCKET_SPILL_ROWS_READ, partitions)
            .iter()
            .sum(),
        batches_written: counter_by_partition(
            &exec,
            ravel_sql::BUCKET_SPILL_BATCHES_WRITTEN,
            partitions,
        )
        .iter()
        .sum(),
        refusals: capped.refusals.load(Ordering::SeqCst),
        events: capped.events.lock().expect("events").clone(),
    })
}

/// Where a spilling run's trigger fires, with the tuning and the bucketed
/// consumers' cap that make it fire there on `SAMPLES_SQL` at 2 partitions.
#[derive(Debug, Clone, Copy)]
struct Trigger {
    name: &'static str,
    engage_groups: usize,
    bits: u8,
    cap_at_two: usize,
    /// Whether the cap follows the rows a stream holds. The phase 2 cap must
    /// hold one bucket's table, whose hash allocation grows in steps, so it
    /// stays put.
    scales: bool,
}

/// The table engages at 64 groups; a refusal after that writes the retained
/// chunks of the buckets not yet aggregated.
const PHASE_2: Trigger = Trigger {
    name: "phase 2",
    engage_groups: ENGAGE_GROUPS,
    bits: 5,
    cap_at_two: 512 << 10,
    scales: false,
};

/// The table never reaches its engage threshold; the refusal routes the stock
/// table to the bucket files.
const PHASE_1: Trigger = Trigger {
    name: "phase 1",
    engage_groups: 1 << 20,
    bits: 2,
    cap_at_two: 1 << 20,
    scales: true,
};

const SPILL_CHUNK_ROWS: usize = 1024;

impl Trigger {
    fn tuning(self) -> BucketedFinalAggregation {
        BucketedFinalAggregation {
            engage_groups: self.engage_groups,
            bits: self.bits,
            routed_chunk_rows: SPILL_CHUNK_ROWS,
            routed_chunk_bytes: 1 << 20,
        }
    }

    /// The cap at `partitions` streams, each holding `2 / partitions` of the
    /// rows a stream holds at 2.
    fn cap(self, partitions: usize) -> usize {
        if self.scales {
            self.cap_at_two * 2 / partitions
        } else {
            self.cap_at_two
        }
    }
}

/// Unique keys, and keys NULL on every seventh row.
const NULL_KEYED_SQL: &str = "SELECT CASE WHEN arrow_cast(ts, 'Int64') % 7 = 0 THEN NULL \
                              ELSE arrow_cast(ts, 'Int64') END AS k, count(*) AS c \
                              FROM samples GROUP BY k";

/// The answer of `sql` with the bucketed final aggregate off.
async fn unspilled_rows(sql: &str, partitions: usize) -> Vec<String> {
    unspilled_rows_of(sql, partitions, SAMPLES).await
}

/// The answer of `sql` over `samples` samples with the bucketed final
/// aggregate off.
async fn unspilled_rows_of(sql: &str, partitions: usize, samples: i64) -> Vec<String> {
    let off = Planned::with_samples(sql, sql_config(partitions, None, None), samples).await;
    let batches = collect(Arc::clone(&off.physical), off.ctx.task_ctx())
        .await
        .expect("unspilled run");
    render_sorted(&batches)
}

/// A spilled run of `sql` that the trigger makes spill.
async fn spilled(sql: &str, partitions: usize, trigger: Trigger) -> SpillRun {
    let planned = Planned::new(sql, sql_config(partitions, Some(trigger.tuning()), None)).await;
    spilled_planned(&planned, partitions, trigger).await
}

/// A spilled run of `planned` that the trigger makes spill.
async fn spilled_planned(planned: &Planned, partitions: usize, trigger: Trigger) -> SpillRun {
    let run = run_spilled(planned, partitions, trigger.cap(partitions))
        .await
        .unwrap_or_else(|e| panic!("{} at {partitions} partitions: {e}", trigger.name));
    assert!(
        run.spilled_rows > 0,
        "{} at {partitions} partitions spilled nothing: {run:?}",
        trigger.name
    );
    run
}

/// The trigger with its phase 1 cap scaled by `share`, for a fixture whose
/// stock table holds `share` of the unique fixture's groups.
fn scaled(trigger: Trigger, share: (usize, usize)) -> Trigger {
    Trigger {
        cap_at_two: if trigger.scales {
            trigger.cap_at_two * share.0 / share.1
        } else {
            trigger.cap_at_two
        },
        ..trigger
    }
}

/// The write-once disk bound of ADR-3565 amendment 2, item 2, checked on a
/// run whose routed rows are `row_bytes` wide over `columns` columns, at
/// `buckets` buckets over all streams: every routed row written at most once
/// (a refusal while phase 2 drains writes only the later buckets' chunks),
/// every written row read back once, the bytes written at most
/// `spilled_rows x w` plus validity plus `FRAMING_PER_MESSAGE` per IPC
/// message and per file, and, uncompressed, at least `spilled_rows x w`; the
/// message count at least the file count and at most the routed chunks, the
/// partly filled chunks each refusal and each stream's end write, and, after
/// a phase 1 trigger, the forced slices' pieces; and from 1 to `buckets`
/// files.
fn assert_within_write_once_bound(
    what: &str,
    run: &SpillRun,
    row_bytes: usize,
    columns: usize,
    buckets: usize,
    forced: bool,
) {
    const FRAMING_PER_MESSAGE: usize = 1024;
    assert!(
        run.spilled_rows <= run.routed_rows,
        "{what}: {} rows written for {} routed",
        run.spilled_rows,
        run.routed_rows
    );
    assert_eq!(
        run.rows_read, run.spilled_rows,
        "{what}: rows decoded against rows written"
    );
    let chunks = run.routed_rows.div_ceil(SPILL_CHUNK_ROWS);
    let forced_pieces = if forced { chunks * buckets } else { 0 };
    let message_bound = chunks + buckets * (run.refusals + 1) + forced_pieces;
    assert!(
        run.batches_written <= message_bound,
        "{what}: {} messages over the {message_bound}-message bound ({chunks} chunks, \
         {} refusals, {buckets} buckets)",
        run.batches_written,
        run.refusals
    );
    let validity = run.spilled_rows.div_ceil(8) * columns;
    let framed = run.batches_written + run.spill_files;
    let bound = run.spilled_rows * row_bytes + validity + framed * FRAMING_PER_MESSAGE;
    eprintln!(
        "{what}: {} bytes in {} batches and {} files, {} column bytes, bound {bound}",
        run.spilled_bytes,
        run.batches_written,
        run.spill_files,
        run.spilled_rows * row_bytes
    );
    assert!(
        run.spilled_bytes <= bound,
        "{what}: {} bytes written over the {bound}-byte bound",
        run.spilled_bytes
    );
    // lz4_frame shrinks a buffer below its column bytes; uncompressed IPC
    // cannot.
    if run.compression == SpillCompression::Uncompressed {
        assert!(
            run.spilled_bytes >= run.spilled_rows * row_bytes,
            "{what}: {} bytes written under the {} column bytes",
            run.spilled_bytes,
            run.spilled_rows * row_bytes
        );
    }
    assert!(
        (1..=buckets).contains(&run.spill_files),
        "{what}: {} files for {buckets} buckets, over 2^bits per stream",
        run.spill_files
    );
    // A file is created by its first message.
    assert!(
        run.batches_written >= run.spill_files,
        "{what}: {} messages for {} files",
        run.batches_written,
        run.spill_files
    );
}

#[tokio::test]
async fn bucket_spill_matches_in_memory_answer() {
    // The NULL-keyed fixture's stock table holds 6/7 of the unique fixture's
    // groups, so the phase 1 share is scaled to match.
    for (sql, share) in [(SAMPLES_SQL, (1, 1)), (NULL_KEYED_SQL, (6, 7))] {
        for partitions in [2, 4, 16] {
            let expected = unspilled_rows(sql, partitions).await;
            for trigger in [PHASE_2, PHASE_1] {
                let trigger = scaled(trigger, share);
                let run = spilled(sql, partitions, trigger).await;
                assert_eq!(
                    run.rows, expected,
                    "{sql}: {} at {partitions} partitions",
                    trigger.name
                );
            }
        }
    }
}

#[tokio::test]
async fn bucket_spill_writes_each_routed_row_once() {
    for trigger in [PHASE_2, PHASE_1] {
        let run = spilled(SAMPLES_SQL, 2, trigger).await;
        assert_eq!(run.rows.len(), SAMPLES as usize, "{}", trigger.name);
        assert_eq!(
            run.routed_rows, SAMPLES as usize,
            "{}: every unique key reaches the files",
            trigger.name
        );
        assert_eq!(
            run.spilled_rows, run.routed_rows,
            "{}: rows written against rows routed",
            trigger.name
        );
    }
}

/// `w` is 16 here, a timestamp key and an `Int64` count, neither ever NULL,
/// so the rows decoded on read-back are exactly 16 bytes each, under the
/// lz4_frame compression the spill retry sets as under none, while the file
/// bytes written under lz4_frame are not.
#[tokio::test]
async fn bucket_spill_disk_within_write_once_bound() {
    const PARTITIONS: usize = 2;
    for compression in [
        SpillCompression::Uncompressed,
        SpillDecision::SPILL_COMPRESSION,
    ] {
        for trigger in [PHASE_2, PHASE_1] {
            let what = format!("{} {compression:?}", trigger.name);
            let planned = Planned::new(
                SAMPLES_SQL,
                sql_config(PARTITIONS, Some(trigger.tuning()), None),
            )
            .await;
            let run = run_spilled_with(
                &planned,
                PARTITIONS,
                CappedPool::new(trigger.cap(PARTITIONS)),
                1 << 30,
                |config| config.options_mut().execution.spill_compression = compression,
                |_| {},
            )
            .await
            .unwrap_or_else(|e| panic!("{what}: {e}"));
            assert!(run.spilled_rows > 0, "{what} spilled nothing: {run:?}");
            assert_eq!(run.spilled_rows, run.routed_rows, "{what}");
            assert_within_write_once_bound(
                &what,
                &run,
                16,
                2,
                PARTITIONS << trigger.bits,
                trigger.scales,
            );
            assert_eq!(
                run.bytes_read,
                run.rows_read * 16,
                "{what}: decoded bytes against rows decoded"
            );
            if compression == SpillDecision::SPILL_COMPRESSION {
                assert!(
                    run.spilled_bytes < run.bytes_read,
                    "{what}: {} file bytes for {} decoded",
                    run.spilled_bytes,
                    run.bytes_read
                );
            }
        }
    }
}

/// A spill-on exec records at execute time whether its disk manager allows
/// temp files, which is what the spill retry's report reads: with the disk
/// manager disabled it ran without spill and says so.
#[tokio::test]
async fn bucket_spill_records_a_disabled_disk_manager() {
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    let expected = unspilled_rows(SAMPLES_SQL, 2).await;
    for (mode, available) in [
        (DiskManagerMode::Disabled, false),
        (DiskManagerMode::OsTmpDirectory, true),
    ] {
        let planned = Planned::new(SAMPLES_SQL, sql_config(2, Some(PHASE_2.tuning()), None)).await;
        let plan = spilling(&planned.physical);
        let exec = find_bucketed(&plan).expect("a bucketed exec");
        let bucketed = exec
            .downcast_ref::<BucketedFinalAggregateExec>()
            .expect("bucketed");
        assert!(bucketed.spill());
        assert!(!bucketed.spill_available(), "before execute");
        let runtime = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(mode))
            .build_arc()
            .expect("runtime");
        let ctx = Arc::try_unwrap(planned.ctx.task_ctx()).unwrap_or_else(|_| panic!("unshared"));
        let batches = collect(Arc::clone(&plan), Arc::new(ctx.with_runtime(runtime)))
            .await
            .expect("answers");
        assert_eq!(render_sorted(&batches), expected);
        assert_eq!(bucketed.spill_available(), available, "{available}");
    }
}

/// The phase 1 trigger routes the stock table under the charge it already
/// holds, and sizes the router's scratch once inside it. At a cap that
/// refuses each stream's table with more than a batch of its input still to
/// come, every bucketed consumer's first refusal is
/// followed by the recomputed charge's shrink, and from there to the end of
/// its input the stream asks the pool for routed chunks and nothing else (a
/// scratch sized per batch would be a grow of another size), over more
/// chunks than one batch of the session's `batch_size` rows can open.
#[tokio::test]
async fn bucket_spill_forced_route_asks_nothing() {
    const BATCH_ROWS: usize = 8192;
    const CAP: usize = 640 << 10;
    const SAMPLES_HERE: i64 = 4 * SAMPLES;
    let tuning = BucketedFinalAggregation {
        bits: 5,
        ..PHASE_1.tuning()
    };
    let planned =
        Planned::with_samples(SAMPLES_SQL, sql_config(2, Some(tuning), None), SAMPLES_HERE).await;
    let run = run_spilled(&planned, 2, CAP)
        .await
        .expect("spills and answers");
    assert!(run.spilled_rows > 0, "nothing spilled: {run:?}");
    assert_eq!(
        run.rows,
        unspilled_rows_of(SAMPLES_SQL, 2, SAMPLES_HERE).await
    );
    let chunk = ravel_sql::routed_chunk_charge(&[8, 8], SPILL_CHUNK_ROWS).expect("fits");
    let one_batch = BATCH_ROWS.div_ceil(SPILL_CHUNK_ROWS) + (1 << tuning.bits);
    let mut by_consumer: std::collections::BTreeMap<&str, Vec<PoolEvent>> = Default::default();
    for (name, event) in &run.events {
        by_consumer.entry(name).or_default().push(*event);
    }
    assert_eq!(by_consumer.len(), 2, "{run:?}");
    for (name, events) in &by_consumer {
        let at = events
            .iter()
            .position(|e| matches!(e, PoolEvent::Refused(_)))
            .unwrap_or_else(|| panic!("{name} was never refused: {events:?}"));
        assert_eq!(
            events.get(at + 1),
            Some(&PoolEvent::Shrunk),
            "{name}: the forced route asked the pool: {:?}",
            &events[at..(at + 4).min(events.len())]
        );
        // The drain's tables and read-back batches follow the last chunk.
        let asks: Vec<usize> = events[at + 1..]
            .iter()
            .filter_map(|e| match e {
                PoolEvent::Grown(bytes) | PoolEvent::Refused(bytes) => Some(*bytes),
                _ => None,
            })
            .collect();
        let routed = asks
            .iter()
            .rposition(|bytes| *bytes == chunk)
            .map_or(&asks[..0], |last| &asks[..=last]);
        assert!(
            routed.iter().all(|bytes| *bytes == chunk),
            "{name} asked for more than {chunk}-byte chunks after its forced route: {routed:?}"
        );
        assert!(
            routed.len() > one_batch,
            "{name}: {} chunk asks after the forced route, one batch opens up to {one_batch}",
            routed.len()
        );
    }
}

/// At a quarter of the phase 2 share the stream runs out of routed state to
/// write while its groups keep arriving.
#[tokio::test]
async fn bucket_spill_late_groups_fail_typed() {
    let planned = Planned::new(SAMPLES_SQL, sql_config(2, Some(PHASE_2.tuning()), None)).await;
    let err = run_spilled(&planned, 2, PHASE_2.cap(2) / 4)
        .await
        .expect_err("the under-shoot fails the stream");
    let root = err.find_root();
    assert!(
        matches!(
            root,
            datafusion::error::DataFusionError::ResourcesExhausted(_)
        ),
        "not ResourcesExhausted: {err}"
    );
    assert!(
        err.to_string().contains(ravel_sql::BUCKET_SPILL_UNDERSHOOT),
        "untyped under-shoot: {err}"
    );
}

#[tokio::test]
async fn bucket_spill_quota_is_typed() {
    let planned = Planned::new(SAMPLES_SQL, sql_config(2, Some(PHASE_2.tuning()), None)).await;
    let err = run_spilled_within(&planned, 2, PHASE_2.cap(2), 64 << 10)
        .await
        .expect_err("the quota refuses a write");
    // The disk manager's refusal reaches the stream through the IPC writer.
    assert!(
        matches!(
            err.find_root(),
            datafusion::error::DataFusionError::ArrowError(inner, _)
                if matches!(inner.as_ref(), datafusion::arrow::error::ArrowError::IoError(..))
        ),
        "not the IPC writer's I/O error: {err:?}"
    );
    assert!(
        err.to_string().contains("exceeded the allowable limit"),
        "not the disk manager's quota refusal: {err}"
    );
    assert!(
        !err.to_string().contains(ravel_sql::BUCKET_SPILL_UNDERSHOOT),
        "a quota refusal labelled as an under-shoot: {err}"
    );
}

/// A scratch directory the stream cannot create its bucket files in fails
/// the query with the I/O error, and leaves nothing reserved or on disk.
#[tokio::test]
async fn bucket_spill_write_fault_is_typed() {
    use std::os::unix::fs::PermissionsExt;
    let planned = Planned::new(SAMPLES_SQL, sql_config(2, Some(PHASE_2.tuning()), None)).await;
    let locked = std::sync::Mutex::new(Vec::new());
    let result = run_spilled_on(
        &planned,
        2,
        CappedPool::new(PHASE_2.cap(2)),
        1 << 30,
        |ctx| {
            for scratch in ctx.runtime_env().disk_manager.temp_dir_paths() {
                std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o500))
                    .expect("locks the scratch directory");
                locked.lock().expect("locked").push(scratch);
            }
        },
    )
    .await;
    let locked = locked.into_inner().expect("locked");
    assert!(!locked.is_empty(), "the disk manager made no directory");
    let err = result.expect_err("the bucket file cannot be created");
    assert!(
        matches!(
            err.find_root(),
            datafusion::error::DataFusionError::IoError(_)
        ),
        "not the I/O error: {err:?}"
    );
}

/// The under-shoot label goes on a refusal over the spill-mode cap or a
/// fair share of it, and on no other refusal: a refusal over the hard bound
/// or of an exhausted pool reaches the caller unchanged.
#[tokio::test]
async fn bucket_spill_undershoot_labels_share_refusals_only() {
    for (phrase, labelled) in [
        ("over its fair share", true),
        ("over the spill-mode cap", true),
        ("over the spill-mode hard bound", false),
        ("query memory pool exhausted", false),
    ] {
        let planned = Planned::new(SAMPLES_SQL, sql_config(2, Some(PHASE_2.tuning()), None)).await;
        let err = run_spilled_on(
            &planned,
            2,
            CappedPool::with_phrase(PHASE_2.cap(2) / 4, phrase),
            1 << 30,
            |_| {},
        )
        .await
        .expect_err("the stream runs out of routed state");
        assert!(
            matches!(
                err.find_root(),
                datafusion::error::DataFusionError::ResourcesExhausted(_)
            ),
            "{phrase}: not ResourcesExhausted: {err:?}"
        );
        let message = err.to_string();
        assert!(message.contains(phrase), "{phrase}: {message}");
        assert_eq!(
            message.contains(ravel_sql::BUCKET_SPILL_UNDERSHOOT),
            labelled,
            "{phrase}: {message}"
        );
    }
}

/// A refusal over the hard bound still spills: a small stock table routed
/// at the phase 1 trigger answers as the in-memory run does.
#[tokio::test]
async fn bucket_spill_hard_bound_refusal_still_answers() {
    let expected = unspilled_rows(SAMPLES_SQL, 2).await;
    let planned = Planned::new(SAMPLES_SQL, sql_config(2, Some(PHASE_1.tuning()), None)).await;
    let run = run_spilled_on(
        &planned,
        2,
        CappedPool::with_phrase(PHASE_1.cap(2), "over the spill-mode hard bound"),
        1 << 30,
        |_| {},
    )
    .await
    .expect("the hard-bound refusal is relieved");
    assert!(run.spilled_rows > 0, "nothing spilled: {run:?}");
    assert_eq!(run.rows, expected);
}

/// A refusal while phase 2 drains writes the chunks of the buckets after the
/// one being aggregated, never that bucket's own: with every routed chunk
/// still in memory at the end of input, a cap between the ingest's peak and
/// the drain's spills some rows but not all, and the answer is unchanged.
#[tokio::test]
async fn bucket_spill_drain_refusal_spills_later_buckets() {
    const PARTITIONS: usize = 2;
    let tuning = BucketedFinalAggregation {
        engage_groups: ENGAGE_GROUPS,
        bits: 2,
        routed_chunk_rows: 256,
        routed_chunk_bytes: 1 << 20,
    };
    let expected = unspilled_rows(SAMPLES_SQL, PARTITIONS).await;
    // Spilling nothing is too loose; spilling every routed row, or failing,
    // means the ingest was refused, which is too tight.
    let (mut tight, mut loose) = (16usize << 10, 64usize << 20);
    let mut found = None;
    for _ in 0..24 {
        let cap = tight + (loose - tight) / 2;
        let planned = Planned::new(SAMPLES_SQL, sql_config(PARTITIONS, Some(tuning), None)).await;
        match run_spilled(&planned, PARTITIONS, cap).await {
            Ok(run) if run.spilled_rows == 0 => loose = cap,
            Ok(run) if run.spilled_rows < run.routed_rows => {
                found = Some((cap, run));
                break;
            }
            _ => tight = cap,
        }
    }
    let (cap, run) = found.unwrap_or_else(|| {
        panic!("no cap between {tight} and {loose} spills during the drain only")
    });
    eprintln!("drain refusal at cap {cap}: {run:?}");
    assert!(run.refusals > 0, "never refused: {run:?}");
    assert_eq!(
        run.rows_read, run.spilled_rows,
        "rows decoded against written"
    );
    assert_eq!(run.rows, expected, "at cap {cap}");
}

/// Each bucket's file is deleted once it is read back, before the bucket's
/// output is returned: the disk manager's count falls with every bucket and
/// is zero at the last batch.
#[tokio::test]
async fn bucket_spill_deletes_each_file_after_reading_it() {
    const PARTITIONS: usize = 2;
    let planned = Planned::new(
        SAMPLES_SQL,
        sql_config(PARTITIONS, Some(PHASE_2.tuning()), None),
    )
    .await;
    let dir = tempfile::tempdir().expect("scratch dir");
    let pool: Arc<dyn MemoryPool> = Arc::new(CappedPool::new(PHASE_2.cap(PARTITIONS)));
    let plan = spilling(&planned.physical);
    let task_ctx = spill_task_ctx(&planned, Arc::clone(&pool), dir.path(), 1 << 30, |_| {});
    let disk = Arc::clone(&task_ctx.runtime_env().disk_manager);
    let exec = find_bucketed(&plan).expect("a bucketed exec");
    // Every stream past its input, so nothing is written from here on.
    let firsts = (0..PARTITIONS).map(|partition| {
        let exec = Arc::clone(&exec);
        let task_ctx = Arc::clone(&task_ctx);
        async move {
            let mut stream = exec.execute(partition, task_ctx).expect("executes");
            stream.next().await.expect("a first batch").expect("batch");
            stream
        }
    });
    let started = futures::future::join_all(firsts).await;
    let mut used = vec![disk.used_disk_space()];
    let mut ends = Vec::new();
    for mut stream in started {
        while let Some(batch) = stream.next().await {
            batch.expect("batch");
            used.push(disk.used_disk_space());
        }
        ends.push(used.len() - 1);
    }
    let files = exec
        .metrics()
        .and_then(|metrics| metrics.spill_count())
        .unwrap_or(0);
    assert!(files > PARTITIONS, "{files} bucket files");
    assert!(
        used.windows(2).all(|pair| pair[1] <= pair[0]),
        "scratch bytes grew during the drain: {used:?}"
    );
    assert!(
        used[0] > used[ends[0]] && used[ends[0]] > 0,
        "the first stream's files outlived its drain: {used:?} (ends {ends:?})"
    );
    assert_eq!(
        used.last(),
        Some(&0),
        "bucket files left at the last batch: {used:?}"
    );
}

/// The derivation's plan inputs come from the planned statement: `P`, `C`,
/// the routed widths, and the logs scan's exact `N`. A plan with no bucketed
/// exec has no one scan to read, and a bucketed exec over a metrics scan has
/// no `LogsScanExec` to count `N` and `n` on.
#[tokio::test]
async fn bucket_spill_plan_shape_reads_the_planned_statement() {
    const PARTITIONS: usize = 2;
    const SQL: &str = "SELECT ts, count(*) FROM logs GROUP BY ts";
    let on = executor(ROWS, sql_config(PARTITIONS, Some(PHASE_2.tuning()), None)).await;
    let plan = physical_plan_tree(&on, SQL).await;
    let shape = BucketSpillPlanShape::read(&plan).expect("a shape");
    assert_eq!(
        shape,
        BucketSpillPlanShape {
            partitions: PARTITIONS,
            // Two bucketed streams, two exchange outputs, one partial stream
            // over the one scan partition.
            spill_consumers: 5,
            column_widths: vec![8, 8],
            variable_width: false,
            total_rows: Some(ROWS as u64),
        },
        "{}",
        displayable(plan.as_ref()).indent(true)
    );
    let off = executor(ROWS, sql_config(PARTITIONS, None, None)).await;
    assert_eq!(
        BucketSpillPlanShape::read(&physical_plan_tree(&off, SQL).await),
        Err(NoTuning::NotOneScan)
    );
    let samples = Planned::new(
        SAMPLES_SQL,
        sql_config(PARTITIONS, Some(PHASE_2.tuning()), None),
    )
    .await;
    assert_eq!(
        BucketSpillPlanShape::read(&samples.physical),
        Err(NoTuning::NotLogsScan)
    );
}

/// Dropping the streams with bucket files on disk returns the pool, the
/// disk manager's count and the scratch directory to zero.
#[tokio::test]
async fn bucket_spill_cancel_releases_share_and_scratch() {
    const PARTITIONS: usize = 2;
    let planned = Planned::new(
        SAMPLES_SQL,
        sql_config(PARTITIONS, Some(PHASE_2.tuning()), None),
    )
    .await;
    let dir = tempfile::tempdir().expect("scratch dir");
    let pool: Arc<dyn MemoryPool> = Arc::new(CappedPool::new(PHASE_2.cap(PARTITIONS)));
    let plan = spilling(&planned.physical);
    let task_ctx = spill_task_ctx(&planned, Arc::clone(&pool), dir.path(), 1 << 30, |_| {});
    let exec = find_bucketed(&plan).expect("a bucketed exec");
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
    let disk = Arc::clone(&task_ctx.runtime_env().disk_manager);
    assert!(
        disk.used_disk_space() > 0,
        "no bucket file on disk mid-drain"
    );
    assert!(pool.reserved() > 0, "nothing reserved mid-drain");

    drop(started);
    for _ in 0..500 {
        if pool.reserved() == 0 && disk.used_disk_space() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(pool.reserved(), 0, "bytes left reserved after the drop");
    assert_eq!(disk.used_disk_space(), 0, "scratch bytes left counted");
    for scratch in disk.temp_dir_paths() {
        let left = std::fs::read_dir(&scratch).map_or(0, |entries| entries.count());
        assert_eq!(left, 0, "files left under {}", scratch.display());
    }
}

/// The q33 shape with ties: `(k, v)` is 40,000 groups, `v` following `k`,
/// with a count of 2 for `k < 24000` and 1 for the rest, so `LIMIT 10` cuts
/// inside the 24,000 groups tied at 2.
const Q33_TIES_SQL: &str = "SELECT k, v, count(*) AS c FROM (SELECT arrow_cast(ts, 'Int64') % \
                            40000 AS k, CAST(value AS BIGINT) % 4 AS v FROM samples) \
                            GROUP BY k, v ORDER BY c DESC LIMIT 10";

#[tokio::test]
async fn bucket_spill_q33_ties_match_in_memory_answer() {
    const PARTITIONS: usize = 2;
    let in_memory = unspilled_rows(Q33_TIES_SQL, PARTITIONS).await;
    let counts = |rows: &[String]| -> Vec<i64> {
        let mut counts: Vec<i64> = rows
            .iter()
            .map(|row| {
                let cells: Vec<i64> = row
                    .split('|')
                    .map(|cell| cell.parse().expect("an integer"))
                    .collect();
                assert_eq!(cells.len(), 3, "{row}");
                assert!(cells[0] < 24000, "{row}: not among the groups tied at 2");
                assert_eq!(cells[1], cells[0] % 4, "{row}");
                assert_eq!(cells[2], 2, "{row}: not the group's whole count");
                cells[2]
            })
            .collect();
        counts.sort_unstable();
        counts
    };
    let expected = counts(&in_memory);
    assert_eq!(expected.len(), 10);
    for trigger in [PHASE_2, PHASE_1] {
        // Sixteen buckets keep each drained bucket's table of this wider key
        // inside the scaled phase 1 share.
        let trigger = Trigger {
            bits: trigger.bits.max(4),
            ..scaled(trigger, (3, 4))
        };
        let run = spilled(Q33_TIES_SQL, PARTITIONS, trigger).await;
        assert_eq!(counts(&run.rows), expected, "{}", trigger.name);
        let mut keys: Vec<&str> = run.rows.iter().map(String::as_str).collect();
        keys.dedup();
        assert_eq!(keys.len(), 10, "{}: a group returned twice", trigger.name);
        assert_eq!(run.spilled_rows, run.routed_rows, "{}", trigger.name);
        assert_within_write_once_bound(
            trigger.name,
            &run,
            24,
            3,
            PARTITIONS << trigger.bits,
            trigger.scales,
        );
    }
}

/// Keys NULL on every row: one group, which no spill can split.
const ALL_NULL_SQL: &str = "SELECT CASE WHEN arrow_cast(ts, 'Int64') < 0 THEN 1 END AS k, \
                            count(*) AS c FROM samples GROUP BY k";

/// One non-NULL group.
const SINGLE_GROUP_SQL: &str = "SELECT arrow_cast(ts, 'Int64') / 1000000 AS k, count(*) AS c \
                                FROM samples GROUP BY k";

/// 90% of the rows on key 0, every tenth row on a unique key.
const SKEW_90_SQL: &str = "SELECT CASE WHEN t % 10 < 9 THEN 0 ELSE t END AS k, count(*) AS c \
                           FROM (SELECT arrow_cast(ts, 'Int64') AS t FROM samples) GROUP BY k";

#[tokio::test]
async fn bucket_spill_single_group_shapes_match_in_memory_answer() {
    for sql in [ALL_NULL_SQL, SINGLE_GROUP_SQL] {
        let expected = unspilled_rows(sql, 2).await;
        assert_eq!(expected.len(), 1, "{sql}");
        for trigger in [PHASE_2, PHASE_1] {
            let planned = Planned::new(sql, sql_config(2, Some(trigger.tuning()), None)).await;
            let run = run_spilled(&planned, 2, trigger.cap(2))
                .await
                .unwrap_or_else(|e| panic!("{sql}: {}: {e}", trigger.name));
            assert_eq!(run.rows, expected, "{sql}: {}", trigger.name);
        }
    }
}

#[tokio::test]
async fn bucket_spill_skewed_keys_match_in_memory_answer() {
    const SKEW_SAMPLES: i64 = 10 * SAMPLES;
    let expected = unspilled_rows_of(SKEW_90_SQL, 2, SKEW_SAMPLES).await;
    assert_eq!(expected.len(), SAMPLES as usize + 1);
    for trigger in [PHASE_2, PHASE_1] {
        let planned = Planned::with_samples(
            SKEW_90_SQL,
            sql_config(2, Some(trigger.tuning()), None),
            SKEW_SAMPLES,
        )
        .await;
        let run = spilled_planned(&planned, 2, trigger).await;
        assert_eq!(run.rows, expected, "{}", trigger.name);
    }
}

/// A filtered logs scan counts the rows it consumed, before its predicate,
/// not the rows it returned: the bucket spill's tuning projects on input
/// rows.
#[tokio::test]
async fn logs_scan_counts_rows_consumed_before_its_filter() {
    const ROWS: i64 = 6000;
    let executor = executor(ROWS, sql_config(2, None, None)).await;
    let plan = physical_plan_tree(&executor, "SELECT a FROM logs WHERE b = 3").await;
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("runs");
    let returned: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(returned, (ROWS / 50) as usize);
    fn scans(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<usize>) {
        if let Some(scan) = plan.downcast_ref::<ravel_sql::LogsScanExec>() {
            out.push(scan.rows_consumed());
        }
        for child in plan.children() {
            scans(child, out);
        }
    }
    let mut consumed = Vec::new();
    scans(&plan, &mut consumed);
    assert_eq!(
        consumed,
        vec![ROWS as usize],
        "{}",
        displayable(plan.as_ref()).indent(false)
    );
}

/// One random key shape: a fixed-width key of `key_type`, NULL on every
/// `null_every`-th row when set, `skew` percent of the rows on `hot`, the
/// rest on `(t x mult) % modulus`.
#[derive(Debug, Clone)]
struct KeyCase {
    key_type: (&'static str, usize),
    null_every: Option<i64>,
    skew: i64,
    hot: i64,
    mult: i64,
    modulus: i64,
    partitions: usize,
}

impl KeyCase {
    fn sql(&self) -> String {
        let nulls = self
            .null_every
            .map(|every| format!("WHEN t % {every} = 0 THEN NULL "))
            .unwrap_or_default();
        format!(
            "SELECT arrow_cast(CASE {nulls}WHEN t % 100 < {} THEN {} ELSE (t * {}) % {} END, \
             '{}') AS k, count(*) AS c FROM (SELECT arrow_cast(ts, 'Int64') AS t FROM samples) \
             GROUP BY k",
            self.skew, self.hot, self.mult, self.modulus, self.key_type.0
        )
    }
}

fn arb_key_case() -> impl proptest::strategy::Strategy<Value = KeyCase> {
    use proptest::prelude::*;
    (
        prop_oneof![Just(("Int16", 2)), Just(("Int32", 4)), Just(("Int64", 8))],
        prop_oneof![Just(None), (2i64..14).prop_map(Some)],
        prop_oneof![Just(0i64), 1i64..=90],
        0i64..30_000,
        1i64..10,
        2_000i64..30_000,
        prop_oneof![Just(2usize), Just(4)],
    )
        .prop_map(
            |(key_type, null_every, skew, hot, mult, modulus, partitions)| KeyCase {
                key_type,
                null_every,
                skew,
                hot,
                mult,
                modulus,
                partitions,
            },
        )
}

/// Batches of this many rows keep a batch's table working set small beside
/// the drain's share.
const KEY_CASE_BATCH_ROWS: usize = 1024;

/// The phase 2 cap of every key case: it holds one bucket's table while the
/// stream drains, and is under the charge of one open
/// `KEY_CASE_CHUNK_ROWS`-row chunk per bucket (32 x 41,984 bytes for an
/// `Int16` key), so routing alone is refused whatever the case's groups.
const KEY_CASE_CAP: usize = 1 << 20;
const KEY_CASE_CHUNK_ROWS: usize = 4 * SPILL_CHUNK_ROWS;

/// The phase 2 spilled answer for `case` is the in-memory answer, within the
/// write-once disk bound, and the case spilled.
async fn check_key_case(case: KeyCase) {
    let sql = case.sql();
    let expected = unspilled_rows(&sql, case.partitions).await;
    let tuning = BucketedFinalAggregation {
        routed_chunk_rows: KEY_CASE_CHUNK_ROWS,
        ..PHASE_2.tuning()
    };
    let open_chunks = ravel_sql::routed_chunk_charge(&[case.key_type.1, 8], KEY_CASE_CHUNK_ROWS)
        .expect("fits")
        << tuning.bits;
    assert!(open_chunks > KEY_CASE_CAP, "{open_chunks}");
    let planned = Planned::new(&sql, sql_config(case.partitions, Some(tuning), None)).await;
    let run = run_spilled_with(
        &planned,
        case.partitions,
        CappedPool::new(KEY_CASE_CAP),
        1 << 30,
        |config| *config = std::mem::take(config).with_batch_size(KEY_CASE_BATCH_ROWS),
        |_| {},
    )
    .await
    .unwrap_or_else(|e| panic!("{sql} ({case:?}): {e}"));
    assert_eq!(run.rows, expected, "{sql} ({case:?})");
    assert!(
        run.spilled_rows > 0,
        "{sql} ({case:?}) spilled nothing: {run:?}"
    );
    assert_within_write_once_bound(
        &sql,
        &run,
        case.key_type.1 + 8,
        2,
        case.partitions << tuning.bits,
        false,
    );
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(12))]

    /// Random fixed-width keys, NULL placement, skew and partition counts,
    /// each spilled through phase 2: the spilled answer is the in-memory
    /// answer and the disk stays within the write-once bound.
    #[test]
    fn bucket_spill_random_keys_match_in_memory_answer(case in arb_key_case()) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime builds")
            .block_on(check_key_case(case));
    }
}

// ---------------------------------------------------------------------------
// The refusal retry (issue #4075)
// ---------------------------------------------------------------------------

/// The final aggregate's partitions on the retry fixtures below unless they
/// say otherwise.
const RETRY_PARTITIONS: usize = 2;

/// The per-query limit `L` on the [`BIG_ROWS`] fixture, above its unlimited
/// peak at every partition count the tests use.
const BIG_QUERY_BYTES: usize = 1 << 30;

/// The logs fixture's [`sql_config`] at `partitions` with spill on and the
/// bucketed rule off, so whatever plans buckets on the retry is the retry's
/// own rule: a per-query limit of `query_bytes`, scratch at `scratch`.
fn retry_config(scratch: &std::path::Path, partitions: usize, query_bytes: usize) -> SqlConfig {
    let mut config = sql_config(partitions, None, None);
    config.max_query_bytes = query_bytes;
    config.spill = Some(ravel_sql::SpillConfig {
        dir: scratch.to_path_buf(),
        max_bytes: 1 << 30,
        merge_fan_in: ravel_sql::DEFAULT_SPILL_MERGE_FAN_IN,
    });
    config
}

/// What one statement run reported about its spill retry.
struct RetryRun {
    /// The result rows in result order.
    rows: Vec<String>,
    retries: u32,
    report: Option<ravel_sql::SpillRetryReport>,
    plan: Option<String>,
    peak_pool_bytes: u64,
    /// [`ravel_sql::SqlOutcome::peak_asked_intermediate_bytes`].
    peak_asked: u64,
}

async fn retry_run(executor: &SqlExecutor, sql: &str) -> Result<RetryRun, String> {
    let outcome = executor
        .execute(tenant().hash(), &request(sql))
        .await
        .map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for batch in outcome.output.batches() {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| array_value_to_string(column, row).expect("renders"))
                .collect();
            rows.push(cells.join("|"));
        }
    }
    Ok(RetryRun {
        rows,
        retries: outcome.stats.spill_retries,
        report: outcome.stats.spill_retry,
        plan: outcome.stats.spill_retry_plan,
        peak_pool_bytes: outcome.accounting.peak_intermediate_bytes,
        peak_asked: outcome.peak_asked_intermediate_bytes,
    })
}

/// Every group ties on its count of 1, so which ten rows answer is decided by
/// the tie order alone.
const RETRY_TIES_SQL: &str =
    "SELECT a, count(*) AS c FROM logs GROUP BY a ORDER BY c DESC LIMIT 10";

/// Every group of a unique fixed-width key, the whole answer, compared as a
/// set: a full sort above the aggregate asks more than this fixture's
/// budgets hold for its own merge reservation.
const RETRY_GROUPS_SQL: &str = "SELECT a, count(*) AS c FROM logs GROUP BY a";

/// A string key, which the bucketed rule does not admit.
const RETRY_STRING_SQL: &str = "SELECT arrow_cast(a, 'Utf8') AS k, count(*) AS c FROM logs \
                                GROUP BY k ORDER BY c DESC LIMIT 10";

/// How a refused run's first attempt is made to refuse: a process budget of
/// `tenths` of the unlimited peak, with the executor's descriptor limit at
/// `nofile_soft` (the default when `None`), over `partitions` final
/// partitions of the [`BIG_ROWS`] fixture when `big`, else of [`ROWS`].
#[derive(Clone, Copy)]
struct Refusal {
    tenths: u64,
    nofile_soft: Option<u64>,
    partitions: usize,
    big: bool,
}

/// An executor over `refusal`'s fixture under `config`.
async fn retry_executor(refusal: Refusal, config: SqlConfig) -> SqlExecutor {
    if refusal.big {
        big_executor(config, 1 << 30).await
    } else {
        executor(ROWS, config).await
    }
}

/// A statement's unlimited run, the same statement refused as `refusal`
/// says (its result: the run, or the error it ended with), and the INFO log
/// the refused run wrote.
async fn refused_run(sql: &str, refusal: Refusal) -> (RetryRun, Result<RetryRun, String>, String) {
    let scratch = tempfile::tempdir().expect("scratch root");
    let query_bytes = if refusal.big {
        BIG_QUERY_BYTES
    } else {
        ravel_sql::DEFAULT_MAX_QUERY_BYTES
    };
    let config = || retry_config(scratch.path(), refusal.partitions, query_bytes);
    let unlimited = retry_run(&retry_executor(refusal, config()).await, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(
        unlimited.retries, 0,
        "{sql}: unlimited, the first attempt answers"
    );
    let budget = unlimited.peak_pool_bytes * refusal.tenths / 10;
    let mut refused = retry_executor(refusal, config())
        .await
        .with_process_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(budget)));
    if let Some(nofile_soft) = refusal.nofile_soft {
        refused = refused.with_nofile_soft(nofile_soft);
    }
    let log = crate::spill::CapturedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(log.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    let run = {
        let _guard = tracing::subscriber::set_default(subscriber);
        retry_run(&refused, sql).await
    };
    (unlimited, run, log.text())
}

/// [`refused_run`], which must answer on its one spill retry.
async fn answered_retry(sql: &str, refusal: Refusal) -> (RetryRun, RetryRun, String) {
    let (unlimited, run, log) = refused_run(sql, refusal).await;
    let run = run.unwrap_or_else(|e| panic!("{sql}: the retry answers: {e}\n{log}"));
    assert_eq!(run.retries, 1, "{sql}: answered on its spill retry");
    (unlimited, run, log)
}

/// The bucket retry's refusal: nine tenths of the [`BIG_ROWS`] fixture's
/// unlimited peak at `partitions`, with a descriptor limit a server raises
/// its soft limit to, since 16 partitions need more than Linux's default.
const fn bucket_refusal(partitions: usize) -> Refusal {
    Refusal {
        tenths: 9,
        nofile_soft: Some(1 << 16),
        partitions,
        big: true,
    }
}

/// `rows` sorted, for an answer compared as a set.
fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

/// `run` answered through the bucketed retry and its execs spilled: the plan
/// is `bucket` at `bits >= 1` over a hash exchange at `partitions`, every
/// bucketed exec runs the tuning the report and the INFO line in `log` give
/// (not the placeholder's 6 bits and 8192 rows), and the execs wrote state
/// rows and read every one of them back.
fn assert_bucket_spilled(run: &RetryRun, log: &str, partitions: usize) {
    let report = run.report.expect("the retry reports");
    assert_eq!(
        (report.retry_plan, report.retry_plan_reason),
        ("bucket", None),
        "{log}"
    );
    let bits = report.bits.expect("bits");
    let rows = report.routed_chunk_rows.expect("rows");
    assert!(bits >= 1, "{report:?}");
    assert_ne!((bits, rows), (6, 8192), "the placeholder's tuning");
    assert_eq!(retry_info_field(log, "retry_plan"), "bucket");
    assert_eq!(retry_info_field(log, "retry_plan_reason"), "none");
    assert_eq!(
        (
            retry_info_field(log, "bits"),
            retry_info_field(log, "routed_chunk_rows")
        ),
        (bits.to_string(), rows.to_string()),
        "the INFO line is the report"
    );
    assert!(report.bucket_spill_bytes_written > 0, "{report:?}");
    assert!(report.bucket_spill_rows_written > 0, "{report:?}");
    assert_eq!(
        report.bucket_spill_rows_read, report.bucket_spill_rows_written,
        "{report:?}"
    );
    let plan = run.plan.as_deref().expect("the retry's plan");
    assert!(
        plan.contains(&format!(
            "RepartitionExec: partitioning=Hash([a@0], {partitions})"
        )),
        "{plan}"
    );
    let execs = bucketed_lines(plan);
    assert!(!execs.is_empty(), "{plan}");
    for exec in execs {
        assert!(
            exec.contains(&format!("bits={bits}, routed_chunk_rows={rows},")),
            "the exec runs at the INFO line's tuning: {exec}"
        );
    }
}

/// Field `name` of the spill retry's INFO line in `log`, quotes stripped.
fn retry_info_field(log: &str, name: &str) -> String {
    let line = log
        .lines()
        .find(|line| line.contains("SQL statement retried with spill"))
        .unwrap_or_else(|| panic!("no spill retry INFO line in:\n{log}"));
    let key = format!(" {name}=");
    let start = line
        .find(&key)
        .unwrap_or_else(|| panic!("no {name} on the INFO line: {line}"))
        + key.len();
    line[start..]
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

/// The `BucketedFinalAggregateExec` lines of a rendered plan.
fn bucketed_lines(plan: &str) -> Vec<&str> {
    plan.lines()
        .filter(|line| line.contains("BucketedFinalAggregateExec:"))
        .collect()
}

/// Issue #4075: a refused statement whose final aggregate the bucketed rule
/// admits retries through the hash exchange and the bucketed final aggregate
/// although `SqlConfig::bucketed_final_aggregation` is off, its execs spill
/// at the tuning the INFO line reports, and it answers the unlimited first
/// attempt's top ten row for row on a top ten decided by ties alone. The
/// fixture puts 1.5M groups in each of the two partitions, past the default
/// engage threshold of 65,536.
///
/// Prove-the-test: plan the candidate with spill disabled and
/// `bucket_spill_bytes_written` is 0; make `with_bucket_spill_tuning` return
/// the plan unchanged and the execs run at `bits=6, routed_chunk_rows=8192`.
#[tokio::test]
async fn bucket_spill_retry_matches_unspilled_answer() {
    let (unlimited, bucket, log) =
        answered_retry(RETRY_TIES_SQL, bucket_refusal(RETRY_PARTITIONS)).await;
    assert_bucket_spilled(&bucket, &log, RETRY_PARTITIONS);
    assert_eq!(bucket.rows.len(), 10);
    assert_eq!(bucket.rows, unlimited.rows);
}

/// The bucketed retry answers every group of the [`BIG_ROWS`] fixture as the
/// unlimited first attempt does at 2, 4 and 16 final partitions, through
/// bucketed execs that spilled at each count.
///
/// Prove-the-test: the same two mutations as
/// `bucket_spill_retry_matches_unspilled_answer`.
#[tokio::test]
async fn bucket_spill_retry_answers_at_2_4_and_16_partitions() {
    for partitions in [2, 4, 16] {
        let (unlimited, bucket, log) =
            answered_retry(RETRY_GROUPS_SQL, bucket_refusal(partitions)).await;
        assert_bucket_spilled(&bucket, &log, partitions);
        let (bucket, unlimited) = (sorted(bucket.rows), sorted(unlimited.rows));
        assert_eq!(bucket.len(), BIG_ROWS as usize, "at {partitions}");
        assert!(bucket == unlimited, "at {partitions}: the answers differ");
    }
}

/// Over the [`BIG_ROWS`] fixture, each key of `a % 2900000` below 100,000
/// occurs twice, 2,900,000 rows apart, so its two rows sit in different
/// batches and, at two scan partitions of 1.5M rows each, in both
/// partitions; every other key occurs once. `v` is `a % 1000 - 500` and
/// 2,900,000 is a multiple of 1000, so a key's `sum(v)` is its count times
/// `k % 1000 - 500`. Most keys stay single so the groups, and so the
/// unlimited peak the refusal is sized from, stay near the fixture's 3M.
const RETRY_MERGE_SQL: &str =
    "SELECT a % 2900000 AS k, count(*) AS c, sum(v) AS s FROM logs GROUP BY k";

/// [`RETRY_MERGE_SQL`]'s top ten by count, then sum, then key: the 100
/// keys `k % 1000 = 999` below 100,000 tie at count 2 and sum 998, so the
/// answer is 999, 1999, ..., 9999, each `|2|998`, and only a merged count
/// reaches 2.
const RETRY_MERGE_TOP_SQL: &str = "SELECT a % 2900000 AS k, count(*) AS c, sum(v) AS s \
                                   FROM logs GROUP BY k ORDER BY c DESC, s DESC, k LIMIT 10";

/// Issue #4114: the bucketed retry merges a key's partial states read back
/// from its bucket file and its in-memory chunks. Each key of
/// [`RETRY_MERGE_SQL`] has a partial state from each scan partition, with
/// different counts, and the retry's count and sum per key, and its exact
/// top ten, equal the unspilled run's.
///
/// Prove-the-test: merge only the first state of each key on read-back and
/// both statements' answers differ; evaluate and cut each bucket to its top
/// k after every chunk instead of once after the last, and the top ten
/// differs.
#[tokio::test]
async fn bucket_spill_retry_merges_each_keys_states() {
    for sql in [RETRY_MERGE_SQL, RETRY_MERGE_TOP_SQL] {
        let (unlimited, bucket, log) = answered_retry(sql, bucket_refusal(RETRY_PARTITIONS)).await;
        let report = bucket.report.expect("the retry reports");
        assert_eq!(
            (report.retry_plan, report.retry_plan_reason),
            ("bucket", None),
            "{sql}: {log}"
        );
        assert!(report.bucket_spill_rows_written > 0, "{sql}: {report:?}");
        assert_eq!(
            report.bucket_spill_rows_read, report.bucket_spill_rows_written,
            "{sql}: {report:?}"
        );
        let plan = bucket.plan.as_deref().expect("the retry's plan");
        assert!(!bucketed_lines(plan).is_empty(), "{sql}: {plan}");
        if sql == RETRY_MERGE_TOP_SQL {
            assert!(
                bucketed_lines(plan)
                    .iter()
                    .all(|line| line.contains("topk=")),
                "each bucket is cut to the top k: {plan}"
            );
            let want: Vec<String> = (1..=10)
                .map(|i| format!("{}|2|998", i * 1000 - 1))
                .collect();
            assert_eq!(bucket.rows, want, "{sql}");
            assert_eq!(bucket.rows, unlimited.rows, "{sql}");
        } else {
            assert_eq!(bucket.rows.len(), 2_900_000, "{sql}");
            let (bucket, unlimited) = (sorted(bucket.rows), sorted(unlimited.rows));
            assert!(bucket == unlimited, "{sql}: the answers differ");
            assert!(bucket.contains(&"999|2|998".to_string()));
            assert!(bucket.contains(&"100999|1|499".to_string()));
        }
    }
}

/// Issue #4114: the retry line's figures checked against sources other
/// than the report. On [`RETRY_GROUPS_SQL`] at two final partitions, refused
/// at nine tenths of its unlimited peak:
/// - `bits`: N = 3,000,000 (the fixture's rows); A and n are read off the
///   first attempt, so `bits` is pinned against the derivation recomputed by
///   hand from them below, and the literal is the value they give on this
///   fixture (see the comment at the assertion);
/// - `descriptor_need` = 2 x P x 2^bits + P with P = 2 written out;
/// - `final_partitions` = the partition count of the hash exchange in the
///   retry's plan text;
/// - `retry_peak_asked_bytes` = the response's
///   `peak_asked_intermediate_bytes`.
#[tokio::test]
async fn the_retry_line_figures_match_sources_outside_the_report() {
    let (_, run, log) = answered_retry(RETRY_GROUPS_SQL, bucket_refusal(RETRY_PARTITIONS)).await;
    let report = run.report.expect("the retry reports");
    let bits = report.bits.expect("a bucketed retry has bits");
    assert_eq!(report.scan_rows_total, Some(3_000_000));
    // By hand, from one run of this fixture: A = 49,218,424 and n = 1,056,768
    // at the first refusal, N = 3,000,000, P = 2, C = 8 (the C table's count
    // over the retry plan, read off the line and pinned below), and the limit
    // 137,753,503 (nine tenths of the unlimited peak, all of the process
    // budget's room). T = A x N / n = 139.7 MB.
    // H = max(64 MiB, 10% of the limit) = 67,108,864, so
    // S = floor((137,753,503 - 67,108,864) / 8) = 8,830,579, and
    // 2 x T / (P x 0.75 x S) = 279.4 MB / 13.25 MB = 21.1, whose ceil(log2) is
    // 5. `bits` stays 5 for that ratio in (16, 32], so A / n can move by a
    // quarter either way between runs and keep it.
    assert_eq!(bits, 5, "{report:?}\n{log}");
    assert_eq!(report.spillable_consumers, Some(8), "{report:?}");
    assert_eq!(
        report.descriptor_need,
        Some(2 * 2 * (1u64 << bits) + 2),
        "{report:?}"
    );
    let plan = run.plan.as_deref().expect("the retry's plan");
    let exchange = plan
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("RepartitionExec: partitioning=Hash([")
        })
        .unwrap_or_else(|| panic!("no hash exchange in the retry's plan:\n{plan}"));
    let partitions: usize = exchange
        .split("], ")
        .nth(1)
        .and_then(|rest| rest.split(')').next())
        .and_then(|count| count.parse().ok())
        .unwrap_or_else(|| panic!("no partition count in {exchange}"));
    assert_eq!(partitions, RETRY_PARTITIONS);
    assert_eq!(report.final_partitions, Some(partitions));
    assert_eq!(
        retry_info_field(&log, "final_partitions"),
        partitions.to_string()
    );
    assert!(run.peak_asked > 0);
    assert_eq!(report.retry_peak_asked_bytes, run.peak_asked);
    assert_eq!(
        retry_info_field(&log, "retry_peak_asked_bytes"),
        run.peak_asked.to_string()
    );
}

/// A candidate plan with one final aggregate the bucketed rule replaced and
/// one it did not keeps the sort-merge retry with `not_admitted`, although
/// the replaced one alone derives a tuning: the union of the planned
/// bucketed statement and the same statement planned without the rule.
///
/// Prove-the-test: drop the `FinalPartitioned` check from
/// `bucket_retry_derivation` and the union's reason is no longer
/// `not_admitted`.
#[tokio::test]
async fn retry_refuses_a_plan_with_an_unreplaced_final_aggregate() {
    use datafusion::common::tree_node::TreeNode;
    use datafusion::physical_plan::union::UnionExec;

    let sql = "SELECT a, count(*) AS c FROM logs GROUP BY a";
    let on = executor(
        ROWS,
        sql_config(RETRY_PARTITIONS, Some(bucketed(ENGAGE_GROUPS)), None),
    )
    .await;
    let replaced = physical_plan_tree(&on, sql).await;
    let off = executor(ROWS, sql_config(RETRY_PARTITIONS, None, None)).await;
    let unreplaced = physical_plan_tree(&off, sql).await;
    let final_partitioned = |plan: &Arc<dyn ExecutionPlan>| {
        let mut found = 0;
        plan.apply(|node| {
            if node
                .downcast_ref::<AggregateExec>()
                .is_some_and(|aggregate| *aggregate.mode() == AggregateMode::FinalPartitioned)
            {
                found += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("the plan walks");
        found
    };
    assert_eq!(final_partitioned(&replaced), 0);
    assert_eq!(final_partitioned(&unreplaced), 1);
    let figures = ravel_sql::BucketRetryFigures {
        asked_bytes: 1 << 20,
        consumed_rows: Some(ROWS as u64),
        query_limit: ravel_sql::DEFAULT_MAX_QUERY_BYTES,
        nofile_soft: ravel_sql::DEFAULT_NOFILE_SOFT,
        slots: 1,
    };
    ravel_sql::bucket_retry_tuning(&replaced, figures)
        .unwrap_or_else(|reason| panic!("the replaced plan alone tunes, got {reason}"));
    let both = UnionExec::try_new(vec![replaced, unreplaced]).expect("the union plans");
    assert_eq!(
        ravel_sql::bucket_retry_tuning(&both, figures),
        Err(ravel_sql::RETRY_NOT_ADMITTED),
        "{}",
        displayable(both.as_ref()).indent(true)
    );
}

/// The derivation's inputs for `report`'s retry of [`RETRY_GROUPS_SQL`] at
/// [`RETRY_PARTITIONS`], at a per-query limit of `query_limit`.
async fn groups_inputs(
    report: &ravel_sql::SpillRetryReport,
    query_limit: usize,
) -> ravel_sql::BucketSpillInputs {
    let on = executor(
        ROWS,
        sql_config(RETRY_PARTITIONS, Some(bucketed(ENGAGE_GROUPS)), None),
    )
    .await;
    let plan = physical_plan_tree(&on, RETRY_GROUPS_SQL).await;
    let shape = BucketSpillPlanShape::read(&plan).expect("the shape reads");
    ravel_sql::BucketSpillInputs {
        asked_bytes: report.first_attempt_asked_bytes,
        total_rows: report.scan_rows_total,
        consumed_rows: report.first_attempt_consumed_rows,
        partitions: report.final_partitions.expect("P"),
        spill_consumers: report.spillable_consumers.expect("C"),
        query_limit,
        column_widths: shape.column_widths,
        variable_width: false,
        nofile_soft: ravel_sql::DEFAULT_NOFILE_SOFT,
        slots: 1,
    }
}

/// A first attempt refused by a tenant budget below `L` while another
/// statement holds part of that budget retries at the effective limit
/// `B - (R - own)`, the budget minus what the other statement holds: the
/// derivation is the one at that limit, finer than at `L`, and the
/// bucketed retry's pool runs under it.
///
/// Prove-the-test: derive at `query_limit: L` and the report's `bits` are
/// the coarser tuning at `L`; derive at the effective limit and leave the
/// candidate's pool at `L`, and `retry_pool_limit_bytes` is `L`.
#[tokio::test]
async fn the_bucketed_retry_runs_at_the_refusing_tenant_budget() {
    use datafusion::execution::memory_pool::MemoryConsumer;

    let scratch = tempfile::tempdir().expect("scratch root");
    let config = || retry_config(scratch.path(), RETRY_PARTITIONS, BIG_QUERY_BYTES);
    let unlimited = retry_run(
        &big_executor(config(), BIG_QUERY_BYTES).await,
        RETRY_GROUPS_SQL,
    )
    .await
    .expect("the unlimited run answers");
    assert_eq!(unlimited.retries, 0);
    let peak = unlimited.peak_pool_bytes as usize;
    let (tenant_bytes, held) = (peak, peak / 10);
    let refused = big_executor(config(), tenant_bytes).await;
    let other = Arc::new(TenantDelegatingPool::new(
        BIG_QUERY_BYTES,
        refused.tenant_budget(tenant().hash()),
        ravel_sql::CeilingBreach::new(),
        QueryAccounting::new(),
    )) as Arc<dyn MemoryPool>;
    let holding = MemoryConsumer::new("another statement").register(&other);
    holding
        .try_grow(held)
        .expect("the other statement holds its bytes");

    let log = crate::spill::CapturedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(log.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    let run = {
        let _guard = tracing::subscriber::set_default(subscriber);
        retry_run(&refused, RETRY_GROUPS_SQL).await
    };
    let log = log.text();
    let run = run.unwrap_or_else(|e| panic!("the retry answers: {e}\n{log}"));
    drop(holding);
    let report = run.report.expect("the retry reports");
    assert_eq!(report.first_refusal, "tenant_budget", "{log}");
    let effective = tenant_bytes - held;
    assert_eq!(report.effective_limit_bytes, Some(effective as u64));
    assert_eq!(report.retry_pool_limit_bytes, effective as u64);
    assert_bucket_spilled(&run, &log, RETRY_PARTITIONS);

    let at = |limit| async move {
        ravel_sql::derive_bucket_spill_tuning(&groups_inputs(&report, limit).await)
            .expect("a tuning derives")
    };
    let (at_effective, at_l) = (at(effective).await, at(BIG_QUERY_BYTES).await);
    assert_eq!(report.bits, Some(at_effective.bits));
    assert!(
        at_effective.bits > at_l.bits,
        "{at_effective:?} is finer than {at_l:?}"
    );
    assert_eq!(sorted(run.rows), sorted(unlimited.rows));
}

/// An effective limit too small to tune keeps the sort-merge retry with the
/// derivation's reason, and that retry runs at `L`, not the effective limit,
/// and answers. A 70 MiB tenant budget leaves 6 MiB above the 64 MiB
/// headroom: no `bits` up to 16 both fits the table and leaves room beside
/// it for routed chunks of 1024 rows, so the reason is `too_many_bits`,
/// while the same first attempt tunes at `L`.
#[tokio::test]
async fn a_retry_whose_effective_limit_is_too_small_keeps_sort_merge_at_l() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let config = || retry_config(scratch.path(), RETRY_PARTITIONS, BIG_QUERY_BYTES);
    let unlimited = retry_run(
        &big_executor(config(), BIG_QUERY_BYTES).await,
        RETRY_GROUPS_SQL,
    )
    .await
    .expect("the unlimited run answers");
    let tenant_bytes = 70 << 20;
    let refused = big_executor(config(), tenant_bytes).await;
    let run = retry_run(&refused, RETRY_GROUPS_SQL)
        .await
        .expect("the sort-merge retry answers");
    let report = run.report.expect("the retry reports");
    assert_eq!(report.first_refusal, "tenant_budget");
    assert_eq!(report.effective_limit_bytes, Some(tenant_bytes as u64));
    assert_eq!(
        (report.retry_plan, report.retry_plan_reason),
        ("sort_merge", Some("too_many_bits"))
    );
    assert_eq!(report.retry_pool_limit_bytes, BIG_QUERY_BYTES as u64);
    assert_eq!(
        ravel_sql::derive_bucket_spill_tuning(&groups_inputs(&report, tenant_bytes).await),
        Err(ravel_sql::NoTuning::TooManyBits)
    );
    ravel_sql::derive_bucket_spill_tuning(&groups_inputs(&report, BIG_QUERY_BYTES).await)
        .expect("the same first attempt tunes at L");
    assert!(
        sorted(run.rows) == sorted(unlimited.rows),
        "the answers differ"
    );
}

/// A retry whose derived tuning needs more descriptors than
/// `floor(nofile_soft / (2 x slots))` keeps the repartition-free sort-merge
/// plan, its INFO line says why, and it answers.
#[tokio::test]
async fn retry_keeps_sort_merge_when_the_descriptor_budget_is_short() {
    let refusal = Refusal {
        nofile_soft: Some(1),
        ..bucket_refusal(RETRY_PARTITIONS)
    };
    let (unlimited, run, log) = refused_run(RETRY_GROUPS_SQL, refusal).await;
    let run = run.unwrap_or_else(|e| panic!("the sort-merge retry answers: {e}\n{log}"));
    let report = run.report.expect("the retry reports");
    assert_eq!(
        (report.retry_plan, report.retry_plan_reason),
        ("sort_merge", Some("descriptor_budget"))
    );
    let plan = run.plan.expect("the retry's plan");
    assert!(bucketed_lines(&plan).is_empty(), "{plan}");
    assert!(!plan.contains("RepartitionExec"), "{plan}");
    assert!(
        sorted(run.rows) == sorted(unlimited.rows),
        "the answers differ"
    );
    assert_eq!(retry_info_field(&log, "retry_plan"), "sort_merge");
    assert_eq!(
        retry_info_field(&log, "retry_plan_reason"),
        "descriptor_budget"
    );
    assert_eq!(
        (
            retry_info_field(&log, "bits"),
            retry_info_field(&log, "routed_chunk_rows")
        ),
        ("none".to_string(), "none".to_string())
    );
}

/// Issue #4114: a first attempt the process budget refuses while a live
/// fetch-class reservation holds part of that budget retries through
/// buckets at the room the budget had left for it, `B` less that
/// reservation, below `L`. The reservation is 60% of `B`, more than the
/// statement's own bytes at the refusal, which are all of SQL's.
///
/// Prove-the-test: drop `non_sql_reserved` from `retry_effective_limit` and
/// the line reads `effective_limit_bytes` of `B` capped at `L`; fold
/// `non_sql_reserved` into `budget_reserved` and the statement's own bytes
/// are under half of the budget's, so the first attempt's refusal is
/// returned with no retry.
#[tokio::test]
async fn a_process_refusal_beside_a_fetch_hold_retries_at_the_room_left() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let config = || retry_config(scratch.path(), RETRY_PARTITIONS, BIG_QUERY_BYTES);
    let unlimited = retry_run(
        &big_executor(config(), BIG_QUERY_BYTES).await,
        RETRY_GROUPS_SQL,
    )
    .await
    .expect("the unlimited run answers");
    assert_eq!(unlimited.retries, 0);
    let room = unlimited.peak_pool_bytes * 9 / 10;
    let held = room * 3 / 2;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(room + held));
    let hold = budget.reserve(held).expect("the fetch-class hold fits");
    assert_eq!(budget.fetch_reserved(), held);
    let refused = big_executor(config(), BIG_QUERY_BYTES)
        .await
        .with_process_memory_budget(Arc::clone(&budget));

    let log = crate::spill::CapturedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(log.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    let run = {
        let _guard = tracing::subscriber::set_default(subscriber);
        retry_run(&refused, RETRY_GROUPS_SQL).await
    };
    let log = log.text();
    let run = run.unwrap_or_else(|e| panic!("the retry answers: {e}\n{log}"));
    drop(hold);
    let report = run.report.expect("the retry reports");
    assert!(
        matches!(
            report.first_refusal,
            "process_budget" | "aggregate_reserve_ahead"
        ),
        "the process budget refused the first attempt: {log}"
    );
    assert!(room < BIG_QUERY_BYTES as u64, "{room} is below L");
    assert_eq!(report.effective_limit_bytes, Some(room));
    assert_eq!(
        retry_info_field(&log, "effective_limit_bytes"),
        room.to_string()
    );
    assert_eq!(report.retry_pool_limit_bytes, room);
    assert_bucket_spilled(&run, &log, RETRY_PARTITIONS);

    let at = |limit| async move {
        ravel_sql::derive_bucket_spill_tuning(&groups_inputs(&report, limit).await)
            .expect("a tuning derives")
    };
    let (at_room, at_l) = (at(room as usize).await, at(BIG_QUERY_BYTES).await);
    assert_eq!(report.bits, Some(at_room.bits));
    assert!(
        at_room.bits > at_l.bits,
        "{at_room:?} is finer than {at_l:?}"
    );
    assert_eq!(sorted(run.rows), sorted(unlimited.rows));
}

/// A bucketed retry that fails is the statement's error, and the statement
/// is not planned again for a memory failure: no sort-merge attempt, no
/// second spill retry. The snapshot retry for a segment that vanished
/// before any row was emitted is separate and unchanged. Room is taken on the
/// process budget after the first refusal and before the retry runs: a
/// hold gate stops the retry's read of the data object, a fetch-class
/// reservation takes all of the budget's unreserved bytes, then the read
/// goes on. The effective limit is sized from the room at the first
/// refusal, so nothing the retry plans can account for room taken after it.
/// The data object is read once per attempt, so the store's GET count is
/// the number of attempts.
///
/// Prove-the-test: run the sort-merge plan after a failed bucketed retry and
/// the one INFO line, the last attempt's, reads `retry_plan="sort_merge"`.
#[tokio::test]
async fn a_failed_bucketed_retry_is_the_statements_error() {
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Sequence};

    let scratch = tempfile::tempdir().expect("scratch root");
    let config = || retry_config(scratch.path(), RETRY_PARTITIONS, BIG_QUERY_BYTES);
    let unlimited = retry_run(
        &big_executor(config(), BIG_QUERY_BYTES).await,
        RETRY_GROUPS_SQL,
    )
    .await
    .expect("the unlimited run answers");
    assert_eq!(unlimited.retries, 0);

    let mut data_gets = Sequence::new(Op::Get).with_key_contains("/l/l0/");
    for _ in 0..8 {
        data_gets = data_gets.then_passthrough();
    }
    let store = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_sequence(data_gets),
    ));
    publish_rlog(
        store.as_ref(),
        &tenant(),
        big_rlog(),
        BIG_ROWS as u64,
        (1, BIG_ROWS),
    )
    .await;
    let retry_read = store.hold(Op::Get, Some("/l/l0/".to_string()), Occurrence::Nth(2));
    let first_room = unlimited.peak_pool_bytes * 9 / 10;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(first_room));
    let refused = executor_over(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        config(),
        BIG_QUERY_BYTES,
    )
    .with_process_memory_budget(Arc::clone(&budget))
    .with_nofile_soft(1 << 16);

    let log = crate::spill::CapturedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(log.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    let take_room = async {
        retry_read.wait_until_held(1).await;
        let taken = budget
            .reserve(budget.unreserved())
            .expect("the room left is reservable");
        for id in retry_read.held() {
            retry_read.release(id);
        }
        taken
    };
    let groups = request(RETRY_GROUPS_SQL);
    let (result, taken) = {
        let _guard = tracing::subscriber::set_default(subscriber);
        tokio::join!(refused.execute(tenant().hash(), &groups), take_room)
    };
    let log = log.text();
    let err = match result {
        Ok(outcome) => panic!(
            "the retry answered with no room left: {:?}\n{log}",
            outcome.stats.spill_retry
        ),
        Err(err) => err,
    };
    assert!(taken.size() > 0, "room was taken before the retry read");
    assert!(
        matches!(&err, ravel_sql::SqlError::ResourcesExhausted(message)
            if message.contains("process memory budget exhausted")),
        "{err}"
    );
    assert_eq!(
        log.matches("SQL statement retried with spill").count(),
        1,
        "{log}"
    );
    assert_eq!(retry_info_field(&log, "outcome"), "failed");
    assert_eq!(retry_info_field(&log, "retry_plan"), "bucket");
    assert_eq!(retry_info_field(&log, "retry_refusal"), "process_budget");
    assert_eq!(
        retry_info_field(&log, "retry_peak_asked_bytes"),
        "0",
        "with no room left the retry's pool granted nothing"
    );
    assert_eq!(
        retry_info_field(&log, "effective_limit_bytes"),
        first_room.to_string(),
        "sized by the room at the first refusal"
    );
    assert_eq!(
        store.sequence_progress(0),
        2,
        "the first attempt and the bucketed retry read the object, nothing after"
    );
    drop(taken);
}

/// A string-keyed final aggregate, which the bucketed rule does not admit,
/// keeps today's sort-merge retry, and the INFO line says `not_admitted`.
#[tokio::test]
async fn retry_keeps_sort_merge_for_unadmitted_shape() {
    let refusal = Refusal {
        tenths: 9,
        nofile_soft: None,
        partitions: RETRY_PARTITIONS,
        big: false,
    };
    let (unlimited, run, log) = answered_retry(RETRY_STRING_SQL, refusal).await;
    let report = run.report.expect("the retry reports");
    assert_eq!(
        (report.retry_plan, report.retry_plan_reason),
        ("sort_merge", Some("not_admitted"))
    );
    assert_eq!(retry_info_field(&log, "retry_plan"), "sort_merge");
    assert_eq!(retry_info_field(&log, "retry_plan_reason"), "not_admitted");
    let plan = run.plan.expect("the retry's plan");
    assert!(bucketed_lines(&plan).is_empty(), "{plan}");
    assert!(!plan.contains("RepartitionExec"), "{plan}");
    assert_eq!(run.rows, unlimited.rows);
}

/// A candidate plan whose hash exchange preserves order is not kept: the
/// bucketed exec's input order is not the one the plan above it was built
/// on. The same plan with its exchange as planned is kept.
///
/// Prove-the-test: drop the `preserve_order` check from
/// `bucket_retry_tuning` and the tampered plan derives a tuning.
#[tokio::test]
async fn retry_refuses_an_order_preserving_exchange() {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;

    let on = executor(
        ROWS,
        sql_config(RETRY_PARTITIONS, Some(bucketed(ENGAGE_GROUPS)), None),
    )
    .await;
    let plan = physical_plan_tree(&on, "SELECT a, count(*) AS c FROM logs GROUP BY a").await;
    let figures = ravel_sql::BucketRetryFigures {
        asked_bytes: 1 << 20,
        consumed_rows: Some(ROWS as u64),
        query_limit: ravel_sql::DEFAULT_MAX_QUERY_BYTES,
        nofile_soft: ravel_sql::DEFAULT_NOFILE_SOFT,
        slots: 1,
    };
    ravel_sql::bucket_retry_tuning(&plan, figures).unwrap_or_else(|reason| {
        panic!(
            "the planned exchange is kept, got {reason}:\n{}",
            displayable(plan.as_ref()).indent(true)
        )
    });

    let tampered = Arc::clone(&plan)
        .transform_up(|node| {
            let Some(exchange) = node.downcast_ref::<RepartitionExec>() else {
                return Ok(Transformed::no(node));
            };
            let partitioning = exchange.partitioning().clone();
            if !matches!(partitioning, Partitioning::Hash(..)) {
                return Ok(Transformed::no(node));
            }
            let input = Arc::clone(exchange.input());
            let ordering =
                LexOrdering::new([PhysicalSortExpr::new_default(col("a", &input.schema())?)])
                    .expect("one sort key");
            let spread = Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::RoundRobinBatch(RETRY_PARTITIONS),
            )?);
            let sorted = Arc::new(SortExec::new(ordering, spread).with_preserve_partitioning(true));
            let ordered = RepartitionExec::try_new(sorted, partitioning)?.with_preserve_order();
            assert!(ordered.preserve_order(), "the exchange preserves order");
            Ok(Transformed::yes(Arc::new(ordered) as Arc<dyn ExecutionPlan>))
        })
        .expect("the plan rewrites")
        .data;
    assert_eq!(
        ravel_sql::bucket_retry_tuning(&tampered, figures),
        Err(ravel_sql::RETRY_ORDER_PRESERVING_EXCHANGE),
        "{}",
        displayable(tampered.as_ref()).indent(true)
    );
}
