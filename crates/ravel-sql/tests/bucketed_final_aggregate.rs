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
use datafusion::physical_plan::aggregates::AggregateExec;
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
        let tenant = tenant_id("acme");
        let specs = vec![SegSpec::new(
            10,
            1,
            1,
            vec![SeriesSpec::new(
                "m",
                (0..SAMPLES).map(|i| (i, i as f64)).collect(),
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
