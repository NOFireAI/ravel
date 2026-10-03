//! Issue #1402: bounded top-k grouped aggregation.
//!
//! Every test drives a real `SqlExecutor` over a real RLOG object in a
//! `MemoryStore`, so what is asserted is the shipped plan and the shipped
//! answer, not a rule invoked in isolation.
//!
//! The "rule off" side of every comparison is
//! `SqlConfig::bounded_topk_max_limit = None`, which does not install
//! `BoundedTopKAggregate` at all. DataFusion's own `TopKAggregation` is off in
//! both configurations (`crate::session_config`), so "off" is a plan with no
//! bounded aggregate anywhere, which is what the gate's negative cases must
//! reproduce byte for byte.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Int64Array, TimestampNanosecondArray};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

/// The declared group-key column: an `I64` attribute, so the group key resolves
/// to Arrow `Int64` and the priority map's supported-key-type check is not the
/// thing under test.
const KEY_COL: &str = "gid";

/// The declared value column the ordering aggregate reads.
const VAL_COL: &str = "val";

/// Distinct group keys in the main fixture. Above the 1,000 the answer-identity
/// case calls for, and far enough above `TOP_K` that a bounded aggregate and an
/// unbounded one hold visibly different amounts of state.
const KEYS: i64 = 200_000;

/// The small fixture's distinct group-key count, one tenth of [`KEYS`]. The
/// bound test is the pair: rule-off peak intermediate bytes must grow with this
/// number, rule-on peak must not.
const SMALL_KEYS: i64 = 20_000;

/// The `LIMIT` every top-k statement here asks for.
const TOP_K: usize = 10;

/// The group whose running maximum sits at the very bottom of the stream for
/// almost its whole length and is then overtaken by one late row. It is the
/// eviction-soundness case: a bounded map that compared it at the wrong time,
/// or that kept partial state per group instead of the group's extreme, loses
/// it.
const LATE_KEY: i64 = 7;

/// The value that late row carries: above every `val` any other group holds, so
/// [`LATE_KEY`] must come first in the top-k.
const LATE_VALUE: i64 = 10_000_000;

fn tenant() -> TenantId {
    TenantId::new("bounded-topk-1402".to_string())
}

fn log_record(ts: i64, gid: i64, val: i64) -> LogRecord {
    log_record_with_attrs(ts, gid, Some(val))
}

/// A record whose declared `val` attribute is entirely absent, so the `val`
/// column reads NULL for it: the fixture for
/// [`a_nullable_ordering_input_does_not_fire`].
fn log_record_no_val(ts: i64, gid: i64) -> LogRecord {
    log_record_with_attrs(ts, gid, None)
}

fn log_record_with_attrs(ts: i64, gid: i64, val: Option<i64>) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let mut attrs = vec![(KEY_COL.to_string(), AttrValue::I64(gid))];
    if let Some(val) = val {
        attrs.push((VAL_COL.to_string(), AttrValue::I64(val)));
    }
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "req".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// `keys` groups of one row each, group `i` carrying `val = i` and `ts = i +
/// 1`, in ascending `gid` order, followed by one extra row for [`LATE_KEY`]
/// carrying `ts = keys + 1` and [`LATE_VALUE`].
///
/// Three properties the assertions rest on, all hand-computable:
///
/// - `max(val)` per group is `gid`, except [`LATE_KEY`], whose maximum is
///   [`LATE_VALUE`]. Every group's maximum is therefore distinct, so the top-k
///   by `max(val) DESC` has no ties to break and a byte-identical row
///   comparison is meaningful. `val` is a declared column, so it is nullable
///   (`ravel_sql::logs_schema`); the ordering-input nullability gate refuses
///   it, which is what the negative test dedicated to that gate exercises.
/// - `max(ts)` per group is `gid + 1`, except [`LATE_KEY`], whose maximum is
///   `keys + 1`, the largest `ts` any group holds. Every group's maximum is
///   therefore distinct here too. `ts` is a fixed, non-nullable column, so
///   this is the ordering input the admitted (positive) tests use.
/// - `count(*)` per group is 1, except [`LATE_KEY`], whose count is 2. So the
///   true top-1 by count is [`LATE_KEY`], and it is the LAST group any
///   count-ordered stream could learn about.
fn records(keys: i64) -> Vec<LogRecord> {
    let mut out: Vec<LogRecord> = (0..keys).map(|i| log_record(i + 1, i, i)).collect();
    out.push(log_record(keys + 1, LATE_KEY, LATE_VALUE));
    out
}

/// Write one RLOG object holding `records` and publish its commit record, so a
/// real `Catalog::resolve` finds it.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [4u8; 16],
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
        writer_id: Uuid::from_u128(9_1402),
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

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new(KEY_COL, DeclaredType::I64),
        DeclaredColumn::new(VAL_COL, DeclaredType::I64),
    ]
}

/// An executor over a fresh tenant holding `records`.
/// `max_limit` is [`SqlConfig::bounded_topk_max_limit`]: `None` is the rule-off
/// side of every comparison here.
async fn executor_with_records(records: Vec<LogRecord>, max_limit: Option<usize>) -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs(store.as_ref(), &tenant(), &records).await;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let config = SqlConfig {
        bounded_topk_max_limit: max_limit,
        ..SqlConfig::default()
    };
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

/// An executor over a fresh tenant holding [`records`]`(keys)`.
async fn executor(keys: i64, max_limit: Option<usize>) -> SqlExecutor {
    executor_with_records(records(keys), max_limit).await
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

/// The physical plan `sql` produces under `executor`, rendered exactly as
/// `EXPLAIN` renders it. Plan equality in the gate's negative tests is equality
/// of this string.
async fn physical_plan(executor: &SqlExecutor, sql: &str) -> String {
    let plan = physical_plan_tree(executor, sql).await;
    format!("{}", displayable(plan.as_ref()).indent(false))
}

/// The physical plan itself, for assertions that inspect operators rather
/// than the rendered text.
async fn physical_plan_tree(executor: &SqlExecutor, sql: &str) -> Arc<dyn ExecutionPlan> {
    let accounting = QueryAccounting::new();
    let declared = executor
        .resolve_declared_columns(tenant().hash(), request(sql).now_ns)
        .await;
    let (snapshot, _) = executor
        .resolve_snapshot(tenant().hash(), &request(sql), &accounting)
        .await
        .expect("snapshot resolves");
    let planned = executor
        .plan_pinned(tenant().hash(), snapshot, sql, &accounting, &declared)
        .await
        .expect("query plans");
    planned
        .create_physical_plan()
        .await
        .expect("physical plan builds")
}

/// Every `AggregateExec` limit in `plan`, in pre-order: the structural form
/// of the `lim=[k]` marker, independent of how DataFusion renders it.
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

/// `column`'s value at `row` as `i64`, whichever of the two integer-shaped
/// arrow types this file's aggregates return: `Int64` for `count(*)` and the
/// group key, `Timestamp(Nanosecond)` for `max(ts)`. Both are `i64` under the
/// hood, so one comparison type serves every test in this file.
fn value_as_i64(column: &dyn Array, row: usize) -> i64 {
    if let Some(a) = column.as_any().downcast_ref::<Int64Array>() {
        return a.value(row);
    }
    if let Some(a) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return a.value(row);
    }
    panic!(
        "unsupported column type for row extraction: {:?}",
        column.data_type()
    );
}

/// Every `(gid, value)` row `sql` returns, in the order it returns them.
async fn rows(executor: &SqlExecutor, sql: &str) -> Vec<(i64, i64)> {
    let outcome = executor
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("query runs");
    let mut out = Vec::new();
    for batch in outcome.output.batches() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("group key is Int64");
        let values = batch.column(1).as_ref();
        for row in 0..batch.num_rows() {
            out.push((keys.value(row), value_as_i64(values, row)));
        }
    }
    out
}

/// `sql`'s peak intermediate bytes under `executor`, as the wire reports them.
async fn peak_intermediate_bytes(executor: &SqlExecutor, sql: &str) -> u64 {
    let outcome = executor
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("query runs");
    outcome.accounting.peak_intermediate_bytes
}

/// The statement the gate admits: a high-cardinality group key, a
/// value-selective ordering aggregate over a fixed non-nullable column, a
/// matching sort direction, a small `LIMIT`. Orders by `ts`, not `val`: `val`
/// is a declared column and every declared column is nullable
/// (`ravel_sql::logs_schema`), which the ordering-input nullability gate now
/// refuses regardless of whether any row's `val` is actually NULL.
fn topk_sql(limit: usize) -> String {
    format!(
        "SELECT {KEY_COL}, max(ts) AS m FROM logs GROUP BY {KEY_COL} \
         ORDER BY m DESC LIMIT {limit}"
    )
}

/// The hand-computed top-`TOP_K` of [`records`]: the late group first with its
/// late `ts`, then the highest `gid`s, each with `max(ts) = gid + 1`.
fn expected_top_k() -> Vec<(i64, i64)> {
    let mut expected = vec![(LATE_KEY, KEYS + 1)];
    expected.extend((0..TOP_K as i64 - 1).map(|i| {
        let gid = KEYS - 1 - i;
        (gid, gid + 1)
    }));
    expected
}

// ---------------------------------------------------------------------------
// Deliverable 1: the shape the rule must target
// ---------------------------------------------------------------------------

/// The operator chain q33 plans to, captured with no rule installed.
///
/// q33 is `GROUP BY "WatchID","ClientIP" ... ORDER BY count(*) DESC LIMIT 10`;
/// the two group keys and the accumulating ordering aggregate are both visible
/// in the chain below, and both are conjuncts the gate refuses. This test is
/// the record of the shape, so a DataFusion upgrade that reshapes it fails here
/// rather than silently moving the rule's target.
#[tokio::test]
async fn q33_shape_plans_to_a_sort_over_a_two_stage_aggregate() {
    let executor = executor(SMALL_KEYS, None).await;
    let sql = format!(
        "SELECT {KEY_COL}, {VAL_COL}, count(*) AS c, sum({VAL_COL}) AS s \
         FROM logs GROUP BY {KEY_COL}, {VAL_COL} ORDER BY c DESC LIMIT 10"
    );
    let plan = physical_plan(&executor, &sql).await;
    let chain: Vec<&str> = plan
        .lines()
        .map(|line| {
            let trimmed = line.trim_start();
            trimmed
                .split_once(':')
                .map_or(trimmed, |(node, _)| node)
                .trim()
        })
        .collect();
    assert_eq!(
        chain,
        vec![
            "SortExec",
            "CoalescePartitionsExec",
            "ProjectionExec",
            "AggregateExec",
            "RepartitionExec",
            "AggregateExec",
            "RepartitionExec",
            "LogsScanExec",
        ],
        "q33's operator chain changed:\n{plan}"
    );
}

// ---------------------------------------------------------------------------
// Answer identity
// ---------------------------------------------------------------------------

/// The rule changes no answer: the same rows, in the same order, with it on and
/// off, over 200,000 (`KEYS`) distinct keys whose top-10 is hand-computable
/// and has no ties.
#[tokio::test]
async fn bounded_top_k_returns_the_same_rows_as_the_unbounded_plan() {
    let sql = topk_sql(TOP_K);
    let on = rows(&executor(KEYS, Some(TOP_K)).await, &sql).await;
    let off = rows(&executor(KEYS, None).await, &sql).await;
    assert_eq!(on, expected_top_k(), "the bounded plan's rows");
    assert_eq!(off, expected_top_k(), "the unbounded plan's rows");
    assert_eq!(on, off);
}

/// The eviction-soundness case the exactness argument rests on.
///
/// [`LATE_KEY`]'s running maximum is 8 (`ts` of its one row in the main loop)
/// while 199,992 groups with larger maxima stream past it, so it is below the
/// k-th best for all but the last row of the scan; that last row, carrying
/// `ts = KEYS + 1`, makes it the largest group of all. A bounded aggregate
/// that compared groups on partial state, or that could not readmit a group
/// after dropping it, returns a top-k without it. Asserted at rank 1
/// specifically, not merely present.
#[tokio::test]
async fn a_group_overtaken_at_the_last_row_is_still_first_in_the_top_k() {
    let sql = topk_sql(TOP_K);
    let out = rows(&executor(KEYS, Some(TOP_K)).await, &sql).await;
    assert_eq!(out[0], (LATE_KEY, KEYS + 1), "full top-k: {out:?}");
}

// ---------------------------------------------------------------------------
// The gate, one negative test per conjunct
// ---------------------------------------------------------------------------

/// Every statement the gate must refuse plans byte-identically to the same
/// statement with the rule uninstalled.
///
/// Plan equality, not answer equality: a rule that fired on the wrong shape and
/// happened to answer correctly would still be unproven, and would still be
/// bounding state it has no argument for.
async fn assert_plan_unchanged(sql: &str, why: &str) {
    let on = physical_plan(&executor(SMALL_KEYS, Some(TOP_K)).await, sql).await;
    let off = physical_plan(&executor(SMALL_KEYS, None).await, sql).await;
    assert_eq!(on, off, "the rule fired on {why}");
}

/// Conjunct: the ordering aggregate must be value-selective. `avg` accumulates,
/// so a group's future rows can move its value in either direction and no
/// pruning argument exists at all.
#[tokio::test]
async fn avg_ordering_does_not_fire() {
    let sql = format!(
        "SELECT {KEY_COL}, avg({VAL_COL}) AS m FROM logs GROUP BY {KEY_COL} \
         ORDER BY m DESC LIMIT {TOP_K}"
    );
    assert_plan_unchanged(&sql, "an avg-ordered top-k").await;
}

/// Conjunct: the sort direction must be the one the aggregate's extreme runs
/// in. `min` finds the smallest value, so a DESCENDING sort on it asks for the
/// groups whose minimum is largest, which the priority map's ordering does not
/// produce.
#[tokio::test]
async fn min_ordered_descending_does_not_fire() {
    let sql = format!(
        "SELECT {KEY_COL}, min(ts) AS m FROM logs GROUP BY {KEY_COL} \
         ORDER BY m DESC LIMIT {TOP_K}"
    );
    assert_plan_unchanged(&sql, "a min-ordered descending top-k").await;
}

/// Conjunct: the ordering aggregate must be an aggregate. Ordering by the group
/// key is a different rewrite with a different argument (DataFusion's own rule
/// admits it; this one does not).
#[tokio::test]
async fn ordering_by_the_group_key_does_not_fire() {
    let sql = format!(
        "SELECT {KEY_COL}, max(ts) AS m FROM logs GROUP BY {KEY_COL} \
         ORDER BY {KEY_COL} DESC LIMIT {TOP_K}"
    );
    assert_plan_unchanged(&sql, "a group-key-ordered top-k").await;
}

/// Conjunct: the `LIMIT` must be at or below
/// [`SqlConfig::bounded_topk_max_limit`]. Asserted at the boundary: `TOP_K + 1`
/// against a ceiling of `TOP_K`, so an off-by-one in the comparison fails here.
#[tokio::test]
async fn a_limit_above_the_threshold_does_not_fire() {
    let sql = topk_sql(TOP_K + 1);
    assert_plan_unchanged(&sql, "a top-k above the configured limit").await;
}

/// Conjunct: there must be a `LIMIT`. Without one the sort materialises every
/// group whatever any aggregate below it does, so there is no `k` to bound by.
#[tokio::test]
async fn no_limit_does_not_fire() {
    let sql =
        format!("SELECT {KEY_COL}, max(ts) AS m FROM logs GROUP BY {KEY_COL} ORDER BY m DESC");
    assert_plan_unchanged(&sql, "an unlimited sort").await;
}

/// Conjunct: a post-aggregation filter decides which groups reach the sort, so
/// keeping only `k` groups underneath it would discard groups `HAVING` would
/// have let through.
#[tokio::test]
async fn a_having_clause_does_not_fire() {
    let sql = format!(
        "SELECT {KEY_COL}, max(ts) AS m FROM logs GROUP BY {KEY_COL} \
         HAVING max(ts) > to_timestamp_nanos(5) ORDER BY m DESC LIMIT {TOP_K}"
    );
    assert_plan_unchanged(&sql, "a HAVING-filtered top-k").await;
}

/// The conjunct issue #1402 asked for and this rule refuses.
///
/// `count` is monotone non-decreasing, which is exactly the property the issue
/// named as sufficient, and it is not: a running count is a LOWER bound on a
/// group's final count, so a group below the k-th can still overtake it.
/// [`LATE_KEY`] is that group here -- it has 2 rows to everyone else's 1, and
/// its second row is the last row of the scan, so a bounded count-eviction with
/// `LIMIT 1` would answer `(0, 1)` instead of `(7, 2)`.
///
/// Both halves are asserted: the true answer, and that the plan is the
/// unbounded one. Widening the gate to `count` reddens the second immediately
/// and the first as soon as the priority map is what executes.
#[tokio::test]
async fn count_ordering_is_refused_and_still_returns_the_true_top_k() {
    let sql = format!(
        "SELECT {KEY_COL}, count(*) AS c FROM logs GROUP BY {KEY_COL} ORDER BY c DESC LIMIT 1"
    );
    assert_plan_unchanged(&sql, "a count-ordered top-k").await;
    let out = rows(&executor(KEYS, Some(TOP_K)).await, &sql).await;
    assert_eq!(out, vec![(LATE_KEY, 2)]);
}

/// The same refusal for `sum`, whose running value is a lower bound on its
/// final value for exactly as long as the summed column is non-negative, and
/// which is therefore prunable for exactly as long as nothing bounds a group's
/// remaining rows from above: never.
#[tokio::test]
async fn sum_ordering_does_not_fire() {
    let sql = format!(
        "SELECT {KEY_COL}, sum({VAL_COL}) AS s FROM logs GROUP BY {KEY_COL} \
         ORDER BY s DESC LIMIT {TOP_K}"
    );
    assert_plan_unchanged(&sql, "a sum-ordered top-k").await;
}

/// Distinct group-key count for [`a_nullable_ordering_input_does_not_fire`]'s
/// fixture. Small and separate from [`KEYS`]/[`SMALL_KEYS`]: this test is
/// about one property, a NULL ordering input, not about scale.
const NULL_FIXTURE_KEYS: i64 = 100;

/// The group whose one row omits `val` entirely: the reviewer's probe. Under
/// `ORDER BY max(val) DESC` (the default `NULLS FIRST`) it must rank first in
/// both the unbounded answer and, because the gate refuses this ordering
/// input, the "bounded" one too.
const NULL_GROUP: i64 = 999_999;

/// `keys` groups of one row each carrying `val = gid`, plus one more group,
/// [`NULL_GROUP`], whose row has no `val` attribute at all.
fn records_with_a_null_group(keys: i64) -> Vec<LogRecord> {
    let mut out: Vec<LogRecord> = (0..keys).map(|i| log_record(i + 1, i, i)).collect();
    out.push(log_record_no_val(keys + 1, NULL_GROUP));
    out
}

/// Every `(gid, Option<value>)` row `sql` returns, in the order it returns
/// them. Unlike [`rows`], this keeps NULL visible, because the property under
/// test is which group ranks first and an all-NULL group has no `i64` to
/// report.
async fn rows_nullable(executor: &SqlExecutor, sql: &str) -> Vec<(i64, Option<i64>)> {
    let outcome = executor
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("query runs");
    let mut out = Vec::new();
    for batch in outcome.output.batches() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("group key is Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("aggregate is Int64");
        for row in 0..batch.num_rows() {
            let value = if values.is_null(row) {
                None
            } else {
                Some(values.value(row))
            };
            out.push((keys.value(row), value));
        }
    }
    out
}

/// Conjunct: the ordering aggregate's own input must be a plain column the
/// input schema marks non-nullable.
///
/// DataFusion's priority map never admits a group whose ordering value is
/// NULL. [`NULL_GROUP`]'s only row omits `val`, so under the default `NULLS
/// FIRST` it ranks first in the unbounded answer; a rule that let this
/// ordering input through would silently drop it instead. That is the
/// reviewer's probe: unbounded `[(999999, None), (99, Some(99)), (98,
/// Some(98))]` against a would-be bounded `[(99, Some(99)), (98, Some(98)),
/// (97, Some(97))]`.
///
/// Both halves are asserted here: the plan is the unbounded one, and the
/// returned rows put the all-NULL group first regardless of which plan ran.
#[tokio::test]
async fn a_nullable_ordering_input_does_not_fire() {
    let sql = format!(
        "SELECT {KEY_COL}, max({VAL_COL}) AS m FROM logs GROUP BY {KEY_COL} \
         ORDER BY m DESC LIMIT 3"
    );
    let on = physical_plan(
        &executor_with_records(records_with_a_null_group(NULL_FIXTURE_KEYS), Some(3)).await,
        &sql,
    )
    .await;
    let off = physical_plan(
        &executor_with_records(records_with_a_null_group(NULL_FIXTURE_KEYS), None).await,
        &sql,
    )
    .await;
    assert_eq!(on, off, "the rule fired on a nullable ordering input");

    let out = rows_nullable(
        &executor_with_records(records_with_a_null_group(NULL_FIXTURE_KEYS), Some(3)).await,
        &sql,
    )
    .await;
    assert_eq!(
        out,
        vec![
            (NULL_GROUP, None),
            (NULL_FIXTURE_KEYS - 1, Some(NULL_FIXTURE_KEYS - 1)),
            (NULL_FIXTURE_KEYS - 2, Some(NULL_FIXTURE_KEYS - 2)),
        ],
        "full top-3: {out:?}"
    );
}

// ---------------------------------------------------------------------------
// The rule actually fires
// ---------------------------------------------------------------------------

/// No test above this one asserts that the rule CHANGES the plan when it
/// fires: a rule stubbed to `Transformed::no` on every node would still pass
/// every negative test (plan unchanged is the point) and the answer-identity
/// tests (an unbounded plan answers the same query correctly too). Only this
/// test and the bytes test below would catch that stub, and this one names
/// the mechanism directly: the rule-on plan differs from the rule-off plan,
/// and an `AggregateExec` in it carries exactly `TOP_K` as its limit,
/// inspected on the operator rather than in the rendered text so a display
/// change cannot fail it while the limit is still there.
#[tokio::test]
async fn the_rule_rewrites_the_gated_shape() {
    let sql = topk_sql(TOP_K);
    let on_tree = physical_plan_tree(&executor(SMALL_KEYS, Some(TOP_K)).await, &sql).await;
    let off_tree = physical_plan_tree(&executor(SMALL_KEYS, None).await, &sql).await;
    let on = format!("{}", displayable(on_tree.as_ref()).indent(false));
    let off = format!("{}", displayable(off_tree.as_ref()).indent(false));
    assert_ne!(on, off, "the rule did not change the plan:\n{on}");
    assert!(
        aggregate_limits(&on_tree).contains(&TOP_K),
        "no AggregateExec in the rule-on plan carries limit {TOP_K}:\n{on}"
    );
    assert!(
        aggregate_limits(&off_tree).is_empty(),
        "the rule-off plan carries an aggregate limit:\n{off}"
    );
}

// ---------------------------------------------------------------------------
// The bound
// ---------------------------------------------------------------------------

/// Measured `peak_intermediate_bytes` for [`topk_sql`]`(TOP_K)` over the
/// 20,000-key fixture with the rule installed. Every figure in this group is
/// reproducible to the byte on this fixture (three consecutive runs, identical
/// values), which is what makes pinning them rather than banding them the
/// stronger check.
const PEAK_ON_SMALL: u64 = 1_050_112;

/// The same statement over the 200,000-key fixture with the rule installed.
/// Ten times the groups for 1.62 times the bytes, and the growth that is left
/// is the scan's own batches, not aggregate state.
const PEAK_ON_LARGE: u64 = 1_706_120;

/// The 20,000-key fixture with the rule uninstalled.
const PEAK_OFF_SMALL: u64 = 1_143_552;

/// The 200,000-key fixture with the rule uninstalled: ten times the groups for
/// 9.4 times the bytes. This is the figure issue #1402 exists to remove, at
/// fixture scale.
const PEAK_OFF_LARGE: u64 = 10_748_928;

/// The claim, as a ratio between two measured pairs.
///
/// With the rule off, `peak_intermediate_bytes` for the admitted statement
/// grows with the distinct-key count (9.4x for 10x the keys): the aggregate
/// holds one accumulator per group. With the rule on it does not (1.62x, and
/// that residue is scan batches), because the aggregate holds `k` groups
/// whatever the key count is. At the large fixture the ratio between the two
/// is 6.30x.
///
/// All four figures are pinned, and the three ratios are asserted separately so
/// a change that moved every figure by the same factor still has to explain
/// itself. Changing `Some(TOP_K)` to `None` on the `on_large` line below is the
/// demonstration that the bound assertion is load-bearing: `on_large` becomes
/// `PEAK_OFF_LARGE` and the first assertion, `assert_eq!(on_large,
/// PEAK_ON_LARGE, ...)`, fails with `10748928` against `1706120`.
#[tokio::test]
async fn peak_intermediate_bytes_stops_scaling_with_the_key_count() {
    let sql = topk_sql(TOP_K);
    let on_small = peak_intermediate_bytes(&executor(SMALL_KEYS, Some(TOP_K)).await, &sql).await;
    let on_large = peak_intermediate_bytes(&executor(KEYS, Some(TOP_K)).await, &sql).await;
    let off_small = peak_intermediate_bytes(&executor(SMALL_KEYS, None).await, &sql).await;
    let off_large = peak_intermediate_bytes(&executor(KEYS, None).await, &sql).await;

    assert_eq!(on_large, PEAK_ON_LARGE, "rule on, {KEYS} keys");
    assert_eq!(on_small, PEAK_ON_SMALL, "rule on, {SMALL_KEYS} keys");
    assert_eq!(off_large, PEAK_OFF_LARGE, "rule off, {KEYS} keys");
    assert_eq!(off_small, PEAK_OFF_SMALL, "rule off, {SMALL_KEYS} keys");

    assert!(
        on_large < 2 * on_small,
        "the bounded plan's peak must not scale with the key count: \
         {on_large} against {on_small} for ten times the groups"
    );
    assert!(
        off_large > 8 * off_small,
        "the unbounded plan's peak must scale with the key count: \
         {off_large} against {off_small} for ten times the groups"
    );
    assert!(
        off_large > 5 * on_large,
        "the bound must be worth having at {KEYS} keys: {off_large} against {on_large}"
    );
}
