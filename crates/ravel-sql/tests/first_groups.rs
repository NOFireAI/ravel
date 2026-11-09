//! Issue #3985 (ADR-3985): the first-groups bound under an unordered
//! `GROUP BY ... LIMIT`.
//!
//! The plan tests read the physical plan the production session builds. The
//! result tests run statements on that session over in-memory tables, so the
//! input partitioning, key types and memory pool are each fixed by the test,
//! and compare every returned group with the figure computed from the input.
//! ClickBench row 18's statement then runs through the `logs` table end to
//! end, and the last test runs statement 9 there under a process budget
//! (issue #4014).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringViewArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    AggregateLayout, AggregateProfile, DeclaredColumn, DeclaredType, FirstGroupsExec,
    LogsTableProvider, SessionPrincipal, SessionTable, SpanSegmentFetcher, SpillDecision,
    SqlConfig, SqlExecutor, SqlRequest, StaticDeclaredColumns, TenantDelegatingPool,
    TenantMemoryAccountant, build_session, plan_aggregate_profile,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

const MIB: usize = 1 << 20;

fn empty_logs() -> SessionTable {
    SessionTable::Logs(Arc::new(LogsTableProvider::new(
        Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TenantHash([0u8; 16]),
        LogSegmentFetcher::new(Arc::new(MemoryStore::new())),
        PhaseAccounting::new(),
    )))
}

/// The production session over `pool`, with `parts` registered as `t`.
fn session(
    config: &SqlConfig,
    pool: Arc<dyn MemoryPool>,
    parts: Vec<Vec<RecordBatch>>,
) -> SessionContext {
    let ctx = build_session(
        config,
        pool,
        empty_logs(),
        true,
        SpillDecision::Disabled,
        &SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session builds");
    let schema = parts[0][0].schema();
    let table = MemTable::try_new(schema, parts).expect("mem table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("table registers");
    ctx
}

async fn plan_of(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds")
}

fn render(plan: &Arc<dyn ExecutionPlan>) -> String {
    format!("{}", displayable(plan.as_ref()).indent(false))
}

fn is_final(mode: &AggregateMode) -> bool {
    matches!(
        mode,
        AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned
    )
}

/// The `n` of every `FirstGroupsExec` in `plan`, in pre-order.
fn first_groups_bounds(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    let mut out = Vec::new();
    if let Some(node) = plan.downcast_ref::<FirstGroupsExec>() {
        out.push(node.n());
    }
    for child in plan.children() {
        out.extend(first_groups_bounds(child));
    }
    out
}

/// The `n` of every `FirstGroupsExec` that is the direct input of a
/// final-mode `AggregateExec` in `plan`, in pre-order.
fn first_groups_under_final(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    let mut out = Vec::new();
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && is_final(aggregate.mode())
        && let Some(node) = aggregate.input().downcast_ref::<FirstGroupsExec>()
    {
        out.push(node.n());
    }
    for child in plan.children() {
        out.extend(first_groups_under_final(child));
    }
    out
}

fn two_key_parts() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, false),
        Field::new("k2", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    (0..4)
        .map(|p| {
            let rows: Vec<i64> = (0..1_000).map(|i| i * 4 + p).collect();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r % 97))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r % 89))),
                Arc::new(Int64Array::from_iter_values(rows.iter().copied())),
            ];
            vec![RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch builds")]
        })
        .collect()
}

/// FLIP: a rule that passes through the `FilterExec` a `HAVING` plans (and
/// its `fetch`) inserts `FirstGroupsExec: n=10` under the `HAVING` statement's
/// aggregate, and the `HAVING` assertion fails.
#[tokio::test]
async fn an_unordered_group_by_limit_puts_a_first_groups_filter_under_the_final_aggregate() {
    async fn bounds(config: &SqlConfig, sql: &str) -> (Vec<usize>, Vec<usize>, String) {
        let (pool, _breach) =
            config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
        let ctx = session(config, pool, two_key_parts());
        let plan = plan_of(&ctx, sql).await;
        (
            first_groups_bounds(&plan),
            first_groups_under_final(&plan),
            render(&plan),
        )
    }

    let config = SqlConfig::default();
    for (sql, n) in [
        (
            "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 LIMIT 10",
            10,
        ),
        (
            "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 OFFSET 5 LIMIT 10",
            15,
        ),
    ] {
        let (all, under_final, text) = bounds(&config, sql).await;
        assert_eq!(all, vec![n], "{sql}\n{text}");
        assert_eq!(under_final, vec![n], "{sql}\n{text}");
        assert!(
            text.contains(&format!("FirstGroupsExec: n={n}")),
            "{sql}\n{text}"
        );
    }

    for (sql, why) in [
        (
            "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 ORDER BY count(*) LIMIT 10",
            "an ORDER BY decides which groups survive",
        ),
        (
            "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 HAVING count(*) > 1 LIMIT 10",
            "a HAVING may drop the first groups",
        ),
        (
            "SELECT k1, k2, count(*) FROM t GROUP BY GROUPING SETS ((k1), (k2)) LIMIT 10",
            "grouping sets expand every row into several groups",
        ),
        (
            "SELECT k1 FROM t GROUP BY k1 LIMIT 10",
            "an aggregate with no aggregate expression carries its own limit",
        ),
        (
            "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 LIMIT 2000",
            "a bound above bounded_groups_max_limit",
        ),
    ] {
        let (all, _, text) = bounds(&config, sql).await;
        assert!(all.is_empty(), "{why}: {sql}\n{text}");
    }

    let off = SqlConfig {
        bounded_groups_max_limit: 0,
        ..SqlConfig::default()
    };
    let sql = "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 LIMIT 10";
    let (all, _, text) = bounds(&off, sql).await;
    assert!(
        all.is_empty(),
        "bounded_groups_max_limit = 0: {sql}\n{text}"
    );
}

const PARTITIONS: i64 = 8;
const GROUPS: i64 = 65_536;

/// Rows group `g` holds: 1 + (g mod 13), from 1 to 13.
fn rows_of(g: i64) -> i64 {
    1 + g % 13
}

/// [`GROUPS`] groups over [`PARTITIONS`] partitions, row `j` of group `g` in
/// partition `(g + j) mod 8`, each row carrying `v = g`. A group of up to 8
/// rows has each row in a different partition; one of 9 to 13 rows (the
/// spec asked for every row in its own partition, which 8 partitions cannot
/// hold) has rows in all 8, at most two in any one.
fn spread_parts() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let mut keys: Vec<Vec<i64>> = vec![Vec::new(); PARTITIONS as usize];
    for g in 0..GROUPS {
        for j in 0..rows_of(g) {
            keys[((g + j) % PARTITIONS) as usize].push(g);
        }
    }
    keys.into_iter()
        .map(|keys| {
            keys.chunks(1024)
                .map(|chunk| {
                    let k = Int64Array::from_iter_values(chunk.iter().copied());
                    let v = Int64Array::from_iter_values(chunk.iter().copied());
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(k), Arc::new(v)])
                        .expect("batch builds")
                })
                .collect()
        })
        .collect()
}

fn int64(column: &ArrayRef) -> &Int64Array {
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("an Int64 column")
}

/// The whole input lives in eight partitions and every group spreads over up
/// to eight of them, so a group's count is right only if the filter sits
/// after the hash repartition, where each group lives in one partition. The
/// pool is 1 MiB, and the run skips partial aggregation from the first row
/// and uses 1,024-row batches ([`probed`]'s `tight`), so the exchange and the
/// partial stage fit and the final aggregate decides whether the pool holds:
/// with the rule off it does not, and the query is refused.
///
/// FLIP 1: a rule that puts the filter on the repartition's input, before
/// the hash exchange, returns counts short of `1 + (k mod 13)`.
/// FLIP 2: a filter that ends its input once it holds `n` groups, as a soft
/// limit would, returns counts short of `1 + (k mod 13)`.
#[tokio::test]
async fn the_returned_groups_carry_every_row_from_every_input_partition() {
    let sql = "SELECT k, count(*), sum(v) FROM t GROUP BY k LIMIT 10";
    let config = SqlConfig {
        max_query_bytes: MIB,
        ..SqlConfig::default()
    };
    let run = probed(&config, spread_parts(), sql, true).await;
    assert_eq!(run.bounds, vec![10], "{}", run.plan);
    let batches = run.result.expect("query runs in 1 MiB");

    let mut keys = BTreeSet::new();
    for batch in &batches {
        let (k, count, sum) = (
            int64(batch.column(0)),
            int64(batch.column(1)),
            int64(batch.column(2)),
        );
        for row in 0..batch.num_rows() {
            let key = k.value(row);
            assert!(
                (0..GROUPS).contains(&key),
                "key {key} is not an input group"
            );
            assert_eq!(count.value(row), rows_of(key), "count(*) of group {key}");
            assert_eq!(sum.value(row), key * rows_of(key), "sum(v) of group {key}");
            assert!(keys.insert(key), "group {key} returned twice");
        }
    }
    assert_eq!(
        keys.len(),
        10,
        "LIMIT 10 over 65,536 groups returns 10 groups"
    );
    assert!(
        run.charged < MIB as u64,
        "charged peak {} reached the 1 MiB pool",
        run.charged
    );

    // The same statement and pool with the rule off: the final aggregate
    // tracks every group and the pool refuses it.
    let off = SqlConfig {
        bounded_groups_max_limit: 0,
        ..config.clone()
    };
    let off = probed(&off, spread_parts(), sql, true).await;
    let err = off.result.expect_err("the rule-off plan exceeds 1 MiB");
    eprintln!(
        "first_groups 1 MiB: charged peak {} with the bound; rule off: {err}",
        run.charged
    );
    assert!(
        err.to_string().contains("Resources exhausted"),
        "rule off: {err}"
    );
}

/// A `Utf8View` key whose groups the filter must count as the aggregate
/// does: a NULL group, a 13-byte string stored out of line and a 12-byte
/// string stored inline, beside seven out-of-line filler groups. Every batch
/// is a slice of one array, so each out-of-line row's view carries its own
/// buffer offset. Partial aggregation is skipped after the first row and the
/// plan has one final partition, so the filter sees every group, and nearly
/// every row as its own view.
///
/// FLIP: interning the raw 128-bit views instead of the strings splits each
/// out-of-line group into one group per view; the filler rows, which sort
/// first, fill the ten slots, and the 13-byte group is dropped.
#[tokio::test]
async fn first_groups_keep_null_and_long_view_keys_as_the_aggregate_does() {
    const LONG: &str = "thirteen-byte";
    const SHORT: &str = "twelve-bytes";
    assert_eq!((LONG.len(), SHORT.len()), (13, 12));

    let mut expected: BTreeMap<Option<String>, i64> = BTreeMap::new();
    expected.insert(None, 7);
    expected.insert(Some(LONG.to_string()), 5);
    expected.insert(Some(SHORT.to_string()), 3);
    for f in 0..7 {
        expected.insert(Some(format!("filler group number {f}")), 4);
    }
    assert_eq!(expected.len(), 10, "every group fits LIMIT 10");

    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8View, true)]));
    let mut rows: Vec<Option<String>> = Vec::new();
    for (key, count) in &expected {
        for _ in 0..*count {
            rows.push(key.clone());
        }
    }
    let all = StringViewArray::from_iter(rows.iter().map(|k| k.as_deref()));
    let mut parts: Vec<Vec<RecordBatch>> = vec![Vec::new(), Vec::new()];
    for (i, start) in (0..rows.len()).step_by(3).enumerate() {
        let array = all.slice(start, 3.min(rows.len() - start));
        parts[i % 2].push(
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).expect("batch builds"),
        );
    }

    let config = SqlConfig::default();
    let (pool, _breach) =
        config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
    let ctx = session(&config, pool, parts);
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        let options = &mut state.config_mut().options_mut().execution;
        options.skip_partial_aggregation_probe_rows_threshold = 1;
        options.skip_partial_aggregation_probe_ratio_threshold = 0.0;
        options.target_partitions = 1;
    }
    let sql = "SELECT k, count(*) FROM t GROUP BY k LIMIT 10";
    let plan = plan_of(&ctx, sql).await;
    assert_eq!(
        first_groups_under_final(&plan),
        vec![10],
        "{}",
        render(&plan)
    );
    let batches = collect(plan, ctx.task_ctx()).await.expect("query runs");

    let mut got: BTreeMap<Option<String>, i64> = BTreeMap::new();
    for batch in &batches {
        let k = cast(batch.column(0), &DataType::Utf8).expect("key casts to Utf8");
        let k = k.as_any().downcast_ref::<StringArray>().expect("Utf8");
        let count = int64(batch.column(1));
        for row in 0..batch.num_rows() {
            let key = k.is_valid(row).then(|| k.value(row).to_string());
            assert!(
                got.insert(key.clone(), count.value(row)).is_none(),
                "{key:?} twice"
            );
        }
    }
    assert_eq!(got.get(&None), Some(&7), "the NULL group");
    assert_eq!(
        got.get(&Some(LONG.to_string())),
        Some(&5),
        "the 13-byte group"
    );
    assert_eq!(
        got.get(&Some(SHORT.to_string())),
        Some(&3),
        "the 12-byte group"
    );
    assert_eq!(got, expected);
}

/// Forwards to the query's `TenantDelegatingPool` and records, after every
/// call, the bytes the process budget holds beyond the tenant's charge: the
/// aggregate envelopes, which are charged on the process budget alone. The
/// tenant accountant and its budget are private to the query, so nothing else
/// moves either figure.
#[derive(Debug)]
struct EnvelopeProbe {
    inner: Arc<dyn MemoryPool>,
    tenant: Arc<TenantMemoryAccountant>,
    peak: AtomicI64,
}

impl EnvelopeProbe {
    fn sample(&self) {
        let envelope = self.tenant.process_reserved() as i64 - self.tenant.reserved() as i64;
        self.peak.fetch_max(envelope, Ordering::Relaxed);
    }
}

impl fmt::Display for EnvelopeProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EnvelopeProbe({})", self.inner)
    }
}

impl MemoryPool for EnvelopeProbe {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
        self.sample();
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
        self.sample();
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.sample();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.sample();
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        let result = self.inner.try_grow(reservation, additional);
        self.sample();
        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

const PEAK_GROUPS: i64 = 1_000_000;

/// [`PEAK_GROUPS`] distinct `(Int64, Utf8View)` groups, one row each, over 8
/// partitions; 93 rows in 100 carry an empty phrase, the rest a 20-odd-byte
/// one, the mix of ClickBench's `(UserID, SearchPhrase)`.
fn wide_parts() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("phrase", DataType::Utf8View, false),
    ]));
    let per_part = PEAK_GROUPS / PARTITIONS;
    (0..PARTITIONS)
        .map(|p| {
            let ids: Vec<i64> = (p * per_part..(p + 1) * per_part).collect();
            ids.chunks(1024)
                .map(|chunk| {
                    let user = Int64Array::from_iter_values(chunk.iter().copied());
                    let phrase = StringViewArray::from_iter_values(chunk.iter().map(|i| {
                        if i % 100 < 93 {
                            String::new()
                        } else {
                            format!("search phrase {i:08}")
                        }
                    }));
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(user), Arc::new(phrase)],
                    )
                    .expect("batch builds")
                })
                .collect()
        })
        .collect()
}

/// One statement's run on a probed pool.
struct Probed {
    /// The `n` of each `FirstGroupsExec` under a final aggregate.
    bounds: Vec<usize>,
    plan: String,
    result: DFResult<Vec<RecordBatch>>,
    /// The query pool's charged peak, `peak_intermediate_bytes`.
    charged: u64,
    /// The largest aggregate envelope the process budget held.
    envelope: i64,
}

/// Run `sql` over `parts` on the production session, its pool wrapped in an
/// [`EnvelopeProbe`]. `tight` fits the run's fixed costs under a 1 MiB pool:
/// DataFusion's partial-aggregation probe decides after the first row, so a
/// partial aggregate over keys that barely repeat passes its rows on rather
/// than holding up to 8,192 rows' worth of groups first, and batches hold
/// 1,024 rows, so each batch the hash exchange buffers per output partition
/// is an eighth of the default.
async fn probed(
    config: &SqlConfig,
    parts: Vec<Vec<RecordBatch>>,
    sql: &str,
    tight: bool,
) -> Probed {
    let tenant =
        TenantMemoryAccountant::with_process_budget(1 << 34, Arc::new(MemoryBudget::unlimited()));
    let accounting = QueryAccounting::new();
    let (inner, _breach) = config.query_pool(Arc::clone(&tenant), accounting.clone());
    let probe = Arc::new(EnvelopeProbe {
        inner: Arc::clone(&inner),
        tenant,
        peak: AtomicI64::new(0),
    });
    let ctx = session(config, Arc::clone(&probe) as Arc<dyn MemoryPool>, parts);
    if tight {
        let state = ctx.state_ref();
        let mut state = state.write();
        let options = state.config_mut().options_mut();
        for (key, value) in [
            (
                "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
                "1",
            ),
            (
                "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                "0.0",
            ),
            ("datafusion.execution.batch_size", "1024"),
        ] {
            options.set(key, value).expect("option sets");
        }
    }
    let plan = plan_of(&ctx, sql).await;
    // The session's own profile rule finds the probe, not the delegating
    // pool, so the profile the executor would install is installed here.
    let profile = plan_aggregate_profile(&plan).expect("the plan has an aggregate profile");
    inner
        .downcast_ref::<TenantDelegatingPool>()
        .expect("the query pool is a TenantDelegatingPool")
        .set_aggregate_profile(profile);
    let bounds = first_groups_under_final(&plan);
    let text = render(&plan);
    let result = collect(plan, ctx.task_ctx()).await;
    Probed {
        bounds,
        plan: text,
        result,
        charged: accounting.snapshot().peak_intermediate_bytes,
        envelope: probe.peak.load(Ordering::Relaxed),
    }
}

/// With the bound, the final aggregate holds at most ten groups per
/// partition, so over a million groups the query's charged peak stays under
/// 16 MiB and the aggregate envelope under 8 MiB; what remains is the hash
/// exchange's buffers and the eight partial aggregates, which build up to
/// the 8,192-row skip probe at the session defaults. With the rule off, the
/// final aggregates hold every group and both figures are above 32 MiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn first_groups_bound_the_final_aggregates_charged_peak() {
    let sql = "SELECT user_id, phrase, count(*) FROM t GROUP BY user_id, phrase LIMIT 10";
    let on = probed(&SqlConfig::default(), wide_parts(), sql, false).await;
    let off_config = SqlConfig {
        bounded_groups_max_limit: 0,
        ..SqlConfig::default()
    };
    let off = probed(&off_config, wide_parts(), sql, false).await;
    eprintln!(
        "first_groups peaks: on charged={} envelope={}; off charged={} envelope={}",
        on.charged, on.envelope, off.charged, off.envelope
    );
    assert_eq!((on.bounds, off.bounds), (vec![10], vec![]));
    for run in [&on.result, &off.result] {
        let batches = run.as_ref().expect("query runs");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);
    }
    assert!(
        on.charged < 16 * MIB as u64,
        "charged peak {} with the bound",
        on.charged
    );
    assert!(
        on.envelope < 8 * MIB as i64,
        "envelope peak {} with the bound",
        on.envelope
    );
    assert!(
        off.charged > 32 * MIB as u64,
        "charged peak {} with the rule off",
        off.charged
    );
    assert!(
        off.envelope > 32 * MIB as i64,
        "envelope peak {} with the rule off",
        off.envelope
    );
}

const USER_COL: &str = "UserID";
const PHRASE_COL: &str = "SearchPhrase";

fn logs_tenant() -> TenantId {
    TenantId::new("first-groups-3985".to_string())
}

/// Row `i` of object `object`: user `i mod 3001`, and 93 rows in 100 an
/// empty phrase, the rest one of seven phrases. The same `(user, phrase)`
/// pair recurs within and across both objects.
fn clickbench_record(object: i64, i: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let phrase = if i % 100 < 93 {
        String::new()
    } else {
        format!("phrase {}", i % 7)
    };
    let ts = object * 1_000_000 + i + 1;
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
        attrs: vec![
            (USER_COL.to_string(), AttrValue::I64(i % 3001)),
            (PHRASE_COL.to_string(), AttrValue::Str(phrase)),
        ],
    }
}

/// Write one RLOG object holding `records` as writer sequence `seq` and
/// publish its commit record.
async fn publish_object(store: &dyn ObjectStoreBackend, seq: u64, records: &[LogRecord]) {
    let tenant = logs_tenant();
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [5u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
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
        writer_id: Uuid::from_u128(3985),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash: [seq as u8; 32],
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

fn logs_request(sql: &str) -> SqlRequest {
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

/// ClickBench row 18's statement over the `logs` table, through the
/// executor: two objects, 40,000 rows, 3,001 users. Every one of the ten
/// groups returned carries the count of its `(UserID, SearchPhrase)` pair
/// over the whole input.
#[tokio::test]
async fn clickbench_row_18_returns_exact_counts_through_the_logs_table() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut reference: BTreeMap<(i64, String), i64> = BTreeMap::new();
    for object in 0..2 {
        let records: Vec<LogRecord> = (0..20_000).map(|i| clickbench_record(object, i)).collect();
        for r in &records {
            let (AttrValue::I64(user), AttrValue::Str(phrase)) = (&r.attrs[0].1, &r.attrs[1].1)
            else {
                panic!("fixture attrs are (I64, Str)");
            };
            *reference.entry((*user, phrase.clone())).or_default() += 1;
        }
        publish_object(store.as_ref(), object as u64 + 1, &records).await;
    }
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(vec![
        DeclaredColumn::new(USER_COL, DeclaredType::I64),
        DeclaredColumn::new(PHRASE_COL, DeclaredType::Str),
    ])));

    let sql = "SELECT \"UserID\", \"SearchPhrase\", COUNT(*) FROM logs \
               GROUP BY \"UserID\", \"SearchPhrase\" LIMIT 10";
    let request = logs_request(sql);
    let accounting = QueryAccounting::new();
    let declared = executor
        .resolve_declared_columns(logs_tenant().hash(), request.now_ns)
        .await;
    let (snapshot, _) = executor
        .resolve_snapshot(logs_tenant().hash(), &request, &accounting)
        .await
        .expect("snapshot resolves");
    let plan = executor
        .plan_pinned(logs_tenant().hash(), snapshot, sql, &accounting, &declared)
        .await
        .expect("query plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds");
    assert_eq!(
        first_groups_under_final(&plan),
        vec![10],
        "{}",
        render(&plan)
    );

    let outcome = executor
        .execute(logs_tenant().hash(), &request)
        .await
        .expect("query runs");
    let mut seen = BTreeSet::new();
    for batch in outcome.output.batches() {
        let user = int64(batch.column(0));
        let phrase = cast(batch.column(1), &DataType::Utf8).expect("phrase casts to Utf8");
        let phrase = phrase.as_any().downcast_ref::<StringArray>().expect("Utf8");
        let count = int64(batch.column(2));
        for row in 0..batch.num_rows() {
            assert!(phrase.is_valid(row), "every row carries a phrase");
            let key = (user.value(row), phrase.value(row).to_string());
            let want = reference.get(&key).copied();
            assert_eq!(Some(count.value(row)), want, "count of {key:?}");
            assert!(seen.insert(key.clone()), "{key:?} returned twice");
        }
    }
    assert_eq!(seen.len(), 10);
}

const REGION_COL: &str = "RegionID";

/// Row `i`'s region: slot `i mod 36` falls in region `r` for `r + 1` of the
/// 36 slots.
fn region_of(i: i64) -> i64 {
    let slot = i % 36;
    (0..8).find(|r| slot < (r + 1) * (r + 2) / 2).unwrap_or(7)
}

/// Row `i`: user `i`, every user distinct, in region [`region_of`].
fn distinct_record(i: i64) -> LogRecord {
    let mut record = clickbench_record(0, i);
    record.attrs = vec![
        (REGION_COL.to_string(), AttrValue::I64(region_of(i))),
        (USER_COL.to_string(), AttrValue::I64(i)),
    ];
    record
}

/// A store holding rows `0..rows` of [`distinct_record`] in one object.
async fn distinct_store(rows: i64) -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let records: Vec<LogRecord> = (0..rows).map(distinct_record).collect();
    publish_object(store.as_ref(), 1, &records).await;
    store
}

/// An executor over `store` with one SQL partition, charging `budget`.
fn distinct_executor(
    store: &Arc<dyn ObjectStoreBackend>,
    budget: Arc<MemoryBudget>,
) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        {
            let mut config = SqlConfig::default();
            config.engine.sql_partition_count = Some(1);
            config
        },
        1 << 40,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(vec![
        DeclaredColumn::new(REGION_COL, DeclaredType::I64),
        DeclaredColumn::new(USER_COL, DeclaredType::I64),
    ])))
    .with_process_memory_budget(budget)
}

/// The `(RegionID, u)` rows of a statement-9 answer, in the order returned.
fn region_counts(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let region = int64(batch.column(0));
        let users = int64(batch.column(1));
        for row in 0..batch.num_rows() {
            rows.push((region.value(row), users.value(row)));
        }
    }
    rows
}

/// ClickBench statement 9 over the `logs` table, end to end (issue #4014):
/// 604,800 rows in one object, every `UserID` distinct, region `r` holding
/// `16,800 x (r + 1)` of them. DataFusion rewrites the `COUNT(DISTINCT)`
/// into a group-by of `(RegionID, UserID)` under a `COUNT` by region, so the
/// inner aggregate builds a 604,800-group table of 16-byte entries. One SQL
/// partition makes the run one stream whatever the host's parallelism. Under
/// a 73 MiB process budget the derived table term answers, and the answer is
/// the unlimited run's: the eight regions by their exact distinct counts.
///
/// FLIP: the table term of a capacity-accounted 16-byte-entry table routed
/// back to `unknown_layout_multiplier x J_max` refuses the bounded run, as
/// the envelope it replaced did.
#[tokio::test]
async fn statement_9_count_distinct_answers_under_a_budget_the_old_envelope_refused() {
    let store = distinct_store(604_800).await;
    let sql = "SELECT \"RegionID\", COUNT(DISTINCT \"UserID\") AS u FROM logs \
               GROUP BY \"RegionID\" ORDER BY u DESC LIMIT 10";
    let request = logs_request(sql);

    let unlimited = distinct_executor(&store, Arc::new(MemoryBudget::unlimited()));
    let accounting = QueryAccounting::new();
    let declared = unlimited
        .resolve_declared_columns(logs_tenant().hash(), request.now_ns)
        .await;
    let (snapshot, _) = unlimited
        .resolve_snapshot(logs_tenant().hash(), &request, &accounting)
        .await
        .expect("snapshot resolves");
    let plan = unlimited
        .plan_pinned(logs_tenant().hash(), snapshot, sql, &accounting, &declared)
        .await
        .expect("query plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds");
    assert_eq!(
        plan_aggregate_profile(&plan),
        Some(AggregateProfile::Layout(AggregateLayout {
            entry_size: 16,
            vec_width_total: 16,
            vec_width_max: 8,
            capacity_accounted: true,
        })),
        "{}",
        render(&plan)
    );

    let want: Vec<(i64, i64)> = (0..8).rev().map(|r| (r, 16_800 * (r + 1))).collect();
    let answer = unlimited
        .execute(logs_tenant().hash(), &request)
        .await
        .expect("the unlimited run answers");
    assert_eq!(region_counts(answer.output.batches()), want);

    let budget = Arc::new(MemoryBudget::new(73 << 20));
    let bounded = distinct_executor(&store, Arc::clone(&budget))
        .execute(logs_tenant().hash(), &request)
        .await
        .expect("statement 9 answers under a 73 MiB budget");
    assert_eq!(region_counts(bounded.output.batches()), want);
    assert_eq!(budget.accounted_refusals(), 0);
}
