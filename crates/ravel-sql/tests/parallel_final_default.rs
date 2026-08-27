//! Issue #741 (ADR-0094 amendment): `parallel_final_aggregation` is on by
//! default, so an exact-typed logs aggregate repartitions its final stage with
//! no operator flag set. These tests drive the real
//! `SqlExecutor::plan_pinned` / `execute` path over a `logs` table with a
//! declared `Int64` column, so they prove the whole chain the amendment turns
//! on by default: the per-query classification (ADR-0094 decision 1), the
//! `repartition_aggregations` flip (decision 2), and the `SqlConfig`
//! default itself.
//!
//! Three properties are pinned:
//!
//! - `default_session_count_distinct_int_repartitions_final`: with
//!   `SqlConfig::default()` (flag now on), the physical plan of
//!   `SELECT COUNT(DISTINCT k) FROM logs` over a declared `Int64` `k` carries a
//!   `AggregateExec: mode=FinalPartitioned` above a
//!   `RepartitionExec: partitioning=Hash`. This is the assertion that goes red
//!   if the default is flipped back to `false` (prove-the-test: the single
//!   flipped line is `parallel_final_aggregation: true` in
//!   crates/ravel-sql/src/config.rs).
//! - `opt_out_count_distinct_int_stays_single_partition`: the same statement
//!   with the opt-out (`parallel_final_aggregation: false`) keeps
//!   `mode=Final` above `CoalescePartitionsExec` and carries neither the
//!   `FinalPartitioned` mode nor a `Hash` repartition.
//! - `avg_stays_single_partition_under_default`: a non-exact-typed plan (`avg`,
//!   resolved to `Float64`) keeps the single-partition final under the new
//!   default, proving the default flip did not weaken the classification gate.
//! - `both_plan_shapes_agree_on_the_count`: the two plan shapes return the
//!   identical `COUNT(DISTINCT k)` over a several-partition `MemoryStore`
//!   fixture.
//!
//! Issue #771 proposed admitting `avg` over an integer column on the premise
//! that its partial state is an exact `(sum, count)` pair. The ADR-0094
//! amendment for #771 rejects that premise; three more tests pin the facts the
//! rejection rests on. They are not a uniform staleness guard, and the
//! amendment states which branch each one covers: a decimal partial state
//! upstream reddens the first, an integer one does not (this crate's
//! `SequentialAvg` displaces DataFusion's accumulator whenever the return type
//! is Float64, so upstream's state never reaches the plan), and the
//! order-dependence test is a pinned illustration that consults no symbol from
//! either crate.
//!
//! - `avg_over_int_column_carries_a_float64_partial_sum_state`: the `Partial`
//!   aggregate for `avg(k)` over a declared `Int64` `k` emits a **Float64**
//!   partial-sum state column, not an integer or decimal one.
//! - `f64_partial_sum_merge_is_order_dependent_for_integer_input`: adding
//!   Float64 partial sums of ordinary `i64` values in two different orders
//!   yields two different bit patterns, so merging avg's partial states across
//!   partitions is not exact.
//! - `avg_group_by_int_key_agrees_and_stays_single_final` and
//!   `avg_over_float_input_stays_single_partition`: a `GROUP BY` avg over the
//!   integer column returns identical pinned values with the flag on and off
//!   and keeps the single-partition final in both, even though its group key is
//!   the non-float kind ADR-0094 otherwise admits; the float-input avg keeps it
//!   too.
//!
//! Both of those carry a `GROUP BY` because an **ungrouped** avg is the wrong
//! shape to test the gate with: DataFusion does not repartition a single-row
//! aggregate, so `avg_stays_single_partition_under_default` above holds whether
//! or not the classifier admits avg (verified by admitting it and watching that
//! test stay green while the grouped ones went red). The grouped shapes are the
//! load-bearing guards.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Float64Array, Int64Array};
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
/// `fetch_concurrency == OBJECTS`, so the fanned-out plan really reaches this
/// many partitions.
const OBJECTS: usize = 4;

/// Distinct values of the declared `Int64` key `k`. Each object writes all of
/// them, so every scan partition holds the whole value space and a repartition
/// of the final aggregate has real work to move.
const DISTINCT_K: i64 = 50;

fn tenant() -> TenantId {
    TenantId::new("adr94-741-parallel-default".to_string())
}

/// The one declared column under test: an `Int64` attribute `k`. A
/// `COUNT(DISTINCT k)` over it is exact-typed under ADR-0094 decision 1.
fn declared() -> Vec<DeclaredColumn> {
    vec![DeclaredColumn::new("k", DeclaredType::I64)]
}

/// One log record on a fixed single-`service.name` stream carrying `k=value`.
fn record(ts: i64, value: i64) -> LogRecord {
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
        attrs: vec![("k".to_string(), AttrValue::I64(value))],
    }
}

/// Publish one RLOG object (index `object`) carrying every value in
/// `0..DISTINCT_K`, on a ts range disjoint from the other objects.
async fn publish_object(store: &dyn ObjectStoreBackend, tenant: &TenantId, object: usize) {
    let base_ts = object as i64 * 100_000;
    let records: Vec<LogRecord> = (0..DISTINCT_K).map(|v| record(base_ts + v, v)).collect();

    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: *Uuid::from_u128(0x741_0000 + object as u128).as_bytes(),
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
        writer_id: Uuid::from_u128(0x741_0000 + object as u128),
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
        segment_format_version: 1,
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

/// A logs fixture over `OBJECTS` RLOG objects, with an executor whose
/// `SqlConfig` is `config` and whose declared column source carries `k`.
struct Fixture {
    executor: SqlExecutor,
    catalog: Arc<Catalog>,
}

impl Fixture {
    async fn build(config: SqlConfig) -> Self {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let t = tenant();
        for object in 0..OBJECTS {
            publish_object(store.as_ref(), &t, object).await;
        }
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
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

    /// A config with the ADR-0094 flag set as given and a fan-out that reaches
    /// every published object.
    fn config(parallel_final_aggregation: bool) -> SqlConfig {
        SqlConfig {
            engine: EngineConfig {
                fetch_concurrency: OBJECTS,
                ..EngineConfig::default()
            },
            parallel_final_aggregation,
            ..SqlConfig::default()
        }
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

    /// The indented physical-plan text for `sql`, planned through the real
    /// `plan_pinned` path (so the classification ran) with `k` declared.
    async fn physical_plan_text(&self, sql: &str) -> String {
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
        format!("{}", displayable(plan.as_ref()).indent(true))
    }

    /// The same physical plan as [`Self::physical_plan_text`], rendered with
    /// each operator's output schema, so an aggregate's partial-state column
    /// types are visible in the text (issue #771).
    async fn physical_plan_text_with_schema(&self, sql: &str) -> String {
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

    /// Every `(group_key, avg)` row `sql` returns, sorted by group key. The
    /// avg column is read as raw bits so the comparison is bit-exact.
    async fn grouped_avg_rows(&self, sql: &str) -> Vec<(i64, u64)> {
        let outcome = self
            .executor
            .execute(tenant().hash(), &request(sql))
            .await
            .expect("query executes");
        let mut rows = Vec::new();
        for batch in outcome.output.batches() {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("group key is Int64");
            let avgs = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("avg is Float64");
            for row in 0..batch.num_rows() {
                rows.push((keys.value(row), avgs.value(row).to_bits()));
            }
        }
        rows.sort_unstable();
        rows
    }

    /// The single scalar `COUNT(DISTINCT k)` returns, executed end to end.
    async fn count_distinct_k(&self) -> i64 {
        let outcome = self
            .executor
            .execute(tenant().hash(), &request(COUNT_DISTINCT_SQL))
            .await
            .expect("query executes");
        let batch = outcome
            .output
            .batches()
            .iter()
            .find(|b| b.num_rows() == 1)
            .cloned()
            .expect("one scalar row");
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("a count aggregate is Int64")
            .value(0)
    }
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
    }
}

/// The exact-typed statement under test: a single-distinct count over the
/// declared `Int64` key. DataFusion rewrites it into a `GROUP BY k` under a
/// scalar count, so the group aggregate below the count is the partial/final
/// pair ADR-0094 repartitions.
const COUNT_DISTINCT_SQL: &str = "SELECT COUNT(DISTINCT k) AS d FROM logs";

/// The 0-based line index of the first plan line containing `needle`.
fn line_of(plan: &str, needle: &str) -> usize {
    plan.lines()
        .position(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("expected a plan line containing {needle:?}; got:\n{plan}"))
}

#[tokio::test]
async fn default_session_count_distinct_int_repartitions_final() {
    // SqlConfig::default() carries the ADR-0094 amendment default (flag on), so
    // no explicit set here: this is exactly the shape a shipped server plans.
    let fixture = Fixture::build(Fixture::config(
        SqlConfig::default().parallel_final_aggregation,
    ))
    .await;
    assert!(
        SqlConfig::default().parallel_final_aggregation,
        "the amendment default must be on; this test proves what that default plans"
    );

    let plan = fixture.physical_plan_text(COUNT_DISTINCT_SQL).await;
    assert!(
        plan.contains("AggregateExec: mode=FinalPartitioned"),
        "an exact-typed COUNT(DISTINCT int) under the default must fan its final \
         aggregation across partitions (FinalPartitioned); got:\n{plan}"
    );
    assert!(
        plan.contains("RepartitionExec: partitioning=Hash"),
        "the fanned-out final aggregate must be fed by a hash repartition; got:\n{plan}"
    );
    // The FinalPartitioned aggregate sits above the hash repartition that feeds
    // it (parents print first in an indented plan).
    assert!(
        line_of(&plan, "AggregateExec: mode=FinalPartitioned")
            < line_of(&plan, "RepartitionExec: partitioning=Hash"),
        "FinalPartitioned must sit above the Hash repartition that feeds it; got:\n{plan}"
    );
}

#[tokio::test]
async fn opt_out_count_distinct_int_stays_single_partition() {
    let fixture = Fixture::build(Fixture::config(false)).await;
    let plan = fixture.physical_plan_text(COUNT_DISTINCT_SQL).await;

    // The opt-out restores the single-partition final: the group aggregate is
    // gathered under CoalescePartitionsExec, not repartitioned.
    assert!(
        !plan.contains("AggregateExec: mode=FinalPartitioned"),
        "the opt-out must not fan the final aggregation out; got:\n{plan}"
    );
    assert!(
        !plan.contains("RepartitionExec: partitioning=Hash"),
        "the opt-out must not insert a hash repartition for the aggregate; got:\n{plan}"
    );
    assert!(
        plan.contains("AggregateExec: mode=Final,"),
        "the opt-out keeps a single-partition Final aggregate; got:\n{plan}"
    );
    assert!(
        line_of(&plan, "AggregateExec: mode=Final,") < line_of(&plan, "CoalescePartitionsExec"),
        "the single-partition Final aggregate must sit above the CoalescePartitionsExec \
         that gathers its input; got:\n{plan}"
    );
}

#[tokio::test]
async fn avg_stays_single_partition_under_default() {
    // avg resolves to Float64 (a float accumulator), so it is not exact-typed
    // and must keep the single-partition final even though the default is on.
    let fixture = Fixture::build(Fixture::config(
        SqlConfig::default().parallel_final_aggregation,
    ))
    .await;
    let plan = fixture
        .physical_plan_text("SELECT avg(k) AS a FROM logs")
        .await;

    assert!(
        !plan.contains("AggregateExec: mode=FinalPartitioned"),
        "a float avg must never fan its final aggregation out, default or not; got:\n{plan}"
    );
    assert!(
        !plan.contains("RepartitionExec: partitioning=Hash"),
        "a float avg must never get a hash repartition, default or not; got:\n{plan}"
    );
    assert!(
        plan.contains("AggregateExec: mode=Final,"),
        "the non-exact avg keeps the single-partition Final aggregate; got:\n{plan}"
    );
}

/// The `GROUP BY` avg under test (issue #771). Its group key is `Int64`, the
/// non-float kind ADR-0094 admits, and its aggregate argument is the declared
/// `Int64` column, so `avg` is the only reason this plan is not exact-typed.
/// Group 1 (`k` in 1, 5, 7 across every object) has a sum that does not divide
/// evenly by its count, so the pinned value exercises the float division.
const GROUP_BY_AVG_SQL: &str =
    "SELECT k % 2 AS g, avg(k) AS a FROM logs WHERE k IN (1, 2, 5, 7, 8) GROUP BY g ORDER BY g";

/// The exact rows `GROUP_BY_AVG_SQL` must return on this fixture, avg as bits.
///
/// Every object writes each `k` once, so group 0 folds `2` and `8` `OBJECTS`
/// times each (sum 40, count 8) and group 1 folds `1`, `5` and `7` `OBJECTS`
/// times each (sum 52, count 12). 52/12 is 13/3, which has no finite binary
/// expansion: the expected bits are the correctly rounded IEEE quotient, not a
/// transcribed decimal.
fn expected_grouped_avg_rows() -> Vec<(i64, u64)> {
    vec![(0, 5.0_f64.to_bits()), (1, (13.0_f64 / 3.0).to_bits())]
}

#[tokio::test]
async fn avg_over_int_column_carries_a_float64_partial_sum_state() {
    // Issue #771's premise was that avg over an integer column carries an exact
    // (sum, count) partial state, making a cross-partition merge exact. It does
    // not: DataFusion 54.1.0 coerces every non-decimal, non-duration avg
    // argument to Float64 (average.rs:101-113 signature, :168 return type,
    // :179 comment), and the accumulator that actually serves this plan holds
    // the numerator as f64: this crate's `SequentialAvgAccumulator` (avg.rs),
    // registered over the built-in. DataFusion's own `AvgAccumulator { sum:
    // Option<f64>, count: u64 }` (average.rs:505-508) is the same shape, so the
    // premise fails either way, but only the replacement is observable here.
    // The partial state column type in the plan is the observable form.
    let fixture = Fixture::build(Fixture::config(
        SqlConfig::default().parallel_final_aggregation,
    ))
    .await;
    let plan = fixture
        .physical_plan_text_with_schema("SELECT avg(k) AS a FROM logs")
        .await;

    let partial = plan
        .lines()
        .find(|line| line.contains("AggregateExec: mode=Partial"))
        .unwrap_or_else(|| panic!("expected a Partial AggregateExec; got:\n{plan}"));
    assert!(
        partial.contains("[avg_sum]:Float64"),
        "avg's partial sum state over an Int64 column must be Float64 (issue #771's \
         exact-(sum, count) premise is false); got:\n{partial}"
    );
    assert!(
        partial.contains("[avg_count]:Int64"),
        "avg's partial count state must be an integer count; got:\n{partial}"
    );
    // The exact-typed aggregate for the same column, for contrast: sum(k)
    // really does carry an Int64 partial state, which is why sum over an
    // integer input is admitted and avg is not.
    let sum_plan = fixture
        .physical_plan_text_with_schema("SELECT sum(k) AS s FROM logs")
        .await;
    let sum_partial = sum_plan
        .lines()
        .find(|line| line.contains("AggregateExec: mode=Partial"))
        .unwrap_or_else(|| panic!("expected a Partial AggregateExec; got:\n{sum_plan}"));
    assert!(
        sum_partial.contains("sum(logs.k)[sum]:Int64"),
        "sum over an Int64 column must carry an Int64 partial state; got:\n{sum_partial}"
    );
}

#[tokio::test]
async fn avg_over_float_input_stays_single_partition() {
    // The other half of the #771 amendment: an avg whose input is float before
    // coercion is excluded too. (The logs surface declares no float column
    // type, so the float input is an explicit cast; the planner then drops the
    // cast, because Float64 is where avg's coercion sends an Int64 argument
    // anyway. That collapse is the amendment's second point in visible form:
    // after analysis, avg over an integer column and avg over a float one are
    // the same plan, so the classifier has no resolved type to admit one by.)
    // The statement carries an Int64 GROUP BY key for the reason the module doc
    // gives: an ungrouped aggregate is never repartitioned whatever the
    // classifier says, so only a grouped shape can tell an excluded avg from an
    // admitted one.
    let fixture = Fixture::build(Fixture::config(
        SqlConfig::default().parallel_final_aggregation,
    ))
    .await;
    let plan = fixture
        .physical_plan_text("SELECT k % 2 AS g, avg(CAST(k AS DOUBLE)) AS a FROM logs GROUP BY g")
        .await;

    assert!(
        !plan.contains("AggregateExec: mode=FinalPartitioned"),
        "avg over a float input must not fan its final aggregation out; got:\n{plan}"
    );
    assert!(
        !plan.contains("RepartitionExec: partitioning=Hash"),
        "avg over a float input must not get a hash repartition; got:\n{plan}"
    );
    assert!(
        plan.contains("AggregateExec: mode=Final,"),
        "avg over a float input keeps the single-partition Final aggregate; got:\n{plan}"
    );
}

#[test]
fn f64_partial_sum_merge_is_order_dependent_for_integer_input() {
    // Three ordinary i64 values a log column can hold. Folded as f64 (which is
    // what avg does, per the test above), the two groupings a repartitioned
    // merge can produce differ in the last bit: 2^53 + 1 rounds back to 2^53,
    // while 1 + 1 = 2 survives and 2^53 + 2 is representable. So the partial
    // sums of a group split across partitions do not merge exactly, and the
    // division that follows inherits the difference.
    let values: [i64; 3] = [1 << 53, 1, 1];
    let one_partition = ((values[0] as f64) + values[1] as f64) + values[2] as f64;
    let two_partitions = (values[0] as f64) + ((values[1] as f64) + values[2] as f64);

    assert_eq!(
        one_partition.to_bits(),
        (9_007_199_254_740_992.0_f64).to_bits()
    );
    assert_eq!(
        two_partitions.to_bits(),
        (9_007_199_254_740_994.0_f64).to_bits()
    );
    assert_ne!(
        one_partition.to_bits(),
        two_partitions.to_bits(),
        "if f64 addition of these i64 values were associative, avg over integer \
         input could merge across partitions exactly (issue #771)"
    );
}

#[tokio::test]
async fn avg_group_by_int_key_agrees_and_stays_single_final() {
    let on = Fixture::build(Fixture::config(true)).await;
    let off = Fixture::build(Fixture::config(false)).await;

    let on_rows = on.grouped_avg_rows(GROUP_BY_AVG_SQL).await;
    let off_rows = off.grouped_avg_rows(GROUP_BY_AVG_SQL).await;

    assert_eq!(
        on_rows,
        expected_grouped_avg_rows(),
        "the pinned per-group averages must come back bit for bit"
    );
    assert_eq!(
        on_rows, off_rows,
        "avg over an integer column must return identical bits with the flag on and off"
    );

    // And it stays on the single-partition final in both, which is why the two
    // agree: the group key is Int64 (admissible on its own), so avg is the only
    // disqualifier here.
    for (label, plan) in [
        ("flag on", on.physical_plan_text(GROUP_BY_AVG_SQL).await),
        ("flag off", off.physical_plan_text(GROUP_BY_AVG_SQL).await),
    ] {
        assert!(
            !plan.contains("AggregateExec: mode=FinalPartitioned"),
            "{label}: an avg must never fan its final aggregation out; got:\n{plan}"
        );
        assert!(
            plan.contains("AggregateExec: mode=Final,"),
            "{label}: the avg keeps the single-partition Final aggregate; got:\n{plan}"
        );
    }
}

#[tokio::test]
async fn both_plan_shapes_agree_on_the_count() {
    let on = Fixture::build(Fixture::config(true)).await;
    let off = Fixture::build(Fixture::config(false)).await;

    let on_count = on.count_distinct_k().await;
    let off_count = off.count_distinct_k().await;

    assert_eq!(
        on_count, DISTINCT_K,
        "the repartitioned final must count every distinct key value"
    );
    assert_eq!(
        on_count, off_count,
        "the repartitioned and single-partition plans must return the identical count"
    );
}
