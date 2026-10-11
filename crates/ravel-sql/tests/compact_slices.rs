//! Issue #2720: a grouped aggregate whose output feeds a round-robin
//! `RepartitionExec` is charged once per slice in flight, not once per slice
//! of the whole output.
//!
//! DataFusion 55.2 hands a grouped aggregate's output out as `batch_size`
//! slices of one materialized batch, and a round-robin `RepartitionExec`
//! reserves each slice's `get_array_memory_size`, which is the size of the
//! whole batch. The q29 shape (`HAVING count > n ORDER BY count DESC LIMIT
//! k` over a single-partition final aggregate, fanned back out to the
//! session's partitions for the `HAVING` filter) is reproduced here on a
//! metrics corpus small enough for a test and sliced finely enough
//! (`batch_size` 256 over 20,000 groups: 79 slices) that the over-count is
//! an order of magnitude or more.
//!
//! The rule is on in every session `build_session` makes; the "off" side
//! rebuilds the same session state without `CompactSlices`, which is the
//! only difference between the two measurements. The two sides are measured
//! at a 1 GiB pool, where neither refuses, and then run at a pool set between
//! their two peaks, where the rule-off side refuses and the rule-on side
//! answers.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigNonZeroUsize;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use ravel_query::{PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    COMPACT_SLICES_RULE, RavelTableProvider, SessionTable, SpillDecision, SqlConfig,
    TenantMemoryAccountant, build_session,
};
use ravel_types::TenantId;
use ravel_types::accounting::QueryAccounting;
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// Groups with one sample each.
const COLD_GROUPS: i64 = 20_000;

/// Groups `0..HOT_GROUPS` get extra samples: group `j` ends with `j + 2`.
const HOT_GROUPS: i64 = 10;

/// Rows per aggregate output slice. 20,000 groups over this is 79 slices.
const BATCH_SIZE: usize = 256;

/// SQL partitions: the round-robin fan-out after the final aggregate.
const PARTITIONS: usize = 4;

/// The q29 shape. `avg` keeps the statement off the exact-typed path, so the
/// final aggregate runs in one partition and DataFusion fans its output back
/// out with a round-robin `RepartitionExec` for the `HAVING` filter, as q29
/// does with its `AVG(length(Referer))`.
const TOP_K: &str = "SELECT concat('g', CAST(CAST(value AS BIGINT) AS VARCHAR)) AS k, \
                     count(value) AS n, min(CAST(ts AS VARCHAR)) AS s, avg(value) AS a \
                     FROM samples GROUP BY k HAVING count(value) > 1 \
                     ORDER BY 2 DESC LIMIT 10";

/// The same groups, every one of them, with no filter, sort or limit: the
/// aggregate's materialized output.
const ALL_GROUPS: &str = "SELECT concat('g', CAST(CAST(value AS BIGINT) AS VARCHAR)) AS k, \
                          count(value) AS n, min(CAST(ts AS VARCHAR)) AS s, avg(value) AS a \
                          FROM samples GROUP BY k";

fn tenant() -> TenantId {
    tenant_id("compact-slices-2720")
}

/// One sample per cold group, then `j + 1` extra samples for hot group `j`,
/// all in one series so the key is the `value` column alone.
fn corpus() -> Vec<SegSpec> {
    let mut samples: Vec<(i64, f64)> = (0..COLD_GROUPS).map(|i| (i, i as f64)).collect();
    let mut ts = COLD_GROUPS;
    for j in 0..HOT_GROUPS {
        for _ in 0..=j {
            samples.push((ts, j as f64));
            ts += 1;
        }
    }
    vec![SegSpec::new(10, 1, 1, vec![SeriesSpec::new("m", samples)])]
}

/// Hot group `j` has `j + 2` samples, so the top 10 by count are the hot
/// groups in descending `j`.
fn expected_top_k() -> Vec<(String, i64)> {
    (0..HOT_GROUPS)
        .rev()
        .map(|j| (format!("g{j}"), j + 2))
        .collect()
}

/// The pool size at which the two sides are measured: neither refuses there.
const MEASURE_POOL_BYTES: usize = 1 << 30;

/// The pool for the refuses-versus-answers test, as a multiple of the
/// aggregate's materialized output: between the measured rule-on peak
/// (2.22x) and rule-off peak (6.00x).
const REFUSAL_POOL_FACTOR: u64 = 4;

/// The substring DataFusion puts in the error a spill request raises when the
/// disk manager is disabled, which is how the exchange reports a pool it
/// could not grow.
const SPILL_DISABLED: &str = "DiskManager is disabled";

fn config(max_query_bytes: usize) -> SqlConfig {
    let mut config = SqlConfig::default();
    config.engine.sql_partition_count = Some(PARTITIONS);
    config.max_query_bytes = max_query_bytes;
    config
}

/// `ctx` with every physical optimizer rule but `CompactSlices`.
fn without_rule(ctx: &SessionContext) -> SessionContext {
    let state = ctx.state();
    let rules: Vec<_> = state
        .physical_optimizers()
        .iter()
        .filter(|rule| rule.name() != COMPACT_SLICES_RULE)
        .cloned()
        .collect();
    assert_eq!(
        rules.len() + 1,
        state.physical_optimizers().len(),
        "build_session installs the rule exactly once"
    );
    SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(state)
            .with_physical_optimizer_rules(rules)
            .build(),
    )
}

struct Run {
    plan: String,
    batches: Vec<RecordBatch>,
    peak_intermediate_bytes: u64,
}

/// Run `sql` on a fresh production session over `fixture` at the measuring
/// pool, with the rule left in or taken out; the query must succeed.
async fn run(fixture: &Fixture, sql: &str, rule_on: bool) -> Run {
    run_with_pool(fixture, sql, rule_on, MEASURE_POOL_BYTES)
        .await
        .expect("query runs")
}

/// Run `sql` on a fresh production session over `fixture` (the production
/// pool capped at `max_query_bytes`, spill disabled, `batch_size`
/// [`BATCH_SIZE`]), with the rule left in or taken out. A refused query comes
/// back as the error the pool raised.
async fn run_with_pool(
    fixture: &Fixture,
    sql: &str,
    rule_on: bool,
    max_query_bytes: usize,
) -> Result<Run, DataFusionError> {
    let config = config(max_query_bytes);
    let accountant = TenantMemoryAccountant::new(1 << 30);
    let accounting = QueryAccounting::new();
    let (pool, _breach) = config.query_pool(Arc::clone(&accountant), accounting.clone());
    let provider = Arc::new(RavelTableProvider::new(
        fixture.snapshot(&tenant()).await,
        tenant().hash(),
        SegmentFetcher::new(Arc::clone(&fixture.store)),
        config.clone(),
        PhaseAccounting::new(),
    ));
    let ctx = build_session(
        &config,
        Arc::clone(&pool) as Arc<dyn MemoryPool>,
        SessionTable::Metrics(provider),
        false,
        SpillDecision::Disabled,
    )
    .expect("session");
    let ctx = if rule_on { ctx } else { without_rule(&ctx) };
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .batch_size = ConfigNonZeroUsize::try_new(BATCH_SIZE).expect("non-zero");

    let physical: Arc<dyn ExecutionPlan> = ctx
        .sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan = displayable(physical.as_ref()).indent(true).to_string();
    let batches = collect(physical, ctx.task_ctx()).await?;
    assert_eq!(
        pool.reserved(),
        0,
        "the pool returns to zero after the query"
    );
    Ok(Run {
        plan,
        batches,
        peak_intermediate_bytes: accounting.snapshot().peak_intermediate_bytes,
    })
}

/// One batch of all of `batches`, for a whole-output comparison.
fn one_batch(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches.first().expect("at least one batch").schema();
    concat_batches(&schema, batches).expect("one schema")
}

/// `(k, n)` per row, in output order.
fn rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let keys = cast(batch.column(0), &DataType::Utf8).expect("k casts");
        let keys = keys.as_string::<i32>();
        let counts = cast(batch.column(1), &DataType::Int64).expect("n casts");
        let counts = counts.as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            out.push((keys.value(row).to_string(), counts.value(row)));
        }
    }
    out
}

/// Index of the first plan line starting (after indentation) with `prefix`.
fn line_of(plan: &str, prefix: &str) -> usize {
    plan.lines()
        .position(|line| line.trim_start().starts_with(prefix))
        .unwrap_or_else(|| panic!("no line starts with {prefix:?}:\n{plan}"))
}

/// The `RepartitionExec` must sit above the `AggregateExec` for the plan to
/// carry the edge under test; with the rule on the exec sits between them.
fn assert_round_robin_over_final_aggregate(plan: &str, rule_on: bool) {
    let repartition = line_of(
        plan,
        &format!("RepartitionExec: partitioning=RoundRobinBatch({PARTITIONS})"),
    );
    let aggregate = line_of(plan, "AggregateExec: mode=Final");
    let execs = plan
        .lines()
        .filter(|line| line.trim_start().starts_with("CompactSlicesExec"))
        .count();
    if rule_on {
        assert_eq!(execs, 1, "one exec with the rule on:\n{plan}");
        assert_eq!(
            line_of(plan, "CompactSlicesExec"),
            repartition + 1,
            "the exec is directly below the repartition:\n{plan}"
        );
        assert_eq!(
            aggregate,
            repartition + 2,
            "the final aggregate is directly below the exec:\n{plan}"
        );
    } else {
        assert_eq!(execs, 0, "no exec with the rule off:\n{plan}");
        assert_eq!(
            aggregate,
            repartition + 1,
            "the final aggregate is directly below the repartition:\n{plan}"
        );
    }
}

/// With the rule on, the query's peak intermediate bytes stay under 3x the
/// aggregate's materialized output; with the rule off every slice in flight
/// at the repartition is charged the whole output, and the peak is an exact
/// multiple of it. Both sides return the same batches, every column of them.
///
/// Measured: output 2,211,069 bytes in 79 slices; rule on peak 4,912,669
/// (2.22x), rule off peak 13,266,414 (6.00x: the aggregate's own
/// reservation of its output plus five slices in flight at the exchange).
///
/// FLIP (rule off): measured in this same test; the rule-off peak fails the
/// 3x bound by itself. FLIP (a copy that does not shrink the charge): make
/// `compact_array` return `Ok(Arc::clone(array))` in the module and the
/// rule-on peak reads 13,266,414, the rule-off figure.
#[tokio::test]
async fn the_peak_with_the_rule_on_is_bounded_by_the_output_and_the_rows_match() {
    let specs = corpus();
    let fixture = Fixture::memory(&[(&tenant(), &specs)]).await;

    // The unfiltered aggregate has no repartition or sort above it, so the
    // rule leaves it alone and its output arrives as the slices of one
    // materialized batch: every slice reports the whole batch's
    // `get_array_memory_size`, which is the figure a consumer charges per
    // slice.
    let all_groups = run(&fixture, ALL_GROUPS, true).await;
    assert!(
        !all_groups.plan.contains("CompactSlicesExec"),
        "the unfiltered aggregate feeds the plan root and gets no exec:\n{}",
        all_groups.plan
    );
    let rows_out: usize = all_groups.batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows_out as i64, COLD_GROUPS, "one row per group");
    let slices = all_groups.batches.len() as u64;
    assert!(
        slices >= 64,
        "the output must span at least 64 slices, got {slices}"
    );
    let output_bytes = all_groups.batches[0].get_array_memory_size() as u64;
    for (i, slice) in all_groups.batches.iter().enumerate() {
        assert_eq!(
            slice.get_array_memory_size() as u64,
            output_bytes,
            "slice {i} of {slices} reports the whole materialized output"
        );
    }

    let on = run(&fixture, TOP_K, true).await;
    let off = run(&fixture, TOP_K, false).await;
    eprintln!(
        "materialized output: {output_bytes} bytes in {slices} slices of {BATCH_SIZE}\n\
         rule on:  peak {} bytes ({:.2}x output)\n\
         rule off: peak {} bytes ({:.2}x output)\n\
         plan with the rule on:\n{}\nplan with the rule off:\n{}",
        on.peak_intermediate_bytes,
        on.peak_intermediate_bytes as f64 / output_bytes as f64,
        off.peak_intermediate_bytes,
        off.peak_intermediate_bytes as f64 / output_bytes as f64,
        on.plan,
        off.plan
    );

    assert_round_robin_over_final_aggregate(&on.plan, true);
    assert_round_robin_over_final_aggregate(&off.plan, false);

    assert_eq!(
        rows(&on.batches),
        expected_top_k(),
        "the rows with the rule on"
    );
    assert_eq!(
        rows(&off.batches),
        expected_top_k(),
        "the rows with the rule off"
    );
    assert_eq!(
        one_batch(&on.batches),
        one_batch(&off.batches),
        "every column, the gc'd `s` and the concatenated `a` included, is the same on both sides"
    );

    assert!(
        on.peak_intermediate_bytes < 3 * output_bytes,
        "with the rule on the peak ({}) stays under 3x the materialized output ({output_bytes})",
        on.peak_intermediate_bytes
    );
    assert!(
        off.peak_intermediate_bytes >= 3 * output_bytes,
        "with the rule off the slices in flight are each charged the whole output: \
         peak {} against output {output_bytes}",
        off.peak_intermediate_bytes
    );
}

/// At a pool set between the two measured peaks ([`REFUSAL_POOL_FACTOR`]
/// times the aggregate's materialized output: 8,844,276 bytes against peaks
/// of 4,912,669 with the rule on and 13,266,414 off) the rule-off run is
/// refused with the pool's spill-disabled error and the rule-on run answers
/// with the expected rows.
///
/// FLIP: make `compact_array` return `Ok(Arc::clone(array))` in the module
/// and the rule-on run is refused too, with the same error.
#[tokio::test]
async fn between_the_two_peaks_the_rule_off_run_refuses_and_the_rule_on_run_answers() {
    let specs = corpus();
    let fixture = Fixture::memory(&[(&tenant(), &specs)]).await;
    let all_groups = run(&fixture, ALL_GROUPS, true).await;
    let output_bytes = all_groups.batches[0].get_array_memory_size() as u64;
    let pool_bytes = usize::try_from(REFUSAL_POOL_FACTOR * output_bytes).expect("fits");

    let refused = run_with_pool(&fixture, TOP_K, false, pool_bytes)
        .await
        .err()
        .unwrap_or_else(|| panic!("the rule-off run must refuse at a {pool_bytes}-byte pool"));
    assert!(
        matches!(refused, DataFusionError::ResourcesExhausted(_))
            && refused.to_string().contains(SPILL_DISABLED),
        "the rule-off run is refused by the pool, spill disabled: {refused}"
    );

    let answered = run_with_pool(&fixture, TOP_K, true, pool_bytes)
        .await
        .unwrap_or_else(|e| panic!("the rule-on run must answer at a {pool_bytes}-byte pool: {e}"));
    assert_round_robin_over_final_aggregate(&answered.plan, true);
    assert_eq!(rows(&answered.batches), expected_top_k());
    assert!(
        answered.peak_intermediate_bytes < pool_bytes as u64,
        "the rule-on peak ({}) fits the pool ({pool_bytes})",
        answered.peak_intermediate_bytes
    );
}

/// The production path: the same statement through `SqlExecutor`, as the
/// server runs it, plans the exec under the round-robin repartition and
/// returns the same rows.
#[tokio::test]
async fn the_executor_plans_the_exec_under_the_round_robin_repartition() {
    let specs = corpus();
    let fixture = Fixture::build(
        Arc::new(ravel_object_store::memory::MemoryStore::new()),
        &[(&tenant(), &specs)],
        config(MEASURE_POOL_BYTES),
        1 << 30,
    )
    .await;
    let accounting = QueryAccounting::new();
    let req = request(TOP_K);
    let declared = fixture
        .executor
        .resolve_declared_columns(tenant().hash(), req.now_ns)
        .await;
    let (snapshot, _) = fixture
        .executor
        .resolve_snapshot(tenant().hash(), &req, &accounting)
        .await
        .expect("snapshot resolves");
    let planned = fixture
        .executor
        .plan_pinned(tenant().hash(), snapshot, TOP_K, &accounting, &declared)
        .await
        .expect("query plans");
    let physical = planned
        .create_physical_plan()
        .await
        .expect("physical plan builds");
    let plan = displayable(physical.as_ref()).indent(true).to_string();
    eprintln!("{plan}");
    assert_round_robin_over_final_aggregate(&plan, true);

    let outcome = fixture
        .executor
        .execute(tenant().hash(), &req)
        .await
        .expect("query runs");
    let batches: Vec<RecordBatch> = outcome.output.batches().to_vec();
    assert_eq!(rows(&batches), expected_top_k());
}
