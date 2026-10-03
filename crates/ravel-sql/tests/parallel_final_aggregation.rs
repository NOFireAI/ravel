//! ADR-0094 decision 5: end-to-end tests for parallel final aggregation on
//! exact-typed queries.
//!
//! Every test drives the real `execute` / `plan_pinned` path (not the
//! classification helper in isolation), so it proves the whole chain: the
//! per-query classification (decision 1), the `repartition_aggregations` config
//! flip (decision 2), and the `SqlConfig::parallel_final_aggregation` gate
//! (decision 4).
//!
//! The four tests mirror decision 5:
//!
//! - `exact_typed_group_by_is_bit_identical_across_partition_counts`: the
//!   differential. The same exact-typed `GROUP BY` query and data at
//!   `fetch_concurrency` 1/4/8/16 (each repartitioned count run several times to
//!   surface a merge-order race), sorted by the group-by key, must equal the
//!   shipped single-partition baseline bit-for-bit -- including an integer sum
//!   that crosses 2^53 within one group (the case that would have caught the
//!   `avg`-exactness mistake this ADR records). The `fetch_concurrency=1` +
//!   flag-on run is compared against flag-off both by result and by physical
//!   plan shape (they must converge).
//! - `disqualified_queries_stay_single_partition`: a float aggregate, a float
//!   GROUP BY key, and `SELECT DISTINCT float_col` each keep the
//!   single-partition plan shape (no `RepartitionExec`) regardless of the flag.
//! - `subquery_hidden_disqualifier_stays_single_partition`: a disqualifying
//!   aggregate hidden inside a scalar subquery forces the single-partition plan.
//! - `count_distinct_float_is_bit_identical_across_partition_counts`: the
//!   post-merge fable-review case -- `count(DISTINCT float_col)` over -0.0/0.0
//!   pairs and mixed-payload NaNs split across partitions, bit-identical across
//!   partition counts.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use datafusion::physical_plan::displayable;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::EngineConfig;
use ravel_sql::SqlConfig;
use ravel_types::TenantId;
use ravel_types::accounting::QueryAccounting;
use std::sync::Arc;

use util::gate::{self, Cell, Row};
use util::{Fixture, SegSpec, SeriesSpec, request};

/// The one tenant every test in this file uses.
fn tenant() -> TenantId {
    util::tenant_id("adr94-parallel-final-agg")
}

/// A value that casts to a large integer (`4e15`, exactly representable as
/// `f64`): three of them in one group sum to `1.2e16`, past 2^53, so an integer
/// accumulator and a float accumulator would disagree if the classifier ever
/// misrouted an exact integer sum through a float path.
const BIG: f64 = 4_000_000_000_000_000.0;

/// The exact integer sum of the `big` group: `3 * 4e15`.
const BIG_SUM: i64 = 12_000_000_000_000_000;

/// Two distinct NaN bit payloads: this repo treats a NaN's payload as
/// significant, so `count(DISTINCT)` must keep them apart, and a third sample
/// reusing `NAN_A` (on a different ts, in a different segment) must collapse
/// back into it.
fn nan_a() -> f64 {
    f64::from_bits(0x7ff8_0000_0000_0001)
}

fn nan_b() -> f64 {
    f64::from_bits(0x7ff8_0000_0000_0002)
}

/// The shared dataset: several string-keyed metric groups over three segments
/// with distinct provenance, so the dedup total order and multi-partition scan
/// both have something real to act on. Every sample in a series carries a
/// distinct `ts`, so nothing is deduplicated away and each contributes to its
/// group's aggregate. Includes:
///
/// - `big`: three `4e15` samples, one per segment, for the 2^53-crossing sum.
/// - `floats`: `-0.0`/`0.0` and mixed-payload NaNs split across partitions, for
///   `count(DISTINCT float_col)`.
fn dataset() -> Vec<SegSpec> {
    let seg1 = SegSpec::new(
        10,
        1,
        1,
        vec![
            SeriesSpec::new("m0", vec![(100, 1.0), (101, 2.0)]),
            SeriesSpec::new("m1", vec![(100, 3.0)]),
            SeriesSpec::new("m2", vec![(100, 5.0), (101, 6.0)]),
            SeriesSpec::new("big", vec![(200, BIG)]),
            SeriesSpec::new("floats", vec![(300, -0.0), (301, nan_a())]),
        ],
    );
    let seg2 = SegSpec::new(
        20,
        1,
        2,
        vec![
            SeriesSpec::new("m0", vec![(110, 4.0)]),
            SeriesSpec::new("m3", vec![(100, 7.0), (101, 8.0)]),
            SeriesSpec::new("m4", vec![(100, 9.0)]),
            SeriesSpec::new("big", vec![(201, BIG)]),
            SeriesSpec::new("floats", vec![(310, 0.0), (311, nan_b())]),
        ],
    );
    let seg3 = SegSpec::new(
        30,
        1,
        3,
        vec![
            SeriesSpec::new("m5", vec![(100, 10.0), (101, 11.0)]),
            SeriesSpec::new("m1", vec![(110, 12.0)]),
            SeriesSpec::new("big", vec![(202, BIG)]),
            // A third NaN reusing NAN_A's payload, distinct ts, distinct
            // segment: DISTINCT must collapse it back into the single NAN_A.
            SeriesSpec::new("floats", vec![(320, nan_a())]),
        ],
    );
    vec![seg1, seg2, seg3]
}

/// A fixture over a fresh store carrying [`dataset`], with the given scan
/// `fetch_concurrency` (the SQL `target_partitions`) and the ADR-0094 flag.
async fn fixture_with(fetch_concurrency: usize, parallel_final_aggregation: bool) -> Fixture {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let config = SqlConfig {
        engine: EngineConfig {
            fetch_concurrency,
            ..EngineConfig::default()
        },
        parallel_final_aggregation,
        ..SqlConfig::default()
    };
    let t = tenant();
    let specs = dataset();
    Fixture::build(store, &[(&t, &specs[..])], config, 1 << 30).await
}

/// The result rows of `sql`, sorted by the group-by key (column 0), reduced to
/// bit-exact comparable cells. `GROUP BY` has no row-order guarantee (unchanged
/// by ADR-0094), and repartitioned merge order is completion-order-dependent, so
/// content is compared under a canonical order, never arrival order.
async fn sorted_rows(fixture: &Fixture, sql: &str) -> Vec<Row> {
    let outcome = fixture
        .executor
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("query executes");
    let mut rows = gate::actual_rows(&outcome.output);
    rows.sort_by_key(group_key);
    rows
}

/// The group-by key of a row (column 0) as a sortable string. Group-by keys are
/// unique per row, so this is a total order over the result.
fn group_key(row: &Row) -> String {
    match &row[0] {
        Cell::Text(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Whether `plan` fans its final aggregation across partitions. The marker is a
/// hash-partitioning `RepartitionExec` (`partitioning=Hash(...)`), which
/// `EnforceDistribution` inserts above the scan to feed a fanned-out `Final`
/// `AggregateExec` only when `repartition_aggregations` is on and the aggregate
/// has group keys. This is distinct from the `RoundRobinBatch` repartition
/// DataFusion always inserts to parallelize the scan, which is present under both
/// plans and is not the ADR-0094 behavior.
fn fans_out_final_aggregation(plan: &str) -> bool {
    plan.contains("partitioning=Hash(")
}

/// The indented physical-plan text for `sql`, planned through the real
/// `plan_pinned` path (so the classification ran) over the fixture's snapshot.
async fn physical_plan_text(fixture: &Fixture, sql: &str) -> String {
    let t = tenant();
    let snapshot = fixture.snapshot(&t).await;
    let accounting = QueryAccounting::new();
    let planned = fixture
        .executor
        .plan_pinned(t.hash(), snapshot, sql, &accounting, &[])
        .await
        .expect("plan_pinned");
    let plan = planned
        .create_physical_plan()
        .await
        .expect("physical plan builds");
    format!("{}", displayable(plan.as_ref()).indent(true))
}

/// The exact-typed query under differential test: a string group key, an
/// integer sum (via `CAST`), and a count. All three are exact-eligible, so with
/// the flag on this repartitions its final aggregation.
// Excludes the `floats` group: it carries NaN values, and `CAST(NaN AS BIGINT)`
// is a hard cast error (unrelated to this ADR). Its float distinctness is
// covered by `count_distinct_float_is_bit_identical_across_partition_counts`. The
// residual string filter does not change the exact-typed classification.
const EXACT_SQL: &str = "SELECT label(labels,'__name__') AS metric, \
     sum(CAST(value AS BIGINT)) AS s, count(*) AS c \
     FROM samples WHERE label(labels,'__name__') <> 'floats' GROUP BY metric";

#[tokio::test]
async fn exact_typed_group_by_is_bit_identical_across_partition_counts() {
    // The shipped baseline: single partition, flag off.
    let baseline_fixture = fixture_with(1, false).await;
    let baseline = sorted_rows(&baseline_fixture, EXACT_SQL).await;

    // Sanity: the dataset actually exercises the 2^53-crossing integer sum, and
    // its exact integer value is present (a float accumulator would not reach
    // this bit pattern for this multiset).
    let big_row = baseline
        .iter()
        .find(|row| group_key(row) == "big")
        .expect("the big group is present");
    assert_eq!(
        big_row[1],
        Cell::Int(BIG_SUM),
        "the integer sum crossing 2^53 must be exact"
    );
    assert!(baseline.len() >= 6, "the dataset has several group keys");

    // Flag on at fetch_concurrency 1 must converge with the baseline both in
    // result and in physical plan shape: target_partitions=1 already satisfies
    // EnforceDistribution, so no RepartitionExec is expected even though the
    // classification and the config flip both ran.
    let one_partition_on = fixture_with(1, true).await;
    assert_eq!(
        sorted_rows(&one_partition_on, EXACT_SQL).await,
        baseline,
        "flag-on at fetch_concurrency=1 must equal the single-partition baseline"
    );
    let baseline_plan = physical_plan_text(&baseline_fixture, EXACT_SQL).await;
    let one_partition_on_plan = physical_plan_text(&one_partition_on, EXACT_SQL).await;
    assert!(
        !fans_out_final_aggregation(&one_partition_on_plan),
        "at target_partitions=1 the final aggregation is not fanned out even with the flag on:\n{one_partition_on_plan}"
    );
    assert_eq!(
        baseline_plan, one_partition_on_plan,
        "flag-on at fetch_concurrency=1 must produce the same plan shape as flag-off"
    );

    // Repartitioned counts: each run several times to surface a merge-order race
    // that a single run could miss by chance.
    for fetch_concurrency in [4usize, 8, 16] {
        let fixture = fixture_with(fetch_concurrency, true).await;
        // Prove the flag actually repartitioned (otherwise the differential
        // would vacuously compare single-partition plans).
        let plan = physical_plan_text(&fixture, EXACT_SQL).await;
        assert!(
            fans_out_final_aggregation(&plan),
            "an exact-typed GROUP BY at fetch_concurrency={fetch_concurrency} with the flag on \
             must fan its final aggregation across partitions:\n{plan}"
        );
        for run in 0..5 {
            let rows = sorted_rows(&fixture, EXACT_SQL).await;
            assert_eq!(
                rows, baseline,
                "repartitioned result at fetch_concurrency={fetch_concurrency} \
                 (run {run}) must equal the single-partition baseline"
            );
        }
    }
}

#[tokio::test]
async fn disqualified_queries_stay_single_partition() {
    // Each query has a disqualifier: a float aggregate, a float GROUP BY key,
    // and SELECT DISTINCT on a float column. None may repartition, flag or not.
    let disqualified = [
        "SELECT label(labels,'__name__') AS metric, sum(value) AS s \
         FROM samples GROUP BY metric",
        "SELECT value, count(*) AS c FROM samples GROUP BY value",
        "SELECT DISTINCT value FROM samples",
    ];

    for parallel in [false, true] {
        let fixture = fixture_with(8, parallel).await;
        for sql in disqualified {
            let plan = physical_plan_text(&fixture, sql).await;
            assert!(
                !fans_out_final_aggregation(&plan),
                "a disqualified query must stay single-partition \
                 (parallel_final_aggregation={parallel}); got:\n{plan}\nfor: {sql}"
            );
        }
    }
}

#[tokio::test]
async fn subquery_hidden_disqualifier_stays_single_partition() {
    // The outer aggregate (count) is exact, but a disqualifying float avg hides
    // inside a scalar subquery. Decision 1's subquery walk must find it, forcing
    // the whole query single-partition.
    let sql = "SELECT count(*) AS c FROM samples \
               WHERE value > (SELECT avg(value) FROM samples)";
    let fixture = fixture_with(8, true).await;
    let plan = physical_plan_text(&fixture, sql).await;
    assert!(
        !fans_out_final_aggregation(&plan),
        "a disqualifying aggregate hidden in a scalar subquery must keep the \
         single-partition plan; got:\n{plan}"
    );
}

#[tokio::test]
async fn count_distinct_float_is_bit_identical_across_partition_counts() {
    // count(DISTINCT float_col) is exact (count is always exact), so it
    // repartitions. The `floats` group holds -0.0/0.0 and mixed-payload NaNs
    // split across segments: the distinct count must be stable across partition
    // counts. This is insurance against a future DataFusion upgrade breaking the
    // bit-pattern hash/eq consistency this relies on.
    let sql = "SELECT label(labels,'__name__') AS metric, \
               count(DISTINCT value) AS d FROM samples GROUP BY metric";

    let baseline = sorted_rows(&fixture_with(1, false).await, sql).await;

    // The `floats` group's four distinct bit patterns (-0.0, 0.0, NAN_A, NAN_B),
    // with the second NAN_A collapsing back into the first.
    let floats_row = baseline
        .iter()
        .find(|row| group_key(row) == "floats")
        .expect("the floats group is present");
    assert_eq!(
        floats_row[1],
        Cell::Int(4),
        "-0.0, 0.0, and two NaN payloads are four distinct values; a repeated \
         payload collapses"
    );

    for fetch_concurrency in [4usize, 8, 16] {
        let fixture = fixture_with(fetch_concurrency, true).await;
        let plan = physical_plan_text(&fixture, sql).await;
        assert!(
            fans_out_final_aggregation(&plan),
            "count(DISTINCT float) is exact and must repartition at \
             fetch_concurrency={fetch_concurrency}:\n{plan}"
        );
        for run in 0..5 {
            let rows = sorted_rows(&fixture, sql).await;
            assert_eq!(
                rows, baseline,
                "count(DISTINCT float) at fetch_concurrency={fetch_concurrency} \
                 (run {run}) must equal the single-partition baseline"
            );
        }
    }
}
