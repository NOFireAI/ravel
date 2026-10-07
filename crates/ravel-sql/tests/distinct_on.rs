//! `SELECT DISTINCT ON (...)` through `SqlExecutor::execute`.
//!
//! DataFusion's optimizer rewrites a `DISTINCT ON` into an ordered
//! `first_value` aggregate that it looks up by name in the session registry,
//! so the session keeps `first_value` registered while every statement that
//! names it is refused. The rewrite keeps, per ON group, the first row under
//! the statement's `ORDER BY`; it is exact only when that order decides the
//! row, so a `DISTINCT ON` whose `ORDER BY` ties is refused before it plans.
//!
//! - `a_total_order_distinct_on_is_exact_under_parallel_final_aggregation`:
//!   three total-order statements over three segments, at several partition
//!   counts with their final aggregation fanned out across partitions, each
//!   return the exact rows a hand derivation gives, signed zero and NaN
//!   payloads included.
//! - `a_distinct_on_whose_order_ties_is_refused`: the typed refusal, at the
//!   top level and inside a subquery.
//! - `first_value_named_directly_is_refused`: the registered `first_value`
//!   stays unreachable by name, in the spellings the text walk matches and in
//!   the quoted spelling it does not.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use datafusion::physical_plan::displayable;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::EngineConfig;
use ravel_sql::{ErrorClass, SqlConfig, SqlError, ValidationError};
use ravel_types::TenantId;
use ravel_types::accounting::QueryAccounting;
use std::sync::Arc;

use util::gate::{self, Cell, Row};
use util::{Fixture, SegSpec, SeriesSpec, request};

fn tenant() -> TenantId {
    util::tenant_id("distinct-on")
}

fn nan_a() -> f64 {
    f64::from_bits(0x7ff8_0000_0000_0001)
}

fn nan_b() -> f64 {
    f64::from_bits(0x7ff8_0000_0000_0002)
}

/// Three segments with distinct provenance. Series `a` spreads its samples
/// over all three, so its latest sample is in the middle segment, and three
/// series share `ts` 500 (`0.0`, `-0.0`, `1.0`) and two share `ts` 600 (two
/// NaN payloads), each pair split across segments, so a group's candidates
/// meet only in the merge of partial states.
fn dataset() -> Vec<SegSpec> {
    vec![
        SegSpec::new(
            10,
            1,
            1,
            vec![
                SeriesSpec::new("a", vec![(100, 1.0), (200, 2.0)]),
                SeriesSpec::new("b", vec![(100, 5.0)]),
                SeriesSpec::new("z0", vec![(500, 0.0), (600, nan_a())]),
            ],
        ),
        SegSpec::new(
            20,
            1,
            2,
            vec![
                SeriesSpec::new("a", vec![(300, 3.0)]),
                SeriesSpec::new("b", vec![(150, -1.0)]),
                SeriesSpec::new("z1", vec![(500, -0.0), (600, nan_b())]),
            ],
        ),
        SegSpec::new(
            30,
            1,
            3,
            vec![
                SeriesSpec::new("a", vec![(250, 4.0)]),
                SeriesSpec::new("c", vec![(100, 7.0)]),
                SeriesSpec::new("z2", vec![(500, 1.0)]),
            ],
        ),
    ]
}

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

/// Each metric's latest sample. Every selected column is an `ORDER BY` term,
/// so the order is total.
const LATEST_PER_METRIC: &str = "SELECT DISTINCT ON (metric) metric, ts, value \
     FROM (SELECT label(labels, '__name__') AS metric, ts, value FROM samples) s \
     ORDER BY metric, ts DESC, value";

/// The rows [`LATEST_PER_METRIC`] must return, in its `ORDER BY` order.
fn latest_per_metric_rows() -> Vec<Row> {
    let row = |metric: &str, ts: i64, value: f64| {
        vec![
            Cell::Text(metric.to_string()),
            Cell::Int(ts),
            Cell::float(value),
        ]
    };
    vec![
        row("a", 300, 3.0),
        row("b", 150, -1.0),
        row("c", 100, 7.0),
        row("z0", 600, nan_a()),
        row("z1", 600, nan_b()),
        row("z2", 500, 1.0),
    ]
}

/// Each timestamp's smallest value under the `ORDER BY` total order on
/// floats: `-0.0` sorts before `0.0`, and a positive NaN sorts after every
/// number and by payload among NaNs.
const SMALLEST_PER_TS: &str = "SELECT DISTINCT ON (ts) ts, value FROM samples ORDER BY ts, value";

fn smallest_per_ts_rows() -> Vec<Row> {
    let row = |ts: i64, value: f64| vec![Cell::Int(ts), Cell::float(value)];
    vec![
        row(100, 1.0),
        row(150, -1.0),
        row(200, 2.0),
        row(250, 4.0),
        row(300, 3.0),
        row(500, -0.0),
        row(600, nan_a()),
    ]
}

/// The latest sample of each series, as the user guide writes it.
const LATEST_PER_SERIES: &str = "SELECT DISTINCT ON (series_id) series_id, ts, value \
     FROM samples ORDER BY series_id, ts DESC, value";

/// [`latest_per_metric_rows`] keyed by series id instead of metric name, in
/// series id order. Each metric is one series.
fn latest_per_series_rows() -> Vec<Row> {
    let t = tenant();
    let mut rows: Vec<Row> = latest_per_metric_rows()
        .into_iter()
        .map(|row| {
            let Cell::Text(metric) = &row[0] else {
                panic!("metric cell")
            };
            let id = util::series_id_for(&t, metric).to_vec();
            vec![Cell::Bytes(id), row[1].clone(), row[2].clone()]
        })
        .collect();
    rows.sort_by(|a, b| match (&a[0], &b[0]) {
        (Cell::Bytes(a), Cell::Bytes(b)) => a.cmp(b),
        _ => unreachable!("series id cells"),
    });
    rows
}

async fn rows(fixture: &Fixture, sql: &str) -> Vec<Row> {
    let outcome = fixture
        .executor
        .execute(tenant().hash(), &request(sql))
        .await
        .unwrap_or_else(|err| panic!("`{sql}` executes: {err}"));
    gate::actual_rows(&outcome.output)
}

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

/// A total-order `DISTINCT ON` returns the exact rows whether its final
/// aggregation runs in one partition or fans out across many. Before
/// `first_value` stayed registered for the optimizer, both statements failed
/// to plan with "There is no UDAF named first_value in the registry".
#[tokio::test]
async fn a_total_order_distinct_on_is_exact_under_parallel_final_aggregation() {
    let cases = [
        (LATEST_PER_METRIC, latest_per_metric_rows()),
        (SMALLEST_PER_TS, smallest_per_ts_rows()),
        (LATEST_PER_SERIES, latest_per_series_rows()),
    ];

    let single = fixture_with(1, false).await;
    for (sql, expected) in &cases {
        assert_eq!(
            &rows(&single, sql).await,
            expected,
            "single partition: {sql}"
        );
    }

    for fetch_concurrency in [4usize, 8, 16] {
        let fixture = fixture_with(fetch_concurrency, true).await;
        for (sql, expected) in &cases {
            // Without the fan-out this would compare one-partition plans.
            let plan = physical_plan_text(&fixture, sql).await;
            assert!(
                plan.contains("partitioning=Hash("),
                "at fetch_concurrency={fetch_concurrency} the DISTINCT ON final aggregation \
                 must fan out across partitions:\n{plan}\nfor: {sql}"
            );
            for run in 0..5 {
                assert_eq!(
                    &rows(&fixture, sql).await,
                    expected,
                    "fetch_concurrency={fetch_concurrency}, run {run}: {sql}"
                );
            }
        }
    }
}

/// A `DISTINCT ON` whose `ORDER BY` leaves a selected column out is refused
/// with the typed error, not the optimizer's missing-UDAF error and not a row
/// chosen by scan order.
#[tokio::test]
async fn a_distinct_on_whose_order_ties_is_refused() {
    let fixture = fixture_with(8, true).await;
    let tied = [
        // No ORDER BY at all.
        "SELECT DISTINCT ON (series_id) series_id, value FROM samples",
        // `value` ties within a series.
        "SELECT DISTINCT ON (series_id) series_id, value FROM samples ORDER BY series_id",
        // `ts` decides the row here, but `value` is not an ORDER BY term.
        "SELECT DISTINCT ON (series_id) series_id, ts, value FROM samples \
         ORDER BY series_id, ts DESC",
        // A selected expression is never a plain ORDER BY column.
        "SELECT DISTINCT ON (series_id) series_id, value + 1 AS v FROM samples \
         ORDER BY series_id, value + 1",
        // Inside a subquery.
        "SELECT count(*) AS c FROM (SELECT DISTINCT ON (series_id) series_id, value \
         FROM samples ORDER BY series_id) d",
    ];
    for sql in tied {
        let err = match fixture
            .executor
            .execute(tenant().hash(), &request(sql))
            .await
        {
            Ok(_) => panic!("a DISTINCT ON whose order ties executed: `{sql}`"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                SqlError::Validation(ValidationError::DistinctOnOrderNotTotal)
            ),
            "expected DistinctOnOrderNotTotal for `{sql}`, got {err}"
        );
        assert_eq!(err.class(), ErrorClass::BadRequest, "{sql}");
        let message = err.client_message();
        assert!(
            message.contains("DISTINCT ON") && message.contains("ORDER BY"),
            "the message must name DISTINCT ON and its ORDER BY: {message}"
        );
    }
}

/// `first_value` is registered for the `DISTINCT ON` rewrite only. Every way
/// of naming it as an aggregate is refused with the excluded-aggregate error.
/// The quoted spellings slip past the validator's text walk; with the
/// aggregate registered they executed until the executor checked the planned
/// statement.
#[tokio::test]
async fn first_value_named_directly_is_refused() {
    let fixture = fixture_with(8, true).await;
    let aggregate = [
        "SELECT first_value(value) FROM samples",
        "SELECT first_value(value ORDER BY ts) FROM samples",
        "SELECT \"first_value\"(value) FROM samples",
        "SELECT series_id, \"first_value\"(value ORDER BY ts) AS v FROM samples GROUP BY series_id",
        "SELECT count(*) AS c FROM samples \
         WHERE value > (SELECT \"first_value\"(value ORDER BY ts) FROM samples)",
    ];
    for sql in aggregate {
        let err = match fixture
            .executor
            .execute(tenant().hash(), &request(sql))
            .await
        {
            Ok(_) => panic!("first_value was reachable by name: `{sql}`"),
            Err(err) => err,
        };
        assert!(
            matches!(
                &err,
                SqlError::Validation(ValidationError::ExcludedAggregate { name })
                    if name == "first_value"
            ),
            "expected ExcludedAggregate(first_value) for `{sql}`, got {err}"
        );
        assert_eq!(err.class(), ErrorClass::BadRequest, "{sql}");
    }

    let sql = "SELECT first_value(value) OVER (ORDER BY ts) FROM samples";
    let err = fixture
        .executor
        .execute(tenant().hash(), &request(sql))
        .await
        .expect_err("first_value was reachable by name as a window");
    assert!(
        matches!(
            &err,
            SqlError::Validation(ValidationError::ExcludedWindow { name }) if name == "first_value"
        ),
        "expected ExcludedWindow(first_value) for `{sql}`, got {err}"
    );

    // The window registry no longer holds `first_value`, and the planner does
    // not fall back to the registered aggregate for an `OVER` call, so the
    // quoted windowed spelling stays the registry gate's plan refusal.
    let sql = "SELECT \"first_value\"(value) OVER (ORDER BY ts) FROM samples";
    let err = fixture
        .executor
        .execute(tenant().hash(), &request(sql))
        .await
        .expect_err("first_value was reachable by name as a window");
    assert!(
        matches!(&err, SqlError::Plan(_)),
        "expected the window registry's plan refusal for `{sql}`, got {err}"
    );
}
