//! Postings pruning by a pushed-down `__name__` predicate when the predicate
//! does not bind every row a `samples` scan produces (issue #2907).
//!
//! Every `samples` scan in a statement reads the same resolved snapshot, so a
//! name filter may prune it only when every scan is required to satisfy that
//! filter. A predicate on one branch of a `UNION ALL`, or on one side of a
//! self-join, must leave the other scan's rows intact, and a predicate above a
//! window or an `ORDER BY ... LIMIT` must leave the rows that node ranks.
//!
//! Fixture: segment 1 holds only metric `a` (three samples, value 1.0),
//! segment 2 holds only metric `b` (two samples, value 2.0), at shared
//! timestamps so the self-join pairs rows across the two metrics.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use ravel_sql::SqlOutcome;
use ravel_types::Signal;
use util::gate::{self, Cell};
use util::{Fixture, NOW_NS, SegSpec, SeriesSpec, request, tenant_id};
use uuid::Uuid;

const T1: i64 = 60_000_000_000;
const T2: i64 = 120_000_000_000;
const T3: i64 = 180_000_000_000;
const A_VALUE: f64 = 1.0;
const B_VALUE: f64 = 2.0;
/// Rows of metric `a` in the fixture (segment 1).
const A_ROWS: usize = 3;
/// Rows of metric `b` in the fixture (segment 2).
const B_ROWS: usize = 2;

async fn folded_fixture(name: &str) -> (Fixture, ravel_types::TenantId) {
    let tenant = tenant_id(name);
    let specs = vec![
        SegSpec::new(
            1,
            1,
            1,
            vec![SeriesSpec::new(
                "a",
                vec![(T1, A_VALUE), (T2, A_VALUE), (T3, A_VALUE)],
            )],
        ),
        SegSpec::new(
            2,
            1,
            2,
            vec![SeriesSpec::new("b", vec![(T1, B_VALUE), (T2, B_VALUE)])],
        ),
    ];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
    // Fold so a postings index exists; without one nothing prunes and the
    // tests below could not tell a pruned snapshot from an unpruned one.
    let report = fixture
        .catalog
        .fold(
            &tenant.hash(),
            Signal::Metrics,
            Uuid::new_v4(),
            NOW_NS,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert!(report.postings_built, "the fold must attach postings");
    (fixture, tenant)
}

async fn run(fixture: &Fixture, tenant: &ravel_types::TenantId, sql: &str) -> SqlOutcome {
    fixture
        .executor
        .execute(tenant.hash(), &request(sql))
        .await
        .expect("execute")
}

/// The single `value` column of `outcome`, split into (rows of 1.0, rows of
/// 2.0, total rows).
fn value_counts(outcome: &SqlOutcome) -> (usize, usize, usize) {
    let rows = gate::actual_rows(&outcome.output);
    let a = rows
        .iter()
        .filter(|r| r.as_slice() == [Cell::float(A_VALUE)])
        .count();
    let b = rows
        .iter()
        .filter(|r| r.as_slice() == [Cell::float(B_VALUE)])
        .count();
    (a, b, rows.len())
}

#[tokio::test]
async fn union_all_with_one_filtered_branch_keeps_other_branch_rows() {
    let (fixture, tenant) = folded_fixture("multi-scan-union").await;
    for sql in [
        "SELECT value FROM samples WHERE label(labels, '__name__') = 'a' \
         UNION ALL SELECT value FROM samples",
        "SELECT value FROM samples \
         UNION ALL SELECT value FROM samples WHERE label(labels, '__name__') = 'a'",
    ] {
        let outcome = run(&fixture, &tenant, sql).await;
        assert_eq!(
            value_counts(&outcome),
            (2 * A_ROWS, B_ROWS, 2 * A_ROWS + B_ROWS),
            "(rows of a, rows of b, total) for {sql}"
        );
        assert_eq!(
            outcome.stats.segments, 2,
            "the unfiltered branch needs both segments: {sql}"
        );
    }
}

#[tokio::test]
async fn self_join_filtered_on_one_side_keeps_other_side_rows() {
    let (fixture, tenant) = folded_fixture("multi-scan-join").await;
    // Each `a` row at ts t pairs with every row at t: at T1 and T2 one `a`
    // and one `b` row, at T3 the `a` row alone.
    let outcome = run(
        &fixture,
        &tenant,
        "SELECT b.value FROM samples a JOIN samples b ON a.ts = b.ts \
         WHERE label(a.labels, '__name__') = 'a'",
    )
    .await;
    assert_eq!(
        value_counts(&outcome),
        (A_ROWS, B_ROWS, A_ROWS + B_ROWS),
        "(rows of a, rows of b, total) on the unfiltered join side"
    );
    assert_eq!(outcome.stats.segments, 2);
}

#[tokio::test]
async fn subquery_scan_keeps_rows_the_outer_filter_excludes() {
    let (fixture, tenant) = folded_fixture("multi-scan-subquery").await;
    // The subquery reads only `b` rows (T1, T2); the outer `a` rows at those
    // two timestamps survive.
    let outcome = run(
        &fixture,
        &tenant,
        "SELECT value FROM samples WHERE label(labels, '__name__') = 'a' \
         AND ts IN (SELECT ts FROM samples WHERE label(labels, '__name__') = 'b')",
    )
    .await;
    assert_eq!(value_counts(&outcome), (B_ROWS, 0, B_ROWS));
    assert_eq!(outcome.stats.segments, 2);
}

#[tokio::test]
async fn single_scan_name_filter_still_prunes() {
    let (fixture, tenant) = folded_fixture("multi-scan-single").await;
    let outcome = run(
        &fixture,
        &tenant,
        "SELECT value FROM samples WHERE label(labels, '__name__') = 'a'",
    )
    .await;
    assert_eq!(value_counts(&outcome), (A_ROWS, 0, A_ROWS));
    assert_eq!(
        outcome.stats.segments, 1,
        "the name filter must prune the snapshot to the segment holding a"
    );
}

#[tokio::test]
async fn union_all_with_same_filter_on_both_branches_still_prunes() {
    let (fixture, tenant) = folded_fixture("multi-scan-both").await;
    let outcome = run(
        &fixture,
        &tenant,
        "SELECT value FROM samples WHERE label(labels, '__name__') = 'a' \
         UNION ALL SELECT value FROM samples WHERE label(labels, '__name__') = 'a'",
    )
    .await;
    assert_eq!(value_counts(&outcome), (2 * A_ROWS, 0, 2 * A_ROWS));
    assert_eq!(
        outcome.stats.segments, 1,
        "both scans require a, so the snapshot prunes to its one segment"
    );
}

#[tokio::test]
async fn single_scan_filter_above_window_keeps_rows_the_window_numbers() {
    let (fixture, tenant) = folded_fixture("single-scan-window").await;
    // Ordered by value descending, `b` takes rn 1-2 and `a` takes rn 3-5, so
    // every `a` row passes `rn > 2`. Pruning to `a` below the window would
    // renumber `a` as rn 1-3 and keep one row.
    let outcome = run(
        &fixture,
        &tenant,
        "SELECT value FROM (SELECT value, labels, \
         row_number() OVER (ORDER BY value DESC) AS rn FROM samples) t \
         WHERE label(t.labels, '__name__') = 'a' AND rn > 2",
    )
    .await;
    assert_eq!(value_counts(&outcome), (A_ROWS, 0, A_ROWS));
    assert_eq!(
        outcome.stats.segments, 2,
        "the window numbers rows of both metrics"
    );
}

#[tokio::test]
async fn single_scan_filter_above_order_by_limit_keeps_rows_the_limit_drops() {
    let (fixture, tenant) = folded_fixture("single-scan-limit").await;
    // The two largest values are both `b`, so no `a` row survives the limit.
    // Pruning to `a` below the limit would let two `a` rows through.
    let outcome = run(
        &fixture,
        &tenant,
        "SELECT value FROM (SELECT value, labels FROM samples \
         ORDER BY value DESC LIMIT 2) t \
         WHERE label(t.labels, '__name__') = 'a'",
    )
    .await;
    assert_eq!(value_counts(&outcome), (0, 0, 0));
    assert_eq!(
        outcome.stats.segments, 2,
        "the limit ranks rows of both metrics"
    );
}

#[tokio::test]
async fn union_all_branch_filtered_above_window_keeps_rows_the_window_numbers() {
    let (fixture, tenant) = folded_fixture("multi-scan-window-branch").await;
    // The derived table carries the scan's own name, so a filter that reached
    // the scan through the window would bind it and agree with the second
    // branch's key.
    let outcome = run(
        &fixture,
        &tenant,
        "SELECT value FROM (SELECT value, labels, \
         row_number() OVER (ORDER BY value DESC) AS rn FROM samples) AS samples \
         WHERE label(samples.labels, '__name__') = 'a' AND rn > 2 \
         UNION ALL SELECT value FROM samples WHERE label(labels, '__name__') = 'a'",
    )
    .await;
    assert_eq!(value_counts(&outcome), (2 * A_ROWS, 0, 2 * A_ROWS));
    assert_eq!(
        outcome.stats.segments, 2,
        "the window branch numbers rows of both metrics"
    );
}

#[tokio::test]
async fn self_join_filtered_on_both_sides_still_prunes() {
    let (fixture, tenant) = folded_fixture("multi-scan-join-both").await;
    // Each `a` row pairs with the one `a` row at its timestamp.
    let outcome = run(
        &fixture,
        &tenant,
        "SELECT a.value FROM samples a JOIN samples b ON a.ts = b.ts \
         WHERE label(a.labels, '__name__') = 'a' \
         AND label(b.labels, '__name__') = 'a'",
    )
    .await;
    assert_eq!(value_counts(&outcome), (A_ROWS, 0, A_ROWS));
    assert_eq!(
        outcome.stats.segments, 1,
        "both join sides require a, so the snapshot prunes to its one segment"
    );
}
