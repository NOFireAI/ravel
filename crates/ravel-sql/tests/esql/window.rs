//! The window a `FROM logs` statement resolves (ADR-2887 D11), and the rows it
//! lets through on every segment layout.

use ravel_sql::esql::EsqlOutcome;
use ravel_types::TimeRange;

use crate::support::{
    BASE, LAYOUTS, NOW, SECOND, prepare, rows, run, run_with_range, segment_count,
    segmented_fixture, segments,
};

const HOUR: i64 = 3_600 * SECOND;

fn window(source: &str) -> TimeRange {
    prepare(source)
        .expect("prepares")
        .window()
        .expect("a FROM statement has a window")
}

fn range(start_ns: i64, end_ns: i64) -> TimeRange {
    TimeRange { start_ns, end_ns }
}

/// The first column of every row, sorted.
fn sorted_cells(outcome: &EsqlOutcome) -> Vec<String> {
    let mut cells: Vec<String> = rows(outcome)
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    cells.sort();
    cells
}

#[test]
fn lower_bound_alone_ends_at_the_query_clock() {
    assert_eq!(
        window("FROM logs | WHERE @timestamp >= NOW() - 1 hour"),
        range(NOW - HOUR, NOW + 1)
    );
    assert_eq!(
        window("FROM logs | WHERE @timestamp > NOW() - 1 hour"),
        range(NOW - HOUR + 1, NOW + 1)
    );
}

#[test]
fn both_bounds_set_both_ends() {
    assert_eq!(
        window(
            r#"FROM logs | WHERE @timestamp >= "2023-11-14T22:13:00Z" AND @timestamp < "2023-11-14T22:14:00Z""#
        ),
        range(BASE, BASE + 60 * SECOND)
    );
    // Bounds in two WHERE stages and with the column on the right intersect.
    assert_eq!(
        window(
            r#"FROM logs | WHERE "2023-11-14T22:13:00Z" < @timestamp | WHERE @timestamp <= "2023-11-14T22:14:00Z" | WHERE @timestamp <= "2023-11-14T22:15:00Z""#
        ),
        range(BASE + 1, BASE + 60 * SECOND + 1)
    );
}

#[test]
fn no_lower_bound_starts_at_the_epoch() {
    assert_eq!(window("FROM logs"), range(0, NOW + 1));
    assert_eq!(
        window("FROM logs | WHERE message == \"a\""),
        range(0, NOW + 1)
    );
    assert_eq!(
        window(r#"FROM logs | WHERE @timestamp < "2023-11-14T22:14:00Z""#),
        range(0, BASE + 60 * SECOND)
    );
    // A disjunction bounds nothing.
    assert_eq!(
        window(r#"FROM logs | WHERE @timestamp >= "2023-11-14T22:13:00Z" OR message == "a""#),
        range(0, NOW + 1)
    );
}

#[test]
fn a_bound_after_limit_stats_or_a_redefined_timestamp_is_ignored() {
    let bound = r#"WHERE @timestamp >= "2023-11-14T22:13:00Z""#;
    assert_eq!(
        window(&format!("FROM logs | LIMIT 10 | {bound}")),
        range(0, NOW + 1)
    );
    assert_eq!(
        window(&format!(
            "FROM logs | STATS c = COUNT(*) BY @timestamp = BUCKET(@timestamp, 1 minute) | {bound}"
        )),
        range(0, NOW + 1)
    );
    assert_eq!(
        window(&format!(
            "FROM logs | EVAL @timestamp = @timestamp + 1 hour | {bound}"
        )),
        range(0, NOW + 1)
    );
    assert_eq!(
        window(&format!(
            "FROM logs | RENAME observed_ts AS @timestamp | {bound}"
        )),
        range(0, NOW + 1)
    );
    // A stage that keeps the column leaves the window open.
    assert_eq!(
        window(&format!(
            "FROM logs | KEEP @timestamp, message | SORT message | {bound}"
        )),
        range(BASE, NOW + 1)
    );
}

/// Segment A straddles the query clock and segment B lies wholly after it.
/// Only A's rows up to the clock come back, and merging the two segments into
/// one changes nothing.
#[tokio::test]
async fn a_lower_bound_alone_returns_rows_up_to_the_clock_on_every_layout() {
    for layout in LAYOUTS {
        let fixture = segmented_fixture(layout).await;
        let outcome = run(
            &fixture,
            "FROM logs | WHERE @timestamp >= NOW() - 1 hour | KEEP message",
        )
        .await
        .unwrap_or_else(|e| panic!("{layout:?}: {e}"));
        assert_eq!(sorted_cells(&outcome), ["a1", "a2", "a3"], "{layout:?}");
    }
}

/// A lower bound after the clock and no upper bound is the empty window, on a
/// layout whose one segment holds rows the WHERE keeps as well.
#[tokio::test]
async fn a_lower_bound_after_the_clock_returns_no_rows_on_every_layout() {
    let source = "FROM logs | WHERE @timestamp >= NOW() + 1 minute";
    assert_eq!(window(source), range(NOW + 60 * SECOND, NOW + 60 * SECOND));
    for layout in LAYOUTS {
        let fixture = segmented_fixture(layout).await;
        let outcome = run(&fixture, source)
            .await
            .unwrap_or_else(|e| panic!("{layout:?}: {e}"));
        assert_eq!(rows(&outcome), Vec::<Vec<String>>::new(), "{layout:?}");
    }
}

/// A request end is a stated upper bound: a request range wholly after the
/// clock returns its rows, with or without an upper bound in the statement.
/// A request start after the clock with no end is the empty window.
#[tokio::test]
async fn a_request_range_after_the_clock_returns_its_rows_on_every_layout() {
    let start = NOW + 300 * SECOND;
    let end = NOW + 306 * SECOND;
    for layout in LAYOUTS {
        let fixture = segmented_fixture(layout).await;
        for source in [
            "FROM logs | KEEP message",
            "FROM logs | WHERE @timestamp < NOW() + 1 hour | KEEP message",
        ] {
            let outcome = run_with_range(&fixture, source, Some(start), Some(end))
                .await
                .unwrap_or_else(|e| panic!("{layout:?} {source}: {e}"));
            assert_eq!(sorted_cells(&outcome), ["b1", "b2"], "{layout:?} {source}");
            assert_eq!(segments(&outcome), 1, "{layout:?} {source}");
        }
        let outcome = run_with_range(&fixture, "FROM logs | KEEP message", Some(start), None)
            .await
            .unwrap_or_else(|e| panic!("{layout:?}: {e}"));
        assert_eq!(rows(&outcome), Vec::<Vec<String>>::new(), "{layout:?}");
    }
}

/// A sort ahead of a filter orders the rows of both segments together.
#[tokio::test]
async fn sort_orders_rows_across_segments() {
    for layout in LAYOUTS {
        let fixture = segmented_fixture(layout).await;
        let outcome = run_with_range(
            &fixture,
            r#"FROM logs | SORT severity_num | WHERE message != "x" | KEEP severity_num"#,
            Some(NOW - 60 * SECOND),
            Some(NOW + 400 * SECOND),
        )
        .await
        .unwrap_or_else(|e| panic!("{layout:?}: {e}"));
        assert_eq!(segments(&outcome), segment_count(layout), "{layout:?}");
        let order: Vec<String> = rows(&outcome)
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(order, ["1", "3", "5", "7", "9", "11", "13"], "{layout:?}");
    }
}

/// A statement with no bounds reads `[epoch, clock]`. On this fixture the
/// catalog admits that window, and the rows after the clock stay out.
#[tokio::test]
async fn unbounded_query_runs_or_is_refused_by_the_catalog() {
    for layout in LAYOUTS {
        let fixture = segmented_fixture(layout).await;
        let outcome = run(&fixture, "FROM logs | KEEP message")
            .await
            .unwrap_or_else(|e| panic!("{layout:?}: the catalog admits [0, clock]: {e}"));
        assert_eq!(sorted_cells(&outcome), ["a1", "a2", "a3"], "{layout:?}");
        assert_eq!(segments(&outcome), 1, "{layout:?}");
    }
}
