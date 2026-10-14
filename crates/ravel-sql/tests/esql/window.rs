//! The window a `FROM logs` statement resolves (ADR-2887 D11).

use ravel_sql::esql::{self, EsqlError, PrepareContext};
use ravel_sql::{ErrorClass, SqlError};
use ravel_types::TimeRange;

use crate::support::{BASE, NOW, SECOND, context, fixture, prepare, rows, run, run_prepared};

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

#[test]
fn lower_bound_alone_ends_at_the_query_clock() {
    assert_eq!(
        window("FROM logs | WHERE @timestamp >= NOW() - 1 hour"),
        range(NOW - HOUR, NOW)
    );
    assert_eq!(
        window("FROM logs | WHERE @timestamp > NOW() - 1 hour"),
        range(NOW - HOUR + 1, NOW)
    );
}

#[test]
fn both_bounds_set_both_ends() {
    assert_eq!(
        window(
            r#"FROM logs | WHERE @timestamp >= "2023-11-14T22:13:00Z" AND @timestamp < "2023-11-14T22:14:00Z""#
        ),
        range(BASE, BASE + 60 * SECOND - 1)
    );
    // Bounds in two WHERE stages and with the column on the right intersect.
    assert_eq!(
        window(
            r#"FROM logs | WHERE "2023-11-14T22:13:00Z" < @timestamp | WHERE @timestamp <= "2023-11-14T22:14:00Z" | WHERE @timestamp <= "2023-11-14T22:15:00Z""#
        ),
        range(BASE + 1, BASE + 60 * SECOND)
    );
}

#[test]
fn no_lower_bound_starts_at_the_epoch() {
    assert_eq!(window("FROM logs"), range(0, NOW));
    assert_eq!(window("FROM logs | WHERE message == \"a\""), range(0, NOW));
    assert_eq!(
        window(r#"FROM logs | WHERE @timestamp < "2023-11-14T22:14:00Z""#),
        range(0, BASE + 60 * SECOND - 1)
    );
    // A disjunction bounds nothing.
    assert_eq!(
        window(r#"FROM logs | WHERE @timestamp >= "2023-11-14T22:13:00Z" OR message == "a""#),
        range(0, NOW)
    );
}

#[tokio::test]
async fn request_start_and_end_narrow_the_window_and_filter_rows() {
    let ctx = PrepareContext {
        start: Some(BASE + 60 * SECOND),
        end: Some(BASE + 120 * SECOND),
        ..context()
    };
    let prepared = esql::prepare(
        "FROM logs | WHERE @timestamp >= NOW() - 1 hour | KEEP message | SORT message",
        &ravel_esql::Limits::default(),
        &ctx,
    )
    .expect("prepares");
    assert_eq!(
        prepared.window(),
        Some(range(BASE + 60 * SECOND, BASE + 120 * SECOND - 1))
    );
    let fixture = fixture().await;
    let outcome = run_prepared(&fixture, &prepared).await.expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["c"], vec!["d"]]);
}

#[test]
fn a_bound_after_limit_stats_or_a_redefined_timestamp_is_ignored() {
    let bound = r#"WHERE @timestamp >= "2023-11-14T22:13:00Z""#;
    assert_eq!(
        window(&format!("FROM logs | LIMIT 10 | {bound}")),
        range(0, NOW)
    );
    assert_eq!(
        window(&format!(
            "FROM logs | STATS c = COUNT(*) BY @timestamp = BUCKET(@timestamp, 1 minute) | {bound}"
        )),
        range(0, NOW)
    );
    assert_eq!(
        window(&format!(
            "FROM logs | EVAL @timestamp = @timestamp + 1 hour | {bound}"
        )),
        range(0, NOW)
    );
    assert_eq!(
        window(&format!(
            "FROM logs | RENAME observed_ts AS @timestamp | {bound}"
        )),
        range(0, NOW)
    );
    // A stage that keeps the column leaves the window open.
    assert_eq!(
        window(&format!(
            "FROM logs | KEEP @timestamp, message | SORT message | {bound}"
        )),
        range(BASE, NOW)
    );
}

#[tokio::test]
async fn unbounded_query_runs_or_is_refused_by_the_catalog() {
    let fixture = fixture().await;
    match run(&fixture, "FROM logs | KEEP message | SORT message").await {
        Ok(outcome) => {
            assert_eq!(
                rows(&outcome),
                vec![
                    vec!["a"],
                    vec!["b"],
                    vec!["c"],
                    vec!["d"],
                    vec!["e"],
                    vec!["f"]
                ]
            );
        }
        Err(EsqlError::Execution(SqlError::Catalog(error))) => {
            let error = SqlError::Catalog(error);
            assert_eq!(error.class(), ErrorClass::Unsupported, "{error}");
        }
        Err(other) => panic!("neither ran nor was refused by the catalog: {other}"),
    }
}
