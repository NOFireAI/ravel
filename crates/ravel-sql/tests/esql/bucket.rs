//! `BUCKET` over a fixed time duration only (ADR-2887 D9).

use ravel_sql::esql::EsqlError;

use crate::support::{fixture, prepare, rows, run};

fn unsupported(source: &str) -> (String, usize, usize) {
    match prepare(source) {
        Err(EsqlError::Unsupported { name, line, column }) => (name, line, column),
        other => panic!("{source}: expected an unsupported error, got {other:?}"),
    }
}

fn by_bucket(width: &str) -> String {
    format!("FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, {width})")
}

#[test]
fn a_date_period_width_is_refused_with_its_unit_and_span() {
    for (width, unit) in [
        ("1 week", "week"),
        ("2 weeks", "weeks"),
        ("1 day", "day"),
        ("3 d", "d"),
        ("1 month", "month"),
        ("1 quarter", "quarter"),
        ("1 year", "year"),
    ] {
        assert_eq!(
            unsupported(&by_bucket(width)),
            (
                format!(
                    "BUCKET over the date period [{width}] (unit [{unit}]); BUCKET takes a fixed time duration in milliseconds, seconds, minutes or hours"
                ),
                1,
                58
            ),
            "{width}"
        );
    }
    // A period inside duration arithmetic is refused too, at the period.
    assert_eq!(
        unsupported(&by_bucket("1 day - 23 hours")).2,
        58,
        "1 day - 23 hours"
    );
    assert_eq!(
        unsupported(&by_bucket("1 hour + 1 week")).0,
        "BUCKET over the date period [1 week] (unit [week]); BUCKET takes a fixed time duration in milliseconds, seconds, minutes or hours"
    );
}

#[test]
fn date_arithmetic_with_a_day_or_a_week_still_binds() {
    for source in [
        "FROM logs | WHERE @timestamp >= NOW() - 1 day",
        "FROM logs | EVAL t = @timestamp + 1 week | KEEP t",
        "FROM logs | EVAL t = @timestamp - (1 day + 2 hours) | KEEP t",
    ] {
        prepare(source).unwrap_or_else(|e| panic!("{source}: {e}"));
    }
}

/// The fixture's rows sit at 22:13:00, :10, 22:14:10, :20, 22:15:10 and :20.
/// Five-minute buckets are aligned to the epoch, so the first bucket starts
/// at 22:10:00, not at the first row's 22:13:00.
#[tokio::test]
async fn a_time_duration_width_buckets_from_the_epoch() {
    let fixture = fixture().await;
    for (width, expected) in [
        ("1 hour", vec![["6", "2023-11-14T22:00:00"]]),
        (
            "5 minutes",
            vec![["4", "2023-11-14T22:10:00"], ["2", "2023-11-14T22:15:00"]],
        ),
        (
            "30 seconds",
            vec![
                ["2", "2023-11-14T22:13:00"],
                ["2", "2023-11-14T22:14:00"],
                ["2", "2023-11-14T22:15:00"],
            ],
        ),
    ] {
        let source = format!("{} | SORT b", by_bucket(width));
        let outcome = run(&fixture, &source)
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"));
        let expected: Vec<Vec<String>> = expected
            .iter()
            .map(|row| row.iter().map(|cell| cell.to_string()).collect())
            .collect();
        assert_eq!(rows(&outcome), expected, "{width}");
    }
}
