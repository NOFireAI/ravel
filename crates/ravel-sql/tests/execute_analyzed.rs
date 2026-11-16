//! `PinnedQuery::execute_analyzed` (ADR-2867 change 7 item 3, issue #2881):
//! it runs the statement to completion, counts the rows, and renders the
//! executed plan with each operator's metrics.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use ravel_types::TenantId;
use ravel_types::accounting::QueryAccounting;
use util::{Fixture, SegSpec, SeriesSpec, tenant_id};

/// Ten samples whose values cycle through three distinct values, so the
/// grouped aggregate below returns exactly three rows.
const GROUPED: &str = "SELECT value, count(*) AS n FROM samples GROUP BY value";
const GROUPS: usize = 3;

fn tenant() -> TenantId {
    tenant_id("execute-analyzed-2881")
}

async fn fixture() -> Fixture {
    let samples = (0..10).map(|i| (i, (i % 3) as f64)).collect();
    let specs = vec![SegSpec::new(10, 1, 1, vec![SeriesSpec::new("m", samples)])];
    Fixture::memory(&[(&tenant(), &specs)]).await
}

async fn planned(fixture: &Fixture) -> ravel_sql::PinnedQuery {
    fixture
        .executor
        .plan_pinned(
            tenant().hash(),
            fixture.snapshot(&tenant()).await,
            GROUPED,
            &QueryAccounting::new(),
            &[],
        )
        .await
        .expect("plan_pinned")
}

/// The value of `name=N` on `line`, when the line carries one.
fn metric(line: &str, name: &str) -> Option<usize> {
    let at = line.find(&format!("{name}="))? + name.len() + 1;
    let digits: String = line[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// The rows are the statement's exact result count, and the rendered plan is
/// the executed one: the root operator carries a `metrics=` section whose
/// `output_rows` equals that count, and the final aggregate's line reports
/// the same nonzero figure.
#[tokio::test]
async fn execute_analyzed_counts_rows_and_renders_the_executed_metrics() {
    let fixture = fixture().await;
    let run = planned(&fixture)
        .await
        .execute_analyzed()
        .await
        .expect("statement runs");
    let plan = &run.plan_with_metrics;
    assert_eq!(run.rows, GROUPS, "{plan}");
    assert!(!run.row_cap_hit);

    let root = plan.lines().next().expect("a non-empty plan");
    assert!(root.contains("metrics=["), "root carries metrics:\n{plan}");
    assert_eq!(metric(root, "output_rows"), Some(GROUPS), "{plan}");

    let aggregate = plan
        .lines()
        .find(|line| line.trim_start().starts_with("AggregateExec: mode=Final"))
        .unwrap_or_else(|| panic!("no final aggregate:\n{plan}"));
    assert_eq!(metric(aggregate, "output_rows"), Some(GROUPS), "{plan}");
    assert!(
        aggregate.contains("elapsed_compute="),
        "the aggregate reports its compute time:\n{plan}"
    );
}

/// The row cap applies as it does on `execute`: the stream stops after the
/// cap plus one row and says so.
#[tokio::test]
async fn execute_analyzed_honours_the_row_cap() {
    let fixture = fixture().await;
    let run = planned(&fixture)
        .await
        .with_row_cap(Some(1))
        .execute_analyzed()
        .await
        .expect("statement runs");
    assert_eq!(run.rows, 2, "{}", run.plan_with_metrics);
    assert!(run.row_cap_hit);
}
