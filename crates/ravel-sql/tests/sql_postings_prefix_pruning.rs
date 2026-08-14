//! Prefix-anchored postings pruning (ADR-0061 decision 3): the SQL executor
//! derives a literal-prefix
//! `__name__` filter from a pushed-down `label_match(labels, '__name__',
//! '^foo.*$')` predicate and resolves through the catalog's sorted-name range
//! scan, so prefix-anchored regex pruning is reachable from SQL, on the same
//! path equality pruning uses.
//!
//! Mirrors `sql_postings_pruning.rs`: the observable proof is the resolved
//! snapshot size, plus a correctness oracle that runs the SAME selection two
//! ways -- once pruned (`^prefix.*$`, accepted by the detector) and once with
//! pruning forcibly disabled (`^prefix.*.*$`, an extra `.*` the detector
//! rejects while the regex still matches the identical set) -- and asserts the
//! result rows are byte-identical.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use datafusion::arrow::util::pretty::pretty_format_batches;
use ravel_sql::SqlOutcome;
use ravel_types::Signal;
use util::{Fixture, NOW_NS, SegSpec, SeriesSpec, request, tenant_id};
use uuid::Uuid;

/// One metric per segment, in ascending sort order; "app" is a prefix of
/// "apple" (the aliasing case). Values are the index so every row is distinct.
const METRICS: [&str; 6] = ["alpha", "apex", "app", "apple", "banana", "zebra"];

async fn folded_fixture() -> Fixture {
    let tenant = tenant_id("prune");
    let specs: Vec<SegSpec> = METRICS
        .iter()
        .enumerate()
        .map(|(i, metric)| {
            SegSpec::new(
                (i + 1) as i64,
                1,
                (i + 1) as u64,
                vec![SeriesSpec::new(metric, vec![(60_000_000_000, i as f64)])],
            )
        })
        .collect();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let report = fixture
        .catalog
        .fold(&tenant.hash(), Signal::Metrics, Uuid::new_v4(), NOW_NS, &[])
        .await
        .expect("fold");
    assert!(
        report.postings_built,
        "the fold must attach a postings object for prefix pruning to be reachable"
    );
    fixture
}

async fn run(fixture: &Fixture, sql: &str) -> SqlOutcome {
    fixture
        .executor
        .execute(tenant_id("prune").hash(), &request(sql))
        .await
        .unwrap_or_else(|e| panic!("execute {sql:?} failed: {e:?}"))
}

fn rendered(outcome: &SqlOutcome) -> String {
    pretty_format_batches(outcome.output.batches())
        .expect("format batches")
        .to_string()
}

/// The correctness oracle. For each prefix shape, the pruned query and its
/// unpruned twin (`^prefix.*.*$`) must return byte-identical rows, and the
/// pruned run must resolve exactly the expected (smaller) segment set.
#[tokio::test]
async fn prefix_pruned_matches_unpruned_across_boundary_cases() {
    let fixture = folded_fixture().await;

    // (prefix, expected resolved-segment count under pruning).
    let cases = [
        ("qux", 0usize), // matches zero names -> prune everything
        ("alph", 1),     // exactly the first name in sort order
        ("zeb", 1),      // exactly the last name in sort order
        ("ap", 3),       // multiple distinct names (apex, app, apple)
        ("app", 2),      // one name (app) that is a prefix of another (apple)
        ("banana", 1),   // a whole-name prefix that aliases nothing
    ];

    for (prefix, expected_segments) in cases {
        let pruned_sql = format!(
            "SELECT label(labels, '__name__') AS name, value FROM samples \
             WHERE label_match(labels, '__name__', '^{prefix}.*$') ORDER BY name"
        );
        let unpruned_sql = format!(
            "SELECT label(labels, '__name__') AS name, value FROM samples \
             WHERE label_match(labels, '__name__', '^{prefix}.*.*$') ORDER BY name"
        );

        let pruned = run(&fixture, &pruned_sql).await;
        let unpruned = run(&fixture, &unpruned_sql).await;

        assert_eq!(
            rendered(&pruned),
            rendered(&unpruned),
            "prefix {prefix:?}: pruned and unpruned rows must be byte-identical"
        );
        assert_eq!(
            pruned.stats.segments, expected_segments,
            "prefix {prefix:?}: pruned resolve segment count"
        );
        assert_eq!(
            unpruned.stats.segments,
            METRICS.len(),
            "prefix {prefix:?}: the unpruned twin must resolve every segment"
        );
    }
}

/// Effectiveness (mirrors `sql_postings_pruning.rs`): a prefix query
/// resolves strictly fewer segments than the same table with no name filter.
#[tokio::test]
async fn prefix_scan_narrows_the_resolve() {
    let fixture = folded_fixture().await;

    let pruned = run(
        &fixture,
        "SELECT ts FROM samples WHERE label_match(labels, '__name__', '^ap.*$')",
    )
    .await;
    assert_eq!(
        pruned.stats.segments, 3,
        "prefix ^ap.*$ prunes to apex|app|apple"
    );

    let all = run(&fixture, "SELECT ts FROM samples").await;
    assert_eq!(
        all.stats.segments, 6,
        "without a name filter all segments resolve"
    );
    assert!(
        pruned.stats.segments < all.stats.segments,
        "the prefix range scan must narrow the resolve"
    );
}

/// A non-prefix regex on `__name__` (infix wildcard or alternation) must still
/// take the unpruned path -- proving the detector is not overly permissive.
#[tokio::test]
async fn non_prefix_regex_still_bypasses_pruning() {
    let fixture = folded_fixture().await;

    // Infix wildcard: resolves every segment (no prune), residual filter
    // selects names containing "ap" (apex, app, apple).
    let infix = run(
        &fixture,
        "SELECT label(labels, '__name__') AS name FROM samples \
         WHERE label_match(labels, '__name__', '.*ap.*') ORDER BY name",
    )
    .await;
    assert_eq!(
        infix.stats.segments, 6,
        "an infix-wildcard regex must bypass pruning (all segments resolved)"
    );
    assert_eq!(
        infix.output.num_rows(),
        3,
        "apex, app, apple match the residual"
    );

    // Alternation: resolves every segment, residual selects exactly app|apple.
    let alt = run(
        &fixture,
        "SELECT label(labels, '__name__') AS name FROM samples \
         WHERE label_match(labels, '__name__', 'app|apple') ORDER BY name",
    )
    .await;
    assert_eq!(
        alt.stats.segments, 6,
        "an alternation regex must bypass pruning (all segments resolved)"
    );
    assert_eq!(alt.output.num_rows(), 2, "app and apple match the residual");
}
