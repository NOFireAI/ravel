//! The SQL executor derives an equality `__name__` filter from
//! the query's pushed-down predicates and resolves through
//! `Catalog::resolve_pruned`, so the measured postings pruning is reachable
//! from SQL.
//!
//! The observable proof is the resolved snapshot size: a query whose
//! `WHERE label(labels,'__name__') = 'aaa'` pins one metric resolves to only
//! the segment carrying it, while the same query without the predicate
//! resolves both segments. That difference exists only if `resolve_pruned`
//! ran with the derived name.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use ravel_types::Signal;
use util::{Fixture, NOW_NS, SegSpec, SeriesSpec, request, tenant_id};
use uuid::Uuid;

#[tokio::test]
async fn sql_pushed_down_name_filter_reaches_resolve_pruned() {
    let tenant = tenant_id("prune");
    // Two segments, each carrying one distinct metric, both in ingest hour 0
    // (event-stamped inside the fixture's full window).
    let specs = vec![
        SegSpec::new(
            1,
            1,
            1,
            vec![SeriesSpec::new("aaa", vec![(60_000_000_000, 1.0)])],
        ),
        SegSpec::new(
            2,
            1,
            2,
            vec![SeriesSpec::new("bbb", vec![(60_000_000_000, 2.0)])],
        ),
    ];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    // Fold so a snapshot with a name-postings index exists. Hour 0 is sealed
    // at NOW_NS (4h) under the default seal margins (~80m).
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
    assert!(
        report.postings_built,
        "the fold must attach a postings object for pruning to be reachable"
    );

    // A query pinning __name__ = 'aaa' must prune to the single segment that
    // carries it. This is only possible because the executor derives the name
    // filter from the pushed-down predicate and resolves through
    // `resolve_pruned`.
    let pruned = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT ts FROM samples WHERE label(labels, '__name__') = 'aaa'"),
        )
        .await
        .expect("execute pruned query");
    assert_eq!(
        pruned.stats.segments, 1,
        "postings pruning must drop the 'bbb' segment, leaving one"
    );

    // Control: the same table without a name predicate resolves both
    // segments, proving the difference is the pushed-down filter, not the
    // fixture.
    let all = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts FROM samples"))
        .await
        .expect("execute unfiltered query");
    assert_eq!(
        all.stats.segments, 2,
        "without a name filter both segments resolve"
    );
}
