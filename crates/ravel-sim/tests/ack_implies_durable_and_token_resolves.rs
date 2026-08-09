//! Proves invariants (a) and (b) from ADR-0068 deliverable 5 hold on a
//! small seeded cycle: every commit token `IngestRouter` returns resolves
//! via `Catalog::resolve`, and the acked samples come back from a query
//! both immediately (read-your-write) and after the fold (strict-ack-
//! implies-durable, with no pinned tokens). `run_cycle` runs both checks
//! internally and returns `Err` on violation, so success here is the
//! assertion.
//!
//! "prove-the-test": confirmed by commenting out the `catalog.fold(...)`
//! call in `src/driver.rs`'s `run_cycle_async` (the fold between the two
//! `check_visible` calls). With the fold skipped, the second check --
//! invariant (a), which queries with no pinned `min_tokens` and so depends
//! on the catalog's own sealed snapshot rather than the caller's tokens --
//! failed with `CycleError::AckNotDurable` as expected, since nothing had
//! made the write listable outside of the caller's own token pin yet. The
//! fold call was then restored.

use ravel_sim::workload::{CardinalityShape, WorkloadConfig};
use ravel_sim::{CycleConfig, MasterSeed};

#[test]
fn ack_implies_durable_and_token_resolves() {
    let config = CycleConfig {
        workload: WorkloadConfig {
            tenant_count: 1,
            series_per_tenant: 3,
            samples_per_series: 4,
            cardinality: CardinalityShape::ManySmallLabels,
            queries_per_tenant: 2,
            ..WorkloadConfig::default()
        },
        ..CycleConfig::default()
    };

    let outcome = ravel_sim::run_cycle(MasterSeed::new(7), &config)
        .expect("small seeded cycle should satisfy read-your-write and ack-durability");

    assert_eq!(outcome.tenants_run, 1);
    assert_eq!(outcome.series_generated, 3);
    assert!(outcome.queries_run > 0);
}
