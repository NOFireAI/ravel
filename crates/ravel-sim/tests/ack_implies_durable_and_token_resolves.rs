//! Proves invariants (a) and (b) from ADR-0068 hold on a
//! small seeded cycle: every commit token `IngestRouter` returns resolves
//! via `Catalog::resolve`, and the acked samples come back from a query
//! both immediately (read-your-write) and after the fold (strict-ack-
//! implies-durable, with no pinned tokens). `run_cycle` runs both checks
//! internally and returns `Err` on violation, so success here is the
//! assertion.
//!
//! "prove-the-test": this test does NOT prove the fold is load-bearing, and
//! an earlier version of this comment claimed it did -- that claim was
//! false. Commenting out the `catalog.fold(...)` call in
//! `src/driver.rs`'s `run_cycle_async` and re-running this test leaves it
//! passing. Two separate resolves happen per checked series here, and
//! neither depends on the fold at this workload's scale:
//!
//! - `check_visible`'s own per-token loop (`catalog.resolve(...,
//!   min_tokens=[token], ...)`, run for every token regardless of
//!   `CheckKind`) is satisfied by `Catalog::resolve_min_token`'s direct GET
//!   on that token's own commit-record key. That GET succeeds whether or
//!   not any fold ever ran, because fold does not delete or move the L0
//!   commit record it reads.
//! - Invariant (a)'s query (`CheckKind::AckDurable`, `query_min_tokens =
//!   &[]`) resolves with no pinned tokens, so it is the one path a fold
//!   could actually make cheaper -- but at this test's scale (one shard,
//!   one tenant, a handful of samples in a single hour) the per-bucket
//!   commit-record LIST fallback (`Catalog::resolve`'s per-(shard, hour)
//!   listing pass) satisfies it just as well with no fold at all. The fold
//!   changes *how* this resolves, not *whether* it resolves, so this test
//!   cannot tell the two cases apart by pass/fail.
//!
//! The genuine, fault-injection-backed proof that the fold is load-bearing
//! for a no-pinned-tokens resolve -- it lets that resolve be served
//! entirely from the folded snapshot's GET-based part index, with the
//! commit-record LIST fallback disabled and never hit -- lives in
//! `tests/fold_makes_resolve_list_free.rs`. This test still exercises the
//! fold as part of a realistic cycle and still asserts every token resolves
//! and every acked sample is readable before and after it; it just does not,
//! by itself, demonstrate that the fold was necessary for either.

#![allow(clippy::expect_used, clippy::unwrap_used)]

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

    let outcome = ravel_sim::run_cycle(MasterSeed::from_env_or(7), &config)
        .expect("small seeded cycle should satisfy read-your-write and ack-durability");

    assert_eq!(outcome.tenants_run, 1);
    assert_eq!(outcome.series_generated, 3);
    assert!(outcome.queries_run > 0);
}
