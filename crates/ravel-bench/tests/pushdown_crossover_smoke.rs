//! Smoke test for the `count_over_time` pushdown crossover bench core (ADR-0103,
//! epic #64): runs `pushdown_crossover::run` over a tiny `MemoryStore` corpus
//! and asserts the STRUCTURAL signal that each arm actually took the path it
//! claims. This is the CI/smoke-runnable target for the bench; it exercises the
//! same `run` path the `pushdown_crossover_bench` bin runs (mirrors
//! `distrib_crossover_smoke.rs`).
//!
//! The load-bearing assertion is the eligibility gate itself: a small smoke
//! corpus can produce a wall-time or byte-count delta too small or in the wrong
//! direction to assert reliably, so instead this calls the PUBLIC, pure
//! `ravel_query::distrib::is_pushdown_eligible` on the exact `(segments,
//! generations)` each arm resolves -- the same inputs `run` feeds its own gate
//! -- and asserts `true` for the eligible arm and `false` for the ineligible
//! one. That cannot be satisfied by accident. The count-agreement check is a
//! secondary correctness sanity.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_bench::pushdown_crossover::{PushdownCrossoverConfig, build_arm, run};
use ravel_object_store::memory::MemoryStore;
use ravel_query::distrib::is_pushdown_eligible;

/// The primary signal: `is_pushdown_eligible` on each arm's resolved inputs is
/// `true` for the eligible arm and `false` for the ineligible arm, exercising
/// BOTH conditions at least once each. Public, pure, and cannot pass by
/// accident.
#[tokio::test]
async fn eligible_and_ineligible_arms_flip_the_gate() {
    let store = Arc::new(MemoryStore::new());

    let eligible = build_arm(Arc::clone(&store) as _, 4, 4, true).await;
    assert_eq!(
        eligible.segments.len(),
        2,
        "the eligible arm resolves the two-segment corpus"
    );
    assert!(
        is_pushdown_eligible(None, &eligible.segments, &eligible.generations),
        "single stable generation over the resolved segments must be pushdown-eligible"
    );

    let ineligible = build_arm(Arc::clone(&store) as _, 4, 4, false).await;
    assert_eq!(
        ineligible.segments.len(),
        2,
        "the ineligible arm resolves the identical two-segment corpus"
    );
    assert!(
        !is_pushdown_eligible(None, &ineligible.segments, &ineligible.generations),
        "two segments straddling a shard-generation boundary must be pushdown-ineligible"
    );
}

/// The full `run` path over a smoke config: one eligible and one ineligible arm
/// per target-series value, each arm's observed gate decision matching its
/// requested condition, and the query answer identical across the two paths.
#[tokio::test]
async fn run_reports_both_arms_with_agreeing_counts() {
    let store = Arc::new(MemoryStore::new());
    let config = PushdownCrossoverConfig::smoke(store, "memory".to_string());
    let report = run(&config).await;

    assert_eq!(
        report.arms.len(),
        config.target_series.len() * 2,
        "one eligible plus one ineligible arm per target_series"
    );

    for a in &report.arms {
        assert_eq!(
            a.pushdown_eligible, a.eligible,
            "arm target_series={} eligible={} observed gate={}: the resolved inputs must \
             gate to the requested condition",
            a.target_series, a.eligible, a.pushdown_eligible
        );
        assert_eq!(a.corpus_segments, 2, "each arm resolves two segments");
        assert!(
            a.matched_series > 0,
            "the query must match series (else the crossover measures nothing)"
        );
    }

    // Both arms present at each series count, and the query answer must not
    // depend on which path served it: matched series and summed count agree.
    for &ts in &config.target_series {
        let eligible = report
            .arms
            .iter()
            .find(|a| a.target_series == ts && a.eligible)
            .expect("an eligible arm at this series count");
        let ineligible = report
            .arms
            .iter()
            .find(|a| a.target_series == ts && !a.eligible)
            .expect("an ineligible arm at this series count");
        assert_eq!(
            eligible.matched_series, ineligible.matched_series,
            "matched series must be identical across paths at target_series={ts}"
        );
        assert_eq!(
            eligible.evaluated_count_sum, ineligible.evaluated_count_sum,
            "the count answer must be identical across paths at target_series={ts}"
        );
        assert_eq!(
            eligible.matched_series, ts,
            "every published series matches the selector at target_series={ts}"
        );
    }
}
