//! Proves the workload generator is actually seed-sensitive at the
//! integration level (`src/workload.rs`'s unit tests already cover this for
//! a single tenant; this exercises the public `ravel_sim::workload` surface
//! across every tenant).
//!
//! "prove-the-test": confirmed by temporarily hardcoding
//! `MasterSeed::new(1)` for both calls below (deleting the `b` seed
//! argument's `2`) -- with both sides seeded identically the assertion
//! failed as expected, since the generated series ids were then equal.
//! Restored to the real seeds 1 and 2 afterward.

use ravel_sim::MasterSeed;
use ravel_sim::workload::{WorkloadConfig, generate};

#[test]
fn different_seed_different_workload() {
    let config = WorkloadConfig::default();
    let a = generate(&MasterSeed::new(1), &config).expect("generate a");
    let b = generate(&MasterSeed::new(2), &config).expect("generate b");

    assert_eq!(a.tenants.len(), b.tenants.len());

    let ids_a: Vec<_> = a
        .tenants
        .iter()
        .flat_map(|t| t.series.iter().map(|s| s.series_id))
        .collect();
    let ids_b: Vec<_> = b
        .tenants
        .iter()
        .flat_map(|t| t.series.iter().map(|s| s.series_id))
        .collect();

    assert_ne!(
        ids_a, ids_b,
        "two different master seeds produced the same generated series ids"
    );
}
