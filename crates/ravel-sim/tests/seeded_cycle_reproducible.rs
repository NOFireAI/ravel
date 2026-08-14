//! Proves reproducibility (ADR-0068): two runs of the same master seed through
//! the whole ingest -> fold -> query cycle produce identical digests.
//!
//! "prove-the-test": this was confirmed to actually catch a break by
//! temporarily widening `mix_value`'s `Value::Vector` arm in
//! `src/digest.rs` to also mix a per-run-unique value (a commit-token-shaped
//! stand-in, `builder.mix_u64(std::process::id() as u64)` is not usable
//! since it is stable within one test process, so the token itself was
//! mixed in directly for the check) -- with that line present, this test
//! failed as expected because the digest scope comment on `digest.rs`
//! promises tokens never leak in. The line was then removed to restore the
//! real (correct) scope. See `src/digest.rs`'s module doc for the full
//! narrative of this check.

use ravel_sim::{CycleConfig, MasterSeed, run_cycle};

#[test]
fn seeded_cycle_reproducible() {
    let seed = MasterSeed::from_env_or(4242);
    let config = CycleConfig::default();

    let a = run_cycle(seed, &config).expect("first cycle");
    let b = run_cycle(seed, &config).expect("second cycle");

    assert_eq!(
        a.digest, b.digest,
        "seed {}: same master seed produced different digests",
        seed.0
    );
    assert_eq!(a.tenants_run, b.tenants_run);
    assert_eq!(a.series_generated, b.series_generated);
    assert_eq!(a.queries_run, b.queries_run);
}
