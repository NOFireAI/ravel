//! Proves F13's fix: `WorkloadConfig::interval_ns` wide enough that one
//! tenant's samples span multiple event-time hours must not fail
//! `strict-ack-implies-durable`, even though every commit's
//! `ingest_hour_bucket` derives from the wall/arrival clock read at flush
//! time (`ravel-ingest`'s `checked_ingest_hour_bucket`, fed by
//! `flush_open_ns`), not from any sample's own event timestamp.
//!
//! "prove-the-test": before the fix, `run_cycle_async` ingested a whole
//! tenant's samples in one `write_values` call at a single simulated
//! instant, so every commit landed under one `ingest_hour_bucket`
//! regardless of how far apart the samples' own event timestamps were.
//! Reproduced by running this exact config (interval_ns = 30 minutes,
//! default tenant/series/sample counts) against the pre-fix driver: every
//! one of seeds 1..=5 failed identically with
//! `CycleError::AckNotDurable`, e.g. (seed 1) "strict-ack-implies-durable
//! violated for tenant sim-tenant-000: commit token
//! djI6MDoyYjFlMjNkMS1hMmY4LTRlZWMtOWViZS1iN2EwZjJjNzdkNDk6MTcwMDAwMDAwMDowOjQ3MjIyMg
//! did not resolve after the fold, or the folded snapshot did not return
//! the acked samples". The fix (advancing the injected `SimClock` to each
//! ingest batch's own event timestamp, batching by sample index across a
//! tenant's series, before every `write_values` call) was then confirmed
//! to make every one of the same five seeds pass. To reproduce the
//! pre-fix failure again directly, temporarily replace
//! `sim_clock.advance_to(batch_ts_ns.unwrap_or(workload.start_ts_ns));` in
//! `src/driver.rs`'s `run_cycle_async` with a no-op: this test then fails
//! with the same `AckNotDurable` message shown above.

use ravel_sim::workload::WorkloadConfig;
use ravel_sim::{CycleConfig, MasterSeed};

#[test]
fn wide_interval_spans_ingest_hours() {
    let config = CycleConfig {
        workload: WorkloadConfig {
            interval_ns: 30 * 60 * 1_000_000_000,
            ..WorkloadConfig::default()
        },
        ..CycleConfig::default()
    };

    for seed in 1u64..=5 {
        let outcome = ravel_sim::run_cycle(MasterSeed::new(seed), &config).unwrap_or_else(|e| {
            panic!(
                "seed {seed}: expected a wide-interval workload to satisfy every invariant, got {e}"
            )
        });
        assert!(outcome.tenants_run > 0);
    }
}
