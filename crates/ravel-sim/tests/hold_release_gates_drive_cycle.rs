//! Exercises the schedule's hold/release gate scripts (ADR-0068 decision 4,
//! ADR-0059 decision 5): with `enable_gates` on, the driver arms the
//! seed-derived gates on the `FaultStore`, and a call matching an armed gate
//! parks on its release channel until released. This drives the hold/release
//! path (a call parking, then resuming) through a real cycle without
//! deadlocking, and every invariant must still hold.
//!
//! "prove-the-test" (issue #878): a gate that never holds anything must not
//! let the cycle pass silently, and a held call must be provably blocking,
//! not just configured. Three tests carry that:
//!
//! - `hold_release_gates_drive_cycle`: the seed-derived schedule (as
//!   production nightly/PR runs would use it) drives a full cycle to a clean
//!   finish, and `CycleOutcome::gates_held` proves whichever seeds armed a
//!   gate actually held a call (previously nothing asserted this at all: a
//!   no-op gate -- wrong `Nth`, too few matching calls -- passed silently).
//! - `hold_release_gate_blocks_the_cycle_until_released`: a
//!   deterministically-reachable gate (`fault_schedule_override`, so the
//!   test does not depend on the seed generator's random `Nth` choice)
//!   proves the *blocking* property itself: the cycle has genuinely not
//!   completed while the gate holds the call, only after it is released.
//! - `never_hit_gate_fails_the_cycle`: a deterministically-unreachable gate
//!   proves the exact false-green the issue describes is now closed --
//!   `run_cycle` fails loud (`CycleError::GateNeverHit`) instead of quietly
//!   aborting the parked waiter and returning `Ok`.

use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::fault::{FaultPlan, Occurrence, Op};
use ravel_sim::fault_plan::{FaultSchedule, GateScript, L0_DATA_SUBSTR};
use ravel_sim::{CycleConfig, CycleError, GateRelease, MasterSeed, run_cycle};

#[test]
fn hold_release_gates_drive_cycle() {
    let config = CycleConfig {
        enable_gates: true,
        ..CycleConfig::default()
    };

    for seed in 1u64..=4 {
        let outcome = run_cycle(MasterSeed::new(seed), &config).unwrap_or_else(|err| {
            panic!("seed {seed}: cycle with gates armed violated an invariant: {err}")
        });
        assert!(outcome.buckets_compacted > 0, "seed {seed}: no compaction");
        assert!(outcome.queries_run > 0, "seed {seed}: no queries");
        // A no-op gate (matched nothing) can no longer reach this line at all:
        // `run_cycle` itself fails loud when `gates_armed > 0` and nothing was
        // ever held (see `never_hit_gate_fails_the_cycle`). This assertion is
        // the direct, redundant-by-construction proof for this seed's own
        // schedule, not just a trust in that invariant holding elsewhere.
        assert_eq!(
            outcome.gates_armed > 0,
            outcome.gates_held > 0,
            "seed {seed}: gates_armed={} gates_held={} (a reached Ok with an armed, \
             never-held gate would be exactly the bug issue #878 describes)",
            outcome.gates_armed,
            outcome.gates_held
        );
    }
}

/// One gate on the first matching L0 data PUT, deterministically reachable
/// (every tenant's ingest writes at least one), with no scripted faults --
/// isolates the hold/release path from the separate fault-rule schedule so
/// this test cannot be confused by an unrelated retry.
fn single_reachable_gate() -> FaultSchedule {
    FaultSchedule {
        plan: FaultPlan::empty(),
        gates: vec![GateScript {
            op: Op::Put,
            key_contains: Some(L0_DATA_SUBSTR.to_string()),
            occurrence: Occurrence::Nth(1),
        }],
        expected_faults: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hold_release_gate_blocks_the_cycle_until_released() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let hook_fired = Arc::new(std::sync::Mutex::new(Some(tx)));
    let config = CycleConfig {
        enable_gates: true,
        fault_schedule_override: Some(single_reachable_gate()),
        gate_release: GateRelease::Manual(Arc::new(move |handle| {
            if let Some(tx) = hook_fired.lock().unwrap().take() {
                let _ = tx.send(handle);
            }
        })),
        // Real wall-clock time, not the driver's usual paused clock: under a
        // paused clock, tokio auto-advances past `ack_deadline` the instant
        // the runtime goes idle parked on the held call, timing the write
        // out instead of leaving it observably blocked (see
        // `CycleConfig::paused_clock`'s doc). No jitter, so the tenant loop
        // reaches the held write immediately under real time.
        paused_clock: false,
        max_jitter: Duration::ZERO,
        ..CycleConfig::default()
    };

    let cycle = tokio::task::spawn_blocking(move || run_cycle(MasterSeed::new(1), &config));

    let gate = rx.await.expect("hook fires exactly once the gate is armed");
    gate.wait_until_held(1).await;
    let held = gate.held();
    assert_eq!(held.len(), 1, "exactly the Nth(1) call should be held");

    // Give a would-be auto-releaser a real window to have already fired.
    // This is the flipped line: with the old instant releaser (or a
    // background task that frees the call on its own, restored temporarily
    // to prove this), `held_count()` drops to 0 well inside this sleep, and
    // this assertion fails -- see the driver's `GateRelease::Manual` arm,
    // which spawns nothing, for why it stays 1 here.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        gate.held_count(),
        1,
        "the call must still be held -- nothing but this test released it"
    );

    assert!(
        !cycle.is_finished(),
        "the cycle must still be blocked on the held call, not completed"
    );

    for id in held {
        assert!(gate.release(id), "the held call must still be releasable");
    }

    // The rest of the cycle now runs on real wall-clock time (see
    // `paused_clock: false` above), including `ravel-ingest`'s real
    // `max_flush_delay`/`flush_tick` buffering, so this join legitimately
    // takes a handful of real seconds rather than the microseconds a paused
    // clock gives every other test in this crate.
    let outcome = cycle
        .await
        .expect("join")
        .expect("cycle completes cleanly once the gate releases");
    assert_eq!(outcome.gates_armed, 1);
    assert_eq!(outcome.gates_held, 1);
}

#[test]
fn never_hit_gate_fails_the_cycle() {
    // `Nth(1_000_000)` cannot be reached by any workload this harness
    // generates: this is the exact false-green the issue names -- a gate
    // that matches zero calls must fail the cycle, not silently pass with
    // the parked waiter aborted.
    let unreachable = FaultSchedule {
        plan: FaultPlan::empty(),
        gates: vec![GateScript {
            op: Op::Put,
            key_contains: Some(L0_DATA_SUBSTR.to_string()),
            occurrence: Occurrence::Nth(1_000_000),
        }],
        expected_faults: Vec::new(),
    };
    let config = CycleConfig {
        enable_gates: true,
        fault_schedule_override: Some(unreachable),
        ..CycleConfig::default()
    };

    let err = run_cycle(MasterSeed::new(1), &config)
        .expect_err("a gate that never matches must fail the cycle, not pass it");
    assert!(
        matches!(err, CycleError::GateNeverHit { armed: 1, .. }),
        "expected GateNeverHit, got: {err}"
    );
}
