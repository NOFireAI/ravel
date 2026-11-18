//! Crash after each publish step (ADR-4233 decision 9, "Crash after each
//! publish step"): every store write that `activate`, `migrate` and `certify`
//! make, crashed in turn.
//!
//! A recording run of the uninterrupted pipeline (init, activate, migrate, the
//! two superseded-input sweep passes, certify, then the late-write pass)
//! enumerates each step's writes as `(operation, key)`. For every write of
//! activate, migrate and certify, a fresh store replays the pipeline with that
//! step run once over a `FaultStore` scripted to fail exactly that write (its
//! `Nth` occurrence among the step's writes to the same key), in two ways:
//!
//!   - applied, acknowledgement lost (`DuplicateDelivery`): the process dies
//!     after the store took the write;
//!   - not applied (`PartialWriteThenError` for a put, `Timeout` for a
//!     delete): the process dies before it.
//!
//! The process is dead from the fault on: every later store call of that run
//! fails without reaching the store, so nothing it would have done next lands.
//! The step is then re-run the way an operator re-runs it, and the rest of the
//! pipeline runs unfaulted, each later step in one run. The end state must
//! equal the uninterrupted run's byte for byte (every key and every body), the
//! hour's query must read exactly the seed sample from an N+1 object, and the
//! `FaultStore` counter must show the scripted fault fired exactly once.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
use ravel_catalog::compat::FAMILY_RSEG;
use ravel_maintain::{
    ActivateError, CertifyRefusal, CertifyReport, FenceCursor, Verification, activate,
    late_write_pass,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_segment::{ReaderLimits, VERSION_DRILL, open_from_full};
use ravel_types::METRIC_NAME_LABEL;

use common::DecodedSeries;
use common::pipeline::{
    Flush, HOUR_NS, METRIC, ManualClock, SECOND_NS, activate_request, certify_at, compactor_config,
    compat_keys, drill, hour_start_ns, init, migrate, provision, query_hour, sealed_now_ns,
    series_id, sweep, sweep_times, writable, write_flush,
};
use common::tap::{TapStore, Write};

const SHARD_COUNT: u32 = 1;
const SEED_VALUE: f64 = 42.0;
/// How many re-runs a step gets after a crash before the test gives up.
const MAX_RUNS: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Step {
    Activate,
    Migrate,
    SweepMark,
    SweepDelete,
    Certify,
}

const STEPS: [Step; 5] = [
    Step::Activate,
    Step::Migrate,
    Step::SweepMark,
    Step::SweepDelete,
    Step::Certify,
];

/// The steps whose writes are crashed. The sweep passes run unfaulted, so
/// certify has a store with no v7 member to certify.
const CRASHED: [Step; 3] = [Step::Activate, Step::Migrate, Step::Certify];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Crash {
    /// The store applied the write; the process died before the acknowledgement.
    AfterApply,
    /// The process died before the write reached the store.
    BeforeApply,
}

impl Crash {
    fn fault(self, op: Op) -> ScriptedFault {
        match (self, op) {
            (Crash::AfterApply, _) => ScriptedFault::DuplicateDelivery,
            (Crash::BeforeApply, Op::Put) => ScriptedFault::PartialWriteThenError,
            (Crash::BeforeApply, _) => ScriptedFault::Timeout,
        }
    }
}

/// The late-write pass that finalizes certify, an hour after the certify pass
/// (which runs at the sweep's deleting instant).
fn late_write_now(sweep_delete_ns: i64) -> i64 {
    sweep_delete_ns + HOUR_NS
}

/// The store before `activate`: one RSEG 7 object in a sealed hour, its
/// bucket provisioned, `sys/compat` initialised at the baseline.
async fn prestate() -> (Arc<MemoryStore>, Arc<ManualClock>) {
    let store = Arc::new(MemoryStore::with_page_size(2));
    let clock = Arc::new(ManualClock::new(sealed_now_ns()));
    write_flush(store.as_ref(), &Flush::v7(0, 0xC4A5, 1, SEED_VALUE)).await;
    provision(store.as_ref(), SHARD_COUNT).await;
    init(store.as_ref(), &clock).await;
    (store, clock)
}

/// One run of `step` against `store`. `Ok` once the step is complete; `Err`
/// with what it reported otherwise. `resume` carries an incomplete fence walk's
/// position from one activate run to the next.
async fn run_once(
    step: Step,
    store: &dyn ObjectStoreBackend,
    base: &Arc<MemoryStore>,
    clock: &Arc<ManualClock>,
    resume: &mut Option<FenceCursor>,
) -> Result<(), String> {
    let config = compactor_config(base.clone(), clock.clone()).await;
    let (mark_ns, delete_ns) = sweep_times(&config);
    match step {
        Step::Activate => {
            match activate(
                store,
                Some(&compat_keys()),
                clock.as_ref(),
                &writable(),
                &activate_request(*resume),
            )
            .await
            {
                Ok(report) if report.walk.complete => Ok(()),
                Ok(report) => {
                    *resume = report.walk.resume_from;
                    Err(format!("{:?}", report.walk))
                }
                // The family is active at N+1 and its fence walk finished:
                // the crash came after the last write of the step.
                Err(ActivateError::NotAboveActive { active, .. }) if active == drill() => Ok(()),
                Err(err) => Err(err.to_string()),
            }
        }
        Step::Migrate => match migrate(store, &config, SHARD_COUNT).await {
            Ok(report)
                if matches!(
                    report.verification,
                    Some(Verification::FloorRaised { floor_version }) if floor_version == drill()
                ) =>
            {
                Ok(())
            }
            Ok(report) => Err(format!("{report:?}")),
            Err(err) => Err(err.to_string()),
        },
        Step::SweepMark | Step::SweepDelete => {
            let now = if step == Step::SweepMark {
                mark_ns
            } else {
                delete_ns
            };
            sweep(store, &config, now, 0)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
        Step::Certify => match certify_at(store, delete_ns, None).await {
            Ok(CertifyReport::Certified { .. }) => Ok(()),
            // The certificate landed and the floors wait for a late-write pass
            // that started after it: the operator runs verify, then certify.
            Ok(pending @ CertifyReport::FinalizationPending { .. }) => {
                finalize(base.as_ref(), late_write_now(delete_ns)).await;
                Err(format!("{pending:?}"))
            }
            // The crash came after the last write of the step.
            Ok(CertifyReport::Refused(CertifyRefusal::AlreadyCertified { version }))
                if version == drill() =>
            {
                Ok(())
            }
            Ok(other) => Err(format!("{other:?}")),
            Err(err) => Err(err.to_string()),
        },
    }
}

/// Re-run `step` against the base store until it completes. Returns how many
/// runs it took.
async fn run_to_completion(
    step: Step,
    base: &Arc<MemoryStore>,
    clock: &Arc<ManualClock>,
    context: &str,
) -> usize {
    let mut resume = None;
    let mut last = String::new();
    for run in 1..=MAX_RUNS {
        match run_once(step, base.as_ref(), base, clock, &mut resume).await {
            Ok(()) => return run,
            Err(report) => last = report,
        }
    }
    panic!("{context}: {step:?} did not complete in {MAX_RUNS} runs; last: {last}");
}

async fn finalize(store: &dyn ObjectStoreBackend, now_ns: i64) {
    let report = late_write_pass(
        store,
        &compat_keys(),
        now_ns,
        FAMILY_RSEG,
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    )
    .await
    .expect("late-write pass");
    assert!(report.late_writes.is_empty(), "{report:?}");
}

async fn finish(base: &Arc<MemoryStore>, clock: &Arc<ManualClock>) {
    let config = compactor_config(base.clone(), clock.clone()).await;
    finalize(base.as_ref(), late_write_now(sweep_times(&config).1)).await;
}

/// The writes each step makes in an uninterrupted run, and that run's end
/// state.
async fn reference() -> (BTreeMap<Step, Vec<Write>>, BTreeMap<String, Vec<u8>>) {
    let (base, clock) = prestate().await;
    let mut writes = BTreeMap::new();
    for step in STEPS {
        let tap = TapStore::recording(base.clone());
        run_once(step, &tap, &base, &clock, &mut None)
            .await
            .unwrap_or_else(|report| panic!("reference: {step:?} completes in one run: {report}"));
        writes.insert(step, tap.take_writes());
    }
    finish(&base, &clock).await;
    assert_reads_the_seed(&base, "reference").await;
    (writes, common::snapshot(base.as_ref()).await)
}

/// What the hour's query reads at the end: one N+1 object holding exactly the
/// seed sample.
async fn assert_reads_the_seed(store: &Arc<MemoryStore>, context: &str) {
    let read = query_hour(store.clone(), SHARD_COUNT, sealed_now_ns() + 24 * HOUR_NS).await;
    assert_eq!(read.len(), 1, "{context}: one live object: {read:?}");
    let (key, series) = &read[0];
    let bytes = store.get(key, GetRange::Full).await.expect("get").data;
    assert_eq!(
        open_from_full(&bytes, ReaderLimits::default())
            .expect("open")
            .version,
        VERSION_DRILL,
        "{context}: the live object {key} is at N+1"
    );
    assert_eq!(
        series,
        &vec![DecodedSeries {
            labels: vec![(METRIC_NAME_LABEL.to_string(), METRIC.to_string())],
            series_id: series_id().0,
            samples: vec![(hour_start_ns() + SECOND_NS, SEED_VALUE.to_bits())],
        }],
        "{context}: the seed sample, bit for bit"
    );
}

/// The keys whose presence or bytes differ between two snapshots.
fn diff(want: &BTreeMap<String, Vec<u8>>, got: &BTreeMap<String, Vec<u8>>) -> Vec<String> {
    let mut out = Vec::new();
    for (key, body) in want {
        match got.get(key) {
            None => out.push(format!("missing {key}")),
            Some(other) if other != body => out.push(format!("bytes differ {key}")),
            Some(_) => {}
        }
    }
    for key in got.keys() {
        if !want.contains_key(key) {
            out.push(format!("extra {key}"));
        }
    }
    out
}

/// A write's key, with the tenant hash, writer and content hashes left out.
fn shape(write: &Write) -> (Op, &'static str) {
    let key = write.key.as_str();
    let class = if key == "sys/compat" {
        "sys/compat"
    } else if key == "sys/maintain/certify/rseg/cursor" {
        "certify cursor"
    } else if key.ends_with("/m/prov") {
        "provisioning"
    } else if key.ends_with("/maint/migrate/rseg/cursor") {
        "migrate cursor"
    } else if key.contains("/m/l1/") && key.ends_with(".rseg") {
        "l1 part"
    } else if key.contains("/m/c/") && key.ends_with(".cmt") {
        "compaction record"
    } else {
        "other"
    };
    (write.op, class)
}

#[tokio::test]
async fn a_crash_at_every_write_of_every_step_resumes_to_the_uninterrupted_end_state() {
    let (writes, want) = reference().await;

    // The enumeration itself: what each crashed step writes, in order. The
    // activate walk CASes sys/compat, fences the bucket and records the walk's
    // completion; migrate publishes the N+1 part and its compaction record,
    // drops its cursor and raises the floor; certify saves its census cursor
    // three times, CASes the certificate and drops the cursor.
    let shapes = |step: Step| writes[&step].iter().map(shape).collect::<Vec<_>>();
    assert_eq!(
        shapes(Step::Activate),
        [
            (Op::Put, "sys/compat"),
            (Op::Put, "provisioning"),
            (Op::Put, "sys/compat"),
        ]
    );
    assert_eq!(
        shapes(Step::Migrate),
        [
            (Op::Put, "l1 part"),
            (Op::Put, "compaction record"),
            (Op::Delete, "migrate cursor"),
            (Op::Put, "provisioning"),
        ]
    );
    assert_eq!(
        shapes(Step::Certify),
        [
            (Op::Put, "certify cursor"),
            (Op::Put, "certify cursor"),
            (Op::Put, "certify cursor"),
            (Op::Put, "sys/compat"),
            (Op::Delete, "certify cursor"),
        ]
    );

    let mut cases = 0usize;
    for step in CRASHED {
        let step_writes = &writes[&step];
        for (index, write) in step_writes.iter().enumerate() {
            let nth = step_writes[..=index]
                .iter()
                .filter(|w| w.op == write.op && w.key.contains(&write.key))
                .count() as u64;
            for crash in [Crash::AfterApply, Crash::BeforeApply] {
                let context = format!(
                    "{step:?} write #{index} ({:?} {}), crash {crash:?}",
                    write.op, write.key
                );
                let fault = crash.fault(write.op);
                let kind = fault.kind();
                let (base, clock) = prestate().await;
                let faulty = Arc::new(FaultStore::new(
                    base.clone(),
                    FaultPlan::empty().with_rule(
                        Rule::new(write.op, fault)
                            .with_key_contains(write.key.clone())
                            .with_occurrence(Occurrence::Nth(nth)),
                    ),
                ));
                let fired = faulty.clone();
                let crashing = TapStore::crashing(faulty.clone(), move || {
                    fired.counters_snapshot().values().sum::<u64>() > 0
                });

                for at in STEPS {
                    if at != step {
                        assert_eq!(
                            run_to_completion(at, &base, &clock, &context).await,
                            1,
                            "{context}: unfaulted {at:?} completes in one run"
                        );
                        continue;
                    }
                    // The crashed run: whatever it reports is lost with the
                    // process.
                    let _ = run_once(step, &crashing, &base, &clock, &mut None).await;
                    assert!(crashing.crashed(), "{context}: the process crashed");
                    run_to_completion(step, &base, &clock, &context).await;
                }
                finish(&base, &clock).await;

                assert_eq!(
                    faulty.fault_count(write.op, kind),
                    1,
                    "{context}: the scripted fault fired exactly once"
                );
                assert_eq!(
                    faulty.counters_snapshot().values().sum::<u64>(),
                    1,
                    "{context}: no other fault fired"
                );
                let got = common::snapshot(base.as_ref()).await;
                assert!(
                    got == want,
                    "{context}: the end state differs from the uninterrupted run: {:?}",
                    diff(&want, &got)
                );
                assert_reads_the_seed(&base, &context).await;
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 2 * (3 + 4 + 5), "every write crashed both ways");
}
