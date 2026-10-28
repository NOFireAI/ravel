//! Timers and their wake hints: a cancel racing the fire, hints that come
//! twice or early, a hint PUT that is lost, and a schedule that crashes
//! between its root write and its hint write. Every sweep pass is recorded
//! as a wake, so the reference checks each fire it published.

use std::sync::Mutex;

use bytes::Bytes;
use ravel_agents_core::error::ErrorKind;
use ravel_agents_core::head::{Head, TimerStatus};
use ravel_agents_core::keys::{DueBucket, WAKE_BUCKET_WIDTH_NS, wake_key, wake_prefix};
use ravel_agents_core::state::Handle;
use ravel_agents_core::wire::{OperationStatus, TimerCancelOutcome};
use ravel_agents_runtime::SweepReport;
use ravel_object_store::fault::{FaultKind, FaultPlan, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::{ObjectStoreBackend, PutOptions};

use super::*;
use crate::drive::{HANDLER, Refusal, T0_NS, TENANT, agent, json_bytes, status_of};
use crate::reference::{ClientOp, Event, Reply};
use crate::{sim_assert, sim_assert_eq};

/// The timer every schedule here sets.
const KEY: &str = "nudge";
/// One millisecond after the rig's clock starts, in the clock's own
/// minute bucket: `T0_NS` is a whole number of buckets.
const DUE: i64 = T0_NS + 1_000_000;

/// Reports of every sweep pass an actor ran, in order.
type Reports = Arc<Mutex<Vec<SweepReport>>>;

fn reports(cell: &Reports) -> Vec<SweepReport> {
    cell.lock().expect("reports").clone()
}

/// Create the agent and set [`KEY`] due at `due`, outside any schedule.
fn armed(rig: &Rig, due: i64) {
    rig.setup(|p| async move {
        p.create().await.map_err(|e| format!("create: {e:?}"))?;
        p.schedule_timer(KEY, due)
            .await
            .map_err(|e| format!("schedule: {e:?}"))?;
        Ok(())
    })
    .expect("setup timer");
}

/// One sweep pass outside any schedule, at the rig's clock.
fn swept(rig: &Rig) -> SweepReport {
    rig.setup(|p| async move { p.sweep().await.map_err(|e| format!("sweep: {e:?}")) })
        .expect("sweep")
}

/// A client running one sweep pass, its report kept in `cell`.
fn sweeper(rig: &Rig, name: &str, id_seed: u64, cell: &Reports) -> Actor {
    let cell = Arc::clone(cell);
    client(rig, name, id_seed, move |p| async move {
        let report = p.sweep().await.map_err(|e| format!("sweep: {e:?}"))?;
        cell.lock().map_err(|_| "reports poisoned")?.push(report);
        Ok(())
    })
}

/// A client cancelling [`KEY`], its answer kept in `cell`.
fn canceller(rig: &Rig, cell: &Arc<Mutex<Option<TimerCancelOutcome>>>) -> Actor {
    let cell = Arc::clone(cell);
    client(rig, "c", 20, move |p| async move {
        let r = p
            .cancel_timer(KEY)
            .await
            .map_err(|e| format!("cancel: {e:?}"))?;
        *cell.lock().map_err(|_| "outcome poisoned")? = Some(r.outcome);
        Ok(())
    })
}

/// Every `_wake/` key the memory store holds.
fn hint_keys(rig: &Rig) -> Vec<String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let prefix = wake_prefix(&TENANT).expect("wake prefix");
    let mut keys = Vec::new();
    let mut token = None;
    loop {
        let page = runtime
            .block_on(rig.memory.list(&prefix, token))
            .expect("list hints");
        keys.extend(page.objects.into_iter().map(|o| o.key));
        match page.next {
            Some(next) => token = Some(next),
            None => return keys,
        }
    }
}

/// The hint key of a timer due at `due`.
fn hint_for(due: i64) -> String {
    let bucket = DueBucket::for_due(due).expect("bucket");
    wake_key(&TENANT, bucket, &agent()).expect("wake key")
}

fn timer_status(head: &Head) -> Option<TimerStatus> {
    head.timers
        .iter()
        .find(|t| t.timer_key == KEY)
        .map(|t| t.status)
}

/// The `TimerFired` replies the history holds.
fn fires(run: &Run) -> Vec<Reply> {
    run.history
        .calls()
        .iter()
        .filter(|c| matches!(c.event, Event::TimerDue { .. }))
        .map(|c| c.observed.clone())
        .collect()
}

fn fired_op() -> Reply {
    Reply::TimerFired(Some(Handle::operation(agent(), 1, 1)))
}

/// Which way a fire/cancel race went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct RaceTally {
    cancelled: u64,
    already_fired: u64,
}

/// Sweeper `s` (actor 0) and canceller `c` (actor 1) over a timer due now.
/// Asserts `AlreadyFired` comes back exactly when the generation was
/// delivered, and that the fire and the cancel together leave no hint.
fn fire_cancel_race(
    seed: u64,
    script: ScheduleScript,
    gates: Option<(GateScript, GateScript)>,
) -> TimerCancelOutcome {
    let rig = Rig::new(FaultPlan::empty());
    armed(&rig, DUE);
    rig.clock.advance_ms(1);
    let swept: Reports = Arc::default();
    let outcome = Arc::new(Mutex::new(None));
    let s = sweeper(&rig, "s", 10, &swept);
    let c = canceller(&rig, &outcome);
    let actors = match gates {
        Some((sg, cg)) => vec![s.with_gates(vec![sg]), c.with_gates(vec![cg])],
        None => vec![s, c],
    };
    let out = run(&rig, seed, script, actors).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    let outcome = outcome.lock().expect("outcome").expect("cancel answered");
    let head = rig.head().expect("head");
    let delivered = timer_status(&head) == Some(TimerStatus::Delivered);
    sim_assert!(
        out,
        (outcome == TimerCancelOutcome::AlreadyFired) == delivered,
        "cancel answered {outcome:?} over {:?}",
        timer_status(&head)
    );
    if delivered {
        sim_assert_eq!(out, fires(&run), vec![fired_op()]);
        sim_assert_eq!(out, head.next_accept_seq, 2);
    } else {
        sim_assert_eq!(out, outcome, TimerCancelOutcome::Cancelled);
        sim_assert_eq!(out, timer_status(&head), Some(TimerStatus::Cancelled));
        sim_assert_eq!(out, fires(&run), Vec::<Reply>::new());
        sim_assert_eq!(out, head.next_accept_seq, 1);
    }
    let report = reports(&swept);
    sim_assert_eq!(out, report.len(), 1);
    sim_assert_eq!(out, report[0].timers_fired, u64::from(delivered));
    sim_assert_eq!(out, hint_keys(&rig), Vec::<String>::new());
    outcome
}

#[test]
fn a_cancel_before_the_fire_wins_and_the_sweep_fires_nothing() {
    let outcome = fire_cancel_race(
        1,
        ScheduleScript::new(vec![ScriptStep::Release(1); 8]),
        None,
    );
    assert_eq!(outcome, TimerCancelOutcome::Cancelled);
}

/// Both CASes read the same root; the cancel's lands first, and the fire's
/// conflicts, rereads HEAD and is refused as stale.
#[test]
fn a_cancel_at_the_fire_that_lands_first_wins() {
    let outcome = fire_cancel_race(
        1,
        ScheduleScript::new(vec![ScriptStep::Release(1), ScriptStep::Release(0)]),
        Some((gate(Op::Put, "/head", 1), gate(Op::Put, "/head", 1))),
    );
    assert_eq!(outcome, TimerCancelOutcome::Cancelled);
}

/// Both CASes read the same root; the fire's lands first, and the cancel's
/// conflicts, rereads HEAD and answers `AlreadyFired`.
#[test]
fn a_cancel_at_the_fire_that_lands_second_is_already_fired() {
    let outcome = fire_cancel_race(
        1,
        ScheduleScript::new(vec![ScriptStep::Release(0), ScriptStep::Release(1)]),
        Some((gate(Op::Put, "/head", 1), gate(Op::Put, "/head", 1))),
    );
    assert_eq!(outcome, TimerCancelOutcome::AlreadyFired);
}

#[test]
fn a_cancel_after_the_fire_is_already_fired() {
    let outcome = fire_cancel_race(1, ScheduleScript::new(Vec::new()), None);
    assert_eq!(outcome, TimerCancelOutcome::AlreadyFired);
}

#[test]
fn fire_cancel_race_across_seeds() {
    let mut runs = 0;
    let mut tally = RaceTally::default();
    for seed in seeds() {
        match fire_cancel_race(seed, seeded(seed, 2, 40, 0), None) {
            TimerCancelOutcome::Cancelled => tally.cancelled += 1,
            TimerCancelOutcome::AlreadyFired => tally.already_fired += 1,
            TimerCancelOutcome::Unknown => panic!("seed {seed}: cancel found no timer"),
        }
        runs += 1;
    }
    assert_eq!(runs, 64);
    assert_eq!(tally, FIRE_CANCEL_TALLY);
}

/// How the batch's races resolved.
const FIRE_CANCEL_TALLY: RaceTally = RaceTally {
    cancelled: 43,
    already_fired: 21,
};

/// A hint listed before its timer is due fires nothing and is kept; the
/// pass at the due time fires it and deletes the hint.
#[test]
fn a_premature_hint_fires_nothing_and_is_kept() {
    let rig = Rig::new(FaultPlan::empty());
    armed(&rig, DUE);
    let early = swept(&rig);
    assert_eq!(
        early,
        SweepReport {
            list_pages: 2,
            head_gets: 1,
            hints_read: 1,
            ..SweepReport::default()
        }
    );
    assert_eq!(hint_keys(&rig), vec![hint_for(DUE)]);
    rig.clock.advance_ms(1);
    let due = swept(&rig);
    assert_eq!(
        due,
        SweepReport {
            list_pages: 2,
            head_gets: 1,
            hints_read: 1,
            hints_deleted: 1,
            timers_fired: 1,
            agents_loaded: 1,
            ..SweepReport::default()
        }
    );
    assert_eq!(hint_keys(&rig), Vec::<String>::new());
    let run = rig.build().expect("build");
    assert!(run.unmappable.is_empty(), "{:?}", run.unmappable);
    run.check().expect("history");
    assert_eq!(fires(&run), vec![fired_op()]);
}

/// A hint in a bucket the clock has not reached is not listed; the
/// directory half reads the agent and fires nothing.
#[test]
fn a_hint_in_a_later_bucket_is_not_listed() {
    let rig = Rig::new(FaultPlan::empty());
    let due = T0_NS + WAKE_BUCKET_WIDTH_NS;
    armed(&rig, due);
    let report = swept(&rig);
    assert_eq!(
        report,
        SweepReport {
            list_pages: 2,
            head_gets: 1,
            ..SweepReport::default()
        }
    );
    assert_eq!(hint_keys(&rig), vec![hint_for(due)]);
    let run = rig.build().expect("build");
    assert!(run.unmappable.is_empty(), "{:?}", run.unmappable);
    run.check().expect("history");
    assert_eq!(fires(&run), Vec::<Reply>::new());
}

/// A stale copy of the hint in an earlier bucket and the real one: one pass
/// reads HEAD once, fires the generation once, and deletes both.
#[test]
fn a_duplicate_hint_fires_its_generation_once() {
    let rig = Rig::new(FaultPlan::empty());
    armed(&rig, DUE);
    let copy = hint_for(DUE - WAKE_BUCKET_WIDTH_NS);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    runtime
        .block_on(rig.memory.put(&copy, Bytes::new(), PutOptions::default()))
        .expect("copy hint");
    rig.clock.advance_ms(1);
    let report = swept(&rig);
    assert_eq!(
        report,
        SweepReport {
            list_pages: 2,
            head_gets: 1,
            hints_read: 2,
            hints_deleted: 2,
            timers_fired: 1,
            agents_loaded: 1,
            ..SweepReport::default()
        }
    );
    assert_eq!(hint_keys(&rig), Vec::<String>::new());
    let run = rig.build().expect("build");
    assert!(run.unmappable.is_empty(), "{:?}", run.unmappable);
    run.check().expect("history");
    assert_eq!(fires(&run), vec![fired_op()]);
}

/// Two sweepers deliver the same hint at once. Returns the passes' summed
/// `(head_gets, hints_deleted)`.
fn duplicate_delivery(seed: u64, script: ScheduleScript, gated: bool) -> (u64, u64) {
    let rig = Rig::new(FaultPlan::empty());
    armed(&rig, DUE);
    rig.clock.advance_ms(1);
    let swept: Reports = Arc::default();
    let s1 = sweeper(&rig, "s1", 10, &swept);
    let s2 = sweeper(&rig, "s2", 20, &swept);
    let actors = if gated {
        // Both read the root holding the generation, then both fire it.
        vec![
            s1.with_gates(vec![gate(Op::Put, "/head", 1)]),
            s2.with_gates(vec![gate(Op::Put, "/head", 1)]),
        ]
    } else {
        vec![s1, s2]
    };
    let out = run(&rig, seed, script, actors).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    sim_assert_eq!(out, fires(&run), vec![fired_op()]);
    let head = rig.head().expect("head");
    sim_assert_eq!(out, head.next_accept_seq, 2);
    sim_assert_eq!(out, timer_status(&head), Some(TimerStatus::Delivered));
    sim_assert_eq!(out, hint_keys(&rig), Vec::<String>::new());
    let all = reports(&swept);
    sim_assert_eq!(out, all.len(), 2);
    sim_assert_eq!(out, all.iter().map(|r| r.timers_fired).sum::<u64>(), 1);
    (
        all.iter().map(|r| r.head_gets).sum(),
        all.iter().map(|r| r.hints_deleted).sum(),
    )
}

#[test]
fn two_sweepers_on_one_hint_fire_it_once() {
    let (gets, deleted) = duplicate_delivery(
        1,
        ScheduleScript::new(vec![ScriptStep::Release(0), ScriptStep::Release(1)]),
        true,
    );
    // Each pass reads the agent once; both delete the hint they listed, and
    // a DELETE of a gone key succeeds.
    assert_eq!((gets, deleted), (2, 2));
}

#[test]
fn duplicate_hint_delivery_across_seeds() {
    let mut runs = 0;
    let mut gets = 0;
    let mut deleted = 0;
    for seed in seeds() {
        let (g, d) = duplicate_delivery(seed, seeded(seed, 2, 40, 0), false);
        gets += g;
        deleted += d;
        runs += 1;
    }
    assert_eq!(runs, 64);
    assert_eq!((gets, deleted), DUPLICATE_DELIVERY_TOTALS);
}

/// Summed `(head_gets, hints_deleted)` over the batch. Every pass reads
/// the agent once, through its hint or the directory; a pass that lists the
/// wake prefix after the other pass deleted the hint has none to delete.
const DUPLICATE_DELIVERY_TOTALS: (u64, u64) = (128, 125);

/// Period of the sweeper in the lost-hint schedules.
const INTERVAL_MS: u64 = 1_000;

/// The schedule's hint PUT times out, so no hint exists. A sweeper passing
/// every `INTERVAL_MS` from the clock's start fires the timer on its first
/// pass at or after the due time, through the directory half, and one
/// worker then runs the one input it enqueued. Returns the pass index that
/// fired.
fn lost_hint(due_ms: u64) -> usize {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Timeout)
            .with_key_contains("/_wake/")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let rig = Rig::new(plan);
    let due = T0_NS + i64::try_from(due_ms * 1_000_000).expect("due");
    armed(&rig, due);
    assert_eq!(rig.rules.fault_count(Op::Put, FaultKind::Timeout), 1);
    assert_eq!(hint_keys(&rig), Vec::<String>::new());
    let clock = Arc::clone(&rig.clock);
    let passes = rig
        .setup(|p| async move {
            let mut passes = Vec::new();
            loop {
                let report = p.sweep().await.map_err(|e| format!("sweep: {e:?}"))?;
                passes.push(report);
                if report.timers_fired > 0 || passes.len() > 16 {
                    return Ok(passes);
                }
                clock.advance_ms(INTERVAL_MS);
            }
        })
        .expect("sweeper");
    let fired_at = passes.len() - 1;
    let pass_ns = i64::try_from(fired_at as u64 * INTERVAL_MS * 1_000_000).expect("pass");
    // The first pass at or after the due time fires, so the delay is under
    // one interval.
    assert!(T0_NS + pass_ns >= due);
    assert!(T0_NS + pass_ns - due < i64::try_from(INTERVAL_MS * 1_000_000).expect("interval"));
    assert_eq!(
        passes[fired_at],
        SweepReport {
            list_pages: 2,
            head_gets: 1,
            timers_fired: 1,
            agents_loaded: 1,
            ..SweepReport::default()
        }
    );
    assert!(passes[..fired_at].iter().all(|r| r.timers_fired == 0));
    let out = drain(&rig, 1);
    let run = checked(&rig, &out);
    assert_eq!(fires(&run), vec![fired_op()]);
    let head = rig.head().expect("head");
    assert_eq!(head.next_accept_seq, 2);
    assert_eq!(status_of(&head, 1), Some(OperationStatus::Completed));
    let drain = rig.proc_named(DRAIN).expect("drain").co.metrics();
    assert_eq!(drain.turns_committed, 1);
    // The fault fired once and only on the hint.
    assert_eq!(rig.rules.fault_count(Op::Put, FaultKind::Timeout), 1);
    fired_at
}

#[test]
fn a_lost_hint_put_is_delivered_by_the_sweep_within_its_interval() {
    assert_eq!(lost_hint(2_500), 3);
}

#[test]
fn a_lost_hint_put_due_on_a_pass_fires_on_that_pass() {
    assert_eq!(lost_hint(2_000), 2);
}

/// A pass whose fire write fails fires nothing and is a `StoreUnavailable`
/// refusal rather than a wake; the timer and its hint stay for a later pass.
#[test]
fn a_sweep_whose_fire_write_fails_is_a_store_unavailable_refusal() {
    let rig = Rig::new(FaultPlan::empty());
    armed(&rig, DUE);
    rig.clock.advance_ms(1);
    let swept: Reports = Arc::default();
    let cell = Arc::clone(&swept);
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Throttled { retry_after_ms: 0 })
            .with_key_contains("/head"),
    );
    let (s, faults) = faulty(&rig, Role::Client, "s", 10, plan, move |p| async move {
        let report = p.sweep().await.map_err(|e| format!("sweep: {e:?}"))?;
        cell.lock().map_err(|_| "reports poisoned")?.push(report);
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![s]).expect("schedule");
    let run = checked(&rig, &out);
    assert_eq!(
        run.refusals,
        vec![Refusal {
            actor: "s".to_string(),
            call: "sweep",
            kind: ErrorKind::StoreUnavailable,
        }]
    );
    assert_eq!(fires(&run), Vec::<Reply>::new());
    // The pass tries the fire once and does not retry it.
    assert_eq!(all_fired(&faults), vec![((Op::Put, FaultKind::Throttled), 1)]);
    let report = reports(&swept);
    assert_eq!(report.len(), 1);
    assert_eq!(report[0].timers_fired, 0);
    let head = rig.head().expect("head");
    assert_eq!(timer_status(&head), Some(TimerStatus::Scheduled));
    assert_eq!(hint_keys(&rig), vec![hint_for(DUE)]);
}

/// Client `c` schedules the timer and is crashed per `script` under the
/// default gates. Then a sweep at the due time runs. Returns where the
/// crash fell.
fn crash_around_the_hint(seed: u64, script: ScheduleScript, gates: Vec<GateScript>) -> Crash {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    let c = client(&rig, "c", 20, |p| async move {
        p.schedule_timer(KEY, DUE)
            .await
            .map_err(|e| format!("schedule: {e:?}"))?;
        Ok(())
    });
    let c = if gates.is_empty() {
        c
    } else {
        c.with_gates(gates)
    };
    let out = run(&rig, seed, script, vec![c]).expect("schedule");
    let head = rig.head().expect("head");
    let hints = hint_keys(&rig);
    let crash = match (timer_status(&head), hints.is_empty()) {
        (None, true) => Crash::BeforeRoot,
        (Some(TimerStatus::Scheduled), true) => Crash::BetweenRootAndHint,
        (Some(TimerStatus::Scheduled), false) => Crash::AfterHint,
        other => panic!("seed {seed}: timer {other:?}"),
    };
    let crashed = out.ends == vec![crate::scheduler::ActorEnd::Crashed];
    sim_assert!(out, crashed || crash == Crash::AfterHint, "{crash:?}");
    rig.clock.advance_ms(1);
    let report = swept(&rig);
    let run = checked(&rig, &out);
    let set = Event::Client(ClientOp::SetTimer {
        timer_key: KEY.to_string(),
        due_unix_ns: DUE,
        handler: HANDLER.to_string(),
        payload: json_bytes(&serde_json::Value::Null),
    });
    if crashed && crash != Crash::BeforeRoot {
        sim_assert_eq!(out, run.inferred, vec![("c".to_string(), set)]);
    } else {
        sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    }
    let fired = u64::from(crash != Crash::BeforeRoot);
    sim_assert_eq!(out, report.timers_fired, fired);
    sim_assert_eq!(out, report.hints_read, u64::from(crash == Crash::AfterHint));
    sim_assert_eq!(out, fires(&run).len() as u64, fired);
    sim_assert_eq!(out, hint_keys(&rig), Vec::<String>::new());
    crash
}

/// Where a crash of the scheduling client fell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Crash {
    /// No root write applied: no timer, no hint.
    BeforeRoot,
    /// The root holds the timer and no hint was written.
    BetweenRootAndHint,
    /// Both writes applied, or the call finished.
    AfterHint,
}

/// The client holds its hint PUT, having published the root, and dies
/// there. The directory half finds the timer at its due time.
#[test]
fn a_crash_between_the_root_write_and_the_hint_write_still_fires() {
    let crash = crash_around_the_hint(
        1,
        ScheduleScript::new(vec![ScriptStep::Crash(0)]),
        vec![gate(Op::Put, "/_wake/", 1)],
    );
    assert_eq!(crash, Crash::BetweenRootAndHint);
}

#[test]
fn crashes_around_the_hint_write_across_seeds() {
    let mut runs = 0;
    let mut tally = [0u64; 3];
    for seed in seeds() {
        let crash = crash_around_the_hint(seed, seeded(seed, 1, 12, 300), Vec::new());
        tally[crash as usize] += 1;
        runs += 1;
    }
    assert_eq!(runs, 64);
    assert_eq!(tally, CRASH_TALLY);
}

/// Runs per [`Crash`] over the batch, in declaration order.
const CRASH_TALLY: [u64; 3] = [42, 7, 15];
