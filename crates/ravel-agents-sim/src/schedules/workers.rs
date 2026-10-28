//! Workers killed at each publication boundary, a suspended worker after a
//! takeover, and a cold restart.
//!
//! One worker turn issues, in order: LIST, a discovery HEAD read, the
//! takeover's HEAD read and CAS, BeginTurn's HEAD read and CAS, the payload
//! GET, then the commit's HEAD read, the state and output object PUTs and
//! the commit CAS. The kill points gate on those ordinals.

use ravel_agents_core::error::ErrorKind;
use ravel_agents_core::head::{ObjectRef, ResultStatus};
use ravel_agents_core::state::Handle;
use ravel_object_store::fault::{FaultKind, FaultPlan, Occurrence, Op, Rule, ScriptedFault};

use super::*;
use crate::drive::{agent, json_bytes, one_terminal_result_per_input, turn_values};
use crate::history::History;
use crate::reference::{Event, Reply};
use crate::scheduler::{Action, FailureContext};
use crate::{sim_assert, sim_assert_eq};

fn op(seq: u64) -> Handle {
    Handle::operation(agent(), 1, seq)
}

/// The output bytes the simulated handler commits for input `seq`.
fn output_of(seq: u64) -> Vec<u8> {
    let assignment_payload = payload(seq);
    json_bytes(&serde_json::json!({ "seq": seq, "echo": assignment_payload }))
}

fn duplicate(key: &str, n: u64) -> FaultPlan {
    FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains(key)
            .with_occurrence(Occurrence::Nth(n)),
    )
}

/// Where worker `w1` dies, and what the run must show afterwards.
struct KillPoint {
    /// The call `w1` is crashed holding.
    gate: GateScript,
    /// Faults on `w1`'s own calls.
    plan: FaultPlan,
    /// Faults `plan` must have injected.
    fired: Vec<((Op, FaultKind), u64)>,
    /// `w1`'s calls the reference had to answer itself.
    inferred: Vec<Event>,
    /// Takeovers and turns the replacement worker publishes.
    drain: (u64, u64),
}

/// One input; `w1` runs a turn and is crashed at the kill point; a
/// replacement worker drains. Exactly one terminal result per input.
fn killed(kp: KillPoint) {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let (w1, faults) = faulty(&rig, Role::Worker, "w1", 10, kp.plan, |p| async move {
        p.work(1).await;
        Ok(())
    });
    let w1 = w1.with_gates(vec![kp.gate.clone()]);
    let out = run(
        &rig,
        1,
        ScheduleScript::new(vec![ScriptStep::Crash(0)]),
        vec![w1],
    )
    .expect("schedule");
    let key = kp.gate.key_contains.clone().unwrap_or_default();
    let crashed: Vec<_> = out
        .trace
        .iter()
        .map(|t| (t.name.as_str(), t.action, t.op, t.key.contains(&key)))
        .collect();
    sim_assert_eq!(
        out,
        crashed,
        vec![("w1", Action::Crashed, kp.gate.op, true)]
    );
    let drained = drain(&rig, 2);
    let run = checked(&rig, &drained);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    let inferred: Vec<Event> = run.inferred.iter().map(|(_, e)| e.clone()).collect();
    sim_assert_eq!(out, inferred, kp.inferred);
    sim_assert!(
        out,
        run.inferred.iter().all(|(actor, _)| actor == "w1"),
        "{:?}",
        run.inferred
    );
    let head = rig.head().expect("head");
    one_terminal_result_per_input(&head)
        .unwrap_or_else(|e| panic!("{e}\n{}", out.failure_context()));
    sim_assert_eq!(
        out,
        head.results[0].output,
        Some(ObjectRef::of(&output_of(1)))
    );
    sim_assert_eq!(out, all_fired(&faults), kp.fired);
    let w1 = rig.proc_named("w1").expect("w1").co.metrics();
    sim_assert_eq!(out, w1.turns_committed, 0);
    let d = rig.proc_named(DRAIN).expect("drain").co.metrics();
    sim_assert_eq!(out, (d.takeovers, d.turns_committed), kp.drain);
}

fn completed(seq: u64) -> Event {
    Event::TurnCompleted {
        operation: op(seq),
        output: output_of(seq),
    }
}

#[test]
fn worker_killed_before_begin_turn_cas() {
    killed(KillPoint {
        gate: gate(Op::Put, "/head", 2),
        plan: FaultPlan::empty(),
        fired: vec![],
        inferred: vec![],
        drain: (1, 1),
    });
}

/// BeginTurn applied with its answer lost; `w1` dies on the reload read the
/// retry starts with. Its poll never answered, so the reference supplies the
/// `TurnStarted` the root records.
#[test]
fn worker_killed_after_begin_turn_cas() {
    killed(KillPoint {
        gate: gate(Op::Get, "/head", 4),
        plan: duplicate("/head", 2),
        fired: vec![((Op::Put, FaultKind::DuplicateDelivery), 1)],
        inferred: vec![Event::TurnStarted { operation: op(1) }],
        drain: (1, 1),
    });
}

#[test]
fn worker_killed_before_output_put() {
    killed(KillPoint {
        gate: gate(Op::Put, "/o/", 2),
        plan: FaultPlan::empty(),
        fired: vec![],
        inferred: vec![],
        drain: (1, 1),
    });
}

/// The output object landed with its answer lost; `w1` dies on the retried
/// PUT. The replacement's identical output finds the object present.
#[test]
fn worker_killed_after_output_put() {
    killed(KillPoint {
        gate: gate(Op::Put, "/o/", 3),
        plan: duplicate("/o/", 2),
        fired: vec![((Op::Put, FaultKind::DuplicateDelivery), 1)],
        inferred: vec![],
        drain: (1, 1),
    });
}

#[test]
fn worker_killed_before_commit_cas() {
    killed(KillPoint {
        gate: gate(Op::Put, "/head", 3),
        plan: FaultPlan::empty(),
        fired: vec![],
        inferred: vec![],
        drain: (1, 1),
    });
}

/// The commit applied with its answer lost; `w1` dies on the settle's
/// resolve read. The replacement finds nothing to do.
#[test]
fn worker_killed_after_commit_cas() {
    killed(KillPoint {
        gate: gate(Op::Get, "/head", 5),
        plan: duplicate("/head", 3),
        fired: vec![((Op::Put, FaultKind::DuplicateDelivery), 1)],
        inferred: vec![completed(1)],
        drain: (0, 0),
    });
}

/// Two clients send two inputs each while two workers run; workers crash
/// at random and `w1`'s `k`-th HEAD write loses its answer. A replacement
/// drains.
fn kills(seed: u64) -> KillCounts {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    let k = 1 + seed % 4;
    let (w1, faults) = faulty(
        &rig,
        Role::Worker,
        "w1",
        10,
        duplicate("/head", k),
        |p| async move {
            p.work(4).await;
            Ok(())
        },
    );
    let w2 = worker(&rig, "w2", 20, |p| async move {
        p.work(4).await;
        Ok(())
    });
    let senders = (0..2u64).map(|c| {
        client(&rig, &format!("c{c}"), 30 + c, move |p| async move {
            for i in 0..2 {
                let id = format!("c{c}-{i}");
                p.send(&id, &payload(10 * c + i))
                    .await
                    .map_err(|e| format!("send {id}: {e:?}"))?;
            }
            Ok(())
        })
    });
    let mut actors = vec![w1, w2];
    actors.extend(senders);
    let script = crash_workers_only(seeded(seed, 4, 120, 60), &[2, 3]);
    let out = run(&rig, seed, script, actors).expect("schedule");
    drain(&rig, seed);
    let run = rig.build().expect("build");
    sim_assert!(out, run.unmappable.is_empty(), "{:?}", run.unmappable);
    let known =
        past_known_divergences(&run).unwrap_or_else(|d| panic!("{d}\n{}", out.failure_context()));
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    let head = rig.head().expect("head");
    sim_assert_eq!(out, head.next_accept_seq, 5);
    one_terminal_result_per_input(&head)
        .unwrap_or_else(|e| panic!("{e}\n{}", out.failure_context()));
    // The rule fires on w1's k-th HEAD write, if w1 lived to issue one. It
    // calls the store for real and answers Transient either way, so a
    // firing is an applied write whose acknowledgement was lost or a
    // conflicted CAS whose conflict was hidden.
    let w1_cas = rig
        .proc_named("w1")
        .expect("w1")
        .store
        .counters()
        .publish
        .cas;
    let fired = usize::from(w1_cas >= k);
    let expected = if fired == 1 {
        vec![((Op::Put, FaultKind::DuplicateDelivery), 1)]
    } else {
        vec![]
    };
    sim_assert_eq!(out, all_fired(&faults), expected);
    // Only the one firing can produce the false negative.
    sim_assert!(
        out,
        known.false_negatives <= fired,
        "{known:?}, fired {fired}"
    );
    let crashes = out
        .trace
        .iter()
        .filter(|t| t.action == Action::Crashed)
        .count();
    KillCounts {
        crashes,
        fired,
        inferred: run.inferred.len(),
        stale_owner: known.stale_owner,
        false_negatives: known.false_negatives,
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct KillCounts {
    crashes: usize,
    /// Runs where the `DuplicateDelivery` rule fired: a lost
    /// acknowledgement of an applied write, or a conflicted CAS answered
    /// `Transient`.
    fired: usize,
    inferred: usize,
    stale_owner: usize,
    false_negatives: usize,
}

/// Divergences [`past_known_divergences`] answered past, by kind.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Known {
    /// A completion refused `OwnershipLost` on an operation another worker
    /// began again after a takeover. The reference has no worker identity:
    /// it expects every completion of the active input to commit, so it
    /// cannot express the fence. Dropped from the history.
    stale_owner: usize,
    /// The shape [`lost_commit_ack_then_takeover_is_a_known_false_negative`]
    /// pins: a completion that committed, answered `OwnershipLost`.
    /// Answered with the reference's `Committed`.
    false_negatives: usize,
}

/// Check the run's history, answering past the two known divergences. Any
/// other divergence is returned.
///
/// A completion refused `OwnershipLost` where the reference expects
/// `Committed` is a stale owner's when its operation began more than once
/// before it, its call applied no HEAD write, and a later completion of
/// the same operation committed. It is the false negative when the
/// operation began once before it and its call applied a write.
fn past_known_divergences(run: &Run) -> Result<Known, String> {
    let mut patched = run.history.clone();
    let mut applied = run.applied.clone();
    if applied.len() != patched.len() {
        return Err(format!(
            "{} applied flags for {} calls",
            applied.len(),
            patched.len()
        ));
    }
    let mut known = Known::default();
    loop {
        let machine = crate::drive::machine()?;
        let Err(d) = patched.check(machine) else {
            return Ok(known);
        };
        let Event::TurnCompleted { operation, .. } = &d.event else {
            return Err(d.to_string());
        };
        if !matches!(d.expected, Reply::Committed { .. })
            || d.observed != Reply::Error(ErrorKind::OwnershipLost)
        {
            return Err(d.to_string());
        }
        let calls = patched.calls();
        let begun = calls[..d.index]
            .iter()
            .filter(|c| matches!(&c.event, Event::TurnStarted { operation: o } if o == operation))
            .count();
        let wrote = applied[d.index];
        let committed_later = calls[d.index + 1..].iter().any(|c| {
            matches!(&c.event, Event::TurnCompleted { operation: o, .. } if o == operation)
                && matches!(c.observed, Reply::Committed { .. })
        });
        let stale = begun >= 2 && !wrote && committed_later;
        let false_negative = begun == 1 && wrote;
        if !stale && !false_negative {
            return Err(format!(
                "{d}\nnot a known shape: begun {begun}, wrote {wrote}, committed later \
                 {committed_later}"
            ));
        }
        let mut next = History::default();
        for (i, call) in calls.iter().enumerate() {
            if i != d.index {
                next.record(call.event.clone(), call.observed.clone());
            } else if false_negative {
                next.record(call.event.clone(), d.expected.clone());
            }
        }
        if false_negative {
            known.false_negatives += 1;
        } else {
            applied.remove(d.index);
            known.stale_owner += 1;
        }
        patched = next;
    }
}

/// One call of a hand-built [`Run`]: its event, the reply recorded in
/// place of the reference's (`None` records the reference's own), and
/// whether it applied a HEAD write.
struct Fed {
    event: Event,
    reply: Option<Reply>,
    wrote: bool,
}

fn answered(event: Event) -> Fed {
    Fed {
        event,
        reply: None,
        wrote: true,
    }
}

fn refused_lost(event: Event, wrote: bool) -> Fed {
    Fed {
        event,
        reply: Some(Reply::Error(ErrorKind::OwnershipLost)),
        wrote,
    }
}

/// A run built by feeding `calls` to a fresh reference machine. A call with
/// its own reply reaches the machine only when it wrote: a refusal that
/// wrote nothing changed nothing.
fn hand_run(calls: Vec<Fed>) -> Run {
    let mut machine = crate::drive::machine().expect("machine");
    let mut history = History::default();
    let mut applied = Vec::new();
    for call in calls {
        let reply = match call.reply {
            None => machine.apply(&call.event),
            Some(reply) => {
                if call.wrote {
                    machine.apply(&call.event);
                }
                reply
            }
        };
        history.record(call.event, reply);
        applied.push(call.wrote);
    }
    Run {
        history,
        inferred: Vec::new(),
        applied,
        refusals: Vec::new(),
        unmappable: Vec::new(),
    }
}

/// Create the agent and accept input 1.
fn accepted() -> Vec<Fed> {
    vec![
        answered(Event::Client(crate::reference::ClientOp::Create)),
        answered(Event::Client(crate::reference::ClientOp::Send {
            handler: crate::drive::HANDLER.to_string(),
            request_id: "r1".to_string(),
            payload: json_bytes(&payload(1)),
        })),
    ]
}

fn started(seq: u64) -> Fed {
    answered(Event::TurnStarted { operation: op(seq) })
}

fn rejected(calls: Vec<Fed>) -> String {
    past_known_divergences(&hand_run(calls)).expect_err("not a known shape")
}

#[test]
fn a_fenced_commit_of_a_turn_begun_twice_then_committed_is_a_stale_owner() {
    let mut calls = accepted();
    calls.extend([
        started(1),
        started(1),
        refused_lost(completed(1), false),
        answered(completed(1)),
    ]);
    assert_eq!(
        past_known_divergences(&hand_run(calls)),
        Ok(Known {
            stale_owner: 1,
            false_negatives: 0,
        })
    );
}

#[test]
fn a_fenced_commit_that_wrote_after_one_begin_is_the_false_negative() {
    let mut calls = accepted();
    calls.extend([started(1), refused_lost(completed(1), true)]);
    assert_eq!(
        past_known_divergences(&hand_run(calls)),
        Ok(Known {
            stale_owner: 0,
            false_negatives: 1,
        })
    );
}

#[test]
fn a_begin_after_the_fenced_commit_does_not_count_toward_a_stale_owner() {
    let mut calls = accepted();
    calls.extend([
        started(1),
        refused_lost(completed(1), false),
        started(1),
        answered(completed(1)),
    ]);
    let err = rejected(calls);
    assert!(
        err.contains("not a known shape: begun 1, wrote false, committed later true"),
        "{err}"
    );
}

#[test]
fn a_fenced_commit_that_wrote_after_two_begins_is_not_a_stale_owner() {
    let mut calls = accepted();
    calls.extend([
        started(1),
        started(1),
        refused_lost(completed(1), true),
        Fed {
            event: completed(1),
            reply: Some(Reply::Committed {
                status: ravel_agents_core::wire::OperationStatus::Completed,
                state_version: 1,
            }),
            wrote: true,
        },
    ]);
    let err = rejected(calls);
    assert!(
        err.contains("not a known shape: begun 2, wrote true, committed later true"),
        "{err}"
    );
}

#[test]
fn a_fenced_commit_with_no_later_commit_is_not_a_stale_owner() {
    let mut calls = accepted();
    calls.extend([started(1), started(1), refused_lost(completed(1), false)]);
    let err = rejected(calls);
    assert!(
        err.contains("not a known shape: begun 2, wrote false, committed later false"),
        "{err}"
    );
}

#[test]
fn a_fenced_commit_that_wrote_nothing_after_one_begin_is_not_the_false_negative() {
    let mut calls = accepted();
    calls.extend([started(1), refused_lost(completed(1), false)]);
    let err = rejected(calls);
    assert!(
        err.contains("not a known shape: begun 1, wrote false, committed later false"),
        "{err}"
    );
}

#[test]
fn worker_kills_across_seeds() {
    let mut runs = 0;
    let mut totals = KillCounts::default();
    for seed in seeds() {
        let k = kills(seed);
        totals.crashes += k.crashes;
        totals.fired += k.fired;
        totals.inferred += k.inferred;
        totals.stale_owner += k.stale_owner;
        totals.false_negatives += k.false_negatives;
        runs += 1;
    }
    assert_eq!(runs, 64);
    assert_eq!(totals, KILL_TOTALS);
}

/// A seed past the batch of [`worker_kills_across_seeds`], which no longer
/// reaches this shape: no worker crashed, `w1` lost the answer to a HEAD
/// write, and its later commit of a turn `w2` had begun again after a
/// takeover was fenced `OwnershipLost` where the reference, which has no
/// worker identity, expected `Committed`.
#[test]
fn stale_owner_commit_after_takeover_is_fenced() {
    assert_eq!(
        kills(0x5eed_00db),
        KillCounts {
            crashes: 0,
            fired: 1,
            inferred: 0,
            stale_owner: 1,
            false_negatives: 0,
        }
    );
}

/// Summed over the batch. Pinned: a scheduler or production change that
/// moves where workers die, or how often a fenced commit races a takeover,
/// moves it.
const KILL_TOTALS: KillCounts = KillCounts {
    crashes: 28,
    fired: 19,
    inferred: 2,
    stale_owner: 0,
    false_negatives: 0,
};

/// `w1` begins op 1 and is held at its commit-time HEAD read. `w2`, on
/// another coordinator, moves the clock past the lease and polls. Returns
/// whether `w2` took over first, in which case `w1`'s commit is refused;
/// otherwise `w1` commits and `w2` finds nothing to do.
fn suspended(seed: u64, script: ScheduleScript) -> bool {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let w1 = worker(&rig, "w1", 10, |p| async move {
        let a = p.poll().await.map_err(|e| format!("poll: {e:?}"))?;
        let a = a.ok_or("no assignment")?;
        let (state, output) = turn_values(&a);
        match p.commit(&a, &state, &output).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind == ErrorKind::OwnershipLost => Ok(()),
            Err(e) => Err(format!("commit: {e:?}")),
        }
    })
    .with_gates(vec![gate(Op::Get, "/head", 4)]);
    let clock = rig.clock.clone();
    let w2 = worker(&rig, "w2", 20, move |p| async move {
        clock.advance_ms(crate::drive::PAST_LEASE_MS);
        p.work(1).await;
        Ok(())
    })
    .with_gates(vec![gate(Op::List, "", 1)]);
    let out = run(&rig, seed, script, vec![w1, w2]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    let commits: Vec<Reply> = run
        .history
        .calls()
        .iter()
        .filter(|c| matches!(c.event, Event::TurnCompleted { .. }))
        .map(|c| c.observed.clone())
        .collect();
    let w1 = rig.proc_named("w1").expect("w1").co.metrics();
    let w2 = rig.proc_named("w2").expect("w2").co.metrics();
    let committed = Reply::Committed {
        status: ravel_agents_core::wire::OperationStatus::Completed,
        state_version: 1,
    };
    let taken_over = w2.takeovers == 1;
    if taken_over {
        sim_assert_eq!(
            out,
            commits,
            vec![committed, Reply::Error(ErrorKind::OwnershipLost)]
        );
        sim_assert_eq!(
            out,
            (w1.turns_committed, w1.ownership_lost_refusals),
            (0, 1)
        );
        sim_assert_eq!(out, w2.turns_committed, 1);
    } else {
        sim_assert_eq!(out, commits, vec![committed]);
        sim_assert_eq!(
            out,
            (w1.turns_committed, w1.ownership_lost_refusals),
            (1, 0)
        );
        sim_assert_eq!(out, (w2.takeovers, w2.turns_committed), (0, 0));
    }
    one_terminal_result_per_input(&rig.head().expect("head")).unwrap_or_else(|e| panic!("{e}"));
    taken_over
}

#[test]
fn suspended_worker_commit_is_refused_after_takeover() {
    let script = ScheduleScript::new(vec![ScriptStep::Release(1), ScriptStep::Release(0)]);
    assert!(suspended(1, script));
}

#[test]
fn suspended_worker_across_seeds() {
    let mut runs = 0;
    let mut taken_over = 0;
    for seed in seeds() {
        if suspended(seed, seeded(seed, 2, 2, 0)) {
            taken_over += 1;
        }
        runs += 1;
    }
    assert_eq!(runs, 64);
    assert_eq!(taken_over, SUSPENDED_TAKEN_OVER);
}

/// Runs of the batch where `w2` took over before `w1`'s commit.
const SUSPENDED_TAKEN_OVER: usize = 31;

/// The cold restart with the restarted client and worker racing: `c`
/// sends `r3` and reads op 1's result while `w-restarted` takes the agent
/// over, then a drain runs what is left. Returns `w-restarted`'s turns.
fn cold_restart_race(seed: u64) -> u64 {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 2);
    let w = worker(&rig, "w", 10, |p| async move {
        p.work(1).await;
        Ok(())
    });
    run(&rig, seed, ScheduleScript::new(Vec::new()), vec![w]).expect("first life");
    rig.clock.advance_ms(crate::drive::PAST_LEASE_MS);
    let c = client(&rig, "c", 40, |p| async move {
        p.send("r3", &payload(3))
            .await
            .map_err(|e| format!("send: {e:?}"))?;
        p.result(&op(1))
            .await
            .map_err(|e| format!("result: {e:?}"))?;
        Ok(())
    });
    let w = worker(&rig, "w-restarted", 50, |p| async move {
        p.work(4).await;
        Ok(())
    });
    let out = run(&rig, seed, seeded(seed, 2, 60, 0), vec![c, w]).expect("restart");
    drain(&rig, seed);
    let run = checked(&rig, &out);
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    let head = rig.head().expect("head");
    one_terminal_result_per_input(&head).unwrap_or_else(|e| panic!("{e}"));
    sim_assert_eq!(out, head.next_accept_seq, 4);
    sim_assert_eq!(out, head.results.len(), 3);
    let restarted = rig.proc_named("w-restarted").expect("w").co.metrics();
    let drained = rig.proc_named(DRAIN).expect("drain").co.metrics();
    sim_assert_eq!(out, restarted.takeovers, 1);
    sim_assert_eq!(out, restarted.turns_committed + drained.turns_committed, 2);
    restarted.turns_committed
}

#[test]
fn cold_restart_across_seeds() {
    let mut runs = 0;
    let mut turns = 0;
    for seed in seeds() {
        turns += cold_restart_race(seed);
        runs += 1;
    }
    assert_eq!(runs, 64);
    assert_eq!(turns, COLD_RESTART_TURNS);
}

/// `w-restarted`'s turns over the batch; the drain ran the rest of 128.
const COLD_RESTART_TURNS: u64 = 112;

/// A worker completes op 1. Then every process restarts cold: a client on a
/// new coordinator sends `r3` and reads `r1`, and a worker on another new
/// coordinator takes the agent over and runs the rest.
#[test]
fn cold_restart_with_a_second_coordinator() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 2);
    let w = worker(&rig, "w", 10, |p| async move {
        p.work(1).await;
        Ok(())
    });
    let first = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![w]).expect("first life");
    rig.clock.advance_ms(crate::drive::PAST_LEASE_MS);
    let c = client(&rig, "c", 40, |p| async move {
        p.send("r3", &payload(3))
            .await
            .map_err(|e| format!("send: {e:?}"))?;
        p.resolve("r1")
            .await
            .map_err(|e| format!("resolve: {e:?}"))?;
        p.result(&op(1))
            .await
            .map_err(|e| format!("result: {e:?}"))?;
        Ok(())
    });
    run(&rig, 2, ScheduleScript::new(Vec::new()), vec![c]).expect("client restart");
    let w = worker(&rig, "w-restarted", 50, |p| async move {
        p.work(4).await;
        Ok(())
    });
    let out = run(&rig, 3, ScheduleScript::new(Vec::new()), vec![w]).expect("worker restart");
    let run = checked(&rig, &out);
    sim_assert!(first, run.inferred.is_empty(), "{:?}", run.inferred);
    let results: Vec<Reply> = run
        .history
        .calls()
        .iter()
        .filter(|c| {
            matches!(
                c.event,
                Event::Client(crate::reference::ClientOp::Result { .. })
            )
        })
        .map(|c| c.observed.clone())
        .collect();
    sim_assert_eq!(
        out,
        results,
        vec![Reply::Result(Some(crate::reference::Outcome {
            status: ravel_agents_core::wire::OperationStatus::Completed,
            output: Some(output_of(1)),
            error: None,
        }))]
    );
    let head = rig.head().expect("head");
    one_terminal_result_per_input(&head).unwrap_or_else(|e| panic!("{e}"));
    sim_assert_eq!(out, head.next_accept_seq, 4);
    let first_metrics = rig.proc_named("w").expect("w").co.metrics();
    let second = rig.proc_named("w-restarted").expect("w").co.metrics();
    sim_assert_eq!(
        out,
        (first_metrics.takeovers, first_metrics.turns_committed),
        (1, 1)
    );
    sim_assert_eq!(out, (second.takeovers, second.turns_committed), (1, 2));
}

/// T9: `w1`'s commit applies with its answer lost; a second input arrives;
/// `w1` is held at its settle read while `w2`, on another coordinator,
/// takes over and runs the second input. The settle then answers
/// `OwnershipLost` for a commit that landed, the false negative
/// `ravel-agents-runtime`'s `failure.rs` documents. The reference expects
/// `Committed` there, and the check reports exactly that divergence.
#[test]
fn lost_commit_ack_then_takeover_is_a_known_false_negative() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let (w1, faults) = faulty(
        &rig,
        Role::Worker,
        "w1",
        10,
        duplicate("/head", 3),
        |p| async move {
            let a = p.poll().await.map_err(|e| format!("poll: {e:?}"))?;
            let a = a.ok_or("no assignment")?;
            let (state, output) = turn_values(&a);
            match p.commit(&a, &state, &output).await {
                Err(e) if e.kind == ErrorKind::OwnershipLost => Ok(()),
                other => Err(format!("commit: {other:?}")),
            }
        },
    );
    let w1 = w1.with_gates(vec![gate(Op::Get, "/head", 5)]);
    let c = client(&rig, "c", 30, |p| async move {
        p.send("r2", &payload(2))
            .await
            .map_err(|e| format!("send: {e:?}"))?;
        Ok(())
    })
    .with_gates(vec![gate(Op::Get, "/head", 1)]);
    let w2 = worker(&rig, "w2", 20, |p| async move {
        p.work(1).await;
        Ok(())
    })
    .with_gates(vec![gate(Op::List, "", 1)]);
    let out = run(
        &rig,
        1,
        ScheduleScript::new(vec![
            ScriptStep::Release(1),
            ScriptStep::Release(2),
            ScriptStep::Release(0),
        ]),
        vec![w1, c, w2],
    )
    .expect("schedule");
    let run = rig.build().expect("build");
    sim_assert!(out, run.unmappable.is_empty(), "{:?}", run.unmappable);
    let divergence = run
        .history
        .check(crate::drive::machine().expect("machine"))
        .expect_err("the false negative diverges");
    sim_assert_eq!(out, divergence.event, completed(1));
    sim_assert_eq!(
        out,
        divergence.expected,
        Reply::Committed {
            status: ravel_agents_core::wire::OperationStatus::Completed,
            state_version: 1
        }
    );
    sim_assert_eq!(
        out,
        divergence.observed,
        Reply::Error(ErrorKind::OwnershipLost)
    );
    let head = rig.head().expect("head");
    one_terminal_result_per_input(&head).unwrap_or_else(|e| panic!("{e}"));
    sim_assert_eq!(out, head.results[0].status, ResultStatus::Completed);
    sim_assert_eq!(
        out,
        head.results[0].output,
        Some(ObjectRef::of(&output_of(1)))
    );
    sim_assert_eq!(out, head.state_version, 2);
    sim_assert_eq!(
        out,
        all_fired(&faults),
        vec![((Op::Put, FaultKind::DuplicateDelivery), 1)]
    );
    let w1 = rig.proc_named("w1").expect("w1").co.metrics();
    let w2 = rig.proc_named("w2").expect("w2").co.metrics();
    sim_assert_eq!(
        out,
        (w1.turns_committed, w1.ownership_lost_refusals),
        (0, 1)
    );
    sim_assert_eq!(out, (w2.takeovers, w2.turns_committed), (1, 1));
}
