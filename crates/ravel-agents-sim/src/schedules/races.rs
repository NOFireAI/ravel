//! A client write racing a worker's completion: an enqueue the commit must
//! rebase over, and a cancel that either lands first or loses.

use ravel_agents_core::state::Handle;
use ravel_agents_core::wire::OperationStatus;
use ravel_object_store::fault::{FaultPlan, Op};

use super::*;
use crate::drive::{agent, status_of};
use crate::reference::{ClientOp, Event, Reply};
use crate::scheduler::FailureContext;
use crate::{sim_assert, sim_assert_eq};

fn op(seq: u64) -> Handle {
    Handle::operation(agent(), 1, seq)
}

fn cancels(run: &Run) -> Vec<Reply> {
    run.history
        .calls()
        .iter()
        .filter(|c| matches!(c.event, Event::Client(ClientOp::Cancel { .. })))
        .map(|c| c.observed.clone())
        .collect()
}

fn commits(run: &Run) -> Vec<Reply> {
    run.history
        .calls()
        .iter()
        .filter(|c| matches!(c.event, Event::TurnCompleted { .. }))
        .map(|c| c.observed.clone())
        .collect()
}

/// `w` completes op 1 while client `c` sends `r2` and `r3`. Returns `w`'s
/// commit-time HEAD conflicts.
fn enqueue_race(seed: u64, script: ScheduleScript, gated: bool) -> u64 {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let w = worker(&rig, "w", 10, |p| async move {
        p.work(1).await;
        Ok(())
    });
    let c = client(&rig, "c", 20, |p| async move {
        for (id, n) in [("r2", 2), ("r3", 3)] {
            p.send(id, &payload(n))
                .await
                .map_err(|e| format!("send {id}: {e:?}"))?;
        }
        Ok(())
    });
    let actors = if gated {
        // w stops at its commit CAS, having read HEAD for it; c's sends
        // then land under it.
        vec![
            w.with_gates(vec![gate(Op::Put, "/head", 3)]),
            c.with_gates(vec![gate(Op::Get, "/head", 1)]),
        ]
    } else {
        vec![w, c]
    };
    let out = run(&rig, seed, script, actors).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    sim_assert_eq!(
        out,
        commits(&run),
        vec![Reply::Committed {
            status: OperationStatus::Completed,
            state_version: 1
        }]
    );
    // The commit kept both inputs it raced, still accepted.
    let head = rig.head().expect("head");
    sim_assert_eq!(out, head.next_accept_seq, 4);
    sim_assert_eq!(
        out,
        head.inbox.iter().map(|e| e.accept_seq).collect::<Vec<_>>(),
        vec![2, 3]
    );
    sim_assert_eq!(out, status_of(&head, 1), Some(OperationStatus::Completed));
    let w = rig.proc_named("w").expect("w");
    sim_assert_eq!(out, w.co.metrics().turns_committed, 1);
    let publish = w.store.counters().publish;
    // Takeover, BeginTurn and the commit applied; every other attempt
    // conflicted with a send.
    sim_assert_eq!(out, publish.cas, 3 + publish.conflict);
    publish.conflict
}

#[test]
fn enqueue_racing_completion_rebases_and_keeps_the_input() {
    let conflicts = enqueue_race(
        1,
        ScheduleScript::new(vec![ScriptStep::Release(1), ScriptStep::Release(0)]),
        true,
    );
    assert_eq!(conflicts, 1);
}

#[test]
fn enqueue_racing_completion_across_seeds() {
    let mut runs = 0;
    let mut conflicts = 0;
    for seed in seeds() {
        conflicts += enqueue_race(seed, seeded(seed, 2, 60, 0), false);
        runs += 1;
    }
    pinned(
        "enqueue_racing_completion",
        runs,
        conflicts,
        ENQUEUE_RACE_CONFLICTS,
    );
}

/// `w`'s HEAD conflicts summed over the batch.
const ENQUEUE_RACE_CONFLICTS: u64 = 42;

/// Which side of the cancel race won.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Winner {
    Cancel,
    Completion,
}

/// `w` runs op 1 while client `c` waits for it to leave `accepted` and
/// cancels it. Exactly one of the two decides the result.
fn cancel_race(seed: u64, script: ScheduleScript, gated: bool) -> Winner {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let w = worker(&rig, "w", 10, |p| async move {
        p.work(1).await;
        Ok(())
    });
    let c = client(&rig, "c", 20, |p| async move {
        let mut running = false;
        for _ in 0..64 {
            let status = p
                .operation(&op(1))
                .await
                .map_err(|e| format!("operation: {e:?}"))?;
            if status != Reply::Status(OperationStatus::Accepted) {
                running = true;
                break;
            }
        }
        if !running {
            return Err("op 1 never left accepted".to_string());
        }
        p.cancel(&op(1))
            .await
            .map_err(|e| format!("cancel: {e:?}"))?;
        p.result(&op(1))
            .await
            .map_err(|e| format!("result: {e:?}"))?;
        Ok(())
    });
    let actors = if gated {
        vec![
            w.with_gates(vec![gate(Op::Put, "/head", 3)]),
            c.with_gates(vec![gate(Op::Get, "/head", 1)]),
        ]
    } else {
        vec![w, c]
    };
    let out = run(&rig, seed, script, actors).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    let cancel = cancels(&run);
    let commit = commits(&run);
    let head = rig.head().expect("head");
    let winner = match status_of(&head, 1) {
        Some(OperationStatus::Cancelled) => Winner::Cancel,
        Some(OperationStatus::Completed) => Winner::Completion,
        other => panic!("op 1 ended {other:?}\n{}", out.failure_context()),
    };
    let (expect_cancel, expect_commit) = match winner {
        Winner::Cancel => (
            Reply::Cancel {
                status: OperationStatus::Running,
                requested: true,
            },
            Reply::Committed {
                status: OperationStatus::Cancelled,
                state_version: 0,
            },
        ),
        Winner::Completion => (
            Reply::Cancel {
                status: OperationStatus::Completed,
                requested: false,
            },
            Reply::Committed {
                status: OperationStatus::Completed,
                state_version: 1,
            },
        ),
    };
    sim_assert_eq!(out, cancel, vec![expect_cancel]);
    sim_assert_eq!(out, commit, vec![expect_commit]);
    sim_assert_eq!(out, head.results.len(), 1);
    winner
}

/// Both orders of one race. `w` holds its commit CAS with HEAD already
/// read; `c` cancels first and the commit rebases into `cancelled`, or the
/// commit lands first and the cancel finds it `completed`.
#[test]
fn cancel_racing_completion_one_wins() {
    let cancel_first = ScheduleScript::new(vec![ScriptStep::Release(1), ScriptStep::Release(0)]);
    assert_eq!(cancel_race(1, cancel_first, true), Winner::Cancel);
    let commit_first = ScheduleScript::new(vec![ScriptStep::Release(0), ScriptStep::Release(1)]);
    assert_eq!(cancel_race(1, commit_first, true), Winner::Completion);
}

#[test]
fn cancel_racing_completion_across_seeds() {
    let mut runs = 0;
    let mut cancels_won = 0;
    for seed in seeds() {
        if cancel_race(seed, seeded(seed, 2, 120, 0), false) == Winner::Cancel {
            cancels_won += 1;
        }
        runs += 1;
    }
    pinned("cancel_racing_completion", runs, cancels_won, CANCELS_WON);
}

/// Runs of the batch the cancel won; the completion won the rest.
const CANCELS_WON: usize = 46;

/// Suspected defect in the kernel's cancel of an input no turn has bound:
/// production answers `requested: true`; ADR-3331 decision 5 and the wire
/// doc answer `requested: false`, since the cancel itself ends the input.
#[test]
#[ignore = "suspected defect #3404: kernel cancel of an accepted input answers requested=true"]
fn cancel_of_an_accepted_input_reports_requested_false() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let c = client(&rig, "c", 20, |p| async move {
        p.cancel(&op(1))
            .await
            .map_err(|e| format!("cancel: {e:?}"))?;
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        cancels(&run),
        vec![Reply::Cancel {
            status: OperationStatus::Cancelled,
            requested: false
        }]
    );
}
