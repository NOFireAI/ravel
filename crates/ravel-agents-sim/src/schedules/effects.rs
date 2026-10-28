//! Steps and their effects: a worker killed or suspended around a tool
//! call under each recovery policy, late results, a handler version change
//! mid-turn, conflicting results, and the cancel wedge.
//!
//! One step that dispatches issues, in order: the HEAD writes that journal
//! the step and authorize an attempt, then the simulated provider's PUT
//! under [`PROVIDER_PREFIX`] (the physical execution), then the result
//! object PUT (key containing `/o/`) and the HEAD CAS that records it. The
//! kill points gate on the provider PUT and the result object PUT.

use std::sync::Mutex;

use ravel_agents_core::error::ErrorKind;
use ravel_agents_core::head::{EffectStatus, Head};
use ravel_agents_core::identity::effect_id;
use ravel_agents_core::state::Handle;
use ravel_agents_core::wire::{HandlerPinBody, OperationStatus};
use ravel_agents_runtime::StepProgress;
use ravel_object_store::fault::{FaultPlan, Op};

use super::*;
use crate::drive::{PAST_LEASE_MS, PROVIDER_PREFIX, agent, json_bytes, pin, turn_values};
use crate::reference::{Event, RecoveryPolicy, Reply, StepCall};
use crate::scheduler::{Action, FailureContext};
use crate::{sim_assert, sim_assert_eq};

const KEY: &str = "charge";

fn op(seq: u64) -> Handle {
    Handle::operation(agent(), 1, seq)
}

fn call(policy: RecoveryPolicy) -> StepCall {
    StepCall {
        key: KEY.to_string(),
        tool: "pay".to_string(),
        version: "1".to_string(),
        input: json_bytes(&json!({ "amount": 5 })),
        policy,
    }
}

/// How a worker's turn ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    NoWork,
    Committed,
    InDoubt,
    Lost,
    /// The poll was refused `IncompatibleHandler`.
    Incompatible,
}

/// What each actor reported, by actor name, in the order they ended.
#[derive(Clone, Default)]
struct Ends(Arc<Mutex<Vec<(String, Ended)>>>);

impl Ends {
    fn push(&self, name: &str, ended: Ended) {
        self.0.lock().expect("ends").push((name.to_string(), ended));
    }

    fn all(&self) -> Vec<(String, Ended)> {
        self.0.lock().expect("ends").clone()
    }
}

/// Poll one turn and run its step until it replays: dispatch, execute,
/// report, step again. Then commit. A step refused `OwnershipLost`, as a
/// cancelled input's is, ends the handler early and it commits what it
/// has, which settles a cancellation.
async fn step_turn(p: &Proc, policy: RecoveryPolicy) -> Result<Ended, String> {
    step_turn_with(p, policy, vec![pin()]).await
}

async fn step_turn_with(
    p: &Proc,
    policy: RecoveryPolicy,
    pins: Vec<HandlerPinBody>,
) -> Result<Ended, String> {
    let a = match p.poll_with(pins).await {
        Ok(Some(a)) => a,
        Ok(None) => return Ok(Ended::NoWork),
        Err(e) if e.kind == ErrorKind::IncompatibleHandler => return Ok(Ended::Incompatible),
        Err(e) => return Err(format!("poll: {e:?}")),
    };
    let operation = Handle::decode(&a.operation).map_err(|e| format!("handle: {e:?}"))?;
    let step = call(policy);
    let commit = || async {
        let (state, output) = turn_values(&a);
        match p.commit(&a, &state, &output).await {
            Ok(_) => Ok(Ended::Committed),
            Err(e) if e.kind == ErrorKind::OwnershipLost => Ok(Ended::Lost),
            Err(e) => Err(format!("commit: {e:?}")),
        }
    };
    for _ in 0..2 {
        match p.step(&a, &step).await {
            Ok(StepProgress::Completed { .. }) => return commit().await,
            Ok(StepProgress::Dispatched(d)) => {
                let result = p.execute(&d).await.map_err(|e| format!("execute: {e:?}"))?;
                p.complete_effect(&operation, &d, &result)
                    .await
                    .map_err(|e| format!("complete: {e:?}"))?;
            }
            Err(e) if e.kind == ErrorKind::EffectInDoubt => return Ok(Ended::InDoubt),
            Err(e) if e.kind == ErrorKind::OwnershipLost => return commit().await,
            Err(e) => return Err(format!("step: {e:?}")),
        }
    }
    Err("the step dispatched twice in one turn".to_string())
}

/// Poll one turn, dispatch its step's first attempt, execute it and report
/// the result, and stop: the result is the tool's callback, which may land
/// after the turn has moved on.
async fn first_attempt(p: &Proc, policy: RecoveryPolicy) -> Result<(), String> {
    let a = p
        .poll()
        .await
        .map_err(|e| format!("poll: {e:?}"))?
        .ok_or("no assignment")?;
    let operation = Handle::decode(&a.operation).map_err(|e| format!("handle: {e:?}"))?;
    let Ok(StepProgress::Dispatched(d)) = p.step(&a, &call(policy)).await else {
        return Err("the first step did not dispatch".to_string());
    };
    let result = p.execute(&d).await.map_err(|e| format!("execute: {e:?}"))?;
    p.complete_effect(&operation, &d, &result)
        .await
        .map_err(|e| format!("complete: {e:?}"))?;
    Ok(())
}

/// A worker that runs [`step_turn`] up to `turns` times and reports each
/// end.
fn stepper(
    rig: &Rig,
    name: &str,
    id_seed: u64,
    policy: RecoveryPolicy,
    turns: usize,
    ends: &Ends,
) -> Actor {
    stepper_with(rig, name, id_seed, policy, turns, ends, vec![pin()])
}

fn stepper_with(
    rig: &Rig,
    name: &str,
    id_seed: u64,
    policy: RecoveryPolicy,
    turns: usize,
    ends: &Ends,
    pins: Vec<HandlerPinBody>,
) -> Actor {
    let ends = ends.clone();
    let owned = name.to_string();
    worker(rig, name, id_seed, move |p| async move {
        for _ in 0..turns {
            let ended = step_turn_with(&p, policy, pins.clone()).await?;
            ends.push(&owned, ended);
            if ended != Ended::Committed {
                break;
            }
        }
        Ok(())
    })
}

/// The provider's executions as `(accept_seq, attempt)`, attempt 0 for an
/// execution keyed by effect id alone.
fn executions(rig: &Rig, inputs: u64) -> Vec<(u64, u32)> {
    let ids: Vec<(u64, String)> = (1..=inputs)
        .map(|seq| (seq, effect_id(1, seq, KEY).to_hex()))
        .collect();
    let mut out: Vec<(u64, u32)> = rig
        .executions()
        .expect("executions")
        .iter()
        .map(|key| {
            let rest = key.strip_prefix(PROVIDER_PREFIX).expect("provider key");
            let (id, attempt) = match rest.split_once("/a") {
                Some((id, n)) => (id, n.parse().expect("attempt")),
                None => (rest, 0),
            };
            let seq = ids
                .iter()
                .find(|(_, hex)| hex == id)
                .map(|(seq, _)| *seq)
                .expect("an effect of an accepted input");
            (seq, attempt)
        })
        .collect();
    out.sort_unstable();
    out
}

/// The effect of input `seq`'s step as `(status, attempt, result_attempt)`.
fn effect_of(head: &Head, seq: u64) -> Option<(EffectStatus, u32, u32)> {
    let id = effect_id(1, seq, KEY);
    head.effects
        .iter()
        .find(|e| e.effect_id == id)
        .map(|e| (e.status, e.attempt, e.result_attempt))
}

/// The step and effect calls of the history, and the commits, in order,
/// each with what it answered.
fn story(run: &Run) -> Vec<String> {
    run.history
        .calls()
        .iter()
        .filter_map(|c| {
            let what = match &c.event {
                Event::TurnStarted { operation } => format!("start {}", seq_of(operation)),
                Event::Step { operation, .. } => format!("step {}", seq_of(operation)),
                Event::StepStarted { operation, .. } => format!("journal {}", seq_of(operation)),
                Event::EffectResult {
                    operation, attempt, ..
                } => format!("result {} a{attempt}", seq_of(operation)),
                Event::ReconcileAttach { operation, .. } => {
                    format!("attach {}", seq_of(operation))
                }
                Event::ReconcileAuthorize {
                    operation, attempt, ..
                } => format!("authorize {} a{attempt}", seq_of(operation)),
                Event::TurnCompleted { operation, .. } => format!("commit {}", seq_of(operation)),
                Event::Client(crate::reference::ClientOp::Operation { operation }) => {
                    format!("status {}", seq_of(operation))
                }
                Event::Client(crate::reference::ClientOp::Cancel { operation }) => {
                    format!("cancel {}", seq_of(operation))
                }
                _ => return None,
            };
            let reply = match &c.observed {
                Reply::TurnStarted => "started".to_string(),
                Reply::StepDispatched { attempt } => format!("dispatched a{attempt}"),
                Reply::StepCompleted { .. } => "replayed".to_string(),
                Reply::EffectRecorded => "recorded".to_string(),
                Reply::EffectEvidence => "evidence".to_string(),
                Reply::Reconciled { dispatched } => format!("reconciled dispatched={dispatched}"),
                Reply::Committed { status, .. } => format!("{status:?}"),
                Reply::Status(status) => format!("{status:?}"),
                Reply::Cancel { status, requested } => format!("{status:?} requested={requested}"),
                other => format!("{other:?}"),
            };
            Some(format!("{what}: {reply}"))
        })
        .collect()
}

fn seq_of(operation: &Handle) -> u64 {
    match operation.item {
        ravel_agents_core::state::HandleItem::Operation(seq) => seq,
        _ => 0,
    }
}

/// Where `w1` stops in its first attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Point {
    /// Killed holding the provider PUT: nothing executed.
    BeforeTool,
    /// Killed holding the result object PUT: executed, never reported.
    AfterExecution,
    /// Held at the result object PUT while `w2` takes over and finishes;
    /// then released, so its result lands late.
    BeforePublication,
}

/// What a kill-point run ends with.
#[derive(Debug, PartialEq, Eq)]
struct Story {
    story: Vec<String>,
    /// `(process, attempt)` of every dispatch.
    dispatched: Vec<(String, u32)>,
    /// `(accept_seq, attempt)` of every physical execution.
    executions: Vec<(u64, u32)>,
    ends: Vec<(String, Ended)>,
    /// The effect as HEAD ends: status, latest attempt, settling attempt.
    effect: Option<(EffectStatus, u32, u32)>,
}

/// One input. `w1` dispatches its step's first attempt and stops at
/// `point`; a replacement worker takes over. Under `manual`, an operator
/// then reconciles the effect in doubt: attaching the result the provider
/// shows executed, or authorizing the next attempt when it shows none, and
/// a third worker finishes the turn.
fn kill_point(policy: RecoveryPolicy, point: Point) -> Story {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let ends = Ends::default();
    let w1 = worker(&rig, "w1", 10, move |p| async move {
        first_attempt(&p, policy).await
    });
    let w1 = match point {
        Point::BeforeTool => w1.with_gates(vec![gate(Op::Put, PROVIDER_PREFIX, 1)]),
        Point::AfterExecution | Point::BeforePublication => {
            w1.with_gates(vec![gate(Op::Put, "/o/", 1)])
        }
    };
    let out = if point == Point::BeforePublication {
        // w2 reads the operation, held, so the clock moves only once w1
        // holds its result.
        let clock = rig.clock.clone();
        let w2_ends = ends.clone();
        let w2 = worker(&rig, "w2", 20, move |p| async move {
            p.operation(&op(1))
                .await
                .map_err(|e| format!("operation: {e:?}"))?;
            clock.advance_ms(PAST_LEASE_MS);
            w2_ends.push("w2", step_turn(&p, policy).await?);
            Ok(())
        })
        .with_gates(vec![gate(Op::Get, "/head", 1)]);
        let out = run(
            &rig,
            1,
            ScheduleScript::new(vec![ScriptStep::Release(1), ScriptStep::Release(0)]),
            vec![w1, w2],
        )
        .expect("schedule");
        let held: Vec<_> = out
            .trace
            .iter()
            .map(|t| (t.name.as_str(), t.action, t.op))
            .collect();
        sim_assert_eq!(
            out,
            held,
            vec![
                ("w2", Action::Released, Op::Get),
                ("w1", Action::Released, Op::Put)
            ]
        );
        out
    } else {
        let out = run(
            &rig,
            1,
            ScheduleScript::new(vec![ScriptStep::Crash(0)]),
            vec![w1],
        )
        .expect("schedule");
        let want = match point {
            Point::BeforeTool => PROVIDER_PREFIX,
            _ => "/o/",
        };
        let crashed: Vec<_> = out
            .trace
            .iter()
            .map(|t| (t.name.as_str(), t.action, t.op, t.key.contains(want)))
            .collect();
        sim_assert_eq!(out, crashed, vec![("w1", Action::Crashed, Op::Put, true)]);
        rig.clock.advance_ms(PAST_LEASE_MS);
        let w2 = stepper(&rig, "w2", 20, policy, 1, &ends);
        run(&rig, 2, ScheduleScript::new(Vec::new()), vec![w2]).expect("takeover")
    };
    let head = rig.head().expect("head");
    let out = if effect_of(&head, 1).map(|e| e.0) == Some(EffectStatus::InDoubt) {
        let attach = executions(&rig, 1).contains(&(1, 1));
        let operator = client(&rig, "op", 40, move |p| async move {
            p.operation(&op(1))
                .await
                .map_err(|e| format!("operation: {e:?}"))?;
            if attach {
                let result = json!({ "step": KEY, "attempt": 1 });
                p.reconcile_attach(&op(1), KEY, &result)
                    .await
                    .map_err(|e| format!("attach: {e:?}"))?;
            } else {
                p.reconcile_authorize(&op(1), KEY, 2)
                    .await
                    .map_err(|e| format!("authorize: {e:?}"))?;
                let d = p.last_dispatch().ok_or("authorize dispatched nothing")?;
                let result = p.execute(&d).await.map_err(|e| format!("execute: {e:?}"))?;
                p.complete_effect(&op(1), &d, &result)
                    .await
                    .map_err(|e| format!("complete: {e:?}"))?;
            }
            Ok(())
        });
        run(&rig, 3, ScheduleScript::new(Vec::new()), vec![operator]).expect("operator");
        rig.clock.advance_ms(PAST_LEASE_MS);
        let w3 = stepper(&rig, "w3", 30, policy, 1, &ends);
        run(&rig, 4, ScheduleScript::new(Vec::new()), vec![w3]).expect("finish")
    } else if head.active_turn.is_some() {
        rig.clock.advance_ms(PAST_LEASE_MS);
        let w3 = stepper(&rig, "w3", 30, policy, 1, &ends);
        run(&rig, 4, ScheduleScript::new(Vec::new()), vec![w3]).expect("finish")
    } else {
        out
    };
    let run = checked(&rig, &out);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    let head = rig.head().expect("head");
    crate::drive::one_terminal_result_per_input(&head)
        .unwrap_or_else(|e| panic!("{e}\n{}", out.failure_context()));
    sim_assert_eq!(
        out,
        crate::drive::status_of(&head, 1),
        Some(OperationStatus::Completed)
    );
    Story {
        story: story(&run),
        dispatched: rig
            .dispatches
            .lock()
            .expect("dispatches")
            .iter()
            .map(|(name, d)| (name.clone(), d.attempt))
            .collect(),
        executions: executions(&rig, 1),
        ends: ends.all(),
        effect: effect_of(&head, 1),
    }
}

/// Two inputs. `w1` dispatches op 1's step and is held before executing
/// it while a client cancels op 1; `w1` then executes, commits with the
/// attempt still open, and, when `late`, reports the result afterwards.
/// `w1` then runs op 2's turn, whose step must dispatch.
fn cancelled_with_open_attempt(late: bool) -> Story {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 2);
    let ends = Ends::default();
    let w1_ends = ends.clone();
    let w1 = worker(&rig, "w1", 10, move |p| async move {
        let a = p
            .poll()
            .await
            .map_err(|e| format!("poll: {e:?}"))?
            .ok_or("no assignment")?;
        let Ok(StepProgress::Dispatched(d)) = p.step(&a, &call(ALO)).await else {
            return Err("the first step did not dispatch".to_string());
        };
        let result = p.execute(&d).await.map_err(|e| format!("execute: {e:?}"))?;
        let (state, output) = turn_values(&a);
        p.commit(&a, &state, &output)
            .await
            .map_err(|e| format!("commit: {e:?}"))?;
        if late {
            p.complete_effect(&op(1), &d, &result)
                .await
                .map_err(|e| format!("complete: {e:?}"))?;
        }
        w1_ends.push("w1", step_turn(&p, ALO).await?);
        Ok(())
    })
    .with_gates(vec![gate(Op::Put, PROVIDER_PREFIX, 1)]);
    let c = client(&rig, "c", 30, |p| async move {
        p.cancel(&op(1))
            .await
            .map_err(|e| format!("cancel: {e:?}"))?;
        Ok(())
    })
    .with_gates(vec![gate(Op::Get, "/head", 1)]);
    let out = run(
        &rig,
        1,
        ScheduleScript::new(vec![ScriptStep::Release(1), ScriptStep::Release(0)]),
        vec![w1, c],
    )
    .expect("schedule");
    let held: Vec<_> = out
        .trace
        .iter()
        .map(|t| (t.name.as_str(), t.action, t.op))
        .collect();
    sim_assert_eq!(
        out,
        held,
        vec![
            ("c", Action::Released, Op::Get),
            ("w1", Action::Released, Op::Put)
        ]
    );
    let run = checked(&rig, &out);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    let head = rig.head().expect("head");
    crate::drive::one_terminal_result_per_input(&head)
        .unwrap_or_else(|e| panic!("{e}\n{}", out.failure_context()));
    sim_assert_eq!(
        out,
        (
            crate::drive::status_of(&head, 1),
            crate::drive::status_of(&head, 2)
        ),
        (
            Some(OperationStatus::Cancelled),
            Some(OperationStatus::Completed)
        )
    );
    sim_assert_eq!(
        out,
        effect_of(&head, 2),
        Some((EffectStatus::Completed, 1, 1))
    );
    Story {
        story: story(&run),
        dispatched: rig
            .dispatches
            .lock()
            .expect("dispatches")
            .iter()
            .map(|(name, d)| (name.clone(), d.attempt))
            .collect(),
        executions: executions(&rig, 2),
        ends: ends.all(),
        effect: effect_of(&head, 1),
    }
}

/// The cancel wedge: a cancelled turn committed with its attempt still
/// open, and the next turn's step dispatches rather than being refused
/// for the earlier input's open attempt.
#[test]
fn cancelled_turn_with_open_attempt_does_not_wedge_the_next_step() {
    assert_eq!(
        cancelled_with_open_attempt(false),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "cancel 1: Running requested=true",
                "commit 1: Cancelled",
                "start 2: started",
                "step 2: dispatched a1",
                "result 2 a1: recorded",
                "step 2: replayed",
                "commit 2: Completed",
            ],
            &[("w1", 1), ("w1", 1)],
            &[(1, 1), (2, 1)],
            &[("w1", Ended::Committed)],
            (EffectStatus::Authorized, 1, 0),
        )
    );
}

/// A result landing after its input settled cancelled is evidence, and
/// the input stays cancelled.
#[test]
fn late_result_after_cancel_is_evidence() {
    assert_eq!(
        cancelled_with_open_attempt(true),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "cancel 1: Running requested=true",
                "commit 1: Cancelled",
                "result 1 a1: evidence",
                "start 2: started",
                "step 2: dispatched a1",
                "result 2 a1: recorded",
                "step 2: replayed",
                "commit 2: Completed",
            ],
            &[("w1", 1), ("w1", 1)],
            &[(1, 1), (2, 1)],
            &[("w1", Ended::Committed)],
            (EffectStatus::Authorized, 1, 0),
        )
    );
}

/// `w1` begins op 1 under `v1`, dispatches its step and is held before
/// executing it. The deployment moves to `v2`: `w2`, presenting only `v2`,
/// is refused the pinned turn. `w3`, still on `v1`, takes over, re-dispatches
/// the same attempt and commits. `w1` then executes, reports the same
/// result, and its commit is refused after `w3`'s.
#[test]
fn handler_version_change_while_a_turn_is_suspended() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let ends = Ends::default();
    let w1_ends = ends.clone();
    let w1 = worker(&rig, "w1", 10, move |p| async move {
        let a = p
            .poll()
            .await
            .map_err(|e| format!("poll: {e:?}"))?
            .ok_or("no assignment")?;
        let Ok(StepProgress::Dispatched(d)) = p.step(&a, &call(PI)).await else {
            return Err("the first step did not dispatch".to_string());
        };
        let result = p.execute(&d).await.map_err(|e| format!("execute: {e:?}"))?;
        p.complete_effect(&op(1), &d, &result)
            .await
            .map_err(|e| format!("complete: {e:?}"))?;
        let (state, output) = turn_values(&a);
        match p.commit(&a, &state, &output).await {
            Err(e) if e.kind == ErrorKind::OwnershipLost => w1_ends.push("w1", Ended::Lost),
            other => return Err(format!("commit: {other:?}")),
        }
        Ok(())
    })
    .with_gates(vec![gate(Op::Put, PROVIDER_PREFIX, 1)]);
    let clock = rig.clock.clone();
    let w2 = worker(&rig, "w2", 20, move |p| async move {
        p.operation(&op(1))
            .await
            .map_err(|e| format!("operation: {e:?}"))?;
        clock.advance_ms(PAST_LEASE_MS);
        let v2 = HandlerPinBody {
            version: "v2".to_string(),
            ..pin()
        };
        match p.poll_with(vec![v2]).await {
            Err(e) if e.kind == ErrorKind::IncompatibleHandler => Ok(()),
            other => Err(format!("poll: {other:?}")),
        }
    })
    .with_gates(vec![gate(Op::Get, "/head", 1)]);
    let w3 = stepper(&rig, "w3", 30, PI, 1, &ends).with_gates(vec![gate(Op::List, "", 1)]);
    let out = run(
        &rig,
        1,
        ScheduleScript::new(vec![
            ScriptStep::Release(1),
            ScriptStep::Release(2),
            ScriptStep::Release(0),
        ]),
        vec![w1, w2, w3],
    )
    .expect("schedule");
    let held: Vec<_> = out
        .trace
        .iter()
        .map(|t| (t.name.as_str(), t.action, t.op))
        .collect();
    sim_assert_eq!(
        out,
        held,
        vec![
            ("w2", Action::Released, Op::Get),
            ("w3", Action::Released, Op::List),
            ("w1", Action::Released, Op::Put)
        ]
    );
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        run.refusals,
        vec![crate::drive::Refusal {
            actor: "w2".to_string(),
            call: "poll",
            kind: ErrorKind::IncompatibleHandler,
        }]
    );
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    sim_assert_eq!(
        out,
        story(&run),
        expect_story(&[
            "start 1: started",
            "step 1: dispatched a1",
            "status 1: Running",
            "start 1: started",
            "step 1: dispatched a1",
            "result 1 a1: recorded",
            "step 1: replayed",
            "commit 1: Completed",
            "result 1 a1: recorded",
            "commit 1: Error(OwnershipLost)",
        ])
    );
    sim_assert_eq!(
        out,
        ends.all(),
        vec![
            ("w3".to_string(), Ended::Committed),
            ("w1".to_string(), Ended::Lost)
        ]
    );
    sim_assert_eq!(out, executions(&rig, 1), vec![(1, 0)]);
    let metrics = |name: &str| rig.proc_named(name).expect("proc").co.metrics();
    sim_assert_eq!(
        out,
        (
            metrics("w1").ownership_lost_refusals,
            metrics("w2").incompatible_handler_refusals,
            metrics("w3").turns_committed
        ),
        (1, 1, 1)
    );
}

/// Two different results reported for the same attempt: the first is the
/// effect's, the second is refused `IdempotencyConflict`, and the step
/// replays the first.
#[test]
fn conflicting_results_for_one_attempt_are_idempotency_conflict() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let w1 = worker(&rig, "w1", 10, |p| async move {
        let a = p
            .poll()
            .await
            .map_err(|e| format!("poll: {e:?}"))?
            .ok_or("no assignment")?;
        let Ok(StepProgress::Dispatched(d)) = p.step(&a, &call(ALO)).await else {
            return Err("the first step did not dispatch".to_string());
        };
        p.complete_effect(&op(1), &d, &json!({ "charged": 5 }))
            .await
            .map_err(|e| format!("first result: {e:?}"))?;
        match p
            .complete_effect(&op(1), &d, &json!({ "charged": 6 }))
            .await
        {
            Err(e) if e.kind == ErrorKind::IdempotencyConflict => {}
            other => return Err(format!("second result: {other:?}")),
        }
        let Ok(StepProgress::Completed { output }) = p.step(&a, &call(ALO)).await else {
            return Err("the step did not replay".to_string());
        };
        if output != json!({ "charged": 5 }) {
            return Err(format!("replayed {output}"));
        }
        let (state, out) = turn_values(&a);
        p.commit(&a, &state, &out)
            .await
            .map_err(|e| format!("commit: {e:?}"))?;
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![w1]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    sim_assert_eq!(
        out,
        story(&run),
        expect_story(&[
            "start 1: started",
            "step 1: dispatched a1",
            "result 1 a1: recorded",
            "result 1 a1: Error(IdempotencyConflict)",
            "step 1: replayed",
            "commit 1: Completed",
        ])
    );
    let head = rig.head().expect("head");
    sim_assert_eq!(
        out,
        effect_of(&head, 1),
        Some((EffectStatus::Completed, 1, 1))
    );
}

/// A client asks after an operation handle naming no accepted input, and
/// resolves a request id never sent.
#[test]
fn query_of_a_handle_with_no_input() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let c = client(&rig, "c", 30, |p| async move {
        let status = p
            .operation(&op(99))
            .await
            .map_err(|e| format!("operation: {e:?}"))?;
        let result = p
            .result(&op(99))
            .await
            .map_err(|e| format!("result: {e:?}"))?;
        let resolved = p
            .resolve("never-sent")
            .await
            .map_err(|e| format!("resolve: {e:?}"))?;
        let got = (status, result, resolved);
        let want = (
            Reply::UnknownOperation,
            Reply::UnknownOperation,
            Reply::Resolved(None),
        );
        if got != want {
            return Err(format!("{got:?}"));
        }
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    let replies: Vec<Reply> = run
        .history
        .calls()
        .iter()
        .filter(|c| {
            matches!(
                c.event,
                Event::Client(
                    crate::reference::ClientOp::Operation { .. }
                        | crate::reference::ClientOp::Result { .. }
                        | crate::reference::ClientOp::Resolve { .. }
                )
            )
        })
        .map(|c| c.observed.clone())
        .collect();
    sim_assert_eq!(
        out,
        replies,
        vec![
            Reply::UnknownOperation,
            Reply::UnknownOperation,
            Reply::Resolved(None)
        ]
    );
}

/// A step journaled before it runs, an attempt that fails, and the journal
/// read again: the failure is the effect's result and the step replays it.
#[test]
fn journaled_step_with_a_failed_attempt_replays_the_failure() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let w1 = worker(&rig, "w1", 10, |p| async move {
        let a = p
            .poll()
            .await
            .map_err(|e| format!("poll: {e:?}"))?
            .ok_or("no assignment")?;
        p.record_step(&a, &call(ALO))
            .await
            .map_err(|e| format!("journal: {e:?}"))?;
        let Ok(StepProgress::Dispatched(d)) = p.step(&a, &call(ALO)).await else {
            return Err("the first step did not dispatch".to_string());
        };
        p.fail_effect(&op(1), &d, "declined")
            .await
            .map_err(|e| format!("fail: {e:?}"))?;
        p.record_step(&a, &call(ALO))
            .await
            .map_err(|e| format!("journal again: {e:?}"))?;
        let Ok(StepProgress::Completed { output }) = p.step(&a, &call(ALO)).await else {
            return Err("the step did not replay".to_string());
        };
        if output != json!({ "error": "declined" }) {
            return Err(format!("replayed {output}"));
        }
        let (state, out) = turn_values(&a);
        p.commit(&a, &state, &out)
            .await
            .map_err(|e| format!("commit: {e:?}"))?;
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![w1]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.refusals.is_empty(), "{:?}", run.refusals);
    sim_assert_eq!(
        out,
        story(&run),
        expect_story(&[
            "start 1: started",
            "journal 1: StepJournaled { completed: false, effect: None }",
            "step 1: dispatched a1",
            "result 1 a1: recorded",
            "journal 1: StepJournaled { completed: false, effect: Some(EffectView { phase: Completed, attempt: 1 }) }",
            "step 1: replayed",
            "commit 1: Completed",
        ])
    );
}

/// Counts a batch run ends with; summed over a batch's seeds.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Totals {
    runs: usize,
    crashes: usize,
    /// Calls a crash left unanswered whose applied write the reference
    /// answered.
    inferred: usize,
    /// Attempts handed to a dispatcher, by a step or a reconcile.
    dispatches: usize,
    /// Distinct (effect, attempt) pairs among them.
    distinct_dispatches: usize,
    /// The latest attempt of every effect HEAD ends with, summed.
    attempts: usize,
    /// Physical executions at the provider.
    executions: usize,
    /// Executions beyond the first of an effect.
    duplicates: usize,
    /// Steps refused `EffectInDoubt`.
    in_doubt: usize,
    /// Effects an operator reconciled.
    reconciled: usize,
    /// Results kept as evidence.
    evidence: usize,
    /// Results refused `IdempotencyConflict`.
    conflicts: usize,
    /// Inputs that settled cancelled.
    cancelled: usize,
    /// Polls refused `IncompatibleHandler`.
    incompatible: usize,
}

impl std::ops::AddAssign for Totals {
    fn add_assign(&mut self, o: Totals) {
        self.runs += o.runs;
        self.crashes += o.crashes;
        self.inferred += o.inferred;
        self.dispatches += o.dispatches;
        self.distinct_dispatches += o.distinct_dispatches;
        self.attempts += o.attempts;
        self.executions += o.executions;
        self.duplicates += o.duplicates;
        self.in_doubt += o.in_doubt;
        self.reconciled += o.reconciled;
        self.evidence += o.evidence;
        self.conflicts += o.conflicts;
        self.cancelled += o.cancelled;
        self.incompatible += o.incompatible;
    }
}

/// An operator settles the effect of input `seq`, in doubt at `attempt`:
/// it attaches that attempt's result when the provider shows it executed,
/// else authorizes the next attempt, executes it and reports its result.
async fn reconcile(p: &Proc, seq: u64, attempt: u32, executed: bool) -> Result<(), String> {
    p.operation(&op(seq))
        .await
        .map_err(|e| format!("operation: {e:?}"))?;
    if executed {
        let result = json!({ "step": KEY, "attempt": attempt });
        p.reconcile_attach(&op(seq), KEY, &result)
            .await
            .map_err(|e| format!("attach: {e:?}"))?;
        return Ok(());
    }
    p.reconcile_authorize(&op(seq), KEY, attempt + 1)
        .await
        .map_err(|e| format!("authorize: {e:?}"))?;
    let d = p.last_dispatch().ok_or("authorize dispatched nothing")?;
    let result = p.execute(&d).await.map_err(|e| format!("execute: {e:?}"))?;
    p.complete_effect(&op(seq), &d, &result)
        .await
        .map_err(|e| format!("complete: {e:?}"))?;
    Ok(())
}

/// Settle what a schedule left. Each round an operator reconciles every
/// effect in doubt, then a fresh worker takes over and runs turns. Returns
/// the reconciles made.
fn finish(rig: &Rig, policy: RecoveryPolicy, seed: u64, inputs: u64) -> usize {
    finish_with(rig, policy, seed, inputs, vec![pin()])
}

fn finish_with(
    rig: &Rig,
    policy: RecoveryPolicy,
    seed: u64,
    inputs: u64,
    pins: Vec<HandlerPinBody>,
) -> usize {
    let mut reconciled = 0;
    for round in 0..4u64 {
        let head = rig.head().expect("head");
        if head.inbox.is_empty() && head.active_turn.is_none() {
            break;
        }
        let doubts: Vec<(u64, u32)> = (1..=inputs)
            .filter_map(|seq| match effect_of(&head, seq) {
                Some((EffectStatus::InDoubt, attempt, _)) => Some((seq, attempt)),
                _ => None,
            })
            .collect();
        if !doubts.is_empty() {
            let executed = executions(rig, inputs);
            reconciled += doubts.len();
            let operator = client(
                rig,
                &format!("op{round}"),
                40 + round,
                move |p| async move {
                    for (seq, attempt) in doubts {
                        reconcile(&p, seq, attempt, executed.contains(&(seq, attempt))).await?;
                    }
                    Ok(())
                },
            );
            run(rig, seed, ScheduleScript::new(Vec::new()), vec![operator]).expect("operator");
        }
        rig.clock.advance_ms(PAST_LEASE_MS);
        let w = stepper_with(
            rig,
            &format!("fin{round}"),
            50 + round,
            policy,
            2 * inputs as usize,
            &Ends::default(),
            pins.clone(),
        );
        run(rig, seed, ScheduleScript::new(Vec::new()), vec![w]).expect("finish");
    }
    reconciled
}

fn crashes(out: &ScheduleOutcome) -> usize {
    out.trace
        .iter()
        .filter(|t| t.action == Action::Crashed)
        .count()
}

/// Run each worker alone under its own seeded script, every lease past its
/// grace before the next. Two workers polling at once can reach the
/// takeover race [`takeover_retried_on_fresh_head_fences_a_live_turn`]
/// pins, so the batches hand work over one worker at a time. Returns the
/// last outcome and the crashes across all of them.
fn relay(rig: &Rig, seed: u64, workers: Vec<Actor>) -> (ScheduleOutcome, usize) {
    let mut last = None;
    let mut crashed = 0;
    for (i, w) in workers.into_iter().enumerate() {
        if i > 0 {
            rig.clock.advance_ms(PAST_LEASE_MS);
        }
        let s = seed ^ ((i as u64) << 32);
        let out = run_in_batch(rig, seed, s, seeded(s, 1, 40, 80), vec![w]).expect("relay");
        crashed += crashes(&out);
        last = Some(out);
    }
    (last.expect("one worker"), crashed)
}

/// A relay worker after the first draws from a seed derived from the
/// batch seed; its failure message labels both and replays the batch.
#[test]
fn a_relay_failure_replays_from_the_batch_seed() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let w1 = worker(&rig, "w1", 10, |p| async move {
        p.work(1).await;
        Ok(())
    });
    let w2 = worker(&rig, "w2", 20, |p| async move {
        p.work(1).await;
        Ok(())
    });
    let (out, _) = relay(&rig, BASE, vec![w1, w2]);
    let derived = BASE ^ (1 << 32);
    assert_eq!((out.batch_seed, out.seed), (BASE, derived));
    let message = out.failure_context();
    assert!(
        message.contains(&format!(
            "batch seed: {BASE} ({BASE:#x})\nscheduler seed: {derived} ({derived:#x})\n"
        )),
        "{message}"
    );
    assert!(
        message.contains(&format!("replay: RAVEL_SIM_SEED={BASE} cargo test")),
        "{message}"
    );
    assert!(
        !message.contains(&format!("RAVEL_SIM_SEED={derived}")),
        "{message}"
    );
}

/// Check the whole run and count it. Every input reached one terminal
/// result, and nothing was refused but an `IncompatibleHandler` poll.
fn tally(rig: &Rig, out: &ScheduleOutcome, inputs: u64, reconciled: usize) -> Totals {
    let run = checked(rig, out);
    sim_assert!(
        *out,
        run.refusals
            .iter()
            .all(|r| r.call == "poll" && r.kind == ErrorKind::IncompatibleHandler),
        "{:?}",
        run.refusals
    );
    let head = rig.head().expect("head");
    crate::drive::one_terminal_result_per_input(&head)
        .unwrap_or_else(|e| panic!("{e}\n{}", out.failure_context()));
    let dispatched: Vec<(ravel_agents_core::identity::Id128, u32)> = rig
        .dispatches
        .lock()
        .expect("dispatches")
        .iter()
        .map(|(_, d)| (d.effect.effect_id, d.attempt))
        .collect();
    let mut distinct = dispatched.clone();
    distinct.sort_unstable();
    distinct.dedup();
    let executed = executions(rig, inputs);
    let mut executed_effects: Vec<u64> = executed.iter().map(|(seq, _)| *seq).collect();
    executed_effects.dedup();
    let observed = |reply: Reply| {
        run.history
            .calls()
            .iter()
            .filter(|c| c.observed == reply)
            .count()
    };
    Totals {
        runs: 1,
        crashes: 0,
        inferred: run.inferred.len(),
        dispatches: dispatched.len(),
        distinct_dispatches: distinct.len(),
        attempts: head.effects.iter().map(|e| e.attempt as usize).sum(),
        executions: executed.len(),
        duplicates: executed.len() - executed_effects.len(),
        in_doubt: observed(Reply::Error(ErrorKind::EffectInDoubt)),
        reconciled,
        evidence: observed(Reply::EffectEvidence),
        conflicts: observed(Reply::Error(ErrorKind::IdempotencyConflict)),
        cancelled: (1..=inputs)
            .filter(|seq| crate::drive::status_of(&head, *seq) == Some(OperationStatus::Cancelled))
            .count(),
        incompatible: run.refusals.len(),
    }
}

/// Two inputs, two workers in turn running step turns under `policy`,
/// killed at random; then reconciliation and a replacement finish the work.
fn kills(policy: RecoveryPolicy, seed: u64) -> Totals {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 2);
    let ends = Ends::default();
    let w1 = stepper(&rig, "w1", 10, policy, 2, &ends);
    let w2 = stepper(&rig, "w2", 20, policy, 2, &ends);
    let (out, crashed) = relay(&rig, seed, vec![w1, w2]);
    let reconciled = finish(&rig, policy, seed, 2);
    let mut t = tally(&rig, &out, 2, reconciled);
    t.crashes = crashed;
    let head = rig.head().expect("head");
    sim_assert_eq!(
        out,
        (
            crate::drive::status_of(&head, 1),
            crate::drive::status_of(&head, 2)
        ),
        (
            Some(OperationStatus::Completed),
            Some(OperationStatus::Completed)
        )
    );
    match policy {
        RecoveryPolicy::ProviderIdempotency => {
            sim_assert_eq!(out, (t.duplicates, t.distinct_dispatches), (0, 2));
        }
        RecoveryPolicy::AtLeastOnce => {
            sim_assert_eq!(out, t.distinct_dispatches, t.dispatches);
        }
        RecoveryPolicy::Manual => {
            sim_assert_eq!(
                out,
                (t.dispatches, t.distinct_dispatches),
                (t.attempts, t.attempts)
            );
        }
    }
    t
}

fn kill_batch(policy: RecoveryPolicy) -> Totals {
    let mut totals = Totals::default();
    for seed in seeds() {
        totals += kills(policy, seed);
    }
    totals
}

#[test]
fn provider_idempotency_kills_across_seeds() {
    let pi = kill_batch(PI);
    pinned("provider_idempotency_kills", pi.runs as u64, pi, PI_TOTALS);
}

/// The acceptance check for recovery policies. Under `at_least_once` a
/// kill between execution and the result's publication re-executes the
/// tool: the batch reaches that duplicate physical execution. Under
/// `manual` no attempt is ever dispatched twice: every dispatch is a
/// distinct attempt and their number is the attempts HEAD authorized.
#[test]
fn at_least_once_duplicate_execution_is_reachable_and_manual_policy_never_redispatches() {
    let alo = kill_batch(ALO);
    pinned("at_least_once_kills", alo.runs as u64, alo, ALO_TOTALS);
    if Batch::from_env().is_default() {
        assert!(alo.duplicates > 0, "{alo:?}");
    }
    let manual = kill_batch(MANUAL);
    pinned("manual_kills", manual.runs as u64, manual, MANUAL_TOTALS);
    assert_eq!(
        (manual.dispatches, manual.distinct_dispatches),
        (manual.attempts, manual.attempts)
    );
}

/// Summed over each batch. Pinned: a scheduler or production change that
/// moves where workers die moves them.
/// A provider-idempotency attempt is dispatched again under the same
/// attempt number, 16 times, and the provider executes each effect once.
const PI_TOTALS: Totals = Totals {
    runs: 64,
    crashes: 114,
    inferred: 27,
    dispatches: 144,
    distinct_dispatches: 128,
    attempts: 128,
    executions: 128,
    duplicates: 0,
    in_doubt: 0,
    reconciled: 0,
    evidence: 0,
    conflicts: 0,
    cancelled: 0,
    incompatible: 0,
};

/// The same kills: each re-dispatch is a new attempt, and 13 of them
/// executed the tool a second time.
const ALO_TOTALS: Totals = Totals {
    runs: 64,
    crashes: 114,
    inferred: 27,
    dispatches: 144,
    distinct_dispatches: 144,
    attempts: 144,
    executions: 141,
    duplicates: 13,
    in_doubt: 0,
    reconciled: 0,
    evidence: 0,
    conflicts: 0,
    cancelled: 0,
    incompatible: 0,
};

/// The same kills: 15 attempts left in doubt, each reconciled by an
/// operator, and no attempt dispatched twice.
const MANUAL_TOTALS: Totals = Totals {
    runs: 64,
    crashes: 112,
    inferred: 27,
    dispatches: 131,
    distinct_dispatches: 131,
    attempts: 131,
    executions: 128,
    duplicates: 0,
    in_doubt: 15,
    reconciled: 15,
    evidence: 0,
    conflicts: 0,
    cancelled: 0,
    incompatible: 0,
};

/// Two inputs; a worker runs step turns under `at_least_once` while a
/// client cancels op 1, the worker killed at random. One worker, for the
/// reason [`relay`] gives.
fn cancels(seed: u64) -> Totals {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 2);
    let ends = Ends::default();
    let w1 = stepper(&rig, "w1", 10, ALO, 2, &ends);
    // A cancel of an accepted input answers `requested: true`, the suspected
    // defect `races::cancel_of_an_accepted_input_reports_requested_false`
    // pins, so `c` cancels only once op 1 has left `accepted`.
    let c = client(&rig, "c", 30, |p| async move {
        for _ in 0..64 {
            let status = p
                .operation(&op(1))
                .await
                .map_err(|e| format!("operation: {e:?}"))?;
            if status != Reply::Status(OperationStatus::Accepted) {
                p.cancel(&op(1))
                    .await
                    .map_err(|e| format!("cancel: {e:?}"))?;
                break;
            }
        }
        Ok(())
    });
    let script = crash_workers_only(seeded(seed, 2, 80, 80), &[1]);
    let out = run(&rig, seed, script, vec![w1, c]).expect("schedule");
    let reconciled = finish(&rig, ALO, seed, 2);
    let mut t = tally(&rig, &out, 2, reconciled);
    t.crashes = crashes(&out);
    let head = rig.head().expect("head");
    sim_assert_eq!(
        out,
        crate::drive::status_of(&head, 2),
        Some(OperationStatus::Completed)
    );
    t
}

#[test]
fn cancels_across_seeds() {
    let mut totals = Totals::default();
    for seed in seeds() {
        totals += cancels(seed);
    }
    pinned("cancels", totals.runs as u64, totals, CANCEL_TOTALS);
}

const CANCEL_TOTALS: Totals = Totals {
    runs: 64,
    crashes: 53,
    inferred: 14,
    dispatches: 94,
    distinct_dispatches: 94,
    attempts: 94,
    executions: 93,
    duplicates: 2,
    in_doubt: 0,
    reconciled: 0,
    evidence: 0,
    conflicts: 0,
    cancelled: 43,
    incompatible: 0,
};

/// One input. `w1` dispatches the step's attempt; two reporters then race
/// different results for it. One is recorded and the other refused, and
/// the turn replays the recorded one.
fn conflicts(seed: u64) -> Totals {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let slot: Arc<Mutex<Option<ravel_agents_runtime::Dispatch>>> = Arc::default();
    let stash = slot.clone();
    let w1 = worker(&rig, "w1", 10, move |p| async move {
        let a = p
            .poll()
            .await
            .map_err(|e| format!("poll: {e:?}"))?
            .ok_or("no assignment")?;
        let Ok(StepProgress::Dispatched(d)) = p.step(&a, &call(ALO)).await else {
            return Err("the first step did not dispatch".to_string());
        };
        *stash.lock().expect("slot") = Some(d);
        Ok(())
    });
    run(&rig, seed, ScheduleScript::new(Vec::new()), vec![w1]).expect("dispatch");
    let d = slot.lock().expect("slot").clone().expect("dispatched");
    let reporters = ["r1", "r2"].map(|name| {
        let d = d.clone();
        client(&rig, name, 60, move |p| async move {
            match p.complete_effect(&op(1), &d, &json!({ "by": name })).await {
                Ok(_) => Ok(()),
                Err(e) if e.kind == ErrorKind::IdempotencyConflict => Ok(()),
                Err(e) => Err(format!("complete: {e:?}")),
            }
        })
    });
    let out = run(&rig, seed, seeded(seed, 2, 40, 0), reporters.into()).expect("schedule");
    let reconciled = finish(&rig, ALO, seed, 1);
    let t = tally(&rig, &out, 1, reconciled);
    sim_assert_eq!(out, t.conflicts, 1);
    t
}

#[test]
fn conflicting_reporters_across_seeds() {
    let mut totals = Totals::default();
    for seed in seeds() {
        totals += conflicts(seed);
    }
    pinned(
        "conflicting_reporters",
        totals.runs as u64,
        totals,
        CONFLICT_TOTALS,
    );
}

const CONFLICT_TOTALS: Totals = Totals {
    runs: 64,
    crashes: 0,
    inferred: 0,
    dispatches: 64,
    distinct_dispatches: 64,
    attempts: 64,
    executions: 0,
    duplicates: 0,
    in_doubt: 0,
    reconciled: 0,
    evidence: 0,
    conflicts: 64,
    cancelled: 0,
    incompatible: 0,
};

/// Two inputs. `w1` on `v1`, `w2` on `v2` only and `w3` on `v1` run step
/// turns in turn, each killed at random; a worker that finds a turn begun
/// on the other version is refused. A replacement holding both finishes.
fn versions(seed: u64) -> Totals {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 2);
    let ends = Ends::default();
    let v2 = HandlerPinBody {
        version: "v2".to_string(),
        ..pin()
    };
    let w1 = stepper(&rig, "w1", 10, PI, 2, &ends);
    let w2 = stepper_with(&rig, "w2", 20, PI, 2, &ends, vec![v2.clone()]);
    let w3 = stepper(&rig, "w3", 30, PI, 2, &ends);
    let (out, crashed) = relay(&rig, seed, vec![w1, w2, w3]);
    let reconciled = finish_with(&rig, PI, seed, 2, vec![pin(), v2]);
    let mut t = tally(&rig, &out, 2, reconciled);
    t.crashes = crashed;
    t
}

/// Two inputs, two workers polling at once, nobody killed and the clock
/// never moved, so no lease runs out. Returns the turns that ended fenced
/// `OwnershipLost`.
fn concurrent_pollers(seed: u64) -> usize {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 2);
    let ends = Ends::default();
    let w1 = stepper(&rig, "w1", 10, PI, 2, &ends);
    let w2 = stepper(&rig, "w2", 20, PI, 2, &ends);
    run(&rig, seed, seeded(seed, 2, 80, 0), vec![w1, w2]).expect("schedule");
    ends.all().iter().filter(|(_, e)| *e == Ended::Lost).count()
}

/// Suspected defect in the runtime's takeover. `takeover_due` reads a HEAD
/// whose owner holds no active turn; the owner then begins one, the
/// takeover's CAS conflicts, and the store's publish loop applies
/// `Takeover` again to the fresh HEAD. The kernel checks only the observed
/// epoch, which `BeginTurn` does not move, so the takeover lands on a live
/// turn whose lease never ran out and its owner is fenced `OwnershipLost`.
/// `Coordinator::takeover_due` documents the takeover as allowed only
/// while the owner holds no active turn or its lease and grace ran out.
/// 25 of the 64 batch seeds reach it with no fault at all; this is the
/// first.
#[test]
#[ignore = "suspected defect #3415: a takeover re-applied after a lost CAS fences a live turn"]
fn takeover_retried_on_fresh_head_fences_a_live_turn() {
    assert_eq!(concurrent_pollers(0x5eed_0001), 0);
}

#[test]
fn handler_version_changes_across_seeds() {
    let mut totals = Totals::default();
    for seed in seeds() {
        totals += versions(seed);
    }
    pinned(
        "handler_version_changes",
        totals.runs as u64,
        totals,
        VERSION_TOTALS,
    );
}

const VERSION_TOTALS: Totals = Totals {
    runs: 64,
    crashes: 136,
    inferred: 21,
    dispatches: 147,
    distinct_dispatches: 128,
    attempts: 128,
    executions: 128,
    duplicates: 0,
    in_doubt: 0,
    reconciled: 0,
    evidence: 0,
    conflicts: 0,
    cancelled: 0,
    incompatible: 37,
};

fn expect_story(lines: &[&str]) -> Vec<String> {
    lines.iter().map(|s| s.to_string()).collect()
}

fn expect(
    story: &[&str],
    dispatched: &[(&str, u32)],
    executions: &[(u64, u32)],
    ends: &[(&str, Ended)],
    effect: (EffectStatus, u32, u32),
) -> Story {
    Story {
        story: story.iter().map(|s| s.to_string()).collect(),
        dispatched: dispatched
            .iter()
            .map(|(n, a)| (n.to_string(), *a))
            .collect(),
        executions: executions.to_vec(),
        ends: ends.iter().map(|(n, e)| (n.to_string(), *e)).collect(),
        effect: Some(effect),
    }
}

const PI: RecoveryPolicy = RecoveryPolicy::ProviderIdempotency;
const ALO: RecoveryPolicy = RecoveryPolicy::AtLeastOnce;
const MANUAL: RecoveryPolicy = RecoveryPolicy::Manual;

/// The takeover re-dispatches the same attempt of the same effect.
#[test]
fn provider_idempotency_killed_before_tool_call() {
    assert_eq!(
        kill_point(PI, Point::BeforeTool),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "start 1: started",
                "step 1: dispatched a1",
                "result 1 a1: recorded",
                "step 1: replayed",
                "commit 1: Completed",
            ],
            &[("w1", 1), ("w2", 1)],
            &[(1, 0)],
            &[("w2", Ended::Committed)],
            (EffectStatus::Completed, 1, 1),
        )
    );
}

/// The re-dispatch of the same effect id executes nothing new at the
/// provider: one execution, one result.
#[test]
fn provider_idempotency_killed_after_execution() {
    assert_eq!(
        kill_point(PI, Point::AfterExecution),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "start 1: started",
                "step 1: dispatched a1",
                "result 1 a1: recorded",
                "step 1: replayed",
                "commit 1: Completed",
            ],
            &[("w1", 1), ("w2", 1)],
            &[(1, 0)],
            &[("w2", Ended::Committed)],
            (EffectStatus::Completed, 1, 1),
        )
    );
}

/// The suspended worker's late result is the same attempt's, with the same
/// bytes: recorded again, idempotently.
#[test]
fn provider_idempotency_suspended_before_result_publication() {
    assert_eq!(
        kill_point(PI, Point::BeforePublication),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "status 1: Running",
                "start 1: started",
                "step 1: dispatched a1",
                "result 1 a1: recorded",
                "step 1: replayed",
                "commit 1: Completed",
                "result 1 a1: recorded",
            ],
            &[("w1", 1), ("w2", 1)],
            &[(1, 0)],
            &[("w2", Ended::Committed)],
            (EffectStatus::Completed, 1, 1),
        )
    );
}

/// The takeover dispatches a new attempt.
#[test]
fn at_least_once_killed_before_tool_call() {
    assert_eq!(
        kill_point(ALO, Point::BeforeTool),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "start 1: started",
                "step 1: dispatched a2",
                "result 1 a2: recorded",
                "step 1: replayed",
                "commit 1: Completed",
            ],
            &[("w1", 1), ("w2", 2)],
            &[(1, 2)],
            &[("w2", Ended::Committed)],
            (EffectStatus::Completed, 2, 2),
        )
    );
}

/// Attempt 1 executed and was never reported; attempt 2 executes again: a
/// duplicate physical execution, as the policy allows.
#[test]
fn at_least_once_killed_after_execution() {
    assert_eq!(
        kill_point(ALO, Point::AfterExecution),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "start 1: started",
                "step 1: dispatched a2",
                "result 1 a2: recorded",
                "step 1: replayed",
                "commit 1: Completed",
            ],
            &[("w1", 1), ("w2", 2)],
            &[(1, 1), (1, 2)],
            &[("w2", Ended::Committed)],
            (EffectStatus::Completed, 2, 2),
        )
    );
}

/// Attempt 2's result is the step's; attempt 1's, landing after the
/// takeover, is evidence.
#[test]
fn at_least_once_late_result_after_takeover_is_evidence() {
    assert_eq!(
        kill_point(ALO, Point::BeforePublication),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "status 1: Running",
                "start 1: started",
                "step 1: dispatched a2",
                "result 1 a2: recorded",
                "step 1: replayed",
                "commit 1: Completed",
                "result 1 a1: evidence",
            ],
            &[("w1", 1), ("w2", 2)],
            &[(1, 1), (1, 2)],
            &[("w2", Ended::Committed)],
            (EffectStatus::Completed, 2, 2),
        )
    );
}

/// The takeover finds the effect in doubt and dispatches nothing; the
/// operation reads blocked until an operator authorizes attempt 2, which
/// is dispatched once.
#[test]
fn manual_killed_before_tool_call() {
    assert_eq!(
        kill_point(MANUAL, Point::BeforeTool),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "start 1: started",
                "step 1: Error(EffectInDoubt)",
                "status 1: Blocked",
                "authorize 1 a2: reconciled dispatched=true",
                "result 1 a2: recorded",
                "start 1: started",
                "step 1: replayed",
                "commit 1: Completed",
            ],
            &[("w1", 1), ("op", 2)],
            &[(1, 2)],
            &[("w2", Ended::InDoubt), ("w3", Ended::Committed)],
            (EffectStatus::Completed, 2, 2),
        )
    );
}

/// The operator attaches the result the provider shows attempt 1 produced;
/// nothing is dispatched again.
#[test]
fn manual_killed_after_execution() {
    assert_eq!(
        kill_point(MANUAL, Point::AfterExecution),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "start 1: started",
                "step 1: Error(EffectInDoubt)",
                "status 1: Blocked",
                "attach 1: recorded",
                "start 1: started",
                "step 1: replayed",
                "commit 1: Completed",
            ],
            &[("w1", 1)],
            &[(1, 1)],
            &[("w2", Ended::InDoubt), ("w3", Ended::Committed)],
            (EffectStatus::Completed, 1, 1),
        )
    );
}

/// The takeover latches the effect in doubt; the suspended worker's result
/// for the latest attempt then settles it, and the next owner replays.
#[test]
fn manual_suspended_before_result_publication() {
    assert_eq!(
        kill_point(MANUAL, Point::BeforePublication),
        expect(
            &[
                "start 1: started",
                "step 1: dispatched a1",
                "status 1: Running",
                "start 1: started",
                "step 1: Error(EffectInDoubt)",
                "result 1 a1: recorded",
                "start 1: started",
                "step 1: replayed",
                "commit 1: Completed",
            ],
            &[("w1", 1)],
            &[(1, 1)],
            &[("w2", Ended::InDoubt), ("w3", Ended::Committed)],
            (EffectStatus::Completed, 1, 1),
        )
    );
}
