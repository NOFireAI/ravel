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
use ravel_agents_core::wire::OperationStatus;
use ravel_agents_runtime::StepProgress;
use ravel_object_store::fault::{FaultPlan, Op};

use super::*;
use crate::drive::{PAST_LEASE_MS, PROVIDER_PREFIX, agent, json_bytes, turn_values};
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
}

/// What each actor reported, by actor name, in the order they ended.
#[derive(Clone, Default)]
struct Ends(Arc<Mutex<Vec<(String, Ended)>>>);

impl Ends {
    fn push(&self, name: &str, ended: Ended) {
        self.0
            .lock()
            .expect("ends")
            .push((name.to_string(), ended));
    }

    fn all(&self) -> Vec<(String, Ended)> {
        self.0.lock().expect("ends").clone()
    }
}

/// Poll one turn and run its step until it replays: dispatch, execute,
/// report, step again. Then commit.
async fn step_turn(p: &Proc, policy: RecoveryPolicy) -> Result<Ended, String> {
    let Some(a) = p.poll().await.map_err(|e| format!("poll: {e:?}"))? else {
        return Ok(Ended::NoWork);
    };
    let operation = Handle::decode(&a.operation).map_err(|e| format!("handle: {e:?}"))?;
    let step = call(policy);
    for _ in 0..2 {
        match p.step(&a, &step).await {
            Ok(StepProgress::Completed { .. }) => {
                let (state, output) = turn_values(&a);
                return match p.commit(&a, &state, &output).await {
                    Ok(_) => Ok(Ended::Committed),
                    Err(e) if e.kind == ErrorKind::OwnershipLost => Ok(Ended::Lost),
                    Err(e) => Err(format!("commit: {e:?}")),
                };
            }
            Ok(StepProgress::Dispatched(d)) => {
                let result = p.execute(&d).await.map_err(|e| format!("execute: {e:?}"))?;
                p.complete_effect(&operation, &d, &result)
                    .await
                    .map_err(|e| format!("complete: {e:?}"))?;
            }
            Err(e) if e.kind == ErrorKind::EffectInDoubt => return Ok(Ended::InDoubt),
            Err(e) if e.kind == ErrorKind::OwnershipLost => return Ok(Ended::Lost),
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
fn stepper(rig: &Rig, name: &str, id_seed: u64, policy: RecoveryPolicy, turns: usize, ends: &Ends) -> Actor {
    let ends = ends.clone();
    let owned = name.to_string();
    worker(rig, name, id_seed, move |p| async move {
        for _ in 0..turns {
            let ended = step_turn(&p, policy).await?;
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
            p.operation(&op(1)).await.map_err(|e| format!("operation: {e:?}"))?;
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
            p.operation(&op(1)).await.map_err(|e| format!("operation: {e:?}"))?;
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

fn expect(
    story: &[&str],
    dispatched: &[(&str, u32)],
    executions: &[(u64, u32)],
    ends: &[(&str, Ended)],
    effect: (EffectStatus, u32, u32),
) -> Story {
    Story {
        story: story.iter().map(|s| s.to_string()).collect(),
        dispatched: dispatched.iter().map(|(n, a)| (n.to_string(), *a)).collect(),
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
