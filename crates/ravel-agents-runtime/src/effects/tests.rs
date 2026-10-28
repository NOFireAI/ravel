#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;
use ravel_agents_core::error::ErrorKind;
use ravel_agents_core::head::{
    Cancellation, EffectStatus, Head, RecoveryPolicy, ResultStatus, TurnBlock,
};
use ravel_agents_core::identity::{Hash256, Id128, step_input_hash};
use ravel_agents_core::kernel::{AUDIT_RECONCILE_ATTEMPT, AUDIT_RECONCILE_RESULT};
use ravel_agents_core::wire::{CancelRequest, OperationStatus, StepState, TurnAssignment};
use ravel_agents_store::AgentStore;
use ravel_object_store::fault::ScriptedFault;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, Sequence, SequenceStep,
};
use ravel_object_store::memory::MemoryStore;
use serde_json::{Value, json};

use super::*;
use crate::FailOutcome;
use crate::tests::{
    TestClock, clock, completed, coordinator, create, head, id_of, past_lease_and_grace, poll_now,
    rig, send, store_over, worker,
};

/// Records every attempt handed to it.
#[derive(Default)]
struct Hook(Mutex<Vec<Dispatch>>);

impl EffectDispatcher for Hook {
    fn dispatch(&self, dispatch: &Dispatch) {
        self.0.lock().push(dispatch.clone());
    }
}

impl Hook {
    fn calls(&self) -> usize {
        self.0.lock().len()
    }
}

/// A tool provider. Under `provider_idempotency` it deduplicates by effect
/// id, so a re-dispatch of an executed effect returns the first result
/// without executing again; under `at_least_once` every attempt executes.
#[derive(Default)]
struct Provider {
    executions: AtomicU32,
    done: Mutex<HashMap<Id128, Value>>,
}

impl Provider {
    fn execute(&self, d: &Dispatch) -> Value {
        if d.policy == RecoveryPolicy::ProviderIdempotency
            && let Some(v) = self.done.lock().get(&d.effect.effect_id)
        {
            return v.clone();
        }
        let n = self.executions.fetch_add(1, Ordering::SeqCst) + 1;
        let v = json!({ "charged": n, "attempt": d.attempt });
        self.done.lock().insert(d.effect.effect_id, v.clone());
        v
    }

    fn executions(&self) -> u32 {
        self.executions.load(Ordering::SeqCst)
    }
}

/// Where the worker that dispatched an attempt died.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Crash {
    /// Before it called the provider.
    BeforeCall,
    /// After the provider executed, before any result came back.
    AfterExecution,
    /// With the result in hand, before publishing it; it publishes late.
    BeforeResultPublication,
}

const CRASHES: [Crash; 3] = [
    Crash::BeforeCall,
    Crash::AfterExecution,
    Crash::BeforeResultPublication,
];

fn spec(key: &str, policy: RecoveryPolicy) -> StepSpec {
    StepSpec {
        step_key: key.to_string(),
        tool_name: "charge".to_string(),
        tool_version: "1".to_string(),
        policy,
        input_hash: step_input_hash("charge", "1", b"{\"amount\":5}"),
    }
}

/// Create the agent, send one input and lease it to a fresh worker.
async fn leased(co: &Coordinator, key: &str) -> (TurnAssignment, String) {
    create(co, key).await;
    let sent = send(co, key, "r-1", json!({"n": 1})).await;
    let w = worker(co, "v1");
    let a = poll_now(co, &w, "v1").await.expect("leased");
    (a, sent.operation)
}

/// The leased worker dies; a replacement takes the turn over.
async fn takeover(co: &Coordinator, clock: &TestClock) -> TurnAssignment {
    clock.advance_ms(past_lease_and_grace());
    let w = worker(co, "v1");
    poll_now(co, &w, "v1").await.expect("replacement leased")
}

async fn dispatched(
    co: &Coordinator,
    a: &TurnAssignment,
    spec: &StepSpec,
    hook: &Hook,
) -> Dispatch {
    match co.run_step(&a.lease, spec, hook).await.expect("run step") {
        StepProgress::Dispatched(d) => d,
        other => panic!("expected a dispatch, got {other:?}"),
    }
}

async fn step_output(co: &Coordinator, a: &TurnAssignment, spec: &StepSpec, hook: &Hook) -> Value {
    match co.run_step(&a.lease, spec, hook).await.expect("run step") {
        StepProgress::Completed { output } => output,
        other => panic!("expected the completed step, got {other:?}"),
    }
}

fn effect_of<'a>(h: &'a Head, d: &Dispatch) -> &'a ravel_agents_core::head::EffectRecord {
    h.effects
        .iter()
        .find(|e| e.effect_id == d.effect.effect_id)
        .expect("effect journaled")
}

fn cas_since(store: &AgentStore, before: &ravel_agents_store::counters::CounterSnapshot) -> u64 {
    store.counters().since(before).publish.cas
}

/// The epochs of the attempts this coordinator holds open.
fn open_epochs(co: &Coordinator) -> Vec<u64> {
    co.open_attempts.lock().values().map(|o| o.epoch).collect()
}

fn audits(h: &Head, event_type: &str) -> usize {
    h.events
        .iter()
        .filter(|e| e.event_type == event_type)
        .count()
}

#[tokio::test(start_paused = true)]
async fn manual_policy_attempt_with_lost_result_blocks_as_effect_in_doubt_and_is_never_redispatched()
 {
    let (clock, store, co) = rig();
    let (a, _) = leased(&co, "o-1").await;
    let s = spec("wire-funds", RecoveryPolicy::Manual);
    let hook = Hook::default();
    let d = dispatched(&co, &a, &s, &hook).await;
    assert_eq!((d.attempt, d.policy), (1, RecoveryPolicy::Manual));
    assert_eq!(hook.calls(), 1);

    // The worker dies holding the attempt; its result is lost.
    let b = takeover(&co, &clock).await;
    assert_eq!(b.lease.turn_id, a.lease.turn_id);
    let err = co
        .run_step(&b.lease, &s, &hook)
        .await
        .expect_err("a lost manual attempt is in doubt");
    assert_eq!(err.kind, ErrorKind::EffectInDoubt);
    let h = head(&store, "o-1").await;
    let effect = effect_of(&h, &d);
    assert_eq!((effect.status, effect.attempt), (EffectStatus::InDoubt, 1));
    assert_eq!(hook.calls(), 1);

    let before = store.counters();
    let idle = worker(&co, "v1");
    for _ in 0..20 {
        assert!(poll_now(&co, &idle, "v1").await.is_none());
        let err = co
            .run_step(&b.lease, &s, &hook)
            .await
            .expect_err("still in doubt");
        assert_eq!(err.kind, ErrorKind::EffectInDoubt);
    }
    assert_eq!(cas_since(&store, &before), 0, "no write while in doubt");
    assert_eq!(hook.calls(), 1, "never re-dispatched");
    assert_eq!(effect_of(&head(&store, "o-1").await, &d).attempt, 1);

    let refused = co
        .fail(&b.lease, "gave up", true)
        .await
        .expect_err("FailTurn while in doubt");
    assert_eq!(refused.kind, ErrorKind::EffectInDoubt);
    let h = head(&store, "o-1").await;
    assert!(h.results.iter().all(|r| r.accept_seq != b.accept_seq));
    assert!(h.active_turn.is_some());

    let verified = json!({"wire": "confirmed by the bank"});
    assert_eq!(
        co.reconcile_attach(&d.effect, &verified)
            .await
            .expect("reconcile"),
        EffectSettled::Recorded
    );
    assert_eq!(hook.calls(), 1);
    assert_eq!(step_output(&co, &b, &s, &hook).await, verified);
    let done = co
        .commit(&completed(&b.lease, json!({"paid": true})))
        .await
        .expect("commit");
    assert_eq!(done.status, OperationStatus::Completed);
    let h = head(&store, "o-1").await;
    let results: Vec<_> = h
        .results
        .iter()
        .filter(|r| r.accept_seq == b.accept_seq)
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ResultStatus::Completed);
    assert_eq!(co.metrics().turns_committed, 1);
    assert_eq!(audits(&h, AUDIT_RECONCILE_RESULT), 1);
}

#[tokio::test(start_paused = true)]
async fn provider_idempotency_redispatches_the_same_effect_at_every_crash_point() {
    for crash in CRASHES {
        let (clock, store, co) = rig();
        let (a, _) = leased(&co, "o-1").await;
        let s = spec("charge-card", RecoveryPolicy::ProviderIdempotency);
        let hook = Hook::default();
        let provider = Provider::default();

        let before = store.counters();
        let d1 = dispatched(&co, &a, &s, &hook).await;
        assert_eq!(
            cas_since(&store, &before),
            3,
            "{crash:?}: step, intent and attempt CASes precede the dispatch"
        );
        let h = head(&store, "o-1").await;
        assert_eq!(
            (effect_of(&h, &d1).status, effect_of(&h, &d1).attempt),
            (EffectStatus::Authorized, 1)
        );
        let late = match crash {
            Crash::BeforeCall => None,
            Crash::AfterExecution => {
                provider.execute(&d1);
                None
            }
            Crash::BeforeResultPublication => Some(provider.execute(&d1)),
        };

        let b = takeover(&co, &clock).await;
        let before = store.counters();
        let d2 = dispatched(&co, &b, &s, &hook).await;
        assert_eq!(cas_since(&store, &before), 0, "{crash:?}: no new attempt");
        assert_eq!((d2.effect, d2.attempt), (d1.effect, 1), "{crash:?}");
        assert_eq!(hook.calls(), 2);
        let r2 = provider.execute(&d2);
        assert_eq!(provider.executions(), 1, "{crash:?}: executed once");

        let before = store.counters();
        if let Some(v) = &late {
            assert_eq!(
                co.complete_effect(&d1.effect, d1.attempt, v)
                    .await
                    .expect("late"),
                EffectSettled::Recorded
            );
        }
        assert_eq!(
            co.complete_effect(&d2.effect, d2.attempt, &r2)
                .await
                .expect("result"),
            EffectSettled::Recorded
        );
        assert_eq!(cas_since(&store, &before), 1, "{crash:?}: lands once");
        assert_eq!(step_output(&co, &b, &s, &hook).await, r2);
        let h = head(&store, "o-1").await;
        let effect = effect_of(&h, &d1);
        assert_eq!(
            (effect.status, effect.result_attempt),
            (EffectStatus::Completed, 1)
        );
        co.commit(&completed(&b.lease, json!("ok")))
            .await
            .expect("commit");
        assert_eq!(hook.calls(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn at_least_once_authorizes_a_new_attempt_at_every_crash_point() {
    for crash in CRASHES {
        let (clock, store, co) = rig();
        let (a, _) = leased(&co, "o-1").await;
        let s = spec("send-email", RecoveryPolicy::AtLeastOnce);
        let hook = Hook::default();
        let provider = Provider::default();
        let d1 = dispatched(&co, &a, &s, &hook).await;
        let late = match crash {
            Crash::BeforeCall => None,
            Crash::AfterExecution => {
                provider.execute(&d1);
                None
            }
            Crash::BeforeResultPublication => Some(provider.execute(&d1)),
        };

        assert_eq!(open_epochs(&co), vec![a.lease.epoch]);
        let b = takeover(&co, &clock).await;
        assert!(
            open_epochs(&co).is_empty(),
            "{crash:?}: the takeover drops the old epoch's attempt"
        );
        let before = store.counters();
        let d2 = dispatched(&co, &b, &s, &hook).await;
        assert_eq!(
            cas_since(&store, &before),
            1,
            "{crash:?}: one authorization"
        );
        assert_eq!(open_epochs(&co), vec![b.lease.epoch]);
        assert_eq!((d2.effect, d2.attempt), (d1.effect, 2), "{crash:?}");
        assert_eq!(hook.calls(), 2);
        let r2 = provider.execute(&d2);
        let expected = if crash == Crash::BeforeCall { 1 } else { 2 };
        assert_eq!(provider.executions(), expected, "{crash:?}");

        if let Some(v) = &late {
            assert!(matches!(
                co.complete_effect(&d1.effect, 1, v).await.expect("late"),
                EffectSettled::Evidence { .. }
            ));
        }
        assert_eq!(
            co.complete_effect(&d2.effect, 2, &r2)
                .await
                .expect("result"),
            EffectSettled::Recorded
        );
        if let Some(v) = &late {
            assert!(matches!(
                co.complete_effect(&d1.effect, 1, v).await.expect("later"),
                EffectSettled::Evidence { .. }
            ));
        }
        assert_eq!(step_output(&co, &b, &s, &hook).await, r2, "{crash:?}");
        let h = head(&store, "o-1").await;
        assert_eq!(effect_of(&h, &d1).result_attempt, 2);
        co.commit(&completed(&b.lease, json!("ok")))
            .await
            .expect("commit");
    }
}

#[tokio::test(start_paused = true)]
async fn a_completed_step_replays_without_a_new_intent_or_attempt() {
    let (clock, store, co) = rig();
    let (a, _) = leased(&co, "o-1").await;
    let s = spec("charge-card", RecoveryPolicy::ProviderIdempotency);
    let hook = Hook::default();
    let d = dispatched(&co, &a, &s, &hook).await;
    co.complete_effect(&d.effect, d.attempt, &json!(42))
        .await
        .expect("result");
    assert_eq!(step_output(&co, &a, &s, &hook).await, json!(42));

    let before = store.counters();
    assert_eq!(step_output(&co, &a, &s, &hook).await, json!(42));
    assert_eq!(cas_since(&store, &before), 0);

    let b = takeover(&co, &clock).await;
    assert_eq!(b.steps.len(), 1);
    assert_eq!(b.steps[0].status, StepState::Completed);
    assert_eq!(b.steps[0].output, Some(json!(42)));
    let before = store.counters();
    assert_eq!(step_output(&co, &b, &s, &hook).await, json!(42));
    assert_eq!(cas_since(&store, &before), 0, "no intent and no attempt");
    assert_eq!(hook.calls(), 1);
    assert_eq!(effect_of(&head(&store, "o-1").await, &d).attempt, 1);
}

#[tokio::test(start_paused = true)]
async fn drift_in_tool_version_or_input_hash_blocks_the_turn_before_dispatch() {
    let drifts: [fn(&mut StepSpec); 2] = [
        |s| s.tool_version = "2".to_string(),
        |s| s.input_hash = Hash256([9; 32]),
    ];
    for (i, drift) in drifts.into_iter().enumerate() {
        let (_, store, co) = rig();
        let key = format!("o-{i}");
        let (a, _) = leased(&co, &key).await;
        let s = spec("charge-card", RecoveryPolicy::AtLeastOnce);
        let hook = Hook::default();
        let d = dispatched(&co, &a, &s, &hook).await;
        co.complete_effect(&d.effect, d.attempt, &json!("paid"))
            .await
            .expect("result");
        assert_eq!(step_output(&co, &a, &s, &hook).await, json!("paid"));

        let mut drifted = s.clone();
        drift(&mut drifted);
        let err = co
            .run_step(&a.lease, &drifted, &hook)
            .await
            .expect_err("drift");
        assert_eq!(err.kind, ErrorKind::NonDeterministicReplay, "drift {i}");
        assert_eq!(hook.calls(), 1, "nothing dispatched");
        let h = head(&store, &key).await;
        assert_eq!(
            h.active_turn.expect("turn").block,
            TurnBlock::NonDeterministicReplay
        );
        assert_eq!(h.effects.len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn a_second_step_while_an_attempt_is_open_is_refused_typed() {
    let (_, store, co) = rig();
    let (a, _) = leased(&co, "o-1").await;
    let hook = Hook::default();
    let first = spec("first", RecoveryPolicy::AtLeastOnce);
    let second = spec("second", RecoveryPolicy::AtLeastOnce);
    let d = dispatched(&co, &a, &first, &hook).await;

    let before = store.counters();
    let err = co
        .run_step(&a.lease, &second, &hook)
        .await
        .expect_err("an attempt is open");
    assert_eq!(err.kind, ErrorKind::IdempotencyConflict);
    let err = co
        .record_step(&a.lease, &second)
        .await
        .expect_err("an attempt is open");
    assert_eq!(err.kind, ErrorKind::IdempotencyConflict);
    assert_eq!(cas_since(&store, &before), 0);
    assert_eq!(hook.calls(), 1);

    co.complete_effect(&d.effect, d.attempt, &json!(1))
        .await
        .expect("result");
    dispatched(&co, &a, &second, &hook).await;
    assert_eq!(hook.calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn late_results_after_takeover_and_after_cancel_are_evidence() {
    // After a takeover authorized attempt 2.
    let (clock, store, co) = rig();
    let (a, _) = leased(&co, "o-1").await;
    let s = spec("send-email", RecoveryPolicy::AtLeastOnce);
    let hook = Hook::default();
    let d1 = dispatched(&co, &a, &s, &hook).await;
    let b = takeover(&co, &clock).await;
    let d2 = dispatched(&co, &b, &s, &hook).await;
    let base = head(&store, "o-1").await;
    let before = store.counters();
    let EffectSettled::Evidence { result } = co
        .complete_effect(&d1.effect, d1.attempt, &json!("late"))
        .await
        .expect("late")
    else {
        panic!("a superseded attempt's result is evidence");
    };
    assert_eq!(cas_since(&store, &before), 0);
    assert_eq!(head(&store, "o-1").await, base);
    let kept = store
        .read_object(&id_of("o-1"), result)
        .await
        .expect("evidence kept");
    assert_eq!(
        serde_json::from_slice::<Value>(&kept).expect("json"),
        json!("late")
    );

    // A conflicting second final for the same attempt.
    co.complete_effect(&d2.effect, d2.attempt, &json!("sent"))
        .await
        .expect("result");
    let err = co
        .complete_effect(&d2.effect, d2.attempt, &json!("different"))
        .await
        .expect_err("conflicting final");
    assert_eq!(err.kind, ErrorKind::IdempotencyConflict);

    // After the input ended cancelled.
    let (_, store, co) = rig();
    let (a, operation) = leased(&co, "o-2").await;
    let d = dispatched(&co, &a, &s, &hook).await;
    co.cancel(&CancelRequest { operation })
        .await
        .expect("cancel");
    let done = co
        .commit(&completed(&a.lease, json!("ok")))
        .await
        .expect("commit");
    assert_eq!(done.status, OperationStatus::Cancelled);
    let base = head(&store, "o-2").await;
    assert!(matches!(
        co.complete_effect(&d.effect, d.attempt, &json!("too late"))
            .await
            .expect("late"),
        EffectSettled::Evidence { .. }
    ));
    assert_eq!(head(&store, "o-2").await, base);
}

#[tokio::test(start_paused = true)]
async fn a_new_attempt_for_a_cancelled_input_is_refused_and_the_completion_settles_cancelled() {
    let (clock, store, co) = rig();
    let (a, operation) = leased(&co, "o-1").await;
    let s = spec("send-email", RecoveryPolicy::AtLeastOnce);
    let hook = Hook::default();
    let d = dispatched(&co, &a, &s, &hook).await;
    co.cancel(&CancelRequest { operation })
        .await
        .expect("cancel");
    let b = takeover(&co, &clock).await;
    assert_eq!(
        head(&store, "o-1")
            .await
            .active_turn
            .expect("turn")
            .cancellation,
        Cancellation::Requested
    );

    let before = store.counters();
    let err = co
        .run_step(&b.lease, &s, &hook)
        .await
        .expect_err("no new attempt");
    assert_eq!(err.kind, ErrorKind::OwnershipLost);
    let err = co
        .authorize_attempt(&b.lease, d.effect.effect_id, 2)
        .await
        .expect_err("the kernel refuses it too");
    assert_eq!(err.kind, ErrorKind::OwnershipLost);
    let err = co
        .run_step(&b.lease, &spec("next", RecoveryPolicy::AtLeastOnce), &hook)
        .await
        .expect_err("no first attempt either");
    assert_eq!(err.kind, ErrorKind::OwnershipLost);
    assert_eq!(hook.calls(), 1);
    assert_eq!(effect_of(&head(&store, "o-1").await, &d).attempt, 1);
    assert_eq!(
        cas_since(&store, &before),
        1,
        "only the next step's journal"
    );

    let done = co
        .commit(&completed(&b.lease, json!("ok")))
        .await
        .expect("commit");
    assert_eq!(done.status, OperationStatus::Cancelled);
    let h = head(&store, "o-1").await;
    assert!(
        h.results
            .iter()
            .any(|r| r.accept_seq == b.accept_seq && r.status == ResultStatus::Cancelled)
    );
}

#[tokio::test(start_paused = true)]
async fn reconcile_is_idempotent_and_audited_once() {
    let (clock, store, co) = rig();
    let (a, _) = leased(&co, "o-1").await;
    let s = spec("wire-funds", RecoveryPolicy::Manual);
    let hook = Hook::default();
    let d = dispatched(&co, &a, &s, &hook).await;

    let err = co
        .reconcile_attach(&d.effect, &json!(1))
        .await
        .expect_err("not in doubt");
    assert_eq!(err.kind, ErrorKind::OwnershipLost);

    let b = takeover(&co, &clock).await;
    co.run_step(&b.lease, &s, &hook)
        .await
        .expect_err("in doubt");
    assert!(
        co.reconcile_authorize(&d.effect, 2, &hook)
            .await
            .expect("authorize")
    );
    assert_eq!(hook.calls(), 2);
    assert!(
        !co.reconcile_authorize(&d.effect, 2, &hook)
            .await
            .expect("again")
    );
    assert_eq!(hook.calls(), 2, "authorized once");
    let h = head(&store, "o-1").await;
    assert_eq!(audits(&h, AUDIT_RECONCILE_ATTEMPT), 1);
    assert_eq!(effect_of(&h, &d).attempt, 2);
    let err = co
        .run_step(&b.lease, &spec("next", RecoveryPolicy::Manual), &hook)
        .await
        .expect_err("the reconciled attempt is open under this epoch");
    assert_eq!(err.kind, ErrorKind::IdempotencyConflict);

    // The reconciled attempt is lost too; the effect latches again.
    let c = takeover(&co, &clock).await;
    co.run_step(&c.lease, &s, &hook)
        .await
        .expect_err("in doubt again");
    for _ in 0..2 {
        assert_eq!(
            co.reconcile_attach(&d.effect, &json!("verified"))
                .await
                .expect("attach"),
            EffectSettled::Recorded
        );
    }
    let h = head(&store, "o-1").await;
    assert_eq!(audits(&h, AUDIT_RECONCILE_RESULT), 1);
    assert_eq!(effect_of(&h, &d).result_attempt, 2);
    assert_eq!(hook.calls(), 2);
    assert_eq!(step_output(&co, &c, &s, &hook).await, json!("verified"));
}

#[tokio::test(start_paused = true)]
async fn fail_effect_records_the_failure_so_the_turn_can_fail() {
    let (clock, _, co) = rig();
    let (a, _) = leased(&co, "o-1").await;
    let s = spec("send-email", RecoveryPolicy::AtLeastOnce);
    let hook = Hook::default();
    let d1 = dispatched(&co, &a, &s, &hook).await;
    let err = co
        .fail(&a.lease, "boom", false)
        .await
        .expect_err("an authorized attempt has no result");
    assert_eq!(err.kind, ErrorKind::EffectInDoubt);

    let b = takeover(&co, &clock).await;
    let d2 = dispatched(&co, &b, &s, &hook).await;
    // An older attempt's failure is evidence and does not unblock FailTurn.
    assert!(matches!(
        co.fail_effect(&d1.effect, d1.attempt, "timeout")
            .await
            .expect("older"),
        EffectSettled::Evidence { .. }
    ));
    let err = co
        .fail(&b.lease, "boom", false)
        .await
        .expect_err("attempt 2 has no result");
    assert_eq!(err.kind, ErrorKind::EffectInDoubt);

    assert_eq!(
        co.fail_effect(&d2.effect, d2.attempt, "rejected")
            .await
            .expect("failure"),
        EffectSettled::Recorded
    );
    assert!(matches!(
        co.fail(&b.lease, "boom", false).await.expect("fail"),
        FailOutcome::Retry { .. }
    ));
}

#[tokio::test(start_paused = true)]
async fn an_effect_result_with_a_lost_ack_lands_once() {
    // HEAD writes: create, enqueue, takeover, begin, then the step journal
    // (5), the intent (6), the authorization (7) and the result (8).
    for nth in 5..=8u64 {
        let clock = clock();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
                .with_key_contains("/head")
                .with_occurrence(Occurrence::Nth(nth)),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store = store_over(fault.clone(), &clock);
        let co = coordinator(&store, &clock, 100);
        let (a, _) = leased(&co, "o-1").await;
        let s = spec("charge-card", RecoveryPolicy::ProviderIdempotency);
        let hook = Hook::default();

        let before = store.counters();
        let d = dispatched(&co, &a, &s, &hook).await;
        assert_eq!(
            co.complete_effect(&d.effect, d.attempt, &json!("paid"))
                .await
                .expect("result"),
            EffectSettled::Recorded
        );
        assert_eq!(
            fault.fault_count(Op::Put, FaultKind::DuplicateDelivery),
            1,
            "write {nth}"
        );
        assert_eq!(
            cas_since(&store, &before),
            4,
            "write {nth}: each lands once"
        );
        assert_eq!(hook.calls(), 1);
        let h = head(&store, "o-1").await;
        assert_eq!(h.effects.len(), 1);
        assert_eq!(effect_of(&h, &d).status, EffectStatus::Completed);
        assert_eq!(step_output(&co, &a, &s, &hook).await, json!("paid"));
        co.commit(&completed(&a.lease, json!("ok")))
            .await
            .expect("commit");
    }
}

/// A coordinator and its store over `backend`, as a restarted process builds
/// them: nothing in memory, a different rng seed and so different worker ids.
fn restarted(
    backend: Arc<MemoryStore>,
    clock: &Arc<TestClock>,
) -> (Arc<AgentStore>, Arc<Coordinator>) {
    let store = store_over(backend, clock);
    let co = coordinator(&store, clock, 200);
    (store, co)
}

/// The first coordinator dispatches attempt 1 of `spec` and is dropped
/// before any result; a second one over the same `MemoryStore` takes over.
async fn dispatched_then_restarted(
    spec: &StepSpec,
    hook: &Hook,
) -> (
    Arc<TestClock>,
    Arc<AgentStore>,
    Arc<Coordinator>,
    TurnAssignment,
    Dispatch,
) {
    let clock = clock();
    let backend = Arc::new(MemoryStore::new());
    let store1 = store_over(backend.clone(), &clock);
    let co1 = coordinator(&store1, &clock, 100);
    let (a, _) = leased(&co1, "o-1").await;
    let d1 = dispatched(&co1, &a, spec, hook).await;
    assert_eq!(d1.attempt, 1);
    drop(co1);
    drop(store1);
    let (store, co) = restarted(backend, &clock);
    (clock, store, co, a, d1)
}

#[tokio::test(start_paused = true)]
async fn provider_idempotency_redispatches_the_same_effect_after_a_coordinator_restart() {
    let s = spec("charge-card", RecoveryPolicy::ProviderIdempotency);
    let hook = Hook::default();
    let (clock, store, co, a, d1) = dispatched_then_restarted(&s, &hook).await;

    // Under the same lease.
    let before = store.counters();
    let d2 = dispatched(&co, &a, &s, &hook).await;
    assert_eq!(cas_since(&store, &before), 0, "no write");
    assert_eq!((d2.effect, d2.attempt), (d1.effect, 1));
    assert_eq!(hook.calls(), 2);

    // After a takeover by the restarted coordinator's worker.
    let b = takeover(&co, &clock).await;
    assert!(b.lease.epoch > a.lease.epoch);
    assert_ne!(b.lease.owner_id, a.lease.owner_id);
    let before = store.counters();
    let d3 = dispatched(&co, &b, &s, &hook).await;
    assert_eq!(cas_since(&store, &before), 0, "no write");
    assert_eq!((d3.effect, d3.attempt), (d1.effect, 1));
    assert_eq!(hook.calls(), 3);
    let h = head(&store, "o-1").await;
    assert_eq!(
        (effect_of(&h, &d1).status, effect_of(&h, &d1).attempt),
        (EffectStatus::Authorized, 1)
    );
}

#[tokio::test(start_paused = true)]
async fn at_least_once_authorizes_a_new_attempt_after_a_coordinator_restart() {
    let s = spec("send-email", RecoveryPolicy::AtLeastOnce);
    let hook = Hook::default();
    let (clock, store, co, a, d1) = dispatched_then_restarted(&s, &hook).await;

    let before = store.counters();
    let d2 = dispatched(&co, &a, &s, &hook).await;
    assert_eq!(cas_since(&store, &before), 1, "one authorization");
    assert_eq!((d2.effect, d2.attempt), (d1.effect, 2));
    assert_eq!(hook.calls(), 2);

    let b = takeover(&co, &clock).await;
    assert!(b.lease.epoch > a.lease.epoch);
    let before = store.counters();
    let d3 = dispatched(&co, &b, &s, &hook).await;
    assert_eq!(cas_since(&store, &before), 1, "one authorization");
    assert_eq!((d3.effect, d3.attempt), (d1.effect, 3));
    assert_eq!(hook.calls(), 3);
    let h = head(&store, "o-1").await;
    assert_eq!(
        (effect_of(&h, &d1).status, effect_of(&h, &d1).attempt),
        (EffectStatus::Authorized, 3)
    );
}

#[tokio::test(start_paused = true)]
async fn manual_latches_in_doubt_after_a_coordinator_restart_and_is_never_redispatched() {
    let s = spec("wire-funds", RecoveryPolicy::Manual);
    let hook = Hook::default();
    let (clock, store, co, a, d1) = dispatched_then_restarted(&s, &hook).await;

    let before = store.counters();
    let err = co
        .run_step(&a.lease, &s, &hook)
        .await
        .expect_err("a lost manual attempt is in doubt");
    assert_eq!(err.kind, ErrorKind::EffectInDoubt);
    assert_eq!(cas_since(&store, &before), 1, "the latch");
    assert_eq!(hook.calls(), 1, "nothing dispatched after the restart");
    let h = head(&store, "o-1").await;
    assert_eq!(
        (effect_of(&h, &d1).status, effect_of(&h, &d1).attempt),
        (EffectStatus::InDoubt, 1)
    );

    let b = takeover(&co, &clock).await;
    assert!(b.lease.epoch > a.lease.epoch);
    let before = store.counters();
    let err = co
        .run_step(&b.lease, &s, &hook)
        .await
        .expect_err("still in doubt after a takeover");
    assert_eq!(err.kind, ErrorKind::EffectInDoubt);
    assert_eq!(cas_since(&store, &before), 0);
    assert_eq!(hook.calls(), 1, "nothing dispatched after the takeover");
    assert_eq!(effect_of(&head(&store, "o-1").await, &d1).attempt, 1);
}

/// HEAD writes up to the reconcile authorization of a latched manual effect,
/// counted by a sequence of pass-through steps.
async fn head_writes_before_reconcile() -> u64 {
    let clock = clock();
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Put)
            .with_key_contains("/head")
            .with_steps(vec![SequenceStep::Passthrough; 100]),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let co = coordinator(&store, &clock, 100);
    let (a, _) = leased(&co, "o-1").await;
    let s = spec("wire-funds", RecoveryPolicy::Manual);
    let hook = Hook::default();
    dispatched(&co, &a, &s, &hook).await;
    let b = takeover(&co, &clock).await;
    co.run_step(&b.lease, &s, &hook)
        .await
        .expect_err("in doubt");
    let writes = fault.sequence_progress(0);
    assert!(writes < 100);
    writes
}

#[tokio::test(start_paused = true)]
async fn a_reconcile_authorization_with_a_lost_ack_dispatches_once() {
    let nth = head_writes_before_reconcile().await + 1;
    let clock = clock();
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Nth(nth)),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let co = coordinator(&store, &clock, 100);
    let (a, _) = leased(&co, "o-1").await;
    let s = spec("wire-funds", RecoveryPolicy::Manual);
    let hook = Hook::default();
    let d = dispatched(&co, &a, &s, &hook).await;
    let b = takeover(&co, &clock).await;
    co.run_step(&b.lease, &s, &hook)
        .await
        .expect_err("in doubt");
    assert_eq!(fault.fault_count(Op::Put, FaultKind::DuplicateDelivery), 0);

    let before = store.counters();
    let resolved = co.metrics().outcome_unknown_resolved;
    assert!(
        co.reconcile_authorize(&d.effect, 2, &hook)
            .await
            .expect("authorize"),
        "this call's attempt landed"
    );
    assert_eq!(fault.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_eq!(cas_since(&store, &before), 1);
    assert_eq!(co.metrics().outcome_unknown_resolved, resolved + 1);
    assert_eq!(hook.calls(), 2);
    let last = hook.0.lock().last().cloned().expect("a dispatch");
    assert_eq!((last.effect, last.attempt), (d.effect, 2));
    let h = head(&store, "o-1").await;
    assert_eq!(
        (effect_of(&h, &d).status, effect_of(&h, &d).attempt),
        (EffectStatus::Authorized, 2)
    );
    assert_eq!(audits(&h, AUDIT_RECONCILE_ATTEMPT), 1);

    // A genuine repeat dispatches nothing.
    assert!(
        !co.reconcile_authorize(&d.effect, 2, &hook)
            .await
            .expect("again")
    );
    assert_eq!(hook.calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_step_key_reused_under_another_tool_name_blocks_the_turn_before_dispatch() {
    let (_, store, co) = rig();
    let (a, _) = leased(&co, "o-1").await;
    let s = spec("charge-card", RecoveryPolicy::AtLeastOnce);
    let hook = Hook::default();
    let d = dispatched(&co, &a, &s, &hook).await;
    co.complete_effect(&d.effect, d.attempt, &json!("paid"))
        .await
        .expect("result");
    assert_eq!(step_output(&co, &a, &s, &hook).await, json!("paid"));

    let renamed = StepSpec {
        tool_name: "refund".to_string(),
        ..s.clone()
    };
    assert_eq!(
        (&renamed.tool_version, renamed.input_hash),
        (&s.tool_version, s.input_hash)
    );
    let err = co
        .run_step(&a.lease, &renamed, &hook)
        .await
        .expect_err("drift");
    assert_eq!(err.kind, ErrorKind::NonDeterministicReplay);
    assert_eq!(hook.calls(), 1, "nothing dispatched");
    let h = head(&store, "o-1").await;
    assert_eq!(
        h.active_turn.expect("turn").block,
        TurnBlock::NonDeterministicReplay
    );
    assert_eq!(h.effects.len(), 1);
}
