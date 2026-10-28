#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use ravel_agents_core::error::ErrorKind;
use ravel_agents_core::head::{Head, ResultStatus};
use ravel_agents_core::identity::{AgentId, agent_id};
use ravel_agents_core::kernel::DerivedIds;
use ravel_agents_core::keys::{AgentKey, AgentKind};
use ravel_agents_core::wire::{
    AgentRef, CreateAgentRequest, HandlerPinBody, OperationStatus, SendRequest, SendResponse,
    TurnAssignment, TurnLease, TurnOutcome, WorkerCommitRequest, WorkerPollRequest,
};
use ravel_agents_store::backoff::NoBackoff;
use ravel_agents_store::counters::PhaseCounts;
use ravel_agents_store::{AgentStore, Clock, HeadState, StoreLimits};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use ravel_types::TenantHash;
use serde_json::{Value, json};
use tokio::time::Instant;

use super::*;

pub(crate) const MS: i64 = 1_000_000;
pub(crate) const T0: i64 = 1_800_000_000_000 * MS;
pub(crate) const LEASE_MS: u64 = 1_000;
pub(crate) const GRACE_MS: u64 = 500;
pub(crate) const MAX_ATTEMPTS: u32 = 3;
pub(crate) const IDLE_MS: u64 = 10_000;
pub(crate) const SWEEP_MS: u64 = 2_000;

pub(crate) struct TestClock(AtomicI64);

impl TestClock {
    pub(crate) fn advance_ms(&self, ms: u64) {
        self.0
            .fetch_add(i64::try_from(ms).expect("ms") * MS, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Counts up from its seed.
pub(crate) struct FixedRng(AtomicU64);

impl Rng for FixedRng {
    fn next_u64(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

pub(crate) fn config() -> RuntimeConfig {
    RuntimeConfig {
        lease_ms: LEASE_MS,
        heartbeat_grace_ms: GRACE_MS,
        max_turn_attempts: MAX_ATTEMPTS,
        idle_unload_after_ms: IDLE_MS,
        poll_wait_cap_ms: DEFAULT_POLL_WAIT_CAP_MS,
    }
}

pub(crate) fn clock() -> Arc<TestClock> {
    Arc::new(TestClock(AtomicI64::new(T0)))
}

/// Variable naming the index page size every rig in this crate runs at,
/// so the whole suite runs again over sealed pages
/// (`RAVEL_AGENTS_PAGE_ENTRIES=2 cargo test -p ravel-agents-runtime`).
pub(crate) const PAGE_ENTRIES_ENV: &str = "RAVEL_AGENTS_PAGE_ENTRIES";

/// The store limits of every rig: the defaults, at the page size
/// [`PAGE_ENTRIES_ENV`] names when it is set. A value that does not parse
/// panics rather than running the default size unnoticed.
pub(crate) fn limits() -> StoreLimits {
    let mut limits = StoreLimits::default();
    if let Ok(raw) = std::env::var(PAGE_ENTRIES_ENV) {
        limits.kernel.max_page_entries = raw
            .trim()
            .parse::<usize>()
            .unwrap_or_else(|e| panic!("{PAGE_ENTRIES_ENV}={raw:?}: {e}"));
    }
    limits
}

pub(crate) fn store_over(
    backend: Arc<dyn ObjectStoreBackend>,
    clock: &Arc<TestClock>,
) -> Arc<AgentStore> {
    store_with(backend, clock, limits())
}

pub(crate) fn store_with(
    backend: Arc<dyn ObjectStoreBackend>,
    clock: &Arc<TestClock>,
    limits: StoreLimits,
) -> Arc<AgentStore> {
    Arc::new(
        AgentStore::new(
            backend,
            TenantHash([7; 16]),
            limits,
            clock.clone(),
            Arc::new(DerivedIds),
        )
        .with_backoff(Arc::new(NoBackoff)),
    )
}

pub(crate) fn coordinator(
    store: &Arc<AgentStore>,
    clock: &Arc<TestClock>,
    seed: u64,
) -> Arc<Coordinator> {
    coordinator_with(
        store,
        clock,
        seed,
        SweepConfig {
            interval_ms: SWEEP_MS,
            ..SweepConfig::default()
        },
    )
}

pub(crate) fn coordinator_with(
    store: &Arc<AgentStore>,
    clock: &Arc<TestClock>,
    seed: u64,
    sweep: SweepConfig,
) -> Arc<Coordinator> {
    Arc::new(
        Coordinator::new(
            store.clone(),
            config(),
            clock.clone(),
            Arc::new(FixedRng(AtomicU64::new(seed))),
        )
        .with_sweep(sweep),
    )
}

/// One coordinator over a fresh `MemoryStore`.
pub(crate) fn rig() -> (Arc<TestClock>, Arc<AgentStore>, Arc<Coordinator>) {
    let clock = clock();
    let store = store_over(Arc::new(MemoryStore::new()), &clock);
    let co = coordinator(&store, &clock, 100);
    (clock, store, co)
}

pub(crate) fn agent_ref(key: &str) -> AgentRef {
    AgentRef {
        kind: "order".to_string(),
        key: key.to_string(),
    }
}

pub(crate) fn id_of(key: &str) -> AgentId {
    agent_id(
        &AgentKind::new("order").expect("kind"),
        &AgentKey::new(key).expect("key"),
    )
}

pub(crate) fn pin(version: &str) -> HandlerPinBody {
    let fill = match version {
        "v1" => 1u8,
        _ => 2u8,
    };
    HandlerPinBody {
        handler: "step".to_string(),
        version: version.to_string(),
        digest: hex::encode([fill; 32]),
    }
}

pub(crate) fn poll_req(worker: &str, version: &str, wait_ms: u32) -> WorkerPollRequest {
    WorkerPollRequest {
        worker_id: worker.to_string(),
        handlers: vec![pin(version)],
        wait_ms,
    }
}

pub(crate) fn worker(co: &Coordinator, version: &str) -> String {
    co.register_worker(&[pin(version)]).expect("register")
}

/// A poll that does not wait.
pub(crate) async fn poll_now(
    co: &Coordinator,
    worker: &str,
    version: &str,
) -> Option<TurnAssignment> {
    co.poll(&poll_req(worker, version, 0), Instant::now())
        .await
        .expect("poll")
}

pub(crate) fn completed(lease: &TurnLease, state: Value) -> WorkerCommitRequest {
    WorkerCommitRequest {
        lease: lease.clone(),
        outcome: TurnOutcome::Completed {
            state,
            output: Value::Null,
            events: Vec::new(),
            timers: Vec::new(),
            intents: Vec::new(),
        },
    }
}

pub(crate) fn failed(lease: &TurnLease) -> WorkerCommitRequest {
    WorkerCommitRequest {
        lease: lease.clone(),
        outcome: TurnOutcome::Failed {
            error: "boom".to_string(),
            terminal: false,
        },
    }
}

pub(crate) async fn create(co: &Coordinator, key: &str) {
    co.create(&CreateAgentRequest {
        agent: agent_ref(key),
    })
    .await
    .expect("create");
}

pub(crate) async fn send(
    co: &Coordinator,
    key: &str,
    request_id: &str,
    payload: Value,
) -> SendResponse {
    co.send(&SendRequest {
        agent: agent_ref(key),
        handler: "step".to_string(),
        request_id: request_id.to_string(),
        payload,
    })
    .await
    .expect("send")
}

pub(crate) async fn head(store: &AgentStore, key: &str) -> Head {
    match store.read_head_full(&id_of(key)).await.expect("read head") {
        HeadState::Present { head, .. } => *head,
        other => panic!("no head: {other:?}"),
    }
}

pub(crate) fn past_lease_and_grace() -> u64 {
    LEASE_MS + GRACE_MS + 1
}

#[tokio::test(start_paused = true)]
async fn worker_paused_past_lease_cannot_commit_after_takeover_epoch_advanced() {
    let clock = clock();
    let store = store_over(Arc::new(MemoryStore::new()), &clock);
    let c1 = coordinator(&store, &clock, 100);
    let c2 = coordinator(&store, &clock, 200);
    create(&c1, "o-1").await;
    let sent = send(&c1, "o-1", "r-1", json!({"n": 1})).await;
    let a = worker(&c1, "v1");
    let b = worker(&c2, "v1");

    let leased = poll_now(&c1, &a, "v1").await.expect("A is leased");
    assert_eq!(leased.accept_seq, sent.accept_seq);
    assert_eq!(leased.lease.owner_id, a);

    // A pauses past its lease and the grace; B takes over through another
    // coordinator that never saw A's lease.
    clock.advance_ms(past_lease_and_grace());
    let before = store.counters();
    let base = head(&store, "o-1").await;
    let reissued = poll_now(&c2, &b, "v1").await.expect("B is leased");
    assert_eq!(reissued.lease.turn_id, leased.lease.turn_id);
    assert_eq!(reissued.accept_seq, leased.accept_seq);
    assert_eq!(reissued.payload, leased.payload);
    assert_eq!(reissued.lease.owner_id, b);
    assert_eq!(reissued.lease.epoch, leased.lease.epoch + 1);
    assert_eq!(c2.metrics().takeovers, 1);

    let refused = c1
        .commit(&completed(&leased.lease, json!({"by": "a"})))
        .await
        .expect_err("A's commit after the takeover");
    assert_eq!(refused.kind, ErrorKind::OwnershipLost);
    let moved = store.counters().since(&before);
    assert_eq!(moved.publish.cas, 1, "only B's takeover CAS");
    assert_eq!(moved.publish.put, 0);
    assert_eq!(head(&store, "o-1").await.state_version, base.state_version);
    assert_eq!(c1.metrics().ownership_lost_refusals, 1);
    assert_eq!(c1.metrics().turns_committed, 0);

    let committed = c2
        .commit(&completed(&reissued.lease, json!({"by": "b"})))
        .await
        .expect("B commits");
    assert_eq!(committed.status, OperationStatus::Completed);
    assert_eq!(committed.state_version, base.state_version + 1);
    assert_eq!(c2.metrics().turns_committed, 1);
    let after = head(&store, "o-1").await;
    let results: Vec<_> = after
        .results
        .iter()
        .filter(|r| r.accept_seq == sent.accept_seq)
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ResultStatus::Completed);
    let state = store
        .read_object(&id_of("o-1"), after.state.expect("state"))
        .await
        .expect("state object");
    assert_eq!(
        serde_json::from_slice::<Value>(&state).expect("json"),
        json!({"by": "b"})
    );
}

#[tokio::test(start_paused = true)]
async fn competing_sends_dispatch_in_accept_seq_order() {
    let (_clock, _store, co) = rig();
    create(&co, "o-1").await;
    let mut tasks = Vec::new();
    for t in 0..4 {
        let co = co.clone();
        tasks.push(tokio::spawn(async move {
            let mut sent = Vec::new();
            for i in 0..5 {
                let payload = json!({"t": t, "i": i});
                let r = send(&co, "o-1", &format!("t{t}-{i}"), payload.clone()).await;
                sent.push((r.accept_seq, payload));
            }
            sent
        }));
    }
    let mut by_seq = BTreeMap::new();
    for task in tasks {
        for (seq, payload) in task.await.expect("join") {
            assert!(
                by_seq.insert(seq, payload).is_none(),
                "accept_seq {seq} twice"
            );
        }
    }
    let expected: Vec<u64> = (1..=20).collect();
    assert_eq!(by_seq.keys().copied().collect::<Vec<_>>(), expected);

    let w = worker(&co, "v1");
    let mut order = Vec::new();
    for _ in 0..20 {
        let a = poll_now(&co, &w, "v1").await.expect("assignment");
        assert_eq!(a.payload, by_seq[&a.accept_seq]);
        order.push(a.accept_seq);
        co.commit(&completed(&a.lease, json!({"last": a.accept_seq})))
            .await
            .expect("commit");
    }
    assert_eq!(order, expected);
    assert!(poll_now(&co, &w, "v1").await.is_none());
    assert_eq!(co.metrics().turns_committed, 20);
}

#[tokio::test(start_paused = true)]
async fn across_agents_the_earliest_accepted_input_is_offered_first() {
    let (clock, _store, co) = rig();
    create(&co, "x").await;
    create(&co, "y").await;
    send(&co, "y", "r-y", json!("y")).await;
    clock.advance_ms(1);
    send(&co, "x", "r-x", json!("x")).await;
    let w = worker(&co, "v1");
    let first = poll_now(&co, &w, "v1").await.expect("first");
    assert_eq!(first.agent.key, "y");
    co.commit(&completed(&first.lease, json!(1)))
        .await
        .expect("commit");
    let second = poll_now(&co, &w, "v1").await.expect("second");
    assert_eq!(second.agent.key, "x");
}

#[tokio::test(start_paused = true)]
async fn completion_racing_an_enqueue_rebases_and_keeps_the_new_input() {
    let clock = clock();
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store = store_over(fault.clone(), &clock);
    let co = coordinator(&store, &clock, 100);
    create(&co, "o-1").await;
    send(&co, "o-1", "r-1", json!(1)).await;
    let w = worker(&co, "v1");
    let leased = poll_now(&co, &w, "v1").await.expect("leased");

    let gate = fault.hold(Op::Put, Some("/head".to_string()), Occurrence::Nth(1));
    let before = store.counters();
    let committing = {
        let co = co.clone();
        let lease = leased.lease.clone();
        tokio::spawn(async move { co.commit(&completed(&lease, json!({"done": 1}))).await })
    };
    gate.wait_until_held(1).await;
    let second = send(&co, "o-1", "r-2", json!(2)).await;
    for id in gate.held() {
        gate.release(id);
    }
    let committed = committing.await.expect("join").expect("commit");
    assert!(committed.rebased);
    assert_eq!(committed.status, OperationStatus::Completed);
    assert_eq!(store.counters().since(&before).publish.conflict, 1);

    let after = head(&store, "o-1").await;
    assert_eq!(after.inbox.len(), 1);
    assert_eq!(after.inbox[0].accept_seq, second.accept_seq);
    let next = poll_now(&co, &w, "v1").await.expect("next");
    assert_eq!(next.accept_seq, second.accept_seq);
}

#[tokio::test(start_paused = true)]
async fn cold_restart_resumes_the_active_turn_from_head_alone() {
    let clock = clock();
    let backend = Arc::new(MemoryStore::new());
    let store1 = store_over(backend.clone(), &clock);
    let co1 = coordinator(&store1, &clock, 100);
    create(&co1, "o-1").await;
    for i in 1..=3 {
        send(&co1, "o-1", &format!("r-{i}"), json!({"i": i})).await;
    }
    let w1 = worker(&co1, "v1");
    let first = poll_now(&co1, &w1, "v1").await.expect("first");
    assert_eq!(first.accept_seq, 1);
    co1.commit(&completed(&first.lease, json!({"done": [1]})))
        .await
        .expect("commit 1");
    let active = poll_now(&co1, &w1, "v1").await.expect("second");
    assert_eq!(active.accept_seq, 2);
    drop(co1);
    drop(store1);

    let store2 = store_over(backend, &clock);
    let co2 = coordinator(&store2, &clock, 200);
    assert_eq!(co2.metrics().agents_loaded, 0);
    clock.advance_ms(past_lease_and_grace());
    let w2 = worker(&co2, "v1");
    let resumed = poll_now(&co2, &w2, "v1").await.expect("resumed");
    assert_eq!(resumed.lease.turn_id, active.lease.turn_id);
    assert_eq!(resumed.accept_seq, active.accept_seq);
    assert_eq!(resumed.payload, json!({"i": 2}));
    assert_eq!(resumed.state, json!({"done": [1]}));
    assert_eq!(co2.metrics().agents_loaded, 1);
    co2.commit(&completed(&resumed.lease, json!({"done": [1, 2]})))
        .await
        .expect("commit 2");
    let third = poll_now(&co2, &w2, "v1").await.expect("third");
    assert_eq!(third.accept_seq, 3);
    co2.commit(&completed(&third.lease, json!({"done": [1, 2, 3]})))
        .await
        .expect("commit 3");
    assert!(poll_now(&co2, &w2, "v1").await.is_none());

    let after = head(&store2, "o-1").await;
    let done: Vec<_> = after
        .results
        .iter()
        .map(|r| (r.accept_seq, r.status))
        .collect();
    assert_eq!(
        done,
        vec![
            (1, ResultStatus::Completed),
            (2, ResultStatus::Completed),
            (3, ResultStatus::Completed)
        ]
    );
    assert_eq!(after.state_version, 3);
}

#[tokio::test(start_paused = true)]
async fn a_takeover_resumes_a_turn_whose_input_is_on_a_sealed_inbox_page() {
    let clock = clock();
    let backend = Arc::new(MemoryStore::new());
    let mut small = limits();
    small.kernel.max_page_entries = 2;
    let store1 = store_with(backend.clone(), &clock, small);
    let co1 = coordinator(&store1, &clock, 100);
    create(&co1, "o-1").await;
    for i in 1..=7 {
        send(&co1, "o-1", &format!("r-{i}"), json!({"i": i})).await;
    }
    let w1 = worker(&co1, "v1");
    let active = poll_now(&co1, &w1, "v1").await.expect("first");
    assert_eq!(active.accept_seq, 1);
    let sealed = head(&store1, "o-1").await;
    assert!(
        sealed
            .index
            .inbox
            .page_for(active.accept_seq)
            .is_some(),
        "the active input sits on a sealed page"
    );
    drop(co1);
    drop(store1);

    let store2 = store_with(backend, &clock, small);
    let co2 = coordinator(&store2, &clock, 200);
    clock.advance_ms(past_lease_and_grace());
    let w2 = worker(&co2, "v1");
    let resumed = poll_now(&co2, &w2, "v1").await.expect("resumed");
    assert_eq!(resumed.lease.turn_id, active.lease.turn_id);
    assert_eq!(resumed.accept_seq, active.accept_seq);
    assert_eq!(resumed.payload, json!({"i": 1}));
    assert_eq!(co2.metrics().takeovers, 1);
    co2.commit(&completed(&resumed.lease, json!({"done": 1})))
        .await
        .expect("commit");
    let next = poll_now(&co2, &w2, "v1").await.expect("next");
    assert_eq!(next.accept_seq, 2);
}

#[tokio::test(start_paused = true)]
async fn incompatible_handler_is_refused_before_any_write() {
    let (clock, store, co) = rig();
    create(&co, "o-1").await;
    send(&co, "o-1", "r-1", json!(1)).await;
    let w1 = worker(&co, "v1");
    poll_now(&co, &w1, "v1").await.expect("turn at v1");
    // A takeover would be due, so only the pin check stands between the v2
    // poller and a write.
    clock.advance_ms(past_lease_and_grace());
    let w2 = worker(&co, "v2");
    let before = store.counters();
    let refused = co
        .poll(&poll_req(&w2, "v2", 0), Instant::now())
        .await
        .expect_err("v2 against a v1 turn");
    assert_eq!(refused.kind, ErrorKind::IncompatibleHandler);
    assert_eq!(
        store.counters().since(&before).publish,
        PhaseCounts::default()
    );
    assert_eq!(co.metrics().incompatible_handler_refusals, 1);
    assert_eq!(co.metrics().takeovers, 1, "only w1's activation");
}

#[tokio::test(start_paused = true)]
async fn failing_max_attempts_dead_letters_the_input() {
    let (_clock, store, co) = rig();
    create(&co, "o-1").await;
    let sent = send(&co, "o-1", "r-1", json!(1)).await;
    let base = head(&store, "o-1").await.state_version;
    let w = worker(&co, "v1");
    let mut statuses = Vec::new();
    let mut turn_ids = Vec::new();
    let mut fail_cas = 0;
    for _ in 0..MAX_ATTEMPTS {
        let a = poll_now(&co, &w, "v1").await.expect("attempt");
        assert_eq!(a.accept_seq, sent.accept_seq);
        turn_ids.push(a.lease.turn_id.clone());
        let before = store.counters();
        let r = co.commit(&failed(&a.lease)).await.expect("fail");
        fail_cas += store.counters().since(&before).publish.cas;
        statuses.push(r.status);
    }
    assert_eq!(
        statuses,
        vec![
            OperationStatus::Accepted,
            OperationStatus::Accepted,
            OperationStatus::Failed
        ]
    );
    turn_ids.dedup();
    assert_eq!(turn_ids.len(), 3, "each attempt is a new turn");
    assert_eq!(fail_cas, u64::from(MAX_ATTEMPTS));
    let m = co.metrics();
    assert_eq!(m.turns_failed, u64::from(MAX_ATTEMPTS));
    assert_eq!(m.turns_dead_lettered, 1);
    assert_eq!(m.turns_committed, 0);
    let after = head(&store, "o-1").await;
    assert_eq!(after.state_version, base);
    assert!(after.inbox.is_empty());
    let result = after
        .results
        .iter()
        .find(|r| r.accept_seq == sent.accept_seq)
        .expect("result");
    assert_eq!(result.status, ResultStatus::Failed);
    assert!(result.dead_lettered);
    assert!(poll_now(&co, &w, "v1").await.is_none());
}

#[tokio::test(start_paused = true)]
async fn one_failure_short_of_the_bound_then_success_completes_the_input() {
    let (_clock, store, co) = rig();
    create(&co, "o-1").await;
    let sent = send(&co, "o-1", "r-1", json!(1)).await;
    let w = worker(&co, "v1");
    for attempt in 1..MAX_ATTEMPTS {
        let a = poll_now(&co, &w, "v1").await.expect("attempt");
        let verdict = co.fail(&a.lease, "boom", false).await.expect("fail");
        assert_eq!(
            verdict,
            FailOutcome::Retry {
                accept_seq: sent.accept_seq,
                failed_attempts: attempt
            }
        );
    }
    let last = poll_now(&co, &w, "v1").await.expect("last attempt");
    let r = co
        .commit(&completed(&last.lease, json!("ok")))
        .await
        .expect("commit");
    assert_eq!(r.status, OperationStatus::Completed);
    let m = co.metrics();
    assert_eq!(m.turns_failed, u64::from(MAX_ATTEMPTS - 1));
    assert_eq!(m.turns_dead_lettered, 0);
    assert_eq!(m.turns_committed, 1);
    let after = head(&store, "o-1").await;
    assert!(after.inbox.is_empty());
    assert_eq!(after.state_version, 1);
}

/// A poll whose `nth` HEAD write loses its acknowledgement. HEAD writes:
/// create 1, enqueue 2, takeover 3, begin 4.
async fn poll_with_lost_head_ack(nth: u64) {
    let clock = clock();
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Nth(nth)),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let co = coordinator(&store, &clock, 100);
    create(&co, "o-1").await;
    let sent = send(&co, "o-1", "r-1", json!(1)).await;
    let w = worker(&co, "v1");
    let before = store.counters();
    let a = poll_now(&co, &w, "v1")
        .await
        .expect("leased despite the lost ack");
    assert_eq!(fault.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_eq!(a.accept_seq, sent.accept_seq);
    assert_eq!((a.lease.owner_id.as_str(), a.lease.epoch), (w.as_str(), 1));
    assert_eq!(store.counters().since(&before).publish.cas, 2);
    let m = co.metrics();
    assert_eq!(m.outcome_unknown_resolved, 1);
    assert_eq!(m.takeovers, 1);
    let h = head(&store, "o-1").await;
    let turn = h.active_turn.expect("active turn");
    assert_eq!(turn.turn_id.to_hex(), a.lease.turn_id);
    co.commit(&completed(&a.lease, json!("ok")))
        .await
        .expect("commit under the settled lease");
}

#[tokio::test(start_paused = true)]
async fn takeover_with_a_lost_acknowledgement_is_settled_from_head() {
    poll_with_lost_head_ack(3).await;
}

#[tokio::test(start_paused = true)]
async fn begin_turn_with_a_lost_acknowledgement_resumes_without_a_second_cas() {
    poll_with_lost_head_ack(4).await;
}

#[tokio::test(start_paused = true)]
async fn lost_acknowledgement_is_resolved_without_a_second_cas() {
    let clock = clock();
    // HEAD writes: create 1, enqueue 2, takeover 3, begin 4, commit 5.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Nth(5)),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let co = coordinator(&store, &clock, 100);
    create(&co, "o-1").await;
    let sent = send(&co, "o-1", "r-1", json!(1)).await;
    let w = worker(&co, "v1");
    let a = poll_now(&co, &w, "v1").await.expect("leased");
    let before = store.counters();
    let r = co
        .commit(&completed(&a.lease, json!("ok")))
        .await
        .expect("commit settles");
    assert_eq!(fault.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_eq!(r.status, OperationStatus::Completed);
    assert_eq!(r.state_version, 1);
    assert_eq!(store.counters().since(&before).publish.cas, 1);
    let m = co.metrics();
    assert_eq!(m.outcome_unknown_resolved, 1);
    assert_eq!(m.turns_committed, 1);
    let after = head(&store, "o-1").await;
    assert_eq!(
        after
            .results
            .iter()
            .filter(|r| r.accept_seq == sent.accept_seq)
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn idle_unload_writes_nothing_and_send_reloads_from_head() {
    let (clock, store, co) = rig();
    create(&co, "o-1").await;
    send(&co, "o-1", "r-1", json!(1)).await;
    let w = worker(&co, "v1");
    let a = poll_now(&co, &w, "v1").await.expect("leased");
    assert_eq!(co.metrics().turns_active, 1);
    co.commit(&completed(&a.lease, json!("s1")))
        .await
        .expect("commit");
    assert_eq!(co.metrics().turns_active, 0);
    let loaded = co.metrics().agents_loaded;
    assert_eq!(loaded, 1);
    let prior = co.cached(&id_of("o-1")).expect("loaded");

    clock.advance_ms(IDLE_MS);
    let before = store.counters();
    assert_eq!(co.unload_idle(), 1);
    assert_eq!(
        store.counters().since(&before).total(),
        PhaseCounts::default()
    );
    assert_eq!(co.metrics().agents_loaded, loaded - 1);
    assert!(co.cached(&id_of("o-1")).is_none());

    send(&co, "o-1", "r-2", json!(2)).await;
    assert_eq!(co.metrics().agents_loaded, loaded);
    let reloaded = co.cached(&id_of("o-1")).expect("reloaded");
    assert_eq!(reloaded.incarnation, prior.incarnation);
    assert_eq!(reloaded.state_version, prior.state_version);
    let next = poll_now(&co, &w, "v1").await.expect("next");
    assert_eq!(next.state, json!("s1"));
}

#[tokio::test(start_paused = true)]
async fn heartbeat_renews_inside_the_lease_and_is_refused_after_takeover() {
    let (clock, store, co) = rig();
    create(&co, "o-1").await;
    send(&co, "o-1", "r-1", json!(1)).await;
    let a = worker(&co, "v1");
    let b = worker(&co, "v1");
    let la = poll_now(&co, &a, "v1").await.expect("A leased");

    clock.advance_ms(LEASE_MS / 2);
    let before = store.counters();
    let expires = co.heartbeat(&la.lease).await.expect("renewed");
    assert_eq!(
        expires,
        clock.now_unix_ns() + i64::try_from(LEASE_MS).expect("ms") * MS
    );
    assert_eq!(
        store.counters().since(&before).publish,
        PhaseCounts::default()
    );

    // The renewal moved the takeover point: B is not leased before it.
    clock.advance_ms(LEASE_MS);
    assert!(poll_now(&co, &b, "v1").await.is_none());
    clock.advance_ms(GRACE_MS + 1);
    let lb = poll_now(&co, &b, "v1").await.expect("B leased");
    assert_eq!(lb.lease.turn_id, la.lease.turn_id);

    let before = store.counters();
    let lost = co
        .heartbeat(&la.lease)
        .await
        .expect_err("A after the takeover");
    assert_eq!(lost.kind, ErrorKind::OwnershipLost);
    assert_eq!(
        store.counters().since(&before).publish,
        PhaseCounts::default()
    );
    assert_eq!(co.metrics().ownership_lost_refusals, 1);
    co.heartbeat(&lb.lease).await.expect("B renews");
}

#[tokio::test(start_paused = true)]
async fn a_waiting_poll_wakes_on_send_and_the_wait_is_capped() {
    let (_clock, store, co) = rig();
    create(&co, "o-1").await;
    let w = worker(&co, "v1");
    let req = poll_req(&w, "v1", 5_000);
    let (polled, sent) = tokio::join!(
        co.poll(&req, Instant::now() + Duration::from_secs(60)),
        async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            send(&co, "o-1", "r-1", json!(1)).await
        }
    );
    let polled = polled.expect("poll").expect("woken with work");
    assert_eq!(polled.accept_seq, sent.accept_seq);
    assert_eq!(co.metrics().polls_waited, 1);

    co.commit(&completed(&polled.lease, json!(1)))
        .await
        .expect("commit");
    let start = Instant::now();
    let none = co
        .poll(
            &poll_req(&w, "v1", 60_000),
            start + Duration::from_secs(120),
        )
        .await
        .expect("poll");
    assert!(none.is_none());
    // hygiene-allow: wall-clock -- `Instant` is tokio's paused clock, so this
    // is the exact virtual wait, not real time.
    assert_eq!(
        start.elapsed(),
        Duration::from_millis(DEFAULT_POLL_WAIT_CAP_MS)
    );
    let m = co.metrics();
    assert_eq!(m.polls_waited, 2);
    assert_eq!(m.store, store.counters());
}

#[tokio::test(start_paused = true)]
async fn activation_and_admin_takeover_are_one_cas_each() {
    let (_clock, store, co) = rig();
    create(&co, "o-1").await;
    let before = store.counters();
    assert_eq!(
        co.activate(&agent_ref("o-1"), "w-a")
            .await
            .expect("activate"),
        1
    );
    assert_eq!(store.counters().since(&before).publish.cas, 1);
    let before = store.counters();
    assert_eq!(
        co.activate(&agent_ref("o-1"), "w-a").await.expect("again"),
        1
    );
    assert_eq!(store.counters().since(&before).publish.cas, 0);
    let before = store.counters();
    let epoch = co
        .takeover(&agent_ref("o-1"), "w-b", TakeoverReason::Admin)
        .await
        .expect("takeover");
    assert_eq!(epoch, 2);
    assert_eq!(store.counters().since(&before).publish.cas, 1);
    assert_eq!(co.metrics().takeovers, 2);
    let h = head(&store, "o-1").await;
    assert_eq!((h.owner_id.as_str(), h.epoch), ("w-b", 2));
}

#[tokio::test(start_paused = true)]
async fn a_stale_lease_of_the_same_owner_cannot_commit_after_an_admin_takeover() {
    let (_clock, store, co) = rig();
    create(&co, "o-1").await;
    let sent = send(&co, "o-1", "r-1", json!(1)).await;
    let w = worker(&co, "v1");
    let l1 = poll_now(&co, &w, "v1").await.expect("leased");
    let base = head(&store, "o-1").await;

    let before = store.counters();
    let epoch = co
        .takeover(&agent_ref("o-1"), &w, TakeoverReason::Admin)
        .await
        .expect("takeover by the current owner");
    assert_eq!(epoch, l1.lease.epoch + 1);
    let refused = co
        .commit(&completed(&l1.lease, json!("stale")))
        .await
        .expect_err("the lease from the older epoch");
    assert_eq!(refused.kind, ErrorKind::OwnershipLost);
    let moved = store.counters().since(&before);
    assert_eq!(moved.publish.cas, 1, "only the takeover's CAS");
    assert_eq!(moved.publish.put, 0);
    let mid = head(&store, "o-1").await;
    assert_eq!(mid.state_version, base.state_version);
    assert_eq!((mid.owner_id.as_str(), mid.epoch), (w.as_str(), epoch));
    assert_eq!(
        mid.active_turn.expect("turn kept").turn_id.to_hex(),
        l1.lease.turn_id
    );
    assert_eq!(co.metrics().turns_committed, 0);
    assert_eq!(co.metrics().ownership_lost_refusals, 1);

    let l2 = poll_now(&co, &w, "v1").await.expect("re-leased");
    assert_eq!(l2.lease.epoch, epoch);
    assert_eq!(l2.lease.turn_id, l1.lease.turn_id);
    assert_eq!(l2.accept_seq, sent.accept_seq);
    let committed = co
        .commit(&completed(&l2.lease, json!("fresh")))
        .await
        .expect("commit under the new epoch");
    assert_eq!(committed.status, OperationStatus::Completed);
    assert_eq!(committed.state_version, base.state_version + 1);
    assert_eq!(co.metrics().turns_committed, 1);
}

#[tokio::test(start_paused = true)]
async fn a_lost_commit_ack_after_a_takeover_is_ownership_lost_not_committed() {
    let clock = clock();
    // HEAD writes in the order the plan sees them: create 1, enqueue 2, W1's
    // takeover 3, W1's begin 4, W2's takeover 5, W2's commit 6, then W1's
    // commit 7, held past W2's and released into a Transient that does not
    // apply.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Transient("ack lost".to_string()))
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Nth(7)),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let c1 = coordinator(&store, &clock, 100);
    let c2 = coordinator(&store, &clock, 200);
    create(&c1, "o-1").await;
    let sent = send(&c1, "o-1", "r-1", json!(1)).await;
    let w1 = worker(&c1, "v1");
    let w2 = worker(&c2, "v1");
    let l1 = poll_now(&c1, &w1, "v1").await.expect("W1 leased");

    let gate = fault.hold(Op::Put, Some("/head".to_string()), Occurrence::Nth(1));
    let committing = {
        let c1 = c1.clone();
        let lease = l1.lease.clone();
        tokio::spawn(async move { c1.commit(&completed(&lease, json!({"by": "w1"}))).await })
    };
    gate.wait_until_held(1).await;
    clock.advance_ms(past_lease_and_grace());
    let l2 = poll_now(&c2, &w2, "v1").await.expect("W2 leased");
    assert_eq!(l2.lease.turn_id, l1.lease.turn_id);
    assert_eq!(l2.lease.epoch, l1.lease.epoch + 1);
    c2.commit(&completed(&l2.lease, json!({"by": "w2"})))
        .await
        .expect("W2 commits");
    for id in gate.held() {
        gate.release(id);
    }
    let lost = committing
        .await
        .expect("join")
        .expect_err("W1's commit after W2's");
    assert_eq!(lost.kind, ErrorKind::OwnershipLost);
    assert_eq!(fault.fault_count(Op::Put, FaultKind::Transient), 1);
    let m1 = c1.metrics();
    assert_eq!(m1.turns_committed, 0);
    assert_eq!(m1.ownership_lost_refusals, 1);
    assert_eq!(m1.outcome_unknown_resolved, 0);
    assert_eq!(m1.turns_committed + c2.metrics().turns_committed, 1);

    let after = head(&store, "o-1").await;
    assert_eq!(
        (after.owner_id.as_str(), after.epoch),
        (w2.as_str(), l2.lease.epoch)
    );
    let results: Vec<_> = after
        .results
        .iter()
        .filter(|r| r.accept_seq == sent.accept_seq)
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ResultStatus::Completed);
    let state = store
        .read_object(&id_of("o-1"), after.state.expect("state"))
        .await
        .expect("state object");
    assert_eq!(
        serde_json::from_slice::<Value>(&state).expect("json"),
        json!({"by": "w2"})
    );
}

#[tokio::test(start_paused = true)]
async fn a_lost_takeover_ack_with_another_owner_in_head_is_ownership_lost() {
    let clock = clock();
    // HEAD writes in the order the plan sees them: create 1, enqueue 2, W2's
    // takeover 3, W2's begin 4, then W1's takeover 5, held past W2's and
    // released into a Transient that does not apply.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Transient("ack lost".to_string()))
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Nth(5)),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let c1 = coordinator(&store, &clock, 100);
    let c2 = coordinator(&store, &clock, 200);
    create(&c1, "o-1").await;
    send(&c1, "o-1", "r-1", json!(1)).await;
    let w1 = worker(&c1, "v1");
    let w2 = worker(&c2, "v1");

    let gate = fault.hold(Op::Put, Some("/head".to_string()), Occurrence::Nth(1));
    let polling = {
        let c1 = c1.clone();
        let w1 = w1.clone();
        tokio::spawn(async move { c1.poll(&poll_req(&w1, "v1", 0), Instant::now()).await })
    };
    gate.wait_until_held(1).await;
    let l2 = poll_now(&c2, &w2, "v1").await.expect("W2 leased");
    for id in gate.held() {
        gate.release(id);
    }
    let polled = polling.await.expect("join");
    assert!(
        matches!(polled, Ok(None)),
        "W1's poll after losing the takeover: {polled:?}"
    );
    assert_eq!(fault.fault_count(Op::Put, FaultKind::Transient), 1);
    let m1 = c1.metrics();
    assert_eq!(m1.takeovers, 0);
    assert_eq!(m1.outcome_unknown_resolved, 0);
    let after = head(&store, "o-1").await;
    assert_eq!(
        (after.owner_id.as_str(), after.epoch),
        (w2.as_str(), l2.lease.epoch)
    );
}

#[tokio::test(start_paused = true)]
async fn a_lost_takeover_write_that_did_not_apply_is_store_unavailable() {
    let clock = clock();
    // HEAD writes: create 1, enqueue 2, takeover 3.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Transient("ack lost".to_string()))
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Nth(3)),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let co = coordinator(&store, &clock, 100);
    create(&co, "o-1").await;
    send(&co, "o-1", "r-1", json!(1)).await;
    let w = worker(&co, "v1");
    let refused = co
        .poll(&poll_req(&w, "v1", 0), Instant::now())
        .await
        .expect_err("the takeover write did not apply");
    assert_eq!(refused.kind, ErrorKind::StoreUnavailable);
    assert_eq!(fault.fault_count(Op::Put, FaultKind::Transient), 1);
    let h = head(&store, "o-1").await;
    assert_eq!((h.owner_id.as_str(), h.epoch), ("", 0));
    assert_eq!(co.metrics().outcome_unknown_resolved, 0);
    let a = poll_now(&co, &w, "v1")
        .await
        .expect("leased on the next poll");
    assert_eq!(a.lease.epoch, 1);
}

#[tokio::test(start_paused = true)]
async fn a_takeover_race_does_not_consume_the_begin_turn_settle() {
    let clock = clock();
    // HEAD writes in the order the plan sees them: create, enqueue, W2's
    // activation, W1's first takeover (held past W2's, then a Transient that
    // does not apply), W1's second takeover, W1's begin (applied, ack lost).
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Put)
            .with_key_contains("/head")
            .then_passthrough()
            .then_passthrough()
            .then_passthrough()
            .then_fault(ScriptedFault::Transient("ack lost".to_string()))
            .then_passthrough()
            .then_fault(ScriptedFault::DuplicateDelivery),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let c1 = coordinator(&store, &clock, 100);
    let c2 = coordinator(&store, &clock, 200);
    create(&c1, "o-1").await;
    let sent = send(&c1, "o-1", "r-1", json!(1)).await;
    let w1 = worker(&c1, "v1");

    let gate = fault.hold(Op::Put, Some("/head".to_string()), Occurrence::Nth(1));
    let polling = {
        let c1 = c1.clone();
        let w1 = w1.clone();
        tokio::spawn(async move { c1.poll(&poll_req(&w1, "v1", 0), Instant::now()).await })
    };
    gate.wait_until_held(1).await;
    assert_eq!(
        c2.activate(&agent_ref("o-1"), "w-other")
            .await
            .expect("activate"),
        1
    );
    for id in gate.held() {
        gate.release(id);
    }
    let leased = polling
        .await
        .expect("join")
        .expect("poll")
        .expect("W1 leased after both settles");
    assert_eq!(fault.fault_count(Op::Put, FaultKind::Transient), 1);
    assert_eq!(fault.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_eq!(leased.accept_seq, sent.accept_seq);
    assert_eq!(
        (leased.lease.owner_id.as_str(), leased.lease.epoch),
        (w1.as_str(), 2)
    );
    let m1 = c1.metrics();
    assert_eq!(m1.takeovers, 1);
    assert_eq!(m1.outcome_unknown_resolved, 1, "only the BeginTurn settle");
    let h = head(&store, "o-1").await;
    assert_eq!(
        h.active_turn.expect("active turn").turn_id.to_hex(),
        leased.lease.turn_id
    );
    c1.commit(&completed(&leased.lease, json!("ok")))
        .await
        .expect("commit under the settled lease");
}

#[tokio::test(start_paused = true)]
async fn a_poll_needs_no_registration_on_this_coordinator() {
    let clock = clock();
    let store = store_over(Arc::new(MemoryStore::new()), &clock);
    let c1 = coordinator(&store, &clock, 100);
    let c2 = coordinator(&store, &clock, 200);
    create(&c1, "o-1").await;
    let sent = send(&c1, "o-1", "r-1", json!(1)).await;
    let w = worker(&c1, "v1");
    let leased = poll_now(&c2, &w, "v1")
        .await
        .expect("served on the poll's own pins");
    assert_eq!(leased.accept_seq, sent.accept_seq);
    assert_eq!(leased.lease.owner_id, w);

    let malformed = HandlerPinBody {
        digest: "not hex".to_string(),
        ..pin("v1")
    };
    let refused = c1
        .register_worker(&[malformed])
        .expect_err("a malformed pin");
    assert_eq!(refused.kind, ErrorKind::IncompatibleHandler);
}

#[tokio::test(start_paused = true)]
async fn a_poll_finds_an_agent_that_got_work_through_another_coordinator() {
    // c2's first poll sweeps an empty tenant; the agent is then created and
    // sent an input through c1, and c2's next poll past the sweep interval
    // finds it through the sweep's directory half.
    let clock = clock();
    let store = store_over(Arc::new(MemoryStore::new()), &clock);
    let c1 = coordinator(&store, &clock, 100);
    let c2 = coordinator(&store, &clock, 200);
    let w = worker(&c2, "v1");
    assert!(poll_now(&c2, &w, "v1").await.is_none());
    let lists_before = store.counters().resolve.list;

    create(&c1, "o-1").await;
    let sent = send(&c1, "o-1", "r-1", json!(1)).await;
    clock.advance_ms(SWEEP_MS);
    let leased = poll_now(&c2, &w, "v1")
        .await
        .expect("the second poll sweeps again and finds the input");
    assert_eq!(leased.accept_seq, sent.accept_seq);
    assert_eq!(leased.lease.owner_id, w);
    assert_eq!(
        store.counters().resolve.list - lists_before,
        2,
        "one wake page and one directory page"
    );
    assert_eq!(c2.metrics().agents_loaded, 1);
}

#[tokio::test(start_paused = true)]
async fn pollers_woken_together_share_one_sweep_pass() {
    // Three dormant agents known only from the directory; two workers wait
    // on long polls; one wake past the sweep interval releases both. The
    // pass's first LIST is held at a gate so the second poller runs its
    // whole loop while the first is inside the pass: it must not start a
    // pass of its own.
    let clock = clock();
    let fa = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store = store_over(fa.clone(), &clock);
    let other = coordinator(&store, &clock, 100);
    let co = coordinator(&store, &clock, 200);
    for key in ["o-1", "o-2", "o-3"] {
        create(&other, key).await;
    }
    let wa = worker(&co, "v1");
    let wb = worker(&co, "v1");
    assert!(poll_now(&co, &wa, "v1").await.is_none(), "the first pass");
    // The next LIST is the pass's wake page; hold it (occurrences count
    // from the hold).
    let gate = fa.hold(Op::List, None, Occurrence::Nth(1));
    let before = store.counters();
    let deadline = Instant::now() + Duration::from_millis(1_000);
    let req_a = poll_req(&wa, "v1", 1_000);
    let req_b = poll_req(&wb, "v1", 1_000);
    let pa = co.poll(&req_a, deadline);
    let pb = co.poll(&req_b, deadline);
    let wake = async {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        clock.advance_ms(SWEEP_MS);
        co.wake.notify_waiters();
        gate.wait_until_held(1).await;
        // The other poller runs its loop against the held pass.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        for id in gate.held() {
            gate.release(id);
        }
    };
    let (ra, rb, ()) = tokio::join!(pa, pb, wake);
    assert!(ra.expect("poll a").is_none());
    assert!(rb.expect("poll b").is_none());
    let delta = store.counters().since(&before);
    assert_eq!(
        delta.resolve.list, 2,
        "one shared pass: a wake and a dir page"
    );
    assert_eq!(delta.resolve.get, 3, "three dormant HEADs read once");
    assert_eq!(co.sweep_metrics().passes, 2);
}

#[tokio::test(start_paused = true)]
async fn a_poll_issues_no_unbounded_list() {
    // Ten dormant agents over two-key pages, known only from the directory,
    // and a sweep capped at four LISTs and four HEAD reads: a poll lists and
    // reads no more than the caps however large the directory is, polls
    // inside the interval list nothing, and the next pass resumes where the
    // last one stopped.
    let clock = clock();
    let store = store_over(Arc::new(MemoryStore::with_page_size(2)), &clock);
    let other = coordinator(&store, &clock, 100);
    let sweep = SweepConfig {
        interval_ms: SWEEP_MS,
        max_list_pages: 4,
        max_head_gets: 4,
    };
    let co = coordinator_with(&store, &clock, 200, sweep);
    for i in 0..10 {
        create(&other, &format!("o-{i}")).await;
    }
    let w = worker(&co, "v1");
    // One empty wake page, then directory pages of two until the four reads
    // are spent and one more page finds none left.
    let expect = |label: &str, delta: PhaseCounts, list: u64, get: u64| {
        assert_eq!((delta.list, delta.get), (list, get), "{label}");
    };
    let before = store.counters();
    assert!(poll_now(&co, &w, "v1").await.is_none());
    expect("first poll", store.counters().since(&before).resolve, 4, 4);

    let before = store.counters();
    clock.advance_ms(SWEEP_MS / 2);
    for _ in 0..5 {
        assert!(poll_now(&co, &w, "v1").await.is_none());
    }
    expect(
        "inside the interval",
        store.counters().since(&before).resolve,
        0,
        0,
    );

    let before = store.counters();
    clock.advance_ms(SWEEP_MS);
    assert!(poll_now(&co, &w, "v1").await.is_none());
    expect("next pass", store.counters().since(&before).resolve, 4, 4);
    assert_eq!(
        co.sweep_metrics().head_gets,
        8,
        "eight distinct agents read"
    );
}

#[tokio::test(start_paused = true)]
async fn a_poll_with_no_wait_runs_at_most_one_bounded_pass() {
    // An agent given work through another coordinator. A poll that does not
    // wait runs one pass, takes the turn the pass found, and the poll after
    // it, at the same instant, runs none.
    let clock = clock();
    let store = store_over(Arc::new(MemoryStore::new()), &clock);
    let other = coordinator(&store, &clock, 100);
    let co = coordinator(&store, &clock, 200);
    create(&other, "o-1").await;
    create(&other, "o-2").await;
    send(&other, "o-1", "r-1", json!(1)).await;
    send(&other, "o-2", "r-1", json!(2)).await;
    let w = worker(&co, "v1");

    let before = store.counters();
    assert!(
        poll_now(&co, &w, "v1").await.is_some(),
        "the pass found work"
    );
    assert_eq!(co.sweep_metrics().passes, 1);
    let first = store.counters().since(&before).resolve;
    assert_eq!(first.list, 2, "one wake page and one directory page");

    let before = store.counters();
    assert!(
        poll_now(&co, &w, "v1").await.is_some(),
        "the second agent, already loaded"
    );
    assert_eq!(co.sweep_metrics().passes, 1, "no pass inside the interval");
    assert_eq!(store.counters().since(&before).resolve.list, 0);
}

#[tokio::test(start_paused = true)]
async fn a_pass_that_skips_an_incompatible_agent_but_assigns_another_counts_no_refusal() {
    let (clock, store, co) = rig();
    create(&co, "o-1").await;
    create(&co, "o-2").await;
    send(&co, "o-1", "r-1", json!(1)).await;
    let w1 = worker(&co, "v1");
    poll_now(&co, &w1, "v1").await.expect("o-1's turn at v1");
    clock.advance_ms(past_lease_and_grace());
    let sent = send(&co, "o-2", "r-2", json!(2)).await;
    let w2 = worker(&co, "v2");
    let before = store.counters();
    let leased = poll_now(&co, &w2, "v2")
        .await
        .expect("o-2 is servable at v2 while o-1 holds a v1 turn");
    assert_eq!(leased.accept_seq, sent.accept_seq);
    assert_eq!(
        co.metrics().incompatible_handler_refusals,
        0,
        "a pass that assigned another agent refused nothing"
    );
    assert_eq!(
        store.counters().since(&before).publish.cas,
        2,
        "o-2's takeover and begin"
    );
    // A third worker at v2: o-2 is leased to w2 under a live lease, so the
    // only candidate left is o-1's v1 turn, and that pass is a refusal.
    let w3 = worker(&co, "v2");
    let refused = co
        .poll(&poll_req(&w3, "v2", 0), Instant::now())
        .await
        .expect_err("with o-2 leased only o-1 is left, and it is refused");
    assert_eq!(refused.kind, ErrorKind::IncompatibleHandler);
    assert_eq!(co.metrics().incompatible_handler_refusals, 1);
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_pending_input_ends_it_cancelled_and_commits_nothing() {
    let (_clock, store, co) = rig();
    create(&co, "o-1").await;
    let sent = send(&co, "o-1", "r-1", json!(1)).await;
    let req = CancelRequest {
        operation: sent.operation.clone(),
    };
    let before = store.counters();
    let r = co.cancel(&req).await.expect("cancel");
    assert_eq!(r.operation, sent.operation);
    assert_eq!(r.status, OperationStatus::Cancelled);
    assert!(r.requested);
    assert_eq!(store.counters().since(&before).publish.cas, 1);
    let after = head(&store, "o-1").await;
    assert!(after.inbox.is_empty());
    let result = after
        .results
        .iter()
        .find(|r| r.accept_seq == sent.accept_seq)
        .expect("result");
    assert_eq!(result.status, ResultStatus::Cancelled);
    let m = co.metrics();
    assert_eq!(m.turns_committed, 0);
    assert_eq!(m.turns_failed, 0);
    let w = worker(&co, "v1");
    assert!(poll_now(&co, &w, "v1").await.is_none());

    let before = store.counters();
    let again = co.cancel(&req).await.expect("cancel again");
    assert_eq!(again.status, OperationStatus::Cancelled);
    assert!(!again.requested);
    assert_eq!(store.counters().since(&before).publish.cas, 0);
}

#[tokio::test(start_paused = true)]
async fn an_enqueue_with_a_lost_ack_is_settled_through_the_receipt_index() {
    let clock = clock();
    // HEAD writes: create 1, enqueue 2.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Nth(2)),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_over(fault.clone(), &clock);
    let co = coordinator(&store, &clock, 100);
    create(&co, "o-1").await;
    let incarnation = head(&store, "o-1").await.incarnation;
    let before = store.counters();
    let sent = send(&co, "o-1", "r-1", json!(1)).await;
    assert_eq!(fault.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_eq!(store.counters().since(&before).publish.cas, 1);
    assert!(!sent.duplicate);
    assert_eq!(sent.accept_seq, 1);
    assert_eq!(
        sent.operation,
        Handle::operation(id_of("o-1"), incarnation, 1).encode()
    );
    assert_eq!(co.metrics().outcome_unknown_resolved, 1);

    let before = store.counters();
    let retried = send(&co, "o-1", "r-1", json!(1)).await;
    assert!(retried.duplicate);
    assert_eq!(retried.operation, sent.operation);
    assert_eq!(store.counters().since(&before).publish.cas, 0);
    assert_eq!(head(&store, "o-1").await.inbox.len(), 1);
}
