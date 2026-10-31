#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use ravel_agents_core::head::{Head, TimerStatus};
use ravel_agents_core::kernel::DerivedIds;
use ravel_agents_core::keys::wake_prefix;
use ravel_agents_core::wire::{
    CancelTimerRequest, TimerCancelOutcome, TimerRequest, TimerResponse,
};
use ravel_agents_store::backoff::NoBackoff;
use ravel_agents_store::{AgentStore, Clock};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_types::TenantHash;
use serde_json::Value;

use crate::tests::{T0, agent_ref, config, create, head};
use crate::{Coordinator, Rng, SweepConfig};

pub(crate) const SEC: i64 = 1_000_000_000;
pub(crate) const TENANT: TenantHash = TenantHash([7; 16]);

/// A clock the test sets to the nanosecond.
pub(crate) struct Now(AtomicI64);

impl Now {
    pub(crate) fn advance(&self, ns: i64) {
        self.0.fetch_add(ns, Ordering::SeqCst);
    }

    pub(crate) fn get(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Clock for Now {
    fn now_unix_ns(&self) -> i64 {
        self.get()
    }
}

struct Seq(AtomicU64);

impl Rng for Seq {
    fn next_u64(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

pub(crate) struct Rig {
    pub(crate) now: Arc<Now>,
    pub(crate) backend: Arc<dyn ObjectStoreBackend>,
    pub(crate) store: Arc<AgentStore>,
    pub(crate) co: Arc<Coordinator>,
}

impl Rig {
    /// A second coordinator over the same store, with nothing loaded.
    pub(crate) fn cold(&self, sweep: SweepConfig) -> Arc<Coordinator> {
        coordinator(&self.store, &self.now, sweep)
    }
}

fn coordinator(store: &Arc<AgentStore>, now: &Arc<Now>, sweep: SweepConfig) -> Arc<Coordinator> {
    Arc::new(
        Coordinator::new(
            store.clone(),
            config(),
            now.clone(),
            Arc::new(Seq(AtomicU64::new(100))),
        )
        .with_sweep(sweep),
    )
}

/// One coordinator over `backend`, its clock at `T0`, which is the start of
/// a due bucket.
pub(crate) fn rig_over(backend: Arc<dyn ObjectStoreBackend>, sweep: SweepConfig) -> Rig {
    rig_with(backend, sweep, crate::tests::limits())
}

pub(crate) fn rig_with(
    backend: Arc<dyn ObjectStoreBackend>,
    sweep: SweepConfig,
    limits: ravel_agents_store::StoreLimits,
) -> Rig {
    let now = Arc::new(Now(AtomicI64::new(T0)));
    let store = Arc::new(
        AgentStore::new(
            backend.clone(),
            TENANT,
            limits,
            now.clone(),
            Arc::new(DerivedIds),
        )
        .with_backoff(Arc::new(NoBackoff)),
    );
    let co = coordinator(&store, &now, sweep);
    Rig {
        now,
        backend,
        store,
        co,
    }
}

pub(crate) fn rig() -> Rig {
    rig_over(Arc::new(MemoryStore::new()), SweepConfig::default())
}

pub(crate) fn timer(key: &str, timer_key: &str, due_unix_ns: i64) -> TimerRequest {
    TimerRequest {
        agent: agent_ref(key),
        timer_key: timer_key.to_string(),
        due_unix_ns,
        handler: "tick".to_string(),
        payload: Value::Null,
    }
}

pub(crate) async fn schedule(
    co: &Coordinator,
    key: &str,
    timer_key: &str,
    due: i64,
) -> TimerResponse {
    co.schedule_timer(&timer(key, timer_key, due))
        .await
        .expect("schedule")
}

pub(crate) async fn cancel(co: &Coordinator, key: &str, timer_key: &str) -> TimerCancelOutcome {
    co.cancel_timer(&CancelTimerRequest {
        agent: agent_ref(key),
        timer_key: timer_key.to_string(),
    })
    .await
    .expect("cancel")
    .outcome
}

/// Every `_wake` key in the backend, read without the store's counters.
pub(crate) async fn hint_keys(backend: &dyn ObjectStoreBackend) -> Vec<String> {
    let prefix = wake_prefix(&TENANT).expect("prefix");
    let mut keys = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = backend
            .list_after(&prefix, after.as_deref(), None)
            .await
            .expect("list");
        let done = page.next.is_none() || page.objects.is_empty();
        after = page.objects.last().map(|m| m.key.clone()).or(after);
        keys.extend(page.objects.into_iter().map(|m| m.key));
        if done {
            return keys;
        }
    }
}

pub(crate) fn status(h: &Head, timer_key: &str) -> TimerStatus {
    h.timers
        .iter()
        .find(|t| t.timer_key == timer_key)
        .expect("timer entry")
        .status
}

pub(crate) fn tick_inputs(h: &Head) -> usize {
    h.inbox.iter().filter(|e| e.handler == "tick").count()
}

#[tokio::test(start_paused = true)]
async fn fire_and_cancel_race_resolves_at_head_and_a_stale_hint_never_fires_a_cancelled_generation()
{
    let r = rig();
    create(&r.co, "o-1").await;

    // 1. Generation 1, due in 90 s; the store writes its hint.
    let first = schedule(&r.co, "o-1", "t", T0 + 90 * SEC).await;
    assert_eq!(first.generation, 1);
    assert_eq!(hint_keys(r.backend.as_ref()).await.len(), 1);

    // 2. Cancelled through the coordinator before it is due.
    assert_eq!(
        cancel(&r.co, "o-1", "t").await,
        TimerCancelOutcome::Cancelled
    );

    // 3. Past due, the sweep reads the stale hint and HEAD, fires nothing
    //    and deletes the hint.
    r.now.advance(120 * SEC);
    let before = r.store.counters();
    let deletes = r.store.wake_hint_deletes();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    let spent = r.store.counters().since(&before);
    assert_eq!(
        spent.publish.cas, 0,
        "a cancelled generation is never fired"
    );
    assert_eq!(spent.resolve.get, 1, "the hint is judged by one HEAD read");
    assert_eq!(spent.resolve.list, 2);
    assert_eq!(r.store.wake_hint_deletes() - deletes, 1);
    assert_eq!(
        (
            pass.hints_read,
            pass.hints_deleted,
            pass.timers_fired,
            pass.head_gets
        ),
        (1, 1, 0, 1)
    );
    assert!(hint_keys(r.backend.as_ref()).await.is_empty());
    let h = head(&r.store, "o-1").await;
    assert_eq!(status(&h, "t"), TimerStatus::Cancelled);
    assert_eq!(tick_inputs(&h), 0);

    // 4. Generation 2, then past its due time.
    let second = schedule(&r.co, "o-1", "t", r.now.get() + 30 * SEC).await;
    assert_eq!(second.generation, 2);
    assert_eq!(second.timer, first.timer);
    r.now.advance(60 * SEC);

    // 5. The sweep fires it exactly once: one CAS, one input for its handler.
    let before = r.store.counters();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(r.store.counters().since(&before).publish.cas, 1);
    assert_eq!((pass.timers_fired, pass.hints_deleted), (1, 1));
    let h = head(&r.store, "o-1").await;
    assert_eq!(status(&h, "t"), TimerStatus::Delivered);
    assert_eq!(tick_inputs(&h), 1);
    let again = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(again.timers_fired, 0);
    assert_eq!(tick_inputs(&head(&r.store, "o-1").await), 1);

    // 6. A cancel after the firing CAS answers AlreadyFired.
    assert_eq!(
        cancel(&r.co, "o-1", "t").await,
        TimerCancelOutcome::AlreadyFired
    );
    assert_eq!(tick_inputs(&head(&r.store, "o-1").await), 1);
}

#[tokio::test(start_paused = true)]
async fn a_timer_fires_at_its_recorded_due_time_and_not_a_nanosecond_before() {
    let r = rig();
    create(&r.co, "o-1").await;
    let due = T0 + 10 * SEC;
    schedule(&r.co, "o-1", "t", due).await;

    r.now.advance(due - 1 - r.now.get());
    let before = r.store.counters();
    let early = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!((early.timers_fired, early.hints_deleted), (0, 0));
    // The runtime's own predicate holds the timer back: no fire is
    // attempted, so neither the skeleton's HEAD read nor its CAS is issued.
    // The kernel would refuse an early fire too, so only these two counts
    // tell a runtime that asks too early from one that does not.
    let spent = r.store.counters().since(&before);
    assert_eq!(spent.publish.cas, 0);
    assert_eq!(spent.resolve.get, 1, "the sweep's own HEAD read only");
    assert_eq!(
        status(&head(&r.store, "o-1").await, "t"),
        TimerStatus::Scheduled
    );
    assert_eq!(hint_keys(r.backend.as_ref()).await.len(), 1);

    r.now.advance(1);
    let on_time = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!((on_time.timers_fired, on_time.hints_deleted), (1, 1));
    assert_eq!(
        status(&head(&r.store, "o-1").await, "t"),
        TimerStatus::Delivered
    );
}

#[tokio::test(start_paused = true)]
async fn a_schedule_whose_hint_was_never_written_still_fires_when_due() {
    // The first schedule's CAS applies and its acknowledgement is lost, so
    // the store writes no hint; the second's hint PUT is held forever.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Nth(3)),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let r = rig_over(fault.clone(), SweepConfig::default());
    create(&r.co, "o-a").await;
    create(&r.co, "o-b").await;
    let due = T0 + 10 * SEC;

    let settled = schedule(&r.co, "o-a", "t", due).await;
    assert_eq!(fault.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_eq!(settled.generation, 1);
    assert_eq!(r.co.metrics().outcome_unknown_resolved, 1);

    let gate = fault.hold(Op::Put, Some("/_wake/".to_string()), Occurrence::Nth(1));
    let co = r.co.clone();
    let task = tokio::spawn(async move { co.schedule_timer(&timer("o-b", "t", due)).await });
    gate.wait_until_held(1).await;
    task.abort();
    assert!(task.await.is_err(), "the schedule never returns");
    assert_eq!(
        gate.held_count(),
        1,
        "the hint PUT is held and never released"
    );
    assert_eq!(
        status(&head(&r.store, "o-b").await, "t"),
        TimerStatus::Scheduled
    );
    assert!(hint_keys(r.backend.as_ref()).await.is_empty());

    r.now.advance(due - r.now.get());
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!((pass.timers_fired, pass.hints_read), (2, 0));
    for key in ["o-a", "o-b"] {
        let h = head(&r.store, key).await;
        assert_eq!(status(&h, "t"), TimerStatus::Delivered, "{key}");
        assert_eq!(tick_inputs(&h), 1, "{key}");
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_timer_never_scheduled_answers_unknown() {
    let r = rig();
    create(&r.co, "o-1").await;
    let before = r.store.counters();
    assert_eq!(
        cancel(&r.co, "o-1", "nope").await,
        TimerCancelOutcome::Unknown
    );
    assert_eq!(r.store.counters().since(&before).publish.cas, 0);
}

#[tokio::test(start_paused = true)]
async fn a_reschedule_supersedes_the_scheduled_generation() {
    let r = rig();
    create(&r.co, "o-1").await;
    let first = schedule(&r.co, "o-1", "t", T0 + 10 * SEC).await;
    let second = schedule(&r.co, "o-1", "t", T0 + 20 * SEC).await;
    assert_eq!((first.generation, second.generation), (1, 2));
    let h = head(&r.store, "o-1").await;
    let entry = h
        .timers
        .iter()
        .find(|t| t.timer_id == id_of_timer(&h, "t"))
        .expect("entry");
    assert_eq!((entry.generation, entry.due_unix_ns), (2, T0 + 20 * SEC));
    assert_eq!(h.timers.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_hint_of_a_destroyed_incarnation_fires_nothing_and_is_deleted() {
    let r = rig();
    create(&r.co, "o-1").await;
    schedule(&r.co, "o-1", "t", T0 + 30 * SEC).await;
    assert_eq!(hint_keys(r.backend.as_ref()).await.len(), 1);
    r.co.destroy(&agent_ref("o-1"), None)
        .await
        .expect("destroy");

    r.now.advance(60 * SEC);
    let before = r.store.counters();
    let deletes = r.store.wake_hint_deletes();
    let pass = r.co.sweep_once(&TENANT).await.expect("sweep");
    assert_eq!(r.store.counters().since(&before).publish.cas, 0);
    assert_eq!(r.store.wake_hint_deletes() - deletes, 1);
    assert_eq!(
        (pass.hints_read, pass.hints_deleted, pass.timers_fired),
        (1, 1, 0)
    );
    assert!(hint_keys(r.backend.as_ref()).await.is_empty());
    let h = head(&r.store, "o-1").await;
    assert_eq!(status(&h, "t"), TimerStatus::Scheduled);
    assert_eq!(tick_inputs(&h), 0);
}

fn id_of_timer(h: &Head, timer_key: &str) -> ravel_agents_core::identity::Id128 {
    ravel_agents_core::identity::timer_id(h.incarnation, timer_key)
}
