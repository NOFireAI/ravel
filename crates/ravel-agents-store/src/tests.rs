#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use proptest::prelude::*;
use ravel_agents_core::command::{
    BeginTurn, Cancel, CancelTimer, CompleteTurn, FailTurn, HandlerPin, OwnerFence, ScheduleTimer,
    Takeover, TimerSpec, Tombstone, TurnFence,
};
use ravel_agents_core::head::Lifecycle;
use ravel_agents_core::identity::Hash256;
use ravel_agents_core::kernel::Transition;
use ravel_agents_core::keys::{DueBucket, dir_key, head_key, wake_key};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, PutOptions};

use super::*;
use crate::counters::PhaseCounts;
use crate::publish::MAX_CAS_ATTEMPTS;

const SECOND: i64 = 1_000_000_000;
const T0: i64 = 1_800_000_000 * SECOND;

struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn tenant() -> TenantHash {
    TenantHash([7; 16])
}

fn kind() -> AgentKind {
    AgentKind::new("order").expect("kind")
}

fn key(name: &str) -> AgentKey {
    AgentKey::new(name).expect("key")
}

fn agent() -> AgentId {
    agent_id(&kind(), &key("o-1"))
}

fn store_with(backend: Arc<dyn ObjectStoreBackend>, limits: StoreLimits) -> AgentStore {
    AgentStore::new(
        backend,
        tenant(),
        limits,
        Arc::new(TestClock(AtomicI64::new(T0))),
        Arc::new(DerivedIds),
    )
}

fn faulty(rules: Vec<Rule>) -> (Arc<FaultStore<MemoryStore>>, AgentStore) {
    let plan = rules
        .into_iter()
        .fold(FaultPlan::empty(), |plan, rule| plan.with_rule(rule));
    let backend = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_with(backend.clone(), StoreLimits::default());
    (backend, store)
}

fn head_rule(op: Op, fault: ScriptedFault, nth: u64) -> Rule {
    Rule::new(op, fault)
        .with_key_contains("/head")
        .with_occurrence(Occurrence::Nth(nth))
}

fn accept_seq(outcome: &Outcome) -> u64 {
    match outcome.reply {
        Reply::Accepted { accept_seq, .. } => accept_seq,
        ref other => panic!("expected Accepted, got {other:?}"),
    }
}

async fn create(store: &AgentStore) -> Outcome {
    store.create(kind(), key("o-1")).await.expect("create")
}

async fn enqueue(
    store: &AgentStore,
    head: &Head,
    rid: &str,
    payload: &[u8],
) -> Result<Outcome, AgentError> {
    store
        .enqueue(
            &agent(),
            head.incarnation,
            rid,
            "h",
            Bytes::copy_from_slice(payload),
        )
        .await
}

#[tokio::test]
async fn create_is_idempotent_through_already_exists() {
    // The second create's HEAD read is a blip, so it takes the absent path
    // and its CreateIfAbsent meets the root the first create wrote.
    let (backend, store) = faulty(vec![head_rule(Op::Get, ScriptedFault::NotFoundBlip, 2)]);
    let first = create(&store).await;
    assert_eq!(first.reply, Reply::Created { incarnation: 1 });
    assert!(first.published);

    let before = store.counters();
    let second = create(&store).await;
    let delta = store.counters().since(&before);
    assert_eq!(backend.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
    assert_eq!(second.reply, Reply::Exists { incarnation: 1 });
    assert!(!second.published);
    assert_eq!(second.head, first.head);
    assert_eq!(second.version, first.version);
    assert_eq!(delta.publish.put, 1, "one CreateIfAbsent on HEAD");
    assert_eq!(delta.publish.conflict, 1, "refused with AlreadyExists");
    assert_eq!(delta.resolve.get, 2, "the blip, then the re-read");

    // Without the blip a repeated create is one read and no write.
    let before = store.counters();
    let third = create(&store).await;
    let delta = store.counters().since(&before);
    assert_eq!(third.reply, Reply::Exists { incarnation: 1 });
    assert_eq!(delta.total().put + delta.total().cas, 0);
}

#[tokio::test]
async fn create_on_a_tombstoned_agent_bumps_the_incarnation() {
    let backend = Arc::new(MemoryStore::new());
    let store = store_with(backend.clone(), StoreLimits::default());
    let created = create(&store).await;
    let tombstone = Command::Tombstone(Tombstone {
        fence: IncarnationFence {
            incarnation: created.head.incarnation,
        },
    });
    let gone = store
        .execute(&agent(), &tombstone)
        .await
        .expect("tombstone");
    assert_eq!(gone.head.lifecycle, Lifecycle::Tombstoned);

    let before = store.counters();
    let again = create(&store).await;
    let delta = store.counters().since(&before);
    assert_eq!(again.reply, Reply::Created { incarnation: 2 });
    assert!(again.published);
    assert_eq!(again.head.incarnation, 2);
    assert_eq!(again.head.lifecycle, Lifecycle::Active);
    assert_eq!(again.head.root_revision, gone.head.root_revision + 1);
    assert_eq!(
        delta.publish.cas, 1,
        "a CAS on the existing root, not a create"
    );
    // The `_dir` entry was written by the first create and is still there.
    let dir = dir_key(&tenant(), &agent()).expect("dir key");
    backend.head(&dir).await.expect("dir entry");
}

#[tokio::test]
async fn enqueue_with_the_same_request_id_and_hash_returns_the_same_handle() {
    let store = store_with(Arc::new(MemoryStore::new()), StoreLimits::default());
    let created = create(&store).await;
    let first = enqueue(&store, &created.head, "r1", b"p")
        .await
        .expect("enqueue");
    assert!(first.published);

    let before = store.counters();
    let again = enqueue(&store, &created.head, "r1", b"p")
        .await
        .expect("duplicate");
    let delta = store.counters().since(&before);
    assert_eq!(again.handle(), first.handle());
    assert_eq!(
        again.reply,
        Reply::Accepted {
            accept_seq: accept_seq(&first),
            duplicate: true
        }
    );
    assert!(!again.published);
    assert_eq!(delta.publish.cas, 0);
    assert_eq!(delta.total().put, 0);

    let before = store.counters();
    let conflict = enqueue(&store, &created.head, "r1", b"other")
        .await
        .expect_err("same id, different hash");
    let delta = store.counters().since(&before);
    assert_eq!(conflict.kind, ErrorKind::IdempotencyConflict);
    assert_eq!(delta.publish.cas, 0);
    assert_eq!(delta.total().put, 0);
}

#[tokio::test]
async fn cas_applied_with_lost_response_is_resolved_by_operation_id_after_a_later_revision() {
    // Put 1 on the head key is the create, put 2 the first enqueue's CAS:
    // it applies and its acknowledgement is lost.
    let (backend, store) = faulty(vec![head_rule(
        Op::Put,
        ScriptedFault::DuplicateDelivery,
        2,
    )]);
    let created = create(&store).await;

    let lost = enqueue(&store, &created.head, "r1", b"first")
        .await
        .expect_err("the CAS answer was lost");
    assert_eq!(lost.kind, ErrorKind::OutcomeUnknown);
    let token = lost.ids.operation.clone().expect("an operation handle");
    let handle = Handle::decode(&token).expect("handle");

    let later = enqueue(&store, &created.head, "r2", b"second")
        .await
        .expect("an unrelated enqueue");
    assert!(later.published);
    assert_ne!(later.handle(), Some(handle));

    let resolution = store.resolve(&handle).await.expect("resolve");
    let HandleItem::Operation(predicted) = handle.item else {
        panic!("not an operation handle");
    };
    assert_eq!(resolution.accept_seq, predicted);
    assert_eq!(resolution.state, Some(OperationState::Accepted));
    assert!(resolution.is_request("r1", "h", &request_hash("h", b"first")));
    assert!(!resolution.is_request("r1", "h", &request_hash("h", b"other")));
    let receipt = resolution.receipt.expect("receipt");
    assert_eq!(receipt.request_hash, request_hash("h", b"first"));
    assert_eq!(receipt.accept_seq, predicted);
    assert_eq!(resolution.root_revision, later.head.root_revision);
    assert!(later.head.root_revision > created.head.root_revision + 1);
    // One input per request: nothing was retried under a fresh id.
    assert_eq!(later.head.inbox.len(), 2);
    assert_eq!(
        backend.fault_count(Op::Put, FaultKind::DuplicateDelivery),
        1
    );
}

#[tokio::test]
async fn an_unknown_cas_outcome_is_never_retried() {
    for fault in [
        ScriptedFault::Transient("lost".to_string()),
        ScriptedFault::Timeout,
        ScriptedFault::Throttled { retry_after_ms: 1 },
    ] {
        let (_, store) = faulty(vec![head_rule(Op::Put, fault.clone(), 2)]);
        let created = create(&store).await;
        let before = store.counters();
        let error = enqueue(&store, &created.head, "r1", b"p")
            .await
            .expect_err("unknown outcome");
        let delta = store.counters().since(&before);
        assert_eq!(error.kind, ErrorKind::OutcomeUnknown, "{fault:?}");
        assert!(error.ids.operation.is_some(), "{fault:?}");
        assert_eq!(delta.publish.cas, 1, "{fault:?}: no second CAS");
    }
}

#[tokio::test]
async fn a_failed_conditional_write_re_reads_and_succeeds_on_retry() {
    let (backend, store) = faulty(vec![head_rule(
        Op::Put,
        ScriptedFault::FailedConditionalWrite,
        2,
    )]);
    let created = create(&store).await;
    let before = store.counters();
    let accepted = enqueue(&store, &created.head, "r1", b"p")
        .await
        .expect("enqueue");
    let delta = store.counters().since(&before);
    assert!(accepted.published);
    assert_eq!(delta.publish.cas, 2);
    assert_eq!(delta.publish.conflict, 1);
    assert_eq!(delta.resolve.get, 2, "read, then re-read after the refusal");
    assert_eq!(accepted.head.root_revision, created.head.root_revision + 1);
    assert_eq!(
        backend.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1
    );
}

#[tokio::test]
async fn a_contended_head_gives_up_after_the_cas_bound() {
    let refuse = || {
        Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Always)
    };

    // CAS path: seed the root underneath the faults, then every CAS fails.
    let (backend, store) = faulty(vec![refuse()]);
    let create = Command::Create(Create {
        tenant: tenant(),
        kind: kind(),
        key: key("o-1"),
    });
    let seeded = Kernel::new(Limits::default())
        .apply(None, &create, T0, &DerivedIds)
        .expect("create");
    let seeded = seeded.head().expect("a new root").clone();
    let head = head_key(&tenant(), &agent()).expect("head key");
    backend
        .inner()
        .put(&head, Bytes::from(seeded.encode()), PutOptions::default())
        .await
        .expect("seed");
    let before = store.counters();
    let error = enqueue(&store, &seeded, "r1", b"p")
        .await
        .expect_err("every CAS is refused");
    let delta = store.counters().since(&before);
    assert_eq!(error.kind, ErrorKind::StoreUnavailable);
    assert_eq!(delta.publish.cas, u64::from(MAX_CAS_ATTEMPTS));
    assert_eq!(delta.publish.conflict, u64::from(MAX_CAS_ATTEMPTS));
    assert_eq!(delta.resolve.get, u64::from(MAX_CAS_ATTEMPTS));

    // Create path: every CreateIfAbsent is refused as AlreadyExists while
    // the root stays absent.
    let (_, store) = faulty(vec![refuse()]);
    let error = store
        .create(kind(), key("o-1"))
        .await
        .expect_err("every create is refused");
    let delta = store.counters();
    assert_eq!(error.kind, ErrorKind::StoreUnavailable);
    assert_eq!(delta.publish.put, u64::from(MAX_CAS_ATTEMPTS));
    assert_eq!(delta.publish.conflict, u64::from(MAX_CAS_ATTEMPTS));
}

#[tokio::test]
async fn a_corrupt_dependency_write_is_a_typed_error() {
    let corrupt = Rule::new(Op::Put, ScriptedFault::CorruptBody)
        .with_key_contains("/o/")
        .with_occurrence(Occurrence::Nth(1));
    let (backend, store) = faulty(vec![corrupt]);
    let created = create(&store).await;
    let before = store.counters();
    let error = enqueue(&store, &created.head, "r1", b"p")
        .await
        .expect_err("the store refuses the flipped upload");
    let delta = store.counters().since(&before);
    assert_eq!(error.kind, ErrorKind::StoreUnavailable);
    assert_eq!(delta.publish.cas, 0, "no CAS over a missing dependency");
    assert_eq!(backend.fault_count(Op::Put, FaultKind::CorruptBody), 1);
}

#[tokio::test]
async fn a_corrupt_dependency_read_is_a_typed_error() {
    let corrupt = Rule::new(Op::Get, ScriptedFault::CorruptRange)
        .with_key_contains("/o/")
        .with_occurrence(Occurrence::Always);
    let (backend, store) = faulty(vec![corrupt]);
    let created = create(&store).await;
    let payload = Bytes::from_static(b"payload");
    let dep = store
        .put_object(&agent(), payload.clone())
        .await
        .expect("put");

    // The enqueue's upload meets the stored object and reads it back.
    let before = store.counters();
    let error = enqueue(&store, &created.head, "r1", &payload)
        .await
        .expect_err("the read-back does not hash to the reference");
    let delta = store.counters().since(&before);
    assert_eq!(error.kind, ErrorKind::StoreUnavailable);
    assert_eq!(error.reason, "stored record failed to decode");
    assert_eq!(delta.store.conflict, 1);
    assert_eq!(delta.publish.cas, 0);

    let error = store.read_object(&agent(), dep).await.expect_err("corrupt");
    assert_eq!(error.reason, "stored record failed to decode");
    assert_eq!(backend.fault_count(Op::Get, FaultKind::CorruptRange), 2);
}

#[tokio::test]
async fn a_head_not_found_blip_is_retried_as_a_transient_read() {
    // Get 1 on the head key is the create's read of an absent root.
    let (backend, store) = faulty(vec![head_rule(Op::Get, ScriptedFault::NotFoundBlip, 2)]);
    let created = create(&store).await;
    let before = store.counters();
    let accepted = enqueue(&store, &created.head, "r1", b"p")
        .await
        .expect("enqueue");
    let delta = store.counters().since(&before);
    assert!(accepted.published);
    assert_eq!(delta.resolve.get, 2);
    assert_eq!(backend.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
}

#[tokio::test]
async fn a_newer_head_format_is_unsupported_and_never_written() {
    let backend = Arc::new(MemoryStore::new());
    let store = store_with(backend.clone(), StoreLimits::default());
    // Field 1 (format_version), varint 2, then a field this release has
    // never seen.
    let newer = Bytes::from_static(&[0x08, 0x02, 0xf8, 0x3f, 0x01]);
    let head = head_key(&tenant(), &agent()).expect("head key");
    backend
        .put(&head, newer.clone(), PutOptions::default())
        .await
        .expect("seed");

    assert_eq!(
        store.read_head(&agent()).await.expect("read"),
        HeadState::Unsupported { found: 2 }
    );
    let before = store.counters();
    let error = store
        .enqueue(&agent(), 1, "r1", "h", Bytes::from_static(b"p"))
        .await
        .expect_err("refused");
    assert_eq!(error.kind, ErrorKind::UnsupportedFormat);
    let error = store.create(kind(), key("o-1")).await.expect_err("refused");
    assert_eq!(error.kind, ErrorKind::UnsupportedFormat);
    let total = store.counters().since(&before).total();
    assert_eq!(total.put, 0);
    assert_eq!(total.cas, 0);
    let stored = backend.get(&head, GetRange::Full).await.expect("get");
    assert_eq!(stored.data, newer);
}

#[tokio::test]
async fn a_full_mailbox_is_refused_before_any_write() {
    let limits = StoreLimits {
        kernel: Limits {
            max_mailbox_depth: 2,
            ..Limits::default()
        },
        ..StoreLimits::default()
    };
    let store = store_with(Arc::new(MemoryStore::new()), limits);
    let created = create(&store).await;
    enqueue(&store, &created.head, "r1", b"1").await.expect("1");
    enqueue(&store, &created.head, "r2", b"2").await.expect("2");

    let before = store.counters();
    let error = enqueue(&store, &created.head, "r3", b"3")
        .await
        .expect_err("mailbox full");
    let delta = store.counters().since(&before);
    assert_eq!(error.kind, ErrorKind::MailboxFull);
    // The depth lives in HEAD, so the one call made is the HEAD read.
    assert_eq!(
        delta.total(),
        PhaseCounts {
            get: 1,
            bytes_in: delta.resolve.bytes_in,
            ..PhaseCounts::default()
        }
    );
    assert_eq!(delta.resolve.get, 1);
}

#[tokio::test]
async fn an_oversized_payload_is_refused_before_any_store_call() {
    let limits = StoreLimits {
        max_object_bytes: 8,
        ..StoreLimits::default()
    };
    let store = store_with(Arc::new(MemoryStore::new()), limits);
    let created = create(&store).await;
    let before = store.counters();
    let error = enqueue(&store, &created.head, "r1", b"123456789")
        .await
        .expect_err("over the limit");
    assert_eq!(error.kind, ErrorKind::BudgetExceeded);
    assert_eq!(store.counters().since(&before), CounterSnapshot::default());
    enqueue(&store, &created.head, "r1", b"12345678")
        .await
        .expect("at the limit");
}

#[tokio::test]
async fn an_oversized_reference_is_refused_before_any_dependency_write() {
    let limits = StoreLimits {
        max_object_bytes: 16,
        ..StoreLimits::default()
    };
    let store = store_with(Arc::new(MemoryStore::new()), limits);
    let mut head = None;
    for step in [
        Step::Create,
        Step::Enqueue {
            request: 0,
            payload: 0,
        },
        Step::Takeover,
        Step::Begin,
    ] {
        let (cmd, deps) = command(&step, head.as_ref(), T0);
        let outcome = store
            .execute_with(&agent(), &cmd, &deps)
            .await
            .expect("setup step");
        head = Some(outcome.head);
    }
    let Some(head) = head else {
        panic!("no head after setup");
    };
    let (Command::CompleteTurn(mut complete), deps) =
        command(&Step::Complete { state: 0 }, Some(&head), T0)
    else {
        panic!("not a turn commit");
    };
    complete.output = Some(ObjectRef::of(&[0u8; 17]));

    let before = store.counters();
    let error = store
        .execute_with(&agent(), &Command::CompleteTurn(complete), &deps)
        .await
        .expect_err("the output is over the limit");
    assert_eq!(error.kind, ErrorKind::BudgetExceeded);
    let delta = store.counters().since(&before);
    assert_eq!(delta.store, PhaseCounts::default());
    assert_eq!(delta.publish, PhaseCounts::default());
}

#[tokio::test]
async fn list_agents_pages_through_the_directory() {
    let store = store_with(
        Arc::new(MemoryStore::with_page_size(2)),
        StoreLimits::default(),
    );
    let mut expected = Vec::new();
    for i in 0..5 {
        let outcome = store
            .create(kind(), key(&format!("o-{i}")))
            .await
            .expect("create");
        expected.push(outcome.agent);
    }
    expected.sort();

    let before = store.counters();
    let mut seen = Vec::new();
    let mut page = None;
    let mut pages = 0;
    loop {
        let listed = store.list_agents(page).await.expect("list");
        pages += 1;
        assert!(listed.agents.len() <= 2);
        seen.extend(listed.agents);
        match listed.next {
            Some(next) => page = Some(next),
            None => break,
        }
    }
    assert_eq!(seen, expected);
    assert_eq!(pages, 3);
    assert_eq!(store.counters().since(&before).resolve.list, 3);
}

fn schedule(head: &Head, due_unix_ns: i64) -> Command {
    Command::ScheduleTimer(ScheduleTimer {
        fence: IncarnationFence {
            incarnation: head.incarnation,
        },
        timer: TimerSpec {
            timer_key: "t".to_string(),
            due_unix_ns,
            handler: "h".to_string(),
            payload: None,
        },
    })
}

#[tokio::test]
async fn a_scheduled_timer_writes_its_wake_hint() {
    let backend = Arc::new(MemoryStore::new());
    let store = store_with(backend.clone(), StoreLimits::default());
    let created = create(&store).await;
    let due = T0 + 60 * SECOND;
    let scheduled = store
        .execute(&agent(), &schedule(&created.head, due))
        .await
        .expect("schedule");
    assert!(matches!(scheduled.reply, Reply::TimerScheduled { .. }));
    let bucket = DueBucket::for_due(due).expect("bucket");
    let wake = wake_key(&tenant(), bucket, &agent()).expect("wake key");
    backend.head(&wake).await.expect("wake hint");
}

#[tokio::test]
async fn a_failed_wake_hint_does_not_fail_the_command() {
    let refuse = Rule::new(Op::Put, ScriptedFault::Permanent("down".to_string()))
        .with_key_contains("/_wake/")
        .with_occurrence(Occurrence::Always);
    let (backend, store) = faulty(vec![refuse]);
    let created = create(&store).await;
    let scheduled = store
        .execute(&agent(), &schedule(&created.head, T0 + 60 * SECOND))
        .await
        .expect("the CAS published, so the command succeeds");
    assert!(scheduled.published);
    assert!(matches!(scheduled.reply, Reply::TimerScheduled { .. }));
    assert_eq!(backend.fault_count(Op::Put, FaultKind::Permanent), 1);
}

// ---- model equivalence ----

#[derive(Debug, Clone)]
enum Step {
    Create,
    Enqueue { request: u8, payload: u8 },
    Takeover,
    Begin,
    Complete { state: u8 },
    Fail { terminal: bool },
    Cancel { seq: u64 },
    Schedule { timer: u8, due_s: i64 },
    CancelTimer { timer: u8 },
    Tombstone,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        1 => Just(Step::Create),
        4 => (0u8..3, 0u8..3).prop_map(|(request, payload)| Step::Enqueue { request, payload }),
        2 => Just(Step::Takeover),
        2 => Just(Step::Begin),
        2 => (0u8..3).prop_map(|state| Step::Complete { state }),
        1 => any::<bool>().prop_map(|terminal| Step::Fail { terminal }),
        1 => (0u64..5).prop_map(|seq| Step::Cancel { seq }),
        1 => (0u8..2, -5i64..120).prop_map(|(timer, due_s)| Step::Schedule { timer, due_s }),
        1 => (0u8..2).prop_map(|timer| Step::CancelTimer { timer }),
        1 => Just(Step::Tombstone),
    ]
}

fn pin() -> HandlerPin {
    HandlerPin {
        handler: "h".to_string(),
        version: "1".to_string(),
        digest: Hash256([1; 32]),
    }
}

/// The command a step stands for against `head`, and the bytes of the
/// objects it references.
fn command(step: &Step, head: Option<&Head>, now: i64) -> (Command, Vec<Bytes>) {
    let incarnation = head.map_or(1, |h| h.incarnation);
    let fence = IncarnationFence { incarnation };
    let owner = OwnerFence {
        incarnation,
        owner_id: head.map_or(String::new(), |h| h.owner_id.clone()),
        epoch: head.map_or(0, |h| h.epoch),
    };
    let turn = TurnFence {
        owner: owner.clone(),
        turn_id: head
            .and_then(|h| h.active_turn.as_ref())
            .map_or(ravel_agents_core::identity::Id128([0; 16]), |t| t.turn_id),
        base_state_version: head
            .and_then(|h| h.active_turn.as_ref())
            .map_or(0, |t| t.base_state_version),
        handler_version: "1".to_string(),
        handler_digest: Hash256([1; 32]),
    };
    match step {
        Step::Create => (
            Command::Create(Create {
                tenant: tenant(),
                kind: kind(),
                key: key("o-1"),
            }),
            Vec::new(),
        ),
        Step::Enqueue { request, payload } => {
            let bytes = Bytes::from(format!("payload-{payload}"));
            (
                Command::Enqueue(Enqueue {
                    fence,
                    request_id: format!("r{request}"),
                    handler: "h".to_string(),
                    request_hash: request_hash("h", &bytes),
                    payload: Some(ObjectRef::of(&bytes)),
                }),
                vec![bytes],
            )
        }
        Step::Takeover => (
            Command::Takeover(Takeover {
                incarnation,
                owner_id: "w".to_string(),
                observed_epoch: owner.epoch,
                lease_hint_unix_ns: now,
            }),
            Vec::new(),
        ),
        Step::Begin => (
            Command::BeginTurn(BeginTurn {
                fence: owner,
                handlers: vec![pin()],
            }),
            Vec::new(),
        ),
        Step::Complete { state } => {
            let bytes = Bytes::from(format!("state-{state}"));
            (
                Command::CompleteTurn(CompleteTurn {
                    fence: turn,
                    observed_root_revision: head.map_or(0, |h| h.root_revision),
                    new_state: ObjectRef::of(&bytes),
                    output: None,
                    events: Vec::new(),
                    timer_changes: Vec::new(),
                    intents: Vec::new(),
                }),
                vec![bytes],
            )
        }
        Step::Fail { terminal } => (
            Command::FailTurn(FailTurn {
                fence: turn,
                error: "boom".to_string(),
                terminal: *terminal,
                max_attempts: 2,
            }),
            Vec::new(),
        ),
        Step::Cancel { seq } => (
            Command::Cancel(Cancel {
                fence,
                accept_seq: *seq,
            }),
            Vec::new(),
        ),
        Step::Schedule { timer, due_s } => (
            Command::ScheduleTimer(ScheduleTimer {
                fence,
                timer: TimerSpec {
                    timer_key: format!("t{timer}"),
                    due_unix_ns: now + due_s * SECOND,
                    handler: "h".to_string(),
                    payload: None,
                },
            }),
            Vec::new(),
        ),
        Step::CancelTimer { timer } => (
            Command::CancelTimer(CancelTimer {
                fence,
                timer_key: format!("t{timer}"),
            }),
            Vec::new(),
        ),
        Step::Tombstone => (Command::Tombstone(Tombstone { fence }), Vec::new()),
    }
}

/// Run `steps` through the store and through the kernel on in-memory heads,
/// compare the result of every step and the HEAD after it, and return the
/// store's results.
fn store_matches_kernel(steps: &[Step]) -> Result<Vec<Result<Reply, ErrorKind>>, TestCaseError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|e| TestCaseError::fail(e.to_string()))?;
    runtime.block_on(async {
        let clock = Arc::new(TestClock(AtomicI64::new(T0)));
        let store = AgentStore::new(
            Arc::new(MemoryStore::new()),
            tenant(),
            StoreLimits::default(),
            clock.clone(),
            Arc::new(DerivedIds),
        );
        let kernel = Kernel::new(Limits::default());
        let mut model: Option<Head> = None;
        let mut results = Vec::with_capacity(steps.len());
        for (i, step) in steps.iter().enumerate() {
            let now = T0 + i as i64 * SECOND;
            clock.0.store(now, Ordering::SeqCst);
            let (cmd, deps) = command(step, model.as_ref(), now);
            let expected = kernel.apply(model.as_ref(), &cmd, now, &DerivedIds);
            let actual = store.execute_with(&agent(), &cmd, &deps).await;
            results.push(actual.as_ref().map(|o| o.reply.clone()).map_err(|e| e.kind));
            match expected {
                Ok(Transition::Publish { head, reply, .. }) => {
                    match reply {
                        Reply::Blocked(error) => {
                            prop_assert_eq!(actual.map(|o| o.reply), Err(error));
                        }
                        reply => {
                            let outcome = actual
                                .map_err(|e| TestCaseError::fail(format!("{step:?}: {e}")))?;
                            prop_assert_eq!(&outcome.reply, &reply);
                            prop_assert!(outcome.published);
                            prop_assert_eq!(&outcome.head, &head);
                        }
                    }
                    model = Some(head);
                }
                Ok(Transition::Unchanged { reply }) => {
                    let outcome =
                        actual.map_err(|e| TestCaseError::fail(format!("{step:?}: {e}")))?;
                    prop_assert_eq!(&outcome.reply, &reply);
                    prop_assert!(!outcome.published);
                    prop_assert_eq!(Some(&outcome.head), model.as_ref());
                }
                Err(error) => {
                    prop_assert_eq!(actual.map(|o| o.reply), Err(error));
                }
            }
            let stored = match store.read_head(&agent()).await {
                Ok(HeadState::Present { head, .. }) => Some(*head),
                Ok(HeadState::Absent) => None,
                other => return Err(TestCaseError::fail(format!("{other:?}"))),
            };
            prop_assert_eq!(&stored, &model, "after step {} {:?}", i, step);
        }
        Ok(results)
    })
}

#[test]
fn the_model_harness_reaches_a_turn_commit_and_a_new_incarnation() {
    let steps = [
        Step::Create,
        Step::Enqueue {
            request: 0,
            payload: 0,
        },
        Step::Enqueue {
            request: 0,
            payload: 0,
        },
        Step::Enqueue {
            request: 0,
            payload: 1,
        },
        Step::Takeover,
        Step::Begin,
        Step::Complete { state: 0 },
        Step::Schedule {
            timer: 0,
            due_s: 30,
        },
        Step::Tombstone,
        Step::Create,
    ];
    let results = store_matches_kernel(&steps).expect("store matches kernel");
    assert_eq!(results[0], Ok(Reply::Created { incarnation: 1 }));
    assert_eq!(
        results[2],
        Ok(Reply::Accepted {
            accept_seq: 1,
            duplicate: true
        })
    );
    assert_eq!(results[3], Err(ErrorKind::IdempotencyConflict));
    assert!(
        matches!(results[6], Ok(Reply::TurnCommitted { accept_seq: 1, .. })),
        "{:?}",
        results[6]
    );
    assert!(
        matches!(results[7], Ok(Reply::TimerScheduled { .. })),
        "{:?}",
        results[7]
    );
    assert_eq!(results[8], Ok(Reply::Tombstoned));
    assert_eq!(results[9], Ok(Reply::Created { incarnation: 2 }));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn the_store_publishes_the_kernels_head_sequence(
        steps in prop::collection::vec(step(), 1..16),
    ) {
        // Lead with a create so most cases exercise a live agent; later
        // creates exercise re-creation after a tombstone.
        let steps: Vec<Step> = std::iter::once(Step::Create).chain(steps).collect();
        store_matches_kernel(&steps)?;
    }
}

#[tokio::test]
async fn a_lost_write_is_not_mistaken_for_a_later_writer_with_the_same_id() {
    // Put 2 (the first enqueue's CAS) is refused without applying and the
    // caller sees a retryable error, so it holds a handle for a write that
    // never landed. A later writer then takes that handle under the same
    // request id with a different payload.
    let (_backend, store) = faulty(vec![head_rule(
        Op::Put,
        ScriptedFault::Transient("lost".to_string()),
        2,
    )]);
    let created = create(&store).await;
    let lost = enqueue(&store, &created.head, "r1", b"first")
        .await
        .expect_err("the CAS answer was lost");
    assert_eq!(lost.kind, ErrorKind::OutcomeUnknown);
    let handle = Handle::decode(&lost.ids.operation.clone().expect("handle")).expect("handle");
    let other = enqueue(&store, &created.head, "r1", b"other")
        .await
        .expect("the other writer is accepted");
    assert_eq!(other.handle(), Some(handle));
    let resolution = store.resolve(&handle).await.expect("resolve");
    assert!(
        !resolution.is_request("r1", "h", &request_hash("h", b"first")),
        "the lost write must not be reported as accepted"
    );
    assert!(resolution.is_request("r1", "h", &request_hash("h", b"other")));
}

#[tokio::test]
async fn resolve_keeps_the_handle_when_head_cannot_be_read() {
    let (backend, store) = faulty(vec![head_rule(
        Op::Put,
        ScriptedFault::DuplicateDelivery,
        2,
    )]);
    let created = create(&store).await;
    let lost = enqueue(&store, &created.head, "r1", b"first")
        .await
        .expect_err("the CAS answer was lost");
    let handle = Handle::decode(&lost.ids.operation.clone().expect("handle")).expect("handle");
    // Every HEAD read now blips NotFound: the agent did not vanish, the read
    // failed, so the caller keeps an unknown outcome and its handle.
    let blips = Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
        .with_key_contains("/head")
        .with_occurrence(Occurrence::Always);
    let blipping = store_with(
        Arc::new(FaultStore::new(
            backend.clone(),
            FaultPlan::empty().with_rule(blips),
        )),
        StoreLimits::default(),
    );
    let err = blipping.resolve(&handle).await.expect_err("unreadable");
    assert_eq!(err.kind, ErrorKind::OutcomeUnknown);
    assert_eq!(err.ids.operation.as_deref(), Some(handle.encode().as_str()));
    let before = blipping.counters();
    assert_eq!(before.total().put + before.total().cas, 0);
}

#[tokio::test]
async fn a_contended_cas_uses_the_version_of_its_own_re_read() {
    let mem = Arc::new(MemoryStore::new());
    let seed = store_with(mem.clone(), StoreLimits::default());
    let created = create(&seed).await;
    let fa = Arc::new(FaultStore::new(mem.clone(), FaultPlan::empty()));
    let gate = fa.hold(Op::Put, Some("/head".to_string()), Occurrence::Nth(1));
    let a = Arc::new(store_with(fa.clone(), StoreLimits::default()));
    let b = store_with(mem.clone(), StoreLimits::default());
    let a2 = a.clone();
    let h = created.head.clone();
    let task = tokio::spawn(async move { enqueue(&a2, &h, "ra", b"a").await });
    gate.wait_until_held(1).await;
    let bo = enqueue(&b, &created.head, "rb", b"b")
        .await
        .expect("b publishes first");
    for id in gate.held() {
        gate.release(id);
    }
    let ao = task
        .await
        .expect("join")
        .expect("a publishes after one conflict");
    let ca = a.counters();
    assert_eq!(ca.publish.conflict, 1, "a lost exactly one CAS to b");
    assert_eq!(ao.head.root_revision, bo.head.root_revision + 1);
    assert_eq!(ao.head.inbox.len(), 2, "both inputs survive the rebase");
}
