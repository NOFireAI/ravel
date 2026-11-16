//! Index pages through the store (ADR-3331 decision 3, the index-page
//! amendment): which pages each path reads, counted per phase, and when the
//! page objects are written relative to the HEAD CAS.

use ravel_agents_core::command::{Authority, AuthorizeAttempt, EventSpec, FireTimer, IntentSpec};
use ravel_agents_core::head::{
    FORMAT_VERSION, MAX_HEAD_BYTES, RecoveryPolicy, peek_format_version,
};
use ravel_agents_core::identity::Id128;
use ravel_agents_core::kernel::AUDIT_RECONCILE_ATTEMPT;
use ravel_agents_core::pages::{ListKind, Need, Representation, receipt_fingerprint};
use ravel_object_store::fault::{Sequence, SequenceStep};

use crate::counters::CounterSnapshot;

use super::*;

/// `Limits::max_page_entries` for every test here, small so a few dozen
/// entries make many pages.
const K: usize = 4;

fn paged_limits() -> StoreLimits {
    StoreLimits {
        kernel: Limits {
            max_mailbox_depth: 1024,
            max_receipts: 1024,
            max_page_entries: K,
            ..Limits::default()
        },
        ..StoreLimits::default()
    }
}

/// HEAD as stored, without its sealed pages.
async fn shallow(store: &AgentStore) -> Head {
    match store.read_head_state(&agent(), true).await.expect("read") {
        HeadState::Present { head, .. } => *head,
        other => panic!("expected a HEAD, got {other:?}"),
    }
}

fn sealed(head: &Head, list: ListKind) -> usize {
    head.index.directory(list).pages.len()
}

/// The sealed page count the seal rule gives a list of `n` entries: the open
/// page seals `K` at a time while it holds more than `K`.
fn pages_for(n: u64) -> usize {
    (n.saturating_sub(1) / K as u64) as usize
}

async fn send(store: &AgentStore, rid: &str) -> Result<Outcome, AgentError> {
    measured_send(store, rid).await.0
}

/// A send, with the requests it made: the caller's own read of HEAD to build
/// the command is not counted.
async fn measured_send(
    store: &AgentStore,
    rid: &str,
) -> (Result<Outcome, AgentError>, CounterSnapshot) {
    let head = shallow(store).await;
    let payload = format!("payload-{rid}");
    let before = store.counters();
    let out = enqueue(store, &head, rid, payload.as_bytes()).await;
    (out, store.counters().since(&before))
}

/// Schedule a timer due now and fire it: one more delivered timer in the
/// history, one more input in the inbox.
async fn schedule_and_fire(store: &AgentStore, n: usize) {
    let head = shallow(store).await;
    let fence = IncarnationFence {
        incarnation: head.incarnation,
    };
    let command = Command::ScheduleTimer(ScheduleTimer {
        fence,
        timer: TimerSpec {
            timer_key: format!("t{n}"),
            due_unix_ns: T0,
            handler: "h".to_string(),
            payload: None,
        },
    });
    let out = store.execute(&agent(), &command).await.expect("schedule");
    let Reply::TimerScheduled {
        timer_id,
        generation,
    } = out.reply
    else {
        panic!("expected TimerScheduled, got {:?}", out.reply);
    };
    let fire = fire_cmd(fence, timer_id, generation);
    let out = store.execute(&agent(), &fire).await.expect("fire");
    assert!(matches!(out.reply, Reply::TimerFired { .. }));
}

fn fire_cmd(fence: IncarnationFence, timer_id: Id128, generation: u64) -> Command {
    Command::FireTimer(FireTimer {
        fence,
        timer_id,
        generation,
    })
}

/// `n` inputs sent as `r0..rn` and `n` timers scheduled and fired.
async fn grow(store: &AgentStore, n: usize) -> Head {
    create(store).await;
    for i in 0..n {
        send(store, &format!("r{i}")).await.expect("send");
    }
    for i in 0..n {
        schedule_and_fire(store, i).await;
    }
    let head = shallow(store).await;
    // Every list is paged exactly as the seal rule says, so none of the
    // counts below is read off a HEAD that happens to hold its list inline.
    assert_eq!(sealed(&head, ListKind::Inbox), pages_for(2 * n as u64));
    assert_eq!(sealed(&head, ListKind::Receipts), pages_for(n as u64));
    assert_eq!(sealed(&head, ListKind::Timers), pages_for(n as u64));
    assert!(sealed(&head, ListKind::Receipts) >= 3);
    head
}

/// The acceptance bound of the amendment, at two history sizes (3 and 12
/// sealed receipt pages): the counts are the same at both.
///
/// - A send of a new request reads HEAD and no page: its receipt
///   fingerprint is in no page's fingerprint set, and the append goes to the
///   open page.
/// - A send that repeats a request reads HEAD and the one receipt page whose
///   fingerprint set lists it, and writes nothing.
/// - A receipt lookup reads HEAD and that one page, or no page for a pair
///   never sent.
/// - Wake (the due-timer read and the `FireTimer` CAS) reads HEAD and no
///   timer page: a sealed timer page never holds a scheduled timer.
#[tokio::test]
async fn send_receipt_lookup_and_wake_read_at_most_one_index_page_each() {
    for n in [3 * K + 1, 12 * K + 1] {
        let store = store_with(Arc::new(MemoryStore::new()), paged_limits());
        grow(&store, n).await;

        let (sent, delta) = measured_send(&store, "new").await;
        let sent = sent.expect("send");
        assert!(sent.published);
        assert_eq!(delta.resolve.get, 1, "n={n}: a new send reads HEAD only");
        let written = sent.head.index.written.len() as u64;
        assert!(
            sent.head
                .index
                .written
                .iter()
                .all(|w| matches!(w.list, ListKind::Inbox | ListKind::Receipts))
        );
        assert_eq!(
            delta.store.put,
            1 + written,
            "n={n}: the payload and the pages it sealed"
        );
        assert_eq!(delta.store.get, 0);
        assert_eq!(delta.publish.cas, 1);

        let (again, delta) = measured_send(&store, "r0").await;
        let again = again.expect("repeat");
        assert!(!again.published);
        assert_eq!(accept_seq(&again), 1);
        assert_eq!(
            delta.resolve.get, 2,
            "n={n}: a repeated send reads one page"
        );
        assert_eq!(delta.store.requests(), 0);
        assert_eq!(delta.publish.requests(), 0);

        let before = store.counters();
        let receipt = store
            .find_receipt(&agent(), "h", "r1")
            .await
            .expect("lookup")
            .expect("r1 was sent");
        assert_eq!(receipt.accept_seq, 2);
        assert_eq!(store.counters().since(&before).resolve.get, 2, "n={n}");
        assert_eq!(store.counters().since(&before).total().requests(), 2);

        let before = store.counters();
        let missing = store
            .find_receipt(&agent(), "h", "never")
            .await
            .expect("lookup");
        assert_eq!(missing, None);
        assert_eq!(store.counters().since(&before).resolve.get, 1, "n={n}");

        let head = shallow(&store).await;
        let fence = IncarnationFence {
            incarnation: head.incarnation,
        };
        let out = store
            .execute(
                &agent(),
                &Command::ScheduleTimer(ScheduleTimer {
                    fence,
                    timer: TimerSpec {
                        timer_key: "wake".to_string(),
                        due_unix_ns: T0,
                        handler: "h".to_string(),
                        payload: None,
                    },
                }),
            )
            .await
            .expect("schedule");
        let Reply::TimerScheduled {
            timer_id,
            generation,
        } = out.reply
        else {
            panic!("expected TimerScheduled");
        };
        let before = store.counters();
        let due = store.due_timers(&agent(), T0).await.expect("due");
        assert_eq!(due.len(), 1);
        assert_eq!(store.counters().since(&before).resolve.get, 1, "n={n}");

        let before = store.counters();
        let fired = store
            .execute(&agent(), &fire_cmd(fence, timer_id, generation))
            .await
            .expect("fire");
        assert!(matches!(fired.reply, Reply::TimerFired { .. }));
        let delta = store.counters().since(&before);
        assert_eq!(delta.resolve.get, 1, "n={n}: a fire reads no timer page");
        assert_eq!(delta.publish.cas, 1);
    }
}

/// Events are read by `event_seq` range: from the page holding `after + 1`
/// until `limit` events below the next page's first key are in hand, never
/// the pages before it. A result
/// the route layer reads by `accept_seq` costs the one page holding it.
#[tokio::test]
async fn events_are_read_by_seq_range() {
    let store = store_with(Arc::new(MemoryStore::new()), paged_limits());
    create(&store).await;
    let turns = 10;
    for i in 0..turns {
        send(&store, &format!("r{i}")).await.expect("send");
    }
    let head = shallow(&store).await;
    let (takeover, _) = command(&Step::Takeover, Some(&head), T0);
    store.execute(&agent(), &takeover).await.expect("takeover");
    for _ in 0..turns {
        let head = shallow(&store).await;
        let (begin, _) = command(&Step::Begin, Some(&head), T0);
        let begun = store.execute(&agent(), &begin).await.expect("begin");
        let (Command::CompleteTurn(mut complete), deps) =
            command(&Step::Complete { state: 0 }, Some(&begun.head), T0)
        else {
            panic!("not a turn commit");
        };
        complete.events = (0..3)
            .map(|e| EventSpec {
                event_type: format!("e{e}"),
                payload: None,
            })
            .collect();
        store
            .execute_with(&agent(), &Command::CompleteTurn(complete), &deps)
            .await
            .expect("complete");
    }
    let head = shallow(&store).await;
    let events = 3 * turns as u64;
    assert_eq!(sealed(&head, ListKind::Events), pages_for(events));
    assert_eq!(
        head.events.first().map(|e| e.event_seq),
        Some(pages_for(events) as u64 * K as u64 + 1)
    );

    // The events after the 17th are on pages 4 and 5 (event_seq is
    // 1-based, pages of 4): two pages for six events.
    let before = store.counters();
    let read = store.read_events(&agent(), 17, 6).await.expect("events");
    let seqs: Vec<u64> = read.iter().map(|e| e.event_seq).collect();
    assert_eq!(seqs, (18..24).collect::<Vec<_>>());
    assert_eq!(store.counters().since(&before).resolve.get, 1 + 2);

    // Past every sealed page: HEAD only.
    let before = store.counters();
    let read = store
        .read_events(&agent(), events - 1, 10)
        .await
        .expect("tail");
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].event_seq, events);
    assert_eq!(store.counters().since(&before).resolve.get, 1);

    // A cursor in sealed history with the open page holding at least
    // `limit` events: the open page's events are newer than the sealed
    // pages, so they do not count toward the page being read.
    let open = head.events.len();
    assert!(open >= 2, "the open page holds {open} events");
    let before = store.counters();
    let read = store.read_events(&agent(), 0, 1).await.expect("oldest");
    let seqs: Vec<u64> = read.iter().map(|e| e.event_seq).collect();
    assert_eq!(seqs, vec![1]);
    assert_eq!(store.counters().since(&before).resolve.get, 1 + 1);

    // A result read by accept_seq: one GET for a sealed result, none for
    // one still on the open page.
    let mut head = shallow(&store).await;
    assert_eq!(sealed(&head, ListKind::Results), pages_for(turns as u64));
    let before = store.counters();
    store
        .attach(&agent(), &mut head, &[(ListKind::Results, Need::Key(1))])
        .await
        .expect("attach");
    assert!(head.results.iter().any(|r| r.accept_seq == 1));
    assert_eq!(store.counters().since(&before).resolve.get, 1);
    let before = store.counters();
    store
        .attach(
            &agent(),
            &mut head,
            &[(ListKind::Results, Need::Key(turns as u64))],
        )
        .await
        .expect("attach");
    assert!(head.results.iter().any(|r| r.accept_seq == turns as u64));
    assert_eq!(store.counters().since(&before).total().requests(), 0);
}

/// Begin a turn on the oldest input and complete it with `events` events and
/// `intents`; returns the input's `accept_seq`.
async fn run_turn(store: &AgentStore, events: usize, intents: Vec<IntentSpec>) -> u64 {
    let head = shallow(store).await;
    let (begin, _) = command(&Step::Begin, Some(&head), T0);
    let begun = store.execute(&agent(), &begin).await.expect("begin");
    let accept_seq = begun
        .head
        .active_turn
        .as_ref()
        .map(|t| t.input_accept_seq)
        .expect("a turn began");
    let (Command::CompleteTurn(mut complete), deps) =
        command(&Step::Complete { state: 0 }, Some(&begun.head), T0)
    else {
        panic!("not a turn commit");
    };
    complete.events = (0..events)
        .map(|e| EventSpec {
            event_type: format!("e{e}"),
            payload: None,
        })
        .collect();
    complete.intents = intents;
    store
        .execute_with(&agent(), &Command::CompleteTurn(complete), &deps)
        .await
        .expect("complete");
    accept_seq
}

/// Create the agent, send `n` inputs and take it over.
async fn ready(store: &AgentStore, n: usize) {
    create(store).await;
    for i in 0..n {
        send(store, &format!("r{i}")).await.expect("send");
    }
    let head = shallow(store).await;
    let (takeover, _) = command(&Step::Takeover, Some(&head), T0);
    store.execute(&agent(), &takeover).await.expect("takeover");
}

/// The `event_seq`s an operation-filtered read returns, and the GETs it
/// issued: the HEAD read plus every sealed page it loaded.
async fn operation_read(
    store: &AgentStore,
    accept_seq: u64,
    limit: usize,
) -> (Vec<EventEntry>, u64) {
    let before = store.counters();
    let head = shallow(store).await;
    let events = store
        .events_in(&agent(), head, 0, limit, Some(accept_seq))
        .await
        .expect("events");
    (events, store.counters().since(&before).resolve.get)
}

/// An operation-filtered events read GETs only the sealed pages whose
/// `accept_seq` range holds the operation. Twelve turns of `K` events each
/// put each operation's events alone on one page: eleven sealed pages and the
/// last turn's on the open page.
#[tokio::test]
async fn an_operation_read_skips_pages_outside_its_accept_range() {
    let store = store_with(Arc::new(MemoryStore::new()), paged_limits());
    let turns = 12;
    ready(&store, turns).await;
    let mut ops = Vec::new();
    for _ in 0..turns {
        ops.push(run_turn(&store, K, Vec::new()).await);
    }
    let head = shallow(&store).await;
    assert_eq!(sealed(&head, ListKind::Events), turns - 1);
    let ranges: Vec<(u64, u64)> = head
        .index
        .events
        .pages
        .iter()
        .map(|p| (p.min_accept_seq, p.max_accept_seq))
        .collect();
    let expected: Vec<(u64, u64)> = ops[..turns - 1].iter().map(|&a| (a, a)).collect();
    assert_eq!(ranges, expected);

    // The sixth operation's events are seqs 21..=24, alone on page 5. A limit
    // above its event count reads on looking for more: HEAD and page 5 only.
    let (events, gets) = operation_read(&store, ops[5], 10).await;
    let seqs: Vec<u64> = events.iter().map(|e| e.event_seq).collect();
    assert_eq!(seqs, (21..=24).collect::<Vec<_>>());
    assert_eq!(gets, 1 + 1);

    // A limit it reaches stops on the same page.
    let (events, gets) = operation_read(&store, ops[5], 2).await;
    let seqs: Vec<u64> = events.iter().map(|e| e.event_seq).collect();
    assert_eq!(seqs, [21, 22]);
    assert_eq!(gets, 1 + 1);

    // The last operation's events are all on the open page: HEAD only.
    let (events, gets) = operation_read(&store, ops[turns - 1], 10).await;
    assert_eq!(events.len(), K);
    assert_eq!(gets, 1);

    // An operation with no event at all: HEAD only, and nothing returned.
    let (events, gets) = operation_read(&store, ops[turns - 1] + 1, 10).await;
    assert!(events.is_empty());
    assert_eq!(gets, 1);
}

/// A reconciliation's audit event carries its effect's input, older than the
/// events before it, and widens its page's `accept_seq` range down to that
/// input. That page is still read for the older operation, and the audit
/// event is returned with the operation's own events.
#[tokio::test]
async fn an_audit_event_widens_its_page_range_and_is_read_for_its_operation() {
    let store = store_with(Arc::new(MemoryStore::new()), paged_limits());
    ready(&store, 5).await;
    let intent = IntentSpec {
        intent_key: "charge".to_string(),
        tool_name: "pay".to_string(),
        tool_version: "1".to_string(),
        input_hash: Hash256([7; 32]),
        policy: RecoveryPolicy::AtLeastOnce,
        input: None,
    };
    // Events 1..=4 (a), 5..=8 (b), 9..=12 (c).
    let a = run_turn(&store, K, vec![intent]).await;
    let b = run_turn(&store, K, Vec::new()).await;
    let c = run_turn(&store, K, Vec::new()).await;
    // An operator authorizes an attempt of a's effect: audit event 13,
    // carrying a's accept_seq, after b's and c's events.
    let head = shallow(&store).await;
    let effect_id = head.effects.first().expect("a's effect").effect_id;
    let reconcile = Command::AuthorizeAttempt(AuthorizeAttempt {
        authority: Authority::Reconcile(IncarnationFence {
            incarnation: head.incarnation,
        }),
        effect_id,
        attempt: 1,
    });
    store
        .execute(&agent(), &reconcile)
        .await
        .expect("reconcile");
    // Events 14..=16 (d) share the audit event's page; 17..=20 (e) seal it.
    let d = run_turn(&store, K - 1, Vec::new()).await;
    let e = run_turn(&store, K, Vec::new()).await;
    let head = shallow(&store).await;
    let ranges: Vec<(u64, u64)> = head
        .index
        .events
        .pages
        .iter()
        .map(|p| (p.min_accept_seq, p.max_accept_seq))
        .collect();
    assert_eq!(ranges, [(a, a), (b, b), (c, c), (a, d)]);
    assert!(head.events.iter().all(|ev| ev.accept_seq == e));

    // a: its own page and the widened one, and the audit event comes back.
    let (events, gets) = operation_read(&store, a, 10).await;
    let seqs: Vec<u64> = events.iter().map(|ev| ev.event_seq).collect();
    assert_eq!(seqs, [1, 2, 3, 4, 13]);
    assert_eq!(events[4].event_type, AUDIT_RECONCILE_ATTEMPT);
    assert_eq!(gets, 1 + 2);
    // b's range lies inside the widened page's too, which costs a read that
    // finds none of its events.
    let (events, gets) = operation_read(&store, b, 10).await;
    let seqs: Vec<u64> = events.iter().map(|ev| ev.event_seq).collect();
    assert_eq!(seqs, [5, 6, 7, 8]);
    assert_eq!(gets, 1 + 2);
    // d: the widened page alone.
    let (events, gets) = operation_read(&store, d, 10).await;
    let seqs: Vec<u64> = events.iter().map(|ev| ev.event_seq).collect();
    assert_eq!(seqs, [14, 15, 16]);
    assert_eq!(gets, 1 + 1);
}

/// A CAS refused once: the retry re-reads HEAD, re-runs the kernel, and seals
/// the same entries into byte-identical pages, so it writes no new page: its
/// `CreateIfAbsent` meets the object the first attempt wrote under the same
/// hash, reads it back, and the HEAD that lands references it.
#[tokio::test]
async fn a_lost_cas_reuses_the_page_hash_on_retry() {
    let backend = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(head_rule(
            Op::Put,
            ScriptedFault::FailedConditionalWrite,
            // Create, then K sends, then the send that seals.
            K as u64 + 2,
        )),
    ));
    let store = store_with(backend.clone(), paged_limits());
    create(&store).await;
    for i in 0..K {
        send(&store, &format!("r{i}")).await.expect("send");
    }
    let (sealing, delta) = measured_send(&store, "seal").await;
    let sealing = sealing.expect("send after a lost CAS");
    assert_eq!(
        backend.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1
    );
    // One inbox page and one receipt page, the same pair on both attempts.
    let written: Vec<ObjectRef> = sealing.head.index.written.iter().map(|w| w.page).collect();
    assert_eq!(written.len(), 2);
    assert_eq!(delta.publish.cas, 2);
    assert_eq!(delta.publish.conflict, 1);
    // Attempt one: the payload and two pages. Attempt two: the same three,
    // each `AlreadyExists` and read back.
    assert_eq!(delta.store.put, 6);
    assert_eq!(delta.store.conflict, 3);
    assert_eq!(delta.store.get, 3);
    assert_eq!(delta.resolve.get, 2);
    let head = shallow(&store).await;
    let referenced: Vec<ObjectRef> = head.index.page_refs().collect();
    assert_eq!(referenced, written);
}

/// Every page a HEAD references is in the store before the CAS that first
/// references it: with every CAS refused, the send fails and leaves its
/// pages behind, under a HEAD that does not name them.
#[tokio::test]
async fn pages_are_written_before_the_cas_that_references_them() {
    // Create and K sends pass; every attempt of the send that seals fails.
    let mut steps = vec![SequenceStep::Passthrough; K + 1];
    steps.resize(
        K + 1 + MAX_CAS_ATTEMPTS as usize,
        SequenceStep::Fault(ScriptedFault::FailedConditionalWrite),
    );
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Put)
            .with_key_contains("/head")
            .with_steps(steps),
    );
    let backend = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store = store_with(backend.clone(), paged_limits());
    create(&store).await;
    for i in 0..K {
        send(&store, &format!("r{i}")).await.expect("send");
    }
    let mut head = shallow(&store).await;
    let error = send(&store, "seal").await.expect_err("every CAS refused");
    assert_eq!(error.kind, ErrorKind::StoreUnavailable);
    assert_eq!(
        backend.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        u64::from(MAX_CAS_ATTEMPTS)
    );
    assert_eq!(shallow(&store).await, head);

    // The pages the refused HEAD would have referenced: seal the same
    // transition locally and read each one back.
    let payload = Bytes::from_static(b"payload-seal");
    let command = Command::Enqueue(Enqueue {
        fence: IncarnationFence {
            incarnation: head.incarnation,
        },
        request_id: "seal".to_string(),
        handler: "h".to_string(),
        request_hash: request_hash("h", &payload),
        payload: Some(ObjectRef::of(&payload)),
    });
    head.index.written.clear();
    let Ok(Transition::Publish { head: new, .. }) =
        Kernel::new(paged_limits().kernel).apply(Some(&head), &command, T0, &DerivedIds)
    else {
        panic!("the send publishes");
    };
    assert_eq!(new.index.written.len(), 2);
    for page in &new.index.written {
        let bytes = store.read_object(&agent(), page.page).await.expect("page");
        assert_eq!(bytes.as_ref(), page.bytes.as_slice());
    }
}

/// Two request ids whose receipt fingerprints collide, found by search over
/// `c0, c1, ...` for handler `h`.
const COLLIDING: [&str; 2] = ["c24337", "c53026"];

/// A fingerprint names candidate pages, not an entry: two receipts sealed
/// into one page under the same fingerprint are each found by their own
/// `(handler, request_id)`, at one page read each, and a repeated send of
/// the second resolves to its own operation.
#[tokio::test]
async fn colliding_receipt_fingerprints_resolve_to_their_own_receipts() {
    let fp = receipt_fingerprint("h", COLLIDING[0]);
    assert_eq!(fp, receipt_fingerprint("h", COLLIDING[1]));
    let store = store_with(Arc::new(MemoryStore::new()), paged_limits());
    create(&store).await;
    for rid in COLLIDING.iter().copied().chain(["f0", "f1", "f2"]) {
        send(&store, rid).await.expect("send");
    }
    let head = shallow(&store).await;
    let pages = &head.index.receipts.pages;
    assert_eq!(pages.len(), 1);
    assert_eq!(
        pages[0].fingerprints.iter().filter(|&&f| f == fp).count(),
        2,
        "one page lists the fingerprint twice"
    );
    let open: Vec<&str> = head
        .receipts
        .iter()
        .map(|r| r.request_id.as_str())
        .collect();
    assert_eq!(open, vec!["f2"], "both colliding receipts are sealed");

    for (rid, seq) in COLLIDING.iter().zip([1, 2]) {
        let before = store.counters();
        let receipt = store
            .find_receipt(&agent(), "h", rid)
            .await
            .expect("lookup")
            .expect("sent");
        assert_eq!(receipt.request_id, *rid);
        assert_eq!(receipt.accept_seq, seq);
        assert_eq!(store.counters().since(&before).resolve.get, 2, "{rid}");
    }

    let again = send(&store, COLLIDING[1]).await.expect("repeat");
    assert!(!again.published);
    assert_eq!(accept_seq(&again), 2);
}

/// A store that plans no page (`AgentStore::without_page_plan`) stands in
/// for a planner that omits one: a repeated send whose receipt is sealed
/// finds the page missing, the miss is counted once, every page is loaded,
/// and the reply is the one the planned path gives.
#[tokio::test]
async fn a_planner_miss_loads_every_page_and_is_counted() {
    let backend = Arc::new(MemoryStore::new());
    let planned = store_with(backend.clone(), paged_limits());
    let head = grow(&planned, 3 * K + 1).await;
    let pages = head.index.page_refs().count() as u64;

    let (out, delta) = measured_send(&planned, "r0").await;
    let out = out.expect("planned repeat");
    assert_eq!(delta.planner_misses, 0);
    assert_eq!(delta.resolve.get, 2);
    let planned_reply = out.reply;

    let blind = store_with(backend.clone(), paged_limits()).without_page_plan();
    let (out, delta) = measured_send(&blind, "r0").await;
    let out = out.expect("repeat after a miss");
    assert_eq!(delta.planner_misses, 1);
    assert_eq!(delta.resolve.get, 1 + pages, "HEAD, then every page");
    assert!(!out.published);
    assert_eq!(accept_seq(&out), 1);
    assert_eq!(out.reply, planned_reply);
    assert_eq!(delta.publish.requests(), 0);
}

/// A version 1 HEAD, every list inline and longer than one page, moves to
/// the paged representation on the next command's own CAS; the HEAD it
/// writes is version 2, under the byte cap, and every page it references is
/// in the store. A HEAD already paged is left as it is by a command that
/// touches none of its pages.
#[tokio::test]
async fn a_version_one_head_migrates_on_the_next_cas() {
    let backend = Arc::new(MemoryStore::new());
    let store = store_with(backend.clone(), StoreLimits::default());
    create(&store).await;
    for i in 0..(3 * K) {
        send(&store, &format!("r{i}")).await.expect("send");
    }
    let inline = shallow(&store).await;
    assert_eq!(sealed(&inline, ListKind::Inbox), 0);
    let key = head_key(&tenant(), &agent()).expect("head key");
    backend
        .put(
            &key,
            Bytes::from(inline.encode_version_one()),
            PutOptions::default(),
        )
        .await
        .expect("seed version 1");
    let seeded = shallow(&store).await;
    assert!(seeded.index.any_inline());

    let paged = store_with(backend.clone(), paged_limits());
    let before = paged.counters();
    let out = send(&paged, "migrate").await.expect("send");
    let delta = paged.counters().since(&before);
    assert_eq!(delta.publish.cas, 1);
    let stored = backend.get(&key, GetRange::Full).await.expect("get");
    assert_eq!(peek_format_version(&stored.data), Ok(FORMAT_VERSION));
    assert!(stored.data.len() <= MAX_HEAD_BYTES);
    let migrated = shallow(&paged).await;
    for list in ListKind::ALL {
        assert_eq!(
            migrated.index.directory(list).representation,
            Representation::Paged
        );
    }
    assert_eq!(
        sealed(&migrated, ListKind::Inbox),
        pages_for(3 * K as u64 + 1)
    );
    assert_eq!(
        delta.store.put,
        1 + out.head.index.written.len() as u64,
        "the payload and every page the migration sealed"
    );
    let full = match paged.read_head_full(&agent()).await.expect("read") {
        HeadState::Present { head, .. } => *head,
        other => panic!("{other:?}"),
    };
    assert_eq!(full.inbox_depth(), 3 * K as u64 + 1);
    assert_eq!(full.inbox.len(), 3 * K + 1);

    // Already paged: a send of a new request writes no page it did not seal.
    let before = paged.counters();
    let out = send(&paged, "after").await.expect("send");
    assert_eq!(
        paged.counters().since(&before).store.put,
        1 + out.head.index.written.len() as u64
    );
    assert_eq!(out.head.index.written.len(), 0);
}
