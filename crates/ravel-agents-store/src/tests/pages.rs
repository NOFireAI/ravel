//! Index pages through the store (ADR-3331 decision 3, the index-page
//! amendment): which pages each path reads, counted per phase, and when the
//! page objects are written relative to the HEAD CAS.

use ravel_agents_core::command::{EventSpec, FireTimer};
use ravel_agents_core::head::{FORMAT_VERSION, MAX_HEAD_BYTES, peek_format_version};
use ravel_agents_core::identity::Id128;
use ravel_agents_core::pages::{ListKind, Representation};
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
        fence: fence.clone(),
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
        assert_eq!(delta.store.put, 1 + written, "n={n}: the payload and the pages it sealed");
        assert_eq!(delta.store.get, 0);
        assert_eq!(delta.publish.cas, 1);

        let (again, delta) = measured_send(&store, "r0").await;
        let again = again.expect("repeat");
        assert!(!again.published);
        assert_eq!(accept_seq(&again), 1);
        assert_eq!(delta.resolve.get, 2, "n={n}: a repeated send reads one page");
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
                    fence: fence.clone(),
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
/// until `limit` events are in hand, never the pages before it.
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
    let read = store.read_events(&agent(), events - 1, 10).await.expect("tail");
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].event_seq, events);
    assert_eq!(store.counters().since(&before).resolve.get, 1);
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
    assert_eq!(sealed(&migrated, ListKind::Inbox), pages_for(3 * K as u64 + 1));
    assert_eq!(
        delta.store.put,
        1 + out.head.index.written.len() as u64,
        "the payload and every page the migration sealed"
    );
    let full = match paged.read_head(&agent()).await.expect("read") {
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
