//! The kernel's side of the index pages: the page reserve of the growth
//! guard, the migration of a version 1 HEAD, and the bound on HEAD with every
//! list at its page cap.

use super::*;
use crate::pages::{PageMeta, Representation};

/// Small pages, so a few dozen entries fill a directory.
fn paged(max_page_entries: usize, max_index_pages: usize) -> Limits {
    Limits {
        max_mailbox_depth: 100_000,
        max_receipts: 100_000,
        max_timers: 100_000,
        max_page_entries,
        max_index_pages,
        ..Limits::default()
    }
}

fn pages(head: &Head, list: ListKind) -> usize {
    head.index.directory(list).pages.len()
}

fn complete_one(sim: &mut Sim, events: usize) -> Result<Transition, AgentError> {
    sim.step(&begin_cmd(sim.head()))?;
    let mut complete = complete_cmd(sim.head(), b"s");
    complete.events = (0..events)
        .map(|e| EventSpec {
            event_type: format!("e{e}"),
            payload: None,
        })
        .collect();
    sim.step(&Command::CompleteTurn(complete))
}

/// A guarded publish is refused once the event pages the pending effects'
/// result publishes may still seal would take the event directory past
/// `Limits::max_index_pages`; at that point every pending effect's result is
/// still recorded, in either order, inside the cap.
#[test]
fn a_guarded_publish_keeps_the_index_page_reserve() {
    const K: usize = 4;
    const MAX_PAGES: usize = 3;
    for ascending in [true, false] {
        let mut sim = Sim::with_limits(paged(K, MAX_PAGES));
        sim.run(&create_cmd());
        sim.run(&takeover_cmd(sim.head(), "w"));
        sim.run(&enqueue_cmd(sim.head(), "r", b"p"));
        sim.run(&begin_cmd(sim.head()));

        let mut effects = Vec::new();
        let refusal = loop {
            let key = format!("k{}", effects.len());
            let commit = commit_intent_cmd(sim.head(), &key, RecoveryPolicy::Manual);
            let effect_id = match sim.step(&commit).map(|t| t.reply().clone()) {
                Ok(Reply::IntentCommitted { effect_id }) => effect_id,
                Ok(other) => panic!("{other:?}"),
                Err(e) => break e,
            };
            if let Err(e) = sim.step(&owner_authorize(&sim, effect_id, 1)) {
                break e;
            }
            // A second manual attempt puts the effect in doubt, and the
            // exempt publish of its result journals an audit event.
            let blocked = sim.run(&owner_authorize(&sim, effect_id, 2));
            assert!(matches!(blocked, Reply::Blocked(_)), "{blocked:?}");
            effects.push(effect_id);
        };
        assert_eq!(refusal.kind, ErrorKind::BudgetExceeded);
        assert_eq!(
            refusal.reason,
            "IndexPagesBelowCap: HEAD would enter the index page reserve"
        );
        let head = sim.head();
        let inline_events = head
            .events
            .iter()
            .filter(|e| head.index.events.is_inline(e.event_seq))
            .count();
        let reserved = sim
            .kernel
            .limits()
            .reserved_index_pages(head, ListKind::Events);
        assert_eq!(reserved, (inline_events + effects.len() - 1) / K);
        // No event yet, and 16 pending effects reserve (16 - 1) / 4 = 3
        // pages: the 17th intent would reserve a fourth.
        assert_eq!(inline_events, 0);
        assert_eq!(effects.len(), 16);
        assert_eq!(pages(head, ListKind::Events) + reserved, MAX_PAGES);

        if !ascending {
            effects.reverse();
        }
        for effect_id in effects {
            sim.step(&effect_result_cmd(sim.head(), effect_id, 1))
                .unwrap_or_else(|e| panic!("result (ascending {ascending}): {e:?}"));
        }
        assert_eq!(pending_effects(sim.head()), 0);
        for list in ListKind::ALL {
            assert!(pages(sim.head(), list) <= MAX_PAGES, "{list:?}");
        }
        // The 16 audit events the exempt publishes appended filled exactly
        // the reserved pages.
        assert_eq!(sim.head().next_event_seq, 17);
        assert_eq!(pages(sim.head(), ListKind::Events), MAX_PAGES);
    }
}

/// The bytes the guard reserves per page cover the real growth of a sealed
/// page. Measured two ways: a directory grown one widest page reference at a
/// time never grows by more than [`max_page_ref_bytes`], the first page (which
/// also adds the directory field) included; and the drain of a HEAD filled
/// under the byte guard, with pages of 2 so the drain seals a result page every
/// other input, publishes every terminal command under the cap.
#[test]
fn the_index_reserve_covers_the_exempt_page_growth() {
    let mut sim = Sim::new();
    sim.run(&create_cmd());
    let base = sim.head().clone();
    for list in [ListKind::Results, ListKind::Events] {
        for k in [1, 4, 64, 256] {
            let mut head = base.clone();
            let mut size = head.encode().len();
            let reserve = max_page_ref_bytes(list, k);
            let mut widest_growth = 0;
            let mut first_growth = 0;
            for i in 0..200u64 {
                let dir = head.index.directory_mut(list);
                dir.pages.push(PageMeta {
                    first_key: (1 << 63) + i,
                    page: ObjectRef {
                        hash: crate::identity::ObjectHash([u8::MAX; 32]),
                        size_bytes: u64::MAX,
                    },
                    entries: u32::MAX,
                    marked: u32::MAX,
                    min_due_unix_ns: i64::MIN,
                    referenced_bytes: u64::MAX,
                    fingerprints: if list.fingerprinted() {
                        vec![u32::MAX; k]
                    } else {
                        Vec::new()
                    },
                });
                dir.loaded.push(true);
                dir.tail_from = u64::MAX;
                let grown = head.encode().len();
                if i == 0 {
                    first_growth = grown - size;
                }
                widest_growth = widest_growth.max(grown - size);
                size = grown;
            }
            assert!(
                widest_growth <= reserve,
                "{list:?} k={k}: {widest_growth} > {reserve}"
            );
            // Not so loose that it hides a missing term: the first page, which
            // also adds the directory field, comes within a few bytes of it.
            assert_eq!(widest_growth, first_growth, "{list:?} k={k}");
            assert!(
                reserve - first_growth <= 4,
                "{list:?} k={k}: {first_growth} of {reserve}"
            );
        }
    }

    let mut drained = 0;
    for cap in (1200..=8192).step_by(256) {
        let limits = Limits {
            max_page_entries: 2,
            max_index_pages: 512,
            ..byte_capped(cap)
        };
        drained += drain_after_worst_exempt_growth(
            limits,
            "h",
            limits.max_owner_bytes,
            limits.max_version_bytes,
        )
        .unwrap_or_else(|e| panic!("cap {cap}: {e}"));
    }
    assert!(drained >= 100, "{drained}");
}

/// A version 1 HEAD as large as the byte guard lets one grow, every list
/// inline, moves to the paged representation in the one CAS of the next
/// command, under the byte cap and inside `Limits::max_index_pages`, and the
/// HEAD that CAS writes is version 2.
#[test]
fn one_cas_migrates_a_maximal_inline_head_under_the_byte_cap() {
    let limits = Limits::default();
    // Pages that never seal stand in for the inline representation while the
    // HEAD grows.
    let mut sim = Sim::with_limits(Limits {
        max_page_entries: 1 << 20,
        ..paged(1 << 20, limits.max_index_pages)
    });
    sim.run(&create_cmd());
    sim.run(&takeover_cmd(sim.head(), "w"));
    for _ in 0..40 {
        sim.run(&enqueue_cmd(
            sim.head(),
            &format!("t{}", sim.head().next_accept_seq),
            b"p",
        ));
        complete_one(&mut sim, 8).expect("turn");
    }
    let accepted = fill_under_the_guard(&mut sim, "h");
    let mut inline = sim.head().clone();
    inline.index = Index::inline();
    let v1 = inline.encode_version_one();
    assert_eq!(
        crate::head::peek_format_version(&v1),
        Ok(crate::head::INLINE_FORMAT_VERSION)
    );
    assert!(v1.len() <= limits.max_head_bytes);
    assert!(
        v1.len() > limits.max_head_bytes - limits.reserved_bytes(&inline) - 1024,
        "the HEAD is not maximal: {} of {}",
        v1.len(),
        limits.max_head_bytes
    );
    let decoded = Head::decode(&v1).expect("version 1 decodes");
    assert!(decoded.index.any_inline());

    let mut sim = Sim::with_limits(limits);
    sim.head = Some(decoded.clone());
    let first = decoded.inbox[0].accept_seq;
    let reply = sim.run(&cancel_cmd(&decoded, first));
    assert_eq!(reply, Reply::Cancelled { accept_seq: first });
    let migrated = sim.head();
    let encoded = migrated.encode();
    assert_eq!(
        crate::head::peek_format_version(&encoded),
        Ok(crate::head::FORMAT_VERSION)
    );
    assert!(encoded.len() <= limits.max_head_bytes);
    assert!(encoded.len() < v1.len());
    let k = limits.max_page_entries as u64;
    let mut sealed = 0;
    for list in ListKind::ALL {
        let dir = migrated.index.directory(list);
        assert_eq!(dir.representation, Representation::Paged, "{list:?}");
        assert!(dir.pages.len() <= limits.max_index_pages);
        sealed += dir.pages.len();
    }
    assert_eq!(migrated.index.written.len(), sealed);
    assert_eq!(
        pages(migrated, ListKind::Inbox) as u64,
        (accepted as u64 - 2) / k
    );
    assert_eq!(
        pages(migrated, ListKind::Receipts) as u64,
        (decoded.receipts.len() as u64 - 1) / k
    );
    assert_eq!(
        pages(migrated, ListKind::Events) as u64,
        (decoded.events.len() as u64 - 1) / k
    );
    // Every timer the fill scheduled is still scheduled, so the timer list
    // stays inline: timers seal only below the lowest scheduled generation.
    assert_eq!(pages(migrated, ListKind::Timers), 0);
    assert!(!decoded.timers.is_empty());
    assert_eq!(Head::decode(&encoded).expect("decodes").encode(), encoded);
}

/// Drive `limits` until no command applies: each round fires one timer,
/// accepts one input and runs every pending input to a result with a full
/// page of events. Returns the largest HEAD any publish wrote and the
/// refusals.
fn grow_every_list(sim: &mut Sim) -> (usize, Vec<AgentError>) {
    let k = sim.kernel.limits().max_page_entries;
    sim.run(&create_cmd());
    sim.run(&takeover_cmd(sim.head(), "w"));
    let mut refusals = Vec::new();
    let mut largest = 0;
    for round in 0.. {
        let mut applied = false;
        let key = format!("t{round}");
        let schedule = Command::ScheduleTimer(ScheduleTimer {
            fence: inc_fence(sim.head()),
            timer: TimerSpec {
                timer_key: key.clone(),
                due_unix_ns: 0,
                handler: "h".to_string(),
                payload: None,
            },
        });
        match sim.step(&schedule).map(|t| t.reply().clone()) {
            Ok(Reply::TimerScheduled { generation, .. }) => {
                let fire = Command::FireTimer(crate::command::FireTimer {
                    fence: inc_fence(sim.head()),
                    timer_id: identity::timer_id(sim.head().incarnation, &key),
                    generation,
                });
                match sim.step(&fire) {
                    Ok(_) => applied = true,
                    Err(e) => refusals.push(e),
                }
            }
            Ok(other) => panic!("{other:?}"),
            Err(e) => refusals.push(e),
        }
        match sim.step(&enqueue_cmd(sim.head(), &format!("r{round}"), b"p")) {
            Ok(_) => applied = true,
            Err(e) => refusals.push(e),
        }
        largest = largest.max(sim.head().encode().len());
        while sim.head().inbox_depth() > 0 {
            if let Err(e) = complete_one(sim, k) {
                refusals.push(e);
                sim.run(&fail_cmd(sim.head(), true, 1));
            }
            largest = largest.max(sim.head().encode().len());
        }
        if !applied {
            break;
        }
    }
    (largest, refusals)
}

/// HEAD stays under `Limits::max_head_bytes` with the lists at
/// `Limits::max_index_pages`. Under a page cap that binds first, the result
/// and event directories reach the cap (receipts and timers cannot at the same
/// time: every receipt and every fired timer is also a result); under a byte
/// cap that binds first, the growth guard's reserve refuses the guarded
/// publishes and every exempt one still fits. Every refusal is
/// `BudgetExceeded` and the final HEAD decodes to the bytes it encodes.
#[test]
fn head_stays_under_the_cap_with_every_list_at_max_pages() {
    const K: usize = 4;
    const MAX_PAGES: usize = 6;
    let page_capped = paged(K, MAX_PAGES);
    let mut sim = Sim::with_limits(page_capped);
    let (largest, refusals) = grow_every_list(&mut sim);
    assert!(largest <= page_capped.max_head_bytes, "{largest}");
    assert!(
        refusals.iter().all(|e| e.kind == ErrorKind::BudgetExceeded),
        "{refusals:?}"
    );
    let head = sim.head();
    for list in [ListKind::Results, ListKind::Events] {
        assert_eq!(pages(head, list), MAX_PAGES, "{list:?}");
    }
    for list in ListKind::ALL {
        assert!(pages(head, list) <= MAX_PAGES, "{list:?}");
    }
    let encoded = head.encode();
    assert_eq!(Head::decode(&encoded).expect("decodes").encode(), encoded);

    for cap in [3072, 4096, 6144] {
        let byte_capped = Limits {
            max_page_entries: K,
            max_index_pages: 512,
            ..byte_capped(cap)
        };
        let mut sim = Sim::with_limits(byte_capped);
        let (largest, refusals) = grow_every_list(&mut sim);
        assert!(largest <= cap, "cap {cap}: {largest}");
        assert!(
            refusals.iter().all(|e| e.kind == ErrorKind::BudgetExceeded),
            "{refusals:?}"
        );
        assert!(
            refusals
                .iter()
                .any(|e| e.reason == "HEAD would enter the terminal reserve of its byte cap"),
            "cap {cap}: the byte guard never bound"
        );
        let head = sim.head();
        let sealed: usize = ListKind::ALL.iter().map(|&l| pages(head, l)).sum();
        assert!(sealed >= MAX_PAGES, "cap {cap}: {sealed}");
        let encoded = head.encode();
        assert_eq!(Head::decode(&encoded).expect("decodes").encode(), encoded);
    }
}
