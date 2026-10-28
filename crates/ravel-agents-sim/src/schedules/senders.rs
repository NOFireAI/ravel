//! Clients sending to one agent: competing senders, duplicates and
//! conflicts on one request id, and a send whose HEAD write landed with its
//! acknowledgement lost.

use ravel_agents_core::error::ErrorKind;
use ravel_agents_core::state::Handle;
use ravel_agents_core::wire::OperationStatus;
use ravel_object_store::fault::{FaultKind, FaultPlan, Occurrence, Op, Rule, ScriptedFault};

use super::*;
use crate::drive::{agent, one_terminal_result_per_input};
use crate::history::History;
use crate::reference::{ClientOp, Event, Reply};
use crate::scheduler::Action;
use crate::{sim_assert, sim_assert_eq};

fn decode(token: &str) -> Result<Handle, String> {
    Handle::decode(token).map_err(|e| format!("handle {token}: {e:?}"))
}

fn op(seq: u64) -> Handle {
    Handle::operation(agent(), 1, seq)
}

fn is_send(event: &Event, id: &str) -> bool {
    matches!(event, Event::Client(ClientOp::Send { request_id, .. }) if request_id == id)
}

fn is_resolve(event: &Event, id: &str) -> bool {
    matches!(event, Event::Client(ClientOp::Resolve { request_id, .. }) if request_id == id)
}

/// The replies recorded for events matching `pick`, in history order.
fn replies(history: &History, pick: impl Fn(&Event) -> bool) -> Vec<Reply> {
    history
        .calls()
        .iter()
        .filter(|c| pick(&c.event))
        .map(|c| c.observed.clone())
        .collect()
}

async fn send_then_read(p: Proc, id: String, n: u64) -> Result<(), String> {
    let sent = p
        .send(&id, &payload(n))
        .await
        .map_err(|e| format!("send {id}: {e:?}"))?;
    p.operation(&decode(&sent.operation)?)
        .await
        .map_err(|e| format!("operation {id}: {e:?}"))?;
    Ok(())
}

/// Three clients each send one request and read its status back. Returns
/// the head CAS conflicts the run took.
fn competing(seed: u64, script: ScheduleScript) -> u64 {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    let actors = (0..3u64)
        .map(|i| {
            client(&rig, &format!("c{i}"), 10 + i, move |p| {
                send_then_read(p, format!("r{i}"), i)
            })
        })
        .collect();
    let out = run(&rig, seed, script, actors).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert!(out, run.refusals.is_empty(), "refusals {:?}", run.refusals);
    sim_assert!(out, run.inferred.is_empty(), "inferred {:?}", run.inferred);
    let head = rig.head().expect("head");
    let mut seqs: Vec<u64> = head.receipts.iter().map(|r| r.accept_seq).collect();
    seqs.sort_unstable();
    sim_assert_eq!(out, seqs, vec![1, 2, 3]);
    for i in 0..3u64 {
        let accepted = replies(&run.history, |e| is_send(e, &format!("r{i}")));
        let seq = head
            .receipts
            .iter()
            .find(|r| r.request_id == format!("r{i}"))
            .map(|r| r.accept_seq)
            .expect("receipt");
        sim_assert_eq!(
            out,
            accepted,
            vec![Reply::Accepted {
                operation: op(seq),
                duplicate: false
            }]
        );
    }
    // Every head CAS either applied one of the three sends or conflicted,
    // and every head write that reached the store is one of those or the
    // setup's create.
    let (cas, conflict) = rig.publish_totals();
    sim_assert_eq!(out, cas, 3 + conflict);
    sim_assert_eq!(out, rig.stamps.head_puts(), 1 + cas);
    conflict
}

/// Each client in turn reaches its next store call: all three read the same
/// root, so two CASes conflict on the first round and one on the second.
#[test]
fn competing_senders_take_distinct_accept_seqs() {
    let script = ScheduleScript::new(
        (0..12)
            .flat_map(|_| [0, 1, 2])
            .map(ScriptStep::Release)
            .collect(),
    );
    assert_eq!(competing(1, script), 3);
}

#[test]
fn competing_senders_across_seeds() {
    let mut runs = 0;
    let mut conflicts = 0;
    for seed in seeds() {
        conflicts += competing(seed, seeded(seed, 3, 40, 0));
        runs += 1;
    }
    assert_eq!(runs, 64);
    assert_eq!(conflicts, COMPETING_CONFLICTS);
}

/// Head CAS conflicts summed over `competing_senders_across_seeds`. Pinned:
/// a scheduler or production change that alters how often senders collide
/// moves it.
const COMPETING_CONFLICTS: u64 = 108;

/// One client sends `r1` twice with the same payload.
#[test]
fn same_request_twice_is_a_duplicate() {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    let c = client(&rig, "c", 10, |p| async move {
        p.send("r1", &payload(1))
            .await
            .map_err(|e| format!("{e:?}"))?;
        p.send("r1", &payload(1))
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        replies(&run.history, |e| is_send(e, "r1")),
        vec![
            Reply::Accepted {
                operation: op(1),
                duplicate: false
            },
            Reply::Accepted {
                operation: op(1),
                duplicate: true
            },
        ]
    );
    // The duplicate is answered from the receipt without a second CAS.
    sim_assert_eq!(out, rig.publish_totals(), (1, 0));
}

/// One client reuses `r1` with a different payload, then resolves it.
#[test]
fn same_id_different_payload_conflicts() {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    let c = client(&rig, "c", 10, |p| async move {
        p.send("r1", &payload(1))
            .await
            .map_err(|e| format!("{e:?}"))?;
        match p.send("r1", &payload(2)).await {
            Err(e) if e.kind == ErrorKind::IdempotencyConflict => {}
            other => return Err(format!("second send: {other:?}")),
        }
        p.resolve("r1").await.map_err(|e| format!("{e:?}"))?;
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        replies(&run.history, |e| is_send(e, "r1")),
        vec![
            Reply::Accepted {
                operation: op(1),
                duplicate: false
            },
            Reply::Error(ErrorKind::IdempotencyConflict),
        ]
    );
    sim_assert_eq!(
        out,
        replies(&run.history, |e| is_resolve(e, "r1")),
        vec![Reply::Resolved(Some((op(1), OperationStatus::Accepted)))]
    );
}

/// Two clients send `r1` with payload 1 and one with payload 2, at once.
/// Whichever lands first decides which of the others is a duplicate and
/// which a conflict. Returns `(duplicates, conflicts)`.
fn dup_and_conflict(seed: u64, script: ScheduleScript) -> (usize, usize) {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    let actors = [1u64, 1, 2]
        .into_iter()
        .enumerate()
        .map(|(i, n)| {
            client(&rig, &format!("c{i}"), 10 + i as u64, move |p| async move {
                match p.send("r1", &payload(n)).await {
                    Ok(_) => Ok(()),
                    Err(e) if e.kind == ErrorKind::IdempotencyConflict => Ok(()),
                    Err(e) => Err(format!("send: {e:?}")),
                }
            })
        })
        .collect();
    let out = run(&rig, seed, script, actors).expect("schedule");
    let run = checked(&rig, &out);
    let all = replies(&run.history, |e| is_send(e, "r1"));
    let first = Reply::Accepted {
        operation: op(1),
        duplicate: false,
    };
    let dup = Reply::Accepted {
        operation: op(1),
        duplicate: true,
    };
    let conflict = Reply::Error(ErrorKind::IdempotencyConflict);
    sim_assert_eq!(out, all.len(), 3);
    sim_assert_eq!(out, all.iter().filter(|r| **r == first).count(), 1);
    let dups = all.iter().filter(|r| **r == dup).count();
    let conflicts = all.iter().filter(|r| **r == conflict).count();
    sim_assert_eq!(out, dups + conflicts, 2);
    sim_assert_eq!(out, rig.head().expect("head").next_accept_seq, 2);
    let (cas, conflict_count) = rig.publish_totals();
    sim_assert_eq!(out, cas, 1 + conflict_count);
    (dups, conflicts)
}

#[test]
fn duplicate_and_conflict_across_seeds() {
    let mut runs = 0;
    let mut dups = 0;
    let mut conflicts = 0;
    for seed in seeds() {
        let (d, c) = dup_and_conflict(seed, seeded(seed, 3, 40, 0));
        dups += d;
        conflicts += c;
        runs += 1;
    }
    assert_eq!(runs, 64);
    // A payload-1 win gives one duplicate and one conflict, a payload-2 win
    // two conflicts: payload 1 won 36 of the 64 runs.
    assert_eq!((dups, conflicts), DUP_AND_CONFLICT);
}

const DUP_AND_CONFLICT: (usize, usize) = (36, 92);

/// Which settle answer a lost-acknowledgement send is checked with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settle {
    /// What production answered.
    Production,
    /// A driver that settles by reading HEAD once and taking the newest
    /// receipt, rather than the receipt of its own request.
    NewestReceipt,
}

struct LostAck {
    out: ScheduleOutcome,
    run: Run,
    faults: Faults,
    rig: Rig,
}

/// Client `a` sends `r1` and its HEAD CAS applies with the acknowledgement
/// lost. `b` sends `r2` and `r3` and worker `w` runs up to three turns.
/// `a` then settles, resolves `r1` and reads op 1's result.
fn lost_ack(seed: u64, script: ScheduleScript, gated: bool) -> LostAck {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    // Every HEAD write of a's loses its acknowledgement. One that conflicts
    // returns the conflict unchanged, so exactly one applies, whatever the
    // interleaving; the settle that follows writes nothing.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/head")
            .with_occurrence(Occurrence::Always),
    );
    let (a, faults) = faulty(&rig, Role::Client, "a", 10, plan, |p| async move {
        let sent = p
            .send("r1", &payload(1))
            .await
            .map_err(|e| format!("send: {e:?}"))?;
        p.resolve("r1")
            .await
            .map_err(|e| format!("resolve: {e:?}"))?;
        p.result(&decode(&sent.operation)?)
            .await
            .map_err(|e| format!("result: {e:?}"))?;
        Ok(())
    });
    let b = client(&rig, "b", 20, |p| async move {
        for (id, n) in [("r2", 2), ("r3", 3)] {
            p.send(id, &payload(n))
                .await
                .map_err(|e| format!("send {id}: {e:?}"))?;
        }
        Ok(())
    });
    let w = worker(&rig, "w", 30, |p| async move {
        p.work(3).await;
        Ok(())
    });
    let actors = if gated {
        // a stops at its first settle read (load, publish read, settle);
        // b and w stop at their first call, so the script runs b's sends,
        // then w's turns, then a's settle.
        vec![
            a.with_gates(vec![gate(Op::Get, "/head", 3)]),
            b.with_gates(vec![gate(Op::Get, "/head", 1)]),
            w.with_gates(vec![gate(Op::List, "", 1)]),
        ]
    } else {
        vec![a, b, w]
    };
    let out = run(&rig, seed, script, actors).expect("schedule");
    let run = rig.build().expect("build");
    LostAck {
        out,
        run,
        faults,
        rig,
    }
}

/// The history with `a`'s send answer replaced by what `settle` answers.
fn settled(l: &LostAck, settle: Settle) -> History {
    let newest = l
        .rig
        .head()
        .expect("head")
        .receipts
        .iter()
        .map(|r| r.accept_seq)
        .max()
        .expect("a receipt");
    let mut history = History::default();
    for call in l.run.history.calls() {
        let observed = match (settle, &call.observed) {
            (Settle::NewestReceipt, Reply::Accepted { duplicate, .. })
                if is_send(&call.event, "r1") =>
            {
                Reply::Accepted {
                    operation: op(newest),
                    duplicate: *duplicate,
                }
            }
            _ => call.observed.clone(),
        };
        history.record(call.event.clone(), observed);
    }
    history
}

fn fixed_lost_ack() -> LostAck {
    lost_ack(
        1,
        ScheduleScript::new(vec![
            ScriptStep::Release(1),
            ScriptStep::Release(2),
            ScriptStep::Release(0),
        ]),
        true,
    )
}

#[test]
fn lost_cas_response_followed_by_later_revisions_still_resolves_the_same_operation() {
    let l = fixed_lost_ack();
    let out = &l.out;
    sim_assert!(*out, l.run.unmappable.is_empty(), "{:?}", l.run.unmappable);
    sim_assert!(*out, l.run.refusals.is_empty(), "{:?}", l.run.refusals);
    let history = settled(&l, Settle::Production);
    history
        .check(crate::drive::machine().expect("machine"))
        .unwrap_or_else(|d| panic!("{d}"));
    // The checker holds the send to its own operation and first acceptance:
    // the same history answering op 3, or a duplicate, diverges at the send.
    for wrong in [
        Reply::Accepted {
            operation: op(3),
            duplicate: false,
        },
        Reply::Accepted {
            operation: op(1),
            duplicate: true,
        },
    ] {
        let mut swapped = History::default();
        for call in history.calls() {
            let observed = if is_send(&call.event, "r1") {
                wrong.clone()
            } else {
                call.observed.clone()
            };
            swapped.record(call.event.clone(), observed);
        }
        let divergence = swapped
            .check(crate::drive::machine().expect("machine"))
            .expect_err("a wrong answer to the lost send must diverge");
        sim_assert!(*out, is_send(&divergence.event, "r1"), "{divergence}");
    }
    sim_assert_eq!(
        *out,
        replies(&l.run.history, |e| is_send(e, "r1")),
        vec![Reply::Accepted {
            operation: op(1),
            duplicate: false
        }]
    );
    sim_assert_eq!(
        *out,
        replies(&l.run.history, |e| is_resolve(e, "r1")),
        vec![Reply::Resolved(Some((op(1), OperationStatus::Completed)))]
    );
    // b ran both sends and w all three turns before a's settle read was
    // released.
    sim_assert_eq!(
        *out,
        out.order(),
        vec![
            ("b".to_string(), Action::Released, Op::Get),
            ("w".to_string(), Action::Released, Op::List),
            ("a".to_string(), Action::Released, Op::Get),
        ]
    );
    let head = l.rig.head().expect("head");
    sim_assert_eq!(*out, head.root_revision, ROOT_AFTER_LOST_ACK);
    one_terminal_result_per_input(&head).unwrap_or_else(|e| panic!("{e}"));
    sim_assert_eq!(
        *out,
        all_fired(&l.faults),
        vec![((Op::Put, FaultKind::DuplicateDelivery), 1)]
    );
    let a = l.rig.proc_named("a").expect("a");
    sim_assert_eq!(*out, a.co.metrics().outcome_unknown_resolved, 1);
    // One CAS for a (whose answer was lost), two for b, and for w one
    // takeover plus a begin and a commit for each of three turns.
    sim_assert_eq!(*out, l.rig.publish_totals(), (10, 0));
}

/// create, a's send, b's two sends, w's takeover, then three begin and
/// three commit writes.
const ROOT_AFTER_LOST_ACK: u64 = 11;

/// The same schedule settled by a driver that takes the newest receipt
/// answers op 3 for `r1`, and the checker refuses it at that send.
#[test]
fn a_driver_taking_the_newest_receipt_diverges() {
    let l = fixed_lost_ack();
    let wrong = settled(&l, Settle::NewestReceipt);
    let divergence = wrong
        .check(crate::drive::machine().expect("machine"))
        .expect_err("the newest receipt is not r1's");
    assert!(is_send(&divergence.event, "r1"), "{divergence}");
    assert_eq!(
        divergence.expected,
        Reply::Accepted {
            operation: op(1),
            duplicate: false
        }
    );
    assert_eq!(
        divergence.observed,
        Reply::Accepted {
            operation: op(3),
            duplicate: false
        }
    );
}

#[test]
fn lost_cas_response_across_seeds() {
    let mut runs = 0;
    let mut accepted_seqs = Vec::new();
    let mut a_conflicts = 0;
    for seed in seeds() {
        let l = lost_ack(seed, seeded(seed, 3, 120, 0), false);
        let out = &l.out;
        sim_assert!(*out, l.run.unmappable.is_empty(), "{:?}", l.run.unmappable);
        sim_assert!(*out, l.run.refusals.is_empty(), "{:?}", l.run.refusals);
        if let Err(d) = l.run.check() {
            panic!(
                "{d}\n{}",
                crate::scheduler::FailureContext::failure_context(out)
            );
        }
        let sent = replies(&l.run.history, |e| is_send(e, "r1"));
        let Some(Reply::Accepted {
            operation,
            duplicate: false,
        }) = sent.first().cloned()
        else {
            panic!("a's send answered {sent:?}");
        };
        let resolved = replies(&l.run.history, |e| is_resolve(e, "r1"));
        let Some(Reply::Resolved(Some((resolved_op, _)))) = resolved.first().cloned() else {
            panic!("a's resolve answered {resolved:?}");
        };
        sim_assert_eq!(*out, resolved_op, operation);
        // Every CAS a made was faulted: the conflicting ones came back as
        // conflicts and the one that applied lost its answer.
        let a = l.rig.proc_named("a").expect("a");
        let a_publish = a.store.counters().publish;
        sim_assert_eq!(*out, a_publish.cas, 1 + a_publish.conflict);
        sim_assert_eq!(
            *out,
            all_fired(&l.faults),
            vec![((Op::Put, FaultKind::DuplicateDelivery), a_publish.cas)]
        );
        sim_assert_eq!(*out, a.co.metrics().outcome_unknown_resolved, 1);
        a_conflicts += a_publish.conflict;
        if let Reply::Accepted { operation, .. } = &sent[0]
            && let ravel_agents_core::state::HandleItem::Operation(seq) = operation.item
        {
            accepted_seqs.push(seq);
        }
        runs += 1;
    }
    assert_eq!(runs, 64);
    // How often a's lost write landed first, second or third.
    let landed = |n| accepted_seqs.iter().filter(|s| **s == n).count();
    assert_eq!((landed(1), landed(2), landed(3)), LOST_ACK_POSITIONS);
    assert_eq!(a_conflicts, LOST_ACK_CONFLICTS);
}

const LOST_ACK_POSITIONS: (usize, usize, usize) = (40, 13, 11);
/// Conflicts a's faulted CASes took, summed over the batch.
const LOST_ACK_CONFLICTS: u64 = 45;
