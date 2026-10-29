//! Typed errors. Each schedule provokes one refusal, asserts its kind, the
//! call that answered it and the store traffic it cost, and checks that the
//! rest of the history still passes the reference.

use bytes::Bytes;
use ravel_agents_core::error::ErrorKind;
use ravel_agents_core::head::ObjectRef;
use ravel_agents_core::kernel::Limits;
use ravel_agents_core::keys::object_key;
use ravel_agents_core::state::Handle;
use ravel_agents_core::wire::{AgentRef, HandlerPinBody, SendRequest};
use ravel_agents_store::StoreLimits;
use ravel_agents_store::counters::CounterSnapshot;
use ravel_object_store::fault::{FaultKind, FaultPlan, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_types::TenantHash;

use super::*;
use crate::drive::{HANDLER, Refusal, TENANT, agent, head_object_key, json_bytes, pin};
use crate::reference::Event;
use crate::{sim_assert, sim_assert_eq};

fn op(seq: u64) -> Handle {
    Handle::operation(agent(), 1, seq)
}

fn refusal(actor: &str, call: &'static str, kind: ErrorKind) -> Refusal {
    Refusal {
        actor: actor.to_string(),
        call,
        kind,
    }
}

/// Run `body` against the rig's memory store, under every fault and
/// recorder.
fn behind_the_store<F, Fut>(rig: &Rig, body: F)
where
    F: FnOnce(Arc<ravel_object_store::memory::MemoryStore>) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    runtime
        .block_on(body(Arc::clone(&rig.memory)))
        .expect("store edit");
}

fn overwrite_head(rig: &Rig, bytes: Vec<u8>) {
    let key = head_object_key().expect("head key");
    behind_the_store(rig, |m| async move {
        m.put(&key, Bytes::from(bytes), PutOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| format!("{e}"))
    });
}

/// The key of input `n`'s payload object.
fn payload_key(n: u64) -> String {
    let hash = ObjectRef::of(&json_bytes(&payload(n))).hash;
    object_key(&TENANT, &agent(), &hash).expect("object key")
}

/// One worker polls and works one input; returns the outcome.
fn one_worker(rig: &Rig) -> ScheduleOutcome {
    let w = worker(rig, "w", 10, |p| async move {
        p.work(1).await;
        Ok(())
    });
    run(rig, 1, ScheduleScript::new(Vec::new()), vec![w]).expect("schedule")
}

/// A payload object whose bytes come back corrupted. BeginTurn applied
/// before the read, so the poll's `TurnStarted` is inferred from the root.
#[test]
fn corrupt_dependency_is_store_unavailable() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::CorruptRange)
            .with_key_contains("/o/")
            .with_occurrence(Occurrence::Always),
    );
    let rig = Rig::new(plan);
    create_with_inputs(&rig, 1);
    let out = one_worker(&rig);
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        run.refusals,
        vec![refusal("w", "poll", ErrorKind::StoreUnavailable)]
    );
    sim_assert_eq!(
        out,
        run.inferred,
        vec![("w".to_string(), Event::TurnStarted { operation: op(1) })]
    );
    sim_assert_eq!(
        out,
        rig.rules.fault_count(Op::Get, FaultKind::CorruptRange),
        1
    );
    let w = rig.proc_named("w").expect("w").co.metrics();
    sim_assert_eq!(out, w.turns_committed, 0);
}

/// A payload object deleted under the root that names it.
#[test]
fn missing_dependency_is_store_unavailable() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let key = payload_key(1);
    behind_the_store(&rig, |m| async move {
        m.delete(&key).await.map_err(|e| format!("{e}"))
    });
    let out = one_worker(&rig);
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        run.refusals,
        vec![refusal("w", "poll", ErrorKind::StoreUnavailable)]
    );
    sim_assert_eq!(
        out,
        run.inferred,
        vec![("w".to_string(), Event::TurnStarted { operation: op(1) })]
    );
}

/// A HEAD written by a newer release: `format_version` 3 and an unknown
/// field this release cannot read.
#[test]
fn unsupported_head_format_is_refused() {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    overwrite_head(&rig, vec![0x08, 0x03, 0xf8, 0x3f, 0x01]);
    let c = client(&rig, "c", 20, |p| async move {
        match p.send("r1", &payload(1)).await {
            Err(e) if e.kind == ErrorKind::UnsupportedFormat => Ok(()),
            other => Err(format!("send: {other:?}")),
        }
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        run.refusals,
        vec![refusal("c", "send", ErrorKind::UnsupportedFormat)]
    );
    let c = rig.proc_named("c").expect("c");
    sim_assert_eq!(out, c.store.counters().publish.cas, 0);
}

/// A HEAD under this tenant's key whose body names another tenant.
#[test]
fn tenant_mismatch_is_store_unavailable() {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    let mut head = rig.head().expect("head");
    head.tenant = TenantHash([8; 16]);
    overwrite_head(&rig, head.encode());
    let c = client(&rig, "c", 20, |p| async move {
        match p.send("r1", &payload(1)).await {
            Err(e) if e.kind == ErrorKind::StoreUnavailable => Ok(()),
            other => Err(format!("send: {other:?}")),
        }
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        run.refusals,
        vec![refusal("c", "send", ErrorKind::StoreUnavailable)]
    );
    let c = rig.proc_named("c").expect("c");
    sim_assert_eq!(out, c.store.counters().publish.cas, 0);
}

/// One hostile send's answer and the HEAD reads it cost.
type HostileAnswer = (Result<(), ErrorKind>, u64);

/// Hostile agent names: path separators, traversal, NUL, empty and
/// oversized parts. Each answers `AgentGone`, writes nothing, and reads at
/// most the HEAD key of its own hashed agent id, never a key spelled from
/// the name. Returns `(kind, HEAD reads)` per name, in order.
fn hostile_sends() -> (ScheduleOutcome, Rig, Vec<HostileAnswer>) {
    let rig = Rig::new(FaultPlan::empty());
    create(&rig);
    let names: Vec<(String, String)> = [
        ("support", ""),
        ("support", "../../ag/x"),
        ("support", "a/b"),
        ("support", "nul\0byte"),
        ("", "ticket-7"),
        ("../support", "ticket-7"),
    ]
    .iter()
    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
    .chain([("support".to_string(), "k".repeat(4096))])
    .collect();
    let answers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&answers);
    let c = client(&rig, "c", 20, move |p| async move {
        for (kind, key) in names {
            let req = SendRequest {
                agent: AgentRef { kind, key },
                handler: HANDLER.to_string(),
                request_id: "r1".to_string(),
                payload: payload(1),
            };
            let before = p.store.counters().resolve.get;
            let answer = p.co.send(&req).await.map(|_| ()).map_err(|e| e.kind);
            let reads = p.store.counters().resolve.get - before;
            sink.lock()
                .map_err(|e| e.to_string())?
                .push((answer, reads));
        }
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let answers = answers.lock().expect("answers").clone();
    (out, rig, answers)
}

#[test]
fn hostile_keys_are_agent_gone() {
    let (out, rig, answers) = hostile_sends();
    checked(&rig, &out);
    sim_assert_eq!(out, answers, HOSTILE_ANSWERS.to_vec());
    let c = rig.proc_named("c").expect("c").store.counters();
    sim_assert_eq!(
        out,
        CounterSnapshot {
            resolve: Default::default(),
            ..c
        },
        CounterSnapshot::default()
    );
    // Only the setup's create wrote HEAD.
    sim_assert_eq!(out, rig.stamps.head_puts(), 1);
    let ours = head_object_key().expect("head key");
    let prefix = format!("t/{}/ag/", TENANT.to_hex());
    for t in &out.trace {
        let id = t
            .key
            .strip_prefix(&prefix)
            .and_then(|rest| rest.strip_suffix("/head"));
        sim_assert!(
            out,
            t.op == Op::Get
                && t.key != ours
                && id.is_some_and(|id| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit())),
            "{t:?}"
        );
    }
}

/// Per hostile name, in [`hostile_sends`] order: the answer and the HEAD
/// reads it cost. An empty part and the oversized key are refused before
/// any store call; the rest are hashed into an agent id nobody created.
const HOSTILE_ANSWERS: [HostileAnswer; 7] = [
    (Err(ErrorKind::AgentGone), 0),
    (Err(ErrorKind::AgentGone), 3),
    (Err(ErrorKind::AgentGone), 3),
    (Err(ErrorKind::AgentGone), 3),
    (Err(ErrorKind::AgentGone), 0),
    (Err(ErrorKind::AgentGone), 3),
    (Err(ErrorKind::AgentGone), 0),
];

/// A payload past the per-object cap is refused before any store call.
#[test]
fn oversized_payload_is_budget_exceeded() {
    let limits = StoreLimits {
        max_object_bytes: 64,
        ..StoreLimits::default()
    };
    let rig = Rig::with_limits(FaultPlan::empty(), limits);
    create(&rig);
    let c = client(&rig, "c", 20, |p| async move {
        let big = serde_json::json!({ "n": "x".repeat(128) });
        match p.send("r1", &big).await {
            Err(e) if e.kind == ErrorKind::BudgetExceeded => {}
            other => return Err(format!("big send: {other:?}")),
        }
        p.send("r2", &payload(2))
            .await
            .map_err(|e| format!("send: {e:?}"))?;
        Ok(())
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        run.refusals,
        vec![refusal("c", "send", ErrorKind::BudgetExceeded)]
    );
    // The refused send cost nothing: one CAS, the second send's.
    let c = rig.proc_named("c").expect("c");
    sim_assert_eq!(out, c.store.counters().publish.cas, 1);
    sim_assert_eq!(out, rig.head().expect("head").next_accept_seq, 2);
}

/// A send to a mailbox at its depth limit.
#[test]
fn full_mailbox_is_refused() {
    let limits = StoreLimits {
        kernel: Limits {
            max_mailbox_depth: 2,
            ..Limits::default()
        },
        ..StoreLimits::default()
    };
    let rig = Rig::with_limits(FaultPlan::empty(), limits);
    create(&rig);
    let c = client(&rig, "c", 20, |p| async move {
        for i in 1..=2 {
            p.send(&format!("r{i}"), &payload(i))
                .await
                .map_err(|e| format!("send r{i}: {e:?}"))?;
        }
        match p.send("r3", &payload(3)).await {
            Err(e) if e.kind == ErrorKind::MailboxFull => Ok(()),
            other => Err(format!("send r3: {other:?}")),
        }
    });
    let out = run(&rig, 1, ScheduleScript::new(Vec::new()), vec![c]).expect("schedule");
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        run.refusals,
        vec![refusal("c", "send", ErrorKind::MailboxFull)]
    );
    let head = rig.head().expect("head");
    sim_assert_eq!(out, head.next_accept_seq, 3);
    sim_assert_eq!(out, head.inbox.len(), 2);
    let c = rig.proc_named("c").expect("c");
    sim_assert_eq!(out, c.store.counters().publish.cas, 2);
}

/// Two clients contend for a mailbox two deep under a 64-byte object cap:
/// `a` sends `a1`, `a2`; `b` sends `b1`, an oversized `b2`, `b3`. Exactly
/// two sends are accepted, two answer `MailboxFull` and `b2` answers
/// `BudgetExceeded`, whatever the order. Returns `(a's accepts, HEAD
/// conflicts)`.
fn contended_refusals(seed: u64) -> (usize, u64) {
    let limits = StoreLimits {
        max_object_bytes: 64,
        kernel: Limits {
            max_mailbox_depth: 2,
            ..Limits::default()
        },
    };
    let rig = Rig::with_limits(FaultPlan::empty(), limits);
    create(&rig);
    let big = serde_json::json!({ "n": "x".repeat(128) });
    let sends = |sends: Vec<(&'static str, Value)>| {
        move |p: crate::drive::Proc| async move {
            for (id, body) in sends {
                match p.send(id, &body).await {
                    Ok(_) => {}
                    Err(e)
                        if matches!(e.kind, ErrorKind::MailboxFull | ErrorKind::BudgetExceeded) => {
                    }
                    Err(e) => return Err(format!("send {id}: {e:?}")),
                }
            }
            Ok(())
        }
    };
    let a = client(
        &rig,
        "a",
        20,
        sends(vec![("a1", payload(1)), ("a2", payload(2))]),
    );
    let b = client(
        &rig,
        "b",
        30,
        sends(vec![("b1", payload(3)), ("b2", big), ("b3", payload(4))]),
    );
    let out = run(&rig, seed, seeded(seed, 2, 60, 0), vec![a, b]).expect("schedule");
    let run = checked(&rig, &out);
    let kinds = |kind: ErrorKind| run.refusals.iter().filter(|r| r.kind == kind).count();
    sim_assert_eq!(out, kinds(ErrorKind::MailboxFull), 2);
    sim_assert_eq!(
        out,
        run.refusals
            .iter()
            .filter(|r| r.kind == ErrorKind::BudgetExceeded)
            .map(|r| r.actor.as_str())
            .collect::<Vec<_>>(),
        vec!["b"]
    );
    sim_assert_eq!(out, run.refusals.len(), 3);
    let head = rig.head().expect("head");
    sim_assert_eq!(out, head.next_accept_seq, 3);
    sim_assert_eq!(out, head.inbox.len(), 2);
    let a = rig.proc_named("a").expect("a").store.counters().publish;
    let b = rig.proc_named("b").expect("b").store.counters().publish;
    // Two accepts applied; every other CAS conflicted.
    sim_assert_eq!(out, a.cas + b.cas, 2 + a.conflict + b.conflict);
    let a_accepts = 2 - run.refusals.iter().filter(|r| r.actor == "a").count();
    (a_accepts, a.conflict + b.conflict)
}

#[test]
fn contended_refusals_across_seeds() {
    let mut runs = 0;
    let mut a_accepts = 0;
    let mut conflicts = 0;
    for seed in seeds() {
        let (a, c) = contended_refusals(seed);
        a_accepts += a;
        conflicts += c;
        runs += 1;
    }
    pinned(
        "contended_refusals",
        runs,
        (a_accepts, conflicts),
        CONTENDED_REFUSALS,
    );
}

/// `a`'s accepts and the HEAD conflicts, summed over the batch.
const CONTENDED_REFUSALS: (usize, u64) = (63, 85);

/// `w1` begins op 1 under `v1` and stalls past its lease. `w2` presents
/// only `v2`, so it cannot resume the turn `v1` is pinned to.
#[test]
fn poll_with_mismatched_pins_is_incompatible_handler() {
    let rig = Rig::new(FaultPlan::empty());
    create_with_inputs(&rig, 1);
    let w1 = worker(&rig, "w1", 10, |p| async move {
        p.poll().await.map_err(|e| format!("poll: {e:?}"))?;
        Ok(())
    });
    run(&rig, 1, ScheduleScript::new(Vec::new()), vec![w1]).expect("first worker");
    rig.clock.advance_ms(crate::drive::PAST_LEASE_MS);
    let w2 = worker(&rig, "w2", 20, |p| async move {
        let other = HandlerPinBody {
            version: "v2".to_string(),
            ..pin()
        };
        match p.poll_with(vec![other]).await {
            Err(e) if e.kind == ErrorKind::IncompatibleHandler => Ok(()),
            other => Err(format!("poll: {other:?}")),
        }
    });
    let out = run(&rig, 2, ScheduleScript::new(Vec::new()), vec![w2]).expect("second worker");
    let run = checked(&rig, &out);
    sim_assert_eq!(
        out,
        run.refusals,
        vec![refusal("w2", "poll", ErrorKind::IncompatibleHandler)]
    );
    sim_assert!(out, run.inferred.is_empty(), "{:?}", run.inferred);
    let w2 = rig.proc_named("w2").expect("w2").co.metrics();
    sim_assert_eq!(
        out,
        (w2.incompatible_handler_refusals, w2.turns_committed),
        (1, 0)
    );
    let head = rig.head().expect("head");
    sim_assert_eq!(out, head.active_turn.map(|t| t.input_accept_seq), Some(1));
}
