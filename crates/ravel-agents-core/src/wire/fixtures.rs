//! Golden JSON fixtures for every wire body, shared with the TypeScript SDK.
//!
//! `RAVEL_UPDATE_WIRE_FIXTURES=1 cargo test -p ravel-agents-core wire::`
//! rewrites `sdk/fixtures/`; without it the test asserts each file matches
//! byte for byte, decodes to the sample, and that the directory holds no
//! file this list does not name.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

use super::*;
use crate::error::{AgentError, ErrorKind, SafeIds};
use crate::identity::{self, AgentId, Id128};
use crate::keys::{AgentKey, AgentKind};
use crate::state::{Handle, HandleItem};

const UPDATE_VAR: &str = "RAVEL_UPDATE_WIRE_FIXTURES";

fn fixtures_dir() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../sdk/fixtures"))
}

fn agent() -> AgentRef {
    AgentRef {
        kind: "order".to_string(),
        key: "o-1".to_string(),
    }
}

fn agent_id() -> AgentId {
    identity::agent_id(
        &AgentKind::new("order").expect("kind"),
        &AgentKey::new("o-1").expect("key"),
    )
}

fn turn_id() -> Id128 {
    identity::turn_id(1, 1, 1, 0)
}

fn operation() -> String {
    Handle::operation(agent_id(), 1, 1).encode()
}

fn effect() -> String {
    Handle {
        agent: agent_id(),
        incarnation: 1,
        item: HandleItem::Effect(identity::effect_id(1, 1, "send-receipt")),
    }
    .encode()
}

fn lease() -> TurnLease {
    TurnLease {
        agent_id: agent_id().to_hex(),
        incarnation: 1,
        owner_id: "worker-a".to_string(),
        epoch: 1,
        turn_id: turn_id().to_hex(),
        base_state_version: 0,
        handler_version: "1.0.0".to_string(),
        handler_digest: identity::request_hash("digest", b"order-handler").to_hex(),
        observed_root_revision: 4,
    }
}

fn result_body() -> ResultBody {
    ResultBody {
        status: OperationStatus::Completed,
        output: Some(json!({"total_cents": 1299})),
        error: None,
        dead_lettered: false,
        finished_unix_ns: 1_767_225_600_000_000_000,
    }
}

type RoundTrip = Box<dyn Fn(&str) -> Result<bool, serde_json::Error>>;

struct Sample {
    name: &'static str,
    json: String,
    round_trips: RoundTrip,
}

fn sample<T>(name: &'static str, value: T) -> Sample
where
    T: Serialize + DeserializeOwned + PartialEq + 'static,
{
    let mut json = serde_json::to_string_pretty(&value).expect("serialize");
    json.push('\n');
    Sample {
        name,
        json,
        round_trips: Box::new(move |text| {
            let decoded: T = serde_json::from_str(text)?;
            Ok(decoded == value)
        }),
    }
}

fn samples() -> Vec<Sample> {
    let error = AgentError::new(
        ErrorKind::OutcomeUnknown,
        SafeIds {
            agent_id: Some(agent_id().to_hex()),
            incarnation: Some(1),
            operation: Some(operation()),
            ..SafeIds::default()
        },
        "store did not confirm the write",
    );
    vec![
        sample(
            "create_agent_request",
            CreateAgentRequest { agent: agent() },
        ),
        sample(
            "create_agent_response",
            CreateAgentResponse {
                agent_id: agent_id().to_hex(),
                incarnation: 1,
                created: true,
            },
        ),
        sample(
            "send_request",
            SendRequest {
                agent: agent(),
                handler: "place".to_string(),
                request_id: "req-1".to_string(),
                payload: json!({"sku": "A-1", "qty": 2}),
            },
        ),
        sample(
            "send_response",
            SendResponse {
                operation: operation(),
                accept_seq: 1,
                duplicate: false,
            },
        ),
        sample(
            "call_request",
            CallRequest {
                agent: agent(),
                handler: "place".to_string(),
                request_id: "req-1".to_string(),
                payload: json!({"sku": "A-1", "qty": 2}),
                wait_ms: 5_000,
            },
        ),
        sample(
            "call_response",
            CallResponse {
                operation: operation(),
                status: OperationStatus::Completed,
                result: Some(result_body()),
            },
        ),
        sample(
            "operation_response",
            OperationResponse {
                operation: operation(),
                status: OperationStatus::Blocked,
                accept_seq: 1,
                blocked_effect: Some(effect()),
            },
        ),
        sample(
            "result_response",
            ResultResponse {
                operation: operation(),
                result: Some(ResultBody {
                    status: OperationStatus::Failed,
                    output: None,
                    error: Some("card declined".to_string()),
                    dead_lettered: true,
                    finished_unix_ns: 1_767_225_600_000_000_000,
                }),
            },
        ),
        sample(
            "result_response_nanosecond",
            ResultResponse {
                operation: operation(),
                result: Some(ResultBody {
                    finished_unix_ns: PRECISE_UNIX_NS,
                    ..result_body()
                }),
            },
        ),
        sample(
            "resolve_request",
            ResolveRequest {
                agent: agent(),
                handler: "place".to_string(),
                request_id: "req-1".to_string(),
            },
        ),
        sample(
            "resolve_response",
            ResolveResponse {
                operation: Some(operation()),
                status: Some(OperationStatus::Running),
            },
        ),
        sample(
            "cancel_request",
            CancelRequest {
                operation: operation(),
            },
        ),
        sample(
            "cancel_response",
            CancelResponse {
                operation: operation(),
                status: OperationStatus::Running,
                requested: true,
            },
        ),
        sample(
            "state_response",
            StateResponse {
                agent_id: agent_id().to_hex(),
                incarnation: 1,
                state_version: 3,
                content_type: "application/json".to_string(),
                state: json!({"items": ["A-1"], "paid": false}),
            },
        ),
        sample(
            "events_request",
            EventsRequest {
                agent: agent(),
                cursor: None,
                limit: 100,
            },
        ),
        sample(
            "events_response",
            EventsResponse {
                events: vec![EventBody {
                    event_seq: 1,
                    accept_seq: 1,
                    turn_id: turn_id().to_hex(),
                    event_type: "order_placed".to_string(),
                    payload: json!({"sku": "A-1"}),
                }],
                next_cursor: "2".to_string(),
            },
        ),
        sample(
            "timer_request",
            TimerRequest {
                agent: agent(),
                timer_key: "payment-reminder".to_string(),
                due_unix_ns: 1_767_229_200_000_000_000,
                handler: "remind".to_string(),
                payload: json!(null),
            },
        ),
        sample(
            "timer_response",
            TimerResponse {
                timer: Handle {
                    agent: agent_id(),
                    incarnation: 1,
                    item: HandleItem::Timer(identity::timer_id(1, "payment-reminder")),
                }
                .encode(),
                generation: 1,
            },
        ),
        sample(
            "cancel_timer_request",
            CancelTimerRequest {
                agent: agent(),
                timer_key: "payment-reminder".to_string(),
            },
        ),
        sample(
            "cancel_timer_response",
            CancelTimerResponse {
                outcome: TimerCancelOutcome::AlreadyFired,
            },
        ),
        sample(
            "effects_response",
            EffectsResponse {
                effects: vec![EffectBody {
                    effect: effect(),
                    turn_id: turn_id().to_hex(),
                    tool_name: "payments.charge".to_string(),
                    tool_version: "2".to_string(),
                    policy: Policy::Manual,
                    status: EffectState::InDoubt,
                    attempt: 2,
                }],
            },
        ),
        sample(
            "effect_inspect_response",
            EffectInspectResponse {
                effect: EffectBody {
                    effect: effect(),
                    turn_id: turn_id().to_hex(),
                    tool_name: "payments.charge".to_string(),
                    tool_version: "2".to_string(),
                    policy: Policy::AtLeastOnce,
                    status: EffectState::Completed,
                    attempt: 2,
                },
                step_key: "charge".to_string(),
                result_attempt: Some(2),
                result: Some(json!({"charge_id": "ch_2"})),
                failure: None,
                attempts: vec![
                    EffectAttemptBody {
                        attempt: 1,
                        status: AttemptState::Superseded,
                    },
                    EffectAttemptBody {
                        attempt: 2,
                        status: AttemptState::Result,
                    },
                ],
                evidence: vec![EffectEvidenceBody {
                    attempt: 1,
                    result: json!({"charge_id": "ch_1"}),
                    failure: None,
                }],
            },
        ),
        sample(
            "effect_inspect_response_failure",
            EffectInspectResponse {
                effect: EffectBody {
                    effect: effect(),
                    turn_id: turn_id().to_hex(),
                    tool_name: "payments.charge".to_string(),
                    tool_version: "2".to_string(),
                    policy: Policy::AtLeastOnce,
                    status: EffectState::Completed,
                    attempt: 2,
                },
                step_key: "charge".to_string(),
                result_attempt: Some(2),
                result: None,
                failure: Some("card declined".to_string()),
                attempts: vec![
                    EffectAttemptBody {
                        attempt: 1,
                        status: AttemptState::Superseded,
                    },
                    EffectAttemptBody {
                        attempt: 2,
                        status: AttemptState::Result,
                    },
                ],
                evidence: vec![EffectEvidenceBody {
                    attempt: 1,
                    result: json!(null),
                    failure: Some("gateway timeout".to_string()),
                }],
            },
        ),
        sample(
            "reconcile_request",
            ReconcileRequest {
                effect: effect(),
                attempt: 2,
                decision: ReconcileDecision::RecordResult {
                    result: json!({"charge_id": "ch_1"}),
                },
            },
        ),
        sample(
            "reconcile_response",
            ReconcileResponse {
                effect: effect(),
                status: EffectState::Completed,
                attempt: 2,
            },
        ),
        sample(
            "worker_poll_request",
            WorkerPollRequest {
                worker_id: "worker-a".to_string(),
                handlers: vec![HandlerPinBody {
                    handler: "place".to_string(),
                    version: "1.0.0".to_string(),
                    digest: identity::request_hash("digest", b"order-handler").to_hex(),
                }],
                wait_ms: 20_000,
            },
        ),
        sample(
            "worker_poll_response",
            WorkerPollResponse {
                assignment: Some(
                    TurnAssignment {
                        lease: lease(),
                        agent: agent(),
                        operation: operation(),
                        accept_seq: 1,
                        handler: "place".to_string(),
                        state: json!({"items": []}),
                        payload: json!({"sku": "A-1", "qty": 2}),
                        steps: vec![StepBody {
                            step_id: identity::step_id(1, 1, "price").to_hex(),
                            step_key: "price".to_string(),
                            tool_name: "catalog.price".to_string(),
                            tool_version: "1".to_string(),
                            input_hash: identity::step_input_hash("catalog.price", "1", b"A-1")
                                .to_hex(),
                            status: StepState::Completed,
                            output: Some(json!({"cents": 649})),
                        }],
                    }
                    .polled(1_767_225_600_000_000_000),
                ),
            },
        ),
        sample(
            "worker_register_request",
            WorkerRegisterRequest {
                handlers: vec![HandlerPinBody {
                    handler: "place".to_string(),
                    version: "1.0.0".to_string(),
                    digest: identity::request_hash("digest", b"order-handler").to_hex(),
                }],
            },
        ),
        sample(
            "worker_register_response",
            WorkerRegisterResponse {
                worker_id: "w-00000000000000a1".to_string(),
            },
        ),
        sample(
            "worker_heartbeat_request",
            WorkerHeartbeatRequest { lease: lease() },
        ),
        sample(
            "worker_heartbeat_response",
            WorkerHeartbeatResponse {
                lease_expires_unix_ns: 1_767_225_630_000_000_000,
            },
        ),
        sample(
            "worker_commit_request",
            WorkerCommitRequest {
                lease: lease(),
                outcome: TurnOutcome::Completed {
                    state: json!({"items": ["A-1"], "paid": false}),
                    output: json!({"total_cents": 1299}),
                    events: vec![EventSpecBody {
                        event_type: "order_placed".to_string(),
                        payload: json!({"sku": "A-1"}),
                    }],
                    timers: vec![
                        TimerChangeBody::Schedule {
                            timer_key: "payment-reminder".to_string(),
                            due_unix_ns: 1_767_229_200_000_000_000,
                            handler: "remind".to_string(),
                            payload: json!(null),
                        },
                        TimerChangeBody::Cancel {
                            timer_key: "abandoned-cart".to_string(),
                        },
                    ],
                    intents: vec![IntentBody {
                        intent_key: "send-receipt".to_string(),
                        tool_name: "mail.send".to_string(),
                        tool_version: "1".to_string(),
                        policy: Policy::ProviderIdempotency,
                        input: json!({"to": "buyer"}),
                    }],
                },
            },
        ),
        sample(
            "worker_commit_failed_request",
            WorkerCommitRequest {
                lease: lease(),
                outcome: TurnOutcome::Failed {
                    error: "inventory service timed out".to_string(),
                    terminal: false,
                },
            },
        ),
        sample(
            "worker_commit_response",
            WorkerCommitResponse {
                operation: operation(),
                status: OperationStatus::Completed,
                state_version: 1,
                rebased: false,
                effects: vec![effect()],
            },
        ),
        sample(
            "step_record_request",
            StepRecordRequest {
                lease: lease(),
                step_key: "price".to_string(),
                tool_name: "catalog.price".to_string(),
                tool_version: "1".to_string(),
                policy: Policy::AtLeastOnce,
                input_hash: price_input_hash(),
            },
        ),
        sample(
            "step_record_response",
            StepRecordResponse {
                step: StepBody {
                    step_id: identity::step_id(1, 1, "price").to_hex(),
                    step_key: "price".to_string(),
                    tool_name: "catalog.price".to_string(),
                    tool_version: "1".to_string(),
                    input_hash: price_input_hash(),
                    status: StepState::Started,
                    output: None,
                },
                effect: Handle {
                    agent: agent_id(),
                    incarnation: 1,
                    item: HandleItem::Effect(identity::effect_id(1, 1, "price")),
                }
                .encode(),
                attempt: 1,
                failure: None,
            },
        ),
        sample(
            "effect_complete_request",
            EffectCompleteRequest {
                effect: effect(),
                attempt: 1,
                result: json!({"message_id": "m-1"}),
            },
        ),
        sample(
            "effect_complete_response",
            EffectCompleteResponse {
                effect: effect(),
                status: EffectState::Completed,
                late: false,
            },
        ),
        sample(
            "effect_fail_request",
            EffectFailRequest {
                effect: effect(),
                attempt: 1,
                error: "mailbox unavailable".to_string(),
            },
        ),
        sample("error_body", ErrorBody::from(&error)),
    ]
}

#[test]
fn fixtures_match_the_wire_types() {
    let dir = fixtures_dir();
    let samples = samples();
    if std::env::var(UPDATE_VAR).as_deref() == Ok("1") {
        std::fs::create_dir_all(&dir).expect("create sdk/fixtures");
        for s in &samples {
            std::fs::write(dir.join(format!("{}.json", s.name)), &s.json).expect("write fixture");
        }
    }
    let mut names = BTreeSet::new();
    for s in &samples {
        assert!(names.insert(format!("{}.json", s.name)), "{} twice", s.name);
        let path = dir.join(format!("{}.json", s.name));
        let on_disk = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "{}: {e}; rerun with {UPDATE_VAR}=1 to write it",
                path.display()
            )
        });
        assert_eq!(
            on_disk, s.json,
            "{} is stale; rerun with {UPDATE_VAR}=1",
            s.name
        );
        assert!(
            (s.round_trips)(&on_disk).expect("fixture decodes"),
            "{} decodes to a different value",
            s.name
        );
    }
    let present: BTreeSet<String> = std::fs::read_dir(&dir)
        .expect("read sdk/fixtures")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.ends_with(".json"))
        .collect();
    assert_eq!(present, names, "sdk/fixtures holds a file no sample names");
}

/// The `input_hash` a worker sends for the `price` step's input
/// `{"sku": "A-1"}`: the hash of its canonical JSON, as the SDK computes it.
fn price_input_hash() -> String {
    let canonical = canonical_json(&json!({"sku": "A-1"}));
    identity::step_input_hash("catalog.price", "1", canonical.as_bytes()).to_hex()
}

/// The canonical step-input encoding [`StepRecordRequest::input_hash`]
/// documents, for the hash fixture only: object keys sorted by UTF-16 code
/// unit at every depth, no whitespace, serde_json's string escaping (equal to
/// `JSON.stringify` for the text the fixture holds) and numbers as written.
fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            let inner: Vec<String> = entries
                .into_iter()
                .map(|(k, v)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).expect("key"),
                        canonical_json(v)
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        other => serde_json::to_string(other).expect("scalar"),
    }
}

/// One step input, its canonical text and its hash, written to
/// `sdk/fixtures/identity/step_input_hash.json` and checked by the SDK's
/// identity test against its own `canonicalJson` and `stepInputHash`. The
/// keys `"\u{ff5e}"` and `"\u{1f600}"` sort one way by UTF-16 code unit and
/// the other way by UTF-8 byte, so a byte-order encoder fails it.
#[test]
fn step_input_hash_fixture_matches_the_canonical_encoding() {
    let input = json!({
        "sku": "A-1",
        "qty": 2,
        "price": {"currency": "EUR", "cents": 649},
        "tags": ["gift", {"z": 1, "a": 2}],
        "note": "Gr\u{fc}\u{df}e\n\"quoted\"",
        "ratio": 12.5,
        "neg": -3,
        "none": null,
        "Z": true,
        "a": false,
        "\u{c4}": 1,
        "\u{ff5e}": "fullwidth tilde",
        "\u{1f600}": "astral",
    });
    let canonical = canonical_json(&input);
    let hash = identity::step_input_hash("catalog.price", "1", canonical.as_bytes()).to_hex();
    let fixture = json!({
        "tool_name": "catalog.price",
        "tool_version": "1",
        "input": input,
        "canonical": canonical,
        "input_hash": hash,
    });
    let mut text = serde_json::to_string_pretty(&fixture).expect("serialize");
    text.push('\n');
    let dir = fixtures_dir().join("identity");
    let path = dir.join("step_input_hash.json");
    if std::env::var(UPDATE_VAR).as_deref() == Ok("1") {
        std::fs::create_dir_all(&dir).expect("create sdk/fixtures/identity");
        std::fs::write(&path, &text).expect("write fixture");
    }
    let on_disk = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{}: {e}; rerun with {UPDATE_VAR}=1 to write it",
            path.display()
        )
    });
    assert_eq!(
        on_disk, text,
        "step_input_hash.json is stale; rerun with {UPDATE_VAR}=1"
    );
    let tilde = canonical.find("\u{ff5e}").expect("tilde key");
    let astral = canonical.find("\u{1f600}").expect("astral key");
    assert!(astral < tilde, "UTF-16 order puts the surrogate pair first");
    assert!(canonical.starts_with("{\"Z\":true,\"a\":false,"));
}

/// The seed every existing fixture carries survives the move onto
/// `identity::derive_128`.
#[test]
fn turn_random_seed_is_unchanged() {
    assert_eq!(
        turn_random_seed(&agent_id().to_hex(), 1, 1),
        "8f8d1b06b1618bd08db0300dbba36423"
    );
}

#[test]
fn error_body_carries_the_code_and_retry_hint() {
    let body = ErrorBody::from(&AgentError::new(
        ErrorKind::MailboxFull,
        SafeIds::default(),
        "mailbox is full",
    ));
    assert_eq!(
        serde_json::to_value(&body).expect("json"),
        json!({
            "code": "mailbox_full",
            "retry": "backoff",
            "reason": "mailbox is full",
            "ids": {}
        })
    );
}

#[test]
fn tagged_bodies_reject_an_unknown_tag() {
    let bad = json!({"decision": "shrug", "effect": "x", "attempt": 1});
    assert!(serde_json::from_value::<ReconcileRequest>(bad).is_err());
    let bad = json!({"outcome": "maybe", "lease": serde_json::to_value(lease()).expect("lease")});
    assert!(serde_json::from_value::<WorkerCommitRequest>(bad).is_err());
}

/// A nanosecond time past 2^53, which a JSON number would round in a
/// JavaScript client.
const PRECISE_UNIX_NS: i64 = 1_767_225_600_123_456_789;

#[test]
fn unix_ns_fields_are_decimal_strings() {
    let exact = serde_json::Value::String(PRECISE_UNIX_NS.to_string());
    let result = serde_json::to_value(ResultBody {
        finished_unix_ns: PRECISE_UNIX_NS,
        ..result_body()
    })
    .expect("serialize");
    assert_eq!(result["finished_unix_ns"], exact);
    let timer = serde_json::to_value(TimerRequest {
        agent: agent(),
        timer_key: "k".to_string(),
        due_unix_ns: PRECISE_UNIX_NS,
        handler: "h".to_string(),
        payload: json!(null),
    })
    .expect("serialize");
    assert_eq!(timer["due_unix_ns"], exact);
    let change = serde_json::to_value(TimerChangeBody::Schedule {
        timer_key: "k".to_string(),
        due_unix_ns: PRECISE_UNIX_NS,
        handler: "h".to_string(),
        payload: json!(null),
    })
    .expect("serialize");
    assert_eq!(change["due_unix_ns"], exact);
    let polled =
        serde_json::to_value(lease_assignment().polled(PRECISE_UNIX_NS)).expect("serialize");
    assert_eq!(polled["lease"]["recorded_unix_ns"], exact);
    let decoded: TimerChangeBody = serde_json::from_value(change).expect("decode");
    assert!(matches!(
        decoded,
        TimerChangeBody::Schedule { due_unix_ns, .. } if due_unix_ns == PRECISE_UNIX_NS
    ));
    // A JSON number is refused rather than read after a client rounded it.
    let mut number = result;
    number["finished_unix_ns"] = json!(PRECISE_UNIX_NS);
    assert!(serde_json::from_value::<ResultBody>(number).is_err());
}

fn lease_assignment() -> TurnAssignment {
    TurnAssignment {
        lease: lease(),
        agent: agent(),
        operation: operation(),
        accept_seq: 1,
        handler: "place".to_string(),
        state: json!(null),
        payload: json!(null),
        steps: Vec::new(),
    }
}

/// The polled lease carries the seed the SDK derives on its own
/// (`derive("random", [agent_id, u64le(incarnation), u64le(accept_seq)])`)
/// and keeps every [`TurnLease`] field at the top level, so a commit that
/// echoes the polled lease is read as the lease it extends.
#[test]
fn polled_lease_extends_the_lease_and_echoes_back_as_it() {
    let polled = lease_assignment().polled(7);
    assert_eq!(
        polled.lease.random_seed,
        turn_random_seed(&agent_id().to_hex(), 1, 1)
    );
    assert_eq!(polled.lease.random_seed.len(), 32);
    assert_ne!(
        polled.lease.random_seed,
        turn_random_seed(&agent_id().to_hex(), 1, 2)
    );
    let mut commit = serde_json::to_value(WorkerCommitRequest {
        lease: lease(),
        outcome: TurnOutcome::Failed {
            error: "e".to_string(),
            terminal: true,
        },
    })
    .expect("serialize");
    commit["lease"] = serde_json::to_value(&polled.lease).expect("serialize");
    let decoded: WorkerCommitRequest = serde_json::from_value(commit).expect("decode");
    assert_eq!(decoded.lease, lease());
}
