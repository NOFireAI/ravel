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
        item: HandleItem::Effect(identity::effect_id(1, turn_id(), "send-receipt")),
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
                assignment: Some(TurnAssignment {
                    lease: lease(),
                    agent: agent(),
                    operation: operation(),
                    accept_seq: 1,
                    handler: "place".to_string(),
                    state: json!({"items": []}),
                    payload: json!({"sku": "A-1", "qty": 2}),
                    steps: vec![StepBody {
                        step_id: identity::step_id(1, turn_id(), "price").to_hex(),
                        step_key: "price".to_string(),
                        tool_name: "catalog.price".to_string(),
                        tool_version: "1".to_string(),
                        input_hash: identity::step_input_hash("catalog.price", "1", b"A-1")
                            .to_hex(),
                        status: StepState::Completed,
                        output: Some(json!({"cents": 649})),
                    }],
                }),
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
