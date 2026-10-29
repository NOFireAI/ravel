//! The six Ravel Agents tool bodies (ADR-3331 decision 10).
//!
//! A body validates its arguments, makes one call to the agents client
//! routes through the [`AgentsBackend`] port, and renders the route's answer
//! as one D4 envelope bounded by the call's `max_response_bytes`. The port is
//! the route itself, served in process, so the principal, the `agents`
//! capability, the admission lane and the coordinator are the ones an HTTP
//! caller meets: nothing here decides who may call what.

use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use crate::catalog::{AgentEventsInput, AgentInput, AgentOperationInput, AgentSendInput};
use crate::envelope::{
    AnyJson, Budget, Cell, Column, Data, Envelope, Failure, FailureClass, NextStep, Row, Status,
};

use super::ToolContext;

/// The names [`dispatch_agents`] serves, in catalog order.
pub const AGENTS_TOOLS: [&str; 6] = [
    "ravel_agent_describe",
    "ravel_agent_send",
    "ravel_agent_operation",
    "ravel_agent_state",
    "ravel_agent_events",
    "ravel_agent_blocked_effects",
];

/// The largest events page the route serves.
pub const MAX_EVENTS_PAGE: u32 = 1_000;

/// `GET` with a query string or `POST` with a JSON body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentsMethod {
    Get,
    Post,
}

/// One call to an agents client route.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentsCall {
    pub method: AgentsMethod,
    /// The route under `/api/v1/agents/`, e.g. `send`.
    pub route: &'static str,
    /// Query parameters, unencoded.
    pub query: Vec<(&'static str, String)>,
    pub body: Option<Value>,
}

/// The route's answer: its HTTP status and its JSON body (`null` when the
/// body was not JSON).
#[derive(Debug, Clone, PartialEq)]
pub struct AgentsReply {
    pub status: u16,
    pub body: Value,
}

/// The agents client routes, as the adapter serves them to a tool body. The
/// adapter forwards the MCP request's own credentials, so the route
/// authenticates the same caller the MCP route did.
#[allow(async_fn_in_trait)]
pub trait AgentsBackend {
    async fn call(&self, call: AgentsCall) -> AgentsReply;
}

/// Route one Ravel Agents tool call. A name outside [`AGENTS_TOOLS`] is
/// answered `None`, so the caller can fall through to the query catalog.
pub async fn dispatch_agents<B: AgentsBackend>(
    name: &str,
    args: Value,
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Option<Envelope> {
    let envelope = match name {
        "ravel_agent_describe" => describe(args, ctx, backend).await,
        "ravel_agent_send" => send(args, ctx, backend).await,
        "ravel_agent_operation" => operation(args, ctx, backend).await,
        "ravel_agent_state" => state(args, ctx, backend).await,
        "ravel_agent_events" => events(args, ctx, backend).await,
        "ravel_agent_blocked_effects" => blocked_effects(args, ctx, backend).await,
        _ => return None,
    };
    Some(envelope)
}

fn parse<T: DeserializeOwned>(
    tool: &str,
    args: Value,
    ctx: &ToolContext<'_>,
) -> Result<T, Box<Envelope>> {
    T::deserialize(args).map_err(|err| {
        Box::new(failed(
            Failure {
                class: FailureClass::InvalidArgument,
                message: format!("{tool} arguments: {err}"),
                counter: None,
            },
            Vec::new(),
            ctx,
        ))
    })
}

fn agent_query(kind: String, key: String) -> Vec<(&'static str, String)> {
    vec![("kind", kind), ("key", key)]
}

async fn describe<B: AgentsBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let input: AgentInput = match parse("ravel_agent_describe", args, ctx) {
        Ok(input) => input,
        Err(refused) => return *refused,
    };
    let body = match get(backend, "state", agent_query(input.kind, input.key), ctx).await {
        Ok(body) => body,
        Err(refused) => return *refused,
    };
    let state_bytes = serde_json::to_vec(&body["state"]).map_or(0, |bytes| bytes.len());
    let mailbox_depth = body["mailbox"].as_array().map_or(0, Vec::len);
    let row = vec![
        cell(&body["agent_id"]),
        cell(&body["incarnation"]),
        cell(&body["state_version"]),
        cell(&body["content_type"]),
        Cell::Int(state_bytes as i128),
        cell(&body["active_turn"]),
        Cell::Int(mailbox_depth as i128),
    ];
    let mut envelope = table(
        "agent",
        &[
            ("agent_id", "string"),
            ("incarnation", "int"),
            ("state_version", "int"),
            ("content_type", "string"),
            ("state_bytes", "int"),
            ("active_turn", "json"),
            ("mailbox_depth", "int"),
        ],
        vec![row],
        ctx,
    );
    envelope.next_steps = vec![
        step("read_state", "ravel_agent_state returns the state itself"),
        step(
            "read_blocked_effects",
            "ravel_agent_blocked_effects lists the effects an open turn waits on",
        ),
    ];
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

async fn send<B: AgentsBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let input: AgentSendInput = match parse("ravel_agent_send", args, ctx) {
        Ok(input) => input,
        Err(refused) => return *refused,
    };
    let request = json!({
        "agent": {"kind": input.kind, "key": input.key},
        "handler": input.handler,
        "request_id": input.idempotency_key,
        "payload": input.payload.0,
    });
    let reply = backend
        .call(AgentsCall {
            method: AgentsMethod::Post,
            route: "send",
            query: Vec::new(),
            body: Some(request),
        })
        .await;
    let outcome_unknown = reply.body["code"] == json!("outcome_unknown");
    let body = match answered(reply, ctx) {
        Ok(body) => body,
        Err(mut refused) => {
            if outcome_unknown {
                refused.next_steps.push(step(
                    "resend_with_the_same_key",
                    "ravel_agent_send with the same idempotency_key answers the operation \
                     if the input was accepted, and accepts it once if it was not",
                ));
            }
            return *refused;
        }
    };
    let row = vec![
        cell(&body["operation"]),
        cell(&body["accept_seq"]),
        cell(&body["duplicate"]),
    ];
    let mut envelope = table(
        "agent_send",
        &[
            ("operation", "string"),
            ("accept_seq", "int"),
            ("duplicate", "bool"),
        ],
        vec![row],
        ctx,
    );
    envelope.next_steps = vec![step(
        "poll_operation",
        "ravel_agent_operation with this operation reports its status",
    )];
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

async fn operation<B: AgentsBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let input: AgentOperationInput = match parse("ravel_agent_operation", args, ctx) {
        Ok(input) => input,
        Err(refused) => return *refused,
    };
    let body = match get(
        backend,
        "operation",
        vec![("operation", input.operation)],
        ctx,
    )
    .await
    {
        Ok(body) => body,
        Err(refused) => return *refused,
    };
    let row = vec![
        cell(&body["operation"]),
        cell(&body["status"]),
        cell(&body["accept_seq"]),
        cell(&body["blocked_effect"]),
    ];
    let mut envelope = table(
        "agent_operation",
        &[
            ("operation", "string"),
            ("status", "string"),
            ("accept_seq", "int"),
            ("blocked_effect", "string"),
        ],
        vec![row],
        ctx,
    );
    if body["status"] == json!("blocked") {
        envelope.next_steps = vec![step(
            "read_blocked_effects",
            "ravel_agent_blocked_effects on the operation's agent names the reconcile \
             request that unblocks it",
        )];
    }
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

async fn state<B: AgentsBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let input: AgentInput = match parse("ravel_agent_state", args, ctx) {
        Ok(input) => input,
        Err(refused) => return *refused,
    };
    let body = match get(backend, "state", agent_query(input.kind, input.key), ctx).await {
        Ok(body) => body,
        Err(refused) => return *refused,
    };
    let row = vec![
        cell(&body["agent_id"]),
        cell(&body["incarnation"]),
        cell(&body["state_version"]),
        cell(&body["content_type"]),
        cell(&body["state"]),
        cell(&body["active_turn"]),
        // The route omits an empty mailbox; the column always carries a list.
        cell(body.get("mailbox").unwrap_or(&json!([]))),
    ];
    let envelope = table(
        "agent_state",
        &[
            ("agent_id", "string"),
            ("incarnation", "int"),
            ("state_version", "int"),
            ("content_type", "string"),
            ("state", "json"),
            ("active_turn", "json"),
            ("mailbox", "json"),
        ],
        vec![row],
        ctx,
    );
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

/// One events page. The page size is the call's `max_rows`, at most
/// [`MAX_EVENTS_PAGE`]. When the page is full, or the response bound cut it
/// short, the envelope's cursor is the last event it carries, so the next
/// call resumes after exactly what this one returned.
async fn events<B: AgentsBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let input: AgentEventsInput = match parse("ravel_agent_events", args, ctx) {
        Ok(input) => input,
        Err(refused) => return *refused,
    };
    let limit = ctx.budgets.max_rows.min(MAX_EVENTS_PAGE);
    let request = json!({
        "agent": {"kind": input.kind, "key": input.key},
        "cursor": input.cursor,
        "limit": limit,
        "operation": input.operation,
    });
    let reply = backend
        .call(AgentsCall {
            method: AgentsMethod::Post,
            route: "events",
            query: Vec::new(),
            body: Some(request),
        })
        .await;
    let body = match answered(reply, ctx) {
        Ok(body) => body,
        Err(refused) => return *refused,
    };
    let events = body["events"].as_array().cloned().unwrap_or_default();
    let seqs: Vec<String> = events
        .iter()
        .map(|event| scalar_text(&event["event_seq"]))
        .collect();
    let rows: Vec<Row> = events
        .iter()
        .map(|event| {
            vec![
                cell(&event["event_seq"]),
                cell(&event["accept_seq"]),
                cell(&event["turn_id"]),
                cell(&event["event_type"]),
                cell(&event["payload"]),
            ]
        })
        .collect();
    let returned = rows.len();
    let mut envelope = table(
        "agent_events",
        &[
            ("event_seq", "int"),
            ("accept_seq", "int"),
            ("turn_id", "string"),
            ("event_type", "string"),
            ("payload", "json"),
        ],
        rows,
        ctx,
    );
    envelope.scope.order_by = vec!["event_seq".to_string()];
    envelope.presentation.max_rows = limit;
    envelope.presentation.row_cap_hit = returned >= limit as usize;
    let next_cursor = scalar_text(&body["next_cursor"]);
    envelope.presentation.cursor = Some(next_cursor.clone());

    let mut envelope = envelope.fit(ctx.budgets.max_response_bytes);
    let kept = envelope.data.rows.len();
    if kept < returned {
        let resume = match kept.checked_sub(1) {
            Some(last) => seqs[last].clone(),
            None => input.cursor.clone().unwrap_or_default(),
        };
        envelope.presentation.cursor = Some(resume);
    }
    let resume_cursor = envelope.presentation.cursor.clone().unwrap_or(next_cursor);
    let mut envelope = envelope.finish(true);
    if envelope.status == Status::Ok {
        let detail = match body.get("terminal").and_then(Value::as_str) {
            Some(terminal) => {
                format!("the operation finished {terminal}; no later page holds its events")
            }
            None => format!(
                "ravel_agent_events with cursor {resume_cursor} returns events after this page"
            ),
        };
        envelope.next_steps.push(step("resume_events", &detail));
    }
    envelope
}

async fn blocked_effects<B: AgentsBackend>(
    args: Value,
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Envelope {
    let input: AgentInput = match parse("ravel_agent_blocked_effects", args, ctx) {
        Ok(input) => input,
        Err(refused) => return *refused,
    };
    let body = match get(backend, "effects", agent_query(input.kind, input.key), ctx).await {
        Ok(body) => body,
        Err(refused) => return *refused,
    };
    let rows: Vec<Row> = body["effects"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter(|effect| effect["status"] == json!("in_doubt"))
        .map(|effect| {
            vec![
                cell(&effect["effect"]),
                cell(&effect["turn_id"]),
                cell(&effect["tool_name"]),
                cell(&effect["tool_version"]),
                cell(&effect["policy"]),
                cell(&effect["attempt"]),
                Cell::Map(unblock(effect)),
            ]
        })
        .collect();
    let mut envelope = table(
        "agent_blocked_effects",
        &[
            ("effect", "string"),
            ("turn_id", "string"),
            ("tool_name", "string"),
            ("tool_version", "string"),
            ("policy", "string"),
            ("attempt", "int"),
            ("unblock", "json"),
        ],
        rows,
        ctx,
    );
    envelope.scope.predicates_applied = vec!["status = in_doubt".to_string()];
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

/// What the agents inspector shows for an effect in doubt: the reason, the
/// sentence naming who acts, and the two reconcile requests to choose from.
fn unblock(effect: &Value) -> Map<String, Value> {
    let tool_name = scalar_text(&effect["tool_name"]);
    let attempt = &effect["attempt"];
    let mut map = Map::new();
    map.insert("reason".to_string(), json!("EffectInDoubt"));
    map.insert(
        "action".to_string(),
        Value::String(format!(
            "effect {tool_name} attempt {} may or may not have run. An agents_admin caller \
             sends POST /api/v1/agents/reconcile with one of:",
            scalar_text(attempt)
        )),
    );
    map.insert(
        "requests".to_string(),
        json!([
            {"effect": effect["effect"], "attempt": attempt, "decision": "retry"},
            {
                "effect": effect["effect"],
                "attempt": attempt,
                "decision": "record_result",
                "result": "<the verified result>",
            },
        ]),
    );
    map
}

async fn get<B: AgentsBackend>(
    backend: &B,
    route: &'static str,
    query: Vec<(&'static str, String)>,
    ctx: &ToolContext<'_>,
) -> Result<Value, Box<Envelope>> {
    let reply = backend
        .call(AgentsCall {
            method: AgentsMethod::Get,
            route,
            query,
            body: None,
        })
        .await;
    answered(reply, ctx)
}

/// A 2xx body, or the refusal envelope for the route's error answer.
fn answered(reply: AgentsReply, ctx: &ToolContext<'_>) -> Result<Value, Box<Envelope>> {
    if (200..300).contains(&reply.status) {
        return Ok(reply.body);
    }
    let body = &reply.body;
    let message = match (body.get("code"), body.get("reason"), body.get("error")) {
        (Some(code), Some(reason), _) => {
            format!("{}: {}", scalar_text(code), scalar_text(reason))
        }
        (_, _, Some(error)) => scalar_text(error),
        _ => format!("the agents route answered HTTP {}", reply.status),
    };
    let mut next_steps = Vec::new();
    let class = match reply.status {
        401 | 403 => {
            next_steps.push(step(
                "use_an_agents_credential",
                "call with a credential whose principal carries the agents capability",
            ));
            FailureClass::Unauthorized
        }
        410 => {
            if let Some(oldest) = body.get("oldest_cursor") {
                next_steps.push(step(
                    "restart_from_oldest_cursor",
                    &format!(
                        "ravel_agent_events with cursor {}; earlier events are no longer \
                         retained",
                        scalar_text(oldest)
                    ),
                ));
            }
            FailureClass::CursorExpired
        }
        404 | 409 | 413 | 422 => FailureClass::InvalidArgument,
        429 | 503 => FailureClass::Unavailable,
        _ => FailureClass::Internal,
    };
    Err(Box::new(failed(
        Failure {
            class,
            message,
            counter: None,
        },
        next_steps,
        ctx,
    )))
}

fn failed(failure: Failure, next_steps: Vec<NextStep>, ctx: &ToolContext<'_>) -> Envelope {
    let envelope = Envelope {
        status: Status::Error,
        failure: Some(failure),
        budget: budget_block(ctx),
        next_steps,
        ..Envelope::default()
    };
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

fn table(name: &str, columns: &[(&str, &str)], rows: Vec<Row>, ctx: &ToolContext<'_>) -> Envelope {
    let mut envelope = Envelope {
        data: Data {
            columns: columns
                .iter()
                .map(|(name, kind)| Column {
                    name: (*name).to_string(),
                    r#type: (*kind).to_string(),
                })
                .collect(),
            row_count: rows.len() as u64,
            rows,
        },
        budget: budget_block(ctx),
        ..Envelope::default()
    };
    envelope.scope.signal = "agents".to_string();
    envelope.scope.table = name.to_string();
    envelope.coverage.complete = true;
    envelope.accuracy.exact = true;
    envelope.presentation.max_rows = ctx.budgets.max_rows;
    envelope
}

/// The bounds an agents tool runs under. The route's own store requests are
/// counted on the agents metrics, not per call, so `actual` and `estimate`
/// stay empty rather than report a zero the call did not measure.
fn budget_block(ctx: &ToolContext<'_>) -> Budget {
    Budget {
        effective: AnyJson(json!({
            "max_rows": ctx.budgets.max_rows,
            "max_response_bytes": ctx.budgets.max_response_bytes,
            "deadline_ms": ctx.budgets.deadline.as_millis() as u64,
        })),
        actual: AnyJson(Value::Object(Map::new())),
        estimate: AnyJson(Value::Object(Map::new())),
        estimate_is_upper_envelope: false,
    }
}

fn step(action: &str, detail: &str) -> NextStep {
    NextStep {
        action: action.to_string(),
        detail: detail.to_string(),
    }
}

/// A route value as a cell. An array has no cell of its own, so it travels
/// as `{"items": [...]}`.
fn cell(value: &Value) -> Cell {
    match value {
        Value::Null => Cell::Null,
        Value::Bool(b) => Cell::Bool(*b),
        Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => Cell::Int(i128::from(i)),
            (None, Some(u), _) => Cell::Int(i128::from(u)),
            (None, None, Some(f)) => Cell::Float(f),
            (None, None, None) => Cell::Str(n.to_string()),
        },
        Value::String(s) => Cell::Str(s.clone()),
        Value::Array(items) => {
            let mut map = Map::new();
            map.insert("items".to_string(), Value::Array(items.clone()));
            Cell::Map(map)
        }
        Value::Object(map) => Cell::Map(map.clone()),
    }
}

/// A scalar's text without JSON quoting.
fn scalar_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::tools::tests::test_context;

    /// A backend that records every call and answers one canned reply.
    struct Canned {
        reply: AgentsReply,
        calls: RefCell<Vec<AgentsCall>>,
    }

    impl Canned {
        fn new(status: u16, body: Value) -> Self {
            Canned {
                reply: AgentsReply { status, body },
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl AgentsBackend for Canned {
        async fn call(&self, call: AgentsCall) -> AgentsReply {
            self.calls.borrow_mut().push(call);
            self.reply.clone()
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("the canned backend never waits"),
        }
    }

    fn run(name: &str, args: Value, backend: &Canned) -> Value {
        let key = CursorKey::from_process_secret([7u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);
        let envelope = block_on(dispatch_agents(name, args, &ctx, backend))
            .expect("an agents tool name routes");
        serde_json::to_value(&envelope).expect("envelope serializes")
    }

    fn agent_args() -> Value {
        json!({"kind": "order", "key": "o-1"})
    }

    #[test]
    fn every_agents_name_routes_and_others_fall_through() {
        let key = CursorKey::from_process_secret([7u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);
        for name in AGENTS_TOOLS {
            let backend = Canned::new(403, json!({}));
            assert!(block_on(dispatch_agents(name, json!({}), &ctx, &backend)).is_some());
        }
        let backend = Canned::new(200, json!({}));
        assert!(
            block_on(dispatch_agents(
                "ravel_capabilities",
                json!({}),
                &ctx,
                &backend
            ))
            .is_none()
        );
        assert!(backend.calls.borrow().is_empty());
    }

    #[test]
    fn send_posts_the_idempotency_key_as_the_request_id() {
        let backend = Canned::new(
            200,
            json!({"operation": "op-1", "accept_seq": 1, "duplicate": false}),
        );
        let envelope = run(
            "ravel_agent_send",
            json!({"kind": "order", "key": "o-1", "handler": "place",
                   "payload": {"sku": "A-1"}, "idempotency_key": "k-1"}),
            &backend,
        );
        assert_eq!(envelope["status"], json!("ok"));
        assert_eq!(envelope["data"]["rows"], json!([["op-1", "1", false]]));
        let calls = backend.calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].method, AgentsMethod::Post);
        assert_eq!(calls[0].route, "send");
        assert_eq!(
            calls[0].body,
            Some(
                json!({"agent": {"kind": "order", "key": "o-1"}, "handler": "place",
                        "request_id": "k-1", "payload": {"sku": "A-1"}})
            )
        );
    }

    #[test]
    fn send_without_an_idempotency_key_is_refused_before_the_route() {
        let backend = Canned::new(200, json!({}));
        let envelope = run(
            "ravel_agent_send",
            json!({"kind": "order", "key": "o-1", "handler": "place"}),
            &backend,
        );
        assert_eq!(envelope["status"], json!("error"));
        assert_eq!(envelope["failure"]["class"], json!("invalid_argument"));
        assert!(backend.calls.borrow().is_empty());
    }

    #[test]
    fn a_forbidden_route_answer_is_the_unauthorized_envelope() {
        let backend = Canned::new(
            403,
            json!({"status": "error", "errorType": "forbidden",
                   "error": "this route requires the agents capability"}),
        );
        let envelope = run("ravel_agent_state", agent_args(), &backend);
        assert_eq!(envelope["status"], json!("error"));
        assert_eq!(envelope["failure"]["class"], json!("unauthorized"));
        assert_eq!(
            envelope["failure"]["message"],
            json!("this route requires the agents capability")
        );
        assert_eq!(
            envelope["next_steps"][0]["action"],
            json!("use_an_agents_credential")
        );
    }

    #[test]
    fn an_expired_events_cursor_names_the_oldest_cursor() {
        let backend = Canned::new(
            410,
            json!({"code": "cursor_expired", "cursor": "1", "oldest_cursor": "7",
                   "restart": "restart_from_oldest_cursor", "reason": "expired"}),
        );
        let envelope = run(
            "ravel_agent_events",
            json!({"kind": "order", "key": "o-1", "cursor": "1"}),
            &backend,
        );
        assert_eq!(envelope["failure"]["class"], json!("cursor_expired"));
        assert_eq!(
            envelope["failure"]["message"],
            json!("cursor_expired: expired")
        );
        assert!(
            envelope["next_steps"][0]["detail"]
                .as_str()
                .expect("detail")
                .contains("cursor 7")
        );
    }

    fn events_body(seqs: std::ops::RangeInclusive<u64>, next: &str) -> Value {
        let events: Vec<Value> = seqs
            .map(|seq| {
                json!({"event_seq": seq, "accept_seq": 1, "turn_id": "t",
                       "event_type": "e", "payload": {"n": seq}})
            })
            .collect();
        json!({"events": events, "next_cursor": next})
    }

    /// A full page is a page: its cursor is the route's next cursor, and the
    /// page size the route was asked for is the call's `max_rows`.
    #[test]
    fn a_full_events_page_carries_the_cursor() {
        let backend = Canned::new(200, events_body(1..=2, "2"));
        let envelope = run(
            "ravel_agent_events",
            json!({"kind": "order", "key": "o-1", "max_rows": 2}),
            &backend,
        );
        // The adapter clamps `max_rows` into the context; the test context
        // runs at the default of 200, so the page asked for is 200 and two
        // events are a short page.
        assert_eq!(envelope["status"], json!("ok"));
        assert_eq!(envelope["presentation"]["cursor"], Value::Null);
        assert_eq!(
            backend.calls.borrow()[0].body.as_ref().expect("body")["limit"],
            json!(200)
        );
        assert!(
            envelope["next_steps"][0]["detail"]
                .as_str()
                .expect("detail")
                .contains("cursor 2")
        );

        let full: Vec<u64> = (1..=200).collect();
        let backend = Canned::new(200, events_body(1..=200, "200"));
        let envelope = run("ravel_agent_events", agent_args(), &backend);
        assert_eq!(envelope["status"], json!("ok_page"));
        assert_eq!(envelope["presentation"]["cursor"], json!("200"));
        assert_eq!(envelope["presentation"]["row_cap_hit"], json!(true));
        assert_eq!(
            envelope["data"]["rows"].as_array().expect("rows").len(),
            full.len()
        );
    }

    /// A page the response bound cut short resumes after the last event it
    /// kept, not after the last event the route returned.
    #[test]
    fn a_bytes_capped_events_page_resumes_after_the_last_kept_event() {
        let big = "x".repeat(4_000);
        let events: Vec<Value> = (1..=150u64)
            .map(|seq| {
                json!({"event_seq": seq, "accept_seq": 1, "turn_id": "t",
                       "event_type": "e", "payload": {"blob": big}})
            })
            .collect();
        let backend = Canned::new(200, json!({"events": events, "next_cursor": "150"}));
        let envelope = run(
            "ravel_agent_events",
            json!({"kind": "order", "key": "o-1", "max_response_bytes": 262_144}),
            &backend,
        );
        assert_eq!(envelope["status"], json!("ok_page"));
        assert_eq!(envelope["presentation"]["bytes_cap_hit"], json!(true));
        let rows = envelope["data"]["rows"].as_array().expect("rows");
        assert!(!rows.is_empty() && rows.len() < 150, "{}", rows.len());
        let last = rows.last().expect("a kept row")[0].clone();
        assert_eq!(envelope["presentation"]["cursor"], last);
        assert_eq!(
            envelope["presentation"]["rows_omitted"],
            json!(150 - rows.len())
        );
    }

    #[test]
    fn blocked_effects_lists_only_effects_in_doubt_with_the_inspector_action() {
        let backend = Canned::new(
            200,
            json!({"effects": [
                {"effect": "e1", "turn_id": "t", "tool_name": "catalog.price",
                 "tool_version": "1", "policy": "manual", "status": "in_doubt", "attempt": 1},
                {"effect": "e2", "turn_id": "t", "tool_name": "catalog.price",
                 "tool_version": "1", "policy": "manual", "status": "completed", "attempt": 1},
            ]}),
        );
        let envelope = run("ravel_agent_blocked_effects", agent_args(), &backend);
        let rows = envelope["data"]["rows"].as_array().expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], json!("e1"));
        assert_eq!(
            rows[0][6]["action"],
            json!(
                "effect catalog.price attempt 1 may or may not have run. An agents_admin \
                 caller sends POST /api/v1/agents/reconcile with one of:"
            )
        );
        assert_eq!(
            rows[0][6]["requests"][0],
            json!({"effect": "e1", "attempt": 1, "decision": "retry"})
        );
        assert_eq!(
            rows[0][6]["requests"][1]["decision"],
            json!("record_result")
        );
        let calls = backend.calls.borrow();
        assert_eq!(calls[0].route, "effects");
        assert_eq!(
            calls[0].query,
            vec![("kind", "order".to_string()), ("key", "o-1".to_string())]
        );
    }

    #[test]
    fn describe_omits_the_state_and_reports_its_size() {
        let backend = Canned::new(
            200,
            json!({"agent_id": "ab", "incarnation": 1, "state_version": 3,
                   "content_type": "application/json", "state": {"a": 1},
                   "mailbox": [{"accept_seq": 4}]}),
        );
        let envelope = run("ravel_agent_describe", agent_args(), &backend);
        assert_eq!(
            envelope["data"]["rows"],
            json!([["ab", "1", "3", "application/json", "7", null, "1"]])
        );
    }
}
