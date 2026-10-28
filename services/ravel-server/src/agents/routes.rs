//! The agents client and worker routes (ADR-3331 decision 9, milestones 1
//! and 2).
//!
//! Every handler runs the same prologue, [`Api::admit`]: resolve the
//! principal on this listener's resolver, check the route's capability,
//! take a permit from the route's admission lane, then read the body under
//! the router's byte limit. Only then does it touch a coordinator or store.
//! Every refusal after authentication leaves through [`agent_error`], the one
//! mapping from an [`AgentError`] to a status and an [`ErrorBody`].

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Request, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use ravel_agents_core::error::{AgentError, ErrorKind, SafeIds};
use ravel_agents_core::head::{EffectStatus, Head, Lifecycle};
use ravel_agents_core::identity::{AgentId, agent_id, effect_id};
use ravel_agents_core::keys::{AgentKey, AgentKind};
use ravel_agents_core::state::{Handle, HandleItem, OperationState, operation_state};
use ravel_agents_core::wire::{
    AgentRef, CallRequest, CallResponse, CancelRequest, CreateAgentRequest, ErrorBody, EventBody,
    EventsRequest, EventsResponse, OperationResponse, OperationStatus, ResolveRequest,
    ResolveResponse, ResultBody, ResultResponse, SendRequest, StateResponse, WorkerCommitRequest,
    WorkerHeartbeatRequest, WorkerHeartbeatResponse, WorkerPollRequest, WorkerPollResponse,
    WorkerRegisterRequest, WorkerRegisterResponse,
};
use ravel_agents_store::HeadState;
use ravel_query::http::TenantResolver;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::OwnedSemaphorePermit;

use super::runtime::{AgentsRuntime, LONG_POLL_CAP_MS, Lane, ResultWaitCounters, TenantAgents};
use crate::service::{ServiceError, authenticate_principal};

/// Most events one `events` page returns, whatever `limit` asks for.
pub const MAX_EVENTS_PAGE: u32 = 1_000;

/// How often a waiting `result` re-reads HEAD when nothing in this process
/// signalled a change, so a commit through another process is seen.
const RESULT_RECHECK: Duration = Duration::from_secs(1);

/// The capability a route requires (ADR-3331 decision 9).
#[derive(Debug, Clone, Copy)]
pub(super) enum Capability {
    /// Client routes.
    Agents,
    /// Worker routes.
    AgentsWorker,
}

/// Router state: the process runtime and this listener's resolver.
#[derive(Clone)]
pub(super) struct Api {
    pub(super) runtime: Arc<AgentsRuntime>,
    pub(super) resolver: Arc<dyn TenantResolver>,
}

/// Why a request was not served.
pub(super) enum Refusal {
    /// No principal (401) or a missing capability (403), in the server's
    /// shared error shape: no agent error kind describes either.
    Auth(ServiceError),
    Agent(AgentError),
}

impl From<AgentError> for Refusal {
    fn from(error: AgentError) -> Self {
        Refusal::Agent(error)
    }
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        match self {
            Refusal::Auth(error) => error.into_response(),
            Refusal::Agent(error) => agent_error(&error),
        }
    }
}

type Reply<T> = Result<Json<T>, Refusal>;

/// The one mapping from a typed agents error to an HTTP response.
pub(super) fn agent_error(error: &AgentError) -> Response {
    let status = match error.kind {
        ErrorKind::AgentGone => StatusCode::NOT_FOUND,
        ErrorKind::IdempotencyConflict
        | ErrorKind::OwnershipLost
        | ErrorKind::IncompatibleHandler
        | ErrorKind::NonDeterministicReplay
        | ErrorKind::EffectInDoubt => StatusCode::CONFLICT,
        ErrorKind::OutcomeUnknown => StatusCode::INTERNAL_SERVER_ERROR,
        ErrorKind::MailboxFull => StatusCode::TOO_MANY_REQUESTS,
        ErrorKind::BudgetExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        ErrorKind::StoreUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::UnsupportedFormat => StatusCode::UNPROCESSABLE_ENTITY,
    };
    (status, Json(ErrorBody::from(error))).into_response()
}

fn refusal(kind: ErrorKind, ids: SafeIds, reason: &'static str) -> Refusal {
    Refusal::Agent(AgentError::new(kind, ids, reason))
}

fn ids_for(agent: &AgentId) -> SafeIds {
    SafeIds {
        agent_id: Some(agent.to_hex()),
        ..SafeIds::default()
    }
}

/// What the prologue hands a handler: the caller's tenant runtime and the
/// admission permit, held until the response is built.
struct Admitted {
    tenant: Arc<TenantAgents>,
    _permit: OwnedSemaphorePermit,
}

impl Api {
    /// Authenticate, check `capability`, then take a permit from `lane`.
    fn admit(
        &self,
        headers: &HeaderMap,
        capability: Capability,
        lane: Lane,
    ) -> Result<Admitted, Refusal> {
        let principal =
            authenticate_principal(self.resolver.as_ref(), headers).map_err(Refusal::Auth)?;
        let (granted, name) = match capability {
            Capability::Agents => (principal.agents, "agents"),
            Capability::AgentsWorker => (principal.agents_worker, "agents_worker"),
        };
        if !granted {
            return Err(Refusal::Auth(ServiceError::forbidden(format!(
                "this route requires the {name} capability"
            ))));
        }
        let Some(permit) = self.runtime.admit(lane) else {
            return Err(refusal(
                ErrorKind::StoreUnavailable,
                SafeIds::default(),
                "agents admission limit reached",
            ));
        };
        Ok(Admitted {
            tenant: self.runtime.tenant(principal.tenant.hash()),
            _permit: permit,
        })
    }

    /// [`Api::admit`], then the JSON body, read under the router's limit.
    async fn admit_body<T: DeserializeOwned>(
        &self,
        req: Request<Body>,
        capability: Capability,
        lane: Lane,
    ) -> Result<(Admitted, T), Refusal> {
        let admitted = self.admit(req.headers(), capability, lane)?;
        let bytes = axum::body::to_bytes(req.into_body(), self.runtime.settings.max_body_bytes)
            .await
            .map_err(|_| {
                refusal(
                    ErrorKind::BudgetExceeded,
                    SafeIds::default(),
                    "request body exceeds the agents body limit",
                )
            })?;
        let body = serde_json::from_slice(&bytes).map_err(|_| {
            refusal(
                ErrorKind::UnsupportedFormat,
                SafeIds::default(),
                "request body is not the expected JSON",
            )
        })?;
        Ok((admitted, body))
    }
}

fn query<T: DeserializeOwned>(uri: &Uri) -> Result<T, Refusal> {
    Query::<T>::try_from_uri(uri)
        .map(|Query(q)| q)
        .map_err(|_| {
            refusal(
                ErrorKind::UnsupportedFormat,
                SafeIds::default(),
                "query string is not the expected parameters",
            )
        })
}

/// `min(wait_ms, cap)` as a deadline from now.
fn deadline(wait_ms: u64) -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_millis(wait_ms.min(LONG_POLL_CAP_MS))
}

pub(super) async fn create(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<CreateAgentRequest>(req, Capability::Agents, Lane::Activation)
            .await?;
        Ok::<_, Refusal>(Json(call.tenant.coordinator.create(&body).await?))
    };
    run.await.into_response()
}

pub(super) async fn send(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<SendRequest>(req, Capability::Agents, Lane::Activation)
            .await?;
        Ok::<_, Refusal>(Json(call.tenant.coordinator.send(&body).await?))
    };
    run.await.into_response()
}

pub(super) async fn call(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<CallRequest>(req, Capability::Agents, Lane::Poll)
            .await?;
        let deadline = deadline(u64::from(body.wait_ms));
        let sent = call
            .tenant
            .coordinator
            .send(&SendRequest {
                agent: body.agent,
                handler: body.handler,
                request_id: body.request_id,
                payload: body.payload,
            })
            .await?;
        let handle = decode_operation(&sent.operation)?;
        let (status, result) =
            wait_result(&call.tenant, &api.runtime.result_waits, &handle, deadline).await?;
        Ok::<_, Refusal>(Json(CallResponse {
            operation: sent.operation,
            status,
            result,
        }))
    };
    run.await.into_response()
}

#[derive(Deserialize)]
struct OperationQuery {
    operation: String,
}

pub(super) async fn operation(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let call = api.admit(req.headers(), Capability::Agents, Lane::Activation)?;
        let q: OperationQuery = query(req.uri())?;
        let handle = decode_operation(&q.operation)?;
        let (head, state) = read_operation(&call.tenant, &handle).await?;
        Ok::<_, Refusal>(Json(OperationResponse {
            operation: q.operation,
            status: state.into(),
            accept_seq: operation_seq(&handle),
            blocked_effect: blocked_effect(&head, &handle, state),
        }))
    };
    run.await.into_response()
}

#[derive(Deserialize)]
struct ResultQuery {
    operation: String,
    #[serde(default)]
    wait_ms: u64,
}

pub(super) async fn result(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let call = api.admit(req.headers(), Capability::Agents, Lane::Poll)?;
        let q: ResultQuery = query(req.uri())?;
        let deadline = deadline(q.wait_ms);
        let handle = decode_operation(&q.operation)?;
        let (_, result) =
            wait_result(&call.tenant, &api.runtime.result_waits, &handle, deadline).await?;
        Ok::<_, Refusal>(Json(ResultResponse {
            operation: q.operation,
            result,
        }))
    };
    run.await.into_response()
}

pub(super) async fn resolve(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<ResolveRequest>(req, Capability::Agents, Lane::Activation)
            .await?;
        let agent = parse_agent(&body.agent)?;
        let head = read_head(&call.tenant, &agent).await?;
        let receipt = head
            .receipts
            .iter()
            .find(|r| r.request_id == body.request_id && r.handler == body.handler);
        let reply: Reply<ResolveResponse> = Ok(Json(match receipt {
            Some(r) => ResolveResponse {
                operation: Some(Handle::operation(agent, head.incarnation, r.accept_seq).encode()),
                status: operation_state(&head, r.accept_seq).map(Into::into),
            },
            None => ResolveResponse {
                operation: None,
                status: None,
            },
        }));
        reply
    };
    run.await.into_response()
}

pub(super) async fn cancel(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<CancelRequest>(req, Capability::Agents, Lane::Activation)
            .await?;
        let out = call.tenant.coordinator.cancel(&body).await?;
        call.tenant.changed.notify_waiters();
        Ok::<_, Refusal>(Json(out))
    };
    run.await.into_response()
}

#[derive(Deserialize)]
struct StateQuery {
    kind: String,
    key: String,
}

pub(super) async fn state(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let call = api.admit(req.headers(), Capability::Agents, Lane::Activation)?;
        let q: StateQuery = query(req.uri())?;
        let agent = parse_agent(&AgentRef {
            kind: q.kind,
            key: q.key,
        })?;
        let head = read_head(&call.tenant, &agent).await?;
        let state = read_json(&call.tenant, &agent, head.state).await?;
        Ok::<_, Refusal>(Json(StateResponse {
            agent_id: agent.to_hex(),
            incarnation: head.incarnation,
            state_version: head.state_version,
            content_type: "application/json".to_string(),
            state,
        }))
    };
    run.await.into_response()
}

pub(super) async fn events(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<EventsRequest>(req, Capability::Agents, Lane::Activation)
            .await?;
        let agent = parse_agent(&body.agent)?;
        let after = match body.cursor.as_deref() {
            None => 0,
            Some(text) => text.parse::<u64>().map_err(|_| {
                refusal(
                    ErrorKind::UnsupportedFormat,
                    ids_for(&agent),
                    "events cursor is not one this server issued",
                )
            })?,
        };
        let head = read_head(&call.tenant, &agent).await?;
        let limit = usize::try_from(body.limit.clamp(1, MAX_EVENTS_PAGE)).unwrap_or(1);
        let mut events = Vec::new();
        for event in head
            .events
            .iter()
            .filter(|e| e.event_seq > after)
            .take(limit)
        {
            events.push(EventBody {
                event_seq: event.event_seq,
                accept_seq: event.accept_seq,
                turn_id: event.turn_id.to_hex(),
                event_type: event.event_type.clone(),
                payload: read_json(&call.tenant, &agent, event.payload).await?,
            });
        }
        let next = events.last().map_or(after, |e| e.event_seq);
        Ok::<_, Refusal>(Json(EventsResponse {
            events,
            next_cursor: next.to_string(),
        }))
    };
    run.await.into_response()
}

pub(super) async fn worker_register(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<WorkerRegisterRequest>(req, Capability::AgentsWorker, Lane::Activation)
            .await?;
        let worker_id = call.tenant.coordinator.register_worker(&body.handlers)?;
        Ok::<_, Refusal>(Json(WorkerRegisterResponse { worker_id }))
    };
    run.await.into_response()
}

pub(super) async fn worker_poll(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<WorkerPollRequest>(req, Capability::AgentsWorker, Lane::Poll)
            .await?;
        let deadline = deadline(u64::from(body.wait_ms));
        let assignment = match call.tenant.coordinator.poll(&body, deadline).await? {
            None => None,
            Some(assignment) => {
                let recorded = recorded_unix_ns(&call.tenant, &assignment.lease).await?;
                Some(assignment.polled(recorded))
            }
        };
        Ok::<_, Refusal>(Json(WorkerPollResponse { assignment }))
    };
    run.await.into_response()
}

pub(super) async fn worker_commit(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<WorkerCommitRequest>(req, Capability::AgentsWorker, Lane::Activation)
            .await?;
        let out = call.tenant.coordinator.commit(&body).await?;
        call.tenant.changed.notify_waiters();
        Ok::<_, Refusal>(Json(out))
    };
    run.await.into_response()
}

pub(super) async fn worker_heartbeat(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<WorkerHeartbeatRequest>(req, Capability::AgentsWorker, Lane::Activation)
            .await?;
        let lease_expires_unix_ns = call.tenant.coordinator.heartbeat(&body.lease).await?;
        Ok::<_, Refusal>(Json(WorkerHeartbeatResponse {
            lease_expires_unix_ns,
        }))
    };
    run.await.into_response()
}

fn parse_agent(agent: &AgentRef) -> Result<AgentId, Refusal> {
    let invalid = || {
        refusal(
            ErrorKind::AgentGone,
            SafeIds::default(),
            "agent kind or key is not valid",
        )
    };
    let kind = AgentKind::new(agent.kind.as_str()).map_err(|_| invalid())?;
    let key = AgentKey::new(agent.key.as_str()).map_err(|_| invalid())?;
    Ok(agent_id(&kind, &key))
}

/// An operation handle. A token that does not decode as one is `AgentGone`
/// with no ids, since its text is not known to be safe to echo.
fn decode_operation(token: &str) -> Result<Handle, Refusal> {
    let handle = Handle::decode_kind(token, "op").map_err(|_| {
        refusal(
            ErrorKind::AgentGone,
            SafeIds::default(),
            "operation handle is malformed",
        )
    })?;
    match handle.item {
        HandleItem::Operation(_) => Ok(handle),
        _ => Err(refusal(
            ErrorKind::AgentGone,
            SafeIds::default(),
            "handle does not name an operation",
        )),
    }
}

fn operation_seq(handle: &Handle) -> u64 {
    match handle.item {
        HandleItem::Operation(seq) => seq,
        _ => 0,
    }
}

async fn read_head(tenant: &TenantAgents, agent: &AgentId) -> Result<Head, Refusal> {
    match tenant.store.read_head(agent).await? {
        HeadState::Present { head, .. } if head.lifecycle != Lifecycle::Tombstoned => Ok(*head),
        HeadState::Present { .. } | HeadState::Absent => Err(refusal(
            ErrorKind::AgentGone,
            ids_for(agent),
            "agent does not exist",
        )),
        HeadState::Unsupported { .. } => Err(refusal(
            ErrorKind::UnsupportedFormat,
            ids_for(agent),
            "HEAD format version is newer than this release",
        )),
    }
}

/// HEAD and the state of the operation `handle` names. A handle this tenant
/// never issued, from an earlier incarnation, or for an input no longer
/// retained, is `AgentGone` carrying the handle. A HEAD read that finds no
/// object is `OutcomeUnknown` carrying the handle, as in
/// `AgentStore::resolve`: a HEAD is never deleted, so the read failed.
async fn read_operation(
    tenant: &TenantAgents,
    handle: &Handle,
) -> Result<(Head, OperationState), Refusal> {
    let handle_ids = || SafeIds {
        operation: Some(handle.encode()),
        ..ids_for(&handle.agent)
    };
    let gone = || {
        refusal(
            ErrorKind::AgentGone,
            handle_ids(),
            "operation is not known to this agent",
        )
    };
    let head = match tenant.store.read_head(&handle.agent).await? {
        HeadState::Present { head, .. } if head.lifecycle != Lifecycle::Tombstoned => *head,
        HeadState::Present { .. } => return Err(gone()),
        HeadState::Absent => {
            return Err(refusal(
                ErrorKind::OutcomeUnknown,
                handle_ids(),
                "HEAD could not be read; resolve the operation again",
            ));
        }
        HeadState::Unsupported { .. } => {
            return Err(refusal(
                ErrorKind::UnsupportedFormat,
                ids_for(&handle.agent),
                "HEAD format version is newer than this release",
            ));
        }
    };
    if head.incarnation != handle.incarnation {
        return Err(gone());
    }
    let state = operation_state(&head, operation_seq(handle)).ok_or_else(gone)?;
    Ok((head, state))
}

fn blocked_effect(head: &Head, handle: &Handle, state: OperationState) -> Option<String> {
    if state != OperationState::Blocked {
        return None;
    }
    let seq = operation_seq(handle);
    head.effects
        .iter()
        .find(|e| {
            e.status == EffectStatus::InDoubt
                && e.effect_id == effect_id(head.incarnation, seq, &e.step_key)
        })
        .map(|e| {
            Handle {
                agent: handle.agent,
                incarnation: handle.incarnation,
                item: HandleItem::Effect(e.effect_id),
            }
            .encode()
        })
}

async fn read_json(
    tenant: &TenantAgents,
    agent: &AgentId,
    dep: Option<ravel_agents_core::head::ObjectRef>,
) -> Result<Value, Refusal> {
    let Some(dep) = dep else {
        return Ok(Value::Null);
    };
    let bytes = tenant.store.read_object(agent, dep).await?;
    serde_json::from_slice(&bytes).map_err(|_| {
        refusal(
            ErrorKind::UnsupportedFormat,
            ids_for(agent),
            "object is not a JSON value",
        )
    })
}

/// The operation's result once terminal, re-read after every commit or
/// cancel through this process and every [`RESULT_RECHECK`], until
/// `deadline`. Returns the last state read and the result, if terminal.
async fn wait_result(
    tenant: &TenantAgents,
    counters: &ResultWaitCounters,
    handle: &Handle,
    deadline: tokio::time::Instant,
) -> Result<(OperationStatus, Option<ResultBody>), Refusal> {
    let mut parked = false;
    loop {
        let changed = tenant.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let (head, state) = read_operation(tenant, handle).await?;
        if state.is_terminal() {
            let seq = operation_seq(handle);
            let Some(entry) = head.results.iter().find(|r| r.accept_seq == seq) else {
                return Ok((state.into(), None));
            };
            let output = match entry.output {
                Some(dep) => Some(read_json(tenant, &handle.agent, Some(dep)).await?),
                None => None,
            };
            return Ok((
                state.into(),
                Some(ResultBody {
                    status: entry.status.into(),
                    output,
                    error: (!entry.error.is_empty()).then(|| entry.error.clone()),
                    dead_lettered: entry.dead_lettered,
                    finished_unix_ns: entry.finished_unix_ns,
                }),
            ));
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            counters.expired.fetch_add(1, Ordering::Relaxed);
            return Ok((state.into(), None));
        }
        if !parked {
            parked = true;
            counters.parked.fetch_add(1, Ordering::Relaxed);
        }
        let recheck = (now + RESULT_RECHECK).min(deadline);
        tokio::select! {
            () = &mut changed => {
                counters.notified.fetch_add(1, Ordering::Relaxed);
            }
            () = tokio::time::sleep_until(recheck) => {}
        }
    }
}

/// When the turn a fresh lease names began, as HEAD records it, so every
/// worker that is handed the turn replays it under the same clock.
async fn recorded_unix_ns(
    tenant: &TenantAgents,
    lease: &ravel_agents_core::wire::TurnLease,
) -> Result<i64, Refusal> {
    let moved = |agent: &AgentId| {
        refusal(
            ErrorKind::OwnershipLost,
            SafeIds {
                turn_id: Some(lease.turn_id.clone()),
                ..ids_for(agent)
            },
            "turn moved on before its assignment was returned",
        )
    };
    let agent = AgentId::from_hex(&lease.agent_id).ok_or_else(|| {
        refusal(
            ErrorKind::AgentGone,
            SafeIds::default(),
            "lease agent id is malformed",
        )
    })?;
    let head = read_head(tenant, &agent).await?;
    match &head.active_turn {
        Some(turn) if turn.turn_id.to_hex() == lease.turn_id => Ok(turn.started_unix_ns),
        _ => Err(moved(&agent)),
    }
}
