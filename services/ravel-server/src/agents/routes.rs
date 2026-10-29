//! The agents client, worker and admin routes (ADR-3331 decision 9,
//! milestones 1 to 3).
//!
//! Every handler runs the same prologue, [`Api::admit`]: resolve the
//! principal on this listener's resolver, check the route's capability,
//! take a permit from the route's admission lane, then read the body under
//! the route's byte limit. Only then does it touch a coordinator or store.
//! Every step and effect command goes through the coordinator's effects
//! methods, never a kernel command built here.
//! Every refusal after authentication leaves through [`agent_error`], the one
//! mapping from an [`AgentError`] to a status and an [`ErrorBody`], except an
//! expired `events` cursor, which is `410` with a [`CursorExpiredBody`].

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Request, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use ravel_agents_core::error::{AgentError, ErrorKind, SafeIds};
use ravel_agents_core::head::{
    Cancellation, EffectRecord, EffectStatus, Head, InboxEntry, InputSource, Lifecycle, StepRecord,
    TurnBlock,
};
use ravel_agents_core::identity::{AgentId, Hash256, agent_id, effect_id, step_id};
use ravel_agents_core::kernel::Usage;
use ravel_agents_core::keys::{AgentKey, AgentKind};
use ravel_agents_core::pages::{ListKind, Need, receipt_fingerprint};
use ravel_agents_core::state::{Handle, HandleItem, OperationState, operation_state};
use ravel_agents_core::wire::{
    ActiveTurnBody, AgentRef, AttemptState, CURSOR_EXPIRED_CODE, CallRequest, CallResponse,
    CancelRequest, CancelTimerRequest, CreateAgentRequest, CursorExpiredBody, CursorRestart,
    EffectAttemptBody, EffectBody, EffectCompleteRequest, EffectCompleteResponse,
    EffectEvidenceBody, EffectFailRequest, EffectInspectResponse, EffectsResponse, ErrorBody,
    EventBody, EventsRequest, EventsResponse, InputSourceBody, MailboxEntryBody, OperationResponse,
    OperationStatus, ReconcileDecision, ReconcileRequest, ReconcileResponse, ResolveRequest,
    ResolveResponse, ResultBody, ResultResponse, SafeIdsBody, SendRequest, StateResponse, StepBody,
    StepRecordRequest, StepRecordResponse, SweepResponse, TimerBody, TimerRequest, TimersResponse,
    UsageBody, WorkerCommitRequest, WorkerHeartbeatRequest, WorkerHeartbeatResponse,
    WorkerPollRequest, WorkerPollResponse, WorkerRegisterRequest, WorkerRegisterResponse,
};
use ravel_agents_runtime::{EffectRef, EffectResult, EffectSettled, StepProgress, StepSpec};
use ravel_agents_store::{Clock, HeadState, SystemClock};
use ravel_maintain::QueryAuditSink;
use ravel_query::http::TenantResolver;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::OwnedSemaphorePermit;

use super::audit::{ReconcileAction, agents_reconcile_audit_event};
use super::runtime::{
    AgentsRuntime, LONG_POLL_CAP_MS, Lane, ResultWaitCounters, TenantAgents, WorkerDispatcher,
};
use crate::service::{ServiceError, authenticate_principal};
use ravel_tenant_resolve::Principal;

/// Most events one `events` page returns, whatever `limit` asks for.
pub const MAX_EVENTS_PAGE: u32 = 1_000;

/// Most attempts `GET effects/{effect}` lists: the latest ones, so an effect
/// retried past this many lists attempts starting above 1.
pub const MAX_INSPECT_ATTEMPTS: u32 = 64;

/// The inspector page, served as is.
pub const INSPECTOR_HTML: &str = include_str!("inspector.html");
/// The inspector page's `content-type`.
pub const INSPECTOR_CONTENT_TYPE: &str = "text/html; charset=utf-8";
/// The inspector page's `content-security-policy`: inline script and style
/// only, and requests to this origin only.
pub const INSPECTOR_CSP: &str = "default-src 'none'; script-src 'unsafe-inline'; \
     style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; \
     frame-ancestors 'none'";

/// How often a waiting `result` re-reads HEAD when nothing in this process
/// signalled a change, so a commit through another process is seen.
const RESULT_RECHECK: Duration = Duration::from_secs(1);

/// The capability a route requires (ADR-3331 decision 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// Client routes and the inspector page.
    Agents,
    /// Worker routes.
    AgentsWorker,
    /// Operator routes: reconcile and sweep.
    AgentsAdmin,
}

impl Capability {
    /// The capability's name, as a 403 names it.
    pub fn name(self) -> &'static str {
        match self {
            Capability::Agents => "agents",
            Capability::AgentsWorker => "agents_worker",
            Capability::AgentsAdmin => "agents_admin",
        }
    }
}

/// Router state: the process runtime, this listener's resolver and the
/// server's audit sink.
#[derive(Clone)]
pub(super) struct Api {
    pub(super) runtime: Arc<AgentsRuntime>,
    pub(super) resolver: Arc<dyn TenantResolver>,
    pub(super) audit: Arc<dyn QueryAuditSink>,
}

/// Why a request was not served.
pub(super) enum Refusal {
    /// No principal (401) or a missing capability (403), in the server's
    /// shared error shape: no agent error kind describes either.
    Auth(ServiceError),
    Agent(AgentError),
    /// An `events` cursor below the retained window (410).
    CursorExpired(Box<CursorExpiredBody>),
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
            Refusal::CursorExpired(body) => (StatusCode::GONE, Json(*body)).into_response(),
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

/// What the prologue hands a handler: the caller, its tenant runtime and the
/// admission permit, held until the response is built.
struct Admitted {
    principal: Principal,
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
        let granted = match capability {
            Capability::Agents => principal.agents,
            Capability::AgentsWorker => principal.agents_worker,
            Capability::AgentsAdmin => principal.agents_admin,
        };
        if !granted {
            return Err(Refusal::Auth(ServiceError::forbidden(format!(
                "this route requires the {} capability",
                capability.name()
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
            tenant: self.runtime.tenant(&principal.tenant),
            principal,
            _permit: permit,
        })
    }

    /// [`Api::admit`], then the JSON body, read under `max_body_bytes`.
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
                    "request body could not be read in full within the agents body limit",
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
        if matches!(
            state,
            OperationState::Completed | OperationState::Failed | OperationState::Cancelled
        ) {
            call.tenant.evidence.note(handle.agent, &head);
        }
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
        let mut head = read_head(&call.tenant, &agent).await?;
        let fingerprint = receipt_fingerprint(&body.handler, &body.request_id);
        call.tenant
            .store
            .attach(
                &agent,
                &mut head,
                &[(ListKind::Receipts, Need::Fingerprint(fingerprint))],
            )
            .await?;
        let accept_seq = head
            .receipts
            .iter()
            .find(|r| r.request_id == body.request_id && r.handler == body.handler)
            .map(|r| r.accept_seq);
        if let Some(seq) = accept_seq {
            attach_operation(&call.tenant, &agent, &mut head, seq).await?;
        }
        let reply: Reply<ResolveResponse> = Ok(Json(match accept_seq {
            Some(seq) => ResolveResponse {
                operation: Some(Handle::operation(agent, head.incarnation, seq).encode()),
                status: operation_state(&head, seq).map(Into::into),
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
        let mut head = read_head(&call.tenant, &agent).await?;
        let usage = UsageBody::new(Usage::of(&head), call.tenant.coordinator.limits());
        // The mailbox lists every input still in the inbox, sealed or open.
        call.tenant
            .store
            .attach(&agent, &mut head, &[(ListKind::Inbox, Need::All)])
            .await?;
        let state = read_json(&call.tenant, &agent, head.state).await?;
        Ok::<_, Refusal>(Json(StateResponse {
            agent_id: agent.to_hex(),
            incarnation: head.incarnation,
            state_version: head.state_version,
            content_type: "application/json".to_string(),
            state,
            active_turn: head.active_turn.as_ref().map(|turn| ActiveTurnBody {
                turn_id: turn.turn_id.to_hex(),
                accept_seq: turn.input_accept_seq,
                handler: turn.handler.clone(),
                handler_version: turn.handler_version.clone(),
                handler_digest: turn.handler_digest.to_hex(),
                base_state_version: turn.base_state_version,
                started_unix_ns: turn.started_unix_ns,
                lease_hint_unix_ns: head.owner_lease_hint_unix_ns,
                cancel_requested: turn.cancellation == Cancellation::Requested,
                non_deterministic_replay: turn.block == TurnBlock::NonDeterministicReplay,
            }),
            mailbox: head.inbox.iter().map(mailbox_entry).collect(),
            usage: Some(usage),
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
            None => None,
            Some(text) => Some(text.parse::<u64>().map_err(|_| {
                refusal(
                    ErrorKind::UnsupportedFormat,
                    ids_for(&agent),
                    "events cursor is not one this server issued",
                )
            })?),
        };
        if body.limit == 0 {
            return Err(refusal(
                ErrorKind::UnsupportedFormat,
                ids_for(&agent),
                "events limit must be at least 1",
            ));
        }
        let (head, operation) = match body.operation.as_deref() {
            None => (read_head(&call.tenant, &agent).await?, None),
            Some(token) => {
                let handle = decode_operation(token)?;
                if handle.agent != agent {
                    return Err(refusal(
                        ErrorKind::AgentGone,
                        ids_for(&agent),
                        "operation is not one of this agent's",
                    ));
                }
                let (head, state) = read_operation(&call.tenant, &handle).await?;
                (head, Some((operation_seq(&handle), state)))
            }
        };
        // A shallow HEAD holds only the open page; sealed history starts at
        // the first sealed page.
        let oldest = head
            .index
            .events
            .pages
            .first()
            .map(|p| p.first_key)
            .into_iter()
            .chain(head.events.first().map(|e| e.event_seq))
            .min()
            .unwrap_or(head.next_event_seq);
        if let Some(cursor) = after
            && cursor.saturating_add(1) < oldest
        {
            return Err(Refusal::CursorExpired(Box::new(CursorExpiredBody {
                code: CURSOR_EXPIRED_CODE.to_string(),
                cursor: cursor.to_string(),
                oldest_cursor: oldest.saturating_sub(1).to_string(),
                restart: CursorRestart::RestartFromOldestCursor,
                reason: "events below this cursor are no longer retained".to_string(),
                ids: SafeIdsBody::from(&ids_for(&agent)),
            })));
        }
        let after = after.unwrap_or(0);
        let limit = usize::try_from(body.limit.min(MAX_EVENTS_PAGE)).unwrap_or(1);
        // A terminal operation's page reads one event past the limit, so
        // the page that drains its events says so.
        let finished = operation.filter(|(_, state)| state.is_terminal());
        let want = limit.saturating_add(usize::from(finished.is_some()));
        let mut page = call
            .tenant
            .store
            .events_in(&agent, head, after, want, operation.map(|(seq, _)| seq))
            .await?;
        let more = page.len() > limit;
        page.truncate(limit);
        let mut events = Vec::new();
        for event in &page {
            events.push(EventBody {
                event_seq: event.event_seq,
                accept_seq: event.accept_seq,
                turn_id: event.turn_id.to_hex(),
                event_type: event.event_type.clone(),
                payload: read_json(&call.tenant, &agent, event.payload).await?,
            });
        }
        let next = events.last().map_or(after, |e| e.event_seq);
        let terminal = finished.filter(|_| !more).map(|(_, state)| state.into());
        Ok::<_, Refusal>(Json(EventsResponse {
            events,
            next_cursor: next.to_string(),
            terminal,
        }))
    };
    run.await.into_response()
}

pub(super) async fn timer_schedule(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<TimerRequest>(req, Capability::Agents, Lane::Activation)
            .await?;
        let scheduled = call.tenant.coordinator.schedule_timer(&body).await?;
        Ok::<_, Refusal>(Json(scheduled))
    };
    run.await.into_response()
}

pub(super) async fn timer_cancel(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<CancelTimerRequest>(req, Capability::Agents, Lane::Activation)
            .await?;
        let cancelled = call.tenant.coordinator.cancel_timer(&body).await?;
        Ok::<_, Refusal>(Json(cancelled))
    };
    run.await.into_response()
}

pub(super) async fn timers(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let call = api.admit(req.headers(), Capability::Agents, Lane::Activation)?;
        let q: StateQuery = query(req.uri())?;
        let agent = parse_agent(&AgentRef {
            kind: q.kind,
            key: q.key,
        })?;
        let mut head = read_head(&call.tenant, &agent).await?;
        // Delivered and cancelled generations seal onto timer pages.
        call.tenant
            .store
            .attach(&agent, &mut head, &[(ListKind::Timers, Need::All)])
            .await?;
        Ok::<_, Refusal>(Json(TimersResponse {
            timers: head
                .timers
                .iter()
                .map(|t| TimerBody {
                    timer: t.timer_id.to_hex(),
                    timer_key: t.timer_key.clone(),
                    generation: t.generation,
                    due_unix_ns: t.due_unix_ns,
                    handler: t.handler.clone(),
                    status: t.status.into(),
                })
                .collect(),
        }))
    };
    run.await.into_response()
}

/// One sweep pass over the caller's tenant, on demand: the same pass the
/// interval task runs, under the same budgets and the same cursor.
pub(super) async fn sweep(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let call = api.admit(req.headers(), Capability::AgentsAdmin, Lane::Activation)?;
        let report = call
            .tenant
            .coordinator
            .sweep_once(&call.principal.tenant.hash())
            .await?;
        Ok::<_, Refusal>(Json(SweepResponse {
            wake_list_pages: report.wake_list_pages,
            dir_list_pages: report.dir_list_pages,
            head_gets: report.head_gets,
            hints_read: report.hints_read,
            hints_deleted: report.hints_deleted,
            timers_fired: report.timers_fired,
            agents_loaded: report.agents_loaded,
            dir_entries_repaired: report.dir_entries_repaired,
        }))
    };
    run.await.into_response()
}

/// The inspector page (ADR-3331 decision 11). It carries no tenant data: the
/// page reads state, timers, effects and events through the routes above with
/// the token its user enters. The policy forbids every origin but this one,
/// so the page can load nothing external.
pub(super) async fn inspector(State(api): State<Api>, req: Request<Body>) -> Response {
    match api.admit(req.headers(), Capability::Agents, Lane::Activation) {
        Err(refused) => refused.into_response(),
        Ok(_call) => (
            [
                (header::CONTENT_TYPE, INSPECTOR_CONTENT_TYPE),
                (header::CONTENT_SECURITY_POLICY, INSPECTOR_CSP),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            INSPECTOR_HTML,
        )
            .into_response(),
    }
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

/// Journal a step, or read back its record, and authorize the attempt the
/// caller runs. The route's dispatcher records the attempt and executes
/// nothing: the caller runs the tool and reports through `effects/complete`
/// or `effects/fail`.
pub(super) async fn worker_step(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<StepRecordRequest>(req, Capability::AgentsWorker, Lane::Activation)
            .await?;
        let agent = lease_agent(&body.lease)?;
        let input_hash = hex::decode(&body.input_hash)
            .ok()
            .and_then(|bytes| Hash256::from_slice(&bytes))
            .filter(|hash| hash.to_hex() == body.input_hash)
            .ok_or_else(|| {
                refusal(
                    ErrorKind::UnsupportedFormat,
                    ids_for(&agent),
                    "step input_hash is not 64 lowercase hex characters",
                )
            })?;
        let spec = StepSpec {
            step_key: body.step_key,
            tool_name: body.tool_name,
            tool_version: body.tool_version,
            policy: body.policy.into(),
            input_hash,
        };
        let dispatcher = WorkerDispatcher;
        let progress = call
            .tenant
            .coordinator
            .run_step(&body.lease, &spec, &dispatcher)
            .await?;
        let head = read_head(&call.tenant, &agent).await?;
        let Some(turn) = head
            .active_turn
            .as_ref()
            .filter(|t| t.turn_id.to_hex() == body.lease.turn_id)
        else {
            return Err(refusal(
                ErrorKind::OwnershipLost,
                ids_for(&agent),
                "the step's turn is no longer active",
            ));
        };
        let seq = turn.input_accept_seq;
        let id = step_id(head.incarnation, seq, &spec.step_key);
        let Some(step) = head.steps.iter().find(|s| s.step_id == id) else {
            return Err(refusal(
                ErrorKind::OwnershipLost,
                ids_for(&agent),
                "the step is no longer journaled",
            ));
        };
        let effect = effect_id(head.incarnation, seq, &spec.step_key);
        let handle = effect_handle(agent, head.incarnation, effect);
        let (step, attempt, failure) = match progress {
            StepProgress::Dispatched(dispatch) => (step_body(step, None), dispatch.attempt, None),
            StepProgress::Completed { output } => (
                step_body(step, Some(output)),
                result_attempt(&head, &agent, effect)?,
                None,
            ),
            StepProgress::Failed { error } => (
                step_body(step, None),
                result_attempt(&head, &agent, effect)?,
                Some(error),
            ),
        };
        Ok::<_, Refusal>(Json(StepRecordResponse {
            step,
            effect: handle,
            attempt,
            failure,
        }))
    };
    run.await.into_response()
}

/// The attempt whose result settled `effect`. A settled step whose effect
/// record HEAD no longer holds cannot say which attempt settled it, and the
/// worker asking no longer owns it.
fn result_attempt(
    head: &Head,
    agent: &AgentId,
    effect: ravel_agents_core::identity::Id128,
) -> Result<u32, Refusal> {
    head.effects
        .iter()
        .find(|e| e.effect_id == effect)
        .map(|e| e.result_attempt)
        .ok_or_else(|| {
            refusal(
                ErrorKind::OwnershipLost,
                ids_for(agent),
                "the effect record is no longer in HEAD",
            )
        })
}

pub(super) async fn effect_complete(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<EffectCompleteRequest>(req, Capability::AgentsWorker, Lane::Activation)
            .await?;
        let effect = decode_effect(&body.effect)?;
        let settled = call
            .tenant
            .coordinator
            .complete_effect(&effect, body.attempt, &body.result)
            .await?;
        settle(&call.tenant, body.effect, &effect, body.attempt, settled).await
    };
    run.await.into_response()
}

pub(super) async fn effect_fail(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<EffectFailRequest>(req, Capability::AgentsWorker, Lane::Activation)
            .await?;
        let effect = decode_effect(&body.effect)?;
        let settled = call
            .tenant
            .coordinator
            .fail_effect(&effect, body.attempt, &body.error)
            .await?;
        settle(&call.tenant, body.effect, &effect, body.attempt, settled).await
    };
    run.await.into_response()
}

/// The answer to a reported result: HEAD's effect status, and `late` when
/// the result was kept as evidence, which [`TenantAgents::evidence`] then
/// names for `GET /api/v1/agents/effects/{effect}`.
async fn settle(
    tenant: &TenantAgents,
    handle: String,
    effect: &EffectRef,
    attempt: u32,
    settled: EffectSettled,
) -> Reply<EffectCompleteResponse> {
    let late = match settled {
        EffectSettled::Recorded => false,
        EffectSettled::Evidence { result } => {
            tenant
                .evidence
                .record(effect.agent, effect.effect_id, attempt, result);
            true
        }
    };
    tenant.changed.notify_waiters();
    let head = read_head(tenant, &effect.agent).await?;
    tenant.evidence.note(effect.agent, &head);
    let record = effect_record(&head, effect)?;
    Ok(Json(EffectCompleteResponse {
        effect: handle,
        status: record.status.into(),
        late,
    }))
}

pub(super) async fn effects(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let call = api.admit(req.headers(), Capability::Agents, Lane::Activation)?;
        let q: StateQuery = query(req.uri())?;
        let agent = parse_agent(&AgentRef {
            kind: q.kind,
            key: q.key,
        })?;
        let head = read_head(&call.tenant, &agent).await?;
        Ok::<_, Refusal>(Json(EffectsResponse {
            effects: head
                .effects
                .iter()
                .map(|e| effect_body(agent, head.incarnation, e))
                .collect(),
        }))
    };
    run.await.into_response()
}

pub(super) async fn effect_inspect(
    State(api): State<Api>,
    Path(token): Path<String>,
    req: Request<Body>,
) -> Response {
    let run = async move {
        let call = api.admit(req.headers(), Capability::Agents, Lane::Activation)?;
        let effect = decode_effect(&token)?;
        let head = read_head(&call.tenant, &effect.agent).await?;
        call.tenant.evidence.note(effect.agent, &head);
        let record = effect_record(&head, &effect)?;
        let recorded = record.result.is_some().then_some(record.result_attempt);
        let first = record
            .attempt
            .saturating_sub(MAX_INSPECT_ATTEMPTS - 1)
            .max(1);
        let attempts = (first..=record.attempt)
            .map(|attempt| EffectAttemptBody {
                attempt,
                status: attempt_state(record, recorded, attempt),
            })
            .collect();
        let mut evidence = Vec::new();
        for (attempt, dep) in call.tenant.evidence.of(effect.agent, effect.effect_id) {
            let (result, failure) = read_result(&call.tenant, &effect.agent, dep).await?;
            evidence.push(EffectEvidenceBody {
                attempt,
                result,
                failure,
            });
        }
        let (result, failure) = match record.result {
            Some(dep) => {
                let (output, failure) = read_result(&call.tenant, &effect.agent, dep).await?;
                (failure.is_none().then_some(output), failure)
            }
            None => (None, None),
        };
        Ok::<_, Refusal>(Json(EffectInspectResponse {
            effect: effect_body(effect.agent, head.incarnation, record),
            step_key: record.step_key.clone(),
            result_attempt: recorded,
            result,
            failure,
            attempts,
            evidence,
        }))
    };
    run.await.into_response()
}

/// What `attempt` of `record` came to. Only the latest attempt can be open
/// or in doubt; an earlier one without the recorded result was superseded.
fn attempt_state(record: &EffectRecord, recorded: Option<u32>, attempt: u32) -> AttemptState {
    if recorded == Some(attempt) {
        return AttemptState::Result;
    }
    match record.status {
        EffectStatus::Authorized if attempt == record.attempt => AttemptState::Open,
        EffectStatus::InDoubt if attempt == record.attempt => AttemptState::InDoubt,
        _ => AttemptState::Superseded,
    }
}

/// Reconcile an effect: attach an operator-verified result to its latest
/// attempt, or authorize the attempt after it. Each is idempotent: a repeat
/// of an applied decision answers the effect as it stands and changes
/// nothing. A decision that will change the effect is audited first, so an
/// audit sink that refuses leaves the effect as it was.
pub(super) async fn reconcile(State(api): State<Api>, req: Request<Body>) -> Response {
    let run = async move {
        let (call, body) = api
            .admit_body::<ReconcileRequest>(req, Capability::AgentsAdmin, Lane::Activation)
            .await?;
        let effect = decode_effect(&body.effect)?;
        let head = read_head(&call.tenant, &effect.agent).await?;
        let record = effect_record(&head, &effect)?.clone();
        let ids = || SafeIds {
            effect_id: Some(effect.effect_id.to_hex()),
            ..ids_for(&effect.agent)
        };
        let not_in_doubt = || {
            refusal(
                ErrorKind::OwnershipLost,
                ids(),
                "EffectStateAdmits: reconciliation needs an effect in doubt",
            )
        };
        let wrong_attempt = || {
            refusal(
                ErrorKind::IdempotencyConflict,
                ids(),
                "reconcile names an attempt other than the effect's latest",
            )
        };
        let in_doubt = record.status == EffectStatus::InDoubt;
        let action = match body.decision {
            ReconcileDecision::RecordResult { .. } => ReconcileAction::RecordResult,
            ReconcileDecision::Retry => ReconcileAction::Retry,
        };
        match (&body.decision, record.status) {
            (ReconcileDecision::RecordResult { .. }, EffectStatus::InDoubt)
                if body.attempt != record.attempt =>
            {
                return Err(wrong_attempt());
            }
            (ReconcileDecision::RecordResult { .. }, EffectStatus::Completed)
                if body.attempt != record.result_attempt =>
            {
                return Err(wrong_attempt());
            }
            (ReconcileDecision::RecordResult { .. }, EffectStatus::InDoubt)
            | (ReconcileDecision::RecordResult { .. }, EffectStatus::Completed) => {}
            (ReconcileDecision::Retry, EffectStatus::InDoubt) if body.attempt != record.attempt => {
                return Err(wrong_attempt());
            }
            (ReconcileDecision::Retry, EffectStatus::Authorized)
                if body.attempt == record.attempt =>
            {
                return Err(not_in_doubt());
            }
            (ReconcileDecision::Retry, EffectStatus::Authorized)
                if body.attempt == 0 || Some(record.attempt) != body.attempt.checked_add(1) =>
            {
                return Err(wrong_attempt());
            }
            // Only a repeat of this process's own reconcile is answered as
            // applied; an attempt a step authorized was never handed over.
            (ReconcileDecision::Retry, EffectStatus::Authorized)
                if !call
                    .tenant
                    .coordinator
                    .holds_hand_over(&effect, record.attempt) =>
            {
                return Err(refusal(
                    ErrorKind::OwnershipLost,
                    ids(),
                    "the effect's latest attempt is not an unclaimed hand-over from a reconcile",
                ));
            }
            (ReconcileDecision::Retry, EffectStatus::InDoubt)
            | (ReconcileDecision::Retry, EffectStatus::Authorized) => {}
            _ => return Err(not_in_doubt()),
        }
        // A retry of the attempt before an unclaimed hand-over is a repeat:
        // it is answered from HEAD as it stands, with no write, no new
        // hand-over and no audit record.
        if matches!(body.decision, ReconcileDecision::Retry) && !in_doubt {
            return Ok(Json(ReconcileResponse {
                effect: body.effect,
                status: record.status.into(),
                attempt: record.attempt,
            }));
        }
        if in_doubt {
            let event = agents_reconcile_audit_event(
                &call.principal.tenant.hash(),
                SystemClock.now_unix_ns(),
                &call.principal,
                &body.effect,
                body.attempt,
                action,
            );
            if let Err(error) = api.audit.submit(event).await {
                tracing::warn!(error = %error, "agents reconcile audit record was not written");
                return Err(refusal(
                    ErrorKind::StoreUnavailable,
                    ids(),
                    "the reconcile audit record could not be written; nothing was changed",
                ));
            }
        }
        match body.decision {
            ReconcileDecision::RecordResult { result } => {
                call.tenant
                    .coordinator
                    .reconcile_attach(&effect, &result)
                    .await?;
            }
            ReconcileDecision::Retry => {
                let next = body.attempt.checked_add(1).ok_or_else(not_in_doubt)?;
                call.tenant
                    .coordinator
                    .reconcile_authorize(&effect, next)
                    .await?;
            }
        }
        call.tenant.changed.notify_waiters();
        let head = read_head(&call.tenant, &effect.agent).await?;
        let record = effect_record(&head, &effect)?;
        if record.status != EffectStatus::InDoubt {
            call.tenant.evidence.forget(effect.agent, effect.effect_id);
        }
        Ok::<_, Refusal>(Json(ReconcileResponse {
            effect: body.effect,
            status: record.status.into(),
            attempt: record.attempt,
        }))
    };
    run.await.into_response()
}

fn mailbox_entry(entry: &InboxEntry) -> MailboxEntryBody {
    MailboxEntryBody {
        accept_seq: entry.accept_seq,
        handler: entry.handler.clone(),
        source: match &entry.source {
            InputSource::Message { request_id, .. } => InputSourceBody::Message {
                request_id: request_id.clone(),
            },
            InputSource::Timer {
                timer_id,
                generation,
            } => InputSourceBody::Timer {
                timer: timer_id.to_hex(),
                generation: *generation,
            },
        },
        accepted_unix_ns: entry.accepted_unix_ns,
        failed_attempts: entry.failed_attempts,
    }
}

fn step_body(step: &StepRecord, output: Option<Value>) -> StepBody {
    StepBody {
        step_id: step.step_id.to_hex(),
        step_key: step.step_key.clone(),
        tool_name: step.tool_name.clone(),
        tool_version: step.tool_version.clone(),
        input_hash: step.input_hash.to_hex(),
        status: step.status.into(),
        output,
    }
}

fn effect_handle(
    agent: AgentId,
    incarnation: u64,
    effect: ravel_agents_core::identity::Id128,
) -> String {
    Handle {
        agent,
        incarnation,
        item: HandleItem::Effect(effect),
    }
    .encode()
}

fn effect_body(agent: AgentId, incarnation: u64, record: &EffectRecord) -> EffectBody {
    EffectBody {
        effect: effect_handle(agent, incarnation, record.effect_id),
        turn_id: record.turn_id.to_hex(),
        tool_name: record.tool_name.clone(),
        tool_version: record.tool_version.clone(),
        policy: record.policy.into(),
        status: record.status.into(),
        attempt: record.attempt,
    }
}

/// An effect handle. A token that does not decode as one is `AgentGone`
/// with no ids, since its text is not known to be safe to echo.
fn decode_effect(token: &str) -> Result<EffectRef, Refusal> {
    let malformed = || {
        refusal(
            ErrorKind::AgentGone,
            SafeIds::default(),
            "effect handle is malformed",
        )
    };
    let handle = Handle::decode_kind(token, "effect").map_err(|_| malformed())?;
    match handle.item {
        HandleItem::Effect(effect_id) => Ok(EffectRef {
            agent: handle.agent,
            incarnation: handle.incarnation,
            effect_id,
        }),
        _ => Err(malformed()),
    }
}

/// The effect `effect` names in `head`: `AgentGone` from another
/// incarnation or when HEAD does not journal it.
fn effect_record<'a>(head: &'a Head, effect: &EffectRef) -> Result<&'a EffectRecord, Refusal> {
    let gone = || {
        refusal(
            ErrorKind::AgentGone,
            SafeIds {
                effect_id: Some(effect.effect_id.to_hex()),
                ..ids_for(&effect.agent)
            },
            "effect is not known to this agent",
        )
    };
    if head.incarnation != effect.incarnation {
        return Err(gone());
    }
    head.effects
        .iter()
        .find(|e| e.effect_id == effect.effect_id)
        .ok_or_else(gone)
}

fn lease_agent(lease: &ravel_agents_core::wire::TurnLease) -> Result<AgentId, Refusal> {
    AgentId::from_hex(&lease.agent_id).ok_or_else(|| {
        refusal(
            ErrorKind::AgentGone,
            SafeIds::default(),
            "lease agent id is malformed",
        )
    })
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

/// HEAD with no sealed page attached: one GET. A route that reads a paged
/// list entry attaches that entry's page with [`attach`].
async fn read_head(tenant: &TenantAgents, agent: &AgentId) -> Result<Head, Refusal> {
    match tenant.store.read_head_shallow(agent).await? {
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

/// Attach to `head` the pages holding the operation `accept_seq` names, so
/// `operation_state` and its result read the root as a whole would: the
/// results page, then the inbox page only when the operation has no result
/// and is not the active turn's.
async fn attach_operation(
    tenant: &TenantAgents,
    agent: &AgentId,
    head: &mut Head,
    accept_seq: u64,
) -> Result<(), Refusal> {
    let store = &tenant.store;
    store
        .attach(agent, head, &[(ListKind::Results, Need::Key(accept_seq))])
        .await?;
    let settled = head.results.iter().any(|r| r.accept_seq == accept_seq);
    let active = head
        .active_turn
        .as_ref()
        .is_some_and(|t| t.input_accept_seq == accept_seq);
    if !settled && !active {
        store
            .attach(agent, head, &[(ListKind::Inbox, Need::Key(accept_seq))])
            .await?;
    }
    Ok(())
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
    let mut head = match tenant.store.read_head_shallow(&handle.agent).await? {
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
    let seq = operation_seq(handle);
    attach_operation(tenant, &handle.agent, &mut head, seq).await?;
    let state = operation_state(&head, seq).ok_or_else(gone)?;
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

/// An effect's result object: its output, or null and the tool failure
/// recorded through `effects/fail`.
async fn read_result(
    tenant: &TenantAgents,
    agent: &AgentId,
    dep: ravel_agents_core::head::ObjectRef,
) -> Result<(Value, Option<String>), Refusal> {
    match tenant.coordinator.read_result(agent, Some(dep)).await? {
        EffectResult::Output(output) => Ok((output, None)),
        EffectResult::Failure(error) => Ok((Value::Null, Some(error))),
    }
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
    let agent = lease_agent(lease)?;
    let head = read_head(tenant, &agent).await?;
    match &head.active_turn {
        Some(turn) if turn.turn_id.to_hex() == lease.turn_id => Ok(turn.started_unix_ns),
        _ => Err(moved(&agent)),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_agents_core::head::RecoveryPolicy;
    use ravel_agents_core::identity::Id128;

    use super::super::runtime::empty_head;
    use super::*;

    /// A settled step's answer names the attempt whose result settled it,
    /// and an effect record gone from HEAD is 409 `ownership_lost`.
    #[test]
    fn result_attempt_is_ownership_lost_once_the_effect_left_head() {
        let agent = AgentId([1; 16]);
        let effect = Id128([2; 16]);
        let mut head = empty_head();
        let Err(Refusal::Agent(error)) = result_attempt(&head, &agent, effect) else {
            panic!("an effect missing from HEAD answered an attempt");
        };
        assert_eq!(error.kind, ErrorKind::OwnershipLost);
        assert_eq!(error.reason, "the effect record is no longer in HEAD");
        assert_eq!(agent_error(&error).status(), StatusCode::CONFLICT);

        head.effects.push(EffectRecord {
            effect_id: effect,
            step_key: "price".to_string(),
            tool_name: "catalog.price".to_string(),
            tool_version: "1".to_string(),
            input_hash: Hash256([0; 32]),
            policy: RecoveryPolicy::AtLeastOnce,
            status: EffectStatus::Completed,
            attempt: 3,
            result: None,
            result_attempt: 2,
            turn_id: Id128([3; 16]),
        });
        assert!(matches!(result_attempt(&head, &agent, effect), Ok(2)));
    }
}
