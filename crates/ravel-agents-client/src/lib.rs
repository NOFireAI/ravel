//! Rust HTTP client for the Ravel Agents API (ADR-3331).
//!
//! One method per client-facing route under `/api/v1/agents/`. Request and
//! response bodies are the wire types of [`ravel_agents_core::wire`]; a
//! non-2xx answer becomes a [`ClientError`] carrying the typed error kind
//! and the retry hint the server sent.
//!
//! The client never retries on its own. A caller that gets
//! [`ClientError::Agent`] reads [`ClientError::retry`]: `outcome_unknown`
//! says `resolve_operation`, which means resolving the handle, not sending
//! the request again under a new id.

use std::time::Duration;

use ravel_agents_core::error::{ErrorKind, RetryHint};
use ravel_agents_core::wire::{
    AgentRef, CallRequest, CallResponse, CancelRequest, CancelResponse, CancelTimerRequest,
    CancelTimerResponse, CreateAgentRequest, CreateAgentResponse, EffectInspectResponse,
    EffectsResponse, ErrorBody, EventsRequest, EventsResponse, OperationResponse,
    ReconcileRequest, ReconcileResponse, ResolveRequest, ResolveResponse, ResultResponse,
    SafeIdsBody, SendRequest, SendResponse, StateResponse, TimerRequest, TimerResponse,
};
use reqwest::{Method, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The server's cap on a long poll's `wait_ms`. The client clamps to it
/// before sending, so a caller asking for longer gets at most this long.
pub const LONG_POLL_CAP_MS: u32 = 20_000;

/// The largest events page the server returns.
pub const MAX_EVENTS_PAGE: u32 = 1_000;

/// Time allowed for a request that does not long-poll.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Headroom past the poll wait before a long poll's request times out.
const POLL_TIMEOUT_SLACK: Duration = Duration::from_secs(10);

/// Everything a call can fail with.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// A typed agents error, decoded from the server's `ErrorBody`.
    #[error("{} (HTTP {status}): {reason}; retry: {}", kind.code(), retry.code())]
    Agent {
        status: u16,
        kind: ErrorKind,
        retry: RetryHint,
        reason: String,
        ids: SafeIdsBody,
    },
    /// 401: no principal on the request.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// 403: the token lacks the capability the route needs.
    #[error("forbidden: {0}")]
    Forbidden(String),
    /// A non-2xx answer in neither the agents nor the auth error shape.
    #[error("HTTP {status}: {body}")]
    UnexpectedStatus { status: u16, body: String },
    /// The request did not complete: connect, write, read or timeout.
    #[error("transport: {0}")]
    Transport(String),
    /// A 2xx answer whose body is not the expected wire type.
    #[error("decode: {0}")]
    Decode(String),
    /// The base URL cannot address the agents routes.
    #[error("invalid base URL {0:?}")]
    InvalidUrl(String),
}

impl ClientError {
    /// The retry hint the server attached, for a typed agents error. Every
    /// other variant has none: the client does not invent one.
    pub fn retry(&self) -> Option<RetryHint> {
        match self {
            ClientError::Agent { retry, .. } => Some(*retry),
            _ => None,
        }
    }

    /// The typed error kind, for a typed agents error.
    pub fn kind(&self) -> Option<ErrorKind> {
        match self {
            ClientError::Agent { kind, .. } => Some(*kind),
            _ => None,
        }
    }
}

fn retry_from_code(code: &str) -> Option<RetryHint> {
    [
        RetryHint::Never,
        RetryHint::Backoff,
        RetryHint::ResolveOperation,
        RetryHint::Reconcile,
    ]
    .into_iter()
    .find(|hint| hint.code() == code)
}

/// The plain error shape the server uses for 401 and 403.
#[derive(serde::Deserialize)]
struct PlainError {
    error: String,
}

/// Maps a non-2xx answer to a [`ClientError`].
pub fn error_from_response(status: u16, body: &[u8]) -> ClientError {
    if status == 401 || status == 403 {
        let message = serde_json::from_slice::<PlainError>(body)
            .map(|plain| plain.error)
            .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned());
        return if status == 401 {
            ClientError::Unauthorized(message)
        } else {
            ClientError::Forbidden(message)
        };
    }
    if let Ok(error) = serde_json::from_slice::<ErrorBody>(body)
        && let Some(kind) = ErrorKind::from_code(&error.code)
    {
        // An unrecognised hint is read as `never`: retrying on a hint this
        // client cannot read is the one choice that could repeat a side effect.
        let retry = retry_from_code(&error.retry).unwrap_or(RetryHint::Never);
        return ClientError::Agent {
            status,
            kind,
            retry,
            reason: error.reason,
            ids: error.ids,
        };
    }
    ClientError::UnexpectedStatus {
        status,
        body: String::from_utf8_lossy(body).into_owned(),
    }
}

/// A client bound to one server and one bearer token.
#[derive(Debug, Clone)]
pub struct AgentsClient {
    base: Url,
    token: Option<String>,
    http: reqwest::Client,
    timeout: Duration,
}

impl AgentsClient {
    /// A client for the server at `base_url` (scheme, host and port; any
    /// path is replaced by the agents route paths).
    pub fn new(base_url: &str, token: Option<String>) -> Result<Self, ClientError> {
        let base = Url::parse(base_url).map_err(|_| ClientError::InvalidUrl(base_url.into()))?;
        if base.cannot_be_a_base() || !matches!(base.scheme(), "http" | "https") {
            return Err(ClientError::InvalidUrl(base_url.into()));
        }
        Ok(AgentsClient {
            base,
            token,
            http: reqwest::Client::new(),
            timeout: DEFAULT_REQUEST_TIMEOUT,
        })
    }

    /// The same client with a different timeout for requests that do not
    /// long-poll.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn url(&self, segments: &[&str], query: &[(&str, &str)]) -> Result<Url, ClientError> {
        let mut url = self.base.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|()| ClientError::InvalidUrl(self.base.to_string()))?;
            path.clear();
            path.extend(["api", "v1", "agents"]);
            path.extend(segments);
        }
        url.set_query(None);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        Ok(url)
    }

    async fn send_json<B: Serialize, T: DeserializeOwned>(
        &self,
        method: Method,
        url: Url,
        body: Option<&B>,
        timeout: Duration,
    ) -> Result<T, ClientError> {
        let mut request = self.http.request(method, url).timeout(timeout);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(error_from_response(status.as_u16(), &bytes));
        }
        serde_json::from_slice(&bytes).map_err(|e| ClientError::Decode(e.to_string()))
    }

    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        route: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let url = self.url(&[route], &[])?;
        self.send_json(Method::POST, url, Some(body), self.timeout)
            .await
    }

    async fn get<T: DeserializeOwned>(
        &self,
        segments: &[&str],
        query: &[(&str, &str)],
        timeout: Duration,
    ) -> Result<T, ClientError> {
        let url = self.url(segments, query)?;
        self.send_json::<(), T>(Method::GET, url, None, timeout)
            .await
    }

    fn poll_timeout(&self, wait_ms: u32) -> Duration {
        self.timeout
            .max(Duration::from_millis(u64::from(wait_ms)) + POLL_TIMEOUT_SLACK)
    }

    /// `POST /api/v1/agents/create`
    pub async fn create(&self, agent: &AgentRef) -> Result<CreateAgentResponse, ClientError> {
        let body = CreateAgentRequest {
            agent: agent.clone(),
        };
        self.post("create", &body).await
    }

    /// `POST /api/v1/agents/send`
    pub async fn send(&self, request: &SendRequest) -> Result<SendResponse, ClientError> {
        self.post("send", request).await
    }

    /// `POST /api/v1/agents/call`, with `wait_ms` clamped to
    /// [`LONG_POLL_CAP_MS`].
    pub async fn call(&self, request: &CallRequest) -> Result<CallResponse, ClientError> {
        let mut body = request.clone();
        body.wait_ms = clamp_wait(body.wait_ms);
        let url = self.url(&["call"], &[])?;
        let timeout = self.poll_timeout(body.wait_ms);
        self.send_json(Method::POST, url, Some(&body), timeout)
            .await
    }

    /// `GET /api/v1/agents/operation`
    pub async fn operation(&self, operation: &str) -> Result<OperationResponse, ClientError> {
        self.get(&["operation"], &[("operation", operation)], self.timeout)
            .await
    }

    /// `GET /api/v1/agents/result`, a long poll with `wait_ms` clamped to
    /// [`LONG_POLL_CAP_MS`]. A poll that ends with no result returns
    /// `result: None`.
    pub async fn result(
        &self,
        operation: &str,
        wait_ms: u32,
    ) -> Result<ResultResponse, ClientError> {
        let wait_ms = clamp_wait(wait_ms);
        let wait = wait_ms.to_string();
        let timeout = self.poll_timeout(wait_ms);
        self.get(
            &["result"],
            &[("operation", operation), ("wait_ms", &wait)],
            timeout,
        )
        .await
    }

    /// `POST /api/v1/agents/resolve`
    pub async fn resolve(&self, request: &ResolveRequest) -> Result<ResolveResponse, ClientError> {
        self.post("resolve", request).await
    }

    /// `POST /api/v1/agents/cancel`
    pub async fn cancel(&self, operation: &str) -> Result<CancelResponse, ClientError> {
        let body = CancelRequest {
            operation: operation.to_string(),
        };
        self.post("cancel", &body).await
    }

    /// `GET /api/v1/agents/state`
    pub async fn state(&self, agent: &AgentRef) -> Result<StateResponse, ClientError> {
        self.get(
            &["state"],
            &[("kind", &agent.kind), ("key", &agent.key)],
            self.timeout,
        )
        .await
    }

    /// `POST /api/v1/agents/events`, one page. `limit` is clamped to
    /// [`MAX_EVENTS_PAGE`] and raised to 1, since the server refuses 0.
    pub async fn events(&self, request: &EventsRequest) -> Result<EventsResponse, ClientError> {
        let mut body = request.clone();
        body.limit = body.limit.clamp(1, MAX_EVENTS_PAGE);
        self.post("events", &body).await
    }

    /// `GET /api/v1/agents/effects`
    pub async fn effects(&self, agent: &AgentRef) -> Result<EffectsResponse, ClientError> {
        self.get(
            &["effects"],
            &[("kind", &agent.kind), ("key", &agent.key)],
            self.timeout,
        )
        .await
    }

    /// `GET /api/v1/agents/effects/{effect}`
    pub async fn effect(&self, effect: &str) -> Result<EffectInspectResponse, ClientError> {
        self.get(&["effects", effect], &[], self.timeout).await
    }

    /// `POST /api/v1/agents/reconcile`; the token needs `agents_admin`.
    pub async fn reconcile(
        &self,
        request: &ReconcileRequest,
    ) -> Result<ReconcileResponse, ClientError> {
        self.post("reconcile", request).await
    }

    /// `POST /api/v1/agents/timers`
    pub async fn set_timer(&self, request: &TimerRequest) -> Result<TimerResponse, ClientError> {
        self.post("timers", request).await
    }

    /// `DELETE /api/v1/agents/timers`
    pub async fn cancel_timer(
        &self,
        request: &CancelTimerRequest,
    ) -> Result<CancelTimerResponse, ClientError> {
        let url = self.url(&["timers"], &[])?;
        self.send_json(Method::DELETE, url, Some(request), self.timeout)
            .await
    }
}

fn clamp_wait(wait_ms: u32) -> u32 {
    wait_ms.min(LONG_POLL_CAP_MS)
}

/// A resumable position in one agent's event log. Each page moves the
/// cursor to the page's `next_cursor`, so an interrupted reader that keeps
/// the cursor (see [`EventCursor::position`]) resumes without gaps or
/// repeats.
#[derive(Debug, Clone, PartialEq)]
pub struct EventCursor {
    agent: AgentRef,
    cursor: Option<String>,
    limit: u32,
}

impl EventCursor {
    /// A cursor at `cursor` (`None` is the start of the log).
    pub fn new(agent: AgentRef, cursor: Option<String>, limit: u32) -> Self {
        EventCursor {
            agent,
            cursor,
            limit,
        }
    }

    /// The cursor to resume from.
    pub fn position(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Fetches the next page and advances. An empty page leaves the cursor
    /// where it was.
    pub async fn next_page(
        &mut self,
        client: &AgentsClient,
    ) -> Result<EventsResponse, ClientError> {
        let page = client
            .events(&EventsRequest {
                agent: self.agent.clone(),
                cursor: self.cursor.clone(),
                limit: self.limit,
            })
            .await?;
        if !page.events.is_empty() {
            self.cursor = Some(page.next_cursor.clone());
        }
        Ok(page)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests;
