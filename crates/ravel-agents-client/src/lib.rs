//! Rust HTTP client for the Ravel Agents API (ADR-3331 decision 9).
//!
//! One method per client route under `/api/v1/agents/`, decoding into the
//! wire types of [`ravel_agents_core::wire`]. Failures are typed: a typed
//! refusal decodes from its [`ErrorBody`] into [`ClientError::Agent`], and a
//! missing credential or capability (the server's plain error shape, not an
//! `ErrorBody`) into [`ClientError::Unauthorized`] or
//! [`ClientError::Forbidden`].
//!
//! The client never retries on its own. An `outcome_unknown` refusal says
//! `retry: resolve_operation`, which means resolve the request id, never
//! resend it; [`AgentsClient::send_resolving`] does exactly that once and
//! nothing more.

use std::time::Duration;

use ravel_agents_core::error::{ErrorKind, RetryHint};
use ravel_agents_core::wire::{
    AgentRef, CallRequest, CallResponse, CancelRequest, CancelResponse, CancelTimerRequest,
    CancelTimerResponse, CreateAgentRequest, CreateAgentResponse, DestroyAgentRequest,
    DestroyAgentResponse, EffectInspectResponse, EffectsResponse, ErrorBody, EventsRequest, EventsResponse, OperationResponse,
    ReconcileDecision, ReconcileRequest, ReconcileResponse, ResolveRequest, ResolveResponse,
    ResultResponse, SafeIdsBody, SendRequest, SendResponse, StateResponse, TimerRequest,
    TimerResponse,
};
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// The server's cap on a long poll (`call`, `result`): a longer wait is
/// clamped to this before the request is sent.
pub const LONG_POLL_CAP_MS: u64 = 20_000;

/// The server's cap on one `events` page; a larger limit is sent as this.
pub const EVENTS_PAGE_CAP: u32 = 1000;

/// Request timeout for every route that does not long-poll.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Added to a long poll's wait for the request timeout, so the server's own
/// deadline answers first.
const LONG_POLL_SLACK: Duration = Duration::from_secs(10);

/// Bytes of an unrecognised error body kept in [`ClientError::Http`].
const MAX_ERROR_BODY: usize = 512;

const API_PREFIX: &str = "api/v1/agents/";

/// A typed refusal decoded from the server's [`ErrorBody`].
#[derive(Debug, Clone, PartialEq)]
pub struct AgentFailure {
    pub status: u16,
    /// The parsed code; `None` for a code this client does not know, which
    /// is kept verbatim in `code`.
    pub kind: Option<ErrorKind>,
    pub code: String,
    /// The parsed retry hint; `None` for an unknown hint, kept in
    /// `retry_code`.
    pub retry: Option<RetryHint>,
    pub retry_code: String,
    pub reason: String,
    pub ids: SafeIdsBody,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("agents request refused: {} ({}), retry {}", .0.code, .0.reason, .0.retry_code)]
    Agent(Box<AgentFailure>),
    #[error("agents request unauthorized: {message}")]
    Unauthorized { message: String },
    #[error("agents request forbidden: {message}")]
    Forbidden { message: String },
    /// A 404 with no `ErrorBody`: the route is not mounted on this server
    /// (the timer routes in this release, or a server without `--agents`).
    #[error("agents route {path} is not served by this server")]
    NotServed { path: String },
    #[error("agents request failed with HTTP {status}: {body}")]
    Http { status: u16, body: String },
    #[error("agents response from {path} did not decode: {source}")]
    Decode {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid agents base URL {url}: {reason}")]
    BaseUrl { url: String, reason: String },
    #[error("agents request transport failure: {0}")]
    Transport(#[from] reqwest::Error),
}

impl ClientError {
    /// The typed error kind, when the server sent an `ErrorBody` with a known
    /// code.
    pub fn kind(&self) -> Option<ErrorKind> {
        match self {
            ClientError::Agent(failure) => failure.kind,
            _ => None,
        }
    }

    /// The server's retry hint, when it sent one this client understands.
    pub fn retry(&self) -> Option<RetryHint> {
        match self {
            ClientError::Agent(failure) => failure.retry,
            _ => None,
        }
    }
}

fn parse_retry(code: &str) -> Option<RetryHint> {
    [
        RetryHint::Never,
        RetryHint::Backoff,
        RetryHint::ResolveOperation,
        RetryHint::Reconcile,
    ]
    .into_iter()
    .find(|hint| hint.code() == code)
}

/// The server's plain error shape for 401 and 403.
#[derive(serde::Deserialize)]
struct PlainError {
    error: String,
}

fn truncated(body: &str) -> String {
    let mut end = body.len().min(MAX_ERROR_BODY);
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    body[..end].to_string()
}

/// Map a non-success response to its typed error.
fn error_from(status: StatusCode, path: &str, body: &str) -> ClientError {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        let message = serde_json::from_str::<PlainError>(body)
            .map(|plain| plain.error)
            .unwrap_or_else(|_| truncated(body));
        return if status == StatusCode::UNAUTHORIZED {
            ClientError::Unauthorized { message }
        } else {
            ClientError::Forbidden { message }
        };
    }
    if let Ok(error) = serde_json::from_str::<ErrorBody>(body) {
        return ClientError::Agent(Box::new(AgentFailure {
            status: status.as_u16(),
            kind: ErrorKind::from_code(&error.code),
            retry: parse_retry(&error.retry),
            code: error.code,
            retry_code: error.retry,
            reason: error.reason,
            ids: error.ids,
        }));
    }
    if status == StatusCode::NOT_FOUND {
        return ClientError::NotServed {
            path: path.to_string(),
        };
    }
    ClientError::Http {
        status: status.as_u16(),
        body: truncated(body),
    }
}

/// What [`AgentsClient::send_resolving`] came to.
#[derive(Debug, Clone, PartialEq)]
pub enum SendOutcome {
    Accepted(SendResponse),
    /// The send ended in `outcome_unknown` and the request id was resolved.
    /// `operation: None` means it was never accepted; resending it under the
    /// same request id is then safe.
    Resolved(ResolveResponse),
}

/// A resumable position in an agent's event log. `next_page` advances it to
/// the page's `next_cursor`, so a caller that persists [`EventCursor::cursor`]
/// resumes after the last event it saw.
#[derive(Debug, Clone, PartialEq)]
pub struct EventCursor {
    pub agent: AgentRef,
    pub cursor: Option<String>,
}

impl EventCursor {
    pub fn new(agent: AgentRef, cursor: Option<String>) -> Self {
        EventCursor { agent, cursor }
    }

    pub async fn next_page(
        &mut self,
        client: &AgentsClient,
        limit: u32,
    ) -> Result<EventsResponse, ClientError> {
        let page = client
            .events(&self.agent, self.cursor.clone(), limit)
            .await?;
        self.cursor = Some(page.next_cursor.clone());
        Ok(page)
    }
}

/// A client for one server and one bearer token.
#[derive(Clone)]
pub struct AgentsClient {
    http: reqwest::Client,
    base: Url,
    token: String,
}

impl std::fmt::Debug for AgentsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentsClient")
            .field("base", &self.base.as_str())
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl AgentsClient {
    /// `base` is the server's HTTP root, e.g. `http://127.0.0.1:4318`.
    pub fn new(base: &str, token: impl Into<String>) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        Self::with_http(http, base, token)
    }

    pub fn with_http(
        http: reqwest::Client,
        base: &str,
        token: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let mut base = Url::parse(base).map_err(|e| ClientError::BaseUrl {
            url: base.to_string(),
            reason: e.to_string(),
        })?;
        if base.cannot_be_a_base() {
            return Err(ClientError::BaseUrl {
                url: base.to_string(),
                reason: "not a base URL".to_string(),
            });
        }
        if !base.path().ends_with('/') {
            let joined = format!("{}/", base.path());
            base.set_path(&joined);
        }
        Ok(AgentsClient {
            http,
            base,
            token: token.into(),
        })
    }

    fn url(&self, route: &str, query: &[(&str, &str)]) -> Result<Url, ClientError> {
        let mut url = self
            .base
            .join(&format!("{API_PREFIX}{route}"))
            .map_err(|e| ClientError::BaseUrl {
                url: self.base.to_string(),
                reason: e.to_string(),
            })?;
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
        let path = url.path().to_string();
        let mut request = self
            .http
            .request(method, url)
            .bearer_auth(&self.token)
            .timeout(timeout);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        let code = response.status();
        let text = response.text().await?;
        if !code.is_success() {
            return Err(error_from(code, &path, &text));
        }
        serde_json::from_str(&text).map_err(|source| ClientError::Decode { path, source })
    }

    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        route: &str,
        body: &B,
        timeout: Duration,
    ) -> Result<T, ClientError> {
        let url = self.url(route, &[])?;
        self.send_json(Method::POST, url, Some(body), timeout).await
    }

    async fn get<T: DeserializeOwned>(
        &self,
        route: &str,
        query: &[(&str, &str)],
        timeout: Duration,
    ) -> Result<T, ClientError> {
        let url = self.url(route, query)?;
        self.send_json::<(), T>(Method::GET, url, None, timeout)
            .await
    }

    /// `GET /api/v1/agents/health`: the agents feature mode and settings. It
    /// answers without a token, so it doubles as a readiness probe.
    pub async fn health(&self) -> Result<Value, ClientError> {
        self.get("health", &[], DEFAULT_TIMEOUT).await
    }

    pub async fn create(&self, agent: &AgentRef) -> Result<CreateAgentResponse, ClientError> {
        let body = CreateAgentRequest {
            agent: agent.clone(),
        };
        self.post("create", &body, DEFAULT_TIMEOUT).await
    }

    /// One send; an error is returned as is, never retried.
    pub async fn send(&self, request: &SendRequest) -> Result<SendResponse, ClientError> {
        self.post("send", request, DEFAULT_TIMEOUT).await
    }

    /// One send; on `outcome_unknown` whose body says `resolve_operation`,
    /// one resolve of the same request id. Any other error, including an
    /// `outcome_unknown` whose retry field says anything else, is returned
    /// without a further request.
    pub async fn send_resolving(&self, request: &SendRequest) -> Result<SendOutcome, ClientError> {
        match self.send(request).await {
            Ok(sent) => Ok(SendOutcome::Accepted(sent)),
            Err(err)
                if err.kind() == Some(ErrorKind::OutcomeUnknown)
                    && err.retry() == Some(RetryHint::ResolveOperation) =>
            {
                let resolved = self
                    .resolve(&ResolveRequest {
                        agent: request.agent.clone(),
                        handler: request.handler.clone(),
                        request_id: request.request_id.clone(),
                    })
                    .await?;
                Ok(SendOutcome::Resolved(resolved))
            }
            Err(err) => Err(err),
        }
    }

    /// Send and wait; `wait_ms` is clamped to [`LONG_POLL_CAP_MS`] before it
    /// is sent.
    pub async fn call(&self, request: &CallRequest) -> Result<CallResponse, ClientError> {
        let wait = clamp_wait(u64::from(request.wait_ms));
        let mut body = request.clone();
        body.wait_ms = u32::try_from(wait).unwrap_or(u32::MAX);
        self.post("call", &body, long_poll_timeout(wait)).await
    }

    pub async fn operation(&self, operation: &str) -> Result<OperationResponse, ClientError> {
        self.get("operation", &[("operation", operation)], DEFAULT_TIMEOUT)
            .await
    }

    /// Read an operation's result, waiting up to `wait_ms`, clamped to
    /// [`LONG_POLL_CAP_MS`].
    pub async fn result(
        &self,
        operation: &str,
        wait_ms: u64,
    ) -> Result<ResultResponse, ClientError> {
        let wait = clamp_wait(wait_ms);
        let wait_text = wait.to_string();
        self.get(
            "result",
            &[("operation", operation), ("wait_ms", &wait_text)],
            long_poll_timeout(wait),
        )
        .await
    }

    pub async fn resolve(&self, request: &ResolveRequest) -> Result<ResolveResponse, ClientError> {
        self.post("resolve", request, DEFAULT_TIMEOUT).await
    }

    pub async fn cancel(&self, operation: &str) -> Result<CancelResponse, ClientError> {
        let body = CancelRequest {
            operation: operation.to_string(),
        };
        self.post("cancel", &body, DEFAULT_TIMEOUT).await
    }

    pub async fn state(&self, agent: &AgentRef) -> Result<StateResponse, ClientError> {
        self.get(
            "state",
            &[("kind", &agent.kind), ("key", &agent.key)],
            DEFAULT_TIMEOUT,
        )
        .await
    }

    /// One page after `cursor`; `limit` above [`EVENTS_PAGE_CAP`] is sent as
    /// the cap. [`EventCursor`] carries the cursor between pages.
    pub async fn events(
        &self,
        agent: &AgentRef,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<EventsResponse, ClientError> {
        let body = EventsRequest {
            agent: agent.clone(),
            cursor,
            limit: limit.min(EVENTS_PAGE_CAP),
            operation: None,
        };
        self.post("events", &body, DEFAULT_TIMEOUT).await
    }

    pub async fn effects(&self, agent: &AgentRef) -> Result<EffectsResponse, ClientError> {
        self.get(
            "effects",
            &[("kind", &agent.kind), ("key", &agent.key)],
            DEFAULT_TIMEOUT,
        )
        .await
    }

    pub async fn effect(&self, effect: &str) -> Result<EffectInspectResponse, ClientError> {
        let mut url = self.url("effects/", &[])?;
        url.path_segments_mut()
            .map_err(|()| ClientError::BaseUrl {
                url: self.base.to_string(),
                reason: "not a base URL".to_string(),
            })?
            .pop_if_empty()
            .push(effect);
        self.send_json::<(), _>(Method::GET, url, None, DEFAULT_TIMEOUT)
            .await
    }

    /// Settle an effect in doubt; needs a token with `agents_admin`.
    pub async fn reconcile(
        &self,
        effect: &str,
        attempt: u32,
        decision: ReconcileDecision,
    ) -> Result<ReconcileResponse, ClientError> {
        let body = ReconcileRequest {
            effect: effect.to_string(),
            attempt,
            decision,
        };
        self.post("reconcile", &body, DEFAULT_TIMEOUT).await
    }

    /// Record the result an operator confirmed out of band.
    pub async fn reconcile_attach_result(
        &self,
        effect: &str,
        attempt: u32,
        result: Value,
    ) -> Result<ReconcileResponse, ClientError> {
        self.reconcile(effect, attempt, ReconcileDecision::RecordResult { result })
            .await
    }

    /// Authorize one more attempt of the effect.
    pub async fn reconcile_authorize_attempt(
        &self,
        effect: &str,
        attempt: u32,
    ) -> Result<ReconcileResponse, ClientError> {
        self.reconcile(effect, attempt, ReconcileDecision::Retry)
            .await
    }

    /// Tombstone the agent's current incarnation, or `incarnation` when it is
    /// given; needs a token with `agents_admin`. A 404 `agent_gone` means no
    /// such agent, or the pinned incarnation is not the current one.
    pub async fn destroy(
        &self,
        agent: &AgentRef,
        incarnation: Option<u64>,
    ) -> Result<DestroyAgentResponse, ClientError> {
        let body = DestroyAgentRequest {
            agent: agent.clone(),
            incarnation,
        };
        self.post("destroy", &body, DEFAULT_TIMEOUT).await
    }

    /// `POST /api/v1/agents/timers`. The server does not mount the timer
    /// routes in this release, so this ends in [`ClientError::NotServed`]
    /// there.
    pub async fn set_timer(&self, request: &TimerRequest) -> Result<TimerResponse, ClientError> {
        self.post("timers", request, DEFAULT_TIMEOUT).await
    }

    /// `DELETE /api/v1/agents/timers`; not mounted in this release, like
    /// [`AgentsClient::set_timer`].
    pub async fn cancel_timer(
        &self,
        request: &CancelTimerRequest,
    ) -> Result<CancelTimerResponse, ClientError> {
        let url = self.url("timers", &[])?;
        self.send_json(Method::DELETE, url, Some(request), DEFAULT_TIMEOUT)
            .await
    }
}

fn clamp_wait(wait_ms: u64) -> u64 {
    wait_ms.min(LONG_POLL_CAP_MS)
}

fn long_poll_timeout(wait_ms: u64) -> Duration {
    Duration::from_millis(wait_ms) + LONG_POLL_SLACK
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, Response, StatusCode as AxumStatus};
    use axum::routing::any;
    use ravel_agents_core::wire::OperationStatus;
    use serde_json::json;

    use super::*;

    /// One request the test server saw.
    #[derive(Debug, Clone)]
    struct Seen {
        method: String,
        path: String,
        query: String,
        auth: String,
        body: Value,
    }

    type Replies = Arc<Mutex<Vec<(u16, String)>>>;

    /// A loopback server answering each request with the next scripted
    /// reply and recording what it was sent.
    struct TestServer {
        base: String,
        seen: Arc<Mutex<Vec<Seen>>>,
    }

    impl TestServer {
        async fn start(replies: Vec<(u16, Value)>) -> TestServer {
            let replies = replies
                .into_iter()
                .map(|(code, body)| (code, body.to_string()))
                .collect();
            TestServer::start_raw(replies).await
        }

        async fn start_raw(replies: Vec<(u16, String)>) -> TestServer {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let replies: Replies = Arc::new(Mutex::new(replies.into_iter().rev().collect()));
            let app = Router::new().fallback(any({
                let seen = seen.clone();
                move |req: Request<Body>| {
                    let seen = seen.clone();
                    let replies = replies.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        let bytes = to_bytes(body, 1 << 20).await.expect("body reads");
                        let body = if bytes.is_empty() {
                            Value::Null
                        } else {
                            serde_json::from_slice(&bytes).expect("request body is JSON")
                        };
                        seen.lock().expect("lock").push(Seen {
                            method: parts.method.to_string(),
                            path: parts.uri.path().to_string(),
                            query: parts.uri.query().unwrap_or("").to_string(),
                            auth: parts
                                .headers
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("")
                                .to_string(),
                            body,
                        });
                        let (code, text) = replies
                            .lock()
                            .expect("lock")
                            .pop()
                            .expect("a scripted reply is left");
                        Response::builder()
                            .status(AxumStatus::from_u16(code).expect("status"))
                            .header("content-type", "application/json")
                            .body(Body::from(text))
                            .expect("response builds")
                    }
                }
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback");
            let addr = listener.local_addr().expect("local addr");
            tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve");
            });
            TestServer {
                base: format!("http://{addr}"),
                seen,
            }
        }

        fn client(&self) -> AgentsClient {
            AgentsClient::new(&self.base, "tok").expect("client builds")
        }

        fn seen(&self) -> Vec<Seen> {
            self.seen.lock().expect("lock").clone()
        }
    }

    fn agent() -> AgentRef {
        AgentRef {
            kind: "assistant".to_string(),
            key: "a b/c".to_string(),
        }
    }

    fn error_body(code: &str, retry: &str) -> Value {
        json!({"code": code, "retry": retry, "reason": "why", "ids": {"agent_id": "ab"}})
    }

    fn send_request() -> SendRequest {
        SendRequest {
            agent: agent(),
            handler: "ask".to_string(),
            request_id: "r1".to_string(),
            payload: json!({"q": 1}),
        }
    }

    #[tokio::test]
    async fn every_error_code_maps_to_its_typed_kind_and_hint() {
        for kind in ErrorKind::ALL {
            let server =
                TestServer::start(vec![(409, error_body(kind.code(), kind.retry().code()))]).await;
            let err = server
                .client()
                .operation("op")
                .await
                .expect_err("a refusal is an error");
            let ClientError::Agent(failure) = &err else {
                panic!("{kind:?} did not map to ClientError::Agent: {err:?}");
            };
            assert_eq!(failure.kind, Some(kind));
            assert_eq!(failure.retry, Some(kind.retry()));
            assert_eq!(failure.status, 409);
            assert_eq!(failure.reason, "why");
            assert_eq!(failure.ids.agent_id.as_deref(), Some("ab"));
        }
    }

    #[tokio::test]
    async fn an_unknown_code_is_kept_verbatim() {
        let server = TestServer::start(vec![(500, error_body("brand_new", "later"))]).await;
        let err = server.client().operation("op").await.expect_err("refused");
        let ClientError::Agent(failure) = err else {
            panic!("not an agent failure");
        };
        assert_eq!(failure.kind, None);
        assert_eq!(failure.code, "brand_new");
        assert_eq!(failure.retry, None);
        assert_eq!(failure.retry_code, "later");
    }

    #[tokio::test]
    async fn plain_401_and_403_bodies_map_to_unauthorized_and_forbidden() {
        let server = TestServer::start(vec![
            (
                401,
                json!({"status": "error", "errorType": "unauthorized", "error": "authentication required"}),
            ),
            (
                403,
                json!({"status": "error", "errorType": "forbidden", "error": "this route requires the agents_admin capability"}),
            ),
        ])
        .await;
        let client = server.client();
        match client.create(&agent()).await {
            Err(ClientError::Unauthorized { message }) => {
                assert_eq!(message, "authentication required");
            }
            other => panic!("expected Unauthorized, got {other:?}"),
        }
        match client.reconcile_authorize_attempt("e", 1).await {
            Err(ClientError::Forbidden { message }) => {
                assert_eq!(message, "this route requires the agents_admin capability");
            }
            other => panic!("expected Forbidden, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_bare_404_is_not_served_and_other_bodies_are_http() {
        let server =
            TestServer::start_raw(vec![(404, String::new()), (502, "bad gateway".to_string())])
                .await;
        let client = server.client();
        let timer = TimerRequest {
            agent: agent(),
            timer_key: "t".to_string(),
            due_unix_ns: 1,
            handler: "h".to_string(),
            payload: Value::Null,
        };
        match client.set_timer(&timer).await {
            Err(ClientError::NotServed { path }) => assert_eq!(path, "/api/v1/agents/timers"),
            other => panic!("expected NotServed, got {other:?}"),
        }
        match client.state(&agent()).await {
            Err(ClientError::Http { status, body }) => {
                assert_eq!(status, 502);
                assert_eq!(body, "bad gateway");
            }
            other => panic!("expected Http, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn outcome_unknown_resolves_once_and_never_resends() {
        let server = TestServer::start(vec![
            (500, error_body("outcome_unknown", "resolve_operation")),
            (200, json!({"operation": "op1", "status": "accepted"})),
        ])
        .await;
        let outcome = server
            .client()
            .send_resolving(&send_request())
            .await
            .expect("resolved");
        assert_eq!(
            outcome,
            SendOutcome::Resolved(ResolveResponse {
                operation: Some("op1".to_string()),
                status: Some(OperationStatus::Accepted),
            })
        );
        let seen = server.seen();
        let paths: Vec<&str> = seen.iter().map(|s| s.path.as_str()).collect();
        assert_eq!(paths, ["/api/v1/agents/send", "/api/v1/agents/resolve"]);
        assert_eq!(seen[1].body["request_id"], "r1");
        assert_eq!(seen[1].body["handler"], "ask");
    }

    #[tokio::test]
    async fn outcome_unknown_whose_body_says_never_is_returned_after_one_request() {
        let server = TestServer::start(vec![(500, error_body("outcome_unknown", "never"))]).await;
        let err = server
            .client()
            .send_resolving(&send_request())
            .await
            .expect_err("not retried");
        assert_eq!(err.kind(), Some(ErrorKind::OutcomeUnknown));
        assert_eq!(err.retry(), Some(RetryHint::Never));
        assert_eq!(server.seen().len(), 1);
    }

    #[tokio::test]
    async fn backoff_errors_are_returned_without_a_retry() {
        let server = TestServer::start(vec![(429, error_body("mailbox_full", "backoff"))]).await;
        let err = server
            .client()
            .send(&send_request())
            .await
            .expect_err("refused");
        assert_eq!(err.kind(), Some(ErrorKind::MailboxFull));
        assert_eq!(server.seen().len(), 1);
    }

    #[test]
    fn long_poll_cap_matches_the_coordinator() {
        assert_eq!(
            LONG_POLL_CAP_MS,
            ravel_agents_runtime::DEFAULT_POLL_WAIT_CAP_MS
        );
    }

    #[tokio::test]
    async fn call_and_result_clamp_the_wait_to_twenty_seconds() {
        let result = json!({"operation": "op", "result": null});
        let call = json!({"operation": "op", "status": "running", "result": null});
        let server = TestServer::start(vec![
            (200, call.clone()),
            (200, call),
            (200, result.clone()),
            (200, result),
        ])
        .await;
        let client = server.client();
        let mut request = CallRequest {
            agent: agent(),
            handler: "ask".to_string(),
            request_id: "r".to_string(),
            payload: Value::Null,
            wait_ms: 60_000,
        };
        client.call(&request).await.expect("call");
        request.wait_ms = 1_500;
        client.call(&request).await.expect("call");
        client.result("op", 3_600_000).await.expect("result");
        client.result("op", 250).await.expect("result");
        let seen = server.seen();
        assert_eq!(seen[0].body["wait_ms"], 20_000);
        assert_eq!(seen[1].body["wait_ms"], 1_500);
        assert_eq!(seen[2].query, "operation=op&wait_ms=20000");
        assert_eq!(seen[3].query, "operation=op&wait_ms=250");
    }

    #[tokio::test]
    async fn event_cursor_resumes_from_next_cursor_and_holds_on_an_empty_page() {
        let event = |seq: u64| json!({"event_seq": seq, "accept_seq": 0, "turn_id": "t", "event_type": "e", "payload": null});
        let server = TestServer::start(vec![
            (
                200,
                json!({"events": [event(3), event(4)], "next_cursor": "4"}),
            ),
            (200, json!({"events": [], "next_cursor": "4"})),
            (200, json!({"events": [event(5)], "next_cursor": "5"})),
        ])
        .await;
        let client = server.client();
        let mut cursor = EventCursor::new(agent(), Some("2".to_string()));
        let first = cursor.next_page(&client, 5000).await.expect("page");
        assert_eq!(first.events.len(), 2);
        cursor.next_page(&client, 10).await.expect("page");
        let third = cursor.next_page(&client, 10).await.expect("page");
        assert_eq!(third.events[0].event_seq, 5);
        assert_eq!(cursor.cursor.as_deref(), Some("5"));
        let sent: Vec<Value> = server
            .seen()
            .iter()
            .map(|s| s.body["cursor"].clone())
            .collect();
        assert_eq!(sent, [json!("2"), json!("4"), json!("4")]);
        assert_eq!(server.seen()[0].body["limit"], EVENTS_PAGE_CAP);
        assert_eq!(server.seen()[1].body["limit"], 10);
    }

    #[tokio::test]
    async fn routes_methods_auth_and_query_encoding() {
        let effect = json!({
            "effect": "e/1", "turn_id": "t", "tool_name": "x", "tool_version": "1",
            "policy": "manual", "status": "in_doubt", "attempt": 1,
            "step_key": "s", "result_attempt": null, "result": null,
            "attempts": [], "evidence": []
        });
        let server = TestServer::start(vec![
            (200, json!({"agent_id": "ab", "incarnation": 1, "created": true})),
            (200, json!({"agent_id": "ab", "incarnation": 1, "state_version": 2, "content_type": "application/json", "state": {}})),
            (200, json!({"effects": []})),
            (200, effect),
            (200, json!({"operation": "op", "status": "cancelled", "requested": false})),
            (200, json!({"effect": "e/1", "status": "completed", "attempt": 1})),
            (200, json!({"outcome": "cancelled"})),
            (200, json!({"mode": "enabled"})),
        ])
        .await;
        let client = server.client();
        client.create(&agent()).await.expect("create");
        client.state(&agent()).await.expect("state");
        client.effects(&agent()).await.expect("effects");
        client.effect("e/1").await.expect("effect");
        client.cancel("op").await.expect("cancel");
        client
            .reconcile_attach_result("e/1", 1, json!({"ticket": "T-1"}))
            .await
            .expect("reconcile");
        client
            .cancel_timer(&CancelTimerRequest {
                agent: agent(),
                timer_key: "k".to_string(),
            })
            .await
            .expect("cancel timer");
        client.health().await.expect("health");
        let seen = server.seen();
        let shape: Vec<(&str, &str)> = seen
            .iter()
            .map(|s| (s.method.as_str(), s.path.as_str()))
            .collect();
        assert_eq!(
            shape,
            [
                ("POST", "/api/v1/agents/create"),
                ("GET", "/api/v1/agents/state"),
                ("GET", "/api/v1/agents/effects"),
                ("GET", "/api/v1/agents/effects/e%2F1"),
                ("POST", "/api/v1/agents/cancel"),
                ("POST", "/api/v1/agents/reconcile"),
                ("DELETE", "/api/v1/agents/timers"),
                ("GET", "/api/v1/agents/health"),
            ]
        );
        assert!(seen.iter().all(|s| s.auth == "Bearer tok"));
        assert_eq!(seen[1].query, "kind=assistant&key=a+b%2Fc");
        assert_eq!(
            seen[5].body,
            json!({"effect": "e/1", "attempt": 1, "decision": "record_result", "result": {"ticket": "T-1"}})
        );
    }

    fn fixture(name: &str) -> String {
        let path = format!(
            "{}/../../sdk/fixtures/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(&path).expect("fixture reads")
    }

    #[tokio::test]
    async fn destroy_posts_the_agent_and_decodes_the_response() {
        let response = fixture("destroy_agent_response");
        let server = TestServer::start_raw(vec![
            (200, response.clone()),
            (200, response),
            (404, error_body("agent_gone", "never").to_string()),
        ])
        .await;
        let client = server.client();
        let order = AgentRef {
            kind: "order".to_string(),
            key: "o-1".to_string(),
        };
        let out = client.destroy(&order, Some(1)).await.expect("destroy");
        assert!(out.destroyed);
        assert_eq!(out.agent_id, "19cd0e6876991314c1b6dbd8bd129297");
        assert_eq!(out.incarnation, 1);
        assert_eq!(out.abandoned.inputs, 2);
        assert!(out.abandoned.active_turn);
        assert_eq!(out.abandoned.effects_in_doubt, 1);
        assert_eq!(out.abandoned.effects_authorized, 0);
        assert_eq!(out.abandoned.timers_scheduled, 1);
        client.destroy(&order, None).await.expect("destroy");
        let err = client
            .destroy(&order, Some(7))
            .await
            .expect_err("agent_gone is an error");
        assert_eq!(err.kind(), Some(ErrorKind::AgentGone));
        assert_eq!(err.retry(), Some(RetryHint::Never));

        let seen = server.seen();
        assert_eq!(seen.len(), 3);
        for s in &seen {
            assert_eq!(s.method, "POST");
            assert_eq!(s.path, "/api/v1/agents/destroy");
            assert_eq!(s.auth, "Bearer tok");
        }
        let request: Value =
            serde_json::from_str(&fixture("destroy_agent_request")).expect("fixture is JSON");
        assert_eq!(seen[0].body, request);
        assert_eq!(seen[1].body, json!({"agent": {"kind": "order", "key": "o-1"}}));
        assert!(seen[1].body.get("incarnation").is_none());
        assert_eq!(seen[2].body["incarnation"], 7);
    }

    #[tokio::test]
    async fn a_base_url_with_a_path_prefix_keeps_it() {
        let server = TestServer::start(vec![(200, json!({"effects": []}))]).await;
        let client = AgentsClient::new(&format!("{}/prefix", server.base), "tok").expect("client");
        client.effects(&agent()).await.expect("effects");
        assert_eq!(server.seen()[0].path, "/prefix/api/v1/agents/effects");
    }

    #[test]
    fn a_relative_base_url_is_refused() {
        assert!(matches!(
            AgentsClient::new("not a url", "tok"),
            Err(ClientError::BaseUrl { .. })
        ));
    }

    #[test]
    fn debug_output_redacts_the_bearer_token() {
        let token = "secret-bearer-4f1c";
        let client = AgentsClient::new("http://127.0.0.1:4318", token).expect("client");
        let printed = format!("{client:?}");
        assert!(!printed.contains(token), "{printed}");
        assert!(printed.contains("127.0.0.1:4318"), "{printed}");
    }
}
