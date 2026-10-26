//! HTTP client for the pinned Prometheus instance: pushes the encoded
//! Remote Write 1.0 payload, then issues the same `/api/v1/query` and
//! `/api/v1/query_range` calls the runner also issues against Ravel.
//!
//! Query calls run under a deadline that encloses the complete operation
//! (connect, request send, response headers, body transfer and JSON parse),
//! and report a typed [`QueryOutcome`] so a query that exceeds its deadline
//! is distinguishable from every other failure (ADR-0927 decision 6). The
//! MetricsBench status mapper (task T3 of #2821, a `metrics_correctness`
//! module in `ravel-bench` that does not exist yet) is the intended consumer
//! of that outcome; [`PrometheusClient::instant_query`] and
//! [`PrometheusClient::range_query`] are thin wrappers that keep the
//! differential runner on its parsed-envelope `Result`.
//!
//! `reqwest` is a crate-local external dependency
//! (crates/ravel-promql-difftest/Cargo.toml only). It is built without a
//! decompression feature, so the body bytes read here are the bytes parsed.

use serde_json::Value as Json;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// Query deadline used by [`PrometheusClient::new`].
pub const DEFAULT_QUERY_DEADLINE: Duration = Duration::from_secs(120);

/// Response body cap used unless [`PrometheusClient::with_max_body_bytes`]
/// sets another.
pub const DEFAULT_MAX_BODY_BYTES: u64 = 256 * 1024 * 1024;

/// How many per-call-deadline query clients a [`PrometheusClient`] keeps
/// before evicting the oldest. Each carries its own connection pool, so the
/// set is bounded rather than grown per distinct deadline.
pub const MAX_OVERRIDE_CLIENTS: usize = 8;

/// How much earlier than the query deadline the transport's own connect
/// timer fires. It exists so a connect stall reports [`TimeoutPhase::Connect`]
/// rather than losing the race to the whole-operation timer, which is armed
/// first and would report [`TimeoutPhase::Headers`].
const CONNECT_MARGIN: Duration = Duration::from_millis(10);

#[derive(Debug, thiserror::Error)]
pub enum PrometheusClientError {
    #[error("http request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("remote-write push rejected: status {status}, body: {body}")]
    RemoteWriteRejected { status: u16, body: String },
    #[error("prometheus did not become ready within {0:?}")]
    NotReady(Duration),
    #[error("query exceeded its deadline in the {phase:?} phase after {elapsed:?}")]
    Timeout {
        phase: TimeoutPhase,
        elapsed: Duration,
    },
    #[error("query transport failure ({kind:?}): {message}")]
    Transport {
        kind: TransportErrorKind,
        message: String,
    },
    #[error("query response body exceeds the {limit}-byte cap")]
    BodyTooLarge { limit: u64 },
}

/// The phase a query was in when its deadline expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutPhase {
    /// The transport's connect timer fired: no connection was established.
    Connect,
    /// The deadline expired after dispatch and before the response headers
    /// arrived.
    Headers,
    /// The headers arrived; the deadline expired before the body completed.
    Body,
    /// The body was complete; the deadline expired before, or while, it was
    /// parsed as JSON.
    Parse,
}

/// What failed in a non-timeout transport failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportErrorKind {
    /// The connection could not be established (refused, unreachable).
    Connect,
    /// Sending the request or reading the response headers failed.
    Request,
    /// The body could not be read to completion (the peer closed early).
    Body,
    /// The complete body is not JSON.
    Decode,
}

/// Per-phase wall-clock split of one query, measured from dispatch. A phase
/// that was never reached is `None`, so a timeout in the body phase has
/// `dispatch_to_headers` set and `headers_to_body_complete` unset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryTiming {
    /// Dispatch to response headers received (connect and send included).
    pub dispatch_to_headers: Option<Duration>,
    /// Response headers received to the last body byte read.
    pub headers_to_body_complete: Option<Duration>,
    /// Duration of the JSON parse of the complete body.
    pub parse: Option<Duration>,
    /// Dispatch to the outcome, whatever it is.
    pub elapsed: Duration,
}

/// Result of one query under a deadline.
#[derive(Debug)]
pub enum QueryOutcome {
    /// The body parsed as JSON. Returned for every HTTP status, 4xx and 5xx
    /// included: the error corpus and the refused/partial classification
    /// read `status`, `errorType` and `error` from the body.
    Envelope {
        http_status: u16,
        body: Json,
        timing: QueryTiming,
    },
    /// The deadline expired in `phase` after `elapsed` from dispatch.
    Timeout {
        phase: TimeoutPhase,
        elapsed: Duration,
        timing: QueryTiming,
    },
    /// The operation failed within the deadline for a reason other than
    /// time.
    Transport {
        kind: TransportErrorKind,
        message: String,
        timing: QueryTiming,
    },
    /// The body (declared or received) exceeds `limit` bytes.
    BodyTooLarge { limit: u64, timing: QueryTiming },
}

impl QueryOutcome {
    pub fn timing(&self) -> &QueryTiming {
        match self {
            QueryOutcome::Envelope { timing, .. }
            | QueryOutcome::Timeout { timing, .. }
            | QueryOutcome::Transport { timing, .. }
            | QueryOutcome::BodyTooLarge { timing, .. } => timing,
        }
    }

    /// The parsed envelope, or the failure as a [`PrometheusClientError`];
    /// a timeout maps to [`PrometheusClientError::Timeout`] and to nothing
    /// else.
    pub fn into_result(self) -> Result<Json, PrometheusClientError> {
        match self {
            QueryOutcome::Envelope { body, .. } => Ok(body),
            QueryOutcome::Timeout { phase, elapsed, .. } => {
                Err(PrometheusClientError::Timeout { phase, elapsed })
            }
            QueryOutcome::Transport { kind, message, .. } => {
                Err(PrometheusClientError::Transport { kind, message })
            }
            QueryOutcome::BodyTooLarge { limit, .. } => {
                Err(PrometheusClientError::BodyTooLarge { limit })
            }
        }
    }
}

pub struct PrometheusClient {
    /// Readiness polling and remote write. Not a query client: `wait_ready`
    /// bounds each probe by its own remaining timeout, and `remote_write`
    /// keeps the unbounded send it had before the query deadline existed.
    control: reqwest::Client,
    /// Query client whose connect timer matches `deadline`.
    query: reqwest::Client,
    /// Query clients for per-call deadlines other than `deadline`, keyed by
    /// that deadline so each keeps its own connection pool. Bounded to
    /// [`MAX_OVERRIDE_CLIENTS`] entries, oldest evicted, so a caller that
    /// passes a fresh remaining-budget deadline per query cannot grow one
    /// client and pool per call.
    overrides: Mutex<Vec<(Duration, reqwest::Client)>>,
    base_url: String,
    deadline: Duration,
    max_body_bytes: u64,
}

impl PrometheusClient {
    /// A client whose queries run under [`DEFAULT_QUERY_DEADLINE`].
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_deadline(base_url, DEFAULT_QUERY_DEADLINE)
    }

    /// A client whose queries each complete, or report
    /// [`QueryOutcome::Timeout`], within `deadline` of dispatch.
    pub fn with_deadline(base_url: impl Into<String>, deadline: Duration) -> Self {
        PrometheusClient {
            control: reqwest::Client::new(),
            query: query_client(deadline),
            overrides: Mutex::new(Vec::new()),
            base_url: base_url.into(),
            deadline,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }

    /// Caps the response body at `limit` bytes; a larger body is
    /// [`QueryOutcome::BodyTooLarge`].
    pub fn with_max_body_bytes(mut self, limit: u64) -> Self {
        self.max_body_bytes = limit;
        self
    }

    pub fn deadline(&self) -> Duration {
        self.deadline
    }

    pub fn max_body_bytes(&self) -> u64 {
        self.max_body_bytes
    }

    /// How many per-call-deadline query clients are cached right now. Never
    /// exceeds [`MAX_OVERRIDE_CLIENTS`]; exposed so a test can pin the bound.
    pub fn cached_override_clients(&self) -> usize {
        self.overrides
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Polls `/-/ready` until it returns 2xx or `timeout` elapses.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<(), PrometheusClientError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // A probe the server accepts but never answers must not outlive
            // the overall timeout: bound each send by what remains of it.
            let probe = tokio::time::timeout_at(
                deadline,
                self.control
                    .get(format!("{}/-/ready", self.base_url))
                    .send(),
            )
            .await;
            if let Ok(Ok(resp)) = probe
                && resp.status().is_success()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(PrometheusClientError::NotReady(timeout));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// POSTs a pre-encoded (protobuf + snappy) Remote Write 1.0 body.
    pub async fn remote_write(&self, body: Vec<u8>) -> Result<(), PrometheusClientError> {
        let resp = self
            .control
            .post(format!("{}/api/v1/write", self.base_url))
            .header("Content-Encoding", "snappy")
            .header("Content-Type", "application/x-protobuf")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(PrometheusClientError::RemoteWriteRejected {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }

    /// `/api/v1/query` at `time_ms`. Returns the parsed JSON envelope even
    /// when the HTTP status is an error (`status: "error"` bodies are the
    /// comparator's input on the error-corpus path).
    pub async fn instant_query(
        &self,
        query: &str,
        time_ms: i64,
    ) -> Result<Json, PrometheusClientError> {
        self.instant_query_with_deadline(query, time_ms, self.deadline)
            .await
            .into_result()
    }

    /// `/api/v1/query_range` over `[start_ms, end_ms]` stepped by `step_ms`.
    pub async fn range_query(
        &self,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Json, PrometheusClientError> {
        self.range_query_with_deadline(query, start_ms, end_ms, step_ms, self.deadline)
            .await
            .into_result()
    }

    /// [`Self::instant_query`] under `deadline` instead of the client's.
    pub async fn instant_query_with_deadline(
        &self,
        query: &str,
        time_ms: i64,
        deadline: Duration,
    ) -> QueryOutcome {
        let params = [
            ("query", query.to_string()),
            ("time", ms_to_seconds_str(time_ms)),
        ];
        self.get_with_deadline("/api/v1/query", &params, deadline)
            .await
    }

    /// [`Self::range_query`] under `deadline` instead of the client's.
    pub async fn range_query_with_deadline(
        &self,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        deadline: Duration,
    ) -> QueryOutcome {
        let params = [
            ("query", query.to_string()),
            ("start", ms_to_seconds_str(start_ms)),
            ("end", ms_to_seconds_str(end_ms)),
            ("step", format!("{}ms", step_ms)),
        ];
        self.get_with_deadline("/api/v1/query_range", &params, deadline)
            .await
    }

    fn client_for(&self, deadline: Duration) -> reqwest::Client {
        if deadline == self.deadline {
            return self.query.clone();
        }
        let mut overrides = self
            .overrides
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, client)) = overrides.iter().find(|(d, _)| *d == deadline) {
            return client.clone();
        }
        let client = query_client(deadline);
        if overrides.len() >= MAX_OVERRIDE_CLIENTS {
            overrides.remove(0);
        }
        overrides.push((deadline, client.clone()));
        client
    }

    /// GETs `path` and runs every phase against one instant, `deadline`
    /// after dispatch. Headers and each body chunk are awaited under
    /// `timeout_at` that instant, and the parse under [`parse_within`], so
    /// no phase gets a budget of its own.
    async fn get_with_deadline(
        &self,
        path: &str,
        params: &[(&str, String)],
        deadline: Duration,
    ) -> QueryOutcome {
        let client = self.client_for(deadline);
        let start = Instant::now();
        let deadline_at = start + deadline;
        let mut timing = QueryTiming::default();

        let send = client
            .get(format!("{}{}", self.base_url, path))
            .query(params)
            .send();
        let mut resp = match tokio::time::timeout_at(deadline_at, send).await {
            Err(_) => return timeout(TimeoutPhase::Headers, start, timing),
            Ok(Err(e)) if e.is_connect() && e.is_timeout() => {
                return timeout(TimeoutPhase::Connect, start, timing);
            }
            Ok(Err(e)) => {
                let kind = if e.is_connect() {
                    TransportErrorKind::Connect
                } else {
                    TransportErrorKind::Request
                };
                return transport(kind, e.to_string(), start, timing);
            }
            Ok(Ok(resp)) => resp,
        };
        let headers_at = Instant::now();
        timing.dispatch_to_headers = Some(headers_at - start);
        let http_status = resp.status().as_u16();

        let limit = self.max_body_bytes;
        if resp.content_length().is_some_and(|len| len > limit) {
            timing.elapsed = start.elapsed();
            return QueryOutcome::BodyTooLarge { limit, timing };
        }
        let mut body = Vec::new();
        loop {
            match tokio::time::timeout_at(deadline_at, resp.chunk()).await {
                Err(_) => return timeout(TimeoutPhase::Body, start, timing),
                Ok(Err(e)) => {
                    return transport(TransportErrorKind::Body, e.to_string(), start, timing);
                }
                Ok(Ok(None)) => break,
                Ok(Ok(Some(chunk))) => {
                    if (body.len() + chunk.len()) as u64 > limit {
                        timing.elapsed = start.elapsed();
                        return QueryOutcome::BodyTooLarge { limit, timing };
                    }
                    body.extend_from_slice(&chunk);
                }
            }
        }
        let body_at = Instant::now();
        timing.headers_to_body_complete = Some(body_at - headers_at);

        match parse_within(body, deadline_at).await {
            ParseResult::Parsed(json) => {
                timing.parse = Some(body_at.elapsed());
                timing.elapsed = start.elapsed();
                QueryOutcome::Envelope {
                    http_status,
                    body: json,
                    timing,
                }
            }
            ParseResult::Timeout => timeout(TimeoutPhase::Parse, start, timing),
            ParseResult::Failed(message) => {
                timing.parse = Some(body_at.elapsed());
                transport(TransportErrorKind::Decode, message, start, timing)
            }
        }
    }
}

fn query_client(deadline: Duration) -> reqwest::Client {
    let connect = deadline
        .saturating_sub(CONNECT_MARGIN)
        .max(Duration::from_millis(1));
    // `build` fails only where `reqwest::Client::new` would panic (the TLS
    // backend cannot initialise); the query path stays bounded either way,
    // only connect stalls then report as `Headers`.
    reqwest::Client::builder()
        .connect_timeout(connect)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

fn timeout(phase: TimeoutPhase, start: Instant, mut timing: QueryTiming) -> QueryOutcome {
    let elapsed = start.elapsed();
    timing.elapsed = elapsed;
    QueryOutcome::Timeout {
        phase,
        elapsed,
        timing,
    }
}

fn transport(
    kind: TransportErrorKind,
    message: String,
    start: Instant,
    mut timing: QueryTiming,
) -> QueryOutcome {
    timing.elapsed = start.elapsed();
    QueryOutcome::Transport {
        kind,
        message,
        timing,
    }
}

#[derive(Debug)]
enum ParseResult {
    Parsed(Json),
    Timeout,
    Failed(String),
}

/// Parses `body` as JSON, returning [`ParseResult::Timeout`] if `deadline`
/// passes first. A budget already spent refuses without parsing. Otherwise
/// the parse runs on the blocking pool and only the join is awaited, under
/// `timeout_at(deadline)`, so a slow parse cannot hold the caller past the
/// deadline by more than the timer's resolution; the abandoned parse runs on
/// to completion on its blocking thread, bounded by the body cap.
async fn parse_within(body: Vec<u8>, deadline: Instant) -> ParseResult {
    if Instant::now() >= deadline {
        return ParseResult::Timeout;
    }
    let parse = tokio::task::spawn_blocking(move || serde_json::from_slice::<Json>(&body));
    match tokio::time::timeout_at(deadline, parse).await {
        Err(_) => ParseResult::Timeout,
        Ok(Err(join)) => ParseResult::Failed(format!("parse task failed: {join}")),
        Ok(Ok(Err(e))) => ParseResult::Failed(e.to_string()),
        Ok(Ok(Ok(json))) => ParseResult::Parsed(json),
    }
}

/// Formats milliseconds as a `[-]seconds.millis` decimal. Splitting on
/// `unsigned_abs` rather than `div_euclid`/`rem_euclid` matters for negative
/// values: `-500ms` must render as `-0.500` (i.e. `-0.5`), not `-1.500`
/// (`-1.5`), which is what a naive euclidean split of the sign and
/// fractional part would produce.
fn ms_to_seconds_str(ms: i64) -> String {
    let sign = if ms < 0 { "-" } else { "" };
    let abs = ms.unsigned_abs();
    format!("{sign}{}.{:03}", abs / 1000, abs % 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_milliseconds_as_seconds_with_millis_precision() {
        assert_eq!(ms_to_seconds_str(1_700_000_000_123), "1700000000.123");
        assert_eq!(ms_to_seconds_str(0), "0.000");
    }

    #[test]
    fn formats_negative_milliseconds_correctly() {
        assert_eq!(ms_to_seconds_str(-500), "-0.500");
        assert_eq!(ms_to_seconds_str(-1_500), "-1.500");
        assert_eq!(ms_to_seconds_str(-1_700_000_000_123), "-1700000000.123");
    }

    #[tokio::test]
    async fn spent_budget_before_parse_is_a_parse_timeout() {
        let body = br#"{"status":"success"}"#.to_vec();
        let result = parse_within(body, Instant::now()).await;
        assert!(matches!(result, ParseResult::Timeout), "{result:?}");
    }

    #[tokio::test]
    async fn budget_left_parses_the_body() {
        let body = br#"{"status":"success"}"#.to_vec();
        let deadline = Instant::now() + Duration::from_secs(3600);
        match parse_within(body, deadline).await {
            ParseResult::Parsed(json) => assert_eq!(json["status"], "success"),
            other => panic!("expected Parsed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_json_body_is_a_decode_failure_not_a_timeout() {
        let deadline = Instant::now() + Duration::from_secs(3600);
        let result = parse_within(b"<html>".to_vec(), deadline).await;
        assert!(matches!(result, ParseResult::Failed(_)), "{result:?}");
    }

    #[test]
    fn per_call_deadline_clients_are_bounded() {
        let client = PrometheusClient::new("http://127.0.0.1:9");
        for extra_ms in 1..=(MAX_OVERRIDE_CLIENTS as u64 * 2) {
            let _ = client.client_for(DEFAULT_QUERY_DEADLINE + Duration::from_millis(extra_ms));
        }
        assert_eq!(client.cached_override_clients(), MAX_OVERRIDE_CLIENTS);
        // The default deadline never enters the cache.
        let _ = client.client_for(DEFAULT_QUERY_DEADLINE);
        assert_eq!(client.cached_override_clients(), MAX_OVERRIDE_CLIENTS);
    }

    #[test]
    fn timeout_outcome_maps_to_the_timeout_error_only() {
        let outcome = QueryOutcome::Timeout {
            phase: TimeoutPhase::Body,
            elapsed: Duration::from_millis(500),
            timing: QueryTiming::default(),
        };
        match outcome.into_result() {
            Err(PrometheusClientError::Timeout { phase, elapsed }) => {
                assert_eq!(phase, TimeoutPhase::Body);
                assert_eq!(elapsed, Duration::from_millis(500));
            }
            other => panic!("expected PrometheusClientError::Timeout, got {other:?}"),
        }
    }
}
