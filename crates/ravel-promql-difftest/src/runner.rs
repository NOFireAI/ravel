//! Runs a parsed corpus against both stacks and diffs each entry
//! (docs/promql-evaluator-plan.md section 5.4/5.5). A failing entry keeps
//! both raw JSON bodies attached so a CI failure can archive them as
//! artifacts without re-running anything.

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use serde_json::Value as Json;
use tower::ServiceExt;

use crate::comparator::{Verdict, compare};
use crate::corpus::{CorpusEntry, Eval};
use crate::prometheus_client::PrometheusClient;

#[derive(Debug, thiserror::Error)]
pub enum RavelQueryError {
    #[error("building request: {0}")]
    Request(#[from] axum::http::Error),
    #[error("reading response body: {0}")]
    Body(String),
    #[error("parsing response json: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug)]
pub struct Failure {
    pub entry_name: String,
    pub query: String,
    pub detail: String,
    pub prometheus_body: Json,
    pub ravel_body: Json,
}

#[derive(Debug, Default)]
pub struct RunReport {
    pub total: usize,
    pub failures: Vec<Failure>,
}

impl RunReport {
    pub fn is_clean(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Runs every `entries` item against `prometheus` and `ravel_app`
/// (`base_ts_ms` anchors the corpus' relative offsets), comparing bodies
/// per each entry's own mode.
pub async fn run_corpus(
    entries: &[CorpusEntry],
    base_ts_ms: i64,
    prometheus: &PrometheusClient,
    ravel_app: &Router,
    ravel_token: &str,
) -> RunReport {
    let mut failures = Vec::new();
    for entry in entries {
        let (prom_result, ravel_result) = match entry.eval {
            Eval::Instant { time_offset_ms } => {
                let time_ms = base_ts_ms + time_offset_ms;
                (
                    prometheus.instant_query(&entry.query, time_ms).await,
                    ravel_instant_query(ravel_app, ravel_token, &entry.query, time_ms).await,
                )
            }
            Eval::Range {
                start_offset_ms,
                end_offset_ms,
                step_ms,
            } => {
                let start_ms = base_ts_ms + start_offset_ms;
                let end_ms = base_ts_ms + end_offset_ms;
                (
                    prometheus
                        .range_query(&entry.query, start_ms, end_ms, step_ms)
                        .await,
                    ravel_range_query(
                        ravel_app,
                        ravel_token,
                        &entry.query,
                        start_ms,
                        end_ms,
                        step_ms,
                    )
                    .await,
                )
            }
        };

        let (prom_body, ravel_body) = match (prom_result, ravel_result) {
            (Ok(p), Ok(r)) => (p, r),
            (prom_result, ravel_result) => {
                failures.push(Failure {
                    entry_name: entry.name.clone(),
                    query: entry.query.clone(),
                    detail: format!(
                        "transport error: prometheus={prom_result:?} ravel={ravel_result:?}"
                    ),
                    prometheus_body: Json::Null,
                    ravel_body: Json::Null,
                });
                continue;
            }
        };

        if let Verdict::Mismatch(detail) =
            compare(entry.mode, entry.tolerance_ulps, &prom_body, &ravel_body)
        {
            failures.push(Failure {
                entry_name: entry.name.clone(),
                query: entry.query.clone(),
                detail,
                prometheus_body: prom_body,
                ravel_body,
            });
        }
    }
    RunReport {
        total: entries.len(),
        failures,
    }
}

async fn ravel_instant_query(
    app: &Router,
    token: &str,
    query: &str,
    time_ms: i64,
) -> Result<Json, RavelQueryError> {
    let uri = format!(
        "/api/v1/query?query={}&time={}",
        encode_query_param(query),
        ms_to_seconds_str(time_ms)
    );
    ravel_get(app, token, &uri).await
}

async fn ravel_range_query(
    app: &Router,
    token: &str,
    query: &str,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<Json, RavelQueryError> {
    let uri = format!(
        "/api/v1/query_range?query={}&start={}&end={}&step={}ms",
        encode_query_param(query),
        ms_to_seconds_str(start_ms),
        ms_to_seconds_str(end_ms),
        step_ms
    );
    ravel_get(app, token, &uri).await
}

async fn ravel_get(app: &Router, token: &str, uri: &str) -> Result<Json, RavelQueryError> {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())?;
    // `Router`'s `Service::Error` is `Infallible`, so this is an exhaustive
    // (empty) match rather than an `unwrap`/`expect` on the `Result`.
    let response = match app.clone().oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|e| RavelQueryError::Body(e.to_string()))?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Percent-encodes everything except unreserved characters, matching the
/// encoding `crates/ravel-query/tests/e2e.rs` uses and the decoder the
/// query-param parser expects (braces, quotes, and `=` all appear in
/// PromQL text).
fn encode_query_param(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn ms_to_seconds_str(ms: i64) -> String {
    let sign = if ms < 0 { "-" } else { "" };
    let abs = ms.unsigned_abs();
    format!("{sign}{}.{:03}", abs / 1000, abs % 1000)
}
