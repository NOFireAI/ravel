//! Runs a parsed corpus against both stacks and diffs each entry.
//! A failing entry keeps
//! both raw JSON bodies attached so a CI failure can archive them as
//! artifacts without re-running anything.

use std::path::{Path, PathBuf};

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use serde::Serialize;
use serde_json::Value as Json;
use tower::ServiceExt;

use crate::comparator::{Verdict, compare};
use crate::corpus::{CorpusEntry, Eval};
use crate::prometheus_client::PrometheusClient;

/// Environment variable naming the path [`run_corpus`] writes its JSON report
/// to. Unset, nothing is written.
///
/// This is the producing half of the conformance table's agreed dimension: the
/// `conformance_table` test reads such a file through `RAVEL_DIFFTEST_REPORT`,
/// and without a producer that row could never be anything but "not measured
/// in this run". The write happens inside `run_corpus`, before it returns, so a
/// caller that panics on a mismatch has already published the report: a
/// mismatching run is exactly the run whose agreed figure matters.
pub const REPORT_OUT_ENV: &str = "RAVEL_DIFFTEST_REPORT_OUT";

#[derive(Debug, thiserror::Error)]
pub enum RavelQueryError {
    #[error("building request: {0}")]
    Request(#[from] axum::http::Error),
    #[error("reading response body: {0}")]
    Body(String),
    #[error("parsing response json: {0}")]
    Json(#[from] serde_json::Error),
}

/// One corpus entry that did not match, with both raw bodies.
///
/// `Serialize` is the contract the archived JSON report rests on: the field
/// names below are the ones a reader of the report
/// (`conformance_table`'s `load_run_report`) looks for, so they are generated
/// from this type rather than written out by hand somewhere else.
#[derive(Debug, Serialize)]
pub struct Failure {
    pub entry_name: String,
    pub query: String,
    pub detail: String,
    pub prometheus_body: Json,
    pub ravel_body: Json,
}

/// Pass/fail totals for one corpus run.
///
/// The per-construct breakdown ADR-0035 asks for is
/// [`crate::scoring::ConformanceReport`], a sibling type rather than a field
/// here: it is derived from the same run (feed this report to
/// [`ConformanceReport::apply_run_report`](crate::scoring::ConformanceReport::apply_run_report))
/// but is also buildable without a pinned Prometheus binary, so `runner.rs`
/// stays a plain corpus-vs-Prometheus differ.
#[derive(Debug, Default, Serialize)]
pub struct RunReport {
    pub total: usize,
    pub failures: Vec<Failure>,
}

impl RunReport {
    pub fn is_clean(&self) -> bool {
        self.failures.is_empty()
    }

    /// Writes the report as JSON to `path`, creating the parent directory if it
    /// does not exist.
    pub fn write_json(&self, path: &Path) -> Result<(), ReportWriteError> {
        let io = |e: std::io::Error| ReportWriteError {
            path: path.display().to_string(),
            message: e.to_string(),
        };
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(io)?;
        }
        let text = serde_json::to_string_pretty(self).map_err(|e| ReportWriteError {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
        std::fs::write(path, text).map_err(io)
    }
}

/// Writing an archived run report failed.
#[derive(Debug, thiserror::Error)]
#[error("writing the run report to '{path}': {message}")]
pub struct ReportWriteError {
    /// The path being written.
    pub path: String,
    /// The underlying error.
    pub message: String,
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
    let report = RunReport {
        total: entries.len(),
        failures,
    };
    publish_report(&report);
    report
}

/// Archives `report` when [`REPORT_OUT_ENV`] names a path.
///
/// A failed write aborts here rather than being reported to the caller: the
/// only callers are tests, the path was asked for explicitly, and the run that
/// follows this call panics on a mismatch, so a swallowed error would end as a
/// CI step reading `RAVEL_DIFFTEST_REPORT` and finding nothing, several minutes
/// later and with no trace of why.
fn publish_report(report: &RunReport) {
    let Some(path) = std::env::var_os(REPORT_OUT_ENV).map(PathBuf::from) else {
        return;
    };
    match report.write_json(&path) {
        Ok(()) => eprintln!(
            "wrote the differential run report ({} entries, {} failures) to {}",
            report.total,
            report.failures.len(),
            path.display()
        ),
        Err(e) => panic!("{REPORT_OUT_ENV}: {e}"),
    }
}

/// Instant-queries the in-process Ravel router. `pub(crate)` so
/// [`crate::scoring::run_ravel_only`] drives the identical request shape
/// (URI, encoding, auth header) the differential run uses, rather than a
/// second near-copy that could drift from it.
pub(crate) async fn ravel_instant_query(
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

/// Range-queries the in-process Ravel router. `pub(crate)` for the same
/// reason as [`ravel_instant_query`].
pub(crate) async fn ravel_range_query(
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
