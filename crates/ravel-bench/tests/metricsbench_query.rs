//! End-to-end tests for the portable query run (issue #938, epic #2821 W2b):
//! the shipping `metricsbench_query` binary against stub engines on loopback
//! HTTP, bound to an ingest report built from `metrics_ingest`'s own types,
//! with its output checked by the shipping `metricsbench_report validate`.
//!
//! The stubs answer every query with one fixed series unless a test scripts
//! a different answer for a query text, so an engine is correct on an entry
//! exactly when it answers like the oracle stub.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ravel_bench::metrics_correctness::{Engine, RefusalReason};
use ravel_bench::metrics_ingest::{
    ClassDigests, IngestPhase, LatencyReport, MetricsIngestReport, PayloadClass, ProfileRecord,
    Readback, ReadbackExpected, RunRecord, ScalingLabelRecord, StreamCounts, SystemIngestResult,
    UNSUPPORTED_CLASSES, UnsupportedClassRecord, workload_digest,
};
use ravel_bench::report_schema::{
    Backend, Comparator, Hardware, Measurement, MetricsBenchReport, Provenance, QueryKind,
    ResultStatus,
};
use serde_json::{Value as Json, json};

const ANCHOR_MS: i64 = 1_700_000_010_000;
const STEPS: u64 = 120;
const RAVEL_TOKEN: &str = "s3cret-ravel-bearer-7f2c";
const RAVEL_TOKEN_ENV: &str = "METRICSBENCH_TEST_RAVEL_TOKEN";
const RAVEL_COMMIT_TOKEN: &str = "0:17,1:4,2:9";

const GAUGE: &str = "metricsbench_gauge_cpu_percent{job=\"metricsbench-api\",region=\"eu-west-1\",instance=\"metricsbench-instance-0\"}";
const COUNTER_RATE: &str = "rate(metricsbench_requests_total{job=\"metricsbench-api\",method=\"GET\",status=\"200\",instance=\"metricsbench-instance-0\"}[5m])";
const JOB_RATE_SUM: &str =
    "sum by (job) (rate(metricsbench_requests_total{job=\"metricsbench-api\"}[5m]))";
const NATIVE_P95: &str = "histogram_quantile(0.95, metricsbench_latency_native)";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn workload_path() -> PathBuf {
    repo_root().join("benchmarks/metrics/workload.json")
}

/// A four-entry corpus: one instant, two range, one reading the native
/// histogram family. Entries copied from the checked-in corpus.
fn corpus() -> Json {
    json!({
        "version": 3,
        "entries": [
            {
                "id": "mb_single_gauge_instant",
                "class": "single_series",
                "promql": GAUGE,
                "eval": "instant",
                "constructs": ["vector selector", "label matcher ="]
            },
            {
                "id": "mb_single_counter_rate",
                "class": "single_series",
                "promql": COUNTER_RATE,
                "eval": "range",
                "constructs": ["function call", "rate", "matrix selector", "vector selector", "label matcher ="]
            },
            {
                "id": "mb_selective_job_rate_sum",
                "class": "selective_multi_series",
                "promql": JOB_RATE_SUM,
                "eval": "range",
                "constructs": ["aggregate expression", "sum", "by", "paren expression", "function call", "rate", "matrix selector", "vector selector", "label matcher ="]
            },
            {
                "id": "mb_histogram_native_p95",
                "class": "histogram",
                "promql": NATIVE_P95,
                "eval": "instant",
                "constructs": ["function call", "histogram_quantile", "number literal", "vector selector"]
            }
        ]
    })
}

/// A scripted answer for one query text.
#[derive(Clone, Copy, Debug)]
enum Answer {
    /// The fixed series every correct engine returns.
    Correct,
    /// The fixed series with a different value.
    Diverge,
    /// The fixed series two ULPs above the correct value.
    TwoUlps,
    /// `success` with no series.
    Empty,
    /// HTTP 422, Mimir's chunk-limit refusal.
    RefuseChunks,
    /// No answer before the runner's deadline.
    Hang,
    /// [`Answer::Hang`] on the first `n` requests for the query, then
    /// [`Answer::Correct`].
    HangFirst(usize),
}

#[derive(Clone, Debug)]
struct Recorded {
    path: &'static str,
    params: HashMap<String, String>,
    /// Every `min_commit_token` value, in order; `params` keeps one value
    /// per name.
    min_commit_tokens: Vec<String>,
    authorization: Option<String>,
}

#[derive(Clone)]
struct Stub {
    script: Arc<HashMap<String, Answer>>,
    bearer: Option<String>,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

impl Stub {
    fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    fn count(&self, path: &str, promql: &str) -> usize {
        self.requests()
            .iter()
            .filter(|r| r.path == path && r.params.get("query").map(String::as_str) == Some(promql))
            .count()
    }
}

fn series(value: &str, kind: &'static str, params: &HashMap<String, String>) -> Json {
    let metric = json!({"job": "metricsbench-api"});
    if kind == "instant" {
        let t: f64 = params["time"].parse().unwrap();
        json!({"resultType": "vector", "result": [{"metric": metric, "value": [t, value]}]})
    } else {
        let start: f64 = params["start"].parse().unwrap();
        let end: f64 = params["end"].parse().unwrap();
        json!({"resultType": "matrix", "result": [{"metric": metric, "values": [[start, value], [end, value]]}]})
    }
}

async fn answer(
    stub: Stub,
    path: &'static str,
    headers: HeaderMap,
    pairs: Vec<(String, String)>,
) -> Response {
    let authorization = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let min_commit_tokens = pairs
        .iter()
        .filter(|(k, _)| k == "min_commit_token")
        .map(|(_, v)| v.clone())
        .collect();
    let params: HashMap<String, String> = pairs.into_iter().collect();
    let query = params.get("query").cloned().unwrap_or_default();
    let earlier = {
        let mut requests = stub.requests.lock().unwrap();
        let earlier = requests
            .iter()
            .filter(|r| r.params.get("query") == Some(&query))
            .count();
        requests.push(Recorded {
            path,
            params: params.clone(),
            min_commit_tokens,
            authorization: authorization.clone(),
        });
        earlier
    };
    if let Some(token) = &stub.bearer
        && authorization.as_deref() != Some(format!("Bearer {token}").as_str())
    {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(
                json!({"status": "error", "errorType": "unauthorized", "error": "missing bearer"}),
            ),
        )
            .into_response();
    }
    let kind = if path == "/api/v1/query" {
        "instant"
    } else {
        "range"
    };
    let scripted = match stub.script.get(&query).copied().unwrap_or(Answer::Correct) {
        Answer::HangFirst(n) if earlier < n => Answer::Hang,
        Answer::HangFirst(_) => Answer::Correct,
        other => other,
    };
    match scripted {
        Answer::Correct => axum::Json(json!({"status": "success", "data": series("1.5", kind, &params)})).into_response(),
        Answer::Diverge => axum::Json(json!({"status": "success", "data": series("99", kind, &params)})).into_response(),
        Answer::TwoUlps => axum::Json(json!({"status": "success", "data": series("1.5000000000000004", kind, &params)})).into_response(),
        Answer::Empty => {
            let result_type = if kind == "instant" { "vector" } else { "matrix" };
            axum::Json(json!({"status": "success", "data": {"resultType": result_type, "result": []}})).into_response()
        }
        Answer::RefuseChunks => (
            StatusCode::UNPROCESSABLE_ENTITY,
            axum::Json(json!({
                "status": "error",
                "errorType": "execution",
                "error": "the query exceeded the maximum number of chunks (limit: 2000000 chunks) (err-mimir-max-chunks-per-query)"
            })),
        )
            .into_response(),
        Answer::Hang | Answer::HangFirst(_) => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            axum::Json(json!({"status": "success", "data": series("1.5", kind, &params)})).into_response()
        }
    }
}

async fn spawn_stub(script: &[(&str, Answer)], bearer: Option<&str>) -> (Stub, String) {
    let stub = Stub {
        script: Arc::new(script.iter().map(|(q, a)| (q.to_string(), *a)).collect()),
        bearer: bearer.map(str::to_string),
        requests: Arc::default(),
    };
    let app = Router::new()
        .route(
            "/api/v1/query",
            get(
                |State(s): State<Stub>, h: HeaderMap, Query(p): Query<Vec<(String, String)>>| {
                    answer(s, "/api/v1/query", h, p)
                },
            ),
        )
        .route(
            "/api/v1/query_range",
            get(
                |State(s): State<Stub>, h: HeaderMap, Query(p): Query<Vec<(String, String)>>| {
                    answer(s, "/api/v1/query_range", h, p)
                },
            ),
        )
        .with_state(stub.clone());
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await });
    (stub, format!("http://{addr}"))
}

/// An ingest phase whose accounting closes and whose accepted digests equal
/// the expected ones, so the row passes band 6.
fn ingest_phase() -> IngestPhase {
    let digests = ClassDigests {
        scalar: "d1".to_string(),
        stale_marker: "d2".to_string(),
        native_histogram: "d3".to_string(),
    };
    IngestPhase {
        stream_counts: StreamCounts {
            generated_lines: 10,
            scalar: 6,
            native_histogram: 2,
            stale_marker: 2,
            malformed: 0,
        },
        excluded_native_histogram: 0,
        excluded_stale_marker: 0,
        offered_samples: 8,
        offered_digest: "offered".to_string(),
        accepted_samples: 8,
        rejected_samples: 0,
        offered_histograms: 2,
        accepted_histograms: 2,
        rejected_histograms: 0,
        dropped_histograms: 0,
        histograms_rejected_by_status: BTreeMap::new(),
        retry_exhausted_histograms: 0,
        histograms_retry_exhausted_by_status: BTreeMap::new(),
        dropped_samples: 0,
        dropped_batches: 0,
        rejected_by_status: BTreeMap::new(),
        retry_exhausted_samples: 0,
        retry_exhausted_by_status: BTreeMap::new(),
        unsupported_samples: 0,
        unsupported_histograms: 0,
        expected_digests: digests.clone(),
        accepted_digests: digests,
        batches: 1,
        retries: 0,
        elapsed_secs: 1.0,
        elapsed_basis: "test fixture".to_string(),
        logical_points_per_sec: 10.0,
        wire_bytes: 100,
        wire_bytes_per_sec: 100.0,
        ack_latency_ms: LatencyReport {
            p50: 1.0,
            p95: 1.0,
            p99: 1.0,
            max: 1.0,
            count: 1,
        },
        peak_rss_bytes: None,
        cpu_secs: None,
    }
}

fn readback(query_url: &str) -> Readback {
    Readback {
        query_url: query_url.to_string(),
        series_count: 1,
        sample_count: 8,
        histogram_series_count: None,
        histogram_sample_count: None,
        stale_marker_honored: None,
        expected: ReadbackExpected {
            series_count: 1,
            sample_count: 8,
            histogram_series_count: None,
            histogram_sample_count: None,
            stale_marker_honored: None,
        },
        mismatches: Vec::new(),
        matches_expected: true,
    }
}

struct Urls {
    prometheus: String,
    ravel: String,
    victoriametrics: String,
    mimir: String,
}

/// The ingest report the run binds to. Prometheus records its query URL in
/// its readback; the other engines take theirs from `url=`.
fn ingest_report(urls: &Urls) -> MetricsIngestReport {
    let workload_bytes = std::fs::read(workload_path()).expect("workload");
    let mut prometheus = SystemIngestResult::buffered_http_row(
        "prometheus",
        format!("{}/api/v1/write", urls.prometheus),
        "test",
        ingest_phase(),
    );
    prometheus.readback = Some(readback(&urls.prometheus));
    let ravel = SystemIngestResult::durable_http_row(
        "ravel",
        format!("{}/api/v1/write", urls.ravel),
        "test",
        ingest_phase(),
        BTreeMap::from([
            (0, "0:17".to_string()),
            (1, "1:4".to_string()),
            (2, "2:9".to_string()),
        ]),
    );
    assert_eq!(ravel.last_commit_token.as_deref(), Some(RAVEL_COMMIT_TOKEN));
    let mut vm_phase = ingest_phase();
    vm_phase.accepted_histograms = 0;
    vm_phase.unsupported_histograms = 2;
    vm_phase.accepted_digests.native_histogram = String::new();
    let mut victoriametrics = SystemIngestResult::buffered_http_row(
        "victoriametrics",
        format!("{}/api/v1/write", urls.victoriametrics),
        "test",
        vm_phase,
    );
    victoriametrics.unsupported_classes = UNSUPPORTED_CLASSES
        .iter()
        .map(|u| UnsupportedClassRecord {
            class: u.class,
            source: u.source.to_string(),
        })
        .collect();
    assert_eq!(
        victoriametrics.unsupported_classes[0].class,
        PayloadClass::NativeHistogram
    );
    let mimir = SystemIngestResult::buffered_http_row(
        "mimir",
        format!("{}/api/v1/push", urls.mimir),
        "test",
        ingest_phase(),
    );
    let mut report = MetricsIngestReport::new(vec![prometheus, ravel, victoriametrics, mimir]);
    report.profile = Some(ProfileRecord {
        name: "ci".to_string(),
        comparable: true,
        comparability_reason: None,
        active_series: 1,
        steps_run: STEPS,
        steps_declared: STEPS,
        samples_per_series: STEPS,
        scrape_interval_secs: 15,
        duration_secs: STEPS * 15,
        total_samples: 10,
        label_cardinalities: BTreeMap::new(),
        scaling_label: ScalingLabelRecord {
            name: "instance".to_string(),
            cardinality_declared: 1,
        },
        churn_basis_points_per_hour: 0,
        families: Vec::new(),
        run: RunRecord {
            steps: STEPS,
            workload_version: Some(2),
            workload_digest: Some(workload_digest(&workload_bytes)),
            seed: Some(927_000_933),
            anchor_ms: Some(ANCHOR_MS),
            stream_digest: Some("stream-digest-fixture".to_string()),
            total_series_created: 1,
            logical_input_bytes: 100,
            total_samples_generated: 10,
            scaling_label_cardinality: 1,
        },
    });
    report
}

fn provenance(corpus_bytes: &[u8]) -> Provenance {
    let workload_bytes = std::fs::read(workload_path()).expect("workload");
    let comparator = |name: &str| Comparator {
        name: name.to_string(),
        version: "1.0.0".to_string(),
        image_digest: "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .to_string(),
    };
    Provenance {
        schema_version: 3,
        ravel_git_commit: "9fc85f421590d360e7979ee167eb38e166b45462".to_string(),
        toolchain: "rustc 1.90.0".to_string(),
        protocol: "remote_write_1.0".to_string(),
        hardware: Hardware {
            os: "Linux 6.8.0 x86_64".to_string(),
            cpu_model: "test".to_string(),
            logical_cores: 8,
            instance_type: None,
        },
        backend: Backend {
            store_backend: "memory".to_string(),
            region: "n/a".to_string(),
            endpoint: "n/a".to_string(),
            backend_bills_requests: false,
        },
        comparators: vec![
            comparator("prometheus"),
            comparator("victoriametrics"),
            comparator("mimir"),
        ],
        generator_digest: format!("blake3:{}", blake3::hash(&workload_bytes).to_hex()),
        corpus_digest: format!("blake3:{}", blake3::hash(corpus_bytes).to_hex()),
        config: Vec::new(),
        allocator: ravel_bench::allocator::Allocator::System,
    }
}

struct Run {
    output: Output,
    out: PathBuf,
    _dir: tempfile::TempDir,
}

const PASSES: u32 = 5;
const REPS: u32 = 2;
const WARMUP: u32 = 1;

/// Writes the fixtures and runs the bin to completion off the runtime, so
/// the stubs keep serving while it runs.
async fn run_bin(ingest: MetricsIngestReport, urls: &Urls) -> Run {
    run_bin_with(ingest, urls, WARMUP, &[]).await
}

/// [`run_bin`] with `warmup` warmup passes and `extra` arguments appended.
async fn run_bin_with(
    ingest: MetricsIngestReport,
    urls: &Urls,
    warmup: u32,
    extra: &[&str],
) -> Run {
    let dir = tempfile::tempdir().expect("tempdir");
    let corpus_bytes = serde_json::to_vec_pretty(&corpus()).expect("corpus");
    let corpus_path = dir.path().join("corpus.json");
    std::fs::write(&corpus_path, &corpus_bytes).expect("write corpus");
    let ingest_path = dir.path().join("ingest.json");
    std::fs::write(
        &ingest_path,
        serde_json::to_vec_pretty(&ingest).expect("ingest"),
    )
    .expect("write ingest");
    let provenance_path = dir.path().join("provenance.json");
    std::fs::write(
        &provenance_path,
        serde_json::to_vec_pretty(&provenance(&corpus_bytes)).expect("provenance"),
    )
    .expect("write provenance");
    let out = dir.path().join("report.json");
    let mut command = Command::new(env!("CARGO_BIN_EXE_metricsbench_query"));
    command
        .arg("--ingest-report")
        .arg(&ingest_path)
        .arg("--workload")
        .arg(workload_path())
        .arg("--corpus")
        .arg(&corpus_path)
        .arg("--provenance")
        .arg(&provenance_path)
        // The prometheus row's readback URL wins over this unreachable one.
        .args(["--engine", "name=prometheus,url=http://127.0.0.1:9"])
        .arg("--engine")
        .arg(format!(
            "name=ravel,url={},bearer_env={RAVEL_TOKEN_ENV}",
            urls.ravel
        ))
        .arg("--engine")
        .arg(format!("name=victoriametrics,url={}", urls.victoriametrics))
        .arg("--engine")
        .arg(format!("name=mimir,url={}", urls.mimir))
        .args(["--warmup-passes", &warmup.to_string()])
        .args(["--passes", &PASSES.to_string()])
        .args(["--reps-per-pass", &REPS.to_string()])
        .args(["--query-timeout-ms", "1000"])
        .args(extra)
        .arg("--out")
        .arg(&out)
        .env(RAVEL_TOKEN_ENV, RAVEL_TOKEN);
    let output =
        tokio::task::spawn_blocking(move || command.output().expect("run metricsbench_query"))
            .await
            .expect("join");
    Run {
        output,
        out,
        _dir: dir,
    }
}

struct Stubs {
    prometheus: Stub,
    ravel: Stub,
    victoriametrics: Stub,
    mimir: Stub,
    urls: Urls,
}

async fn stubs() -> Stubs {
    let (prometheus, p) = spawn_stub(&[], None).await;
    let (ravel, r) = spawn_stub(&[], Some(RAVEL_TOKEN)).await;
    let (victoriametrics, v) = spawn_stub(
        &[(NATIVE_P95, Answer::Empty), (GAUGE, Answer::Diverge)],
        None,
    )
    .await;
    let (mimir, m) = spawn_stub(
        &[
            (JOB_RATE_SUM, Answer::RefuseChunks),
            (COUNTER_RATE, Answer::Hang),
        ],
        None,
    )
    .await;
    Stubs {
        prometheus,
        ravel,
        victoriametrics,
        mimir,
        urls: Urls {
            prometheus: p,
            ravel: r,
            victoriametrics: v,
            mimir: m,
        },
    }
}

fn row<'a>(report: &'a MetricsBenchReport, engine: Engine, id: &str) -> &'a Measurement {
    report
        .measurements
        .iter()
        .find(|m| m.engine == Some(engine) && m.id == id)
        .unwrap_or_else(|| panic!("no row for {engine:?}/{id}"))
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_corpus_runs_against_every_engine_and_validates() {
    let s = stubs().await;
    let run = run_bin(ingest_report(&s.urls), &s.urls).await;
    assert!(
        run.output.status.success(),
        "metricsbench_query failed: {}",
        stderr(&run.output)
    );
    let text = std::fs::read_to_string(&run.out).expect("report written");
    let report: MetricsBenchReport = serde_json::from_str(&text).expect("report parses");

    let validated = Command::new(env!("CARGO_BIN_EXE_metricsbench_report"))
        .arg("validate")
        .arg(&run.out)
        .output()
        .expect("run metricsbench_report");
    assert!(
        validated.status.success(),
        "validate refused the output: {}",
        stderr(&validated)
    );

    // Every (engine, entry) case is expected, written from the corpus.
    assert_eq!(report.cases_expected.len(), 16);
    assert_eq!(config(&report, "oracle_retries_used"), Some("0"));
    assert_eq!(config(&report, "oracle_retry_entries"), Some("none"));

    use ResultStatus::*;
    let expected: &[(Engine, &str, ResultStatus)] = &[
        (Engine::Prometheus, "mb_single_gauge_instant", Ok),
        (Engine::Prometheus, "mb_single_counter_rate", Ok),
        (Engine::Prometheus, "mb_selective_job_rate_sum", Ok),
        (Engine::Prometheus, "mb_histogram_native_p95", Ok),
        (Engine::Ravel, "mb_single_gauge_instant", Ok),
        (Engine::Ravel, "mb_single_counter_rate", Ok),
        (Engine::Ravel, "mb_selective_job_rate_sum", Ok),
        (Engine::Ravel, "mb_histogram_native_p95", Ok),
        (
            Engine::VictoriaMetrics,
            "mb_single_gauge_instant",
            Incorrect,
        ),
        (Engine::VictoriaMetrics, "mb_single_counter_rate", Ok),
        (Engine::VictoriaMetrics, "mb_selective_job_rate_sum", Ok),
        (
            Engine::VictoriaMetrics,
            "mb_histogram_native_p95",
            UnsupportedConstruct,
        ),
        (Engine::Mimir, "mb_single_gauge_instant", Ok),
        (Engine::Mimir, "mb_single_counter_rate", Timeout),
        (Engine::Mimir, "mb_selective_job_rate_sum", Refused),
        (Engine::Mimir, "mb_histogram_native_p95", Ok),
    ];
    assert_eq!(report.measurements.len(), expected.len());
    for (engine, id, status) in expected {
        let m = row(&report, *engine, id);
        assert_eq!(m.status, *status, "{engine:?}/{id}");
        assert_eq!(
            m.pass_statuses,
            vec![*status; PASSES as usize],
            "{engine:?}/{id}"
        );
        assert_eq!(m.oracle, *engine == Engine::Prometheus, "{engine:?}/{id}");
    }
    assert_eq!(
        row(&report, Engine::Mimir, "mb_selective_job_rate_sum").refusal_reason,
        Some(RefusalReason::Budget)
    );
    assert_eq!(
        row(&report, Engine::Ravel, "mb_single_counter_rate").query_kind,
        Some(QueryKind::Range)
    );

    let denominators: Vec<(Engine, usize, usize, usize, usize)> = report
        .denominators
        .iter()
        .map(|d| {
            (
                d.engine,
                d.corpus,
                d.correctness,
                d.timing,
                d.coverage_success,
            )
        })
        .collect();
    assert_eq!(
        denominators,
        vec![
            (Engine::Prometheus, 4, 4, 4, 4),
            (Engine::Ravel, 4, 4, 4, 4),
            (Engine::VictoriaMetrics, 4, 3, 2, 2),
            (Engine::Mimir, 4, 2, 2, 2),
        ]
    );

    // The bearer header reached Ravel on every query, and the token appears
    // nowhere in the report or the process output.
    let ravel_requests = s.ravel.requests();
    assert!(!ravel_requests.is_empty());
    for r in &ravel_requests {
        assert_eq!(
            r.authorization.as_deref(),
            Some(format!("Bearer {RAVEL_TOKEN}").as_str())
        );
    }
    assert!(
        !text.contains(RAVEL_TOKEN),
        "the bearer token leaked into the report"
    );
    assert!(!stderr(&run.output).contains(RAVEL_TOKEN));
    assert!(!String::from_utf8_lossy(&run.output.stdout).contains(RAVEL_TOKEN));
    assert!(
        s.prometheus
            .requests()
            .iter()
            .all(|r| r.authorization.is_none())
    );
}

/// Only an `ok` row is timed. Fails against a runner that times every
/// answered query: the incorrect VictoriaMetrics row and the refused Mimir
/// row were answered, and carry no latency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_ok_rows_carry_latency() {
    let s = stubs().await;
    let run = run_bin(ingest_report(&s.urls), &s.urls).await;
    assert!(run.output.status.success(), "{}", stderr(&run.output));
    let report: MetricsBenchReport =
        serde_json::from_str(&std::fs::read_to_string(&run.out).expect("report")).expect("parse");
    for m in &report.measurements {
        let names: Vec<&str> = m.figures.iter().map(|f| f.name.as_str()).collect();
        if m.status == ResultStatus::Ok {
            assert_eq!(names, ["min_ms", "median_ms", "max_ms"], "{}", m.id);
            assert_eq!(m.pass_medians_ms.len(), PASSES as usize, "{}", m.id);
            assert_eq!(m.reps_per_pass, Some(REPS), "{}", m.id);
            assert!(m.rel_mad_pct.is_some(), "{}", m.id);
        } else {
            assert!(
                names.is_empty(),
                "{:?}/{} carries {names:?}",
                m.engine,
                m.id
            );
            assert!(m.pass_medians_ms.is_empty(), "{:?}/{}", m.engine, m.id);
            assert_eq!(m.reps_per_pass, None, "{:?}/{}", m.engine, m.id);
            assert_eq!(m.rel_mad_pct, None, "{:?}/{}", m.engine, m.id);
        }
    }
    assert!(s.victoriametrics.count("/api/v1/query", GAUGE) > 0);
}

/// A range entry is one range query per repetition over the whole grid, and
/// never an instant query per grid point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_range_entry_is_one_range_request_per_repetition() {
    let s = stubs().await;
    let run = run_bin(ingest_report(&s.urls), &s.urls).await;
    assert!(run.output.status.success(), "{}", stderr(&run.output));
    let per_engine = ((WARMUP + PASSES) * REPS) as usize;
    // newest = anchor + 119 * 15 s; the ci profile's range_eval is
    // {start_offset 1485, end_offset 0, step 15}.
    let newest_s = (ANCHOR_MS + 119 * 15_000) / 1000;
    for stub in [&s.prometheus, &s.ravel, &s.victoriametrics, &s.mimir] {
        for promql in [COUNTER_RATE, JOB_RATE_SUM] {
            assert_eq!(stub.count("/api/v1/query_range", promql), per_engine);
            assert_eq!(stub.count("/api/v1/query", promql), 0);
        }
        for promql in [GAUGE, NATIVE_P95] {
            assert_eq!(stub.count("/api/v1/query", promql), per_engine);
            assert_eq!(stub.count("/api/v1/query_range", promql), 0);
        }
        for r in stub.requests() {
            if r.path == "/api/v1/query_range" {
                assert_eq!(
                    r.params["start"].parse::<f64>().unwrap(),
                    (newest_s - 1485) as f64
                );
                assert_eq!(r.params["end"].parse::<f64>().unwrap(), newest_s as f64);
                assert_eq!(r.params["step"], "15000ms");
            } else {
                assert_eq!(r.params["time"].parse::<f64>().unwrap(), newest_s as f64);
            }
        }
    }
}

/// Ravel receives every shard's token from its row's `last_commit_token` on
/// every query, one `min_commit_token` parameter per token: the query API
/// decodes each parameter as a single token, so the comma-joined list sent
/// as one value would be refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ravel_receives_the_full_multi_shard_commit_token() {
    let s = stubs().await;
    let run = run_bin(ingest_report(&s.urls), &s.urls).await;
    assert!(run.output.status.success(), "{}", stderr(&run.output));
    let requests = s.ravel.requests();
    assert!(!requests.is_empty());
    let expected: Vec<String> = RAVEL_COMMIT_TOKEN.split(',').map(str::to_string).collect();
    assert_eq!(expected.len(), 3);
    for r in &requests {
        assert_eq!(r.min_commit_tokens, expected);
    }
    for other in [&s.prometheus, &s.victoriametrics, &s.mimir] {
        assert!(
            other
                .requests()
                .iter()
                .all(|r| !r.params.contains_key("min_commit_token"))
        );
    }
}

/// An entry reading the native-histogram family is `unsupported_construct`
/// on VictoriaMetrics, whose ingest row lists the class, before any data
/// comparison: its empty answer would otherwise classify `incorrect`. The
/// query is still sent, and the entry stays in the corpus denominator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsupported_class_entry_is_unsupported_not_incorrect() {
    let s = stubs().await;
    let run = run_bin(ingest_report(&s.urls), &s.urls).await;
    assert!(run.output.status.success(), "{}", stderr(&run.output));
    let report: MetricsBenchReport =
        serde_json::from_str(&std::fs::read_to_string(&run.out).expect("report")).expect("parse");
    let m = row(&report, Engine::VictoriaMetrics, "mb_histogram_native_p95");
    assert_eq!(m.status, ResultStatus::UnsupportedConstruct);
    assert!(s.victoriametrics.count("/api/v1/query", NATIVE_P95) > 0);
    let vm = report
        .denominators
        .iter()
        .find(|d| d.engine == Engine::VictoriaMetrics)
        .expect("vm denominators");
    assert_eq!(vm.corpus, 4);
    // Mimir lists no unsupported class and answers the same entry correctly.
    assert_eq!(
        row(&report, Engine::Mimir, "mb_histogram_native_p95").status,
        ResultStatus::Ok
    );
    let err = stderr(&run.output);
    assert!(
        err.contains("victoriametrics/mb_histogram_native_p95"),
        "{err}"
    );
    assert!(err.contains("lib/prompb/prompb.go"), "{err}");
    assert!(err.contains("0 series"), "{err}");
}

/// A Ravel row whose tokens are all empty refuses to bind rather than
/// querying Ravel with no read-your-write fence: exit 65, nothing written,
/// no query sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ravel_row_with_no_commit_token_refuses_the_run() {
    for joined in [",", ""] {
        let s = stubs().await;
        let mut ingest = ingest_report(&s.urls);
        let ravel = ingest
            .systems
            .iter_mut()
            .find(|r| r.system == "ravel")
            .expect("ravel row");
        ravel.commit_tokens_by_shard = None;
        ravel.last_commit_token = Some(joined.to_string());
        let run = run_bin(ingest, &s.urls).await;
        assert_eq!(
            run.output.status.code(),
            Some(65),
            "{joined:?}: {}",
            stderr(&run.output)
        );
        assert!(
            stderr(&run.output).contains("no commit token"),
            "{}",
            stderr(&run.output)
        );
        assert!(!run.out.exists());
        for stub in [&s.prometheus, &s.ravel, &s.victoriametrics, &s.mimir] {
            assert!(stub.requests().is_empty());
        }
    }
}

/// The per-shard map is the token source when the row carries it: a
/// `last_commit_token` that disagrees with it is not what Ravel receives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ravel_tokens_come_from_the_per_shard_map() {
    let s = stubs().await;
    let mut ingest = ingest_report(&s.urls);
    let ravel = ingest
        .systems
        .iter_mut()
        .find(|r| r.system == "ravel")
        .expect("ravel row");
    ravel.last_commit_token = Some(",".to_string());
    let run = run_bin(ingest, &s.urls).await;
    assert!(run.output.status.success(), "{}", stderr(&run.output));
    let expected: Vec<String> = RAVEL_COMMIT_TOKEN.split(',').map(str::to_string).collect();
    let requests = s.ravel.requests();
    assert!(!requests.is_empty());
    for r in &requests {
        assert_eq!(r.min_commit_tokens, expected);
    }
}

/// A portable ingest row failing band 6 voids the binding: exit 65, the
/// reason named, nothing written, no query sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_band6_failure_refuses_the_run() {
    let s = stubs().await;
    let mut ingest = ingest_report(&s.urls);
    let mimir = ingest
        .systems
        .iter_mut()
        .find(|r| r.system == "mimir")
        .expect("mimir row");
    mimir.ingest.accepted_digests.scalar = "not-the-offered-stream".to_string();
    let run = run_bin(ingest, &s.urls).await;
    assert_eq!(
        run.output.status.code(),
        Some(65),
        "{}",
        stderr(&run.output)
    );
    assert!(
        stderr(&run.output).contains("`mimir` fails band 6"),
        "{}",
        stderr(&run.output)
    );
    assert!(!run.out.exists());
    for stub in [&s.prometheus, &s.ravel, &s.victoriametrics, &s.mimir] {
        assert!(stub.requests().is_empty());
    }
}

/// A workload file other than the one the ingest run replayed refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workload_digest_mismatch_refuses_the_run() {
    let s = stubs().await;
    let mut ingest = ingest_report(&s.urls);
    ingest
        .profile
        .as_mut()
        .expect("profile")
        .run
        .workload_digest = Some("0".repeat(64));
    let run = run_bin(ingest, &s.urls).await;
    assert_eq!(
        run.output.status.code(),
        Some(65),
        "{}",
        stderr(&run.output)
    );
    assert!(
        stderr(&run.output).contains("run.workload_digest"),
        "{}",
        stderr(&run.output)
    );
    assert!(!run.out.exists());
}

/// An oracle that answers empty on an entry not marked intended-empty is a
/// fixture error and voids the run, naming the entry and the engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_classify_error_voids_the_run() {
    let mut s = stubs().await;
    let (prometheus, url) = spawn_stub(&[(GAUGE, Answer::Empty)], None).await;
    s.prometheus = prometheus;
    s.urls.prometheus = url;
    let run = run_bin(ingest_report(&s.urls), &s.urls).await;
    assert_eq!(
        run.output.status.code(),
        Some(65),
        "{}",
        stderr(&run.output)
    );
    let err = stderr(&run.output);
    assert!(err.contains("mb_single_gauge_instant"), "{err}");
    assert!(err.contains("the run is void"), "{err}");
    assert!(!run.out.exists());
}

/// The runner hands each entry's `tolerance_ulps` to the comparator. The
/// checked-in `mb_full_range_avg_over_time` declares one, so Ravel answering
/// two ULPs off the oracle classifies `ok`; the same entry with its tolerance
/// removed classifies `incorrect`, because absent means bit-exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_tolerance_reaches_the_comparator() {
    use ravel_bench::metrics_query::{RunOptions, bind, parse_engine_spec, run};

    let corpus = ravel_bench::promql_corpus::load_corpus(
        repo_root().join("benchmarks/metrics/metrics.corpus.json"),
    )
    .expect("checked-in corpus");
    let tolerant = corpus
        .into_iter()
        .find(|e| e.id == "mb_full_range_avg_over_time")
        .expect("the full-range entry");
    assert_eq!(tolerant.tolerance_ulps, Some(1_048_576));
    let mut exact = tolerant.clone();
    exact.id = "mb_full_range_avg_over_time_exact".to_string();
    exact.tolerance_ulps = None;

    let (_prometheus, p) = spawn_stub(&[], None).await;
    let (_ravel, r) = spawn_stub(&[(&tolerant.promql, Answer::TwoUlps)], Some(RAVEL_TOKEN)).await;
    let urls = Urls {
        prometheus: p,
        ravel: r.clone(),
        victoriametrics: "http://127.0.0.1:9".to_string(),
        mimir: "http://127.0.0.1:9".to_string(),
    };
    let workload_bytes = std::fs::read(workload_path()).expect("workload");
    let workload = ravel_bench::metrics_workload::load_workload(workload_path()).expect("workload");
    let specs = [
        parse_engine_spec("name=prometheus,url=http://127.0.0.1:9").expect("spec"),
        parse_engine_spec(&format!("name=ravel,url={r},bearer_env={RAVEL_TOKEN_ENV}"))
            .expect("spec"),
    ];
    let binding = bind(
        &ingest_report(&urls),
        &workload_bytes,
        &workload,
        &specs,
        |_| Some(RAVEL_TOKEN.to_string()),
    )
    .expect("bind");
    let output = run(
        &binding,
        &[tolerant, exact],
        &workload,
        RunOptions {
            warmup_passes: 0,
            passes: 1,
            reps_per_pass: 1,
            deadline: Duration::from_secs(5),
            oracle_retries: 0,
        },
    )
    .await
    .expect("run");
    let status = |id: &str| {
        output
            .measurements
            .iter()
            .find(|m| m.engine == Some(Engine::Ravel) && m.id == id)
            .unwrap_or_else(|| panic!("no ravel row for {id}"))
            .status
    };
    assert_eq!(status("mb_full_range_avg_over_time"), ResultStatus::Ok);
    assert_eq!(
        status("mb_full_range_avg_over_time_exact"),
        ResultStatus::Incorrect
    );
}

fn config<'a>(report: &'a MetricsBenchReport, key: &str) -> Option<&'a str> {
    report
        .provenance
        .config
        .iter()
        .find(|c| c.key == key)
        .map(|c| c.value.as_str())
}

/// An oracle query that times out once is re-sent, and the run completes:
/// the other engines are classified against the retried answer, the
/// provenance config records one retry on the entry, and Prometheus' own row
/// keeps the first attempt's timeout. Fails against a runner that does not
/// retry (the run is void) and against one that records the retried
/// attempt on Prometheus' row (every pass would be `ok`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oracle_timeout_is_retried_and_prometheus_keeps_its_first_attempt() {
    let mut s = stubs().await;
    let (prometheus, url) = spawn_stub(&[(GAUGE, Answer::HangFirst(1))], None).await;
    s.prometheus = prometheus;
    s.urls.prometheus = url;
    let run = run_bin_with(ingest_report(&s.urls), &s.urls, 0, &[]).await;
    let err = stderr(&run.output);
    assert!(run.output.status.success(), "{err}");
    let report: MetricsBenchReport =
        serde_json::from_str(&std::fs::read_to_string(&run.out).expect("report")).expect("parse");

    assert_eq!(config(&report, "oracle_retries"), Some("2"));
    assert_eq!(config(&report, "oracle_retries_used"), Some("1"));
    assert_eq!(
        config(&report, "oracle_retry_entries"),
        Some("mb_single_gauge_instant")
    );
    assert!(
        err.contains(
            "oracle retry: entry `mb_single_gauge_instant`, pass 1, repetition 1, attempt 2 of 3"
        ),
        "{err}"
    );
    // One request per repetition, plus the one retry.
    assert_eq!(
        s.prometheus.count("/api/v1/query", GAUGE),
        (PASSES * REPS) as usize + 1
    );

    let p = row(&report, Engine::Prometheus, "mb_single_gauge_instant");
    assert_eq!(p.pass_statuses[0], ResultStatus::Timeout);
    assert!(
        p.pass_statuses[1..].iter().all(|s| *s == ResultStatus::Ok),
        "{:?}",
        p.pass_statuses
    );
    assert_ne!(p.status, ResultStatus::Ok);
    for (engine, status) in [
        (Engine::Ravel, ResultStatus::Ok),
        (Engine::VictoriaMetrics, ResultStatus::Incorrect),
        (Engine::Mimir, ResultStatus::Ok),
    ] {
        let m = row(&report, engine, "mb_single_gauge_instant");
        assert_eq!(m.pass_statuses, vec![status; PASSES as usize], "{engine:?}");
    }

    let validated = Command::new(env!("CARGO_BIN_EXE_metricsbench_report"))
        .arg("validate")
        .arg(&run.out)
        .output()
        .expect("run metricsbench_report");
    assert!(validated.status.success(), "{}", stderr(&validated));
}

/// An oracle that times out on every attempt the budget allows voids the
/// run as an unretried failure does: exit 65, nothing written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oracle_timeout_past_the_retry_budget_voids_the_run() {
    let mut s = stubs().await;
    let (prometheus, url) = spawn_stub(&[(GAUGE, Answer::HangFirst(2))], None).await;
    s.prometheus = prometheus;
    s.urls.prometheus = url;
    let run = run_bin_with(
        ingest_report(&s.urls),
        &s.urls,
        0,
        &["--oracle-retries", "1"],
    )
    .await;
    let err = stderr(&run.output);
    assert_eq!(run.output.status.code(), Some(65), "{err}");
    assert!(err.contains("the run is void"), "{err}");
    assert!(err.contains("mb_single_gauge_instant"), "{err}");
    assert!(err.contains("oracle timed out"), "{err}");
    assert!(!run.out.exists());
    assert_eq!(s.prometheus.count("/api/v1/query", GAUGE), 2);
}

/// A 4xx from the oracle is an answer, not a lost request: it is not
/// retried and voids the run. Fails against a runner that retries every
/// non-`ok` oracle outcome (three requests would reach the stub).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oracle_4xx_is_not_retried() {
    let mut s = stubs().await;
    let (prometheus, url) = spawn_stub(&[(GAUGE, Answer::RefuseChunks)], None).await;
    s.prometheus = prometheus;
    s.urls.prometheus = url;
    let run = run_bin_with(ingest_report(&s.urls), &s.urls, 0, &[]).await;
    let err = stderr(&run.output);
    assert_eq!(run.output.status.code(), Some(65), "{err}");
    assert!(err.contains("oracle answered HTTP 422"), "{err}");
    assert!(!err.contains("oracle retry"), "{err}");
    assert!(!run.out.exists());
    assert_eq!(s.prometheus.count("/api/v1/query", GAUGE), 1);
}
