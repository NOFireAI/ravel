//! The MetricsBench portable query runner (ADR-0927, epic #2821 W2b, issue
//! #938): the PromQL corpus run over HTTP against every engine an ingest run
//! loaded, classified against Prometheus as the oracle (decision 4) and
//! written as a version-3 report ([`crate::report_schema`]).
//!
//! The run is bound to one ingest run. [`bind`] reads the anchor, the step
//! count, the workload version and digest, the seed and the stream digest from
//! the ingest report's `profile.run` record, refuses a workload file whose
//! digest differs, refuses an ingest report any portable row of which fails
//! [`check_row_band6`] or [`check_row_accounting`], and takes each engine's
//! query URL from its ingest row's `readback.query_url` when the row records
//! one. Every query time derives from the recorded anchor
//! ([`QueryGrid::from_run`]), not from the clock.
//!
//! [`run`] then issues `warmup_passes` untimed passes and `passes` timed
//! passes. In each pass, per corpus entry, the oracle is queried first and
//! its first answer of the pass is the answer every other engine's
//! repetitions are classified against with [`classify`]. A
//! [`ClassifyError`] voids the run ([`RunError::Classify`]), except that an
//! oracle answer lost to a timeout or a transport failure is first re-sent up
//! to [`RunOptions::oracle_retries`] times ([`oracle_answer`]). The retried
//! requests answer the oracle only: Prometheus' own row keeps the outcome of
//! the first attempt. A range entry is
//! one `/api/v1/query_range` request per repetition over the profile's
//! `range_eval` grid (amendment E).
//!
//! An entry that reads a native-histogram family, run on an engine whose
//! ingest row lists `native_histogram` among its `unsupported_classes`, is
//! `unsupported_construct` on that engine before any comparison. Its query
//! is still sent, and its
//! answer is summarised on stderr beside the row's citation, because the
//! report row has no message field.

use std::collections::BTreeMap;
use std::time::Duration;

use ravel_promql_difftest::comparator::ulp_distance;
use ravel_promql_difftest::corpus::ComparisonMode;
use ravel_promql_difftest::prometheus_client::{
    PrometheusClient, QueryOutcome, TransportErrorKind,
};
use serde_json::Value as Json;

use crate::metrics_correctness::{
    Classified, ClassifyError, Denominators, Engine, EntryExpectation, OracleAnswer, classify,
};
use crate::metrics_ingest::{
    IN_PROCESS_SYSTEM, MetricsIngestReport, PayloadClass, check_row_accounting, check_row_band6,
    workload_digest,
};
use crate::metrics_workload::{FamilyKind, Profile, WorkloadFile};
use crate::promql_corpus::{CorpusEntry, EvalKind};
use crate::report_schema::{
    EngineDenominators, ExpectedCase, Figure, MIN_VARIANCE_PASSES, Measurement, ResultStatus,
    rel_mad_pct, row_status,
};

/// The query parameter Ravel's HTTP API reads commit tokens from.
pub const MIN_COMMIT_TOKEN_PARAM: &str = "min_commit_token";

/// The ingest `system` value that names `engine`.
pub fn system_name(engine: Engine) -> &'static str {
    match engine {
        Engine::Ravel => "ravel",
        Engine::Prometheus => "prometheus",
        Engine::VictoriaMetrics => "victoriametrics",
        Engine::Mimir => "mimir",
    }
}

/// The engine an ingest `system` value names. `ravel-in-process` names none:
/// it has no HTTP endpoint (amendment A).
pub fn engine_of_system(system: &str) -> Option<Engine> {
    [
        Engine::Ravel,
        Engine::Prometheus,
        Engine::VictoriaMetrics,
        Engine::Mimir,
    ]
    .into_iter()
    .find(|e| system_name(*e) == system)
}

/// One `--engine name=<system>[,url=<base>][,bearer_env=VAR]` argument.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineSpec {
    pub engine: Engine,
    /// The query base URL, used only when the ingest row records none.
    pub url: Option<String>,
    /// The environment variable holding the bearer token. The token itself
    /// is never an argument.
    pub bearer_env: Option<String>,
}

/// Parses an `--engine` value.
pub fn parse_engine_spec(spec: &str) -> Result<EngineSpec, String> {
    let mut name = None;
    let mut url = None;
    let mut bearer_env = None;
    for part in spec.split(',') {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| format!("engine spec `{spec}`: `{part}` is not key=value"))?;
        if value.trim().is_empty() {
            return Err(format!("engine spec `{spec}`: `{key}` is empty"));
        }
        let slot = match key {
            "name" => &mut name,
            "url" => &mut url,
            "bearer_env" => &mut bearer_env,
            other => {
                return Err(format!(
                    "engine spec `{spec}`: unknown key `{other}` (name, url, bearer_env)"
                ));
            }
        };
        if slot.replace(value.to_string()).is_some() {
            return Err(format!("engine spec `{spec}`: `{key}` given twice"));
        }
    }
    let name = name.ok_or_else(|| format!("engine spec `{spec}` has no name="))?;
    let engine = engine_of_system(&name).ok_or_else(|| {
        format!("engine `{name}` is not one of prometheus, ravel, victoriametrics, mimir")
    })?;
    Ok(EngineSpec {
        engine,
        url,
        bearer_env,
    })
}

/// Why an ingest report, workload or engine list cannot bind a query run.
/// Every variant is a data error (exit 65 in the bin).
#[derive(Debug, thiserror::Error)]
pub enum BindError {
    #[error("ingest report has no `profile.run.{0}`; it predates the field and cannot bind a run")]
    MissingRunField(&'static str),
    #[error(
        "workload digest {file} does not match the ingest report's `run.workload_digest` {report}"
    )]
    WorkloadDigest { file: String, report: String },
    #[error(
        "workload version {file} does not match the ingest report's `run.workload_version` {report}"
    )]
    WorkloadVersion { file: u32, report: u32 },
    #[error("workload seed {file} does not match the ingest report's `run.seed` {report}")]
    Seed { file: u64, report: u64 },
    #[error("the workload declares no profile `{0}`, which the ingest report ran")]
    UnknownProfile(String),
    #[error("ingest row `{system}` fails band 6: {detail}")]
    Band6 { system: String, detail: String },
    #[error("ingest row `{system}` fails its accounting: {detail}")]
    Accounting { system: String, detail: String },
    #[error(
        "ingest row `{system}` failed its read-back ({detail}); the report is kept for \
         diagnosis and is not a run result"
    )]
    Readback { system: String, detail: String },
    #[error(
        "Prometheus is the oracle and must be one of the engines (--engine name=prometheus,...)"
    )]
    NoOracle,
    #[error("engine `{0}` is given twice")]
    DuplicateEngine(&'static str),
    #[error("the ingest report has no row for `{0}`; an engine that was not loaded is not queried")]
    NoIngestRow(&'static str),
    #[error("engine `{0}` has no query URL: its ingest row records none and no url= was given")]
    NoUrl(&'static str),
    #[error(
        "engine `{system}`: environment variable `{var}` named by bearer_env is unset or empty"
    )]
    BearerUnset { system: &'static str, var: String },
    #[error(
        "the Ravel ingest row has no commit token (`commit_tokens_by_shard` or \
         `last_commit_token`) to pass as min_commit_token"
    )]
    NoCommitToken,
    #[error(
        "the profile's range grid starts {start_offset_secs}s before the newest sample, but the \
         ingest run's {steps} steps span only {span_secs}s"
    )]
    GridOutsideRun {
        start_offset_secs: u64,
        steps: u64,
        span_secs: u64,
    },
    #[error("{0}")]
    Grid(String),
}

/// The run parameters an ingest report fixes, verbatim from `profile.run`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestRun {
    pub profile: String,
    pub steps: u64,
    pub anchor_ms: i64,
    pub workload_version: u32,
    pub workload_digest: String,
    pub seed: u64,
    pub stream_digest: String,
}

/// The query times every entry runs at, derived from the anchor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryGrid {
    /// The newest generated sample: `anchor + (steps - 1) * scrape interval`.
    /// Instant entries evaluate here.
    pub newest_ms: i64,
    pub range_start_ms: i64,
    pub range_end_ms: i64,
    pub range_step_ms: i64,
}

impl QueryGrid {
    /// The grid for `profile` over `run`. The range grid is the profile's
    /// `range_eval`, as offsets back from the newest sample, and it must fit
    /// inside the steps the ingest run actually wrote.
    pub fn from_run(run: &IngestRun, profile: &Profile) -> Result<Self, BindError> {
        let interval_ms = secs_to_ms(profile.scrape_interval_secs)?;
        let span_secs = run
            .steps
            .saturating_sub(1)
            .saturating_mul(profile.scrape_interval_secs);
        let eval = &profile.range_eval;
        if eval.start_offset_secs > span_secs {
            return Err(BindError::GridOutsideRun {
                start_offset_secs: eval.start_offset_secs,
                steps: run.steps,
                span_secs,
            });
        }
        let steps_back = i64::try_from(run.steps.saturating_sub(1))
            .map_err(|_| BindError::Grid(format!("{} steps overflow", run.steps)))?;
        let newest_ms = steps_back
            .checked_mul(interval_ms)
            .and_then(|d| run.anchor_ms.checked_add(d))
            .ok_or_else(|| BindError::Grid("the newest sample time overflows".to_string()))?;
        Ok(QueryGrid {
            newest_ms,
            range_start_ms: newest_ms - secs_to_ms(eval.start_offset_secs)?,
            range_end_ms: newest_ms - secs_to_ms(eval.end_offset_secs)?,
            range_step_ms: secs_to_ms(eval.step_secs)?,
        })
    }
}

fn secs_to_ms(secs: u64) -> Result<i64, BindError> {
    i64::try_from(secs)
        .ok()
        .and_then(|s| s.checked_mul(1000))
        .ok_or_else(|| BindError::Grid(format!("{secs}s overflows milliseconds")))
}

/// One engine as the run queries it. `Debug` leaves the bearer token out.
pub struct BoundEngine {
    pub engine: Engine,
    pub query_url: String,
    bearer_token: Option<String>,
    /// One `min_commit_token` per token in Ravel's `last_commit_token`, the
    /// comma-joined per-shard list, since the query API decodes each
    /// parameter as a single token; empty for every other engine.
    pub extra_params: Vec<(String, String)>,
    /// The classes the engine's ingest row cites as unsupported, with the
    /// citation.
    pub unsupported: Vec<(PayloadClass, String)>,
}

impl std::fmt::Debug for BoundEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundEngine")
            .field("engine", &self.engine)
            .field("query_url", &self.query_url)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("extra_params", &self.extra_params)
            .field("unsupported", &self.unsupported)
            .finish()
    }
}

impl BoundEngine {
    /// The client this engine is queried through, under `deadline`.
    pub fn client(&self, deadline: Duration) -> PrometheusClient {
        let mut client = PrometheusClient::with_deadline(self.query_url.clone(), deadline)
            .with_extra_params(self.extra_params.clone());
        if let Some(token) = &self.bearer_token {
            client = client.with_bearer_token(token.clone());
        }
        client
    }

    /// The citation for `class`, when the ingest row lists it unsupported.
    pub fn unsupported_citation(&self, class: PayloadClass) -> Option<&str> {
        self.unsupported
            .iter()
            .find(|(c, _)| *c == class)
            .map(|(_, source)| source.as_str())
    }
}

/// A query run bound to one ingest run.
#[derive(Debug)]
pub struct Binding {
    pub run: IngestRun,
    pub grid: QueryGrid,
    /// The oracle first, then the others in command-line order.
    pub engines: Vec<BoundEngine>,
}

/// Binds a query run to `ingest`. `workload_bytes` are the manifest file's
/// bytes, digested as the ingest lane digests them; `env` resolves a
/// `bearer_env` name.
pub fn bind(
    ingest: &MetricsIngestReport,
    workload_bytes: &[u8],
    workload: &WorkloadFile,
    specs: &[EngineSpec],
    env: impl Fn(&str) -> Option<String>,
) -> Result<Binding, BindError> {
    let profile_record = ingest
        .profile
        .as_ref()
        .ok_or(BindError::MissingRunField("profile"))?;
    let r = &profile_record.run;
    let run = IngestRun {
        profile: profile_record.name.clone(),
        steps: r.steps,
        anchor_ms: r.anchor_ms.ok_or(BindError::MissingRunField("anchor_ms"))?,
        workload_version: r
            .workload_version
            .ok_or(BindError::MissingRunField("workload_version"))?,
        workload_digest: r
            .workload_digest
            .clone()
            .ok_or(BindError::MissingRunField("workload_digest"))?,
        seed: r.seed.ok_or(BindError::MissingRunField("seed"))?,
        stream_digest: r
            .stream_digest
            .clone()
            .ok_or(BindError::MissingRunField("stream_digest"))?,
    };

    let file_digest = workload_digest(workload_bytes);
    if file_digest != run.workload_digest {
        return Err(BindError::WorkloadDigest {
            file: file_digest,
            report: run.workload_digest,
        });
    }
    if workload.version != run.workload_version {
        return Err(BindError::WorkloadVersion {
            file: workload.version,
            report: run.workload_version,
        });
    }
    if workload.seed != run.seed {
        return Err(BindError::Seed {
            file: workload.seed,
            report: run.seed,
        });
    }
    let profile = workload
        .profile(&run.profile)
        .ok_or_else(|| BindError::UnknownProfile(run.profile.clone()))?;

    for row in ingest
        .systems
        .iter()
        .filter(|r| r.system != IN_PROCESS_SYSTEM)
    {
        check_row_band6(row).map_err(|e| BindError::Band6 {
            system: row.system.clone(),
            detail: e.to_string(),
        })?;
        check_row_accounting(&row.ingest, &row.ingest.stream_counts).map_err(|e| {
            BindError::Accounting {
                system: row.system.clone(),
                detail: e.to_string(),
            }
        })?;
        let failed_readback = match (&row.readback_error, &row.readback) {
            (Some(error), _) => Some(error.clone()),
            (None, Some(readback)) if !readback.matches_expected => {
                Some(format!("mismatches: {}", readback.mismatches.join("; ")))
            }
            _ => None,
        };
        if let Some(detail) = failed_readback {
            return Err(BindError::Readback {
                system: row.system.clone(),
                detail,
            });
        }
    }

    let grid = QueryGrid::from_run(&run, profile)?;

    if !specs.iter().any(|s| s.engine == Engine::Prometheus) {
        return Err(BindError::NoOracle);
    }
    let mut ordered: Vec<&EngineSpec> = Vec::with_capacity(specs.len());
    ordered.extend(specs.iter().filter(|s| s.engine == Engine::Prometheus));
    ordered.extend(specs.iter().filter(|s| s.engine != Engine::Prometheus));
    let mut engines: Vec<BoundEngine> = Vec::with_capacity(specs.len());
    for spec in ordered {
        let system = system_name(spec.engine);
        if engines.iter().any(|e| e.engine == spec.engine) {
            return Err(BindError::DuplicateEngine(system));
        }
        let row = ingest.row(system).ok_or(BindError::NoIngestRow(system))?;
        let query_url = row
            .readback
            .as_ref()
            .map(|rb| rb.query_url.clone())
            .filter(|u| !u.trim().is_empty())
            .or_else(|| spec.url.clone())
            .ok_or(BindError::NoUrl(system))?;
        let bearer_token =
            match &spec.bearer_env {
                None => None,
                Some(var) => Some(env(var).filter(|t| !t.is_empty()).ok_or_else(|| {
                    BindError::BearerUnset {
                        system,
                        var: var.clone(),
                    }
                })?),
            };
        let extra_params = if spec.engine == Engine::Ravel {
            let tokens: Vec<String> = match &row.commit_tokens_by_shard {
                Some(by_shard) => by_shard.values().cloned().collect(),
                None => row
                    .last_commit_token
                    .as_deref()
                    .unwrap_or_default()
                    .split(',')
                    .map(str::to_string)
                    .collect(),
            };
            let params: Vec<(String, String)> = tokens
                .into_iter()
                .filter(|t| !t.trim().is_empty())
                .map(|t| (MIN_COMMIT_TOKEN_PARAM.to_string(), t))
                .collect();
            if params.is_empty() {
                return Err(BindError::NoCommitToken);
            }
            params
        } else {
            Vec::new()
        };
        engines.push(BoundEngine {
            engine: spec.engine,
            query_url: query_url.trim_end_matches('/').to_string(),
            bearer_token,
            extra_params,
            unsupported: row
                .unsupported_classes
                .iter()
                .map(|u| (u.class, u.source.clone()))
                .collect(),
        });
    }
    Ok(Binding { run, grid, engines })
}

/// Whether `entry` reads a native-histogram family of `workload`.
///
/// Read from the query text, not from `constructs`: the difftest registry
/// names functions and syntax, and no construct name says "native
/// histogram" (`histogram_quantile` reads classic and native histograms
/// alike). An entry needs native histograms when an identifier in its query
/// is the name of a family whose kind is `native_histogram`.
pub fn needs_native_histograms(entry: &CorpusEntry, workload: &WorkloadFile) -> bool {
    let native: Vec<&str> = workload
        .families
        .iter()
        .filter(|f| f.kind == FamilyKind::NativeHistogram)
        .map(|f| f.name.as_str())
        .collect();
    entry
        .promql
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
        .any(|word| native.contains(&word))
}

/// The pass and repetition counts, the per-query deadline, and the oracle
/// retry budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunOptions {
    pub warmup_passes: u32,
    pub passes: u32,
    pub reps_per_pass: u32,
    pub deadline: Duration,
    /// How many more times an oracle query lost to a timeout or a transport
    /// failure is re-sent before the run is void.
    pub oracle_retries: u32,
}

/// The fixed pause before each oracle retry.
pub const ORACLE_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// One re-sent oracle query.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct OracleRetry {
    pub entry: String,
    /// The timed pass, from 1.
    pub pass: u32,
    /// The repetition whose outcome was unavailable, from 1.
    pub repetition: u32,
    /// The retry's attempt number; the first attempt is 1, so the first
    /// retry is 2.
    pub attempt: u32,
}

/// Whether an oracle outcome is worth re-sending: a timeout, a transport
/// failure before a complete body arrived, or a 502, 503 or 504 (a gateway or
/// an overloaded server saying "not now"), whether its body is a JSON envelope
/// or not. Any other answer that arrived (another status, 4xx and 500
/// included, a body over the cap, a complete body that is not JSON) is not.
pub fn oracle_retryable(outcome: &QueryOutcome) -> bool {
    match outcome {
        QueryOutcome::Timeout { .. } => true,
        QueryOutcome::Transport {
            kind, http_status, ..
        } => {
            matches!(
                kind,
                TransportErrorKind::Connect
                    | TransportErrorKind::Request
                    | TransportErrorKind::Body
            ) || (matches!(kind, TransportErrorKind::Decode)
                && matches!(*http_status, Some(s) if transient_http_status(s)))
        }
        QueryOutcome::Envelope { http_status, .. } => transient_http_status(*http_status),
        QueryOutcome::BodyTooLarge { .. } => false,
    }
}

/// A status that says the server could not answer now rather than that the
/// query is wrong: 502 Bad Gateway, 503 Service Unavailable, 504 Gateway
/// Timeout.
fn transient_http_status(status: u16) -> bool {
    matches!(status, 502..=504)
}

/// Why a run produced no report.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("corpus entry `{entry}` has no cost class; a version-3 row needs one")]
    Unclassed { entry: String },
    #[error("the run is void: corpus entry `{entry}` on engine `{engine}`: {source}")]
    Classify {
        entry: String,
        engine: &'static str,
        source: ClassifyError,
    },
    #[error("passes and reps_per_pass must each be at least 1")]
    NoPasses,
}

/// The rows and the per-engine figures a run produced.
#[derive(Debug)]
pub struct RunOutput {
    pub cases_expected: Vec<ExpectedCase>,
    pub measurements: Vec<Measurement>,
    pub denominators: Vec<EngineDenominators>,
    /// Every oracle retry the run used, in the order sent.
    pub oracle_retries: Vec<OracleRetry>,
    /// One per (entry, engine other than the oracle), engine-major in the
    /// binding's engine order.
    pub ulp_gaps: Vec<UlpGap>,
}

/// The `--diagnostics` sidecar: figures a pre-registered check needs that
/// the version-3 report does not carry. It is not part of the report and
/// `report_schema::validate` never reads it.
#[derive(Debug, serde::Serialize)]
pub struct Diagnostics<'a> {
    pub profile: &'a str,
    pub corpus_digest: &'a str,
    pub oracle: &'static str,
    pub ulp_gaps: &'a [UlpGap],
    pub oracle_retries: &'a [OracleRetry],
}

/// The largest ULP gap between one engine's answers to one entry and the
/// oracle's, over every timed pass and repetition, for the diagnostics
/// sidecar. Only answers classified against the oracle are measured; an
/// entry unsupported on the engine by declaration measures none.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct UlpGap {
    pub entry: String,
    pub engine: &'static str,
    /// The largest [`ulp_distance`] over every finite value pair of every
    /// aligned answer; `None` when no aligned answer had one.
    pub max_ulps: Option<u64>,
    /// Answers with the oracle's series set and timestamps.
    pub aligned_answers: u64,
    /// Answers that did not align: a refusal, an error, a timeout, or a
    /// different result type, series set, series length or timestamp.
    pub unaligned_answers: u64,
    /// The value pairs compared across the aligned answers.
    pub value_pairs: u64,
    /// The value pairs among those where a NaN or an infinity met anything
    /// but itself, which [`ulp_distance`] has no count for.
    pub non_finite_mismatches: u64,
}

impl UlpGap {
    fn new(entry: &str, engine: Engine) -> Self {
        UlpGap {
            entry: entry.to_string(),
            engine: system_name(engine),
            max_ulps: None,
            aligned_answers: 0,
            unaligned_answers: 0,
            value_pairs: 0,
            non_finite_mismatches: 0,
        }
    }

    /// Folds in one answer: `answer` is the body of a 2xx envelope, or
    /// `None` for any other outcome.
    fn add(&mut self, oracle: &Json, answer: Option<&Json>) {
        let mut pairs = ValuePairs::default();
        if answer.is_some_and(|a| pairs.answer(oracle, a).is_some()) {
            self.aligned_answers += 1;
            self.value_pairs += pairs.count;
            self.non_finite_mismatches += pairs.non_finite;
            self.max_ulps = self.max_ulps.max(pairs.max_ulps);
        } else {
            self.unaligned_answers += 1;
        }
    }
}

/// The value pairs of one answer aligned with the oracle's. Each method
/// returns `None` at the first point where the two shapes differ.
#[derive(Default)]
struct ValuePairs {
    max_ulps: Option<u64>,
    count: u64,
    non_finite: u64,
}

impl ValuePairs {
    fn answer(&mut self, oracle: &Json, other: &Json) -> Option<()> {
        if oracle["status"].as_str() != Some("success")
            || other["status"].as_str() != Some("success")
        {
            return None;
        }
        let kind = oracle["data"]["resultType"].as_str()?;
        if other["data"]["resultType"].as_str() != Some(kind) {
            return None;
        }
        let (o, a) = (&oracle["data"]["result"], &other["data"]["result"]);
        match kind {
            "scalar" => self.sample(o, a),
            "vector" | "matrix" => {
                let (o, a) = (series_by_labels(o)?, series_by_labels(a)?);
                if o.len() != a.len() {
                    return None;
                }
                for ((o_key, o), (a_key, a)) in o.iter().zip(&a) {
                    if o_key != a_key {
                        return None;
                    }
                    if kind == "vector" {
                        self.element(o, a)?;
                    } else {
                        self.steps(&o["values"], &a["values"], Self::sample)?;
                        self.steps(&o["histograms"], &a["histograms"], Self::histogram)?;
                    }
                }
                Some(())
            }
            _ => None,
        }
    }

    fn element(&mut self, o: &Json, a: &Json) -> Option<()> {
        match (o.get("histogram"), a.get("histogram")) {
            (Some(o), Some(a)) => self.histogram(o, a),
            (None, None) => self.sample(&o["value"], &a["value"]),
            _ => None,
        }
    }

    /// Pairs a matrix series' steps by position; an absent array is empty.
    fn steps(
        &mut self,
        o: &Json,
        a: &Json,
        pair: fn(&mut Self, &Json, &Json) -> Option<()>,
    ) -> Option<()> {
        let (o, a) = (steps_of(o)?, steps_of(a)?);
        if o.len() != a.len() {
            return None;
        }
        for (o, a) in o.iter().zip(a) {
            pair(self, o, a)?;
        }
        Some(())
    }

    fn sample(&mut self, o: &Json, a: &Json) -> Option<()> {
        let (o_ts, o) = timestamped(o)?;
        let (a_ts, a) = timestamped(a)?;
        if o_ts != a_ts {
            return None;
        }
        self.value(parse_value(o.as_str()?)?, parse_value(a.as_str()?)?);
        Some(())
    }

    /// A native histogram's `count` and `sum`, the two floats the comparator
    /// compares under the ULP rule; its buckets compare exactly there.
    fn histogram(&mut self, o: &Json, a: &Json) -> Option<()> {
        let (o_ts, o) = timestamped(o)?;
        let (a_ts, a) = timestamped(a)?;
        if o_ts != a_ts {
            return None;
        }
        for field in ["count", "sum"] {
            self.value(
                parse_value(o[field].as_str()?)?,
                parse_value(a[field].as_str()?)?,
            );
        }
        Some(())
    }

    fn value(&mut self, o: f64, a: f64) {
        self.count += 1;
        match ulp_distance(o, a) {
            Some(d) => self.max_ulps = self.max_ulps.max(Some(d)),
            None => self.non_finite += 1,
        }
    }
}

/// A vector or matrix result keyed by its sorted label set; `None` when it
/// is not an array of labelled elements or two elements share a label set.
fn series_by_labels(result: &Json) -> Option<BTreeMap<Vec<(String, String)>, &Json>> {
    let mut by_labels = BTreeMap::new();
    for elem in result.as_array()? {
        let mut labels: Vec<(String, String)> = elem["metric"]
            .as_object()?
            .iter()
            .map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
            .collect::<Option<_>>()?;
        labels.sort();
        if by_labels.insert(labels, elem).is_some() {
            return None;
        }
    }
    Some(by_labels)
}

fn steps_of(steps: &Json) -> Option<&[Json]> {
    match steps {
        Json::Null => Some(&[]),
        other => other.as_array().map(Vec::as_slice),
    }
}

/// A `[timestamp, value]` pair.
fn timestamped(pair: &Json) -> Option<(f64, &Json)> {
    match pair.as_array()?.as_slice() {
        [ts, value] => Some((ts.as_f64()?, value)),
        _ => None,
    }
}

/// A Prometheus sample value string.
fn parse_value(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "+Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        other => other.parse().ok(),
    }
}

/// The body of a 2xx envelope.
fn answer_body(outcome: &QueryOutcome) -> Option<&Json> {
    match outcome {
        QueryOutcome::Envelope {
            http_status, body, ..
        } if (200..300).contains(http_status) => Some(body),
        _ => None,
    }
}

/// The cases a run over `entries` and `engines` must report, one per
/// (engine, entry, query kind).
pub fn expected_cases(entries: &[CorpusEntry], engines: &[BoundEngine]) -> Vec<ExpectedCase> {
    let mut cases = Vec::with_capacity(entries.len() * engines.len());
    for engine in engines {
        for entry in entries {
            cases.push(ExpectedCase {
                engine: engine.engine,
                id: entry.id.clone(),
                query_kind: entry.eval,
            });
        }
    }
    cases
}

/// What one pass of one case came to.
struct PassResult {
    classified: Classified,
    /// The median of the pass's repetition latencies, in milliseconds.
    median_ms: f64,
}

/// A case's per-pass results.
struct CaseRecord {
    passes: Vec<PassResult>,
    /// The ingest row's citation when the case is unsupported by declaration.
    unsupported_citation: Option<String>,
    /// The last answer an unsupported case got, summarised.
    unsupported_answer: Option<String>,
}

/// Runs `entries` (already filtered to the ingest run's profile) against
/// every engine of `binding`.
pub async fn run(
    binding: &Binding,
    entries: &[CorpusEntry],
    workload: &WorkloadFile,
    options: RunOptions,
) -> Result<RunOutput, RunError> {
    if options.passes == 0 || options.reps_per_pass == 0 {
        return Err(RunError::NoPasses);
    }
    if let Some(entry) = entries.iter().find(|e| e.class.is_none()) {
        return Err(RunError::Unclassed {
            entry: entry.id.clone(),
        });
    }
    let cases_expected = expected_cases(entries, &binding.engines);
    let clients: Vec<PrometheusClient> = binding
        .engines
        .iter()
        .map(|e| e.client(options.deadline))
        .collect();
    let needs_native: Vec<bool> = entries
        .iter()
        .map(|e| needs_native_histograms(e, workload))
        .collect();

    for _ in 0..options.warmup_passes {
        for entry in entries {
            for client in &clients {
                for _ in 0..options.reps_per_pass {
                    let _ = send(client, entry, &binding.grid, options.deadline).await;
                }
            }
        }
    }

    // records[engine][entry]
    let mut records: Vec<Vec<CaseRecord>> = binding
        .engines
        .iter()
        .map(|engine| {
            entries
                .iter()
                .zip(&needs_native)
                .map(|(_, native)| CaseRecord {
                    passes: Vec::new(),
                    unsupported_citation: if *native {
                        engine
                            .unsupported_citation(PayloadClass::NativeHistogram)
                            .map(str::to_string)
                    } else {
                        None
                    },
                    unsupported_answer: None,
                })
                .collect()
        })
        .collect();

    // ulp_gaps[engine][entry]
    let mut ulp_gaps: Vec<Vec<UlpGap>> = binding
        .engines
        .iter()
        .map(|engine| {
            entries
                .iter()
                .map(|entry| UlpGap::new(&entry.id, engine.engine))
                .collect()
        })
        .collect();
    let mut oracle_retries: Vec<OracleRetry> = Vec::new();
    for pass in 1..=options.passes {
        for (entry_index, entry) in entries.iter().enumerate() {
            let expectation = EntryExpectation {
                intended_empty: entry.expected.intended_empty,
                tolerance_ulps: entry.tolerance_ulps,
                mode: ComparisonMode::Unordered,
                constructs: entry.constructs.clone(),
            };
            let mut oracle: Option<OracleAnswer> = None;
            for (engine_index, engine) in binding.engines.iter().enumerate() {
                let record = &mut records[engine_index][entry_index];
                let mut reps: Vec<(Classified, f64)> = Vec::new();
                for repetition in 1..=options.reps_per_pass {
                    let outcome = send(
                        &clients[engine_index],
                        entry,
                        &binding.grid,
                        options.deadline,
                    )
                    .await;
                    let latency_ms = outcome.timing().elapsed.as_secs_f64() * 1000.0;
                    let void = |source| RunError::Classify {
                        entry: entry.id.clone(),
                        engine: system_name(engine.engine),
                        source,
                    };
                    if engine.engine == Engine::Prometheus && oracle.is_none() {
                        let answer = oracle_answer(
                            &clients[engine_index],
                            entry,
                            &binding.grid,
                            &options,
                            &outcome,
                            (pass, repetition),
                            &mut oracle_retries,
                        )
                        .await;
                        oracle = Some(answer.map_err(void)?);
                    }
                    if let Some(citation) = &record.unsupported_citation {
                        record.unsupported_answer = Some(summarize(&outcome));
                        reps.push((
                            Classified {
                                status: ResultStatus::UnsupportedConstruct,
                                refusal_reason: None,
                                error_class: None,
                                incomplete: false,
                                annotations: Default::default(),
                                message: Some(citation.clone()),
                            },
                            latency_ms,
                        ));
                        continue;
                    }
                    let classified =
                        classify(engine.engine, &outcome, oracle.as_ref(), &expectation)
                            .map_err(void)?;
                    if engine.engine != Engine::Prometheus
                        && let Some(oracle) = &oracle
                    {
                        ulp_gaps[engine_index][entry_index]
                            .add(oracle.envelope(), answer_body(&outcome));
                    }
                    reps.push((classified, latency_ms));
                }
                let latencies: Vec<f64> = reps.iter().map(|(_, ms)| *ms).collect();
                let classified = reps
                    .iter()
                    .find(|(c, _)| c.status != ResultStatus::Ok)
                    .or_else(|| reps.first())
                    .map(|(c, _)| c.clone())
                    .ok_or(RunError::NoPasses)?;
                record.passes.push(PassResult {
                    classified,
                    median_ms: median(&latencies),
                });
            }
        }
    }

    let mut measurements = Vec::with_capacity(cases_expected.len());
    let mut denominators = Vec::with_capacity(binding.engines.len());
    for (engine, engine_records) in binding.engines.iter().zip(&records) {
        let mut rows: Vec<Classified> = Vec::with_capacity(entries.len());
        for (entry, record) in entries.iter().zip(engine_records) {
            let (measurement, row) = row_of(engine, entry, record, options.reps_per_pass);
            if let Some(citation) = &record.unsupported_citation {
                eprintln!(
                    "metricsbench_query: {}/{}: unsupported_construct by declaration ({citation}); \
                     answer: {}",
                    system_name(engine.engine),
                    entry.id,
                    record.unsupported_answer.as_deref().unwrap_or("none"),
                );
            } else if let Some(message) = &row.message
                && row.status != ResultStatus::Ok
            {
                eprintln!(
                    "metricsbench_query: {}/{}: {}: {message}",
                    system_name(engine.engine),
                    entry.id,
                    status_name(row.status),
                );
            }
            measurements.push(measurement);
            rows.push(row);
        }
        let d = Denominators::of(&rows);
        denominators.push(EngineDenominators {
            engine: engine.engine,
            corpus: d.corpus,
            correctness: d.correctness,
            timing: d.timing,
            coverage_success: d.coverage_success,
        });
    }
    Ok(RunOutput {
        cases_expected,
        measurements,
        denominators,
        oracle_retries,
        ulp_gaps: binding
            .engines
            .iter()
            .zip(ulp_gaps)
            .filter(|(engine, _)| engine.engine != Engine::Prometheus)
            .flat_map(|(_, gaps)| gaps)
            .collect(),
    })
}

/// The oracle's answer for one entry in one pass. `first` is the outcome of
/// the Prometheus repetition that asked, and that repetition is classified
/// from `first` whatever happens here: a retry is a separate request that
/// answers the oracle only, so it can never make Prometheus' row faster or
/// turn its timeout into `ok`. While the latest outcome is
/// [`oracle_retryable`] and the budget lasts, the query is re-sent after
/// [`ORACLE_RETRY_BACKOFF`]; each retry is appended to `retries` and named on
/// stderr. An exhausted budget, or an outcome that is not retryable, returns
/// the [`ClassifyError::OracleUnavailable`] of the last outcome.
async fn oracle_answer(
    client: &PrometheusClient,
    entry: &CorpusEntry,
    grid: &QueryGrid,
    options: &RunOptions,
    first: &QueryOutcome,
    (pass, repetition): (u32, u32),
    retries: &mut Vec<OracleRetry>,
) -> Result<OracleAnswer, ClassifyError> {
    let mut answer = OracleAnswer::from_outcome(first);
    let mut retryable = oracle_retryable(first);
    let mut last = summarize(first);
    let mut attempt = 1;
    while answer.is_err() && retryable && attempt <= options.oracle_retries {
        attempt += 1;
        eprintln!(
            "metricsbench_query: oracle retry: entry `{}`, pass {pass}, repetition {repetition}, \
             attempt {attempt} of {}: {}",
            entry.id,
            options.oracle_retries + 1,
            last,
        );
        tokio::time::sleep(ORACLE_RETRY_BACKOFF).await;
        let outcome = send(client, entry, grid, options.deadline).await;
        retries.push(OracleRetry {
            entry: entry.id.clone(),
            pass,
            repetition,
            attempt,
        });
        answer = OracleAnswer::from_outcome(&outcome);
        retryable = oracle_retryable(&outcome);
        last = summarize(&outcome);
    }
    answer
}

/// The report row for one case, and the row-level classification its
/// denominators count. Only an `ok` row carries latency.
fn row_of(
    engine: &BoundEngine,
    entry: &CorpusEntry,
    record: &CaseRecord,
    reps_per_pass: u32,
) -> (Measurement, Classified) {
    let pass_statuses: Vec<ResultStatus> =
        record.passes.iter().map(|p| p.classified.status).collect();
    let status = row_status(&pass_statuses);
    let deciding = record
        .passes
        .iter()
        .find(|p| p.classified.status == status)
        .map(|p| p.classified.clone());
    let any_incomplete = record.passes.iter().any(|p| p.classified.incomplete);
    let incomplete = match status {
        ResultStatus::Partial => true,
        ResultStatus::Incorrect => any_incomplete,
        _ => false,
    };
    let mut m = Measurement {
        id: entry.id.clone(),
        // `run` refused an unclassed entry before any query.
        class: entry
            .class
            .unwrap_or(crate::promql_corpus::CostClass::HighFanOut),
        status,
        figures: Vec::new(),
        engine: Some(engine.engine),
        oracle: engine.engine == Engine::Prometheus,
        endpoint: Some(engine.query_url.clone()),
        query_kind: Some(entry.eval),
        refusal_reason: deciding.as_ref().and_then(|c| c.refusal_reason),
        error_class: deciding.as_ref().and_then(|c| c.error_class.clone()),
        incomplete,
        pass_statuses,
        reps_per_pass: None,
        pass_medians_ms: Vec::new(),
        rel_mad_pct: None,
    };
    if status.is_timed() {
        let medians: Vec<f64> = record.passes.iter().map(|p| p.median_ms).collect();
        let min = medians.iter().copied().fold(f64::INFINITY, f64::min);
        let max = medians.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        m.figures = vec![
            Figure {
                name: "min_ms".to_string(),
                value: min,
            },
            Figure {
                name: "median_ms".to_string(),
                value: median(&medians),
            },
            Figure {
                name: "max_ms".to_string(),
                value: max,
            },
        ];
        m.reps_per_pass = Some(reps_per_pass);
        m.rel_mad_pct = if medians.len() >= MIN_VARIANCE_PASSES {
            rel_mad_pct(&medians)
        } else {
            None
        };
        m.pass_medians_ms = medians;
    }
    let row = Classified {
        status,
        refusal_reason: m.refusal_reason,
        error_class: m.error_class.clone(),
        incomplete,
        annotations: Default::default(),
        message: deciding.and_then(|c| c.message),
    };
    (m, row)
}

/// One query for `entry`: an instant query at the newest sample, or one
/// range query over the whole grid.
async fn send(
    client: &PrometheusClient,
    entry: &CorpusEntry,
    grid: &QueryGrid,
    deadline: Duration,
) -> QueryOutcome {
    match entry.eval {
        EvalKind::Instant => {
            client
                .instant_query_with_deadline(&entry.promql, grid.newest_ms, deadline)
                .await
        }
        EvalKind::Range => {
            client
                .range_query_with_deadline(
                    &entry.promql,
                    grid.range_start_ms,
                    grid.range_end_ms,
                    grid.range_step_ms,
                    deadline,
                )
                .await
        }
    }
}

/// A one-line description of an answer, for the stderr record of an
/// unsupported case.
fn summarize(outcome: &QueryOutcome) -> String {
    match outcome {
        QueryOutcome::Envelope {
            http_status, body, ..
        } => {
            let series = body["data"]["result"].as_array().map(Vec::len);
            match series {
                Some(n) => format!("HTTP {http_status}, {n} series"),
                None => format!(
                    "HTTP {http_status}, status `{}`",
                    body["status"].as_str().unwrap_or("?")
                ),
            }
        }
        QueryOutcome::Timeout { phase, .. } => format!("timeout in the {phase:?} phase"),
        QueryOutcome::Transport { kind, .. } => format!("transport failure: {kind:?}"),
        QueryOutcome::BodyTooLarge { limit, .. } => format!("body over {limit} bytes"),
    }
}

fn status_name(status: ResultStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{status:?}"))
}

/// The median `report_schema::validate` recomputes, so producer and checker
/// share one definition; `NaN` for an empty slice.
fn median(values: &[f64]) -> f64 {
    crate::report_schema::median(values).unwrap_or(f64::NAN)
}

/// Every engine's query URL, for the provenance config.
pub fn endpoints(binding: &Binding) -> BTreeMap<&'static str, String> {
    binding
        .engines
        .iter()
        .map(|e| (system_name(e.engine), e.query_url.clone()))
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn engine_specs_parse_and_refuse() {
        let spec = parse_engine_spec("name=ravel,url=http://r:9090,bearer_env=TOK").expect("ok");
        assert_eq!(spec.engine, Engine::Ravel);
        assert_eq!(spec.url.as_deref(), Some("http://r:9090"));
        assert_eq!(spec.bearer_env.as_deref(), Some("TOK"));
        assert!(parse_engine_spec("name=ravel-in-process").is_err());
        assert!(parse_engine_spec("url=http://x").is_err());
        assert!(parse_engine_spec("name=mimir,token=abc").is_err());
        assert!(parse_engine_spec("name=mimir,name=ravel").is_err());
    }

    #[test]
    fn only_lost_oracle_requests_are_retryable() {
        use ravel_promql_difftest::prometheus_client::{QueryTiming, TimeoutPhase};
        let timing = QueryTiming::default();
        let transport = |kind, http_status| QueryOutcome::Transport {
            kind,
            http_status,
            message: String::new(),
            timing,
        };
        assert!(oracle_retryable(&QueryOutcome::Timeout {
            phase: TimeoutPhase::Headers,
            elapsed: Duration::from_secs(1),
            timing,
        }));
        assert!(oracle_retryable(&transport(
            TransportErrorKind::Connect,
            None
        )));
        assert!(oracle_retryable(&transport(
            TransportErrorKind::Request,
            None
        )));
        assert!(oracle_retryable(&transport(
            TransportErrorKind::Body,
            Some(200)
        )));
        for http_status in [502, 503, 504] {
            assert!(oracle_retryable(&transport(
                TransportErrorKind::Decode,
                Some(http_status)
            )));
            assert!(oracle_retryable(&QueryOutcome::Envelope {
                http_status,
                body: serde_json::json!({"status": "error"}),
                timing,
            }));
        }
        for http_status in [Some(200), Some(500), None] {
            assert!(!oracle_retryable(&transport(
                TransportErrorKind::Decode,
                http_status
            )));
        }
        assert!(!oracle_retryable(&QueryOutcome::BodyTooLarge {
            limit: 1,
            timing,
        }));
        for http_status in [200, 400, 422, 500, 501, 505] {
            assert!(!oracle_retryable(&QueryOutcome::Envelope {
                http_status,
                body: serde_json::json!({"status": "error"}),
                timing,
            }));
        }
    }

    fn matrix(series: &[(&str, &[(f64, &str)])]) -> Json {
        let result: Vec<Json> = series
            .iter()
            .map(|(instance, steps)| {
                let values: Vec<Json> = steps
                    .iter()
                    .map(|(t, v)| serde_json::json!([t, v]))
                    .collect();
                serde_json::json!({"metric": {"instance": instance}, "values": values})
            })
            .collect();
        serde_json::json!({"status": "success", "data": {"resultType": "matrix", "result": result}})
    }

    fn gap_of(oracle: &Json, answer: Option<&Json>) -> UlpGap {
        let mut gap = UlpGap::new("e", Engine::Ravel);
        gap.add(oracle, answer);
        gap
    }

    #[test]
    fn ulp_gaps_pair_series_by_labels_and_steps_by_position() {
        let oracle = matrix(&[("a", &[(1.0, "1.5"), (2.0, "2")]), ("b", &[(1.0, "4")])]);
        let one_above_two = f64::from_bits(2.0f64.to_bits() + 1).to_string();
        let reordered = matrix(&[
            ("b", &[(1.0, "4")]),
            ("a", &[(1.0, "1.5"), (2.0, &one_above_two)]),
        ]);
        let gap = gap_of(&oracle, Some(&reordered));
        assert_eq!(gap.max_ulps, Some(1));
        assert_eq!((gap.aligned_answers, gap.value_pairs), (1, 3));
        for other in [
            matrix(&[("a", &[(1.0, "1.5"), (3.0, "2")]), ("b", &[(1.0, "4")])]),
            matrix(&[("a", &[(1.0, "1.5")]), ("b", &[(1.0, "4")])]),
            matrix(&[("a", &[(1.0, "1.5"), (2.0, "2")]), ("c", &[(1.0, "4")])]),
            matrix(&[("a", &[(1.0, "1.5"), (2.0, "2")])]),
        ] {
            let gap = gap_of(&oracle, Some(&other));
            assert_eq!(
                (gap.max_ulps, gap.aligned_answers, gap.unaligned_answers),
                (None, 0, 1),
                "{other}"
            );
        }
        let gap = gap_of(&oracle, None);
        assert_eq!((gap.max_ulps, gap.unaligned_answers), (None, 1));
    }

    #[test]
    fn a_non_finite_mismatch_is_counted_not_measured() {
        let oracle = matrix(&[("a", &[(1.0, "NaN"), (2.0, "+Inf"), (3.0, "1")])]);
        let same = gap_of(&oracle, Some(&oracle));
        assert_eq!((same.max_ulps, same.non_finite_mismatches), (Some(0), 0));
        let other = matrix(&[("a", &[(1.0, "1"), (2.0, "+Inf"), (3.0, "1")])]);
        let gap = gap_of(&oracle, Some(&other));
        assert_eq!(gap.max_ulps, Some(0));
        assert_eq!((gap.value_pairs, gap.non_finite_mismatches), (3, 1));
        let only = matrix(&[("a", &[(1.0, "NaN")])]);
        let finite = matrix(&[("a", &[(1.0, "1")])]);
        let gap = gap_of(&only, Some(&finite));
        assert_eq!((gap.max_ulps, gap.aligned_answers), (None, 1));
        assert_eq!(gap.non_finite_mismatches, 1);
    }

    #[test]
    fn median_of_even_and_odd_counts() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[10.0, 12.0, 11.0, 15.0]), 11.5);
        assert!(median(&[]).is_nan());
    }
}
