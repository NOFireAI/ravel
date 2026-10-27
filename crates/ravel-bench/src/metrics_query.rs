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
//! [`ClassifyError`] voids the run ([`RunError::Classify`]). A range entry is
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

use ravel_promql_difftest::corpus::ComparisonMode;
use ravel_promql_difftest::prometheus_client::{PrometheusClient, QueryOutcome};

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

/// The pass and repetition counts and the per-query deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunOptions {
    pub warmup_passes: u32,
    pub passes: u32,
    pub reps_per_pass: u32,
    pub deadline: Duration,
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

    for _ in 0..options.passes {
        for (entry_index, entry) in entries.iter().enumerate() {
            let expectation = EntryExpectation {
                intended_empty: entry.expected.intended_empty,
                tolerance_ulps: None,
                mode: ComparisonMode::Unordered,
                constructs: entry.constructs.clone(),
            };
            let mut oracle: Option<OracleAnswer> = None;
            for (engine_index, engine) in binding.engines.iter().enumerate() {
                let record = &mut records[engine_index][entry_index];
                let mut reps: Vec<(Classified, f64)> = Vec::new();
                for _ in 0..options.reps_per_pass {
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
                        oracle = Some(OracleAnswer::from_outcome(&outcome).map_err(void)?);
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
    })
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
    fn median_of_even_and_odd_counts() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[10.0, 12.0, 11.0, 15.0]), 11.5);
        assert!(median(&[]).is_nan());
    }
}
