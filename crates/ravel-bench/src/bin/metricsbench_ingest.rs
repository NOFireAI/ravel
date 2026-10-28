//! MetricsBench Remote Write 1.0 ingest lane (ADR-0927, issue #937, task M5).
//!
//! Replays one logical stream (scalar samples, staleness markers and native
//! histograms) into the in-process Ravel path (`WriteMode::Strict`,
//! durable-on-ack, commit tokens) and, behind endpoints supplied on the
//! command line with no hardcoded hosts, over portable Remote Write 1.0 into
//! `ravel-server` (`--ravel-endpoint`, ADR-0927 2026-10-26 amendment A),
//! Prometheus, VictoriaMetrics and Mimir. It prints one JSON report whose
//! every row states that system's acknowledgement meaning (ADR-0927 decision
//! 3), and it fails the run if the accounting does not close (band 4) rather
//! than reporting a silent drop.
//!
//! The stream is the deterministic MetricsBench workload
//! (`metricsbench_gen`'s generator), streamed one scrape step at a time
//! through the shipping encoder and classified line by line, so this lane
//! grows no second workload generator and holds one batch rather than the
//! whole history. Each comparator replays its own regeneration of the same
//! stream from the same anchor, and the run fails unless every regeneration's
//! digest matches the Ravel replay's. `--profile` has no default: the profile
//! selects which data the run touches (CLAUDE.md measurement discipline).
//! `--anchor-ms` has none either (ADR-0927, 2026-10-26 amendment, section C):
//! the stream is a pure function of the manifest, the profile, the step count
//! and the anchor, so a clock-derived anchor would make every run a different
//! stream. A missing or invalid anchor exits 64.
//!
//! `--no-in-process-row` drops the in-process row for a run of portable rows
//! only: no store is opened and no post-ingest query runs, and the report
//! says `in_process_row: "skipped"`. Band 6 still runs across the portable
//! rows (`check_band6_across_rows`).
//!
//! A portable row whose read-back fails (an error, a refusal or a mismatch)
//! or that the stall ceiling stops does not stop the replay: the row is
//! recorded (`readback_error` or `readback`, or an entry in `stalled`), the
//! later rows are replayed, and once every row has run the bin prints the
//! report and exits with the first failing row's code, 1
//! (`metricsbench_ingest_smoke::a_failed_readback_does_not_stop_the_later_rows`).
//! Any other failure (band 4, a diverged stream, a transport error) on a row
//! before the first failing one still stops the run at that row with no
//! report. Once a row has failed, a later row's failure is recorded instead
//! (`row_failure` on a row with figures, an entry in `failed_replays` on one
//! without) and the report is still printed
//! (`metricsbench_ingest_smoke::a_later_hard_failure_after_a_failed_readback_keeps_the_report`).
//!
//! Reachable as `cargo run -p ravel-bench --bin metricsbench_ingest -- \
//! --profile ci --store memory --steps 10 --anchor-ms 1700000010000`.
#![allow(clippy::expect_used)]

use std::path::PathBuf;

use clap::Parser;
use ravel_bench::harness::{
    StoreKind, backend_bills_requests, endpoint_host_from_env, store_and_metrics_from_env,
};
use ravel_bench::metrics_gen::{GenerationReport, Generator};
use ravel_bench::metrics_ingest::{
    AnchorError, Band6Violation, DEFAULT_HTTP_BATCH_SIZE, DEFAULT_HTTP_SHARDS, FailedReplay,
    GeneratedStream, HttpReplayConfig, IN_PROCESS_SYSTEM, LaneError, MetricsIngestReport,
    PayloadClass, ProfileRecord, RavelReplayConfig, ReadbackConfig, Receiver,
    RowAccountingViolation, RowFailure, RunProvenance, STALL_GRACE, STALL_RECHECK, StallCeiling,
    StalledRow, StreamBandViolation, StreamCounts, Substrate, SystemIngestResult,
    SystemQueryResult, build_profile_record, check_anchor, check_expected_digests_agree,
    check_row_accounting, check_row_band6, check_stream_band, query_after_replay,
    readback_over_http, replay_into_ravel, replay_over_http_with_plan, workload_digest,
};
use ravel_bench::metrics_workload::{Profile, WorkloadFile, load_workload};
use ravel_types::TenantId;

/// The exit code for a usage error (BSD `EX_USAGE`): a missing or invalid run
/// parameter, refused before anything is generated.
const EXIT_USAGE: i32 = 64;
/// The exit code for input data that cannot be used (BSD `EX_DATAERR`): the
/// manifest's profile declares no scrape grid, which no flag can fix.
const EXIT_DATAERR: i32 = 65;

/// Where the checked-in workload manifest lives, relative to this crate's
/// manifest dir. The same default `metricsbench_gen` uses.
const DEFAULT_WORKLOAD: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/metrics/workload.json"
);

#[derive(Parser, Debug)]
#[command(
    about = "Replay the MetricsBench sample stream over Remote Write 1.0 into Ravel and \
             config-supplied comparators (ADR-0927 M5)"
)]
struct Args {
    /// The workload manifest to generate the stream from.
    #[arg(long, default_value = DEFAULT_WORKLOAD)]
    workload: PathBuf,
    /// Which profile to generate. No default: the choice selects which data the
    /// run touches.
    #[arg(long)]
    profile: String,
    /// Scrapes to generate. Defaults to the profile's full
    /// `samples_per_series`.
    #[arg(long)]
    steps: Option<u64>,
    /// Timestamp of the first generated step, in milliseconds since the
    /// epoch. Required, with no default: the stream is a function of its run
    /// parameters, and two runs with the same anchor replay the same bytes.
    /// Must be non-negative, a multiple of the profile's scrape interval in
    /// milliseconds, and put the last step at or before 9223372036854 ms.
    /// Missing or invalid, the run exits 64.
    #[arg(long, allow_negative_numbers = true)]
    anchor_ms: Option<i64>,
    /// The object store backing the in-process Ravel path. Unused with
    /// `--no-in-process-row`.
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    /// Build no in-process row: open no store, run no post-ingest query, and
    /// replay into the portable rows only. Needs at least one portable
    /// endpoint. For profiles whose in-process store would not fit beside the
    /// engines on the host.
    #[arg(long)]
    no_in_process_row: bool,
    /// Ingest shard count.
    #[arg(long, default_value_t = 4)]
    shards: u32,
    /// Logical samples per write batch.
    #[arg(long, default_value_t = 500)]
    batch_size: usize,
    /// Per-batch strict-ack deadline for the Ravel path.
    #[arg(long, default_value_t = 30)]
    ack_timeout_secs: u64,
    /// Tenant the in-process row writes under, recorded as its `tenant`.
    /// Unused with `--no-in-process-row`.
    #[arg(long, default_value = "metricsbench")]
    tenant: String,
    /// `ravel-server` `/api/v1/write` endpoint, the portable Ravel row
    /// (`system: ravel`, `endpoint: <url>`). Omitted, only the in-process
    /// row is written.
    #[arg(long)]
    ravel_endpoint: Option<String>,
    /// The tenant `ravel-server` resolves the portable Ravel row's requests
    /// to (from its bearer token, or its dev default), recorded as that row's
    /// `tenant`. Required with `--ravel-endpoint`, and unless
    /// `--no-in-process-row` is given it must equal `--tenant`, so the two
    /// Ravel rows describe one tenant's data.
    #[arg(long)]
    ravel_tenant: Option<String>,
    /// Base URL of `ravel-server`'s Prometheus query API, for the portable
    /// Ravel row's read-back. Required with `--ravel-endpoint`.
    #[arg(long)]
    ravel_query_url: Option<String>,
    /// Base URL of Prometheus's query API. Required with
    /// `--prometheus-endpoint`.
    #[arg(long)]
    prometheus_query_url: Option<String>,
    /// Base URL of VictoriaMetrics's query API. Required with
    /// `--victoriametrics-endpoint`.
    #[arg(long)]
    victoriametrics_query_url: Option<String>,
    /// Base URL of Mimir's Prometheus query API (its `/prometheus` prefix
    /// included). Required with `--mimir-endpoint`.
    #[arg(long)]
    mimir_query_url: Option<String>,
    /// Name of the environment variable holding the bearer token for
    /// `--ravel-endpoint`. The token is read from the environment, never
    /// from argv, and never written into the report. Omitted, no
    /// `Authorization` header is sent.
    #[arg(long)]
    ravel_bearer_token_env: Option<String>,
    /// Prometheus `/api/v1/write` endpoint. Omitted, Prometheus is not
    /// replayed. No hardcoded host: this is the only way to reach it.
    #[arg(long)]
    prometheus_endpoint: Option<String>,
    /// VictoriaMetrics `/api/v1/write` endpoint. Omitted, it is not replayed.
    #[arg(long)]
    victoriametrics_endpoint: Option<String>,
    /// Mimir push endpoint (Remote Write 1.0). Omitted, it is not replayed.
    #[arg(long)]
    mimir_endpoint: Option<String>,
    /// Logical points per RW1.0 request to an HTTP receiver: a shard sends
    /// once it holds this many, or at stream end. Prometheus'
    /// `max_samples_per_send`. Zero is refused.
    #[arg(long, default_value_t = DEFAULT_HTTP_BATCH_SIZE)]
    http_batch_size: usize,
    /// Series shards per HTTP row, each with one request in flight, fixed
    /// for the whole replay. Prometheus' `max_shards`. Zero is refused.
    #[arg(long, default_value_t = DEFAULT_HTTP_SHARDS)]
    http_shards: usize,
    /// Stop an HTTP row once, after 60 s of sending, its projected ingest
    /// wall time exceeds this many seconds. Omitted, a row runs to its end.
    #[arg(long)]
    max_ingest_secs: Option<u64>,
    /// Retries on a 429/5xx before a comparator batch is dropped.
    #[arg(long, default_value_t = 3)]
    http_max_retries: u32,
    /// Per-request timeout for a comparator POST, bounding the complete
    /// operation including the body read (ADR-0927 decision 6). Writes
    /// only: the read-back has `--readback-timeout-secs`.
    #[arg(long, default_value_t = 30)]
    http_timeout_secs: u64,
    /// Per-request timeout for every read-back query of a portable row,
    /// bounding the complete operation including the body read. Separate
    /// from `--http-timeout-secs` because a read over a whole profile (a
    /// 1,000,000-series `/api/v1/series` on cardinality) takes longer than
    /// one write batch.
    #[arg(long, default_value_t = 30)]
    readback_timeout_secs: u64,
    /// How long, in seconds, a portable row's read-back is repeated while its
    /// figures do not match what was sent, for a receiver that acknowledges
    /// a write before it can be queried. The window bounds when the last
    /// read may start, so a read begun just inside it can add up to five
    /// `--readback-timeout-secs` queries. The same window applies to every
    /// system, and each row records `attempts` and `settled_after_ms`. 0
    /// reads once.
    #[arg(long, default_value_t = 60)]
    readback_settle_secs: u64,
}

/// A band violation: the run measured something outside what was pre-registered
/// (ADR-0927 band 4/5), separate from an artifact or replay error.
#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(
        "--anchor-ms is required: the stream digest covers every sample's timestamp, so an \
         anchor read from the clock would make each run replay a different stream, and two \
         runs could never be shown to have replayed the same one. Pass the first step's \
         timestamp in milliseconds since the epoch, a multiple of the profile's scrape \
         interval{grid}"
    )]
    MissingAnchor { grid: String },
    #[error(
        "--ravel-endpoint needs --ravel-tenant, the tenant ravel-server resolves its requests \
         to, so the portable Ravel row records whose data it wrote"
    )]
    MissingRavelTenant,
    #[error(
        "--ravel-tenant `{ravel_tenant}` differs from --tenant `{tenant}`: the in-process and \
         portable Ravel rows must write one tenant's data to be compared"
    )]
    TenantMismatch {
        tenant: String,
        ravel_tenant: String,
    },
    #[error("--{flag}-endpoint needs --{flag}-query-url: every HTTP row is read back")]
    MissingQueryUrl { flag: &'static str },
    #[error("--{flag}-query-url needs --{flag}-endpoint: there is no {flag} row to read back")]
    QueryUrlWithoutEndpoint { flag: &'static str },
    #[error(
        "--ravel-tenant needs --ravel-endpoint: there is no portable Ravel row to record it on"
    )]
    TenantWithoutEndpoint,
    #[error(
        "--no-in-process-row needs at least one portable endpoint (--ravel-endpoint, \
         --prometheus-endpoint, --victoriametrics-endpoint or --mimir-endpoint): a run with no \
         rows measures nothing"
    )]
    NoRows,
    #[error("--{flag} must be at least 1: an HTTP row with no {what} sends nothing")]
    ZeroHttpSetting {
        flag: &'static str,
        what: &'static str,
    },
    #[error("{0}")]
    Band6(#[from] Band6Violation),
    #[error(
        "{system}'s expected scalar set (digest {got}) differs from the in-process row's \
         ({expected}), so the rows were not offered the same scalar points"
    )]
    ScalarExpectedDiverged {
        system: String,
        expected: String,
        got: String,
    },
    #[error("reading {system} back failed: {source}")]
    Readback {
        system: String,
        source: ravel_bench::metrics_ingest::LaneError,
    },
    #[error("{system}'s read-back does not match what it accepted: {mismatches}")]
    ReadbackMismatch { system: String, mismatches: String },
    #[error(
        "--ravel-bearer-token-env names `{var}`, which is unset, empty or not UTF-8; export the \
         token under that name (the token is never taken from argv)"
    )]
    MissingBearerToken { var: String },
    #[error(
        "--ravel-bearer-token-env needs --ravel-endpoint: there is no portable Ravel row to authenticate"
    )]
    BearerWithoutEndpoint,
    #[error(transparent)]
    Anchor(#[from] AnchorError),
    #[error("loading the workload manifest failed: {0}")]
    Workload(String),
    #[error("generating the stream failed: {0}")]
    Generate(String),
    #[error("replaying into Ravel failed: {0}")]
    Replay(String),
    #[error("replaying into {system} over Remote Write failed: {source}")]
    Http {
        system: String,
        source: ravel_bench::metrics_ingest::LaneError,
    },
    #[error("{system}: {source}")]
    StreamBand {
        system: String,
        source: StreamBandViolation,
    },
    #[error(
        "{system} replayed a regeneration whose digest {got} differs from the Ravel replay's \
         {expected}, so the rows did not see the same stream"
    )]
    StreamDiverged {
        system: String,
        expected: String,
        got: String,
    },
    #[error("{system}: {source}")]
    AccountingMismatch {
        system: String,
        source: Box<RowAccountingViolation>,
    },
    #[error(
        "the {system} replay accepted zero samples, so it measured nothing; a stream that \
         ingests nothing is a failed run, not an empty result"
    )]
    NothingIngested { system: String },
    #[error("the post-ingest read-your-write query phase failed: {0}")]
    Query(String),
}

impl RunError {
    /// The ADR-0927 band a row check belongs to, `None` for a failure that
    /// is not a numbered band.
    fn band(&self) -> Option<u32> {
        match self {
            RunError::StreamBand { .. } | RunError::AccountingMismatch { .. } => Some(4),
            RunError::Band6(_) | RunError::ScalarExpectedDiverged { .. } => Some(6),
            _ => None,
        }
    }

    /// The row a band 6 across rows failure names: the second of the two
    /// rows that disagree.
    fn implicated_system(&self) -> Option<&str> {
        match self {
            RunError::ScalarExpectedDiverged { system, .. } => Some(system),
            RunError::Band6(Band6Violation::ExpectedDiverged { second_system, .. }) => {
                Some(second_system)
            }
            RunError::Band6(Band6Violation::AcceptedDiffers { system, .. }) => Some(system),
            _ => None,
        }
    }
}

/// Steps to generate: the flag if set, else the profile's full
/// `samples_per_series`.
fn resolve_steps(args: &Args, workload: &WorkloadFile) -> Result<u64, RunError> {
    if let Some(steps) = args.steps {
        return Ok(steps);
    }
    workload
        .profile(&args.profile)
        .map(|p| p.samples_per_series)
        .ok_or_else(|| {
            RunError::Generate(format!("manifest declares no profile `{}`", args.profile))
        })
}

/// The first step's timestamp, from the required flag, checked against the
/// profile's grid and the nanosecond range. No clock is read.
/// A zero interval is refused first, so a missing anchor never names a
/// "0 ms" grid (`tests::a_zero_interval_exits_65_before_a_missing_anchor`).
fn resolve_anchor(args: &Args, scrape_interval_secs: u64, steps: u64) -> Result<i64, RunError> {
    let Some(anchor_ms) = args.anchor_ms else {
        if scrape_interval_secs == 0 {
            return Err(RunError::Anchor(AnchorError::ZeroInterval));
        }
        return Err(RunError::MissingAnchor {
            grid: format!(
                " ({} ms for profile `{}`)",
                scrape_interval_secs.saturating_mul(1_000),
                args.profile
            ),
        });
    };
    check_anchor(anchor_ms, scrape_interval_secs, steps)?;
    Ok(anchor_ms)
}

/// The portable Ravel row's tenant: required with `--ravel-endpoint`, equal
/// to `--tenant` when there is an in-process row to compare it with, and
/// refused without the endpoint.
fn resolve_ravel_tenant(args: &Args) -> Result<Option<String>, RunError> {
    if args.ravel_endpoint.is_none() {
        if args.ravel_tenant.is_some() {
            return Err(RunError::TenantWithoutEndpoint);
        }
        return Ok(None);
    }
    let Some(ravel_tenant) = &args.ravel_tenant else {
        return Err(RunError::MissingRavelTenant);
    };
    if !args.no_in_process_row && *ravel_tenant != args.tenant {
        return Err(RunError::TenantMismatch {
            tenant: args.tenant.clone(),
            ravel_tenant: ravel_tenant.clone(),
        });
    }
    Ok(Some(ravel_tenant.clone()))
}

/// Each supplied endpoint's query URL, refused when one is missing, and each
/// query URL refused when its endpoint is.
fn resolve_query_urls(args: &Args) -> Result<(), RunError> {
    for (flag, endpoint, query_url) in [
        ("ravel", &args.ravel_endpoint, &args.ravel_query_url),
        (
            "prometheus",
            &args.prometheus_endpoint,
            &args.prometheus_query_url,
        ),
        (
            "victoriametrics",
            &args.victoriametrics_endpoint,
            &args.victoriametrics_query_url,
        ),
        ("mimir", &args.mimir_endpoint, &args.mimir_query_url),
    ] {
        if endpoint.is_some() && query_url.is_none() {
            return Err(RunError::MissingQueryUrl { flag });
        }
        if endpoint.is_none() && query_url.is_some() {
            return Err(RunError::QueryUrlWithoutEndpoint { flag });
        }
    }
    Ok(())
}

/// `--no-in-process-row` with no portable endpoint is refused: it would
/// replay into no row at all
/// (`tests::skipping_the_in_process_row_needs_a_portable_endpoint`).
fn resolve_rows(args: &Args) -> Result<(), RunError> {
    let any_portable = [
        &args.ravel_endpoint,
        &args.prometheus_endpoint,
        &args.victoriametrics_endpoint,
        &args.mimir_endpoint,
    ]
    .iter()
    .any(|e| e.is_some());
    if args.no_in_process_row && !any_portable {
        return Err(RunError::NoRows);
    }
    Ok(())
}

/// Refuse a zero `--http-shards` or `--http-batch-size` before anything is
/// generated.
fn resolve_http_settings(args: &Args) -> Result<(), RunError> {
    if args.http_shards == 0 {
        return Err(RunError::ZeroHttpSetting {
            flag: "http-shards",
            what: "shards",
        });
    }
    if args.http_batch_size == 0 {
        return Err(RunError::ZeroHttpSetting {
            flag: "http-batch-size",
            what: "points per request",
        });
    }
    Ok(())
}

/// The process exit code for a failed run.
fn exit_code(err: &RunError) -> i32 {
    match err {
        RunError::Anchor(AnchorError::ZeroInterval) => EXIT_DATAERR,
        RunError::MissingAnchor { .. }
        | RunError::Anchor(_)
        | RunError::MissingBearerToken { .. }
        | RunError::BearerWithoutEndpoint
        | RunError::MissingRavelTenant
        | RunError::TenantMismatch { .. }
        | RunError::TenantWithoutEndpoint
        | RunError::MissingQueryUrl { .. }
        | RunError::QueryUrlWithoutEndpoint { .. }
        | RunError::NoRows
        | RunError::ZeroHttpSetting { .. } => EXIT_USAGE,
        _ => 1,
    }
}

/// The portable Ravel row's bearer token, read from the environment
/// variable `--ravel-bearer-token-env` names. Refused before anything is
/// generated when the variable is unset or empty, or when the flag is given
/// without `--ravel-endpoint`.
fn resolve_bearer_token(args: &Args) -> Result<Option<String>, RunError> {
    let Some(var) = &args.ravel_bearer_token_env else {
        return Ok(None);
    };
    if args.ravel_endpoint.is_none() {
        return Err(RunError::BearerWithoutEndpoint);
    }
    match std::env::var(var) {
        Ok(token) if !token.is_empty() => Ok(Some(token)),
        _ => Err(RunError::MissingBearerToken { var: var.clone() }),
    }
}

/// Close a drained stream and check one row against it: the classification
/// identity of band 4, then the row's offering identity and the split of its
/// offered points, then band 6 for each class the row stores.
fn close_row(
    label: &str,
    stream: GeneratedStream<'_>,
    row: &SystemIngestResult,
) -> Result<(GenerationReport, StreamCounts), RunError> {
    let (report, counts) = stream
        .finish()
        .map_err(|e| RunError::Generate(e.to_string()))?;
    check_stream_band(&counts, &report).map_err(|source| RunError::StreamBand {
        system: label.to_string(),
        source,
    })?;
    check_row_accounting(&row.ingest, &counts).map_err(|source| RunError::AccountingMismatch {
        system: label.to_string(),
        source,
    })?;
    check_row_band6(row)?;
    Ok((report, counts))
}

/// Close a portable row: [`close_row`], then its regeneration against the
/// run's reference stream, or, for the first row to close, a non-empty
/// accepted set. Returns the row's generator report.
fn close_portable_row(
    system: &str,
    stream: GeneratedStream<'_>,
    row: &SystemIngestResult,
    reference: Option<&GenerationReport>,
) -> Result<GenerationReport, RunError> {
    let (report, _) = close_row(system, stream, row)?;
    match reference {
        Some(expected) if report.digest != expected.digest => Err(RunError::StreamDiverged {
            system: system.to_string(),
            expected: expected.digest.clone(),
            got: report.digest,
        }),
        None if row.ingest.accepted_samples == 0 => Err(RunError::NothingIngested {
            system: system.to_string(),
        }),
        _ => Ok(report),
    }
}

/// Band 6 across rows: every portable row carries the same expected digest
/// for every class, and, when the run has an in-process row, the same scalar
/// expected digest as that row's `in_process_scalar`. Without an in-process
/// row only that second comparison is skipped
/// (`tests::portable_rows_still_cross_check_without_the_in_process_row`).
fn check_band6_across_rows(
    in_process_scalar: Option<&str>,
    portable: &[&SystemIngestResult],
) -> Result<(), RunError> {
    for row in portable {
        check_scalar_expected(
            &row.system,
            row.ingest.expected_digests.get(PayloadClass::Scalar),
            in_process_scalar,
        )?;
    }
    check_expected_digests_agree(portable, &PayloadClass::ALL)?;
    Ok(())
}

/// A portable row's expected scalar digest `got` equals the in-process
/// row's (`tests::a_portable_scalar_digest_must_equal_the_in_process_one`).
fn check_scalar_expected(
    label: &str,
    got: &str,
    in_process_scalar: Option<&str>,
) -> Result<(), RunError> {
    match in_process_scalar {
        Some(expected) if got != expected => Err(RunError::ScalarExpectedDiverged {
            system: label.to_string(),
            expected: expected.to_string(),
            got: got.to_string(),
        }),
        _ => Ok(()),
    }
}

/// A failed run: the error, and, when the failure was a read-back or a
/// stall, the report of every row, so their ingest figures are still printed
/// (`metricsbench_ingest_smoke::a_failed_readback_still_prints_the_report`,
/// `metricsbench_ingest_smoke::a_failed_readback_does_not_stop_the_later_rows`).
struct RunFailure {
    error: RunError,
    report: Option<MetricsIngestReport>,
}

impl From<RunError> for RunFailure {
    fn from(error: RunError) -> Self {
        RunFailure {
            error,
            report: None,
        }
    }
}

/// What every report of one run is built from besides its rows.
struct ReportContext<'a> {
    workload: &'a WorkloadFile,
    profile: &'a Profile,
    steps: u64,
    provenance: RunProvenance,
}

impl ReportContext<'_> {
    /// The profile record, built from the reference stream's generator
    /// report.
    fn profile_record(
        &self,
        reference: Option<&GenerationReport>,
    ) -> Result<ProfileRecord, RunError> {
        // A run with no failed row has a reference: `resolve_rows` refused a
        // run with neither an in-process nor a portable row before anything
        // was generated, and every row that closed set or matched it.
        let gen_report = reference
            .ok_or_else(|| RunError::Generate("no row replayed the stream".to_string()))?;

        // ADR-0927 decision 11: the profile's own pre-registered figures, plus the
        // generator's exact counts for the figures that are not directly declared
        // (total series created, logical input bytes) -- never derived from the
        // offered sample count.
        build_profile_record(
            self.workload,
            self.profile,
            self.steps,
            gen_report.bytes,
            gen_report,
            self.provenance.clone(),
        )
        .map_err(|e| RunError::Generate(e.to_string()))
    }
}

/// The report over `rows`, with whichever of the profile record, the query
/// phase and the substrate the run produced.
fn assemble(
    rows: Vec<SystemIngestResult>,
    profile: Option<ProfileRecord>,
    query: Option<SystemQueryResult>,
    substrate: Option<Substrate>,
) -> MetricsIngestReport {
    let mut report = MetricsIngestReport::new(rows);
    if let Some(profile) = profile {
        report = report.with_profile(profile);
    }
    if let Some(query) = query {
        report = report.with_query(query);
    }
    if let Some(substrate) = substrate {
        report = report.with_substrate(substrate);
    }
    report
}

async fn run(args: &Args) -> Result<MetricsIngestReport, RunFailure> {
    let workload = load_workload(&args.workload).map_err(|e| RunError::Workload(e.to_string()))?;
    let steps = resolve_steps(args, &workload)?;
    let profile = workload.profile(&args.profile).ok_or_else(|| {
        RunError::Generate(format!("manifest declares no profile `{}`", args.profile))
    })?;
    let anchor_ms = resolve_anchor(args, profile.scrape_interval_secs, steps)?;
    let ravel_bearer_token = resolve_bearer_token(args)?;
    let ravel_tenant = resolve_ravel_tenant(args)?;
    resolve_query_urls(args)?;
    resolve_rows(args)?;
    resolve_http_settings(args)?;
    // `load_workload` read the same file; a second read for the digest keeps
    // the loader's signature unchanged.
    let manifest_bytes = std::fs::read(&args.workload).map_err(|e| {
        RunError::Workload(format!(
            "reading {} for its digest: {e}",
            args.workload.display()
        ))
    })?;
    let context = ReportContext {
        workload: &workload,
        profile,
        steps,
        provenance: RunProvenance {
            workload_digest: workload_digest(&manifest_bytes),
            anchor_ms,
        },
    };
    let stream_from_anchor = || -> Result<GeneratedStream<'_>, RunError> {
        let generator = Generator::new(&workload, &args.profile, anchor_ms)
            .map_err(|e| RunError::Generate(e.to_string()))?;
        Ok(GeneratedStream::new(generator, steps))
    };

    let mut rows = Vec::new();
    let mut query = None;
    let mut substrate = None;
    // The generator report every row's regeneration must match: the
    // in-process replay's, else the first portable row's.
    let mut reference: Option<GenerationReport> = None;
    if !args.no_in_process_row {
        // The in-process Ravel path: strict, durable-on-ack, commit tokens.
        let (store, store_metrics) = store_and_metrics_from_env(args.store);
        let backend_bills_requests = backend_bills_requests(args.store);
        substrate = Some(Substrate {
            store_backend: args.store.to_string(),
            endpoint_host: endpoint_host_from_env(args.store),
            backend_bills_requests,
        });
        let ravel_cfg = RavelReplayConfig {
            store,
            store_metrics,
            backend_bills_requests,
            shards: args.shards,
            batch_size: args.batch_size,
            ack_timeout_secs: args.ack_timeout_secs,
            tenant: TenantId::new(args.tenant.clone()),
        };
        let mut ravel_stream = stream_from_anchor()?;
        let ravel = replay_into_ravel(&ravel_cfg, &mut ravel_stream)
            .await
            .map_err(|e| RunError::Replay(e.to_string()))?;

        // Band 4: the stream closes against the generator and the Ravel
        // accounting closes over every line.
        let (gen_report, _) = close_row("ravel (in-process)", ravel_stream, &ravel.result)?;
        if ravel.result.ingest.accepted_samples == 0 {
            return Err(RunFailure::from(RunError::NothingIngested {
                system: IN_PROCESS_SYSTEM.to_string(),
            }));
        }
        reference = Some(gen_report);

        // Post-ingest read-your-write query phase (ADR-0927 decision 3): pass
        // the strict writes' commit tokens as `min_commit_token` and evaluate
        // an instant query at the replay's newest sample, so the read
        // resolves the exact committed segments deterministically. Recorded
        // as its own phase, separate from ingest (decision 9). Runs while
        // `ravel` is still whole, before its result row is moved into `rows`.
        let query_expr = ravel
            .first_metric
            .clone()
            .ok_or_else(|| RunError::NothingIngested {
                system: IN_PROCESS_SYSTEM.to_string(),
            })?;
        query = Some(
            query_after_replay(&ravel, &query_expr)
                .await
                .map_err(|e| RunError::Query(e.to_string()))?,
        );
        rows.push(ravel.result);
    }
    let in_process_scalar = rows
        .iter()
        .find(|r| r.system == IN_PROCESS_SYSTEM)
        .map(|r| {
            r.ingest
                .expected_digests
                .get(PayloadClass::Scalar)
                .to_string()
        });

    // Portable rows, only those a caller supplied an endpoint for. Their
    // `system` values are the names the query runner joins on.
    let mut portable = Vec::new();
    let mut stalled = Vec::new();
    let mut failed_replays = Vec::new();
    // The first row whose read-back failed or that stalled: every row is
    // still replayed, and the run exits with this one's code. Once it is
    // set, a later row's failure is recorded rather than returned.
    let mut first_failure: Option<RunError> = None;
    for (receiver, endpoint, query_url, bearer_token, tenant) in [
        (
            Receiver::Ravel,
            &args.ravel_endpoint,
            &args.ravel_query_url,
            ravel_bearer_token.clone(),
            ravel_tenant.clone(),
        ),
        (
            Receiver::Prometheus,
            &args.prometheus_endpoint,
            &args.prometheus_query_url,
            None,
            None,
        ),
        (
            Receiver::VictoriaMetrics,
            &args.victoriametrics_endpoint,
            &args.victoriametrics_query_url,
            None,
            None,
        ),
        (
            Receiver::Mimir,
            &args.mimir_endpoint,
            &args.mimir_query_url,
            None,
            None,
        ),
    ] {
        let (Some(endpoint), Some(query_url)) = (endpoint, query_url) else {
            continue;
        };
        let system = receiver.system();
        let http_cfg = HttpReplayConfig {
            receiver,
            endpoint: endpoint.clone(),
            bearer_token: bearer_token.clone(),
            tenant,
            batch_size: args.http_batch_size,
            max_retries: args.http_max_retries,
            timeout_secs: args.http_timeout_secs,
            shards: args.http_shards,
            stall_ceiling: args.max_ingest_secs.map(|max_ingest_secs| StallCeiling {
                max_ingest_secs,
                // The generator writes one line per active series per step
                // less anomalies; once a row has run, its report is exact.
                stream_points: reference
                    .as_ref()
                    .map_or(profile.active_series.saturating_mul(steps), |report| {
                        report.emitted_samples
                    }),
                grace: STALL_GRACE,
                recheck: STALL_RECHECK,
            }),
        };
        let mut stream = stream_from_anchor()?;
        let outcome = match replay_over_http_with_plan(&http_cfg, &mut stream).await {
            Ok(outcome) => outcome,
            Err(source) => {
                let error = RunError::Http {
                    system: system.to_string(),
                    source,
                };
                let RunError::Http {
                    source:
                        LaneError::IngestStalled {
                            projected_secs,
                            ceiling_secs,
                            offered_points,
                            stream_points,
                            ..
                        },
                    ..
                } = &error
                else {
                    if first_failure.is_none() {
                        return Err(RunFailure::from(error));
                    }
                    eprintln!("metricsbench_ingest: {error}; replaying the next row");
                    failed_replays.push(FailedReplay {
                        system: system.to_string(),
                        endpoint: endpoint.clone(),
                        error: error.to_string(),
                    });
                    continue;
                };
                // A stalled row stops only itself (decision 7); the rows
                // after it are still replayed.
                stalled.push(StalledRow {
                    system: system.to_string(),
                    endpoint: endpoint.clone(),
                    projected_secs: *projected_secs,
                    ceiling_secs: *ceiling_secs,
                    offered_points: *offered_points,
                    stream_points: *stream_points,
                    error: error.to_string(),
                });
                eprintln!("metricsbench_ingest: {error}; replaying the next row");
                first_failure.get_or_insert(error);
                continue;
            }
        };
        let mut row = outcome.result;
        match close_portable_row(system, stream, &row, reference.as_ref()) {
            Ok(report) => {
                reference.get_or_insert(report);
            }
            Err(error) if first_failure.is_none() => return Err(RunFailure::from(error)),
            Err(error) => {
                // A row that failed to close is not read back.
                eprintln!("metricsbench_ingest: {error}; replaying the next row");
                row.row_failure = Some(RowFailure {
                    band: error.band(),
                    error: error.to_string(),
                });
                portable.push(row);
                continue;
            }
        }
        let readback_cfg = ReadbackConfig {
            query_url: query_url.clone(),
            bearer_token,
            timeout_secs: args.readback_timeout_secs,
            settle_secs: args.readback_settle_secs,
        };
        let failed = match readback_over_http(&readback_cfg, &row, &outcome.plan).await {
            Ok(readback) => {
                let failed = (!readback.matches_expected).then(|| RunError::ReadbackMismatch {
                    system: system.to_string(),
                    mismatches: readback.mismatches.join("; "),
                });
                row.readback = Some(readback);
                failed
            }
            Err(source) => {
                row.readback_error = Some(source.to_string());
                Some(RunError::Readback {
                    system: system.to_string(),
                    source,
                })
            }
        };
        portable.push(row);
        if let Some(error) = failed {
            // The row stays in the report marked failed, and the rows after
            // it are still replayed; the run exits with the first failure.
            eprintln!("metricsbench_ingest: {error}; replaying the next row");
            first_failure.get_or_insert(error);
        }
    }
    // A row that failed to close is left out: its failure is already
    // recorded, and its stream need not be the reference's.
    let closed: Vec<&SystemIngestResult> = portable
        .iter()
        .filter(|r| r.row_failure.is_none())
        .collect();
    if let Err(error) = check_band6_across_rows(in_process_scalar.as_deref(), &closed) {
        if first_failure.is_none() {
            return Err(RunFailure::from(error));
        }
        record_across_rows_failure(&mut portable, &error);
    }
    rows.extend(portable);
    let profile = match context.profile_record(reference.as_ref()) {
        Ok(profile) => Some(profile),
        Err(error) if first_failure.is_none() => return Err(RunFailure::from(error)),
        Err(error) => {
            eprintln!("metricsbench_ingest: {error}; printing the report without a profile");
            None
        }
    };
    let mut report = assemble(rows, profile, query, substrate);
    report.stalled = stalled;
    report.failed_replays = failed_replays;
    match first_failure {
        None => Ok(report),
        Some(error) => Err(RunFailure {
            error,
            report: Some(report),
        }),
    }
}

/// Records a band 6 across rows failure that follows an earlier row's
/// failure as `row_failure` on the row it names, so the report is still
/// printed
/// (`tests::band6_across_rows_after_a_failure_is_recorded_on_the_named_row`).
fn record_across_rows_failure(portable: &mut [SystemIngestResult], error: &RunError) {
    eprintln!("metricsbench_ingest: {error}");
    let named = error.implicated_system();
    if let Some(row) = portable
        .iter_mut()
        .find(|r| Some(r.system.as_str()) == named)
    {
        row.row_failure.get_or_insert(RowFailure {
            band: error.band(),
            error: error.to_string(),
        });
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match run(&args).await {
        Ok(report) => {
            let json = serde_json::to_string_pretty(&report).expect("serialize report");
            println!("{json}");
            for row in &report.systems {
                eprintln!(
                    "{:>16}  endpoint={}  ack={:<14}  samples accepted={} rejected={} \
                     dropped={}  histograms accepted={} rejected={} dropped={}  p99={:.3}ms",
                    row.system,
                    row.endpoint,
                    row.ack_semantics.label(),
                    row.ingest.accepted_samples,
                    row.ingest.rejected_samples,
                    row.ingest.dropped_samples,
                    row.ingest.accepted_histograms,
                    row.ingest.rejected_histograms,
                    row.ingest.dropped_histograms,
                    row.ingest.ack_latency_ms.p99,
                );
            }
            for q in &report.queries {
                eprintln!(
                    "{:>16}  query={:<20} matched_series={} tokens={} eval_ts_ms={}",
                    q.system,
                    q.query,
                    q.matched_series,
                    q.min_commit_tokens.len(),
                    q.eval_ts_ms,
                );
            }
        }
        Err(RunFailure { error, report }) => {
            if let Some(report) = report {
                let json = serde_json::to_string_pretty(&report).expect("serialize report");
                println!("{json}");
            }
            eprintln!("metricsbench_ingest: {error}");
            std::process::exit(exit_code(&error));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ravel_bench::metrics_ingest::{ClassDigests, IngestPhase, LatencyReport};

    use super::*;

    /// A portable row whose expected (and accepted) digest is `scalar` for
    /// the scalar class and fixed for the other two.
    fn portable_row(system: &str, scalar: &str) -> SystemIngestResult {
        let digests = ClassDigests {
            scalar: scalar.to_string(),
            stale_marker: "d2".to_string(),
            native_histogram: "d3".to_string(),
        };
        let ingest = IngestPhase {
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
            generation_secs: 0.0,
            sender_bound: false,
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
        };
        SystemIngestResult::buffered_http_row(system, "http://x/api/v1/write", "test", ingest)
    }

    /// Without an in-process row, two portable rows whose expected digests
    /// differ still fail band 6; only the comparison with an in-process
    /// scalar digest is skipped.
    #[test]
    fn band6_across_rows_after_a_failure_is_recorded_on_the_named_row() {
        let mut rows = vec![
            portable_row("prometheus", "aa"),
            portable_row("victoriametrics", "aa"),
            portable_row("mimir", "ab"),
        ];
        let refs: Vec<&SystemIngestResult> = rows.iter().collect();
        let err = check_band6_across_rows(None, &refs).expect_err("mimir's expected set differs");
        record_across_rows_failure(&mut rows, &err);
        let failure = rows[2]
            .row_failure
            .as_ref()
            .expect("mimir carries the failure");
        assert_eq!(failure.band, Some(6));
        assert_eq!(failure.error, err.to_string());
        assert!(rows[..2].iter().all(|r| r.row_failure.is_none()));
    }

    #[test]
    fn portable_rows_still_cross_check_without_the_in_process_row() {
        let prometheus = portable_row("prometheus", "aa");
        let mimir = portable_row("mimir", "aa");
        let diverged = portable_row("mimir", "ab");
        check_band6_across_rows(None, &[&prometheus, &mimir]).expect("rows agree");
        let err = check_band6_across_rows(None, &[&prometheus, &diverged])
            .expect_err("portable rows differ");
        assert!(
            matches!(
                &err,
                RunError::Band6(Band6Violation::ExpectedDiverged {
                    class: PayloadClass::Scalar,
                    first_system,
                    second_system,
                    ..
                }) if first_system == "prometheus" && second_system == "mimir"
            ),
            "{err}"
        );

        let with_in_process = check_band6_across_rows(Some("ab"), &[&prometheus, &mimir])
            .expect_err("in-process scalar differs");
        assert!(
            matches!(
                &with_in_process,
                RunError::ScalarExpectedDiverged { system, expected, got }
                    if system == "prometheus" && expected == "ab" && got == "aa"
            ),
            "{with_in_process}"
        );
        check_band6_across_rows(Some("aa"), &[&prometheus, &mimir]).expect("all agree");
    }

    #[test]
    fn skipping_the_in_process_row_needs_a_portable_endpoint() {
        let err = resolve_rows(&args(&["--no-in-process-row"])).expect_err("no rows");
        assert!(matches!(err, RunError::NoRows), "{err}");
        assert_eq!(exit_code(&err), EXIT_USAGE);
        resolve_rows(&args(&[])).expect("the in-process row alone");
        resolve_rows(&args(&[
            "--no-in-process-row",
            "--prometheus-endpoint",
            "http://p/api/v1/write",
        ]))
        .expect("a portable row");
    }

    /// With no in-process row there is no second Ravel row for the portable
    /// one's tenant to match, so `--tenant` is not compared.
    #[test]
    fn without_the_in_process_row_the_ravel_tenant_stands_alone() {
        let tenant = resolve_ravel_tenant(&args(&[
            "--no-in-process-row",
            "--ravel-endpoint",
            "http://x/api/v1/write",
            "--ravel-tenant",
            "b",
        ]))
        .expect("no in-process tenant to match");
        assert_eq!(tenant.as_deref(), Some("b"));
    }

    fn args(extra: &[&str]) -> Args {
        let mut argv = vec!["metricsbench_ingest", "--profile", "ci"];
        argv.extend_from_slice(extra);
        Args::parse_from(argv)
    }

    #[test]
    fn a_zero_interval_exits_65_before_a_missing_anchor() {
        let err = resolve_anchor(&args(&[]), 0, 1).expect_err("zero interval");
        assert!(matches!(err, RunError::Anchor(AnchorError::ZeroInterval)));
        assert_eq!(exit_code(&err), EXIT_DATAERR);
        assert_eq!(EXIT_DATAERR, 65);
        let with_anchor = resolve_anchor(&args(&["--anchor-ms", "0"]), 0, 1).expect_err("zero");
        assert_eq!(exit_code(&with_anchor), 65);
        let missing = resolve_anchor(&args(&[]), 10, 1).expect_err("missing anchor");
        assert_eq!(exit_code(&missing), 64);
        let text = missing.to_string();
        assert!(text.contains("(10000 ms for profile `ci`)"), "{text}");
        assert!(!text.contains("(0 ms"), "{text}");
    }

    #[test]
    fn a_portable_scalar_digest_must_equal_the_in_process_one() {
        check_scalar_expected("prometheus", "aa", Some("aa")).expect("equal");
        check_scalar_expected("prometheus", "aa", None).expect("no in-process row");
        let err = check_scalar_expected("prometheus", "ab", Some("aa")).expect_err("differs");
        assert!(
            matches!(
                &err,
                RunError::ScalarExpectedDiverged { system, expected, got }
                    if system == "prometheus" && expected == "aa" && got == "ab"
            ),
            "{err}"
        );
    }

    #[test]
    fn the_ravel_tenant_must_be_given_and_equal_the_tenant() {
        let none = resolve_ravel_tenant(&args(&["--ravel-endpoint", "http://x/api/v1/write"]))
            .expect_err("missing");
        assert!(matches!(none, RunError::MissingRavelTenant));
        assert_eq!(exit_code(&none), 64);
        let differs = resolve_ravel_tenant(&args(&[
            "--ravel-endpoint",
            "http://x/api/v1/write",
            "--tenant",
            "a",
            "--ravel-tenant",
            "b",
        ]))
        .expect_err("differs");
        assert!(matches!(differs, RunError::TenantMismatch { .. }));
        assert_eq!(exit_code(&differs), 64);
        let same = resolve_ravel_tenant(&args(&[
            "--ravel-endpoint",
            "http://x/api/v1/write",
            "--tenant",
            "a",
            "--ravel-tenant",
            "a",
        ]))
        .expect("same tenant");
        assert_eq!(same.as_deref(), Some("a"));
    }

    #[test]
    fn every_endpoint_needs_its_query_url() {
        let err = resolve_query_urls(&args(&["--mimir-endpoint", "http://m/api/v1/push"]))
            .expect_err("missing query url");
        assert!(matches!(err, RunError::MissingQueryUrl { flag: "mimir" }));
        assert_eq!(exit_code(&err), 64);
        resolve_query_urls(&args(&[
            "--mimir-endpoint",
            "http://m/api/v1/push",
            "--mimir-query-url",
            "http://m/prometheus",
        ]))
        .expect("query url given");
    }

    #[test]
    fn a_query_url_without_its_endpoint_is_refused() {
        for flag in ["ravel", "prometheus", "victoriametrics", "mimir"] {
            let query_flag = format!("--{flag}-query-url");
            let err = resolve_query_urls(&args(&[query_flag.as_str(), "http://q"]))
                .expect_err("query url without endpoint");
            assert!(
                matches!(err, RunError::QueryUrlWithoutEndpoint { flag: f } if f == flag),
                "{err}"
            );
            assert_eq!(exit_code(&err), 64);
        }
    }

    #[test]
    fn a_ravel_tenant_without_the_ravel_endpoint_is_refused() {
        let err = resolve_ravel_tenant(&args(&["--ravel-tenant", "metricsbench"]))
            .expect_err("tenant without endpoint");
        assert!(matches!(err, RunError::TenantWithoutEndpoint), "{err}");
        assert_eq!(exit_code(&err), 64);
        assert_eq!(resolve_ravel_tenant(&args(&[])).expect("neither"), None);
    }

    /// The HTTP rows default to Prometheus' 50 shards and 2000 points per
    /// request (ADR-0927, 2026-10-28 amendment, decision 1), with no stall
    /// ceiling unless one is given.
    #[test]
    fn http_rows_default_to_50_shards_of_2000_points() {
        let defaults = args(&[]);
        assert_eq!(defaults.http_shards, 50);
        assert_eq!(defaults.http_batch_size, 2000);
        assert_eq!(defaults.max_ingest_secs, None);
        let given = args(&["--max-ingest-secs", "600"]);
        assert_eq!(given.max_ingest_secs, Some(600));
    }

    /// Zero shards or zero points per request is a usage error.
    #[test]
    fn zero_shards_or_batch_size_exits_64() {
        resolve_http_settings(&args(&[])).expect("defaults");
        for (flag, name) in [
            ("--http-shards", "http-shards"),
            ("--http-batch-size", "http-batch-size"),
        ] {
            let err = resolve_http_settings(&args(&[flag, "0"])).expect_err("zero");
            assert!(
                matches!(err, RunError::ZeroHttpSetting { flag: f, .. } if f == name),
                "{err}"
            );
            assert_eq!(exit_code(&err), 64);
        }
    }

    /// A stalled row exits 1, the code a band violation exits with.
    #[test]
    fn a_stalled_row_exits_like_a_band_violation() {
        let err = RunError::Http {
            system: "prometheus".to_string(),
            source: ravel_bench::metrics_ingest::LaneError::IngestStalled {
                system: "prometheus".to_string(),
                projected_secs: 1_200.0,
                ceiling_secs: 600,
                offered_points: 50,
                stream_points: 1_000,
            },
        };
        assert_eq!(exit_code(&err), 1);
        let text = err.to_string();
        assert!(
            text.contains("projected 1200 s")
                && text.contains("600 s ceiling")
                && text.contains("50 of 1000 points"),
            "{text}"
        );
    }
}
