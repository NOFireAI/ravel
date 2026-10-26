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
    AnchorError, Band6Violation, GeneratedStream, HttpReplayConfig, MetricsIngestReport,
    PayloadClass, RavelReplayConfig, ReadbackConfig, Receiver, RowAccountingViolation,
    RunProvenance, StreamBandViolation, StreamCounts, Substrate, SystemIngestResult,
    build_profile_record, check_anchor, check_expected_digests_agree, check_row_accounting,
    check_row_band6, check_stream_band, query_after_replay, readback_over_http, replay_into_ravel,
    replay_over_http_with_plan, workload_digest,
};
use ravel_bench::metrics_workload::{WorkloadFile, load_workload};
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
    /// The object store backing the in-process Ravel path.
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
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
    #[arg(long, default_value = "metricsbench")]
    tenant: String,
    /// `ravel-server` `/api/v1/write` endpoint, the portable Ravel row
    /// (`system: ravel`, `endpoint: <url>`). Omitted, only the in-process
    /// row is written.
    #[arg(long)]
    ravel_endpoint: Option<String>,
    /// The tenant `ravel-server` resolves the portable Ravel row's requests
    /// to (from its bearer token, or its dev default), recorded as that row's
    /// `tenant`. Required with `--ravel-endpoint`, and it must equal
    /// `--tenant`, so the two Ravel rows describe one tenant's data.
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
    /// Logical points per RW1.0 request to an HTTP receiver.
    #[arg(long, default_value_t = 500)]
    http_batch_size: usize,
    /// Retries on a 429/5xx before a comparator batch is dropped.
    #[arg(long, default_value_t = 3)]
    http_max_retries: u32,
    /// Per-request timeout for a comparator POST, bounding the complete
    /// operation including the body read (ADR-0927 decision 6).
    #[arg(long, default_value_t = 30)]
    http_timeout_secs: u64,
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
        "the Ravel replay accepted zero samples, so it measured nothing; a stream that ingests \
         nothing is a failed run, not an empty result"
    )]
    NothingIngested,
    #[error("the post-ingest read-your-write query phase failed: {0}")]
    Query(String),
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

/// The portable Ravel row's tenant: required with `--ravel-endpoint` and
/// equal to `--tenant`.
fn resolve_ravel_tenant(args: &Args) -> Result<Option<String>, RunError> {
    if args.ravel_endpoint.is_none() {
        return Ok(None);
    }
    let Some(ravel_tenant) = &args.ravel_tenant else {
        return Err(RunError::MissingRavelTenant);
    };
    if *ravel_tenant != args.tenant {
        return Err(RunError::TenantMismatch {
            tenant: args.tenant.clone(),
            ravel_tenant: ravel_tenant.clone(),
        });
    }
    Ok(Some(ravel_tenant.clone()))
}

/// Each supplied endpoint's query URL, refused when one is missing.
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
        | RunError::MissingQueryUrl { .. } => EXIT_USAGE,
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
/// offered points, then band 6 for each class the row stores. A portable row
/// passes the in-process row's scalar expected digest as `in_process_scalar`
/// and must carry the same one.
fn close_row(
    label: &str,
    stream: GeneratedStream<'_>,
    row: &SystemIngestResult,
    in_process_scalar: Option<&str>,
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
    check_scalar_expected(
        label,
        row.ingest.expected_digests.get(PayloadClass::Scalar),
        in_process_scalar,
    )?;
    Ok((report, counts))
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

async fn run(args: &Args) -> Result<MetricsIngestReport, RunError> {
    let workload = load_workload(&args.workload).map_err(|e| RunError::Workload(e.to_string()))?;
    let steps = resolve_steps(args, &workload)?;
    let profile = workload.profile(&args.profile).ok_or_else(|| {
        RunError::Generate(format!("manifest declares no profile `{}`", args.profile))
    })?;
    let anchor_ms = resolve_anchor(args, profile.scrape_interval_secs, steps)?;
    let ravel_bearer_token = resolve_bearer_token(args)?;
    let ravel_tenant = resolve_ravel_tenant(args)?;
    resolve_query_urls(args)?;
    // `load_workload` read the same file; a second read for the digest keeps
    // the loader's signature unchanged.
    let manifest_bytes = std::fs::read(&args.workload).map_err(|e| {
        RunError::Workload(format!(
            "reading {} for its digest: {e}",
            args.workload.display()
        ))
    })?;
    let provenance = RunProvenance {
        workload_digest: workload_digest(&manifest_bytes),
        anchor_ms,
    };
    let stream_from_anchor = || -> Result<GeneratedStream<'_>, RunError> {
        let generator = Generator::new(&workload, &args.profile, anchor_ms)
            .map_err(|e| RunError::Generate(e.to_string()))?;
        Ok(GeneratedStream::new(generator, steps))
    };

    // The in-process Ravel path: strict, durable-on-ack, commit tokens.
    let (store, store_metrics) = store_and_metrics_from_env(args.store);
    let backend_bills_requests = backend_bills_requests(args.store);
    let substrate = Substrate {
        store_backend: args.store.to_string(),
        endpoint_host: endpoint_host_from_env(args.store),
        backend_bills_requests,
    };
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
    let (gen_report, _) = close_row("ravel (in-process)", ravel_stream, &ravel.result, None)?;
    let in_process_scalar = ravel
        .result
        .ingest
        .expected_digests
        .get(PayloadClass::Scalar)
        .to_string();
    if ravel.result.ingest.accepted_samples == 0 {
        return Err(RunError::NothingIngested);
    }

    // ADR-0927 decision 11: the profile's own pre-registered figures, plus the
    // generator's exact counts for the figures that are not directly declared
    // (total series created, logical input bytes) -- never derived from the
    // offered sample count.
    let profile_record = build_profile_record(
        &workload,
        profile,
        steps,
        gen_report.bytes,
        &gen_report,
        provenance,
    )
    .map_err(|e| RunError::Generate(e.to_string()))?;

    // Post-ingest read-your-write query phase (ADR-0927 decision 3): pass the
    // strict writes' commit tokens as `min_commit_token` and evaluate an instant
    // query at the replay's newest sample, so the read resolves the exact
    // committed segments deterministically. Recorded as its own phase, separate
    // from ingest (decision 9). Runs while `ravel` is still whole, before its
    // result row is moved into `rows`.
    let query_expr = ravel
        .first_metric
        .clone()
        .ok_or(RunError::NothingIngested)?;
    let query = query_after_replay(&ravel, &query_expr)
        .await
        .map_err(|e| RunError::Query(e.to_string()))?;

    let mut rows = vec![ravel.result];

    // Portable rows, only those a caller supplied an endpoint for. Their
    // `system` values are the names the query runner joins on.
    let mut portable = Vec::new();
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
        };
        let mut stream = stream_from_anchor()?;
        let outcome = replay_over_http_with_plan(&http_cfg, &mut stream)
            .await
            .map_err(|source| RunError::Http {
                system: system.to_string(),
                source,
            })?;
        let mut row = outcome.result;
        let (report, _) = close_row(system, stream, &row, Some(&in_process_scalar))?;
        if report.digest != gen_report.digest {
            return Err(RunError::StreamDiverged {
                system: system.to_string(),
                expected: gen_report.digest.clone(),
                got: report.digest,
            });
        }
        let readback_cfg = ReadbackConfig {
            query_url: query_url.clone(),
            bearer_token,
            timeout_secs: args.http_timeout_secs,
        };
        let readback = readback_over_http(&readback_cfg, &row, &outcome.plan)
            .await
            .map_err(|source| RunError::Readback {
                system: system.to_string(),
                source,
            })?;
        if !readback.matches_expected {
            return Err(RunError::ReadbackMismatch {
                system: system.to_string(),
                mismatches: readback.mismatches.join("; "),
            });
        }
        row.readback = Some(readback);
        portable.push(row);
    }
    let portable_refs: Vec<&SystemIngestResult> = portable.iter().collect();
    check_expected_digests_agree(&portable_refs, &PayloadClass::ALL)?;
    rows.extend(portable);

    Ok(MetricsIngestReport::new(rows)
        .with_query(query)
        .with_profile(profile_record)
        .with_substrate(substrate))
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
        Err(err) => {
            eprintln!("metricsbench_ingest: {err}");
            std::process::exit(exit_code(&err));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
