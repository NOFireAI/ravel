//! MetricsBench M5 (ADR-0927 decisions 2 and 3, issue #937): the Remote Write
//! 1.0 ingest lane, reported with per-system acknowledgement semantics.
//!
//! The substance here is acknowledgement semantics, not throughput. Ravel's
//! Remote Write surface is strict-mode only, unconditionally
//! (`services/ravel-server/src/remote_write.rs`): a 2xx means the data object
//! and its commit record are both durably stored, and the response carries
//! `x-ravel-commit-token`. Buffered mode exists but Remote Write cannot reach
//! it; it is an OTLP-only header. So a Remote Write 1.0 ack is durable-on-ack,
//! and a buffered ack (reachable only through OTLP) means something else
//! entirely: the samples were accepted into a buffer and are not yet durable.
//!
//! ADR-0927 decision 3: "A durable-on-ack latency is never placed in the same
//! column as a buffered ack without the distinction on the same row." This
//! module makes that structural rather than documentary. Every ingest row
//! carries a typed [`AckSemantics`], and [`validate`] refuses a report that
//! emits an ack-latency figure without stating what the ack it measured means
//! ([`AckSemantics::Unspecified`]). A reader comparing two ack latencies always
//! sees which ack each one measured, on the same row.
//!
//! ## Lanes never compare against each other (ADR-0927 decision 2)
//!
//! Remote Write 1.0 is the cross-engine baseline. Remote Write 2.0 and OTLP are
//! separate lanes and must never be compared against a 1.0 figure as though
//! protocol overhead were identical. A [`Lane`] travels with every row, so a
//! comparison across lanes is visible in the data rather than depending on a
//! reader's care.
//!
//! ## Remote Read does not exist in Ravel
//!
//! There is no `/api/v1/read` handler, no proto, and no ADR proposing one
//! (confirmed by grep over `services/ravel-server` and `crates/`). So there is
//! no portable read-back path: read-your-write is proven in-process by passing
//! the returned commit tokens as `min_commit_token` to the query engine
//! (ADR-0927 decision 3), never by sleeping. Sleeping would measure the flush
//! delay (default `--max-flush-delay` 2 s) and report it as query latency,
//! which is a wrong number that looks plausible.
//!
//! ## Bytes are named for what they count
//!
//! Two figures called "bytes" that count different things cannot be compared or
//! summed (ADR-0927; the repo-wide measurement discipline). Every byte figure
//! this module emits states which bytes it counts: [`STORED_BYTES`] and
//! [`PUT_BYTES`] are bytes durably stored; [`LOGICAL_INPUT_BYTES_PER_SEC`] is
//! the logical input (`ts_ns: i64` + `value: f64`, 16 bytes per sample), NOT
//! the Remote Write 1.0 wire encoding. True RW1.0 wire-bytes-as-transferred
//! would need the snappy-compressed protobuf body; `ravel-remote-write` is
//! decode-only (no encoder, no snappy compressor), so this module names that as
//! a gap rather than emitting a logical figure under a wire label.
//!
//! ## Cost figures carry the retry caveat
//!
//! Request counts in this repo are counts of logical store calls, not billed
//! requests (ADR-0927 decision 8, issue #928): `object_store` retries below
//! `InstrumentedStore` with `max_retries = 10`. [`RETRY_CAVEAT`] rides in the
//! rendered output next to every cost-shaped figure ([`LOGICAL_PUT_COUNT`],
//! [`LOGICAL_STORE_RETRIES`]).
//!
//! Report-only, like the rest of `ravel-bench`: this never changes library
//! behaviour, it only drives and measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{Clock, IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::{CommitToken, Signal, TenantId};
use serde::{Deserialize, Serialize};

use crate::generator::{BatchSizeDistribution, WorkloadConfig, generate_batches};
use crate::report_schema::{
    Figure, Provenance, RETRY_CAVEAT, ValidationError, validate_provenance,
};

/// Logical input bytes per sample: `ts_ns: i64` + `value: f64`. The same
/// constant `ingest_bench` and `e2e` use. This is the logical input size, NOT
/// the Remote Write wire encoding.
const LOGICAL_BYTES_PER_SAMPLE: u64 = 16;

// --- Figure names. A consumer finds a figure by name, so these are the stable
// keys the report is keyed on. Each byte figure names what it counts. -----------

/// Samples the ingest path accepted and acknowledged.
pub const ACCEPTED_SAMPLES: &str = "accepted_samples";
/// Samples the ingest path rejected (admission or normalization refusal).
pub const REJECTED_SAMPLES: &str = "rejected_samples";
/// Logical store-call retries below `InstrumentedStore`. A logical-call count,
/// not a billed-request count (see [`RETRY_CAVEAT`], #928).
pub const LOGICAL_STORE_RETRIES: &str = "logical_store_retries";
/// Batches abandoned entirely (retry exhausted, or input rejected).
pub const DROPPED_BATCHES: &str = "dropped_batches";
/// Accepted logical points per wall-clock second of the write phase.
pub const LOGICAL_POINTS_PER_SEC: &str = "logical_points_per_sec";
/// Logical input bytes per second (16 bytes/sample). NOT wire bytes: the
/// Remote Write 1.0 snappy-compressed protobuf body is not encoded here (the
/// `ravel-remote-write` crate is decode-only), so this counts the logical input
/// that would be encoded, and the gap is named rather than emitted under a wire
/// label.
pub const LOGICAL_INPUT_BYTES_PER_SEC: &str = "logical_input_bytes_per_sec";
/// Acknowledgement latency, 50th percentile, milliseconds. The MEANING of the
/// ack is [`SystemIngest::ack`]; this figure is inadmissible without it.
pub const ACK_P50_MS: &str = "ack_p50_ms";
/// Acknowledgement latency, 95th percentile, milliseconds.
pub const ACK_P95_MS: &str = "ack_p95_ms";
/// Acknowledgement latency, 99th percentile, milliseconds.
pub const ACK_P99_MS: &str = "ack_p99_ms";
/// Process CPU seconds (user + system) over the write phase.
pub const CPU_SECONDS: &str = "cpu_seconds";
/// Peak resident set size in bytes (process high-water mark).
pub const PEAK_RSS_BYTES: &str = "peak_rss_bytes";
/// Bytes durably STORED for this run's tenant (summed object sizes). Stored
/// bytes, not wire bytes and not a pool charge.
pub const STORED_BYTES: &str = "stored_bytes";
/// Stored bytes per accepted sample: [`STORED_BYTES`] / [`ACCEPTED_SAMPLES`].
pub const STORED_BYTES_PER_SAMPLE: &str = "stored_bytes_per_sample";
/// Write amplification: stored bytes / logical input bytes.
pub const WRITE_AMPLIFICATION: &str = "write_amplification";
/// Distinct objects the run's tenant prefix holds after the run.
pub const OBJECT_COUNT: &str = "object_count";
/// Logical PUT count: two PUTs per flush (data object + commit record,
/// `ravel_commit::publish::publish`), excluding retries. A logical-call count,
/// not a billed-request count (see [`RETRY_CAVEAT`], #928).
pub const LOGICAL_PUT_COUNT: &str = "logical_put_count";
/// Bytes durably written via PUT for this run's tenant. Stored bytes; equal to
/// [`STORED_BYTES`] on an otherwise-empty tenant prefix.
pub const PUT_BYTES: &str = "put_bytes";

/// The ack-latency figures whose presence on a row requires a specified
/// [`AckSemantics`]. An ack latency without its semantics is the exact
/// misleading-column trap ADR-0927 decision 3 forecloses, so a row carrying any
/// of these with an [`AckSemantics::Unspecified`] fails [`validate`].
pub const ACK_LATENCY_FIGURES: &[&str] = &[ACK_P50_MS, ACK_P95_MS, ACK_P99_MS];

/// The protocol lane a figure belongs to (ADR-0927 decision 2). A lane travels
/// with every row so a cross-lane comparison is visible in the data: a Remote
/// Write 1.0 figure is never comparable with a 2.0 or OTLP figure as though
/// protocol overhead were identical.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lane {
    /// Prometheus Remote Write 1.0, the cross-engine ingest baseline.
    RemoteWrite1_0,
    /// Prometheus Remote Write 2.0, a separate lane.
    RemoteWrite2_0,
    /// OTLP ingest, a separate lane. The only lane that can reach buffered
    /// acknowledgement mode.
    Otlp,
}

impl Lane {
    /// The slug this lane renders and serializes as.
    pub fn slug(self) -> &'static str {
        match self {
            Lane::RemoteWrite1_0 => "remote_write_1.0",
            Lane::RemoteWrite2_0 => "remote_write_2.0",
            Lane::Otlp => "otlp",
        }
    }
}

/// What a system's acknowledgement MEANS (ADR-0927 decision 3). The distinction
/// rides on every ingest row so a durable-on-ack latency is never read beside a
/// buffered ack without the reader seeing which is which.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckSemantics {
    /// A 2xx means the data object and its commit record are both durably
    /// stored (Ravel's strict Remote Write mode). The response carries a commit
    /// token.
    DurableOnAck,
    /// A 2xx means the samples were accepted into a buffer and are not yet
    /// durable (Ravel's buffered mode, reachable only through OTLP).
    BufferedAtEnqueue,
    /// The ack semantics were not recorded. A row carrying an ack-latency figure
    /// with this value fails [`validate`]: it is the misleading column ADR-0927
    /// decision 3 forecloses. Real measurements never produce it; it exists so
    /// the "distinction on the same row" rule is enforceable rather than
    /// unrepresentable.
    Unspecified,
}

impl AckSemantics {
    /// The slug this value renders and serializes as.
    pub fn slug(self) -> &'static str {
        match self {
            AckSemantics::DurableOnAck => "durable_on_ack",
            AckSemantics::BufferedAtEnqueue => "buffered_at_enqueue",
            AckSemantics::Unspecified => "unspecified",
        }
    }

    /// The one-line meaning a report states beside the figure.
    pub fn meaning(self) -> &'static str {
        match self {
            AckSemantics::DurableOnAck => {
                "2xx means the data object and its commit record are both durably stored"
            }
            AckSemantics::BufferedAtEnqueue => {
                "2xx means the samples were accepted into a buffer and are not yet durable"
            }
            AckSemantics::Unspecified => "ack semantics not recorded",
        }
    }

    /// Whether the semantics are recorded. An unspecified ack is not a
    /// measurement of anything comparable.
    pub fn is_specified(self) -> bool {
        !matches!(self, AckSemantics::Unspecified)
    }

    /// The acknowledgement semantics Ravel's ingest surface gives for `mode`.
    /// Strict is durable-on-ack; buffered acks at enqueue and is never durable
    /// on its own (docs/consistency-model.md).
    pub fn for_write_mode(mode: WriteMode) -> Self {
        match mode {
            WriteMode::Strict => AckSemantics::DurableOnAck,
            WriteMode::Buffered => AckSemantics::BufferedAtEnqueue,
        }
    }
}

/// One system's ingestion under one lane, with the acknowledgement semantics
/// that make its figures interpretable. The `(system, lane)` pair is the row
/// identity, so two rows for the same system under the same lane are refused.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemIngest {
    /// The system that ingested, e.g. `"ravel"`.
    pub system: String,
    /// The protocol lane this row measured (ADR-0927 decision 2).
    pub lane: Lane,
    /// What this system's acknowledgement means (ADR-0927 decision 3).
    pub ack: AckSemantics,
    /// The figures this row reports, each named for what it counts.
    #[serde(default)]
    pub figures: Vec<Figure>,
}

impl SystemIngest {
    /// The value of the figure named `name`, or `None` if absent.
    pub fn figure(&self, name: &str) -> Option<f64> {
        self.figures
            .iter()
            .find(|f| f.name == name)
            .map(|f| f.value)
    }

    /// Whether this row carries any ack-latency figure. Such a row requires a
    /// specified [`AckSemantics`].
    pub fn carries_ack_latency(&self) -> bool {
        ACK_LATENCY_FIGURES
            .iter()
            .any(|name| self.figure(name).is_some())
    }
}

/// A MetricsBench ingest report: shared provenance plus one row per
/// system-and-lane. Built on [`crate::report_schema`]'s [`Provenance`] and
/// [`Figure`] rather than a parallel provenance or figure shape; only the
/// per-row ack/lane structure ingest needs is added here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestReport {
    /// The reconciled provenance block. Its `protocol` names the run's primary
    /// lane; each row additionally names its own lane, so a multi-lane report
    /// stays unambiguous.
    pub provenance: Provenance,
    /// The per-system ingestion rows, the source of truth.
    pub systems: Vec<SystemIngest>,
}

/// Everything [`validate`] can reject. Provenance failures reuse the report
/// schema's [`ValidationError`] so there is one provenance contract, not two.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum IngestValidationError {
    /// The shared provenance block failed the report-schema contract.
    #[error("provenance failed validation: {0}")]
    Provenance(#[from] ValidationError),
    /// The report carries no ingest rows. Per-system results are the source of
    /// truth, and a report with none measures nothing.
    #[error("report carries no ingest rows; per-system results are the source of truth")]
    NoSystems,
    /// Two rows share a `(system, lane)` identity. A later row would silently
    /// overwrite an earlier one for a consumer that looks up by identity.
    #[error(
        "system `{system}` appears more than once for lane `{lane}`; a duplicate row would \
         overwrite an earlier one"
    )]
    DuplicateSystemLane {
        /// The duplicated system.
        system: String,
        /// The lane it was duplicated under.
        lane: &'static str,
    },
    /// A figure is structurally malformed: a blank name, or a name that appears
    /// twice on one row.
    #[error("row `{system}`/`{lane}` has a malformed figure: {reason}")]
    MalformedFigure {
        /// The offending system.
        system: String,
        /// The offending lane.
        lane: &'static str,
        /// What is wrong.
        reason: String,
    },
    /// A figure value is not finite. A NaN is the worst case: a band comparison
    /// against it reads as met.
    #[error("row `{system}`/`{lane}` figure `{figure}` is not finite ({value})")]
    NonFiniteFigure {
        /// The offending system.
        system: String,
        /// The offending lane.
        lane: &'static str,
        /// The offending figure name.
        figure: String,
        /// The value that was not finite.
        value: f64,
    },
    /// A figure value is negative. No ingest figure is negative and one would
    /// drag a total toward a passing value.
    #[error("row `{system}`/`{lane}` figure `{figure}` is negative ({value})")]
    NegativeFigure {
        /// The offending system.
        system: String,
        /// The offending lane.
        lane: &'static str,
        /// The offending figure name.
        figure: String,
        /// The negative value.
        value: f64,
    },
    /// A row reports an ack-latency figure without a specified [`AckSemantics`].
    /// This is the misleading-column trap ADR-0927 decision 3 forecloses: a
    /// durable-on-ack latency and a buffered ack cannot share a column without
    /// the distinction on the same row, and an unspecified row erases the
    /// distinction. The one that makes the rule enforceable rather than
    /// documentary.
    #[error(
        "row `{system}`/`{lane}` reports an ack-latency figure but its acknowledgement semantics \
         are unspecified; a durable-on-ack latency and a buffered ack cannot share a column \
         without the distinction on the same row (ADR-0927 decision 3)"
    )]
    AckSemanticsUnspecified {
        /// The offending system.
        system: String,
        /// The offending lane.
        lane: &'static str,
    },
}

/// Validate one row's figures: no blank name, no duplicate name, finite,
/// non-negative.
fn validate_figures(row: &SystemIngest) -> Result<(), IngestValidationError> {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for f in &row.figures {
        if f.name.trim().is_empty() {
            return Err(IngestValidationError::MalformedFigure {
                system: row.system.clone(),
                lane: row.lane.slug(),
                reason: "a figure has a blank name".to_string(),
            });
        }
        if !f.value.is_finite() {
            return Err(IngestValidationError::NonFiniteFigure {
                system: row.system.clone(),
                lane: row.lane.slug(),
                figure: f.name.clone(),
                value: f.value,
            });
        }
        if f.value < 0.0 {
            return Err(IngestValidationError::NegativeFigure {
                system: row.system.clone(),
                lane: row.lane.slug(),
                figure: f.name.clone(),
                value: f.value,
            });
        }
        if !seen.insert(f.name.as_str()) {
            return Err(IngestValidationError::MalformedFigure {
                system: row.system.clone(),
                lane: row.lane.slug(),
                reason: format!("figure `{}` appears more than once", f.name),
            });
        }
    }
    Ok(())
}

/// Validate a whole ingest report, fail-closed. The provenance must satisfy the
/// report-schema contract, the report must carry at least one row, no two rows
/// may share a `(system, lane)` identity, every figure must be well formed, and
/// every row that reports an ack-latency figure must state what its ack means.
///
/// That last check is the load-bearing one (ADR-0927 decision 3): a report
/// whose rows mix ack semantics without the distinction on the same row is
/// refused here, which is what makes the rule enforceable rather than a
/// sentence in a doc.
pub fn validate(report: &IngestReport) -> Result<(), IngestValidationError> {
    validate_provenance(&report.provenance)?;
    if report.systems.is_empty() {
        return Err(IngestValidationError::NoSystems);
    }
    let mut seen: BTreeSet<(&str, Lane)> = BTreeSet::new();
    for row in &report.systems {
        if !seen.insert((row.system.as_str(), row.lane)) {
            return Err(IngestValidationError::DuplicateSystemLane {
                system: row.system.clone(),
                lane: row.lane.slug(),
            });
        }
        validate_figures(row)?;
        // The distinction-on-the-same-row rule. Flip the condition below to
        // `false` to watch `a_report_mixing_ack_semantics_without_the_distinction_fails_validation`
        // fail: with the guard neutered, validate returns Ok and the test's
        // expect_err finds Ok(()).
        if row.carries_ack_latency() && !row.ack.is_specified() {
            return Err(IngestValidationError::AckSemanticsUnspecified {
                system: row.system.clone(),
                lane: row.lane.slug(),
            });
        }
    }
    Ok(())
}

/// Render the ingest report as a human-readable table derived entirely from the
/// artifact, after validating it (integrity by identity first). Each row states
/// its lane and what its ack means, so a strict-ack row and a buffered-ack row
/// are visibly distinct, and the retry caveat rides in the output.
pub fn render(report: &IngestReport) -> Result<String, IngestValidationError> {
    validate(report)?;

    let mut out = String::new();
    let p = &report.provenance;
    out.push_str("metricsbench ingest report (schema v");
    let _ = writeln!(out, "{})", p.schema_version);
    let _ = writeln!(out, "  commit    : {}", p.ravel_git_commit);
    let _ = writeln!(out, "  toolchain : {}", p.toolchain);
    let _ = writeln!(out, "  protocol  : {}", p.protocol);
    let _ = writeln!(
        out,
        "  backend   : {} (bills_requests={})",
        p.backend.store_backend, p.backend.backend_bills_requests
    );
    let _ = writeln!(out, "  NOTE: {RETRY_CAVEAT}");

    for row in &report.systems {
        out.push('\n');
        let _ = writeln!(
            out,
            "  system={} lane={} ack={} ({})",
            row.system,
            row.lane.slug(),
            row.ack.slug(),
            row.ack.meaning()
        );
        for f in &row.figures {
            let _ = writeln!(out, "    {:<28} {:>16.4}", f.name, f.value);
        }
    }
    Ok(out)
}

fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

/// Peak resident set size in bytes (VmHWM from `/proc/self/status`), or `None`
/// off Linux or on an unreadable line. Matches the pattern the compaction bench
/// uses.
fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kib * 1024);
        }
    }
    None
}

/// Process CPU time (user + system) from `/proc/self/stat`, or `None` off Linux
/// or on an unparseable line. Fields 14 (`utime`) and 15 (`stime`) are in clock
/// ticks; Linux fixes `USER_HZ` at 100. Same reader the columnar-load bench
/// uses.
fn process_cpu() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let rest = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    const USER_HZ: u64 = 100;
    Some(Duration::from_nanos(
        (utime + stime).saturating_mul(1_000_000_000 / USER_HZ),
    ))
}

/// The measured result of driving one lane: the raw counts a test asserts on
/// exactly, plus the assembled figures. `matched_series` is populated only when
/// a read-your-write pass ran (strict mode), where the returned commit tokens
/// make the just-written data visible without sleeping.
pub struct LaneMeasurement {
    /// The lane driven.
    pub lane: Lane,
    /// The acknowledgement semantics of the write mode driven.
    pub ack: AckSemantics,
    /// Samples the generator produced.
    pub generated_samples: u64,
    /// Samples the ingest path accepted.
    pub accepted_samples: u64,
    /// Series a read-your-write query matched, or `None` if no such pass ran.
    pub matched_series: Option<u64>,
    /// The assembled figures, ready to hang on a [`SystemIngest`] row.
    pub figures: Vec<Figure>,
}

impl LaneMeasurement {
    /// Build the reportable row for `system` from this measurement.
    pub fn into_row(self, system: &str) -> SystemIngest {
        SystemIngest {
            system: system.to_string(),
            lane: self.lane,
            ack: self.ack,
            figures: self.figures,
        }
    }
}

/// Drive one Remote-Write-shaped ingest lane against `store` and measure it.
///
/// Points are generated deterministically and written through `IngestRouter`
/// with `mode`, the same router the Remote Write 1.0 handler reaches after
/// decode and normalization. When `read_your_write` is set and the mode is
/// strict, the returned commit tokens are passed as `min_commit_token` to a
/// fresh query engine so the just-written data is read back deterministically,
/// with no sleep. Buffered mode acks at enqueue and returns no tokens, so it
/// carries no read-your-write pass.
///
/// `series_count` gauges of `samples_per_series` samples are written; all
/// series are gauges named `bench_gauge` so a read-your-write selector matches
/// by name and the matched count equals `series_count` exactly.
pub async fn measure_lane(
    store: Arc<dyn ObjectStoreBackend>,
    lane: Lane,
    mode: WriteMode,
    shards: u32,
    series_count: usize,
    samples_per_series: usize,
    read_your_write: bool,
) -> LaneMeasurement {
    let tenant = TenantId::new("metricsbench-m5");
    let tenant_hash = tenant.hash();
    let signal = Signal::Metrics;
    let ack_deadline = Duration::from_secs(30);

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let router = Arc::new(IngestRouter::new(
        IngestConfig {
            shard_count: shards,
            ..IngestConfig::default()
        },
        Arc::clone(&store),
        signal,
        Arc::clone(&clock),
    ));

    let run_start_ns = clock.now_ns();
    let interval_ns = 1_000_000_000; // 1 s between samples.
    let workload = WorkloadConfig {
        tenant: tenant.as_str().to_string(),
        series_count,
        samples_per_series,
        start_ts_ns: run_start_ns,
        interval_ns,
        // Deterministic single-name workload: all gauges, no churn, so the
        // read-your-write selector `bench_gauge` matches exactly `series_count`
        // series.
        counter_fraction: 0.0,
        label_churn_rate: 0.0,
        out_of_order_fraction: 0.0,
        jitter_ns: 0,
        batch_size: BatchSizeDistribution::fixed(samples_per_series.max(1)),
        ..WorkloadConfig::default()
    };
    let batches = generate_batches(&workload).expect("generate workload");
    let generated_samples: u64 = batches.iter().map(|b| b.len() as u64).sum();

    let cpu_before = process_cpu();
    let wall_start = std::time::Instant::now();

    let mut latencies_ns: Vec<u64> = Vec::with_capacity(batches.len());
    let mut accepted_samples: u64 = 0;
    let mut tokens: Vec<CommitToken> = Vec::new();
    for batch in batches {
        let batch_len = batch.len() as u64;
        let start = std::time::Instant::now();
        let result = router
            .write(tenant.clone(), batch, mode, ack_deadline)
            .await;
        latencies_ns.push(start.elapsed().as_nanos() as u64);
        match result {
            Ok(receipt) => {
                accepted_samples += batch_len;
                tokens.extend(receipt.tokens);
            }
            Err(err) => eprintln!("metrics_ingest write error: {err}"),
        }
    }
    let elapsed_secs = wall_start.elapsed().as_secs_f64().max(1e-9);
    let cpu_seconds = match (process_cpu(), cpu_before) {
        (Some(after), Some(before)) => Some(after.saturating_sub(before).as_secs_f64()),
        _ => None,
    };

    // Drain trailing buffered/partial flushes so the stored-bytes and object
    // figures reflect everything the run wrote.
    router.flush_all().await;

    // Read-your-write, strict mode only: pass the commit tokens as
    // min_commit_token so the write is visible without sleeping. Buffered mode
    // returns no tokens (ack at enqueue), so there is nothing to prove this way.
    let matched_series = if read_your_write && !tokens.is_empty() {
        let query_catalog = Arc::new(
            Catalog::new(
                Arc::clone(&store),
                CatalogConfig {
                    shard_count: shards,
                    ..CatalogConfig::default()
                },
            )
            .expect("query catalog config"),
        );
        let engine = QueryEngine::new(query_catalog, Arc::clone(&store), EngineConfig::default());
        let last_sample_ns = run_start_ns + interval_ns * (samples_per_series.max(1) as i64 - 1);
        let query_t_ms = last_sample_ns / 1_000_000;
        let now_ns = clock.now_ns();
        let (value, _coverage) = engine
            .instant(
                tenant_hash,
                "bench_gauge",
                query_t_ms,
                &tokens,
                now_ns,
                Duration::from_secs(30),
            )
            .await
            .expect("read-your-write instant query");
        Some(match value {
            Value::Vector(v) => v.len() as u64,
            _ => 0,
        })
    } else {
        None
    };

    // Cost and stored-bytes figures, scoped to this run's tenant prefix so
    // objects other tenants left in a shared bucket cannot inflate them.
    let metrics = router.metrics().snapshot();
    let tenant_prefix = format!("t/{}/", tenant_hash.to_hex());
    let objects = list_all(store.as_ref(), &tenant_prefix)
        .await
        .expect("list tenant objects");
    let stored_bytes: u64 = objects.iter().map(|o| o.size).sum();
    let object_count = objects.len() as u64;
    let logical_bytes = accepted_samples * LOGICAL_BYTES_PER_SAMPLE;
    let logical_put_count = 2
        * (metrics.flushes_by_size
            + metrics.flushes_by_age
            + metrics.flushes_by_age_adaptive
            + metrics.flushes_manual);

    let mut sorted = latencies_ns.clone();
    sorted.sort_unstable();
    let ms = |ns: u64| ns as f64 / 1e6;

    let mut figures = vec![
        Figure {
            name: ACCEPTED_SAMPLES.to_string(),
            value: accepted_samples as f64,
        },
        Figure {
            name: REJECTED_SAMPLES.to_string(),
            value: (generated_samples - accepted_samples.min(generated_samples)) as f64,
        },
        Figure {
            name: LOGICAL_STORE_RETRIES.to_string(),
            value: metrics.put_retries as f64,
        },
        Figure {
            name: DROPPED_BATCHES.to_string(),
            value: (metrics.abandoned_retry_exhausted + metrics.abandoned_input_rejected) as f64,
        },
        Figure {
            name: LOGICAL_POINTS_PER_SEC.to_string(),
            value: accepted_samples as f64 / elapsed_secs,
        },
        Figure {
            name: LOGICAL_INPUT_BYTES_PER_SEC.to_string(),
            value: logical_bytes as f64 / elapsed_secs,
        },
        Figure {
            name: ACK_P50_MS.to_string(),
            value: ms(percentile(&sorted, 0.50)),
        },
        Figure {
            name: ACK_P95_MS.to_string(),
            value: ms(percentile(&sorted, 0.95)),
        },
        Figure {
            name: ACK_P99_MS.to_string(),
            value: ms(percentile(&sorted, 0.99)),
        },
        Figure {
            name: STORED_BYTES.to_string(),
            value: stored_bytes as f64,
        },
        Figure {
            name: STORED_BYTES_PER_SAMPLE.to_string(),
            value: if accepted_samples == 0 {
                0.0
            } else {
                stored_bytes as f64 / accepted_samples as f64
            },
        },
        Figure {
            name: WRITE_AMPLIFICATION.to_string(),
            value: if logical_bytes == 0 {
                0.0
            } else {
                stored_bytes as f64 / logical_bytes as f64
            },
        },
        Figure {
            name: OBJECT_COUNT.to_string(),
            value: object_count as f64,
        },
        Figure {
            name: LOGICAL_PUT_COUNT.to_string(),
            value: logical_put_count as f64,
        },
        Figure {
            name: PUT_BYTES.to_string(),
            value: stored_bytes as f64,
        },
    ];
    if let Some(cpu) = cpu_seconds {
        figures.push(Figure {
            name: CPU_SECONDS.to_string(),
            value: cpu,
        });
    }
    if let Some(rss) = peak_rss_bytes() {
        figures.push(Figure {
            name: PEAK_RSS_BYTES.to_string(),
            value: rss as f64,
        });
    }

    LaneMeasurement {
        lane,
        ack: AckSemantics::for_write_mode(mode),
        generated_samples,
        accepted_samples,
        matched_series,
        figures,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report_schema::{Backend, Hardware};
    use ravel_object_store::memory::MemoryStore;

    /// A provenance block that validates clean under the report-schema contract,
    /// for a fixture report to carry. `remote_write_1.0` is the run's primary
    /// lane; individual rows still name their own lane.
    fn provenance() -> Provenance {
        Provenance {
            schema_version: crate::report_schema::SCHEMA_VERSION,
            ravel_git_commit: "9fc85f421590d360e7979ee167eb38e166b45462".to_string(),
            toolchain: "rustc 1.90.0".to_string(),
            protocol: "remote_write_1.0".to_string(),
            hardware: Hardware {
                os: "Linux 6.12 aarch64".to_string(),
                cpu_model: "test".to_string(),
                logical_cores: 4,
                instance_type: None,
            },
            backend: Backend {
                store_backend: "memory".to_string(),
                region: "n/a".to_string(),
                endpoint: "n/a".to_string(),
                backend_bills_requests: false,
            },
            comparators: Vec::new(),
            generator_digest: "blake3:1111".to_string(),
            corpus_digest: "blake3:2222".to_string(),
            config: Vec::new(),
        }
    }

    /// A strict-ack Remote Write 1.0 row with a durable-on-ack latency, and a
    /// buffered-ack OTLP row, with hand-set exact figures so every assertion
    /// pins an exact value rather than a `> 0`.
    fn strict_and_buffered_report() -> IngestReport {
        let strict = SystemIngest {
            system: "ravel".to_string(),
            lane: Lane::RemoteWrite1_0,
            ack: AckSemantics::DurableOnAck,
            figures: vec![
                Figure {
                    name: ACCEPTED_SAMPLES.to_string(),
                    value: 100.0,
                },
                Figure {
                    name: ACK_P50_MS.to_string(),
                    value: 3.5,
                },
                Figure {
                    name: ACK_P99_MS.to_string(),
                    value: 9.0,
                },
            ],
        };
        let buffered = SystemIngest {
            system: "ravel".to_string(),
            lane: Lane::Otlp,
            ack: AckSemantics::BufferedAtEnqueue,
            figures: vec![
                Figure {
                    name: ACCEPTED_SAMPLES.to_string(),
                    value: 100.0,
                },
                Figure {
                    name: ACK_P50_MS.to_string(),
                    value: 0.2,
                },
                Figure {
                    name: ACK_P99_MS.to_string(),
                    value: 0.6,
                },
            ],
        };
        IngestReport {
            provenance: provenance(),
            systems: vec![strict, buffered],
        }
    }

    /// ACCEPTANCE TEST (issue #937). A strict-ack row and a buffered-ack row are
    /// distinguishable in the emitted report, not merely both present: they
    /// carry different [`AckSemantics`] on their own rows, under different lanes,
    /// and the rendered output states each ack's meaning beside its figures. The
    /// report validates, so the distinction is admissible, not smuggled in.
    #[test]
    fn strict_ack_is_reported_separately_from_buffered() {
        let report = strict_and_buffered_report();
        validate(&report).expect("the strict+buffered fixture validates");

        // Exactly one durable-on-ack row and one buffered-at-enqueue row.
        let strict: Vec<&SystemIngest> = report
            .systems
            .iter()
            .filter(|r| r.ack == AckSemantics::DurableOnAck)
            .collect();
        let buffered: Vec<&SystemIngest> = report
            .systems
            .iter()
            .filter(|r| r.ack == AckSemantics::BufferedAtEnqueue)
            .collect();
        assert_eq!(strict.len(), 1, "exactly one strict-ack row");
        assert_eq!(buffered.len(), 1, "exactly one buffered-ack row");

        // The distinction is on the row: the two ack-latency figures live under
        // different, named ack semantics, so a reader never sees them share a
        // column without knowing which is which.
        assert_ne!(
            strict[0].ack, buffered[0].ack,
            "the two rows must carry different ack semantics"
        );
        assert_eq!(strict[0].lane, Lane::RemoteWrite1_0);
        assert_eq!(buffered[0].lane, Lane::Otlp);
        assert_ne!(
            strict[0].lane, buffered[0].lane,
            "a Remote Write 1.0 figure is never comparable with an OTLP figure"
        );
        assert_eq!(strict[0].figure(ACK_P50_MS), Some(3.5));
        assert_eq!(buffered[0].figure(ACK_P50_MS), Some(0.2));

        // The rendered output states each ack's meaning beside its figures.
        let rendered = render(&report).expect("render");
        assert!(
            rendered.contains("ack=durable_on_ack"),
            "render names the strict ack: {rendered}"
        );
        assert!(
            rendered.contains("ack=buffered_at_enqueue"),
            "render names the buffered ack: {rendered}"
        );
        assert!(
            rendered.contains(AckSemantics::DurableOnAck.meaning()),
            "render states what durable-on-ack means"
        );
    }

    /// THE ENFORCEABLE RULE (issue #937). A report that emits an ack-latency
    /// figure without stating what its ack means fails validation. This is what
    /// makes ADR-0927 decision 3 a rule rather than a doc: without it, a
    /// durable-on-ack latency and a buffered ack could sit in one column with no
    /// distinction.
    ///
    /// To watch this test FAIL before the guard existed, change the condition in
    /// `validate` from
    ///     `if row.carries_ack_latency() && !row.ack.is_specified() {`
    /// to
    ///     `if false {`
    /// (the line marked "The distinction-on-the-same-row rule"): validate then
    /// returns Ok(()) for the mutated report, `expect_err` finds Ok, and this
    /// test fails.
    #[test]
    fn a_report_mixing_ack_semantics_without_the_distinction_fails_validation() {
        let mut report = strict_and_buffered_report();
        // Erase the distinction on the row that carries an ack latency: it now
        // reports a latency without saying whether it is durable or buffered.
        assert!(
            report.systems[0].carries_ack_latency(),
            "the mutated row carries an ack latency"
        );
        report.systems[0].ack = AckSemantics::Unspecified;
        let err = validate(&report)
            .expect_err("an ack latency without its semantics must fail validation");
        assert_eq!(
            err,
            IngestValidationError::AckSemanticsUnspecified {
                system: "ravel".to_string(),
                lane: Lane::RemoteWrite1_0.slug(),
            }
        );
    }

    /// An unspecified ack is fine when the row reports NO ack latency: the trap
    /// is specifically an ack figure without its meaning, not the enum value
    /// existing. This pins the guard to the ack-latency condition so a later
    /// tightening that rejects every unspecified row (breaking non-ack rows) is
    /// caught.
    #[test]
    fn an_unspecified_ack_without_an_ack_figure_validates() {
        let mut report = strict_and_buffered_report();
        report.systems[0].ack = AckSemantics::Unspecified;
        report.systems[0]
            .figures
            .retain(|f| !ACK_LATENCY_FIGURES.contains(&f.name.as_str()));
        validate(&report).expect("an unspecified ack with no ack figure validates");
    }

    /// A duplicate `(system, lane)` row is refused: a consumer looking up by
    /// identity would silently read one and ignore the other.
    #[test]
    fn a_duplicate_system_and_lane_is_refused() {
        let mut report = strict_and_buffered_report();
        report.systems.push(report.systems[0].clone());
        let err = validate(&report).expect_err("a duplicate (system, lane) must fail");
        assert_eq!(
            err,
            IngestValidationError::DuplicateSystemLane {
                system: "ravel".to_string(),
                lane: Lane::RemoteWrite1_0.slug(),
            }
        );
    }

    /// A non-finite figure is refused: a NaN would make every band comparison
    /// against it read as met.
    #[test]
    fn a_non_finite_figure_is_refused() {
        let mut report = strict_and_buffered_report();
        report.systems[0].figures.push(Figure {
            name: STORED_BYTES.to_string(),
            value: f64::NAN,
        });
        let err = validate(&report).expect_err("a NaN figure must fail");
        assert!(
            matches!(err, IngestValidationError::NonFiniteFigure { ref figure, .. } if figure == STORED_BYTES),
            "wrong error variant: {err:?}"
        );
    }

    /// A negative figure is refused: no ingest figure is negative.
    #[test]
    fn a_negative_figure_is_refused() {
        let mut report = strict_and_buffered_report();
        report.systems[0].figures.push(Figure {
            name: STORED_BYTES.to_string(),
            value: -1.0,
        });
        let err = validate(&report).expect_err("a negative figure must fail");
        assert!(
            matches!(err, IngestValidationError::NegativeFigure { value, .. } if value == -1.0),
            "wrong error variant: {err:?}"
        );
    }

    /// An empty report is refused: per-system rows are the source of truth.
    #[test]
    fn a_report_with_no_rows_is_refused() {
        let report = IngestReport {
            provenance: provenance(),
            systems: Vec::new(),
        };
        assert_eq!(validate(&report), Err(IngestValidationError::NoSystems));
    }

    /// Read-your-write via `min_commit_token` returns the written samples with
    /// NO sleep. Strict-mode Remote Write returns commit tokens; passing them as
    /// `min_commit_token` makes the just-written data visible deterministically.
    /// Sleeping instead would measure the flush delay and call it query latency
    /// (ADR-0927 decision 3): the alternative is wrong, so this test does not
    /// take it. Every figure asserted is exact or bounded to a quantity the
    /// workload owns (`series_count`, `generated_samples`), never `> 0`.
    #[tokio::test(flavor = "multi_thread")]
    async fn read_your_write_via_min_commit_token_returns_the_written_samples() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let series_count = 20usize;
        let samples_per_series = 5usize;
        let m = measure_lane(
            store,
            Lane::RemoteWrite1_0,
            WriteMode::Strict,
            2,
            series_count,
            samples_per_series,
            true,
        )
        .await;

        // Ack is durable-on-ack for the strict Remote Write 1.0 lane.
        assert_eq!(m.ack, AckSemantics::DurableOnAck);
        assert_eq!(m.lane, Lane::RemoteWrite1_0);

        // No silent drop: accepted equals generated equals the workload's own
        // series_count * samples_per_series.
        let expected_samples = (series_count * samples_per_series) as u64;
        assert_eq!(m.generated_samples, expected_samples);
        assert_eq!(m.accepted_samples, expected_samples);

        // Read-your-write matched exactly one series per generated gauge, with
        // no sleep: the tokens made every write visible.
        assert_eq!(
            m.matched_series,
            Some(series_count as u64),
            "min_commit_token read-your-write must see every written series"
        );

        // The assembled row is admissible, and its ack-latency figures sit under
        // a specified, durable ack.
        let row = m.into_row("ravel");
        assert!(row.carries_ack_latency());
        let report = IngestReport {
            provenance: provenance(),
            systems: vec![row.clone()],
        };
        validate(&report).expect("a measured strict-ack report validates");
        // accepted_samples on the row equals the workload's owned quantity.
        assert_eq!(row.figure(ACCEPTED_SAMPLES), Some(expected_samples as f64));
        assert_eq!(row.figure(REJECTED_SAMPLES), Some(0.0));
    }

    /// A buffered-mode measurement acks at enqueue and carries no read-your-write
    /// pass (no commit tokens), and its row is a buffered-ack row distinct from a
    /// strict one. Kept small; asserts exact accepted count, not a latency band.
    #[tokio::test(flavor = "multi_thread")]
    async fn buffered_mode_measures_a_buffered_ack_row() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let m = measure_lane(store, Lane::Otlp, WriteMode::Buffered, 2, 10, 4, true).await;
        assert_eq!(m.ack, AckSemantics::BufferedAtEnqueue);
        assert_eq!(m.matched_series, None, "buffered ack returns no tokens");
        assert_eq!(m.accepted_samples, 40);
        let row = m.into_row("ravel");
        assert_eq!(row.ack, AckSemantics::BufferedAtEnqueue);
        assert_ne!(row.ack, AckSemantics::DurableOnAck);
    }
}
