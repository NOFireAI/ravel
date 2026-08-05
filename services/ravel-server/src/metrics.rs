//! `GET /metrics`: a hand-written Prometheus text exposition renderer over
//! counters Ravel already computes (ADR-0044 section 4).
//!
//! The renderer is written in this repository rather than pulled from the
//! `prometheus` or `metrics` crate. Every counter Ravel has is already a
//! snapshot struct with a fixed shape, and a registry abstraction would add a
//! second place where label sets are decided; keeping the renderer in-tree
//! means the label allowlist is enforced by the type system, not by
//! convention (ADR-0044, rejected alternative 3).
//!
//! # Label allowlist
//!
//! [`Label`] is the only way to attach a label to a rendered sample, and its
//! variants are exhaustively `tenant_hash`, `signal`, `mode`, `op`,
//! `error_kind`, `workload_class`, `level`, and `reason` (ADR-0044 section 4,
//! `reason` added by ADR-0051 section 6 for the admission-rejection family).
//! Every variant's payload is a closed enum or [`TenantHash`]'s fixed-width
//! hash, so there is no `String` or `&str` anywhere on this path an unlisted
//! label could travel through, and adding a ninth variant is a compile error
//! everywhere this module matches on `Label` exhaustively. `shard` is
//! deliberately absent: shard count times tenant count times operation count
//! is unbounded in the dimension Ravel controls least (ADR-0044, rejected
//! alternative 6). Query text, metric names, label values beyond the closed
//! sets above, stream ids, trace ids, and object keys are never labels.
//!
//! Every sample this module renders carries `mode`, so one Prometheus job can
//! scrape a fleet of `--mode` processes without their series colliding.
//!
//! # Extending with a new source
//!
//! Issue #425 adds per-query cost accounting on top of this renderer. Doing
//! so means building a new snapshot-to-[`Label`] mapping and a new family
//! function beside [`render_store_family`]/[`render_ingest_family`], called
//! from [`render`]; it does not mean reshaping [`Label`] or the escaping and
//! line-writing helpers below.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use ravel_cache::CacheMetricsSnapshot;
use ravel_catalog::Catalog;
use ravel_ingest::{
    AdmissionController, IngestMetricsSnapshot, IngestRouter, LogIngestMetricsSnapshot,
    LogIngestRouter, SpanIngestMetricsSnapshot, SpanIngestRouter, TenantUsage,
};
use ravel_object_store::StoreMetrics;
use ravel_object_store::instrument::{
    LATENCY_BUCKET_BOUNDS_MICROS, LATENCY_BUCKET_COUNT, StoreErrorClass, StoreMetricsSnapshot,
    StoreOp,
};
use ravel_types::accounting::{
    CostEstimate, QueryAccountingSnapshot, QueryCostRecorder, QueryWorkloadClass,
};
use ravel_types::{Signal, TenantHash};

use crate::config::Mode;

/// How a query reached the engine, the `workload_class` label on the
/// per-query cost family (issue #425, ADR-0044 section 4). A closed set:
/// `interactive` is a client-driven HTTP or Flight query, `background` is an
/// internally scheduled query (alert-rule evaluation). Bounded like every
/// other label here, so it can dimension the query-cost series without
/// unbounding cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadClass {
    Interactive,
    Background,
}

impl WorkloadClass {
    /// The `workload_class` label value and the span-field spelling; public so
    /// query handlers can stamp the same bounded string on their request span
    /// (ADR-0044 section 5) that this module renders on `/metrics`.
    pub fn name(self) -> &'static str {
        match self {
            WorkloadClass::Interactive => "interactive",
            WorkloadClass::Background => "background",
        }
    }
}

/// Reserved for future level-dimensioned log series; no sample this module
/// renders uses it yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    fn name(self) -> &'static str {
        match self {
            Level::Trace => "trace",
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}

/// The `reason` label on `ravel_admission_rejected_total` (ADR-0051 section
/// 6). ADR-0051 names a closed set of six reasons
/// `{body_size, byte_rate, series_rate, series_cap, skew, structural}`; the
/// three here are exactly the ones `AdmissionController::usage_snapshot`
/// counts today (`ravel_ingest::TenantUsage`). The other three are enforced
/// at layers that keep no per-tenant counter in that snapshot yet (body size
/// at the transport, skew and structural in normalization), so a variant for
/// them would render samples no data source can fill. They join this enum
/// when their counters do, additively, the same way a new `Signal` variant
/// joins `signal_name`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    ByteRate,
    SeriesRate,
    SeriesCap,
}

impl RejectReason {
    /// Every reason with a counter, so the rejected family renders all three
    /// series per (tenant, signal) even when some are zero (the same
    /// zero-is-not-absence discipline the other families keep).
    const ALL: [RejectReason; 3] = [
        RejectReason::ByteRate,
        RejectReason::SeriesRate,
        RejectReason::SeriesCap,
    ];

    fn name(self) -> &'static str {
        match self {
            RejectReason::ByteRate => "byte_rate",
            RejectReason::SeriesRate => "series_rate",
            RejectReason::SeriesCap => "series_cap",
        }
    }
}

/// A `tenant_hash` label value: either a configured tenant's fixed-width hash
/// or the `other` bucket every unconfigured tenant folds into (ADR-0044
/// section 4), so per-tenant cardinality is bounded by the configured tenant
/// count rather than by traffic. `StoreMetrics`, ingest metrics, and the
/// catalog anomaly counters stay process-global by design; the admission
/// usage family (ADR-0051 section 6) is the one family that renders real
/// `tenant_hash` values, and only when `--metrics-tenant-labels` is set --
/// otherwise it too folds into `other` like every other family here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantHashLabel {
    Hash(TenantHash),
    Other,
}

impl TenantHashLabel {
    fn value(&self) -> String {
        match self {
            TenantHashLabel::Hash(hash) => hash.to_hex(),
            TenantHashLabel::Other => "other".to_string(),
        }
    }
}

/// One label attached to a rendered sample. See the [module docs](self) for
/// why this exhaustive enum, not a `(&str, String)` pair, is the renderer's
/// only way to attach a label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Label {
    TenantHash(TenantHashLabel),
    Signal(Signal),
    Mode(Mode),
    Op(StoreOp),
    ErrorKind(StoreErrorClass),
    WorkloadClass(WorkloadClass),
    Level(Level),
    RejectReason(RejectReason),
}

impl Label {
    fn key(&self) -> &'static str {
        match self {
            Label::TenantHash(_) => "tenant_hash",
            Label::Signal(_) => "signal",
            Label::Mode(_) => "mode",
            Label::Op(_) => "op",
            Label::ErrorKind(_) => "error_kind",
            Label::WorkloadClass(_) => "workload_class",
            Label::Level(_) => "level",
            Label::RejectReason(_) => "reason",
        }
    }

    fn value(&self) -> String {
        match self {
            Label::TenantHash(hash) => hash.value(),
            Label::Signal(signal) => signal_name(*signal).to_string(),
            Label::Mode(mode) => mode_name(*mode).to_string(),
            Label::Op(op) => op.name().to_string(),
            Label::ErrorKind(class) => class.name().to_string(),
            Label::WorkloadClass(class) => class.name().to_string(),
            Label::Level(level) => level.name().to_string(),
            Label::RejectReason(reason) => reason.name().to_string(),
        }
    }
}

/// Exhaustive: adding a [`Signal`] variant breaks this compile until it is
/// handled here, same discipline as `StoreErrorClass::of`.
fn signal_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Metrics => "metrics",
        Signal::Logs => "logs",
        Signal::Spans => "spans",
        Signal::Profiles => "profiles",
        Signal::Alerts => "alerts",
        Signal::Audit => "audit",
    }
}

/// Exhaustive: adding a [`Mode`] variant breaks this compile until it is
/// handled here.
fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::All => "all",
        Mode::Gateway => "gateway",
        Mode::Query => "query",
        Mode::Maintain => "maintain",
    }
}

/// Escape a label value per the Prometheus text exposition format. The
/// allowlist makes this nearly unreachable (every [`Label::value`] comes from
/// a closed enum or a hex string), but the format requires it regardless of
/// how unreachable a byte sequence is in practice.
fn escape_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Writes `{k="v",...}`, plus one trailing `le="<le>"` pair when `le` is
/// `Some`. `le` is a Prometheus histogram-reserved label, not one of this
/// module's allowlisted keys: it is structural to the exposition format
/// itself (every histogram bucket carries it), not a Ravel-chosen dimension,
/// so it is threaded through separately rather than added to [`Label`].
fn write_labels(out: &mut String, labels: &[Label], le: Option<&str>) {
    if labels.is_empty() && le.is_none() {
        return;
    }
    out.push('{');
    for (i, label) in labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(label.key());
        out.push_str("=\"");
        out.push_str(&escape_label_value(&label.value()));
        out.push('"');
    }
    if let Some(le) = le {
        if !labels.is_empty() {
            out.push(',');
        }
        out.push_str("le=\"");
        out.push_str(&escape_label_value(le));
        out.push('"');
    }
    out.push('}');
}

fn write_sample(out: &mut String, name: &str, labels: &[Label], value: u64) {
    out.push_str(name);
    write_labels(out, labels, None);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn write_sample_f64(out: &mut String, name: &str, labels: &[Label], value: f64) {
    out.push_str(name);
    write_labels(out, labels, None);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn write_histogram_bucket(out: &mut String, name: &str, labels: &[Label], le: &str, value: u64) {
    out.push_str(name);
    write_labels(out, labels, Some(le));
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn write_header(out: &mut String, name: &str, help: &str, kind: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

/// Cumulative bucket counts for a Prometheus histogram, computed from
/// `ravel_object_store::instrument`'s fixed per-bucket (non-cumulative)
/// counts: bucket `i` there counts only observations that landed exactly in
/// that bucket, while a Prometheus `_bucket{le}` series must be the count of
/// observations `<= le`, monotonically non-decreasing, with the last bucket
/// equal to `_count`.
fn cumulative_buckets(raw: &[u64; LATENCY_BUCKET_COUNT]) -> [u64; LATENCY_BUCKET_COUNT] {
    let mut cumulative = [0u64; LATENCY_BUCKET_COUNT];
    let mut running = 0u64;
    for (slot, count) in cumulative.iter_mut().zip(raw.iter()) {
        running = running.saturating_add(*count);
        *slot = running;
    }
    cumulative
}

/// One histogram bucket's `le` value: the bound in seconds for a real bound,
/// `+Inf` for the overflow bucket.
fn bucket_le(index: usize) -> String {
    match LATENCY_BUCKET_BOUNDS_MICROS.get(index) {
        Some(bound_micros) => (*bound_micros as f64 / 1_000_000.0).to_string(),
        None => "+Inf".to_string(),
    }
}

fn render_store_family(out: &mut String, mode: Mode, snapshot: &StoreMetricsSnapshot) {
    write_header(
        out,
        "ravel_store_calls_total",
        "Completed object-store calls, by operation.",
        "counter",
    );
    for op in StoreOp::ALL {
        write_sample(
            out,
            "ravel_store_calls_total",
            &[Label::Mode(mode), Label::Op(op)],
            snapshot.op(op).calls,
        );
    }

    write_header(
        out,
        "ravel_store_ok_total",
        "Object-store calls that returned Ok, by operation.",
        "counter",
    );
    for op in StoreOp::ALL {
        write_sample(
            out,
            "ravel_store_ok_total",
            &[Label::Mode(mode), Label::Op(op)],
            snapshot.op(op).ok,
        );
    }

    write_header(
        out,
        "ravel_store_errors_total",
        "Object-store call failures, by operation and error kind.",
        "counter",
    );
    for op in StoreOp::ALL {
        let op_snapshot = snapshot.op(op);
        for class in StoreErrorClass::ALL {
            write_sample(
                out,
                "ravel_store_errors_total",
                &[Label::Mode(mode), Label::Op(op), Label::ErrorKind(class)],
                op_snapshot.error_count(class),
            );
        }
    }

    write_header(
        out,
        "ravel_store_bytes_total",
        "Bytes returned by a successful get or offered by a put, by operation.",
        "counter",
    );
    for op in StoreOp::ALL {
        write_sample(
            out,
            "ravel_store_bytes_total",
            &[Label::Mode(mode), Label::Op(op)],
            snapshot.op(op).bytes,
        );
    }

    write_header(
        out,
        "ravel_store_latency_seconds",
        "Object-store call latency, by operation.",
        "histogram",
    );
    for op in StoreOp::ALL {
        let op_snapshot = snapshot.op(op);
        let cumulative = cumulative_buckets(&op_snapshot.latency_micros_buckets);
        for (i, count) in cumulative.iter().enumerate() {
            write_histogram_bucket(
                out,
                "ravel_store_latency_seconds_bucket",
                &[Label::Mode(mode), Label::Op(op)],
                &bucket_le(i),
                *count,
            );
        }
        write_sample_f64(
            out,
            "ravel_store_latency_seconds_sum",
            &[Label::Mode(mode), Label::Op(op)],
            op_snapshot.latency_nanos_total as f64 / 1_000_000_000.0,
        );
        // `_count` must equal the `+Inf` bucket, so it is read from the same
        // cumulative array rather than from `op_snapshot.calls`. `snapshot()`
        // is a scrape, not a consistent cut: `OpMetrics::record` increments
        // `calls` before the latency bucket, and `snapshot()` loads the
        // buckets before `calls`, so a scrape concurrent with a call would
        // otherwise report `_count` greater than `+Inf` and violate the
        // exposition format.
        write_sample(
            out,
            "ravel_store_latency_seconds_count",
            &[Label::Mode(mode), Label::Op(op)],
            cumulative[LATENCY_BUCKET_COUNT - 1],
        );
    }
}

/// One ingest pipeline's counters, normalized to one shape so metrics,
/// logs, and spans render under the same metric names split by the `signal`
/// label rather than as three separately named families (ADR-0044 section 4:
/// `signal` exists exactly for this). `collisions` is `None` for spans, which
/// derive no identity that could collide (`ravel_ingest::SpanWriteError`
/// module docs); this is a structural absence, not a zero, so the collisions
/// family simply has no `signal="spans"` sample.
pub struct IngestPipelineSnapshot {
    pub signal: Signal,
    pub flushes_by_size: u64,
    pub flushes_by_age: u64,
    pub flushes_manual: u64,
    pub put_retries: u64,
    pub abandoned_retry_exhausted: u64,
    pub abandoned_input_rejected: u64,
    pub buffered_bytes_total: u64,
    pub buffered_items_total: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    pub collisions: Option<u64>,
    pub shard_deaths: u64,
    /// Flushes failed closed because the router's cached provisioning view for
    /// the tenant was older than the refresh interval `C` (ADR-0052 section 3).
    pub stale_provisioning_flushes: u64,
    /// Write-side POSTINGS counters (ADR-0049, issue #511). `Some` only for the
    /// log pipeline; `None` for metrics and spans, which build no POSTINGS
    /// section, so the postings family renders no sample for them.
    pub postings: Option<PostingsCounters>,
}

/// The log pipeline's write-side POSTINGS counters, cumulative over flushed
/// objects (ADR-0049 decision 4, issue #511). Rendered without any per-field
/// label, which the ADR-0044 allowlist forbids: `distinct_values_total` over
/// `indexed_fields_total` is the mean distinct-per-field, and `bytes_total`
/// over `objects` the mean section bytes per indexed object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PostingsCounters {
    pub objects: u64,
    pub bytes_total: u64,
    pub indexed_fields_total: u64,
    pub distinct_values_total: u64,
    pub capped_fields_total: u64,
}

impl IngestPipelineSnapshot {
    pub fn from_metrics(snapshot: IngestMetricsSnapshot) -> Self {
        IngestPipelineSnapshot {
            signal: Signal::Metrics,
            flushes_by_size: snapshot.flushes_by_size,
            flushes_by_age: snapshot.flushes_by_age,
            flushes_manual: snapshot.flushes_manual,
            put_retries: snapshot.put_retries,
            abandoned_retry_exhausted: snapshot.abandoned_retry_exhausted,
            abandoned_input_rejected: snapshot.abandoned_input_rejected,
            buffered_bytes_total: snapshot.buffered_bytes_total,
            buffered_items_total: snapshot.buffered_points_total,
            acks_ok: snapshot.acks_ok,
            acks_err: snapshot.acks_err,
            collisions: Some(snapshot.series_id_collisions),
            shard_deaths: snapshot.shard_deaths,
            stale_provisioning_flushes: snapshot.stale_provisioning_flushes,
            postings: None,
        }
    }

    pub fn from_log_metrics(snapshot: LogIngestMetricsSnapshot) -> Self {
        IngestPipelineSnapshot {
            signal: Signal::Logs,
            flushes_by_size: snapshot.flushes_by_size,
            flushes_by_age: snapshot.flushes_by_age,
            flushes_manual: snapshot.flushes_manual,
            put_retries: snapshot.put_retries,
            abandoned_retry_exhausted: snapshot.abandoned_retry_exhausted,
            abandoned_input_rejected: snapshot.abandoned_input_rejected,
            buffered_bytes_total: snapshot.buffered_bytes_total,
            buffered_items_total: snapshot.buffered_records_total,
            acks_ok: snapshot.acks_ok,
            acks_err: snapshot.acks_err,
            collisions: Some(snapshot.stream_id_collisions),
            shard_deaths: snapshot.shard_deaths,
            stale_provisioning_flushes: snapshot.stale_provisioning_flushes,
            postings: Some(PostingsCounters {
                objects: snapshot.postings_objects,
                bytes_total: snapshot.postings_bytes_total,
                indexed_fields_total: snapshot.postings_indexed_fields_total,
                distinct_values_total: snapshot.postings_distinct_values_total,
                capped_fields_total: snapshot.postings_capped_fields_total,
            }),
        }
    }

    pub fn from_span_metrics(snapshot: SpanIngestMetricsSnapshot) -> Self {
        IngestPipelineSnapshot {
            signal: Signal::Spans,
            flushes_by_size: snapshot.flushes_by_size,
            flushes_by_age: snapshot.flushes_by_age,
            flushes_manual: snapshot.flushes_manual,
            put_retries: snapshot.put_retries,
            abandoned_retry_exhausted: snapshot.abandoned_retry_exhausted,
            abandoned_input_rejected: snapshot.abandoned_input_rejected,
            buffered_bytes_total: snapshot.buffered_bytes_total,
            buffered_items_total: snapshot.buffered_spans_total,
            acks_ok: snapshot.acks_ok,
            acks_err: snapshot.acks_err,
            collisions: None,
            shard_deaths: snapshot.shard_deaths,
            stale_provisioning_flushes: snapshot.stale_provisioning_flushes,
            postings: None,
        }
    }
}

fn render_ingest_family(out: &mut String, mode: Mode, pipelines: &[IngestPipelineSnapshot]) {
    fn labels(mode: Mode, signal: Signal) -> [Label; 2] {
        [Label::Mode(mode), Label::Signal(signal)]
    }

    write_header(
        out,
        "ravel_ingest_flushes_by_size_total",
        "Flushes opened because the tenant buffer reached target_bytes, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_flushes_by_size_total",
            &labels(mode, pipeline.signal),
            pipeline.flushes_by_size,
        );
    }

    write_header(
        out,
        "ravel_ingest_flushes_by_age_total",
        "Flushes opened because the tenant buffer aged past max_flush_delay, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_flushes_by_age_total",
            &labels(mode, pipeline.signal),
            pipeline.flushes_by_age,
        );
    }

    write_header(
        out,
        "ravel_ingest_flushes_manual_total",
        "Flushes opened by an explicit, shutdown, or drop-path drain, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_flushes_manual_total",
            &labels(mode, pipeline.signal),
            pipeline.flushes_manual,
        );
    }

    write_header(
        out,
        "ravel_ingest_put_retries_total",
        "Retried PUT attempts on the data-object or commit-record path, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_put_retries_total",
            &labels(mode, pipeline.signal),
            pipeline.put_retries,
        );
    }

    write_header(
        out,
        "ravel_ingest_abandoned_retry_exhausted_total",
        "Flushes abandoned by retry-budget or lifetime exhaustion, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_abandoned_retry_exhausted_total",
            &labels(mode, pipeline.signal),
            pipeline.abandoned_retry_exhausted,
        );
    }

    write_header(
        out,
        "ravel_ingest_abandoned_input_rejected_total",
        "Flushes abandoned because the input could not build a durable object, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_abandoned_input_rejected_total",
            &labels(mode, pipeline.signal),
            pipeline.abandoned_input_rejected,
        );
    }

    write_header(
        out,
        "ravel_ingest_buffered_bytes_total",
        "Bytes admitted into shard buffers at enqueue time, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_buffered_bytes_total",
            &labels(mode, pipeline.signal),
            pipeline.buffered_bytes_total,
        );
    }

    write_header(
        out,
        "ravel_ingest_buffered_items_total",
        "Samples, records, or spans admitted into shard buffers, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_buffered_items_total",
            &labels(mode, pipeline.signal),
            pipeline.buffered_items_total,
        );
    }

    write_header(
        out,
        "ravel_ingest_acks_ok_total",
        "Strict-mode waiters acked with a commit token, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_acks_ok_total",
            &labels(mode, pipeline.signal),
            pipeline.acks_ok,
        );
    }

    write_header(
        out,
        "ravel_ingest_acks_err_total",
        "Strict-mode waiters acked with a write error, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_acks_err_total",
            &labels(mode, pipeline.signal),
            pipeline.acks_err,
        );
    }

    let with_collisions: Vec<_> = pipelines
        .iter()
        .filter(|pipeline| pipeline.collisions.is_some())
        .collect();
    if !with_collisions.is_empty() {
        write_header(
            out,
            "ravel_ingest_collisions_total",
            "Batches rejected for a series or stream identity collision, by signal.",
            "counter",
        );
        for pipeline in with_collisions {
            write_sample(
                out,
                "ravel_ingest_collisions_total",
                &labels(mode, pipeline.signal),
                pipeline.collisions.unwrap_or_default(),
            );
        }
    }

    write_header(
        out,
        "ravel_ingest_shard_deaths_total",
        "Distinct shard actors observed dead by the router, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_shard_deaths_total",
            &labels(mode, pipeline.signal),
            pipeline.shard_deaths,
        );
    }

    write_header(
        out,
        "ravel_ingest_stale_provisioning_flushes_total",
        "Flushes failed closed because the router's cached shard-generation view was older than \
         the refresh interval C (ADR-0052 section 3), by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_stale_provisioning_flushes_total",
            &labels(mode, pipeline.signal),
            pipeline.stale_provisioning_flushes,
        );
    }
}

/// The write-side POSTINGS counters (ADR-0049 decision 4, issue #511): section
/// bytes and per-field distinct-value counts per indexed object, and the
/// cap-exceeded counter.
///
/// Only the pipelines that build POSTINGS (the log pipeline;
/// `IngestPipelineSnapshot::postings` is `Some`) render a sample, so the family
/// is empty in a metrics- or spans-only process. Every sample carries exactly
/// `{mode, signal}` and no more: the per-field distinct counts are summed into
/// `ravel_logs_postings_distinct_values_total` with the field count in
/// `ravel_logs_postings_indexed_fields_total`, so a scraper derives the mean
/// distinct-per-field without any field-name label, which the ADR-0044 label
/// allowlist forbids. The prune-selectivity metric is rendered separately, off
/// the query path's DataFusion counters (issue #511 deliverable 2).
fn render_logs_postings_family(out: &mut String, mode: Mode, pipelines: &[IngestPipelineSnapshot]) {
    fn labels(mode: Mode, signal: Signal) -> [Label; 2] {
        [Label::Mode(mode), Label::Signal(signal)]
    }

    // (metric name, HELP text, counter selector).
    type PostingsMetric = (&'static str, &'static str, fn(&PostingsCounters) -> u64);

    // Each metric is one header then one sample per pipeline that builds
    // postings, keeping the zero-is-not-absence discipline the other families
    // keep for a configured-but-idle pipeline.
    let metrics: [PostingsMetric; 5] = [
        (
            "ravel_logs_postings_objects_total",
            "Flushed log objects that carried a POSTINGS section, by signal (the denominator for average section bytes per indexed object).",
            |p| p.objects,
        ),
        (
            "ravel_logs_postings_bytes_total",
            "Cumulative encoded POSTINGS section bytes across flushed log objects, by signal.",
            |p| p.bytes_total,
        ),
        (
            "ravel_logs_postings_indexed_fields_total",
            "Cumulative count of indexed fields that emitted a posting list, summed over objects, by signal (the denominator for mean distinct-per-field).",
            |p| p.indexed_fields_total,
        ),
        (
            "ravel_logs_postings_distinct_values_total",
            "Cumulative distinct-value count across non-capped indexed fields, summed over objects, by signal.",
            |p| p.distinct_values_total,
        ),
        (
            "ravel_logs_postings_capped_fields_total",
            "Indexed fields dropped from POSTINGS for exceeding the per-field distinct-value cap (ADR-0049 decision 4), summed over objects, by signal.",
            |p| p.capped_fields_total,
        ),
    ];

    for (name, help, get) in metrics {
        write_header(out, name, help, "counter");
        for pipeline in pipelines {
            if let Some(postings) = &pipeline.postings {
                write_sample(out, name, &labels(mode, pipeline.signal), get(postings));
            }
        }
    }
}

/// The catalog anomaly and hard-failure counters
/// (`crates/ravel-catalog/src/catalog.rs`), decoupled from `Catalog` itself
/// so the renderer is testable with a plain struct literal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CatalogCountersSnapshot {
    pub interlock_violations: u64,
    pub compaction_input_set_conflicts: u64,
    /// ADR-0050 §2 hard isolation-breach failures: a HEAD/postings
    /// tenant_hash mismatch or an out-of-prefix listing result. Unlike the
    /// two counters above, each of these also failed its query.
    pub isolation_breaches: u64,
}

fn render_catalog_family(out: &mut String, mode: Mode, snapshot: &CatalogCountersSnapshot) {
    write_header(
        out,
        "ravel_catalog_interlock_violations_total",
        "Unlisted L0 commit records observed postdating a compaction record in their bucket.",
        "counter",
    );
    write_sample(
        out,
        "ravel_catalog_interlock_violations_total",
        &[Label::Mode(mode)],
        snapshot.interlock_violations,
    );

    write_header(
        out,
        "ravel_catalog_compaction_input_set_conflicts_total",
        "Buckets observed holding two compaction records with different input_set_hash.",
        "counter",
    );
    write_sample(
        out,
        "ravel_catalog_compaction_input_set_conflicts_total",
        &[Label::Mode(mode)],
        snapshot.compaction_input_set_conflicts,
    );

    write_header(
        out,
        "ravel_catalog_isolation_breach_total",
        "Hard-failed queries from a HEAD/postings tenant_hash mismatch or an out-of-prefix listing result (ADR-0050 section 2).",
        "counter",
    );
    write_sample(
        out,
        "ravel_catalog_isolation_breach_total",
        &[Label::Mode(mode)],
        snapshot.isolation_breaches,
    );
}

/// Tenancy adoption counter (ADR-0050 section 3). Counts buckets this process
/// pinned to `V1_UNKEYED` because they held `t/` data but no `sys/tenancy`
/// marker (a pre-ADR-0050 bucket adopted once, permanently). A nonzero value
/// is the visible signal that the one-time migration happened; it is a
/// process-global atomic read directly from [`crate::tenancy`], not a
/// snapshot struct, since it has a single source and no labels.
fn render_tenancy_family(out: &mut String, mode: Mode, v1_unkeyed_adoptions: u64) {
    write_header(
        out,
        "ravel_tenancy_v1_unkeyed_adoptions_total",
        "Buckets pinned to the unkeyed tenant hash on adoption of a pre-ADR-0050 bucket (t/ data present, sys/tenancy absent).",
        "counter",
    );
    write_sample(
        out,
        "ravel_tenancy_v1_unkeyed_adoptions_total",
        &[Label::Mode(mode)],
        v1_unkeyed_adoptions,
    );
}

/// The logs prune-selectivity family (ADR-0049, issue #511 deliverable 2):
/// blocks the logs scans saw, survived, and pruned by postings, cumulative
/// across queries. Reads the `LogsScanExec` DataFusion counters (#544) that
/// `ravel-sql` surfaces on `SqlOutcome::stats`, folded into a process-global by
/// the SQL endpoint. Selectivity is `blocks_survived / blocks_total` (blocks
/// surviving over blocks total); the raw counters are exposed so a scraper
/// derives the ratio over any window. Every sample carries only `{mode,
/// signal}` (the ADR-0044 allowlist), and `signal` is always `logs`: only the
/// logs scan publishes these counters.
fn render_query_postings_family(out: &mut String, mode: Mode, blocks: (u64, u64, u64)) {
    let (total, survived, pruned_by_postings) = blocks;
    let labels = [Label::Mode(mode), Label::Signal(Signal::Logs)];

    write_header(
        out,
        "ravel_logs_prune_blocks_total",
        "Blocks the logs scans considered before postings pruning, cumulative (the denominator of prune selectivity).",
        "counter",
    );
    write_sample(out, "ravel_logs_prune_blocks_total", &labels, total);

    write_header(
        out,
        "ravel_logs_prune_blocks_survived_total",
        "Blocks that survived postings pruning and were scanned, cumulative (the numerator of prune selectivity: survived over total).",
        "counter",
    );
    write_sample(
        out,
        "ravel_logs_prune_blocks_survived_total",
        &labels,
        survived,
    );

    write_header(
        out,
        "ravel_logs_prune_blocks_pruned_by_postings_total",
        "Blocks dropped by the POSTINGS index before scanning, cumulative (ADR-0049).",
        "counter",
    );
    write_sample(
        out,
        "ravel_logs_prune_blocks_pruned_by_postings_total",
        &labels,
        pruned_by_postings,
    );
}

/// Dynamic-tenant `shard_count` provisioning failures (ADR-0050 section 5,
/// EC5): a dynamically-resolved tenant's durable provisioning check failed,
/// either a real disagreement against this process's configured `--shards`
/// (failing that single first-touch request), an unreadable record (corrupt
/// or a future format version, also a hard failure), or the same class of
/// failure caught on the maintain per-tenant loop instead (which skips that
/// tenant's tick rather than failing a request). A static tenant's mismatch
/// refuses startup instead and never reaches this counter. A nonzero value
/// means at least one dynamic tenant's provisioning record could not be
/// validated as expected; the operations guide pages on any increase.
/// Process-global atomic read from [`crate::provisioning`], single source,
/// no labels.
fn render_provisioning_family(out: &mut String, mode: Mode, shard_count_mismatches: u64) {
    write_header(
        out,
        "ravel_provisioning_shard_count_mismatch_total",
        "Dynamic-tenant provisioning checks that failed: a shard_count disagreement, an unreadable record, or a maintain-loop check catching either (ADR-0050 section 5).",
        "counter",
    );
    write_sample(
        out,
        "ravel_provisioning_shard_count_mismatch_total",
        &[Label::Mode(mode)],
        shard_count_mismatches,
    );
}

/// Store-reachability probe family (ADR-0050 section 7, EC7): the
/// `ravel_store_reachable` gauge (1 = the background probe currently reports the
/// store reachable, 0 = unhealthy after `store_probe::K` consecutive failures)
/// and the `ravel_store_probe_failures_total` counter (every failed probe
/// cycle, monotonic). Both are process-global atomic reads from
/// [`crate::store_probe`], single source and no labels, the same shape as the
/// tenancy and provisioning families above. Exported unconditionally so an
/// operator sees a store outage on a metrics-only monitoring setup, even where
/// nothing consumes `/readyz`.
fn render_store_probe_family(out: &mut String, mode: Mode, reachable: bool, failures_total: u64) {
    write_header(
        out,
        "ravel_store_reachable",
        "Whether the background store probe currently reports the object store reachable (1) or unhealthy after K consecutive failed probes (0), with hysteresis (ADR-0050 section 7).",
        "gauge",
    );
    write_sample(
        out,
        "ravel_store_reachable",
        &[Label::Mode(mode)],
        u64::from(reachable),
    );

    write_header(
        out,
        "ravel_store_probe_failures_total",
        "Store-reachability probe cycles that failed to GET sys/tenancy, monotonic (ADR-0050 section 7). Increments on every failed probe, whether or not it crossed the readiness threshold.",
        "counter",
    );
    write_sample(
        out,
        "ravel_store_probe_failures_total",
        &[Label::Mode(mode)],
        failures_total,
    );
}

/// Storage-derived tenant discovery counters for the maintenance driver
/// (ADR-0048 decision 3, issue #504), decoupled from
/// [`crate::tenant_discovery::TenantDiscoveryMetrics`] so the renderer is
/// testable with a plain struct literal, matching [`CatalogCountersSnapshot`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceDiscoverySnapshot {
    /// Tenant prefixes storage reported under `t/` on the last successful
    /// discovery cycle. Stays at its last known-good value across a failed
    /// cycle; never reset to zero by a failure.
    pub tenants_discovered: u64,
    /// Of `tenants_discovered`, the ones actually maintained this cycle
    /// (narrowed by a flag restriction, when one is configured). Equal to
    /// `tenants_discovered` when no restriction is configured.
    pub tenants_maintained: u64,
    /// Cycles where the discovery LIST itself failed and the whole cycle was
    /// skipped (never an empty-set fallback).
    pub tenant_discovery_failures: u64,
}

/// The alarm this family exists for (ADR-0048 decision 3 "What alarms"): a
/// prefix under `t/` holds data storage discovered, but nothing maintained
/// it this cycle. `tenants_maintained < tenants_discovered` is the flag-scoped
/// version of that condition (some discovered tenants were deliberately
/// excluded); `tenants_maintained == 0` while `tenants_discovered > 0` is the
/// version this task exists to make impossible outside a deliberate
/// exclusion, so an operator's alert rule should distinguish the two using
/// the excluded count logged alongside this gauge, not this snapshot alone.
fn render_maintain_family(out: &mut String, mode: Mode, snapshot: &MaintenanceDiscoverySnapshot) {
    write_header(
        out,
        "ravel_maintain_tenants_discovered",
        "Tenant prefixes storage reported under t/ on the last successful discovery cycle.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_maintain_tenants_discovered",
        &[Label::Mode(mode)],
        snapshot.tenants_discovered,
    );

    write_header(
        out,
        "ravel_maintain_tenants_maintained",
        "Discovered tenants actually maintained this cycle, after any flag restriction.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_maintain_tenants_maintained",
        &[Label::Mode(mode)],
        snapshot.tenants_maintained,
    );

    write_header(
        out,
        "ravel_maintain_tenant_discovery_failures_total",
        "Maintenance cycles skipped because tenant discovery itself failed.",
        "counter",
    );
    write_sample(
        out,
        "ravel_maintain_tenant_discovery_failures_total",
        &[Label::Mode(mode)],
        snapshot.tenant_discovery_failures,
    );
}

/// One signal's maintenance-safety counters for one scrape (ADR-0048
/// decisions 4 and 6; issue #517).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceSafetySignalSnapshot {
    pub signal: Signal,
    /// Compaction publishes aborted by the record-count conservation gate
    /// (ADR-0048 decision 6): inputs and built parts disagreed on record
    /// count, so nothing was written.
    pub conservation_aborts: u64,
    /// Orphan-GC mass-orphan circuit breaker trips (ADR-0048 decision 4).
    /// Monotonic: a later pass that no longer trips (dilution or partial
    /// restoration, issue #500) does not decrement this. An operator's alert
    /// rule must fire on the first trip (`increase(...) > 0`), never on a
    /// sustained "currently tripped" condition, because the condition can
    /// clear itself while the withheld data loss persists.
    pub orphan_breaker_trips: u64,
    /// Orphan candidates withheld by the most recent sweep pass. Drops to
    /// `0` the moment a pass no longer trips, even though
    /// `orphan_breaker_trips` still records that an earlier one did; this
    /// gauge alone must never be read as "the breaker cleared, so the data
    /// loss is resolved."
    pub orphans_withheld: u64,
}

/// One scrape's maintenance-safety counters (ADR-0048 decisions 1, 4, 6;
/// issue #517): the three safety controls that, before this issue, reached
/// an operator only through a `tracing` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceSafetySnapshot {
    /// Legal-hold refresh failures (ADR-0048 decision 1). Not signal-scoped:
    /// one refresh gates an entire tenant tick, so a failure skips every
    /// signal and shard of that tick at once.
    pub legal_hold_refresh_failures: u64,
    pub signals: Vec<MaintenanceSafetySignalSnapshot>,
}

/// No `tenant_hash` label on any series here. ADR-0048 decision 4 names
/// `tenant_hash` for the breaker-trip counter, but ADR-0044 section 4 blocks
/// any per-tenant series on this unauthenticated route pending an
/// authentication decision. ADR-0051's `--metrics-tenant-labels` flag now
/// exists, but it only applies to the admission usage family (ADR-0051
/// section 6); this maintenance-safety family is untouched by it. Adding a
/// raw tenant hash here would violate ADR-0044's safety precondition; see
/// [`crate::maintain::MaintenanceSafetyMetrics`] and the issue #517 report
/// for the full contradiction.
fn render_maintain_safety_family(
    out: &mut String,
    mode: Mode,
    snapshot: &MaintenanceSafetySnapshot,
) {
    write_header(
        out,
        "ravel_maintain_legal_hold_refresh_failures_total",
        "Legal-hold refresh failures; each one skips that tenant's whole maintenance tick.",
        "counter",
    );
    write_sample(
        out,
        "ravel_maintain_legal_hold_refresh_failures_total",
        &[Label::Mode(mode)],
        snapshot.legal_hold_refresh_failures,
    );

    fn labels(mode: Mode, signal: Signal) -> [Label; 2] {
        [Label::Mode(mode), Label::Signal(signal)]
    }

    write_header(
        out,
        "ravel_maintain_conservation_aborts_total",
        "Compaction publishes aborted by the record-count conservation gate, by signal.",
        "counter",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_maintain_conservation_aborts_total",
            &labels(mode, signal.signal),
            signal.conservation_aborts,
        );
    }

    write_header(
        out,
        "ravel_maintain_orphan_breaker_tripped_total",
        "Orphan-GC mass-orphan circuit breaker trips, by signal. Alert on increase() > 0, not \
         on sustained state: the condition can clear itself while the withheld data loss \
         persists (issue #500).",
        "counter",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_maintain_orphan_breaker_tripped_total",
            &labels(mode, signal.signal),
            signal.orphan_breaker_trips,
        );
    }

    write_header(
        out,
        "ravel_maintain_orphans_withheld",
        "Orphan candidates withheld by the most recent sweep pass, by signal. 0 does not mean \
         a prior trip was resolved; see ravel_maintain_orphan_breaker_tripped_total.",
        "gauge",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_maintain_orphans_withheld",
            &labels(mode, signal.signal),
            signal.orphans_withheld,
        );
    }
}

/// The ADR-0046 read cache's counters (issues #445, #502), one process-wide
/// `Cache` shared by the PromQL and SQL fetchers (`ravel_server::query`), so
/// this family carries only `mode`, no `signal` split -- the same discipline
/// `render_store_family` uses (separate metric names per outcome, e.g.
/// `calls_total`/`ok_total`/`errors_total`, rather than an unlisted "result"
/// label). Request hit rate is `hits / (hits + misses)`; byte hit rate is
/// `bytes_served / (bytes_served + bytes_admitted)`; both are left for
/// PromQL to compute from the raw counters, not baked in here. Deliberately
/// omits `single_flight_collapses`: issue #503 is its own fleet-wide
/// collapse-rate metric, not this one, and this family must not preempt that
/// decision by shipping a shape it did not choose.
fn render_cache_family(out: &mut String, mode: Mode, snapshot: &CacheMetricsSnapshot) {
    write_header(
        out,
        "ravel_cache_hits_total",
        "Read-cache lookups served from the cache.",
        "counter",
    );
    write_sample(
        out,
        "ravel_cache_hits_total",
        &[Label::Mode(mode)],
        snapshot.hits,
    );

    write_header(
        out,
        "ravel_cache_misses_total",
        "Read-cache lookups not found in the cache.",
        "counter",
    );
    write_sample(
        out,
        "ravel_cache_misses_total",
        &[Label::Mode(mode)],
        snapshot.misses,
    );

    write_header(
        out,
        "ravel_cache_bytes_served_total",
        "Bytes served from the cache on a hit.",
        "counter",
    );
    write_sample(
        out,
        "ravel_cache_bytes_served_total",
        &[Label::Mode(mode)],
        snapshot.bytes_served,
    );

    write_header(
        out,
        "ravel_cache_bytes_admitted_total",
        "Bytes admitted into the cache after a miss.",
        "counter",
    );
    write_sample(
        out,
        "ravel_cache_bytes_admitted_total",
        &[Label::Mode(mode)],
        snapshot.bytes_admitted,
    );

    write_header(
        out,
        "ravel_cache_evictions_total",
        "Entries evicted from the read cache by its S3-FIFO policy.",
        "counter",
    );
    write_sample(
        out,
        "ravel_cache_evictions_total",
        &[Label::Mode(mode)],
        snapshot.evictions,
    );

    write_header(
        out,
        "ravel_cache_disk_errors_degraded_to_misses_total",
        "Disk-tier reads that found an entry at its canonical path but discarded it (short \
         read, bad header, key mismatch, or a failed crc32c check) rather than a clean miss. \
         Nonzero here means the disk tier is unhealthy, not merely cold.",
        "counter",
    );
    write_sample(
        out,
        "ravel_cache_disk_errors_degraded_to_misses_total",
        &[Label::Mode(mode)],
        snapshot.disk_errors_degraded_to_misses,
    );
}

/// The per-(tenant, signal) admission counters (ADR-0051 section 6), read
/// from [`AdmissionController::usage_snapshot`] at scrape time and paired with
/// the `--metrics-tenant-labels` decision, matching every other family's
/// snapshot-plus-config shape ([`CatalogCountersSnapshot`]). `tenant_labels`
/// off (the default) folds every tenant's row into `tenant_hash="other"` and
/// sums, so the exposition's cardinality is bounded by the closed [`Signal`]
/// and [`RejectReason`] enums alone, regardless of tenant count; on, each
/// observed tenant keeps its own `tenant_hash`, one set of counters per
/// (tenant, signal). The fold is the same bounded-cardinality mechanism
/// [`TenantHashLabel`] provides everywhere else, and the flag is the opt-in
/// ADR-0044 section 4 blocked per-tenant series on: turned on only where the
/// operator attests the scrape network is trusted (ADR-0051 section 6).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AdmissionCountersSnapshot {
    pub usage: Vec<TenantUsage>,
    pub tenant_labels: bool,
}

/// The counters this family sums per rendered series. Split out so the fold
/// (many tenants into `other`) and the per-tenant case share one accumulator;
/// summing the active-series gauge across folded tenants is correct, it is the
/// fleet-total active count for that signal.
#[derive(Debug, Clone, Copy, Default)]
struct AdmissionAcc {
    active: u64,
    requests_admitted: u64,
    bytes_admitted: u64,
    rejected_byte_rate: u64,
    rejected_series_rate: u64,
    rejected_series_cap: u64,
}

impl AdmissionAcc {
    fn rejected(&self, reason: RejectReason) -> u64 {
        match reason {
            RejectReason::ByteRate => self.rejected_byte_rate,
            RejectReason::SeriesRate => self.rejected_series_rate,
            RejectReason::SeriesCap => self.rejected_series_cap,
        }
    }
}

/// `Some(hash)` renders that tenant's real hash; `None` renders `other`.
fn tenant_label(hash: Option<TenantHash>) -> TenantHashLabel {
    match hash {
        Some(hash) => TenantHashLabel::Hash(hash),
        None => TenantHashLabel::Other,
    }
}

fn render_admission_family(out: &mut String, mode: Mode, snapshot: &AdmissionCountersSnapshot) {
    // Fold to (tenant_hash key, signal). With tenant labels off every row keys
    // to `None` (rendered `tenant_hash="other"`) and its counters sum, so N
    // tenants collapse to one series per signal and the exposition's
    // cardinality never grows with tenant count; with them on each observed
    // tenant keeps its own hash, one series set per (tenant, signal).
    let mut rows: std::collections::HashMap<(Option<TenantHash>, Signal), AdmissionAcc> =
        std::collections::HashMap::new();
    for row in &snapshot.usage {
        let key = (
            snapshot.tenant_labels.then_some(row.tenant_hash),
            row.signal,
        );
        let acc = rows.entry(key).or_default();
        acc.active = acc.active.saturating_add(row.active_series);
        acc.requests_admitted = acc
            .requests_admitted
            .saturating_add(row.requests_admitted_total);
        acc.bytes_admitted = acc.bytes_admitted.saturating_add(row.bytes_admitted_total);
        acc.rejected_byte_rate = acc
            .rejected_byte_rate
            .saturating_add(row.requests_rejected_byte_rate_total);
        acc.rejected_series_rate = acc
            .rejected_series_rate
            .saturating_add(row.requests_rejected_series_rate_total);
        acc.rejected_series_cap = acc
            .rejected_series_cap
            .saturating_add(row.series_rejected_cap_total);
    }

    // A HashMap iterates in an unspecified order; Prometheus does not require
    // sorted output, but a stable render keeps scrapes and test assertions
    // diffable. Order by tenant label then signal name.
    let mut ordered: Vec<((Option<TenantHash>, Signal), AdmissionAcc)> = rows.into_iter().collect();
    ordered.sort_by(|(a_key, _), (b_key, _)| {
        tenant_label(a_key.0)
            .value()
            .cmp(&tenant_label(b_key.0).value())
            .then_with(|| signal_name(a_key.1).cmp(signal_name(b_key.1)))
    });

    // Every sample carries `mode` like every other family here (the module
    // docs' invariant), in addition to the {tenant_hash, signal[, reason]}
    // dimensions ADR-0051 section 6 names.
    fn labels(mode: Mode, hash: Option<TenantHash>, signal: Signal) -> [Label; 3] {
        [
            Label::Mode(mode),
            Label::TenantHash(tenant_label(hash)),
            Label::Signal(signal),
        ]
    }

    write_header(
        out,
        "ravel_admission_active_series",
        "Active series (metrics) or streams (logs) tracked for the active-cap, by tenant and \
         signal.",
        "gauge",
    );
    for ((hash, signal), acc) in &ordered {
        write_sample(
            out,
            "ravel_admission_active_series",
            &labels(mode, *hash, *signal),
            acc.active,
        );
    }

    write_header(
        out,
        "ravel_admission_admitted_total",
        "Requests admitted past the ingest byte-rate layer, by tenant and signal.",
        "counter",
    );
    for ((hash, signal), acc) in &ordered {
        write_sample(
            out,
            "ravel_admission_admitted_total",
            &labels(mode, *hash, *signal),
            acc.requests_admitted,
        );
    }

    write_header(
        out,
        "ravel_admission_admitted_bytes_total",
        "Wire body bytes admitted past the ingest byte-rate layer, by tenant and signal.",
        "counter",
    );
    for ((hash, signal), acc) in &ordered {
        write_sample(
            out,
            "ravel_admission_admitted_bytes_total",
            &labels(mode, *hash, *signal),
            acc.bytes_admitted,
        );
    }

    write_header(
        out,
        "ravel_admission_rejected_total",
        "Admission rejections by tenant, signal, and reason (byte_rate, series_rate, series_cap).",
        "counter",
    );
    for ((hash, signal), acc) in &ordered {
        for reason in RejectReason::ALL {
            let mut sample_labels = labels(mode, *hash, *signal).to_vec();
            sample_labels.push(Label::RejectReason(reason));
            write_sample(
                out,
                "ravel_admission_rejected_total",
                &sample_labels,
                acc.rejected(reason),
            );
        }
    }
}

/// One (tenant bucket, workload class) row's accumulated per-query cost
/// counters (issue #425, ADR-0044 section 1 and 3). Both the actuals summed
/// from each query's [`QueryAccountingSnapshot`] and the estimates summed from
/// each query's [`CostEstimate`] live here side by side, but they render as
/// separate metric families ([`render_query_family`]): the estimate never
/// replaces the actual, so their divergence stays directly measurable (ADR-0044
/// section 3, "the estimate's accuracy is itself a measurable quantity").
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryCostCounters {
    /// Queries that recorded accounting into this row (the denominator an
    /// operator divides the sums by for a per-query average).
    pub queries: u64,
    pub s3_requests: u64,
    pub s3_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub decompressed_bytes: u64,
    pub estimated_requests: u64,
    pub estimated_store_bytes: u64,
    pub estimated_decompressed_bytes: u64,
}

/// One rendered row of the per-query cost family: the (tenant bucket, workload
/// class) key plus its accumulated [`QueryCostCounters`]. `tenant` is `None`
/// for the folded `other` bucket and `Some(hash)` for a configured tenant, the
/// same convention [`tenant_label`] renders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryAccountingRow {
    pub tenant: Option<TenantHash>,
    pub workload_class: WorkloadClass,
    pub counters: QueryCostCounters,
}

/// Process-global aggregator for per-query cost accounting (ADR-0044 section
/// 4), written once per completed query by every query handler and read at
/// scrape time by [`metrics_handler`]. One instance per process, shared with
/// each query path's handler state, so it is the query analogue of the
/// process-global `StoreMetrics`.
///
/// # Bounded cardinality by a record-time fold
///
/// The `configured` set is the per-tenant allowlist ADR-0044 section 4 names:
/// a query for a tenant in it records under that tenant's real `tenant_hash`;
/// every other tenant folds into the shared `other` bucket *at record time*,
/// so an unconfigured tenant can never allocate a new row no matter how much
/// traffic it drives. Cardinality is therefore bounded by
/// `(configured.len() + 1) * WorkloadClass` regardless of how many distinct
/// tenants query, which is the whole point of the allowlist. The set is empty
/// unless `--metrics-tenant-labels` is set (ADR-0051 section 6): on this
/// unauthenticated route a real `tenant_hash` discloses one tenant's query
/// volumes, so per-tenant query series are gated on the same operator opt-in
/// the admission family's are (ADR-0044 consequences, "blocked on an
/// authentication decision").
#[derive(Debug)]
pub struct QueryAccountingMetrics {
    configured: HashSet<TenantHash>,
    rows: parking_lot::Mutex<HashMap<(Option<TenantHash>, WorkloadClass), QueryCostCounters>>,
}

impl QueryAccountingMetrics {
    /// A new aggregator whose per-tenant allowlist is `configured`; every
    /// tenant outside it folds into `other`. Pass an empty set for the
    /// cardinality-safe default (every tenant folds).
    pub fn new(configured: HashSet<TenantHash>) -> Self {
        QueryAccountingMetrics {
            configured,
            rows: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Fold one completed query's actual counters and its pre-execution
    /// estimate into the (tenant bucket, workload class) row. Called once per
    /// query, off the hot per-store-call path, so a plain mutex is cheaper
    /// than a fixed atomic block that could not key on an open tenant set.
    pub fn record(
        &self,
        tenant_hash: TenantHash,
        workload_class: WorkloadClass,
        accounting: &QueryAccountingSnapshot,
        estimate: &CostEstimate,
    ) {
        // The fold that bounds cardinality: a non-configured tenant keys to
        // `None` (the `other` bucket) here, at record time, so it never
        // allocates a row of its own.
        let bucket = self
            .configured
            .contains(&tenant_hash)
            .then_some(tenant_hash);
        let mut rows = self.rows.lock();
        let acc = rows.entry((bucket, workload_class)).or_default();
        acc.queries = acc.queries.saturating_add(1);
        acc.s3_requests = acc
            .s3_requests
            .saturating_add(accounting.total_s3_requests());
        acc.s3_bytes = acc.s3_bytes.saturating_add(accounting.total_s3_bytes());
        acc.cache_hits = acc.cache_hits.saturating_add(accounting.cache_hits);
        acc.cache_misses = acc.cache_misses.saturating_add(accounting.cache_misses);
        acc.decompressed_bytes = acc
            .decompressed_bytes
            .saturating_add(accounting.decompressed_bytes);
        acc.estimated_requests = acc
            .estimated_requests
            .saturating_add(estimate.estimated_requests);
        acc.estimated_store_bytes = acc
            .estimated_store_bytes
            .saturating_add(estimate.estimated_store_bytes);
        acc.estimated_decompressed_bytes = acc
            .estimated_decompressed_bytes
            .saturating_add(estimate.estimated_decompressed_bytes);
    }

    /// A stable-ordered copy of every observed row, for rendering. Order by
    /// tenant label then workload class name so scrapes and test assertions
    /// stay diffable (a `HashMap` iterates in an unspecified order), matching
    /// [`render_admission_family`]'s discipline.
    pub fn snapshot(&self) -> Vec<QueryAccountingRow> {
        let rows = self.rows.lock();
        let mut out: Vec<QueryAccountingRow> = rows
            .iter()
            .map(|((tenant, workload_class), counters)| QueryAccountingRow {
                tenant: *tenant,
                workload_class: *workload_class,
                counters: *counters,
            })
            .collect();
        out.sort_by(|a, b| {
            tenant_label(a.tenant)
                .value()
                .cmp(&tenant_label(b.tenant).value())
                .then_with(|| a.workload_class.name().cmp(b.workload_class.name()))
        });
        out
    }
}

/// The recorder seam (ADR-0044 section 4, issue #425): this is what lets the
/// Prometheus-shaped query handlers in `ravel-query` and the Flight SQL path in
/// `ravel-sql` fold their per-query cost into this process-global aggregator
/// without depending on `services/ravel-server`. Both hold an
/// `Arc<dyn QueryCostRecorder>`; a deployment hands them this type, so all four
/// read surfaces (PromQL instant/range, PromQL labels/series, Flight SQL, and
/// the HTTP SQL and analytics paths wired in `sql.rs`/`analytics.rs`) sum into
/// one `ravel_query_*` family.
///
/// The fold is bounded and non-blocking, as the trait requires: it maps the
/// bounded workload class and takes the row mutex briefly in
/// [`QueryAccountingMetrics::record`].
impl QueryCostRecorder for QueryAccountingMetrics {
    fn record(
        &self,
        accounting: &QueryAccountingSnapshot,
        estimate: &CostEstimate,
        tenant_hash: TenantHash,
        workload_class: QueryWorkloadClass,
    ) {
        let workload = match workload_class {
            QueryWorkloadClass::Interactive => WorkloadClass::Interactive,
            QueryWorkloadClass::Background => WorkloadClass::Background,
        };
        // Fully qualified so this resolves to the inherent fold method, not this
        // very trait method, which shares its name.
        QueryAccountingMetrics::record(self, tenant_hash, workload, accounting, estimate);
    }
}

/// The per-query cost family (issue #425, ADR-0044 section 4). Every sample
/// carries `mode`, `tenant_hash`, and `workload_class`, all closed or
/// allowlist-bounded (see [`QueryAccountingMetrics`] for the tenant fold). The
/// estimate series (`*_estimated_*`) render beside the actuals under distinct
/// names, never in place of them, so `estimated_requests / s3_requests` and the
/// like are computable in PromQL: ADR-0044 section 3 asks for both precisely so
/// the estimate's divergence from the actual is measurable before a later ADR
/// enforces on it.
fn render_query_family(out: &mut String, mode: Mode, rows: &[QueryAccountingRow]) {
    fn labels(mode: Mode, row: &QueryAccountingRow) -> [Label; 3] {
        [
            Label::Mode(mode),
            Label::TenantHash(tenant_label(row.tenant)),
            Label::WorkloadClass(row.workload_class),
        ]
    }

    write_header(
        out,
        "ravel_query_queries_total",
        "Completed queries that reported cost accounting, by tenant and workload class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_queries_total",
            &labels(mode, row),
            row.counters.queries,
        );
    }

    write_header(
        out,
        "ravel_query_s3_requests_total",
        "Actual object-store requests issued by accounted queries, by tenant and workload class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_s3_requests_total",
            &labels(mode, row),
            row.counters.s3_requests,
        );
    }

    write_header(
        out,
        "ravel_query_s3_bytes_total",
        "Actual object-store bytes transferred by accounted queries, by tenant and workload class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_s3_bytes_total",
            &labels(mode, row),
            row.counters.s3_bytes,
        );
    }

    write_header(
        out,
        "ravel_query_cache_hits_total",
        "In-process read-cache hits attributed to accounted queries, by tenant and workload class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_cache_hits_total",
            &labels(mode, row),
            row.counters.cache_hits,
        );
    }

    write_header(
        out,
        "ravel_query_cache_misses_total",
        "In-process read-cache misses attributed to accounted queries, by tenant and workload \
         class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_cache_misses_total",
            &labels(mode, row),
            row.counters.cache_misses,
        );
    }

    write_header(
        out,
        "ravel_query_decompressed_bytes_total",
        "Actual decompressed sample bytes decoded by accounted queries, by tenant and workload \
         class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_decompressed_bytes_total",
            &labels(mode, row),
            row.counters.decompressed_bytes,
        );
    }

    // The estimate families: separate names from the actuals above, per
    // ADR-0044 section 3. An estimate that silently replaced the actual would
    // defeat the reason the ADR records both.
    write_header(
        out,
        "ravel_query_estimated_requests_total",
        "Pre-execution upper-envelope estimate of object-store requests, summed over accounted \
         queries, by tenant and workload class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_estimated_requests_total",
            &labels(mode, row),
            row.counters.estimated_requests,
        );
    }

    write_header(
        out,
        "ravel_query_estimated_store_bytes_total",
        "Pre-execution upper-envelope estimate of object-store bytes, summed over accounted \
         queries, by tenant and workload class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_estimated_store_bytes_total",
            &labels(mode, row),
            row.counters.estimated_store_bytes,
        );
    }

    write_header(
        out,
        "ravel_query_estimated_decompressed_bytes_total",
        "Pre-execution upper-envelope estimate of decompressed sample bytes, summed over accounted \
         queries, by tenant and workload class.",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_query_estimated_decompressed_bytes_total",
            &labels(mode, row),
            row.counters.estimated_decompressed_bytes,
        );
    }
}

/// Render every source this module knows about into one Prometheus text
/// exposition document. `ingest` is empty in a mode that builds no ingest
/// router (`Mode::Query`, `Mode::Maintain`): those families are omitted
/// entirely rather than rendered with no samples, since the pipelines
/// structurally do not exist in that mode. `store` and `catalog` are always
/// present: the store and the catalog are built in every mode. `maintain` is
/// `None` in every mode but [`Mode::Maintain`], the only mode that runs
/// [`crate::maintain::spawn`]. `admission` is always present: the controller
/// is built in every mode (ADR-0051), and renders no per-tenant samples in a
/// mode that serves no ingest.
// One argument per metric source, each a distinct snapshot type: bundling
// them into one struct would only move the same list behind a name without
// removing a caller's need to build every field, so the sources stay
// positional and this lint is allowed here rather than worked around.
#[allow(clippy::too_many_arguments)]
pub fn render(
    mode: Mode,
    store: &StoreMetricsSnapshot,
    ingest: &[IngestPipelineSnapshot],
    catalog: &CatalogCountersSnapshot,
    maintain: Option<&MaintenanceDiscoverySnapshot>,
    maintain_safety: Option<&MaintenanceSafetySnapshot>,
    cache: Option<&CacheMetricsSnapshot>,
    admission: &AdmissionCountersSnapshot,
    query_accounting: &[QueryAccountingRow],
) -> String {
    let mut out = String::new();
    render_store_family(&mut out, mode, store);
    if !ingest.is_empty() {
        render_ingest_family(&mut out, mode, ingest);
        render_logs_postings_family(&mut out, mode, ingest);
    }
    render_catalog_family(&mut out, mode, catalog);
    render_tenancy_family(&mut out, mode, crate::tenancy::v1_unkeyed_adoption_count());
    render_provisioning_family(
        &mut out,
        mode,
        crate::provisioning::shard_count_mismatch_count(),
    );
    render_store_probe_family(
        &mut out,
        mode,
        crate::store_probe::store_reachable(),
        crate::store_probe::probe_failures_total(),
    );
    render_query_postings_family(&mut out, mode, crate::query_postings_metrics::snapshot());
    if let Some(snapshot) = maintain {
        render_maintain_family(&mut out, mode, snapshot);
    }
    if let Some(snapshot) = maintain_safety {
        render_maintain_safety_family(&mut out, mode, snapshot);
    }
    if let Some(snapshot) = cache {
        render_cache_family(&mut out, mode, snapshot);
    }
    render_admission_family(&mut out, mode, admission);
    render_query_family(&mut out, mode, query_accounting);
    out
}

/// Router state for `GET /metrics`. Every field is a handle already built by
/// [`crate::start`]; the handler below reads through them at scrape time
/// (atomic loads only) rather than baking a snapshot in at construction.
#[derive(Clone)]
pub struct MetricsState {
    pub mode: Mode,
    pub store_metrics: Arc<StoreMetrics>,
    pub ingest_router: Option<Arc<IngestRouter>>,
    pub log_ingest_router: Option<Arc<LogIngestRouter>>,
    pub span_ingest_router: Option<Arc<SpanIngestRouter>>,
    pub catalog: Arc<Catalog>,
    /// `Some` only in [`Mode::Maintain`], the one mode that spawns
    /// [`crate::maintain::spawn`] and therefore has tenant discovery counters
    /// to render (ADR-0048 decision 3, issue #504).
    pub tenant_discovery: Option<Arc<crate::tenant_discovery::TenantDiscoveryMetrics>>,
    /// `Some` only in [`Mode::Maintain`], alongside `tenant_discovery` above
    /// (ADR-0048 decisions 1, 4, 6; issue #517).
    pub maintenance_safety: Option<Arc<crate::maintain::MaintenanceSafetyMetrics>>,
    /// The ADR-0046 read cache's counters handle (issues #445, #502), or
    /// `None` when `--disable-cache` leaves no cache constructed at all.
    pub cache_metrics: Option<Arc<ravel_cache::CacheMetrics>>,
    /// The one process-wide admission controller (ADR-0051), shared with every
    /// ingest path. Always present (built in every mode); in a mode that
    /// serves no ingest its `usage_snapshot` is simply empty, so the admission
    /// family renders its headers with no per-tenant samples.
    pub admission: Arc<AdmissionController>,
    /// `--metrics-tenant-labels` (ADR-0051 section 6, default off): off folds
    /// every tenant's admission counters into `tenant_hash="other"`; on renders
    /// each observed tenant's real hash. Off keeps the exposition's cardinality
    /// bounded regardless of tenant count, which is why it is opt-in.
    pub metrics_tenant_labels: bool,
    /// The process-global per-query cost aggregator (issue #425, ADR-0044
    /// section 4), written by every query handler and read here at scrape time.
    /// Always present; renders no samples until a query records into it.
    pub query_accounting: Arc<QueryAccountingMetrics>,
}

/// `GET /metrics`, mounted in every mode (ADR-0044 section 4). Reads only
/// in-memory atomics: no object-store call, unlike `/readyz`'s deliberate
/// avoidance of one for the same underlying reason (probe cost and blast
/// radius).
async fn metrics_handler(State(state): State<MetricsState>) -> impl IntoResponse {
    let store_snapshot = state.store_metrics.snapshot();

    let mut pipelines = Vec::new();
    if let Some(router) = &state.ingest_router {
        pipelines.push(IngestPipelineSnapshot::from_metrics(
            router.metrics().snapshot(),
        ));
    }
    if let Some(router) = &state.log_ingest_router {
        pipelines.push(IngestPipelineSnapshot::from_log_metrics(
            router.metrics().snapshot(),
        ));
    }
    if let Some(router) = &state.span_ingest_router {
        pipelines.push(IngestPipelineSnapshot::from_span_metrics(
            router.metrics().snapshot(),
        ));
    }

    let catalog_snapshot = CatalogCountersSnapshot {
        interlock_violations: state.catalog.interlock_violations(),
        compaction_input_set_conflicts: state.catalog.compaction_input_set_conflicts(),
        isolation_breaches: state.catalog.isolation_breaches(),
    };

    let maintain_snapshot =
        state
            .tenant_discovery
            .as_ref()
            .map(|metrics| MaintenanceDiscoverySnapshot {
                tenants_discovered: metrics.tenants_discovered(),
                tenants_maintained: metrics.tenants_maintained(),
                tenant_discovery_failures: metrics.discovery_failures(),
            });

    let maintain_safety_snapshot =
        state
            .maintenance_safety
            .as_ref()
            .map(|metrics| MaintenanceSafetySnapshot {
                legal_hold_refresh_failures: metrics.legal_hold_refresh_failures(),
                signals: crate::maintain::MAINTAINED_SIGNALS
                    .iter()
                    .map(|&signal| MaintenanceSafetySignalSnapshot {
                        signal,
                        conservation_aborts: metrics.conservation_aborts(signal),
                        orphan_breaker_trips: metrics.orphan_breaker_trips(signal),
                        orphans_withheld: metrics.orphans_withheld(signal),
                    })
                    .collect(),
            });

    let cache_snapshot = state
        .cache_metrics
        .as_ref()
        .map(|metrics| metrics.snapshot());

    // Read the admission counters at scrape time (a lock-and-copy, no
    // `.await`), like every other family, rather than baking a snapshot in at
    // construction.
    let admission_snapshot = AdmissionCountersSnapshot {
        usage: state.admission.usage_snapshot(),
        tenant_labels: state.metrics_tenant_labels,
    };

    // Per-query cost rows, read at scrape time like every other family (a
    // lock-and-copy, no `.await`).
    let query_rows = state.query_accounting.snapshot();

    let body = render(
        state.mode,
        &store_snapshot,
        &pipelines,
        &catalog_snapshot,
        maintain_snapshot.as_ref(),
        maintain_safety_snapshot.as_ref(),
        cache_snapshot.as_ref(),
        &admission_snapshot,
        &query_rows,
    );
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

/// Router carrying `GET /metrics`, mirroring [`crate::health::router`]'s
/// pattern of baking its state in with `with_state` so the returned `Router`
/// merges into the main router like every other mode's routes.
pub fn router(state: MetricsState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::HashSet;

    use ravel_object_store::instrument::{OpMetricsSnapshot, STORE_ERROR_CLASS_COUNT};

    use super::*;

    fn populated_store_snapshot() -> StoreMetricsSnapshot {
        let mut errors = [0u64; STORE_ERROR_CLASS_COUNT];
        errors[StoreErrorClass::NotFound.index()] = 2;
        let mut latency_micros_buckets = [0u64; LATENCY_BUCKET_COUNT];
        latency_micros_buckets[0] = 3;
        latency_micros_buckets[2] = 4;
        let get = OpMetricsSnapshot {
            calls: 7,
            ok: 5,
            errors,
            bytes: 4096,
            latency_micros_buckets,
            latency_nanos_total: 900_000,
        };
        StoreMetricsSnapshot {
            get,
            ..StoreMetricsSnapshot::default()
        }
    }

    /// THE ACCEPTANCE TEST for issue #423. Proves both halves: a populated
    /// `StoreMetrics` snapshot renders to well-formed exposition text with the
    /// expected sample names and values, and the renderer's label API cannot
    /// express a label outside ADR-0044 section 4's allowlist.
    #[test]
    fn exposition_renders_store_metrics_and_rejects_unlisted_labels() {
        let snapshot = populated_store_snapshot();
        let body = render(
            Mode::All,
            &snapshot,
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        assert!(
            body.contains("ravel_store_calls_total{mode=\"all\",op=\"get\"} 7"),
            "missing calls sample:\n{body}"
        );
        assert!(
            body.contains("ravel_store_ok_total{mode=\"all\",op=\"get\"} 5"),
            "missing ok sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_store_errors_total{mode=\"all\",op=\"get\",error_kind=\"not_found\"} 2"
            ),
            "missing errors sample:\n{body}"
        );
        assert!(
            body.contains("ravel_store_bytes_total{mode=\"all\",op=\"get\"} 4096"),
            "missing bytes sample:\n{body}"
        );
        assert!(
            body.contains("ravel_store_latency_seconds_count{mode=\"all\",op=\"get\"} 7"),
            "missing histogram count sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_store_latency_seconds_bucket{mode=\"all\",op=\"get\",le=\"+Inf\"} 7"
            ),
            "overflow bucket must equal the count:\n{body}"
        );

        // Half two: the label API makes an unlisted label unrepresentable.
        // This match has no wildcard arm, so a ninth `Label` variant fails
        // this compile until a case is added here, and the fixed array below
        // then fails the length assertion until it is extended too -- two
        // independent breaks for one added variant, by design. `reason` is the
        // eighth, added by ADR-0051 section 6 for the admission family.
        let one_of_each = [
            Label::TenantHash(TenantHashLabel::Other),
            Label::Signal(Signal::Metrics),
            Label::Mode(Mode::All),
            Label::Op(StoreOp::Get),
            Label::ErrorKind(StoreErrorClass::NotFound),
            Label::WorkloadClass(WorkloadClass::Interactive),
            Label::Level(Level::Info),
            Label::RejectReason(RejectReason::ByteRate),
        ];
        let keys: Vec<&'static str> = one_of_each
            .iter()
            .map(|label| match label {
                Label::TenantHash(_) => "tenant_hash",
                Label::Signal(_) => "signal",
                Label::Mode(_) => "mode",
                Label::Op(_) => "op",
                Label::ErrorKind(_) => "error_kind",
                Label::WorkloadClass(_) => "workload_class",
                Label::Level(_) => "level",
                Label::RejectReason(_) => "reason",
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                "tenant_hash",
                "signal",
                "mode",
                "op",
                "error_kind",
                "workload_class",
                "level",
                "reason",
            ],
            "ADR-0044 section 4's allowlist plus ADR-0051 section 6's `reason`; `shard` must \
             never appear here"
        );
        assert_eq!(one_of_each.len(), 8, "exactly 8 permitted label keys");
    }

    /// The POSTINGS family (issue #511) renders one sample per metric for the
    /// log pipeline, each carrying exactly the labels the ADR-0044 allowlist
    /// permits for it: `{mode, signal}` and nothing else. The label *set* is
    /// asserted, not just the values, so a future stray label (a field name,
    /// say) fails here loudly rather than silently unbounding `/metrics`
    /// cardinality. Metrics and spans build no POSTINGS, so they render no
    /// sample in this family.
    #[test]
    fn postings_family_carries_only_allowlisted_labels() {
        let ingest = vec![
            IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot {
                postings_objects: 3,
                postings_bytes_total: 900,
                postings_indexed_fields_total: 6,
                postings_distinct_values_total: 42,
                postings_capped_fields_total: 1,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot::default()),
            IngestPipelineSnapshot::from_span_metrics(SpanIngestMetricsSnapshot::default()),
        ];
        let body = render(
            Mode::Gateway,
            &populated_store_snapshot(),
            &ingest,
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        let postings_lines: Vec<&str> = body
            .lines()
            .filter(|l| l.starts_with("ravel_logs_postings_"))
            .collect();
        // Five metrics, one sample each (only the log pipeline has postings).
        assert_eq!(
            postings_lines.len(),
            5,
            "one sample per postings metric, log pipeline only:\n{body}"
        );

        for line in &postings_lines {
            let labels = line
                .split_once('{')
                .and_then(|(_, rest)| rest.split_once('}'))
                .map(|(inner, _)| inner)
                .expect("sample carries a label block");
            let keys: HashSet<&str> = labels
                .split(',')
                .map(|kv| kv.split_once('=').expect("label is key=value").0)
                .collect();
            assert_eq!(
                keys,
                HashSet::from(["mode", "signal"]),
                "postings sample must carry only {{mode, signal}}: {line}"
            );
            // And the sample is the logs signal, not metrics or spans.
            assert!(
                line.contains("signal=\"logs\""),
                "postings is a log-only family: {line}"
            );
        }
    }

    /// The prune-selectivity family (issue #511) renders its three counters,
    /// each labelled with exactly `{mode, signal="logs"}` and nothing more.
    /// Rendered directly rather than through the process-global so the values
    /// are deterministic and do not race another test's queries.
    #[test]
    fn prune_selectivity_family_carries_only_allowlisted_labels() {
        let mut out = String::new();
        // total=100, survived=12, pruned=88.
        render_query_postings_family(&mut out, Mode::Query, (100, 12, 88));

        let sample_lines: Vec<&str> = out
            .lines()
            .filter(|l| l.starts_with("ravel_logs_prune_"))
            .collect();
        assert_eq!(
            sample_lines.len(),
            3,
            "three counters, one sample each:\n{out}"
        );

        for line in &sample_lines {
            let labels = line
                .split_once('{')
                .and_then(|(_, rest)| rest.split_once('}'))
                .map(|(inner, _)| inner)
                .expect("sample carries a label block");
            let keys: HashSet<&str> = labels
                .split(',')
                .map(|kv| kv.split_once('=').expect("label is key=value").0)
                .collect();
            assert_eq!(
                keys,
                HashSet::from(["mode", "signal"]),
                "prune sample must carry only {{mode, signal}}: {line}"
            );
            assert!(line.contains("signal=\"logs\""), "logs-only family: {line}");
        }
        // The survived (numerator) and total (denominator) both render, so a
        // scraper can form the ratio.
        assert!(out.contains("ravel_logs_prune_blocks_total{mode=\"query\",signal=\"logs\"} 100"));
        assert!(
            out.contains(
                "ravel_logs_prune_blocks_survived_total{mode=\"query\",signal=\"logs\"} 12"
            )
        );
    }

    /// Every non-comment line is `name{labels} value`, every `# TYPE`
    /// precedes its samples, and histogram buckets are non-decreasing with
    /// the last bucket equal to `_count`.
    #[test]
    fn exposition_output_parses_as_valid_text() {
        let store = populated_store_snapshot();
        let ingest = vec![
            IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot {
                flushes_by_size: 1,
                buffered_points_total: 10,
                series_id_collisions: 1,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot {
                flushes_by_age: 2,
                buffered_records_total: 20,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_span_metrics(SpanIngestMetricsSnapshot {
                flushes_manual: 3,
                buffered_spans_total: 30,
                ..Default::default()
            }),
        ];
        let catalog = CatalogCountersSnapshot {
            interlock_violations: 1,
            compaction_input_set_conflicts: 2,
            isolation_breaches: 3,
        };
        let body = render(
            Mode::Gateway,
            &store,
            &ingest,
            &catalog,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        let mut declared_types: HashSet<String> = HashSet::new();
        let mut bucket_state: std::collections::HashMap<String, (Vec<u64>, u64)> =
            std::collections::HashMap::new();
        let mut count_state: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();

        for line in body.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest
                    .split_whitespace()
                    .next()
                    .expect("TYPE line names a metric");
                declared_types.insert(name.to_string());
                continue;
            }
            if line.starts_with('#') {
                continue;
            }
            if line.is_empty() {
                continue;
            }

            let (head, value) = line.rsplit_once(' ').expect("sample has a value token");
            value.parse::<f64>().expect("value token is numeric");

            let (name, labels) = match head.find('{') {
                Some(brace) => {
                    assert!(head.ends_with('}'), "unterminated label block: {line}");
                    (&head[..brace], Some(&head[brace + 1..head.len() - 1]))
                }
                None => (head, None),
            };
            assert!(!name.is_empty(), "sample line has no metric name: {line}");

            let base_name = name
                .strip_suffix("_bucket")
                .or_else(|| name.strip_suffix("_sum"))
                .or_else(|| name.strip_suffix("_count"))
                .unwrap_or(name);
            assert!(
                declared_types.contains(base_name) || declared_types.contains(name),
                "sample {name} rendered before its # TYPE line"
            );

            let mut le = None;
            // The series key for a histogram is its label set with `le`
            // removed, so every bucket of one series lands in one entry. It is
            // built from the parsed pairs, never by string-replacing a Debug
            // rendering of them: an earlier version did that, never matched,
            // gave every bucket its own entry, and made the non-decreasing
            // assertion below unreachable.
            let mut series_key_pairs: Vec<&str> = Vec::new();
            if let Some(labels) = labels {
                assert!(!labels.is_empty(), "empty label block: {line}");
                for pair in labels.split(',') {
                    let (key, quoted) = pair.split_once('=').expect("label is key=value");
                    assert!(!key.is_empty(), "empty label key: {line}");
                    if key != "le" {
                        series_key_pairs.push(pair);
                    }
                    assert!(
                        quoted.starts_with('"') && quoted.ends_with('"') && quoted.len() >= 2,
                        "label value not quoted: {line}"
                    );
                    if key == "le" {
                        le = Some(quoted[1..quoted.len() - 1].to_string());
                    }
                }
            }

            if name.ends_with("_bucket") {
                let le = le.expect("a _bucket sample carries le");
                let entry = bucket_state
                    .entry(format!("{name}{{{}}}", series_key_pairs.join(",")))
                    .or_insert_with(|| (Vec::new(), 0));
                let value: u64 = value.parse().expect("bucket value is an integer");
                if let Some(last) = entry.0.last() {
                    assert!(
                        value >= *last,
                        "histogram buckets must be non-decreasing: {line}"
                    );
                }
                entry.0.push(value);
                if le == "+Inf" {
                    entry.1 = value;
                }
            }

            if name.ends_with("_count") {
                let key = format!(
                    "{}_bucket{{{}}}",
                    name.trim_end_matches("_count"),
                    series_key_pairs.join(",")
                );
                count_state.insert(key, value.parse::<u64>().expect("count is an integer"));
            }
        }

        assert!(
            declared_types.contains("ravel_store_latency_seconds"),
            "histogram TYPE line missing"
        );

        // Every histogram series must have been seen, and its `+Inf` bucket
        // must equal its `_count`. Prometheus requires this, and reading
        // `_count` from a different field than the bucket array is how it
        // gets violated under a concurrent scrape.
        assert!(
            !bucket_state.is_empty(),
            "no histogram series parsed; the series key is wrong and every \
             assertion below it is unreachable"
        );
        for (series, (values, inf)) in &bucket_state {
            assert!(
                values.len() > 1,
                "series {series} has {} bucket(s): the series key is not \
                 grouping buckets, so the non-decreasing check is vacuous",
                values.len()
            );
            let count = count_state
                .get(series)
                .unwrap_or_else(|| panic!("no _count sample for histogram series {series}"));
            assert_eq!(
                *inf, *count,
                "+Inf bucket must equal _count for series {series}"
            );
        }
    }

    #[test]
    fn isolation_breach_counter_renders_at_metrics() {
        let catalog = CatalogCountersSnapshot {
            interlock_violations: 0,
            compaction_input_set_conflicts: 0,
            isolation_breaches: 5,
        };
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &catalog,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        assert!(
            body.contains("ravel_catalog_isolation_breach_total{mode=\"gateway\"} 5"),
            "isolation-breach counter must render its current value:\n{body}"
        );
    }

    #[test]
    fn zero_valued_snapshot_renders_valid_output_not_omitted() {
        let body = render(
            Mode::Maintain,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        assert!(!body.is_empty(), "a zero snapshot must still render text");
        assert!(
            body.contains("ravel_store_calls_total{mode=\"maintain\",op=\"get\"} 0"),
            "zero store counters must render, not be omitted:\n{body}"
        );
        assert!(
            body.contains("ravel_catalog_interlock_violations_total{mode=\"maintain\"} 0"),
            "zero catalog counters must render, not be omitted:\n{body}"
        );
        assert!(
            body.contains("ravel_catalog_isolation_breach_total{mode=\"maintain\"} 0"),
            "zero isolation-breach counter must render, not be omitted:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_store_latency_seconds_bucket{mode=\"maintain\",op=\"get\",le=\"+Inf\"} 0"
            ),
            "zero histogram must still render every bucket:\n{body}"
        );
        assert!(
            !body.contains("ravel_maintain_tenants_discovered"),
            "the maintain family must be omitted entirely when no snapshot is passed, \
             not rendered with zeroes: a mode without tenant discovery has no counters to zero"
        );
        assert!(
            !body.contains("ravel_maintain_conservation_aborts_total"),
            "the maintain safety family must be omitted entirely when no snapshot is passed"
        );
    }

    /// ADR-0048 decision 3 / issue #504: the tenant discovery gauges and
    /// failure counter render through this same closed-label renderer, no
    /// second registry, exactly like every other family here.
    #[test]
    fn maintain_family_renders_tenant_discovery_gauges_and_failure_counter() {
        let snapshot = MaintenanceDiscoverySnapshot {
            tenants_discovered: 5,
            tenants_maintained: 3,
            tenant_discovery_failures: 2,
        };
        let body = render(
            Mode::Maintain,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            Some(&snapshot),
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        assert!(
            body.contains("ravel_maintain_tenants_discovered{mode=\"maintain\"} 5"),
            "missing tenants_discovered sample:\n{body}"
        );
        assert!(
            body.contains("ravel_maintain_tenants_maintained{mode=\"maintain\"} 3"),
            "missing tenants_maintained sample:\n{body}"
        );
        assert!(
            body.contains("ravel_maintain_tenant_discovery_failures_total{mode=\"maintain\"} 2"),
            "missing tenant_discovery_failures sample:\n{body}"
        );
    }

    #[test]
    fn ingest_families_share_one_metric_name_split_by_signal() {
        let ingest = vec![
            IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot {
                flushes_by_size: 5,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot {
                flushes_by_size: 6,
                ..Default::default()
            }),
        ];
        let body = render(
            Mode::All,
            &StoreMetricsSnapshot::default(),
            &ingest,
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        assert!(
            body.contains("ravel_ingest_flushes_by_size_total{mode=\"all\",signal=\"metrics\"} 5")
        );
        assert!(
            body.contains("ravel_ingest_flushes_by_size_total{mode=\"all\",signal=\"logs\"} 6")
        );
        // Spans derive no collision-prone identity, so a span pipeline
        // present in `ingest` still yields no `signal="spans"` collisions
        // sample; not exercised further here since no span pipeline was
        // constructed in this test.
    }

    /// EC7 (ADR-0050 section 7): the store-reachability family renders on this
    /// same closed-label endpoint, in every mode, so a metrics-only monitoring
    /// setup sees an outage even where nothing consumes `/readyz`. No probe runs
    /// in this unit test, so the reachability flag reads its default (healthy).
    #[test]
    fn render_includes_store_probe_family() {
        let body = render(
            Mode::All,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );
        // Default reachability is healthy (1); the process runs no probe here.
        assert!(
            body.contains("ravel_store_reachable{mode=\"all\"} 1"),
            "missing store-reachable gauge:\n{body}"
        );
        assert!(
            body.contains("ravel_store_probe_failures_total{mode=\"all\"} 0"),
            "missing store-probe failure counter:\n{body}"
        );
        // Both carry the standard TYPE headers.
        assert!(body.contains("# TYPE ravel_store_reachable gauge"));
        assert!(body.contains("# TYPE ravel_store_probe_failures_total counter"));
    }

    /// Issue #517: the three maintenance safety controls (ADR-0048 decisions
    /// 1, 4, 6) render on this same closed-label endpoint, no second
    /// registry, exactly like every other family here.
    #[test]
    fn render_includes_maintain_safety_counters() {
        let snapshot = MaintenanceSafetySnapshot {
            legal_hold_refresh_failures: 3,
            signals: vec![
                MaintenanceSafetySignalSnapshot {
                    signal: Signal::Metrics,
                    conservation_aborts: 1,
                    orphan_breaker_trips: 2,
                    orphans_withheld: 7,
                },
                MaintenanceSafetySignalSnapshot {
                    signal: Signal::Logs,
                    conservation_aborts: 0,
                    orphan_breaker_trips: 0,
                    orphans_withheld: 0,
                },
            ],
        };
        let body = render(
            Mode::Maintain,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            Some(&snapshot),
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        assert!(
            body.contains("ravel_maintain_legal_hold_refresh_failures_total{mode=\"maintain\"} 3"),
            "missing legal_hold_refresh_failures sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_maintain_conservation_aborts_total{mode=\"maintain\",signal=\"metrics\"} 1"
            ),
            "missing conservation_aborts sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_maintain_orphan_breaker_tripped_total{mode=\"maintain\",signal=\"metrics\"} 2"
            ),
            "missing orphan_breaker_tripped sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_maintain_orphans_withheld{mode=\"maintain\",signal=\"metrics\"} 7"
            ),
            "missing orphans_withheld sample:\n{body}"
        );
        // The zero-valued signal (logs) still renders, not omitted, matching
        // every other family's zero-is-not-absence discipline.
        assert!(
            body.contains(
                "ravel_maintain_orphan_breaker_tripped_total{mode=\"maintain\",signal=\"logs\"} 0"
            ),
            "a zero-valued signal must still render:\n{body}"
        );
    }

    /// ADR-0044 section 4's allowlist is closed at the `Label` type (see
    /// `exposition_renders_store_metrics_and_rejects_unlisted_labels`), but
    /// that only proves a label *could* be constructed safely, not that this
    /// family declines to construct the unsafe one. ADR-0048 names
    /// `tenant_hash` for these counters; ADR-0044 blocks any per-tenant
    /// series on this unauthenticated route until ADR-0051's opt-in flag
    /// exists, and it does not exist in this codebase today (see
    /// `crate::maintain::MaintenanceSafetyMetrics`'s doc comment). This test
    /// pins the resulting decision -- `mode` and `signal` only, never
    /// `tenant_hash` -- so a later change cannot reintroduce it silently.
    #[test]
    fn maintain_safety_family_never_renders_a_tenant_hash_label() {
        let snapshot = MaintenanceSafetySnapshot {
            legal_hold_refresh_failures: 1,
            signals: vec![MaintenanceSafetySignalSnapshot {
                signal: Signal::Metrics,
                conservation_aborts: 1,
                orphan_breaker_trips: 1,
                orphans_withheld: 1,
            }],
        };
        let body = render(
            Mode::Maintain,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            Some(&snapshot),
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        for line in body.lines() {
            let expected_keys =
                if line.starts_with("ravel_maintain_legal_hold_refresh_failures_total") {
                    vec!["mode"]
                } else if line.starts_with("ravel_maintain_conservation_aborts_total")
                    || line.starts_with("ravel_maintain_orphan_breaker_tripped_total")
                    || line.starts_with("ravel_maintain_orphans_withheld")
                {
                    vec!["mode", "signal"]
                } else {
                    continue;
                };
            let brace = line.find('{').expect("sample line carries labels");
            let labels = &line[brace + 1..line.find('}').expect("closed label block")];
            let keys: Vec<&str> = labels
                .split(',')
                .map(|pair| pair.split_once('=').expect("label is key=value").0)
                .collect();
            assert_eq!(
                keys, expected_keys,
                "maintain-safety sample carries an unexpected label set: {line}"
            );
        }
    }

    #[test]
    fn cache_family_renders_when_present_and_omits_single_flight_collapses() {
        let snapshot = CacheMetricsSnapshot {
            hits: 10,
            misses: 4,
            bytes_served: 2048,
            bytes_admitted: 1024,
            admissions_rejected_size: 1,
            evictions: 2,
            single_flight_collapses: 99,
            disk_errors_degraded_to_misses: 3,
        };
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            Some(&snapshot),
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        assert!(
            body.contains("ravel_cache_hits_total{mode=\"gateway\"} 10"),
            "missing cache hits sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_misses_total{mode=\"gateway\"} 4"),
            "missing cache misses sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_bytes_served_total{mode=\"gateway\"} 2048"),
            "missing cache bytes_served sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_bytes_admitted_total{mode=\"gateway\"} 1024"),
            "missing cache bytes_admitted sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_evictions_total{mode=\"gateway\"} 2"),
            "missing cache evictions sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_disk_errors_degraded_to_misses_total{mode=\"gateway\"} 3"),
            "missing cache disk_errors_degraded_to_misses sample:\n{body}"
        );
        assert!(
            !body.contains("single_flight_collapse"),
            "issue #503: fleet-wide single-flight collapse rate must never be \
             emitted on /metrics, found in:\n{body}"
        );
    }

    #[test]
    fn cache_family_omitted_entirely_when_no_cache_is_attached() {
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
        );

        assert!(
            !body.contains("ravel_cache_"),
            "a server run with --disable-cache must not render any cache \
             family at all:\n{body}"
        );
    }

    fn tenant_usage(tenant: &str, signal: Signal) -> TenantUsage {
        TenantUsage {
            tenant_hash: ravel_types::TenantId::new(tenant).hash(),
            signal,
            active_series: 0,
            requests_admitted_total: 0,
            bytes_admitted_total: 0,
            series_admitted_total: 0,
            requests_rejected_byte_rate_total: 0,
            requests_rejected_series_rate_total: 0,
            series_rejected_cap_total: 0,
        }
    }

    /// Count the distinct `tenant_hash` label values across every
    /// `ravel_admission_*` sample line.
    fn admission_tenant_hashes(body: &str) -> HashSet<String> {
        let mut out = HashSet::new();
        for line in body.lines() {
            if !line.starts_with("ravel_admission_") {
                continue;
            }
            let brace = line.find('{').expect("admission sample carries labels");
            let labels = &line[brace + 1..line.find('}').expect("closed label block")];
            for pair in labels.split(',') {
                if let Some(value) = pair.strip_prefix("tenant_hash=\"") {
                    out.insert(value.trim_end_matches('"').to_string());
                }
            }
        }
        out
    }

    /// Default (`--metrics-tenant-labels` off): every tenant's admission
    /// counters fold into `tenant_hash="other"` and sum, so the exposition's
    /// cardinality is bounded by the closed `Signal`/`RejectReason` enums,
    /// never by tenant count (ADR-0051 section 6). This is the render-level
    /// half of the `metrics_endpoint::admission_family_tenant_labels_bounded`
    /// acceptance test.
    #[test]
    fn admission_family_folds_every_tenant_to_other_by_default() {
        let usage: Vec<TenantUsage> = (0..50)
            .map(|i| {
                let mut row = tenant_usage(&format!("tenant-{i}"), Signal::Metrics);
                row.active_series = 2;
                row.requests_admitted_total = 3;
                row.bytes_admitted_total = 100;
                row.series_rejected_cap_total = 1;
                row
            })
            .collect();
        let snapshot = AdmissionCountersSnapshot {
            usage,
            tenant_labels: false,
        };
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &snapshot,
            &[],
        );

        assert_eq!(
            admission_tenant_hashes(&body),
            HashSet::from(["other".to_string()]),
            "50 tenants must collapse to exactly tenant_hash=\"other\":\n{body}"
        );
        // The fold sums, so the single "other" series carries every tenant's
        // contribution: 50 * 3 admitted requests, 50 * 100 bytes, 50 * 2
        // active, 50 * 1 cap rejections.
        assert!(
            body.contains(
                "ravel_admission_admitted_total{mode=\"gateway\",tenant_hash=\"other\",\
                 signal=\"metrics\"} 150"
            ),
            "folded admitted counter must sum across tenants:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_admission_active_series{mode=\"gateway\",tenant_hash=\"other\",\
                 signal=\"metrics\"} 100"
            ),
            "folded active gauge must sum across tenants:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_admission_rejected_total{mode=\"gateway\",tenant_hash=\"other\",\
                 signal=\"metrics\",reason=\"series_cap\"} 50"
            ),
            "folded cap-rejection counter must sum across tenants:\n{body}"
        );
    }

    /// With `--metrics-tenant-labels` on, each observed tenant keeps its own
    /// real `tenant_hash`, one set of counters per (tenant, signal), and the
    /// three rejection reasons are distinguishable.
    #[test]
    fn admission_family_renders_real_hashes_and_all_reasons_when_enabled() {
        let mut byte_rate = tenant_usage("byte-heavy", Signal::Metrics);
        byte_rate.requests_rejected_byte_rate_total = 4;
        let mut series_rate = tenant_usage("churny", Signal::Logs);
        series_rate.requests_rejected_series_rate_total = 5;
        let mut series_cap = tenant_usage("wide", Signal::Metrics);
        series_cap.series_rejected_cap_total = 6;

        let hashes: Vec<String> = [&byte_rate, &series_rate, &series_cap]
            .iter()
            .map(|row| row.tenant_hash.to_hex())
            .collect();

        let snapshot = AdmissionCountersSnapshot {
            usage: vec![byte_rate, series_rate, series_cap],
            tenant_labels: true,
        };
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &snapshot,
            &[],
        );

        let rendered = admission_tenant_hashes(&body);
        for hash in &hashes {
            assert!(
                rendered.contains(hash),
                "tenant hash {hash} must appear with labels on:\n{body}"
            );
        }
        assert!(
            !rendered.contains("other"),
            "no tenant folds to other with labels on:\n{body}"
        );

        // Each reason is a distinct series with its own counter value.
        assert!(
            body.contains(&format!(
                "ravel_admission_rejected_total{{mode=\"gateway\",tenant_hash=\"{}\",\
                 signal=\"metrics\",reason=\"byte_rate\"}} 4",
                hashes[0]
            )),
            "byte_rate rejection must render distinctly:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "ravel_admission_rejected_total{{mode=\"gateway\",tenant_hash=\"{}\",\
                 signal=\"logs\",reason=\"series_rate\"}} 5",
                hashes[1]
            )),
            "series_rate rejection must render distinctly:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "ravel_admission_rejected_total{{mode=\"gateway\",tenant_hash=\"{}\",\
                 signal=\"metrics\",reason=\"series_cap\"}} 6",
                hashes[2]
            )),
            "series_cap rejection must render distinctly:\n{body}"
        );
    }

    // --- Issue #425: the per-query cost family ---

    use ravel_types::accounting::{CostEstimate, QueryAccountingSnapshot};

    fn tenant_hash(name: &str) -> TenantHash {
        ravel_types::TenantId::new(name).hash()
    }

    /// An accounting snapshot with distinct, non-zero, easily-recognized
    /// counter values so a summed render can be checked against them.
    fn accounting(get_requests: u64, get_bytes: u64, decompressed: u64) -> QueryAccountingSnapshot {
        QueryAccountingSnapshot {
            s3_requests: [get_requests, 0, 0],
            s3_bytes: [get_bytes, 0, 0],
            decompressed_bytes: decompressed,
            ..QueryAccountingSnapshot::default()
        }
    }

    fn render_query_only(metrics: &QueryAccountingMetrics) -> String {
        render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &metrics.snapshot(),
        )
    }

    /// Every distinct `tenant_hash` value across the `ravel_query_*` lines.
    fn query_tenant_hashes(body: &str) -> HashSet<String> {
        let mut out = HashSet::new();
        for line in body.lines() {
            if !line.starts_with("ravel_query_") {
                continue;
            }
            let brace = line.find('{').expect("query sample carries labels");
            let labels = &line[brace + 1..line.find('}').expect("closed label block")];
            for pair in labels.split(',') {
                if let Some(value) = pair.strip_prefix("tenant_hash=\"") {
                    out.insert(value.trim_end_matches('"').to_string());
                }
            }
        }
        out
    }

    /// THE FOLD TEST (issue #425 deliverable 2). With no tenant configured
    /// (the safe default), every tenant's per-query cost folds into
    /// `tenant_hash="other"` *at record time*, so an unconfigured tenant can
    /// never allocate a new series no matter how many distinct tenants query.
    /// Asserts the label *set*, not only the values, so a later change that
    /// leaked a raw hash onto this route would fail here.
    #[test]
    fn query_family_folds_every_unconfigured_tenant_to_other() {
        let metrics = QueryAccountingMetrics::new(HashSet::new());
        // 50 distinct tenants, all unconfigured.
        for i in 0..50 {
            metrics.record(
                tenant_hash(&format!("tenant-{i}")),
                WorkloadClass::Interactive,
                &accounting(2, 100, 10),
                &CostEstimate::new(3, 200, 20, 1, 1),
            );
        }
        let body = render_query_only(&metrics);

        assert_eq!(
            query_tenant_hashes(&body),
            HashSet::from(["other".to_string()]),
            "50 unconfigured tenants must collapse to exactly tenant_hash=\"other\":\n{body}"
        );

        // The label set on every query sample is exactly {mode, tenant_hash,
        // workload_class} -- never a raw per-tenant dimension beyond the fold.
        for line in body.lines() {
            if !line.starts_with("ravel_query_") {
                continue;
            }
            let brace = line.find('{').expect("query sample carries labels");
            let labels = &line[brace + 1..line.find('}').expect("closed label block")];
            let keys: Vec<&str> = labels
                .split(',')
                .map(|pair| pair.split_once('=').expect("label is key=value").0)
                .collect();
            assert_eq!(
                keys,
                vec!["mode", "tenant_hash", "workload_class"],
                "query cost sample carries an unexpected label set: {line}"
            );
        }

        // The fold sums: one `other` series carries every query's contribution
        // (50 queries, 50*2 requests, 50*100 bytes).
        assert!(
            body.contains(
                "ravel_query_queries_total{mode=\"gateway\",tenant_hash=\"other\",\
                 workload_class=\"interactive\"} 50"
            ),
            "folded query count must sum across tenants:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_query_s3_requests_total{mode=\"gateway\",tenant_hash=\"other\",\
                 workload_class=\"interactive\"} 100"
            ),
            "folded request counter must sum across tenants:\n{body}"
        );
    }

    /// A configured tenant keeps its own `tenant_hash`; an unconfigured one
    /// beside it still folds into `other`. Proves the allowlist is per-tenant,
    /// not all-or-nothing.
    #[test]
    fn query_family_renders_configured_tenant_and_folds_the_rest() {
        let configured = tenant_hash("configured");
        let metrics = QueryAccountingMetrics::new(HashSet::from([configured]));
        metrics.record(
            configured,
            WorkloadClass::Interactive,
            &accounting(1, 10, 5),
            &CostEstimate::new(2, 20, 10, 1, 1),
        );
        metrics.record(
            tenant_hash("unconfigured"),
            WorkloadClass::Interactive,
            &accounting(4, 40, 20),
            &CostEstimate::new(8, 80, 40, 1, 1),
        );
        let body = render_query_only(&metrics);

        assert_eq!(
            query_tenant_hashes(&body),
            HashSet::from([configured.to_hex(), "other".to_string()]),
            "the configured tenant keeps its hash; the other folds to \"other\":\n{body}"
        );
    }

    /// The estimate and the actual render as SEPARATE, differently-named
    /// series (issue #425 deliverable 3, ADR-0044 section 3), so their
    /// divergence is directly measurable. A single query with a deliberately
    /// higher estimate than actual proves neither replaced the other.
    #[test]
    fn query_family_estimate_and_actual_are_separate_series() {
        let metrics = QueryAccountingMetrics::new(HashSet::new());
        metrics.record(
            tenant_hash("t"),
            WorkloadClass::Interactive,
            &accounting(7, 700, 70),
            &CostEstimate::new(9, 900, 90, 1, 1),
        );
        let body = render_query_only(&metrics);

        // Actual: 7 requests.
        assert!(
            body.contains(
                "ravel_query_s3_requests_total{mode=\"gateway\",tenant_hash=\"other\",\
                 workload_class=\"interactive\"} 7"
            ),
            "actual request series missing or wrong:\n{body}"
        );
        // Estimate: 9 requests, under a distinct metric name.
        assert!(
            body.contains(
                "ravel_query_estimated_requests_total{mode=\"gateway\",tenant_hash=\"other\",\
                 workload_class=\"interactive\"} 9"
            ),
            "estimate request series missing, wrong, or collapsed onto the actual:\n{body}"
        );
        // The two names are genuinely distinct families in the output.
        assert!(
            body.contains("# TYPE ravel_query_s3_requests_total counter")
                && body.contains("# TYPE ravel_query_estimated_requests_total counter"),
            "estimate and actual must each declare their own TYPE line:\n{body}"
        );
    }

    /// A `background` (alert-evaluation) query and an `interactive` one for the
    /// same tenant bucket stay distinct rows, so the workload split is real.
    #[test]
    fn query_family_splits_interactive_from_background() {
        let metrics = QueryAccountingMetrics::new(HashSet::new());
        metrics.record(
            tenant_hash("t"),
            WorkloadClass::Interactive,
            &accounting(1, 0, 0),
            &CostEstimate::new(0, 0, 0, 0, 0),
        );
        metrics.record(
            tenant_hash("t"),
            WorkloadClass::Background,
            &accounting(1, 0, 0),
            &CostEstimate::new(0, 0, 0, 0, 0),
        );
        let body = render_query_only(&metrics);
        assert!(
            body.contains(
                "ravel_query_queries_total{mode=\"gateway\",tenant_hash=\"other\",\
                 workload_class=\"interactive\"} 1"
            ),
            "interactive row missing:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_query_queries_total{mode=\"gateway\",tenant_hash=\"other\",\
                 workload_class=\"background\"} 1"
            ),
            "background row missing:\n{body}"
        );
    }
}
