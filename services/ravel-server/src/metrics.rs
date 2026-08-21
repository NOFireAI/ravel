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
//! `error_kind`, `workload_class`, `level`, `reason`, and `cache` (ADR-0044
//! section 4; `reason` added by ADR-0051 section 6 for the admission-rejection
//! family and reused by ADR-0059 section 2 for the scrub seal-divergence family,
//! `cache` to split the read-cache family into the
//! fetcher and catalog byte caches). Every variant's payload is a closed enum
//! or [`TenantHash`]'s fixed-width hash, so there is no `String` or `&str`
//! anywhere on this path an unlisted label could travel through, and adding a
//! tenth variant is a compile error everywhere this module matches on `Label`
//! exhaustively. `shard` is
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
//! Adding a new source such as per-query cost accounting on top of this
//! renderer means building a new snapshot-to-[`Label`] mapping and a new family
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
    LogIngestRouter, SpanIngestMetricsSnapshot, SpanIngestRouter, TenantPutAttribution,
    TenantUsage,
};
use ravel_object_store::StoreMetrics;
use ravel_object_store::instrument::{
    LATENCY_BUCKET_BOUNDS_MICROS, LATENCY_BUCKET_COUNT, StoreErrorClass, StoreMetricsSnapshot,
    StoreOp,
};
use ravel_query::http::MetadataCacheCounters;
use ravel_types::accounting::{
    CostEstimate, QueryAccountingSnapshot, QueryCostRecorder, QueryWorkloadClass,
};
use ravel_types::{Signal, TenantHash};

use crate::config::Mode;

/// How a query reached the engine, the `workload_class` label on the
/// per-query cost family (ADR-0044 section 4). A closed set:
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
/// 6, extended by the 2026-08-13 amendment). ADR-0051 named a closed set of
/// six reasons `{body_size, byte_rate, series_rate, series_cap, skew,
/// structural}`; the amendment adds a seventh, `clock`, for the receiver-clock
/// floor. The four here are exactly the ones
/// `AdmissionController::usage_snapshot` counts today
/// (`ravel_ingest::TenantUsage`). The remaining three (body_size, skew,
/// structural) are enforced at layers that keep no per-tenant counter in that
/// snapshot yet (body size at the transport, skew and structural in
/// normalization, surfaced there through OTLP partial success), so a variant
/// for them would render samples no data source can fill. They join this enum
/// when their counters do, additively, the same way a new `Signal` variant
/// joins `signal_name`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    ByteRate,
    SeriesRate,
    SeriesCap,
    /// The receiver's admission clock was implausible (below the 2020 floor or
    /// non-representable), so the whole request was rejected 503 / `UNAVAILABLE`
    /// (ADR-0051 amendment). The fault is the replica's, not the data's.
    Clock,
}

impl RejectReason {
    /// Every reason with a counter, so the rejected family renders all four
    /// series per (tenant, signal) even when some are zero (the same
    /// zero-is-not-absence discipline the other families keep).
    const ALL: [RejectReason; 4] = [
        RejectReason::ByteRate,
        RejectReason::SeriesRate,
        RejectReason::SeriesCap,
        RejectReason::Clock,
    ];

    fn name(self) -> &'static str {
        match self {
            RejectReason::ByteRate => "byte_rate",
            RejectReason::SeriesRate => "series_rate",
            RejectReason::SeriesCap => "series_cap",
            RejectReason::Clock => "clock",
        }
    }
}

/// The `reason` label on `ravel_scrub_seal_divergence_total` (ADR-0059 decision
/// 2). A closed set of two values: `missing` (a sealed commit record
/// absent from the folded snapshot, an under-count) and `mismatched` (a
/// snapshot entry whose `content_hash` disagrees with the sealed record).
/// `orphaned` divergences (a snapshot entry with no surviving commit record) are
/// the expected retention-after-fold shape and deliberately have no label value:
/// they are never counted (see [`crate::scrub`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrubReason {
    Missing,
    Mismatched,
}

impl ScrubReason {
    /// Both reasons, so the family renders a series per (signal, reason) even at
    /// zero, the same zero-is-not-absence discipline every other family keeps.
    const ALL: [ScrubReason; 2] = [ScrubReason::Missing, ScrubReason::Mismatched];

    fn name(self) -> &'static str {
        match self {
            ScrubReason::Missing => "missing",
            ScrubReason::Mismatched => "mismatched",
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
    ScrubReason(ScrubReason),
    Cache(CacheFamily),
    MergeMemoryKind(MergeMemoryKind),
}

/// Which high-water mark a `ravel_maintain_rlog_merge_peak_bytes` sample is
/// (ADR-0065 decision 4): `transient` is the in-flight fetched-minus-released
/// block bytes at any instant during a k-way merge, `total` additionally
/// includes the writer's buffered output bytes. One family, split by this
/// `kind=` label, the same discipline `CacheFamily` above uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMemoryKind {
    Transient,
    Total,
}

impl MergeMemoryKind {
    fn name(self) -> &'static str {
        match self {
            MergeMemoryKind::Transient => "transient",
            MergeMemoryKind::Total => "total",
        }
    }
}

/// Which ADR-0046 read cache a `ravel_cache_*` sample belongs to.
/// Both caches share one metric family and are told apart only by this
/// `cache=` label, the same discipline every other family here uses to split
/// one metric name across a closed dimension. `fetch` is the query fetchers'
/// RAM cache (`ravel_server::store::build_cache`); `catalog` is the catalog's
/// content-addressed byte cache (`ravel_catalog::Catalog`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheFamily {
    Fetch,
    Catalog,
}

impl CacheFamily {
    fn name(self) -> &'static str {
        match self {
            CacheFamily::Fetch => "fetch",
            CacheFamily::Catalog => "catalog",
        }
    }
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
            Label::ScrubReason(_) => "reason",
            Label::Cache(_) => "cache",
            Label::MergeMemoryKind(_) => "kind",
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
            Label::ScrubReason(reason) => reason.name().to_string(),
            Label::Cache(family) => family.name().to_string(),
            Label::MergeMemoryKind(kind) => kind.name().to_string(),
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
/// handled here. Public so `Cli::otlp_export_config` (`config.rs`) can derive
/// the OTLP export `ravel.mode` resource attribute (ADR-0060 decision 5)
/// from this single spelling instead of a second, independently-derived
/// rendering.
pub fn mode_name(mode: Mode) -> &'static str {
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
    /// Write-side POSTINGS counters (ADR-0049). `Some` only for the
    /// log pipeline; `None` for metrics and spans, which build no POSTINGS
    /// section, so the postings family renders no sample for them.
    pub postings: Option<PostingsCounters>,
    /// Metric metadata sink counters (ADR-0085 decision 1). `Some` only for
    /// the metrics pipeline: the sink and its record are metrics-only
    /// concepts, so logs and spans render no sample for this family, the
    /// same structural-absence convention `collisions` and `postings` use.
    pub metadata_sink: Option<MetadataSinkCounters>,
}

/// Metric metadata sink counters (ADR-0085 decision 1), mirroring
/// [`ravel_ingest::IngestMetricsSnapshot`]'s four `metadata_*` fields. A
/// separate struct rather than four more flat fields on
/// [`IngestPipelineSnapshot`] because they are always present or always
/// absent together (one sink, one set of counters), which `Option<Self>`
/// says once instead of four times.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetadataSinkCounters {
    pub flush_gets_total: u64,
    pub flush_puts_total: u64,
    pub flush_dropped_total: u64,
    pub entries_dropped_total: u64,
}

/// The log pipeline's write-side POSTINGS counters, cumulative over flushed
/// objects (ADR-0049 decision 4). Rendered without any per-field
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
    /// Dynamic-column budget counters (ADR-0100 decision 1). `used_total` and
    /// `overflowed_total` are cumulative over flushed objects;
    /// `used_max` is a running maximum of one object's used count, so an
    /// operator sees budget pressure before any object crosses the cap.
    pub dynamic_columns_used_total: u64,
    pub dynamic_columns_overflowed_total: u64,
    pub dynamic_columns_used_max: u64,
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
            metadata_sink: Some(MetadataSinkCounters {
                flush_gets_total: snapshot.metadata_flush_gets_total,
                flush_puts_total: snapshot.metadata_flush_puts_total,
                flush_dropped_total: snapshot.metadata_flush_dropped_total,
                entries_dropped_total: snapshot.metadata_entries_dropped_total,
            }),
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
                dynamic_columns_used_total: snapshot.dynamic_columns_used_total,
                dynamic_columns_overflowed_total: snapshot.dynamic_columns_overflowed_total,
                dynamic_columns_used_max: snapshot.dynamic_columns_used_max,
            }),
            metadata_sink: None,
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
            metadata_sink: None,
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

    // Metric metadata sink counters (ADR-0085 decision 1): only the metrics
    // pipeline builds this record, so `metadata_sink` is `Some` only there
    // (same structural-absence convention as `collisions` above), and the
    // family is empty in a logs- or spans-only process.
    let with_metadata_sink: Vec<_> = pipelines
        .iter()
        .filter_map(|pipeline| pipeline.metadata_sink.map(|counters| (pipeline, counters)))
        .collect();
    if !with_metadata_sink.is_empty() {
        write_header(
            out,
            "ravel_ingest_metadata_flush_gets_total",
            "Metric metadata record GETs issued by the flush window (ADR-0085 decision 1), by \
             signal.",
            "counter",
        );
        for (pipeline, counters) in &with_metadata_sink {
            write_sample(
                out,
                "ravel_ingest_metadata_flush_gets_total",
                &labels(mode, pipeline.signal),
                counters.flush_gets_total,
            );
        }

        write_header(
            out,
            "ravel_ingest_metadata_flush_puts_total",
            "Metric metadata record CAS PUTs attempted by the flush window (ADR-0085 decision \
             1), by signal. Counts attempts, so a conflicted-and-retried write counts more than \
             once.",
            "counter",
        );
        for (pipeline, counters) in &with_metadata_sink {
            write_sample(
                out,
                "ravel_ingest_metadata_flush_puts_total",
                &labels(mode, pipeline.signal),
                counters.flush_puts_total,
            );
        }

        write_header(
            out,
            "ravel_ingest_metadata_flush_dropped_total",
            "Flush windows whose metric metadata update was dropped: CAS retries exhausted, or \
             a read/write failure against the record (ADR-0085 decision 1), by signal. Never \
             fatal to an ingest request.",
            "counter",
        );
        for (pipeline, counters) in &with_metadata_sink {
            write_sample(
                out,
                "ravel_ingest_metadata_flush_dropped_total",
                &labels(mode, pipeline.signal),
                counters.flush_dropped_total,
            );
        }

        write_header(
            out,
            "ravel_ingest_metadata_entries_dropped_total",
            "Metric family names not stored in a tenant's metadata record because it was \
             already at the per-tenant entry cap (ADR-0085 decision 1), by signal. The points \
             themselves are still ingested and queryable.",
            "counter",
        );
        for (pipeline, counters) in &with_metadata_sink {
            write_sample(
                out,
                "ravel_ingest_metadata_entries_dropped_total",
                &labels(mode, pipeline.signal),
                counters.entries_dropped_total,
            );
        }
    }
}

/// The write-side POSTINGS counters (ADR-0049 decision 4): section
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
/// the query path's DataFusion counters.
///
/// The same family also carries the dynamic-column budget counters (ADR-0100
/// decision 1): `ravel_logs_dynamic_columns_used_total` and
/// `_overflowed_total` are cumulative, and `ravel_logs_dynamic_columns_used_max`
/// is a gauge (a running per-object maximum), all under the same `{mode, signal}`
/// labels with no per-field dimension.
fn render_logs_postings_family(out: &mut String, mode: Mode, pipelines: &[IngestPipelineSnapshot]) {
    fn labels(mode: Mode, signal: Signal) -> [Label; 2] {
        [Label::Mode(mode), Label::Signal(signal)]
    }

    // (metric name, HELP text, counter selector).
    type PostingsMetric = (&'static str, &'static str, fn(&PostingsCounters) -> u64);

    // Each metric is one header then one sample per pipeline that builds
    // postings, keeping the zero-is-not-absence discipline the other families
    // keep for a configured-but-idle pipeline.
    let metrics: [PostingsMetric; 7] = [
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
        (
            "ravel_logs_dynamic_columns_used_total",
            "Distinct (name, type) attribute pairs that received a real dynamic column, summed over flushed log objects, by signal (ADR-0100 decision 1).",
            |p| p.dynamic_columns_used_total,
        ),
        (
            "ravel_logs_dynamic_columns_overflowed_total",
            "Distinct (name, type) attribute pairs that overflowed the max_dynamic_columns budget and folded into attrs_raw, summed over flushed log objects, by signal (ADR-0100 decision 1).",
            |p| p.dynamic_columns_overflowed_total,
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

    // The per-object maximum of dynamic_columns_used. A running maximum, not a
    // cumulative sum, so it is a gauge and its name carries no `_total` suffix
    // (ADR-0100 decision 1: it shows budget pressure before the cap is crossed,
    // which a total cannot).
    write_header(
        out,
        "ravel_logs_dynamic_columns_used_max",
        "Largest per-object dynamic-column count seen so far, by signal: the budget-pressure gauge that rises before any object overflows max_dynamic_columns (ADR-0100 decision 1).",
        "gauge",
    );
    for pipeline in pipelines {
        if let Some(postings) = &pipeline.postings {
            write_sample(
                out,
                "ravel_logs_dynamic_columns_used_max",
                &labels(mode, pipeline.signal),
                postings.dynamic_columns_used_max,
            );
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

/// Process-wide in-flight ingest-request shed counter. Mode-only
/// labeled like `render_tenancy_family` above: the controller is a single
/// semaphore shared across OTLP metrics/logs/traces and Remote Write, on
/// every listener and transport, with no per-signal breakdown to render.
fn render_ingest_concurrency_family(out: &mut String, mode: Mode, shed_total: u64) {
    write_header(
        out,
        "ravel_ingest_concurrency_shed_total",
        "Ingest requests rejected immediately by the process-wide in-flight concurrency ceiling (--max-inflight-ingest-requests).",
        "counter",
    );
    write_sample(
        out,
        "ravel_ingest_concurrency_shed_total",
        &[Label::Mode(mode)],
        shed_total,
    );
}

/// The process-wide ingest buffer byte budget family (ADR-0069 decision 1): the current gauge of estimated buffered bytes, the configured
/// ceiling, and the cumulative shed counter. Mode-only labeled like
/// `render_ingest_concurrency_family` above: the budget is a single gauge
/// shared across metrics/logs/traces with no per-signal breakdown.
///
/// `ravel_ingest_buffer_bytes_limit` is `0` when the ceiling is unlimited
/// (`--max-ingest-buffer-bytes 0`), matching the flag's own "0 = unlimited"
/// convention; a scraper reads a `0` limit as "no ceiling", not "reject
/// everything".
fn render_ingest_buffer_budget_family(
    out: &mut String,
    mode: Mode,
    in_flight_bytes: u64,
    ceiling: u64,
    shed_total: u64,
) {
    write_header(
        out,
        "ravel_ingest_buffer_bytes",
        "Estimated buffered ingest bytes currently held across all tenants and signals (the process-wide ingest byte budget gauge, ADR-0069).",
        "gauge",
    );
    write_sample(
        out,
        "ravel_ingest_buffer_bytes",
        &[Label::Mode(mode)],
        in_flight_bytes,
    );
    write_header(
        out,
        "ravel_ingest_buffer_bytes_limit",
        "Configured ingest buffer byte budget ceiling (--max-ingest-buffer-bytes); 0 means unlimited.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_ingest_buffer_bytes_limit",
        &[Label::Mode(mode)],
        ceiling,
    );
    write_header(
        out,
        "ravel_ingest_buffer_shed_total",
        "Ingest requests shed before buffering by the process-wide ingest byte budget ceiling (--max-ingest-buffer-bytes).",
        "counter",
    );
    write_sample(
        out,
        "ravel_ingest_buffer_shed_total",
        &[Label::Mode(mode)],
        shed_total,
    );
}

/// The logs prune-selectivity family (ADR-0049):
/// blocks the logs scans saw, survived, and pruned by postings, cumulative
/// across queries. Reads the `LogsScanExec` DataFusion counters that
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

/// The declared-typed-attribute-column staleness family (ADR-0090 decision 2):
/// query-time resolutions of a tenant's declared `logs` columns that were
/// served from a stale cache entry, a backoff-suppressed read, a failed
/// `TenantConfig` read, or a malformed durable declaration, cumulative.
///
/// Nonzero means at least one query planned against a declaration that is not
/// the durable one: a newly written declaration is not in effect yet, and two
/// query replicas can disagree about a tenant's `logs` schema. A brief blip
/// after a config write is expected (the staleness horizon); a counter that
/// keeps climbing means the config object is unreadable and the operations
/// guide pages on it. Process-global atomic read from
/// [`crate::typed_attr_metrics`], single source, and `signal` is always `logs`:
/// declared typed attribute columns exist only on the `logs` table.
fn render_typed_attr_columns_family(out: &mut String, mode: Mode, stale_fallbacks: u64) {
    write_header(
        out,
        "ravel_typed_attr_columns_stale_fallback_total",
        "Declared typed attribute column resolutions served from a stale cache entry or a failed TenantConfig read, cumulative (ADR-0090 decision 2).",
        "counter",
    );
    write_sample(
        out,
        "ravel_typed_attr_columns_stale_fallback_total",
        &[Label::Mode(mode), Label::Signal(Signal::Logs)],
        stale_fallbacks,
    );
}

/// The per-process metric-metadata cache family (ADR-0085 decision 1 read
/// path), read at scrape time from
/// [`ravel_query::http::MetadataCache::counters`]. The cache serves
/// `/api/v1/metadata` at one GET per (tenant, refresh horizon, process); these
/// four cumulative counters expose its hit rate and refresh health so an
/// operator can see the cache is doing its job and that background refreshes are
/// not silently failing (a climbing `refresh_errors_total` means the record is
/// becoming unreadable while stale data is still served).
///
/// Every sample carries only `{mode}` (the ADR-0044 allowlist): the cache is one
/// process-global structure over every tenant it has answered for, with no
/// per-tenant or per-signal breakdown to render, the same mode-only shape as
/// [`render_durable_auth_family`]. Rendered only when the process built a cache
/// (a request-serving mode, `Mode::All`/`Mode::Query`); a process without one
/// omits the whole `query_metadata_cache_*` family. All four are cumulative
/// totals, so each name carries the `_total` suffix.
fn render_metadata_cache_family(out: &mut String, mode: Mode, counters: &MetadataCacheCounters) {
    write_header(
        out,
        "query_metadata_cache_hits_total",
        "Metric-metadata requests served from an already-cached tenant record, fresh or stale (ADR-0085 decision 1).",
        "counter",
    );
    write_sample(
        out,
        "query_metadata_cache_hits_total",
        &[Label::Mode(mode)],
        counters.hits,
    );

    write_header(
        out,
        "query_metadata_cache_misses_total",
        "Metric-metadata requests that found no cached record and did an inline fill GET (ADR-0085 decision 1).",
        "counter",
    );
    write_sample(
        out,
        "query_metadata_cache_misses_total",
        &[Label::Mode(mode)],
        counters.misses,
    );

    write_header(
        out,
        "query_metadata_cache_refreshes_total",
        "Background metric-metadata refreshes started by a past-horizon request that won the single-flight; includes refreshes that later errored (ADR-0085 decision 1).",
        "counter",
    );
    write_sample(
        out,
        "query_metadata_cache_refreshes_total",
        &[Label::Mode(mode)],
        counters.refreshes,
    );

    write_header(
        out,
        "query_metadata_cache_refresh_errors_total",
        "Background metric-metadata refreshes that failed their GET or decode; the stale record keeps being served and the client never sees the error (ADR-0085 decision 1).",
        "counter",
    );
    write_sample(
        out,
        "query_metadata_cache_refresh_errors_total",
        &[Label::Mode(mode)],
        counters.refresh_errors,
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

/// The `ravel_bucket_protection_unknown` gauge (ADR-0072 decision 3): 1 when
/// the last `--require-bucket-protection` startup check observed
/// [`crate::bucket_protection::BucketProtectionOutcome::Unknown`] (every
/// backend reachable only through `ObjectStoreBackend` today), 0 otherwise,
/// including when the flag is off. Single source, no labels, the same shape
/// as [`render_store_probe_family`]; exported unconditionally so a fleet can
/// alarm on it from a metrics-only monitoring setup.
fn render_bucket_protection_family(out: &mut String, mode: Mode, unknown: u64) {
    write_header(
        out,
        "ravel_bucket_protection_unknown",
        "Whether the --require-bucket-protection startup check (ADR-0072 decision 3) could not confirm Object Lock / versioning status for this backend (1), or was off or confirmed Enabled (0).",
        "gauge",
    );
    write_sample(
        out,
        "ravel_bucket_protection_unknown",
        &[Label::Mode(mode)],
        unknown,
    );
}

/// The durable auth (`sys/auth`) background-refresh loop's three counters
/// (ADR-0066 decision 6), decoupled from
/// [`crate::lifecycle_refresh::DurableAuthState`] so the renderer is testable
/// with a plain struct literal, matching [`CatalogCountersSnapshot`]. Rendered
/// only when the process built a `DurableAuthState` (a keyed deployment with
/// `--deployment-key`, in `Mode::All`/`Gateway`/`Query`); a process without one
/// (or `Mode::Maintain`) omits the whole `ravel_durable_auth_*` family.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DurableAuthCountersSnapshot {
    /// Background refreshes that failed to read or decode `sys/auth` (store,
    /// decode, wrong-key, or corruption error). The staleness gate is not
    /// advanced on a failure, so a sustained inability to refresh eventually
    /// drives the cached map hard-stale and fails auth closed. This is the
    /// credential-break early-warning signal: it climbs long before the
    /// hard-stale horizon, which is why an operator alerts on its increase.
    pub refresh_failures: u64,
    /// Off-horizon on-miss re-reads of `sys/auth` actually begun after the rate
    /// limiter, when the request path saw an unknown token.
    pub on_miss_rereads: u64,
    /// Bearer-token resolutions refused because the cached map was hard-stale
    /// (fail-closed, ADR-0066 decision 6).
    pub stale_fail_closed: u64,
}

/// The durable-auth refresh-loop counter family (ADR-0066 decision 6). Every
/// sample carries only `{mode}` (the ADR-0044 allowlist): the loop is
/// process-wide, one cached `sys/auth` map per deployment key, with no
/// per-tenant or per-signal breakdown to render. The same mode-only shape as
/// [`render_catalog_family`] and [`render_store_probe_family`] above. Rendered
/// only when a `DurableAuthState` exists, so a process with no keyed deployment
/// omits the whole family rather than exporting three permanent zeros.
fn render_durable_auth_family(
    out: &mut String,
    mode: Mode,
    snapshot: &DurableAuthCountersSnapshot,
) {
    write_header(
        out,
        "ravel_durable_auth_refresh_failures_total",
        "Durable auth (sys/auth) background refreshes that failed to read or decode the token map; the staleness gate is not advanced on a failure, so a sustained failure eventually fails auth closed (ADR-0066 decision 6).",
        "counter",
    );
    write_sample(
        out,
        "ravel_durable_auth_refresh_failures_total",
        &[Label::Mode(mode)],
        snapshot.refresh_failures,
    );

    write_header(
        out,
        "ravel_durable_auth_on_miss_rereads_total",
        "Off-horizon on-miss re-reads of sys/auth begun after the rate limiter, when the request path saw an unknown token (ADR-0066 decision 6).",
        "counter",
    );
    write_sample(
        out,
        "ravel_durable_auth_on_miss_rereads_total",
        &[Label::Mode(mode)],
        snapshot.on_miss_rereads,
    );

    write_header(
        out,
        "ravel_durable_auth_stale_fail_closed_total",
        "Bearer-token resolutions refused because the cached sys/auth map was hard-stale, failing closed (ADR-0066 decision 6).",
        "counter",
    );
    write_sample(
        out,
        "ravel_durable_auth_stale_fail_closed_total",
        &[Label::Mode(mode)],
        snapshot.stale_fail_closed,
    );
}

/// Storage-derived tenant discovery counters for the maintenance driver
/// (ADR-0048 decision 3), decoupled from
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
/// decisions 4 and 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceSafetySignalSnapshot {
    pub signal: Signal,
    /// Compaction publishes aborted by the record-count conservation gate
    /// (ADR-0048 decision 6): inputs and built parts disagreed on record
    /// count, so nothing was written.
    pub conservation_aborts: u64,
    /// Orphan-GC mass-orphan circuit breaker trips (ADR-0048 decision 4).
    /// Monotonic: a later pass that no longer trips (dilution or partial
    /// restoration) does not decrement this. An operator's alert
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
    /// Orphan candidates the most recent sweep pass found, tripped or not
    /// (ADR-0058 decision 1): `orphans_deleted + orphans_withheld`. Nonzero
    /// for small-scale commit-record loss the breaker's ratio/count thresholds
    /// are deliberately too coarse to trip on, which is why it is a distinct
    /// gauge from `orphans_withheld` (that one stays `0` precisely when the
    /// breaker does not trip). Like `orphans_withheld` it reflects only the
    /// most recent pass and drops as candidates are deleted or their records
    /// restored; a drop is not "resolved," just this pass's count.
    pub orphans_present: u64,
}

/// One scrape's maintenance-safety counters (ADR-0048 decisions 1, 4, 6): the three safety controls that, before this issue, reached
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
/// [`crate::maintain::MaintenanceSafetyMetrics`] for the full contradiction.
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
         persists.",
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

    write_header(
        out,
        "ravel_maintain_orphans_present",
        "Orphan candidates the most recent sweep pass found, by signal, whether or not the \
         mass-orphan breaker tripped. Nonzero flags small-scale commit-record loss the breaker's \
         thresholds are too coarse to catch (ADR-0058). A drop is not resolution, only this \
         pass's count.",
        "gauge",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_maintain_orphans_present",
            &labels(mode, signal.signal),
            signal.orphans_present,
        );
    }
}

/// One scrape's ADR-0065 stuck-owner mitigation counters: how
/// many in-process workers are live, how many units this process currently
/// owns, how many warm-started from a durable memo snapshot, how many full
/// (unscoped) sweep passes have run, and how many owned units are stalled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MaintenanceOwnershipSnapshot {
    pub workers_live: u64,
    pub units_owned: u64,
    pub units_stalled: u64,
    pub memo_warm_start_units: u64,
    pub full_sweep_passes_total: u64,
}

/// No `tenant_hash` label on any series here (ADR-0044 section 4): every
/// sample is process-wide, not per-tenant, so the closed-label-set rule this
/// unauthenticated route enforces is satisfied trivially -- there is no
/// tenant dimension to add in the first place.
fn render_maintain_ownership_family(
    out: &mut String,
    mode: Mode,
    snapshot: &MaintenanceOwnershipSnapshot,
) {
    write_header(
        out,
        "ravel_maintain_workers_live",
        "In-process maintenance workers this supervisor currently sees as live \
         (ADR-0065 decision 1).",
        "gauge",
    );
    write_sample(
        out,
        "ravel_maintain_workers_live",
        &[Label::Mode(mode)],
        snapshot.workers_live,
    );

    write_header(
        out,
        "ravel_maintain_units_owned",
        "Owned (tenant, signal, shard) units this process is currently maintaining \
         (ADR-0065 decision 2).",
        "gauge",
    );
    write_sample(
        out,
        "ravel_maintain_units_owned",
        &[Label::Mode(mode)],
        snapshot.units_owned,
    );

    write_header(
        out,
        "ravel_maintain_units_stalled",
        "Owned units with consecutive failing ticks past the configured threshold \
         (ADR-0065 decision 2's stuck-owner mitigation). Alert on a sustained nonzero \
         value, not on any single scrape.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_maintain_units_stalled",
        &[Label::Mode(mode)],
        snapshot.units_stalled,
    );

    write_header(
        out,
        "ravel_maintain_memo_warm_start_units_total",
        "Units seeded from a durable memo snapshot on handoff or startup, instead of \
         rescanning cold (ADR-0065 decision 3).",
        "counter",
    );
    write_sample(
        out,
        "ravel_maintain_memo_warm_start_units_total",
        &[Label::Mode(mode)],
        snapshot.memo_warm_start_units,
    );

    write_header(
        out,
        "ravel_maintain_full_sweep_passes_total",
        "Full (unscoped) sweep passes run, as opposed to a zone-scoped sweep \
         (ADR-0065 decision 3).",
        "counter",
    );
    write_sample(
        out,
        "ravel_maintain_full_sweep_passes_total",
        &[Label::Mode(mode)],
        snapshot.full_sweep_passes_total,
    );
}

/// One scrape's RLOG k-way merge peak-bytes gauge (ADR-0065 decision 4),
/// sourced from `ravel_maintain::MergeMemoryTracker`. No `tenant_hash`: the
/// tracker is one process-wide handle shared across every tenant's merges.
fn render_merge_memory_family(
    out: &mut String,
    mode: Mode,
    tracker: &ravel_maintain::MergeMemoryTracker,
) {
    write_header(
        out,
        "ravel_maintain_rlog_merge_peak_bytes",
        "High-water mark of RLOG k-way merge memory, by kind: transient (in-flight \
         fetched-minus-released block bytes) or total (transient plus buffered writer \
         output) (ADR-0065 decision 4).",
        "gauge",
    );
    write_sample(
        out,
        "ravel_maintain_rlog_merge_peak_bytes",
        &[
            Label::Mode(mode),
            Label::MergeMemoryKind(MergeMemoryKind::Transient),
        ],
        tracker.peak_transient_bytes(),
    );
    write_sample(
        out,
        "ravel_maintain_rlog_merge_peak_bytes",
        &[
            Label::Mode(mode),
            Label::MergeMemoryKind(MergeMemoryKind::Total),
        ],
        tracker.peak_total_bytes(),
    );
}

/// One signal's at-rest scrubber counters for one scrape (ADR-0059 decisions
/// 1, 3).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScrubSignalSnapshot {
    pub signal: Signal,
    /// Objects that failed at-rest integrity re-verification for this signal:
    /// a whole-object blake3 mismatch against the recorded content hash (bit
    /// rot / partial write) or a footer/section crc failure. Both are
    /// data-object corruption, so both increment this one counter.
    pub checksum_mismatch: u64,
    /// Objects where the covering name-postings object omitted a `__name__`
    /// the object really carries (a false negative). Wired but only
    /// nonzero once covering-postings resolution lands in the scrub task.
    pub postings_disagreement: u64,
    /// Sealed commit records absent from the folded snapshot for this signal
    /// (an under-count): `ravel_scrub_seal_divergence_total{reason="missing"}`
    /// (ADR-0059 decision 2).
    pub seal_divergence_missing: u64,
    /// Snapshot entries whose `content_hash` disagreed with the sealed commit
    /// record for this signal:
    /// `ravel_scrub_seal_divergence_total{reason="mismatched"}`.
    pub seal_divergence_mismatched: u64,
    /// Fraction of the current rotation the content-tier cursor has covered so
    /// far for this signal, in `[0.0, 1.0]` (operator visibility into cadence).
    pub cursor_position: f64,
}

/// One scrape's at-rest scrubber counters (ADR-0059 decisions 1, 3), per
/// signal. `Some` only in [`Mode::Maintain`], the one mode that runs
/// the scrubber.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrubSnapshot {
    pub signals: Vec<ScrubSignalSnapshot>,
}

/// The at-rest scrubber family (ADR-0059 decision 3). Follows
/// [`render_maintain_safety_family`]'s conventions exactly: per-signal series
/// under `{mode, signal}` and deliberately no `tenant_hash` label (ADR-0044
/// section 4 blocks any per-tenant series on this unauthenticated route). The
/// two anomaly counters carry the same zero-is-not-absence discipline every
/// other family keeps (a series per maintained signal even at zero), so an
/// alert can fire on `increase(...) > 0`; `cursor_position` is a gauge.
fn render_scrub_family(out: &mut String, mode: Mode, snapshot: &ScrubSnapshot) {
    fn labels(mode: Mode, signal: Signal) -> [Label; 2] {
        [Label::Mode(mode), Label::Signal(signal)]
    }

    write_header(
        out,
        "ravel_scrub_checksum_mismatch_total",
        "Data objects that failed at-rest integrity re-verification (whole-object blake3 mismatch \
         or footer/section crc failure), by signal (ADR-0059). Alert on increase() > 0: there is \
         no redundant copy to repair from, so any nonzero increase is corruption an operator must \
         investigate.",
        "counter",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_scrub_checksum_mismatch_total",
            &labels(mode, signal.signal),
            signal.checksum_mismatch,
        );
    }

    write_header(
        out,
        "ravel_scrub_postings_disagreement_total",
        "Objects whose covering name-postings object omitted a __name__ the object really carries \
         (a false negative), by signal (ADR-0059).",
        "counter",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_scrub_postings_disagreement_total",
            &labels(mode, signal.signal),
            signal.postings_disagreement,
        );
    }

    write_header(
        out,
        "ravel_scrub_seal_divergence_total",
        "Divergences between the folded snapshot and the re-listed sealed commit history, by \
         signal and reason (ADR-0059 decision 2): reason=\"missing\" is a sealed commit record \
         absent from the snapshot (an under-count), reason=\"mismatched\" a snapshot entry \
         whose content_hash disagrees with the sealed record. Orphaned entries (a snapshot entry \
         with no surviving commit record) are the expected retention-after-fold shape and are \
         never counted. Alert on increase() > 0.",
        "counter",
    );
    for signal in &snapshot.signals {
        for reason in ScrubReason::ALL {
            let value = match reason {
                ScrubReason::Missing => signal.seal_divergence_missing,
                ScrubReason::Mismatched => signal.seal_divergence_mismatched,
            };
            write_sample(
                out,
                "ravel_scrub_seal_divergence_total",
                &[
                    Label::Mode(mode),
                    Label::Signal(signal.signal),
                    Label::ScrubReason(reason),
                ],
                value,
            );
        }
    }

    write_header(
        out,
        "ravel_scrub_cursor_position",
        "Fraction of the current scrub rotation the content-tier cursor has covered so far, by \
         signal, in [0,1] (ADR-0059 decision 3). A rotation completes in about the configured \
         --scrub-period P; a value stuck near 0 means scrubbing is not keeping pace with P.",
        "gauge",
    );
    for signal in &snapshot.signals {
        write_sample_f64(
            out,
            "ravel_scrub_cursor_position",
            &labels(mode, signal.signal),
            signal.cursor_position,
        );
    }
}

/// The ADR-0046 read caches' counters. Two caches
/// share this one family: the query fetchers' RAM cache (`fetch`) and the
/// catalog's content-addressed byte cache (`catalog`), told apart by the
/// `cache=` label, the same one-name-split-by-a-closed-dimension discipline
/// every other family here uses. There is no `signal` split. Request hit rate
/// is `hits / (hits + misses)` and byte hit rate is `bytes_served /
/// (bytes_served plus bytes_admitted)`; both are left for PromQL to compute per
/// `cache` from the raw counters, not baked in here. The family deliberately
/// omits `single_flight_collapses` because that is a separate fleet-wide
/// collapse-rate metric, not this one, and this family must not preempt that
/// decision by shipping a shape it did not choose.
///
/// Each cache is rendered only when it is attached (`Some`): a `--disable-cache`
/// process passes `None` for both and this family is skipped entirely (see
/// [`render`]); a process with the fetcher cache off but the catalog byte cache
/// on, or vice versa, renders only the family that exists. Every metric name's
/// header is written once even when both caches are present, so the exposition
/// stays well-formed (one HELP/TYPE line per name, then its samples).
fn render_cache_family(
    out: &mut String,
    mode: Mode,
    fetch: Option<&CacheMetricsSnapshot>,
    catalog: Option<&CacheMetricsSnapshot>,
) {
    let families = [(CacheFamily::Fetch, fetch), (CacheFamily::Catalog, catalog)];

    // One metric name at a time: header once, then a sample per attached cache
    // under its `cache=` label. `field` picks the counter this metric renders.
    let mut emit = |name: &str, help: &str, field: fn(&CacheMetricsSnapshot) -> u64| {
        write_header(out, name, help, "counter");
        for (family, snapshot) in families {
            if let Some(snapshot) = snapshot {
                write_sample(
                    out,
                    name,
                    &[Label::Mode(mode), Label::Cache(family)],
                    field(snapshot),
                );
            }
        }
    };

    emit(
        "ravel_cache_hits_total",
        "Read-cache lookups served from the cache.",
        |s| s.hits,
    );
    emit(
        "ravel_cache_misses_total",
        "Read-cache lookups not found in the cache.",
        |s| s.misses,
    );
    emit(
        "ravel_cache_bytes_served_total",
        "Bytes served from the cache on a hit.",
        |s| s.bytes_served,
    );
    emit(
        "ravel_cache_bytes_admitted_total",
        "Bytes admitted into the cache after a miss.",
        |s| s.bytes_admitted,
    );
    emit(
        "ravel_cache_evictions_total",
        "Entries evicted from the read cache by its S3-FIFO policy.",
        |s| s.evictions,
    );
    emit(
        "ravel_cache_disk_errors_degraded_to_misses_total",
        "Disk-tier reads that found an entry at its canonical path but discarded it (short \
         read, bad header, key mismatch, or a failed crc32c check) rather than a clean miss. \
         Nonzero here means the disk tier is unhealthy, not merely cold.",
        |s| s.disk_errors_degraded_to_misses,
    );
    emit(
        "ravel_cache_disk_entries_expired_max_age_total",
        "Disk-tier entries dropped because their stamped write time aged past the configured \
         max-age (ADR-0064), by a read, the startup scan, or the periodic sweep. This is an \
         expiry, not corruption: the bytes of an erased subject are physically removed from \
         local disk within the max-age bound.",
        |s| s.disk_entries_expired_max_age,
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
    /// Per-tenant wire (compressed) request-body bytes (ADR-0084 decision 5),
    /// rendered as `ravel_ingest_wire_bytes_total` alongside this family's
    /// charged-bytes counter. Sourced from
    /// [`crate::ingest_byte_metrics::IngestByteMetrics`], not the admission
    /// `usage_snapshot`: the byte-rate bucket charges the decompressed size for
    /// a compressed request, so the wire quantity has no home in that snapshot.
    /// Folded by the same `tenant_labels` gate as `usage`.
    pub wire_bytes: Vec<crate::ingest_byte_metrics::TenantWireBytes>,
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
    rejected_clock: u64,
    reconciliation_failures: u64,
}

impl AdmissionAcc {
    fn rejected(&self, reason: RejectReason) -> u64 {
        match reason {
            RejectReason::ByteRate => self.rejected_byte_rate,
            RejectReason::SeriesRate => self.rejected_series_rate,
            RejectReason::SeriesCap => self.rejected_series_cap,
            RejectReason::Clock => self.rejected_clock,
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
        acc.rejected_clock = acc
            .rejected_clock
            .saturating_add(row.requests_rejected_clock_total);
        acc.reconciliation_failures = acc
            .reconciliation_failures
            .saturating_add(row.reconciliation_failures_total);
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
        "Bytes charged against the ingest byte-rate layer for admitted requests, by tenant and \
         signal. For a gzip-compressed OTLP request this is the decompressed size (ADR-0084 \
         decision 4); for an uncompressed request it equals the wire size. Compare with \
         ravel_ingest_wire_bytes_total to distinguish a tenant that increased telemetry from one \
         that turned compression off.",
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

    // Wire (compressed) request-body bytes per tenant/signal (ADR-0084 decision
    // 5), sourced from `IngestByteMetrics`, not the admission usage snapshot.
    // Folded by the same `tenant_labels` gate as the counters above so its
    // cardinality is bounded identically. Its ratio to
    // `ravel_admission_admitted_bytes_total` is a tenant's effective
    // compression factor.
    let mut wire_rows: std::collections::HashMap<(Option<TenantHash>, Signal), u64> =
        std::collections::HashMap::new();
    for row in &snapshot.wire_bytes {
        let key = (
            snapshot.tenant_labels.then_some(row.tenant_hash),
            row.signal,
        );
        let acc = wire_rows.entry(key).or_default();
        *acc = acc.saturating_add(row.wire_bytes_total);
    }
    let mut wire_ordered: Vec<((Option<TenantHash>, Signal), u64)> =
        wire_rows.into_iter().collect();
    wire_ordered.sort_by(|(a_key, _), (b_key, _)| {
        tenant_label(a_key.0)
            .value()
            .cmp(&tenant_label(b_key.0).value())
            .then_with(|| signal_name(a_key.1).cmp(signal_name(b_key.1)))
    });
    write_header(
        out,
        "ravel_ingest_wire_bytes_total",
        "Wire (on-the-wire, compressed when the client compressed) OTLP request body bytes \
         admitted, by tenant and signal (ADR-0084 decision 5). Divide \
         ravel_admission_admitted_bytes_total by this to read a tenant's effective compression \
         factor.",
        "counter",
    );
    for ((hash, signal), wire_bytes) in &wire_ordered {
        write_sample(
            out,
            "ravel_ingest_wire_bytes_total",
            &labels(mode, *hash, *signal),
            *wire_bytes,
        );
    }

    write_header(
        out,
        "ravel_admission_rejected_total",
        "Admission rejections by tenant, signal, and reason (byte_rate, series_rate, series_cap, clock).",
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

    // Fleet-global reconciliation read failures (ADR-0057 section 3). Same
    // {mode, tenant_hash, signal} labels as the rest of this family. A sustained
    // nonzero rate means a process is repeatedly unable to read its siblings'
    // snapshots and is falling back to its last-computed soft threshold rather
    // than a fresh fleet view; admission never fails closed on it, so this is
    // the signal that fleet-wide accuracy is degrading, not that ingest is down.
    write_header(
        out,
        "ravel_admission_reconciliation_failures_total",
        "Fleet-admission reconciliation cycles whose sibling-snapshot read (LIST or GET) failed, \
         by tenant and signal; the last-known soft threshold stays in force (ADR-0057 section 3).",
        "counter",
    );
    for ((hash, signal), acc) in &ordered {
        write_sample(
            out,
            "ravel_admission_reconciliation_failures_total",
            &labels(mode, *hash, *signal),
            acc.reconciliation_failures,
        );
    }
}

/// One (tenant bucket, workload class) row's accumulated per-query cost
/// counters (ADR-0044 section 1 and 3). Both the actuals summed
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

/// The recorder seam (ADR-0044 section 4): this is what lets the
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

/// The per-query cost family (ADR-0044 section 4). Every sample
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

/// One rendered row of the per-tenant PUT attribution family: the (signal,
/// tenant bucket) key plus the accounted PUT count. `tenant` is `None` for the
/// folded `other` bucket and `Some(hash)` for an allowlisted tenant, the same
/// convention [`tenant_label`] renders everywhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantAttributionRow {
    pub signal: Signal,
    pub tenant: Option<TenantHash>,
    pub puts: u64,
}

/// Fold one signal's [`TenantPutAttribution::top_n`] snapshot into rendered
/// rows, bounded the same way [`QueryAccountingMetrics`] bounds the per-query
/// family: `allowlist` is the `--metrics-tenant-labels` (ADR-0051 section 6)
/// per-tenant set built from `config.limits.tenants` at server start, shared
/// with `query_accounting`. A tenant outside it folds into the shared `other`
/// bucket for this signal, summing its PUTs rather than allocating a series of
/// its own. `TenantPutAttribution` already bounds its own tracked set to
/// `MAX_TRACKED_TENANTS` (ADR-0076 decision 2); this fold is the second,
/// narrower bound the unauthenticated `/metrics` route needs on top of that,
/// matching every other tenant-labeled family here (ADR-0076: "never an
/// unbounded per-tenant Prometheus label").
pub fn attribution_rows(
    signal: Signal,
    attribution: &TenantPutAttribution,
    allowlist: &HashSet<TenantHash>,
) -> Vec<TenantAttributionRow> {
    let mut folded: HashMap<Option<TenantHash>, u64> = HashMap::new();
    // usize::MAX, not tracked_len(): a separate lock-then-len() call here would
    // race a growing table between the two locks and silently drop the newest
    // entries from the fold instead of all of them.
    for entry in attribution.top_n(usize::MAX) {
        let bucket = allowlist.contains(&entry.tenant).then_some(entry.tenant);
        *folded.entry(bucket).or_default() += entry.puts;
    }
    let mut rows: Vec<TenantAttributionRow> = folded
        .into_iter()
        .map(|(tenant, puts)| TenantAttributionRow {
            signal,
            tenant,
            puts,
        })
        .collect();
    rows.sort_by(|a, b| {
        tenant_label(a.tenant)
            .value()
            .cmp(&tenant_label(b.tenant).value())
    });
    rows
}

/// The per-tenant PUT attribution family (ADR-0076 decision 2 / T3): answers
/// "which tenant is generating the PUT bill" per signal, the gap the ADR names
/// as blocking a per-tenant shard-count cost lever. `rows` already folded
/// unconfigured tenants into `other` in [`attribution_rows`], so this function
/// only renders whatever it is handed, the same discipline
/// [`render_query_family`] follows.
fn render_attribution_family(out: &mut String, mode: Mode, rows: &[TenantAttributionRow]) {
    fn labels(mode: Mode, row: &TenantAttributionRow) -> [Label; 3] {
        [
            Label::Mode(mode),
            Label::TenantHash(tenant_label(row.tenant)),
            Label::Signal(row.signal),
        ]
    }

    write_header(
        out,
        "ravel_ingest_attribution_puts_total",
        "Object-store PUT requests attributed to completed ingest flushes, by tenant and \
         signal (ADR-0076 decision 2). Tenants outside --metrics-tenant-labels' allowlist fold \
         into tenant_hash=\"other\".",
        "counter",
    );
    for row in rows {
        write_sample(
            out,
            "ravel_ingest_attribution_puts_total",
            &labels(mode, row),
            row.puts,
        );
    }
}

/// One scrape's ADR-0071 distributed read fan-out counters. Read
/// at scrape time from [`crate::distrib::FragmentMetrics`]; `Some` only when the
/// process serves queries with `--distributed-query` on. Carries no per-shard,
/// per-worker, or per-tenant field: the `ravel_distrib_*` family renders under
/// the closed `{mode}` label alone (ADR-0044 section 4).
#[derive(Debug, Clone)]
pub struct DistribSnapshot {
    pub fragment_requests_total: u64,
    pub fragment_auth_failures_total: u64,
    pub fragment_inflight: u64,
    pub slices_local_total: u64,
    pub slices_remote_total: u64,
    pub slices_redispatched_total: u64,
    pub slices_fallback_total: u64,
    pub slice_fetch_micros_buckets: [u64; LATENCY_BUCKET_COUNT],
    pub slice_fetch_nanos_total: u64,
    /// Dead-endpoint quarantine marks, cumulative (ADR-0071 amendment "dead-
    /// endpoint quarantine", decision 3). A counter.
    pub quarantine_marks_total: u64,
    /// Dead-endpoint quarantine readmits, cumulative (ADR-0071 amendment
    /// decision 3). A counter.
    pub quarantine_readmits_total: u64,
    /// Endpoints currently held in the coordinator's quarantine map (ADR-0071
    /// amendment decision 3). A currently-N value, so a gauge.
    pub quarantine_current: u64,
}

impl DistribSnapshot {
    /// Read a scrape from the live [`crate::distrib::FragmentMetrics`], the same
    /// atomic-load mapping the `/metrics` handler uses. Kept beside the snapshot
    /// so the handler and the tests share one definition of which counter feeds
    /// which field, rather than two copies that can drift.
    pub fn from_metrics(metrics: &crate::distrib::FragmentMetrics) -> Self {
        DistribSnapshot {
            fragment_requests_total: metrics.fragment_requests_total(),
            fragment_auth_failures_total: metrics.fragment_auth_failures_total(),
            fragment_inflight: metrics.fragment_inflight(),
            slices_local_total: metrics.slices_local_total(),
            slices_remote_total: metrics.slices_remote_total(),
            slices_redispatched_total: metrics.slices_redispatched_total(),
            slices_fallback_total: metrics.slices_fallback_total(),
            slice_fetch_micros_buckets: metrics.slice_fetch_buckets(),
            slice_fetch_nanos_total: metrics.slice_fetch_nanos_total(),
            quarantine_marks_total: metrics.quarantine_marks_total(),
            quarantine_readmits_total: metrics.quarantine_readmits_total(),
            quarantine_current: metrics.quarantine_current(),
        }
    }
}

/// The ADR-0071 distributed read fan-out family. Follows the store
/// and maintenance families exactly: every series carries only `{mode}`, and the
/// three slice-routing outcomes are distinct metric names rather than one metric
/// with a `route` label, so no label outside the closed [`Label`] allowlist is
/// introduced. The slice-fetch histogram reuses the store-latency bucket layout
/// (`LATENCY_BUCKET_BOUNDS_MICROS`).
fn render_distrib_family(out: &mut String, mode: Mode, snapshot: &DistribSnapshot) {
    write_header(
        out,
        "ravel_distrib_fragment_requests_total",
        "Inbound fragment SeriesFetch requests served after token auth and admission.",
        "counter",
    );
    write_sample(
        out,
        "ravel_distrib_fragment_requests_total",
        &[Label::Mode(mode)],
        snapshot.fragment_requests_total,
    );

    write_header(
        out,
        "ravel_distrib_fragment_auth_failures_total",
        "Inbound fragment requests refused for a missing or invalid bearer token.",
        "counter",
    );
    write_sample(
        out,
        "ravel_distrib_fragment_auth_failures_total",
        &[Label::Mode(mode)],
        snapshot.fragment_auth_failures_total,
    );

    write_header(
        out,
        "ravel_distrib_fragment_inflight",
        "Fragment requests currently holding an admission permit.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_distrib_fragment_inflight",
        &[Label::Mode(mode)],
        snapshot.fragment_inflight,
    );

    write_header(
        out,
        "ravel_distrib_slices_local_total",
        "Query slices this coordinator executed locally with no network hop.",
        "counter",
    );
    write_sample(
        out,
        "ravel_distrib_slices_local_total",
        &[Label::Mode(mode)],
        snapshot.slices_local_total,
    );

    write_header(
        out,
        "ravel_distrib_slices_remote_total",
        "Query slices this coordinator dispatched to a remote worker successfully.",
        "counter",
    );
    write_sample(
        out,
        "ravel_distrib_slices_remote_total",
        &[Label::Mode(mode)],
        snapshot.slices_remote_total,
    );

    write_header(
        out,
        "ravel_distrib_slices_redispatched_total",
        "Query slices re-dispatched once to the next rendezvous worker after a lost or unavailable first attempt.",
        "counter",
    );
    write_sample(
        out,
        "ravel_distrib_slices_redispatched_total",
        &[Label::Mode(mode)],
        snapshot.slices_redispatched_total,
    );

    write_header(
        out,
        "ravel_distrib_slices_fallback_total",
        "Query slices whose remote dispatch failed at transport and fell back to local.",
        "counter",
    );
    write_sample(
        out,
        "ravel_distrib_slices_fallback_total",
        &[Label::Mode(mode)],
        snapshot.slices_fallback_total,
    );

    write_header(
        out,
        "ravel_distrib_slice_fetch_seconds",
        "Per-slice fetch latency, local and remote alike.",
        "histogram",
    );
    let cumulative = cumulative_buckets(&snapshot.slice_fetch_micros_buckets);
    for (i, count) in cumulative.iter().enumerate() {
        write_histogram_bucket(
            out,
            "ravel_distrib_slice_fetch_seconds_bucket",
            &[Label::Mode(mode)],
            &bucket_le(i),
            *count,
        );
    }
    write_sample_f64(
        out,
        "ravel_distrib_slice_fetch_seconds_sum",
        &[Label::Mode(mode)],
        snapshot.slice_fetch_nanos_total as f64 / 1_000_000_000.0,
    );
    write_sample(
        out,
        "ravel_distrib_slice_fetch_seconds_count",
        &[Label::Mode(mode)],
        cumulative[LATENCY_BUCKET_COUNT - 1],
    );

    // Dead-endpoint quarantine (ADR-0071 amendment decision 3). The two totals
    // are cumulative counters; the currently-quarantined count is a gauge (a
    // present-value, no `_total` suffix), kept in step with the coordinator's
    // quarantine map after every mark, readmit, and prune. Same `{mode}`-only
    // label as the rest of the family.
    write_header(
        out,
        "ravel_distrib_quarantine_marks_total",
        "Dead fragment endpoints marked into the coordinator's quarantine map after a re-dispatchable dispatch failure (transport loss or an Unavailable summary), cumulative (ADR-0071 amendment decision 3).",
        "counter",
    );
    write_sample(
        out,
        "ravel_distrib_quarantine_marks_total",
        &[Label::Mode(mode)],
        snapshot.quarantine_marks_total,
    );

    write_header(
        out,
        "ravel_distrib_quarantine_readmits_total",
        "Quarantined fragment endpoints readmitted by a strictly newer worker heartbeat stamp (the half-open probe), cumulative (ADR-0071 amendment decision 3).",
        "counter",
    );
    write_sample(
        out,
        "ravel_distrib_quarantine_readmits_total",
        &[Label::Mode(mode)],
        snapshot.quarantine_readmits_total,
    );

    write_header(
        out,
        "ravel_distrib_quarantine_current",
        "Fragment endpoints currently held in the coordinator's quarantine map (ADR-0071 amendment decision 3).",
        "gauge",
    );
    write_sample(
        out,
        "ravel_distrib_quarantine_current",
        &[Label::Mode(mode)],
        snapshot.quarantine_current,
    );
}

// One argument per metric source, each a distinct snapshot type: bundling
// them into one struct would only move the same list behind a name without
// removing a caller's need to build every field, so the sources stay
// positional and this lint is allowed here rather than worked around.
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
#[allow(clippy::too_many_arguments)]
pub fn render(
    mode: Mode,
    store: &StoreMetricsSnapshot,
    ingest: &[IngestPipelineSnapshot],
    catalog: &CatalogCountersSnapshot,
    maintain: Option<&MaintenanceDiscoverySnapshot>,
    maintain_safety: Option<&MaintenanceSafetySnapshot>,
    maintain_ownership: Option<&MaintenanceOwnershipSnapshot>,
    merge_memory: Option<&ravel_maintain::MergeMemoryTracker>,
    scrub: Option<&ScrubSnapshot>,
    cache: Option<&CacheMetricsSnapshot>,
    catalog_cache: Option<&CacheMetricsSnapshot>,
    admission: &AdmissionCountersSnapshot,
    query_accounting: &[QueryAccountingRow],
    ingest_concurrency_shed_total: u64,
    ingest_buffer_budget: IngestBufferBudgetSnapshot,
    distrib: Option<&DistribSnapshot>,
    durable_auth: Option<&DurableAuthCountersSnapshot>,
    attribution: &[TenantAttributionRow],
    metadata_cache: Option<&MetadataCacheCounters>,
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
    render_bucket_protection_family(
        &mut out,
        mode,
        crate::bucket_protection::bucket_protection_unknown(),
    );
    if let Some(snapshot) = durable_auth {
        render_durable_auth_family(&mut out, mode, snapshot);
    }
    render_query_postings_family(&mut out, mode, crate::query_postings_metrics::snapshot());
    render_typed_attr_columns_family(&mut out, mode, crate::typed_attr_metrics::stale_fallbacks());
    if let Some(counters) = metadata_cache {
        render_metadata_cache_family(&mut out, mode, counters);
    }
    if let Some(snapshot) = maintain {
        render_maintain_family(&mut out, mode, snapshot);
    }
    if let Some(snapshot) = maintain_safety {
        render_maintain_safety_family(&mut out, mode, snapshot);
    }
    if let Some(snapshot) = maintain_ownership {
        render_maintain_ownership_family(&mut out, mode, snapshot);
    }
    if let Some(tracker) = merge_memory {
        render_merge_memory_family(&mut out, mode, tracker);
    }
    if let Some(snapshot) = scrub {
        render_scrub_family(&mut out, mode, snapshot);
    }
    if cache.is_some() || catalog_cache.is_some() {
        render_cache_family(&mut out, mode, cache, catalog_cache);
    }
    render_admission_family(&mut out, mode, admission);
    render_query_family(&mut out, mode, query_accounting);
    render_attribution_family(&mut out, mode, attribution);
    render_ingest_concurrency_family(&mut out, mode, ingest_concurrency_shed_total);
    render_ingest_buffer_budget_family(
        &mut out,
        mode,
        ingest_buffer_budget.in_flight_bytes,
        ingest_buffer_budget.ceiling,
        ingest_buffer_budget.shed_total,
    );
    if let Some(snapshot) = distrib {
        render_distrib_family(&mut out, mode, snapshot);
    }
    out
}

/// The three process-wide ingest byte budget readings the `/metrics` handler
/// snapshots from [`ravel_ingest::IngestByteBudget`] at scrape time (ADR-0069).
/// `ceiling` is `0` for unlimited, matching the flag's convention.
#[derive(Debug, Clone, Copy, Default)]
pub struct IngestBufferBudgetSnapshot {
    pub in_flight_bytes: u64,
    pub ceiling: u64,
    pub shed_total: u64,
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
    /// to render (ADR-0048 decision 3).
    pub tenant_discovery: Option<Arc<crate::tenant_discovery::TenantDiscoveryMetrics>>,
    /// `Some` only in [`Mode::Maintain`], alongside `tenant_discovery` above
    /// (ADR-0048 decisions 1, 4, 6).
    pub maintenance_safety: Option<Arc<crate::maintain::MaintenanceSafetyMetrics>>,
    /// `Some` only in [`Mode::Maintain`], alongside `maintenance_safety` above:
    /// ADR-0065's stuck-owner mitigation counters.
    pub maintenance_ownership: Option<Arc<crate::maintain::MaintenanceOwnershipMetrics>>,
    /// `Some` only in [`Mode::Maintain`]: the ADR-0065 decision 4 RLOG k-way
    /// merge peak-bytes tracker, the same handle `ravel_maintain::rlog`'s real
    /// merge call sites record into.
    pub merge_memory: Option<ravel_maintain::MergeMemoryTracker>,
    /// `Some` only in [`Mode::Maintain`], alongside `maintenance_safety` above,
    /// the one mode that spawns the at-rest scrubber (ADR-0059).
    pub scrub: Option<Arc<crate::scrub::ScrubMetrics>>,
    /// The ADR-0046 fetcher cache's counters handle, or
    /// `None` when `--disable-cache` leaves no fetcher cache constructed at all.
    /// Rendered under `cache="fetch"`.
    pub cache_metrics: Option<Arc<ravel_cache::CacheMetrics>>,
    /// The ADR-0046 catalog byte cache's counters handle, or
    /// `None` when `--disable-cache` leaves no catalog byte cache constructed
    /// at all. Rendered under `cache="catalog"`, the same family as the fetcher
    /// cache above, so the documented hit-rate formula covers every ADR-0046
    /// cache in the process, not just the fetcher one. Sourced from
    /// [`ravel_catalog::Catalog::byte_cache_metrics`].
    pub catalog_cache_metrics: Option<Arc<ravel_cache::CacheMetrics>>,
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
    /// The process-global per-query cost aggregator (ADR-0044
    /// section 4), written by every query handler and read here at scrape time.
    /// Always present; renders no samples until a query records into it.
    pub query_accounting: Arc<QueryAccountingMetrics>,
    /// The per-tenant allowlist the PUT attribution family (ADR-0076 decision
    /// 2) folds against, same set and same `--metrics-tenant-labels` gate as
    /// `query_accounting`'s `configured` set: empty unless the flag is on, in
    /// which case it is `config.limits.tenants.keys()`. A tenant outside it
    /// folds into `tenant_hash="other"` at render time.
    pub metrics_tenant_allowlist: Arc<HashSet<TenantHash>>,
    /// The process-wide in-flight ingest-request ceiling, shared
    /// with every OTLP HTTP/gRPC service and Remote Write on both the public
    /// and mTLS listeners. Always present; its `shed_total` is simply `0`
    /// until the ceiling first rejects a request.
    pub ingest_concurrency: Arc<crate::ingest_concurrency::IngestConcurrencyController>,
    /// The process-wide ingest buffer byte budget (ADR-0069 decision 1), read
    /// at scrape time for the `ravel_ingest_buffer_bytes` gauge, its limit, and
    /// the `ravel_ingest_buffer_shed_total` counter.
    pub ingest_buffer_budget: Arc<ravel_ingest::IngestByteBudget>,
    /// The ADR-0071 distributed read fan-out counters. `Some` only
    /// when the process serves queries with `--distributed-query` on; `None`
    /// otherwise leaves the whole `ravel_distrib_*` family off the exposition.
    pub distrib: Option<Arc<crate::distrib::FragmentMetrics>>,
    /// The durable `sys/auth` background-refresh state (ADR-0066 decision 6),
    /// read at scrape time for its three refresh-loop counters. `Some` only
    /// when `--deployment-key` is set in a request-serving mode
    /// (`Mode::All`/`Gateway`/`Query`); `None` otherwise leaves the whole
    /// `ravel_durable_auth_*` family off the exposition.
    pub durable_auth: Option<Arc<crate::lifecycle_refresh::DurableAuthState>>,
    /// Per-tenant wire (compressed) request-body bytes (ADR-0084 decision 5),
    /// rendered alongside the admission family's charged (decompressed) bytes so
    /// the two together distinguish a tenant that increased telemetry from one
    /// that turned compression off. Always present; empty until an OTLP request
    /// is admitted. Folded by the same `--metrics-tenant-labels` gate.
    pub ingest_byte_metrics: Arc<crate::ingest_byte_metrics::IngestByteMetrics>,
    /// The per-process metric-metadata cache (ADR-0085 decision 1), read at
    /// scrape time for its four `query_metadata_cache_*` counters. `Some` only
    /// in a request-serving mode that built one (`Mode::All`/`Mode::Query`);
    /// `None` otherwise leaves the whole family off the exposition.
    pub metadata_cache: Option<Arc<ravel_query::http::MetadataCache>>,
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
                        orphans_present: metrics.orphans_present(signal),
                    })
                    .collect(),
            });

    let maintain_ownership_snapshot =
        state
            .maintenance_ownership
            .as_ref()
            .map(|metrics| MaintenanceOwnershipSnapshot {
                workers_live: metrics.workers_live(),
                units_owned: metrics.units_owned(),
                units_stalled: metrics.units_stalled(),
                memo_warm_start_units: metrics.memo_warm_start_units(),
                full_sweep_passes_total: metrics.full_sweep_passes_total(),
            });

    let scrub_snapshot = state.scrub.as_ref().map(|metrics| ScrubSnapshot {
        signals: crate::maintain::MAINTAINED_SIGNALS
            .iter()
            .map(|&signal| ScrubSignalSnapshot {
                signal,
                checksum_mismatch: metrics.checksum_mismatch(signal),
                postings_disagreement: metrics.postings_disagreement(signal),
                seal_divergence_missing: metrics.seal_divergence_missing(signal),
                seal_divergence_mismatched: metrics.seal_divergence_mismatched(signal),
                cursor_position: metrics.cursor_position(signal),
            })
            .collect(),
    });

    let cache_snapshot = state
        .cache_metrics
        .as_ref()
        .map(|metrics| metrics.snapshot());
    let catalog_cache_snapshot = state
        .catalog_cache_metrics
        .as_ref()
        .map(|metrics| metrics.snapshot());

    // Read the admission counters at scrape time (a lock-and-copy, no
    // `.await`), like every other family, rather than baking a snapshot in at
    // construction.
    let admission_snapshot = AdmissionCountersSnapshot {
        usage: state.admission.usage_snapshot(),
        tenant_labels: state.metrics_tenant_labels,
        wire_bytes: state.ingest_byte_metrics.snapshot(),
    };

    // Per-query cost rows, read at scrape time like every other family (a
    // lock-and-copy, no `.await`).
    let query_rows = state.query_accounting.snapshot();

    let ingest_concurrency_shed_total = state.ingest_concurrency.shed_total();

    let ingest_buffer_budget = IngestBufferBudgetSnapshot {
        in_flight_bytes: state.ingest_buffer_budget.in_flight_bytes(),
        ceiling: state.ingest_buffer_budget.ceiling().unwrap_or(0),
        shed_total: state.ingest_buffer_budget.shed_total(),
    };

    let distrib_snapshot = state
        .distrib
        .as_ref()
        .map(|metrics| DistribSnapshot::from_metrics(metrics));

    // Metric-metadata cache counters (ADR-0085 decision 1), read at scrape time
    // (atomic loads). `None` when this process built no cache (a non-request
    // mode), which omits the whole `query_metadata_cache_*` family.
    let metadata_cache_snapshot = state.metadata_cache.as_ref().map(|cache| cache.counters());

    // Read the durable-auth refresh-loop counters at scrape time (atomic loads),
    // like every other family. `None` when this process built no
    // `DurableAuthState`, which omits the whole family.
    let durable_auth_snapshot =
        state
            .durable_auth
            .as_ref()
            .map(|auth| DurableAuthCountersSnapshot {
                refresh_failures: auth.refresh_failures(),
                on_miss_rereads: auth.on_miss_rereads(),
                stale_fail_closed: auth.stale_fail_closed(),
            });

    // Per-tenant PUT attribution (ADR-0076 decision 2), read at scrape time
    // from each present router's `TenantPutAttribution` and folded through the
    // same allowlist `query_accounting` uses. One signal's router missing
    // (`Mode::Query`/`Mode::Maintain` build none) simply contributes no rows.
    let mut attribution = Vec::new();
    if let Some(router) = &state.ingest_router {
        attribution.extend(attribution_rows(
            Signal::Metrics,
            router.metrics().tenant_put_attribution(),
            &state.metrics_tenant_allowlist,
        ));
    }
    if let Some(router) = &state.log_ingest_router {
        attribution.extend(attribution_rows(
            Signal::Logs,
            router.metrics().tenant_put_attribution(),
            &state.metrics_tenant_allowlist,
        ));
    }
    if let Some(router) = &state.span_ingest_router {
        attribution.extend(attribution_rows(
            Signal::Spans,
            router.metrics().tenant_put_attribution(),
            &state.metrics_tenant_allowlist,
        ));
    }

    let body = render(
        state.mode,
        &store_snapshot,
        &pipelines,
        &catalog_snapshot,
        maintain_snapshot.as_ref(),
        maintain_safety_snapshot.as_ref(),
        maintain_ownership_snapshot.as_ref(),
        state.merge_memory.as_ref(),
        scrub_snapshot.as_ref(),
        cache_snapshot.as_ref(),
        catalog_cache_snapshot.as_ref(),
        &admission_snapshot,
        &query_rows,
        ingest_concurrency_shed_total,
        ingest_buffer_budget,
        distrib_snapshot.as_ref(),
        durable_auth_snapshot.as_ref(),
        &attribution,
        metadata_cache_snapshot.as_ref(),
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

    /// The acceptance test for the exposition renderer. Proves both halves: a populated
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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
        // This match has no wildcard arm, so a new `Label` variant fails this
        // compile until a case is added here, and the fixed array below then
        // fails the length assertion until it is extended too -- two
        // independent breaks for one added variant, by design. `reason` is the
        // eighth, added by ADR-0051 section 6 for the admission family; `cache`
        // is the ninth, added to split the read-cache family into
        // the fetcher and catalog byte caches; `kind` is the tenth, added by
        // ADR-0065 decision 4 for the RLOG merge-memory gauge.
        let one_of_each = [
            Label::TenantHash(TenantHashLabel::Other),
            Label::Signal(Signal::Metrics),
            Label::Mode(Mode::All),
            Label::Op(StoreOp::Get),
            Label::ErrorKind(StoreErrorClass::NotFound),
            Label::WorkloadClass(WorkloadClass::Interactive),
            Label::Level(Level::Info),
            Label::RejectReason(RejectReason::ByteRate),
            Label::ScrubReason(ScrubReason::Missing),
            Label::Cache(CacheFamily::Fetch),
            Label::MergeMemoryKind(MergeMemoryKind::Transient),
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
                Label::ScrubReason(_) => "reason",
                Label::Cache(_) => "cache",
                Label::MergeMemoryKind(_) => "kind",
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
                // ScrubReason (ADR-0059 section 2) reuses the `reason` key, so
                // the allowlist of distinct keys is unchanged; two variants map
                // to it.
                "reason",
                "cache",
                "kind",
            ],
            "ADR-0044 section 4's allowlist plus ADR-0051 section 6's `reason` (also reused by \
             ADR-0059 section 2's scrub seal-divergence family), the `cache` label, and \
             ADR-0065 decision 4's `kind`; `shard` must never appear here"
        );
        assert_eq!(
            one_of_each.len(),
            11,
            "exactly 11 label variants, 10 distinct keys"
        );
    }

    /// The POSTINGS family renders one sample per metric for the
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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

    /// The dynamic-column budget family (ADR-0100 decision 1) renders its three
    /// samples, each labelled with exactly `{mode, signal="logs"}`.
    ///
    /// A sibling of `postings_family_carries_only_allowlisted_labels` rather
    /// than an addition to it: that test filters the `ravel_logs_postings_`
    /// prefix and asserts an exact sample count, so these three names fall
    /// outside its net entirely. Without this, a stray per-attribute-name label
    /// on the budget family would violate the ADR-0044 allowlist with no test
    /// failing.
    #[test]
    fn dynamic_columns_family_carries_only_allowlisted_labels() {
        let ingest = vec![
            IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot {
                dynamic_columns_used_total: 13,
                dynamic_columns_overflowed_total: 5,
                dynamic_columns_used_max: 8,
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );

        let lines: Vec<&str> = body
            .lines()
            .filter(|l| l.starts_with("ravel_logs_dynamic_columns_"))
            .collect();
        // Three metrics, one sample each: only the log pipeline carries them.
        assert_eq!(
            lines.len(),
            3,
            "one sample per dynamic-column metric, log pipeline only:\n{body}"
        );

        for line in &lines {
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
                "a dynamic-column sample must carry only {{mode, signal}}, never an \
                 attribute key (ADR-0044): {line}"
            );
            assert!(
                line.contains("signal=\"logs\""),
                "the dynamic-column budget is a log-only family: {line}"
            );
        }
    }

    /// The prune-selectivity family renders its three counters,
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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

    /// The four metric metadata sink counters (ADR-0085 decision 1) render
    /// for the metrics pipeline, carrying the driven values, and render
    /// nothing for logs/spans pipelines -- `metadata_sink` is a
    /// metrics-only concept, the same structural-absence convention
    /// `collisions` and `postings` use, checked here so a future edit
    /// cannot silently stop exporting them the way this family started out
    /// (ADR-0085's own doc comments claimed "Exported as ..." for a build
    /// that rendered nothing).
    #[test]
    fn metadata_sink_counters_render_for_metrics_only() {
        let ingest = vec![
            IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot {
                metadata_flush_gets_total: 7,
                metadata_flush_puts_total: 3,
                metadata_flush_dropped_total: 1,
                metadata_entries_dropped_total: 42,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot::default()),
            IngestPipelineSnapshot::from_span_metrics(SpanIngestMetricsSnapshot::default()),
        ];
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &ingest,
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );

        assert!(body.contains(
            "ravel_ingest_metadata_flush_gets_total{mode=\"gateway\",signal=\"metrics\"} 7"
        ));
        assert!(body.contains(
            "ravel_ingest_metadata_flush_puts_total{mode=\"gateway\",signal=\"metrics\"} 3"
        ));
        assert!(body.contains(
            "ravel_ingest_metadata_flush_dropped_total{mode=\"gateway\",signal=\"metrics\"} 1"
        ));
        assert!(body.contains(
            "ravel_ingest_metadata_entries_dropped_total{mode=\"gateway\",signal=\"metrics\"} 42"
        ));
        assert!(
            !body.contains(
                "ravel_ingest_metadata_flush_gets_total{mode=\"gateway\",signal=\"logs\""
            ),
            "logs pipeline must render no metadata_sink sample"
        );
        assert!(
            !body.contains(
                "ravel_ingest_metadata_flush_gets_total{mode=\"gateway\",signal=\"spans\""
            ),
            "spans pipeline must render no metadata_sink sample"
        );
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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

    /// ADR-0048 decision 3: the tenant discovery gauges and
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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

    /// Proves all three durable-auth refresh-loop counters reach `/metrics`
    /// and carry the real values their underlying conditions produced, not
    /// zero placeholders. Each of the three is driven by its genuine trigger
    /// on a real [`DurableAuthState`] -- a failed refresh (a faulting
    /// sys/auth GET), an on-miss re-read, and a hard-stale fail-closed
    /// resolution -- then that state's counters are snapshotted and rendered
    /// exactly as the `/metrics` handler does, so the full
    /// condition->counter->exposition chain is asserted end to end.
    #[tokio::test]
    async fn render_includes_durable_auth_counters_that_incremented() {
        use ravel_object_store::ObjectStoreBackend;
        use ravel_object_store::fault::{FaultPlan, Op, Rule, ScriptedFault};
        use ravel_object_store::memory::MemoryStore;

        use crate::lifecycle_refresh::{AuthResolution, DurableAuthState};

        const KEY: [u8; 32] = [0x42u8; 32];
        let horizon_ns: i64 = 60_000_000_000;
        let hard_multiple: i64 = 3;
        let on_miss_ns: i64 = 1_000_000_000;
        let t0 = 1_000 * horizon_ns;

        // A store that fails every sys/auth GET, so the background refresh can
        // never succeed and never advances the staleness gate off its t0 seed.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("auth store down".into()))
                .with_key_contains("sys/auth"),
        );
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(
            ravel_object_store::fault::FaultStore::new(MemoryStore::new(), plan),
        );
        let state = DurableAuthState::new(store, KEY, horizon_ns, hard_multiple, on_miss_ns, t0);

        // Condition 1: a refresh that genuinely cannot read sys/auth.
        assert!(
            state.refresh(t0).await.is_err(),
            "a faulting sys/auth GET must fail the refresh"
        );
        // Condition 2: an on-miss re-read begun after the rate-limit window.
        assert!(
            state.try_begin_on_miss_reread(t0),
            "the first on-miss re-read is allowed"
        );
        // Condition 3: a resolution one ns past the hard staleness bound fails
        // closed (the gate was never advanced past its t0 seed).
        assert_eq!(
            state.resolve_token(b"tok", t0 + horizon_ns * hard_multiple + 1),
            AuthResolution::StaleFailClosed,
            "past the hard bound the resolver must fail closed"
        );

        // Each counter observed its condition exactly once; the family is not a
        // zero placeholder.
        assert_eq!(state.refresh_failures(), 1);
        assert_eq!(state.on_miss_rereads(), 1);
        assert_eq!(state.stale_fail_closed(), 1);

        // Snapshot exactly as the /metrics handler does, then render.
        let snapshot = DurableAuthCountersSnapshot {
            refresh_failures: state.refresh_failures(),
            on_miss_rereads: state.on_miss_rereads(),
            stale_fail_closed: state.stale_fail_closed(),
        };
        let body = render(
            Mode::Query,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            Some(&snapshot),
            &[],
            None,
        );

        // All three counters appear, mode-labeled, carrying the driven value.
        assert!(
            body.contains("ravel_durable_auth_refresh_failures_total{mode=\"query\"} 1"),
            "missing refresh-failures counter:\n{body}"
        );
        assert!(
            body.contains("ravel_durable_auth_on_miss_rereads_total{mode=\"query\"} 1"),
            "missing on-miss-rereads counter:\n{body}"
        );
        assert!(
            body.contains("ravel_durable_auth_stale_fail_closed_total{mode=\"query\"} 1"),
            "missing stale-fail-closed counter:\n{body}"
        );
        // Each carries a counter TYPE header.
        assert!(body.contains("# TYPE ravel_durable_auth_refresh_failures_total counter"));
        assert!(body.contains("# TYPE ravel_durable_auth_on_miss_rereads_total counter"));
        assert!(body.contains("# TYPE ravel_durable_auth_stale_fail_closed_total counter"));
    }

    /// The `ravel_durable_auth_*` family is omitted entirely when the process
    /// built no `DurableAuthState` (no `--deployment-key`, or `Mode::Maintain`),
    /// the same omission discipline every optional family here keeps rather than
    /// exporting three permanent zeros.
    #[test]
    fn render_omits_durable_auth_family_when_absent() {
        let body = render(
            Mode::Maintain,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );
        assert!(
            !body.contains("ravel_durable_auth_"),
            "durable-auth family must be absent when no DurableAuthState exists:\n{body}"
        );
    }

    /// the three maintenance safety controls (ADR-0048 decisions
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
                    orphans_present: 9,
                },
                MaintenanceSafetySignalSnapshot {
                    signal: Signal::Logs,
                    conservation_aborts: 0,
                    orphan_breaker_trips: 0,
                    orphans_withheld: 0,
                    orphans_present: 0,
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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
        assert!(
            body.contains("ravel_maintain_orphans_present{mode=\"maintain\",signal=\"metrics\"} 9"),
            "missing orphans_present sample:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_maintain_orphans_present gauge"),
            "orphans_present must carry a gauge TYPE header:\n{body}"
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

    /// The ADR-0071 distributed read fan-out family renders under
    /// the new `ravel_distrib_*` names, and every one of its series carries only
    /// the closed `{mode}` label: no per-shard, per-worker, or per-tenant label
    /// (ADR-0044 section 4). Also asserts the family is absent entirely when the
    /// snapshot is `None`, matching the "off unless --distributed-query" wiring.
    #[test]
    fn render_includes_distrib_family_with_only_allowlisted_labels() {
        let mut buckets = [0u64; LATENCY_BUCKET_COUNT];
        buckets[0] = 5;
        buckets[2] = 3;
        let snapshot = DistribSnapshot {
            fragment_requests_total: 11,
            fragment_auth_failures_total: 2,
            fragment_inflight: 1,
            slices_local_total: 7,
            slices_remote_total: 4,
            slices_redispatched_total: 2,
            slices_fallback_total: 1,
            slice_fetch_micros_buckets: buckets,
            slice_fetch_nanos_total: 123_000,
            quarantine_marks_total: 0,
            quarantine_readmits_total: 0,
            quarantine_current: 0,
        };
        let body = render(
            Mode::Query,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            Some(&snapshot),
            None,
            &[],
            None,
        );

        for expected in [
            "ravel_distrib_fragment_requests_total{mode=\"query\"} 11",
            "ravel_distrib_fragment_auth_failures_total{mode=\"query\"} 2",
            "ravel_distrib_fragment_inflight{mode=\"query\"} 1",
            "ravel_distrib_slices_local_total{mode=\"query\"} 7",
            "ravel_distrib_slices_remote_total{mode=\"query\"} 4",
            "ravel_distrib_slices_redispatched_total{mode=\"query\"} 2",
            "ravel_distrib_slices_fallback_total{mode=\"query\"} 1",
        ] {
            assert!(body.contains(expected), "missing `{expected}`:\n{body}");
        }
        // The histogram: cumulative buckets, a `_sum` in seconds, and a `_count`
        // equal to the `+Inf` bucket (5 + 3 = 8 observations).
        assert!(
            body.contains("ravel_distrib_slice_fetch_seconds_bucket{mode=\"query\",le=\"+Inf\"} 8"),
            "histogram +Inf bucket must total every observation:\n{body}"
        );
        assert!(
            body.contains("ravel_distrib_slice_fetch_seconds_count{mode=\"query\"} 8"),
            "histogram _count must equal the +Inf bucket:\n{body}"
        );
        assert!(
            body.contains("ravel_distrib_slice_fetch_seconds_sum{mode=\"query\"} 0.000123"),
            "histogram _sum must render seconds:\n{body}"
        );

        // Every ravel_distrib_ series line carries exactly the `{mode}` label
        // (plus `le` on histogram buckets); no disallowed label leaks in.
        for line in body.lines() {
            if !line.starts_with("ravel_distrib_") {
                continue;
            }
            let labels = line
                .split_once('{')
                .and_then(|(_, rest)| rest.split_once('}'))
                .map(|(labels, _)| labels)
                .unwrap_or("");
            for pair in labels.split(',').filter(|p| !p.is_empty()) {
                let key = pair.split('=').next().unwrap_or(pair);
                assert!(
                    key == "mode" || key == "le",
                    "disallowed label `{key}` on ravel_distrib series: {line}"
                );
            }
        }

        // Absent entirely when the process is not distributing.
        let off = render(
            Mode::Query,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );
        assert!(
            !off.contains("ravel_distrib_"),
            "the distrib family must be absent without --distributed-query:\n{off}"
        );
    }

    #[test]
    fn render_includes_scrub_family() {
        let snapshot = ScrubSnapshot {
            signals: vec![
                ScrubSignalSnapshot {
                    signal: Signal::Metrics,
                    checksum_mismatch: 2,
                    postings_disagreement: 1,
                    seal_divergence_missing: 3,
                    seal_divergence_mismatched: 4,
                    cursor_position: 0.5,
                },
                ScrubSignalSnapshot {
                    signal: Signal::Logs,
                    checksum_mismatch: 0,
                    postings_disagreement: 0,
                    seal_divergence_missing: 0,
                    seal_divergence_mismatched: 0,
                    cursor_position: 0.0,
                },
            ],
        };
        let body = render(
            Mode::Maintain,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            Some(&snapshot),
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );

        assert!(
            body.contains(
                "ravel_scrub_checksum_mismatch_total{mode=\"maintain\",signal=\"metrics\"} 2"
            ),
            "missing checksum_mismatch sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_scrub_postings_disagreement_total{mode=\"maintain\",signal=\"metrics\"} 1"
            ),
            "missing postings_disagreement sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_scrub_seal_divergence_total{mode=\"maintain\",signal=\"metrics\",reason=\"missing\"} 3"
            ),
            "missing seal_divergence missing sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_scrub_seal_divergence_total{mode=\"maintain\",signal=\"metrics\",reason=\"mismatched\"} 4"
            ),
            "missing seal_divergence mismatched sample:\n{body}"
        );
        // Zero-valued signal still renders both reasons (zero-is-not-absence).
        assert!(
            body.contains(
                "ravel_scrub_seal_divergence_total{mode=\"maintain\",signal=\"logs\",reason=\"missing\"} 0"
            ),
            "a zero-valued signal must still render both seal-divergence reasons:\n{body}"
        );
        assert!(
            body.contains("ravel_scrub_cursor_position{mode=\"maintain\",signal=\"metrics\"} 0.5"),
            "missing cursor_position gauge sample:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_scrub_cursor_position gauge"),
            "cursor_position must carry a gauge TYPE header:\n{body}"
        );
        // Zero-valued signal (logs) still renders: zero-is-not-absence.
        assert!(
            body.contains(
                "ravel_scrub_checksum_mismatch_total{mode=\"maintain\",signal=\"logs\"} 0"
            ),
            "a zero-valued signal must still render:\n{body}"
        );
        // No tenant_hash label on this unauthenticated route (ADR-0044 §4).
        for line in body.lines().filter(|l| l.starts_with("ravel_scrub_")) {
            assert!(
                !line.contains("tenant_hash"),
                "scrub family must never render a tenant_hash label: {line}"
            );
        }
    }

    /// A `None` scrub snapshot (every non-Maintain mode) renders no scrub
    /// series at all, matching the maintain families' Maintain-only gating.
    #[test]
    fn render_omits_scrub_family_when_absent() {
        let body = render(
            Mode::Query,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );
        assert!(
            !body.contains("ravel_scrub_"),
            "no scrub series should render when the snapshot is absent:\n{body}"
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
                orphans_present: 1,
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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );

        for line in body.lines() {
            let expected_keys =
                if line.starts_with("ravel_maintain_legal_hold_refresh_failures_total") {
                    vec!["mode"]
                } else if line.starts_with("ravel_maintain_conservation_aborts_total")
                    || line.starts_with("ravel_maintain_orphan_breaker_tripped_total")
                    || line.starts_with("ravel_maintain_orphans_withheld")
                    || line.starts_with("ravel_maintain_orphans_present")
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
    fn cache_family_renders_both_caches_labeled_distinctly_and_omits_single_flight_collapses() {
        // the fetcher cache (cache="fetch") and the catalog byte
        // cache (cache="catalog") share this family, told apart by the `cache`
        // label, so the documented hit-rate formula covers every ADR-0046
        // cache. Distinct values per cache so a mislabeled sample is caught.
        let fetch = CacheMetricsSnapshot {
            hits: 10,
            misses: 4,
            bytes_served: 2048,
            bytes_admitted: 1024,
            admissions_rejected_size: 1,
            evictions: 2,
            single_flight_collapses: 99,
            disk_errors_degraded_to_misses: 3,
            disk_entries_expired_max_age: 7,
        };
        let catalog = CacheMetricsSnapshot {
            hits: 70,
            misses: 5,
            bytes_served: 4096,
            bytes_admitted: 8192,
            admissions_rejected_size: 0,
            evictions: 6,
            single_flight_collapses: 11,
            disk_errors_degraded_to_misses: 0,
            disk_entries_expired_max_age: 0,
        };
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            Some(&fetch),
            Some(&catalog),
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );

        // Fetcher cache, labeled cache="fetch".
        assert!(
            body.contains("ravel_cache_hits_total{mode=\"gateway\",cache=\"fetch\"} 10"),
            "missing fetch cache hits sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_misses_total{mode=\"gateway\",cache=\"fetch\"} 4"),
            "missing fetch cache misses sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_bytes_served_total{mode=\"gateway\",cache=\"fetch\"} 2048"),
            "missing fetch cache bytes_served sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_cache_bytes_admitted_total{mode=\"gateway\",cache=\"fetch\"} 1024"
            ),
            "missing fetch cache bytes_admitted sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_evictions_total{mode=\"gateway\",cache=\"fetch\"} 2"),
            "missing fetch cache evictions sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_cache_disk_errors_degraded_to_misses_total{mode=\"gateway\",cache=\"fetch\"} 3"
            ),
            "missing fetch cache disk_errors_degraded_to_misses sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_cache_disk_entries_expired_max_age_total{mode=\"gateway\",cache=\"fetch\"} 7"
            ),
            "missing fetch cache disk_entries_expired_max_age sample:\n{body}"
        );

        // Catalog byte cache, labeled cache="catalog", same metric names.
        assert!(
            body.contains("ravel_cache_hits_total{mode=\"gateway\",cache=\"catalog\"} 70"),
            "missing catalog cache hits sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_misses_total{mode=\"gateway\",cache=\"catalog\"} 5"),
            "missing catalog cache misses sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_cache_bytes_served_total{mode=\"gateway\",cache=\"catalog\"} 4096"
            ),
            "missing catalog cache bytes_served sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_cache_bytes_admitted_total{mode=\"gateway\",cache=\"catalog\"} 8192"
            ),
            "missing catalog cache bytes_admitted sample:\n{body}"
        );
        assert!(
            body.contains("ravel_cache_evictions_total{mode=\"gateway\",cache=\"catalog\"} 6"),
            "missing catalog cache evictions sample:\n{body}"
        );

        // Each metric name still carries exactly one HELP/TYPE header even with
        // both caches present (Prometheus requires one per name).
        assert_eq!(
            body.matches("# TYPE ravel_cache_hits_total counter")
                .count(),
            1,
            "each cache metric name must declare its TYPE exactly once:\n{body}"
        );

        assert!(
            !body.contains("single_flight_collapse"),
            "fleet-wide single-flight collapse rate must never be \
             emitted on /metrics, found in:\n{body}"
        );
    }

    #[test]
    fn only_the_attached_cache_family_renders() {
        // The fetcher cache is off (None) but the catalog byte cache is on:
        // only cache="catalog" renders, no cache="fetch" phantom sample.
        let catalog = CacheMetricsSnapshot {
            hits: 7,
            ..Default::default()
        };
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&catalog),
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );
        assert!(
            body.contains("ravel_cache_hits_total{mode=\"gateway\",cache=\"catalog\"} 7"),
            "catalog cache must render when it is the only cache attached:\n{body}"
        );
        assert!(
            !body.contains("cache=\"fetch\""),
            "no fetch cache sample when the fetcher cache is off:\n{body}"
        );
    }

    #[test]
    fn cache_family_omitted_entirely_when_no_cache_is_attached() {
        // A `--disable-cache` process attaches neither the fetcher cache nor
        // the catalog byte cache, so the whole family is absent.
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );

        assert!(
            !body.contains("ravel_cache_"),
            "a server run with --disable-cache must not render any cache \
             family at all, neither fetch nor catalog:\n{body}"
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
            requests_rejected_clock_total: 0,
            series_rejected_cap_total: 0,
            reconciliation_failures_total: 0,
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
            wire_bytes: Vec::new(),
        };
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &snapshot,
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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
            wire_bytes: Vec::new(),
        };
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &snapshot,
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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

    /// The 2026-08-13 amendment's `reason="clock"` series renders from the
    /// per-tenant clock-rejection counter, and it is present even at zero
    /// (zero-is-not-absence), so a scraper can alert on it appearing.
    #[test]
    fn admission_family_renders_the_clock_reason() {
        let mut row = tenant_usage("skewed", Signal::Metrics);
        row.requests_rejected_clock_total = 7;
        let snapshot = AdmissionCountersSnapshot {
            usage: vec![row],
            tenant_labels: true,
            wire_bytes: Vec::new(),
        };
        let hash = ravel_types::TenantId::new("skewed").hash().to_hex();
        let body = render(
            Mode::Gateway,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &snapshot,
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
        );
        assert!(
            body.contains(&format!(
                "ravel_admission_rejected_total{{mode=\"gateway\",tenant_hash=\"{hash}\",\
                 signal=\"metrics\",reason=\"clock\"}} 7"
            )),
            "clock rejection must render distinctly:\n{body}"
        );
    }

    // --- the per-query cost family ---

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
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &metrics.snapshot(),
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
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

    /// THE FOLD TEST. With no tenant configured
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
    /// series (ADR-0044 section 3), so their
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

    /// Drive the real [`render`] entry point with every optional source off
    /// except `distrib` and `metadata_cache`, so a family test asserts what the
    /// scrape actually produces, not what a formatting helper produces in
    /// isolation.
    fn render_distrib_and_metadata(
        mode: Mode,
        distrib: Option<&DistribSnapshot>,
        metadata_cache: Option<&MetadataCacheCounters>,
    ) -> String {
        render(
            mode,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            distrib,
            None,
            &[],
            metadata_cache,
        )
    }

    /// Every line for `name` (a sample, not a `# HELP`/`# TYPE` header) carries
    /// exactly the allowed label keys and no other, mirroring the distrib
    /// family's own label guard.
    fn assert_only_labels(body: &str, name_prefix: &str, allowed: &[&str]) -> usize {
        let mut samples = 0;
        for line in body.lines() {
            if !line.starts_with(name_prefix) || line.starts_with("# ") {
                continue;
            }
            samples += 1;
            let labels = line
                .split_once('{')
                .and_then(|(_, rest)| rest.split_once('}'))
                .map(|(labels, _)| labels)
                .unwrap_or("");
            for pair in labels.split(',').filter(|p| !p.is_empty()) {
                let key = pair.split('=').next().unwrap_or(pair);
                assert!(
                    allowed.contains(&key),
                    "disallowed label `{key}` on `{name_prefix}` series: {line}"
                );
            }
        }
        samples
    }

    /// The metric-metadata cache family (#258, ADR-0085 decision 1) renders its
    /// four `query_metadata_cache_*` counters through the real [`render`] entry
    /// point, each as a counter carrying only the closed `{mode}` label, and the
    /// whole family is absent when no cache is wired (a non-request mode).
    ///
    /// prove-the-test: removing the `render_metadata_cache_family` call from
    /// [`render`] drops every `query_metadata_cache_*` line, so the
    /// `hits`/`misses`/`refreshes`/`refresh_errors` `assert!`s below fail with
    /// "missing" and the sample-count assertion fails `4 != 0`.
    #[test]
    fn metadata_cache_family_carries_only_allowlisted_labels() {
        let counters = MetadataCacheCounters {
            hits: 9,
            misses: 1,
            refreshes: 3,
            refresh_errors: 2,
        };
        let body = render_distrib_and_metadata(Mode::Query, None, Some(&counters));

        for expected in [
            "query_metadata_cache_hits_total{mode=\"query\"} 9",
            "query_metadata_cache_misses_total{mode=\"query\"} 1",
            "query_metadata_cache_refreshes_total{mode=\"query\"} 3",
            "query_metadata_cache_refresh_errors_total{mode=\"query\"} 2",
        ] {
            assert!(body.contains(expected), "missing `{expected}`:\n{body}");
        }
        // Each of the four names is a counter, not a gauge.
        for name in [
            "query_metadata_cache_hits_total",
            "query_metadata_cache_misses_total",
            "query_metadata_cache_refreshes_total",
            "query_metadata_cache_refresh_errors_total",
        ] {
            assert!(
                body.contains(&format!("# TYPE {name} counter")),
                "`{name}` must declare a counter TYPE line:\n{body}"
            );
        }
        // Exactly four samples, each carrying only `{mode}`.
        let samples = assert_only_labels(&body, "query_metadata_cache_", &["mode"]);
        assert_eq!(
            samples, 4,
            "the family renders exactly four samples:\n{body}"
        );

        // Absent entirely when no cache is wired.
        let without = render_distrib_and_metadata(Mode::Query, None, None);
        assert!(
            !without.contains("query_metadata_cache_"),
            "the family must be omitted when no cache is present:\n{without}"
        );
    }

    /// The metadata-cache counters reach the renderer from their real source:
    /// a live [`ravel_query::http::MetadataCache`] over a `MemoryStore` driven
    /// through a miss then a hit, whose [`MetadataCache::counters`] snapshot the
    /// renderer then emits. A hand-built snapshot could pass even if the exporter
    /// never read the cache; this drives the same `counters()` the handler reads.
    #[tokio::test]
    async fn metadata_cache_counters_reach_renderer_from_real_cache() {
        use ravel_object_store::ObjectStoreBackend;
        use std::sync::Arc;

        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(ravel_object_store::memory::MemoryStore::new());
        let cache = ravel_query::http::MetadataCache::new(
            store,
            ravel_query::http::MetadataCacheConfig::default(),
            Arc::new(ravel_cache::SystemClock),
        );
        let tenant = TenantHash([0x5Au8; 16]);
        // First request: a miss that fills inline. Second: a hit within the
        // horizon, no I/O. So misses == 1 and hits == 1 at the real source.
        let _ = cache.get(tenant).await;
        let _ = cache.get(tenant).await;
        let counters = cache.counters();
        assert_eq!(counters.misses, 1, "one miss drove the fill");
        assert_eq!(counters.hits, 1, "the second request was a hit");

        let body = render_distrib_and_metadata(Mode::Query, None, Some(&counters));
        assert!(
            body.contains("query_metadata_cache_misses_total{mode=\"query\"} 1"),
            "the real miss count must reach the exposition:\n{body}"
        );
        assert!(
            body.contains("query_metadata_cache_hits_total{mode=\"query\"} 1"),
            "the real hit count must reach the exposition:\n{body}"
        );
    }

    /// The dead-endpoint quarantine metrics (#269, ADR-0071 amendment decision 3)
    /// render through the real [`render`] entry point: the two totals as counters
    /// and the currently-quarantined value as a gauge, each carrying only the
    /// closed `{mode}` label.
    ///
    /// prove-the-test: removing the three quarantine `write_header`/`write_sample`
    /// blocks at the end of `render_distrib_family` drops these lines, so the
    /// `contains` assertions fail with "missing" and the gauge-TYPE assertion
    /// fails.
    #[test]
    fn distrib_quarantine_family_carries_only_allowlisted_labels() {
        let snapshot = DistribSnapshot {
            fragment_requests_total: 0,
            fragment_auth_failures_total: 0,
            fragment_inflight: 0,
            slices_local_total: 0,
            slices_remote_total: 0,
            slices_redispatched_total: 0,
            slices_fallback_total: 0,
            slice_fetch_micros_buckets: [0u64; LATENCY_BUCKET_COUNT],
            slice_fetch_nanos_total: 0,
            quarantine_marks_total: 4,
            quarantine_readmits_total: 2,
            quarantine_current: 1,
        };
        let body = render_distrib_and_metadata(Mode::Query, Some(&snapshot), None);

        for expected in [
            "ravel_distrib_quarantine_marks_total{mode=\"query\"} 4",
            "ravel_distrib_quarantine_readmits_total{mode=\"query\"} 2",
            "ravel_distrib_quarantine_current{mode=\"query\"} 1",
        ] {
            assert!(body.contains(expected), "missing `{expected}`:\n{body}");
        }
        // The two totals are counters; the currently-quarantined value is a gauge
        // (a present-value with no `_total` suffix).
        assert!(
            body.contains("# TYPE ravel_distrib_quarantine_marks_total counter"),
            "marks must be a counter:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_distrib_quarantine_readmits_total counter"),
            "readmits must be a counter:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_distrib_quarantine_current gauge"),
            "the currently-quarantined value must be a gauge:\n{body}"
        );
        // Every quarantine sample carries only `{mode}`, no per-worker or
        // per-endpoint label (the exact unbounded-cardinality shape ADR-0044
        // rejects for this data).
        let samples = assert_only_labels(&body, "ravel_distrib_quarantine_", &["mode"]);
        assert_eq!(
            samples, 3,
            "the quarantine family renders exactly three samples:\n{body}"
        );
    }
}
