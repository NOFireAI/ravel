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
//! `error_kind`, `workload_class`, and `level` (ADR-0044 section 4). Every
//! variant's payload is a closed enum or [`TenantHash`]'s fixed-width hash, so
//! there is no `String` or `&str` anywhere on this path an unlisted label
//! could travel through, and adding an eighth variant is a compile error
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

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use ravel_catalog::Catalog;
use ravel_ingest::{
    IngestMetricsSnapshot, IngestRouter, LogIngestMetricsSnapshot, LogIngestRouter,
    SpanIngestMetricsSnapshot, SpanIngestRouter,
};
use ravel_object_store::StoreMetrics;
use ravel_object_store::instrument::{
    LATENCY_BUCKET_BOUNDS_MICROS, LATENCY_BUCKET_COUNT, StoreErrorClass, StoreMetricsSnapshot,
    StoreOp,
};
use ravel_types::{Signal, TenantHash};

use crate::config::Mode;

/// Reserved for future query-classification series (issue #425); no sample
/// this module renders uses it yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadClass {
    Interactive,
    Background,
}

impl WorkloadClass {
    fn name(self) -> &'static str {
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

/// A `tenant_hash` label value: either a configured tenant's fixed-width hash
/// or the `other` bucket every unconfigured tenant folds into (ADR-0044
/// section 4), so per-tenant cardinality is bounded by the configured tenant
/// count rather than by traffic. No sample this module renders is per-tenant
/// yet (`StoreMetrics`, ingest metrics, and the catalog anomaly counters are
/// all process-global by design); this exists so a future per-tenant source
/// has a value to construct.
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
        write_sample(
            out,
            "ravel_store_latency_seconds_count",
            &[Label::Mode(mode), Label::Op(op)],
            op_snapshot.calls,
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
}

/// The two catalog anomaly counters (`crates/ravel-catalog/src/catalog.rs`),
/// decoupled from `Catalog` itself so the renderer is testable with a plain
/// struct literal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CatalogCountersSnapshot {
    pub interlock_violations: u64,
    pub compaction_input_set_conflicts: u64,
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
}

/// Render every source this module knows about into one Prometheus text
/// exposition document. `ingest` is empty in a mode that builds no ingest
/// router (`Mode::Query`, `Mode::Maintain`): those families are omitted
/// entirely rather than rendered with no samples, since the pipelines
/// structurally do not exist in that mode. `store` and `catalog` are always
/// present: the store and the catalog are built in every mode.
pub fn render(
    mode: Mode,
    store: &StoreMetricsSnapshot,
    ingest: &[IngestPipelineSnapshot],
    catalog: &CatalogCountersSnapshot,
) -> String {
    let mut out = String::new();
    render_store_family(&mut out, mode, store);
    if !ingest.is_empty() {
        render_ingest_family(&mut out, mode, ingest);
    }
    render_catalog_family(&mut out, mode, catalog);
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
    };

    let body = render(state.mode, &store_snapshot, &pipelines, &catalog_snapshot);
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
        // independent breaks for one added variant, by design.
        let one_of_each = [
            Label::TenantHash(TenantHashLabel::Other),
            Label::Signal(Signal::Metrics),
            Label::Mode(Mode::All),
            Label::Op(StoreOp::Get),
            Label::ErrorKind(StoreErrorClass::NotFound),
            Label::WorkloadClass(WorkloadClass::Interactive),
            Label::Level(Level::Info),
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
            ],
            "ADR-0044 section 4's exhaustive label allowlist; `shard` must never appear here"
        );
        assert_eq!(one_of_each.len(), 7, "exactly 7 permitted label keys");
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
        };
        let body = render(Mode::Gateway, &store, &ingest, &catalog);

        let mut declared_types: HashSet<String> = HashSet::new();
        let mut bucket_state: std::collections::HashMap<String, (Vec<u64>, u64)> =
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
            if let Some(labels) = labels {
                assert!(!labels.is_empty(), "empty label block: {line}");
                for pair in labels.split(',') {
                    let (key, quoted) = pair.split_once('=').expect("label is key=value");
                    assert!(!key.is_empty(), "empty label key: {line}");
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
                    .entry(
                        format!("{name}{labels:?}")
                            .replace("le=\\\"", "")
                            .replace(&format!(",le=\"{le}\""), ""),
                    )
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
        }

        assert!(
            declared_types.contains("ravel_store_latency_seconds"),
            "histogram TYPE line missing"
        );
    }

    #[test]
    fn zero_valued_snapshot_renders_valid_output_not_omitted() {
        let body = render(
            Mode::Maintain,
            &StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
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
            body.contains(
                "ravel_store_latency_seconds_bucket{mode=\"maintain\",op=\"get\",le=\"+Inf\"} 0"
            ),
            "zero histogram must still render every bucket:\n{body}"
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
}
