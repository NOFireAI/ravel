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
//! [`Label`] is the only way to attach a label to a rendered sample, and it
//! renders exactly eighteen label keys: `tenant_hash`, `signal`, `mode`, `op`,
//! `error_kind`, `workload_class`, `level`, `reason`, `shard`, `cache`, `tier`,
//! `kind`, `outcome`, `allocator`, `stat`, `component`, `class`, and `carrier`
//! (ADR-0044 section 4; `reason` added by ADR-0051 section 6 for the
//! admission-rejection family and reused by ADR-0059 section 2 for the scrub
//! seal-divergence family, `shard` added by ADR-1692 decision 2 as the ninth
//! key for the per-shard ingest-skew family, `cache` to split the read-cache
//! family into the fetcher and catalog byte caches, `tier` added by #97 to
//! split each of those into its RAM and local-disk tiers when a disk tier is
//! configured, `kind` added by ADR-0065 decision 4 to split the maintenance
//! merge-memory gauge into its transient and total high-water marks,
//! `outcome` added by #532 to split the alert-tick family by how one
//! evaluation tick ended, `allocator`/`stat` added by #1170 for the process
//! allocator gauges, `component` added by ADR-1170 decision 4 to split the
//! process memory budget's reserved-bytes gauge by which side reserved it,
//! `class` added by ADR-0071's admission disjointness deliverable (issue
//! #1722) to split the fragment in-flight gauge and admission-wait counters
//! into their `Pinned` and `Resolve` classes, and `carrier` added by
//! ADR-0873 decision 2 to split the declared-statistics drop tally across
//! its four carriers). The eighteen keys come from twenty-one `Label`
//! variants, because three pairs share a key: `RejectReason` and
//! `ScrubReason` both render `reason`, `Level` (log/tracing severity) and
//! `ScrubLevel` (issue #1686, which part of the commit lineage --
//! `l0`/`l1`/`rewrite` -- a scrub target came from) both render `level`, and
//! `MergeMemoryKind` and `DeletedObjectKind` both render `kind`.
//! Every variant's payload is a closed enum
//! or [`TenantHash`]'s fixed-width hash, so there is no `String` or `&str`
//! anywhere on this path an unlisted label could travel through, and adding a
//! variant is a compile error everywhere this module matches on `Label`
//! exhaustively. A per-shard family is permitted when it carries no tenant
//! label and no operation label (ADR-1692 decision 1, narrowing ADR-0044
//! rejected alternative 6): `shard` is never combined with `tenant_hash` on
//! any sample, whatever `--metrics-tenant-labels` is set to, and the bare
//! `Label::Shard(u32)` payload is bounded by `MAX_SHARD_COUNT` so its own
//! cardinality cannot grow unbounded. Query text, metric names, label values
//! beyond the closed sets above, stream ids, trace ids, and object keys are
//! never labels.
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
use ravel_maintain::ScrubLevel;
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
/// floor. Six of the seven are here. Four come from
/// `AdmissionController::usage_snapshot` (`ravel_ingest::TenantUsage`), which
/// covers the byte-rate and active-cap layers plus the receiver-clock floor.
/// The other two, `skew` and `structural`, come from
/// [`crate::normalize_reject_metrics::NormalizeRejectMetrics`]: the
/// normalization layer keeps no row in that snapshot, so the ingest surfaces
/// count its decisions where they observe them.
///
/// The seventh, `body_size`, is enforced at the transport and still keeps no
/// per-tenant counter, so a variant for it would render samples no data source
/// can fill. It joins this enum when its counter does, additively, the same
/// way a new `Signal` variant joins `signal_name`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    ByteRate,
    SeriesRate,
    SeriesCap,
    /// The receiver's admission clock was implausible (below the 2020 floor or
    /// non-representable), so the whole request was rejected 503 / `UNAVAILABLE`
    /// (ADR-0051 amendment). The fault is the replica's, not the data's.
    Clock,
    /// A point, record, or span whose event timestamp fell outside the
    /// admissible window (too far in the future, or older than the maximum
    /// ingest lag). Counted per rejected datum, matching the count the same
    /// request reports back to the sender through OTLP partial success.
    Skew,
    /// A point, record, or span rejected by a structural bound in
    /// normalization: an unsupported metric type or aggregation temporality, a
    /// name or attribute over its limit, a malformed identifier, an
    /// inconsistent histogram. Counted per rejected datum, like `skew`.
    Structural,
}

impl RejectReason {
    /// Every reason with a counter, so the rejected family renders all six
    /// series per (tenant, signal) even when some are zero (the same
    /// zero-is-not-absence discipline the other families keep).
    const ALL: [RejectReason; 6] = [
        RejectReason::ByteRate,
        RejectReason::SeriesRate,
        RejectReason::SeriesCap,
        RejectReason::Clock,
        RejectReason::Skew,
        RejectReason::Structural,
    ];

    fn name(self) -> &'static str {
        match self {
            RejectReason::ByteRate => "byte_rate",
            RejectReason::SeriesRate => "series_rate",
            RejectReason::SeriesCap => "series_cap",
            RejectReason::Clock => "clock",
            RejectReason::Skew => "skew",
            RejectReason::Structural => "structural",
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
    /// One shard index of the per-shard ingest-skew family (ADR-1692
    /// decision 2). The payload is a bare `u32`, not a closed enum, but stays
    /// within the closed-payload rule because [`Label::shard`] is the only
    /// constructor and refuses any index at or above
    /// [`ravel_catalog::MAX_SHARD_COUNT`], so the rendered cardinality is
    /// bounded by the same cap the accumulator is preallocated to
    /// (crates/ravel-ingest/src/metrics.rs). Never combined with
    /// [`Label::TenantHash`] on any sample (ADR-1692 decision 1).
    Shard(u32),
    /// Which commit-lineage part a scrub target came from (issue #1686):
    /// `l0`, `l1`, or `rewrite`. Shares the `level` key with [`Label::Level`]
    /// (log/tracing severity), the same shared-key discipline
    /// `RejectReason`/`ScrubReason` already use for `reason`.
    ScrubLevel(ScrubLevel),
    Cache(CacheFamily),
    CacheTier(CacheTier),
    MergeMemoryKind(MergeMemoryKind),
    DeletedObjectKind(DeletedObjectKind),
    /// How one alert evaluation tick ended (issue #532). A closed enum owned by
    /// [`crate::alerting`], since the outcomes are the alerting loop's own, not
    /// a dimension this renderer invents.
    AlertOutcome(crate::alerting::AlertTickOutcome),
    /// The allocator this process runs under (#1170): `"jemalloc"` on every
    /// target this repo builds, or whatever [`crate::mem_stats::read`] names
    /// otherwise. A bare `&'static str` rather than a closed enum because the
    /// value is compile-time-fixed per target (never derived from request or
    /// tenant input), so there is no cardinality this label could blow up.
    Allocator(&'static str),
    AllocatorStat(AllocatorStat),
    /// Which side of the ADR-1170 process memory budget a
    /// `ravel_memory_reserved_bytes` sample is. `Fetch` always renders `0`:
    /// decision 2 (fetch-layer reservation against this same budget) has not
    /// landed upstream, so nothing yet charges the budget on the fetcher's
    /// behalf. This is an honest gap, not a bug -- the gauge exists now so a
    /// dashboard need not change shape once decision 2 lands.
    ///
    /// The split is not yet a real split, and landing decision 2 is more than
    /// flipping the hardcoded `Fetch` constant to a reader. `Sql` renders
    /// `MemoryBudget::reserved()`, the WHOLE process budget's reserved total,
    /// which is only equal to SQL's share because SQL is the sole reserver
    /// today. Wire a fetcher to the same instance and `component="sql"`
    /// silently becomes the process total while `component="fetch"` reports
    /// its own share, so the two double-count and a dashboard summing them
    /// reads high. Decision 2 has to give the budget per-component
    /// accounting (or give each component its own counter) before either
    /// sample can be read as a share.
    MemoryComponent(MemoryComponent),
    /// Which fragment admission class a `ravel_distrib_fragment_*`
    /// sample belongs to (issue #1722): `Pinned`
    /// (intra-cluster fan-out) or `Resolve` (cross-cluster federation). A
    /// closed enum owned by [`crate::distrib`], since the classes are the
    /// admission layer's own, not a dimension this renderer invents.
    AdmissionClass(crate::distrib::AdmissionClass),
    /// Which ADR-0873 declared-statistics carrier a
    /// `ravel_declared_stats_drops_observed_total` sample counts drops under.
    /// A closed enum owned by [`ravel_commit::declared_stats`], which is also
    /// where the dashed label spelling lives, since "which writer do I look
    /// at" is the question a nonzero drop tally asks and the answer has to
    /// name the carrier the ADR names.
    StatCarrier(ravel_commit::declared_stats::StatCarrier),
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

/// Which `SweepReport` field a `ravel_maintain_objects_deleted_total` sample
/// counts (issue #1729): the four fields that represent an actual physical
/// object delete, never a move to quarantine
/// (`orphans_deleted`/`orphans_quarantined`, already covered by the
/// `signal`-labeled `ravel_maintain_orphans_quarantined_total`) or a
/// withheld/refused candidate. Named for the report field each counts,
/// verbatim, since a downstream rule-file task keys alerts on these exact
/// `kind=` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletedObjectKind {
    QuarantineReaped,
    SupersededRecordsDeleted,
    SupersededDataDeleted,
    UnreferencedPartsDeleted,
}

impl DeletedObjectKind {
    fn name(self) -> &'static str {
        match self {
            DeletedObjectKind::QuarantineReaped => "quarantine_reaped",
            DeletedObjectKind::SupersededRecordsDeleted => "superseded_records_deleted",
            DeletedObjectKind::SupersededDataDeleted => "superseded_data_deleted",
            DeletedObjectKind::UnreferencedPartsDeleted => "unreferenced_parts_deleted",
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

/// Which ADR-0046 cache tier a `ravel_cache_*` sample belongs to (#97), the
/// second, orthogonal `tier=` label alongside `cache=`. Both caches (`fetch`,
/// `catalog`) may carry a RAM tier and, when `--cache-dir` is set, a local-disk
/// tier; this label tells the two tiers apart, the same one-name-split-by-a-
/// closed-dimension discipline [`MergeMemoryKind`] uses. A process with no disk
/// tier renders no `tier=` label at all (today's exact output); only when a
/// disk tier exists for a family does that family's RAM sample gain `tier=ram`
/// and its disk sample carry `tier=disk`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTier {
    Ram,
    Disk,
}

impl CacheTier {
    fn name(self) -> &'static str {
        match self {
            CacheTier::Ram => "ram",
            CacheTier::Disk => "disk",
        }
    }
}

/// Which jemalloc-reported figure a `ravel_process_allocator_bytes` sample is
/// (#1170): `allocated` is bytes the application requested, `active` adds
/// page-rounding and thread-cache slop, `resident` additionally counts pages
/// jemalloc has not yet returned to the OS. Three separate series, not one
/// number, because a single process-RSS figure cannot say which of these --
/// or which subsystem's cache -- grew, the exact ambiguity that forced two
/// published ClickBench memory claims to be retracted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocatorStat {
    Allocated,
    Active,
    Resident,
}

impl AllocatorStat {
    fn name(self) -> &'static str {
        match self {
            AllocatorStat::Allocated => "allocated",
            AllocatorStat::Active => "active",
            AllocatorStat::Resident => "resident",
        }
    }
}

/// Which side of the ADR-1170 process memory budget reserved a share of it:
/// `Sql` is the `SqlExecutor`'s per-tenant accountants
/// (`ravel_memory::TenantMemoryAccountant`), all sharing the one process
/// `MemoryBudget`; `Fetch` is the fetch layer's own reservation against that
/// same budget, decision 2, not yet landed (see [`Label::MemoryComponent`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryComponent {
    Sql,
    Fetch,
}

impl MemoryComponent {
    fn name(self) -> &'static str {
        match self {
            MemoryComponent::Sql => "sql",
            MemoryComponent::Fetch => "fetch",
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
            Label::Shard(_) => "shard",
            Label::ScrubLevel(_) => "level",
            Label::Cache(_) => "cache",
            Label::CacheTier(_) => "tier",
            Label::MergeMemoryKind(_) => "kind",
            Label::DeletedObjectKind(_) => "kind",
            Label::AlertOutcome(_) => "outcome",
            Label::Allocator(_) => "allocator",
            Label::AllocatorStat(_) => "stat",
            Label::MemoryComponent(_) => "component",
            Label::AdmissionClass(_) => "class",
            Label::StatCarrier(_) => "carrier",
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
            Label::Shard(index) => index.to_string(),
            Label::ScrubLevel(level) => level.as_str().to_string(),
            Label::Cache(family) => family.name().to_string(),
            Label::CacheTier(tier) => tier.name().to_string(),
            Label::MergeMemoryKind(kind) => kind.name().to_string(),
            Label::DeletedObjectKind(kind) => kind.name().to_string(),
            Label::AlertOutcome(outcome) => alert_outcome_name(*outcome).to_string(),
            Label::Allocator(name) => name.to_string(),
            Label::AllocatorStat(stat) => stat.name().to_string(),
            Label::MemoryComponent(component) => component.name().to_string(),
            Label::AdmissionClass(class) => admission_class_name(*class).to_string(),
            Label::StatCarrier(carrier) => carrier.label().to_string(),
        }
    }

    /// Bounded constructor for [`Label::Shard`] (ADR-1692 decision 2): refuses
    /// any index at or above [`ravel_catalog::MAX_SHARD_COUNT`] rather than
    /// rendering it, so a caller cannot smuggle an unbounded shard index onto
    /// the scrape.
    pub fn shard(index: u32) -> Option<Label> {
        (index < ravel_catalog::MAX_SHARD_COUNT).then_some(Label::Shard(index))
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

/// Exhaustive: adding an [`crate::alerting::AlertTickOutcome`] variant breaks
/// this compile until it is handled here, so a new tick outcome cannot reach
/// `/metrics` without a spelling.
fn alert_outcome_name(outcome: crate::alerting::AlertTickOutcome) -> &'static str {
    use crate::alerting::AlertTickOutcome;
    match outcome {
        AlertTickOutcome::Evaluated => "evaluated",
        AlertTickOutcome::LeaseNotHeld => "lease_not_held",
        AlertTickOutcome::LeaseUnavailable => "lease_unavailable",
        AlertTickOutcome::HistoryUnavailable => "history_unavailable",
    }
}

/// Exhaustive: adding a [`crate::distrib::AdmissionClass`] variant breaks
/// this compile until it is handled here, so a new admission class cannot
/// reach `/metrics` without a spelling.
fn admission_class_name(class: crate::distrib::AdmissionClass) -> &'static str {
    use crate::distrib::AdmissionClass;
    match class {
        AdmissionClass::Pinned => "pinned",
        AdmissionClass::Resolve => "resolve",
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
        "ravel_store_attempts_total",
        "Billed HTTP requests, by operation (issue #928). >= \
         ravel_store_calls_total when every store the call counter sees shares \
         this metrics handle, which the server wires up: the base S3 store and \
         every per-tenant KMS-routed store record attempts into one block. \
         Retries are one reason it exceeds the call count and not the only one: \
         a whole-object read and a multipart write each issue several HTTP \
         requests per logical call, so the difference is not a retry count. Zero \
         for a non-HTTP backend.",
        "counter",
    );
    for op in StoreOp::ALL {
        write_sample(
            out,
            "ravel_store_attempts_total",
            &[Label::Mode(mode), Label::Op(op)],
            snapshot.op(op).attempts,
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
    /// Flushes abandoned by their flush-open deadline while queued for a
    /// `max_inflight_flushes` permit, before any store call (issue #1739).
    /// Distinct from `abandoned_retry_exhausted`, which is a store failure.
    pub abandoned_queue_deadline: u64,
    pub abandoned_input_rejected: u64,
    pub buffered_bytes_total: u64,
    pub buffered_items_total: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    pub collisions: Option<u64>,
    pub shard_deaths: u64,
    /// Shards condemned and no longer accepting writes (issue #1299). Carried
    /// unconditionally, not as an `Option`: all three pipelines condemn and all
    /// three feed readiness, so the family renders a sample for every signal.
    /// The threshold differs by signal, not the presence of the counter: the
    /// metrics router condemns on the death that exhausts a shard's respawn
    /// budget, the log and span routers on the first shard-actor death
    /// (issue #1691).
    pub shards_condemned: u64,
    /// Multi-shard Strict writes that committed on at least one shard and
    /// then failed on a sibling: partial multi-shard commits, reported to the
    /// client as a retryable error carrying the durable tokens.
    pub partial_writes: u64,
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
    /// Exemplar admission counters. `Some` only for the metrics pipeline:
    /// exemplars ride on metric points, so logs and spans render no sample
    /// for this family rather than a zero that would read as "the cap never
    /// engaged".
    pub exemplars: Option<ExemplarCounters>,
    /// Flushes routed on a last-known-good provisioning view inside the bounded
    /// grace window rather than failed closed (ADR-0052). Carried for every
    /// signal, the same as `stale_provisioning_flushes`: the metrics, logs, and
    /// spans ingest snapshots all expose it, and the two form the
    /// degraded-vs-failed pair for a stale provisioning view.
    pub grace_extended_stale_flushes: u64,
    /// The metrics-pipeline-only adaptive-delay age-trigger counter ADR-0067
    /// added. `Some` only for the metrics pipeline; the log and span ingest
    /// snapshots expose no such figure, so logs and spans render no sample
    /// for this family, the same structural-absence convention `exemplars`
    /// uses.
    pub adaptive_flushes: Option<AdaptiveFlushCounters>,
    /// Flush tasks spawned but not yet acked, summed across shards at
    /// snapshot time (ADR-0067 decision 2 pipelining). A gauge, carried for
    /// every signal: unlike `adaptive_flushes` this is not metrics-only, so
    /// it is a flat field rather than `Option`-gated, and a logs- or
    /// spans-only process still renders a real (possibly zero) sample.
    pub in_flight_flushes_total: u64,
    /// Total nanoseconds every flush on this pipeline has spent waiting for a
    /// `max_inflight_flushes` permit (issue #865), summed across shards.
    /// Carried for every signal for the same reason `in_flight_flushes_total`
    /// is: it stays at zero unless a shard is actually asked for a second
    /// concurrent flush.
    pub flush_permit_wait_ns_total: u64,
    /// Flush tasks spawned and not yet reaped, summed across shards at
    /// snapshot time: the queue depth `--max-queued-flushes` caps per shard
    /// (issue #1740). A gauge, carried for every signal because the cap runs
    /// in all three ingest pipelines. It can read above
    /// `shard_count * max-queued-flushes`: a buffer over its memory backstop
    /// spawns past the cap, which is what `flush_trigger_deferred_total`
    /// staying flat under a rising depth distinguishes.
    pub flushes_queued_total: u64,
    /// Size and age flush triggers refused because their shard was already at
    /// `--max-queued-flushes`, summed across shards (issue #1740). Cumulative,
    /// and carried for every signal for the same reason
    /// `flushes_queued_total` is. A refusal is a deferral: the buffer rides
    /// back untouched and the next tick re-fires, so a rise means flush
    /// latency slipped past `--max-flush-delay`, not that anything was shed.
    pub flush_trigger_deferred_total: u64,
    /// Tenants a `DrainIntent::Teardown` flush left with buffered rows still
    /// unflushed, summed across shards (issue #1742). Those rows were already
    /// acknowledged in buffered mode (docs/consistency-model.md), so a
    /// nonzero count is data loss, not backpressure. Carried for every
    /// signal for the same reason `flushes_queued_total` is: all three
    /// pipelines run the same teardown path.
    pub flush_all_residue_tenants: u64,
    /// Per-shard ingest-skew figures (issue #865, ADR-1692), one entry per
    /// shard with recorded activity; an idle shard is simply absent here and
    /// the renderer fills it with zeros. Empty for a pipeline whose router is
    /// not configured on this process, the same structural-absence
    /// convention `postings` uses.
    pub shard_skew: Vec<(u32, ravel_ingest::ShardSkewStats)>,
    /// This pipeline's router's current active shard count (ADR-1692
    /// decision 4), the upper bound the renderer zero-fills up to. A shard
    /// index in `shard_skew` at or above this count is a retiring
    /// generation (ADR-0052) still carrying recorded activity, and renders
    /// too.
    pub active_shard_count: u32,
}

/// Exemplar admission counters, mirroring
/// [`ravel_ingest::IngestMetricsSnapshot`]'s two `exemplars_*` fields. Grouped
/// in one struct for the same reason [`MetadataSinkCounters`] is: they are
/// always present or always absent together, which `Option<Self>` says once
/// instead of twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExemplarCounters {
    pub written_total: u64,
    pub dropped_total: u64,
}

/// The metrics-pipeline-only flush figure from ADR-0067, mirroring
/// [`ravel_ingest::IngestMetricsSnapshot`]'s `flushes_by_age_adaptive`. A
/// single-field struct rather than a flat field on
/// [`IngestPipelineSnapshot`] because it exists only on the metrics
/// pipeline, which `Option<Self>` says directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AdaptiveFlushCounters {
    /// Monotonic total: flushes opened because the tenant buffer aged past a
    /// per-(shard, tenant) threshold computed within the adaptive-delay
    /// corridor rather than the fixed `max_flush_delay` (ADR-0067 decision 3).
    pub flushes_by_age_adaptive: u64,
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
            abandoned_queue_deadline: snapshot.abandoned_queue_deadline,
            abandoned_input_rejected: snapshot.abandoned_input_rejected,
            buffered_bytes_total: snapshot.buffered_bytes_total,
            buffered_items_total: snapshot.buffered_points_total,
            acks_ok: snapshot.acks_ok,
            acks_err: snapshot.acks_err,
            collisions: Some(snapshot.series_id_collisions),
            shard_deaths: snapshot.shard_deaths,
            shards_condemned: snapshot.shards_condemned,
            partial_writes: snapshot.partial_writes,
            stale_provisioning_flushes: snapshot.stale_provisioning_flushes,
            postings: None,
            metadata_sink: Some(MetadataSinkCounters {
                flush_gets_total: snapshot.metadata_flush_gets_total,
                flush_puts_total: snapshot.metadata_flush_puts_total,
                flush_dropped_total: snapshot.metadata_flush_dropped_total,
                entries_dropped_total: snapshot.metadata_entries_dropped_total,
            }),
            exemplars: Some(ExemplarCounters {
                written_total: snapshot.exemplars_written_total,
                dropped_total: snapshot.exemplars_dropped_total,
            }),
            grace_extended_stale_flushes: snapshot.grace_extended_stale_flushes,
            adaptive_flushes: Some(AdaptiveFlushCounters {
                flushes_by_age_adaptive: snapshot.flushes_by_age_adaptive,
            }),
            in_flight_flushes_total: snapshot.in_flight_flushes_total,
            flush_permit_wait_ns_total: snapshot.flush_permit_wait_ns_total,
            flushes_queued_total: snapshot.flushes_queued_total,
            flush_trigger_deferred_total: snapshot.flush_trigger_deferred_total,
            flush_all_residue_tenants: snapshot.flush_all_residue_tenants,
            shard_skew: Vec::new(),
            active_shard_count: 0,
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
            abandoned_queue_deadline: snapshot.abandoned_queue_deadline,
            abandoned_input_rejected: snapshot.abandoned_input_rejected,
            buffered_bytes_total: snapshot.buffered_bytes_total,
            buffered_items_total: snapshot.buffered_records_total,
            acks_ok: snapshot.acks_ok,
            acks_err: snapshot.acks_err,
            collisions: Some(snapshot.stream_id_collisions),
            shard_deaths: snapshot.shard_deaths,
            shards_condemned: snapshot.shards_condemned,
            partial_writes: snapshot.partial_writes,
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
            exemplars: None,
            grace_extended_stale_flushes: snapshot.grace_extended_stale_flushes,
            adaptive_flushes: None,
            in_flight_flushes_total: snapshot.in_flight_flushes_total,
            flush_permit_wait_ns_total: snapshot.flush_permit_wait_ns_total,
            flushes_queued_total: snapshot.flushes_queued_total,
            flush_trigger_deferred_total: snapshot.flush_trigger_deferred_total,
            flush_all_residue_tenants: snapshot.flush_all_residue_tenants,
            shard_skew: Vec::new(),
            active_shard_count: 0,
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
            abandoned_queue_deadline: snapshot.abandoned_queue_deadline,
            abandoned_input_rejected: snapshot.abandoned_input_rejected,
            buffered_bytes_total: snapshot.buffered_bytes_total,
            buffered_items_total: snapshot.buffered_spans_total,
            acks_ok: snapshot.acks_ok,
            acks_err: snapshot.acks_err,
            collisions: None,
            shard_deaths: snapshot.shard_deaths,
            shards_condemned: snapshot.shards_condemned,
            partial_writes: snapshot.partial_writes,
            stale_provisioning_flushes: snapshot.stale_provisioning_flushes,
            postings: None,
            metadata_sink: None,
            exemplars: None,
            grace_extended_stale_flushes: snapshot.grace_extended_stale_flushes,
            adaptive_flushes: None,
            in_flight_flushes_total: snapshot.in_flight_flushes_total,
            flush_permit_wait_ns_total: snapshot.flush_permit_wait_ns_total,
            flushes_queued_total: snapshot.flushes_queued_total,
            flush_trigger_deferred_total: snapshot.flush_trigger_deferred_total,
            flush_all_residue_tenants: snapshot.flush_all_residue_tenants,
            shard_skew: Vec::new(),
            active_shard_count: 0,
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
        "Flushes abandoned by retry-budget or lifetime exhaustion during their \
         own store calls, by signal.",
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
        "ravel_ingest_abandoned_queue_deadline_total",
        "Flushes abandoned by their flush-open deadline while queued for a \
         permit, before any store call, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_abandoned_queue_deadline_total",
            &labels(mode, pipeline.signal),
            pipeline.abandoned_queue_deadline,
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
        "Shard-actor deaths observed by the router (issue #1299), by signal. The metrics router \
         respawns, so its figure counts each respawned incarnation and can exceed the shard \
         count; the log and span routers never respawn, so their deaths are counted once per \
         shard per live generation and each one also condemns that shard (issue #1691).",
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

    // Rendered for every signal: all three pipelines condemn. The condemnation
    // threshold differs by signal: the metrics router respawns a shard up to
    // its budget and condemns only on the death that exhausts it, while the log
    // and span routers do not respawn and condemn on the first shard death.
    write_header(
        out,
        "ravel_ingest_shards_condemned_total",
        "Shards condemned and no longer accepting writes (issue #1299), counted at most once \
         per shard per live generation (bounded by live_generations * shard_count under \
         resharding, not shard_count); the metrics router condemns after exhausting a shard's \
         respawn budget, logs and spans on the first shard death; any nonzero value makes the \
         process report /readyz 503, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_shards_condemned_total",
            &labels(mode, pipeline.signal),
            pipeline.shards_condemned,
        );
    }

    write_header(
        out,
        "ravel_ingest_partial_writes_total",
        "Multi-shard Strict writes that committed on some shards and failed on a sibling, by signal.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_partial_writes_total",
            &labels(mode, pipeline.signal),
            pipeline.partial_writes,
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

    write_header(
        out,
        "ravel_ingest_grace_extended_stale_flushes_total",
        "Flushes routed on a last-known-good provisioning view inside the bounded grace window \
         because the provisioning re-read could not complete but the cached view's validity \
         horizon had not been crossed (ADR-0052), by signal. Degraded rather than failed, and \
         distinct from ravel_ingest_stale_provisioning_flushes_total, which counts a flush that \
         failed closed: a sustained rise means the store is slow and this router is \
         degraded-but-available.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_grace_extended_stale_flushes_total",
            &labels(mode, pipeline.signal),
            pipeline.grace_extended_stale_flushes,
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

    // Exemplars ride on metric points, so only the metrics pipeline sets
    // `exemplars` and the family is empty in a logs- or spans-only process.
    let with_exemplars: Vec<_> = pipelines
        .iter()
        .filter_map(|pipeline| pipeline.exemplars.map(|counters| (pipeline, counters)))
        .collect();
    if !with_exemplars.is_empty() {
        write_header(
            out,
            "ravel_ingest_exemplars_written_total",
            "Exemplars stored on flushed objects, by signal.",
            "counter",
        );
        for (pipeline, counters) in &with_exemplars {
            write_sample(
                out,
                "ravel_ingest_exemplars_written_total",
                &labels(mode, pipeline.signal),
                counters.written_total,
            );
        }

        write_header(
            out,
            "ravel_ingest_exemplars_dropped_total",
            "Exemplars discarded by the per-series admission cap, by signal. A rising figure \
             says the cap is engaging; the points themselves are still ingested.",
            "counter",
        );
        for (pipeline, counters) in &with_exemplars {
            write_sample(
                out,
                "ravel_ingest_exemplars_dropped_total",
                &labels(mode, pipeline.signal),
                counters.dropped_total,
            );
        }
    }

    // The adaptive-delay age trigger is metrics-pipeline-only (ADR-0067), so
    // `adaptive_flushes` is `Some` only there (the same structural-absence
    // convention as `exemplars` above), and the family is empty in a logs- or
    // spans-only process.
    let with_adaptive: Vec<_> = pipelines
        .iter()
        .filter_map(|pipeline| {
            pipeline
                .adaptive_flushes
                .map(|counters| (pipeline, counters))
        })
        .collect();
    if !with_adaptive.is_empty() {
        write_header(
            out,
            "ravel_ingest_flushes_by_age_adaptive_total",
            "Flushes opened because the tenant buffer aged past a per-(shard, tenant) threshold \
             computed within the adaptive-delay corridor rather than the fixed max_flush_delay \
             (ADR-0067 decision 3), by signal. Zero unless adaptive delay is enabled; a rise \
             means the corridor, not the fixed delay, is driving age flushes.",
            "counter",
        );
        for (pipeline, counters) in &with_adaptive {
            write_sample(
                out,
                "ravel_ingest_flushes_by_age_adaptive_total",
                &labels(mode, pipeline.signal),
                counters.flushes_by_age_adaptive,
            );
        }
    }

    // Unlike the adaptive-delay age trigger above, the in-flight-flush gauge
    // (ADR-0067 decision 2 pipelining) is not metrics-only: every pipeline's
    // permit-wait acquire runs off-actor (ADR-1642), so it is a flat field on
    // every signal's snapshot rather than `Option`-gated, and this family
    // renders a real sample for a logs- or spans-only process too.
    write_header(
        out,
        "ravel_ingest_in_flight_flushes",
        "Flush tasks spawned but not yet acked, summed across shards at scrape time \
         (ADR-0067 decision 2 pipelining), by signal. A gauge: it rises as flushes start and \
         falls as they finish, so a sustained high value means flushes are not keeping up \
         with the load.",
        "gauge",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_in_flight_flushes",
            &labels(mode, pipeline.signal),
            pipeline.in_flight_flushes_total,
        );
    }

    // Flush permit wait (issue #865): the same off-actor acquire the
    // in-flight gauge above measures, timed on the injected clock. Carried
    // for every signal for the same reason.
    write_header(
        out,
        "ravel_ingest_flush_permit_wait_seconds_total",
        "Total seconds every flush on this pipeline has spent waiting for a \
         max_inflight_flushes permit (issue #865), summed across shards, by signal. Zero unless \
         a shard is actually asked for a second concurrent flush; a rise means \
         max_inflight_flushes is the binding window.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample_f64(
            out,
            "ravel_ingest_flush_permit_wait_seconds_total",
            &labels(mode, pipeline.signal),
            pipeline.flush_permit_wait_ns_total as f64 / 1_000_000_000.0,
        );
    }

    // The queued-flush cap (issue #1740) and the refusals it produces. Both run
    // in all three pipelines, so both are flat fields rather than
    // `Option`-gated, the same as the two flush families above. The pair is
    // read together: `--max-queued-flushes`' help sends an operator to the
    // deferred counter, and the depth gauge is what says whether a flat
    // counter means a healthy queue or the backstop exemption spawning past
    // the cap.
    write_header(
        out,
        "ravel_ingest_queued_flushes",
        "Flush tasks spawned and not yet reaped, summed across shards at scrape time, by \
         signal: the per-shard queue --max-queued-flushes caps (issue #1740). A gauge. It can \
         exceed shard_count times the cap, because a tenant buffer over its memory backstop \
         spawns whatever the queue depth; that case is this gauge rising while \
         ravel_ingest_flush_trigger_deferred_total stays flat.",
        "gauge",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_queued_flushes",
            &labels(mode, pipeline.signal),
            pipeline.flushes_queued_total,
        );
    }

    write_header(
        out,
        "ravel_ingest_flush_trigger_deferred_total",
        "Size and age flush triggers refused because their shard was already holding \
         --max-queued-flushes spawned flush tasks (issue #1740), summed across shards, by \
         signal. A refusal is a deferral, not a shed: the buffer rides back untouched and the \
         next tick re-fires once a flush has been reaped, so a rise means flush latency slipped \
         past --max-flush-delay and nothing was dropped.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_flush_trigger_deferred_total",
            &labels(mode, pipeline.signal),
            pipeline.flush_trigger_deferred_total,
        );
    }

    write_header(
        out,
        "ravel_ingest_flush_all_residue_tenants_total",
        "Tenants a teardown drain (DrainIntent::Teardown) left with buffered rows still \
         unflushed, summed across shards, by signal (issue #1742). In buffered mode those rows \
         were already acknowledged (docs/consistency-model.md), so a nonzero count is data \
         loss, not backpressure. It counts TENANTS, and a residual buffer can also hold \
         strict-mode rows whose waiters were answered 503 and will retry, so the count is an \
         upper bound on tenants that lost acknowledged data. The same drain also logs an \
         ERROR per residual shard, \
         carrying that shard's tenant count. Cumulative across the process lifetime, so a \
         rise always means a new teardown lost rows, never that an old loss is still \
         outstanding.",
        "counter",
    );
    for pipeline in pipelines {
        write_sample(
            out,
            "ravel_ingest_flush_all_residue_tenants_total",
            &labels(mode, pipeline.signal),
            pipeline.flush_all_residue_tenants,
        );
    }
}

/// Per-shard skew figures for one pipeline (ADR-1692 decision 4): every index
/// below the router's active shard count, in order, plus any index at or
/// above it that still carries recorded activity (a retiring generation,
/// ADR-0052). A shard absent from `shard_skew` renders the zero
/// `ShardSkewStats::default()` rather than being omitted: an idle shard is
/// the finding this family exists to show.
fn shard_stats(pipeline: &IngestPipelineSnapshot) -> Vec<(u32, ravel_ingest::ShardSkewStats)> {
    let by_shard: HashMap<u32, ravel_ingest::ShardSkewStats> =
        pipeline.shard_skew.iter().copied().collect();
    let mut indices: Vec<u32> = (0..pipeline.active_shard_count).collect();
    for (shard, _) in &pipeline.shard_skew {
        if *shard >= pipeline.active_shard_count {
            indices.push(*shard);
        }
    }
    indices.sort_unstable();
    indices
        .into_iter()
        .map(|shard| (shard, by_shard.get(&shard).copied().unwrap_or_default()))
        .collect()
}

/// The per-shard ingest-skew family (ADR-1692): six samples per configured
/// shard, labelled `mode`, `signal`, `shard`, fed from each router's
/// `shard_skew_by_shard` and active shard count. No tenant and no operation
/// label ever joins `shard` on these samples (decision 1). An index at or
/// above `MAX_SHARD_COUNT` is refused by `Label::shard` rather than rendered;
/// `shard_stats` never produces one in practice, since the accumulator itself
/// is capped there, but the renderer stays defensive rather than trusting it.
fn render_ingest_shard_family(out: &mut String, mode: Mode, pipelines: &[IngestPipelineSnapshot]) {
    fn labels(mode: Mode, signal: Signal, shard: Label) -> [Label; 3] {
        [Label::Mode(mode), Label::Signal(signal), shard]
    }

    let per_pipeline: Vec<(Signal, Vec<(u32, ravel_ingest::ShardSkewStats)>)> = pipelines
        .iter()
        .map(|pipeline| (pipeline.signal, shard_stats(pipeline)))
        .collect();

    write_header(
        out,
        "ravel_ingest_shard_messages_enqueued_total",
        "Write messages the router sent into this shard's channel, by signal and shard \
         (issue #865, ADR-1692). Every configured shard renders, idle ones at zero.",
        "counter",
    );
    for (signal, stats) in &per_pipeline {
        for (shard, s) in stats {
            let Some(label) = Label::shard(*shard) else {
                continue;
            };
            write_sample(
                out,
                "ravel_ingest_shard_messages_enqueued_total",
                &labels(mode, *signal, label),
                s.messages_enqueued,
            );
        }
    }

    write_header(
        out,
        "ravel_ingest_shard_messages_processed_total",
        "Write messages this shard's actor pulled off its channel and handled, by signal \
         and shard (issue #865, ADR-1692). messages_enqueued minus this is the channel depth.",
        "counter",
    );
    for (signal, stats) in &per_pipeline {
        for (shard, s) in stats {
            let Some(label) = Label::shard(*shard) else {
                continue;
            };
            write_sample(
                out,
                "ravel_ingest_shard_messages_processed_total",
                &labels(mode, *signal, label),
                s.messages_processed,
            );
        }
    }

    write_header(
        out,
        "ravel_ingest_shard_queue_depth",
        "This shard's channel depth: messages_enqueued minus messages_processed at scrape \
         time, by signal and shard (issue #865, ADR-1692). A gauge.",
        "gauge",
    );
    for (signal, stats) in &per_pipeline {
        for (shard, s) in stats {
            let Some(label) = Label::shard(*shard) else {
                continue;
            };
            write_sample(
                out,
                "ravel_ingest_shard_queue_depth",
                &labels(mode, *signal, label),
                s.queue_depth,
            );
        }
    }

    write_header(
        out,
        "ravel_ingest_shard_on_actor_seconds_total",
        "Total seconds this shard's actor has spent handling write messages, excluding \
         both the flush-permit wait and the flush itself, by signal and shard (issue #865, \
         ADR-1692).",
        "counter",
    );
    for (signal, stats) in &per_pipeline {
        for (shard, s) in stats {
            let Some(label) = Label::shard(*shard) else {
                continue;
            };
            write_sample_f64(
                out,
                "ravel_ingest_shard_on_actor_seconds_total",
                &labels(mode, *signal, label),
                s.on_actor_ns as f64 / 1_000_000_000.0,
            );
        }
    }

    write_header(
        out,
        "ravel_ingest_shard_flush_permit_wait_seconds_total",
        "Total seconds flush tasks on this shard have spent waiting for a \
         max_inflight_flushes permit, by signal and shard (issue #865, ADR-1692). A sum \
         over concurrently waiting tasks, so it can exceed wall time (ADR-1642).",
        "counter",
    );
    for (signal, stats) in &per_pipeline {
        for (shard, s) in stats {
            let Some(label) = Label::shard(*shard) else {
                continue;
            };
            write_sample_f64(
                out,
                "ravel_ingest_shard_flush_permit_wait_seconds_total",
                &labels(mode, *signal, label),
                s.flush_permit_wait_ns as f64 / 1_000_000_000.0,
            );
        }
    }

    write_header(
        out,
        "ravel_ingest_shard_off_actor_seconds_total",
        "Total seconds this shard's flush tasks have spent flushing off the actor, by \
         signal and shard (issue #865, ADR-1692).",
        "counter",
    );
    for (signal, stats) in &per_pipeline {
        for (shard, s) in stats {
            let Some(label) = Label::shard(*shard) else {
                continue;
            };
            write_sample_f64(
                out,
                "ravel_ingest_shard_off_actor_seconds_total",
                &labels(mode, *signal, label),
                s.off_actor_ns as f64 / 1_000_000_000.0,
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

/// One signal's fold-liveness figures. The fold runs as one independent task
/// per [`crate::fold::FOLD_SIGNALS`] entry, so these are per signal rather
/// than per process: a process-global set reads as healthy whenever any one
/// of those tasks is still running, which is the blind spot a dead signal
/// loop would otherwise sit in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogFoldCounters {
    /// The signal these figures belong to, carried in the entry rather than
    /// implied by its position, so the renderer cannot mislabel a series by
    /// iterating out of step with `FOLD_SIGNALS`.
    pub signal: Signal,
    /// `Catalog::fold` calls for this signal that returned `Ok`, no-op cycles
    /// included.
    pub cycles: u64,
    /// `Catalog::fold` calls for this signal that returned `Err`.
    pub failures: u64,
    /// `now_ns` of the most recent successful fold of this signal, or `0` when
    /// none has succeeded in this process. Carried in nanoseconds (the
    /// catalog's own unit, and an exact integer) and divided down to seconds
    /// only at the render below, so this struct stays `Eq`-comparable in
    /// tests.
    pub last_success_unix_ns: i64,
}

impl CatalogFoldCounters {
    /// One zeroed entry per folded signal: the `Default` shape of the
    /// per-signal array, spelled here because [`Signal`] has no `Default` of
    /// its own and the entries must still name their signals.
    fn zeroed_per_signal() -> [CatalogFoldCounters; crate::fold::FOLD_SIGNALS.len()] {
        crate::fold::FOLD_SIGNALS.map(|signal| CatalogFoldCounters {
            signal,
            cycles: 0,
            failures: 0,
            last_success_unix_ns: 0,
        })
    }
}

/// The catalog anomaly and hard-failure counters
/// (`crates/ravel-catalog/src/catalog.rs`), decoupled from `Catalog` itself
/// so the renderer is testable with a plain struct literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogCountersSnapshot {
    pub interlock_violations: u64,
    pub compaction_input_set_conflicts: u64,
    /// ADR-0050 §2 hard isolation-breach failures: a HEAD/postings
    /// tenant_hash mismatch or an out-of-prefix listing result. Unlike the
    /// two counters above, each of these also failed its query.
    pub isolation_breaches: u64,
    /// Fold liveness, one entry per signal the fold covers.
    pub fold: [CatalogFoldCounters; crate::fold::FOLD_SIGNALS.len()],
}

impl Default for CatalogCountersSnapshot {
    fn default() -> Self {
        CatalogCountersSnapshot {
            interlock_violations: 0,
            compaction_input_set_conflicts: 0,
            isolation_breaches: 0,
            fold: CatalogFoldCounters::zeroed_per_signal(),
        }
    }
}

impl CatalogCountersSnapshot {
    /// Read every counter off a live [`ravel_catalog::Catalog`]. The scrape
    /// handler calls this rather than listing the reads inline, so the
    /// catalog-to-exposition wiring is a function a test can call: a renderer
    /// driven only by struct literals proves the formatting and nothing about
    /// whether the numbers came from the catalog at all.
    pub fn from_catalog(catalog: &ravel_catalog::Catalog) -> Self {
        CatalogCountersSnapshot {
            interlock_violations: catalog.interlock_violations(),
            compaction_input_set_conflicts: catalog.compaction_input_set_conflicts(),
            isolation_breaches: catalog.isolation_breaches(),
            fold: crate::fold::FOLD_SIGNALS.map(|signal| CatalogFoldCounters {
                signal,
                cycles: catalog.fold_cycles(signal),
                failures: catalog.fold_failures(signal),
                last_success_unix_ns: catalog.fold_last_success_unix_ns(signal),
            }),
        }
    }
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

    // The three fold families carry a `signal` label because the fold is one
    // independent task per signal (`crate::fold::FOLD_SIGNALS`). Every folded
    // signal renders every cycle, whether or not its loop is still alive, so a
    // loop that has died leaves its own series standing and going stale
    // instead of vanishing into an aggregate its siblings keep fresh.
    fn labels(mode: Mode, signal: Signal) -> [Label; 2] {
        [Label::Mode(mode), Label::Signal(signal)]
    }

    write_header(
        out,
        "ravel_catalog_fold_cycles_total",
        "Catalog folds that completed successfully, by signal, including the no-op folds that are the healthy steady state.",
        "counter",
    );
    for fold in &snapshot.fold {
        write_sample(
            out,
            "ravel_catalog_fold_cycles_total",
            &labels(mode, fold.signal),
            fold.cycles,
        );
    }

    write_header(
        out,
        "ravel_catalog_fold_failures_total",
        "Catalog folds that failed, by signal. A fold that fails every cycle leaves the unsealed ingest span growing without bound.",
        "counter",
    );
    for fold in &snapshot.fold {
        write_sample(
            out,
            "ravel_catalog_fold_failures_total",
            &labels(mode, fold.signal),
            fold.failures,
        );
    }

    // The liveness gauge, and the only figure here that moves when the fold
    // STOPS rather than when it runs. `0` means no fold of that signal has
    // succeeded since this process started, which is why the alert rule in
    // docs/guides/observability.md carries a `for:` long enough to cover a
    // freshly started process's first fold interval.
    write_header(
        out,
        "ravel_catalog_fold_last_success_timestamp_seconds",
        "Unix time of the last successful catalog fold of this signal in this process, 0 if none has succeeded yet. Its age is the fold-liveness signal.",
        "gauge",
    );
    for fold in &snapshot.fold {
        write_sample_f64(
            out,
            "ravel_catalog_fold_last_success_timestamp_seconds",
            &labels(mode, fold.signal),
            fold.last_success_unix_ns as f64 / 1e9,
        );
    }
}

/// ADR-0873 declared-statistics observability: decision 2's per-carrier drop
/// tally, and the fold's stamp-coverage pair.
///
/// `drops` renders in every mode, one sample per carrier in
/// [`ravel_commit::declared_stats::StatCarrier::ALL`] whether or not that
/// carrier has moved, because the four carriers are read by four different
/// subsystems and no single mode covers them: ingest stamps commit records,
/// maintenance stamps compaction parts, ravel-sql reads `.cstat`, and the
/// fourth carrier, `snapshot-entry`, renders so the label set is visibly
/// closed even though nothing in the shipped tree increments it (see the
/// stamp-coverage section of docs/guides/observability.md). Gating this
/// family on folding would hide the compaction-part drops in the one mode
/// that compacts.
///
/// `coverage` is the fold's `(stamped_records, stamped_entries)` totals, and
/// is `None` when no fold can run in this process by either route -- neither
/// the background task nor `POST /api/v1/admin/fold` (see
/// [`MetricsState::can_fold`]): both fold families are then omitted
/// rather than rendered as zero, the same structural absence the ingest
/// families use. The absence is load-bearing for the alert rules in
/// docs/guides/observability.md. An old fold cannot emit a counter it does
/// not have, so the detectable signal is a divergence between the two
/// counters, or the fold-side family being absent while the ingest side
/// rises; a family always present at zero would make those two cases read
/// the same as a healthy idle fold. The gate asks whether this process can
/// fold at all, by the background task or the on-demand route, rather than
/// whether the mode permits one: a maintain process omits both families, and
/// `--mode all --disable-fold` still renders them, because the admin fold
/// route is mounted and a fold through it moves them. Those counters sit at
/// zero until someone calls that route, which is the honest reading of a
/// process that can fold and has not.
fn render_declared_stats_family(
    out: &mut String,
    mode: Mode,
    drops: &[(ravel_commit::declared_stats::StatCarrier, u64)],
    coverage: Option<(u64, u64)>,
) {
    write_header(
        out,
        "ravel_declared_stats_drops_observed_total",
        "Declared-column statistics entries a reader dropped as defective, by carrier. Counts observations, not distinct defects: one bad entry read by many queries counts many times.",
        "counter",
    );
    for (carrier, observed) in drops {
        write_sample(
            out,
            "ravel_declared_stats_drops_observed_total",
            &[Label::Mode(mode), Label::StatCarrier(*carrier)],
            *observed,
        );
    }

    let Some((stamped_records, stamped_entries)) = coverage else {
        return;
    };

    write_header(
        out,
        "ravel_catalog_fold_stamped_records_total",
        "Carriers of declared-column statistics that the fold read: L0 commit records and L1 compaction parts. The denominator of the fold's stamp coverage.",
        "counter",
    );
    write_sample(
        out,
        "ravel_catalog_fold_stamped_records_total",
        &[Label::Mode(mode)],
        stamped_records,
    );

    write_header(
        out,
        "ravel_catalog_fold_stamped_entries_total",
        "Snapshot entries the fold built carrying declared-column statistics, from either carrier. Below the records total means stamps are being read and not carried through.",
        "counter",
    );
    write_sample(
        out,
        "ravel_catalog_fold_stamped_entries_total",
        &[Label::Mode(mode)],
        stamped_entries,
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

/// The process-wide ingest buffer byte budget family (ADR-0069 decision 1,
/// amended): the current gauge of estimated buffered bytes plus in-flight
/// OTLP HTTP gzip and Remote Write snappy decode state, the configured
/// ceiling, and the cumulative shed counter. Mode-only labeled like `render_ingest_concurrency_family`
/// above: the budget is a single gauge shared across metrics/logs/traces
/// with no per-signal breakdown.
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
        "Estimated buffered ingest bytes currently held across all tenants and signals, plus in-flight OTLP HTTP gzip and Remote Write snappy decode state (the process-wide ingest byte budget gauge, ADR-0069).",
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

/// Clamps the exposed `ravel_memory_budget_bytes` reading to `u64::MAX` when
/// `is_fallback` is set (the budget was sized on an unmeasured host,
/// `config::PERF_SOURCE_FALLBACK`). On that path `raw_limit` is
/// `ravel_memory::MemoryBudget::limit()`'s actual value: `u64::MAX` minus
/// the two hard cache carves (`DEFAULT_CACHE_MAX_BYTES` each), not `u64::MAX`
/// itself, which would leave the "unlimited" doc claim on
/// `ravel_memory_budget_bytes` unmet and a `== u64::MAX` dashboard check
/// permanently unmatched.
fn exposed_memory_budget_limit(raw_limit: u64, is_fallback: bool) -> u64 {
    if is_fallback { u64::MAX } else { raw_limit }
}

/// The ADR-1170 decisions 3/4 process memory budget family: the derived
/// ceiling, the reserved share per component, and the tenant handoff overlap
/// the same one `ravel_memory::MemoryBudget` tracks. Unconditional, like
/// `render_ingest_buffer_budget_family` above: `MetricsState::process_memory_budget`
/// is always built (`crate::start`), regardless of the `sql` feature or mode,
/// so this family renders in every build even where nothing yet reserves
/// against the budget.
///
/// `ravel_memory_budget_bytes` is the POST-carve remainder, not the pre-carve
/// `memory_budget_bytes` the startup log names: `main.rs` sizes
/// `ServerConfig::process_memory_budget_bytes` from
/// `ResolvedPerformanceDefaults::memory_remainder_bytes`, and this gauge
/// renders that one instance's `limit()`. That is the right quantity for a
/// budget accountant gauge (it is what reservations are refused against), but
/// both names appear in the startup log with a multi-GB gap between them, so
/// the HELP string below says which one this is.
///
/// It is `u64::MAX` when the process was built with no derived budget
/// (matching `ravel_memory::MemoryBudget::unlimited`'s own convention), not
/// `0`: a `0` ceiling would misread as "everything refused."
/// [`exposed_memory_budget_limit`] clamps that path before it reaches this
/// family.
///
/// `ravel_memory_reserved_bytes{component="fetch"}` and
/// `ravel_memory_handoff_overlap_bytes` are both always `0` here.
/// `ravel-query`'s fetchers do reserve and mark handoffs, but against the
/// private `MemoryBudget::unlimited` each one carries by default:
/// `crate::query::build_sql_state` wires no fetcher to the process-wide
/// instance, so nothing this family reads ever sees a fetch reservation. See
/// [`Label::MemoryComponent`]'s doc comment.
fn render_memory_budget_family(out: &mut String, mode: Mode, budget: MemoryBudgetSnapshot) {
    write_header(
        out,
        "ravel_memory_budget_bytes",
        "Ceiling of the ADR-1170 shared SQL/fetch memory budget: the startup log's memory_remainder_bytes, which is memory_budget_bytes minus memory_hard_caps_bytes (the two resolved cache ceilings), NOT the pre-carve memory_budget_bytes that log line names; u64::MAX means unlimited.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_memory_budget_bytes",
        &[Label::Mode(mode)],
        budget.limit,
    );

    write_header(
        out,
        "ravel_memory_reserved_bytes",
        "Bytes currently reserved against the ADR-1170 process memory budget, by component. component=\"sql\" is the budget's whole reserved total, equal to SQL's share only because SQL is its sole reserver today; component=\"fetch\" reads 0 until decision 2 (fetch-layer reservation) lands upstream.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_memory_reserved_bytes",
        &[
            Label::Mode(mode),
            Label::MemoryComponent(MemoryComponent::Sql),
        ],
        budget.reserved,
    );
    write_sample(
        out,
        "ravel_memory_reserved_bytes",
        &[
            Label::Mode(mode),
            Label::MemoryComponent(MemoryComponent::Fetch),
        ],
        0,
    );

    write_header(
        out,
        "ravel_memory_handoff_overlap_bytes",
        "Bytes double-counted because a tenant's memory handed off between components overlaps in the ADR-1170 process budget's accounting window; inactive (always 0) until fetch handoff accounting lands.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_memory_handoff_overlap_bytes",
        &[Label::Mode(mode)],
        budget.handoff_overlap,
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

/// The `shard_count` provisioning family (ADR-0050 section 5, EC5; ADR-0082).
///
/// `ravel_provisioning_shard_count_mismatch_total` counts hard provisioning
/// failures: an unreadable record (corrupt or a future format version), a
/// decodable record whose generation history fails structural validation
/// (`CorruptGenerations`: a scalar/generation-0 mismatch or a nonzero first
/// activation hour), or pre-ADR data a lower `shard_count` would hide, caught
/// on a dynamic tenant's first touch or on the maintain per-tenant loop. A
/// nonzero value means at least one tenant failed a hard provisioning check:
/// either an existing record could not be validated, or an adoption was
/// refused before any record was written; the operations guide pages on any
/// increase.
///
/// `ravel_provisioning_shard_count_drift_total` counts the ADR-0082 case: a
/// decodable record with a structurally valid generation history whose
/// recorded generation-0 `shard_count` differs from this process's live
/// `--shards` default (a `CorruptGenerations` record fails hard above and is
/// never counted here). That drift is tolerated (routing uses the
/// record's own generation history), so it is an informational signal, not a
/// failure. Both are process-global atomic reads with no labels beyond mode.
fn render_provisioning_family(
    out: &mut String,
    mode: Mode,
    shard_count_mismatches: u64,
    shard_count_drifts: u64,
) {
    write_header(
        out,
        "ravel_provisioning_shard_count_mismatch_total",
        "Provisioning checks that failed hard: an unreadable record, a decodable record with a structurally invalid generation history, or pre-ADR data a lower shard_count would hide (ADR-0050 section 5). A recorded shard_count that merely differs from the live default is tolerated and counted by ravel_provisioning_shard_count_drift_total instead.",
        "counter",
    );
    write_sample(
        out,
        "ravel_provisioning_shard_count_mismatch_total",
        &[Label::Mode(mode)],
        shard_count_mismatches,
    );
    write_header(
        out,
        "ravel_provisioning_shard_count_drift_total",
        "Provisioning validations where a decodable record with a structurally valid generation history had a recorded shard_count that differed from the live --shards default; the drift is tolerated and routing uses the record's own generation history (ADR-0082).",
        "counter",
    );
    write_sample(
        out,
        "ravel_provisioning_shard_count_drift_total",
        &[Label::Mode(mode)],
        shard_count_drifts,
    );
}

/// Store-reachability probe family (ADR-0050 section 7, EC7; issue #1728):
/// the `ravel_store_reachable` gauge (1 = the background probe currently
/// reports the store reachable, 0 = unhealthy after `store_probe::K`
/// consecutive failures), the `ravel_store_probe_failures_total` counter
/// (every failed probe cycle, monotonic), and the
/// `ravel_store_probe_last_run_timestamp_seconds` liveness gauge (unix time
/// the probe task last completed a cycle, whatever its outcome). All three
/// are process-global atomic reads from [`crate::store_probe`], single source
/// and no labels, the same shape as the tenancy and provisioning families
/// above. Exported unconditionally so an operator sees a store outage, or a
/// dead probe task, on a metrics-only monitoring setup, even where nothing
/// consumes `/readyz`.
///
/// The first two gauges are only written while the probe task is alive: if
/// `tokio::spawn`'s task in [`crate::store_probe::spawn`] dies, they freeze at
/// their last values and `/readyz` keeps reading a stale-but-plausible
/// answer. The last-run gauge is the one signal here that answers "is the
/// probe still running", by its AGE rather than its value; see the alert in
/// docs/guides/observability.md.
fn render_store_probe_family(
    out: &mut String,
    mode: Mode,
    reachable: bool,
    failures_total: u64,
    last_run_unix_ns: i64,
) {
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

    write_header(
        out,
        "ravel_store_probe_last_run_timestamp_seconds",
        // claim-allow: store-probe-zero -- this HELP line ships in /metrics
        // output, where the reader has no link to follow, so it carries a
        // one-line summary of the three causes rather than a pointer.
        "Unix time of the background store probe's last completed cycle or of its spawn, whichever is later; a cycle stamps it whether it succeeded or failed. A reading of 0 has three causes: no probe task in this process, a scrape inside the startup window before the probe is spawned, or a pre-1970 host clock, which a live probe re-stamps as 0 every interval (see the What 0 means section of docs/guides/observability.md). Its age is the probe-liveness signal: unlike ravel_store_reachable and ravel_store_probe_failures_total, which only move while the probe task is alive, this stops advancing the moment the task itself dies.",
        "gauge",
    );
    write_sample_f64(
        out,
        "ravel_store_probe_last_run_timestamp_seconds",
        &[Label::Mode(mode)],
        last_run_unix_ns as f64 / 1e9,
    );
}

/// The `ravel_shutdown_drain_overrun_total` counter (issue #1742): graceful
/// shutdowns this process ran past `--shutdown-timeout`, single source, no
/// labels, the same shape as [`render_store_probe_family`]. See
/// `Running::shutdown`'s reachability note: the client-facing listener stops
/// accepting new connections at the top of that function, before the drain
/// this counter measures even starts, so a scrape landing after that point
/// cannot open a new connection to observe a value this counter changes to.
/// Exported anyway because it is still genuinely rendered on `/metrics` (a
/// scrape already in flight, or one landing before shutdown begins, reads it
/// correctly) and it is the only machine-readable form of the fact the
/// accompanying log line also carries.
fn render_shutdown_family(out: &mut String, mode: Mode, drain_overrun_total: u64) {
    write_header(
        out,
        "ravel_shutdown_drain_overrun_total",
        "Graceful shutdowns this process ran past --shutdown-timeout (issue #1742), monotonic. Set only on the branch where the drain's outer timeout elapsed, never unconditionally after it.",
        "counter",
    );
    write_sample(
        out,
        "ravel_shutdown_drain_overrun_total",
        &[Label::Mode(mode)],
        drain_overrun_total,
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

/// The query-audit pipeline's write-failure counter (ADR-0062 decision 2c).
///
/// Rendered only by a process that installed a pipeline: `maintain` and
/// `gateway` serve no query surface, and a zero for a subsystem they never ran
/// would read as "no failures" rather than "not applicable".
///
/// Under `--audit-mode required` a failed write is returned to the query as a
/// 503 and is not counted here, so any nonzero value is the best-effort
/// posture reporting queries that were served with no durable audit record.
/// It increments once per tenant group whose write fails within a flush, not
/// once per flush. It carries no tenant label: that would disclose which
/// tenant's writes failed on this unauthenticated route.
fn render_audit_family(out: &mut String, mode: Mode, write_failures: u64) {
    write_header(
        out,
        "ravel_audit_write_failures_total",
        "Query-audit writes that failed and were released anyway under --audit-mode best-effort. Each one is a query served with no durable audit record.",
        "counter",
    );
    write_sample(
        out,
        "ravel_audit_write_failures_total",
        &[Label::Mode(mode)],
        write_failures,
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
    /// (ADR-0058 decision 1): `orphans_deleted + orphans_withheld +
    /// orphans_quarantine_refused`. The third term is not optional: a
    /// candidate whose copy to `quarantine/` failed is left live, so it is
    /// still present, and it fails in exactly the store-fault case this gauge
    /// exists to surface. Nonzero
    /// for small-scale commit-record loss the breaker's ratio/count thresholds
    /// are deliberately too coarse to trip on, which is why it is a distinct
    /// gauge from `orphans_withheld` (that one stays `0` precisely when the
    /// breaker does not trip). Like `orphans_withheld` it reflects only the
    /// most recent pass and drops as candidates are deleted or their records
    /// restored; a drop is not "resolved," just this pass's count.
    pub orphans_present: u64,
    /// Orphan candidates moved to `quarantine/` since process start (ADR-0058
    /// amendment). A counter, unlike the two gauges above: quarantining is an
    /// event a later pass does not undo, and the pass-local figure
    /// `SweepReport` reports is summed on
    /// [`crate::maintain::MaintenanceSafetyMetrics`] rather than here.
    pub orphans_quarantined: u64,
    /// Orphan candidates whose copy to `quarantine/` failed since process
    /// start, so the live object was left in place (fail-closed: the delete
    /// never runs when its copy did not). The steady state is a flat line, so
    /// an alert reads `increase(...) > 0` like the breaker-trip counter.
    pub orphans_quarantine_refused: u64,
    /// Objects physically deleted from `quarantine/` past the quarantine
    /// horizon since process start. Read against `orphans_quarantined`: that
    /// one climbing while this one stays flat is a quarantine prefix filling
    /// and never being reaped.
    pub quarantine_reaped: u64,
    /// L0 commit records sitting sealed and still below
    /// `min_compaction_inputs` (issue #1729), summed over every tenant and
    /// shard this process maintains and republished once per maintenance
    /// cycle. A gauge, and a per-process, per-cycle total rather than one
    /// pass's count: a scrape mid-cycle reads the previous complete value.
    /// A bucket the interior memo skipped contributes its last-known count,
    /// which can be up to `interior_reverify_ns` old, so the figure is the
    /// whole pending population rather than only what this cycle re-read. A
    /// value that keeps rising means buckets for this signal are sealing
    /// faster than they cross the compaction threshold.
    pub l0_records_pending: u64,
}

/// One scrape's maintenance-safety counters (ADR-0048 decisions 1, 4, 6): the three safety controls that, before this issue, reached
/// an operator only through a `tracing` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceSafetySnapshot {
    /// Legal-hold refresh failures (ADR-0048 decision 1). Not signal-scoped:
    /// one refresh gates an entire tenant tick, so a failure skips every
    /// signal and shard of that tick at once.
    pub legal_hold_refresh_failures: u64,
    /// Objects physically deleted from `quarantine/` past the quarantine
    /// horizon, summed over every sweep pass of every signal since process
    /// start. Not signal-scoped, unlike the per-signal
    /// `quarantine_reaped` on [`MaintenanceSafetySignalSnapshot`]: this is
    /// the `kind`-labeled `ravel_maintain_objects_deleted_total` series
    /// (issue #1729).
    pub objects_deleted_quarantine_reaped: u64,
    /// Superseded L0 commit records rule 2 physically deleted, summed over
    /// every sweep pass since process start.
    pub objects_deleted_superseded_records_deleted: u64,
    /// Superseded L0 data objects rule 2 physically deleted, summed over
    /// every sweep pass since process start.
    pub objects_deleted_superseded_data_deleted: u64,
    /// Unreferenced L1 parts rule 3 physically deleted, summed over every
    /// sweep pass since process start.
    pub objects_deleted_unreferenced_parts_deleted: u64,
    pub signals: Vec<MaintenanceSafetySignalSnapshot>,
}

impl MaintenanceSafetySnapshot {
    /// Read every counter for every maintained signal at scrape time (atomic
    /// loads, no `.await`), like every other family's snapshot constructor.
    ///
    /// A constructor rather than a literal at the `/metrics` handler so a
    /// field added to the metrics struct and left out of the exposition is a
    /// failing render test rather than a series nobody notices is missing.
    pub fn from_metrics(metrics: &crate::maintain::MaintenanceSafetyMetrics) -> Self {
        MaintenanceSafetySnapshot {
            legal_hold_refresh_failures: metrics.legal_hold_refresh_failures(),
            objects_deleted_quarantine_reaped: metrics.objects_deleted_quarantine_reaped(),
            objects_deleted_superseded_records_deleted: metrics
                .objects_deleted_superseded_records_deleted(),
            objects_deleted_superseded_data_deleted: metrics
                .objects_deleted_superseded_data_deleted(),
            objects_deleted_unreferenced_parts_deleted: metrics
                .objects_deleted_unreferenced_parts_deleted(),
            signals: crate::maintain::MAINTAINED_SIGNALS
                .iter()
                .map(|&signal| MaintenanceSafetySignalSnapshot {
                    signal,
                    conservation_aborts: metrics.conservation_aborts(signal),
                    orphan_breaker_trips: metrics.orphan_breaker_trips(signal),
                    orphans_withheld: metrics.orphans_withheld(signal),
                    orphans_present: metrics.orphans_present(signal),
                    orphans_quarantined: metrics.orphans_quarantined(signal),
                    orphans_quarantine_refused: metrics.orphans_quarantine_refused(signal),
                    quarantine_reaped: metrics.quarantine_reaped(signal),
                    l0_records_pending: metrics.l0_records_pending(signal),
                })
                .collect(),
        }
    }
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

    // The quarantine leg of orphan GC (ADR-0058 amendment). Counters, not
    // gauges like the two above: each counts what a pass did, which the next
    // pass does not undo, so the operator question they answer is a rate.
    write_header(
        out,
        "ravel_maintain_orphans_quarantined_total",
        "Orphan candidates moved from the live L0 set to the quarantine prefix, by signal. The \
         deletion orphan GC performs is a copy plus a delete, so this is the rate at which \
         record-less data objects are being taken out of the live set.",
        "counter",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_maintain_orphans_quarantined_total",
            &labels(mode, signal.signal),
            signal.orphans_quarantined,
        );
    }

    write_header(
        out,
        "ravel_maintain_orphans_quarantine_refused_total",
        "Orphan candidates whose copy to the quarantine prefix failed, by signal; the live \
         object was left in place rather than deleted without a copy. The steady state is a \
         flat line, so alert on increase() > 0: a refusal means quarantine cannot make \
         progress, from a store fault or a permissions or capacity problem on that prefix.",
        "counter",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_maintain_orphans_quarantine_refused_total",
            &labels(mode, signal.signal),
            signal.orphans_quarantine_refused,
        );
    }

    write_header(
        out,
        "ravel_maintain_quarantine_reaped_total",
        "Objects physically deleted from the quarantine prefix past the quarantine horizon, by \
         signal. The only place orphan-GC'd data is ever physically removed. Read it against \
         ravel_maintain_orphans_quarantined_total: that one climbing while this one stays flat \
         is a quarantine prefix that fills and is never reaped.",
        "counter",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_maintain_quarantine_reaped_total",
            &labels(mode, signal.signal),
            signal.quarantine_reaped,
        );
    }

    write_header(
        out,
        "ravel_maintain_l0_records_pending",
        "L0 commit records sitting sealed and still below min_compaction_inputs, by signal, \
         summed over every tenant and shard this process maintains and republished once per \
         maintenance cycle. A gauge, not a running total, and a per-process total rather than \
         one pass or one unit: a mid-cycle scrape reads the previous cycle's complete value, and \
         a bucket the interior memo skipped contributes its last-known count. A steadily rising \
         value means buckets for that signal are sealing faster than they cross the compaction \
         threshold; see the troubleshooting guide.",
        "gauge",
    );
    for signal in &snapshot.signals {
        write_sample(
            out,
            "ravel_maintain_l0_records_pending",
            &labels(mode, signal.signal),
            signal.l0_records_pending,
        );
    }

    // The kind-labeled deleted-objects family (issue #1729): the four
    // SweepReport fields that represent an actual physical delete, summed
    // across every signal and shard since process start, not split by
    // signal like the counters above.
    fn kind_labels(mode: Mode, kind: DeletedObjectKind) -> [Label; 2] {
        [Label::Mode(mode), Label::DeletedObjectKind(kind)]
    }

    write_header(
        out,
        "ravel_maintain_objects_deleted_total",
        "Objects physically deleted by the GC sweeper, by kind, named for the SweepReport field \
         each counts: quarantine_reaped, superseded_records_deleted, superseded_data_deleted, or \
         unreferenced_parts_deleted. Not signal-scoped: summed across every signal and shard \
         this process has swept.",
        "counter",
    );
    for (kind, value) in [
        (
            DeletedObjectKind::QuarantineReaped,
            snapshot.objects_deleted_quarantine_reaped,
        ),
        (
            DeletedObjectKind::SupersededRecordsDeleted,
            snapshot.objects_deleted_superseded_records_deleted,
        ),
        (
            DeletedObjectKind::SupersededDataDeleted,
            snapshot.objects_deleted_superseded_data_deleted,
        ),
        (
            DeletedObjectKind::UnreferencedPartsDeleted,
            snapshot.objects_deleted_unreferenced_parts_deleted,
        ),
    ] {
        write_sample(
            out,
            "ravel_maintain_objects_deleted_total",
            &kind_labels(mode, kind),
            value,
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
    /// Unix nanoseconds the supervised maintenance loop last completed a cycle,
    /// `0` if none has completed yet. Rendered as
    /// `ravel_maintain_last_cycle_completed_timestamp_seconds`; its age is the
    /// maintain-liveness signal (issue #1683).
    pub last_cycle_completed_unix_ns: i64,
    /// Panics the supervisor caught in the loop body and restarted after.
    pub loop_panics_total: u64,
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

    // The liveness gauge, and the only figure in this family that moves when the
    // loop STOPS rather than when it runs. Every other maintenance gauge is
    // written at the end of a cycle that completed, so a dead loop freezes them
    // at their last healthy values; this one's age keeps growing. `0` means no
    // cycle has completed since this process started, which is why the alert
    // rule in docs/guides/observability.md carries a `for:` long enough to
    // cover a freshly started process's first interval (issue #1683, mirroring
    // ravel_catalog_fold_last_success_timestamp_seconds).
    write_header(
        out,
        "ravel_maintain_last_cycle_completed_timestamp_seconds",
        "Unix time the maintenance loop last completed a cycle in this process, 0 if none has completed yet. Its age is the maintain-liveness signal.",
        "gauge",
    );
    write_sample_f64(
        out,
        "ravel_maintain_last_cycle_completed_timestamp_seconds",
        &[Label::Mode(mode)],
        snapshot.last_cycle_completed_unix_ns as f64 / 1e9,
    );

    write_header(
        out,
        "ravel_maintain_loop_panics_total",
        "Panics caught in the maintenance loop body and restarted by the supervisor. \
         The loop's only crash record: the supervisor keeps the pod up, so an \
         increase() here is the signal that the loop is crash-looping.",
        "counter",
    );
    write_sample(
        out,
        "ravel_maintain_loop_panics_total",
        &[Label::Mode(mode)],
        snapshot.loop_panics_total,
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

/// One scrape's alert-evaluation counters (issue #532), folded across every
/// tenant this process evaluates, plus the loop's liveness gauge.
///
/// Decoupled from [`crate::alerting::AlertMetrics`] so the renderer is testable
/// with a plain struct literal, matching [`MaintenanceOwnershipSnapshot`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AlertSnapshot {
    pub rules_evaluated: u64,
    pub rules_failed: u64,
    pub records_written: u64,
    pub repeats_queued: u64,
    pub notifications_delivered: u64,
    pub notifications_failed: u64,
    /// Ticks that held the lease and evaluated every rule.
    pub ticks_evaluated: u64,
    /// Ticks that skipped evaluation because a peer replica held the lease.
    /// Healthy, and the steady state of every non-holding replica.
    pub ticks_lease_not_held: u64,
    /// Ticks that skipped evaluation because the lease read or write failed.
    pub ticks_lease_unavailable: u64,
    /// Ticks that evaluated nothing because the alert history was unreadable.
    pub ticks_history_unavailable: u64,
    /// Unix nanoseconds the alert loop last completed a tick, `0` if none has
    /// completed yet. Rendered as
    /// `ravel_alert_last_tick_completed_timestamp_seconds`; its age is the
    /// alert-loop liveness signal.
    pub last_tick_completed_unix_ns: i64,
}

impl AlertSnapshot {
    /// This scrape's tick count for one outcome. Exhaustive, so adding an
    /// outcome breaks the compile here rather than rendering a silent zero.
    fn ticks(&self, outcome: crate::alerting::AlertTickOutcome) -> u64 {
        use crate::alerting::AlertTickOutcome;
        match outcome {
            AlertTickOutcome::Evaluated => self.ticks_evaluated,
            AlertTickOutcome::LeaseNotHeld => self.ticks_lease_not_held,
            AlertTickOutcome::LeaseUnavailable => self.ticks_lease_unavailable,
            AlertTickOutcome::HistoryUnavailable => self.ticks_history_unavailable,
        }
    }
}

/// No `tenant_hash` label on any series here (ADR-0044 section 4): one process
/// runs one evaluator per tenant that has rules, and every counter below is the
/// sum across them. A per-tenant breakdown would put a raw tenant hash on this
/// unauthenticated route, which that section blocks.
///
/// Rendered only when this process built at least one evaluator
/// ([`crate::alerting::active_alert_metrics`]). A deployment that configured no
/// alert rules therefore carries none of these series at all, rather than a row
/// of permanent zeros an alert rule would have to special-case.
fn render_alert_family(out: &mut String, mode: Mode, snapshot: &AlertSnapshot) {
    write_header(
        out,
        "ravel_alert_rules_evaluated_total",
        "Alert rules whose query ran and whose condition was decided, cumulative across ticks and tenants.",
        "counter",
    );
    write_sample(
        out,
        "ravel_alert_rules_evaluated_total",
        &[Label::Mode(mode)],
        snapshot.rules_evaluated,
    );

    write_header(
        out,
        "ravel_alert_rules_failed_total",
        "Alert rules skipped because the query, the condition, or the write failed. Every one is logged; the rule is retried next tick.",
        "counter",
    );
    write_sample(
        out,
        "ravel_alert_rules_failed_total",
        &[Label::Mode(mode)],
        snapshot.rules_failed,
    );

    write_header(
        out,
        "ravel_alert_records_written_total",
        "Alert transition records durably written.",
        "counter",
    );
    write_sample(
        out,
        "ravel_alert_records_written_total",
        &[Label::Mode(mode)],
        snapshot.records_written,
    );

    write_header(
        out,
        "ravel_alert_repeats_queued_total",
        "Repeat notifications queued for a still-firing alert. A repeat re-sends the folded latest record with no new durable write, so it advances this counter and then ravel_alert_notifications_delivered_total, never ravel_alert_records_written_total.",
        "counter",
    );
    write_sample(
        out,
        "ravel_alert_repeats_queued_total",
        &[Label::Mode(mode)],
        snapshot.repeats_queued,
    );

    write_header(
        out,
        "ravel_alert_notifications_delivered_total",
        "Notifications delivered to every configured sink, including ones carried over from an earlier tick's failure.",
        "counter",
    );
    write_sample(
        out,
        "ravel_alert_notifications_delivered_total",
        &[Label::Mode(mode)],
        snapshot.notifications_delivered,
    );

    write_header(
        out,
        "ravel_alert_notifications_failed_total",
        "Notifications still undelivered after a tick's attempt, counted once per tick per notification, so one stuck notification keeps advancing this while it is retried.",
        "counter",
    );
    write_sample(
        out,
        "ravel_alert_notifications_failed_total",
        &[Label::Mode(mode)],
        snapshot.notifications_failed,
    );

    // One counter split by a closed outcome, not three independent flags. The
    // outcomes are mutually exclusive per tick, and `lease_not_held` is the
    // healthy steady state of every replica that is not the lease holder, so it
    // has to be countable without being failure: an alert rule that sums it
    // with the two store-failure outcomes turns a normal multi-replica
    // deployment into a permanent alarm.
    write_header(
        out,
        "ravel_alert_ticks_total",
        "Alert evaluation ticks by outcome: evaluated (this replica held the tenant lease and evaluated every rule), lease_not_held (a peer held it, the healthy multi-replica steady state), lease_unavailable (the lease read or write failed), history_unavailable (the alert history was unreadable, so nothing was evaluated).",
        "counter",
    );
    for outcome in crate::alerting::AlertTickOutcome::ALL {
        write_sample(
            out,
            "ravel_alert_ticks_total",
            &[Label::Mode(mode), Label::AlertOutcome(outcome)],
            snapshot.ticks(outcome),
        );
    }

    // The liveness gauge, and the only figure in this family that moves when
    // the loop STOPS rather than when it runs. Every counter above is
    // cumulative, so a dead evaluator leaves them frozen and indistinguishable
    // from a healthy deployment whose rules never fire; this one's age keeps
    // growing. `0` means no tick has completed since this process started,
    // which is why the alert rule in docs/guides/observability.md carries a
    // `for:` long enough to cover a freshly started process's first interval
    // (the same shape as ravel_maintain_last_cycle_completed_timestamp_seconds).
    // A tick that ended in lease_not_held stamps it: a standby replica is
    // alive, and holding it back would alarm on the steady state.
    write_header(
        out,
        "ravel_alert_last_tick_completed_timestamp_seconds",
        "Unix time the alert evaluation loop last completed a tick in this process, 0 if none has completed yet. Its age is the alert-loop liveness signal.",
        "gauge",
    );
    write_sample_f64(
        out,
        "ravel_alert_last_tick_completed_timestamp_seconds",
        &[Label::Mode(mode)],
        snapshot.last_tick_completed_unix_ns as f64 / 1e9,
    );
}

/// Per-level breakdown of one signal's `checksum_mismatch` counter (issue
/// #1686): which commit-lineage part -- `l0`, `l1`, or `rewrite` -- the
/// scrub corpus found the corruption in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScrubLevelCounts {
    pub l0: u64,
    pub l1: u64,
    pub rewrite: u64,
}

/// One signal's at-rest scrubber counters for one scrape (ADR-0059 decisions
/// 1, 3).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScrubSignalSnapshot {
    pub signal: Signal,
    /// Objects that failed at-rest integrity re-verification for this signal:
    /// a whole-object blake3 mismatch against the recorded content hash (bit
    /// rot / partial write) or a footer/section crc failure. Both are
    /// data-object corruption, so both increment this one counter, broken
    /// down by which commit-lineage part (`l0`, `l1`, `rewrite`) it came from
    /// (issue #1686).
    pub checksum_mismatch: ScrubLevelCounts,
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
         or footer/section crc failure), by signal and level (ADR-0059, issue #1686): \
         level=\"l0\" is an original ingested segment, level=\"l1\" a compaction output part, \
         level=\"rewrite\" a selective-erasure rewrite output part. Alert on increase() > 0: there \
         is no redundant copy to repair from, so a nonzero increase is corruption an operator \
         must investigate, with one caveat on level=\"l0\" only: an L0 segment a live compaction \
         has already folded into an l1 part is scrubbed anyway, so check whether the hour is \
         compacted before treating an l0 mismatch as unrecoverable.",
        "counter",
    );
    for signal in &snapshot.signals {
        for level in [ScrubLevel::L0, ScrubLevel::L1, ScrubLevel::Rewrite] {
            let value = match level {
                ScrubLevel::L0 => signal.checksum_mismatch.l0,
                ScrubLevel::L1 => signal.checksum_mismatch.l1,
                ScrubLevel::Rewrite => signal.checksum_mismatch.rewrite,
            };
            write_sample(
                out,
                "ravel_scrub_checksum_mismatch_total",
                &[
                    Label::Mode(mode),
                    Label::Signal(signal.signal),
                    Label::ScrubLevel(level),
                ],
                value,
            );
        }
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
    fetch_disk: Option<&CacheMetricsSnapshot>,
    catalog: Option<&CacheMetricsSnapshot>,
    catalog_disk: Option<&CacheMetricsSnapshot>,
) {
    // Per family: (cache label, RAM-tier snapshot, disk-tier snapshot). The disk
    // snapshot is `Some` only when `--cache-dir` attached a disk tier to that
    // family (#97).
    let families = [
        (CacheFamily::Fetch, fetch, fetch_disk),
        (CacheFamily::Catalog, catalog, catalog_disk),
    ];

    // One metric name at a time: header once, then a sample per attached tier.
    // With NO disk tier for a family, its RAM sample carries only the `cache=`
    // label, byte-for-byte the pre-#97 output. With a disk tier present, the RAM
    // sample gains `tier=ram` and the disk sample carries `tier=disk`, so the two
    // orthogonal labels (`cache=`, `tier=`) split the family. `field` picks the
    // counter this metric renders.
    let mut emit = |name: &str, help: &str, field: fn(&CacheMetricsSnapshot) -> u64| {
        write_header(out, name, help, "counter");
        for (family, ram, disk) in families {
            if let Some(ram) = ram {
                if disk.is_some() {
                    write_sample(
                        out,
                        name,
                        &[
                            Label::Mode(mode),
                            Label::Cache(family),
                            Label::CacheTier(CacheTier::Ram),
                        ],
                        field(ram),
                    );
                } else {
                    write_sample(
                        out,
                        name,
                        &[Label::Mode(mode), Label::Cache(family)],
                        field(ram),
                    );
                }
            }
            if let Some(disk) = disk {
                write_sample(
                    out,
                    name,
                    &[
                        Label::Mode(mode),
                        Label::Cache(family),
                        Label::CacheTier(CacheTier::Disk),
                    ],
                    field(disk),
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

/// Live (not cumulative) resident bytes and entry count for the ADR-0046
/// fetcher read cache's tiers, plus each cache's resolved startup byte
/// ceiling, so an operator can compare held-vs-budgeted (#1170) without
/// deriving it from a cumulative counter that cannot answer "what is
/// resident right now" -- [`render_cache_family`] above renders that other,
/// cumulative half of the picture (hits, misses, evictions).
///
/// The catalog byte cache's own live residency is deliberately NOT rendered
/// here: reaching it requires a `pub` accessor on `crates/ravel-catalog`,
/// outside this task's declared scope (`services/ravel-server` and
/// `crates/ravel-cache` only). Only its resolved ceiling is exposed, under
/// `cache="catalog"`, so the gap is a missing residency row, not a missing
/// cache row.
fn render_cache_residency_family(
    out: &mut String,
    mode: Mode,
    fetch_ram: Option<(usize, u64)>,
    fetch_disk: Option<(usize, u64)>,
    fetch_max_bytes: Option<u64>,
    catalog_max_bytes: Option<u64>,
) {
    // Same disk-tier-present-or-not label discipline `render_cache_family`
    // uses: with no disk tier, the RAM sample carries only `cache=`; with one,
    // the RAM sample gains `tier="ram"` and the disk sample carries
    // `tier="disk"`.
    let mut emit_tier = |name: &str, help: &str, ram: Option<u64>, disk: Option<u64>| {
        write_header(out, name, help, "gauge");
        if let Some(value) = ram {
            if disk.is_some() {
                write_sample(
                    out,
                    name,
                    &[
                        Label::Mode(mode),
                        Label::Cache(CacheFamily::Fetch),
                        Label::CacheTier(CacheTier::Ram),
                    ],
                    value,
                );
            } else {
                write_sample(
                    out,
                    name,
                    &[Label::Mode(mode), Label::Cache(CacheFamily::Fetch)],
                    value,
                );
            }
        }
        if let Some(value) = disk {
            write_sample(
                out,
                name,
                &[
                    Label::Mode(mode),
                    Label::Cache(CacheFamily::Fetch),
                    Label::CacheTier(CacheTier::Disk),
                ],
                value,
            );
        }
    };

    emit_tier(
        "ravel_cache_resident_entries",
        "Entries currently held in this read-cache tier (ADR-0046), live rather than cumulative.",
        fetch_ram.map(|(len, _)| len as u64),
        fetch_disk.map(|(len, _)| len as u64),
    );
    emit_tier(
        "ravel_cache_resident_bytes",
        "Payload bytes currently held in this read-cache tier (ADR-0046), live rather than cumulative.",
        fetch_ram.map(|(_, bytes)| bytes),
        fetch_disk.map(|(_, bytes)| bytes),
    );

    write_header(
        out,
        "ravel_cache_max_bytes",
        "The resolved startup byte ceiling for this cache, shared by its RAM and disk tiers, for \
         comparing held-vs-budgeted.",
        "gauge",
    );
    if let Some(max_bytes) = fetch_max_bytes {
        write_sample(
            out,
            "ravel_cache_max_bytes",
            &[Label::Mode(mode), Label::Cache(CacheFamily::Fetch)],
            max_bytes,
        );
    }
    if let Some(max_bytes) = catalog_max_bytes {
        write_sample(
            out,
            "ravel_cache_max_bytes",
            &[Label::Mode(mode), Label::Cache(CacheFamily::Catalog)],
            max_bytes,
        );
    }
}

/// This process's own allocator-reported figures (#1170), read live at
/// scrape time via [`crate::mem_stats::read`]: a whole-process RSS number
/// cannot say which subsystem grew, so this surfaces the allocator's own
/// breakdown -- or names the allocator plainly when it is not jemalloc,
/// rather than reporting zeros for stats an allocator that isn't jemalloc
/// does not expose.
fn render_allocator_family(
    out: &mut String,
    mode: Mode,
    allocator: crate::mem_stats::AllocatorStats,
) {
    match allocator {
        crate::mem_stats::AllocatorStats::Jemalloc {
            allocated,
            active,
            resident,
        } => {
            write_header(
                out,
                "ravel_process_allocator_bytes",
                "This process's jemalloc-reported byte figures, split by stat= (allocated/active/resident).",
                "gauge",
            );
            for (stat, value) in [
                (AllocatorStat::Allocated, allocated),
                (AllocatorStat::Active, active),
                (AllocatorStat::Resident, resident),
            ] {
                write_sample(
                    out,
                    "ravel_process_allocator_bytes",
                    &[
                        Label::Mode(mode),
                        Label::Allocator("jemalloc"),
                        Label::AllocatorStat(stat),
                    ],
                    value,
                );
            }
        }
        crate::mem_stats::AllocatorStats::Other { name } => {
            write_header(
                out,
                "ravel_process_allocator_info",
                "Which allocator this process runs under, when it is not jemalloc: a 1-valued info series \
                 naming it, never a stand-in zero for figures this allocator does not expose.",
                "gauge",
            );
            write_sample(
                out,
                "ravel_process_allocator_info",
                &[Label::Mode(mode), Label::Allocator(name)],
                1,
            );
        }
    }
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
    /// Per-tenant normalization-layer decisions (ADR-0051 section 3, layer 3),
    /// rendered as the `skew` and `structural` reasons of this family's
    /// rejection counter plus the separate body-conversion counter. Sourced
    /// from [`crate::normalize_reject_metrics::NormalizeRejectMetrics`], not
    /// the admission `usage_snapshot`: the controller enforces layers 2 and 4
    /// and keeps no row for a decision normalization made. Folded by the same
    /// `tenant_labels` gate as `usage`.
    pub normalize_rejects: Vec<crate::normalize_reject_metrics::TenantNormalizeRejects>,
    /// What the last completed fleet-reconciliation cycle cost and saw
    /// (ADR-0057). Process-global, not per (tenant, signal): one cycle covers
    /// every tenant this process tracks, so its series carry `mode` alone.
    /// All-zero before the first cycle, and in a mode that runs no
    /// reconciliation loop at all.
    pub reconcile_cycle: ReconcileCycleSnapshot,
}

/// The reconciliation cycle figures for one scrape (ADR-0057), assembled from
/// the two places they live: the controller publishes the last cycle's own
/// figures, and the exporter-side accumulator keeps the running reaped total
/// the controller deliberately does not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileCycleSnapshot {
    /// Last cycle's duration in nanoseconds, measured on the controller's
    /// injected clock. Rendered in seconds.
    pub cycle_duration_ns: i64,
    /// Distinct non-stale sibling processes the last cycle saw, which is the
    /// live fleet size this process reconciled against.
    pub siblings_observed: u64,
    /// Listed keys the last cycle did not GET because the LIST already showed
    /// them past the staleness window.
    pub stale_keys_skipped: u64,
    /// Keys past the reap horizon deleted by every cycle since process start.
    /// A running total, unlike the three figures above: see
    /// [`crate::admission_reconcile::ReconcileCycleMetrics`] for why the
    /// per-cycle count cannot carry the `_total` name itself.
    pub keys_reaped_total: u64,
}

impl ReconcileCycleSnapshot {
    /// Pair the controller's last-cycle copy with the exporter's running
    /// reaped total, the two reads the `/metrics` handler makes.
    pub fn from_parts(
        last_cycle: ravel_ingest::ReconcileCycleStats,
        keys_reaped_total: u64,
    ) -> Self {
        ReconcileCycleSnapshot {
            cycle_duration_ns: last_cycle.cycle_duration_ns,
            siblings_observed: last_cycle.siblings_observed,
            stale_keys_skipped: last_cycle.stale_keys_skipped,
            keys_reaped_total,
        }
    }
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
    rejected_skew: u64,
    rejected_structural: u64,
    body_conversions: u64,
    resource_attrs_dropped: u64,
    reconciliation_failures: u64,
}

impl AdmissionAcc {
    fn rejected(&self, reason: RejectReason) -> u64 {
        match reason {
            RejectReason::ByteRate => self.rejected_byte_rate,
            RejectReason::SeriesRate => self.rejected_series_rate,
            RejectReason::SeriesCap => self.rejected_series_cap,
            RejectReason::Clock => self.rejected_clock,
            RejectReason::Skew => self.rejected_skew,
            RejectReason::Structural => self.rejected_structural,
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

    // Normalization-layer decisions fold into the same rows, under the same
    // tenant-label gate. A (tenant, signal) that has only these and no
    // admission-controller usage still gets a full row: every other counter
    // renders zero, which is what it is, and the rejection reasons the sender
    // was told about are visible rather than absent.
    for row in &snapshot.normalize_rejects {
        let key = (
            snapshot.tenant_labels.then_some(row.tenant_hash),
            row.signal,
        );
        let acc = rows.entry(key).or_default();
        acc.rejected_skew = acc.rejected_skew.saturating_add(row.skew_total);
        acc.rejected_structural = acc.rejected_structural.saturating_add(row.structural_total);
        acc.body_conversions = acc
            .body_conversions
            .saturating_add(row.body_conversions_total);
        acc.resource_attrs_dropped = acc
            .resource_attrs_dropped
            .saturating_add(row.resource_attrs_dropped_total);
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
        "Admission rejections by tenant, signal, and reason (byte_rate, series_rate, series_cap, \
         clock, skew, structural). byte_rate and clock count whole requests; series_rate and \
         series_cap count series; skew and structural count individual data points, log records, \
         or spans rejected in normalization, matching what the sender is told through OTLP \
         partial success.",
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

    // Structured log bodies converted rather than rejected. Deliberately its
    // own family and not a `reason` on the counter above: a conversion is not
    // a rejection, so an operator alerting on rejection reasons must see
    // nothing from them.
    write_header(
        out,
        "ravel_ingest_body_conversions_total",
        "Log records whose structured (array or kvlist) body was converted to its canonical JSON \
         form at normalization, by tenant and signal. Not a rejection, and not a count of stored \
         records: it is counted before the active-stream cap and before the write. Read it as a \
         conversion rate. A sustained rate means a sender is emitting structured bodies, which \
         query paths see as JSON text.",
        "counter",
    );
    for ((hash, signal), acc) in &ordered {
        write_sample(
            out,
            "ravel_ingest_body_conversions_total",
            &labels(mode, *hash, *signal),
            acc.body_conversions,
        );
    }

    // Resource attributes outside the allowlist, dropped rather than turned
    // into labels. Its own family for the same reason body-conversions is:
    // this is not a rejection, counted before the series cap and the write,
    // so not a count of stored points. Key names are caller-controlled and
    // unbounded, a cardinality hazard as a label, so this counts drops,
    // never names them; see `crate::normalize_reject_metrics` and the crate
    // docs on `Rejection::ResourceAttributesDropped` for the full reasoning.
    // Metrics-only: OTLP HTTP and OTLP gRPC build resource labels this way;
    // OTAP builds none at all (`services/ravel-server/src/otap_grpc.rs`),
    // and logs/traces never call the recorder, so a logs or spans row would
    // always render 0. Skip them rather than render a figure that can never
    // be anything else.
    write_header(
        out,
        "ravel_ingest_resource_attrs_dropped_total",
        "Metric resource attributes outside the configured allowlist, dropped rather than turned \
         into labels, by tenant, for the metrics signal only. Not a rejection: counted before the \
         series cap and the write, so not a count of stored points. Covers OTLP HTTP and OTLP \
         gRPC ingest; OTAP is not covered, since it builds no resource labels at all. The \
         allowlist is not configurable today. This counter only makes an existing silent drop \
         visible, including the case where two resources differing only in a dropped attribute \
         collapse into one series.",
        "counter",
    );
    for ((hash, signal), acc) in &ordered {
        if *signal != Signal::Metrics {
            continue;
        }
        write_sample(
            out,
            "ravel_ingest_resource_attrs_dropped_total",
            &labels(mode, *hash, *signal),
            acc.resource_attrs_dropped,
        );
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

    // The cycle itself, beside the failure counter above. `mode` alone, no
    // {tenant_hash, signal}: one cycle reconciles every tenant this process
    // tracks, so there is no per-tenant figure to label. The failure counter
    // keeps its tenant dimension because a read failure is per (tenant,
    // signal); nothing here is.
    //
    // The three per-cycle figures are gauges. Each is the last completed
    // cycle's value and each can fall (a fleet that shrinks, a cycle that
    // finishes faster), so none of them takes a `_total` name. Only the reaped
    // keys accumulate, on the exporter side, and only that one is a counter.
    let cycle = &snapshot.reconcile_cycle;
    write_header(
        out,
        "ravel_admission_reconciliation_cycle_duration_seconds",
        "Duration of the last completed fleet-admission reconciliation cycle. A cycle \
         approaching the 2R staleness window (twice the reconciliation interval) makes every \
         sibling snapshot read as stale, at which point each process starts enforcing the whole \
         fleet cap alone while no failure counter moves.",
        "gauge",
    );
    write_sample_f64(
        out,
        "ravel_admission_reconciliation_cycle_duration_seconds",
        &[Label::Mode(mode)],
        // The cycle saturates at zero rather than going negative if the clock
        // steps backwards mid-cycle; clamp anyway, since a negative duration
        // here would be a silently nonsensical sample rather than an error.
        cycle.cycle_duration_ns.max(0) as f64 / 1_000_000_000.0,
    );

    write_header(
        out,
        "ravel_admission_reconciliation_siblings_observed",
        "Distinct non-stale sibling processes the last completed reconciliation cycle saw, the \
         live fleet size this process reconciled its share of each tenant's cap against. It \
         falling to 0 while replicas are up means this process is reading no sibling as live.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_admission_reconciliation_siblings_observed",
        &[Label::Mode(mode)],
        cycle.siblings_observed,
    );

    write_header(
        out,
        "ravel_admission_reconciliation_stale_keys_skipped",
        "Snapshot keys the last completed reconciliation cycle skipped reading because the LIST \
         already showed them past the staleness window. Sustained growth alongside a flat \
         siblings_observed is a control-plane prefix filling with dead processes' keys.",
        "gauge",
    );
    write_sample(
        out,
        "ravel_admission_reconciliation_stale_keys_skipped",
        &[Label::Mode(mode)],
        cycle.stale_keys_skipped,
    );

    write_header(
        out,
        "ravel_admission_reconciliation_keys_reaped_total",
        "Snapshot keys past the reap horizon deleted by reconciliation cycles since process \
         start. A rate at zero while stale_keys_skipped climbs means the prefix is filling \
         faster than it is being cleared.",
        "counter",
    );
    write_sample(
        out,
        "ravel_admission_reconciliation_keys_reaped_total",
        &[Label::Mode(mode)],
        cycle.keys_reaped_total,
    );
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

impl QueryCostCounters {
    /// Fold one query's cost into this row.
    ///
    /// Both the workload-class family and the outcome-status family accumulate
    /// the identical set of counters and differ only in which map and key they
    /// select, so the arithmetic lives here once. Adding a field to this struct
    /// and not to this method under-reports it in BOTH families rather than
    /// silently in one, which is the failure mode the duplicated version had.
    fn fold(&mut self, accounting: &QueryAccountingSnapshot, estimate: &CostEstimate) {
        self.queries = self.queries.saturating_add(1);
        self.s3_requests = self
            .s3_requests
            .saturating_add(accounting.total_s3_requests());
        self.s3_bytes = self.s3_bytes.saturating_add(accounting.total_s3_bytes());
        self.cache_hits = self.cache_hits.saturating_add(accounting.cache_hits);
        self.cache_misses = self.cache_misses.saturating_add(accounting.cache_misses);
        self.decompressed_bytes = self
            .decompressed_bytes
            .saturating_add(accounting.decompressed_bytes);
        self.estimated_requests = self
            .estimated_requests
            .saturating_add(estimate.estimated_requests);
        self.estimated_store_bytes = self
            .estimated_store_bytes
            .saturating_add(estimate.estimated_store_bytes);
        self.estimated_decompressed_bytes = self
            .estimated_decompressed_bytes
            .saturating_add(estimate.estimated_decompressed_bytes);
    }
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

/// The final disposition of one query, recorded alongside its cost so a total
/// can be split by outcome (issue #809). A closed set of four: every query
/// exits as exactly one of these, never a fifth kind invented to describe an
/// exit these do not cover.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryOutcomeStatus {
    Success,
    Error,
    Timeout,
    Canceled,
}

impl QueryOutcomeStatus {
    pub fn name(self) -> &'static str {
        match self {
            QueryOutcomeStatus::Success => "success",
            QueryOutcomeStatus::Error => "error",
            QueryOutcomeStatus::Timeout => "timeout",
            QueryOutcomeStatus::Canceled => "canceled",
        }
    }
}

/// One rendered row of the per-query outcome-status split (issue #809): the
/// (tenant bucket, status) key plus its accumulated [`QueryCostCounters`].
/// `tenant` follows the same `None`-is-`other` convention as
/// [`QueryAccountingRow`]. Kept as an independent map from `rows` rather than
/// widening the existing `ravel_query_*` family's key by a `status` label: the
/// existing family is fed only on success today (issue #680's stale-comment
/// finding aside, error/timeout/canceled queries have never folded into it),
/// and giving `render_query_family` a new label dimension would multiply its
/// series count for a rendering path this ticket does not require. This row
/// is queryable via [`QueryAccountingMetrics::outcome_snapshot`]; it does not
/// (yet) render on `/metrics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryOutcomeRow {
    pub tenant: Option<TenantHash>,
    pub status: QueryOutcomeStatus,
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
    /// The outcome-status split (issue #809), independent of `rows` above;
    /// see [`QueryOutcomeRow`] for why it is a separate map.
    outcomes:
        parking_lot::Mutex<HashMap<(Option<TenantHash>, QueryOutcomeStatus), QueryCostCounters>>,
}

impl QueryAccountingMetrics {
    /// A new aggregator whose per-tenant allowlist is `configured`; every
    /// tenant outside it folds into `other`. Pass an empty set for the
    /// cardinality-safe default (every tenant folds).
    pub fn new(configured: HashSet<TenantHash>) -> Self {
        QueryAccountingMetrics {
            configured,
            rows: parking_lot::Mutex::new(HashMap::new()),
            outcomes: parking_lot::Mutex::new(HashMap::new()),
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
        acc.fold(accounting, estimate);
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

    /// Fold one completed query's actual counters and its pre-execution
    /// estimate into the (tenant bucket, status) row (issue #809). Same
    /// cardinality-bounding fold and `saturating_add` discipline as
    /// [`QueryAccountingMetrics::record`], into the independent `outcomes`
    /// map so this never changes what the existing `ravel_query_*` family
    /// reports.
    pub fn record_outcome(
        &self,
        tenant_hash: TenantHash,
        status: QueryOutcomeStatus,
        accounting: &QueryAccountingSnapshot,
        estimate: &CostEstimate,
    ) {
        let bucket = self
            .configured
            .contains(&tenant_hash)
            .then_some(tenant_hash);
        let mut outcomes = self.outcomes.lock();
        let acc = outcomes.entry((bucket, status)).or_default();
        acc.fold(accounting, estimate);
    }

    /// A stable-ordered copy of every observed outcome-status row, mirroring
    /// [`QueryAccountingMetrics::snapshot`]'s sort discipline (tenant label,
    /// then a secondary key -- here the status name).
    pub fn outcome_snapshot(&self) -> Vec<QueryOutcomeRow> {
        let outcomes = self.outcomes.lock();
        let mut out: Vec<QueryOutcomeRow> = outcomes
            .iter()
            .map(|((tenant, status), counters)| QueryOutcomeRow {
                tenant: *tenant,
                status: *status,
                counters: *counters,
            })
            .collect();
        out.sort_by(|a, b| {
            tenant_label(a.tenant)
                .value()
                .cmp(&tenant_label(b.tenant).value())
                .then_with(|| a.status.name().cmp(b.status.name()))
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
    /// In-flight fragment requests per fragment admission class
    /// (`Pinned`, `Resolve`; issue #1722).
    pub fragment_inflight_by_class: [(crate::distrib::AdmissionClass, u64); 2],
    /// Cumulative admission-queue waits per class (issue #1722).
    pub fragment_admission_waits_by_class: [(crate::distrib::AdmissionClass, u64); 2],
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
            fragment_inflight_by_class: metrics.fragment_inflight_by_class(),
            fragment_admission_waits_by_class: metrics.fragment_admission_waits_by_class(),
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
        "Fragment requests currently holding an admission permit, by ADR-0071 admission \
         class (`pinned` for intra-cluster fan-out, `resolve` for cross-cluster \
         federation; issue #1722).",
        "gauge",
    );
    for (class, inflight) in snapshot.fragment_inflight_by_class {
        write_sample(
            out,
            "ravel_distrib_fragment_inflight",
            &[Label::Mode(mode), Label::AdmissionClass(class)],
            inflight,
        );
    }

    write_header(
        out,
        "ravel_distrib_fragment_admission_waits_total",
        "Fragment requests that found their admission class's semaphore saturated and had \
         to queue, by ADR-0071 admission class (issue #1722).",
        "counter",
    );
    for (class, waits) in snapshot.fragment_admission_waits_by_class {
        write_sample(
            out,
            "ravel_distrib_fragment_admission_waits_total",
            &[Label::Mode(mode), Label::AdmissionClass(class)],
            waits,
        );
    }

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
    cache_disk: Option<&CacheMetricsSnapshot>,
    catalog_cache: Option<&CacheMetricsSnapshot>,
    catalog_cache_disk: Option<&CacheMetricsSnapshot>,
    admission: &AdmissionCountersSnapshot,
    query_accounting: &[QueryAccountingRow],
    ingest_concurrency_shed_total: u64,
    ingest_buffer_budget: IngestBufferBudgetSnapshot,
    distrib: Option<&DistribSnapshot>,
    durable_auth: Option<&DurableAuthCountersSnapshot>,
    attribution: &[TenantAttributionRow],
    metadata_cache: Option<&MetadataCacheCounters>,
    allocator: crate::mem_stats::AllocatorStats,
    cache_ram_residency: Option<(usize, u64)>,
    cache_disk_residency: Option<(usize, u64)>,
    cache_max_bytes: Option<u64>,
    catalog_cache_max_bytes: Option<u64>,
    audit_write_failures: Option<u64>,
    memory_budget: MemoryBudgetSnapshot,
    can_fold: bool,
) -> String {
    let mut out = String::new();
    render_allocator_family(&mut out, mode, allocator);
    render_store_family(&mut out, mode, store);
    if !ingest.is_empty() {
        render_ingest_family(&mut out, mode, ingest);
        render_ingest_shard_family(&mut out, mode, ingest);
        render_logs_postings_family(&mut out, mode, ingest);
    }
    render_catalog_family(&mut out, mode, catalog);
    // Process-global reads, like `crate::tenancy::v1_unkeyed_adoption_count`
    // below: the drop tally is incremented by every reader in the process
    // (ravel-commit and ravel-sql), and the fold totals are accumulated by
    // `ravel_catalog::fold` as each fold attempt commits its HEAD, so neither
    // has a snapshot struct the `/metrics` route is handed. The fold totals
    // are read only when `can_fold` says a fold can run in this process by
    // either route (`MetricsState::can_fold`'s doc comment has the exact
    // gate): a mode that merely permits a fold is not the same as a process
    // that can run one, so the two fold families are omitted rather than
    // pinned at zero on a process that can fold by neither route.
    render_declared_stats_family(
        &mut out,
        mode,
        &ravel_commit::declared_stats::declared_stat_drops_observed_all(),
        can_fold.then(|| {
            (
                ravel_catalog::fold_stamped_records_total(),
                ravel_catalog::fold_stamped_entries_total(),
            )
        }),
    );
    render_tenancy_family(&mut out, mode, crate::tenancy::v1_unkeyed_adoption_count());
    render_provisioning_family(
        &mut out,
        mode,
        crate::provisioning::shard_count_mismatch_count(),
        ravel_catalog::shard_count_drift_count(),
    );
    render_store_probe_family(
        &mut out,
        mode,
        crate::store_probe::store_reachable(),
        crate::store_probe::probe_failures_total(),
        crate::store_probe::probe_last_run_unix_ns(),
    );
    render_shutdown_family(&mut out, mode, crate::drain_overrun_total());
    render_bucket_protection_family(
        &mut out,
        mode,
        crate::bucket_protection::bucket_protection_unknown(),
    );
    if let Some(snapshot) = durable_auth {
        render_durable_auth_family(&mut out, mode, snapshot);
    }
    if let Some(write_failures) = audit_write_failures {
        render_audit_family(&mut out, mode, write_failures);
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
    // Read from the process-global alerting handle rather than an argument,
    // like `crate::query_postings_metrics::snapshot` and
    // `crate::store_probe::store_reachable` above: the evaluator is spawned per
    // tenant from `crate::alerting::spawn` and holds no router or state struct
    // the `/metrics` route is given. `None` when this process built no
    // evaluator, which omits the family.
    //
    // That omission is load-bearing: every alert rule in
    // `docs/guides/observability.md` relies on the family being absent, not
    // zero, on a deployment that configured no alerting, since an expression
    // over an absent series is the empty vector. It is deliberately not
    // covered by a `render`-level test. The handle is a process-global
    // `OnceLock`, so a test asserting absence passes or fails on whether some
    // other test in the same binary spawned an evaluator first, and a test
    // that depends on binary-internal ordering is worse than none.
    // `render_alert_family`'s own test covers the present case.
    if let Some(metrics) = crate::alerting::active_alert_metrics() {
        use crate::alerting::AlertTickOutcome;
        render_alert_family(
            &mut out,
            mode,
            &AlertSnapshot {
                rules_evaluated: metrics.rules_evaluated(),
                rules_failed: metrics.rules_failed(),
                records_written: metrics.records_written(),
                repeats_queued: metrics.repeats_queued(),
                notifications_delivered: metrics.notifications_delivered(),
                notifications_failed: metrics.notifications_failed(),
                ticks_evaluated: metrics.ticks(AlertTickOutcome::Evaluated),
                ticks_lease_not_held: metrics.ticks(AlertTickOutcome::LeaseNotHeld),
                ticks_lease_unavailable: metrics.ticks(AlertTickOutcome::LeaseUnavailable),
                ticks_history_unavailable: metrics.ticks(AlertTickOutcome::HistoryUnavailable),
                last_tick_completed_unix_ns: metrics.last_tick_completed_unix_ns(),
            },
        );
    }
    if let Some(snapshot) = scrub {
        render_scrub_family(&mut out, mode, snapshot);
    }
    if cache.is_some()
        || cache_disk.is_some()
        || catalog_cache.is_some()
        || catalog_cache_disk.is_some()
    {
        render_cache_family(
            &mut out,
            mode,
            cache,
            cache_disk,
            catalog_cache,
            catalog_cache_disk,
        );
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
    if cache_ram_residency.is_some()
        || cache_disk_residency.is_some()
        || cache_max_bytes.is_some()
        || catalog_cache_max_bytes.is_some()
    {
        render_cache_residency_family(
            &mut out,
            mode,
            cache_ram_residency,
            cache_disk_residency,
            cache_max_bytes,
            catalog_cache_max_bytes,
        );
    }
    render_memory_budget_family(&mut out, mode, memory_budget);
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

/// The ADR-1170 process memory budget readings the `/metrics` handler
/// snapshots from [`ravel_memory::MemoryBudget`] at scrape time (atomic
/// loads). `Default` (all zero) is the reading of an unpopulated test
/// snapshot, not a real process's; a real process's `limit` is never `0`
/// (see [`render_memory_budget_family`]'s doc comment on the `u64::MAX`
/// unlimited convention).
#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryBudgetSnapshot {
    pub limit: u64,
    pub reserved: u64,
    pub handoff_overlap: u64,
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
    /// The ADR-0046 fetcher cache's RAM-tier counters handle, or
    /// `None` when `--disable-cache` leaves no fetcher cache constructed at all.
    /// Rendered under `cache="fetch"` (with `tier="ram"` when a disk tier
    /// coexists). Sourced from [`ravel_query::ReadCache::ram_metrics`].
    pub cache_metrics: Option<Arc<ravel_cache::CacheMetrics>>,
    /// The ADR-0046 fetcher cache's disk-tier counters handle (#97), or `None`
    /// when the fetcher cache is RAM-only (no `--cache-dir`) or disabled.
    /// Rendered under `cache="fetch",tier="disk"`. Sourced from
    /// [`ravel_query::ReadCache::disk_metrics`].
    pub cache_disk_metrics: Option<Arc<ravel_cache::CacheMetrics>>,
    /// The ADR-0046 catalog byte cache's RAM-tier counters handle, or
    /// `None` when `--disable-cache` leaves no catalog byte cache constructed
    /// at all. Rendered under `cache="catalog"` (with `tier="ram"` when a disk
    /// tier coexists), the same family as the fetcher
    /// cache above, so the documented hit-rate formula covers every ADR-0046
    /// cache in the process, not just the fetcher one. Sourced from
    /// [`ravel_catalog::Catalog::byte_cache_metrics`].
    pub catalog_cache_metrics: Option<Arc<ravel_cache::CacheMetrics>>,
    /// The ADR-0046 catalog byte cache's disk-tier counters handle (#97), or
    /// `None` when the catalog byte cache is RAM-only (no `--cache-dir`) or
    /// disabled. Rendered under `cache="catalog",tier="disk"`. Sourced from
    /// [`ravel_catalog::Catalog::byte_cache_disk_metrics`].
    pub catalog_cache_disk_metrics: Option<Arc<ravel_cache::CacheMetrics>>,
    /// The fetcher cache handle itself (as distinct from `cache_metrics`'s
    /// cumulative counters), for the live resident-bytes/entry-count gauges
    /// (#1170): `Cache::len`/`total_bytes` or, tiered, `TieredCache::ram_len`/
    /// `ram_total_bytes`/`disk_len`/`disk_total_bytes`. `None` under exactly
    /// the same condition as `cache_metrics` (`--disable-cache`).
    pub cache: Option<ravel_query::ReadCache>,
    /// The resolved startup byte ceiling for the fetcher cache
    /// (`ravel_server::store::build_cache`'s `cache_max_bytes`), rendered
    /// under `cache="fetch"` alongside the live residency gauges above so an
    /// operator can compare held-vs-budgeted. Meaningful only when
    /// `cache_metrics` is `Some`; the value is otherwise whatever
    /// `ServerConfig` resolved regardless of use.
    pub cache_max_bytes: u64,
    /// The resolved startup byte ceiling for the catalog byte cache
    /// (`ravel_server::query::build_catalog`'s `cache_max_bytes`, a SEPARATE
    /// ceiling from `cache_max_bytes` above), rendered under
    /// `cache="catalog"`. Meaningful only when `catalog_cache_metrics` is
    /// `Some`. The catalog cache's live residency is not rendered (see
    /// [`render_cache_residency_family`]'s doc comment for the scope gap).
    pub catalog_cache_max_bytes: u64,
    /// The one process-wide admission controller (ADR-0051), shared with every
    /// ingest path. Always present (built in every mode); in a mode that
    /// serves no ingest its `usage_snapshot` is simply empty, so the admission
    /// family renders its headers with no per-tenant samples.
    pub admission: Arc<AdmissionController>,
    /// The exporter-side running total of reaped snapshot keys, shared with
    /// [`crate::admission_reconcile`]'s loop. Always present; it stays at zero
    /// in a mode that spawns no reconciliation loop, which is the same reading
    /// as a loop that has reaped nothing. The last cycle's other figures come
    /// straight off `admission` above.
    pub reconcile_cycle: Arc<crate::admission_reconcile::ReconcileCycleMetrics>,
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
    /// Per-tenant normalization-layer decisions (ADR-0051 section 3, layer 3),
    /// rendered as the admission family's `skew` and `structural` reasons and
    /// its body-conversion counter. The same `Arc` every ingest surface holds.
    /// Always present; empty until a request is normalized. Folded by the same
    /// `--metrics-tenant-labels` gate.
    pub normalize_reject_metrics: Arc<crate::normalize_reject_metrics::NormalizeRejectMetrics>,
    /// The per-process metric-metadata cache (ADR-0085 decision 1), read at
    /// scrape time for its four `query_metadata_cache_*` counters. `Some` only
    /// in a request-serving mode that built one (`Mode::All`/`Mode::Query`);
    /// `None` otherwise leaves the whole family off the exposition.
    pub metadata_cache: Option<Arc<ravel_query::http::MetadataCache>>,
    /// The process's one query-audit pipeline (ADR-0062 decision 2b), read at
    /// scrape time for its write-failure counter. `Some` only in a mode that
    /// installed one (`Mode::All`/`Mode::Query`); `None` otherwise leaves the
    /// `ravel_audit_write_failures_total` family off the exposition.
    pub audit_pipeline: Option<Arc<ravel_maintain::AuditPipeline>>,
    /// The ADR-1170 decisions 1/3/4 process-wide memory accountant, the SAME
    /// instance installed on the `sql`-featured `SqlExecutor` via
    /// `SqlExecutor::with_process_memory_budget` (when `sql` is compiled in
    /// and the mode serves queries) so a tenant's SQL reservation and this
    /// gauge read one counter, not two independently drifting ones. Always
    /// present: `crate::start` builds it in every mode from
    /// `ServerConfig::process_memory_budget_bytes`, unconditionally of the
    /// `sql` feature, so the gauge family renders in every build.
    pub process_memory_budget: Arc<ravel_memory::MemoryBudget>,
    /// `ServerConfig::process_memory_budget_is_fallback`: whether the budget
    /// above was sized with the host's memory unknown, so the scrape handler
    /// clamps the exposed `ravel_memory_budget_bytes` gauge to `u64::MAX`
    /// rather than the raw near-miss remainder. See
    /// [`exposed_memory_budget_limit`].
    pub process_memory_budget_is_fallback: bool,
    /// Whether a catalog fold can run in this process at all, by either
    /// route: the background fold task, or the on-demand
    /// `POST /api/v1/admin/fold` route. Computed by
    /// [`crate::ServerConfig::folds_in_process`], which is where the two
    /// spawn/mount gates are stated, rather than re-derived from `mode`
    /// here.
    ///
    /// Gates the `ravel_catalog_fold_stamped_records_total` /
    /// `ravel_catalog_fold_stamped_entries_total` pair. Both routes fold
    /// into the same process-global totals, so either one opens the gate and
    /// only a process with neither omits the pair. Each single-route gate
    /// gets a different config wrong: following the mode alone leaves
    /// `--mode gateway --disable-fold`, which can fold by neither route,
    /// rendering both at a zero that never moves, reading as steady coverage
    /// instead of no fold at all; following the background task alone leaves
    /// `--mode all --disable-fold` rendering no family while an operator
    /// drives real coverage through the route it still mounts.
    pub can_fold: bool,
}

/// `GET /metrics`, mounted in every mode (ADR-0044 section 4). Reads only
/// in-memory atomics: no object-store call, unlike `/readyz`'s deliberate
/// avoidance of one for the same underlying reason (probe cost and blast
/// radius).
async fn metrics_handler(State(state): State<MetricsState>) -> impl IntoResponse {
    let store_snapshot = state.store_metrics.snapshot();

    let mut pipelines = Vec::new();
    if let Some(router) = &state.ingest_router {
        let mut pipeline = IngestPipelineSnapshot::from_metrics(router.metrics().snapshot());
        pipeline.shard_skew = router.metrics().shard_skew_by_shard();
        pipeline.active_shard_count = router.shard_count();
        pipelines.push(pipeline);
    }
    if let Some(router) = &state.log_ingest_router {
        let mut pipeline = IngestPipelineSnapshot::from_log_metrics(router.metrics().snapshot());
        pipeline.shard_skew = router.metrics().shard_skew_by_shard();
        pipeline.active_shard_count = router.shard_count();
        pipelines.push(pipeline);
    }
    if let Some(router) = &state.span_ingest_router {
        let mut pipeline = IngestPipelineSnapshot::from_span_metrics(router.metrics().snapshot());
        pipeline.shard_skew = router.metrics().shard_skew_by_shard();
        pipeline.active_shard_count = router.shard_count();
        pipelines.push(pipeline);
    }

    let catalog_snapshot = CatalogCountersSnapshot::from_catalog(state.catalog.as_ref());

    let maintain_snapshot =
        state
            .tenant_discovery
            .as_ref()
            .map(|metrics| MaintenanceDiscoverySnapshot {
                tenants_discovered: metrics.tenants_discovered(),
                tenants_maintained: metrics.tenants_maintained(),
                tenant_discovery_failures: metrics.discovery_failures(),
            });

    let maintain_safety_snapshot = state
        .maintenance_safety
        .as_ref()
        .map(|metrics| MaintenanceSafetySnapshot::from_metrics(metrics));

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
                last_cycle_completed_unix_ns: metrics.last_cycle_completed_unix_ns(),
                loop_panics_total: metrics.loop_panics_total(),
            });

    let scrub_snapshot = state.scrub.as_ref().map(|metrics| ScrubSnapshot {
        signals: crate::maintain::MAINTAINED_SIGNALS
            .iter()
            .map(|&signal| ScrubSignalSnapshot {
                signal,
                checksum_mismatch: ScrubLevelCounts {
                    l0: metrics.checksum_mismatch(signal, ScrubLevel::L0),
                    l1: metrics.checksum_mismatch(signal, ScrubLevel::L1),
                    rewrite: metrics.checksum_mismatch(signal, ScrubLevel::Rewrite),
                },
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
    let cache_disk_snapshot = state
        .cache_disk_metrics
        .as_ref()
        .map(|metrics| metrics.snapshot());
    let catalog_cache_snapshot = state
        .catalog_cache_metrics
        .as_ref()
        .map(|metrics| metrics.snapshot());
    let catalog_cache_disk_snapshot = state
        .catalog_cache_disk_metrics
        .as_ref()
        .map(|metrics| metrics.snapshot());

    // Live (not cumulative) resident bytes/entries for the fetcher cache's
    // tiers (#1170), read straight off the S3-FIFO structures via
    // `ravel_query::ReadCache`'s two variants rather than `cache_metrics`'s
    // cumulative counters, which cannot answer "what is resident right now."
    let cache_ram_residency = state.cache.as_ref().map(|cache| match cache {
        ravel_query::ReadCache::Ram(ram) => (ram.len(), ram.total_bytes()),
        ravel_query::ReadCache::Tiered(tiered) => (tiered.ram_len(), tiered.ram_total_bytes()),
    });
    let cache_disk_residency = state.cache.as_ref().and_then(|cache| match cache {
        ravel_query::ReadCache::Ram(_) => None,
        ravel_query::ReadCache::Tiered(tiered) => {
            Some((tiered.disk_len(), tiered.disk_total_bytes()))
        }
    });
    let cache_max_bytes = state
        .cache_metrics
        .is_some()
        .then_some(state.cache_max_bytes);
    let catalog_cache_max_bytes = state
        .catalog_cache_metrics
        .is_some()
        .then_some(state.catalog_cache_max_bytes);

    // This process's own allocator figures (#1170), read live via mallctl
    // (or named plainly when the allocator is not jemalloc) rather than
    // relying on whole-process RSS to say which subsystem grew.
    let allocator_stats = crate::mem_stats::read();

    // Read the admission counters at scrape time (a lock-and-copy, no
    // `.await`), like every other family, rather than baking a snapshot in at
    // construction.
    let admission_snapshot = AdmissionCountersSnapshot {
        usage: state.admission.usage_snapshot(),
        tenant_labels: state.metrics_tenant_labels,
        wire_bytes: state.ingest_byte_metrics.snapshot(),
        normalize_rejects: state.normalize_reject_metrics.snapshot(),
        reconcile_cycle: ReconcileCycleSnapshot::from_parts(
            state.admission.last_reconcile_cycle_stats(),
            state.reconcile_cycle.keys_reaped_total(),
        ),
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

    // The query-audit pipeline's failure counter, read at scrape time (an
    // atomic load). `None` in a mode that installed no pipeline.
    let audit_write_failures = state
        .audit_pipeline
        .as_ref()
        .map(|pipeline| pipeline.flush_failures());
    // The ADR-1170 process memory budget readings (atomic loads), like every
    // other family, rather than baking a snapshot in at construction.
    let memory_budget_snapshot = MemoryBudgetSnapshot {
        limit: exposed_memory_budget_limit(
            state.process_memory_budget.limit(),
            state.process_memory_budget_is_fallback,
        ),
        reserved: state.process_memory_budget.reserved(),
        handoff_overlap: state.process_memory_budget.handoff_overlap(),
    };

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
        cache_disk_snapshot.as_ref(),
        catalog_cache_snapshot.as_ref(),
        catalog_cache_disk_snapshot.as_ref(),
        &admission_snapshot,
        &query_rows,
        ingest_concurrency_shed_total,
        ingest_buffer_budget,
        distrib_snapshot.as_ref(),
        durable_auth_snapshot.as_ref(),
        &attribution,
        metadata_cache_snapshot.as_ref(),
        allocator_stats,
        cache_ram_residency,
        cache_disk_residency,
        cache_max_bytes,
        catalog_cache_max_bytes,
        audit_write_failures,
        memory_budget_snapshot,
        state.can_fold,
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
            // Above calls: the retry (billed) overhead #928 exposes.
            attempts: 9,
            latency_micros_buckets,
            latency_nanos_total: 900_000,
        };
        StoreMetricsSnapshot {
            get,
            ..StoreMetricsSnapshot::default()
        }
    }

    /// Pins the exact near-miss value `ravel_memory::MemoryBudget::limit()`
    /// reads on a fallback (unmeasured-host) build: `u64::MAX` minus the two
    /// hard cache carves, each `crate::config::DEFAULT_CACHE_MAX_BYTES`. This
    /// is the value the fallback gauge clamp must catch; asserting the exact
    /// number, not `> 0` or "large", so a change to `DEFAULT_CACHE_MAX_BYTES`
    /// or to the clamp logic surfaces here rather than only in production.
    #[test]
    fn memory_budget_gauge_clamps_fallback_near_miss_to_u64_max() {
        let raw_near_miss = u64::MAX - 2 * crate::config::DEFAULT_CACHE_MAX_BYTES;
        assert_eq!(raw_near_miss, 18_446_744_073_172_680_703);
        assert_eq!(exposed_memory_budget_limit(raw_near_miss, true), u64::MAX);
    }

    /// The non-fallback path exposes the raw limit unchanged: a derived
    /// budget's ceiling is meaningful and must not be clamped away.
    #[test]
    fn memory_budget_gauge_exposes_raw_limit_when_not_fallback() {
        assert_eq!(
            exposed_memory_budget_limit(21_045_339_751, false),
            21_045_339_751
        );
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            body.contains("ravel_store_calls_total{mode=\"all\",op=\"get\"} 7"),
            "missing calls sample:\n{body}"
        );
        assert!(
            body.contains("ravel_store_attempts_total{mode=\"all\",op=\"get\"} 9"),
            "missing attempts sample (billed requests incl. retries, #928):\n{body}"
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
        // eighth, added by ADR-0051 section 6 for the admission family;
        // `shard` is the ninth, added by ADR-1692 decision 2 for the
        // per-shard ingest skew family; `cache`
        // is the tenth, added to split the read-cache family into
        // the fetcher and catalog byte caches; `kind` is the eleventh, added by
        // ADR-0065 decision 4 for the RLOG merge-memory gauge; `tier` is the
        // twelfth, added by #97 to split each read cache into its RAM and
        // local-disk tiers; `allocator` and `stat` are the thirteenth and
        // fourteenth, added by #1170 for the process allocator gauges;
        // `outcome` is the fifteenth, added by #532 to split the alert
        // evaluation tick counter; `component` is the sixteenth, added by
        // ADR-1170 decision 4 for the process memory budget's reserved-bytes
        // gauge; `class` is the seventeenth, added by ADR-0071's admission
        // disjointness deliverable (issue #1722) to split the fragment
        // in-flight gauge and admission-wait counters into their `Pinned`
        // and `Resolve` classes; `carrier` is the eighteenth, added by
        // ADR-0873 decision 2 to split the declared-statistics drop tally
        // across its four carriers.
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
            Label::Shard(0),
            Label::ScrubLevel(ScrubLevel::L0),
            Label::Cache(CacheFamily::Fetch),
            Label::CacheTier(CacheTier::Ram),
            Label::MergeMemoryKind(MergeMemoryKind::Transient),
            Label::DeletedObjectKind(DeletedObjectKind::QuarantineReaped),
            Label::AlertOutcome(crate::alerting::AlertTickOutcome::Evaluated),
            Label::Allocator("jemalloc"),
            Label::AllocatorStat(AllocatorStat::Allocated),
            Label::MemoryComponent(MemoryComponent::Sql),
            Label::AdmissionClass(crate::distrib::AdmissionClass::Pinned),
            Label::StatCarrier(ravel_commit::declared_stats::StatCarrier::CommitRecord),
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
                Label::Shard(_) => "shard",
                Label::ScrubLevel(_) => "level",
                Label::Cache(_) => "cache",
                Label::CacheTier(_) => "tier",
                Label::MergeMemoryKind(_) => "kind",
                Label::DeletedObjectKind(_) => "kind",
                Label::AlertOutcome(_) => "outcome",
                Label::Allocator(_) => "allocator",
                Label::AllocatorStat(_) => "stat",
                Label::MemoryComponent(_) => "component",
                Label::AdmissionClass(_) => "class",
                Label::StatCarrier(_) => "carrier",
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
                // Shard (ADR-1692 decision 2) is the ninth key, for the
                // per-shard ingest skew family only; never combined with
                // tenant_hash.
                "shard",
                // ScrubLevel (issue #1686) reuses the `level` key, so the
                // allowlist of distinct keys is unchanged; two variants map to
                // it.
                "level",
                "cache",
                "tier",
                "kind",
                // DeletedObjectKind (issue #1729) reuses the `kind` key, so the
                // allowlist of distinct keys is unchanged; two variants map to
                // it.
                "kind",
                "outcome",
                "allocator",
                "stat",
                "component",
                "class",
                "carrier",
            ],
            "ADR-0044 section 4's allowlist plus ADR-0051 section 6's `reason` (also reused by \
             ADR-0059 section 2's scrub seal-divergence family), ADR-1692 decision 2's `shard` \
             for the per-shard ingest skew family, the `cache` label, #97's `tier` \
             label, ADR-0065 decision 4's `kind` (also reused by issue #1729's deleted-objects \
             family), #532's `outcome`, #1170's `allocator`/`stat`, ADR-1170 decision 4's \
             `component`, ADR-0071's `class` (issue #1722), ADR-0873 decision 2's `carrier`, and \
             issue #1686's `level` reuse by `ScrubLevel`"
        );
        assert_eq!(
            one_of_each.len(),
            21,
            "exactly 21 label variants, 18 distinct keys"
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            ..Default::default()
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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

    /// `shards_condemned` renders one sample per signal (metrics, logs, spans)
    /// with each pipeline's own driven value (issue #1299). The distinct counts
    /// catch cross-pipeline miswiring: a `from_log_metrics` that read the span
    /// snapshot's field, or a render that labelled all three the same, would
    /// fail here. A family that stopped rendering for any signal would silently disarm the
    /// `shards_condemned > 0` alert docs/guides/observability.md tells operators
    /// to set.
    #[test]
    fn shards_condemned_counters_render_for_every_signal() {
        let ingest = vec![
            IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot {
                shards_condemned: 2,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot {
                shards_condemned: 1,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_span_metrics(SpanIngestMetricsSnapshot {
                shards_condemned: 3,
                ..Default::default()
            }),
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            body.contains(
                "ravel_ingest_shards_condemned_total{mode=\"gateway\",signal=\"metrics\"} 2"
            ),
            "the metrics pipeline must render its driven condemned count"
        );
        assert!(
            body.contains(
                "ravel_ingest_shards_condemned_total{mode=\"gateway\",signal=\"logs\"} 1"
            ),
            "the logs pipeline must render its driven condemned count"
        );
        assert!(
            body.contains(
                "ravel_ingest_shards_condemned_total{mode=\"gateway\",signal=\"spans\"} 3"
            ),
            "the spans pipeline must render its driven condemned count"
        );
    }

    /// The two exemplar counters render one sample each, with the family's
    /// `{mode, signal}` labels and the driven values. Built by setting
    /// `exemplars` directly, so this test pins the rendering alone; the
    /// conversion from the ingest crate's snapshot is pinned by
    /// `exemplar_counters_survive_conversion_from_ingest_snapshot` below.
    #[test]
    fn exemplar_counters_render_under_the_ingest_family() {
        let mut pipeline = IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot::default());
        pipeline.exemplars = Some(ExemplarCounters {
            written_total: 7,
            dropped_total: 3,
        });
        let ingest = vec![
            pipeline,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        let written = "ravel_ingest_exemplars_written_total{mode=\"gateway\",signal=\"metrics\"} 7";
        let dropped = "ravel_ingest_exemplars_dropped_total{mode=\"gateway\",signal=\"metrics\"} 3";
        assert_eq!(
            body.matches(written).count(),
            1,
            "written counter must render exactly once:\n{body}"
        );
        assert_eq!(
            body.matches(dropped).count(),
            1,
            "dropped counter must render exactly once:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_ingest_exemplars_written_total counter"),
            "written counter must carry a TYPE line:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_ingest_exemplars_dropped_total counter"),
            "dropped counter must carry a TYPE line:\n{body}"
        );
        assert!(
            !body.contains("ravel_ingest_exemplars_written_total{mode=\"gateway\",signal=\"logs\""),
            "logs pipeline must render no exemplar sample:\n{body}"
        );
        assert!(
            !body
                .contains("ravel_ingest_exemplars_dropped_total{mode=\"gateway\",signal=\"spans\""),
            "spans pipeline must render no exemplar sample:\n{body}"
        );
    }

    /// The values travel from the ingest crate's counters to the rendered
    /// text: a constructor that leaves `exemplars` at zero (or `None`) while
    /// the source snapshot carries them fails here, not in the render test
    /// above.
    #[test]
    fn exemplar_counters_survive_conversion_from_ingest_snapshot() {
        let ingest = vec![IngestPipelineSnapshot::from_metrics(
            IngestMetricsSnapshot {
                exemplars_written_total: 7,
                exemplars_dropped_total: 3,
                ..Default::default()
            },
        )];
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            body.contains(
                "ravel_ingest_exemplars_written_total{mode=\"gateway\",signal=\"metrics\"} 7"
            ),
            "conversion must carry the written count, not zero:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_exemplars_dropped_total{mode=\"gateway\",signal=\"metrics\"} 3"
            ),
            "conversion must carry the dropped count, not zero:\n{body}"
        );
    }

    /// The three remaining flush figures render one sample each with the
    /// family's `{mode, signal}` labels and the right TYPE. `grace_extended`
    /// and `in_flight_flushes_total` are carried for every signal (all three
    /// ingest snapshots expose them), so logs and spans render them too; the
    /// metrics-only ADR-0067 adaptive-age figure renders no logs or spans
    /// sample. Built by setting the fields directly, so this pins the
    /// rendering alone; the conversion is pinned by
    /// `flush_counters_survive_conversion_from_ingest_snapshot` below.
    #[test]
    fn flush_counters_render_under_the_ingest_family() {
        let mut metrics = IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot::default());
        metrics.grace_extended_stale_flushes = 3;
        metrics.adaptive_flushes = Some(AdaptiveFlushCounters {
            flushes_by_age_adaptive: 7,
        });
        metrics.in_flight_flushes_total = 2;
        let ingest = vec![
            metrics,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        let adaptive =
            "ravel_ingest_flushes_by_age_adaptive_total{mode=\"gateway\",signal=\"metrics\"} 7";
        let grace = "ravel_ingest_grace_extended_stale_flushes_total{mode=\"gateway\",signal=\"metrics\"} 3";
        let in_flight = "ravel_ingest_in_flight_flushes{mode=\"gateway\",signal=\"metrics\"} 2";
        assert_eq!(
            body.matches(adaptive).count(),
            1,
            "adaptive-age counter must render exactly once:\n{body}"
        );
        assert_eq!(
            body.matches(grace).count(),
            1,
            "grace-extended counter must render exactly once:\n{body}"
        );
        assert_eq!(
            body.matches(in_flight).count(),
            1,
            "in-flight gauge must render exactly once:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_ingest_flushes_by_age_adaptive_total counter"),
            "adaptive-age figure must be a counter:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_ingest_grace_extended_stale_flushes_total counter"),
            "grace-extended figure must be a counter:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_ingest_in_flight_flushes gauge"),
            "in-flight figure must be a gauge:\n{body}"
        );
        // `grace_extended` is carried for every signal, so logs render it (at
        // zero here), proving it is not gated to the metrics pipeline.
        assert!(
            body.contains(
                "ravel_ingest_grace_extended_stale_flushes_total{mode=\"gateway\",signal=\"logs\"} 0"
            ),
            "logs pipeline must render the grace-extended counter:\n{body}"
        );
        // The adaptive-age figure is metrics-only, so logs and spans render
        // neither. (The in-flight gauge is NOT metrics-only; its logs/spans
        // rendering with real nonzero values is pinned by
        // `logs_only_process_renders_the_in_flight_gauge` and
        // `flush_counters_survive_conversion_from_ingest_snapshot` below,
        // since asserting a hardcoded zero here would pass vacuously.)
        assert!(
            !body.contains(
                "ravel_ingest_flushes_by_age_adaptive_total{mode=\"gateway\",signal=\"logs\""
            ),
            "logs pipeline must render no adaptive-age sample:\n{body}"
        );
        assert!(
            !body.contains(
                "ravel_ingest_flushes_by_age_adaptive_total{mode=\"gateway\",signal=\"spans\""
            ),
            "spans pipeline must render no adaptive-age sample:\n{body}"
        );
    }

    /// The values travel from the ingest crate's counters to the rendered text:
    /// a constructor that drops any of the three fields while the source
    /// snapshot carries them fails here, not in the render test above. The
    /// log and span in-flight totals are distinct nonzero values (3 and 5,
    /// not each other's and not the metrics pipeline's 2) so a constructor
    /// that mixed up which snapshot's field feeds which pipeline's sample
    /// would be caught here rather than passing on a shared placeholder.
    #[test]
    fn flush_counters_survive_conversion_from_ingest_snapshot() {
        let ingest = vec![
            IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot {
                flushes_by_age_adaptive: 7,
                grace_extended_stale_flushes: 3,
                in_flight_flushes_total: 2,
                flush_permit_wait_ns_total: 11_500_000_000,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot {
                in_flight_flushes_total: 3,
                flush_permit_wait_ns_total: 4_250_000_000,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_span_metrics(SpanIngestMetricsSnapshot {
                in_flight_flushes_total: 5,
                flush_permit_wait_ns_total: 6_750_000_000,
                ..Default::default()
            }),
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            body.contains(
                "ravel_ingest_flushes_by_age_adaptive_total{mode=\"gateway\",signal=\"metrics\"} 7"
            ),
            "conversion must carry the adaptive-age count, not zero:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_grace_extended_stale_flushes_total{mode=\"gateway\",signal=\"metrics\"} 3"
            ),
            "conversion must carry the grace-extended count, not zero:\n{body}"
        );
        assert!(
            body.contains("ravel_ingest_in_flight_flushes{mode=\"gateway\",signal=\"metrics\"} 2"),
            "conversion must carry the in-flight gauge, not zero:\n{body}"
        );
        assert!(
            body.contains("ravel_ingest_in_flight_flushes{mode=\"gateway\",signal=\"logs\"} 3"),
            "conversion must carry the log pipeline's own in-flight gauge, not the metrics \
             pipeline's:\n{body}"
        );
        assert!(
            body.contains("ravel_ingest_in_flight_flushes{mode=\"gateway\",signal=\"spans\"} 5"),
            "conversion must carry the span pipeline's own in-flight gauge, not another \
             pipeline's:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_ingest_flush_permit_wait_seconds_total counter\n"),
            "permit-wait family must declare its TYPE as a counter:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_flush_permit_wait_seconds_total{mode=\"gateway\",signal=\"metrics\"} 11.5\n"
            ),
            "conversion must carry the metrics pipeline's permit-wait total in seconds, not a \
             wrong divisor:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_flush_permit_wait_seconds_total{mode=\"gateway\",signal=\"logs\"} 4.25\n"
            ),
            "conversion must carry the log pipeline's own permit-wait total, not another \
             pipeline's or a wrong divisor:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_flush_permit_wait_seconds_total{mode=\"gateway\",signal=\"spans\"} 6.75\n"
            ),
            "conversion must carry the span pipeline's own permit-wait total, not another \
             pipeline's or a wrong divisor:\n{body}"
        );
    }

    /// Issue #1742: `ravel_ingest_flush_all_residue_tenants_total` must render
    /// for all three ingest signals, each carrying its OWN pipeline's count.
    /// The three values (4, 6, 9) are distinct from each other and from every
    /// other counter this fixture sets, so a constructor that exported the
    /// residue count for the metrics pipeline only (leaving logs/spans at the
    /// snapshot default of 0) or that mixed up which snapshot's field feeds
    /// which pipeline's sample fails here, not silently.
    #[test]
    fn flush_all_residue_renders_distinctly_for_every_signal() {
        let ingest = vec![
            IngestPipelineSnapshot::from_metrics(IngestMetricsSnapshot {
                flush_all_residue_tenants: 4,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot {
                flush_all_residue_tenants: 6,
                ..Default::default()
            }),
            IngestPipelineSnapshot::from_span_metrics(SpanIngestMetricsSnapshot {
                flush_all_residue_tenants: 9,
                ..Default::default()
            }),
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            body.contains("# TYPE ravel_ingest_flush_all_residue_tenants_total counter\n"),
            "residue family must declare its TYPE as a counter:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_flush_all_residue_tenants_total{mode=\"gateway\",signal=\"metrics\"} 4\n"
            ),
            "conversion must carry the metrics pipeline's own residue count:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_flush_all_residue_tenants_total{mode=\"gateway\",signal=\"logs\"} 6\n"
            ),
            "conversion must carry the log pipeline's own residue count, not the metrics \
             pipeline's or zero:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_flush_all_residue_tenants_total{mode=\"gateway\",signal=\"spans\"} 9\n"
            ),
            "conversion must carry the span pipeline's own residue count, not another \
             pipeline's or zero:\n{body}"
        );
    }

    /// Issue #1741: `ravel_ingest_in_flight_flushes` must render for a
    /// logs-only process. Before the fix, the render loop lived inside the
    /// `with_adaptive` block, which is empty whenever no pipeline sets
    /// `adaptive_flushes` -- true of a logs-only process, since that field is
    /// `Some` only for the metrics pipeline. A logs-only scrape therefore
    /// rendered neither the TYPE header nor any sample for this family.
    ///
    /// Prove-the-test: this exact assertion fails against the pre-fix
    /// renderer, where both the header and sample below are absent for a
    /// pipeline list that contains no metrics signal at all.
    #[test]
    fn logs_only_process_renders_the_in_flight_gauge() {
        let ingest = vec![IngestPipelineSnapshot::from_log_metrics(
            LogIngestMetricsSnapshot {
                in_flight_flushes_total: 4,
                flush_permit_wait_ns_total: 2_500_000_000,
                ..Default::default()
            },
        )];
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            body.contains("# TYPE ravel_ingest_in_flight_flushes gauge"),
            "a logs-only process must still declare the in-flight gauge's TYPE:\n{body}"
        );
        assert!(
            body.contains("ravel_ingest_in_flight_flushes{mode=\"gateway\",signal=\"logs\"} 4"),
            "a logs-only process must render its own in-flight gauge sample:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_ingest_flush_permit_wait_seconds_total counter\n"),
            "a logs-only process must still declare the permit-wait family's TYPE:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_ingest_flush_permit_wait_seconds_total{mode=\"gateway\",signal=\"logs\"} 2.5\n"
            ),
            "a logs-only process must render its own permit-wait sample:\n{body}"
        );
    }

    #[test]
    fn isolation_breach_counter_renders_at_metrics() {
        let catalog = CatalogCountersSnapshot {
            interlock_violations: 0,
            compaction_input_set_conflicts: 0,
            isolation_breaches: 5,
            ..Default::default()
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            body.contains("ravel_catalog_isolation_breach_total{mode=\"gateway\"} 5"),
            "isolation-breach counter must render its current value:\n{body}"
        );
    }

    /// The fold-liveness family reaches `/metrics` as one series per folded
    /// signal, each carrying the values it was driven with (issue #1306),
    /// following [`metadata_sink_counters_render_for_metrics_only`]: a family
    /// whose doc comments describe it but whose renderer emits nothing is
    /// exactly the defect that test exists for. The gauge is asserted as the
    /// exact rendered string, so a unit slip (nanoseconds emitted where
    /// seconds are declared) fails here rather than reading as a 54-year-old
    /// fold.
    ///
    /// The three signals are driven with three DIFFERENT values, and each is
    /// asserted against its own label set. A renderer that emitted one signal
    /// three times, or that paired the values with the wrong signals, passes
    /// an assertion that only checks that three series exist; it fails here.
    /// The series count is pinned exactly too, so a fourth signal cannot
    /// appear unnoticed.
    #[test]
    fn catalog_fold_liveness_family_renders_one_series_per_signal() {
        // 1_758_000_123_500_000_000 ns is 1758000123.5 s: a value with a
        // fractional second, so a renderer that truncated to whole seconds
        // would not match.
        let catalog = CatalogCountersSnapshot {
            fold: [
                CatalogFoldCounters {
                    signal: Signal::Metrics,
                    cycles: 41,
                    failures: 3,
                    last_success_unix_ns: 1_758_000_123_500_000_000,
                },
                CatalogFoldCounters {
                    signal: Signal::Logs,
                    cycles: 17,
                    failures: 0,
                    last_success_unix_ns: 1_700_000_000_500_000_000,
                },
                CatalogFoldCounters {
                    signal: Signal::Spans,
                    cycles: 0,
                    failures: 9,
                    last_success_unix_ns: 0,
                },
            ],
            ..Default::default()
        };
        let body = render(
            Mode::All,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            body.contains("# TYPE ravel_catalog_fold_cycles_total counter"),
            "the cycle counter must declare its type:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_catalog_fold_last_success_timestamp_seconds gauge"),
            "the liveness timestamp is a gauge, not a counter:\n{body}"
        );

        // Exact label sets and exact values, per signal. Every sample line the
        // renderer must emit is named here in full.
        for expected in [
            "ravel_catalog_fold_cycles_total{mode=\"all\",signal=\"metrics\"} 41",
            "ravel_catalog_fold_cycles_total{mode=\"all\",signal=\"logs\"} 17",
            "ravel_catalog_fold_cycles_total{mode=\"all\",signal=\"spans\"} 0",
            "ravel_catalog_fold_failures_total{mode=\"all\",signal=\"metrics\"} 3",
            "ravel_catalog_fold_failures_total{mode=\"all\",signal=\"logs\"} 0",
            "ravel_catalog_fold_failures_total{mode=\"all\",signal=\"spans\"} 9",
            "ravel_catalog_fold_last_success_timestamp_seconds{mode=\"all\",signal=\"metrics\"} 1758000123.5",
            "ravel_catalog_fold_last_success_timestamp_seconds{mode=\"all\",signal=\"logs\"} 1700000000.5",
            "ravel_catalog_fold_last_success_timestamp_seconds{mode=\"all\",signal=\"spans\"} 0",
        ] {
            assert_eq!(
                body.lines().filter(|line| *line == expected).count(),
                1,
                "expected exactly one `{expected}` sample line:\n{body}"
            );
        }

        // Exactly three series per family: an unlabelled process-global series
        // rendered alongside the per-signal ones, or a fourth signal, fails
        // here rather than at the alert.
        for family in [
            "ravel_catalog_fold_cycles_total",
            "ravel_catalog_fold_failures_total",
            "ravel_catalog_fold_last_success_timestamp_seconds",
        ] {
            let samples = body
                .lines()
                .filter(|line| {
                    // `{` catches a labelled series, ` ` an unlabelled one, so
                    // a process-global sample counts here instead of slipping
                    // past a labelled-only match. `#` lines are HELP and TYPE.
                    line.strip_prefix(family)
                        .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
                })
                .count();
            assert_eq!(
                samples, 3,
                "{family} must render one series per folded signal and no other:\n{body}"
            );
        }
    }

    /// The maintenance-liveness gauge and panic counter render on `/metrics`
    /// with the right TYPE lines and values (issue #1683), mirroring the fold
    /// liveness family test above. The timestamp is driven with a fractional
    /// second, so a renderer emitting nanoseconds where seconds are declared
    /// (or truncating to whole seconds) fails here rather than reading as a
    /// 54-year-old cycle at the alert.
    #[test]
    fn maintain_liveness_family_renders_the_gauge_and_panic_counter() {
        let snapshot = MaintenanceOwnershipSnapshot {
            workers_live: 1,
            units_owned: 4,
            units_stalled: 0,
            memo_warm_start_units: 0,
            full_sweep_passes_total: 7,
            // 1_758_000_123_500_000_000 ns is 1758000123.5 s.
            last_cycle_completed_unix_ns: 1_758_000_123_500_000_000,
            loop_panics_total: 2,
        };
        let mut out = String::new();
        render_maintain_ownership_family(&mut out, Mode::All, &snapshot);

        assert!(
            out.contains("# TYPE ravel_maintain_last_cycle_completed_timestamp_seconds gauge"),
            "the liveness timestamp is a gauge, not a counter:\n{out}"
        );
        assert!(
            out.contains("# TYPE ravel_maintain_loop_panics_total counter"),
            "the panic tally is a counter:\n{out}"
        );

        for expected in [
            "ravel_maintain_last_cycle_completed_timestamp_seconds{mode=\"all\"} 1758000123.5",
            "ravel_maintain_loop_panics_total{mode=\"all\"} 2",
            "ravel_maintain_full_sweep_passes_total{mode=\"all\"} 7",
        ] {
            assert_eq!(
                out.lines().filter(|line| *line == expected).count(),
                1,
                "expected exactly one `{expected}` sample line:\n{out}"
            );
        }
    }

    /// The alerting family renders every `AlertEvalReport` figure with the
    /// right TYPE lines and values (issue #532). Each counter gets a distinct
    /// value, so a renderer that wires two headers to one snapshot field fails
    /// here rather than reading as a healthy pipeline. The timestamp is driven
    /// with a fractional second, so a renderer emitting nanoseconds where
    /// seconds are declared reads as a 54-year-old tick at the alert and fails
    /// here instead.
    #[test]
    fn alert_family_renders_every_counter_and_the_liveness_gauge() {
        let snapshot = AlertSnapshot {
            rules_evaluated: 11,
            rules_failed: 2,
            records_written: 3,
            repeats_queued: 4,
            notifications_delivered: 5,
            notifications_failed: 6,
            ticks_evaluated: 7,
            ticks_lease_not_held: 8,
            ticks_lease_unavailable: 9,
            ticks_history_unavailable: 10,
            // 1_758_000_123_500_000_000 ns is 1758000123.5 s.
            last_tick_completed_unix_ns: 1_758_000_123_500_000_000,
        };
        let mut out = String::new();
        render_alert_family(&mut out, Mode::Query, &snapshot);

        for expected_type in [
            "# TYPE ravel_alert_rules_evaluated_total counter",
            "# TYPE ravel_alert_rules_failed_total counter",
            "# TYPE ravel_alert_records_written_total counter",
            "# TYPE ravel_alert_repeats_queued_total counter",
            "# TYPE ravel_alert_notifications_delivered_total counter",
            "# TYPE ravel_alert_notifications_failed_total counter",
            "# TYPE ravel_alert_ticks_total counter",
            "# TYPE ravel_alert_last_tick_completed_timestamp_seconds gauge",
        ] {
            assert_eq!(
                out.lines().filter(|line| *line == expected_type).count(),
                1,
                "expected exactly one `{expected_type}` line:\n{out}"
            );
        }

        for expected in [
            "ravel_alert_rules_evaluated_total{mode=\"query\"} 11",
            "ravel_alert_rules_failed_total{mode=\"query\"} 2",
            "ravel_alert_records_written_total{mode=\"query\"} 3",
            "ravel_alert_repeats_queued_total{mode=\"query\"} 4",
            "ravel_alert_notifications_delivered_total{mode=\"query\"} 5",
            "ravel_alert_notifications_failed_total{mode=\"query\"} 6",
            "ravel_alert_ticks_total{mode=\"query\",outcome=\"evaluated\"} 7",
            "ravel_alert_ticks_total{mode=\"query\",outcome=\"lease_not_held\"} 8",
            "ravel_alert_ticks_total{mode=\"query\",outcome=\"lease_unavailable\"} 9",
            "ravel_alert_ticks_total{mode=\"query\",outcome=\"history_unavailable\"} 10",
            "ravel_alert_last_tick_completed_timestamp_seconds{mode=\"query\"} 1758000123.5",
        ] {
            assert_eq!(
                out.lines().filter(|line| *line == expected).count(),
                1,
                "expected exactly one `{expected}` sample line:\n{out}"
            );
        }

        // Four outcomes and no fifth: an outcome that renders no series on a
        // scrape where it has not happened yet would make an alert rule's
        // `increase()` silently undefined until the first occurrence.
        let tick_samples = out
            .lines()
            .filter(|line| line.starts_with("ravel_alert_ticks_total{"))
            .count();
        assert_eq!(tick_samples, 4, "one series per outcome, always:\n{out}");
    }

    /// The scrape path reads the fold figures off the live `Catalog`, not off
    /// a zero placeholder. The two render tests above drive a struct literal,
    /// so on their own they would still pass if the handler never asked the
    /// catalog anything; this drives a real fold through a real catalog and
    /// asserts the snapshot carries what that fold left behind.
    #[tokio::test]
    async fn catalog_fold_snapshot_reads_the_live_catalog() {
        use ravel_catalog::{Catalog, CatalogConfig};
        use ravel_object_store::memory::MemoryStore;
        use ravel_types::{Signal, TenantId};

        /// The snapshot entry for one signal, by the signal it names rather
        /// than by its position in the array.
        fn entry(
            snapshot: &CatalogCountersSnapshot,
            signal: Signal,
        ) -> &super::CatalogFoldCounters {
            snapshot
                .fold
                .iter()
                .find(|fold| fold.signal == signal)
                .expect("every folded signal has a snapshot entry")
        }

        let store = std::sync::Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store, CatalogConfig::default()).expect("catalog");
        let before = CatalogCountersSnapshot::from_catalog(&catalog);
        assert_eq!(entry(&before, Signal::Metrics).cycles, 0);
        assert_eq!(entry(&before, Signal::Metrics).last_success_unix_ns, 0);

        // A fold over an empty store: the watermark advances over hours that
        // hold nothing, so it publishes a first HEAD naming no entry at all.
        // That is exactly the shape of a healthy cycle on a quiet tenant, and
        // the cycle counter has to count it.
        let tenant = TenantId::new("metrics-fold-snapshot").hash();
        let now_ns = 1_758_000_123_500_000_000;
        let report = catalog
            .fold(
                &tenant,
                Signal::Metrics,
                uuid::Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold over an empty store succeeds");
        assert_eq!(report.entry_count, 0, "an empty store folds no entry");

        let after = CatalogCountersSnapshot::from_catalog(&catalog);
        assert_eq!(entry(&after, Signal::Metrics).cycles, 1);
        assert_eq!(entry(&after, Signal::Metrics).failures, 0);
        assert_eq!(
            entry(&after, Signal::Metrics).last_success_unix_ns,
            now_ns,
            "the snapshot carries the catalog's own stamp, not a placeholder"
        );

        // The snapshot reads each signal's own slot: the two signals that were
        // not folded are still at zero, so a `from_catalog` that read one
        // signal's counters into every entry fails here.
        for untouched in [Signal::Logs, Signal::Spans] {
            assert_eq!(
                entry(&after, untouched).cycles,
                0,
                "{untouched:?} was not folded"
            );
            assert_eq!(
                entry(&after, untouched).last_success_unix_ns,
                0,
                "{untouched:?} was not folded"
            );
        }
    }

    /// A process that has never folded successfully still renders the gauge,
    /// at `0`, rather than omitting the series. An absent series cannot be
    /// alerted on with the `time() - gauge` expression the observability
    /// guide publishes: the alert would simply never fire for the process
    /// whose fold never worked at all.
    #[test]
    fn catalog_fold_liveness_gauge_renders_zero_before_any_successful_fold() {
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        for signal in ["metrics", "logs", "spans"] {
            assert!(
                body.contains(&format!(
                    "ravel_catalog_fold_last_success_timestamp_seconds{{mode=\"all\",signal=\"{signal}\"}} 0"
                )),
                "the {signal} liveness gauge must render its zero sentinel, not be omitted:\n{body}"
            );
            assert!(
                body.contains(&format!(
                    "ravel_catalog_fold_cycles_total{{mode=\"all\",signal=\"{signal}\"}} 0"
                )),
                "a zero {signal} cycle counter must render, not be omitted:\n{body}"
            );
            assert!(
                body.contains(&format!(
                    "ravel_catalog_fold_failures_total{{mode=\"all\",signal=\"{signal}\"}} 0"
                )),
                "a zero {signal} failure counter must render, not be omitted:\n{body}"
            );
        }
    }

    /// Every carrier is driven with a DIFFERENT drop count and asserted
    /// against its own label, so a renderer that paired the values with the
    /// wrong carriers, or emitted one carrier four times, fails here. The
    /// fold pair is likewise asserted at two different values, since a
    /// renderer that read the records total for both samples matches any
    /// assertion that only checks both names appear.
    #[test]
    fn declared_stats_family_renders_every_carrier_and_the_fold_coverage_pair() {
        use ravel_commit::declared_stats::StatCarrier;

        let mut out = String::new();
        render_declared_stats_family(
            &mut out,
            Mode::All,
            &[
                (StatCarrier::CommitRecord, 11),
                (StatCarrier::CompactionPart, 22),
                (StatCarrier::SnapshotEntry, 33),
                (StatCarrier::Cstat, 44),
            ],
            Some((907, 903)),
        );

        for (carrier, observed) in [
            ("commit-record", 11),
            ("compaction-part", 22),
            ("snapshot-entry", 33),
            ("cstat", 44),
        ] {
            let sample = format!(
                "ravel_declared_stats_drops_observed_total{{mode=\"all\",carrier=\"{carrier}\"}} {observed}\n"
            );
            assert_eq!(
                out.matches(&sample).count(),
                1,
                "the {carrier} drop tally must render exactly once at {observed}:\n{out}"
            );
        }
        assert_eq!(
            out.matches("ravel_declared_stats_drops_observed_total{")
                .count(),
            4,
            "exactly the four ADR-0873 carriers may render:\n{out}"
        );
        assert_eq!(
            out.matches("ravel_catalog_fold_stamped_records_total{mode=\"all\"} 907\n")
                .count(),
            1,
            "the stamped-records total must render exactly once at 907:\n{out}"
        );
        assert_eq!(
            out.matches("ravel_catalog_fold_stamped_entries_total{mode=\"all\"} 903\n")
                .count(),
            1,
            "the stamped-entries total must render exactly once at 903:\n{out}"
        );
        assert!(
            out.contains("# TYPE ravel_catalog_fold_stamped_records_total counter")
                && out.contains("# TYPE ravel_catalog_fold_stamped_entries_total counter")
                && out.contains("# TYPE ravel_declared_stats_drops_observed_total counter"),
            "each family must declare its type:\n{out}"
        );
    }

    /// A mode that runs no fold task omits the coverage pair entirely instead
    /// of rendering it at zero, while the drop tally still renders: the
    /// shortfall alert reads absence as "this process does not fold", which a
    /// zero sample would make indistinguishable from an idle fold. The drop
    /// tally is not gated the same way, because the compaction-part drops are
    /// observed in exactly the mode that folds nothing.
    #[test]
    fn declared_stats_fold_coverage_is_absent_when_the_mode_runs_no_fold() {
        use ravel_commit::declared_stats::StatCarrier;

        let mut out = String::new();
        render_declared_stats_family(
            &mut out,
            Mode::Maintain,
            &[
                (StatCarrier::CommitRecord, 0),
                (StatCarrier::CompactionPart, 5),
                (StatCarrier::SnapshotEntry, 0),
                (StatCarrier::Cstat, 0),
            ],
            None,
        );

        assert_eq!(
            out.matches(
                "ravel_declared_stats_drops_observed_total{mode=\"maintain\",carrier=\"compaction-part\"} 5\n"
            )
            .count(),
            1,
            "the compaction-part drop tally must still render in maintain mode:\n{out}"
        );
        assert!(
            !out.contains("ravel_catalog_fold_stamped_records_total"),
            "a mode that runs no fold must omit the stamped-records family:\n{out}"
        );
        assert!(
            !out.contains("ravel_catalog_fold_stamped_entries_total"),
            "a mode that runs no fold must omit the stamped-entries family:\n{out}"
        );
    }

    /// Drives the real [`render`] entry point (not [`render_declared_stats_family`]
    /// directly) with a mode that PERMITS folding (`Mode::All`) but
    /// `can_fold: false`: a mode check alone cannot tell a folding process
    /// from a non-folding one, so before this flag was threaded through,
    /// `render` derived the pair from `!matches!(mode, Mode::Maintain)` alone
    /// and rendered both families at zero forever, a false all-clear no alert
    /// on the shortfall would ever catch. Which configurations set the flag
    /// is [`crate::ServerConfig::folds_in_process`]'s question, pinned by the
    /// mode/flag table in `services/ravel-server/tests/metrics_endpoint.rs`
    /// and by `services/ravel-server/tests/fold_on_demand_stamp_coverage.rs`;
    /// this one pins what the renderer does with the answer.
    #[test]
    fn a_process_that_cannot_fold_renders_neither_stamp_coverage_family() {
        let body = render_with_can_fold(Mode::All, false);
        assert!(
            !body.contains("ravel_catalog_fold_stamped_records_total"),
            "a process that can fold by neither route must omit the \
             stamped-records family, not render it at zero:\n{body}"
        );
        assert!(
            !body.contains("ravel_catalog_fold_stamped_entries_total"),
            "a process that can fold by neither route must omit the \
             stamped-entries family, not render it at zero:\n{body}"
        );
    }

    /// The counterpart to the test above: a process that can fold renders
    /// both families exactly once, not omitted and not duplicated. The sample
    /// values themselves are read from the real process-global counters
    /// (shared with every other test in this binary), so this asserts
    /// presence and cardinality, not a value.
    #[test]
    fn a_process_that_can_fold_renders_both_stamp_coverage_families_exactly_once() {
        let body = render_with_can_fold(Mode::All, true);
        assert_eq!(
            body.matches("ravel_catalog_fold_stamped_records_total{mode=\"all\"} ")
                .count(),
            1,
            "a folding process must render the stamped-records sample exactly once:\n{body}"
        );
        assert_eq!(
            body.matches("ravel_catalog_fold_stamped_entries_total{mode=\"all\"} ")
                .count(),
            1,
            "a folding process must render the stamped-entries sample exactly once:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_catalog_fold_stamped_records_total counter")
                && body.contains("# TYPE ravel_catalog_fold_stamped_entries_total counter"),
            "both fold-coverage families must declare their type:\n{body}"
        );
    }

    /// Shared arg list for the two `can_fold` tests above: every other
    /// source left at its "not built in this mode" `None`/empty value, since
    /// only the fold-coverage pair is under test.
    fn render_with_can_fold(mode: Mode, can_fold: bool) -> String {
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            can_fold,
        )
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            false,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            false,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
        // This renderer runs no probe, so the liveness gauge reads its
        // unstamped 0 (issue #1728). For the causes a 0 reading has in a real
        // process, see the "What `0` means" section of
        // docs/guides/observability.md.
        assert!(
            body.contains("ravel_store_probe_last_run_timestamp_seconds{mode=\"all\"} 0"),
            "missing store-probe last-run gauge:\n{body}"
        );
        // All three carry the standard TYPE headers.
        assert!(body.contains("# TYPE ravel_store_reachable gauge"));
        assert!(body.contains("# TYPE ravel_store_probe_failures_total counter"));
        assert!(body.contains("# TYPE ravel_store_probe_last_run_timestamp_seconds gauge"));
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            false,
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
            objects_deleted_quarantine_reaped: 10,
            objects_deleted_superseded_records_deleted: 11,
            objects_deleted_superseded_data_deleted: 12,
            objects_deleted_unreferenced_parts_deleted: 13,
            signals: vec![
                MaintenanceSafetySignalSnapshot {
                    signal: Signal::Metrics,
                    conservation_aborts: 1,
                    orphan_breaker_trips: 2,
                    orphans_withheld: 7,
                    orphans_present: 9,
                    orphans_quarantined: 4,
                    orphans_quarantine_refused: 5,
                    quarantine_reaped: 6,
                    l0_records_pending: 8,
                },
                MaintenanceSafetySignalSnapshot {
                    signal: Signal::Logs,
                    conservation_aborts: 0,
                    orphan_breaker_trips: 0,
                    orphans_withheld: 0,
                    orphans_present: 0,
                    orphans_quarantined: 0,
                    orphans_quarantine_refused: 0,
                    quarantine_reaped: 0,
                    l0_records_pending: 0,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            false,
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
        assert!(
            body.contains(
                "ravel_maintain_l0_records_pending{mode=\"maintain\",signal=\"metrics\"} 8"
            ),
            "missing l0_records_pending sample:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_maintain_l0_records_pending gauge"),
            "l0_records_pending must carry a gauge TYPE header:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_maintain_objects_deleted_total counter"),
            "objects_deleted_total must carry a counter TYPE header:\n{body}"
        );
        for (kind, value) in [
            ("quarantine_reaped", 10),
            ("superseded_records_deleted", 11),
            ("superseded_data_deleted", 12),
            ("unreferenced_parts_deleted", 13),
        ] {
            let sample = format!(
                "ravel_maintain_objects_deleted_total{{mode=\"maintain\",kind=\"{kind}\"}} {value}"
            );
            assert!(
                body.contains(&sample),
                "missing objects_deleted_total sample {sample}:\n{body}"
            );
        }
    }

    /// The ADR-0071 distributed read fan-out family renders under
    /// the new `ravel_distrib_*` names, and every one of its series carries only
    /// the closed `{mode}` label, except the per-class fragment in-flight gauge
    /// and admission-wait counter, which also carry `class` (ADR-0044 section 4;
    /// `class` added for the fragment admission classes, issue #1722): no per-shard, per-worker, or per-tenant label. Also asserts the
    /// family is absent entirely when the snapshot is `None`, matching the "off
    /// unless --distributed-query" wiring.
    #[test]
    fn render_includes_distrib_family_with_only_allowlisted_labels() {
        let mut buckets = [0u64; LATENCY_BUCKET_COUNT];
        buckets[0] = 5;
        buckets[2] = 3;
        let snapshot = DistribSnapshot {
            fragment_requests_total: 11,
            fragment_auth_failures_total: 2,
            fragment_inflight_by_class: [
                (crate::distrib::AdmissionClass::Pinned, 1),
                (crate::distrib::AdmissionClass::Resolve, 4),
            ],
            fragment_admission_waits_by_class: [
                (crate::distrib::AdmissionClass::Pinned, 0),
                (crate::distrib::AdmissionClass::Resolve, 9),
            ],
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        for expected in [
            "ravel_distrib_fragment_requests_total{mode=\"query\"} 11",
            "ravel_distrib_fragment_auth_failures_total{mode=\"query\"} 2",
            "ravel_distrib_fragment_inflight{mode=\"query\",class=\"pinned\"} 1",
            "ravel_distrib_fragment_inflight{mode=\"query\",class=\"resolve\"} 4",
            "ravel_distrib_fragment_admission_waits_total{mode=\"query\",class=\"pinned\"} 0",
            "ravel_distrib_fragment_admission_waits_total{mode=\"query\",class=\"resolve\"} 9",
            "ravel_distrib_slices_local_total{mode=\"query\"} 7",
            "ravel_distrib_slices_remote_total{mode=\"query\"} 4",
            "ravel_distrib_slices_redispatched_total{mode=\"query\"} 2",
            "ravel_distrib_slices_fallback_total{mode=\"query\"} 1",
        ] {
            assert!(body.contains(expected), "missing `{expected}`:\n{body}");
        }
        assert_eq!(
            body.matches("ravel_distrib_fragment_inflight{mode=")
                .count(),
            2,
            "exactly one in-flight sample per admission class:\n{body}"
        );
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
        // (plus `le` on histogram buckets, and `class` on the per-admission-class
        // fragment in-flight gauge and admission-wait counter, ADR-0071 issue
        // #1722); no other label leaks in.
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
                    key == "mode" || key == "le" || key == "class",
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
                    checksum_mismatch: ScrubLevelCounts {
                        l0: 2,
                        l1: 5,
                        rewrite: 0,
                    },
                    postings_disagreement: 1,
                    seal_divergence_missing: 3,
                    seal_divergence_mismatched: 4,
                    cursor_position: 0.5,
                },
                ScrubSignalSnapshot {
                    signal: Signal::Logs,
                    checksum_mismatch: ScrubLevelCounts::default(),
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            false,
        );

        assert!(
            body.contains(
                "ravel_scrub_checksum_mismatch_total{mode=\"maintain\",signal=\"metrics\",level=\"l0\"} 2"
            ),
            "missing checksum_mismatch l0 sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_scrub_checksum_mismatch_total{mode=\"maintain\",signal=\"metrics\",level=\"l1\"} 5"
            ),
            "missing checksum_mismatch l1 sample:\n{body}"
        );
        assert!(
            body.contains(
                "ravel_scrub_checksum_mismatch_total{mode=\"maintain\",signal=\"metrics\",level=\"rewrite\"} 0"
            ),
            "missing checksum_mismatch rewrite sample:\n{body}"
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
        // Zero-valued signal (logs) still renders every level: zero-is-not-absence.
        assert!(
            body.contains(
                "ravel_scrub_checksum_mismatch_total{mode=\"maintain\",signal=\"logs\",level=\"l0\"} 0"
            ),
            "a zero-valued signal must still render every level:\n{body}"
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );
        assert!(
            !body.contains("ravel_scrub_"),
            "no scrub series should render when the snapshot is absent:\n{body}"
        );
    }

    /// A real cycle's figures reach `/metrics`, not just hand-built ones: run
    /// `reconcile_once`, read the copy it published on the controller, and
    /// render that. The duration is deterministic because the cycle measures
    /// itself on the controller's injected clock, so a fixed step gives a
    /// fixed rendered value and nothing here reads wall time.
    ///
    /// `render_includes_reconcile_cycle_series` below asserts the other three
    /// figures against distinct values; this one pins that the chain from the
    /// producing path to the exposition is connected at all.
    #[tokio::test]
    async fn reconcile_cycle_figures_reach_metrics_from_the_controller() {
        use ravel_ingest::{AdmissionController, AdmissionLimits, Clock};
        use ravel_object_store::memory::MemoryStore;
        use std::sync::atomic::{AtomicI64, Ordering};

        /// Advances a fixed step on every reading and returns the value before
        /// the step, so a cycle that takes one end stamp measures exactly one
        /// step. No wall-clock read enters the figure.
        struct SteppingClock {
            now_ns: AtomicI64,
            step_ns: i64,
        }

        impl Clock for SteppingClock {
            fn now_ns(&self) -> i64 {
                self.now_ns.fetch_add(self.step_ns, Ordering::Relaxed)
            }
        }

        const STEP_NS: i64 = 250_000;
        let clock = std::sync::Arc::new(SteppingClock {
            now_ns: AtomicI64::new(1_700_000_000_000_000_000),
            step_ns: STEP_NS,
        });
        let controller = AdmissionController::new(clock.clone(), AdmissionLimits::default());
        let store = MemoryStore::new();

        let start_ns = clock.now_ns();
        let stats = ravel_ingest::reconcile_once(
            &controller,
            &store,
            std::time::Duration::from_secs(30),
            start_ns,
        )
        .await;
        let cycle_metrics = crate::admission_reconcile::ReconcileCycleMetrics::default();
        cycle_metrics.record_cycle(&stats);

        let admission = AdmissionCountersSnapshot {
            reconcile_cycle: ReconcileCycleSnapshot::from_parts(
                controller.last_reconcile_cycle_stats(),
                cycle_metrics.keys_reaped_total(),
            ),
            ..Default::default()
        };
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
            None,
            None,
            &admission,
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert_eq!(
            stats.cycle_duration_ns, STEP_NS,
            "the cycle measures one step of the injected clock"
        );
        assert!(
            body.contains(
                "ravel_admission_reconciliation_cycle_duration_seconds{mode=\"all\"} 0.00025"
            ),
            "the cycle's own duration must render, in seconds:\n{body}"
        );
    }

    /// The reconciliation cycle figures reach `/metrics` (ADR-0057). Every
    /// value asserted, and each field given a different one: a renderer that
    /// read the wrong field, or wrote a constant, passes a presence-only test
    /// and fails this one.
    ///
    /// `keys_reaped_total` is the accumulated total from
    /// `admission_reconcile::ReconcileCycleMetrics`, not the last cycle's
    /// count, which is why it comes in through `from_parts` separately from
    /// the controller's per-cycle copy.
    #[test]
    fn render_includes_reconcile_cycle_series() {
        let admission = AdmissionCountersSnapshot {
            reconcile_cycle: ReconcileCycleSnapshot::from_parts(
                ravel_ingest::ReconcileCycleStats {
                    cycle_duration_ns: 1_250_000_000,
                    siblings_observed: 4,
                    stale_keys_skipped: 6,
                    keys_reaped: 2,
                },
                9,
            ),
            ..Default::default()
        };
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
            None,
            None,
            &admission,
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        for (header, sample) in [
            (
                "# TYPE ravel_admission_reconciliation_cycle_duration_seconds gauge",
                "ravel_admission_reconciliation_cycle_duration_seconds{mode=\"all\"} 1.25",
            ),
            (
                "# TYPE ravel_admission_reconciliation_siblings_observed gauge",
                "ravel_admission_reconciliation_siblings_observed{mode=\"all\"} 4",
            ),
            (
                "# TYPE ravel_admission_reconciliation_stale_keys_skipped gauge",
                "ravel_admission_reconciliation_stale_keys_skipped{mode=\"all\"} 6",
            ),
            (
                "# TYPE ravel_admission_reconciliation_keys_reaped_total counter",
                "ravel_admission_reconciliation_keys_reaped_total{mode=\"all\"} 9",
            ),
        ] {
            assert!(body.contains(header), "missing TYPE line {header}:\n{body}");
            assert!(body.contains(sample), "missing sample {sample}:\n{body}");
        }
    }

    /// The quarantine leg of orphan GC reaches `/metrics` (ADR-0058
    /// amendment), through the same `MaintenanceSafetyMetrics` the server
    /// already feeds every `SweepReport` into. The report is what a sweep pass
    /// returns, so this covers the whole chain the figures were stopping one
    /// step short of: `SweepReport` to counter to rendered sample.
    ///
    /// Three distinct values, all asserted: a renderer reading the wrong field
    /// of the snapshot renders a plausible number and fails here.
    #[test]
    fn render_includes_orphan_quarantine_series() {
        let safety = crate::maintain::MaintenanceSafetyMetrics::default();
        safety.record_sweep(
            Signal::Metrics,
            &ravel_maintain::SweepReport {
                orphans_deleted: 5,
                orphans_quarantined: 5,
                orphans_quarantine_refused: 2,
                quarantine_reaped: 3,
                ..Default::default()
            },
        );
        let snapshot = MaintenanceSafetySnapshot::from_metrics(&safety);
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            false,
        );
        for (header, sample) in [
            (
                "# TYPE ravel_maintain_orphans_quarantined_total counter",
                "ravel_maintain_orphans_quarantined_total{mode=\"maintain\",signal=\"metrics\"} 5",
            ),
            (
                "# TYPE ravel_maintain_orphans_quarantine_refused_total counter",
                "ravel_maintain_orphans_quarantine_refused_total{mode=\"maintain\",\
                 signal=\"metrics\"} 2",
            ),
            (
                "# TYPE ravel_maintain_quarantine_reaped_total counter",
                "ravel_maintain_quarantine_reaped_total{mode=\"maintain\",signal=\"metrics\"} 3",
            ),
        ] {
            assert!(body.contains(header), "missing TYPE line {header}:\n{body}");
            assert!(body.contains(sample), "missing sample {sample}:\n{body}");
        }

        // A signal the pass never touched still renders, at zero, like every
        // other series in this family.
        assert!(
            body.contains(
                "ravel_maintain_orphans_quarantined_total{mode=\"maintain\",\
                           signal=\"logs\"} 0"
            ),
            "an untouched signal must still render:\n{body}"
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
            objects_deleted_quarantine_reaped: 1,
            objects_deleted_superseded_records_deleted: 1,
            objects_deleted_superseded_data_deleted: 1,
            objects_deleted_unreferenced_parts_deleted: 1,
            signals: vec![MaintenanceSafetySignalSnapshot {
                signal: Signal::Metrics,
                conservation_aborts: 1,
                orphan_breaker_trips: 1,
                orphans_withheld: 1,
                orphans_present: 1,
                orphans_quarantined: 1,
                orphans_quarantine_refused: 1,
                quarantine_reaped: 1,
                l0_records_pending: 1,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            false,
        );

        for line in body.lines() {
            let expected_keys =
                if line.starts_with("ravel_maintain_legal_hold_refresh_failures_total") {
                    vec!["mode"]
                } else if line.starts_with("ravel_maintain_conservation_aborts_total")
                    || line.starts_with("ravel_maintain_orphan_breaker_tripped_total")
                    || line.starts_with("ravel_maintain_orphans_withheld")
                    || line.starts_with("ravel_maintain_orphans_present")
                    || line.starts_with("ravel_maintain_orphans_quarantined_total")
                    || line.starts_with("ravel_maintain_orphans_quarantine_refused_total")
                    || line.starts_with("ravel_maintain_quarantine_reaped_total")
                    || line.starts_with("ravel_maintain_l0_records_pending")
                {
                    vec!["mode", "signal"]
                } else if line.starts_with("ravel_maintain_objects_deleted_total") {
                    vec!["mode", "kind"]
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
            None,
            Some(&catalog),
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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

    /// #97: with NO disk tier configured for either cache, the `ravel_cache_*`
    /// family renders byte-for-byte the pre-#97 output -- every sample carries
    /// only `mode=`/`cache=`, and no `tier=` label appears anywhere. Pinned as a
    /// byte-for-byte fixture so an accidental unconditional `tier=` label (or any
    /// other shape drift) breaks this compile-independent contract loudly.
    #[test]
    fn no_disk_tier_renders_pre_task_output_byte_for_byte() {
        let fetch = CacheMetricsSnapshot {
            hits: 10,
            misses: 4,
            bytes_served: 2048,
            bytes_admitted: 1024,
            evictions: 2,
            disk_errors_degraded_to_misses: 3,
            disk_entries_expired_max_age: 7,
            ..Default::default()
        };
        let catalog = CacheMetricsSnapshot {
            hits: 70,
            misses: 5,
            bytes_served: 4096,
            bytes_admitted: 8192,
            evictions: 6,
            ..Default::default()
        };
        let mut out = String::new();
        // No disk snapshot for either family: the pre-#97 shape.
        render_cache_family(
            &mut out,
            Mode::Gateway,
            Some(&fetch),
            None,
            Some(&catalog),
            None,
        );

        const EXPECTED: &str = "\
# HELP ravel_cache_hits_total Read-cache lookups served from the cache.
# TYPE ravel_cache_hits_total counter
ravel_cache_hits_total{mode=\"gateway\",cache=\"fetch\"} 10
ravel_cache_hits_total{mode=\"gateway\",cache=\"catalog\"} 70
# HELP ravel_cache_misses_total Read-cache lookups not found in the cache.
# TYPE ravel_cache_misses_total counter
ravel_cache_misses_total{mode=\"gateway\",cache=\"fetch\"} 4
ravel_cache_misses_total{mode=\"gateway\",cache=\"catalog\"} 5
# HELP ravel_cache_bytes_served_total Bytes served from the cache on a hit.
# TYPE ravel_cache_bytes_served_total counter
ravel_cache_bytes_served_total{mode=\"gateway\",cache=\"fetch\"} 2048
ravel_cache_bytes_served_total{mode=\"gateway\",cache=\"catalog\"} 4096
# HELP ravel_cache_bytes_admitted_total Bytes admitted into the cache after a miss.
# TYPE ravel_cache_bytes_admitted_total counter
ravel_cache_bytes_admitted_total{mode=\"gateway\",cache=\"fetch\"} 1024
ravel_cache_bytes_admitted_total{mode=\"gateway\",cache=\"catalog\"} 8192
# HELP ravel_cache_evictions_total Entries evicted from the read cache by its S3-FIFO policy.
# TYPE ravel_cache_evictions_total counter
ravel_cache_evictions_total{mode=\"gateway\",cache=\"fetch\"} 2
ravel_cache_evictions_total{mode=\"gateway\",cache=\"catalog\"} 6
# HELP ravel_cache_disk_errors_degraded_to_misses_total Disk-tier reads that found an entry at its canonical path but discarded it (short read, bad header, key mismatch, or a failed crc32c check) rather than a clean miss. Nonzero here means the disk tier is unhealthy, not merely cold.
# TYPE ravel_cache_disk_errors_degraded_to_misses_total counter
ravel_cache_disk_errors_degraded_to_misses_total{mode=\"gateway\",cache=\"fetch\"} 3
ravel_cache_disk_errors_degraded_to_misses_total{mode=\"gateway\",cache=\"catalog\"} 0
# HELP ravel_cache_disk_entries_expired_max_age_total Disk-tier entries dropped because their stamped write time aged past the configured max-age (ADR-0064), by a read, the startup scan, or the periodic sweep. This is an expiry, not corruption: the bytes of an erased subject are physically removed from local disk within the max-age bound.
# TYPE ravel_cache_disk_entries_expired_max_age_total counter
ravel_cache_disk_entries_expired_max_age_total{mode=\"gateway\",cache=\"fetch\"} 7
ravel_cache_disk_entries_expired_max_age_total{mode=\"gateway\",cache=\"catalog\"} 0
";
        assert_eq!(
            out, EXPECTED,
            "with no disk tier the cache family must render byte-for-byte its pre-#97 shape, with \
             no tier= label anywhere"
        );
        assert!(
            !out.contains("tier="),
            "no --cache-dir means no tier= label on any cache sample:\n{out}"
        );
    }

    /// #97: once a disk tier exists for a family, that family's RAM sample gains
    /// `tier="ram"` and its disk sample carries `tier="disk"`, the two orthogonal
    /// labels splitting the family. The counterpart to the pinned no-disk fixture
    /// above: this is the shape the `tier=` label is allowed to appear in.
    #[test]
    fn disk_tier_present_splits_the_family_by_tier_label() {
        let fetch = CacheMetricsSnapshot {
            hits: 10,
            ..Default::default()
        };
        let fetch_disk = CacheMetricsSnapshot {
            hits: 3,
            ..Default::default()
        };
        let mut out = String::new();
        render_cache_family(
            &mut out,
            Mode::Gateway,
            Some(&fetch),
            Some(&fetch_disk),
            None,
            None,
        );
        assert!(
            out.contains(
                "ravel_cache_hits_total{mode=\"gateway\",cache=\"fetch\",tier=\"ram\"} 10"
            ),
            "the RAM sample must carry tier=ram when a disk tier coexists:\n{out}"
        );
        assert!(
            out.contains(
                "ravel_cache_hits_total{mode=\"gateway\",cache=\"fetch\",tier=\"disk\"} 3"
            ),
            "the disk sample must carry tier=disk:\n{out}"
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
            None,
            Some(&catalog),
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert!(
            !body.contains("ravel_cache_"),
            "a server run with --disable-cache must not render any cache \
             family at all, neither fetch nor catalog:\n{body}"
        );
    }

    /// Exact figures (#1170), not `> 0`: on a jemalloc build the three stats
    /// render as three distinct samples under their own `stat=` label, with
    /// the values passed straight through, and the allocator-name info series
    /// does not also render.
    #[test]
    fn allocator_family_renders_jemalloc_exact_figures() {
        let mut out = String::new();
        render_allocator_family(
            &mut out,
            Mode::Query,
            crate::mem_stats::AllocatorStats::Jemalloc {
                allocated: 111,
                active: 222,
                resident: 333,
            },
        );
        assert!(out.contains(
            "ravel_process_allocator_bytes{mode=\"query\",allocator=\"jemalloc\",stat=\"allocated\"} 111"
        ));
        assert!(out.contains(
            "ravel_process_allocator_bytes{mode=\"query\",allocator=\"jemalloc\",stat=\"active\"} 222"
        ));
        assert!(out.contains(
            "ravel_process_allocator_bytes{mode=\"query\",allocator=\"jemalloc\",stat=\"resident\"} 333"
        ));
        assert!(
            !out.contains("ravel_process_allocator_info"),
            "a jemalloc build renders the byte figures, never the named-allocator \
             fallback series:\n{out}"
        );
    }

    /// On a non-jemalloc build there are no allocated/active/resident figures
    /// to report: the fallback is a single 1-valued info series naming the
    /// allocator, never a stand-in zero for stats jemalloc alone exposes.
    #[test]
    fn allocator_family_renders_named_allocator_when_not_jemalloc() {
        let mut out = String::new();
        render_allocator_family(
            &mut out,
            Mode::Query,
            crate::mem_stats::AllocatorStats::Other { name: "system" },
        );
        assert!(
            out.contains("ravel_process_allocator_info{mode=\"query\",allocator=\"system\"} 1")
        );
        assert!(
            !out.contains("ravel_process_allocator_bytes"),
            "a non-jemalloc build must never report the jemalloc-only byte \
             figures:\n{out}"
        );
    }

    /// An empty fetcher cache (no tiered disk cache attached) must report
    /// valid zeros, not omit the family: a reader scraping right after
    /// startup needs to see `0`, not a missing series it cannot distinguish
    /// from a scrape error.
    #[test]
    fn cache_residency_family_reports_valid_zeros_on_empty_cache() {
        let mut out = String::new();
        render_cache_residency_family(&mut out, Mode::Query, Some((0, 0)), None, Some(1000), None);
        assert!(out.contains("ravel_cache_resident_entries{mode=\"query\",cache=\"fetch\"} 0"));
        assert!(out.contains("ravel_cache_resident_bytes{mode=\"query\",cache=\"fetch\"} 0"));
        assert!(out.contains("ravel_cache_max_bytes{mode=\"query\",cache=\"fetch\"} 1000"));
        assert!(
            !out.contains("tier="),
            "a RAM-only cache (no disk tier) must render without a tier label:\n{out}"
        );
    }

    /// A tiered cache with both tiers populated renders each tier's exact
    /// entry count and byte total under its own `tier=` label, plus each
    /// cache's resolved ceiling -- the held-vs-budgeted comparison this
    /// family exists for.
    #[test]
    fn cache_residency_family_reports_exact_figures_for_both_tiers() {
        let mut out = String::new();
        render_cache_residency_family(
            &mut out,
            Mode::Query,
            Some((3, 300)),
            Some((5, 500)),
            Some(1_000),
            Some(2_000),
        );
        assert!(out.contains(
            "ravel_cache_resident_entries{mode=\"query\",cache=\"fetch\",tier=\"ram\"} 3"
        ));
        assert!(out.contains(
            "ravel_cache_resident_bytes{mode=\"query\",cache=\"fetch\",tier=\"ram\"} 300"
        ));
        assert!(out.contains(
            "ravel_cache_resident_entries{mode=\"query\",cache=\"fetch\",tier=\"disk\"} 5"
        ));
        assert!(out.contains(
            "ravel_cache_resident_bytes{mode=\"query\",cache=\"fetch\",tier=\"disk\"} 500"
        ));
        assert!(out.contains("ravel_cache_max_bytes{mode=\"query\",cache=\"fetch\"} 1000"));
        assert!(out.contains("ravel_cache_max_bytes{mode=\"query\",cache=\"catalog\"} 2000"));
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
            normalize_rejects: Vec::new(),
            reconcile_cycle: ReconcileCycleSnapshot::default(),
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            normalize_rejects: Vec::new(),
            reconcile_cycle: ReconcileCycleSnapshot::default(),
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            normalize_rejects: Vec::new(),
            reconcile_cycle: ReconcileCycleSnapshot::default(),
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );
        assert!(
            body.contains(&format!(
                "ravel_admission_rejected_total{{mode=\"gateway\",tenant_hash=\"{hash}\",\
                 signal=\"metrics\",reason=\"clock\"}} 7"
            )),
            "clock rejection must render distinctly:\n{body}"
        );
    }

    /// Normalization's own rejections render under the reserved `skew` and
    /// `structural` reasons of the same family, and converted structured
    /// bodies render as their own family rather than as a reason. The tenant
    /// here has no admission usage row at all, so this also pins that a
    /// normalize-only (tenant, signal) still renders a full row.
    ///
    /// `ravel_ingest_resource_attrs_dropped_total` is metrics-only (OTAP and
    /// logs/traces never call the recorder), so its row is pinned on a
    /// `Signal::Metrics` entry, a state the renderer can actually produce,
    /// rather than on the `Signal::Logs` row above: a nonzero count on a
    /// signal the counter never renders would pass vacuously.
    #[test]
    fn admission_family_renders_the_skew_and_structural_reasons() {
        let hash = ravel_types::TenantId::new("noisy").hash();
        let snapshot = AdmissionCountersSnapshot {
            usage: Vec::new(),
            tenant_labels: true,
            wire_bytes: Vec::new(),
            normalize_rejects: vec![
                crate::normalize_reject_metrics::TenantNormalizeRejects {
                    tenant_hash: hash,
                    signal: Signal::Logs,
                    skew_total: 2,
                    structural_total: 3,
                    body_conversions_total: 4,
                    resource_attrs_dropped_total: 0,
                },
                crate::normalize_reject_metrics::TenantNormalizeRejects {
                    tenant_hash: hash,
                    signal: Signal::Metrics,
                    skew_total: 0,
                    structural_total: 0,
                    body_conversions_total: 0,
                    resource_attrs_dropped_total: 5,
                },
            ],
            reconcile_cycle: ReconcileCycleSnapshot::default(),
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );
        let hash = hash.to_hex();
        assert!(
            body.contains(&format!(
                "ravel_admission_rejected_total{{mode=\"gateway\",tenant_hash=\"{hash}\",\
                 signal=\"logs\",reason=\"skew\"}} 2"
            )),
            "event-time rejections must render under reason=skew:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "ravel_admission_rejected_total{{mode=\"gateway\",tenant_hash=\"{hash}\",\
                 signal=\"logs\",reason=\"structural\"}} 3"
            )),
            "structural rejections must render under reason=structural:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "ravel_ingest_body_conversions_total{{mode=\"gateway\",tenant_hash=\"{hash}\",\
                 signal=\"logs\"}} 4"
            )),
            "converted bodies are their own family, not a rejection reason:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "ravel_ingest_resource_attrs_dropped_total{{mode=\"gateway\",tenant_hash=\"{hash}\",\
                 signal=\"metrics\"}} 5"
            )),
            "dropped resource attributes are their own family, not a rejection reason:\n{body}"
        );
        assert!(
            !body.contains(&format!(
                "ravel_ingest_resource_attrs_dropped_total{{mode=\"gateway\",tenant_hash=\"{hash}\",\
                 signal=\"logs\""
            )),
            "the counter is metrics-only; a logs row must not render at all:\n{body}"
        );
        assert!(
            body.contains(
                "# HELP ravel_ingest_resource_attrs_dropped_total Metric resource attributes \
                 outside the configured allowlist, dropped rather than turned into labels, by \
                 tenant, for the metrics signal only."
            ),
            "HELP text must state the metrics-only scope:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_ingest_resource_attrs_dropped_total counter"),
            "must be typed as a counter:\n{body}"
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            !matches!(mode, Mode::Maintain),
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
            fragment_inflight_by_class: [
                (crate::distrib::AdmissionClass::Pinned, 0),
                (crate::distrib::AdmissionClass::Resolve, 0),
            ],
            fragment_admission_waits_by_class: [
                (crate::distrib::AdmissionClass::Pinned, 0),
                (crate::distrib::AdmissionClass::Resolve, 0),
            ],
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

    /// A trigger the queued-flush cap really refused reaches the rendered
    /// `/metrics` body as a nonzero
    /// `ravel_ingest_flush_trigger_deferred_total`.
    ///
    /// Every other test of this family builds the snapshot by hand, so all of
    /// them would keep passing if the shard actor stopped counting deferrals
    /// or the router stopped summing them: they pin the renderer, not the
    /// path. This one drives a live `IngestRouter` over a `FaultStore` whose
    /// data PUT is held, takes the shard to its one-deep queue, has the next
    /// age trigger refused, and only then snapshots and renders. The exact
    /// count is asserted, not merely that it is nonzero, so a refusal counted
    /// twice fails here as well.
    ///
    /// Paired with the ingest-side
    /// `ravel_ingest::shard::tests::a_deferred_flush_takes_the_ingest_hour_it_opened_in`:
    /// that one pins what a deferral does to the data, this one pins that an
    /// operator can see the deferral happen. The second matters because of
    /// what the first records. A deferral moves the ingest hour the rows land
    /// in, past what the read-side scan slack covers, and this counter is the
    /// only signal an operator has that it is happening (issue #1916).
    #[tokio::test]
    async fn a_real_deferral_renders_on_the_metrics_body() {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::atomic::{AtomicI64, Ordering};
        use std::time::Duration;

        use ravel_ingest::{
            Clock, IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, WriteMode,
        };
        use ravel_object_store::ObjectStoreBackend;
        use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
        use ravel_object_store::memory::MemoryStore;
        use ravel_otlp::normalize::NormalizedPoint;
        use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};
        use tokio::sync::watch;

        struct TestClock {
            now_ns: AtomicI64,
            wake_tx: watch::Sender<()>,
        }

        impl Clock for TestClock {
            fn now_ns(&self) -> i64 {
                self.now_ns.load(Ordering::SeqCst)
            }

            fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
                let deadline = self
                    .now_ns()
                    .saturating_add(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX));
                let mut rx = self.wake_tx.subscribe();
                Box::pin(async move {
                    loop {
                        if self.now_ns() >= deadline {
                            return;
                        }
                        if rx.changed().await.is_err() {
                            return;
                        }
                    }
                })
            }
        }

        let (wake_tx, _rx) = watch::channel(());
        let clock = Arc::new(TestClock {
            now_ns: AtomicI64::new(1_700_000_000_000_000_000),
            wake_tx,
        });
        let advance = |ns: i64| {
            clock.now_ns.fetch_add(ns, Ordering::SeqCst);
            let _ = clock.wake_tx.send(());
        };

        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        // One shard, one permit, one queue slot, and only the age trigger can
        // fire: the second trigger is refused by construction.
        let config = IngestConfig {
            shard_count: 1,
            target_bytes: 8 * 1024 * 1024,
            max_flush_delay: Duration::from_millis(50),
            flush_tick: Duration::from_millis(10),
            max_inflight_flushes: 1,
            max_queued_flushes: 1,
            ..IngestConfig::default()
        };
        let router = Arc::new(
            IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone())
                .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        );
        let acme = TenantId::new("acme");
        let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

        let point = |host: &str| {
            let labels = LabelSet::new(vec![
                Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: "cpu_usage".to_string(),
                },
                Label {
                    name: "host".to_string(),
                    value: host.to_string(),
                },
            ])
            .expect("distinct label names");
            let series_id = SeriesId::compute(&acme, "cpu_usage", &labels).expect("series id");
            NormalizedPoint {
                series_id,
                labels: Arc::new(labels),
                sample: Sample {
                    ts_ns: 1_000,
                    value: 1.0,
                },
                is_monotonic_sum: false,
            }
        };
        let write = |host: &'static str| {
            let router = Arc::clone(&router);
            let tenant = acme.clone();
            let points = vec![point(host)];
            tokio::spawn(async move {
                router
                    .write(tenant, points, WriteMode::Strict, Duration::from_secs(60))
                    .await
            })
        };
        // Cooperative polling only: every probe reads a metric the actor
        // publishes, so no wall-clock wait decides anything here.
        async fn until(mut probe: impl FnMut() -> bool) {
            while !probe() {
                tokio::task::yield_now().await;
            }
        }
        let deferred = || {
            router
                .metrics()
                .shard_skew_by_shard()
                .into_iter()
                .map(|(_, s)| s.flush_trigger_deferred)
                .sum::<u64>()
        };
        let in_flight = || {
            router
                .metrics()
                .in_flight_flushes_by_shard()
                .into_iter()
                .map(|(_, n)| n)
                .sum::<u64>()
        };

        let parked = write("h0");
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        advance(100_000_000);
        until(|| in_flight() == 1).await;
        gate.wait_until_held(1).await;

        let refused = write("h1");
        until(|| router.metrics().snapshot().buffered_points_total >= 2).await;
        advance(100_000_000);
        until(|| deferred() >= 1 || in_flight() > 1).await;
        assert_eq!(
            in_flight(),
            1,
            "the trigger past the cap must be deferred, not spawned"
        );

        // Snapshot and render while the deferral is still the only thing that
        // has happened, so the body below is about this refusal and nothing
        // else.
        let mut body = String::new();
        render_ingest_family(
            &mut body,
            Mode::Gateway,
            &[IngestPipelineSnapshot::from_metrics(
                router.metrics().snapshot(),
            )],
        );
        assert!(
            body.contains("# TYPE ravel_ingest_flush_trigger_deferred_total counter"),
            "the deferral family must be declared:\n{body}"
        );
        assert_eq!(
            body.matches(
                "ravel_ingest_flush_trigger_deferred_total{mode=\"gateway\",signal=\"metrics\"} 1"
            )
            .count(),
            1,
            "the one refused trigger must render as exactly 1:\n{body}"
        );
        assert_eq!(
            body.matches("ravel_ingest_queued_flushes{mode=\"gateway\",signal=\"metrics\"} 1")
                .count(),
            1,
            "the queue depth the refusal was measured against renders beside \
             it:\n{body}"
        );

        for id in gate.held() {
            assert!(gate.release(id));
        }
        parked
            .await
            .expect("parked write task")
            .expect("parked write acks once the gate is released");
        advance(100_000_000);
        refused
            .await
            .expect("deferred write task")
            .expect("deferred write acks once the cap clears");
        router.flush_all().await;
    }

    /// ADR-1692 acceptance test: a four-shard log router with three writes
    /// driven to one shard renders exactly four series per family through the
    /// same `render()` the `/metrics` handler calls, idle shards at zero and
    /// the busy shard at the exact enqueued and processed count.
    #[tokio::test]
    async fn metrics_render_one_series_per_configured_log_shard_idle_ones_included() {
        use std::time::Duration;

        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use ravel_ingest::{
            AdmissionController, AdmissionLimits, IngestConfig, SystemClock, WriteMode,
        };
        use ravel_object_store::ObjectStoreBackend;
        use ravel_object_store::memory::MemoryStore;
        use ravel_types::logstream::{AttrValue, log_stream_id};
        use ravel_types::{TenantId, shard_for_log};

        use crate::logs_ingest::{LogIngestState, handle_export_logs};
        use crate::normalize_reject_metrics::NormalizeRejectMetrics;

        const BASE_TS_NS: i64 = 1_767_225_600_000_000_000;
        const SHARD_COUNT: u32 = 4;
        const BUSY_SHARD: u32 = 2;
        const WRITES: u32 = 3;

        fn string_kv(key: &str, value: &str) -> KeyValue {
            KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(value.to_string())),
                }),
                ..Default::default()
            }
        }

        fn host_for_shard(want_shard: u32, shard_count: u32) -> String {
            for i in 0..100_000u32 {
                let host = i.to_string();
                let attrs = vec![
                    (
                        "service.name".to_string(),
                        AttrValue::Str("api".to_string()),
                    ),
                    ("host".to_string(), AttrValue::Str(host.clone())),
                ];
                let stream_id = log_stream_id(&attrs, "", "", &[]);
                if shard_for_log(&stream_id, shard_count) == want_shard {
                    return host;
                }
            }
            panic!("no host found for shard {want_shard} of {shard_count}");
        }

        let host = host_for_shard(BUSY_SHARD, SHARD_COUNT);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(LogIngestRouter::new(
            IngestConfig {
                shard_count: SHARD_COUNT,
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        ));
        let state = LogIngestState {
            router: router.clone(),
            limits: ravel_otlp::LogIngestLimits::default(),
            ack_deadline: Duration::from_secs(5),
            admission: Arc::new(AdmissionController::new(
                Arc::new(SystemClock),
                AdmissionLimits::default(),
            )),
            store,
            recovery: None,
            provisioning: None,
            normalize_metrics: Arc::new(NormalizeRejectMetrics::new()),
        };

        for i in 0..WRITES {
            let request = ExportLogsServiceRequest {
                resource_logs: vec![ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![
                            string_kv("service.name", "api"),
                            string_kv("host", &host),
                        ],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            time_unix_nano: BASE_TS_NS as u64,
                            observed_time_unix_nano: BASE_TS_NS as u64,
                            severity_number: 9,
                            severity_text: "INFO".to_string(),
                            body: Some(AnyValue {
                                value: Some(AnyValueVariant::StringValue(format!("line-{i}"))),
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            handle_export_logs(
                &state,
                TenantId::new("acme"),
                WriteMode::Strict,
                request,
                BASE_TS_NS,
                None,
            )
            .await
            .expect("write must succeed");
        }

        // Cooperative polling only: the actor records `messages_processed`
        // just after it sends the caller's ack, so waiting for the ack alone
        // races the actor's own bookkeeping. No wall-clock wait.
        async fn until(mut probe: impl FnMut() -> bool) {
            while !probe() {
                tokio::task::yield_now().await;
            }
        }
        until(|| {
            router
                .metrics()
                .shard_skew_by_shard()
                .into_iter()
                .find(|(shard, _)| *shard == BUSY_SHARD)
                .is_some_and(|(_, s)| s.messages_processed == u64::from(WRITES))
        })
        .await;

        let mut pipeline = IngestPipelineSnapshot::from_log_metrics(router.metrics().snapshot());
        pipeline.shard_skew = router.metrics().shard_skew_by_shard();
        pipeline.active_shard_count = router.shard_count();
        let ingest = vec![pipeline];

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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        const FAMILIES: [&str; 6] = [
            "ravel_ingest_shard_messages_enqueued_total",
            "ravel_ingest_shard_messages_processed_total",
            "ravel_ingest_shard_queue_depth",
            "ravel_ingest_shard_on_actor_seconds_total",
            "ravel_ingest_shard_flush_permit_wait_seconds_total",
            "ravel_ingest_shard_off_actor_seconds_total",
        ];
        for family in FAMILIES {
            let count = body
                .matches(&format!("{family}{{mode=\"gateway\",signal=\"logs\","))
                .count();
            assert_eq!(
                count, SHARD_COUNT as usize,
                "{family} must render exactly {SHARD_COUNT} series, one per configured \
                 shard:\n{body}"
            );
        }
        for shard in 0..SHARD_COUNT {
            if shard == BUSY_SHARD {
                continue;
            }
            assert_eq!(
                body.matches(&format!(
                    "ravel_ingest_shard_messages_enqueued_total{{mode=\"gateway\",\
                     signal=\"logs\",shard=\"{shard}\"}} 0"
                ))
                .count(),
                1,
                "idle shard {shard} must render zero enqueued:\n{body}"
            );
        }
        assert_eq!(
            body.matches(&format!(
                "ravel_ingest_shard_messages_enqueued_total{{mode=\"gateway\",\
                 signal=\"logs\",shard=\"{BUSY_SHARD}\"}} {WRITES}"
            ))
            .count(),
            1,
            "the busy shard must render the exact enqueued count:\n{body}"
        );
        assert_eq!(
            body.matches(&format!(
                "ravel_ingest_shard_messages_processed_total{{mode=\"gateway\",\
                 signal=\"logs\",shard=\"{BUSY_SHARD}\"}} {WRITES}"
            ))
            .count(),
            1,
            "the busy shard must render the exact processed count:\n{body}"
        );
    }

    /// ADR-1692 decision 2: a shard index at or above `MAX_SHARD_COUNT` is
    /// refused by `Label::shard` rather than rendered, even when it is
    /// synthetically present in a pipeline's `shard_skew` (a corrupt or
    /// future-format snapshot). The in-bound neighbor still renders, so this
    /// pins refusal of the one index, not suppression of the whole family.
    #[test]
    fn shard_at_or_above_max_shard_count_is_refused_not_rendered() {
        assert_eq!(
            Label::shard(ravel_catalog::MAX_SHARD_COUNT),
            None,
            "the cap itself must be refused"
        );
        assert!(
            Label::shard(ravel_catalog::MAX_SHARD_COUNT - 1).is_some(),
            "the index just under the cap must still be accepted"
        );

        let mut pipeline = IngestPipelineSnapshot::from_log_metrics(LogIngestMetricsSnapshot {
            ..Default::default()
        });
        pipeline.shard_skew = vec![
            (
                0,
                ravel_ingest::ShardSkewStats {
                    messages_enqueued: 5,
                    ..Default::default()
                },
            ),
            (
                ravel_catalog::MAX_SHARD_COUNT,
                ravel_ingest::ShardSkewStats {
                    messages_enqueued: 9,
                    ..Default::default()
                },
            ),
        ];
        pipeline.active_shard_count = 1;
        let ingest = vec![pipeline];

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
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            true,
        );

        assert_eq!(
            body.matches(
                "ravel_ingest_shard_messages_enqueued_total{mode=\"gateway\",signal=\"logs\",shard=\"0\"} 5"
            )
            .count(),
            1,
            "the in-bound shard must still render:\n{body}"
        );
        assert!(
            !body.contains(&format!("shard=\"{}\"", ravel_catalog::MAX_SHARD_COUNT)),
            "an index at MAX_SHARD_COUNT must never render, got:\n{body}"
        );
    }
}
