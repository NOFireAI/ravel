//! The resolved-series intermediate that both the RW1 decoder (this crate,
//! phase A1) and the future RW2 decoder (phase A2) converge on
//! (ADR-0015, docs/ingest-breadth-plan.md section 2.1). The normalizer in
//! [`crate::normalize`] consumes only this shape, never a wire message
//! directly, so it stays version-blind.
//!
//! Label strings are carried unsanitized and unvalidated: RW payloads are
//! already in the Prometheus data model, so the normalizer only validates
//! (length, uniqueness, presence of `__name__`) rather than mutating names,
//! per ADR-0015's no-sanitization rule.

use ravel_types::Label;

/// One sample as carried on the wire: milliseconds since epoch, not yet
/// converted or validated.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedSample {
    pub ts_ms: i64,
    pub value: f64,
}

/// One populated run of buckets on one histogram side, as carried on the
/// wire (`prometheus.BucketSpan`, identical in RW1 and RW2). `offset` is
/// relative to the end of the previous span on the same side; `length` is
/// the run's bucket count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedSpan {
    pub offset: i32,
    pub length: u32,
}

/// One side of a wire `count`/`zero_count` oneof. Which variant the `count`
/// oneof carries is what decides whether the whole histogram is integer- or
/// float-valued, exactly as Prometheus's own `Histogram.IsFloatHistogram`
/// decides it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResolvedCount {
    Int(u64),
    Float(f64),
}

/// One native histogram as carried on the wire: milliseconds since epoch,
/// bucket counts still in their wire encoding (integer sides as deltas,
/// float sides as absolute counts), nothing validated yet.
///
/// Deliberately faithful rather than pre-digested. Both `positive_deltas`
/// and `positive_counts` are carried even though a well-formed message uses
/// only the one matching its count kind, and an unset `count` oneof stays
/// `None` instead of being coerced to integer zero here, so that
/// [`crate::normalize`] is the single place that decides what is admissible
/// and reports every violation as a typed rejection.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedHistogram {
    pub ts_ms: i64,
    /// Wire `schema`; the storage model calls the same quantity `scale`.
    pub schema: i32,
    pub zero_threshold: f64,
    /// Wire `sum`, a plain `double` with no presence in either RW version,
    /// so a native histogram always carries one.
    pub sum: f64,
    /// `None` when the wire `count` oneof was unset. Proto3 gives an unset
    /// oneof no presence bit, and Prometheus reads that case as integer 0.
    pub count: Option<ResolvedCount>,
    pub zero_count: Option<ResolvedCount>,
    pub positive_spans: Vec<ResolvedSpan>,
    pub negative_spans: Vec<ResolvedSpan>,
    pub positive_deltas: Vec<i64>,
    pub negative_deltas: Vec<i64>,
    pub positive_counts: Vec<f64>,
    pub negative_counts: Vec<f64>,
    /// Wire `reset_hint` enum discriminant, unmapped: RW1 and RW2 give the
    /// four states the same numbers, and both match the storage model's
    /// [`ravel_segment::ResetHint`] ordering.
    pub reset_hint: i32,
    /// Wire `custom_values`: ascending boundaries for a `schema == -53`
    /// custom-bucket histogram, empty otherwise.
    pub custom_values: Vec<f64>,
}

/// One exemplar as carried on the wire (`prometheus.Exemplar` in RW1,
/// `io.prometheus.write.v2.Exemplar` in RW2; the two are field-identical
/// once RW2's symbol references are resolved): milliseconds since epoch, the
/// value verbatim as an `f64`, and the exemplar's own labels.
///
/// Remote Write has no dedicated trace-id or span-id field. By convention
/// the sender carries them as labels named `trace_id` and `span_id`
/// (`io/prometheus/write/v2/types.proto` calls the `trace_id` name a best
/// practice, and it is what Grafana's exemplar-to-trace link reads). This
/// layer keeps them in `labels` alongside every other label, unsplit and
/// undecoded, for the same reason nothing else here is validated:
/// [`crate::normalize`] owns every admission and mapping decision (ADR-0047
/// decision 1).
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedExemplar {
    pub ts_ms: i64,
    pub value: f64,
    pub labels: Vec<Label>,
}

/// One series (label set plus its samples, native histograms, and exemplars)
/// resolved from either RW1 or RW2, before normalization.
///
/// Exemplar bodies survive decode since ADR-0047 gave exemplars storage and
/// an admission cap: they used to be a bare count here, because everything
/// beyond the tally was thrown away. Native histograms have survived since
/// RSEG v5 admitted and wrote them (docs/rseg-v3-plan.md phase C8).
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<ResolvedSample>,
    pub histograms: Vec<ResolvedHistogram>,
    pub exemplars: Vec<ResolvedExemplar>,
}

/// A resolved `WriteRequest`, version-blind.
///
/// `metadata_count` is a whole-request tally: RW1's `MetricMetadata` list
/// has no per-series correlation (unlike RW2's per-`TimeSeries` `metadata`
/// field), so it is dropped with one request-level counter rather than
/// distributed across series.
///
/// `created_timestamps_count` tallies non-zero `start_timestamp` fields
/// (RW2's `Sample.start_timestamp` and `Histogram.start_timestamp`; RW1's
/// `prometheus.Sample` has no such field, so RW1 always resolves this to
/// 0).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResolvedRequest {
    pub series: Vec<ResolvedSeries>,
    pub metadata_count: usize,
    pub created_timestamps_count: usize,
}
