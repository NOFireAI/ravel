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

/// One series (label set plus its samples) resolved from either RW1 or RW2,
/// before normalization.
///
/// `histogram_count` and `exemplar_count` are tallies only: native
/// `Histogram` messages are rejected at admission (typed, counted; no
/// native-histogram storage until ADR-0017 lands) and exemplars are
/// accepted-and-dropped (counted), so neither the histogram values nor the
/// exemplar bodies themselves need to survive past decode.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<ResolvedSample>,
    pub histogram_count: usize,
    pub exemplar_count: usize,
}

/// A resolved `WriteRequest`, version-blind.
///
/// `metadata_count` is a whole-request tally: RW1's `MetricMetadata` list
/// has no per-series correlation (unlike RW2's per-`TimeSeries` `metadata`
/// field), so it is dropped with one request-level counter rather than
/// distributed across series.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResolvedRequest {
    pub series: Vec<ResolvedSeries>,
    pub metadata_count: usize,
}
