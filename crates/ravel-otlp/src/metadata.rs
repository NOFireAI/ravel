//! Protocol-neutral per-metric metadata (ADR-0085 Decision 1).
//!
//! Every ingest surface (OTLP, OTAP, Remote Write v1/v2) already receives a
//! metric's type, help text, and unit on the wire and, before this, discarded
//! all three. This module defines the small, store-agnostic tuple each decoder
//! now surfaces alongside its normalized points: one [`MetricMetadata`] per
//! metric family. The decoders build it and hand it back; they never write it
//! anywhere. The single metadata sink that persists it lives in `ravel-ingest`
//! (ticket #235), off the ingest acknowledgement path.
//!
//! The `family_name` is defined per path so a lookup at query time matches
//! what was stored (ADR-0085 Decision 1, "the map key is the family name"):
//! for OTLP/OTAP it is the name after the Decision 2 suffix pass (unit and
//! `_total` applied) but before the classic histogram/summary explosion that
//! appends `_bucket`/`_sum`/`_count`; for RW1 it is `metric_family_name` as
//! parsed; for RW2 it is the per-series `__name__` stripped of one structural
//! `_bucket`/`_sum`/`_count`/`_total` suffix.

/// A metric's Prometheus type. `Unknown` covers every wire type Ravel does not
/// model as one of the four canonical kinds (RW's `GAUGEHISTOGRAM`, `INFO`,
/// `STATESET`, and the unspecified/unknown discriminants), so a decoder never
/// has to guess or drop a tuple it cannot classify precisely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Unknown,
}

/// One metric family's metadata, keyed by [`family_name`](Self::family_name).
///
/// `unit` is the mapped Prometheus word for the OTLP/OTAP surfaces (`bytes`,
/// `seconds`; the same word an OpenMetrics `# UNIT` line carries, and the same
/// word the name was suffixed with in Decision 2), or the raw sanitized unit
/// when it maps to nothing, or empty. Remote Write payloads are already in the
/// Prometheus model, so RW1/RW2 carry `unit` through verbatim as decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricMetadata {
    pub family_name: String,
    pub kind: MetricKind,
    pub help: String,
    pub unit: String,
}
