//! Exemplar type and per-series admission cap shared by every OTLP-family
//! ingest path (ADR-0047). The type and the cap live here, once, so OTLP
//! normalization and its OTAP and Remote Write siblings (issue #473) call
//! the same code instead of drifting into three implementations of "at
//! most one exemplar per series per window."

use crate::{Label, SeriesId};
use std::collections::HashMap;

/// One exemplar: a metric sample's correlation hint to the trace that
/// produced it (ADR-0047 decision 1). Field-for-field, this is the RSEG v6
/// `EXEMPLARS` section record (docs/segment-format.md) minus the
/// series/dictionary indices the writer resolves at flush time, so the
/// writer that eventually consumes this value (issue #474) needs no
/// reshaping.
///
/// `value_bits` is the sample value's `f64` bit pattern: values are
/// compared and stored by bit pattern, never `==`, so a NaN payload or a
/// negative zero round-trips exactly (repository invariant).
///
/// `trace_id`/`span_id` use the same convention as the RSEG layout:
/// all-zero means absent. This is not a lossy conversion: the W3C Trace
/// Context spec (which OTel's trace and span ids follow) reserves the
/// all-zero id as invalid, so a genuinely-set id emitted by any conformant
/// tracer is never all-zero. OTLP separately allows the wire field itself
/// to be empty (zero bytes) when the measurement was not recorded inside a
/// trace at all; both "empty on the wire" and "16/8 zero bytes on the
/// wire" collapse to the same all-zero value here, and both read back on
/// the query side as "no trace/span id," which is the only distinction
/// that ever mattered.
#[derive(Debug, Clone, PartialEq)]
pub struct Exemplar {
    pub ts_ns: i64,
    pub value_bits: u64,
    /// All-zero means absent; see the struct-level doc.
    pub trace_id: [u8; 16],
    /// All-zero means absent; see the struct-level doc.
    pub span_id: [u8; 8],
    pub filtered_attributes: Vec<Label>,
}

#[derive(Debug, Clone, Copy)]
struct KeptWindow {
    window_start_ns: i64,
}

/// Per-series, per-window exemplar admission cap (ADR-0047 decision 2): a
/// security control, not a tuning knob. A trace id is high-entropy by
/// construction, so an uncapped exemplar path lets a client multiply
/// object size at will and defeats every dictionary the segment format
/// relies on.
///
/// Enforced per shard actor, with no cross-shard coordination, the same
/// shape the cardinality limiter takes: a real per-series-per-window cap
/// only means something across many requests over wall-clock time, so
/// whatever holds the long-lived per-series map (the ingest shard,
/// issue #474) owns one `ExemplarCap` and passes it by `&mut` reference
/// into every normalize call that shard makes. Normalization itself stays
/// a stateless, one-request-at-a-time function otherwise (mirroring why
/// delta-to-cumulative conversion is rejected: it would need the same
/// kind of cross-request state).
#[derive(Debug, Clone)]
pub struct ExemplarCap {
    window_ns: i64,
    kept: HashMap<SeriesId, KeptWindow>,
}

impl ExemplarCap {
    /// ADR-0047 decision 2 default window: 10 seconds.
    pub const DEFAULT_WINDOW_NS: i64 = 10_000_000_000;

    pub fn new(window_ns: i64) -> Self {
        assert!(window_ns > 0, "exemplar cap window_ns must be positive");
        ExemplarCap {
            window_ns,
            kept: HashMap::new(),
        }
    }

    pub fn window_ns(&self) -> i64 {
        self.window_ns
    }

    /// Decide whether the exemplar candidate at `candidate_ts_ns` for
    /// `series_id` is admitted. Windows are anchored to
    /// `floor(ts_ns / window_ns) * window_ns` in event time, so admission
    /// depends on when the measurement happened, not on request arrival
    /// order.
    ///
    /// The first candidate to claim a series' window wins; this call never
    /// retracts an admission already handed back to an earlier caller.
    /// A caller holding several same-window candidates for one series (a
    /// single OTLP data point can carry exemplars from many measurements
    /// inside one collection interval) and wanting "keep the newest" per
    /// decision 2 must offer them in descending `ts_ns` order.
    pub fn admit(&mut self, series_id: SeriesId, candidate_ts_ns: i64) -> bool {
        let window_start_ns = candidate_ts_ns.div_euclid(self.window_ns) * self.window_ns;
        match self.kept.get(&series_id) {
            Some(kept) if kept.window_start_ns == window_start_ns => false,
            _ => {
                self.kept.insert(series_id, KeptWindow { window_start_ns });
                true
            }
        }
    }
}

impl Default for ExemplarCap {
    fn default() -> Self {
        ExemplarCap::new(Self::DEFAULT_WINDOW_NS)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn series(byte: u8) -> SeriesId {
        SeriesId([byte; 16])
    }

    #[test]
    fn first_candidate_in_a_window_is_admitted() {
        let mut cap = ExemplarCap::new(10_000_000_000);
        assert!(cap.admit(series(1), 1_000_000_000));
    }

    #[test]
    fn second_candidate_in_same_window_is_rejected() {
        let mut cap = ExemplarCap::new(10_000_000_000);
        assert!(cap.admit(series(1), 1_000_000_000));
        assert!(!cap.admit(series(1), 5_000_000_000));
    }

    #[test]
    fn candidate_in_next_window_is_admitted() {
        let mut cap = ExemplarCap::new(10_000_000_000);
        assert!(cap.admit(series(1), 1_000_000_000));
        assert!(cap.admit(series(1), 11_000_000_000));
    }

    #[test]
    fn distinct_series_have_independent_windows() {
        let mut cap = ExemplarCap::new(10_000_000_000);
        assert!(cap.admit(series(1), 1_000_000_000));
        assert!(cap.admit(series(2), 1_500_000_000));
    }

    #[test]
    fn default_window_is_ten_seconds() {
        assert_eq!(ExemplarCap::DEFAULT_WINDOW_NS, 10_000_000_000);
        assert_eq!(ExemplarCap::default().window_ns(), 10_000_000_000);
    }
}
