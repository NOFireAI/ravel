//! Ingest admission limits and typed rejection reasons for OTLP
//! normalization (ADR-0010 §6, §8; docs/consistency-model.md "Late and
//! skewed data").
//!
//! Ordering and cost. These limits are enforced inside `normalize_metrics`,
//! which runs only after the transport layer has already decoded the whole
//! `ExportMetricsServiceRequest` into memory: the HTTP path decodes the
//! full body in `services/ravel-server` before calling in, and the gRPC
//! path does the same through tonic. `max_data_points_per_request` and the
//! other limits here therefore bound per-point label allocation and what
//! reaches the shard buffer; they do not bound decode-time allocation. The
//! only bound on the work a hostile or misconfigured sender can force
//! before any check here runs is the transport body/message limit, which
//! lives in the services crate, not in this module.
//!
//! Rejections are typed rather than bare errors: the OTLP partial-success
//! response reports a rejected-point count, and
//! [`Rejection::rejected_count`] gives the multiplier for a rejection that
//! covers more than one point (an oversized request, an unsupported metric
//! type). A resource-scoped rejection (every point under a `ResourceMetrics`
//! whose resource labels failed to build) uses [`Rejection::Grouped`] for the
//! same purpose: one `Rejection` value carries the shared reason plus the
//! point count it covers, instead of `normalize_resource` materializing one
//! clone per point (issue #209; see also finding a8-F07 in
//! `docs/reviews/2026-07-27-storage-engine-quality-audit/a8-ingest-otlp.md`,
//! which flagged the per-point materialization cost). The counting itself
//! (what `rejected_count` sums to) was already correct before #209 and is
//! unchanged by it; #209 only changes how that count is represented in
//! memory.
//!
//! Cross-reference (not corrected here): decoding the full body before the
//! request-size check is a separate behavior gap, also recorded under a8-F07.

/// Admission limits checked at OTLP ingest, before allocating per-point
/// label structures.
#[derive(Debug, Clone, PartialEq)]
pub struct IngestLimits {
    /// Total data points across a single `ExportMetricsServiceRequest`,
    /// counted from data-point vector lengths only (no per-point allocation
    /// happens before this check).
    pub max_data_points_per_request: usize,
    /// Attributes on a single data point.
    pub max_attributes_per_point: usize,
    /// Bytes in a label name, checked after sanitization.
    pub max_label_name_len: usize,
    /// Bytes in a label value.
    pub max_label_value_len: usize,
    /// Bytes in a metric name, checked before sanitization.
    pub max_metric_name_len: usize,
    /// Attributes on a Resource.
    pub max_resource_attributes: usize,
    /// Nanoseconds a data point's event time may lead ingest time
    /// (ADR-0010 §8). Default 10 minutes.
    pub max_future_skew_ns: i64,
    /// Nanoseconds a data point's event time may lag ingest time
    /// (ADR-0010 §8). Default 2 hours.
    pub max_ingest_lag_ns: i64,
    /// Resource attribute keys flattened into labels beyond the fixed
    /// job/instance mapping (`service.name`, `service.namespace`,
    /// `service.instance.id`). Configurable because deployments vary in
    /// which resource semantic conventions they rely on for routing and
    /// alerting.
    pub resource_attribute_allowlist: Vec<String>,
}

const SECOND_NANOS: i64 = 1_000_000_000;
const MINUTE_NANOS: i64 = 60 * SECOND_NANOS;
const HOUR_NANOS: i64 = 60 * MINUTE_NANOS;

impl Default for IngestLimits {
    fn default() -> Self {
        IngestLimits {
            max_data_points_per_request: 100_000,
            max_attributes_per_point: 64,
            max_label_name_len: 256,
            max_label_value_len: 4096,
            max_metric_name_len: 512,
            max_resource_attributes: 128,
            max_future_skew_ns: 10 * MINUTE_NANOS,
            max_ingest_lag_ns: 2 * HOUR_NANOS,
            resource_attribute_allowlist: default_resource_attribute_allowlist(),
        }
    }
}

/// Default allowlist for resource attributes flattened into labels, beyond
/// the fixed job/instance mapping.
pub fn default_resource_attribute_allowlist() -> Vec<String> {
    [
        "k8s.namespace.name",
        "k8s.pod.name",
        "k8s.container.name",
        "host.name",
        "deployment.environment.name",
        "cloud.provider",
        "cloud.region",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// Why a single OTLP data point (or a group of them) was not admitted.
/// Every variant is meant to be reported back to the sender via the OTLP
/// partial-success mechanism, never just logged and dropped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Rejection {
    #[error("request has {count} data points, more than the per-request limit of {max}")]
    TooManyDataPoints { count: usize, max: usize },

    #[error(
        "resource has more attributes than the limit of {max}; rejecting {count} data points under it"
    )]
    TooManyResourceAttributes { count: usize, max: usize },

    #[error(
        "metric name is {len} bytes, more than the limit of {max}; rejecting {count} data points"
    )]
    MetricNameTooLong {
        len: usize,
        max: usize,
        count: usize,
    },

    #[error("metric name is empty after sanitization; rejecting {count} data points")]
    EmptyMetricName { count: usize },

    #[error("data point has {attribute_count} attributes, more than the limit of {max}")]
    TooManyAttributes { attribute_count: usize, max: usize },

    #[error("label name is {len} bytes, more than the limit of {max}")]
    LabelNameTooLong { len: usize, max: usize },

    #[error("label value is {len} bytes, more than the limit of {max}")]
    LabelValueTooLong { len: usize, max: usize },

    #[error("duplicate label name after sanitization: {0}")]
    DuplicateLabelName(String),

    #[error(
        "attribute value is an array, kvlist, bytes, or string-table reference (strindex) value, which has no label representation"
    )]
    ComplexAttributeValue,

    #[error("data point has neither an int nor a double value set")]
    MissingValue,

    #[error("metric type {metric_type} is not supported in phase 1; rejecting {count} data points")]
    UnsupportedMetricType {
        metric_type: &'static str,
        count: usize,
    },

    #[error(
        "only cumulative-temporality sums are supported in phase 1; rejecting {count} data points"
    )]
    UnsupportedTemporality { count: usize },

    #[error("event timestamp is zero")]
    ZeroTimestamp,

    #[error(
        "event timestamp is {skew_ns} ns ahead of ingest time, more than the max future skew of {max_ns} ns"
    )]
    FutureSkew { skew_ns: i64, max_ns: i64 },

    #[error(
        "event timestamp is {lag_ns} ns behind ingest time, more than the max ingest lag of {max_ns} ns"
    )]
    TooOld { lag_ns: i64, max_ns: i64 },

    #[error("series identity component exceeds encoding limits")]
    OversizedSeriesComponent,

    #[error(
        "histogram has {bounds} explicit bounds but {buckets} bucket counts (expected {expected})"
    )]
    HistogramBucketCountMismatch {
        bounds: usize,
        buckets: usize,
        expected: usize,
    },

    #[error("histogram explicit_bounds contains a NaN or infinite value")]
    NonFiniteHistogramBound,

    #[error("histogram explicit_bounds is not strictly increasing")]
    HistogramBoundsNotIncreasing,

    #[error("histogram bucket_counts overflow u64 during cumulative accumulation")]
    HistogramCountOverflow,

    #[error(
        "exponential histogram scale {scale} is unsupported: scale below -53 is invalid, and OTLP has no custom-bucket-boundary field to back the -53 custom-buckets sentinel"
    )]
    NativeHistogramScaleUnsupported { scale: i32 },

    #[error(
        "exponential histogram count is smaller than its zero_count plus bucket counts, which the segment format's reader would reject as corrupted"
    )]
    NativeHistogramCountInconsistent,

    #[error("exponential histogram zero_count plus bucket counts overflow u64")]
    NativeHistogramCountOverflow,

    #[error("summary quantile value is NaN or infinite")]
    NonFiniteQuantile,

    #[error("summary has two quantile_values entries with the same quantile")]
    DuplicateQuantile,

    /// Informational, not an admission failure: the data point was admitted
    /// and its exploded series stored, but its `min`/`max` fields have no
    /// Prometheus-convention representation (ADR-0016) and were dropped.
    /// `rejected_count()` returns 0 for this variant so it never inflates
    /// the sender-facing rejected-points count.
    #[error("histogram min/max field(s) dropped: no Prometheus-convention representation")]
    HistogramMinMaxDropped { count: usize },

    /// Informational, not an admission failure: same as
    /// [`Rejection::HistogramMinMaxDropped`] but for exemplars, pending
    /// ADR-0017's exemplar decision.
    #[error("histogram exemplar(s) dropped: no Prometheus-convention representation")]
    HistogramExemplarsDropped { count: usize },

    /// Informational, not an admission failure: the data point was admitted
    /// and stored, but its OTLP `as_int` value has a magnitude above 2^53 and
    /// is not exactly representable as the `f64` the segment format stores, so
    /// the persisted sample is the nearest `f64` rather than the exact integer.
    /// `rejected_count()` returns 0 so it never inflates the sender-facing
    /// rejected-points count. Emitting it makes the (given `f64`-only storage,
    /// unavoidable) approximation visible rather than silent, per the
    /// "exact semantics by default; approximation is opt-in and visible"
    /// invariant. `value` is the original integer; the stored `f64` is shown
    /// in the message.
    #[error(
        "integer value {value} has magnitude above 2^53 and was stored as the nearest f64 {}",
        *value as f64
    )]
    IntegerValuePrecisionLoss { value: i64 },

    /// `reason` applied identically to `count` data points that share one
    /// scope (currently: every point under a `ResourceMetrics` whose
    /// resource labels failed to build). Represents the same information as
    /// `count` clones of `reason` without materializing them (#209).
    #[error("{reason} (rejecting {count} data points under it)")]
    Grouped {
        reason: Box<Rejection>,
        count: usize,
    },
}

impl Rejection {
    /// Number of underlying OTLP data points this rejection accounts for.
    /// Summing this over [`crate::normalize::NormalizeOutput::rejected`]
    /// gives the count to report in an OTLP `rejected_data_points` field.
    pub fn rejected_count(&self) -> usize {
        match self {
            Rejection::TooManyDataPoints { count, .. }
            | Rejection::TooManyResourceAttributes { count, .. }
            | Rejection::MetricNameTooLong { count, .. }
            | Rejection::EmptyMetricName { count }
            | Rejection::UnsupportedMetricType { count, .. }
            | Rejection::UnsupportedTemporality { count }
            | Rejection::Grouped { count, .. } => *count,
            Rejection::HistogramMinMaxDropped { .. }
            | Rejection::HistogramExemplarsDropped { .. }
            | Rejection::IntegerValuePrecisionLoss { .. } => 0,
            _ => 1,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_spec() {
        let limits = IngestLimits::default();
        assert_eq!(limits.max_data_points_per_request, 100_000);
        assert_eq!(limits.max_attributes_per_point, 64);
        assert_eq!(limits.max_label_name_len, 256);
        assert_eq!(limits.max_label_value_len, 4096);
        assert_eq!(limits.max_metric_name_len, 512);
        assert_eq!(limits.max_resource_attributes, 128);
        assert_eq!(limits.max_future_skew_ns, 600_000_000_000);
        assert_eq!(limits.max_ingest_lag_ns, 7_200_000_000_000);
        assert_eq!(
            limits.resource_attribute_allowlist,
            vec![
                "k8s.namespace.name",
                "k8s.pod.name",
                "k8s.container.name",
                "host.name",
                "deployment.environment.name",
                "cloud.provider",
                "cloud.region",
            ]
        );
    }

    #[test]
    fn rejected_count_uses_group_count_when_present() {
        let r = Rejection::UnsupportedMetricType {
            metric_type: "histogram",
            count: 7,
        };
        assert_eq!(r.rejected_count(), 7);
    }

    #[test]
    fn rejected_count_defaults_to_one_for_point_scoped_reasons() {
        assert_eq!(Rejection::ZeroTimestamp.rejected_count(), 1);
        assert_eq!(
            Rejection::DuplicateLabelName("x".to_string()).rejected_count(),
            1
        );
    }

    #[test]
    fn grouped_rejected_count_is_the_carried_count_not_one() {
        let r = Rejection::Grouped {
            reason: Box::new(Rejection::ComplexAttributeValue),
            count: 100_000,
        };
        assert_eq!(r.rejected_count(), 100_000);
        assert!(r.to_string().contains("100000"));
    }
}
