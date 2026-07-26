//! Ingest admission limits and typed rejection reasons for OTLP
//! normalization (ADR-0010 §6, §8; docs/consistency-model.md "Late and
//! skewed data").
//!
//! Limits are checked before any expensive per-point allocation, so a
//! hostile or misconfigured sender cannot force unbounded work per request.
//! Every rejection is typed rather than a bare error: the OTLP partial
//! success response reports a rejected-point count, and
//! [`Rejection::rejected_count`] gives the right multiplier for rejections
//! that cover more than one point (an oversized request, an oversized
//! resource, an unsupported metric type) without the normalizer having to
//! materialize one entry per point just to prove it didn't drop them
//! silently.

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
        "attribute value is an array, kvlist, or bytes value, which has no label representation"
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
            | Rejection::UnsupportedTemporality { count } => *count,
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
}
