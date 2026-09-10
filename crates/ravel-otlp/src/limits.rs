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
//! clone per point. The counting itself (what `rejected_count` sums to) is
//! independent of this representation: `Grouped` only changes how that count
//! is carried in memory, not the count.
//!
//! Note (not addressed here): the full request body is decoded before the
//! request-size check runs, so that check does not bound decode-time
//! allocation; the transport body/message limit does.

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
    /// Width of the per-series exemplar admission window (ADR-0047 decision
    /// 2): at most one exemplar per series is kept per window of this many
    /// nanoseconds, the newest one. A security control, not a tuning knob —
    /// a trace id is high-entropy, so an uncapped exemplar path lets a
    /// client multiply object size at will. Default 10 seconds, matching
    /// [`ravel_types::ExemplarCap::DEFAULT_WINDOW_NS`].
    pub exemplar_cap_window_ns: i64,
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
            exemplar_cap_window_ns: ravel_types::ExemplarCap::DEFAULT_WINDOW_NS,
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

    #[error("metric type {metric_type} is not supported; rejecting {count} data points")]
    UnsupportedMetricType {
        metric_type: &'static str,
        count: usize,
    },

    #[error("only cumulative-temporality sums are supported; rejecting {count} data points")]
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

    /// Informational, not an admission failure: the data point was admitted,
    /// but `count` of its exemplars were not carried (ADR-0047 decision 2).
    /// Despite the name (kept for compatibility with existing callers that
    /// match on this variant), this covers exemplars dropped from any
    /// metric type, not only histograms: a malformed exemplar (no
    /// recognized value in its oneof) or one that lost the per-series,
    /// per-window admission cap. Exemplars that survive normalization are
    /// not reflected here; see [`crate::normalize::NormalizedExemplar`].
    #[error("exemplar(s) dropped: malformed, or beyond the per-series admission cap")]
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
    /// `count` clones of `reason` without materializing them.
    #[error("{reason} (rejecting {count} data points under it)")]
    Grouped {
        reason: Box<Rejection>,
        count: usize,
    },
}

/// Which admission `reason` a normalization-layer rejection is counted under
/// (ADR-0051 section 3 layer 3, section 6).
///
/// Layer 3 is "structural and event-time bounds, in normalization, per point /
/// record / span". Those are the only two reasons the layer can produce, so
/// the classification is total over every rejection that costs the sender a
/// point, record, or span:
///
/// * [`AdmissionClass::Skew`] is the event-time arm: the sender's timestamp
///   could not be placed in the admission window around ingest time.
/// * [`AdmissionClass::Structural`] is everything else the layer refuses:
///   a shape, a type, a limit, or a value the storage format cannot represent.
///
/// A rejection that costs the sender nothing (an informational drop of a
/// histogram `min`/`max`, an exemplar, or a single attribute of an otherwise
/// admitted record) has no class: it is not an admission rejection and must
/// never move a rejected counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionClass {
    Skew,
    Structural,
}

/// Rejected points, records, or spans totalled per [`AdmissionClass`] over one
/// normalization pass, ready to be recorded against the `reason` label on
/// `ravel_admission_rejected_total`.
///
/// Each unit is counted once, with the same `rejected_count()` multiplier the
/// OTLP partial-success response reports, so the counter and the response
/// cannot disagree about how many units a request lost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NormalizeRejectCounts {
    pub skew: usize,
    pub structural: usize,
}

impl NormalizeRejectCounts {
    /// Whether anything at all was rejected, so a caller can skip taking a
    /// counter lock on the (overwhelmingly common) clean request.
    pub fn is_empty(&self) -> bool {
        self.skew == 0 && self.structural == 0
    }

    pub fn add(&mut self, class: Option<AdmissionClass>, count: usize) {
        match class {
            Some(AdmissionClass::Skew) => self.skew += count,
            Some(AdmissionClass::Structural) => self.structural += count,
            None => {}
        }
    }

    /// Total the metric path's rejections by class.
    pub fn from_metric_rejections(rejected: &[Rejection]) -> Self {
        let mut counts = Self::default();
        for rejection in rejected {
            counts.add(rejection.admission_class(), rejection.rejected_count());
        }
        counts
    }

    /// Total the log path's rejections by class.
    pub fn from_log_rejections(rejected: &[crate::logs_limits::LogRejection]) -> Self {
        let mut counts = Self::default();
        for rejection in rejected {
            counts.add(rejection.admission_class(), rejection.rejected_count());
        }
        counts
    }

    /// Total the trace path's rejections by class.
    pub fn from_span_rejections(rejected: &[crate::traces_limits::SpanRejection]) -> Self {
        let mut counts = Self::default();
        for rejection in rejected {
            counts.add(rejection.admission_class(), rejection.rejected_count());
        }
        counts
    }
}

impl Rejection {
    /// The admission `reason` this rejection is counted under, or `None` when
    /// it costs the sender no data point (the informational variants, whose
    /// [`Rejection::rejected_count`] is 0).
    ///
    /// Exhaustive on purpose: a new variant does not compile until it has been
    /// classified, so a normalize-layer rejection cannot be added and then
    /// silently go uncounted.
    pub fn admission_class(&self) -> Option<AdmissionClass> {
        match self {
            // Event-time arm. `ZeroTimestamp` belongs here with the two bound
            // breaches: all three come out of the event-time check, and a zero
            // event time is unbounded lag against any plausible ingest clock.
            Rejection::ZeroTimestamp | Rejection::FutureSkew { .. } | Rejection::TooOld { .. } => {
                Some(AdmissionClass::Skew)
            }

            // Structural arm: a shape, type, limit, or value the storage
            // format cannot represent.
            Rejection::TooManyDataPoints { .. }
            | Rejection::TooManyResourceAttributes { .. }
            | Rejection::MetricNameTooLong { .. }
            | Rejection::EmptyMetricName { .. }
            | Rejection::TooManyAttributes { .. }
            | Rejection::LabelNameTooLong { .. }
            | Rejection::LabelValueTooLong { .. }
            | Rejection::DuplicateLabelName(_)
            | Rejection::ComplexAttributeValue
            | Rejection::MissingValue
            | Rejection::UnsupportedMetricType { .. }
            | Rejection::UnsupportedTemporality { .. }
            | Rejection::OversizedSeriesComponent
            | Rejection::HistogramBucketCountMismatch { .. }
            | Rejection::NonFiniteHistogramBound
            | Rejection::HistogramBoundsNotIncreasing
            | Rejection::HistogramCountOverflow
            | Rejection::NativeHistogramScaleUnsupported { .. }
            | Rejection::NativeHistogramCountInconsistent
            | Rejection::NativeHistogramCountOverflow
            | Rejection::NonFiniteQuantile
            | Rejection::DuplicateQuantile => Some(AdmissionClass::Structural),

            // Informational: the point was admitted and stored.
            Rejection::HistogramMinMaxDropped { .. }
            | Rejection::HistogramExemplarsDropped { .. }
            | Rejection::IntegerValuePrecisionLoss { .. } => None,

            // A grouped rejection carries its own point count; the class is
            // the shared reason's.
            Rejection::Grouped { reason, .. } => reason.admission_class(),
        }
    }

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
        assert_eq!(limits.exemplar_cap_window_ns, 10_000_000_000);
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

    /// Every [`Rejection`] variant's admission class, one case per variant.
    ///
    /// The `match` is exhaustive so a variant added to the enum does not
    /// compile until it has been given an expected class here, which is what
    /// stops a new metric-path rejection from going silently unclassified.
    /// Before this test, `Rejection::admission_class` was covered only by two
    /// ravel-server integration tests pinning `UnsupportedTemporality` and
    /// `TooOld`, so every other variant could change arm unnoticed.
    ///
    /// `ZeroTimestamp` is the judgement call and is named explicitly below: a
    /// zero event time is classified as skew, not structural, because it
    /// comes out of the event-time check and is unbounded lag against any
    /// plausible ingest clock. Moving it to the structural arm is a change to
    /// what `ravel_admission_rejected_total{reason=...}` reports, and it
    /// fails here.
    #[test]
    fn every_rejection_variant_has_its_expected_admission_class() {
        fn expected(rejection: &Rejection) -> Option<AdmissionClass> {
            let skew = Some(AdmissionClass::Skew);
            let structural = Some(AdmissionClass::Structural);
            match rejection {
                Rejection::ZeroTimestamp => skew,
                Rejection::FutureSkew { .. } => skew,
                Rejection::TooOld { .. } => skew,

                Rejection::TooManyDataPoints { .. } => structural,
                Rejection::TooManyResourceAttributes { .. } => structural,
                Rejection::MetricNameTooLong { .. } => structural,
                Rejection::EmptyMetricName { .. } => structural,
                Rejection::TooManyAttributes { .. } => structural,
                Rejection::LabelNameTooLong { .. } => structural,
                Rejection::LabelValueTooLong { .. } => structural,
                Rejection::DuplicateLabelName(_) => structural,
                Rejection::ComplexAttributeValue => structural,
                Rejection::MissingValue => structural,
                Rejection::UnsupportedMetricType { .. } => structural,
                Rejection::UnsupportedTemporality { .. } => structural,
                Rejection::OversizedSeriesComponent => structural,
                Rejection::HistogramBucketCountMismatch { .. } => structural,
                Rejection::NonFiniteHistogramBound => structural,
                Rejection::HistogramBoundsNotIncreasing => structural,
                Rejection::HistogramCountOverflow => structural,
                Rejection::NativeHistogramScaleUnsupported { .. } => structural,
                Rejection::NativeHistogramCountInconsistent => structural,
                Rejection::NativeHistogramCountOverflow => structural,
                Rejection::NonFiniteQuantile => structural,
                Rejection::DuplicateQuantile => structural,

                // Informational: the point was admitted and stored, so it
                // costs the sender nothing and must move no counter.
                Rejection::HistogramMinMaxDropped { .. } => None,
                Rejection::HistogramExemplarsDropped { .. } => None,
                Rejection::IntegerValuePrecisionLoss { .. } => None,

                // Carries its reason's class, whatever that reason is.
                Rejection::Grouped { reason, .. } => expected(reason),
            }
        }

        // One value per variant. The `match` above does not compile with a
        // variant missing; this list is what makes each case run.
        let variants = [
            Rejection::TooManyDataPoints {
                count: 1,
                max: 100_000,
            },
            Rejection::TooManyResourceAttributes { count: 1, max: 128 },
            Rejection::MetricNameTooLong {
                len: 600,
                max: 512,
                count: 1,
            },
            Rejection::EmptyMetricName { count: 1 },
            Rejection::TooManyAttributes {
                attribute_count: 65,
                max: 64,
            },
            Rejection::LabelNameTooLong { len: 300, max: 256 },
            Rejection::LabelValueTooLong {
                len: 5000,
                max: 4096,
            },
            Rejection::DuplicateLabelName("x".to_string()),
            Rejection::ComplexAttributeValue,
            Rejection::MissingValue,
            Rejection::UnsupportedMetricType {
                metric_type: "histogram",
                count: 1,
            },
            Rejection::UnsupportedTemporality { count: 1 },
            Rejection::ZeroTimestamp,
            Rejection::FutureSkew {
                skew_ns: 1,
                max_ns: 0,
            },
            Rejection::TooOld {
                lag_ns: 1,
                max_ns: 0,
            },
            Rejection::OversizedSeriesComponent,
            Rejection::HistogramBucketCountMismatch {
                bounds: 1,
                buckets: 1,
                expected: 2,
            },
            Rejection::NonFiniteHistogramBound,
            Rejection::HistogramBoundsNotIncreasing,
            Rejection::HistogramCountOverflow,
            Rejection::NativeHistogramScaleUnsupported { scale: -54 },
            Rejection::NativeHistogramCountInconsistent,
            Rejection::NativeHistogramCountOverflow,
            Rejection::NonFiniteQuantile,
            Rejection::DuplicateQuantile,
            Rejection::HistogramMinMaxDropped { count: 1 },
            Rejection::HistogramExemplarsDropped { count: 1 },
            Rejection::IntegerValuePrecisionLoss { value: i64::MAX },
            Rejection::Grouped {
                reason: Box::new(Rejection::ComplexAttributeValue),
                count: 3,
            },
        ];

        // One entry per variant, each a distinct one, so no variant is
        // covered twice while another is missing.
        assert_eq!(variants.len(), 29);
        for (i, a) in variants.iter().enumerate() {
            for b in &variants[i + 1..] {
                assert_ne!(
                    std::mem::discriminant(a),
                    std::mem::discriminant(b),
                    "{a} and {b} are the same variant"
                );
            }
        }

        for rejection in &variants {
            assert_eq!(
                rejection.admission_class(),
                expected(rejection),
                "{rejection}"
            );
        }

        // `ZeroTimestamp` by name, so the judgement call is pinned where a
        // reader looking for it will find it and not only inside the loop.
        assert_eq!(
            Rejection::ZeroTimestamp.admission_class(),
            Some(AdmissionClass::Skew)
        );

        // A grouped rejection takes its inner reason's class, including the
        // skew arm, so the wrapper cannot silently reclassify.
        assert_eq!(
            Rejection::Grouped {
                reason: Box::new(Rejection::ZeroTimestamp),
                count: 4,
            }
            .admission_class(),
            Some(AdmissionClass::Skew)
        );
    }

    /// `add` routes each class to its own field and leaves the other
    /// untouched, and `None` moves neither. Pinned in this crate because a
    /// crate-scoped gate here otherwise cannot catch an arm swap: only the
    /// server crate exercised this arithmetic before.
    #[test]
    fn add_routes_each_class_to_its_own_field() {
        let mut counts = NormalizeRejectCounts::default();
        counts.add(Some(AdmissionClass::Skew), 3);
        assert_eq!(counts.skew, 3);
        assert_eq!(counts.structural, 0);

        counts.add(Some(AdmissionClass::Structural), 5);
        assert_eq!(counts.skew, 3);
        assert_eq!(counts.structural, 5);

        counts.add(None, 100);
        assert_eq!(counts.skew, 3);
        assert_eq!(counts.structural, 5);
        assert!(!counts.is_empty());
    }
}
