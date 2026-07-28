//! Normalization from the version-blind [`ResolvedRequest`] into Ravel's
//! canonical metric point representation (ADR-0015, docs/ingest-breadth-plan.md
//! section 2.1).
//!
//! Unlike `ravel_otlp::normalize`, this module never sanitizes a label name
//! or metric name: Remote Write payloads are already in the Prometheus data
//! model, and mutating them would silently alias a series relative to what
//! the sender believes it wrote. Every check here is validation, not
//! mutation.
//!
//! Per RW convention, a label with an empty value is treated as absent and
//! dropped before duplicate-name and length checks run; this is expected
//! Prometheus behavior (used by classic senders to signal "no value"), not
//! a leniency this crate invented.
//!
//! Values pass through as raw `f64` bit patterns, so the Prometheus stale
//! marker (a specific NaN payload) round-trips unchanged: to the storage
//! layer it is an ordinary sample.

use ravel_otlp::normalize::NormalizedPoint;
use ravel_otlp::{IngestLimits, Rejection};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};

use crate::resolved::{ResolvedRequest, ResolvedSample, ResolvedSeries};

/// Why a Remote Write series or sample was not admitted.
///
/// `Otlp` wraps the semantics [`ravel_otlp::Rejection`] already defines
/// (label/timestamp/limit validation is identical to OTLP's) rather than
/// redefining them; `count` carries how many resolved data points (samples
/// plus native-histogram entries) this rejection accounts for, since a
/// single RW series can hold many points behind one label-validation
/// failure, unlike `Rejection`'s own per-variant count fields, which are
/// sized for OTLP's per-metric or per-resource grouping.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RwRejection {
    #[error("{reason} (rejecting {count} point(s))")]
    Otlp { reason: Rejection, count: usize },

    #[error(
        "native histogram samples have no storage yet (ADR-0017); rejecting {count} histogram sample(s)"
    )]
    NativeHistogramUnsupported { count: usize },

    #[error("sample timestamp in milliseconds overflows nanosecond conversion")]
    TimestampOverflow,
}

impl RwRejection {
    /// Number of underlying resolved data points this rejection accounts
    /// for; mirrors [`Rejection::rejected_count`] for the RW surface.
    pub fn rejected_count(&self) -> usize {
        match self {
            RwRejection::Otlp { count, .. } => *count,
            RwRejection::NativeHistogramUnsupported { count } => *count,
            RwRejection::TimestampOverflow => 1,
        }
    }
}

/// Result of normalizing one resolved Remote Write request.
#[derive(Debug, Clone, PartialEq)]
pub struct RwNormalizeOutput {
    pub points: Vec<NormalizedPoint>,
    pub rejected: Vec<RwRejection>,
    /// Native histogram samples seen and rejected (a subset of `rejected`'s
    /// point count; broken out because the RW stats surface reports it as
    /// its own header, distinct from the generic rejected-sample count).
    pub histograms_dropped: usize,
    /// Exemplars accepted-and-dropped (ADR-0017 deferral): every exemplar
    /// attached to an otherwise-processed series counts here, whether or
    /// not that series' own points were admitted.
    pub exemplars_dropped: usize,
    /// Metric metadata entries accepted-and-dropped: Ravel has no
    /// metric-metadata store yet (ADR-0015).
    pub metadata_dropped: usize,
    /// Created/start timestamps accepted-and-dropped. Always 0 for RW1:
    /// `prometheus.Sample` has no such field on the wire; RW2's
    /// `Sample.start_timestamp` and `Histogram.start_timestamp` populate
    /// [`ResolvedRequest::created_timestamps_count`], which this mirrors.
    pub created_timestamps_dropped: usize,
}

/// Normalize a resolved Remote Write request into Ravel canonical points.
///
/// `ingest_ts_ns` is the receiver's clock reading at admission time, used to
/// bound event-time skew identically to the OTLP surface (ADR-0010 section
/// 8). Nothing here panics for malformed or oversized input: every problem
/// becomes an [`RwRejection`].
pub fn normalize_resolved(
    tenant: &TenantId,
    resolved: ResolvedRequest,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
) -> RwNormalizeOutput {
    let total_samples: usize = resolved.series.iter().map(|s| s.samples.len()).sum();
    if total_samples > limits.max_data_points_per_request {
        return RwNormalizeOutput {
            points: Vec::new(),
            rejected: vec![RwRejection::Otlp {
                reason: Rejection::TooManyDataPoints {
                    count: total_samples,
                    max: limits.max_data_points_per_request,
                },
                count: total_samples,
            }],
            histograms_dropped: 0,
            exemplars_dropped: 0,
            metadata_dropped: resolved.metadata_count,
            created_timestamps_dropped: resolved.created_timestamps_count,
        };
    }

    let mut points = Vec::new();
    let mut rejected = Vec::new();
    let mut histograms_dropped = 0usize;
    let mut exemplars_dropped = 0usize;

    for series in &resolved.series {
        exemplars_dropped += series.exemplar_count;
        normalize_series(
            tenant,
            series,
            limits,
            ingest_ts_ns,
            &mut points,
            &mut rejected,
            &mut histograms_dropped,
        );
    }

    RwNormalizeOutput {
        points,
        rejected,
        histograms_dropped,
        exemplars_dropped,
        metadata_dropped: resolved.metadata_count,
        created_timestamps_dropped: resolved.created_timestamps_count,
    }
}

fn normalize_series(
    tenant: &TenantId,
    series: &ResolvedSeries,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    points: &mut Vec<NormalizedPoint>,
    rejected: &mut Vec<RwRejection>,
    histograms_dropped: &mut usize,
) {
    let point_count = series.samples.len() + series.histogram_count;

    let (metric_name, label_set) = match resolve_series_identity(series, limits, point_count) {
        Ok(v) => v,
        Err(reason) => {
            rejected.push(RwRejection::Otlp {
                reason,
                count: point_count,
            });
            return;
        }
    };

    let series_id = match SeriesId::compute(tenant, &metric_name, &label_set) {
        Ok(id) => id,
        Err(_) => {
            rejected.push(RwRejection::Otlp {
                reason: Rejection::OversizedSeriesComponent,
                count: point_count,
            });
            return;
        }
    };

    if series.histogram_count > 0 {
        *histograms_dropped += series.histogram_count;
        rejected.push(RwRejection::NativeHistogramUnsupported {
            count: series.histogram_count,
        });
    }

    for sample in &series.samples {
        match build_sample(sample, ingest_ts_ns, limits) {
            Ok(sample) => points.push(NormalizedPoint {
                series_id,
                labels: label_set.clone(),
                sample,
                // RW1's metadata is a flat request-level list with no
                // per-series correlation (ADR-0015), so whether this
                // series' underlying metric is a monotonic sum is not
                // knowable here; conservatively false, matching the
                // metadata-is-dropped scope of this phase.
                is_monotonic_sum: false,
            }),
            Err(reason) => rejected.push(reason),
        }
    }
}

/// Resolve one series' `__name__` and remaining labels into a metric name
/// and validated [`LabelSet`], per docs/ingest-breadth-plan.md section 2.1:
/// `__name__` must be present and non-empty; empty-value labels are dropped
/// as absent; duplicate names (including a duplicated `__name__`) reject;
/// existing `IngestLimits` length/count limits apply unchanged, with
/// `max_attributes_per_point` counted over labels excluding `__name__` to
/// match the OTLP surface, where the metric name is a separate proto field.
fn resolve_series_identity(
    series: &ResolvedSeries,
    limits: &IngestLimits,
    point_count: usize,
) -> Result<(String, LabelSet), Rejection> {
    let mut name_labels = series.labels.iter().filter(|l| l.name == METRIC_NAME_LABEL);
    let name_label = name_labels.next();
    if name_labels.next().is_some() {
        return Err(Rejection::DuplicateLabelName(METRIC_NAME_LABEL.to_string()));
    }
    let metric_name = match name_label {
        None => return Err(Rejection::EmptyMetricName { count: point_count }),
        Some(l) if l.value.is_empty() => {
            return Err(Rejection::EmptyMetricName { count: point_count });
        }
        Some(l) => l.value.clone(),
    };
    if metric_name.len() > limits.max_metric_name_len {
        return Err(Rejection::MetricNameTooLong {
            len: metric_name.len(),
            max: limits.max_metric_name_len,
            count: point_count,
        });
    }

    let mut labels = Vec::with_capacity(series.labels.len());
    for l in &series.labels {
        if l.name == METRIC_NAME_LABEL || l.value.is_empty() {
            continue;
        }
        if l.name.len() > limits.max_label_name_len {
            return Err(Rejection::LabelNameTooLong {
                len: l.name.len(),
                max: limits.max_label_name_len,
            });
        }
        if l.value.len() > limits.max_label_value_len {
            return Err(Rejection::LabelValueTooLong {
                len: l.value.len(),
                max: limits.max_label_value_len,
            });
        }
        labels.push(l.clone());
    }
    if labels.len() > limits.max_attributes_per_point {
        return Err(Rejection::TooManyAttributes {
            attribute_count: labels.len(),
            max: limits.max_attributes_per_point,
        });
    }
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric_name.clone(),
    });

    let label_set = LabelSet::new(labels).map_err(|err| match err {
        ravel_types::TypeError::DuplicateLabelName(name) => Rejection::DuplicateLabelName(name),
        // LabelSet::new only ever returns DuplicateLabelName; this arm
        // exists so a future TypeError variant can't silently pass through
        // as an accepted series.
        _ => Rejection::DuplicateLabelName(String::new()),
    })?;

    Ok((metric_name, label_set))
}

/// Convert one resolved sample's millisecond timestamp to nanoseconds and
/// apply the same skew/lag admission bounds as OTLP (ADR-0010 section 8).
fn build_sample(
    sample: &ResolvedSample,
    ingest_ts_ns: i64,
    limits: &IngestLimits,
) -> Result<Sample, RwRejection> {
    let ts_ns = sample
        .ts_ms
        .checked_mul(1_000_000)
        .ok_or(RwRejection::TimestampOverflow)?;
    if ts_ns == 0 {
        return Err(RwRejection::Otlp {
            reason: Rejection::ZeroTimestamp,
            count: 1,
        });
    }

    // Convention (ADR-0010 section 8): the bound itself passes, only
    // strictly exceeding it rejects.
    let skew_ns = ts_ns.saturating_sub(ingest_ts_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(RwRejection::Otlp {
            reason: Rejection::FutureSkew {
                skew_ns,
                max_ns: limits.max_future_skew_ns,
            },
            count: 1,
        });
    }
    let lag_ns = ingest_ts_ns.saturating_sub(ts_ns);
    if lag_ns > limits.max_ingest_lag_ns {
        return Err(RwRejection::Otlp {
            reason: Rejection::TooOld {
                lag_ns,
                max_ns: limits.max_ingest_lag_ns,
            },
            count: 1,
        });
    }

    Ok(Sample {
        ts_ns,
        value: sample.value,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn tenant() -> TenantId {
        TenantId::new("acme")
    }

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn series(labels: Vec<Label>, samples: Vec<ResolvedSample>) -> ResolvedSeries {
        ResolvedSeries {
            labels,
            samples,
            histogram_count: 0,
            exemplar_count: 0,
        }
    }

    fn sample(ts_ms: i64, value: f64) -> ResolvedSample {
        ResolvedSample { ts_ms, value }
    }

    fn request(series: Vec<ResolvedSeries>) -> ResolvedRequest {
        ResolvedRequest {
            series,
            metadata_count: 0,
            created_timestamps_count: 0,
        }
    }

    // --- happy path ---

    #[test]
    fn admits_a_well_formed_series() {
        let req = request(vec![series(
            vec![label("__name__", "up"), label("job", "svc")],
            vec![sample(1_700_000_000_000, 1.0)],
        )]);
        let out = normalize_resolved(
            &tenant(),
            req,
            &IngestLimits::default(),
            1_700_000_001_000_000_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 1);
        assert_eq!(out.points[0].labels.get("__name__"), Some("up"));
        assert_eq!(out.points[0].labels.get("job"), Some("svc"));
        assert_eq!(out.points[0].sample.ts_ns, 1_700_000_000_000_000_000);
        assert_eq!(out.points[0].sample.value, 1.0);
        assert!(!out.points[0].is_monotonic_sum);
    }

    #[test]
    fn no_sanitization_dotted_label_name_passes_through_unchanged() {
        // Unlike OTLP, RW labels are never mutated: a dotted name (invalid
        // as Prometheus label syntax, but this crate validates, not
        // sanitizes) is carried verbatim rather than rewritten to
        // underscores.
        let req = request(vec![series(
            vec![label("__name__", "up"), label("weird.name", "v")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].labels.get("weird.name"), Some("v"));
    }

    // --- empty-value labels dropped as absent ---

    #[test]
    fn empty_value_label_is_dropped_not_rejected() {
        let req = request(vec![series(
            vec![label("__name__", "up"), label("optional", "")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].labels.get("optional"), None);
    }

    // --- __name__ rules ---

    #[test]
    fn missing_name_label_rejects_whole_series() {
        let req = request(vec![series(
            vec![label("job", "svc")],
            vec![sample(1_000, 1.0), sample(2_000, 2.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::EmptyMetricName { count: 2 },
                count: 2,
            }]
        );
    }

    #[test]
    fn empty_name_value_rejects_whole_series() {
        let req = request(vec![series(
            vec![label("__name__", "")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::EmptyMetricName { count: 1 },
                count: 1,
            }]
        );
    }

    #[test]
    fn duplicate_name_label_rejects_series() {
        let req = request(vec![series(
            vec![label("__name__", "up"), label("__name__", "down")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::DuplicateLabelName("__name__".to_string()),
                count: 1,
            }]
        );
    }

    #[test]
    fn metric_name_too_long_rejects_series() {
        let limits = IngestLimits {
            max_metric_name_len: 3,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "toolong")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::MetricNameTooLong {
                    len: 7,
                    max: 3,
                    count: 1,
                },
                count: 1,
            }]
        );
    }

    // --- duplicate and oversized labels ---

    #[test]
    fn duplicate_label_name_rejects_series() {
        let req = request(vec![series(
            vec![
                label("__name__", "up"),
                label("job", "a"),
                label("job", "b"),
            ],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::DuplicateLabelName("job".to_string()),
                count: 1,
            }]
        );
    }

    #[test]
    fn too_many_attributes_excludes_name_label() {
        let limits = IngestLimits {
            max_attributes_per_point: 1,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "up"), label("a", "1"), label("b", "2")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::TooManyAttributes {
                    attribute_count: 2,
                    max: 1,
                },
                count: 1,
            }]
        );
    }

    #[test]
    fn label_name_too_long_rejects_series() {
        let limits = IngestLimits {
            max_label_name_len: 3,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "up"), label("toolong", "v")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::LabelNameTooLong { len: 7, max: 3 },
                count: 1,
            }]
        );
    }

    #[test]
    fn label_value_too_long_rejects_series() {
        let limits = IngestLimits {
            max_label_value_len: 3,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "up"), label("k", "toolong")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::LabelValueTooLong { len: 7, max: 3 },
                count: 1,
            }]
        );
    }

    // --- timestamps ---

    #[test]
    fn zero_timestamp_rejected() {
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(0, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::ZeroTimestamp,
                count: 1,
            }]
        );
    }

    #[test]
    fn ms_to_ns_overflow_rejected_typed() {
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(i64::MAX, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![RwRejection::TimestampOverflow]);
    }

    #[test]
    fn future_skew_exactly_at_bound_passes() {
        let limits = IngestLimits {
            max_future_skew_ns: 100,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000;
        let event_ts_ns = ingest_ts + 100;
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(event_ts_ns / 1_000_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, ingest_ts);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    }

    #[test]
    fn future_skew_one_ns_past_bound_fails() {
        let limits = IngestLimits {
            max_future_skew_ns: 0,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000_000;
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(ingest_ts / 1_000_000 + 1, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, ingest_ts);
        assert!(out.points.is_empty());
        assert!(matches!(
            out.rejected[0],
            RwRejection::Otlp {
                reason: Rejection::FutureSkew { .. },
                ..
            }
        ));
    }

    #[test]
    fn too_old_one_ns_past_bound_fails() {
        let limits = IngestLimits {
            max_ingest_lag_ns: 0,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000_000;
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(ingest_ts / 1_000_000 - 1, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, ingest_ts);
        assert!(out.points.is_empty());
        assert!(matches!(
            out.rejected[0],
            RwRejection::Otlp {
                reason: Rejection::TooOld { .. },
                ..
            }
        ));
    }

    // --- stale marker bit-pattern round trip ---

    #[test]
    fn stale_marker_nan_bit_pattern_round_trips() {
        const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
        let stale = f64::from_bits(STALE_NAN_BITS);
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, stale)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value.to_bits(), STALE_NAN_BITS);
    }

    #[test]
    fn negative_zero_bit_pattern_round_trips() {
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, -0.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value.to_bits(), (-0.0f64).to_bits());
    }

    // --- native histograms: rejected but scalar samples in the same
    // request still admit ---

    #[test]
    fn native_histogram_series_rejected_typed_and_counted() {
        let mut s = series(vec![label("__name__", "h")], vec![]);
        s.histogram_count = 2;
        let out = normalize_resolved(
            &tenant(),
            request(vec![s]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.histograms_dropped, 2);
        assert_eq!(
            out.rejected,
            vec![RwRejection::NativeHistogramUnsupported { count: 2 }]
        );
    }

    #[test]
    fn native_histogram_and_scalar_samples_in_same_series_are_independent() {
        let mut s = series(vec![label("__name__", "mixed")], vec![sample(1_000, 1.0)]);
        s.histogram_count = 1;
        let out = normalize_resolved(
            &tenant(),
            request(vec![s]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert_eq!(out.points.len(), 1);
        assert_eq!(out.histograms_dropped, 1);
        assert_eq!(
            out.rejected,
            vec![RwRejection::NativeHistogramUnsupported { count: 1 }]
        );
    }

    #[test]
    fn invalid_labels_reject_histogram_entries_too() {
        // No __name__: the whole series (its scalar samples AND its
        // histogram entries) is unidentifiable, so both reject together
        // rather than the histogram counter firing independently.
        let mut s = series(vec![label("job", "svc")], vec![sample(1_000, 1.0)]);
        s.histogram_count = 3;
        let out = normalize_resolved(
            &tenant(),
            request(vec![s]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.histograms_dropped, 0);
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::EmptyMetricName { count: 4 },
                count: 4,
            }]
        );
    }

    // --- exemplars and metadata: accepted-and-dropped, counted ---

    #[test]
    fn exemplars_counted_regardless_of_series_admission() {
        let mut admitted = series(vec![label("__name__", "up")], vec![sample(1_000, 1.0)]);
        admitted.exemplar_count = 2;
        let mut rejected_series = series(vec![label("job", "svc")], vec![sample(1_000, 1.0)]);
        rejected_series.exemplar_count = 1;

        let out = normalize_resolved(
            &tenant(),
            request(vec![admitted, rejected_series]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert_eq!(out.exemplars_dropped, 3);
    }

    #[test]
    fn metadata_dropped_is_request_level_tally() {
        let mut req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, 1.0)],
        )]);
        req.metadata_count = 5;
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert_eq!(out.metadata_dropped, 5);
    }

    #[test]
    fn created_timestamps_dropped_is_always_zero_for_rw1() {
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert_eq!(out.created_timestamps_dropped, 0);
    }

    #[test]
    fn created_timestamps_dropped_mirrors_resolved_tally() {
        let mut req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, 1.0)],
        )]);
        req.created_timestamps_count = 3;
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert_eq!(out.created_timestamps_dropped, 3);
    }

    // --- request-level cap ---

    #[test]
    fn too_many_data_points_rejects_whole_request() {
        let limits = IngestLimits {
            max_data_points_per_request: 2,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, 1.0), sample(2_000, 2.0), sample(3_000, 3.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::TooManyDataPoints { count: 3, max: 2 },
                count: 3,
            }]
        );
    }

    // --- CH-1 cross-protocol identity vector (plan section 4.1) ---

    #[test]
    fn ch1_classic_histogram_series_match_expected_series_ids_and_values() {
        let tenant = TenantId::new("t-fixture");
        let ts_ms = 1_700_000_000_000;
        let base_labels = |extra: &[(&str, &str)]| -> Vec<Label> {
            let mut labels = vec![
                label("__name__", ""), // overwritten per-series below
                label("job", "svc"),
                label("instance", "i-1"),
            ];
            labels.remove(0);
            for (n, v) in extra {
                labels.push(label(n, v));
            }
            labels
        };

        let mut series_list = Vec::new();
        for (le, value) in [("0.1", 1.0), ("1", 3.0), ("10", 6.0), ("+Inf", 10.0)] {
            let mut labels = base_labels(&[("le", le)]);
            labels.push(label("__name__", "http_request_duration_seconds_bucket"));
            series_list.push(series(labels, vec![sample(ts_ms, value)]));
        }
        for (suffix, value) in [("_sum", 42.5), ("_count", 10.0)] {
            let mut labels = base_labels(&[]);
            labels.push(label(
                "__name__",
                &format!("http_request_duration_seconds{suffix}"),
            ));
            series_list.push(series(labels, vec![sample(ts_ms, value)]));
        }

        let out = normalize_resolved(
            &tenant,
            request(series_list),
            &IngestLimits::default(),
            ts_ms * 1_000_000 + 1,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 6);

        let expect_id = |metric_name: &str, extra: &[(&str, &str)]| {
            let mut labels = vec![label("job", "svc"), label("instance", "i-1")];
            for (n, v) in extra {
                labels.push(label(n, v));
            }
            SeriesId::compute(&tenant, metric_name, &LabelSet::new(labels).expect("valid"))
                .expect("id")
        };

        let bucket_ids: Vec<_> = [("0.1", 1.0), ("1", 3.0), ("10", 6.0), ("+Inf", 10.0)]
            .iter()
            .map(|(le, _)| expect_id("http_request_duration_seconds_bucket", &[("le", le)]))
            .collect();
        let sum_id = expect_id("http_request_duration_seconds_sum", &[]);
        let count_id = expect_id("http_request_duration_seconds_count", &[]);

        for (i, (_, expected_value)) in [("0.1", 1.0), ("1", 3.0), ("10", 6.0), ("+Inf", 10.0)]
            .iter()
            .enumerate()
        {
            assert_eq!(out.points[i].series_id, bucket_ids[i]);
            assert_eq!(out.points[i].sample.value, *expected_value);
        }
        assert_eq!(out.points[4].series_id, sum_id);
        assert_eq!(out.points[4].sample.value, 42.5);
        assert_eq!(out.points[5].series_id, count_id);
        assert_eq!(out.points[5].sample.value, 10.0);
    }
}
