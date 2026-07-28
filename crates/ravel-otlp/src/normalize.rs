//! Normalization from a decoded OTLP `ExportMetricsServiceRequest` into
//! Ravel's canonical metric point representation (ADR-0005;
//! docs/architecture.md).
//!
//! Scope: `Gauge` and cumulative `Sum` metrics with `NumberDataPoint` values
//! (`as_int` and `as_double`; integers convert to `f64`, and the segment
//! format stores `f64` only, so an `as_int` value whose magnitude exceeds
//! 2^53 is rounded to the nearest representable `f64`. That rounding is not
//! silent: such a point is still admitted, but it also emits an informational
//! [`Rejection::IntegerValuePrecisionLoss`] carrying the original integer, so
//! the loss is observable per the "approximation is opt-in and visible"
//! invariant); cumulative
//! `Histogram` (explicit bucket bounds) and `Summary` metrics, exploded into
//! Prometheus-convention scalar series per ADR-0016 (`{name}_bucket{le=...}`,
//! `{name}_sum`, `{name}_count`, `{name}{quantile=...}`). `Histogram` and
//! `Sum` reject non-cumulative temporality typed
//! ([`Rejection::UnsupportedTemporality`]); delta-to-cumulative conversion
//! needs cross-request state, which stateless compute forbids.
//! `ExponentialHistogram` is never silently dropped: it produces
//! [`Rejection::UnsupportedMetricType`] carrying the number of data points
//! skipped, pending ADR-0017.
//!
//! Label mapping follows the standard OTel-to-Prometheus convention:
//! `resource.attributes["service.name"]` (namespaced by
//! `service.namespace` when present) becomes the `job` label,
//! `service.instance.id` becomes `instance`, and a configurable allowlist of
//! other resource attributes is flattened through the same label-name
//! sanitization applied to data-point attributes (dots and every other
//! character outside a valid Prometheus label name become `_`).
//! Instrumentation scope (name, version, attributes) is
//! ignored for series identity in Phase 1; this is a deliberate
//! simplification, revisited when native OTel querying lands (ADR-0005).
//!
//! Every accepted point is atomic: a data point either becomes one
//! [`NormalizedPoint`] with a complete, duplicate-free label set, or it is
//! rejected wholesale with a single [`Rejection`]. Nothing is partially
//! labeled.
//!
//! Sanitization replaces each disallowed character with `_` in place; it
//! does not prefix a leading digit. This means two distinct source names
//! that differ only in a leading digit alias to the same sanitized name
//! (`1foo` and `_foo` both become `_foo`) and therefore the same series.
//! This matches the letter of the OTel-to-Prometheus convention and is
//! intentional, not an oversight; callers who need collision-free names
//! for adversarial input should pre-validate before calling in.

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, DataPointFlags, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, SummaryDataPoint, metric::Data as MetricData,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId, TypeError};

use crate::limits::{IngestLimits, Rejection};
use crate::promcompat::format_float;

/// Prometheus stale marker: a NaN with this exact bit pattern (upstream
/// convention). Duplicated from `ravel-promql` rather than imported: an
/// ingest crate must not depend on the query-engine crate for one constant.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN_BITS)
}

/// `DataPointFlags::NoRecordedValueMask` maps to a Prometheus stale marker
/// on the point's sample(s), matching the collector's mapping, so staleness
/// semantics survive the OTLP-to-Prometheus boundary (ADR-0016).
fn has_no_recorded_value(flags: u32) -> bool {
    flags & DataPointFlags::NoRecordedValueMask as u32 != 0
}

/// One admitted OTLP data point, normalized to Ravel's canonical shape.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedPoint {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub sample: Sample,
    /// `true` only for points from a `Sum` metric with `is_monotonic` set;
    /// always `false` for `Gauge` points.
    pub is_monotonic_sum: bool,
}

/// Result of normalizing one `ExportMetricsServiceRequest`.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizeOutput {
    pub points: Vec<NormalizedPoint>,
    pub rejected: Vec<Rejection>,
}

/// Decode and normalize gauge and sum data points from `req`.
///
/// `ingest_ts_ns` is the receiver's clock reading at admission time, used to
/// bound event-time skew (ADR-0010 §8). Nothing here panics or returns an
/// error for malformed or oversized input: every problem becomes a
/// [`Rejection`] so the caller can build an OTLP partial-success response.
pub fn normalize_metrics(
    tenant: &TenantId,
    req: ExportMetricsServiceRequest,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
) -> NormalizeOutput {
    let total_points = count_data_points(&req);
    if total_points > limits.max_data_points_per_request {
        return NormalizeOutput {
            points: Vec::new(),
            rejected: vec![Rejection::TooManyDataPoints {
                count: total_points,
                max: limits.max_data_points_per_request,
            }],
        };
    }

    let mut points = Vec::new();
    let mut rejected = Vec::new();

    for rm in &req.resource_metrics {
        normalize_resource(tenant, rm, limits, ingest_ts_ns, &mut points, &mut rejected);
    }

    NormalizeOutput { points, rejected }
}

fn count_data_points(req: &ExportMetricsServiceRequest) -> usize {
    req.resource_metrics
        .iter()
        .map(resource_metrics_point_count)
        .sum()
}

fn resource_metrics_point_count(rm: &ResourceMetrics) -> usize {
    rm.scope_metrics
        .iter()
        .flat_map(|sm| sm.metrics.iter())
        .map(metric_data_point_count)
        .sum()
}

fn metric_data_point_count(metric: &Metric) -> usize {
    match &metric.data {
        Some(MetricData::Gauge(g)) => g.data_points.len(),
        Some(MetricData::Sum(s)) => s.data_points.len(),
        Some(MetricData::Histogram(h)) => h.data_points.len(),
        Some(MetricData::ExponentialHistogram(h)) => h.data_points.len(),
        Some(MetricData::Summary(s)) => s.data_points.len(),
        None => 0,
    }
}

fn normalize_resource(
    tenant: &TenantId,
    rm: &ResourceMetrics,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    points: &mut Vec<NormalizedPoint>,
    rejected: &mut Vec<Rejection>,
) {
    let resource_point_count = resource_metrics_point_count(rm);
    if resource_point_count == 0 {
        return;
    }

    let resource = rm.resource.as_ref();
    let resource_attr_count = resource.map_or(0, |r| r.attributes.len());
    if resource_attr_count > limits.max_resource_attributes {
        rejected.push(Rejection::TooManyResourceAttributes {
            count: resource_point_count,
            max: limits.max_resource_attributes,
        });
        return;
    }

    let resource_labels = match build_resource_labels(resource, limits) {
        Ok(labels) => labels,
        Err(reason) => {
            rejected.extend(std::iter::repeat_n(reason, resource_point_count));
            return;
        }
    };

    for sm in &rm.scope_metrics {
        for metric in &sm.metrics {
            normalize_metric(
                tenant,
                metric,
                &resource_labels,
                limits,
                ingest_ts_ns,
                points,
                rejected,
            );
        }
    }
}

fn normalize_metric(
    tenant: &TenantId,
    metric: &Metric,
    resource_labels: &[Label],
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    points: &mut Vec<NormalizedPoint>,
    rejected: &mut Vec<Rejection>,
) {
    let point_count = metric_data_point_count(metric);
    if point_count == 0 {
        return;
    }

    if metric.name.len() > limits.max_metric_name_len {
        rejected.push(Rejection::MetricNameTooLong {
            len: metric.name.len(),
            max: limits.max_metric_name_len,
            count: point_count,
        });
        return;
    }

    let name = sanitize_metric_name(&metric.name);
    if name.is_empty() {
        rejected.push(Rejection::EmptyMetricName { count: point_count });
        return;
    }

    match &metric.data {
        Some(MetricData::Gauge(gauge)) => {
            let ctx = PointContext {
                tenant,
                metric_name: &name,
                resource_labels,
                limits,
                ingest_ts_ns,
                is_sum: false,
                is_monotonic: false,
            };
            let mut memo = SeriesIdMemo::new();
            for dp in &gauge.data_points {
                match build_point(&ctx, dp, &mut memo) {
                    Ok((p, info)) => {
                        points.push(p);
                        rejected.extend(info);
                    }
                    Err(r) => rejected.push(r),
                }
            }
        }
        Some(MetricData::Sum(sum)) => {
            let temporality = AggregationTemporality::try_from(sum.aggregation_temporality)
                .unwrap_or(AggregationTemporality::Unspecified);
            if temporality != AggregationTemporality::Cumulative {
                rejected.push(Rejection::UnsupportedTemporality {
                    count: sum.data_points.len(),
                });
                return;
            }
            let ctx = PointContext {
                tenant,
                metric_name: &name,
                resource_labels,
                limits,
                ingest_ts_ns,
                is_sum: true,
                is_monotonic: sum.is_monotonic,
            };
            let mut memo = SeriesIdMemo::new();
            for dp in &sum.data_points {
                match build_point(&ctx, dp, &mut memo) {
                    Ok((p, info)) => {
                        points.push(p);
                        rejected.extend(info);
                    }
                    Err(r) => rejected.push(r),
                }
            }
        }
        Some(MetricData::Histogram(h)) => {
            let temporality = AggregationTemporality::try_from(h.aggregation_temporality)
                .unwrap_or(AggregationTemporality::Unspecified);
            if temporality != AggregationTemporality::Cumulative {
                rejected.push(Rejection::UnsupportedTemporality {
                    count: h.data_points.len(),
                });
                return;
            }
            let ctx = ExplodeContext {
                tenant,
                metric_name: &name,
                resource_labels,
                limits,
                ingest_ts_ns,
            };
            let mut memo = SeriesIdMemo::new();
            for dp in &h.data_points {
                match explode_histogram(&ctx, dp, &mut memo) {
                    Ok((mut new_points, informational)) => {
                        points.append(&mut new_points);
                        rejected.extend(informational);
                    }
                    Err(r) => rejected.push(r),
                }
            }
        }
        Some(MetricData::ExponentialHistogram(h)) => {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: "exponential_histogram",
                count: h.data_points.len(),
            });
        }
        Some(MetricData::Summary(s)) => {
            let ctx = ExplodeContext {
                tenant,
                metric_name: &name,
                resource_labels,
                limits,
                ingest_ts_ns,
            };
            let mut memo = SeriesIdMemo::new();
            for dp in &s.data_points {
                match explode_summary(&ctx, dp, &mut memo) {
                    Ok(mut new_points) => points.append(&mut new_points),
                    Err(r) => rejected.push(r),
                }
            }
        }
        None => {}
    }
}

/// Per-metric context shared by every data point of an exploded `Histogram`
/// or `Summary`, mirroring [`PointContext`] for the multi-series case.
struct ExplodeContext<'a> {
    tenant: &'a TenantId,
    metric_name: &'a str,
    resource_labels: &'a [Label],
    limits: &'a IngestLimits,
    ingest_ts_ns: i64,
}

/// Per-metric context shared by every data point in a `Gauge` or `Sum`,
/// bundled so `build_point` takes two arguments instead of eight.
struct PointContext<'a> {
    tenant: &'a TenantId,
    metric_name: &'a str,
    resource_labels: &'a [Label],
    limits: &'a IngestLimits,
    ingest_ts_ns: i64,
    is_sum: bool,
    is_monotonic: bool,
}

/// Last-seen memo for `SeriesId::compute`, scoped to one `Metric`.
///
/// Within a metric `tenant` and `metric_name` are constant, and the built
/// `LabelSet` (which carries `__name__`) fully determines the canonical
/// series identity (ADR-0005), so an equal `LabelSet` always yields a
/// bit-identical `SeriesId`. The realistic OTLP shape is a single series
/// sampled over time: one metric holding many data points with identical
/// attributes, emitted consecutively. A one-entry last-seen memo turns the
/// per-point BLAKE3 hash into one pointer compare across such a run while
/// staying correct for interleaved or single-point metrics (a mismatch just
/// recomputes). Fixed capacity of one entry; dropped when the metric's loop
/// ends, so memory is bounded to a single label set.
struct SeriesIdMemo {
    last: Option<(LabelSet, SeriesId)>,
}

impl SeriesIdMemo {
    fn new() -> Self {
        SeriesIdMemo { last: None }
    }

    /// Return the id for `labels`, reusing the last computation when the
    /// label set is unchanged and otherwise computing and caching it. The
    /// returned id is identical to calling `SeriesId::compute` directly.
    fn series_id(
        &mut self,
        tenant: &TenantId,
        metric_name: &str,
        labels: &LabelSet,
    ) -> Result<SeriesId, TypeError> {
        if let Some((last_labels, last_id)) = &self.last
            && last_labels == labels
        {
            return Ok(*last_id);
        }
        let id = SeriesId::compute(tenant, metric_name, labels)?;
        self.last = Some((labels.clone(), id));
        Ok(id)
    }
}

/// Whether an OTLP `as_int` value survives conversion to the `f64` the
/// segment format stores. `f64` carries a 53-bit mantissa, so every integer
/// with magnitude up to 2^53 is exact and some larger even integers are too.
/// The `i128` round-trip is exact across the whole `i64` range: a bare
/// `as i64` round-trip would saturate for values that round up to 2^63 at the
/// top of the range and so falsely accept a rounded value.
fn int_survives_f64(v: i64) -> bool {
    (v as f64) as i128 == v as i128
}

/// Build one point, and, when the point's `as_int` value did not survive the
/// conversion to `f64`, an informational [`Rejection::IntegerValuePrecisionLoss`]
/// to carry alongside it (`None` otherwise). The point is admitted either way;
/// the rounding is unavoidable given `f64`-only storage, so it is surfaced, not
/// rejected.
fn build_point(
    ctx: &PointContext,
    dp: &NumberDataPoint,
    memo: &mut SeriesIdMemo,
) -> Result<(NormalizedPoint, Option<Rejection>), Rejection> {
    if dp.attributes.len() > ctx.limits.max_attributes_per_point {
        return Err(Rejection::TooManyAttributes {
            attribute_count: dp.attributes.len(),
            max: ctx.limits.max_attributes_per_point,
        });
    }

    let event_ts_ns = checked_event_ts(dp.time_unix_nano, ctx.ingest_ts_ns, ctx.limits)?;

    // A NoRecordedValue point still carries a (usually meaningless) value in
    // the wire message; matching the collector's mapping, the sample value
    // is unconditionally replaced by the stale marker and the underlying
    // oneof is never consulted, so a point with no value set can still
    // signal staleness.
    // A NoRecordedValue point never consults the oneof, so a large `as_int`
    // there is discarded, not rounded, and reports no precision loss.
    let mut precision_loss = None;
    let value = if has_no_recorded_value(dp.flags) {
        stale_nan()
    } else {
        match dp.value {
            Some(NumberValue::AsDouble(v)) => v,
            Some(NumberValue::AsInt(v)) => {
                if !int_survives_f64(v) {
                    precision_loss = Some(Rejection::IntegerValuePrecisionLoss { value: v });
                }
                v as f64
            }
            None => return Err(Rejection::MissingValue),
        }
    };

    let mut labels = Vec::with_capacity(ctx.resource_labels.len() + dp.attributes.len() + 1);
    labels.extend_from_slice(ctx.resource_labels);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: ctx.metric_name.to_string(),
    });
    push_attribute_labels(&mut labels, &dp.attributes, ctx.limits)?;

    let label_set = LabelSet::new(labels).map_err(|err| match err {
        TypeError::DuplicateLabelName(name) => Rejection::DuplicateLabelName(name),
        // LabelSet::new only ever returns DuplicateLabelName; this arm
        // exists so a future TypeError variant can't silently pass through
        // as an accepted point.
        _ => Rejection::DuplicateLabelName(String::new()),
    })?;

    let series_id = memo
        .series_id(ctx.tenant, ctx.metric_name, &label_set)
        .map_err(|_| Rejection::OversizedSeriesComponent)?;

    Ok((
        NormalizedPoint {
            series_id,
            labels: label_set,
            sample: Sample {
                ts_ns: event_ts_ns,
                value,
            },
            is_monotonic_sum: ctx.is_sum && ctx.is_monotonic,
        },
        precision_loss,
    ))
}

/// Validate and resolve a data point's event timestamp: zero rejects
/// outright, then the result is bounded to `[ingest_ts_ns - max_ingest_lag_ns,
/// ingest_ts_ns + max_future_skew_ns]` (ADR-0010 §8, docs/consistency-model.md
/// "Late and skewed data"). The bound itself passes; only strictly exceeding
/// it rejects: `event_ts == ingest_ts + max_future_skew` is accepted,
/// `event_ts == ingest_ts + max_future_skew + 1` is not.
fn checked_event_ts(
    time_unix_nano: u64,
    ingest_ts_ns: i64,
    limits: &IngestLimits,
) -> Result<i64, Rejection> {
    // time_unix_nano is u64; real event times fit comfortably in i64, and a
    // value that doesn't is far outside any sane admission window, so it
    // saturates rather than wrapping negative.
    let event_ts_ns = i64::try_from(time_unix_nano).unwrap_or(i64::MAX);
    if event_ts_ns == 0 {
        return Err(Rejection::ZeroTimestamp);
    }

    let skew_ns = event_ts_ns.saturating_sub(ingest_ts_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(Rejection::FutureSkew {
            skew_ns,
            max_ns: limits.max_future_skew_ns,
        });
    }
    let lag_ns = ingest_ts_ns.saturating_sub(event_ts_ns);
    if lag_ns > limits.max_ingest_lag_ns {
        return Err(Rejection::TooOld {
            lag_ns,
            max_ns: limits.max_ingest_lag_ns,
        });
    }
    Ok(event_ts_ns)
}

/// Sanitize and push each attribute as a label, checking the same
/// name/value length limits applied to every other label.
fn push_attribute_labels(
    labels: &mut Vec<Label>,
    attributes: &[KeyValue],
    limits: &IngestLimits,
) -> Result<(), Rejection> {
    for attr in attributes {
        let name = sanitize_label_name(&attr.key);
        let value = any_value_to_label_value(attr.value.as_ref())?;
        push_checked(labels, name, value, limits)?;
    }
    Ok(())
}

/// Build the resource-plus-attribute label prefix shared by every series
/// exploded from one `Histogram`/`Summary` data point (everything but the
/// `__name__` label and the synthesized `le`/`quantile` label, which differ
/// per series).
fn build_explode_base_labels(
    ctx: &ExplodeContext,
    attributes: &[KeyValue],
) -> Result<Vec<Label>, Rejection> {
    if attributes.len() > ctx.limits.max_attributes_per_point {
        return Err(Rejection::TooManyAttributes {
            attribute_count: attributes.len(),
            max: ctx.limits.max_attributes_per_point,
        });
    }
    let mut labels = Vec::with_capacity(ctx.resource_labels.len() + attributes.len());
    labels.extend_from_slice(ctx.resource_labels);
    push_attribute_labels(&mut labels, attributes, ctx.limits)?;
    Ok(labels)
}

/// Finish one exploded series: attach `__name__` and an optional `le`/
/// `quantile` label to the shared base labels, then build the label set and
/// resolve its series id exactly like [`build_point`] does for gauge/sum.
fn finish_point(
    ctx: &ExplodeContext,
    memo: &mut SeriesIdMemo,
    base_labels: &[Label],
    metric_name: &str,
    extra_label: Option<(&'static str, String)>,
    ts_ns: i64,
    value: f64,
) -> Result<NormalizedPoint, Rejection> {
    let mut labels = Vec::with_capacity(base_labels.len() + 2);
    labels.extend_from_slice(base_labels);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric_name.to_string(),
    });
    if let Some((name, value)) = extra_label {
        push_checked(&mut labels, name.to_string(), value, ctx.limits)?;
    }

    let label_set = LabelSet::new(labels).map_err(|err| match err {
        TypeError::DuplicateLabelName(name) => Rejection::DuplicateLabelName(name),
        _ => Rejection::DuplicateLabelName(String::new()),
    })?;

    let series_id = memo
        .series_id(ctx.tenant, metric_name, &label_set)
        .map_err(|_| Rejection::OversizedSeriesComponent)?;

    Ok(NormalizedPoint {
        series_id,
        labels: label_set,
        sample: Sample { ts_ns, value },
        // Whether an exploded bucket/sum/count series behaves like a
        // monotonic counter downstream is not decided by this ticket
        // (docs/ingest-breadth-plan.md §7: "no change to is_monotonic_sum
        // handling"); the field is carried but never consumed today.
        is_monotonic_sum: false,
    })
}

/// Explode one `HistogramDataPoint` into its Prometheus-convention series:
/// one `{name}_bucket{le=<bound>}` per explicit bound plus
/// `{name}_bucket{le="+Inf"}` (= the point's count), `{name}_sum` when `sum`
/// is present, and `{name}_count`. Rejection is atomic: an `Err` means none
/// of this point's series were admitted, never a partial set (ADR-0016).
/// Ok also carries zero-weight informational rejections for dropped
/// min/max/exemplar fields, since the point itself was admitted.
fn explode_histogram(
    ctx: &ExplodeContext,
    dp: &HistogramDataPoint,
    memo: &mut SeriesIdMemo,
) -> Result<(Vec<NormalizedPoint>, Vec<Rejection>), Rejection> {
    let event_ts_ns = checked_event_ts(dp.time_unix_nano, ctx.ingest_ts_ns, ctx.limits)?;

    let expected_buckets = dp.explicit_bounds.len() + 1;
    if dp.bucket_counts.len() != expected_buckets {
        return Err(Rejection::HistogramBucketCountMismatch {
            bounds: dp.explicit_bounds.len(),
            buckets: dp.bucket_counts.len(),
            expected: expected_buckets,
        });
    }
    if dp.explicit_bounds.iter().any(|b| !b.is_finite()) {
        return Err(Rejection::NonFiniteHistogramBound);
    }
    if !dp.explicit_bounds.windows(2).all(|w| w[0] < w[1]) {
        return Err(Rejection::HistogramBoundsNotIncreasing);
    }

    let base_labels = build_explode_base_labels(ctx, &dp.attributes)?;
    let stale = has_no_recorded_value(dp.flags);
    let bucket_name = format!("{}_bucket", ctx.metric_name);

    let mut series = Vec::with_capacity(expected_buckets + 2);
    let mut cumulative: u64 = 0;
    for (bound, count) in dp.explicit_bounds.iter().zip(&dp.bucket_counts) {
        cumulative = cumulative
            .checked_add(*count)
            .ok_or(Rejection::HistogramCountOverflow)?;
        let value = if stale {
            stale_nan()
        } else {
            cumulative as f64
        };
        series.push(finish_point(
            ctx,
            memo,
            &base_labels,
            &bucket_name,
            Some(("le", format_float(*bound))),
            event_ts_ns,
            value,
        )?);
    }

    // The +Inf bucket and _count both use the point's raw count directly,
    // not the accumulated bucket sum (matches the collector's mapping).
    let count_value = if stale { stale_nan() } else { dp.count as f64 };
    series.push(finish_point(
        ctx,
        memo,
        &base_labels,
        &bucket_name,
        Some(("le", "+Inf".to_string())),
        event_ts_ns,
        count_value,
    )?);

    if let Some(sum) = dp.sum {
        let sum_value = if stale { stale_nan() } else { sum };
        let sum_name = format!("{}_sum", ctx.metric_name);
        series.push(finish_point(
            ctx,
            memo,
            &base_labels,
            &sum_name,
            None,
            event_ts_ns,
            sum_value,
        )?);
    }

    let count_name = format!("{}_count", ctx.metric_name);
    series.push(finish_point(
        ctx,
        memo,
        &base_labels,
        &count_name,
        None,
        event_ts_ns,
        count_value,
    )?);

    let mut informational = Vec::new();
    if dp.min.is_some() || dp.max.is_some() {
        informational.push(Rejection::HistogramMinMaxDropped { count: 1 });
    }
    if !dp.exemplars.is_empty() {
        informational.push(Rejection::HistogramExemplarsDropped {
            count: dp.exemplars.len(),
        });
    }

    Ok((series, informational))
}

/// Explode one `SummaryDataPoint` into its Prometheus-convention series:
/// one `{name}{quantile=<q>}` per quantile plus `{name}_sum` and
/// `{name}_count`. Rejection is atomic, same as [`explode_histogram`].
fn explode_summary(
    ctx: &ExplodeContext,
    dp: &SummaryDataPoint,
    memo: &mut SeriesIdMemo,
) -> Result<Vec<NormalizedPoint>, Rejection> {
    let event_ts_ns = checked_event_ts(dp.time_unix_nano, ctx.ingest_ts_ns, ctx.limits)?;

    let mut seen_quantiles = std::collections::HashSet::with_capacity(dp.quantile_values.len());
    for qv in &dp.quantile_values {
        if !qv.quantile.is_finite() {
            return Err(Rejection::NonFiniteQuantile);
        }
        if !seen_quantiles.insert(qv.quantile.to_bits()) {
            return Err(Rejection::DuplicateQuantile);
        }
    }

    let base_labels = build_explode_base_labels(ctx, &dp.attributes)?;
    let stale = has_no_recorded_value(dp.flags);

    let mut series = Vec::with_capacity(dp.quantile_values.len() + 2);
    for qv in &dp.quantile_values {
        let value = if stale { stale_nan() } else { qv.value };
        series.push(finish_point(
            ctx,
            memo,
            &base_labels,
            ctx.metric_name,
            Some(("quantile", format_float(qv.quantile))),
            event_ts_ns,
            value,
        )?);
    }

    let sum_value = if stale { stale_nan() } else { dp.sum };
    let sum_name = format!("{}_sum", ctx.metric_name);
    series.push(finish_point(
        ctx,
        memo,
        &base_labels,
        &sum_name,
        None,
        event_ts_ns,
        sum_value,
    )?);

    let count_value = if stale { stale_nan() } else { dp.count as f64 };
    let count_name = format!("{}_count", ctx.metric_name);
    series.push(finish_point(
        ctx,
        memo,
        &base_labels,
        &count_name,
        None,
        event_ts_ns,
        count_value,
    )?);

    Ok(series)
}

fn build_resource_labels(
    resource: Option<&Resource>,
    limits: &IngestLimits,
) -> Result<Vec<Label>, Rejection> {
    let Some(resource) = resource else {
        return Ok(Vec::new());
    };

    let mut labels = Vec::new();

    let service_name = find_attr_value(&resource.attributes, "service.name")?;
    let service_namespace = find_attr_value(&resource.attributes, "service.namespace")?;
    if let Some(name) = service_name {
        let job = match service_namespace {
            Some(ns) if !ns.is_empty() => format!("{ns}/{name}"),
            _ => name,
        };
        push_checked(&mut labels, "job".to_string(), job, limits)?;
    }

    if let Some(instance) = find_attr_value(&resource.attributes, "service.instance.id")? {
        push_checked(&mut labels, "instance".to_string(), instance, limits)?;
    }

    for key in &limits.resource_attribute_allowlist {
        if matches!(
            key.as_str(),
            "service.name" | "service.namespace" | "service.instance.id"
        ) {
            continue;
        }
        if let Some(value) = find_attr_value(&resource.attributes, key)? {
            push_checked(&mut labels, sanitize_label_name(key), value, limits)?;
        }
    }

    Ok(labels)
}

/// Push a resource-derived label after enforcing the same length limits
/// applied to data-point attributes. `job` in particular is attacker
/// influenced (`service.namespace`/`service.name` are free-form strings) and
/// its value is a `format!` join, so it must be checked after joining, not
/// before: two under-limit halves can still produce an over-limit label.
fn push_checked(
    labels: &mut Vec<Label>,
    name: String,
    value: String,
    limits: &IngestLimits,
) -> Result<(), Rejection> {
    if name.len() > limits.max_label_name_len {
        return Err(Rejection::LabelNameTooLong {
            len: name.len(),
            max: limits.max_label_name_len,
        });
    }
    if value.len() > limits.max_label_value_len {
        return Err(Rejection::LabelValueTooLong {
            len: value.len(),
            max: limits.max_label_value_len,
        });
    }
    labels.push(Label { name, value });
    Ok(())
}

fn find_attr_value(attrs: &[KeyValue], key: &str) -> Result<Option<String>, Rejection> {
    match attrs.iter().find(|kv| kv.key == key) {
        None => Ok(None),
        Some(kv) => any_value_to_label_value(kv.value.as_ref()).map(Some),
    }
}

/// string verbatim; bool/int/double canonical strings; kvlist/array/bytes
/// have no label representation and reject the point (or, for a resource
/// attribute, every point under that resource).
fn any_value_to_label_value(value: Option<&AnyValue>) -> Result<String, Rejection> {
    match value.and_then(|v| v.value.as_ref()) {
        None => Ok(String::new()),
        Some(AnyValueVariant::StringValue(s)) => Ok(s.clone()),
        Some(AnyValueVariant::BoolValue(b)) => Ok(b.to_string()),
        Some(AnyValueVariant::IntValue(i)) => Ok(i.to_string()),
        Some(AnyValueVariant::DoubleValue(d)) => Ok(d.to_string()),
        Some(AnyValueVariant::ArrayValue(_))
        | Some(AnyValueVariant::KvlistValue(_))
        | Some(AnyValueVariant::BytesValue(_))
        | Some(AnyValueVariant::StringValueStrindex(_)) => Err(Rejection::ComplexAttributeValue),
    }
}

fn sanitize_metric_name(name: &str) -> String {
    sanitize(name, is_metric_name_start, is_metric_name_continue)
}

fn sanitize_label_name(name: &str) -> String {
    sanitize(name, is_label_name_start, is_label_name_continue)
}

fn sanitize(input: &str, is_start: fn(char) -> bool, is_continue: fn(char) -> bool) -> String {
    input
        .chars()
        .enumerate()
        .map(|(i, c)| {
            let allowed = if i == 0 { is_start(c) } else { is_continue(c) };
            if allowed { c } else { '_' }
        })
        .collect()
}

fn is_metric_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == ':'
}

fn is_metric_name_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

fn is_label_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_label_name_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::{
        Exemplar, Gauge, Histogram, ScopeMetrics, Sum, Summary, summary_data_point,
    };
    use std::collections::HashSet;

    fn tenant() -> TenantId {
        TenantId::new("acme")
    }

    fn string_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    fn int_kv(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::IntValue(value)),
            }),
            ..Default::default()
        }
    }

    fn bool_kv(key: &str, value: bool) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::BoolValue(value)),
            }),
            ..Default::default()
        }
    }

    fn bytes_kv(key: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::BytesValue(vec![1, 2, 3])),
            }),
            ..Default::default()
        }
    }

    fn number_point(attrs: Vec<KeyValue>, ts_ns: i64, value: NumberValue) -> NumberDataPoint {
        NumberDataPoint {
            attributes: attrs,
            time_unix_nano: ts_ns as u64,
            value: Some(value),
            ..Default::default()
        }
    }

    fn gauge_metric(name: &str, points: Vec<NumberDataPoint>) -> Metric {
        Metric {
            name: name.to_string(),
            data: Some(MetricData::Gauge(Gauge {
                data_points: points,
            })),
            ..Default::default()
        }
    }

    fn sum_metric(
        name: &str,
        points: Vec<NumberDataPoint>,
        temporality: AggregationTemporality,
        is_monotonic: bool,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            data: Some(MetricData::Sum(Sum {
                data_points: points,
                aggregation_temporality: temporality as i32,
                is_monotonic,
            })),
            ..Default::default()
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn histogram_point(
        attrs: Vec<KeyValue>,
        ts_ns: i64,
        count: u64,
        sum: Option<f64>,
        bounds: Vec<f64>,
        bucket_counts: Vec<u64>,
    ) -> HistogramDataPoint {
        HistogramDataPoint {
            attributes: attrs,
            time_unix_nano: ts_ns as u64,
            count,
            sum,
            bucket_counts,
            explicit_bounds: bounds,
            ..Default::default()
        }
    }

    fn histogram_metric(
        name: &str,
        points: Vec<HistogramDataPoint>,
        temporality: AggregationTemporality,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            data: Some(MetricData::Histogram(Histogram {
                data_points: points,
                aggregation_temporality: temporality as i32,
            })),
            ..Default::default()
        }
    }

    fn value_at_quantile(quantile: f64, value: f64) -> summary_data_point::ValueAtQuantile {
        summary_data_point::ValueAtQuantile { quantile, value }
    }

    fn summary_point(
        attrs: Vec<KeyValue>,
        ts_ns: i64,
        count: u64,
        sum: f64,
        quantiles: Vec<summary_data_point::ValueAtQuantile>,
    ) -> SummaryDataPoint {
        SummaryDataPoint {
            attributes: attrs,
            time_unix_nano: ts_ns as u64,
            count,
            sum,
            quantile_values: quantiles,
            ..Default::default()
        }
    }

    fn summary_metric(name: &str, points: Vec<SummaryDataPoint>) -> Metric {
        Metric {
            name: name.to_string(),
            data: Some(MetricData::Summary(Summary {
                data_points: points,
            })),
            ..Default::default()
        }
    }

    fn resource_metrics(resource_attrs: Vec<KeyValue>, metrics: Vec<Metric>) -> ResourceMetrics {
        ResourceMetrics {
            resource: Some(Resource {
                attributes: resource_attrs,
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn request(resources: Vec<ResourceMetrics>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: resources,
        }
    }

    // --- empty request ---

    #[test]
    fn empty_request_yields_empty_output() {
        let out = normalize_metrics(
            &tenant(),
            request(vec![]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert!(out.points.is_empty());
        assert!(out.rejected.is_empty());
    }

    // --- mapping table: job/instance synthesis ---

    #[test]
    fn job_synthesized_with_namespace() {
        let rm = resource_metrics(
            vec![
                string_kv("service.name", "checkout"),
                string_kv("service.namespace", "payments"),
                string_kv("service.instance.id", "pod-1"),
            ],
            vec![gauge_metric(
                "requests",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 1);
        let labels = &out.points[0].labels;
        assert_eq!(labels.get("job"), Some("payments/checkout"));
        assert_eq!(labels.get("instance"), Some("pod-1"));
        assert_eq!(labels.get(METRIC_NAME_LABEL), Some("requests"));
    }

    #[test]
    fn job_synthesized_without_namespace() {
        let rm = resource_metrics(
            vec![string_kv("service.name", "checkout")],
            vec![gauge_metric(
                "requests",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].labels.get("job"), Some("checkout"));
        assert_eq!(out.points[0].labels.get("instance"), None);
    }

    #[test]
    fn no_job_label_without_service_name() {
        // namespace alone, no service.name: nothing to synthesize job from.
        let rm = resource_metrics(
            vec![string_kv("service.namespace", "payments")],
            vec![gauge_metric(
                "requests",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].labels.get("job"), None);
    }

    // --- allowlist flattening ---

    #[test]
    fn allowlisted_resource_attributes_flatten_with_dots_to_underscores() {
        let rm = resource_metrics(
            vec![
                string_kv("service.name", "svc"),
                string_kv("k8s.pod.name", "pod-abc"),
                string_kv("host.name", "node1"),
                // not in the default allowlist: must not appear as a label.
                string_kv("some.other.attr", "ignored"),
            ],
            vec![gauge_metric(
                "up",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        let labels = &out.points[0].labels;
        assert_eq!(labels.get("job"), Some("svc"));
        assert_eq!(labels.get("k8s_pod_name"), Some("pod-abc"));
        assert_eq!(labels.get("host_name"), Some("node1"));
        assert_eq!(labels.get("some_other_attr"), None);
    }

    #[test]
    fn allowlisted_resource_attribute_name_is_sanitized_like_metric_attributes() {
        // Regression for a8-F08. An allowlist entry that is invalid as a
        // Prometheus label name (leading digit, plus a dash beyond the dots)
        // must pass through sanitize_label_name, not merely have its dots
        // replaced. The sanitized name must equal the one the data-point
        // attribute path produces for the same key, and it must be what
        // reaches the persistent series identity.
        let limits = IngestLimits {
            resource_attribute_allowlist: vec!["0bad-key.name".to_string()],
            ..IngestLimits::default()
        };

        // Same logical label carried as a resource attribute (allowlist path).
        let via_resource = resource_metrics(
            vec![string_kv("0bad-key.name", "v")],
            vec![gauge_metric(
                "up",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out_resource =
            normalize_metrics(&tenant(), request(vec![via_resource]), &limits, 1_000);
        assert!(
            out_resource.rejected.is_empty(),
            "{:?}",
            out_resource.rejected
        );
        let resource_labels = &out_resource.points[0].labels;
        // sanitize_label_name maps a leading digit to '_' and '-' to '_'.
        assert_eq!(resource_labels.get("_bad_key_name"), Some("v"));
        // The old dots-only replacement left an invalid name; it must be gone.
        assert_eq!(resource_labels.get("0bad-key_name"), None);

        // The same key as a data-point attribute already takes the
        // sanitize_label_name path; it must produce the identical name.
        let via_attr = resource_metrics(
            vec![],
            vec![gauge_metric(
                "up",
                vec![number_point(
                    vec![string_kv("0bad-key.name", "v")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out_attr = normalize_metrics(&tenant(), request(vec![via_attr]), &limits, 1_000);
        assert!(out_attr.rejected.is_empty(), "{:?}", out_attr.rejected);
        assert_eq!(out_attr.points[0].labels.get("_bad_key_name"), Some("v"));

        // The sanitized name is what reaches series identity: both paths yield
        // the same canonical label set and therefore the same SeriesId.
        assert_eq!(
            out_resource.points[0].series_id,
            out_attr.points[0].series_id
        );
    }

    // --- sanitization collisions -> duplicate rejection ---

    #[test]
    fn sanitization_collision_rejects_point() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "requests",
                vec![number_point(
                    vec![string_kv("foo.bar", "1"), string_kv("foo-bar", "2")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected.len(), 1);
        assert!(matches!(
            out.rejected[0],
            Rejection::DuplicateLabelName(ref n) if n == "foo_bar"
        ));
    }

    // --- int and double points ---

    #[test]
    fn int_value_converts_to_f64() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsInt(42))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value, 42.0);
    }

    #[test]
    fn int_value_at_2_pow_53_is_exact_and_silent() {
        // 2^53 is the largest magnitude every integer below which is exactly
        // representable as f64; it is itself exact. It must be admitted with
        // no precision-loss signal and the bit-exact sample value.
        let boundary: i64 = 1 << 53; // 9_007_199_254_740_992
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsInt(boundary))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 1);
        // Bit-exact, not just ==: the stored f64 round-trips back to the input.
        assert_eq!(
            out.points[0].sample.value.to_bits(),
            (boundary as f64).to_bits()
        );
        assert_eq!(out.points[0].sample.value as i64, boundary);
    }

    #[test]
    fn int_value_above_2_pow_53_is_admitted_but_flagged() {
        // 2^53 + 1 is the first integer that rounds when cast to f64 (down to
        // 2^53). The point is still admitted (rejecting legitimate large
        // counters would be worse), but the loss becomes observable through an
        // informational rejection that does not inflate the rejected-point
        // count.
        let above: i64 = (1 << 53) + 1; // 9_007_199_254_740_993
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsInt(above))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        // Admitted: the sample is stored as the nearest f64.
        assert_eq!(out.points.len(), 1);
        assert_eq!(out.points[0].sample.value, above as f64);
        assert_ne!(out.points[0].sample.value as i64, above);
        // Observable: exactly one informational rejection carrying the input.
        assert_eq!(out.rejected.len(), 1);
        assert!(matches!(
            out.rejected[0],
            Rejection::IntegerValuePrecisionLoss { value } if value == above
        ));
        // Informational: it does not count against the sender's points.
        assert_eq!(out.rejected[0].rejected_count(), 0);
    }

    #[test]
    fn int_value_at_i64_max_is_flagged_not_silently_saturated() {
        // i64::MAX casts to 2^63 as f64, which a bare `as i64` round-trip would
        // saturate back to i64::MAX and so falsely accept. The i128 round-trip
        // in `int_survives_f64` catches it.
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsInt(i64::MAX))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert_eq!(out.points.len(), 1);
        assert_eq!(out.rejected.len(), 1);
        assert!(matches!(
            out.rejected[0],
            Rejection::IntegerValuePrecisionLoss { value } if value == i64::MAX
        ));
    }

    #[test]
    fn large_int_no_recorded_value_reports_no_precision_loss() {
        // A NoRecordedValue point never reads the int, so a value above 2^53
        // there is discarded (stale marker), not rounded; no signal is due.
        let dp = NumberDataPoint {
            attributes: vec![],
            time_unix_nano: 1_000,
            value: Some(NumberValue::AsInt((1 << 53) + 1)),
            flags: DataPointFlags::NoRecordedValueMask as u32,
            ..Default::default()
        };
        let rm = resource_metrics(vec![], vec![gauge_metric("widgets", vec![dp])]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert_eq!(out.points.len(), 1);
        assert!(out.points[0].sample.value.is_nan());
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    }

    #[test]
    fn double_value_passes_through() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(3.5))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value, 3.5);
    }

    #[test]
    fn nan_sample_value_passes_through() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(f64::NAN))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert!(out.points[0].sample.value.is_nan());
    }

    // --- limits: every violation is a Rejection, not a panic/error ---

    #[test]
    fn too_many_data_points_rejects_whole_request() {
        let limits = IngestLimits {
            max_data_points_per_request: 2,
            ..IngestLimits::default()
        };
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![
                    number_point(vec![], 1_000, NumberValue::AsDouble(1.0)),
                    number_point(vec![], 1_000, NumberValue::AsDouble(2.0)),
                    number_point(vec![], 1_000, NumberValue::AsDouble(3.0)),
                ],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, 1_000);
        assert!(out.points.is_empty());
        assert_eq!(out.rejected.len(), 1);
        assert_eq!(
            out.rejected[0],
            Rejection::TooManyDataPoints { count: 3, max: 2 }
        );
    }

    #[test]
    fn too_many_attributes_rejects_point() {
        let limits = IngestLimits {
            max_attributes_per_point: 1,
            ..IngestLimits::default()
        };
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![string_kv("a", "1"), string_kv("b", "2")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, 1_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::TooManyAttributes {
                attribute_count: 2,
                max: 1,
            }]
        );
    }

    #[test]
    fn label_name_too_long_rejects_point() {
        let limits = IngestLimits {
            max_label_name_len: 3,
            ..IngestLimits::default()
        };
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![string_kv("toolong", "v")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, 1_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::LabelNameTooLong { len: 7, max: 3 }]
        );
    }

    #[test]
    fn label_value_too_long_rejects_point() {
        let limits = IngestLimits {
            max_label_value_len: 3,
            ..IngestLimits::default()
        };
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![string_kv("k", "toolong")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, 1_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::LabelValueTooLong { len: 7, max: 3 }]
        );
    }

    #[test]
    fn metric_name_too_long_rejects_all_points_in_metric() {
        let limits = IngestLimits {
            max_metric_name_len: 3,
            ..IngestLimits::default()
        };
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "toolong",
                vec![
                    number_point(vec![], 1_000, NumberValue::AsDouble(1.0)),
                    number_point(vec![], 1_000, NumberValue::AsDouble(2.0)),
                ],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, 1_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::MetricNameTooLong {
                len: 7,
                max: 3,
                count: 2,
            }]
        );
    }

    #[test]
    fn too_many_resource_attributes_rejects_all_points_under_resource() {
        let limits = IngestLimits {
            max_resource_attributes: 1,
            ..IngestLimits::default()
        };
        let rm = resource_metrics(
            vec![string_kv("a", "1"), string_kv("b", "2")],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, 1_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::TooManyResourceAttributes { count: 1, max: 1 }]
        );
    }

    #[test]
    fn empty_metric_name_after_sanitization_rejects_all_points() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![Rejection::EmptyMetricName { count: 1 }]);
    }

    #[test]
    fn int_and_bool_attribute_values_canonicalize_to_strings() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![int_kv("retries", 3), bool_kv("cached", true)],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        let labels = &out.points[0].labels;
        assert_eq!(labels.get("retries"), Some("3"));
        assert_eq!(labels.get("cached"), Some("true"));
    }

    #[test]
    fn complex_attribute_value_rejects_point() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![bytes_kv("blob")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![Rejection::ComplexAttributeValue]);
    }

    #[test]
    fn resource_complex_attribute_value_rejects_every_point_under_it() {
        // service.name as a bytes value is invalid; every point under this
        // resource is rejected, one Rejection per point (the `repeat_n`
        // path in `normalize_resource`), not one shared entry.
        let rm = resource_metrics(
            vec![bytes_kv("service.name")],
            vec![gauge_metric(
                "widgets",
                vec![
                    number_point(vec![], 1_000, NumberValue::AsDouble(1.0)),
                    number_point(vec![], 1_000, NumberValue::AsDouble(2.0)),
                ],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![
                Rejection::ComplexAttributeValue,
                Rejection::ComplexAttributeValue,
            ]
        );
    }

    #[test]
    fn resource_label_value_too_long_rejects_point() {
        let limits = IngestLimits {
            max_label_value_len: 3,
            ..IngestLimits::default()
        };
        let rm = resource_metrics(
            vec![string_kv("service.name", "toolong")],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, 1_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::LabelValueTooLong { len: 7, max: 3 }]
        );
    }

    #[test]
    fn resource_job_join_over_limit_rejects_even_though_each_half_is_under() {
        // namespace "ns" (2 bytes) and name "svc" (3 bytes) are each under
        // the limit, but the joined "ns/svc" (6 bytes) is not: the check
        // must happen after the format! join, not before.
        let limits = IngestLimits {
            max_label_value_len: 5,
            ..IngestLimits::default()
        };
        let rm = resource_metrics(
            vec![
                string_kv("service.name", "svc"),
                string_kv("service.namespace", "ns"),
            ],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, 1_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::LabelValueTooLong { len: 6, max: 5 }]
        );
    }

    #[test]
    fn data_point_attribute_named_job_collides_with_synthesized_job_label() {
        let rm = resource_metrics(
            vec![string_kv("service.name", "svc")],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![string_kv("job", "override")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::DuplicateLabelName("job".to_string())]
        );
    }

    #[test]
    fn metric_name_leading_digit_aliases_with_underscore_prefixed_name() {
        // Documented behavior, not a bug: sanitization replaces disallowed
        // characters in place and does not shift/prefix. "1foo" and "_foo"
        // both sanitize to "_foo" and therefore share a series id.
        let rm_digit = resource_metrics(
            vec![],
            vec![gauge_metric(
                "1foo",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let rm_underscore = resource_metrics(
            vec![],
            vec![gauge_metric(
                "_foo",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let out_digit = normalize_metrics(
            &tenant(),
            request(vec![rm_digit]),
            &IngestLimits::default(),
            1_000,
        );
        let out_underscore = normalize_metrics(
            &tenant(),
            request(vec![rm_underscore]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out_digit.rejected.is_empty());
        assert!(out_underscore.rejected.is_empty());
        assert_eq!(
            out_digit.points[0].series_id,
            out_underscore.points[0].series_id
        );
    }

    // --- unsupported metric types: never silently dropped ---

    #[test]
    fn exponential_histogram_metric_rejected_with_counts() {
        use opentelemetry_proto::tonic::metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint,
        };
        let metric = Metric {
            name: "latency_exp".to_string(),
            data: Some(MetricData::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![
                    ExponentialHistogramDataPoint {
                        time_unix_nano: 1_000,
                        ..Default::default()
                    },
                    ExponentialHistogramDataPoint {
                        time_unix_nano: 1_000,
                        ..Default::default()
                    },
                    ExponentialHistogramDataPoint {
                        time_unix_nano: 1_000,
                        ..Default::default()
                    },
                ],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        };
        let rm = resource_metrics(vec![], vec![metric]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::UnsupportedMetricType {
                metric_type: "exponential_histogram",
                count: 3,
            }]
        );
    }

    // --- delta sums rejected ---

    #[test]
    fn delta_sum_rejected() {
        let rm = resource_metrics(
            vec![],
            vec![sum_metric(
                "requests_total",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
                AggregationTemporality::Delta,
                true,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::UnsupportedTemporality { count: 1 }]
        );
    }

    #[test]
    fn cumulative_sum_accepted_and_records_is_monotonic() {
        let rm = resource_metrics(
            vec![],
            vec![sum_metric(
                "requests_total",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(5.0))],
                AggregationTemporality::Cumulative,
                true,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert!(out.points[0].is_monotonic_sum);
    }

    #[test]
    fn gauge_is_never_monotonic_sum() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(5.0))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(!out.points[0].is_monotonic_sum);
    }

    // --- zero timestamps ---

    #[test]
    fn zero_timestamp_rejected() {
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 0, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![Rejection::ZeroTimestamp]);
    }

    // --- future skew / too old boundaries ---
    // Convention: the bound itself passes; one ns past it fails.

    #[test]
    fn future_skew_exactly_at_bound_passes() {
        let limits = IngestLimits {
            max_future_skew_ns: 100,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000;
        let event_ts = ingest_ts + 100;
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], event_ts, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, ingest_ts);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 1);
    }

    #[test]
    fn future_skew_one_ns_past_bound_fails() {
        let limits = IngestLimits {
            max_future_skew_ns: 100,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000;
        let event_ts = ingest_ts + 101;
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], event_ts, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, ingest_ts);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::FutureSkew {
                skew_ns: 101,
                max_ns: 100,
            }]
        );
    }

    #[test]
    fn too_old_exactly_at_bound_passes() {
        let limits = IngestLimits {
            max_ingest_lag_ns: 100,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000;
        let event_ts = ingest_ts - 100;
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], event_ts, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, ingest_ts);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 1);
    }

    #[test]
    fn too_old_one_ns_past_bound_fails() {
        let limits = IngestLimits {
            max_ingest_lag_ns: 100,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000;
        let event_ts = ingest_ts - 101;
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], event_ts, NumberValue::AsDouble(1.0))],
            )],
        );
        let out = normalize_metrics(&tenant(), request(vec![rm]), &limits, ingest_ts);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::TooOld {
                lag_ns: 101,
                max_ns: 100,
            }]
        );
    }

    // --- series id stability across differently ordered requests ---

    #[test]
    fn series_id_stable_across_attribute_order() {
        let rm_a = resource_metrics(
            vec![string_kv("service.name", "svc")],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![string_kv("a", "1"), string_kv("b", "2")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let rm_b = resource_metrics(
            vec![string_kv("service.name", "svc")],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![string_kv("b", "2"), string_kv("a", "1")],
                    1_000,
                    NumberValue::AsDouble(1.0),
                )],
            )],
        );
        let out_a = normalize_metrics(
            &tenant(),
            request(vec![rm_a]),
            &IngestLimits::default(),
            1_000,
        );
        let out_b = normalize_metrics(
            &tenant(),
            request(vec![rm_b]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out_a.rejected.is_empty());
        assert!(out_b.rejected.is_empty());
        assert_eq!(out_a.points[0].series_id, out_b.points[0].series_id);
    }

    #[test]
    fn series_id_set_stable_across_resource_order() {
        let rm1 = resource_metrics(
            vec![string_kv("service.name", "svc-1")],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(1.0))],
            )],
        );
        let rm2 = resource_metrics(
            vec![string_kv("service.name", "svc-2")],
            vec![gauge_metric(
                "widgets",
                vec![number_point(vec![], 1_000, NumberValue::AsDouble(2.0))],
            )],
        );

        let out_forward = normalize_metrics(
            &tenant(),
            request(vec![rm1.clone(), rm2.clone()]),
            &IngestLimits::default(),
            1_000,
        );
        let out_reversed = normalize_metrics(
            &tenant(),
            request(vec![rm2, rm1]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out_forward.rejected.is_empty());
        assert!(out_reversed.rejected.is_empty());

        let ids_forward: HashSet<_> = out_forward.points.iter().map(|p| p.series_id).collect();
        let ids_reversed: HashSet<_> = out_reversed.points.iter().map(|p| p.series_id).collect();
        assert_eq!(ids_forward, ids_reversed);
    }

    // --- series-id memoization (issue #96) ---

    #[test]
    fn memoized_and_recomputed_series_ids_are_bit_identical() {
        // Consecutive run of series "/a" (the memo hits), switch to "/b" (a
        // miss), then back to "/a" (a miss again, since the one-entry memo now
        // holds "/b"). Every emitted id must equal a direct SeriesId::compute
        // over the same label set: the memo is an optimization, never an
        // aliasing change.
        let base_ts: i64 = 1_700_000_000_000_000_000;
        let mut pts = Vec::new();
        for i in 0..4i64 {
            pts.push(number_point(
                vec![string_kv("path", "/a")],
                base_ts + i,
                NumberValue::AsInt(i),
            ));
        }
        for i in 0..3i64 {
            pts.push(number_point(
                vec![string_kv("path", "/b")],
                base_ts + 10 + i,
                NumberValue::AsInt(i),
            ));
        }
        pts.push(number_point(
            vec![string_kv("path", "/a")],
            base_ts + 20,
            NumberValue::AsInt(0),
        ));
        let rm = resource_metrics(
            vec![string_kv("service.name", "svc")],
            vec![gauge_metric("widgets", pts)],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            base_ts + 1_000_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 8);

        for p in &out.points {
            let recomputed =
                SeriesId::compute(&tenant(), "widgets", &p.labels).expect("compute id");
            assert_eq!(
                p.series_id, recomputed,
                "memoized id must be bit-identical to a fresh compute"
            );
        }
        // The two "/a" runs (positions 0..4 and 7) share an id: reuse is by
        // label set, not position. They differ from the "/b" run (position 4),
        // so the one-entry memo never aliases distinct series.
        assert_eq!(out.points[0].series_id, out.points[7].series_id);
        assert_ne!(out.points[0].series_id, out.points[4].series_id);
    }

    #[test]
    fn memo_throughput_before_after() {
        // Realistic ingest shape: many series, each sampled 100 times in a
        // consecutive run within one metric (one series over time). This is
        // the shape the last-seen memo targets. The assertion is deterministic
        // (memo path bit-identical to full recompute); the timing is
        // informational and printed under `--nocapture`.
        const SERIES: usize = 50;
        const POINTS_PER_SERIES: usize = 100;
        let base_ts: i64 = 1_700_000_000_000_000_000;

        let mut pts = Vec::with_capacity(SERIES * POINTS_PER_SERIES);
        for s in 0..SERIES {
            let path = format!("/api/v{s}");
            for i in 0..POINTS_PER_SERIES {
                pts.push(number_point(
                    vec![string_kv("path", &path), string_kv("method", "GET")],
                    base_ts + i as i64,
                    NumberValue::AsInt(i as i64),
                ));
            }
        }
        let rm = resource_metrics(
            vec![string_kv("service.name", "svc")],
            vec![gauge_metric("http_requests_total", pts)],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            base_ts + 1_000_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), SERIES * POINTS_PER_SERIES);

        let label_sets: Vec<LabelSet> = out.points.iter().map(|p| p.labels.clone()).collect();
        let name = "http_requests_total";

        // before: recompute the id for every point (pre-memo behavior).
        let t0 = std::time::Instant::now();
        let mut before_ids = Vec::with_capacity(label_sets.len());
        for ls in &label_sets {
            before_ids.push(SeriesId::compute(&tenant(), name, ls).expect("compute id"));
        }
        let before = t0.elapsed();

        // after: last-seen memo over the same order.
        let t1 = std::time::Instant::now();
        let mut memo = SeriesIdMemo::new();
        let mut after_ids = Vec::with_capacity(label_sets.len());
        for ls in &label_sets {
            after_ids.push(memo.series_id(&tenant(), name, ls).expect("compute id"));
        }
        let after = t1.elapsed();

        // Deterministic: the memo path is bit-identical to full recompute, and
        // to the ids normalize embedded in the points.
        assert_eq!(before_ids, after_ids);
        for (p, id) in out.points.iter().zip(&after_ids) {
            assert_eq!(p.series_id, *id);
        }

        let n = label_sets.len() as f64;
        let before_thr = n / before.as_secs_f64();
        let after_thr = n / after.as_secs_f64();
        println!(
            "series-id memo: {SERIES} series x {POINTS_PER_SERIES} pts = {} points",
            label_sets.len()
        );
        println!("  before (recompute each): {before_thr:>12.0} ids/s ({before:?})");
        println!("  after  (last-seen memo): {after_thr:>12.0} ids/s ({after:?})");
        println!("  speedup: {:.2}x", after_thr / before_thr);
    }

    // --- histogram/summary explosion (ADR-0016) ---

    #[test]
    fn ch1_histogram_explosion_matches_cross_protocol_identity_vector() {
        // docs/ingest-breadth-plan.md §4.1 (CH-1): the same logical histogram
        // ingested OTLP-exploded and RW-classic must land on identical
        // SeriesIds and values. The RW-classic side has its own test in its
        // own crate (track A); this asserts the OTLP side reaches the exact
        // canonical label sets that side would also construct.
        let tenant_fixture = TenantId::new("t-fixture");
        let rm = resource_metrics(
            vec![
                string_kv("service.name", "svc"),
                string_kv("service.instance.id", "i-1"),
            ],
            vec![histogram_metric(
                "http_request_duration_seconds",
                vec![histogram_point(
                    vec![],
                    1_700_000_000_000_000_000,
                    10,
                    Some(42.5),
                    vec![0.1, 1.0, 10.0],
                    vec![1, 2, 3, 4],
                )],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant_fixture,
            request(vec![rm]),
            &IngestLimits::default(),
            1_700_000_000_000_000_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 6);

        type ExpectedSeries<'a> = (&'a str, &'a [(&'a str, &'a str)], f64);
        let expected: &[ExpectedSeries] = &[
            (
                "http_request_duration_seconds_bucket",
                &[("le", "0.1")],
                1.0,
            ),
            ("http_request_duration_seconds_bucket", &[("le", "1")], 3.0),
            ("http_request_duration_seconds_bucket", &[("le", "10")], 6.0),
            (
                "http_request_duration_seconds_bucket",
                &[("le", "+Inf")],
                10.0,
            ),
            ("http_request_duration_seconds_sum", &[], 42.5),
            ("http_request_duration_seconds_count", &[], 10.0),
        ];

        for &(name, extra, value) in expected {
            let p = out
                .points
                .iter()
                .find(|p| {
                    p.labels.get(METRIC_NAME_LABEL) == Some(name)
                        && extra.iter().all(|&(k, v)| p.labels.get(k) == Some(v))
                })
                .unwrap_or_else(|| panic!("missing series {name} {extra:?}"));
            assert_eq!(p.sample.value, value, "{name} {extra:?}");
            assert_eq!(p.labels.get("job"), Some("svc"));
            assert_eq!(p.labels.get("instance"), Some("i-1"));

            let mut labels = vec![
                Label {
                    name: "job".to_string(),
                    value: "svc".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "i-1".to_string(),
                },
                Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: name.to_string(),
                },
            ];
            for &(k, v) in extra {
                labels.push(Label {
                    name: k.to_string(),
                    value: v.to_string(),
                });
            }
            let label_set = LabelSet::new(labels).expect("label set");
            let expected_id =
                SeriesId::compute(&tenant_fixture, name, &label_set).expect("compute id");
            assert_eq!(p.series_id, expected_id, "{name} {extra:?}");
        }
    }

    #[test]
    fn histogram_without_sum_omits_sum_series() {
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![histogram_point(
                    vec![],
                    1_000,
                    3,
                    None,
                    vec![1.0],
                    vec![1, 2],
                )],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        // le=1, le=+Inf, _count -- no _sum.
        assert_eq!(out.points.len(), 3);
        assert!(
            out.points
                .iter()
                .all(|p| p.labels.get(METRIC_NAME_LABEL) != Some("latency_sum"))
        );
    }

    #[test]
    fn delta_histogram_rejected() {
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![histogram_point(
                    vec![],
                    1_000,
                    3,
                    Some(1.0),
                    vec![1.0],
                    vec![1, 2],
                )],
                AggregationTemporality::Delta,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::UnsupportedTemporality { count: 1 }]
        );
    }

    #[test]
    fn histogram_bucket_count_mismatch_rejects_point() {
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![histogram_point(
                    vec![],
                    1_000,
                    3,
                    Some(1.0),
                    vec![1.0, 2.0],
                    vec![1, 2],
                )],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::HistogramBucketCountMismatch {
                bounds: 2,
                buckets: 2,
                expected: 3,
            }]
        );
    }

    #[test]
    fn histogram_non_finite_bound_rejects_point() {
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![histogram_point(
                    vec![],
                    1_000,
                    3,
                    Some(1.0),
                    vec![f64::NAN],
                    vec![1, 2],
                )],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![Rejection::NonFiniteHistogramBound]);
    }

    #[test]
    fn histogram_bounds_not_increasing_rejects_point() {
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![histogram_point(
                    vec![],
                    1_000,
                    3,
                    Some(1.0),
                    vec![2.0, 1.0],
                    vec![1, 2, 3],
                )],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![Rejection::HistogramBoundsNotIncreasing]);
    }

    #[test]
    fn histogram_count_overflow_rejects_point() {
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![histogram_point(
                    vec![],
                    1_000,
                    3,
                    Some(1.0),
                    vec![1.0, 2.0],
                    vec![u64::MAX, u64::MAX, 0],
                )],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![Rejection::HistogramCountOverflow]);
    }

    #[test]
    fn histogram_min_max_dropped_is_informational_not_a_rejection() {
        let mut dp = histogram_point(vec![], 1_000, 3, None, vec![1.0], vec![1, 2]);
        dp.min = Some(0.5);
        dp.max = Some(5.0);
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![dp],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert_eq!(out.points.len(), 3);
        assert_eq!(
            out.rejected,
            vec![Rejection::HistogramMinMaxDropped { count: 1 }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 0);
    }

    #[test]
    fn histogram_exemplars_dropped_is_informational_not_a_rejection() {
        let mut dp = histogram_point(vec![], 1_000, 3, None, vec![1.0], vec![1, 2]);
        dp.exemplars = vec![
            Exemplar {
                time_unix_nano: 999,
                ..Default::default()
            },
            Exemplar {
                time_unix_nano: 998,
                ..Default::default()
            },
        ];
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![dp],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert_eq!(out.points.len(), 3);
        assert_eq!(
            out.rejected,
            vec![Rejection::HistogramExemplarsDropped { count: 2 }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 0);
    }

    #[test]
    fn histogram_no_recorded_value_flag_maps_every_series_to_stale_marker() {
        let mut dp = histogram_point(vec![], 1_000, 3, Some(1.0), vec![1.0], vec![1, 2]);
        dp.flags = DataPointFlags::NoRecordedValueMask as u32;
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![dp],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 4);
        for p in &out.points {
            assert_eq!(p.sample.value.to_bits(), STALE_NAN_BITS);
        }
    }

    #[test]
    fn histogram_attribute_named_le_collides_atomically() {
        // The synthesized "le" bucket label collides with a same-named
        // attribute; the whole data point must reject with zero partial
        // series admitted, not some buckets landing and others not.
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![histogram_point(
                    vec![string_kv("le", "bogus")],
                    1_000,
                    3,
                    Some(1.0),
                    vec![1.0],
                    vec![1, 2],
                )],
                AggregationTemporality::Cumulative,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::DuplicateLabelName("le".to_string())]
        );
    }

    #[test]
    fn ch1_summary_explosion_basic_shape_and_identity() {
        let rm = resource_metrics(
            vec![string_kv("service.name", "svc")],
            vec![summary_metric(
                "http_request_duration_seconds",
                vec![summary_point(
                    vec![],
                    1_000,
                    10,
                    42.5,
                    vec![value_at_quantile(0.5, 0.2), value_at_quantile(0.99, 0.9)],
                )],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 4);

        let p50 = out
            .points
            .iter()
            .find(|p| {
                p.labels.get(METRIC_NAME_LABEL) == Some("http_request_duration_seconds")
                    && p.labels.get("quantile") == Some("0.5")
            })
            .expect("p50 series");
        assert_eq!(p50.sample.value, 0.2);

        let p99 = out
            .points
            .iter()
            .find(|p| {
                p.labels.get(METRIC_NAME_LABEL) == Some("http_request_duration_seconds")
                    && p.labels.get("quantile") == Some("0.99")
            })
            .expect("p99 series");
        assert_eq!(p99.sample.value, 0.9);

        let sum = out
            .points
            .iter()
            .find(|p| p.labels.get(METRIC_NAME_LABEL) == Some("http_request_duration_seconds_sum"))
            .expect("sum series");
        assert_eq!(sum.sample.value, 42.5);

        let count = out
            .points
            .iter()
            .find(|p| {
                p.labels.get(METRIC_NAME_LABEL) == Some("http_request_duration_seconds_count")
            })
            .expect("count series");
        assert_eq!(count.sample.value, 10.0);
    }

    #[test]
    fn summary_non_finite_quantile_rejects_point() {
        let rm = resource_metrics(
            vec![],
            vec![summary_metric(
                "latency",
                vec![summary_point(
                    vec![],
                    1_000,
                    1,
                    1.0,
                    vec![value_at_quantile(f64::INFINITY, 1.0)],
                )],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![Rejection::NonFiniteQuantile]);
    }

    #[test]
    fn summary_duplicate_quantile_rejects_point() {
        let rm = resource_metrics(
            vec![],
            vec![summary_metric(
                "latency",
                vec![summary_point(
                    vec![],
                    1_000,
                    1,
                    1.0,
                    vec![value_at_quantile(0.5, 1.0), value_at_quantile(0.5, 2.0)],
                )],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![Rejection::DuplicateQuantile]);
    }

    #[test]
    fn summary_no_recorded_value_flag_maps_every_series_to_stale_marker() {
        let mut dp = summary_point(vec![], 1_000, 1, 1.0, vec![value_at_quantile(0.5, 0.3)]);
        dp.flags = DataPointFlags::NoRecordedValueMask as u32;
        let rm = resource_metrics(vec![], vec![summary_metric("latency", vec![dp])]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 3);
        for p in &out.points {
            assert_eq!(p.sample.value.to_bits(), STALE_NAN_BITS);
        }
    }

    #[test]
    fn summary_attribute_named_quantile_collides_atomically() {
        let rm = resource_metrics(
            vec![],
            vec![summary_metric(
                "latency",
                vec![summary_point(
                    vec![string_kv("quantile", "bogus")],
                    1_000,
                    1,
                    1.0,
                    vec![value_at_quantile(0.5, 0.3)],
                )],
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::DuplicateLabelName("quantile".to_string())]
        );
    }

    #[test]
    fn gauge_no_recorded_value_flag_maps_to_stale_marker() {
        // Gauge/Sum flags handling was previously entirely ignored; part of
        // this ticket per docs/ingest-breadth-plan.md §8 ("B1 fixes
        // gauge/sum staleness mapping in the same change").
        let mut dp = number_point(vec![], 1_000, NumberValue::AsDouble(5.0));
        dp.flags = DataPointFlags::NoRecordedValueMask as u32;
        let rm = resource_metrics(vec![], vec![gauge_metric("widgets", vec![dp])]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value.to_bits(), STALE_NAN_BITS);
    }

    #[test]
    fn sum_no_recorded_value_flag_maps_to_stale_marker() {
        let mut dp = number_point(vec![], 1_000, NumberValue::AsDouble(5.0));
        dp.flags = DataPointFlags::NoRecordedValueMask as u32;
        let rm = resource_metrics(
            vec![],
            vec![sum_metric(
                "requests_total",
                vec![dp],
                AggregationTemporality::Cumulative,
                true,
            )],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value.to_bits(), STALE_NAN_BITS);
    }

    #[test]
    fn gauge_flags_zero_is_byte_identical_to_pre_flags_behavior() {
        // Regression pin: flags = 0 (the default before this ticket added
        // flags handling) must produce the exact same value as before, not
        // a stale marker.
        let dp = number_point(vec![], 1_000, NumberValue::AsDouble(5.0));
        assert_eq!(dp.flags, 0);
        let rm = resource_metrics(vec![], vec![gauge_metric("widgets", vec![dp])]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value, 5.0);
    }
}
