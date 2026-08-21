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
//! Cumulative `ExponentialHistogram` (native histogram) data points are
//! admitted as a native-histogram sample (ADR-0017), materialized as a
//! [`NormalizedHistogramPoint`] carrying a `ravel_segment::HistogramSample`
//! rather than exploded into scalar series. Non-cumulative temporality is
//! rejected typed ([`Rejection::UnsupportedTemporality`]) like the other
//! aggregating types. `min`/`max` and exemplars have no place in the
//! segment's native-histogram sample and are dropped informationally. OTLP
//! carries no custom-bucket boundaries, so `scale == -53` (the custom-buckets
//! sentinel) is rejected ([`Rejection::NativeHistogramScaleUnsupported`])
//! rather than stored unbacked; the Remote Write surface admits it, since
//! that wire format carries the boundaries losslessly.
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

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::exemplar::Value as OtlpExemplarValue;
use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, DataPointFlags, Exemplar as OtlpExemplar,
    ExponentialHistogramDataPoint, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
    SummaryDataPoint, metric::Data as MetricData,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_segment::{HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, ResetHint};
use ravel_types::{
    Exemplar, ExemplarCap, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId,
    TypeError,
};

use crate::limits::{IngestLimits, Rejection};
use crate::metadata::{MetricKind, MetricMetadata};
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

/// One admitted OTLP data point carrying a scalar sample, normalized to
/// Ravel's canonical shape. Native-histogram points use the sibling
/// [`NormalizedHistogramPoint`] instead, since scalar and histogram values
/// have no common storage shape (a scalar is one `f64`, a histogram is a
/// span-based bucket layout); keeping them as separate types rather than one
/// enum leaves this struct, and every crate that already builds it, unchanged.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedPoint {
    pub series_id: SeriesId,
    /// One label set shared across the points of a series run (ADR-0098). A
    /// run of points with identical attributes clone the same `Arc` rather
    /// than each building and owning a copy.
    pub labels: Arc<LabelSet>,
    pub sample: Sample,
    /// `true` only for points from a `Sum` metric with `is_monotonic` set;
    /// always `false` for `Gauge` points.
    pub is_monotonic_sum: bool,
}

/// One admitted OTLP native-histogram data point (a cumulative
/// `ExponentialHistogram` point, ADR-0017). The sibling of
/// [`NormalizedPoint`] for the histogram value shape; it carries no
/// `is_monotonic_sum` because a native histogram is never treated as a
/// monotonic scalar counter (its counter-reset signal lives in the stored
/// value's `reset_hint`).
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedHistogramPoint {
    pub series_id: SeriesId,
    /// Shared per series run, as on [`NormalizedPoint`] (ADR-0098).
    pub labels: Arc<LabelSet>,
    pub sample: HistogramSample,
}

/// Result of normalizing one `ExportMetricsServiceRequest`. Scalar points
/// and native-histogram points are carried in separate vectors, matching
/// their separate normalized types; the caller feeds both into ingest.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizeOutput {
    pub points: Vec<NormalizedPoint>,
    pub histogram_points: Vec<NormalizedHistogramPoint>,
    pub rejected: Vec<Rejection>,
}

/// One exemplar admitted through the per-series cap (ADR-0047 decisions 1
/// and 2), paired with the series it was attached to. A classic `Histogram`
/// data point explodes into several series (`{name}_bucket{le=...}`,
/// `{name}_sum`, `{name}_count`); an exemplar from it attaches to the bucket
/// series whose `le` bound is the smallest one at or above the exemplar's
/// value, matching Prometheus's own bucket-exemplar convention (falling
/// back to the `+Inf` bucket when no explicit bound qualifies).
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedExemplar {
    pub series_id: SeriesId,
    pub exemplar: Exemplar,
}

/// Result of normalizing one `ExportMetricsServiceRequest`, including the
/// exemplars admitted through the caller's [`ExemplarCap`]. The sibling of
/// [`NormalizeOutput`] that also threads exemplars through; kept as a
/// separate type (rather than adding a field to `NormalizeOutput`) because
/// `NormalizeOutput` is constructed via struct literal by other ingest
/// paths (`ravel-otap`, `ravel-remote-write`) that this change must not
/// break.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsNormalizeResult {
    pub output: NormalizeOutput,
    pub exemplars: Vec<NormalizedExemplar>,
}

/// Decode and normalize gauge and sum data points from `req`, discarding any
/// exemplars they carried. Kept with its original signature and return type
/// for existing callers; internally this is a thin wrapper around
/// [`normalize_metrics_with_exemplars`] with a throwaway, request-scoped
/// [`ExemplarCap`], so the reported [`Rejection::HistogramExemplarsDropped`]
/// count is accurate (cap-based) rather than "every exemplar, always."
/// Callers that have somewhere to put admitted exemplars should
/// call [`normalize_metrics_with_exemplars`] instead, with a cap that
/// outlives a single request so the per-series window means something.
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
    let mut cap = ExemplarCap::new(limits.exemplar_cap_window_ns);
    let result = normalize_metrics_with_exemplars(tenant, req, limits, ingest_ts_ns, &mut cap);
    let mut output = result.output;
    // The cap admitted these, but this entry point has nowhere to store them and
    // discards them. A discarded admission is still a dropped exemplar and must
    // be counted, or an operator watching the dropped-data counter sees it read
    // zero while the same exemplars are lost (ADR-0047 decision 2: an exemplar
    // that is not stored is dropped and counted, never silent). Matches the
    // OTAP and Remote Write wrappers.
    if !result.exemplars.is_empty() {
        output.rejected.push(Rejection::HistogramExemplarsDropped {
            count: result.exemplars.len(),
        });
    }
    output
}

/// Decode and normalize gauge and sum data points from `req`, admitting
/// exemplars through `exemplar_cap` (ADR-0047 decisions 1 and 2). `cap` is
/// `&mut` and caller-owned rather than built here: a per-series-per-window
/// cap only means something across many requests over wall-clock time, so
/// whoever holds the long-lived per-shard state must own one
/// `ExemplarCap` and pass it into every call this shard makes, exactly like
/// the existing `SeriesIdMemo` pattern but living longer than one request.
///
/// See [`normalize_metrics`] for the panic/error-handling contract, which is
/// identical here.
pub fn normalize_metrics_with_exemplars(
    tenant: &TenantId,
    req: ExportMetricsServiceRequest,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
) -> MetricsNormalizeResult {
    normalize_impl(tenant, req, limits, ingest_ts_ns, exemplar_cap, None)
}

/// Decode and normalize gauge and sum data points exactly like
/// [`normalize_metrics_with_exemplars`], and additionally surface one
/// [`MetricMetadata`] per metric family that produced at least one point
/// (ADR-0085 Decision 1). The returned [`MetricsNormalizeResult`] is
/// byte-for-byte what [`normalize_metrics_with_exemplars`] returns for the same
/// input, including the now-suffixed series names (Decision 2); the metadata is
/// the only addition. `family_name` is the suffixed name before the classic
/// histogram/summary explosion; metadata is deduplicated by `family_name`,
/// first write wins.
///
/// The metric metadata store that consumes this is ticket #235; it has no
/// production consumer yet. The suffix pass, in contrast, is reachable the
/// moment this ships: every OTLP metric ingested through the existing
/// `ravel-server` call sites gets its new Prometheus name.
pub fn normalize_metrics_with_metadata(
    tenant: &TenantId,
    req: ExportMetricsServiceRequest,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
) -> (MetricsNormalizeResult, Vec<MetricMetadata>) {
    let mut collector = MetadataCollector::default();
    let result = normalize_impl(
        tenant,
        req,
        limits,
        ingest_ts_ns,
        exemplar_cap,
        Some(&mut collector),
    );
    (result, collector.entries)
}

/// Shared body of the two exemplar-carrying entry points. `metadata_out` is
/// `Some` only for [`normalize_metrics_with_metadata`]; when `None`, the suffix
/// pass still runs (names are unconditional) but no metadata is collected, so
/// the older entry points stay behaviorally identical apart from the names.
fn normalize_impl(
    tenant: &TenantId,
    mut req: ExportMetricsServiceRequest,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
    mut metadata_out: Option<&mut MetadataCollector>,
) -> MetricsNormalizeResult {
    let total_points = count_data_points(&req);
    if total_points > limits.max_data_points_per_request {
        let mut rejected = vec![Rejection::TooManyDataPoints {
            count: total_points,
            max: limits.max_data_points_per_request,
        }];
        // The whole request is rejected before any data point is inspected, so
        // every exemplar it carried is dropped with it. Count them, matching the
        // Remote Write twin, so the dropped-data counter does not read zero
        // while exemplars are lost (ADR-0047 decision 2). Remote Write can count
        // here because its exemplars are already decoded into the resolved
        // request; OTLP can because they ride inline on the decoded request.
        // OTAP deliberately does not count here: its exemplar payloads are not
        // decoded at the point-count check, a genuine structural difference, so
        // the OTAP/OTLP differential gate compares this path at the
        // exemplar-carrying layer rather than at the wrapper.
        let dropped_exemplars = count_exemplars(&req);
        if dropped_exemplars > 0 {
            rejected.push(Rejection::HistogramExemplarsDropped {
                count: dropped_exemplars,
            });
        }
        return MetricsNormalizeResult {
            output: NormalizeOutput {
                points: Vec::new(),
                histogram_points: Vec::new(),
                rejected,
            },
            exemplars: Vec::new(),
        };
    }

    let mut points = Vec::new();
    let mut histogram_points = Vec::new();
    let mut rejected = Vec::new();
    let mut exemplars = Vec::new();

    for rm in &mut req.resource_metrics {
        normalize_resource(
            tenant,
            rm,
            limits,
            ingest_ts_ns,
            exemplar_cap,
            &mut points,
            &mut histogram_points,
            &mut exemplars,
            &mut rejected,
            metadata_out.as_deref_mut(),
        );
    }

    MetricsNormalizeResult {
        output: NormalizeOutput {
            points,
            histogram_points,
            rejected,
        },
        exemplars,
    }
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

/// Total exemplars carried on every data point in `req`, used only by the
/// `TooManyDataPoints` early return to count what the whole-request rejection
/// drops. Summary data points carry no exemplars in OTLP.
fn count_exemplars(req: &ExportMetricsServiceRequest) -> usize {
    req.resource_metrics
        .iter()
        .flat_map(|rm| rm.scope_metrics.iter())
        .flat_map(|sm| sm.metrics.iter())
        .map(metric_exemplar_count)
        .sum()
}

fn metric_exemplar_count(metric: &Metric) -> usize {
    match &metric.data {
        Some(MetricData::Gauge(g)) => g.data_points.iter().map(|dp| dp.exemplars.len()).sum(),
        Some(MetricData::Sum(s)) => s.data_points.iter().map(|dp| dp.exemplars.len()).sum(),
        Some(MetricData::Histogram(h)) => h.data_points.iter().map(|dp| dp.exemplars.len()).sum(),
        Some(MetricData::ExponentialHistogram(h)) => {
            h.data_points.iter().map(|dp| dp.exemplars.len()).sum()
        }
        Some(MetricData::Summary(_)) | None => 0,
    }
}

#[allow(clippy::too_many_arguments)]
fn normalize_resource(
    tenant: &TenantId,
    rm: &mut ResourceMetrics,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
    points: &mut Vec<NormalizedPoint>,
    histogram_points: &mut Vec<NormalizedHistogramPoint>,
    exemplars: &mut Vec<NormalizedExemplar>,
    rejected: &mut Vec<Rejection>,
    mut metadata_out: Option<&mut MetadataCollector>,
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
            rejected.push(Rejection::Grouped {
                reason: Box::new(reason),
                count: resource_point_count,
            });
            return;
        }
    };

    for sm in &mut rm.scope_metrics {
        for metric in &mut sm.metrics {
            normalize_metric(
                tenant,
                metric,
                &resource_labels,
                limits,
                ingest_ts_ns,
                exemplar_cap,
                points,
                histogram_points,
                exemplars,
                rejected,
                metadata_out.as_deref_mut(),
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn normalize_metric(
    tenant: &TenantId,
    metric: &mut Metric,
    resource_labels: &[Label],
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
    points: &mut Vec<NormalizedPoint>,
    histogram_points: &mut Vec<NormalizedHistogramPoint>,
    exemplars: &mut Vec<NormalizedExemplar>,
    rejected: &mut Vec<Rejection>,
    metadata_out: Option<&mut MetadataCollector>,
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

    // Apply the ADR-0085 Decision 2 suffix pass to the sanitized base name
    // before it reaches any `SeriesId::compute` call and before the classic
    // histogram/summary explosion. `point_count > 0` guarantees `data` is set.
    let Some(data) = metric.data.as_ref() else {
        return;
    };
    let (kind, is_monotonic_sum) = metric_kind_of(data);
    let name = prometheus_family_name(&name, &metric.unit, kind, is_monotonic_sum);
    let points_before = points.len();
    let hist_before = histogram_points.len();

    match &mut metric.data {
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
            for dp in &mut gauge.data_points {
                match build_point(&ctx, dp, &mut memo, exemplar_cap, exemplars) {
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
            for dp in &mut sum.data_points {
                match build_point(&ctx, dp, &mut memo, exemplar_cap, exemplars) {
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
            for dp in &mut h.data_points {
                match explode_histogram(&ctx, dp, &mut memo, exemplar_cap, exemplars) {
                    Ok((mut new_points, informational)) => {
                        points.append(&mut new_points);
                        rejected.extend(informational);
                    }
                    Err(r) => rejected.push(r),
                }
            }
        }
        Some(MetricData::ExponentialHistogram(h)) => {
            let temporality = AggregationTemporality::try_from(h.aggregation_temporality)
                .unwrap_or(AggregationTemporality::Unspecified);
            if temporality != AggregationTemporality::Cumulative {
                rejected.push(Rejection::UnsupportedTemporality {
                    count: h.data_points.len(),
                });
                return;
            }
            let ctx = NativeHistogramContext {
                tenant,
                metric_name: &name,
                resource_labels,
                limits,
                ingest_ts_ns,
            };
            let mut memo = SeriesIdMemo::new();
            for dp in &mut h.data_points {
                match build_native_histogram_point(&ctx, dp, &mut memo, exemplar_cap, exemplars) {
                    Ok((p, informational)) => {
                        histogram_points.push(p);
                        rejected.extend(informational);
                    }
                    Err(r) => rejected.push(r),
                }
            }
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
            for dp in &mut s.data_points {
                match explode_summary(&ctx, dp, &mut memo) {
                    Ok(mut new_points) => points.append(&mut new_points),
                    Err(r) => rejected.push(r),
                }
            }
        }
        None => {}
    }

    // One metadata tuple per metric family that produced at least one point.
    // `family_name` is the suffixed name above, before the explosion that
    // appends `_bucket`/`_sum`/`_count`, so it names the family the exploded
    // series belong to (ADR-0085 Decision 1).
    if let Some(collector) = metadata_out
        && (points.len() > points_before || histogram_points.len() > hist_before)
    {
        collector.record(MetricMetadata {
            family_name: name.clone(),
            kind,
            help: metric.description.clone(),
            unit: metadata_unit_word(&metric.unit, kind),
        });
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

/// Per-metric context shared by every data point of an `ExponentialHistogram`,
/// mirroring [`PointContext`] but without `is_sum`/`is_monotonic`: a native
/// histogram is never treated as a monotonic counter series.
struct NativeHistogramContext<'a> {
    tenant: &'a TenantId,
    metric_name: &'a str,
    resource_labels: &'a [Label],
    limits: &'a IngestLimits,
    ingest_ts_ns: i64,
}

/// Last-seen memo scoped to one `Metric`, keyed on the normalizer's input and
/// caching the built `Arc<LabelSet>` alongside the `SeriesId` (ADR-0098
/// decision 2). A hit clones an `Arc` and rebuilds nothing; the realistic OTLP
/// shape is one series sampled over time, so on a run of points with identical
/// attributes every point after the first pays one refcount bump instead of
/// rebuilding the label set and recomputing the id.
///
/// Two keying modes, one per producer shape, chosen by which method the caller
/// uses:
///
/// - [`series_id_for_attributes`](Self::series_id_for_attributes), for gauge,
///   sum, and native-histogram points. Within a metric `tenant`, `metric_name`
///   and the resource labels are constant, so the raw attribute slice alone
///   determines the label set. The key is that slice, compared by borrowed
///   equality against the previous point's, which allocates nothing;
///   determinism makes the sound direction hold (byte-equal raw input under a
///   fixed scope always builds the same label set), and the comparison is
///   order-sensitive on purpose (a reordered slice misses and rebuilds, which
///   is correct and merely unsaved). Sanitisation is many-to-one, so distinct
///   raw slices can build equal label sets; that direction only ever causes a
///   miss.
///
/// - [`series_id_for_built`](Self::series_id_for_built), for the classic
///   histogram and summary explosion. The exploded series of one data point
///   share a single raw attribute slice and differ only in the exploded name
///   and the synthesized `le`/`quantile` label, so a raw-attributes-only key
///   would hand every bucket the first bucket's id and labels -- silent data
///   corruption the merge collision check cannot detect, because the wrong id
///   and wrong labels travel as a consistent pair (ADR-0098). This path keeps
///   the built-set comparison instead: it is correct, and with a one-entry
///   memo the exploded series never repeat consecutively anyway, so it shares
///   nothing and merely computes each id once.
///
/// A hit records only after `LabelSet::new` and `SeriesId::compute` both
/// succeeded, and skips only label construction; every other admission check
/// runs unconditionally in the caller, so the set of rejected points is
/// identical with and without the memo.
///
/// Fixed capacity of one entry; dropped when the metric's loop ends, so memory
/// is bounded to a single label set plus, on the attribute-keyed path, one
/// copy of the raw attribute slice that keyed it.
struct SeriesIdMemo {
    last: Option<MemoEntry>,
}

/// One cached series identity and the input that produced it.
struct MemoEntry {
    /// The raw attribute slice of the point that built this entry, for the
    /// attribute-keyed path; `None` on the explode paths, which compare the
    /// built label set held in `labels` instead.
    attr_key: Option<Vec<KeyValue>>,
    labels: Arc<LabelSet>,
    series_id: SeriesId,
}

impl SeriesIdMemo {
    fn new() -> Self {
        SeriesIdMemo { last: None }
    }

    /// Resolve the id and shared label set for a gauge/sum/native-histogram
    /// point keyed on its raw `attributes`. On a hit the cached `Arc` is
    /// cloned and `build_labels` is never called; on a miss `build_labels`
    /// consumes `attributes` to build the set (moving attribute-name strings,
    /// the #367 optimisation), the id is computed, and both are cached along
    /// with a copy of `attributes` as the key.
    fn series_id_for_attributes<F>(
        &mut self,
        tenant: &TenantId,
        metric_name: &str,
        attributes: Vec<KeyValue>,
        build_labels: F,
    ) -> Result<(SeriesId, Arc<LabelSet>), Rejection>
    where
        F: FnOnce(Vec<KeyValue>) -> Result<LabelSet, Rejection>,
    {
        if let Some(entry) = &self.last
            && entry.attr_key.as_deref() == Some(attributes.as_slice())
        {
            #[cfg(any(test, feature = "memo-stats"))]
            memo_stats::record_hit();
            return Ok((entry.series_id, Arc::clone(&entry.labels)));
        }
        #[cfg(any(test, feature = "memo-stats"))]
        memo_stats::record_miss();
        let key = attributes.clone();
        let label_set = build_labels(attributes)?;
        let series_id = SeriesId::compute(tenant, metric_name, &label_set)
            .map_err(|_| Rejection::OversizedSeriesComponent)?;
        let labels = Arc::new(label_set);
        self.last = Some(MemoEntry {
            attr_key: Some(key),
            labels: Arc::clone(&labels),
            series_id,
        });
        Ok((series_id, labels))
    }

    /// Resolve the id and shared label set for an already-built `label_set`,
    /// keyed on the built set itself. Used by the explode paths, where the raw
    /// attribute slice does not distinguish the exploded series (see the type
    /// doc). The returned id is identical to calling `SeriesId::compute`
    /// directly.
    fn series_id_for_built(
        &mut self,
        tenant: &TenantId,
        metric_name: &str,
        label_set: LabelSet,
    ) -> Result<(SeriesId, Arc<LabelSet>), TypeError> {
        if let Some(entry) = &self.last
            && entry.attr_key.is_none()
            && *entry.labels == label_set
        {
            #[cfg(any(test, feature = "memo-stats"))]
            memo_stats::record_hit();
            return Ok((entry.series_id, Arc::clone(&entry.labels)));
        }
        #[cfg(any(test, feature = "memo-stats"))]
        memo_stats::record_miss();
        let series_id = SeriesId::compute(tenant, metric_name, &label_set)?;
        let labels = Arc::new(label_set);
        self.last = Some(MemoEntry {
            attr_key: None,
            labels: Arc::clone(&labels),
            series_id,
        });
        Ok((series_id, labels))
    }
}

/// Hit/miss counters for [`SeriesIdMemo`], compiled only under `cfg(test)` or
/// the `memo-stats` cargo feature; the production normalize path (neither cfg
/// active) carries none of this. Counts are thread-local and process-global
/// across every `SeriesIdMemo` on the thread, so a caller measures the memo's
/// aggregate behaviour over a whole `normalize_metrics` call by resetting
/// before it and reading the snapshot after. Used by the `normalize_alloc`
/// bench and pinned by a unit test; there is no production reader.
#[cfg(any(test, feature = "memo-stats"))]
pub mod memo_stats {
    use std::cell::Cell;

    thread_local! {
        static HITS: Cell<u64> = const { Cell::new(0) };
        static MISSES: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn record_hit() {
        HITS.with(|h| h.set(h.get() + 1));
    }

    pub(super) fn record_miss() {
        MISSES.with(|m| m.set(m.get() + 1));
    }

    /// Zero both counters on the current thread.
    pub fn reset() {
        HITS.with(|h| h.set(0));
        MISSES.with(|m| m.set(0));
    }

    /// `(hits, misses)` recorded on the current thread since the last
    /// [`reset`].
    pub fn snapshot() -> (u64, u64) {
        (HITS.with(Cell::get), MISSES.with(Cell::get))
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

/// Decode `dp_exemplars`, admit each through `cap` (ADR-0047 decision 2),
/// and push admitted ones into `exemplars_out`, tagged with whatever series
/// `series_id_for` resolves each candidate to (a fixed series for
/// [`build_point`] and [`build_native_histogram_point`]; a bucket lookup for
/// [`explode_histogram`]). Exemplars that are too malformed to carry (no
/// recognized value in the oneof) or that lose the cap are counted into
/// `informational` via the existing drop counter, so the count stays visible
/// rather than becoming invisible now that some exemplars are kept.
///
/// Candidates are offered to `cap` newest-first: [`ExemplarCap::admit`] never
/// retracts an earlier admission, so within one call (and therefore within
/// one series' window, whichever series that turns out to be) this is what
/// makes "keep the newest" hold even when a single data point carries
/// several exemplars for the same window.
fn admit_exemplars<F>(
    dp_exemplars: Vec<OtlpExemplar>,
    limits: &IngestLimits,
    cap: &mut ExemplarCap,
    series_id_for: F,
    exemplars_out: &mut Vec<NormalizedExemplar>,
    informational: &mut Vec<Rejection>,
) where
    F: Fn(&Exemplar) -> SeriesId,
{
    if dp_exemplars.is_empty() {
        return;
    }

    let capacity = dp_exemplars.len();
    let mut candidates = Vec::with_capacity(capacity);
    let mut dropped = 0usize;
    for ex in dp_exemplars {
        match decode_exemplar(ex, limits) {
            Some(e) => candidates.push(e),
            None => dropped += 1,
        }
    }
    // Stable, not unstable: candidates are built in wire order, so a stable
    // descending sort makes the first exemplar in wire order win a tie among
    // equal `ts_ns` (ExemplarCap::admit is first-wins). An unstable sort would
    // leave the tie winner implementation-defined rather than a property of the
    // input (ADR-0047 amendment: encoded bytes are a function of input order).
    candidates.sort_by_key(|c| std::cmp::Reverse(c.ts_ns));

    for candidate in candidates {
        let series_id = series_id_for(&candidate);
        if cap.admit(series_id, candidate.ts_ns) {
            exemplars_out.push(NormalizedExemplar {
                series_id,
                exemplar: candidate,
            });
        } else {
            dropped += 1;
        }
    }

    if dropped > 0 {
        informational.push(Rejection::HistogramExemplarsDropped { count: dropped });
    }
}

/// Decode one OTLP exemplar into the shared canonical shape (ADR-0047
/// decision 1), or `None` if it is too malformed to carry: OTLP itself calls
/// an exemplar "invalid" when neither oneof value is set, and that is the
/// only condition under which this returns `None` — a wrong-length trace or
/// span id is treated as absent rather than as a reason to drop the whole
/// exemplar (see [`parse_id`]), and an unparsable filtered-attribute value is
/// skipped rather than failing the exemplar, since filtered attributes are
/// informational context, not part of series identity.
fn decode_exemplar(ex: OtlpExemplar, limits: &IngestLimits) -> Option<Exemplar> {
    let value_bits = match ex.value {
        Some(OtlpExemplarValue::AsDouble(v)) => v.to_bits(),
        Some(OtlpExemplarValue::AsInt(v)) => (v as f64).to_bits(),
        None => return None,
    };
    let ts_ns = i64::try_from(ex.time_unix_nano).unwrap_or(i64::MAX);
    let trace_id = parse_id::<16>(&ex.trace_id);
    let span_id = parse_id::<8>(&ex.span_id);

    let mut filtered_attributes = Vec::with_capacity(
        ex.filtered_attributes
            .len()
            .min(limits.max_attributes_per_point),
    );
    for attr in ex
        .filtered_attributes
        .into_iter()
        .take(limits.max_attributes_per_point)
    {
        let value = any_value_to_label_value(attr.value.as_ref()).unwrap_or_default();
        let name = sanitize_label_name(attr.key);
        filtered_attributes.push(Label { name, value });
    }

    Some(Exemplar {
        ts_ns,
        value_bits,
        trace_id,
        span_id,
        filtered_attributes,
    })
}

/// Parse `bytes` into a fixed-size id, treating any length other than `N`
/// (well-formed) as absent (all-zero) rather than rejecting the exemplar
/// that carries it. This includes OTLP's own explicit "empty means absent"
/// case (length 0) and a malformed length, identically: both collapse to
/// the all-zero sentinel the RSEG layout and this module use for "no
/// trace/span id" (see `ravel_types::exemplar`'s module doc).
fn parse_id<const N: usize>(bytes: &[u8]) -> [u8; N] {
    <[u8; N]>::try_from(bytes).unwrap_or([0u8; N])
}

/// Build one point, plus zero or more informational rejections alongside it:
/// [`Rejection::IntegerValuePrecisionLoss`] when the point's `as_int` value
/// did not survive the conversion to `f64`, and
/// [`Rejection::HistogramExemplarsDropped`] for any of the point's exemplars
/// that were malformed or lost the per-series admission cap. The point is
/// admitted either way; the rounding is unavoidable given `f64`-only storage,
/// so it is surfaced, not rejected.
fn build_point(
    ctx: &PointContext,
    dp: &mut NumberDataPoint,
    memo: &mut SeriesIdMemo,
    exemplar_cap: &mut ExemplarCap,
    exemplars_out: &mut Vec<NormalizedExemplar>,
) -> Result<(NormalizedPoint, Vec<Rejection>), Rejection> {
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

    // ADR-0098: key the memo on the raw attribute slice, not on a built set.
    // On a hit `build_labels` never runs and the cached `Arc` is cloned; on a
    // miss it builds the set once (resource-label prefix, `__name__`, then the
    // sanitised attributes with their names moved in). Every check above ran
    // regardless, so a hit changes only whether labels are rebuilt.
    let (series_id, label_set) = memo.series_id_for_attributes(
        ctx.tenant,
        ctx.metric_name,
        std::mem::take(&mut dp.attributes),
        |attributes| {
            let mut labels = Vec::with_capacity(ctx.resource_labels.len() + attributes.len() + 1);
            labels.extend_from_slice(ctx.resource_labels);
            labels.push(Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: ctx.metric_name.to_string(),
            });
            push_attribute_labels(&mut labels, attributes, ctx.limits)?;
            LabelSet::new(labels).map_err(|err| match err {
                TypeError::DuplicateLabelName(name) => Rejection::DuplicateLabelName(name),
                // LabelSet::new only ever returns DuplicateLabelName; this arm
                // exists so a future TypeError variant can't silently pass
                // through as an accepted point.
                _ => Rejection::DuplicateLabelName(String::new()),
            })
        },
    )?;

    let mut informational: Vec<Rejection> = precision_loss.into_iter().collect();
    admit_exemplars(
        std::mem::take(&mut dp.exemplars),
        ctx.limits,
        exemplar_cap,
        |_| series_id,
        exemplars_out,
        &mut informational,
    );

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
        informational,
    ))
}

/// Build one native-histogram point from an `ExponentialHistogramDataPoint`
/// (ADR-0017). Rejection is atomic like [`build_point`]; `Ok` additionally
/// carries an informational drop for `min`/`max`, which the segment's
/// native-histogram sample has no place to store (ADR-0047 decision 6:
/// deliberately out of scope, unlike exemplars, which this function does
/// carry, admitted through the same per-series cap as every other point
/// type).
fn build_native_histogram_point(
    ctx: &NativeHistogramContext,
    dp: &mut ExponentialHistogramDataPoint,
    memo: &mut SeriesIdMemo,
    exemplar_cap: &mut ExemplarCap,
    exemplars_out: &mut Vec<NormalizedExemplar>,
) -> Result<(NormalizedHistogramPoint, Vec<Rejection>), Rejection> {
    if dp.attributes.len() > ctx.limits.max_attributes_per_point {
        return Err(Rejection::TooManyAttributes {
            attribute_count: dp.attributes.len(),
            max: ctx.limits.max_attributes_per_point,
        });
    }

    let event_ts_ns = checked_event_ts(dp.time_unix_nano, ctx.ingest_ts_ns, ctx.limits)?;
    let value = build_histogram_value(dp)?;

    // ADR-0098: a native histogram is one series per data point, keyed on the
    // raw attribute slice like gauge/sum (see [`build_point`]).
    let (series_id, label_set) = memo.series_id_for_attributes(
        ctx.tenant,
        ctx.metric_name,
        std::mem::take(&mut dp.attributes),
        |attributes| {
            let mut labels = Vec::with_capacity(ctx.resource_labels.len() + attributes.len() + 1);
            labels.extend_from_slice(ctx.resource_labels);
            labels.push(Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: ctx.metric_name.to_string(),
            });
            push_attribute_labels(&mut labels, attributes, ctx.limits)?;
            LabelSet::new(labels).map_err(|err| match err {
                TypeError::DuplicateLabelName(name) => Rejection::DuplicateLabelName(name),
                _ => Rejection::DuplicateLabelName(String::new()),
            })
        },
    )?;

    let mut informational = Vec::new();
    if dp.min.is_some() || dp.max.is_some() {
        informational.push(Rejection::HistogramMinMaxDropped { count: 1 });
    }
    admit_exemplars(
        std::mem::take(&mut dp.exemplars),
        ctx.limits,
        exemplar_cap,
        |_| series_id,
        exemplars_out,
        &mut informational,
    );

    Ok((
        NormalizedHistogramPoint {
            series_id,
            labels: label_set,
            sample: HistogramSample {
                ts_ns: event_ts_ns,
                value,
            },
        },
        informational,
    ))
}

/// Build the storage-side [`HistogramValue`] from an OTLP exponential
/// histogram data point. OTLP's `Buckets{offset, bucket_counts}` is a single
/// contiguous run per side (never sparse spans, unlike Prometheus Remote
/// Write's wire format), so each side maps to at most one [`HistogramSpan`].
/// OTLP counts are always integer (`fixed64`/`uint64`), so `counts` is
/// always [`HistogramCounts::Int`]; OTLP has no field to carry custom bucket
/// boundaries, so `custom_values` is always `None` and `scale == -53` (the
/// custom-buckets sentinel) is rejected rather than silently mismatched
/// against an absent boundary list. `reset_hint` is always `Unknown`: OTLP
/// carries no per-point reset signal, and only cumulative temporality is
/// admitted (delta rejects earlier, in `normalize_metric`).
fn build_histogram_value(dp: &ExponentialHistogramDataPoint) -> Result<HistogramValue, Rejection> {
    if dp.scale <= -53 {
        return Err(Rejection::NativeHistogramScaleUnsupported { scale: dp.scale });
    }

    let positive_spans = buckets_to_spans(dp.positive.as_ref());
    let negative_spans = buckets_to_spans(dp.negative.as_ref());
    let positive = dp
        .positive
        .as_ref()
        .map(|b| b.bucket_counts.clone())
        .unwrap_or_default();
    let negative = dp
        .negative
        .as_ref()
        .map(|b| b.bucket_counts.clone())
        .unwrap_or_default();

    let counts = HistogramCounts::Int {
        zero_count: dp.zero_count,
        count: dp.count,
        positive,
        negative,
    };
    validate_native_histogram_counts(&counts)?;

    Ok(HistogramValue {
        scale: dp.scale,
        zero_threshold: dp.zero_threshold,
        sum: dp.sum,
        custom_values: None,
        positive_spans,
        negative_spans,
        counts,
        reset_hint: ResetHint::Unknown,
    })
}

/// Map one OTLP bucket side to at most one contiguous [`HistogramSpan`]. An
/// absent or empty `Buckets` message means no populated buckets on that
/// side, encoded as zero spans (never a zero-length span, which the segment
/// writer rejects).
fn buckets_to_spans(buckets: Option<&Buckets>) -> Vec<HistogramSpan> {
    match buckets {
        Some(b) if !b.bucket_counts.is_empty() => vec![HistogramSpan {
            offset: b.offset,
            length: b.bucket_counts.len() as u32,
        }],
        _ => Vec::new(),
    }
}

/// `count` must be at least `zero_count` plus the sum of every bucket count
/// on both sides: the segment format's reader treats a violation as a
/// corrupted record, so an OTLP-admitted native histogram that fails this
/// check would be written today and reported unreadable at query time
/// forever after (data objects are immutable). Rejecting it here instead
/// keeps nothing ever written that a reader is documented to reject.
/// Integer arithmetic is overflow-checked; OTLP's counts are always integer,
/// so the float-kind arm this function's RW1/RW2 counterpart needs does not
/// apply here.
fn validate_native_histogram_counts(counts: &HistogramCounts) -> Result<(), Rejection> {
    match counts {
        HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative,
        } => {
            let bucket_sum = positive
                .iter()
                .chain(negative)
                .try_fold(0u64, |acc, v| acc.checked_add(*v))
                .ok_or(Rejection::NativeHistogramCountOverflow)?;
            let total = zero_count
                .checked_add(bucket_sum)
                .ok_or(Rejection::NativeHistogramCountOverflow)?;
            if *count < total {
                return Err(Rejection::NativeHistogramCountInconsistent);
            }
            Ok(())
        }
        HistogramCounts::Float { .. } => Ok(()),
    }
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
    attributes: Vec<KeyValue>,
    limits: &IngestLimits,
) -> Result<(), Rejection> {
    for attr in attributes {
        let value = any_value_to_label_value(attr.value.as_ref())?;
        let name = sanitize_label_name(attr.key);
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
    attributes: Vec<KeyValue>,
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

    // ADR-0098: the explode paths keep the built-set comparison. The exploded
    // series of one data point share a raw attribute slice and differ only in
    // `__name__` and the synthesized `le`/`quantile` label, so keying on that
    // slice would false-hit every bucket after the first onto the first
    // bucket's id and labels; comparing the built set cannot.
    let (series_id, label_set) = memo
        .series_id_for_built(ctx.tenant, metric_name, label_set)
        .map_err(|_| Rejection::OversizedSeriesComponent)?;

    Ok(NormalizedPoint {
        series_id,
        labels: label_set,
        sample: Sample { ts_ns, value },
        // Whether an exploded bucket/sum/count series behaves like a
        // monotonic counter downstream is not decided here; the field is
        // carried but never consumed today.
        is_monotonic_sum: false,
    })
}

/// Explode one `HistogramDataPoint` into its Prometheus-convention series:
/// one `{name}_bucket{le=<bound>}` per explicit bound plus
/// `{name}_bucket{le="+Inf"}` (= the point's count), `{name}_sum` when `sum`
/// is present, and `{name}_count`. Rejection is atomic: an `Err` means none
/// of this point's series were admitted, never a partial set (ADR-0016).
/// Ok also carries a zero-weight informational rejection for a dropped
/// min/max field, since the point itself was admitted. Exemplars are carried
/// (not dropped): each attaches to the bucket series whose `le` bound is the
/// smallest one at or above the exemplar's value (falling back to `+Inf`),
/// matching Prometheus's own bucket-exemplar convention, then is admitted
/// through the same per-series cap as every other point type.
fn explode_histogram(
    ctx: &ExplodeContext,
    dp: &mut HistogramDataPoint,
    memo: &mut SeriesIdMemo,
    exemplar_cap: &mut ExemplarCap,
    exemplars_out: &mut Vec<NormalizedExemplar>,
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

    let base_labels = build_explode_base_labels(ctx, std::mem::take(&mut dp.attributes))?;
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

    // Bucket series occupy series[0..expected_buckets] regardless of whether
    // `_sum` was pushed after them, since it and `_count` are always
    // appended last.
    let bucket_series = &series[0..expected_buckets];
    let dp_exemplars = std::mem::take(&mut dp.exemplars);
    let explicit_bounds = &dp.explicit_bounds;
    admit_exemplars(
        dp_exemplars,
        ctx.limits,
        exemplar_cap,
        |candidate| {
            let value = f64::from_bits(candidate.value_bits);
            let idx = exemplar_bucket_index(explicit_bounds, value);
            bucket_series[idx].series_id
        },
        exemplars_out,
        &mut informational,
    );

    Ok((series, informational))
}

/// The index into a histogram's ordered bucket series (explicit bounds, then
/// `+Inf`) that `value` falls into: the first bound at or above `value`, or
/// the `+Inf` bucket (index `explicit_bounds.len()`) if none qualifies. `<=`
/// is an ordering comparison against already-validated finite bounds, not an
/// equality test on a stored sample, so it does not conflict with the
/// bit-pattern comparison rule for exemplar values.
fn exemplar_bucket_index(explicit_bounds: &[f64], value: f64) -> usize {
    explicit_bounds
        .iter()
        .position(|&bound| value <= bound)
        .unwrap_or(explicit_bounds.len())
}

/// Explode one `SummaryDataPoint` into its Prometheus-convention series:
/// one `{name}{quantile=<q>}` per quantile plus `{name}_sum` and
/// `{name}_count`. Rejection is atomic, same as [`explode_histogram`].
fn explode_summary(
    ctx: &ExplodeContext,
    dp: &mut SummaryDataPoint,
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

    let base_labels = build_explode_base_labels(ctx, std::mem::take(&mut dp.attributes))?;
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
            // `key` is borrowed from the long-lived allowlist config, so it
            // cannot be moved; clone it to feed the by-value sanitiser. This is
            // once per resource attribute, not per data point.
            push_checked(&mut labels, sanitize_label_name(key.clone()), value, limits)?;
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
    // ADR-0038: a label with an empty value is treated as absent from the
    // series, matching Prometheus convention and what ravel-remote-write
    // already does (normalize.rs's `l.value.is_empty()` drop) and what
    // ravel-promql's matcher assumes (a missing label reads as ""). Drop it
    // before the length checks, exactly like remote-write, so every ingest
    // path hands SeriesId::compute the same label set for one logical series.
    if value.is_empty() {
        return Ok(());
    }
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

/// Apply the OTLP-to-Prometheus name suffixes (ADR-0085 Decision 2) to a
/// metric name that has already been through [`sanitize_metric_name`].
///
/// Order, matching the OpenTelemetry Collector's `prometheusexporter`
/// translator: the mapped unit suffix (appended only when the name does not
/// already end with it), then `_total` for a monotonic Sum (appended only when
/// the name does not already end with it), then a final re-sanitize so any
/// character the unit table introduced is still a valid metric-name character.
///
/// `kind` selects the dimensionless-ratio behavior (`unit == "1"` becomes
/// `_ratio` on a `Gauge` only) and is redundant with `is_monotonic_sum` for
/// the `_total` decision (`is_monotonic_sum` is `true` exactly when
/// `kind == Counter`); both are taken so a caller need not derive one from the
/// other. Pure and total: no allocation beyond the returned `String`, and it
/// never fails.
pub fn prometheus_family_name(
    sanitized: &str,
    unit: &str,
    kind: MetricKind,
    is_monotonic_sum: bool,
) -> String {
    let mut name = sanitized.to_string();
    if let Some(suffix) = unit_suffix(unit, kind) {
        let with_sep = format!("_{suffix}");
        if !name.ends_with(&with_sep) {
            name.push_str(&with_sep);
        }
    }
    if is_monotonic_sum && !name.ends_with("_total") {
        name.push_str("_total");
    }
    sanitize_metric_name(&name)
}

/// Map a whole (non-compound) UCUM unit through the OpenTelemetry Collector's
/// `unitMapper` table (ADR-0085 Decision 2), returning the Prometheus word or
/// `None` for an empty or unrecognized unit. The dimensionless `1` is not in
/// this table: its `_ratio` mapping is gauge-specific and lives in
/// [`unit_suffix`], since this mapper has no metric kind.
pub fn map_unit(unit: &str) -> Option<String> {
    let mapped = match unit {
        // time
        "d" => "days",
        "h" => "hours",
        "min" => "minutes",
        "s" => "seconds",
        "ms" => "milliseconds",
        "us" => "microseconds",
        "ns" => "nanoseconds",
        // bytes
        "By" => "bytes",
        "KiBy" => "kibibytes",
        "MiBy" => "mebibytes",
        "GiBy" => "gibibytes",
        "TiBy" => "tibibytes",
        "KBy" => "kilobytes",
        "MBy" => "megabytes",
        "GBy" => "gigabytes",
        "TBy" => "terabytes",
        // SI
        "m" => "meters",
        "V" => "volts",
        "A" => "amperes",
        "J" => "joules",
        "W" => "watts",
        "g" => "grams",
        // misc
        "Cel" => "celsius",
        "Hz" => "hertz",
        "%" => "percent",
        _ => return None,
    };
    Some(mapped.to_string())
}

/// Map a per-unit denominator through the Collector's `perUnitMapper` table
/// (ADR-0085 Decision 2). Unlike [`map_unit`], `m` here is `minute`, not
/// `meters`. An unrecognized denominator returns `None`; callers pass it
/// through sanitized rather than dropping it.
fn map_per_unit(unit: &str) -> Option<String> {
    let mapped = match unit {
        "s" => "second",
        "m" => "minute",
        "h" => "hour",
        "d" => "day",
        "w" => "week",
        "mo" => "month",
        "y" => "year",
        _ => return None,
    };
    Some(mapped.to_string())
}

/// Strip every `{...}` annotation from a unit, anywhere it appears and at any
/// nesting (ADR-0085 Decision 2: `{packet}/s` becomes `/s`). Unbalanced braces
/// are tolerated rather than treated as an error, since a unit string is
/// attacker-influenced input and must never panic here.
fn strip_annotations(unit: &str) -> String {
    let mut out = String::with_capacity(unit.len());
    let mut depth: usize = 0;
    for c in unit.chars() {
        match c {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    out
}

/// Compute the mapped unit suffix (without a leading `_`) for a unit string,
/// or `None` when it should add no suffix (empty, an unrecognized simple unit,
/// or `1` on a non-gauge). Handles the `a/b` compound form (each side mapped
/// independently, joined as `<a>_per_<b>`) and the annotation-only-side
/// collapse (`{packet}/s` yields `per_second`) from ADR-0085 Decision 2.
fn unit_suffix(unit: &str, kind: MetricKind) -> Option<String> {
    let stripped = strip_annotations(unit);
    let trimmed = stripped.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Dimensionless ratio: `_ratio` on a gauge, nothing on any other kind.
    if trimmed == "1" {
        return match kind {
            MetricKind::Gauge => Some("ratio".to_string()),
            _ => None,
        };
    }
    if let Some((num_raw, den_raw)) = trimmed.split_once('/') {
        let num = num_raw.trim();
        let den = den_raw.trim();
        // An empty side (its content was entirely an annotation) contributes
        // nothing; an unrecognized side passes through sanitized rather than
        // being guessed or dropped.
        let num_mapped =
            (!num.is_empty()).then(|| map_unit(num).unwrap_or_else(|| num.to_string()));
        let den_mapped =
            (!den.is_empty()).then(|| map_per_unit(den).unwrap_or_else(|| den.to_string()));
        match (num_mapped, den_mapped) {
            (Some(n), Some(d)) => Some(format!("{n}_per_{d}")),
            (Some(n), None) => Some(n),
            (None, Some(d)) => Some(format!("per_{d}")),
            (None, None) => None,
        }
    } else {
        map_unit(trimmed)
    }
}

/// The unit word stored in a metric's [`MetricMetadata`]: the mapped Prometheus
/// word when the unit maps (agreeing with the name suffix), otherwise the raw
/// unit with annotations stripped and trimmed, otherwise empty (ADR-0085
/// Decision 1/2).
///
/// This is the canonical OTel-unit-to-metadata-word mapping. Other crates that
/// store a [`MetricMetadata`] unit for an OTel metric (`ravel-otap`) must call
/// this rather than re-derive the table: one edit here changes every ingest
/// surface at once, and a private second copy silently drifts on the first
/// edit to either. It shares the same compound (`a/b`) and annotation handling
/// as the [`prometheus_family_name`] suffix, so the word stored in metadata and
/// the word appended to the metric name always agree.
pub fn metadata_unit_word(unit: &str, kind: MetricKind) -> String {
    if let Some(word) = unit_suffix(unit, kind) {
        return word;
    }
    // Unmapped: carry the raw unit through as free text. This is a metadata
    // field, not a metric name, so metric-name sanitizing rules do not apply
    // (they would turn `1` into `_` and `2h` into `_h`, neither of which is
    // the word an OpenMetrics `# UNIT` line would carry).
    strip_annotations(unit).trim().to_string()
}

/// The Prometheus [`MetricKind`] and monotonic-Sum flag a metric's `data`
/// oneof implies (ADR-0085 Decision 1): a monotonic `Sum` is a `Counter`, a
/// non-monotonic `Sum` and a `Gauge` are both `Gauge`, `Histogram` and
/// `ExponentialHistogram` are `Histogram`, and `Summary` is `Summary`. `None`
/// data carries no points and is handled before this is called.
fn metric_kind_of(data: &MetricData) -> (MetricKind, bool) {
    match data {
        MetricData::Gauge(_) => (MetricKind::Gauge, false),
        MetricData::Sum(s) if s.is_monotonic => (MetricKind::Counter, true),
        MetricData::Sum(_) => (MetricKind::Gauge, false),
        MetricData::Histogram(_) | MetricData::ExponentialHistogram(_) => {
            (MetricKind::Histogram, false)
        }
        MetricData::Summary(_) => (MetricKind::Summary, false),
    }
}

/// Accumulates one [`MetricMetadata`] per metric family that produced at least
/// one point, deduplicated by `family_name` with the first write winning
/// (ADR-0085 Decision 1). Threaded through normalization only by the
/// [`normalize_metrics_with_metadata`] entry point; the exemplar-only entry
/// points pass `None` and never allocate it.
#[derive(Default)]
struct MetadataCollector {
    entries: Vec<MetricMetadata>,
    seen: std::collections::HashSet<String>,
}

impl MetadataCollector {
    fn record(&mut self, meta: MetricMetadata) {
        if self.seen.insert(meta.family_name.clone()) {
            self.entries.push(meta);
        }
    }
}

fn sanitize_metric_name(name: &str) -> String {
    sanitize(
        name.to_owned(),
        is_metric_name_start,
        is_metric_name_continue,
    )
}

fn sanitize_label_name(name: String) -> String {
    sanitize(name, is_label_name_start, is_label_name_continue)
}

/// Rewrite every disallowed character to `_`, classifying the first character
/// with `is_start` and the rest with `is_continue`. Takes the name by value
/// and returns it unchanged (moved, not reallocated) when every character is
/// already valid; only a name that actually needs rewriting allocates a new
/// `String`. The produced bytes are identical either way. This is on the OTLP
/// metrics ingest path once per label per data point, where a clean name is
/// the common case, so the clean path must not allocate.
fn sanitize(input: String, is_start: fn(char) -> bool, is_continue: fn(char) -> bool) -> String {
    let all_valid = input
        .chars()
        .enumerate()
        .all(|(i, c)| if i == 0 { is_start(c) } else { is_continue(c) });
    if all_valid {
        return input;
    }
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
        Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
        ScopeMetrics, Sum, Summary, summary_data_point,
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

    fn strindex_kv(key: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::StringValueStrindex(1)),
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

    // --- SeriesIdMemo hit rate (pins the memo_stats counter, deliverable 3
    //     of #367) ---

    #[test]
    fn memo_hit_rate_grouped_vs_interleaved() {
        let tenant = tenant();
        let limits = IngestLimits::default();
        const P: usize = 100;

        // Grouped: one metric, P points all carrying the same attribute set, so
        // every point after the first is one series the memo already holds.
        let grouped = request(vec![resource_metrics(
            vec![],
            vec![gauge_metric(
                "grouped",
                (0..P)
                    .map(|i| {
                        number_point(
                            vec![string_kv("k", "v")],
                            1_000 + i as i64,
                            NumberValue::AsDouble(i as f64),
                        )
                    })
                    .collect(),
            )],
        )]);
        memo_stats::reset();
        let out = normalize_metrics(&tenant, grouped, &limits, 1_000_000);
        assert_eq!(out.points.len(), P);
        let (hits, misses) = memo_stats::snapshot();
        // One miss to seed the single series, then a hit for every later point.
        assert_eq!(misses, 1, "grouped: expected one seeding miss");
        assert_eq!(hits, (P - 1) as u64, "grouped: expected P-1 hits");

        // Interleaved: one metric, P points alternating between two distinct
        // attribute sets, so no point ever matches the one before it. The
        // one-entry memo never hits.
        let interleaved = request(vec![resource_metrics(
            vec![],
            vec![gauge_metric(
                "interleaved",
                (0..P)
                    .map(|i| {
                        let v = if i % 2 == 0 { "a" } else { "b" };
                        number_point(
                            vec![string_kv("k", v)],
                            1_000 + i as i64,
                            NumberValue::AsDouble(i as f64),
                        )
                    })
                    .collect(),
            )],
        )]);
        memo_stats::reset();
        let out = normalize_metrics(&tenant, interleaved, &limits, 1_000_000);
        assert_eq!(out.points.len(), P);
        let (hits, misses) = memo_stats::snapshot();
        assert_eq!(hits, 0, "interleaved: one-entry memo must never hit");
        assert_eq!(misses, P as u64, "interleaved: every point is a miss");
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
        // An allowlist entry that is invalid as a
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
    fn string_reference_attribute_rejection_names_the_strindex_case() {
        // A StringValueStrindex (string-table reference) attribute is
        // rejected like the other complex kinds, but the diagnostic must
        // name the string-reference case so a sender using string-table
        // references can tell what shape was rejected.
        let rm = resource_metrics(
            vec![],
            vec![gauge_metric(
                "widgets",
                vec![number_point(
                    vec![strindex_kv("region")],
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
        let message = out.rejected[0].to_string();
        assert!(
            message.contains("string-table reference") && message.contains("strindex"),
            "rejection message must name the string-reference case, got: {message}"
        );
    }

    #[test]
    fn resource_complex_attribute_value_rejects_every_point_under_it() {
        // service.name as a bytes value is invalid; every point under this
        // resource is rejected via one aggregated `Rejection::Grouped` entry
        // carrying the point count, not one clone per point.
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
            vec![Rejection::Grouped {
                reason: Box::new(Rejection::ComplexAttributeValue),
                count: 2,
            }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 2);
    }

    #[test]
    fn whole_resource_rejection_over_many_points_is_one_aggregated_entry() {
        // The scenario: a request with a huge number of data points, all
        // under one resource whose labels fail to build. Normalization must
        // produce exactly one `Rejection` value (not N clones) whose
        // `rejected_count()` still equals the point total (the counting
        // invariant), so the response can be built without materializing or
        // joining one string per point.
        const POINT_COUNT: usize = 50_000;
        let points = (0..POINT_COUNT)
            .map(|i| number_point(vec![], 1_000, NumberValue::AsDouble(i as f64)))
            .collect();
        let rm = resource_metrics(
            vec![bytes_kv("service.name")],
            vec![gauge_metric("w", points)],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected.len(),
            1,
            "expected one aggregated entry, not one per point"
        );
        assert_eq!(
            out.rejected[0],
            Rejection::Grouped {
                reason: Box::new(Rejection::ComplexAttributeValue),
                count: POINT_COUNT,
            }
        );
        let total: usize = out.rejected.iter().map(Rejection::rejected_count).sum();
        assert_eq!(total, POINT_COUNT);
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
            vec![Rejection::Grouped {
                reason: Box::new(Rejection::LabelValueTooLong { len: 7, max: 3 }),
                count: 1,
            }]
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
            vec![Rejection::Grouped {
                reason: Box::new(Rejection::LabelValueTooLong { len: 6, max: 5 }),
                count: 1,
            }]
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

    // --- native histograms (ADR-0017) ---

    fn exponential_histogram_metric(
        name: &str,
        data_points: Vec<ExponentialHistogramDataPoint>,
        temporality: AggregationTemporality,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            data: Some(MetricData::ExponentialHistogram(ExponentialHistogram {
                data_points,
                aggregation_temporality: temporality as i32,
            })),
            ..Default::default()
        }
    }

    #[test]
    fn exponential_histogram_metric_admitted_as_native_histogram_points() {
        let metric = exponential_histogram_metric(
            "latency_exp",
            vec![
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
            AggregationTemporality::Cumulative,
        );
        let rm = resource_metrics(vec![], vec![metric]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        // Native histograms never land in the scalar `points` vector.
        assert!(out.points.is_empty());
        assert_eq!(out.histogram_points.len(), 3);
        for p in &out.histogram_points {
            assert_eq!(p.sample.ts_ns, 1_000);
            assert_eq!(p.sample.value.scale, 0);
            assert_eq!(p.sample.value.custom_values, None);
            assert_eq!(p.sample.value.reset_hint, ResetHint::Unknown);
        }
    }

    #[test]
    fn exponential_histogram_captures_scale_and_bucket_shape() {
        let dp = ExponentialHistogramDataPoint {
            time_unix_nano: 1_000,
            count: 8,
            sum: Some(12.5),
            scale: 3,
            zero_count: 1,
            zero_threshold: 0.001,
            positive: Some(Buckets {
                offset: 2,
                bucket_counts: vec![1, 2, 3],
            }),
            negative: Some(Buckets {
                offset: -1,
                bucket_counts: vec![1],
            }),
            ..Default::default()
        };
        let metric = exponential_histogram_metric(
            "latency_exp",
            vec![dp],
            AggregationTemporality::Cumulative,
        );
        let rm = resource_metrics(vec![], vec![metric]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.histogram_points.len(), 1);
        let sample = &out.histogram_points[0].sample;
        assert_eq!(sample.value.scale, 3);
        assert_eq!(sample.value.zero_threshold, 0.001);
        assert_eq!(sample.value.sum, Some(12.5));
        assert_eq!(
            sample.value.positive_spans,
            vec![HistogramSpan {
                offset: 2,
                length: 3
            }]
        );
        assert_eq!(
            sample.value.negative_spans,
            vec![HistogramSpan {
                offset: -1,
                length: 1
            }]
        );
        assert_eq!(
            sample.value.counts,
            HistogramCounts::Int {
                zero_count: 1,
                count: 8,
                positive: vec![1, 2, 3],
                negative: vec![1],
            }
        );
    }

    #[test]
    fn exponential_histogram_delta_temporality_rejected() {
        let metric = exponential_histogram_metric(
            "latency_exp",
            vec![ExponentialHistogramDataPoint {
                time_unix_nano: 1_000,
                ..Default::default()
            }],
            AggregationTemporality::Delta,
        );
        let rm = resource_metrics(vec![], vec![metric]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.points.is_empty());
        assert!(out.histogram_points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::UnsupportedTemporality { count: 1 }]
        );
    }

    #[test]
    fn exponential_histogram_custom_buckets_scale_rejected() {
        // OTLP has no field for custom bucket boundaries; a scale == -53
        // (Prometheus native-histogram custom-buckets sentinel) has nothing
        // to attach it to and is rejected, not silently coerced.
        let metric = exponential_histogram_metric(
            "latency_exp",
            vec![ExponentialHistogramDataPoint {
                time_unix_nano: 1_000,
                scale: -53,
                ..Default::default()
            }],
            AggregationTemporality::Cumulative,
        );
        let rm = resource_metrics(vec![], vec![metric]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.histogram_points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::NativeHistogramScaleUnsupported { scale: -53 }]
        );
    }

    #[test]
    fn exponential_histogram_scale_below_minimum_rejected() {
        let metric = exponential_histogram_metric(
            "latency_exp",
            vec![ExponentialHistogramDataPoint {
                time_unix_nano: 1_000,
                scale: -54,
                ..Default::default()
            }],
            AggregationTemporality::Cumulative,
        );
        let rm = resource_metrics(vec![], vec![metric]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.histogram_points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::NativeHistogramScaleUnsupported { scale: -54 }]
        );
    }

    #[test]
    fn exponential_histogram_count_below_bucket_sum_rejected() {
        // count (1) is less than zero_count (0) plus the bucket sum (5): the
        // segment reader would treat this as corrupted, so it is rejected
        // typed at admission instead of ever being written.
        let dp = ExponentialHistogramDataPoint {
            time_unix_nano: 1_000,
            count: 1,
            positive: Some(Buckets {
                offset: 0,
                bucket_counts: vec![5],
            }),
            ..Default::default()
        };
        let metric = exponential_histogram_metric(
            "latency_exp",
            vec![dp],
            AggregationTemporality::Cumulative,
        );
        let rm = resource_metrics(vec![], vec![metric]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert!(out.histogram_points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::NativeHistogramCountInconsistent]
        );
    }

    #[test]
    fn exponential_histogram_min_max_and_exemplars_dropped_informationally() {
        let dp = ExponentialHistogramDataPoint {
            time_unix_nano: 1_000,
            min: Some(0.1),
            max: Some(9.9),
            exemplars: vec![
                Exemplar {
                    time_unix_nano: 999,
                    ..Default::default()
                },
                Exemplar {
                    time_unix_nano: 998,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let metric = exponential_histogram_metric(
            "latency_exp",
            vec![dp],
            AggregationTemporality::Cumulative,
        );
        let rm = resource_metrics(vec![], vec![metric]);
        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );
        assert_eq!(out.histogram_points.len(), 1);
        assert_eq!(
            out.rejected,
            vec![
                Rejection::HistogramMinMaxDropped { count: 1 },
                Rejection::HistogramExemplarsDropped { count: 2 },
            ]
        );
        for r in &out.rejected {
            assert_eq!(r.rejected_count(), 0);
        }
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

    // --- series-id memoization ---

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

        let label_sets: Vec<Arc<LabelSet>> = out.points.iter().map(|p| p.labels.clone()).collect();
        let name = "http_requests_total";

        // before: recompute the id for every point (pre-memo behavior).
        let t0 = std::time::Instant::now();
        let mut before_ids = Vec::with_capacity(label_sets.len());
        for ls in &label_sets {
            before_ids.push(SeriesId::compute(&tenant(), name, ls).expect("compute id"));
        }
        let before = t0.elapsed();

        // after: last-seen memo over the same order (built-set comparison).
        let t1 = std::time::Instant::now();
        let mut memo = SeriesIdMemo::new();
        let mut after_ids = Vec::with_capacity(label_sets.len());
        for ls in &label_sets {
            after_ids.push(
                memo.series_id_for_built(&tenant(), name, (**ls).clone())
                    .expect("compute id")
                    .0,
            );
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
    fn histogram_explosion_matches_cross_protocol_identity_vector() {
        // The same logical histogram ingested OTLP-exploded and RW-classic
        // must land on identical SeriesIds and values. The RW-classic side
        // has its own test in its own crate; this asserts the OTLP side
        // reaches the exact canonical label sets that side would also
        // construct.
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

    // --- exemplars (ADR-0047) ---

    fn otlp_exemplar(
        ts_ns: i64,
        value: f64,
        trace_id: Vec<u8>,
        span_id: Vec<u8>,
        filtered_attributes: Vec<KeyValue>,
    ) -> Exemplar {
        Exemplar {
            time_unix_nano: ts_ns as u64,
            span_id,
            trace_id,
            value: Some(OtlpExemplarValue::AsDouble(value)),
            filtered_attributes,
        }
    }

    /// Two exemplars on one series with identical `ts_ns` must break the
    /// tie deterministically. Candidates are sorted descending by `ts_ns` with a
    /// stable sort, so the first in wire order wins the tie and is offered to the
    /// cap first. Distinguishable values let the assertion name the survivor; an
    /// unstable sort could keep either.
    ///
    /// 300 exemplars over 5 timestamps, all inside one cap window, so the cap
    /// admits exactly one. The size and the mixed timestamps are the point:
    /// `sort_unstable` insertion-sorts a short slice, which preserves input
    /// order, so a two-element or all-equal input cannot tell a stable sort from
    /// an unstable one. At this length, with duplicates among distinct keys,
    /// swapping `sort_by_key` for `sort_unstable_by_key` does fail it.
    #[test]
    fn identical_timestamp_exemplars_keep_the_first_in_wire_order() {
        let timestamps_ns = [3_000i64, 9_000, 1_000, 9_000, 5_000];
        let mut exemplars = Vec::with_capacity(300);
        for round in 0..60i64 {
            for (slot, ts_ns) in timestamps_ns.iter().enumerate() {
                // Value encodes wire position, so the assertion names one
                // exemplar exactly.
                let position = round * 5 + slot as i64;
                exemplars.push(otlp_exemplar(
                    *ts_ns,
                    position as f64,
                    vec![],
                    vec![],
                    vec![],
                ));
            }
        }
        // The newest timestamp is 9_000 ns, and its first appearance in wire
        // order is position 1.
        let expected_position = 1.0f64;

        let mut dp = number_point(vec![], 1_000, NumberValue::AsDouble(42.0));
        dp.exemplars = exemplars;
        let rm = resource_metrics(vec![], vec![gauge_metric("request_latency", vec![dp])]);

        // 10 s window: every timestamp above lands in the window starting at 0.
        let mut cap = ExemplarCap::new(10_000_000_000);
        let result = normalize_metrics_with_exemplars(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
            &mut cap,
        );

        assert_eq!(result.exemplars.len(), 1);
        assert_eq!(
            result.exemplars[0].exemplar.ts_ns, 9_000,
            "the newest timestamp in the window must win"
        );
        assert_eq!(
            result.exemplars[0].exemplar.value_bits,
            expected_position.to_bits(),
            "among equal timestamps the first in wire order must win"
        );
        assert_eq!(
            result.output.rejected,
            vec![Rejection::HistogramExemplarsDropped { count: 299 }]
        );
    }

    /// One series, one exemplar, through the production entry point
    /// (`normalize_metrics`, the throwaway-cap wrapper). The exemplar is admitted
    /// by the cap but discarded by the wrapper, since nothing stores exemplars on
    /// this path yet; it must still be counted as dropped, never silently thrown
    /// away with the counter reading zero.
    #[test]
    fn one_admitted_then_discarded_exemplar_is_counted_as_dropped() {
        let mut dp = number_point(vec![], 1_000, NumberValue::AsDouble(1.0));
        dp.exemplars = vec![otlp_exemplar(1_000, 1.0, vec![7; 16], vec![3; 8], vec![])];
        let rm = resource_metrics(vec![], vec![gauge_metric("m", vec![dp])]);

        let out = normalize_metrics(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
        );

        assert!(out.points.len() == 1);
        assert_eq!(
            out.rejected,
            vec![Rejection::HistogramExemplarsDropped { count: 1 }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 0);
    }

    /// A request over the data-point limit is rejected whole,
    /// before any point is inspected. Every exemplar it carried is dropped with
    /// it and must be counted, matching the Remote Write twin's
    /// `TooManyDataPoints` early return, so the dropped-data counter does not
    /// read zero while exemplars are lost.
    #[test]
    fn too_many_data_points_counts_the_requests_exemplars_as_dropped() {
        let limits = IngestLimits {
            max_data_points_per_request: 2,
            ..IngestLimits::default()
        };
        let mut dp1 = number_point(vec![], 1_000, NumberValue::AsDouble(1.0));
        dp1.exemplars = vec![otlp_exemplar(1_000, 1.0, vec![], vec![], vec![])];
        let mut dp2 = number_point(vec![], 1_000, NumberValue::AsDouble(2.0));
        dp2.exemplars = vec![
            otlp_exemplar(1_000, 2.0, vec![], vec![], vec![]),
            otlp_exemplar(1_000, 3.0, vec![], vec![], vec![]),
        ];
        let dp3 = number_point(vec![], 1_000, NumberValue::AsDouble(4.0));
        let rm = resource_metrics(vec![], vec![gauge_metric("m", vec![dp1, dp2, dp3])]);

        let mut cap = ExemplarCap::default();
        let result = normalize_metrics_with_exemplars(
            &tenant(),
            request(vec![rm]),
            &limits,
            1_000,
            &mut cap,
        );

        assert!(result.exemplars.is_empty());
        assert_eq!(
            result.output.rejected,
            vec![
                Rejection::TooManyDataPoints { count: 3, max: 2 },
                Rejection::HistogramExemplarsDropped { count: 3 },
            ]
        );
    }

    #[test]
    fn exemplars_are_carried_and_capped_per_series_window() {
        let mut dp = number_point(vec![], 1_000, NumberValue::AsDouble(42.0));
        dp.exemplars = vec![
            otlp_exemplar(100, 3.5, vec![], vec![], vec![]),
            otlp_exemplar(500, 1.5, vec![], vec![], vec![]),
            // Newest of the three, and the only one carrying trace/span id
            // and a filtered attribute: this is the one that must survive.
            otlp_exemplar(
                700,
                2.5,
                vec![7; 16],
                vec![3; 8],
                vec![string_kv("db.name", "orders")],
            ),
        ];
        let metric = gauge_metric("request_latency", vec![dp]);
        let rm = resource_metrics(vec![], vec![metric]);

        let mut cap = ExemplarCap::new(10_000_000_000);
        let result = normalize_metrics_with_exemplars(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
            &mut cap,
        );

        assert_eq!(result.output.points.len(), 1);
        let series_id = result.output.points[0].series_id;

        // Three exemplars land in the same window; exactly the newest
        // survives, every field intact.
        assert_eq!(result.exemplars.len(), 1);
        let kept = &result.exemplars[0].exemplar;
        assert_eq!(result.exemplars[0].series_id, series_id);
        assert_eq!(kept.ts_ns, 700);
        assert_eq!(kept.value_bits, 2.5f64.to_bits());
        assert_eq!(kept.trace_id, [7u8; 16]);
        assert_eq!(kept.span_id, [3u8; 8]);
        assert_eq!(
            kept.filtered_attributes,
            vec![Label {
                name: "db_name".to_string(),
                value: "orders".to_string(),
            }]
        );

        // The other two are counted as dropped, not silently discarded and
        // not inflating the sender-facing rejected-points count.
        assert_eq!(
            result.output.rejected,
            vec![Rejection::HistogramExemplarsDropped { count: 2 }]
        );
        assert_eq!(result.output.rejected[0].rejected_count(), 0);
    }

    #[test]
    fn exemplar_value_round_trips_nan_payload_and_negative_zero_by_bit_pattern() {
        let nan_bits: u64 = 0x7ff8_0000_0000_0001;
        let nan_value = f64::from_bits(nan_bits);

        let mut dp_nan = number_point(vec![], 1_000, NumberValue::AsDouble(1.0));
        dp_nan.exemplars = vec![otlp_exemplar(1_000, nan_value, vec![], vec![], vec![])];
        let mut dp_negzero = number_point(vec![], 1_000, NumberValue::AsDouble(1.0));
        dp_negzero.exemplars = vec![otlp_exemplar(1_000, -0.0, vec![], vec![], vec![])];

        let rm = resource_metrics(
            vec![],
            vec![
                gauge_metric("nan_metric", vec![dp_nan]),
                gauge_metric("negzero_metric", vec![dp_negzero]),
            ],
        );

        let mut cap = ExemplarCap::default();
        let result = normalize_metrics_with_exemplars(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
            &mut cap,
        );

        assert_eq!(result.exemplars.len(), 2);
        assert_eq!(result.exemplars[0].exemplar.value_bits, nan_bits);
        assert_eq!(result.exemplars[1].exemplar.value_bits, (-0.0f64).to_bits());
        // Bit patterns distinguish -0.0 from 0.0, unlike `==`.
        assert_ne!(result.exemplars[1].exemplar.value_bits, 0.0f64.to_bits());
    }

    #[test]
    fn exemplar_with_absent_trace_id_or_absent_span_id_is_carried() {
        let mut dp_no_trace = number_point(vec![], 1_000, NumberValue::AsDouble(1.0));
        dp_no_trace.exemplars = vec![otlp_exemplar(1_000, 1.0, vec![], vec![9; 8], vec![])];
        let mut dp_no_span = number_point(vec![], 1_000, NumberValue::AsDouble(2.0));
        dp_no_span.exemplars = vec![otlp_exemplar(1_000, 2.0, vec![5; 16], vec![], vec![])];

        let rm = resource_metrics(
            vec![],
            vec![
                gauge_metric("no_trace_metric", vec![dp_no_trace]),
                gauge_metric("no_span_metric", vec![dp_no_span]),
            ],
        );

        let mut cap = ExemplarCap::default();
        let result = normalize_metrics_with_exemplars(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
            &mut cap,
        );

        assert_eq!(result.exemplars.len(), 2);
        assert_eq!(result.exemplars[0].exemplar.trace_id, [0u8; 16]);
        assert_eq!(result.exemplars[0].exemplar.span_id, [9u8; 8]);
        assert_eq!(result.exemplars[1].exemplar.trace_id, [5u8; 16]);
        assert_eq!(result.exemplars[1].exemplar.span_id, [0u8; 8]);
    }

    #[test]
    fn exemplar_cap_is_per_series_not_global() {
        let mut dp_a = number_point(
            vec![string_kv("shard", "a")],
            1_000,
            NumberValue::AsDouble(1.0),
        );
        dp_a.exemplars = vec![otlp_exemplar(1_000, 1.0, vec![1; 16], vec![1; 8], vec![])];
        let mut dp_b = number_point(
            vec![string_kv("shard", "b")],
            1_000,
            NumberValue::AsDouble(2.0),
        );
        dp_b.exemplars = vec![otlp_exemplar(1_000, 2.0, vec![2; 16], vec![2; 8], vec![])];

        let metric = gauge_metric("m", vec![dp_a, dp_b]);
        let rm = resource_metrics(vec![], vec![metric]);

        let mut cap = ExemplarCap::default();
        let result = normalize_metrics_with_exemplars(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
            &mut cap,
        );

        assert_eq!(result.output.points.len(), 2);
        assert!(result.output.rejected.is_empty());
        assert_eq!(result.exemplars.len(), 2);

        let point_series: HashSet<_> = result.output.points.iter().map(|p| p.series_id).collect();
        let exemplar_series: HashSet<_> = result.exemplars.iter().map(|e| e.series_id).collect();
        assert_eq!(point_series, exemplar_series);
    }

    #[test]
    fn exemplar_with_no_recognized_value_is_dropped_and_counted_not_carried() {
        let mut dp = number_point(vec![], 1_000, NumberValue::AsDouble(1.0));
        dp.exemplars = vec![Exemplar {
            time_unix_nano: 999,
            ..Default::default()
        }];
        let metric = gauge_metric("m", vec![dp]);
        let rm = resource_metrics(vec![], vec![metric]);

        let mut cap = ExemplarCap::default();
        let result = normalize_metrics_with_exemplars(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
            &mut cap,
        );

        assert!(result.exemplars.is_empty());
        assert_eq!(
            result.output.rejected,
            vec![Rejection::HistogramExemplarsDropped { count: 1 }]
        );
    }

    #[test]
    fn classic_histogram_exemplar_attaches_to_matching_bucket_series() {
        let mut dp = histogram_point(vec![], 1_000, 3, Some(6.0), vec![1.0, 10.0], vec![1, 1, 1]);
        // Falls in the `le=10.0` bucket (1.0 < 5.0 <= 10.0).
        dp.exemplars = vec![otlp_exemplar(1_000, 5.0, vec![9; 16], vec![9; 8], vec![])];
        let rm = resource_metrics(
            vec![],
            vec![histogram_metric(
                "latency",
                vec![dp],
                AggregationTemporality::Cumulative,
            )],
        );

        let mut cap = ExemplarCap::default();
        let result = normalize_metrics_with_exemplars(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000,
            &mut cap,
        );

        assert_eq!(result.exemplars.len(), 1);
        let bucket_10 = result
            .output
            .points
            .iter()
            .find(|p| p.labels.iter().any(|l| l.name == "le" && l.value == "10"))
            .expect("le=10 bucket series exists");
        assert_eq!(result.exemplars[0].series_id, bucket_10.series_id);
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
    fn summary_explosion_basic_shape_and_identity() {
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
        // A gauge/sum data point carrying the NoRecordedValue flag maps to a
        // stale marker rather than having its flags ignored.
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

    // --- ADR-0085 Decision 2: OTLP-to-Prometheus name suffixing, and
    // Decision 1: per-metric metadata surfaced from normalize ---

    fn cap() -> ExemplarCap {
        ExemplarCap::new(IngestLimits::default().exemplar_cap_window_ns)
    }

    fn gauge_u(name: &str, unit: &str) -> Metric {
        Metric {
            name: name.to_string(),
            unit: unit.to_string(),
            data: Some(MetricData::Gauge(Gauge {
                data_points: vec![number_point(vec![], 1_000_000, NumberValue::AsDouble(1.0))],
            })),
            ..Default::default()
        }
    }

    fn mono_sum_u(name: &str, unit: &str) -> Metric {
        Metric {
            name: name.to_string(),
            unit: unit.to_string(),
            data: Some(MetricData::Sum(Sum {
                data_points: vec![number_point(vec![], 1_000_000, NumberValue::AsDouble(1.0))],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        }
    }

    /// Normalize a one-metric request with metadata, returning the distinct
    /// `__name__` values its points carry (sorted) and the metadata list.
    fn suffixed(metric: Metric) -> (Vec<String>, Vec<MetricMetadata>) {
        let rm = resource_metrics(vec![], vec![metric]);
        let mut c = cap();
        let (result, metadata) = normalize_metrics_with_metadata(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000_000,
            &mut c,
        );
        assert!(
            result.output.rejected.is_empty(),
            "{:?}",
            result.output.rejected
        );
        let mut names: Vec<String> = result
            .output
            .points
            .iter()
            .map(|p| {
                p.labels
                    .get(METRIC_NAME_LABEL)
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        names.sort();
        names.dedup();
        (names, metadata)
    }

    #[test]
    fn monotonic_sum_with_unit_gets_unit_then_total_suffix() {
        let mut metric = mono_sum_u("foo", "By");
        metric.description = "bytes processed".to_string();
        let (names, metadata) = suffixed(metric);
        assert_eq!(names, vec!["foo_bytes_total".to_string()]);
        assert_eq!(
            metadata,
            vec![MetricMetadata {
                family_name: "foo_bytes_total".to_string(),
                kind: MetricKind::Counter,
                help: "bytes processed".to_string(),
                unit: "bytes".to_string(),
            }]
        );
    }

    #[test]
    fn name_suffix_cases() {
        use MetricKind::{Counter, Gauge};
        assert_eq!(
            prometheus_family_name("foo", "1", Gauge, false),
            "foo_ratio"
        );
        assert_eq!(
            prometheus_family_name("foo", "1", Counter, true),
            "foo_total"
        );
        assert_eq!(
            prometheus_family_name("foo", "By/s", Gauge, false),
            "foo_bytes_per_second"
        );
        assert_eq!(
            prometheus_family_name("foo", "{packet}/s", Gauge, false),
            "foo_per_second"
        );
        assert_eq!(
            prometheus_family_name("foo", "furlong", Gauge, false),
            "foo"
        );
        assert_eq!(
            prometheus_family_name("foo_total", "", Counter, true),
            "foo_total"
        );
        assert_eq!(
            prometheus_family_name("foo_bytes", "By", Gauge, false),
            "foo_bytes"
        );
        assert_eq!(
            prometheus_family_name("foo", "ms", Gauge, false),
            "foo_milliseconds"
        );
        // A passthrough compound side with characters needing sanitizing
        // survives the final re-sanitize: `a.b/c` -> `foo_a_b_per_c`.
        assert_eq!(
            prometheus_family_name("foo", "a.b/c", Gauge, false),
            "foo_a_b_per_c"
        );
    }

    #[test]
    fn unit_table_is_complete() {
        let table = [
            ("d", "days"),
            ("h", "hours"),
            ("min", "minutes"),
            ("s", "seconds"),
            ("ms", "milliseconds"),
            ("us", "microseconds"),
            ("ns", "nanoseconds"),
            ("By", "bytes"),
            ("KiBy", "kibibytes"),
            ("MiBy", "mebibytes"),
            ("GiBy", "gibibytes"),
            ("TiBy", "tibibytes"),
            ("KBy", "kilobytes"),
            ("MBy", "megabytes"),
            ("GBy", "gigabytes"),
            ("TBy", "terabytes"),
            ("m", "meters"),
            ("V", "volts"),
            ("A", "amperes"),
            ("J", "joules"),
            ("W", "watts"),
            ("g", "grams"),
            ("Cel", "celsius"),
            ("Hz", "hertz"),
            ("%", "percent"),
        ];
        for (unit, word) in table {
            assert_eq!(map_unit(unit).as_deref(), Some(word), "map_unit({unit})");
            assert_eq!(
                prometheus_family_name("foo", unit, MetricKind::Gauge, false),
                format!("foo_{word}"),
                "family({unit})"
            );
        }
        assert_eq!(map_unit("furlong"), None);
        assert_eq!(map_unit(""), None);
    }

    /// Direct contract test for the now-`pub` [`metadata_unit_word`], the
    /// canonical mapping `ravel-otap` calls rather than re-derive. Other tests
    /// exercise it only through the end-to-end `suffixed` metadata; this pins
    /// each shape at the public boundary so a caller in another crate sees a
    /// stable contract: mapped word, dimensionless ratio (gauge only), raw
    /// passthrough for an unmapped or non-gauge `1`, annotation stripping, the
    /// compound per-unit form, and empty.
    #[test]
    fn metadata_unit_word_contract() {
        use MetricKind::{Counter, Gauge};
        assert_eq!(metadata_unit_word("By", Counter), "bytes");
        assert_eq!(metadata_unit_word("s", MetricKind::Histogram), "seconds");
        assert_eq!(metadata_unit_word("", Gauge), "");
        assert_eq!(metadata_unit_word("1", Gauge), "ratio");
        // `1` maps to nothing off a gauge, so the raw `1` carries through.
        assert_eq!(metadata_unit_word("1", Counter), "1");
        assert_eq!(metadata_unit_word("furlong", Gauge), "furlong");
        assert_eq!(metadata_unit_word("2h", Gauge), "2h");
        assert_eq!(metadata_unit_word("{request}", Gauge), "");
        assert_eq!(metadata_unit_word("{packet}/s", Gauge), "per_second");
        assert_eq!(metadata_unit_word("By/s", Gauge), "bytes_per_second");
        assert_eq!(metadata_unit_word("By/m", Gauge), "bytes_per_minute");
        assert_eq!(
            metadata_unit_word("furlong/fortnight", Gauge),
            "furlong_per_fortnight"
        );
    }

    #[test]
    fn gauge_ratio_end_to_end() {
        let (names, metadata) = suffixed(gauge_u("cpu", "1"));
        assert_eq!(names, vec!["cpu_ratio".to_string()]);
        assert_eq!(metadata[0].unit, "ratio");
        assert_eq!(metadata[0].kind, MetricKind::Gauge);
    }

    #[test]
    fn metadata_unit_is_mapped_or_raw_or_empty_never_name_sanitized() {
        let (_names, metadata) = suffixed(gauge_u("foo", "furlong"));
        assert_eq!(metadata[0].unit, "furlong");
        let (_names, metadata) = suffixed(gauge_u("bar", ""));
        assert_eq!(metadata[0].unit, "");
        let (_names, metadata) = suffixed(gauge_u("baz", "By"));
        assert_eq!(metadata[0].unit, "bytes");
        // A monotonic Sum with unit `1` gets no `_ratio` (Gauge-only), so the
        // unit is unmapped for it; the metadata field must carry the raw `1`,
        // not the metric-name-sanitized `_`. Same for a unit like `2h`: free
        // text, not an identifier.
        let (names, metadata) = suffixed(mono_sum_u("qux", "1"));
        assert_eq!(names, vec!["qux_total".to_string()]);
        assert_eq!(metadata[0].unit, "1");
        let (_names, metadata) = suffixed(gauge_u("quux", "2h"));
        assert_eq!(metadata[0].unit, "2h");
        // Annotations are still stripped from the raw carry-through.
        let (_names, metadata) = suffixed(gauge_u("corge", "{request}"));
        assert_eq!(metadata[0].unit, "");
    }

    #[test]
    fn classic_histogram_unit_suffix_before_structural() {
        let hp = histogram_point(
            vec![],
            1_000_000,
            3,
            Some(6.0),
            vec![1.0, 2.0],
            vec![1, 1, 1],
        );
        let metric = Metric {
            name: "foo".to_string(),
            unit: "s".to_string(),
            data: Some(MetricData::Histogram(Histogram {
                data_points: vec![hp],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        };
        let (names, metadata) = suffixed(metric);
        assert!(
            names.contains(&"foo_seconds_bucket".to_string()),
            "{names:?}"
        );
        assert!(names.contains(&"foo_seconds_sum".to_string()), "{names:?}");
        assert!(
            names.contains(&"foo_seconds_count".to_string()),
            "{names:?}"
        );
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].family_name, "foo_seconds");
        assert_eq!(metadata[0].kind, MetricKind::Histogram);
    }

    #[test]
    fn metadata_deduplicated_by_family_first_wins() {
        let mut a = gauge_u("foo", "");
        a.description = "first".to_string();
        let mut b = gauge_u("foo", "");
        b.description = "second".to_string();
        let rm = resource_metrics(vec![], vec![a, b]);
        let mut c = cap();
        let (_r, metadata) = normalize_metrics_with_metadata(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000_000,
            &mut c,
        );
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].help, "first");
    }

    #[test]
    fn metadata_absent_when_no_points_produced() {
        let m = sum_metric(
            "requests",
            vec![number_point(vec![], 1_000_000, NumberValue::AsDouble(1.0))],
            AggregationTemporality::Delta,
            true,
        );
        let rm = resource_metrics(vec![], vec![m]);
        let mut c = cap();
        let (r, metadata) = normalize_metrics_with_metadata(
            &tenant(),
            request(vec![rm]),
            &IngestLimits::default(),
            1_000_000,
            &mut c,
        );
        assert!(r.output.points.is_empty());
        assert!(metadata.is_empty());
    }

    // ---- #367: label-name sanitiser (by value, no allocation when clean) ----

    /// Reference oracle: the pre-#367 sanitiser, kept verbatim. The property
    /// tests below prove the by-value implementation returns byte-identical
    /// output to this for arbitrary input, so canonical series identity
    /// (ADR-0005) is unchanged.
    fn sanitize_oracle(
        input: &str,
        is_start: fn(char) -> bool,
        is_continue: fn(char) -> bool,
    ) -> String {
        input
            .chars()
            .enumerate()
            .map(|(i, c)| {
                let allowed = if i == 0 { is_start(c) } else { is_continue(c) };
                if allowed { c } else { '_' }
            })
            .collect()
    }

    proptest::proptest! {
        #[test]
        fn sanitize_label_name_matches_oracle(
            chars in proptest::collection::vec(proptest::prelude::any::<char>(), 0..24)
        ) {
            let s: String = chars.into_iter().collect();
            let expected = sanitize_oracle(&s, is_label_name_start, is_label_name_continue);
            proptest::prop_assert_eq!(sanitize_label_name(s.clone()), expected);
        }

        #[test]
        fn sanitize_metric_name_matches_oracle(
            chars in proptest::collection::vec(proptest::prelude::any::<char>(), 0..24)
        ) {
            let s: String = chars.into_iter().collect();
            let expected = sanitize_oracle(&s, is_metric_name_start, is_metric_name_continue);
            proptest::prop_assert_eq!(sanitize_metric_name(&s), expected);
        }
    }

    #[test]
    fn sanitize_label_name_covers_edge_inputs() {
        // Deterministic coverage of the cases the random strategy reaches only
        // rarely, each compared against the oracle.
        for input in [
            "",              // empty
            "0abc",          // leading digit
            "...",           // entirely disallowed
            "naïve.count",   // non-ASCII plus a rewrite
            "λ",             // non-ASCII, single char, disallowed
            "already_clean", // already valid, hits the no-rewrite path
        ] {
            let expected = sanitize_oracle(input, is_label_name_start, is_label_name_continue);
            assert_eq!(
                sanitize_label_name(input.to_string()),
                expected,
                "input {input:?}"
            );
        }
    }

    #[test]
    fn clean_label_name_is_returned_without_reallocating() {
        // A clean name must be moved into the Label, not copied. Capture the
        // heap pointer and capacity before the move; the returned String must
        // own the same allocation. A byte-equality check alone would pass
        // against the old always-allocating implementation and prove nothing.
        let input = String::from("http_status_code");
        let ptr_before = input.as_ptr();
        let cap_before = input.capacity();
        let out = sanitize_label_name(input);
        assert_eq!(out.as_ptr(), ptr_before, "clean name was reallocated");
        assert_eq!(out.capacity(), cap_before, "clean name was reallocated");
        assert_eq!(out, "http_status_code");
    }

    #[test]
    fn dirty_label_name_allocates_and_rewrites() {
        // The complement: a name needing a rewrite does build a new String,
        // with bytes identical to the oracle's.
        let out = sanitize_label_name(String::from("http.status.code"));
        assert_eq!(out, "http_status_code");
    }

    // --- ADR-0098: shared label set per series run ---

    const MEMO_TS: i64 = 1_700_000_000_000_000_000;

    /// ADR-0098 test 1 (the trap). One classic histogram data point with
    /// several bounds explodes into `{name}_bucket{le=..}` per bound, `+Inf`,
    /// `_sum`, and `_count`. Every exploded series must get a DISTINCT series
    /// id, each equal to computing that id directly with no memo. A memo keyed
    /// on the raw attribute slice alone would hand every bucket after the first
    /// the first bucket's id and labels (they share one attribute slice and
    /// differ only in `__name__` and `le`), which this distinctness check
    /// catches; the explode path keeps the built-set comparison instead.
    #[test]
    fn histogram_explode_yields_distinct_ids_through_the_memo() {
        let dp = histogram_point(
            vec![string_kv("host", "a")],
            MEMO_TS,
            10,
            Some(5.0),
            vec![1.0, 2.5, 5.0],
            vec![2, 3, 4, 1],
        );
        let out = normalize_metrics(
            &tenant(),
            request(vec![resource_metrics(
                Vec::new(),
                vec![histogram_metric(
                    "lat",
                    vec![dp],
                    AggregationTemporality::Cumulative,
                )],
            )]),
            &IngestLimits::default(),
            MEMO_TS,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        // 3 explicit bounds -> 3 bucket series + `+Inf` bucket + `_sum` +
        // `_count` = 6 exploded series.
        assert_eq!(out.points.len(), 6);

        let distinct: HashSet<SeriesId> = out.points.iter().map(|p| p.series_id).collect();
        assert_eq!(
            distinct.len(),
            out.points.len(),
            "every exploded series must have a distinct id; a raw-attributes-only \
             key would collapse the buckets onto the first bucket's id"
        );

        // Each id equals computing it directly, with the exploded name (the
        // point's own `__name__`) and its full label set.
        for p in &out.points {
            let name = p.labels.get(METRIC_NAME_LABEL).expect("__name__");
            let direct = SeriesId::compute(&tenant(), name, &p.labels).expect("compute id");
            assert_eq!(
                p.series_id, direct,
                "memoised id diverged from direct compute"
            );
        }
    }

    /// ADR-0098 test 4. A run of points with identical attributes shares ONE
    /// `Arc<LabelSet>`: every produced point clones the same allocation, pinned
    /// with `Arc::ptr_eq` rather than assumed.
    #[test]
    fn identical_attribute_run_shares_one_arc() {
        let points: Vec<NumberDataPoint> = (0..5)
            .map(|i| {
                number_point(
                    vec![string_kv("host", "a")],
                    MEMO_TS + i,
                    NumberValue::AsDouble(i as f64),
                )
            })
            .collect();
        let out = normalize_metrics(
            &tenant(),
            request(vec![resource_metrics(
                Vec::new(),
                vec![gauge_metric("cpu", points)],
            )]),
            &IngestLimits::default(),
            MEMO_TS,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 5);
        let first = &out.points[0].labels;
        for p in &out.points[1..] {
            assert!(
                Arc::ptr_eq(first, &p.labels),
                "points of one run must share one Arc<LabelSet>"
            );
        }
    }

    /// ADR-0098 test 2. Memoised output is bit-identical to unmemoised output
    /// over arbitrary requests, rejections included. The unmemoised oracle is
    /// the same points split into one-data-point metrics of the same name: each
    /// gets its own single-entry memo, so the memo never hits, while the
    /// per-point results (id and full label set) are unchanged. `SeriesId`
    /// bytes and full `LabelSet` contents are compared via the derived
    /// `PartialEq` on `NormalizeOutput`.
    ///
    /// This exercises the attribute-keyed path (gauge points, where the memo
    /// does hit on repeats); the intra-data-point explosion trap is pinned
    /// separately by [`histogram_explode_yields_distinct_ids_through_the_memo`],
    /// which the split oracle cannot see (one data point explodes through one
    /// memo in both the grouped and split shapes).
    fn attrs_of(spec: &[(u8, u8)]) -> Vec<KeyValue> {
        spec.iter()
            .enumerate()
            .map(|(i, (_, v))| string_kv(&format!("k{i}"), &format!("v{v}")))
            .collect()
    }

    proptest::proptest! {
        #[test]
        fn memoised_output_matches_unmemoised(
            specs in proptest::collection::vec(
                proptest::collection::vec((0u8..3, 0u8..3), 0usize..4),
                1usize..12,
            )
        ) {
            let tenant = tenant();
            let limits = IngestLimits::default();

            // Grouped: all data points in one metric -> the memo hits on
            // repeated attribute slices.
            let grouped_points: Vec<NumberDataPoint> = specs
                .iter()
                .enumerate()
                .map(|(i, spec)| number_point(
                    attrs_of(spec),
                    MEMO_TS + i as i64,
                    NumberValue::AsDouble(i as f64),
                ))
                .collect();
            let grouped = request(vec![resource_metrics(
                Vec::new(),
                vec![gauge_metric("m", grouped_points)],
            )]);

            // Unmemoised oracle: each data point alone in its own metric of the
            // same name, so every SeriesIdMemo sees exactly one point.
            let split_metrics: Vec<Metric> = specs
                .iter()
                .enumerate()
                .map(|(i, spec)| gauge_metric("m", vec![number_point(
                    attrs_of(spec),
                    MEMO_TS + i as i64,
                    NumberValue::AsDouble(i as f64),
                )]))
                .collect();
            let split = request(vec![resource_metrics(Vec::new(), split_metrics)]);

            let memoised = normalize_metrics(&tenant, grouped, &limits, MEMO_TS);
            let unmemoised = normalize_metrics(&tenant, split, &limits, MEMO_TS);
            proptest::prop_assert_eq!(memoised, unmemoised);
        }
    }
}
