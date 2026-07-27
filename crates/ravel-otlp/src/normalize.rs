//! Normalization from a decoded OTLP `ExportMetricsServiceRequest` into
//! Ravel's canonical metric point representation (ADR-0005;
//! docs/architecture.md).
//!
//! Phase 1 scope: `Gauge` and cumulative `Sum` metrics with `NumberDataPoint`
//! values (`as_int` and `as_double`; integers convert to `f64`). Histogram,
//! ExponentialHistogram, and Summary metrics are never silently dropped:
//! they produce [`Rejection::UnsupportedMetricType`] carrying the number of
//! data points skipped.
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
    AggregationTemporality, Metric, NumberDataPoint, ResourceMetrics, metric::Data as MetricData,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId, TypeError};

use crate::limits::{IngestLimits, Rejection};

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
            for dp in &gauge.data_points {
                match build_point(&ctx, dp) {
                    Ok(p) => points.push(p),
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
            for dp in &sum.data_points {
                match build_point(&ctx, dp) {
                    Ok(p) => points.push(p),
                    Err(r) => rejected.push(r),
                }
            }
        }
        Some(MetricData::Histogram(h)) => rejected.push(Rejection::UnsupportedMetricType {
            metric_type: "histogram",
            count: h.data_points.len(),
        }),
        Some(MetricData::ExponentialHistogram(h)) => {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: "exponential_histogram",
                count: h.data_points.len(),
            });
        }
        Some(MetricData::Summary(s)) => rejected.push(Rejection::UnsupportedMetricType {
            metric_type: "summary",
            count: s.data_points.len(),
        }),
        None => {}
    }
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

fn build_point(ctx: &PointContext, dp: &NumberDataPoint) -> Result<NormalizedPoint, Rejection> {
    if dp.attributes.len() > ctx.limits.max_attributes_per_point {
        return Err(Rejection::TooManyAttributes {
            attribute_count: dp.attributes.len(),
            max: ctx.limits.max_attributes_per_point,
        });
    }

    // time_unix_nano is u64; real event times fit comfortably in i64, and a
    // value that doesn't is far outside any sane admission window, so it
    // saturates rather than wrapping negative.
    let event_ts_ns = i64::try_from(dp.time_unix_nano).unwrap_or(i64::MAX);
    if event_ts_ns == 0 {
        return Err(Rejection::ZeroTimestamp);
    }

    // Convention (ADR-0010 §8, docs/consistency-model.md): the bound itself
    // passes, only strictly exceeding it rejects. event_ts == ingest_ts +
    // max_future_skew is accepted; event_ts == ingest_ts + max_future_skew + 1
    // is not.
    let skew_ns = event_ts_ns.saturating_sub(ctx.ingest_ts_ns);
    if skew_ns > ctx.limits.max_future_skew_ns {
        return Err(Rejection::FutureSkew {
            skew_ns,
            max_ns: ctx.limits.max_future_skew_ns,
        });
    }
    let lag_ns = ctx.ingest_ts_ns.saturating_sub(event_ts_ns);
    if lag_ns > ctx.limits.max_ingest_lag_ns {
        return Err(Rejection::TooOld {
            lag_ns,
            max_ns: ctx.limits.max_ingest_lag_ns,
        });
    }

    let value = match dp.value {
        Some(NumberValue::AsDouble(v)) => v,
        Some(NumberValue::AsInt(v)) => v as f64,
        None => return Err(Rejection::MissingValue),
    };

    let mut labels = Vec::with_capacity(ctx.resource_labels.len() + dp.attributes.len() + 1);
    labels.extend_from_slice(ctx.resource_labels);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: ctx.metric_name.to_string(),
    });

    for attr in &dp.attributes {
        let name = sanitize_label_name(&attr.key);
        let value = any_value_to_label_value(attr.value.as_ref())?;
        push_checked(&mut labels, name, value, ctx.limits)?;
    }

    let label_set = LabelSet::new(labels).map_err(|err| match err {
        TypeError::DuplicateLabelName(name) => Rejection::DuplicateLabelName(name),
        // LabelSet::new only ever returns DuplicateLabelName; this arm
        // exists so a future TypeError variant can't silently pass through
        // as an accepted point.
        _ => Rejection::DuplicateLabelName(String::new()),
    })?;

    let series_id = SeriesId::compute(ctx.tenant, ctx.metric_name, &label_set)
        .map_err(|_| Rejection::OversizedSeriesComponent)?;

    Ok(NormalizedPoint {
        series_id,
        labels: label_set,
        sample: Sample {
            ts_ns: event_ts_ns,
            value,
        },
        is_monotonic_sum: ctx.is_sum && ctx.is_monotonic,
    })
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
    use opentelemetry_proto::tonic::metrics::v1::{Gauge, ScopeMetrics, Sum};
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
    fn histogram_metric_rejected_with_counts() {
        use opentelemetry_proto::tonic::metrics::v1::{Histogram, HistogramDataPoint};
        let metric = Metric {
            name: "latency".to_string(),
            data: Some(MetricData::Histogram(Histogram {
                data_points: vec![
                    HistogramDataPoint {
                        time_unix_nano: 1_000,
                        ..Default::default()
                    },
                    HistogramDataPoint {
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
                metric_type: "histogram",
                count: 2,
            }]
        );
    }

    #[test]
    fn summary_metric_rejected_with_counts() {
        use opentelemetry_proto::tonic::metrics::v1::{Summary, SummaryDataPoint};
        let metric = Metric {
            name: "latency_summary".to_string(),
            data: Some(MetricData::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    time_unix_nano: 1_000,
                    ..Default::default()
                }],
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
                metric_type: "summary",
                count: 1,
            }]
        );
    }

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
}
