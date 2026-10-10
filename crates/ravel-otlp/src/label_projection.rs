//! Read-only projection of the resolved-label bytes an OTLP normalizer would
//! build for one request (ADR-2708 D2).
//!
//! Each normalizer copies attribute bytes per unit: a metric point carries
//! its own label set (resource prefix, `__name__`, attributes), a log record
//! carries its own copy of its resource and scope stream attributes, and a
//! span carries its resource and scope attributes merged into its own. None
//! of the per-unit limits bounds that product, so a request under every one
//! of them can still make normalization allocate without bound. The three
//! `project_*` functions here walk a decoded request without building any
//! label set and return an upper bound on what normalization would build, so
//! the caller can refuse the request, and charge the ingest byte budget,
//! before the allocation happens.
//!
//! The projection must never fall below what the normalizer builds: an
//! undercount is a defect, not a tuning choice. It skips only units the
//! normalizer is certain to reject before building their labels (a scope over
//! its attribute limit, a point over its attribute or bucket cap, a
//! non-cumulative sum), and otherwise counts every copy whatever its later
//! fate. Every label or attribute is charged its name and value bytes plus
//! [`RESOLVED_LABEL_OVERHEAD_BYTES`] for the element holding them, and an
//! element slot reserved for an attribute the normalizer then drops is
//! charged the overhead alone; a log
//! record's copy of its stream preimage, a flat byte string, is charged its
//! exact encoded length.
//!
//! No function here allocates per point, record or span. The metrics
//! projection allocates one family name per metric, as normalization does.

use std::fmt;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Metric, ResourceMetrics, metric::Data as MetricData,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_types::METRIC_NAME_LABEL;

use crate::limits::IngestLimits;
use crate::logs_limits::LogIngestLimits;
use crate::logs_normalize::MAX_ATTRIBUTE_NESTING_DEPTH;
use crate::normalize::{metric_kind_of, prometheus_family_name, sanitize_metric_name};
use crate::traces_limits::SpanIngestLimits;
use crate::traces_normalize::{
    ATTR_EVENTS_RAW, ATTR_LINKS_RAW, ATTR_SPAN_FLAGS, ATTR_SPAN_KIND, ATTR_TRACE_STATE,
};

/// Bytes charged per resolved label or attribute on top of its name and
/// value: at least the inline size of the element holding it on a 64-bit
/// target, whether a `ravel_types::Label` or `(String, String)` pair (48) or a
/// log attribute's `(String, AttrValue)` pair (56).
pub const RESOLVED_LABEL_OVERHEAD_BYTES: usize = 64;

/// Longest suffix the classic histogram and summary explosion appends to a
/// family name (`_bucket`, `_count`, `_sum`).
const EXPLODE_SUFFIX_MAX_LEN: usize = "_bucket".len();

/// The `_count` and `_sum` series of an exploded histogram or summary point:
/// each is built in a `Vec` with a slot reserved for the `le`/`quantile`
/// label it never gets, and the kept set keeps that slot.
const EXPLODE_UNFILLED_SLOT_BYTES: usize = 2 * RESOLVED_LABEL_OVERHEAD_BYTES;

/// `le="+Inf"`, the bucket label every classic histogram adds.
const INF_LE_LABEL_BYTES: usize = "le".len() + "+Inf".len() + RESOLVED_LABEL_OVERHEAD_BYTES;

/// Counts the bytes a `Display` value would format to without allocating.
struct LenCounter(usize);

impl fmt::Write for LenCounter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0 = self.0.saturating_add(s.len());
        Ok(())
    }
}

fn display_len(value: impl fmt::Display) -> usize {
    use fmt::Write;
    let mut counter = LenCounter(0);
    // Writing into a `LenCounter` cannot fail; a failure would only shorten
    // the count, and `Display` impls in std never report one.
    let _ = write!(counter, "{value}");
    counter.0
}

/// Length of [`crate::promcompat::format_float`]'s output for `v`.
fn format_float_len(v: f64) -> usize {
    if v.is_infinite() { 4 } else { display_len(v) }
}

fn label_bytes(name_len: usize, value_len: usize) -> usize {
    name_len
        .saturating_add(value_len)
        .saturating_add(RESOLVED_LABEL_OVERHEAD_BYTES)
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Upper bound on the resolved-label bytes [`crate::normalize_metrics`] and
/// its siblings would build for `req` (ADR-2708 D2): per resource, its label
/// prefix once; per point, `multiplicity x (prefix + __name__ + attribute
/// labels)`, where multiplicity is 1 for gauges, sums and exponential
/// histograms, `bounds + 3` for classic histograms (plus each bucket's `le`
/// label) and `quantiles + 2` for summaries (plus each `quantile` label, and
/// the slot the `_count` and `_sum` series reserve for one and leave empty). A
/// run of consecutive points within one metric whose raw attributes are equal
/// counts once, because the label memo builds once per such run and shares
/// the set; the explode paths share nothing and get no run rule.
///
/// Allocates no label set and nothing per point.
pub fn project_resolved_label_bytes(
    req: &ExportMetricsServiceRequest,
    limits: &IngestLimits,
) -> usize {
    req.resource_metrics
        .iter()
        .map(|rm| project_resource_metrics(rm, limits))
        .fold(0usize, usize::saturating_add)
}

fn project_resource_metrics(rm: &ResourceMetrics, limits: &IngestLimits) -> usize {
    let resource = rm.resource.as_ref();
    if resource.map_or(0, |r| r.attributes.len()) > limits.max_resource_attributes {
        return 0;
    }
    let Some(prefix) = resource_prefix_bytes(resource, limits) else {
        return 0;
    };
    let mut total = prefix;
    for sm in &rm.scope_metrics {
        for metric in &sm.metrics {
            total = total.saturating_add(project_metric(metric, prefix, limits));
        }
    }
    total
}

/// Bytes of the label prefix `build_resource_labels` builds, or `None` when a
/// value it reads has no label form, which rejects every point under the
/// resource before any point label is built.
fn resource_prefix_bytes(resource: Option<&Resource>, limits: &IngestLimits) -> Option<usize> {
    let Some(resource) = resource else {
        return Some(0);
    };
    let attrs = &resource.attributes;
    let mut total = 0usize;

    if let Some(name_len) = first_attr_label_len(attrs, "service.name")? {
        let ns_len = first_attr_label_len(attrs, "service.namespace")?.unwrap_or(0);
        let job_len = if ns_len > 0 {
            ns_len.saturating_add(1).saturating_add(name_len)
        } else {
            name_len
        };
        if job_len > 0 {
            total = total.saturating_add(label_bytes("job".len(), job_len));
        }
    } else {
        // `build_resource_labels` reads `service.namespace` even without a
        // name, so an unrepresentable value there still rejects the resource.
        first_attr_label_len(attrs, "service.namespace")?;
    }

    if let Some(len) = first_attr_label_len(attrs, "service.instance.id")?
        && len > 0
    {
        total = total.saturating_add(label_bytes("instance".len(), len));
    }

    for key in &limits.resource_attribute_allowlist {
        if matches!(
            key.as_str(),
            "service.name" | "service.namespace" | "service.instance.id"
        ) {
            continue;
        }
        // Sanitizing a label name replaces characters one for one with `_`,
        // so it never lengthens the name.
        if let Some(len) = first_attr_label_len(attrs, key)?
            && len > 0
        {
            total = total.saturating_add(label_bytes(key.len(), len));
        }
    }
    Some(total)
}

/// `Some(Some(len))` for the first attribute named `key`, `Some(None)` when
/// none is, and `None` when its value has no label representation.
fn first_attr_label_len(attrs: &[KeyValue], key: &str) -> Option<Option<usize>> {
    match attrs.iter().find(|kv| kv.key == key) {
        None => Some(None),
        Some(kv) => label_value_len(kv.value.as_ref()).map(Some),
    }
}

/// Length of `any_value_to_label_value`'s output, or `None` where it rejects.
fn label_value_len(value: Option<&AnyValue>) -> Option<usize> {
    match value.and_then(|v| v.value.as_ref()) {
        None => Some(0),
        Some(AnyValueVariant::StringValue(s)) => Some(s.len()),
        Some(AnyValueVariant::BoolValue(b)) => Some(if *b { 4 } else { 5 }),
        Some(AnyValueVariant::IntValue(i)) => Some(display_len(i)),
        Some(AnyValueVariant::DoubleValue(d)) => Some(display_len(d)),
        Some(AnyValueVariant::ArrayValue(_))
        | Some(AnyValueVariant::KvlistValue(_))
        | Some(AnyValueVariant::BytesValue(_))
        | Some(AnyValueVariant::StringValueStrindex(_)) => None,
    }
}

/// Bytes of the attribute labels one point builds, or `None` when one of its
/// attributes has no label representation (the point is rejected before its
/// set is kept). An empty value is dropped from the set, as `push_checked`
/// drops it, but where the set's `Vec` was sized before the drops
/// (`reserved_slots`) its slot stays allocated in the kept set and is charged
/// [`RESOLVED_LABEL_OVERHEAD_BYTES`].
fn point_attribute_bytes(attributes: &[KeyValue], reserved_slots: bool) -> Option<usize> {
    let mut total = 0usize;
    for kv in attributes {
        let len = label_value_len(kv.value.as_ref())?;
        if len > 0 {
            total = total.saturating_add(label_bytes(kv.key.len(), len));
        } else if reserved_slots {
            total = total.saturating_add(RESOLVED_LABEL_OVERHEAD_BYTES);
        }
    }
    Some(total)
}

fn is_cumulative(temporality: i32) -> bool {
    matches!(
        AggregationTemporality::try_from(temporality),
        Ok(AggregationTemporality::Cumulative)
    )
}

fn project_metric(metric: &Metric, prefix: usize, limits: &IngestLimits) -> usize {
    let Some(data) = metric.data.as_ref() else {
        return 0;
    };
    if metric.name.len() > limits.max_metric_name_len {
        return 0;
    }
    let sanitized = sanitize_metric_name(&metric.name);
    if sanitized.is_empty() {
        return 0;
    }
    let (kind, is_monotonic_sum) = metric_kind_of(data);
    let name_len = prometheus_family_name(&sanitized, &metric.unit, kind, is_monotonic_sum).len();
    let name_label = label_bytes(METRIC_NAME_LABEL.len(), name_len);
    let explode_name_label = name_label.saturating_add(EXPLODE_SUFFIX_MAX_LEN);
    let max_attrs = limits.max_attributes_per_point;

    match data {
        MetricData::Gauge(g) => project_memo_run(
            g.data_points.iter().map(|dp| dp.attributes.as_slice()),
            prefix.saturating_add(name_label),
            max_attrs,
        ),
        MetricData::Sum(s) => {
            if !is_cumulative(s.aggregation_temporality) {
                return 0;
            }
            project_memo_run(
                s.data_points.iter().map(|dp| dp.attributes.as_slice()),
                prefix.saturating_add(name_label),
                max_attrs,
            )
        }
        MetricData::ExponentialHistogram(h) => {
            if !is_cumulative(h.aggregation_temporality) {
                return 0;
            }
            project_memo_run(
                h.data_points.iter().map(|dp| dp.attributes.as_slice()),
                prefix.saturating_add(name_label),
                max_attrs,
            )
        }
        MetricData::Histogram(h) => {
            if !is_cumulative(h.aggregation_temporality) {
                return 0;
            }
            let mut total = 0usize;
            for dp in &h.data_points {
                if dp.explicit_bounds.len() > limits.max_histogram_buckets
                    || dp.attributes.len() > max_attrs
                {
                    continue;
                }
                let Some(attrs) = point_attribute_bytes(&dp.attributes, false) else {
                    continue;
                };
                let per_series = prefix
                    .saturating_add(attrs)
                    .saturating_add(explode_name_label);
                let multiplicity = dp.explicit_bounds.len().saturating_add(3);
                let le_labels = dp
                    .explicit_bounds
                    .iter()
                    .map(|b| label_bytes("le".len(), format_float_len(*b)))
                    .fold(INF_LE_LABEL_BYTES, usize::saturating_add);
                total = total
                    .saturating_add(per_series.saturating_mul(multiplicity))
                    .saturating_add(le_labels)
                    .saturating_add(EXPLODE_UNFILLED_SLOT_BYTES);
            }
            total
        }
        MetricData::Summary(s) => {
            let mut total = 0usize;
            for dp in &s.data_points {
                if dp.quantile_values.len() > limits.max_summary_quantiles
                    || dp.attributes.len() > max_attrs
                {
                    continue;
                }
                let Some(attrs) = point_attribute_bytes(&dp.attributes, false) else {
                    continue;
                };
                let per_series = prefix
                    .saturating_add(attrs)
                    .saturating_add(explode_name_label);
                let multiplicity = dp.quantile_values.len().saturating_add(2);
                let quantile_labels = dp
                    .quantile_values
                    .iter()
                    .map(|q| label_bytes("quantile".len(), format_float_len(q.quantile)))
                    .fold(0usize, usize::saturating_add);
                total = total
                    .saturating_add(per_series.saturating_mul(multiplicity))
                    .saturating_add(quantile_labels)
                    .saturating_add(EXPLODE_UNFILLED_SLOT_BYTES);
            }
            total
        }
    }
}

/// Gauge, sum and exponential-histogram points: one label set per point,
/// except that a point whose raw attributes equal the previous counted
/// point's shares that point's set (the `SeriesIdMemo` hit rule). A point
/// over the attribute cap is rejected before the memo is consulted, so it
/// neither counts nor resets the run.
///
/// These paths size each set's `Vec` from the raw attribute count before
/// empty values are dropped, and `LabelSet::new` keeps that `Vec` as built,
/// so every empty-valued attribute still costs its slot.
fn project_memo_run<'a>(
    points: impl Iterator<Item = &'a [KeyValue]>,
    base: usize,
    max_attrs: usize,
) -> usize {
    let mut total = 0usize;
    let mut previous: Option<&[KeyValue]> = None;
    for attributes in points {
        if attributes.len() > max_attrs {
            continue;
        }
        if previous == Some(attributes) {
            continue;
        }
        previous = Some(attributes);
        if let Some(attrs) = point_attribute_bytes(attributes, true) {
            total = total.saturating_add(base.saturating_add(attrs));
        }
    }
    total
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

/// Upper bound on the attribute bytes [`crate::normalize_logs`] would build
/// for `req` (ADR-2708 D2). Every record carries its own copy of its stream's
/// encoded preimage (resource attributes, scope name, scope version, scope
/// attributes, in the ADR-0029 canonical encoding) plus its own converted
/// record attributes, so the projection is, per record, the preimage's
/// encoded length plus each record attribute's key, value payload and
/// [`RESOLVED_LABEL_OVERHEAD_BYTES`]; plus, once per resource and once per
/// scope, the converted attributes and the preimage the records copy from.
/// No run collapsing: the log normalizer copies for every record.
pub fn project_log_resolved_label_bytes(
    req: &ExportLogsServiceRequest,
    limits: &LogIngestLimits,
) -> usize {
    let mut total = 0usize;
    for rl in &req.resource_logs {
        let resource_attributes = rl
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        if resource_attributes.len() > limits.max_resource_attributes {
            continue;
        }
        total = total.saturating_add(log_attrs_bytes(resource_attributes));
        let resource_encoded = encoded_attrs_len(resource_attributes);
        for sl in &rl.scope_logs {
            let scope = sl.scope.as_ref();
            let scope_attributes = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
            if scope_attributes.len() > limits.max_scope_attributes {
                continue;
            }
            let scope_name = scope.map_or(0, |s| s.name.len());
            let scope_version = scope.map_or(0, |s| s.version.len());
            let stream_encoded = resource_encoded
                .saturating_add(encoded_bytes_len(scope_name))
                .saturating_add(encoded_bytes_len(scope_version))
                .saturating_add(encoded_attrs_len(scope_attributes));
            total = total
                .saturating_add(log_attrs_bytes(scope_attributes))
                .saturating_add(stream_encoded);
            for record in &sl.log_records {
                if record.attributes.len() > limits.max_attributes_per_record {
                    continue;
                }
                total = total
                    .saturating_add(stream_encoded)
                    .saturating_add(log_attrs_bytes(&record.attributes));
            }
        }
    }
    total
}

/// Bytes of the LEB128 encoding of `value`.
fn uvarint_len(value: u64) -> usize {
    let bits = 64 - value.leading_zeros() as usize;
    bits.div_ceil(7).max(1)
}

/// A length-prefixed byte string in the canonical encoding.
fn encoded_bytes_len(len: usize) -> usize {
    uvarint_len(len as u64).saturating_add(len)
}

/// Length of `ravel_types::logstream::canonical_attr_bytes` over the
/// converted `attributes`: an entry count, then per entry the length-prefixed
/// key and the encoded value. An attribute whose value the converter rejects
/// rejects the whole set, so counting it as encoded can only overcount.
fn encoded_attrs_len(attributes: &[KeyValue]) -> usize {
    attributes
        .iter()
        .map(|kv| {
            encoded_bytes_len(kv.key.len()).saturating_add(encoded_value_len(kv.value.as_ref(), 1))
        })
        .fold(uvarint_len(attributes.len() as u64), usize::saturating_add)
}

fn encoded_value_len(value: Option<&AnyValue>, depth: usize) -> usize {
    if depth > MAX_ATTRIBUTE_NESTING_DEPTH {
        return 0;
    }
    match value.and_then(|v| v.value.as_ref()) {
        None | Some(AnyValueVariant::StringValueStrindex(_)) => 0,
        Some(AnyValueVariant::StringValue(s)) => 1usize.saturating_add(encoded_bytes_len(s.len())),
        Some(AnyValueVariant::BytesValue(b)) => 1usize.saturating_add(encoded_bytes_len(b.len())),
        Some(AnyValueVariant::IntValue(i)) => {
            let zigzag = ((*i << 1) ^ (*i >> 63)) as u64;
            1 + uvarint_len(zigzag)
        }
        Some(AnyValueVariant::DoubleValue(_)) => 1 + 8,
        Some(AnyValueVariant::BoolValue(_)) => 1 + 1,
        Some(AnyValueVariant::ArrayValue(array)) => array
            .values
            .iter()
            .map(|v| encoded_value_len(Some(v), depth + 1))
            .fold(
                1 + uvarint_len(array.values.len() as u64),
                usize::saturating_add,
            ),
        Some(AnyValueVariant::KvlistValue(kvlist)) => kvlist
            .values
            .iter()
            .map(|kv| {
                encoded_bytes_len(kv.key.len())
                    .saturating_add(encoded_value_len(kv.value.as_ref(), depth + 1))
            })
            .fold(
                1 + uvarint_len(kvlist.values.len() as u64),
                usize::saturating_add,
            ),
    }
}

fn log_attrs_bytes(attributes: &[KeyValue]) -> usize {
    attributes
        .iter()
        .map(|kv| label_bytes(kv.key.len(), log_value_payload(kv.value.as_ref(), 1)))
        .fold(0usize, usize::saturating_add)
}

/// Payload bytes of a converted log attribute value: a string or bytes value
/// counts its length, a scalar nothing beyond the per-attribute overhead, and
/// every list element or map entry one more overhead plus its own payload
/// (and, for a map entry, its key). A value nested past
/// [`MAX_ATTRIBUTE_NESTING_DEPTH`] is rejected by the converter, so the walk
/// stops there.
fn log_value_payload(value: Option<&AnyValue>, depth: usize) -> usize {
    if depth > MAX_ATTRIBUTE_NESTING_DEPTH {
        return 0;
    }
    match value.and_then(|v| v.value.as_ref()) {
        None
        | Some(AnyValueVariant::BoolValue(_))
        | Some(AnyValueVariant::IntValue(_))
        | Some(AnyValueVariant::DoubleValue(_))
        | Some(AnyValueVariant::StringValueStrindex(_)) => 0,
        Some(AnyValueVariant::StringValue(s)) => s.len(),
        Some(AnyValueVariant::BytesValue(b)) => b.len(),
        Some(AnyValueVariant::ArrayValue(array)) => array
            .values
            .iter()
            .map(|v| label_bytes(0, log_value_payload(Some(v), depth + 1)))
            .fold(0usize, usize::saturating_add),
        Some(AnyValueVariant::KvlistValue(kvlist)) => kvlist
            .values
            .iter()
            .map(|kv| {
                label_bytes(
                    kv.key.len(),
                    log_value_payload(kv.value.as_ref(), depth + 1),
                )
            })
            .fold(0usize, usize::saturating_add),
    }
}

// ---------------------------------------------------------------------------
// Traces
// ---------------------------------------------------------------------------

/// Upper bound on the attribute bytes [`crate::normalize_traces`] would build
/// for `req` (ADR-2708 D2). Every span's stored attributes are its resource
/// and scope attributes (with `otel.scope.name`/`otel.scope.version`) merged
/// with its own, plus the reserved attributes carrying its kind, trace state,
/// flags, events and links; the projection charges each of those per span,
/// plus the once-per-resource and once-per-scope conversion copies. No run
/// collapsing: the span normalizer merges for every span.
pub fn project_span_resolved_label_bytes(
    req: &ExportTraceServiceRequest,
    limits: &SpanIngestLimits,
) -> usize {
    let mut total = 0usize;
    for rs in &req.resource_spans {
        let resource_attributes = rs
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        if resource_attributes.len() > limits.max_resource_attributes {
            continue;
        }
        let resource_part = span_attrs_bytes(resource_attributes);
        total = total.saturating_add(resource_part);
        for ss in &rs.scope_spans {
            let scope = ss.scope.as_ref();
            let scope_attributes = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
            if scope_attributes.len() > limits.max_scope_attributes {
                continue;
            }
            let mut scope_part = span_attrs_bytes(scope_attributes);
            if let Some(scope) = scope {
                if !scope.name.is_empty() {
                    scope_part = scope_part
                        .saturating_add(label_bytes("otel.scope.name".len(), scope.name.len()));
                }
                if !scope.version.is_empty() {
                    scope_part = scope_part.saturating_add(label_bytes(
                        "otel.scope.version".len(),
                        scope.version.len(),
                    ));
                }
            }
            total = total.saturating_add(scope_part);
            let inherited = resource_part.saturating_add(scope_part);
            for span in &ss.spans {
                if span.attributes.len() > limits.max_attributes_per_span {
                    continue;
                }
                total = total
                    .saturating_add(inherited)
                    .saturating_add(span_attrs_bytes(&span.attributes))
                    .saturating_add(reserved_attrs_bytes(span, limits));
            }
        }
    }
    total
}

fn span_attrs_bytes(attributes: &[KeyValue]) -> usize {
    attributes
        .iter()
        .map(|kv| label_bytes(kv.key.len(), span_value_len(kv.value.as_ref())))
        .fold(0usize, usize::saturating_add)
}

/// Length of the traces converter's string for a value; arrays, kvlists and
/// unset values are dropped there and count nothing.
fn span_value_len(value: Option<&AnyValue>) -> usize {
    match value.and_then(|v| v.value.as_ref()) {
        Some(AnyValueVariant::StringValue(s)) => s.len(),
        Some(AnyValueVariant::BoolValue(b)) => {
            if *b {
                4
            } else {
                5
            }
        }
        Some(AnyValueVariant::IntValue(i)) => display_len(i),
        Some(AnyValueVariant::DoubleValue(d)) => format_float_len(*d),
        Some(AnyValueVariant::BytesValue(b)) => b.len().saturating_mul(2),
        None
        | Some(AnyValueVariant::ArrayValue(_))
        | Some(AnyValueVariant::KvlistValue(_))
        | Some(AnyValueVariant::StringValueStrindex(_)) => 0,
    }
}

/// Bytes of the reserved attributes `reserved_attrs` would add to `span`.
fn reserved_attrs_bytes(
    span: &opentelemetry_proto::tonic::trace::v1::Span,
    limits: &SpanIngestLimits,
) -> usize {
    let mut total = 0usize;
    if span.kind != 0 {
        // A known kind is at most "internal"/"consumer"/"producer" (8
        // bytes); an unknown one is stored in decimal.
        total = total.saturating_add(label_bytes(
            ATTR_SPAN_KIND.len(),
            display_len(span.kind).max("internal".len()),
        ));
    }
    if !span.trace_state.is_empty() {
        total = total.saturating_add(label_bytes(ATTR_TRACE_STATE.len(), span.trace_state.len()));
    }
    if span.flags != 0 {
        total = total.saturating_add(label_bytes(ATTR_SPAN_FLAGS.len(), display_len(span.flags)));
    }
    if !span.events.is_empty() {
        total = total.saturating_add(blob_bytes(ATTR_EVENTS_RAW, &span.events, limits));
    }
    if !span.links.is_empty() {
        total = total.saturating_add(blob_bytes(ATTR_LINKS_RAW, &span.links, limits));
    }
    total
}

/// The hex blob `encode_blob` would store for `items`, or nothing when its
/// raw length is over `max_raw_blob_len` and the blob is dropped.
fn blob_bytes<T: Message>(key: &str, items: &[T], limits: &SpanIngestLimits) -> usize {
    let raw = items
        .iter()
        .map(|item| {
            let len = item.encoded_len();
            prost::length_delimiter_len(len).saturating_add(len)
        })
        .fold(0usize, usize::saturating_add);
    if raw > limits.max_raw_blob_len {
        return 0;
    }
    label_bytes(key.len(), raw.saturating_mul(2))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn overhead_covers_a_label() {
        assert!(std::mem::size_of::<ravel_types::Label>() <= RESOLVED_LABEL_OVERHEAD_BYTES);
        assert!(std::mem::size_of::<(String, String)>() <= RESOLVED_LABEL_OVERHEAD_BYTES);
        assert!(
            std::mem::size_of::<(String, ravel_types::logstream::AttrValue)>()
                <= RESOLVED_LABEL_OVERHEAD_BYTES
        );
    }

    #[test]
    fn display_len_matches_formatting() {
        for v in [0.0f64, -0.0, 1.5, 1e300, -2.25e-7, f64::NAN] {
            assert_eq!(display_len(v), v.to_string().len(), "{v}");
            assert_eq!(
                format_float_len(v),
                crate::promcompat::format_float(v).len()
            );
        }
        assert_eq!(format_float_len(f64::INFINITY), 4);
        assert_eq!(format_float_len(f64::NEG_INFINITY), 4);
        assert_eq!(display_len(i64::MIN), i64::MIN.to_string().len());
    }

    #[test]
    fn encoded_attrs_len_matches_the_canonical_encoding() {
        use ravel_types::logstream::{AttrValue, canonical_attr_bytes};
        let otlp = |v: AnyValueVariant| AnyValue { value: Some(v) };
        let kv = |k: &str, v: AnyValueVariant| KeyValue {
            key: k.to_string(),
            value: Some(otlp(v)),
            ..Default::default()
        };
        let long = "x".repeat(200);
        let attributes = vec![
            kv("s", AnyValueVariant::StringValue(long.clone())),
            kv("i", AnyValueVariant::IntValue(i64::MIN)),
            kv("small", AnyValueVariant::IntValue(-3)),
            kv("f", AnyValueVariant::DoubleValue(0.5)),
            kv("b", AnyValueVariant::BoolValue(true)),
            kv("raw", AnyValueVariant::BytesValue(vec![9; 130])),
            kv(
                "list",
                AnyValueVariant::ArrayValue(opentelemetry_proto::tonic::common::v1::ArrayValue {
                    values: vec![otlp(AnyValueVariant::IntValue(1)); 129],
                }),
            ),
            kv(
                "map",
                AnyValueVariant::KvlistValue(
                    opentelemetry_proto::tonic::common::v1::KeyValueList {
                        values: vec![kv("k", AnyValueVariant::StringValue("v".into()))],
                    },
                ),
            ),
        ];
        let converted = vec![
            ("s".to_string(), AttrValue::Str(long)),
            ("i".to_string(), AttrValue::I64(i64::MIN)),
            ("small".to_string(), AttrValue::I64(-3)),
            ("f".to_string(), AttrValue::F64(0.5)),
            ("b".to_string(), AttrValue::Bool(true)),
            ("raw".to_string(), AttrValue::Bytes(vec![9; 130])),
            (
                "list".to_string(),
                AttrValue::List(vec![AttrValue::I64(1); 129]),
            ),
            (
                "map".to_string(),
                AttrValue::Map(vec![("k".to_string(), AttrValue::Str("v".into()))]),
            ),
        ];
        assert_eq!(
            encoded_attrs_len(&attributes),
            canonical_attr_bytes(&converted).len()
        );
        assert_eq!(encoded_attrs_len(&[]), canonical_attr_bytes(&[]).len());
        for v in [0u64, 127, 128, 16_383, 16_384, u64::MAX] {
            let mut n = v;
            let mut expected = 1;
            while n >= 0x80 {
                n >>= 7;
                expected += 1;
            }
            assert_eq!(uvarint_len(v), expected, "{v}");
        }
    }
}
