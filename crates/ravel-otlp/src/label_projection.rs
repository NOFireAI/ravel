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
use prost::Message;
use ravel_types::METRIC_NAME_LABEL;

use ravel_ipcrypt::rules::TenantRules;

use crate::limits::IngestLimits;
use crate::logs_limits::LogIngestLimits;
use crate::logs_normalize::MAX_ATTRIBUTE_NESTING_DEPTH;
use crate::normalize::{
    ResourceAttrValue, lookup_label_rule, metric_kind_of, prometheus_family_name,
    resource_attr_pairs, sanitize_metric_name,
};
use crate::traces_limits::SpanIngestLimits;
use crate::traces_normalize::{
    ATTR_EVENTS_RAW, ATTR_LINKS_RAW, ATTR_SPAN_FLAGS, ATTR_SPAN_KIND, ATTR_TRACE_STATE,
};

/// The length a value of `raw_len` bytes is charged at for the label/attribute
/// `name` would be stored as: unchanged when no rule in `rules` matches it,
/// [`TenantRules::max_stored_len`] when one does (ADR-2993 decision 3). `name`
/// must be the same key the normalizer matches a rule against: sanitized
/// label name for metrics ([`TenantRules::lookup_label`]), raw attribute key
/// for logs and traces ([`TenantRules::lookup_raw`]).
///
/// A matched value is charged at `max(47, raw)` whether or not it parses as
/// an address, and an unparsed value is stored unchanged at `raw`, so this
/// over-counts. That is the intended direction: the projection is an upper
/// bound checked before anything is built, and under-counting would admit a
/// request whose stored bytes exceed the bound.
pub fn charged_len(matched: bool, raw_len: usize) -> usize {
    if matched {
        TenantRules::max_stored_len(raw_len)
    } else {
        raw_len
    }
}

/// Bytes charged per resolved label or attribute on top of its name and
/// value: at least the inline size of the element holding it on a 64-bit
/// target, whether a `ravel_types::Label` or `(String, String)` pair (48) or a
/// log attribute's `(String, AttrValue)` pair (56).
pub const RESOLVED_LABEL_OVERHEAD_BYTES: usize = 64;

/// The result of one `project_*` walk over a request. Only the projection
/// functions construct one, so a value handed to a `_projected` normalizer
/// entry point is a real projection rather than an arbitrary number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectedLabelBytes(usize);

impl ProjectedLabelBytes {
    /// The projected byte count.
    pub fn get(self) -> usize {
        self.0
    }

    /// The result of a projection walk over a wire format other than the OTLP
    /// protobuf (OTAP's Arrow batches), built from the helpers here so the
    /// per-label charge cannot drift between formats.
    pub fn from_walk(n: usize) -> Self {
        Self(n)
    }

    /// A projection of `n` bytes that no walk produced, for tests of the
    /// `_projected` entry points.
    #[cfg(test)]
    pub(crate) fn for_test(n: usize) -> Self {
        Self(n)
    }
}

/// Longest suffix the classic histogram and summary explosion appends to a
/// family name (`_bucket`, `_count`, `_sum`).
const EXPLODE_SUFFIX_MAX_LEN: usize = "_bucket".len();

/// The `_count` and `_sum` series of an exploded histogram or summary point:
/// each is built in a `Vec` with a slot reserved for the `le`/`quantile`
/// label it never gets, and the kept set keeps that slot.
const EXPLODE_UNFILLED_SLOT_BYTES: usize = 2 * RESOLVED_LABEL_OVERHEAD_BYTES;

/// `le="+Inf"`, the bucket label every classic histogram adds.
const INF_LE_LABEL_BYTES: usize = label_bytes("le".len(), "+Inf".len());

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

/// The per-label charge, the one formula every projection here uses: the
/// element slot ([`RESOLVED_LABEL_OVERHEAD_BYTES`]) plus the name and value
/// lengths. Lengths suffice because every name and value a normalizer keeps
/// goes through [`exact_capacity`], so its heap allocation is its length.
pub const fn label_bytes(name_len: usize, value_len: usize) -> usize {
    name_len
        .saturating_add(value_len)
        .saturating_add(RESOLVED_LABEL_OVERHEAD_BYTES)
}

/// Drop any spare capacity from a string a normalizer is about to keep in a
/// label or attribute, so it allocates exactly the length [`label_bytes`]
/// charges. `to_string()` on an integer or float, `format!`, sanitising and
/// prost decoding all leave capacity beyond the length.
pub(crate) fn exact_capacity(mut s: String) -> String {
    s.shrink_to_fit();
    s
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
    rules: &TenantRules,
) -> ProjectedLabelBytes {
    ProjectedLabelBytes(
        req.resource_metrics
            .iter()
            .map(|rm| project_resource_metrics(rm, limits, rules))
            .fold(0usize, usize::saturating_add),
    )
}

fn project_resource_metrics(
    rm: &ResourceMetrics,
    limits: &IngestLimits,
    rules: &TenantRules,
) -> usize {
    let attrs = resource_attr_pairs(rm.resource.as_ref());
    if attrs.len() > limits.max_resource_attributes {
        return 0;
    }
    let Some(prefix) = resource_prefix_bytes(&attrs, limits, rules) else {
        return 0;
    };
    let mut total = prefix;
    for sm in &rm.scope_metrics {
        for metric in &sm.metrics {
            total = total.saturating_add(project_metric(metric, prefix, limits, rules));
        }
    }
    total
}

/// Bytes of the label prefix [`crate::normalize::build_resource_labels_from`]
/// builds from `attrs`, or `None` when a value it reads has no label form,
/// which rejects every point under the resource before any point label is
/// built. Shared by every wire format's projection, as the builder is.
/// `job` and `instance` are matched against `rules` under those literal
/// names, like `push_checked` matches them, not under their source resource
/// attribute keys (ADR-2993 decision 3).
pub fn resource_prefix_bytes(
    attrs: &[(&str, ResourceAttrValue<'_>)],
    limits: &IngestLimits,
    rules: &TenantRules,
) -> Option<usize> {
    let mut total = 0usize;

    if let Some(name_len) = first_attr_label_len(attrs, "service.name")? {
        let ns_len = first_attr_label_len(attrs, "service.namespace")?.unwrap_or(0);
        let job_len = if ns_len > 0 {
            ns_len.saturating_add(1).saturating_add(name_len)
        } else {
            name_len
        };
        if job_len > 0 {
            let job_len = charged_len(rules.lookup_label("job").is_some(), job_len);
            total = total.saturating_add(label_bytes("job".len(), job_len));
        }
    } else {
        // `build_resource_labels_from` reads `service.namespace` even without
        // a name, so an unrepresentable value there still rejects the
        // resource.
        first_attr_label_len(attrs, "service.namespace")?;
    }

    if let Some(len) = first_attr_label_len(attrs, "service.instance.id")?
        && len > 0
    {
        let len = charged_len(rules.lookup_label("instance").is_some(), len);
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
            let len = charged_len(lookup_label_rule(rules, key).is_some(), len);
            total = total.saturating_add(label_bytes(key.len(), len));
        }
    }
    Some(total)
}

/// `Some(Some(len))` for the first attribute named `key`, `Some(None)` when
/// none is, and `None` when its value has no label representation.
fn first_attr_label_len(
    attrs: &[(&str, ResourceAttrValue<'_>)],
    key: &str,
) -> Option<Option<usize>> {
    match attrs.iter().find(|(k, _)| *k == key) {
        None => Some(None),
        Some((_, value)) => resource_value_label_len(*value).map(Some),
    }
}

/// Length of the label value `value` converts to, or `None` where the
/// conversion rejects.
pub fn resource_value_label_len(value: ResourceAttrValue<'_>) -> Option<usize> {
    match value {
        ResourceAttrValue::Empty => Some(0),
        ResourceAttrValue::Str(s) => Some(s.len()),
        ResourceAttrValue::Bool(b) => Some(if b { 4 } else { 5 }),
        ResourceAttrValue::Int(i) => Some(display_len(i)),
        ResourceAttrValue::Double(d) => Some(display_len(d)),
        ResourceAttrValue::Complex => None,
    }
}

/// Length of `any_value_to_label_value`'s output, or `None` where it rejects.
fn label_value_len(value: Option<&AnyValue>) -> Option<usize> {
    resource_value_label_len(ResourceAttrValue::from_any_value(value))
}

/// Bytes of the attribute labels one point builds, or `None` when one of its
/// attributes has no label representation (the point is rejected before its
/// set is kept). An empty value is dropped from the set, as `push_checked`
/// drops it, but where the set's `Vec` was sized before the drops
/// (`reserved_slots`) its slot stays allocated in the kept set and is charged
/// [`RESOLVED_LABEL_OVERHEAD_BYTES`].
fn point_attribute_bytes(
    attributes: &[KeyValue],
    reserved_slots: bool,
    rules: &TenantRules,
) -> Option<usize> {
    let mut total = 0usize;
    for kv in attributes {
        let len = label_value_len(kv.value.as_ref())?;
        if len > 0 {
            let len = charged_len(lookup_label_rule(rules, &kv.key).is_some(), len);
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

fn project_metric(
    metric: &Metric,
    prefix: usize,
    limits: &IngestLimits,
    rules: &TenantRules,
) -> usize {
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
    let name_label = name_label_bytes(name_len);
    let explode_name_label = explode_name_label_bytes(name_len);
    let max_attrs = limits.max_attributes_per_point;

    match data {
        MetricData::Gauge(g) => project_memo_run(
            g.data_points.iter().map(|dp| dp.attributes.as_slice()),
            prefix.saturating_add(name_label),
            max_attrs,
            rules,
        ),
        MetricData::Sum(s) => {
            if !is_cumulative(s.aggregation_temporality) {
                return 0;
            }
            project_memo_run(
                s.data_points.iter().map(|dp| dp.attributes.as_slice()),
                prefix.saturating_add(name_label),
                max_attrs,
                rules,
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
                rules,
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
                let Some(attrs) = point_attribute_bytes(&dp.attributes, false, rules) else {
                    continue;
                };
                let per_series = prefix
                    .saturating_add(attrs)
                    .saturating_add(explode_name_label);
                total =
                    total.saturating_add(histogram_point_bytes(per_series, &dp.explicit_bounds));
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
                let Some(attrs) = point_attribute_bytes(&dp.attributes, false, rules) else {
                    continue;
                };
                let per_series = prefix
                    .saturating_add(attrs)
                    .saturating_add(explode_name_label);
                total = total.saturating_add(summary_point_bytes(
                    per_series,
                    dp.quantile_values.iter().map(|q| q.quantile),
                ));
            }
            total
        }
    }
}

/// The `__name__` label of a family name `name_len` bytes long.
pub const fn name_label_bytes(name_len: usize) -> usize {
    label_bytes(METRIC_NAME_LABEL.len(), name_len)
}

/// The `__name__` label of any series a classic histogram or summary point
/// explodes into, family name `name_len` bytes long: the longest suffix the
/// explosion appends is charged on every series.
pub const fn explode_name_label_bytes(name_len: usize) -> usize {
    name_label_bytes(name_len).saturating_add(EXPLODE_SUFFIX_MAX_LEN)
}

/// Bytes one classic histogram point builds when exploded, given
/// `per_series`, the prefix, attribute and
/// [`explode_name_label_bytes`] charge every series of the point repeats:
/// `bounds + 3` series (each bucket, `+Inf`, `_count`, `_sum`), each bucket's
/// `le` label, and the slot `_count` and `_sum` reserve and leave empty.
pub fn histogram_point_bytes(per_series: usize, bounds: &[f64]) -> usize {
    let multiplicity = bounds.len().saturating_add(3);
    let le_labels = bounds
        .iter()
        .map(|b| label_bytes("le".len(), format_float_len(*b)))
        .fold(INF_LE_LABEL_BYTES, usize::saturating_add);
    per_series
        .saturating_mul(multiplicity)
        .saturating_add(le_labels)
        .saturating_add(EXPLODE_UNFILLED_SLOT_BYTES)
}

/// Bytes one summary point builds when exploded, given `per_series` as for
/// [`histogram_point_bytes`]: `quantiles + 2` series (each quantile, `_count`,
/// `_sum`), each `quantile` label, and the slot `_count` and `_sum` reserve
/// and leave empty.
pub fn summary_point_bytes(
    per_series: usize,
    quantiles: impl ExactSizeIterator<Item = f64>,
) -> usize {
    let multiplicity = quantiles.len().saturating_add(2);
    let quantile_labels = quantiles
        .map(|q| label_bytes("quantile".len(), format_float_len(q)))
        .fold(0usize, usize::saturating_add);
    per_series
        .saturating_mul(multiplicity)
        .saturating_add(quantile_labels)
        .saturating_add(EXPLODE_UNFILLED_SLOT_BYTES)
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
    rules: &TenantRules,
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
        if let Some(attrs) = point_attribute_bytes(attributes, true, rules) {
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
    rules: &TenantRules,
) -> ProjectedLabelBytes {
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
        total = total.saturating_add(log_attrs_bytes(resource_attributes, rules));
        let resource_encoded = encoded_attrs_len(resource_attributes, rules);
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
                .saturating_add(encoded_attrs_len(scope_attributes, rules));
            total = total
                .saturating_add(log_attrs_bytes(scope_attributes, rules))
                .saturating_add(stream_encoded);
            for record in &sl.log_records {
                if record.attributes.len() > limits.max_attributes_per_record {
                    continue;
                }
                total = total
                    .saturating_add(stream_encoded)
                    .saturating_add(log_attrs_bytes(&record.attributes, rules));
            }
        }
    }
    ProjectedLabelBytes(total)
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
/// rejects the whole set, so counting it as encoded can only overcount. Logs
/// match a rule by raw attribute key
/// ([`TenantRules::lookup_raw`](ravel_ipcrypt::rules::TenantRules::lookup_raw)),
/// and a matched non-empty string value is charged at
/// [`TenantRules::max_stored_len`] (ADR-2993 decision 3): the stream preimage
/// is built from attribute values after encryption, so a stream identity
/// derived from an inflated-length ciphertext must be projected at that
/// inflated length too.
fn encoded_attrs_len(attributes: &[KeyValue], rules: &TenantRules) -> usize {
    attributes
        .iter()
        .map(|kv| {
            let matched = !rules.is_empty() && rules.lookup_raw(&kv.key).is_some();
            encoded_bytes_len(kv.key.len()).saturating_add(encoded_value_len(
                kv.value.as_ref(),
                1,
                matched,
            ))
        })
        .fold(uvarint_len(attributes.len() as u64), usize::saturating_add)
}

fn encoded_value_len(value: Option<&AnyValue>, depth: usize, matched: bool) -> usize {
    if depth > MAX_ATTRIBUTE_NESTING_DEPTH {
        return 0;
    }
    match value.and_then(|v| v.value.as_ref()) {
        None | Some(AnyValueVariant::StringValueStrindex(_)) => 0,
        Some(AnyValueVariant::StringValue(s)) => {
            let len = if matched && !s.is_empty() {
                charged_len(true, s.len())
            } else {
                s.len()
            };
            1usize.saturating_add(encoded_bytes_len(len))
        }
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
            .map(|v| encoded_value_len(Some(v), depth + 1, false))
            .fold(
                1 + uvarint_len(array.values.len() as u64),
                usize::saturating_add,
            ),
        Some(AnyValueVariant::KvlistValue(kvlist)) => kvlist
            .values
            .iter()
            .map(|kv| {
                encoded_bytes_len(kv.key.len()).saturating_add(encoded_value_len(
                    kv.value.as_ref(),
                    depth + 1,
                    false,
                ))
            })
            .fold(
                1 + uvarint_len(kvlist.values.len() as u64),
                usize::saturating_add,
            ),
    }
}

/// Matches [`encoded_attrs_len`]'s raw-key rule lookup for the stored
/// (non-preimage) copy of the same attributes.
fn log_attrs_bytes(attributes: &[KeyValue], rules: &TenantRules) -> usize {
    attributes
        .iter()
        .map(|kv| {
            let matched = !rules.is_empty() && rules.lookup_raw(&kv.key).is_some();
            label_bytes(
                kv.key.len(),
                log_value_payload(kv.value.as_ref(), 1, matched),
            )
        })
        .fold(0usize, usize::saturating_add)
}

/// Payload bytes of a converted log attribute value: a string or bytes value
/// counts its length, a scalar nothing beyond the per-attribute overhead, and
/// every list element or map entry one more overhead plus its own payload
/// (and, for a map entry, its key). A value nested past
/// [`MAX_ATTRIBUTE_NESTING_DEPTH`] is rejected by the converter, so the walk
/// stops there. `matched` charges a non-empty top-level string value at
/// [`TenantRules::max_stored_len`]; nested values are never independently
/// rule-matched.
fn log_value_payload(value: Option<&AnyValue>, depth: usize, matched: bool) -> usize {
    if depth > MAX_ATTRIBUTE_NESTING_DEPTH {
        return 0;
    }
    match value.and_then(|v| v.value.as_ref()) {
        None
        | Some(AnyValueVariant::BoolValue(_))
        | Some(AnyValueVariant::IntValue(_))
        | Some(AnyValueVariant::DoubleValue(_))
        | Some(AnyValueVariant::StringValueStrindex(_)) => 0,
        Some(AnyValueVariant::StringValue(s)) => {
            if matched && !s.is_empty() {
                charged_len(true, s.len())
            } else {
                s.len()
            }
        }
        Some(AnyValueVariant::BytesValue(b)) => b.len(),
        Some(AnyValueVariant::ArrayValue(array)) => array
            .values
            .iter()
            .map(|v| label_bytes(0, log_value_payload(Some(v), depth + 1, false)))
            .fold(0usize, usize::saturating_add),
        Some(AnyValueVariant::KvlistValue(kvlist)) => kvlist
            .values
            .iter()
            .map(|kv| {
                label_bytes(
                    kv.key.len(),
                    log_value_payload(kv.value.as_ref(), depth + 1, false),
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
    rules: &TenantRules,
) -> ProjectedLabelBytes {
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
        let resource_part = span_attrs_bytes(resource_attributes, rules);
        total = total.saturating_add(resource_part);
        for ss in &rs.scope_spans {
            let scope = ss.scope.as_ref();
            let scope_attributes = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
            if scope_attributes.len() > limits.max_scope_attributes {
                continue;
            }
            let mut scope_part = span_attrs_bytes(scope_attributes, rules);
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
                    .saturating_add(span_attrs_bytes(&span.attributes, rules))
                    .saturating_add(reserved_attrs_bytes(span, limits, rules));
            }
        }
    }
    ProjectedLabelBytes(total)
}

/// Spans match a rule by raw attribute key
/// ([`TenantRules::lookup_raw`](ravel_ipcrypt::rules::TenantRules::lookup_raw)),
/// same as logs. A matched non-empty converted value is charged at
/// [`TenantRules::max_stored_len`] (ADR-2993 decision 3).
fn span_attrs_bytes(attributes: &[KeyValue], rules: &TenantRules) -> usize {
    attributes
        .iter()
        .map(|kv| {
            let len = span_value_len(kv.value.as_ref());
            let matched = len > 0 && !rules.is_empty() && rules.lookup_raw(&kv.key).is_some();
            label_bytes(kv.key.len(), charged_len(matched, len))
        })
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
    rules: &TenantRules,
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
        total = total.saturating_add(blob_bytes(
            ATTR_EVENTS_RAW,
            &span.events,
            limits,
            rules,
            |e| &e.attributes,
        ));
    }
    if !span.links.is_empty() {
        total = total.saturating_add(blob_bytes(
            ATTR_LINKS_RAW,
            &span.links,
            limits,
            rules,
            |l| &l.attributes,
        ));
    }
    total
}

/// The hex blob `encode_blob` would store for `items`, or nothing when its
/// raw length is over `max_raw_blob_len` and the blob is dropped.
///
/// When `rules` matches an event or link attribute by raw key, its non-empty
/// string value is charged at [`TenantRules::max_stored_len`]: the blob is
/// measured as if that value were padded to that length, computed from the
/// encoded lengths without building the padded copy. Protobuf length is
/// monotone in a string's length, so the padded blob is never shorter than
/// the encrypted one. The padded length can cross `max_raw_blob_len` while
/// the encrypted blob does not, so such a blob is charged at the cap instead
/// of nothing. Both choices can only over-count, which is the safe direction:
/// a projection under the stored bytes would admit a request over the bound.
fn blob_bytes<T: Message>(
    key: &str,
    items: &[T],
    limits: &SpanIngestLimits,
    rules: &TenantRules,
    attributes: fn(&T) -> &[KeyValue],
) -> usize {
    let mut raw = 0usize;
    let mut padded = 0usize;
    let mut matched = false;
    for item in items {
        let len = item.encoded_len();
        raw = raw.saturating_add(delimited_len(len));
        let mut item_len = len;
        if !rules.is_empty() {
            for kv in attributes(item) {
                let Some(grown) = padded_kv_growth(kv, rules) else {
                    continue;
                };
                item_len = item_len.saturating_add(grown);
                matched = true;
            }
        }
        padded = padded.saturating_add(delimited_len(item_len));
    }
    let charged = if matched {
        padded.min(limits.max_raw_blob_len)
    } else if raw > limits.max_raw_blob_len {
        return 0;
    } else {
        raw
    };
    label_bytes(key.len(), charged.saturating_mul(2))
}

/// A protobuf length-delimited field's payload of `len` bytes plus its length
/// prefix (the tag excluded, as it never changes here).
fn delimited_len(len: usize) -> usize {
    prost::length_delimiter_len(len).saturating_add(len)
}

/// How many bytes `kv`'s encoding inside its event or link grows by when a
/// rule matches its raw key and its non-empty string value is padded to
/// [`TenantRules::max_stored_len`], or `None` when it is not charged so.
/// Each enclosing length prefix (the string, the `AnyValue`, the `KeyValue`)
/// is recomputed for its grown payload.
fn padded_kv_growth(kv: &KeyValue, rules: &TenantRules) -> Option<usize> {
    rules.lookup_raw(&kv.key)?;
    let any = kv.value.as_ref()?;
    let Some(AnyValueVariant::StringValue(s)) = any.value.as_ref() else {
        return None;
    };
    if s.is_empty() {
        return None;
    }
    let grown = |len: usize, by: usize| {
        delimited_len(len.saturating_add(by)).saturating_sub(delimited_len(len))
    };
    let string_growth = grown(s.len(), charged_len(true, s.len()) - s.len());
    let any_growth = grown(any.encoded_len(), string_growth);
    Some(grown(kv.encoded_len(), any_growth))
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
        let rules = TenantRules::none();
        assert_eq!(
            encoded_attrs_len(&attributes, &rules),
            canonical_attr_bytes(&converted).len()
        );
        assert_eq!(
            encoded_attrs_len(&[], &rules),
            canonical_attr_bytes(&[]).len()
        );
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

    use super::capacity_cases::{rules_on, sanitize};

    /// The charge the arithmetic in [`blob_bytes`] must reproduce: pad a copy
    /// of every matched value and measure the copy.
    fn blob_bytes_by_padding_a_copy(
        key: &str,
        items: &[Event],
        limits: &SpanIngestLimits,
        rules: &TenantRules,
    ) -> usize {
        let raw_len = |items: &[Event]| {
            items
                .iter()
                .map(|item| delimited_len(item.encoded_len()))
                .sum::<usize>()
        };
        let raw = raw_len(items);
        let mut copy = items.to_vec();
        let mut matched = false;
        for item in &mut copy {
            for kv in &mut item.attributes {
                if rules.lookup_raw(&kv.key).is_none() {
                    continue;
                }
                if let Some(AnyValueVariant::StringValue(s)) =
                    kv.value.as_mut().and_then(|v| v.value.as_mut())
                    && !s.is_empty()
                {
                    *s = "0".repeat(charged_len(true, s.len()));
                    matched = true;
                }
            }
        }
        let charged = if matched {
            raw_len(&copy).min(limits.max_raw_blob_len)
        } else if raw > limits.max_raw_blob_len {
            return 0;
        } else {
            raw
        };
        label_bytes(key.len(), charged * 2)
    }

    fn client_ip(value: &str) -> KeyValue {
        KeyValue {
            key: "client.ip".to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    fn span_with(events: Vec<Event>, links: Vec<Link>) -> Span {
        Span {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            name: "op".to_string(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            events,
            links,
            ..Default::default()
        }
    }

    /// The stored attributes of `span` normalized alone under `rules`.
    fn stored_attrs(
        span: Span,
        limits: &SpanIngestLimits,
        rules: &TenantRules,
    ) -> Vec<(String, String)> {
        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let out = crate::normalize_traces(req, limits, rules, 5_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.spans.len(), 1);
        out.spans.into_iter().flat_map(|s| s.attrs).collect()
    }

    fn stored_len(attrs: &[(String, String)], key: &str) -> Option<usize> {
        attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v.len())
    }

    /// A 9-byte address under `client.ip` in an event and in a link. The
    /// event is that one attribute: a 24-byte `KeyValue`, 26 bytes as a
    /// field, 27 with the event's own length prefix. The link adds its
    /// 16-byte trace id and 8-byte span id (28 bytes as fields), 55 in all.
    /// Padding the value from 9 to 47 bytes grows the string, the `AnyValue`
    /// and the `KeyValue` fields by 38 each with no prefix crossing a varint
    /// boundary, so each blob is charged 38 bytes more: 65 and 93 bytes, hex
    /// doubled. The stored blobs, holding the ciphertext, fit the charge.
    #[test]
    fn blob_bytes_charges_ruled_event_and_link_attributes_exactly() {
        let rules = rules_on(&["client.ip"]);
        let limits = SpanIngestLimits::default();
        let event = Event {
            attributes: vec![client_ip("192.0.2.1")],
            ..Default::default()
        };
        let link = Link {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            attributes: vec![client_ip("192.0.2.2")],
            ..Default::default()
        };
        let span = span_with(vec![event], vec![link]);

        let unruled = TenantRules::none();
        assert_eq!(
            reserved_attrs_bytes(&span, &limits, &unruled),
            label_bytes(ATTR_EVENTS_RAW.len(), 2 * 27) + label_bytes(ATTR_LINKS_RAW.len(), 2 * 55)
        );
        let events_charge = label_bytes(ATTR_EVENTS_RAW.len(), 2 * 65);
        let links_charge = label_bytes(ATTR_LINKS_RAW.len(), 2 * 93);
        assert_eq!(
            blob_bytes(ATTR_EVENTS_RAW, &span.events, &limits, &rules, |e| &e
                .attributes),
            events_charge
        );
        assert_eq!(
            blob_bytes(ATTR_LINKS_RAW, &span.links, &limits, &rules, |l| &l
                .attributes),
            links_charge
        );
        assert_eq!(
            reserved_attrs_bytes(&span, &limits, &rules),
            events_charge + links_charge
        );

        let attrs = stored_attrs(span, &limits, &rules);
        let events_len = stored_len(&attrs, ATTR_EVENTS_RAW).expect("events blob kept");
        let links_len = stored_len(&attrs, ATTR_LINKS_RAW).expect("links blob kept");
        assert!(events_len <= 2 * 65, "{events_len}");
        assert!(links_len <= 2 * 93, "{links_len}");
    }

    /// A 19-byte hostname under `client.ip` in an event: 37 bytes stored
    /// unchanged (it does not parse as an address), 65 once padded to 47.
    /// Under a 50-byte cap the normalizer keeps the 37-byte blob, and the
    /// projection, which cannot tell a hostname from an address, charges the
    /// cap rather than nothing.
    #[test]
    fn blob_bytes_charges_the_cap_when_padding_crosses_it() {
        let rules = rules_on(&["client.ip"]);
        let limits = SpanIngestLimits {
            max_raw_blob_len: 50,
            ..SpanIngestLimits::default()
        };
        let event = Event {
            attributes: vec![client_ip("db.example.internal")],
            ..Default::default()
        };
        let events = vec![event];
        assert_eq!(
            blob_bytes(
                ATTR_EVENTS_RAW,
                &events,
                &limits,
                &TenantRules::none(),
                |e| { &e.attributes }
            ),
            label_bytes(ATTR_EVENTS_RAW.len(), 2 * 37)
        );
        assert_eq!(
            blob_bytes(ATTR_EVENTS_RAW, &events, &limits, &rules, |e| &e.attributes),
            label_bytes(ATTR_EVENTS_RAW.len(), 2 * 50)
        );

        let attrs = stored_attrs(span_with(events, vec![]), &limits, &rules);
        assert_eq!(stored_len(&attrs, ATTR_EVENTS_RAW), Some(2 * 37));
    }

    use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use proptest::prelude::*;

    const RULED: [&str; 2] = ["client.ip", "peer"];

    fn event_attr() -> impl Strategy<Value = KeyValue> {
        let key = prop_oneof![
            prop::sample::select(RULED.to_vec()).prop_map(str::to_string),
            "[a-z]{1,6}",
        ];
        let value = prop_oneof![
            // Lengths either side of the 47-byte floor and of the one- and
            // two-byte varint boundaries every enclosing prefix crosses.
            prop::sample::select(vec![
                0usize, 1, 46, 47, 48, 80, 100, 120, 127, 128, 200, 16_400
            ])
            .prop_map(|n| Some(AnyValueVariant::StringValue("v".repeat(n)))),
            any::<i64>().prop_map(|i| Some(AnyValueVariant::IntValue(i))),
            Just(None),
        ];
        (key, value).prop_map(|(key, value)| KeyValue {
            key,
            value: value.map(|v| AnyValue { value: Some(v) }),
            ..Default::default()
        })
    }

    fn events() -> impl Strategy<Value = Vec<Event>> {
        prop::collection::vec(
            (prop::collection::vec(event_attr(), 0..4), "[a-z]{0,90}").prop_map(
                |(attributes, name)| Event {
                    time_unix_nano: 1,
                    name,
                    attributes,
                    ..Default::default()
                },
            ),
            0..4,
        )
    }

    proptest! {
        #[test]
        fn blob_bytes_matches_padding_a_copy(
            items in events(),
            cap in prop::sample::select(vec![0usize, 100, 300, 1_000, 65_536]),
        ) {
            let rules = rules_on(&RULED);
            let limits = SpanIngestLimits {
                max_raw_blob_len: cap,
                ..SpanIngestLimits::default()
            };
            prop_assert_eq!(
                blob_bytes(ATTR_EVENTS_RAW, &items, &limits, &rules, |e| &e.attributes),
                blob_bytes_by_padding_a_copy(ATTR_EVENTS_RAW, &items, &limits, &rules)
            );
            let none = TenantRules::none();
            prop_assert_eq!(
                blob_bytes(ATTR_EVENTS_RAW, &items, &limits, &none, |e| &e.attributes),
                blob_bytes_by_padding_a_copy(ATTR_EVENTS_RAW, &items, &limits, &none)
            );
        }

        #[test]
        fn lookup_label_rule_matches_sanitizing_then_looking_up(
            key in prop_oneof![
                Just(String::new()),
                Just("client.ip".to_string()),
                Just("client_ip".to_string()),
                Just("client-ip".to_string()),
                Just("clïent.ip".to_string()),
                Just("1lient.ip".to_string()),
                Just(format!("{}.ip", "a".repeat(300))),
                Just(format!("{}_ip", "a".repeat(300))),
                "[a-z._\\-é0-9]{0,12}",
            ],
        ) {
            let rules = rules_on(&["client.ip", "cl_ent.ip", "_lient.ip", &format!("{}.ip", "a".repeat(300))]);
            prop_assert_eq!(
                lookup_label_rule(&rules, &key).map(|info| info.raw_key),
                rules.lookup_label(&sanitize(&key)).map(|info| info.raw_key)
            );
            prop_assert!(lookup_label_rule(&TenantRules::none(), &key).is_none());
        }
    }
}

/// Attribute sets for the property tests that hold each normalizer to
/// [`label_bytes`]: every kept name and value at exact capacity, and the
/// projection at or above what normalization allocates.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod capacity_cases {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use proptest::prelude::*;

    /// A 17-byte key that sanitising rewrites (`.` and `-` become `_`).
    pub(crate) const SANITISED_17: &str = "http.request-path";

    pub(crate) fn sanitize(key: &str) -> String {
        crate::normalize::sanitize_label_name(key.to_string())
    }

    /// A rule set over `keys`, one rule of `variant` each.
    pub(crate) fn rules_with(keys: &[&str], variant: &str) -> ravel_ipcrypt::rules::TenantRules {
        let entries: Vec<String> = keys
            .iter()
            .map(|k| format!("{{ key = \"{k}\", variant = \"{variant}\" }}"))
            .collect();
        let policy = ravel_ipcrypt::policy::parse_policy_file(
            &format!("[defaults]\nrules = [{}]\n", entries.join(", ")),
            sanitize,
        )
        .expect("policy parses");
        ravel_ipcrypt::rules::TenantRules::resolve(
            &policy,
            "acme",
            &ravel_types::TenantHash([9u8; 16]),
            &[7u8; 32],
            sanitize,
        )
        .expect("rules resolve")
    }

    /// A rule set over `keys`, one deterministic rule each.
    pub(crate) fn rules_on(keys: &[&str]) -> ravel_ipcrypt::rules::TenantRules {
        rules_with(keys, "deterministic")
    }

    pub(crate) fn kv(key: &str, value: Option<AnyValueVariant>) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.map(|v| AnyValue { value: Some(v) }),
            ..Default::default()
        }
    }

    pub(crate) fn int(v: i64) -> Option<AnyValueVariant> {
        Some(AnyValueVariant::IntValue(v))
    }

    pub(crate) fn double(v: f64) -> Option<AnyValueVariant> {
        Some(AnyValueVariant::DoubleValue(v))
    }

    /// Doubles whose formatted length or growth pattern stresses a string's
    /// capacity: `1e260` formats to 261 bytes in a buffer grown to 520.
    pub(crate) const SPECIAL_DOUBLES: [f64; 11] = [
        0.0,
        -0.0,
        5e-324,
        1e-310,
        1e260,
        -1e-300,
        0.1,
        f64::MAX,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];

    pub(crate) fn special_double() -> impl Strategy<Value = f64> {
        prop::sample::select(SPECIAL_DOUBLES.to_vec())
    }

    pub(crate) fn key() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("k".to_string()),
            Just(SANITISED_17.to_string()),
            "[a-z][a-z0-9_]{0,15}",
            "[a-z.\\-/]{1,24}",
            "[a-z]{60,90}",
        ]
    }

    /// Every value shape an attribute can carry: `None` is an attribute with
    /// no value, which metrics treat as empty and logs and traces drop.
    pub(crate) fn value() -> impl Strategy<Value = Option<AnyValueVariant>> {
        prop_oneof![
            any::<i64>().prop_map(int),
            prop::sample::select(vec![0i64, 7, -200, i64::MIN, i64::MAX]).prop_map(int),
            special_double().prop_map(double),
            any::<f64>().prop_map(double),
            "[a-z]{0,3}".prop_map(|s| Some(AnyValueVariant::StringValue(s))),
            "[ -~]{40,300}".prop_map(|s| Some(AnyValueVariant::StringValue(s))),
            any::<bool>().prop_map(|b| Some(AnyValueVariant::BoolValue(b))),
            Just(None),
        ]
    }

    /// Up to `max` attributes with distinct sanitised keys, so a set is never
    /// rejected for a duplicate label name.
    pub(crate) fn attributes(max: usize) -> impl Strategy<Value = Vec<KeyValue>> {
        prop::collection::vec((key(), value()), 0..=max).prop_map(|pairs| {
            let mut seen = std::collections::HashSet::new();
            pairs
                .into_iter()
                .filter(|(k, _)| seen.insert(crate::normalize::sanitize_label_name(k.clone())))
                .map(|(k, v)| kv(&k, v))
                .collect()
        })
    }

    /// The fixed cases each property test also runs: one attribute alone in
    /// an otherwise empty resource, where the projection has no other label
    /// to borrow slack from.
    pub(crate) fn fixed_single_attributes() -> Vec<KeyValue> {
        vec![
            kv("k", int(0)),
            kv("k", double(1e260)),
            kv("k", int(-200)),
            kv(SANITISED_17, int(7)),
            kv(SANITISED_17, Some(AnyValueVariant::StringValue("v".into()))),
        ]
    }

    /// [`attributes`], with each of [`fixed_single_attributes`] alone mixed in.
    pub(crate) fn attribute_sets(max: usize) -> impl Strategy<Value = Vec<KeyValue>> {
        prop_oneof![
            1 => prop::sample::select(fixed_single_attributes()).prop_map(|kv| vec![kv]),
            4 => attributes(max),
        ]
    }
}
