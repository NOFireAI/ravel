//! Columnar normalization of decoded OTAP METRICS payloads (Part 2 of
//! issue #12) into Ravel's canonical metric point representation.
//!
//! This mirrors `ravel_otlp::normalize::normalize_metrics` point-for-point:
//! same admission limits (`ravel_otlp::IngestLimits`), same sanitization,
//! same [`Rejection`] classes, same `is_monotonic_sum` semantics. The
//! differential gate (tests/differential.rs) asserts the two paths produce
//! identical `SeriesId` sets, identical `(series, ts, value)` samples, and
//! identical rejection classes for the same logical input. Where
//! `ravel_otlp::normalize` exposes a helper as `pub`, we call it directly;
//! where a helper is private (sanitization, `push_checked`,
//! `any_value_to_label_value`), we mirror it exactly below. See the crate
//! report for a shared-helper refactor suggestion.
//!
//! Known scope gap (flagged, not silently worked around): the OTAP test
//! encoder in `encode.rs` never emits `RESOURCE_ATTRS` or `SCOPE_ATTRS`
//! payloads, and the `METRICS` table it produces carries no
//! `resource_id`/`scope_id` columns to join them against even if it did.
//! Every point normalized here therefore gets an empty resource label set
//! (equivalent to OTLP's `resource: None` case: no `job`/`instance`
//! synthesis, no allowlisted resource attributes). Wiring resource/scope
//! identity through `StreamState`/`encode.rs` is follow-up work; until then
//! the "group by distinct (resource identity, metric, attr-set)" dimension
//! from docs/otap-ingest.md collapses to (metric, attr-set) here.
//!
//! Performance shape (docs/otap-ingest.md): data points join their attrs
//! table by `parent_id` via a sort-and-binary-search over "sorted runs"
//! (never a per-point hash lookup), and the root METRICS table joins by a
//! dense id-indexed array (metric ids are `u16`, so this is a bounded
//! "dictionary index", not a hash map). Distinct attribute sets are grouped
//! per metric so `LabelSet`/`SeriesId` are computed once per distinct group
//! rather than once per point; a `HashMap` is used for that grouping step
//! (not the join), which is exactly where docs/otap-ingest.md says the
//! win comes from ("the BLAKE3 canonicalization runs once per distinct
//! combination per batch instead of once per point").

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, DictionaryArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow::datatypes::UInt8Type;
use ravel_otlp::{IngestLimits, NormalizeOutput, NormalizedPoint, Rejection};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId, TypeError};

use crate::proto::experimental::arrow::v1::ArrowPayloadType;
use crate::stream::DecodedBatch;

/// AnyValue `type` discriminant (otap-spec.md section 5.5.1).
pub const ANY_VALUE_TYPE_EMPTY: u8 = 0;
pub const ANY_VALUE_TYPE_STRING: u8 = 1;
pub const ANY_VALUE_TYPE_BOOL: u8 = 2;
pub const ANY_VALUE_TYPE_INT: u8 = 3;
pub const ANY_VALUE_TYPE_DOUBLE: u8 = 4;
pub const ANY_VALUE_TYPE_BYTES: u8 = 5;
pub const ANY_VALUE_TYPE_ARRAY: u8 = 6;
pub const ANY_VALUE_TYPE_MAP: u8 = 7;

/// METRICS table `metric_type` discriminant (data_model.md). No fixed
/// numeric mapping is given in the vendored spec docs beyond "Metric type
/// enum (Gauge, Sum, Histogram, etc.)" (otap-spec.md line 506); these
/// ordinals match OTLP's `pmetric.MetricType` Go enum so fixtures built
/// against one convention are meaningful against the other.
pub const METRIC_TYPE_GAUGE: u8 = 1;
pub const METRIC_TYPE_SUM: u8 = 2;
pub const METRIC_TYPE_HISTOGRAM: u8 = 3;
pub const METRIC_TYPE_EXPONENTIAL_HISTOGRAM: u8 = 4;
pub const METRIC_TYPE_SUMMARY: u8 = 5;

/// METRICS table `aggregation_temporality` ordinals, matching OTLP's
/// `AggregationTemporality` enum (the OTAP data model copies this field
/// verbatim from the OTLP proto).
pub const AGGREGATION_TEMPORALITY_UNSPECIFIED: i32 = 0;
pub const AGGREGATION_TEMPORALITY_DELTA: i32 = 1;
pub const AGGREGATION_TEMPORALITY_CUMULATIVE: i32 = 2;

/// Root metric ids are `u16`; a dense lookup array of this size is a
/// bounded "dictionary index", not a per-point hash map.
const MAX_METRIC_IDS: usize = 1 << 16;

/// Fallback classification for a `NUMBER_DATA_POINTS` row whose parent
/// metric id is missing from the root table, or whose root row is not a
/// Gauge or Sum (a `NUMBER_DATA_POINTS` row should never point at anything
/// else per otap-spec.md's payload-type table; this is defensive, not a
/// path our own encoder can produce).
const UNKNOWN_OTAP_METRIC_TYPE: &str = "unknown_otap_metric_type";

/// Decode and normalize gauge and sum data points from one decoded OTAP
/// `BatchArrowRecords` message (`batch.batch_id`'s payloads).
///
/// Mirrors [`ravel_otlp::normalize_metrics`]'s contract: nothing here
/// panics on malformed or oversized input, and every unsupported or
/// rejected data point is accounted for in `NormalizeOutput::rejected`,
/// never silently dropped.
pub fn normalize_decoded(
    tenant: &TenantId,
    batch: &DecodedBatch,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
) -> NormalizeOutput {
    let total_points = count_total_points(batch);
    if total_points > limits.max_data_points_per_request {
        return NormalizeOutput {
            points: Vec::new(),
            rejected: vec![Rejection::TooManyDataPoints {
                count: total_points,
                max: limits.max_data_points_per_request,
            }],
        };
    }

    let mut rejected = Vec::new();
    push_unsupported_type_rejections(batch, &mut rejected);

    let root_batches: Vec<&RecordBatch> = payloads_of(batch, ArrowPayloadType::UnivariateMetrics);
    let dp_batches: Vec<&RecordBatch> = payloads_of(batch, ArrowPayloadType::NumberDataPoints);
    let attr_batches: Vec<&RecordBatch> = payloads_of(batch, ArrowPayloadType::NumberDpAttrs);

    let flat_dp = flatten_dp(&dp_batches);
    let flat_attrs = flatten_attrs(&attr_batches);

    let dense_size = dense_size_for(&root_batches, &flat_dp);
    let root = build_root_table(&root_batches, dense_size);
    let (metric_decision, unknown_metric_count) =
        build_metric_decisions(&root, &flat_dp, limits, dense_size, &mut rejected);
    if unknown_metric_count > 0 {
        rejected.push(Rejection::UnsupportedMetricType {
            metric_type: UNKNOWN_OTAP_METRIC_TYPE,
            count: unknown_metric_count,
        });
    }

    let attr_order = sort_attrs_by_parent(&flat_attrs);
    let mut points = Vec::with_capacity(flat_dp.len());
    let mut group_cache: GroupCache = HashMap::new();

    for dp in &flat_dp {
        let Some(decision) = metric_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };

        let range = attr_range_for(&flat_attrs, &attr_order, dp.id);
        if range.len() > limits.max_attributes_per_point {
            rejected.push(Rejection::TooManyAttributes {
                attribute_count: range.len(),
                max: limits.max_attributes_per_point,
            });
            continue;
        }

        if dp.ts_ns == 0 {
            rejected.push(Rejection::ZeroTimestamp);
            continue;
        }
        let skew_ns = dp.ts_ns.saturating_sub(ingest_ts_ns);
        if skew_ns > limits.max_future_skew_ns {
            rejected.push(Rejection::FutureSkew {
                skew_ns,
                max_ns: limits.max_future_skew_ns,
            });
            continue;
        }
        let lag_ns = ingest_ts_ns.saturating_sub(dp.ts_ns);
        if lag_ns > limits.max_ingest_lag_ns {
            rejected.push(Rejection::TooOld {
                lag_ns,
                max_ns: limits.max_ingest_lag_ns,
            });
            continue;
        }

        let mut raw: Vec<(String, RawCell)> = range
            .iter()
            .map(|&i| {
                let a = &flat_attrs[i as usize];
                (a.key.to_string(), raw_cell(a))
            })
            .collect();
        raw.sort_by(|a, b| a.0.cmp(&b.0));

        let metric_name = &decision.name;
        let metric_cache = group_cache.entry(dp.parent_id as u32).or_default();
        let outcome = metric_cache
            .entry(raw)
            .or_insert_with_key(|raw_key| build_group(raw_key, metric_name, tenant, limits));

        match outcome {
            Ok((label_set, series_id)) => {
                points.push(NormalizedPoint {
                    series_id: *series_id,
                    labels: label_set.clone(),
                    sample: Sample {
                        ts_ns: dp.ts_ns,
                        value: dp.value,
                    },
                    is_monotonic_sum: decision.is_sum && decision.is_monotonic,
                });
            }
            Err(rejection) => rejected.push(rejection.clone()),
        }
    }

    NormalizeOutput { points, rejected }
}

fn payloads_of(batch: &DecodedBatch, ty: ArrowPayloadType) -> Vec<&RecordBatch> {
    batch
        .payloads
        .iter()
        .filter(|(t, _)| *t == ty)
        .map(|(_, rb)| rb)
        .collect()
}

fn count_total_points(batch: &DecodedBatch) -> usize {
    [
        ArrowPayloadType::NumberDataPoints,
        ArrowPayloadType::HistogramDataPoints,
        ArrowPayloadType::ExpHistogramDataPoints,
        ArrowPayloadType::SummaryDataPoints,
    ]
    .into_iter()
    .map(|ty| {
        batch
            .payloads
            .iter()
            .filter(|(t, _)| *t == ty)
            .map(|(_, rb)| rb.num_rows())
            .sum::<usize>()
    })
    .sum()
}

/// Unsupported metric payload types are counted rejections, never silent
/// drops: one combined [`Rejection::UnsupportedMetricType`] per payload
/// type present in this batch, summing rows across every metric that
/// shares it (see module docs: OTAP's columnar tables don't carry a
/// natural per-metric split without a root-table join we don't need for
/// any other reason, so we reject at payload-type granularity).
fn push_unsupported_type_rejections(batch: &DecodedBatch, rejected: &mut Vec<Rejection>) {
    for (ty, label) in [
        (ArrowPayloadType::HistogramDataPoints, "histogram"),
        (
            ArrowPayloadType::ExpHistogramDataPoints,
            "exponential_histogram",
        ),
        (ArrowPayloadType::SummaryDataPoints, "summary"),
    ] {
        let count: usize = batch
            .payloads
            .iter()
            .filter(|(t, _)| *t == ty)
            .map(|(_, rb)| rb.num_rows())
            .sum();
        if count > 0 {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: label,
                count,
            });
        }
    }
}

struct RootEntry {
    name: String,
    kind: RootKind,
}

enum RootKind {
    Gauge,
    Sum {
        temporality: i32,
        is_monotonic: bool,
    },
    Unsupported,
}

struct MetricDecision {
    name: String,
    is_sum: bool,
    is_monotonic: bool,
}

struct FlatDp {
    id: u32,
    parent_id: u16,
    ts_ns: i64,
    value: f64,
}

struct FlatAttr<'b> {
    parent_id: u32,
    key: &'b str,
    ty: u8,
    str_val: Option<&'b str>,
    bool_val: Option<bool>,
    int_val: Option<i64>,
    double_val: Option<f64>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum RawCell {
    Str(String),
    Bool(bool),
    Int(i64),
    /// `f64::to_bits()`: float comparisons in dedup paths use bit patterns,
    /// never `==` (NaN payloads and -0.0 are significant).
    Double(u64),
    Complex,
}

/// Memoization cache for the series-grouping step: per metric id, maps a
/// canonicalized (sorted-by-key) attribute set to its already-computed
/// `(LabelSet, SeriesId)` or rejection, so BLAKE3 canonicalization runs
/// once per distinct combination per batch instead of once per point.
type GroupCache =
    HashMap<u32, HashMap<Vec<(String, RawCell)>, Result<(LabelSet, SeriesId), Rejection>>>;

fn dense_size_for(root_batches: &[&RecordBatch], flat_dp: &[FlatDp]) -> usize {
    let mut max_id: u32 = 0;
    for rb in root_batches {
        if let Some(ids) = column_as::<UInt16Array>(rb, "id") {
            for &v in ids.values() {
                max_id = max_id.max(u32::from(v));
            }
        }
    }
    for dp in flat_dp {
        max_id = max_id.max(u32::from(dp.parent_id));
    }
    (max_id as usize + 1).min(MAX_METRIC_IDS)
}

fn column_as<'b, T: 'static>(rb: &'b RecordBatch, name: &str) -> Option<&'b T> {
    rb.column_by_name(name)?.as_any().downcast_ref::<T>()
}

fn name_at(col: &Arc<dyn Array>, i: usize) -> Option<String> {
    if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<UInt8Type>>() {
        let keys = dict.keys();
        if keys.is_null(i) {
            return None;
        }
        let idx = keys.value(i) as usize;
        let values = dict.values().as_any().downcast_ref::<StringArray>()?;
        return Some(values.value(idx).to_string());
    }
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        if s.is_null(i) {
            return None;
        }
        return Some(s.value(i).to_string());
    }
    None
}

fn opt_i32(col: Option<&Int32Array>, i: usize) -> Option<i32> {
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn opt_bool(col: Option<&BooleanArray>, i: usize) -> Option<bool> {
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn opt_str(col: Option<&StringArray>, i: usize) -> Option<&str> {
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn opt_i64(col: Option<&Int64Array>, i: usize) -> Option<i64> {
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn opt_f64(col: Option<&Float64Array>, i: usize) -> Option<f64> {
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn build_root_table(root_batches: &[&RecordBatch], dense_size: usize) -> Vec<Option<RootEntry>> {
    let mut entries: Vec<Option<RootEntry>> = (0..dense_size).map(|_| None).collect();
    for rb in root_batches {
        let Some(ids) = column_as::<UInt16Array>(rb, "id") else {
            continue;
        };
        let Some(types) = column_as::<UInt8Array>(rb, "metric_type") else {
            continue;
        };
        let Some(name_col) = rb.column_by_name("name") else {
            continue;
        };
        let temporality_col = column_as::<Int32Array>(rb, "aggregation_temporality");
        let monotonic_col = column_as::<BooleanArray>(rb, "is_monotonic");

        for i in 0..rb.num_rows() {
            let id = ids.value(i);
            let Some(name) = name_at(name_col, i) else {
                continue;
            };
            let kind = match types.value(i) {
                METRIC_TYPE_GAUGE => RootKind::Gauge,
                METRIC_TYPE_SUM => {
                    let temporality =
                        opt_i32(temporality_col, i).unwrap_or(AGGREGATION_TEMPORALITY_UNSPECIFIED);
                    let is_monotonic = opt_bool(monotonic_col, i).unwrap_or(false);
                    RootKind::Sum {
                        temporality,
                        is_monotonic,
                    }
                }
                _ => RootKind::Unsupported,
            };
            if (id as usize) < entries.len() {
                entries[id as usize] = Some(RootEntry { name, kind });
            }
        }
    }
    entries
}

fn flatten_dp(dp_batches: &[&RecordBatch]) -> Vec<FlatDp> {
    let mut out = Vec::new();
    for rb in dp_batches {
        let Some(ids) = column_as::<UInt32Array>(rb, "id") else {
            continue;
        };
        let Some(parents) = column_as::<UInt16Array>(rb, "parent_id") else {
            continue;
        };
        let Some(times) = column_as::<TimestampNanosecondArray>(rb, "time_unix_nano") else {
            continue;
        };
        let Some(values) = column_as::<Float64Array>(rb, "double_value") else {
            continue;
        };
        for i in 0..rb.num_rows() {
            out.push(FlatDp {
                id: ids.value(i),
                parent_id: parents.value(i),
                ts_ns: times.value(i),
                value: values.value(i),
            });
        }
    }
    out
}

fn flatten_attrs<'b>(attr_batches: &[&'b RecordBatch]) -> Vec<FlatAttr<'b>> {
    let mut out = Vec::new();
    for rb in attr_batches {
        let Some(parents) = column_as::<UInt32Array>(rb, "parent_id") else {
            continue;
        };
        let Some(keys) = column_as::<StringArray>(rb, "key") else {
            continue;
        };
        let Some(types) = column_as::<UInt8Array>(rb, "type") else {
            continue;
        };
        let strs = column_as::<StringArray>(rb, "str");
        let bools = column_as::<BooleanArray>(rb, "bool");
        let ints = column_as::<Int64Array>(rb, "int");
        let doubles = column_as::<Float64Array>(rb, "double");
        for i in 0..rb.num_rows() {
            out.push(FlatAttr {
                parent_id: parents.value(i),
                key: keys.value(i),
                ty: types.value(i),
                str_val: opt_str(strs, i),
                bool_val: opt_bool(bools, i),
                int_val: opt_i64(ints, i),
                double_val: opt_f64(doubles, i),
            });
        }
    }
    out
}

fn raw_cell(a: &FlatAttr) -> RawCell {
    match a.ty {
        ANY_VALUE_TYPE_EMPTY => RawCell::Str(String::new()),
        ANY_VALUE_TYPE_STRING => RawCell::Str(a.str_val.unwrap_or("").to_string()),
        ANY_VALUE_TYPE_BOOL => RawCell::Bool(a.bool_val.unwrap_or(false)),
        ANY_VALUE_TYPE_INT => RawCell::Int(a.int_val.unwrap_or(0)),
        ANY_VALUE_TYPE_DOUBLE => RawCell::Double(a.double_val.unwrap_or(0.0).to_bits()),
        _ => RawCell::Complex,
    }
}

/// Per-metric total data-point count, checked once per metric id (matches
/// `ravel_otlp::normalize_metric`'s single up-front check per `Metric`
/// rather than per point) plus the id-space dense lookup.
fn build_metric_decisions(
    root: &[Option<RootEntry>],
    flat_dp: &[FlatDp],
    limits: &IngestLimits,
    dense_size: usize,
    rejected: &mut Vec<Rejection>,
) -> (Vec<Option<MetricDecision>>, usize) {
    let mut dp_count = vec![0u32; dense_size];
    for dp in flat_dp {
        if (dp.parent_id as usize) < dp_count.len() {
            dp_count[dp.parent_id as usize] += 1;
        }
    }

    let mut decisions: Vec<Option<MetricDecision>> = (0..dense_size).map(|_| None).collect();
    let mut unknown_count = 0usize;

    for id in 0..dense_size {
        let count = dp_count[id] as usize;
        if count == 0 {
            continue;
        }
        match root.get(id).and_then(|e| e.as_ref()) {
            None => unknown_count += count,
            Some(entry) => match &entry.kind {
                RootKind::Unsupported => unknown_count += count,
                RootKind::Gauge => {
                    if let Some(name) = process_metric_name(&entry.name, limits, count, rejected) {
                        decisions[id] = Some(MetricDecision {
                            name,
                            is_sum: false,
                            is_monotonic: false,
                        });
                    }
                }
                RootKind::Sum {
                    temporality,
                    is_monotonic,
                } => {
                    if *temporality != AGGREGATION_TEMPORALITY_CUMULATIVE {
                        rejected.push(Rejection::UnsupportedTemporality { count });
                        continue;
                    }
                    if let Some(name) = process_metric_name(&entry.name, limits, count, rejected) {
                        decisions[id] = Some(MetricDecision {
                            name,
                            is_sum: true,
                            is_monotonic: *is_monotonic,
                        });
                    }
                }
            },
        }
    }

    (decisions, unknown_count)
}

fn process_metric_name(
    name: &str,
    limits: &IngestLimits,
    count: usize,
    rejected: &mut Vec<Rejection>,
) -> Option<String> {
    if name.len() > limits.max_metric_name_len {
        rejected.push(Rejection::MetricNameTooLong {
            len: name.len(),
            max: limits.max_metric_name_len,
            count,
        });
        return None;
    }
    let sanitized = sanitize_metric_name(name);
    if sanitized.is_empty() {
        rejected.push(Rejection::EmptyMetricName { count });
        return None;
    }
    Some(sanitized)
}

fn sort_attrs_by_parent(attrs: &[FlatAttr]) -> Vec<u32> {
    let mut order: Vec<u32> = (0..attrs.len() as u32).collect();
    order.sort_by_key(|&i| attrs[i as usize].parent_id);
    order
}

fn attr_range_for<'a>(attrs: &[FlatAttr], order: &'a [u32], parent_id: u32) -> &'a [u32] {
    let start = order.partition_point(|&i| attrs[i as usize].parent_id < parent_id);
    let end = order.partition_point(|&i| attrs[i as usize].parent_id <= parent_id);
    &order[start..end]
}

/// Build the `(LabelSet, SeriesId)` for one distinct attribute-set group
/// under one metric; called at most once per distinct group thanks to the
/// caller's memoizing `HashMap` (see module docs).
fn build_group(
    raw: &[(String, RawCell)],
    metric_name: &str,
    tenant: &TenantId,
    limits: &IngestLimits,
) -> Result<(LabelSet, SeriesId), Rejection> {
    let mut labels = Vec::with_capacity(raw.len() + 1);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric_name.to_string(),
    });
    for (raw_key, cell) in raw {
        let name = sanitize_label_name(raw_key);
        let value = match cell {
            RawCell::Str(s) => s.clone(),
            RawCell::Bool(b) => b.to_string(),
            RawCell::Int(i) => i.to_string(),
            RawCell::Double(bits) => f64::from_bits(*bits).to_string(),
            RawCell::Complex => return Err(Rejection::ComplexAttributeValue),
        };
        push_checked(&mut labels, name, value, limits)?;
    }
    let label_set = LabelSet::new(labels).map_err(|err| match err {
        TypeError::DuplicateLabelName(name) => Rejection::DuplicateLabelName(name),
        _ => Rejection::DuplicateLabelName(String::new()),
    })?;
    let series_id = SeriesId::compute(tenant, metric_name, &label_set)
        .map_err(|_| Rejection::OversizedSeriesComponent)?;
    Ok((label_set, series_id))
}

// --- Mirrors of ravel_otlp::normalize's private helpers (see module docs:
// these are not `pub` in ravel-otlp, so they are copied verbatim rather
// than reused; flagged for a shared-helper crate in the final report). ---

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
