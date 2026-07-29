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
//!
//! Histogram and summary data points (ADR-0016 phase B2) explode into the
//! same Prometheus-convention scalar series as
//! `ravel_otlp::normalize::explode_histogram`/`explode_summary`, mirrored
//! here rather than reused (both are private to `ravel_otlp::normalize`):
//! same bucket accumulation and overflow checks, same `le`/`quantile`
//! formatting (`ravel_otlp::promcompat::format_float`, which *is* `pub`
//! and called directly), same stale-marker and min/max/exemplar drop-
//! counter rules, same atomic per-point rejection. Unlike the gauge/sum
//! path's batch-wide `GroupCache`, exploded series are resolved with a
//! per-metric last-seen [`SeriesIdMemo`] -- the same memoization
//! granularity `ravel_otlp` itself uses -- since one input point yields
//! several series with different synthesized names/labels rather than one
//! series per distinct attribute set. OTAP carries histogram exemplars in
//! a separate `HISTOGRAM_DP_EXEMPLARS` table (OTLP embeds them inline on
//! the data point), so the `HistogramExemplarsDropped` counter is derived
//! by counting exemplar rows per parent data-point id rather than reading
//! a field. OTAP-carried exponential histograms keep rejecting typed via
//! [`push_unsupported_type_rejections`] (ADR-0017 is a separate, later
//! ticket).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, DictionaryArray, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, StructArray, TimestampNanosecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::UInt8Type;
use ravel_otlp::promcompat::format_float;
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

/// Tag for a whole `NUMBER_DATA_POINTS` batch that is missing a column
/// required to decode any of its rows (`id`, `parent_id`, `time_unix_nano`,
/// or both `double_value` and `int_value`). Reported via the same counted
/// [`Rejection::UnsupportedMetricType`] mechanism [`push_unsupported_type_rejections`]
/// uses for the exponential-histogram class and [`build_metric_decisions`]
/// uses for [`UNKNOWN_OTAP_METRIC_TYPE`], since `ravel_otlp::Rejection` has no
/// bespoke "malformed batch" variant (see the crate report for that gap).
const MALFORMED_NUMBER_DATA_POINTS_BATCH: &str = "malformed_number_data_points_batch";

/// otap-spec.md section 6.4: the two `id`/`parent_id` transport encodings
/// this pass decodes. Quasi-delta (`*Attrs.parent_id`, `*DpExemplars.parent_id`)
/// is not decoded yet -- those columns are still read as literal values (see
/// docs/otap-ingest.md).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IdEncoding {
    Plain,
    Delta,
}

/// Tag for a whole batch whose `id`/`parent_id` field metadata declares an
/// `encoding` value this pass does not recognize as a valid encoding for
/// that column (anything other than `"plain"`/`"delta"`, including
/// `"quasidelta"` on a column this pass does not decode as quasi-delta).
/// Rejected outright via the same counted mechanism as
/// [`MALFORMED_NUMBER_DATA_POINTS_BATCH`], never guessed.
const UNRECOGNIZED_ID_ENCODING: &str = "unrecognized_id_encoding";

/// Resolve the declared encoding of an `id`/`parent_id` column from its
/// Arrow field metadata (otap-spec.md section 6.4: key `"encoding"`, values
/// `"plain"`, `"delta"`, `"quasidelta"`). No declaration defaults to
/// [`IdEncoding::Delta`], the spec's stated default for these columns. `Err`
/// means the column is missing (caller already checked for this) or the
/// declaration is something this call site does not recognize.
fn id_encoding(rb: &RecordBatch, column: &str) -> Result<IdEncoding, ()> {
    let field = rb.schema_ref().field_with_name(column).map_err(|_| ())?;
    match field.metadata().get("encoding").map(String::as_str) {
        None => Ok(IdEncoding::Delta),
        Some("plain") => Ok(IdEncoding::Plain),
        Some("delta") => Ok(IdEncoding::Delta),
        Some(_) => Err(()),
    }
}

/// Decode a delta-encoded `u32` id column (otap-spec.md section 6.4.2):
/// each decoded value is the running sum of encoded values up to and
/// including that row. Null values are excluded from the running sum (the
/// accumulator carries through unchanged); the summation wraps rather than
/// panics, since a decode path must never panic on adversarial input.
fn decode_delta_u32(values: &UInt32Array) -> Vec<u32> {
    let mut acc: u32 = 0;
    (0..values.len())
        .map(|i| {
            if !values.is_null(i) {
                acc = acc.wrapping_add(values.value(i));
            }
            acc
        })
        .collect()
}

/// Decode a delta-encoded `u16` id column; see [`decode_delta_u32`].
fn decode_delta_u16(values: &UInt16Array) -> Vec<u16> {
    let mut acc: u16 = 0;
    (0..values.len())
        .map(|i| {
            if !values.is_null(i) {
                acc = acc.wrapping_add(values.value(i));
            }
            acc
        })
        .collect()
}

/// Resolve `column`'s declared encoding and decode it into absolute values,
/// pushing [`UNRECOGNIZED_ID_ENCODING`] (tagged with `rb`'s row count) and
/// returning `None` if the declaration is not recognized.
fn decode_id_column_u32(
    rb: &RecordBatch,
    column: &str,
    values: &UInt32Array,
    rejected: &mut Vec<Rejection>,
) -> Option<Vec<u32>> {
    match id_encoding(rb, column) {
        Ok(IdEncoding::Plain) => Some(values.values().to_vec()),
        Ok(IdEncoding::Delta) => Some(decode_delta_u32(values)),
        Err(()) => {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: UNRECOGNIZED_ID_ENCODING,
                count: rb.num_rows(),
            });
            None
        }
    }
}

/// `u16` counterpart of [`decode_id_column_u32`].
fn decode_id_column_u16(
    rb: &RecordBatch,
    column: &str,
    values: &UInt16Array,
    rejected: &mut Vec<Rejection>,
) -> Option<Vec<u16>> {
    match id_encoding(rb, column) {
        Ok(IdEncoding::Plain) => Some(values.values().to_vec()),
        Ok(IdEncoding::Delta) => Some(decode_delta_u16(values)),
        Err(()) => {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: UNRECOGNIZED_ID_ENCODING,
                count: rb.num_rows(),
            });
            None
        }
    }
}

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
            // OTAP carries only scalar metric points; native-histogram
            // admission is the OTLP/Remote Write surface's concern, so this
            // vector is always empty here (ravel_otlp::NormalizeOutput gained
            // it for those surfaces).
            histogram_points: Vec::new(),
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
    let hist_dp_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::HistogramDataPoints);
    let hist_attr_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::HistogramDpAttrs);
    let hist_exemplar_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::HistogramDpExemplars);
    let summary_dp_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::SummaryDataPoints);
    let summary_attr_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::SummaryDpAttrs);

    let flat_dp = flatten_dp(&dp_batches, &mut rejected);
    let flat_attrs = flatten_attrs(&attr_batches);
    let flat_hist_dp = flatten_histogram_dp(&hist_dp_batches, &mut rejected);
    let flat_hist_attrs = flatten_attrs(&hist_attr_batches);
    let hist_exemplar_counts = count_by_parent_id(&hist_exemplar_batches);
    let flat_summary_dp = flatten_summary_dp(&summary_dp_batches, &mut rejected);
    let flat_summary_attrs = flatten_attrs(&summary_attr_batches);

    let root_ids = decode_root_ids(&root_batches, &mut rejected);
    let dense_size = dense_size_for(
        &root_ids,
        flat_dp
            .iter()
            .map(|d| d.parent_id)
            .chain(flat_hist_dp.iter().map(|d| d.parent_id))
            .chain(flat_summary_dp.iter().map(|d| d.parent_id)),
    );
    let root = build_root_table(&root_batches, &root_ids, dense_size);
    let (metric_decision, unknown_number_count) =
        build_metric_decisions(&root, &flat_dp, limits, dense_size, &mut rejected);
    let (hist_decision, unknown_hist_count) =
        build_histogram_decisions(&root, &flat_hist_dp, limits, dense_size, &mut rejected);
    let (summary_decision, unknown_summary_count) =
        build_summary_decisions(&root, &flat_summary_dp, limits, dense_size, &mut rejected);
    let unknown_metric_count = unknown_number_count + unknown_hist_count + unknown_summary_count;
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

        // A NoRecordedValue point never consults double_value/int_value,
        // matching OTLP's build_point: the sample is unconditionally the
        // stale marker even if this row's value columns are both absent.
        let value = if has_no_recorded_value(dp.flags) {
            stale_nan()
        } else {
            match dp.value {
                Some(v) => v,
                None => {
                    rejected.push(Rejection::MissingValue);
                    continue;
                }
            }
        };

        let raw: Vec<(String, RawCell)> = range
            .iter()
            .map(|&i| {
                let a = &flat_attrs[i as usize];
                (a.key.to_string(), raw_cell(a))
            })
            .collect();

        // Per-label admission (complex value, then sanitized-name length,
        // then value length) runs in the attributes' input order with the
        // first violation winning, exactly as ravel_otlp::build_point checks a
        // data point's attributes in wire order. Attribute order is not part
        // of series identity, so the canonical grouping key below is sorted;
        // this pre-check is the only place attribute order is observable, and
        // it must line up with the OTLP oracle or the two paths class the same
        // input differently (ADR-0011; a9-F01 mechanism b).
        if let Err(rejection) = check_attrs_in_input_order(&raw, limits) {
            rejected.push(rejection);
            continue;
        }

        let mut group_key = raw;
        group_key.sort_by(|a, b| a.0.cmp(&b.0));

        let metric_name = &decision.name;
        let metric_cache = group_cache.entry(dp.parent_id as u32).or_default();
        let outcome = metric_cache
            .entry(group_key)
            .or_insert_with_key(|raw_key| build_group(raw_key, metric_name, tenant, limits));

        match outcome {
            Ok((label_set, series_id)) => {
                points.push(NormalizedPoint {
                    series_id: *series_id,
                    labels: label_set.clone(),
                    sample: Sample {
                        ts_ns: dp.ts_ns,
                        value,
                    },
                    is_monotonic_sum: decision.is_sum && decision.is_monotonic,
                });
            }
            Err(rejection) => rejected.push(rejection.clone()),
        }
    }

    let hist_attr_order = sort_attrs_by_parent(&flat_hist_attrs);
    let mut hist_memos: HashMap<u16, SeriesIdMemo> = HashMap::new();
    for dp in &flat_hist_dp {
        let Some(decision) = hist_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };
        let range = attr_range_for(&flat_hist_attrs, &hist_attr_order, dp.id);
        let exemplar_count = hist_exemplar_counts.get(&dp.id).copied().unwrap_or(0);
        let memo = hist_memos
            .entry(dp.parent_id)
            .or_insert_with(SeriesIdMemo::new);
        match explode_histogram_point(
            tenant,
            &decision.name,
            dp,
            range,
            &flat_hist_attrs,
            limits,
            ingest_ts_ns,
            exemplar_count,
            memo,
        ) {
            Ok((mut new_points, informational)) => {
                points.append(&mut new_points);
                rejected.extend(informational);
            }
            Err(rejection) => rejected.push(rejection),
        }
    }

    let summary_attr_order = sort_attrs_by_parent(&flat_summary_attrs);
    let mut summary_memos: HashMap<u16, SeriesIdMemo> = HashMap::new();
    for dp in &flat_summary_dp {
        let Some(decision) = summary_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };
        let range = attr_range_for(&flat_summary_attrs, &summary_attr_order, dp.id);
        let memo = summary_memos
            .entry(dp.parent_id)
            .or_insert_with(SeriesIdMemo::new);
        match explode_summary_point(
            tenant,
            &decision.name,
            dp,
            range,
            &flat_summary_attrs,
            limits,
            ingest_ts_ns,
            memo,
        ) {
            Ok(mut new_points) => points.append(&mut new_points),
            Err(rejection) => rejected.push(rejection),
        }
    }

    NormalizeOutput {
        points,
        // Always empty: OTAP admits only scalar points (see the early
        // return above).
        histogram_points: Vec::new(),
        rejected,
    }
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

/// Exponential histograms are the one remaining unsupported metric payload
/// type (ADR-0017, out of B2's scope per docs/ingest-breadth-plan.md §7):
/// counted as one [`Rejection::UnsupportedMetricType`], never a silent drop.
/// Histogram and summary payloads are normalized below instead of rejected
/// here.
fn push_unsupported_type_rejections(batch: &DecodedBatch, rejected: &mut Vec<Rejection>) {
    let count: usize = batch
        .payloads
        .iter()
        .filter(|(t, _)| *t == ArrowPayloadType::ExpHistogramDataPoints)
        .map(|(_, rb)| rb.num_rows())
        .sum();
    if count > 0 {
        rejected.push(Rejection::UnsupportedMetricType {
            metric_type: "exponential_histogram",
            count,
        });
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
    Histogram {
        temporality: i32,
    },
    Summary,
    Unsupported,
}

struct MetricDecision {
    name: String,
    is_sum: bool,
    is_monotonic: bool,
}

/// A histogram metric's decision: only the sanitized name survives past
/// decision-building, since the temporality check happens once here (see
/// [`build_histogram_decisions`]), mirroring `ravel_otlp::normalize_metric`'s
/// single up-front check per `Metric` rather than per point.
struct HistogramDecision {
    name: String,
}

/// A summary metric's decision. Summaries carry no temporality field in
/// OTLP or OTAP, so unlike [`HistogramDecision`] there is nothing to check
/// beyond the name.
struct SummaryDecision {
    name: String,
}

/// One flattened `NUMBER_DATA_POINTS` row. `value` is `None` only when both
/// `double_value` and `int_value` are absent/null for this row: a decode
/// error unless the row is also flagged stale (otap-spec.md section 5.3.2:
/// the two value columns are alternatives, neither individually required;
/// a `NoRecordedValue` point never consults either, matching OTLP's
/// `build_point`).
struct FlatDp {
    id: u32,
    parent_id: u16,
    ts_ns: i64,
    value: Option<f64>,
    flags: u32,
}

/// One flattened `HISTOGRAM_DATA_POINTS` row. `bucket_counts` and
/// `explicit_bounds` are read from `List(UInt64)`/`List(Float64)` columns
/// (see [`list_u64_at`]/[`list_f64_at`]); a null list or null timestamp
/// reads as empty/zero rather than panicking, matching this crate's
/// no-panic-on-malformed-input contract even though this encoder's own test
/// data never emits either.
struct FlatHistogramDp {
    id: u32,
    parent_id: u16,
    ts_ns: i64,
    count: u64,
    sum: Option<f64>,
    bucket_counts: Vec<u64>,
    explicit_bounds: Vec<f64>,
    flags: u32,
    min: Option<f64>,
    max: Option<f64>,
}

/// One flattened `SUMMARY_DATA_POINTS` row. `quantiles` is read from the
/// `List(Struct{quantile: Float64, value: Float64})` column (see
/// [`quantiles_at`]).
struct FlatSummaryDp {
    id: u32,
    parent_id: u16,
    ts_ns: i64,
    count: u64,
    sum: f64,
    quantiles: Vec<(f64, f64)>,
    flags: u32,
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

fn dense_size_for(root_ids: &[Option<Vec<u16>>], parent_ids: impl Iterator<Item = u16>) -> usize {
    let mut max_id: u32 = 0;
    for ids in root_ids.iter().flatten() {
        for &v in ids {
            max_id = max_id.max(u32::from(v));
        }
    }
    for parent_id in parent_ids {
        max_id = max_id.max(u32::from(parent_id));
    }
    (max_id as usize + 1).min(MAX_METRIC_IDS)
}

/// Decode each root batch's `id` column (otap-spec.md section 5.3.1:
/// UNIVARIATE_METRICS.id, default DELTA) up front, once, so both
/// [`dense_size_for`] and [`build_root_table`] see decoded ids -- computing
/// the dense array's size from still-encoded (typically small, delta) values
/// would undersize it. `None` at an index means that batch's `id` column was
/// missing (pre-existing, silently skipped, matching [`build_root_table`]'s
/// prior behavior) or carried an unrecognized encoding declaration (freshly
/// rejected via [`UNRECOGNIZED_ID_ENCODING`]).
fn decode_root_ids(
    root_batches: &[&RecordBatch],
    rejected: &mut Vec<Rejection>,
) -> Vec<Option<Vec<u16>>> {
    root_batches
        .iter()
        .map(|rb| {
            let ids = column_as::<UInt16Array>(rb, "id")?;
            decode_id_column_u16(rb, "id", ids, rejected)
        })
        .collect()
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

fn build_root_table(
    root_batches: &[&RecordBatch],
    root_ids: &[Option<Vec<u16>>],
    dense_size: usize,
) -> Vec<Option<RootEntry>> {
    let mut entries: Vec<Option<RootEntry>> = (0..dense_size).map(|_| None).collect();
    for (rb, ids) in root_batches.iter().zip(root_ids) {
        let Some(ids) = ids else {
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

        for (i, &id) in ids.iter().enumerate() {
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
                METRIC_TYPE_HISTOGRAM => {
                    let temporality =
                        opt_i32(temporality_col, i).unwrap_or(AGGREGATION_TEMPORALITY_UNSPECIFIED);
                    RootKind::Histogram { temporality }
                }
                METRIC_TYPE_SUMMARY => RootKind::Summary,
                _ => RootKind::Unsupported,
            };
            if (id as usize) < entries.len() {
                entries[id as usize] = Some(RootEntry { name, kind });
            }
        }
    }
    entries
}

fn flatten_dp(dp_batches: &[&RecordBatch], rejected: &mut Vec<Rejection>) -> Vec<FlatDp> {
    let mut out = Vec::new();
    for rb in dp_batches {
        let ids = column_as::<UInt32Array>(rb, "id");
        let parents = column_as::<UInt16Array>(rb, "parent_id");
        let times = column_as::<TimestampNanosecondArray>(rb, "time_unix_nano");
        let doubles = column_as::<Float64Array>(rb, "double_value");
        let ints = column_as::<Int64Array>(rb, "int_value");
        let (Some(ids), Some(parents), Some(times)) = (ids, parents, times) else {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: MALFORMED_NUMBER_DATA_POINTS_BATCH,
                count: rb.num_rows(),
            });
            continue;
        };
        if doubles.is_none() && ints.is_none() {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: MALFORMED_NUMBER_DATA_POINTS_BATCH,
                count: rb.num_rows(),
            });
            continue;
        }
        // otap-spec.md section 5.3.2: both `id` and `parent_id` default to
        // DELTA when the field carries no explicit `encoding` metadata.
        let Some(decoded_ids) = decode_id_column_u32(rb, "id", ids, rejected) else {
            continue;
        };
        let Some(decoded_parents) = decode_id_column_u16(rb, "parent_id", parents, rejected) else {
            continue;
        };
        let flags_col = column_as::<UInt32Array>(rb, "flags");
        for i in 0..rb.num_rows() {
            let value = opt_f64(doubles, i).or_else(|| opt_i64(ints, i).map(|v| v as f64));
            out.push(FlatDp {
                id: decoded_ids[i],
                parent_id: decoded_parents[i],
                ts_ns: times.value(i),
                value,
                flags: flags_col.map_or(0, |c| c.value(i)),
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
                    // Mirror ravel_otlp::normalize_metric's order: the metric
                    // name is validated (MetricNameTooLong, then
                    // EmptyMetricName) before the Sum temporality check, so a
                    // delta Sum with a rejected name is classed on the name,
                    // not on temporality. Reversing this diverged the two
                    // ingest paths' rejection classes (ADR-0011 identical-
                    // rejection-class contract; a9-F01 mechanism a).
                    let Some(name) = process_metric_name(&entry.name, limits, count, rejected)
                    else {
                        continue;
                    };
                    if *temporality != AGGREGATION_TEMPORALITY_CUMULATIVE {
                        rejected.push(Rejection::UnsupportedTemporality { count });
                        continue;
                    }
                    decisions[id] = Some(MetricDecision {
                        name,
                        is_sum: true,
                        is_monotonic: *is_monotonic,
                    });
                }
                // A NUMBER_DATA_POINTS row whose parent metric id is
                // declared Histogram/Summary is a table/type mismatch: our
                // own encoder can't produce it, but a malformed producer
                // could, so it's defensive unknown classification, same as
                // `RootKind::Unsupported`.
                RootKind::Histogram { .. } | RootKind::Summary => unknown_count += count,
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

/// Render a decoded attribute cell to its label-value string, rejecting a
/// complex (array/map/bytes) value. Shared by [`check_attrs_in_input_order`]
/// and [`build_group`] so value formatting and the `ComplexAttributeValue`
/// rejection never drift between the ordered admission pass and the canonical
/// group build.
fn raw_cell_value(cell: &RawCell) -> Result<String, Rejection> {
    Ok(match cell {
        RawCell::Str(s) => s.clone(),
        RawCell::Bool(b) => b.to_string(),
        RawCell::Int(i) => i.to_string(),
        RawCell::Double(bits) => f64::from_bits(*bits).to_string(),
        RawCell::Complex => return Err(Rejection::ComplexAttributeValue),
    })
}

/// Apply the per-label admission checks to a point's attributes in input
/// order, returning the first violation. This mirrors the per-attribute loop
/// in `ravel_otlp::build_point` (complex-value check, then the sanitized-name
/// length check, then the value length check, first violation winning). The
/// columnar path groups attributes by a sorted key for series identity, so
/// this ordered pass is the only place the two paths' attribute order is
/// observable; keeping it identical is what preserves the ADR-0011 identical-
/// rejection-class contract (a9-F01 mechanism b). The length bounds here
/// intentionally match [`push_checked`], which the canonical build applies to
/// the sorted set; because a set that passes in input order also passes in any
/// order, the sorted build only ever surfaces the order-independent
/// `DuplicateLabelName` / `OversizedSeriesComponent` rejections.
fn check_attrs_in_input_order(
    raw: &[(String, RawCell)],
    limits: &IngestLimits,
) -> Result<(), Rejection> {
    for (raw_key, cell) in raw {
        let name = sanitize_label_name(raw_key);
        let value = raw_cell_value(cell)?;
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
    }
    Ok(())
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
        let value = raw_cell_value(cell)?;
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

// --- Histogram/summary explosion (ADR-0016 phase B2). Mirrors
// `ravel_otlp::normalize`'s private `explode_histogram`/`explode_summary`
// and their shared helpers exactly (see module docs); duplicated rather than
// reused since none of it is `pub` there. ---

/// Prometheus stale marker, duplicated from `ravel_otlp::normalize` (itself
/// duplicated from `ravel-promql`): an ingest crate must not depend on the
/// query-engine crate, or on a sibling ingest crate, for one constant.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN_BITS)
}

/// `DataPointFlags::NoRecordedValueMask` bit value (otel-arrow copies the
/// OTLP `flags` field verbatim as a plain `u32` column, so there is no
/// generated enum to reference here). Hardcoded rather than pulling in
/// `opentelemetry-proto` for one constant: that crate is a dev-dependency of
/// this one, and this small a value is cheaper to duplicate than to promote
/// a test-only dependency to production code.
const NO_RECORDED_VALUE_MASK: u32 = 1;

fn has_no_recorded_value(flags: u32) -> bool {
    flags & NO_RECORDED_VALUE_MASK != 0
}

/// Last-seen memo for `SeriesId::compute`, scoped to one metric id. Mirrors
/// `ravel_otlp::normalize`'s private `SeriesIdMemo` exactly: within one
/// metric, `tenant` and the metric name are constant, and the built
/// `LabelSet` fully determines the canonical `SeriesId` (ADR-0005), so an
/// equal `LabelSet` always yields a bit-identical id. One input data point
/// explodes into several series with different synthesized names/labels
/// (unlike the gauge/sum path's one-point-one-series shape), so this is
/// keyed per metric id via a `HashMap<u16, SeriesIdMemo>` at the call site
/// rather than shared across a whole batch.
struct SeriesIdMemo {
    last: Option<(LabelSet, SeriesId)>,
}

impl SeriesIdMemo {
    fn new() -> Self {
        SeriesIdMemo { last: None }
    }

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

/// Read a `List(UInt64)` column's `i`-th list, or empty for a null list or a
/// values array of an unexpected type (defensive: this crate's own test
/// encoder never emits either, but malformed input must not panic).
fn list_u64_at(list: &ListArray, i: usize) -> Vec<u64> {
    if list.is_null(i) {
        return Vec::new();
    }
    let value = list.value(i);
    match value.as_any().downcast_ref::<UInt64Array>() {
        Some(values) => values.values().to_vec(),
        None => Vec::new(),
    }
}

/// Read a `List(Float64)` column's `i`-th list; see [`list_u64_at`].
fn list_f64_at(list: &ListArray, i: usize) -> Vec<f64> {
    if list.is_null(i) {
        return Vec::new();
    }
    let value = list.value(i);
    match value.as_any().downcast_ref::<Float64Array>() {
        Some(values) => values.values().to_vec(),
        None => Vec::new(),
    }
}

/// Read a `List(Struct{quantile: Float64, value: Float64})` column's `i`-th
/// list of `(quantile, value)` pairs; see [`list_u64_at`].
fn quantiles_at(list: &ListArray, i: usize) -> Vec<(f64, f64)> {
    if list.is_null(i) {
        return Vec::new();
    }
    let value = list.value(i);
    let Some(s) = value.as_any().downcast_ref::<StructArray>() else {
        return Vec::new();
    };
    let Some(quantiles) = s
        .column_by_name("quantile")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
    else {
        return Vec::new();
    };
    let Some(values) = s
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
    else {
        return Vec::new();
    };
    (0..s.len())
        .map(|j| (quantiles.value(j), values.value(j)))
        .collect()
}

fn flatten_histogram_dp(
    dp_batches: &[&RecordBatch],
    rejected: &mut Vec<Rejection>,
) -> Vec<FlatHistogramDp> {
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
        let Some(counts) = column_as::<UInt64Array>(rb, "count") else {
            continue;
        };
        let Some(bucket_counts_col) = column_as::<ListArray>(rb, "bucket_counts") else {
            continue;
        };
        let Some(bounds_col) = column_as::<ListArray>(rb, "explicit_bounds") else {
            continue;
        };
        let sums = column_as::<Float64Array>(rb, "sum");
        let flags_col = column_as::<UInt32Array>(rb, "flags");
        let mins = column_as::<Float64Array>(rb, "min");
        let maxs = column_as::<Float64Array>(rb, "max");
        // otap-spec.md section 5.3.4: both `id` and `parent_id` default to
        // DELTA when the field carries no explicit `encoding` metadata.
        let Some(decoded_ids) = decode_id_column_u32(rb, "id", ids, rejected) else {
            continue;
        };
        let Some(decoded_parents) = decode_id_column_u16(rb, "parent_id", parents, rejected) else {
            continue;
        };
        for i in 0..rb.num_rows() {
            out.push(FlatHistogramDp {
                id: decoded_ids[i],
                parent_id: decoded_parents[i],
                ts_ns: times.value(i),
                count: counts.value(i),
                sum: opt_f64(sums, i),
                bucket_counts: list_u64_at(bucket_counts_col, i),
                explicit_bounds: list_f64_at(bounds_col, i),
                flags: flags_col.map_or(0, |c| c.value(i)),
                min: opt_f64(mins, i),
                max: opt_f64(maxs, i),
            });
        }
    }
    out
}

fn flatten_summary_dp(
    dp_batches: &[&RecordBatch],
    rejected: &mut Vec<Rejection>,
) -> Vec<FlatSummaryDp> {
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
        let Some(counts) = column_as::<UInt64Array>(rb, "count") else {
            continue;
        };
        let Some(sums) = column_as::<Float64Array>(rb, "sum") else {
            continue;
        };
        let Some(quantile_col) = column_as::<ListArray>(rb, "quantile") else {
            continue;
        };
        let flags_col = column_as::<UInt32Array>(rb, "flags");
        // otap-spec.md section 5.3.3: both `id` and `parent_id` default to
        // DELTA when the field carries no explicit `encoding` metadata.
        let Some(decoded_ids) = decode_id_column_u32(rb, "id", ids, rejected) else {
            continue;
        };
        let Some(decoded_parents) = decode_id_column_u16(rb, "parent_id", parents, rejected) else {
            continue;
        };
        for i in 0..rb.num_rows() {
            out.push(FlatSummaryDp {
                id: decoded_ids[i],
                parent_id: decoded_parents[i],
                ts_ns: times.value(i),
                count: counts.value(i),
                sum: sums.value(i),
                quantiles: quantiles_at(quantile_col, i),
                flags: flags_col.map_or(0, |c| c.value(i)),
            });
        }
    }
    out
}

/// Count rows by `parent_id` across a set of batches: used for the
/// `HISTOGRAM_DP_EXEMPLARS` table, whose only role here is giving an
/// exemplar count per histogram data-point id (OTAP carries exemplars in
/// their own table joined by `parent_id`; OTLP embeds them inline on the
/// data point as `dp.exemplars`).
fn count_by_parent_id(batches: &[&RecordBatch]) -> HashMap<u32, usize> {
    let mut counts = HashMap::new();
    for rb in batches {
        let Some(parents) = column_as::<UInt32Array>(rb, "parent_id") else {
            continue;
        };
        for i in 0..rb.num_rows() {
            *counts.entry(parents.value(i)).or_insert(0) += 1;
        }
    }
    counts
}

/// Per-metric total data-point count for Histogram roots, checked once per
/// metric id: mirrors `ravel_otlp::normalize_metric`'s Histogram arm, which
/// validates the (already up-front-checked) name then the cumulative-only
/// temporality requirement, name first.
fn build_histogram_decisions(
    root: &[Option<RootEntry>],
    flat_dp: &[FlatHistogramDp],
    limits: &IngestLimits,
    dense_size: usize,
    rejected: &mut Vec<Rejection>,
) -> (Vec<Option<HistogramDecision>>, usize) {
    let mut dp_count = vec![0u32; dense_size];
    for dp in flat_dp {
        if (dp.parent_id as usize) < dp_count.len() {
            dp_count[dp.parent_id as usize] += 1;
        }
    }

    let mut decisions: Vec<Option<HistogramDecision>> = (0..dense_size).map(|_| None).collect();
    let mut unknown_count = 0usize;

    for id in 0..dense_size {
        let count = dp_count[id] as usize;
        if count == 0 {
            continue;
        }
        match root.get(id).and_then(|e| e.as_ref()) {
            None => unknown_count += count,
            Some(entry) => match &entry.kind {
                RootKind::Histogram { temporality } => {
                    let Some(name) = process_metric_name(&entry.name, limits, count, rejected)
                    else {
                        continue;
                    };
                    if *temporality != AGGREGATION_TEMPORALITY_CUMULATIVE {
                        rejected.push(Rejection::UnsupportedTemporality { count });
                        continue;
                    }
                    decisions[id] = Some(HistogramDecision { name });
                }
                _ => unknown_count += count,
            },
        }
    }

    (decisions, unknown_count)
}

/// Per-metric total data-point count for Summary roots, checked once per
/// metric id: mirrors `ravel_otlp::normalize_metric`'s Summary arm, which
/// has only the up-front name check (summaries carry no temporality field).
fn build_summary_decisions(
    root: &[Option<RootEntry>],
    flat_dp: &[FlatSummaryDp],
    limits: &IngestLimits,
    dense_size: usize,
    rejected: &mut Vec<Rejection>,
) -> (Vec<Option<SummaryDecision>>, usize) {
    let mut dp_count = vec![0u32; dense_size];
    for dp in flat_dp {
        if (dp.parent_id as usize) < dp_count.len() {
            dp_count[dp.parent_id as usize] += 1;
        }
    }

    let mut decisions: Vec<Option<SummaryDecision>> = (0..dense_size).map(|_| None).collect();
    let mut unknown_count = 0usize;

    for id in 0..dense_size {
        let count = dp_count[id] as usize;
        if count == 0 {
            continue;
        }
        match root.get(id).and_then(|e| e.as_ref()) {
            None => unknown_count += count,
            Some(entry) => match &entry.kind {
                RootKind::Summary => {
                    if let Some(name) = process_metric_name(&entry.name, limits, count, rejected) {
                        decisions[id] = Some(SummaryDecision { name });
                    }
                }
                _ => unknown_count += count,
            },
        }
    }

    (decisions, unknown_count)
}

/// Validate and resolve one data point's event timestamp exactly like
/// `ravel_otlp::normalize`'s private `checked_event_ts`, except `ts_ns` is
/// already `i64` here (read from an Arrow `TimestampNanosecondArray`, not a
/// wire `u64`), so there is no `i64::try_from` saturation step.
fn checked_event_ts(
    ts_ns: i64,
    ingest_ts_ns: i64,
    limits: &IngestLimits,
) -> Result<i64, Rejection> {
    if ts_ns == 0 {
        return Err(Rejection::ZeroTimestamp);
    }
    let skew_ns = ts_ns.saturating_sub(ingest_ts_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(Rejection::FutureSkew {
            skew_ns,
            max_ns: limits.max_future_skew_ns,
        });
    }
    let lag_ns = ingest_ts_ns.saturating_sub(ts_ns);
    if lag_ns > limits.max_ingest_lag_ns {
        return Err(Rejection::TooOld {
            lag_ns,
            max_ns: limits.max_ingest_lag_ns,
        });
    }
    Ok(ts_ns)
}

/// Build the attribute-derived label prefix shared by every series exploded
/// from one histogram/summary data point (everything but the `__name__`
/// label and the synthesized `le`/`quantile` label, which differ per
/// series). Mirrors `ravel_otlp::normalize`'s private
/// `build_explode_base_labels`, minus the resource-label prefix: as the
/// module docs explain, this crate's test encoder never emits
/// `RESOURCE_ATTRS`/`SCOPE_ATTRS`, so every point here already carries an
/// empty resource label set.
fn build_base_labels(
    range: &[u32],
    attrs: &[FlatAttr],
    limits: &IngestLimits,
) -> Result<Vec<Label>, Rejection> {
    if range.len() > limits.max_attributes_per_point {
        return Err(Rejection::TooManyAttributes {
            attribute_count: range.len(),
            max: limits.max_attributes_per_point,
        });
    }
    let mut labels = Vec::with_capacity(range.len());
    for &i in range {
        let a = &attrs[i as usize];
        let name = sanitize_label_name(a.key);
        let value = raw_cell_value(&raw_cell(a))?;
        push_checked(&mut labels, name, value, limits)?;
    }
    Ok(labels)
}

/// Finish one exploded series: attach `__name__` and an optional `le`/
/// `quantile` label to the shared base labels, then build the label set and
/// resolve its series id. Mirrors `ravel_otlp::normalize`'s private
/// `finish_point` exactly.
#[allow(clippy::too_many_arguments)]
fn finish_exploded_point(
    tenant: &TenantId,
    memo: &mut SeriesIdMemo,
    base_labels: &[Label],
    metric_name: &str,
    extra_label: Option<(&'static str, String)>,
    ts_ns: i64,
    value: f64,
    limits: &IngestLimits,
) -> Result<NormalizedPoint, Rejection> {
    let mut labels = Vec::with_capacity(base_labels.len() + 2);
    labels.extend_from_slice(base_labels);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric_name.to_string(),
    });
    if let Some((name, value)) = extra_label {
        push_checked(&mut labels, name.to_string(), value, limits)?;
    }

    let label_set = LabelSet::new(labels).map_err(|err| match err {
        TypeError::DuplicateLabelName(name) => Rejection::DuplicateLabelName(name),
        _ => Rejection::DuplicateLabelName(String::new()),
    })?;

    let series_id = memo
        .series_id(tenant, metric_name, &label_set)
        .map_err(|_| Rejection::OversizedSeriesComponent)?;

    Ok(NormalizedPoint {
        series_id,
        labels: label_set,
        sample: Sample { ts_ns, value },
        is_monotonic_sum: false,
    })
}

/// Explode one `HISTOGRAM_DATA_POINTS` row into its Prometheus-convention
/// series: one `{name}_bucket{le=<bound>}` per explicit bound plus
/// `{name}_bucket{le="+Inf"}` (= the point's count), `{name}_sum` when `sum`
/// is present, and `{name}_count`. Mirrors `ravel_otlp::normalize`'s private
/// `explode_histogram` exactly, including its check order (event timestamp
/// first, then bucket-count/bound structural checks, then attribute admission)
/// and its atomic-rejection contract: an `Err` means none of this point's
/// series were admitted, never a partial set (ADR-0016). `Ok` also carries
/// zero-weight informational rejections for dropped min/max/exemplar fields,
/// since the point itself was admitted; `exemplar_count` comes from
/// [`count_by_parent_id`] over the `HISTOGRAM_DP_EXEMPLARS` table rather than
/// an inline field, since OTAP carries exemplars in their own table.
#[allow(clippy::too_many_arguments)]
fn explode_histogram_point(
    tenant: &TenantId,
    metric_name: &str,
    dp: &FlatHistogramDp,
    range: &[u32],
    attrs: &[FlatAttr],
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_count: usize,
    memo: &mut SeriesIdMemo,
) -> Result<(Vec<NormalizedPoint>, Vec<Rejection>), Rejection> {
    let event_ts_ns = checked_event_ts(dp.ts_ns, ingest_ts_ns, limits)?;

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

    let base_labels = build_base_labels(range, attrs, limits)?;
    let stale = has_no_recorded_value(dp.flags);
    let bucket_name = format!("{metric_name}_bucket");

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
        series.push(finish_exploded_point(
            tenant,
            memo,
            &base_labels,
            &bucket_name,
            Some(("le", format_float(*bound))),
            event_ts_ns,
            value,
            limits,
        )?);
    }

    // The +Inf bucket and _count both use the point's raw count directly,
    // not the accumulated bucket sum (matches the collector's mapping and
    // ravel_otlp::explode_histogram).
    let count_value = if stale { stale_nan() } else { dp.count as f64 };
    series.push(finish_exploded_point(
        tenant,
        memo,
        &base_labels,
        &bucket_name,
        Some(("le", "+Inf".to_string())),
        event_ts_ns,
        count_value,
        limits,
    )?);

    if let Some(sum) = dp.sum {
        let sum_value = if stale { stale_nan() } else { sum };
        let sum_name = format!("{metric_name}_sum");
        series.push(finish_exploded_point(
            tenant,
            memo,
            &base_labels,
            &sum_name,
            None,
            event_ts_ns,
            sum_value,
            limits,
        )?);
    }

    let count_name = format!("{metric_name}_count");
    series.push(finish_exploded_point(
        tenant,
        memo,
        &base_labels,
        &count_name,
        None,
        event_ts_ns,
        count_value,
        limits,
    )?);

    let mut informational = Vec::new();
    if dp.min.is_some() || dp.max.is_some() {
        informational.push(Rejection::HistogramMinMaxDropped { count: 1 });
    }
    if exemplar_count > 0 {
        informational.push(Rejection::HistogramExemplarsDropped {
            count: exemplar_count,
        });
    }

    Ok((series, informational))
}

/// Explode one `SUMMARY_DATA_POINTS` row into its Prometheus-convention
/// series: one `{name}{quantile=<q>}` per quantile plus `{name}_sum` and
/// `{name}_count`. Mirrors `ravel_otlp::normalize`'s private
/// `explode_summary` exactly, including its check order (event timestamp
/// first, then per-quantile finite/duplicate checks, then attribute
/// admission) and its atomic-rejection contract, same as
/// [`explode_histogram_point`].
#[allow(clippy::too_many_arguments)]
fn explode_summary_point(
    tenant: &TenantId,
    metric_name: &str,
    dp: &FlatSummaryDp,
    range: &[u32],
    attrs: &[FlatAttr],
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    memo: &mut SeriesIdMemo,
) -> Result<Vec<NormalizedPoint>, Rejection> {
    let event_ts_ns = checked_event_ts(dp.ts_ns, ingest_ts_ns, limits)?;

    let mut seen_quantiles = std::collections::HashSet::with_capacity(dp.quantiles.len());
    for (quantile, _) in &dp.quantiles {
        if !quantile.is_finite() {
            return Err(Rejection::NonFiniteQuantile);
        }
        if !seen_quantiles.insert(quantile.to_bits()) {
            return Err(Rejection::DuplicateQuantile);
        }
    }

    let base_labels = build_base_labels(range, attrs, limits)?;
    let stale = has_no_recorded_value(dp.flags);

    let mut series = Vec::with_capacity(dp.quantiles.len() + 2);
    for (quantile, value) in &dp.quantiles {
        let v = if stale { stale_nan() } else { *value };
        series.push(finish_exploded_point(
            tenant,
            memo,
            &base_labels,
            metric_name,
            Some(("quantile", format_float(*quantile))),
            event_ts_ns,
            v,
            limits,
        )?);
    }

    let sum_value = if stale { stale_nan() } else { dp.sum };
    let sum_name = format!("{metric_name}_sum");
    series.push(finish_exploded_point(
        tenant,
        memo,
        &base_labels,
        &sum_name,
        None,
        event_ts_ns,
        sum_value,
        limits,
    )?);

    let count_value = if stale { stale_nan() } else { dp.count as f64 };
    let count_name = format!("{metric_name}_count");
    series.push(finish_exploded_point(
        tenant,
        memo,
        &base_labels,
        &count_name,
        None,
        event_ts_ns,
        count_value,
        limits,
    )?);

    Ok(series)
}
