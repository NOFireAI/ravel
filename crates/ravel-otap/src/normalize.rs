//! Columnar normalization of decoded OTAP METRICS payloads into Ravel's
//! canonical metric point representation.
//!
//! This mirrors `ravel_otlp::normalize::normalize_metrics` point-for-point:
//! same admission limits (`ravel_otlp::IngestLimits`), same sanitization,
//! same [`Rejection`] classes, same `is_monotonic_sum` semantics, and the same
//! ADR-0085 Decision 2 name-suffix pass, applied through the pub
//! `ravel_otlp::normalize::prometheus_family_name` (called directly, like
//! `format_float`, rather than re-implemented) so the METRICS `unit` column
//! and monotonic flag drive the same `foo_bytes_total` name on both paths. The
//! differential gate (tests/differential.rs) asserts the two paths produce
//! identical `SeriesId` sets, identical `(series, ts, value)` samples, and
//! identical rejection classes for the same logical input. Where
//! `ravel_otlp::normalize` exposes a helper as `pub`, we call it directly;
//! where a helper is private (sanitization, `push_checked`,
//! `any_value_to_label_value`), we mirror it exactly below. The OTel unit
//! mapping is one such shared helper: the metadata unit word is taken from the
//! canonical `ravel_otlp::normalize::metadata_unit_word` rather than re-derived
//! here, so a single edit to that table changes both ingest paths at once.
//!
//! Resource labels (ADR-2708 D4): the METRICS table's `resource.id` joins
//! each metric to its `RESOURCE_ATTRS` rows, and the resource's label prefix
//! (`job`, `instance`, allowlisted attributes) is built by
//! [`ravel_otlp::admit_resource_labels`], the same function the OTLP path
//! calls, so one resource yields the same prefix, the same dropped count and
//! the same rejections on both paths. The prefix is built once per resource
//! id per batch. A metric whose `resource` is null gets an empty prefix, as
//! OTLP's `resource: None` does; `RESOURCE_ATTRS` rows no metric references
//! are ignored, and `SCOPE_ATTRS` is not read (OTLP builds no label from
//! scope attributes either). A resource that cannot be read is rejected,
//! never read as no resource. A METRICS `resource` column of the wrong type
//! rejects every point; a `RESOURCE_ATTRS` table whose `parent_id`, `key`,
//! `type`, `str`, `bool`, `int` or `double` column is present and cannot be
//! read, or that has a null `parent_id` slot, rejects every point under a
//! resource. Both count as `malformed_resource`. An unrecognized encoding on
//! `RESOURCE_ATTRS.parent_id` or on the METRICS `resource.id` child rejects
//! every point under a resource as `unrecognized_id_encoding`.
//!
//! One count differs from OTLP's: a resource's point count, which its
//! rejections carry and which decides whether it is admitted at all, covers
//! number, histogram and summary points only. Exponential-histogram points
//! are rejected on their own as an unsupported type, where OTLP counts them
//! under their resource too; so a resource whose only points are exponential
//! histograms is not admitted and its dropped attributes are not counted.
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
//! series per distinct attribute set. OTAP carries exemplars in separate
//! `NUMBER_DP_EXEMPLARS` and `HISTOGRAM_DP_EXEMPLARS` tables (OTLP embeds
//! them inline on the data point), with their attributes in the matching
//! `*_DP_EXEMPLAR_ATTRS` table below each, so both are joined by id here
//! rather than read from a field. The two tables have identical columns, so
//! one decoder and one admission path serve both.
//! Since ADR-0047 they are carried, not just counted: each is offered to the
//! caller's [`ravel_types::ExemplarCap`] and attaches to the bucket series
//! whose `le` bound is the smallest at or above its value for a histogram, or
//! to the data point's own series for a gauge or sum, which is the same rule
//! the OTLP path applies in each case, and the `HistogramExemplarsDropped`
//! counter now reports what the cap turned away. OTAP-carried exponential
//! histograms keep rejecting typed via [`push_unsupported_type_rejections`]
//! (ADR-0017 is a separate, later ticket), and the exemplars they carry are
//! counted as dropped by [`push_exp_histogram_exemplar_drops`], which also
//! records the attachment rule waiting for them.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Float64Array, Int32Array,
    Int64Array, ListArray, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{UInt8Type, UInt16Type};
use ravel_otlp::label_projection::{
    RESOLVED_LABEL_OVERHEAD_BYTES, explode_name_label_bytes, histogram_point_bytes, label_bytes,
    name_label_bytes, resource_value_label_len, summary_point_bytes,
};
use ravel_otlp::metadata::{MetricKind as PromMetricKind, MetricMetadata};
use ravel_otlp::normalize::{
    MetricsNormalizeResult, NormalizedExemplar, metadata_unit_word, prometheus_family_name,
};
use ravel_otlp::promcompat::format_float;
use ravel_otlp::{
    IngestLimits, NormalizeOutput, NormalizedPoint, ProjectedLabelBytes, Rejection,
    ResourceAttrValue, admit_resource_labels, resource_prefix_bytes,
};
use ravel_types::{
    Exemplar, ExemplarCap, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId,
    TypeError,
};

use crate::proto::experimental::arrow::v1::ArrowPayloadType;
use crate::stream::DecodedBatch;

/// AnyValue `type` discriminant, matching otap-spec.md section 5.5.1's
/// mapping table exactly (0=empty, 1=str, 2=int, 3=double, 4=bool, 5=map,
/// 6=slice, 7=bytes). This is a wire contract: `normalize_decoded` reads
/// these values out of real decoded OTAP batches, so they must be the
/// spec's discriminants, not an internal convention. These deliberately do
/// not follow OTLP's `AnyValue` oneof field numbers (2=bool, 3=int, 4=double,
/// 5=bytes, 6=array, 7=map), which disagree with the OTAP spec on every slot
/// from 2 upward: ordering by those would misclassify real attributes (e.g. a
/// spec `int`, type 2, read as bool).
pub const ANY_VALUE_TYPE_EMPTY: u8 = 0;
pub const ANY_VALUE_TYPE_STRING: u8 = 1;
pub const ANY_VALUE_TYPE_INT: u8 = 2;
pub const ANY_VALUE_TYPE_DOUBLE: u8 = 3;
pub const ANY_VALUE_TYPE_BOOL: u8 = 4;
pub const ANY_VALUE_TYPE_MAP: u8 = 5;
pub const ANY_VALUE_TYPE_SLICE: u8 = 6;
pub const ANY_VALUE_TYPE_BYTES: u8 = 7;

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

/// otap-spec.md section 6.4: the three `id`/`parent_id` transport encodings
/// this pass understands. `Delta` is the default and only non-`Plain`
/// encoding allowed on the core metric/data-point id chain (section 6.4.2);
/// `QuasiDelta` (section 6.4.3) is the default for the `*Attrs.parent_id` and
/// `*DpExemplars.parent_id` columns and is decoded only there (a `quasidelta`
/// declaration on the core chain stays a typed rejection, see [`id_encoding`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IdEncoding {
    Plain,
    Delta,
    QuasiDelta,
}

/// Tag for a whole batch whose `id`/`parent_id` field metadata declares an
/// `encoding` value this pass does not recognize as valid for that column:
/// anything other than `"plain"`/`"delta"` on the core id chain (including
/// `"quasidelta"`, which is valid only on the `*Attrs`/`*DpExemplars`
/// columns), or anything other than `"plain"`/`"delta"`/`"quasidelta"` on
/// those columns. Rejected outright via the same counted mechanism as
/// [`MALFORMED_NUMBER_DATA_POINTS_BATCH`], never guessed.
const UNRECOGNIZED_ID_ENCODING: &str = "unrecognized_id_encoding";

/// Tag for the points of metrics whose resource cannot be read: a METRICS
/// `resource` column or `id` child of the wrong type, or a `RESOURCE_ATTRS`
/// batch missing its `parent_id`, `key` or `type` column, holding a null
/// `parent_id` slot, or carrying a `str`, `bool`, `int` or `double` column of
/// a type this pass cannot read. Such points are rejected, never written
/// without their resource labels.
const MALFORMED_RESOURCE: &str = "malformed_resource";

/// Resolve the declared encoding of a core `id`/`parent_id` column (the
/// metric/data-point chain) from its Arrow field metadata
/// (otap-spec.md section 6.4: key `"encoding"`). No declaration defaults to
/// [`IdEncoding::Delta`], the spec's stated default for these columns
/// (sections 5.3.1-5.3.4). Only `"plain"` and `"delta"` are valid here:
/// `"quasidelta"` is a valid encoding for the `*Attrs`/`*DpExemplars`
/// columns (see [`quasi_delta_default_encoding`]), never for this chain, so
/// it maps to `Err` and the caller rejects it rather than guessing. `Err`
/// also means the column is missing (caller already checked for this).
fn id_encoding(rb: &RecordBatch, column: &str) -> Result<IdEncoding, ()> {
    let field = rb.schema_ref().field_with_name(column).map_err(|_| ())?;
    field_id_encoding(field)
}

/// [`id_encoding`] for a field already in hand, such as the `id` child of the
/// METRICS `resource` struct, whose encoding metadata sits on the child.
fn field_id_encoding(field: &arrow::datatypes::Field) -> Result<IdEncoding, ()> {
    match field.metadata().get("encoding").map(String::as_str) {
        None => Ok(IdEncoding::Delta),
        Some("plain") => Ok(IdEncoding::Plain),
        Some("delta") => Ok(IdEncoding::Delta),
        Some(_) => Err(()),
    }
}

/// Resolve the declared encoding of a `*Attrs.parent_id` or
/// `*DpExemplars.parent_id` column, whose spec default is QUASI-DELTA
/// (otap-spec.md section 6.4.3), unlike the DELTA default [`id_encoding`]
/// applies to the core id chain. Accepts `"plain"`, `"delta"`, and
/// `"quasidelta"`; anything else is `Err` (rejected, never guessed). `Err`
/// also means the column is missing (caller already checked for this).
fn quasi_delta_default_encoding(rb: &RecordBatch, column: &str) -> Result<IdEncoding, ()> {
    let field = rb.schema_ref().field_with_name(column).map_err(|_| ())?;
    match field.metadata().get("encoding").map(String::as_str) {
        None => Ok(IdEncoding::QuasiDelta),
        Some("plain") => Ok(IdEncoding::Plain),
        Some("delta") => Ok(IdEncoding::Delta),
        Some("quasidelta") => Ok(IdEncoding::QuasiDelta),
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
        // `id_encoding` never yields `QuasiDelta` for the core chain (it maps
        // a `quasidelta` declaration to `Err`); this arm keeps the match
        // exhaustive and rejects it explicitly, never guessing an encoding.
        Ok(IdEncoding::QuasiDelta) | Err(()) => {
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
        // See [`decode_id_column_u32`]: `QuasiDelta` is unreachable here but
        // kept exhaustive and rejected, never guessed.
        Ok(IdEncoding::QuasiDelta) | Err(()) => {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: UNRECOGNIZED_ID_ENCODING,
                count: rb.num_rows(),
            });
            None
        }
    }
}

/// Equality-column value for one `*Attrs` row: the Active Field value
/// selected by the row's `type` discriminant (otap-spec.md section 5.5.1).
/// Doubles compare by bit pattern, matching this crate's float-in-dedup-paths
/// convention. Only scalar-valued types produce a variant; empty/complex
/// types (which have no populated scalar column here) make the row ineligible
/// for delta encoding entirely (see [`decode_quasi_delta_attrs`]).
#[derive(Clone, Copy, PartialEq)]
enum AttrEqValue<'b> {
    Str(&'b str),
    Bool(bool),
    Int(i64),
    Double(u64),
}

/// Decode a QUASI-DELTA `*Attrs.parent_id` column (otap-spec.md section
/// 6.4.3): each row's `parent_id` is a delta from the previous row when that
/// row's equality columns -- `type`, `key`, and the type's Active Field value
/// -- all match, and an absolute value otherwise. Rows whose type has no
/// scalar Active Field (a null `type` discriminant, empty, and the complex
/// Bytes/Array/Map arms this crate reads no value column for) or whose active
/// value is null are never delta-encoded and always read absolute, matching the
/// spec's "Map and Slice ... and null values" exceptions. The running sum wraps rather than panics
/// on adversarial input, matching [`decode_delta_u32`]; a null `parent_id`
/// carries the accumulator through unchanged, as delta decoding does.
fn decode_quasi_delta_attrs(rb: &RecordBatch, encoded: &ParentIds<'_>) -> Vec<u32> {
    let types = column_as::<UInt8Array>(rb, "type");
    let keys = StrCol::from_column(rb, "key");
    let strs = StrCol::from_column(rb, "str");
    let bools = column_as::<BooleanArray>(rb, "bool");
    let ints = IntCol::from_column(rb, "int");
    let doubles = column_as::<Float64Array>(rb, "double");

    // The row's (type, key, Active Field value), or `None` when the row is
    // ineligible for delta encoding (missing key/type column, null key, a
    // non-scalar type, or a null active value).
    let eq_key = |i: usize| -> Option<(u8, &str, AttrEqValue)> {
        let key = keys.as_ref()?.get(i)?;
        // A null `type` discriminant has no in-contract Active Field, so the
        // row is ineligible for delta encoding and reads absolute, exactly like
        // the null-active-value scalar arms below and the non-scalar arm. Read
        // through the null-checking `opt_u8` (same as the sibling `str`/`bool`/
        // `int`/`double` value columns), never `types.value(i)` directly: a
        // malformed producer can unset the discriminant's validity bit while
        // leaving a scalar-type byte in the buffer, and without this guard the
        // run-continuation decision would read that stale byte instead of
        // taking the "no scalar Active Field -> always absolute" fallback this
        // function's doc comment promises. `opt_u8` also yields `None` when the
        // `type` column is absent entirely (the prior `let types = types?`).
        let ty = opt_u8(types, i)?;
        let value = match ty {
            ANY_VALUE_TYPE_STRING => AttrEqValue::Str(strs.as_ref()?.get(i)?),
            ANY_VALUE_TYPE_BOOL => AttrEqValue::Bool(opt_bool(bools, i)?),
            ANY_VALUE_TYPE_INT => AttrEqValue::Int(ints.as_ref()?.get(i)?),
            ANY_VALUE_TYPE_DOUBLE => AttrEqValue::Double(opt_f64(doubles, i)?.to_bits()),
            _ => return None,
        };
        Some((ty, key, value))
    };

    let mut acc: u32 = 0;
    let mut prev: Option<(u8, &str, AttrEqValue)> = None;
    let mut out = Vec::with_capacity(encoded.len());
    for i in 0..encoded.len() {
        let key = eq_key(i);
        let Some(value) = encoded.get(i) else {
            out.push(acc);
            prev = key;
            continue;
        };
        match (prev, key) {
            (Some(p), Some(k)) if p == k => acc = encoded.wrapping_add(acc, value),
            _ => acc = value,
        }
        out.push(acc);
        prev = key;
    }
    out
}

/// A `*Attrs.parent_id` column in any shape otap-spec.md section 5.4 allows:
/// `UInt16` for the u16-parent tables (5.4.2, `RESOURCE_ATTRS` among them),
/// and `UInt32`, plain or dictionary-encoded with `UInt8`/`UInt16` keys, for
/// the u32-parent tables (5.4.1). Delta and quasi-delta running sums wrap at
/// the column's own width.
enum ParentIds<'b> {
    U16(&'b UInt16Array),
    U32(&'b UInt32Array),
    U32Dict8(&'b DictionaryArray<UInt8Type>, &'b UInt32Array),
    U32Dict16(&'b DictionaryArray<UInt16Type>, &'b UInt32Array),
}

impl<'b> ParentIds<'b> {
    fn from_column(rb: &'b RecordBatch, name: &str) -> Option<Self> {
        let col = rb.column_by_name(name)?.as_any();
        if let Some(a) = col.downcast_ref::<UInt16Array>() {
            return Some(Self::U16(a));
        }
        if let Some(a) = col.downcast_ref::<UInt32Array>() {
            return Some(Self::U32(a));
        }
        if let Some(d) = col.downcast_ref::<DictionaryArray<UInt8Type>>() {
            let values = d.values().as_any().downcast_ref::<UInt32Array>()?;
            return Some(Self::U32Dict8(d, values));
        }
        if let Some(d) = col.downcast_ref::<DictionaryArray<UInt16Type>>() {
            let values = d.values().as_any().downcast_ref::<UInt32Array>()?;
            return Some(Self::U32Dict16(d, values));
        }
        None
    }

    fn len(&self) -> usize {
        match self {
            Self::U16(a) => a.len(),
            Self::U32(a) => a.len(),
            Self::U32Dict8(d, _) => d.len(),
            Self::U32Dict16(d, _) => d.len(),
        }
    }

    /// The encoded value at row `i`, or `None` for a null slot (a dictionary
    /// key that is null or out of range included).
    fn get(&self, i: usize) -> Option<u32> {
        match self {
            Self::U16(a) => (!a.is_null(i)).then(|| u32::from(a.value(i))),
            Self::U32(a) => (!a.is_null(i)).then(|| a.value(i)),
            Self::U32Dict8(d, values) => dict_value(d.key(i), values),
            Self::U32Dict16(d, values) => dict_value(d.key(i), values),
        }
    }

    /// The PLAIN-encoded value at row `i`: the buffer value for a primitive
    /// column, as the decode always read it, and 0 for a null dictionary slot.
    fn plain(&self, i: usize) -> u32 {
        match self {
            Self::U16(a) => u32::from(a.values()[i]),
            Self::U32(a) => a.values()[i],
            Self::U32Dict8(..) | Self::U32Dict16(..) => self.get(i).unwrap_or(0),
        }
    }

    fn wrapping_add(&self, acc: u32, value: u32) -> u32 {
        match self {
            Self::U16(_) => u32::from((acc as u16).wrapping_add(value as u16)),
            _ => acc.wrapping_add(value),
        }
    }

    fn decode_plain(&self) -> Vec<u32> {
        (0..self.len()).map(|i| self.plain(i)).collect()
    }

    /// Whether any slot is null, or a dictionary key is out of range: a plain
    /// decode would read the buffer value or parent 0 there, and a delta or
    /// quasi-delta decode the previous row's parent.
    fn has_null_slot(&self) -> bool {
        (0..self.len()).any(|i| self.get(i).is_none())
    }

    /// DELTA decode (otap-spec.md section 6.4.2), as [`decode_delta_u32`].
    fn decode_delta(&self) -> Vec<u32> {
        let mut acc: u32 = 0;
        (0..self.len())
            .map(|i| {
                if let Some(value) = self.get(i) {
                    acc = self.wrapping_add(acc, value);
                }
                acc
            })
            .collect()
    }
}

fn dict_value(key: Option<usize>, values: &UInt32Array) -> Option<u32> {
    let k = key?;
    (k < values.len() && !values.is_null(k)).then(|| values.value(k))
}

/// A string column of an `*Attrs` table, plain or dictionary-encoded with
/// `UInt8`/`UInt16` keys (otap-spec.md section 5.4: `key` may be either,
/// `str` is a `UInt16` dictionary by default). Also the METRICS `name`,
/// `unit` and `description` columns.
enum StrCol<'b> {
    Plain(&'b StringArray),
    Dict8(&'b DictionaryArray<UInt8Type>, &'b StringArray),
    Dict16(&'b DictionaryArray<UInt16Type>, &'b StringArray),
}

impl<'b> StrCol<'b> {
    fn from_array(col: &'b dyn Array) -> Option<Self> {
        let col = col.as_any();
        if let Some(a) = col.downcast_ref::<StringArray>() {
            return Some(Self::Plain(a));
        }
        if let Some(d) = col.downcast_ref::<DictionaryArray<UInt8Type>>() {
            let values = d.values().as_any().downcast_ref::<StringArray>()?;
            return Some(Self::Dict8(d, values));
        }
        if let Some(d) = col.downcast_ref::<DictionaryArray<UInt16Type>>() {
            let values = d.values().as_any().downcast_ref::<StringArray>()?;
            return Some(Self::Dict16(d, values));
        }
        None
    }

    fn from_column(rb: &'b RecordBatch, name: &str) -> Option<Self> {
        Self::from_array(rb.column_by_name(name)?.as_ref())
    }

    /// The string at row `i`, or `None` for a null slot or a dictionary key
    /// that is null or out of range.
    fn get(&self, i: usize) -> Option<&'b str> {
        let (values, idx) = match self {
            Self::Plain(a) => (*a, Some(i)),
            Self::Dict8(d, values) => (*values, d.key(i)),
            Self::Dict16(d, values) => (*values, d.key(i)),
        };
        let idx = idx?;
        (idx < values.len() && !values.is_null(idx)).then(|| values.value(idx))
    }
}

/// The `int` column of an `*Attrs` table, plain or a `UInt8` or `UInt16`
/// dictionary. Section 5.4 lists `UInt16` keys as the default, and section 4.3
/// lets an encoder start with smaller keys and widen them on overflow, as
/// [`StrCol`] accepts for `str`.
enum IntCol<'b> {
    Plain(&'b Int64Array),
    Dict8(&'b DictionaryArray<UInt8Type>, &'b Int64Array),
    Dict16(&'b DictionaryArray<UInt16Type>, &'b Int64Array),
}

impl<'b> IntCol<'b> {
    fn from_column(rb: &'b RecordBatch, name: &str) -> Option<Self> {
        let col = rb.column_by_name(name)?.as_any();
        if let Some(a) = col.downcast_ref::<Int64Array>() {
            return Some(Self::Plain(a));
        }
        if let Some(d) = col.downcast_ref::<DictionaryArray<UInt8Type>>() {
            let values = d.values().as_any().downcast_ref::<Int64Array>()?;
            return Some(Self::Dict8(d, values));
        }
        let d = col.downcast_ref::<DictionaryArray<UInt16Type>>()?;
        let values = d.values().as_any().downcast_ref::<Int64Array>()?;
        Some(Self::Dict16(d, values))
    }

    fn get(&self, i: usize) -> Option<i64> {
        let (values, idx) = match self {
            Self::Plain(a) => (*a, Some(i)),
            Self::Dict8(d, values) => (*values, d.key(i)),
            Self::Dict16(d, values) => (*values, d.key(i)),
        };
        let idx = idx?;
        (idx < values.len() && !values.is_null(idx)).then(|| values.value(idx))
    }
}

/// Decode a QUASI-DELTA `*DpExemplars.parent_id` column (otap-spec.md section
/// 6.4.3): the equality columns for exemplar tables are `int_value` and
/// `double_value`, so a row's `parent_id` is a delta from the previous row
/// when both of those match the previous row, and absolute otherwise. Doubles
/// compare by bit pattern; a missing equality column reads as absent for
/// every row (all rows then share one equality key), matching the spec's
/// "if the column ... is not present, the value ... is empty" rule. Wraps
/// rather than panics, and carries a null `parent_id` through unchanged, like
/// [`decode_quasi_delta_attrs`].
fn decode_quasi_delta_exemplars(rb: &RecordBatch, encoded: &ParentIds<'_>) -> Vec<u32> {
    let ints = column_as::<Int64Array>(rb, "int_value");
    let doubles = column_as::<Float64Array>(rb, "double_value");

    let mut acc: u32 = 0;
    let mut prev: Option<(Option<i64>, Option<u64>)> = None;
    let mut out = Vec::with_capacity(encoded.len());
    for i in 0..encoded.len() {
        let key = (opt_i64(ints, i), opt_f64(doubles, i).map(f64::to_bits));
        let Some(value) = encoded.get(i) else {
            out.push(acc);
            prev = Some(key);
            continue;
        };
        match prev {
            Some(p) if p == key => acc = encoded.wrapping_add(acc, value),
            _ => acc = value,
        }
        out.push(acc);
        prev = Some(key);
    }
    out
}

/// Resolve a `*Attrs`/`*DpExemplars` `parent_id` column's encoding (default
/// QUASI-DELTA) and decode it into absolute values. `quasi` decodes the
/// QUASI-DELTA case with that table's equality columns
/// ([`decode_quasi_delta_attrs`] or [`decode_quasi_delta_exemplars`]). Pushes
/// [`UNRECOGNIZED_ID_ENCODING`] (tagged with `rb`'s row count) and returns
/// `None` if the declaration is not recognized, never guessing.
fn decode_quasi_delta_column(
    rb: &RecordBatch,
    encoded: &ParentIds<'_>,
    rejected: &mut Vec<Rejection>,
    quasi: impl Fn(&RecordBatch, &ParentIds<'_>) -> Vec<u32>,
) -> Option<Vec<u32>> {
    match quasi_delta_default_encoding(rb, "parent_id") {
        Ok(IdEncoding::Plain) => Some(encoded.decode_plain()),
        Ok(IdEncoding::Delta) => Some(encoded.decode_delta()),
        Ok(IdEncoding::QuasiDelta) => Some(quasi(rb, encoded)),
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
/// `BatchArrowRecords` message (`batch.batch_id`'s payloads), discarding any
/// exemplars it carried.
///
/// Mirrors [`ravel_otlp::normalize_metrics`]'s contract: nothing here
/// panics on malformed or oversized input, and every unsupported or
/// rejected data point is accounted for in `NormalizeOutput::rejected`,
/// never silently dropped. Kept with its original signature for existing
/// callers, as a thin wrapper around [`normalize_decoded_with_exemplars`] with
/// a throwaway, batch-scoped [`ExemplarCap`], so the reported
/// [`Rejection::HistogramExemplarsDropped`] count is cap-based rather than
/// "every exemplar, always" (exactly how `ravel_otlp::normalize_metrics` wraps
/// its own exemplar-carrying twin).
///
/// This wrapper has nowhere to store the exemplars the cap admits, so it
/// discards them; each discarded admission is counted into
/// [`Rejection::HistogramExemplarsDropped`] (ADR-0047 decision 2: an exemplar
/// that is not stored is dropped and counted, never silent), on top of the
/// ones the cap itself turned away.
pub fn normalize_decoded(
    tenant: &TenantId,
    batch: &DecodedBatch,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
) -> NormalizeOutput {
    let mut cap = ExemplarCap::new(limits.exemplar_cap_window_ns);
    let result = normalize_decoded_with_exemplars(tenant, batch, limits, ingest_ts_ns, &mut cap);
    let mut output = result.output;
    // The cap admitted these, but this entry point drops them (nothing stores
    // exemplars on the OTAP path yet). A discarded admission is still a dropped
    // exemplar and must be counted, or an operator watching the points-dropped
    // counter sees the number fall while the same data is still being dropped.
    if !result.exemplars.is_empty() {
        output.rejected.push(Rejection::HistogramExemplarsDropped {
            count: result.exemplars.len(),
        });
    }
    output
}

/// Decode and normalize one decoded OTAP `BatchArrowRecords` message,
/// admitting exemplars through `exemplar_cap` (ADR-0047 decisions 1 and 2).
///
/// `exemplar_cap` is `&mut` and caller-owned rather than built here: a
/// per-series-per-window cap only means something across many requests over
/// wall-clock time, so whoever holds the long-lived per-shard state owns one
/// `ExemplarCap` and passes it into every call that shard makes.
///
/// See [`normalize_decoded`] for the panic and error-handling contract, which
/// is identical here.
pub fn normalize_decoded_with_exemplars(
    tenant: &TenantId,
    batch: &DecodedBatch,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
) -> MetricsNormalizeResult {
    normalize_impl(
        tenant,
        prepare_batch(batch, limits),
        limits,
        ingest_ts_ns,
        exemplar_cap,
        false,
    )
    .0
}

/// Normalize exactly like [`normalize_decoded_with_exemplars`], and additionally
/// surface one [`MetricMetadata`] per metric family that produced at least one
/// point (ADR-0085 Decision 1). The returned [`MetricsNormalizeResult`] is what
/// [`normalize_decoded_with_exemplars`] returns for the same input, unchanged;
/// the metadata is the only addition.
///
/// This mirrors `ravel_otlp::normalize::normalize_metrics_with_metadata` field
/// for field, since the two surfaces must store the same tuple for the same
/// logical metric:
///
/// - `family_name` is the Decision 2 suffixed name (unit word and `_total`
///   applied) *before* the classic histogram/summary explosion appends
///   `_bucket`/`_sum`/`_count`, i.e. exactly the decision name the exploded
///   series are derived from.
/// - `kind` comes from the METRICS `metric_type` column and the `is_monotonic`
///   flag, the OTAP equivalent of OTLP's `data` oneof: a monotonic Sum is a
///   Counter, a non-monotonic Sum and a Gauge are both Gauge.
/// - `help` is the METRICS `description` column, empty when the producer omits
///   it (OTLP's `Metric.description`).
/// - `unit` is the mapped Prometheus word (`bytes`, `seconds`), the raw unit
///   when it maps to nothing, or empty, matching the word the name was suffixed
///   with.
///
/// Metadata is deduplicated by `family_name`, first write wins. A metric whose
/// every point was rejected contributes nothing: metadata describes families
/// Ravel actually stored points for.
pub fn normalize_decoded_with_metadata(
    tenant: &TenantId,
    batch: &DecodedBatch,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
) -> (MetricsNormalizeResult, Vec<MetricMetadata>) {
    normalize_impl(
        tenant,
        prepare_batch(batch, limits),
        limits,
        ingest_ts_ns,
        exemplar_cap,
        true,
    )
}

/// [`normalize_decoded_with_metadata`] for a batch already prepared by
/// [`prepare_batch`], for a caller that charged its
/// [`PreparedBatch::projected_label_bytes`] to a budget before normalizing
/// (ADR-2708 D2): the batch is flattened and resolved once for both.
pub fn normalize_prepared_with_metadata(
    tenant: &TenantId,
    prepared: PreparedBatch<'_>,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
) -> (MetricsNormalizeResult, Vec<MetricMetadata>) {
    normalize_impl(tenant, prepared, limits, ingest_ts_ns, exemplar_cap, true)
}

/// One batch flattened, resolved and projected, with no label built yet.
///
/// The point bounds and the resolved-label projection (ADR-2708 D2) need the
/// batch flattened and every metric resolved, so [`prepare_batch`] does that
/// once and the normalize reuses it. Resource prefixes are planned here from
/// value lengths and built only after the label-byte bound passes, so a batch
/// over the bound is rejected before any label exists.
pub struct PreparedBatch<'b> {
    batch: &'b DecodedBatch,
    stage: Stage<'b>,
}

enum Stage<'b> {
    /// A point bound rejects the whole request; nothing was resolved.
    Rejected(Rejection),
    Resolved(Box<Resolved<'b>>),
}

struct Resolved<'b> {
    total_points: usize,
    rejected: Vec<Rejection>,
    flat: FlatPoints<'b>,
    tables: MetricTables<'b>,
    orders: AttrOrders,
    projected: usize,
}

/// Each data-point attribute table's row indices sorted by `parent_id`.
struct AttrOrders {
    number: Vec<u32>,
    hist: Vec<u32>,
    summary: Vec<u32>,
}

impl PreparedBatch<'_> {
    /// The resolved-label bytes normalizing this batch would build; zero when
    /// a point bound already rejects it whole, since then nothing is built.
    pub fn projected_label_bytes(&self) -> ProjectedLabelBytes {
        match &self.stage {
            Stage::Rejected(_) => ProjectedLabelBytes::from_walk(0),
            Stage::Resolved(r) => ProjectedLabelBytes::from_walk(r.projected),
        }
    }
}

/// Flatten `batch`, check its two point bounds, resolve every metric and its
/// resource, and project the label bytes the normalize would build. Allocates
/// no label: resource prefixes are only planned here.
pub fn prepare_batch<'b>(batch: &'b DecodedBatch, limits: &IngestLimits) -> PreparedBatch<'b> {
    let total_points = count_total_points(batch);
    if total_points > limits.max_data_points_per_request {
        return PreparedBatch {
            batch,
            stage: Stage::Rejected(Rejection::TooManyDataPoints {
                count: total_points,
                max: limits.max_data_points_per_request,
            }),
        };
    }

    let mut rejected = Vec::new();
    push_unsupported_type_rejections(batch, &mut rejected);
    push_exp_histogram_exemplar_drops(batch, &mut rejected);

    let flat = flatten_points(batch, &mut rejected);

    // Second bound, on what the request expands to rather than on what it
    // carries on the wire, mirroring `ravel_otlp::normalize_impl`. One classic
    // histogram or summary data point becomes one point per bucket or quantile
    // plus its `_sum`/`_count` (ADR-0016), each copying the whole base label
    // set, so a request well under `max_data_points_per_request` in wire points
    // can still expand into an arbitrary number of allocated ones. Unlike OTLP,
    // whose protobuf is decoded before its wire-count check, OTAP learns the
    // per-point bound counts only after flattening the columnar payloads, so
    // this sits here rather than beside the wire-count check above; it still
    // runs before any per-point series is built (the explode loops below).
    //
    // The exemplar payloads are decoded BELOW this check, not above it, and the
    // rejection still counts them. They were decoded above it once, which made
    // rejecting here drop them uncounted while OTLP's twin counted them at both
    // of its early returns: the ADR-0047 decision 2 hazard, a dropped-data
    // counter reading zero while data is lost. Moving the decode below keeps
    // the bound-before-allocate principle the per-point cap uses, and
    // `dropped_exemplar_rows` restores the count from the columnar row counts,
    // which is metadata rather than a decode. Parity with OTLP is a property
    // here, not a documented divergence.
    let exploded_points = count_exploded_points(&flat.dp, &flat.hist_dp, &flat.summary_dp);
    if exploded_points > limits.max_data_points_per_request {
        return PreparedBatch {
            batch,
            stage: Stage::Rejected(Rejection::TooManyExplodedPoints {
                exploded: exploded_points,
                count: total_points,
                max: limits.max_data_points_per_request,
            }),
        };
    }

    let tables = resolve_metrics(batch, &flat, limits, &mut rejected);
    let orders = AttrOrders {
        number: sort_attrs_by_parent(&flat.attrs),
        hist: sort_attrs_by_parent(&flat.hist_attrs),
        summary: sort_attrs_by_parent(&flat.summary_attrs),
    };
    let projected = project_flat(&flat, &tables, &orders, limits);
    PreparedBatch {
        batch,
        stage: Stage::Resolved(Box::new(Resolved {
            total_points,
            rejected,
            flat,
            tables,
            orders,
            projected,
        })),
    }
}

/// Shared body of the exemplar-carrying entry points. `collect_metadata` is
/// true only for the metadata entry points; the points, exemplars, and
/// rejections it produces do not depend on it, so the older entry points are
/// behaviorally unchanged.
fn normalize_impl(
    tenant: &TenantId,
    prepared: PreparedBatch<'_>,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplar_cap: &mut ExemplarCap,
    collect_metadata: bool,
) -> (MetricsNormalizeResult, Vec<MetricMetadata>) {
    let PreparedBatch { batch, stage } = prepared;
    let resolved = match stage {
        Stage::Rejected(reason) => {
            return whole_request_rejection(reason, dropped_exemplar_rows(batch));
        }
        Stage::Resolved(resolved) => *resolved,
    };
    let Resolved {
        total_points,
        mut rejected,
        flat,
        mut tables,
        orders,
        projected,
    } = resolved;

    // Third bound, on the label bytes the points would carry (ADR-2708 D2),
    // mirroring `ravel_otlp::normalize_impl`: checked before any label exists,
    // resource prefixes included, and before the exemplar payloads are
    // decoded, for the reason the exploded check in [`prepare_batch`] gives.
    if projected > limits.max_resolved_label_bytes_per_request {
        return whole_request_rejection(
            Rejection::ResolvedLabelBytesExceeded {
                projected,
                max: limits.max_resolved_label_bytes_per_request,
                count: total_points,
            },
            dropped_exemplar_rows(batch),
        );
    }
    tables.resources.build(limits, &mut rejected);

    let FlatPoints {
        dp: flat_dp,
        attrs: flat_attrs,
        hist_dp: flat_hist_dp,
        hist_attrs: flat_hist_attrs,
        summary_dp: flat_summary_dp,
        summary_attrs: flat_summary_attrs,
    } = &flat;
    let hist_exemplar_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::HistogramDpExemplars);
    let hist_exemplar_attr_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::HistogramDpExemplarAttrs);
    let number_exemplar_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::NumberDpExemplars);
    let number_exemplar_attr_batches: Vec<&RecordBatch> =
        payloads_of(batch, ArrowPayloadType::NumberDpExemplarAttrs);

    let hist_exemplar_rows = group_exemplars_by_parent_id(&hist_exemplar_batches, &mut rejected);
    let flat_hist_exemplar_attrs = flatten_attrs(&hist_exemplar_attr_batches, &mut rejected);
    let number_exemplar_rows =
        group_exemplars_by_parent_id(&number_exemplar_batches, &mut rejected);
    let flat_number_exemplar_attrs = flatten_attrs(&number_exemplar_attr_batches, &mut rejected);

    let MetricTables {
        root,
        resources,
        metric_decision,
        hist_decision,
        summary_decision,
    } = &tables;

    // Which metric ids actually contributed a point. Read only by the metadata
    // build below (a family whose every point was rejected describes nothing),
    // and one bool write per admitted point regardless, so the metadata-free
    // entry points pay nothing measurable for it.
    let mut produced = vec![false; root.len()];

    let mut points = Vec::with_capacity(flat_dp.len());
    let mut group_cache = GroupCache::default();
    let mut exemplars = Vec::new();
    let number_exemplar_attr_order = sort_attrs_by_parent(&flat_number_exemplar_attrs);
    let mut number_exemplar_admission = ExemplarAdmission {
        rows: &number_exemplar_rows,
        attrs: &flat_number_exemplar_attrs,
        attr_order: &number_exemplar_attr_order,
        cap: exemplar_cap,
        out: &mut exemplars,
    };

    for dp in flat_dp {
        let Some(decision) = metric_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };
        // `None` only when the resource's build was rejected after planning,
        // which counted these points already.
        let Some(prefix) = resources.prefix_for(root, dp.parent_id) else {
            continue;
        };

        let range = attr_range_for(flat_attrs, &orders.number, dp.id);
        if range.len() > limits.max_attributes_per_point {
            rejected.push(Rejection::TooManyAttributes {
                attribute_count: range.len(),
                max: limits.max_attributes_per_point,
            });
            continue;
        }
        group_cache.enter_run(dp.parent_id, range, flat_attrs);

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

        // Per-label admission (complex value, then sanitized-name length,
        // then value length) runs in the attributes' input order with the
        // first violation winning, exactly as ravel_otlp::build_point checks a
        // data point's attributes in wire order. Attribute order is not part
        // of series identity, so the canonical grouping key below is sorted;
        // this pre-check is the only place attribute order is observable, and
        // it must line up with the OTLP oracle or the two paths class the same
        // input differently (ADR-0011).
        if let Err(rejection) = check_flat_attrs_in_input_order(range, flat_attrs, limits) {
            rejected.push(rejection);
            continue;
        }

        // One metric id belongs to exactly one resource (its root row's
        // `resource.id`), so a per-metric cache is already per resource.
        let outcome = group_cache.resolve(dp.parent_id, range, flat_attrs, |key| {
            build_group(key, &decision.name, prefix, tenant, limits)
        });

        match outcome {
            Ok((label_set, series_id)) => {
                let series_id = *series_id;
                points.push(NormalizedPoint {
                    series_id,
                    // ADR-0098: ravel-otap keeps its own built-set memo and
                    // shares nothing across points; it wraps each built set in
                    // a fresh Arc. Rekeying it is a follow-up.
                    labels: Arc::new(label_set.clone()),
                    sample: Sample {
                        ts_ns: dp.ts_ns,
                        value,
                    },
                    is_monotonic_sum: decision.is_sum && decision.is_monotonic,
                });
                if let Some(slot) = produced.get_mut(dp.parent_id as usize) {
                    *slot = true;
                }
                // A number data point has no buckets, so its exemplars attach
                // to the point's own series: there is no `le` bound to resolve
                // This is the same rule `ravel_otlp::build_point`
                // applies to a gauge or sum exemplar, which is what keeps the
                // two paths' exemplar decisions identical (ADR-0011).
                number_exemplar_admission.admit(dp.id, limits, |_| series_id, &mut rejected);
            }
            Err(rejection) => rejected.push(rejection.clone()),
        }
    }
    // Rows whose `parent_id` matches no NUMBER_DATA_POINTS row at all: no
    // series to attach to and no point rejection accounting for them, so
    // without this they would vanish uncounted.
    number_exemplar_admission.count_orphans(flat_dp.iter().map(|dp| dp.id), &mut rejected);
    #[cfg(test)]
    held::record(group_cache.held_bytes() + resources.held_bytes());
    drop(group_cache);

    let hist_exemplar_attr_order = sort_attrs_by_parent(&flat_hist_exemplar_attrs);
    let mut exemplar_admission = ExemplarAdmission {
        rows: &hist_exemplar_rows,
        attrs: &flat_hist_exemplar_attrs,
        attr_order: &hist_exemplar_attr_order,
        cap: exemplar_cap,
        out: &mut exemplars,
    };
    let mut hist_memos: HashMap<u16, SeriesIdMemo> = HashMap::new();
    for dp in flat_hist_dp {
        let Some(decision) = hist_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };
        let Some(prefix) = resources.prefix_for(root, dp.parent_id) else {
            continue;
        };
        let range = attr_range_for(flat_hist_attrs, &orders.hist, dp.id);
        let memo = hist_memos
            .entry(dp.parent_id)
            .or_insert_with(SeriesIdMemo::new);
        match explode_histogram_point(
            tenant,
            &decision.name,
            prefix,
            dp,
            range,
            flat_hist_attrs,
            limits,
            ingest_ts_ns,
            &mut exemplar_admission,
            memo,
        ) {
            Ok((mut new_points, informational)) => {
                if !new_points.is_empty()
                    && let Some(slot) = produced.get_mut(dp.parent_id as usize)
                {
                    *slot = true;
                }
                points.append(&mut new_points);
                rejected.extend(informational);
            }
            Err(rejection) => rejected.push(rejection),
        }
    }
    exemplar_admission.count_orphans(flat_hist_dp.iter().map(|dp| dp.id), &mut rejected);
    #[cfg(test)]
    held::record(resources.held_bytes() + SeriesIdMemo::held_bytes(hist_memos.values()));

    let mut summary_memos: HashMap<u16, SeriesIdMemo> = HashMap::new();
    for dp in flat_summary_dp {
        let Some(decision) = summary_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };
        let Some(prefix) = resources.prefix_for(root, dp.parent_id) else {
            continue;
        };
        let range = attr_range_for(flat_summary_attrs, &orders.summary, dp.id);
        let memo = summary_memos
            .entry(dp.parent_id)
            .or_insert_with(SeriesIdMemo::new);
        match explode_summary_point(
            tenant,
            &decision.name,
            prefix,
            dp,
            range,
            flat_summary_attrs,
            limits,
            ingest_ts_ns,
            memo,
        ) {
            Ok(mut new_points) => {
                if !new_points.is_empty()
                    && let Some(slot) = produced.get_mut(dp.parent_id as usize)
                {
                    *slot = true;
                }
                points.append(&mut new_points);
            }
            Err(rejection) => rejected.push(rejection),
        }
    }
    #[cfg(test)]
    held::record(
        resources.held_bytes()
            + SeriesIdMemo::held_bytes(hist_memos.values().chain(summary_memos.values())),
    );

    let metadata = if collect_metadata {
        build_metadata(
            root,
            metric_decision,
            hist_decision,
            summary_decision,
            &produced,
        )
    } else {
        Vec::new()
    };

    (
        MetricsNormalizeResult {
            output: NormalizeOutput {
                points,
                // Always empty: OTAP admits only scalar points (see the early
                // return above).
                histogram_points: Vec::new(),
                rejected,
            },
            exemplars,
        },
        metadata,
    )
}

/// Upper bound on the resolved-label bytes [`normalize_decoded`] and its
/// siblings would build for `batch` (ADR-2708 D2), the OTAP twin of
/// [`ravel_otlp::label_projection::project_resolved_label_bytes`]: per admitted
/// resource, its label prefix once; per number point, the label set it builds
/// (unless its attributes equal the previous counted point's under the same
/// metric) and the copy every admitted point carries; per classic histogram or
/// summary point, every exploded series, and per such metric, the series its
/// [`SeriesIdMemo`] copies. Built from the same per-label helpers,
/// so the charge per label cannot drift between the formats.
///
/// Builds no label: it decodes the batch's payloads and reads value lengths.
/// Zero when a point bound already rejects the batch whole. A caller that
/// normalizes afterwards should use [`prepare_batch`] instead, which keeps the
/// decode for the normalize.
pub fn project_resolved_label_bytes(
    batch: &DecodedBatch,
    limits: &IngestLimits,
) -> ProjectedLabelBytes {
    prepare_batch(batch, limits).projected_label_bytes()
}

/// The flattened data-point and attribute rows of one batch, decoded once and
/// shared by the projection and the build.
struct FlatPoints<'b> {
    dp: Vec<FlatDp>,
    attrs: Vec<FlatAttr<'b>>,
    hist_dp: Vec<FlatHistogramDp>,
    hist_attrs: Vec<FlatAttr<'b>>,
    summary_dp: Vec<FlatSummaryDp>,
    summary_attrs: Vec<FlatAttr<'b>>,
}

fn flatten_points<'b>(batch: &'b DecodedBatch, rejected: &mut Vec<Rejection>) -> FlatPoints<'b> {
    FlatPoints {
        dp: flatten_dp(
            &payloads_of(batch, ArrowPayloadType::NumberDataPoints),
            rejected,
        ),
        attrs: flatten_attrs(
            &payloads_of(batch, ArrowPayloadType::NumberDpAttrs),
            rejected,
        ),
        hist_dp: flatten_histogram_dp(
            &payloads_of(batch, ArrowPayloadType::HistogramDataPoints),
            rejected,
        ),
        hist_attrs: flatten_attrs(
            &payloads_of(batch, ArrowPayloadType::HistogramDpAttrs),
            rejected,
        ),
        summary_dp: flatten_summary_dp(
            &payloads_of(batch, ArrowPayloadType::SummaryDataPoints),
            rejected,
        ),
        summary_attrs: flatten_attrs(
            &payloads_of(batch, ArrowPayloadType::SummaryDpAttrs),
            rejected,
        ),
    }
}

/// Everything resolved per metric id before any point is built: the root
/// table, the planned resource prefixes, and the three decision tables.
struct MetricTables<'b> {
    root: Vec<Option<RootEntry>>,
    resources: ResourcePrefixes<'b>,
    metric_decision: Vec<Option<MetricDecision>>,
    hist_decision: Vec<Option<HistogramDecision>>,
    summary_decision: Vec<Option<SummaryDecision>>,
}

fn resolve_metrics<'b>(
    batch: &'b DecodedBatch,
    flat: &FlatPoints<'_>,
    limits: &IngestLimits,
    rejected: &mut Vec<Rejection>,
) -> MetricTables<'b> {
    let root_batches = payloads_of(batch, ArrowPayloadType::UnivariateMetrics);
    let root_ids = decode_root_ids(&root_batches, rejected);
    let dense_size = dense_size_for(
        &root_ids,
        flat.dp
            .iter()
            .map(|d| d.parent_id)
            .chain(flat.hist_dp.iter().map(|d| d.parent_id))
            .chain(flat.summary_dp.iter().map(|d| d.parent_id)),
    );
    let root = build_root_table(&root_batches, &root_ids, dense_size);

    let resource_attrs =
        flatten_resource_attrs(&payloads_of(batch, ArrowPayloadType::ResourceAttrs));
    let resources = plan_resources(&root, resource_attrs, flat, limits, rejected);
    let blocked = &resources.blocked;

    let (metric_decision, unknown_number_count) =
        build_metric_decisions(&root, &flat.dp, limits, dense_size, blocked, rejected);
    let (hist_decision, unknown_hist_count) =
        build_histogram_decisions(&root, &flat.hist_dp, limits, dense_size, blocked, rejected);
    let (summary_decision, unknown_summary_count) = build_summary_decisions(
        &root,
        &flat.summary_dp,
        limits,
        dense_size,
        blocked,
        rejected,
    );
    let unknown_metric_count = unknown_number_count + unknown_hist_count + unknown_summary_count;
    if unknown_metric_count > 0 {
        rejected.push(Rejection::UnsupportedMetricType {
            metric_type: UNKNOWN_OTAP_METRIC_TYPE,
            count: unknown_metric_count,
        });
    }

    MetricTables {
        root,
        resources,
        metric_decision,
        hist_decision,
        summary_decision,
    }
}

/// The resource label prefixes of one batch, keyed by the batch-scoped
/// `resource.id` (ADR-2708 D4), and which metric ids sit under a rejected
/// resource. [`plan_resources`] admits each resource from value lengths;
/// [`ResourcePrefixes::build`] builds the labels once the label-byte bound
/// has passed.
struct ResourcePrefixes<'b> {
    /// The `RESOURCE_ATTRS` rows, and their indices sorted by `parent_id`.
    attrs: Vec<FlatAttr<'b>>,
    order: Vec<u32>,
    /// Resources admitted by the plan, in id order.
    planned: BTreeMap<u16, PlannedResource>,
    /// Built prefixes, empty until [`Self::build`].
    prefixes: HashMap<u16, Vec<Label>>,
    /// Indexed by metric id: true when the metric's resource was rejected, so
    /// its points are already accounted for and must not be built or counted
    /// again.
    blocked: Vec<bool>,
}

struct PlannedResource {
    /// Number, histogram, and summary points under the resource.
    points: usize,
    /// The prefix's projected bytes.
    bytes: usize,
    /// Where in the rejection list the build's rejections belong: where an
    /// admission at plan time would have pushed them.
    rejection_at: usize,
}

impl<'b> ResourcePrefixes<'b> {
    /// The label prefix every point of `metric_id` starts with: empty for a
    /// metric with no resource, `None` when its resource has no built prefix.
    fn prefix_for(&self, root: &[Option<RootEntry>], metric_id: u16) -> Option<&[Label]> {
        match resource_of(root, metric_id) {
            ResourceRef::Absent => Some(&[]),
            ResourceRef::Id(id) => self.prefixes.get(&id).map(Vec::as_slice),
            ResourceRef::Unreadable(_) => None,
        }
    }

    fn prefix_bytes_for(&self, root: &[Option<RootEntry>], metric_id: u16) -> usize {
        match resource_of(root, metric_id) {
            ResourceRef::Id(id) => self.planned.get(&id).map_or(0, |p| p.bytes),
            ResourceRef::Absent | ResourceRef::Unreadable(_) => 0,
        }
    }

    /// Build every planned prefix with [`admit_resource_labels`], the function
    /// OTLP calls per `ResourceMetrics`, placing its rejections where the plan
    /// recorded them so the rejection order is the eager build's.
    fn build(&mut self, limits: &IngestLimits, rejected: &mut Vec<Rejection>) {
        let mut prefixes = HashMap::with_capacity(self.planned.len());
        let mut inserts: Vec<(usize, Vec<Rejection>)> = Vec::new();
        for (&resource_id, plan) in &self.planned {
            let pairs = resource_pairs(&self.attrs, &self.order, resource_id);
            let mut scratch = Vec::new();
            if let Some(labels) = admit_resource_labels(&pairs, plan.points, limits, &mut scratch) {
                prefixes.insert(resource_id, labels);
            }
            if !scratch.is_empty() {
                inserts.push((plan.rejection_at, scratch));
            }
        }
        self.prefixes = prefixes;
        // `rejection_at` never decreases in id order, so inserting from the
        // last one leaves every earlier index valid.
        for (at, rejections) in inserts.into_iter().rev() {
            let at = at.min(rejected.len());
            rejected.splice(at..at, rejections);
        }
    }

    /// Heap bytes the built prefixes hold.
    #[cfg(test)]
    fn held_bytes(&self) -> usize {
        self.prefixes
            .values()
            .map(|labels| held::labels(labels.capacity(), labels.iter()))
            .sum()
    }
}

fn resource_of(root: &[Option<RootEntry>], metric_id: u16) -> ResourceRef {
    root.get(metric_id as usize)
        .and_then(|e| e.as_ref())
        .map_or(ResourceRef::Absent, |e| e.resource)
}

/// One resource's attributes as the resource-label builder reads them: its
/// `RESOURCE_ATTRS` rows in table order.
fn resource_pairs<'b>(
    attrs: &[FlatAttr<'b>],
    order: &[u32],
    resource_id: u16,
) -> Vec<(&'b str, ResourceAttrValue<'b>)> {
    attr_range_for(attrs, order, u32::from(resource_id))
        .iter()
        .map(|&i| {
            let a = &attrs[i as usize];
            (a.key, resource_attr_value(a))
        })
        .collect()
}

/// The rejection [`ravel_otlp::build_resource_labels_from`] returns for
/// `attrs`, decided from value lengths without building a label: the same
/// reads in the same order (`service.name`, `service.namespace`, the joined
/// `job`, `service.instance.id`, then the allowlist), each checked as its
/// `push_checked` checks it.
fn check_resource_labels(
    attrs: &[(&str, ResourceAttrValue<'_>)],
    limits: &IngestLimits,
) -> Result<(), Rejection> {
    let first_len = |key: &str| -> Result<Option<usize>, Rejection> {
        match attrs.iter().find(|(k, _)| *k == key) {
            None => Ok(None),
            Some((_, value)) => resource_value_label_len(*value)
                .map(Some)
                .ok_or(Rejection::ComplexAttributeValue),
        }
    };
    let name = first_len("service.name")?;
    let namespace = first_len("service.namespace")?;
    if let Some(name) = name {
        let job = match namespace {
            Some(ns) if ns > 0 => ns.saturating_add(1).saturating_add(name),
            _ => name,
        };
        check_label_len("job".len(), job, limits)?;
    }
    if let Some(instance) = first_len("service.instance.id")? {
        check_label_len("instance".len(), instance, limits)?;
    }
    for key in &limits.resource_attribute_allowlist {
        if matches!(
            key.as_str(),
            "service.name" | "service.namespace" | "service.instance.id"
        ) {
            continue;
        }
        if let Some(len) = first_len(key)? {
            check_label_len(sanitized_label_name_len(key), len, limits)?;
        }
    }
    Ok(())
}

/// The length checks [`push_checked`] applies to a label of these lengths.
fn check_label_len(
    name_len: usize,
    value_len: usize,
    limits: &IngestLimits,
) -> Result<(), Rejection> {
    if value_len == 0 {
        return Ok(());
    }
    if name_len > limits.max_label_name_len {
        return Err(Rejection::LabelNameTooLong {
            len: name_len,
            max: limits.max_label_name_len,
        });
    }
    if value_len > limits.max_label_value_len {
        return Err(Rejection::LabelValueTooLong {
            len: value_len,
            max: limits.max_label_value_len,
        });
    }
    Ok(())
}

/// Length of [`sanitize_label_name`]'s output: every character it keeps is
/// ASCII and every one it replaces becomes `_`.
fn sanitized_label_name_len(name: &str) -> usize {
    name.chars().count()
}

/// Admit every resource a metric with points refers to with the checks
/// [`admit_resource_labels`] applies, which OTLP calls per `ResourceMetrics`,
/// decided from value lengths so no label is built before the label-byte
/// bound. One resource yields the same prefix, the same dropped count, and the
/// same rejection on both paths. A resource's point count is the number,
/// histogram, and summary points of every metric under it. Its attributes are
/// its `RESOURCE_ATTRS` rows in table order; rows whose `parent_id` no metric
/// refers to are ignored.
///
/// A metric whose resource cannot be read is rejected, never read as having
/// no resource: an unreadable `resource.id` rejects its metric's points, and
/// an unreadable or unrecognized `RESOURCE_ATTRS` table rejects every point
/// that refers to a resource.
fn plan_resources<'b>(
    root: &[Option<RootEntry>],
    resource_attrs: Result<Vec<FlatAttr<'b>>, &'static str>,
    flat: &FlatPoints<'_>,
    limits: &IngestLimits,
    rejected: &mut Vec<Rejection>,
) -> ResourcePrefixes<'b> {
    let mut metric_points = vec![0usize; root.len()];
    let parents = flat
        .dp
        .iter()
        .map(|d| d.parent_id)
        .chain(flat.hist_dp.iter().map(|d| d.parent_id))
        .chain(flat.summary_dp.iter().map(|d| d.parent_id));
    for parent_id in parents {
        if let Some(n) = metric_points.get_mut(parent_id as usize) {
            *n += 1;
        }
    }
    let resources: Vec<ResourceRef> = root
        .iter()
        .map(|e| e.as_ref().map_or(ResourceRef::Absent, |e| e.resource))
        .collect();
    let mut blocked = vec![false; root.len()];

    for tag in [MALFORMED_RESOURCE, UNRECOGNIZED_ID_ENCODING] {
        let count: usize = resources
            .iter()
            .zip(&metric_points)
            .filter(|(r, _)| **r == ResourceRef::Unreadable(tag))
            .map(|(_, &n)| n)
            .sum();
        if count > 0 {
            rejected.push(Rejection::UnsupportedMetricType {
                metric_type: tag,
                count,
            });
        }
    }
    for (b, r) in blocked.iter_mut().zip(&resources) {
        *b |= matches!(r, ResourceRef::Unreadable(_));
    }

    let attrs = match resource_attrs {
        Ok(attrs) => attrs,
        Err(tag) => {
            let mut count = 0usize;
            for ((b, r), &n) in blocked.iter_mut().zip(&resources).zip(&metric_points) {
                if matches!(r, ResourceRef::Id(_)) {
                    *b = true;
                    count += n;
                }
            }
            if count > 0 {
                rejected.push(Rejection::UnsupportedMetricType {
                    metric_type: tag,
                    count,
                });
            }
            return ResourcePrefixes {
                attrs: Vec::new(),
                order: Vec::new(),
                planned: BTreeMap::new(),
                prefixes: HashMap::new(),
                blocked,
            };
        }
    };

    let mut resource_points: BTreeMap<u16, usize> = BTreeMap::new();
    for (r, &points) in resources.iter().zip(&metric_points) {
        if let ResourceRef::Id(resource_id) = *r {
            *resource_points.entry(resource_id).or_default() += points;
        }
    }

    let order = sort_attrs_by_parent(&attrs);
    let mut planned = BTreeMap::new();
    let mut rejected_resources = HashSet::new();
    for (&resource_id, &points) in &resource_points {
        if points == 0 {
            continue;
        }
        let pairs = resource_pairs(&attrs, &order, resource_id);
        if pairs.len() > limits.max_resource_attributes {
            rejected.push(Rejection::TooManyResourceAttributes {
                count: points,
                max: limits.max_resource_attributes,
            });
            rejected_resources.insert(resource_id);
        } else if let Err(reason) = check_resource_labels(&pairs, limits) {
            rejected.push(Rejection::Grouped {
                reason: Box::new(reason),
                count: points,
            });
            rejected_resources.insert(resource_id);
        } else {
            planned.insert(
                resource_id,
                PlannedResource {
                    points,
                    bytes: resource_prefix_bytes(&pairs, limits).unwrap_or(0),
                    rejection_at: rejected.len(),
                },
            );
        }
    }

    for (b, r) in blocked.iter_mut().zip(&resources) {
        if let ResourceRef::Id(id) = r {
            *b |= rejected_resources.contains(id);
        }
    }
    ResourcePrefixes {
        attrs,
        order,
        planned,
        prefixes: HashMap::new(),
        blocked,
    }
}

/// One attribute row as the resource-label builder reads it. Agrees with
/// [`raw_cell`] on every type, including a null or complex discriminant.
fn resource_attr_value<'b>(a: &FlatAttr<'b>) -> ResourceAttrValue<'b> {
    match a.ty {
        Some(ANY_VALUE_TYPE_EMPTY) => ResourceAttrValue::Empty,
        Some(ANY_VALUE_TYPE_STRING) => ResourceAttrValue::Str(a.str_val.unwrap_or("")),
        Some(ANY_VALUE_TYPE_BOOL) => ResourceAttrValue::Bool(a.bool_val.unwrap_or(false)),
        Some(ANY_VALUE_TYPE_INT) => ResourceAttrValue::Int(a.int_val.unwrap_or(0)),
        Some(ANY_VALUE_TYPE_DOUBLE) => ResourceAttrValue::Double(a.double_val.unwrap_or(0.0)),
        None | Some(_) => ResourceAttrValue::Complex,
    }
}

/// The bytes one point's attribute labels take, as `(kept, reserved)`: `kept`
/// for the labels a set holds, `reserved` adding the slot an empty value leaves
/// allocated in a set whose `Vec` was sized before the drop ([`build_group`]).
/// `None` when an attribute has no label form, which rejects the point before
/// any set is kept.
fn flat_attr_bytes(range: &[u32], attrs: &[FlatAttr<'_>]) -> Option<(usize, usize)> {
    let mut kept = 0usize;
    let mut reserved = 0usize;
    for &i in range {
        let a = &attrs[i as usize];
        let len = resource_value_label_len(resource_attr_value(a))?;
        if len > 0 {
            let bytes = label_bytes(a.key.len(), len);
            kept = kept.saturating_add(bytes);
            reserved = reserved.saturating_add(bytes);
        } else {
            reserved = reserved.saturating_add(RESOLVED_LABEL_OVERHEAD_BYTES);
        }
    }
    Some((kept, reserved))
}

/// Whether two points' attribute rows are equal in order, the condition under
/// which the second point's group is already in the build's cache.
fn same_flat_attrs(a: &[u32], b: &[u32], attrs: &[FlatAttr<'_>]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(&x, &y)| {
            let (x, y) = (&attrs[x as usize], &attrs[y as usize]);
            x.key == y.key
                && x.ty == y.ty
                && x.str_val == y.str_val
                && x.bool_val == y.bool_val
                && x.int_val == y.int_val
                && x.double_val.map(f64::to_bits) == y.double_val.map(f64::to_bits)
        })
}

/// The bytes of one [`GroupCache`] key for these attribute rows: each
/// attribute's key and string value copied, charged at the per-label rate,
/// which covers the key's `(String, RawCell)` slot.
fn group_key_bytes(range: &[u32], attrs: &[FlatAttr<'_>]) -> usize {
    range
        .iter()
        .map(|&i| {
            let a = &attrs[i as usize];
            let str_len = match a.ty {
                Some(ANY_VALUE_TYPE_STRING) => a.str_val.map_or(0, str::len),
                _ => 0,
            };
            label_bytes(a.key.len(), str_len)
        })
        .fold(0usize, usize::saturating_add)
}

/// Heap bytes held by label sets and group-cache keys during a normalize,
/// recorded for the capacity test.
#[cfg(test)]
mod held {
    use std::cell::Cell;

    use ravel_types::Label;

    thread_local! {
        static HELD: Cell<usize> = const { Cell::new(0) };
    }

    /// Record what one phase of the build holds; [`take`] reads the largest.
    pub(super) fn record(bytes: usize) {
        HELD.with(|h| h.set(h.get().max(bytes)));
    }

    pub(super) fn take() -> usize {
        HELD.with(|h| h.replace(0))
    }

    /// Bytes of `slots` label slots plus the strings of `labels`.
    pub(super) fn labels<'a>(slots: usize, labels: impl Iterator<Item = &'a Label>) -> usize {
        slots * std::mem::size_of::<Label>()
            + labels
                .map(|l| l.name.capacity() + l.value.capacity())
                .sum::<usize>()
    }
}

/// The projection walk behind [`project_resolved_label_bytes`], over rows
/// already flattened and decisions already made. A point the build would reject
/// before keeping a set (no decision, a blocked resource, over the attribute or
/// bucket or quantile cap, an attribute with no label form) is not charged.
///
/// A number point that starts a run (the first of consecutive points of its
/// metric with equal attribute rows) is charged twice more: for the set the
/// [`GroupCache`] keeps, and for the cache key, which copies each attribute's
/// key and string value.
///
/// Each histogram or summary metric is charged once more for its
/// [`SeriesIdMemo`], which holds a copy of one exploded series' set: the
/// largest series any of the metric's charged points explodes into.
fn project_flat(
    flat: &FlatPoints<'_>,
    tables: &MetricTables,
    orders: &AttrOrders,
    limits: &IngestLimits,
) -> usize {
    let MetricTables {
        root,
        resources,
        metric_decision,
        hist_decision,
        summary_decision,
    } = tables;
    let max_attrs = limits.max_attributes_per_point;
    let mut total = resources
        .planned
        .values()
        .map(|p| p.bytes)
        .fold(0usize, usize::saturating_add);

    let order = &orders.number;
    let mut previous: HashMap<u16, &[u32]> = HashMap::new();
    for dp in &flat.dp {
        let Some(decision) = metric_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };
        let range = attr_range_for(&flat.attrs, order, dp.id);
        if range.len() > max_attrs {
            continue;
        }
        let Some((kept, reserved)) = flat_attr_bytes(range, &flat.attrs) else {
            continue;
        };
        let base = resources
            .prefix_bytes_for(root, dp.parent_id)
            .saturating_add(name_label_bytes(decision.name.len()));
        // The copy of the set every admitted point carries, which clones to
        // exact capacity.
        total = total.saturating_add(base.saturating_add(kept));
        let repeat = previous
            .insert(dp.parent_id, range)
            .is_some_and(|p| same_flat_attrs(p, range, &flat.attrs));
        if !repeat {
            total = total
                .saturating_add(base.saturating_add(reserved))
                .saturating_add(group_key_bytes(range, &flat.attrs));
        }
    }

    // Per histogram or summary metric id, the largest series its memo can copy.
    let mut memos: HashMap<u16, usize> = HashMap::new();
    for dp in &flat.hist_dp {
        let Some(decision) = hist_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };
        let range = attr_range_for(&flat.hist_attrs, &orders.hist, dp.id);
        if dp.explicit_bounds.len() > limits.max_histogram_buckets || range.len() > max_attrs {
            continue;
        }
        let Some((kept, _)) = flat_attr_bytes(range, &flat.hist_attrs) else {
            continue;
        };
        let per_series = resources
            .prefix_bytes_for(root, dp.parent_id)
            .saturating_add(kept)
            .saturating_add(explode_name_label_bytes(decision.name.len()));
        total = total.saturating_add(histogram_point_bytes(per_series, &dp.explicit_bounds));
        let le = dp
            .explicit_bounds
            .iter()
            .copied()
            .chain([f64::INFINITY])
            .map(le_label_bytes)
            .max()
            .unwrap_or(0);
        let memo = memos.entry(dp.parent_id).or_default();
        *memo = (*memo).max(per_series.saturating_add(le));
    }

    for dp in &flat.summary_dp {
        let Some(decision) = summary_decision
            .get(dp.parent_id as usize)
            .and_then(|d| d.as_ref())
        else {
            continue;
        };
        let range = attr_range_for(&flat.summary_attrs, &orders.summary, dp.id);
        if dp.quantiles.len() > limits.max_summary_quantiles || range.len() > max_attrs {
            continue;
        }
        let Some((kept, _)) = flat_attr_bytes(range, &flat.summary_attrs) else {
            continue;
        };
        let per_series = resources
            .prefix_bytes_for(root, dp.parent_id)
            .saturating_add(kept)
            .saturating_add(explode_name_label_bytes(decision.name.len()));
        total = total.saturating_add(summary_point_bytes(
            per_series,
            dp.quantiles.iter().map(|q| q.0),
        ));
        let quantile = dp
            .quantiles
            .iter()
            .map(|q| quantile_label_bytes(q.0))
            .max()
            .unwrap_or(0);
        let memo = memos.entry(dp.parent_id).or_default();
        *memo = (*memo).max(per_series.saturating_add(quantile));
    }
    memos.into_values().fold(total, usize::saturating_add)
}

/// The `le` label a histogram series for `bound` carries, as
/// [`histogram_point_bytes`] charges it.
fn le_label_bytes(bound: f64) -> usize {
    histogram_point_bytes(0, &[bound]).saturating_sub(histogram_point_bytes(0, &[]))
}

/// The `quantile` label a summary series for `quantile` carries, as
/// [`summary_point_bytes`] charges it.
fn quantile_label_bytes(quantile: f64) -> usize {
    summary_point_bytes(0, std::iter::once(quantile))
        .saturating_sub(summary_point_bytes(0, std::iter::empty()))
}

/// One [`MetricMetadata`] per metric id that contributed a point, in metric-id
/// order, deduplicated by `family_name` with the first occurrence winning
/// (`ravel_otlp`'s `MetadataCollector` rule). The family name is the decision's
/// already-suffixed name, so it agrees with the series the points carry.
fn build_metadata(
    root: &[Option<RootEntry>],
    metric_decision: &[Option<MetricDecision>],
    hist_decision: &[Option<HistogramDecision>],
    summary_decision: &[Option<SummaryDecision>],
    produced: &[bool],
) -> Vec<MetricMetadata> {
    let mut out: Vec<MetricMetadata> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for (id, contributed) in produced.iter().enumerate() {
        if !contributed {
            continue;
        }
        let Some(entry) = root.get(id).and_then(|e| e.as_ref()) else {
            continue;
        };
        // Exactly one of the three decision tables holds this id: the tables
        // are built from the same `metric_type` column, and a row whose type
        // disagrees with its data-point table was classed unknown and produced
        // no point.
        let family_name = metric_decision
            .get(id)
            .and_then(|d| d.as_ref())
            .map(|d| d.name.as_str())
            .or_else(|| {
                hist_decision
                    .get(id)
                    .and_then(|d| d.as_ref())
                    .map(|d| d.name.as_str())
            })
            .or_else(|| {
                summary_decision
                    .get(id)
                    .and_then(|d| d.as_ref())
                    .map(|d| d.name.as_str())
            });
        let Some(family_name) = family_name else {
            continue;
        };
        if !seen.insert(family_name) {
            continue;
        }
        let kind = metadata_kind(&entry.kind);
        out.push(MetricMetadata {
            family_name: family_name.to_string(),
            kind,
            help: entry.description.clone(),
            unit: metadata_unit_word(&entry.unit, kind),
        });
    }
    out
}

/// The Prometheus kind a METRICS root row implies, the OTAP twin of
/// `ravel_otlp`'s `metric_kind_of` over the `data` oneof: a monotonic Sum is a
/// Counter, a non-monotonic Sum and a Gauge are both Gauge. An `Unsupported`
/// row never reaches here (it produces no point and so no metadata), and
/// [`PromMetricKind::Unknown`] is the honest answer if one ever did.
fn metadata_kind(kind: &RootKind) -> PromMetricKind {
    match kind {
        RootKind::Gauge => PromMetricKind::Gauge,
        RootKind::Sum {
            is_monotonic: true, ..
        } => PromMetricKind::Counter,
        RootKind::Sum { .. } => PromMetricKind::Gauge,
        RootKind::Histogram { .. } => PromMetricKind::Histogram,
        RootKind::Summary => PromMetricKind::Summary,
        RootKind::Unsupported => PromMetricKind::Unknown,
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

/// Normalized points a flattened request would expand into: one per number data
/// point, and the exploded series count (ADR-0016) for the two metric types
/// that do not admit one point per data point. The OTAP twin of
/// `ravel_otlp::count_exploded_data_points`, but it counts what this path
/// actually builds, not what OTLP builds: exponential histograms are rejected
/// wholesale upstream ([`push_unsupported_type_rejections`]) and never reach a
/// flattened vector, so they explode into nothing and are absent here. Reads the
/// flattened vectors' lengths (and, per histogram, whether a `sum` is present)
/// only, never bucket or quantile contents, so it allocates nothing.
fn count_exploded_points(
    flat_dp: &[FlatDp],
    flat_hist_dp: &[FlatHistogramDp],
    flat_summary_dp: &[FlatSummaryDp],
) -> usize {
    let number = flat_dp.len();
    // `{name}_bucket` per explicit bound, plus the `+Inf` bucket, `_count`, and
    // `_sum` when present: exactly what [`explode_histogram_point`] pushes.
    let histogram = flat_hist_dp
        .iter()
        .map(|dp| {
            dp.explicit_bounds
                .len()
                .saturating_add(2)
                .saturating_add(usize::from(dp.sum.is_some()))
        })
        .fold(0usize, usize::saturating_add);
    // `{name}{quantile=...}` per quantile, plus `_sum` and `_count`, both
    // unconditional: exactly what [`explode_summary_point`] pushes.
    let summary = flat_summary_dp
        .iter()
        .map(|dp| dp.quantiles.len().saturating_add(2))
        .fold(0usize, usize::saturating_add);
    number.saturating_add(histogram).saturating_add(summary)
}

/// Reject a whole request for `reason` before any data point is built, so
/// nothing it carried is admitted. Both OTAP whole-request early returns share
/// this shape, the same way `ravel_otlp::whole_request_rejection` is shared by
/// both of its own early returns.
///
/// `dropped_exemplars` is what the rejection discards, counted from the
/// columnar payloads' row counts rather than from a decode, so both returns
/// report it exactly as OTLP's do (ADR-0047 decision 2: a dropped-data counter
/// must not read zero while data is lost). Counting without decoding is what
/// lets the exploded check stay above `group_exemplars_by_parent_id`.
///
/// A whole-request rejection stored no point, so it describes no metric family:
/// the metadata vector is empty, exactly as OTLP's own early returns.
/// `histogram_points` is likewise always empty; OTAP admits only scalar points
/// (`ravel_otlp::NormalizeOutput` gained that vector for the OTLP and Remote
/// Write native-histogram surfaces).
fn whole_request_rejection(
    reason: Rejection,
    dropped_exemplars: usize,
) -> (MetricsNormalizeResult, Vec<MetricMetadata>) {
    let mut rejected = vec![reason];
    if dropped_exemplars > 0 {
        rejected.push(Rejection::HistogramExemplarsDropped {
            count: dropped_exemplars,
        });
    }
    (
        MetricsNormalizeResult {
            output: NormalizeOutput {
                points: Vec::new(),
                histogram_points: Vec::new(),
                rejected,
            },
            exemplars: Vec::new(),
        },
        Vec::new(),
    )
}

/// Exemplars a whole-request rejection drops, counted from the columnar
/// payloads' row counts.
///
/// One row is one exemplar, so this is batch metadata rather than a decode:
/// it costs nothing and it does not reach the values, which is what lets the
/// exploded-points check keep rejecting before `group_exemplars_by_parent_id`
/// runs. Counting it matters because the OTLP twin counts exemplars at both of
/// its early returns (ADR-0047 decision 2, the dropped-data counter must not
/// read zero while data is lost), and an OTAP-fronted deployment that
/// under-reported them would diverge from an OTLP-fronted one on the same
/// overload input.
///
/// All three exemplar payloads count, exponential histograms included. Those
/// data points are rejected as an unsupported type on the admitted path, but
/// `count_total_points` still counts them, so a request of them can trip the
/// wire-count bound and OTLP reports their exemplars as dropped when it does.
/// `push_exp_histogram_exemplar_drops` counts the same rows on the admitted
/// path, so leaving them out here would under-report on one payload type only,
/// which is the same hazard one layer along.
fn dropped_exemplar_rows(batch: &DecodedBatch) -> usize {
    payloads_of(batch, ArrowPayloadType::HistogramDpExemplars)
        .iter()
        .chain(payloads_of(batch, ArrowPayloadType::NumberDpExemplars).iter())
        .chain(payloads_of(batch, ArrowPayloadType::ExpHistogramDpExemplars).iter())
        .map(|b| b.num_rows())
        .sum()
}

/// Exponential histograms are the one remaining unsupported metric payload
/// type (ADR-0017): counted as one [`Rejection::UnsupportedMetricType`],
/// never a silent drop.
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

/// Count every `EXP_HISTOGRAM_DP_EXEMPLARS` row as dropped.
///
/// None of these can be carried today, and the reason is structural rather
/// than a missing decode: `EXP_HISTOGRAM_DATA_POINTS` is rejected outright by
/// [`push_unsupported_type_rejections`] (ADR-0017 is a separate ticket on this
/// path), so the data point an exemplar would attach to is never normalized
/// and no series exists to attach it to. Reading the exemplar's own columns
/// would produce a value with nowhere to go.
///
/// The attachment rule this path will use once exp histograms are supported is
/// already decided, and it is the same one the number path above uses: the
/// exemplar attaches to the data point's OWN series. A native histogram is one
/// Ravel series carrying one native-histogram sample per timestamp; unlike a
/// classic histogram it does not explode into per-bucket `le` series, so
/// resolving the exemplar's value to a bucket index through the exponential
/// schema's scale would name a series that does not exist. That is why
/// ADR-0047 needs no amendment here: its decision 1 attaches an exemplar to a
/// `(series, ts_ns)`, and the explicit-bucket rule is a consequence of the
/// classic histogram exploding, not a separate rule. `ravel_otlp`'s
/// `build_native_histogram_point`, which does carry these exemplars, already
/// resolves them with `|_| series_id` for exactly this reason.
///
/// Counting rows, rather than decoding and then discarding them, is what keeps
/// this exact: one row on the wire is one exemplar, whatever its columns hold.
fn push_exp_histogram_exemplar_drops(batch: &DecodedBatch, rejected: &mut Vec<Rejection>) {
    let count: usize = batch
        .payloads
        .iter()
        .filter(|(t, _)| *t == ArrowPayloadType::ExpHistogramDpExemplars)
        .map(|(_, rb)| rb.num_rows())
        .sum();
    if count > 0 {
        rejected.push(Rejection::HistogramExemplarsDropped { count });
    }
}

struct RootEntry {
    name: String,
    kind: RootKind,
    /// The metric's UCUM unit (METRICS.unit column), empty when absent. Fed to
    /// [`prometheus_family_name`] when building each decision's suffixed name
    /// (ADR-0085 Decision 2), mirroring `ravel_otlp::normalize_metric`.
    unit: String,
    /// The metric's help text (METRICS.description column), empty when the
    /// column is absent or null. OTAP's counterpart to OTLP's
    /// `Metric.description`, and the `help` field of the metadata tuple
    /// [`normalize_decoded_with_metadata`] surfaces (ADR-0085 Decision 1). It
    /// has no effect on points, names, or rejections, so a producer that omits
    /// the column normalizes exactly as before with an empty help.
    description: String,
    /// The batch-scoped `resource.id` this metric's resource label prefix is
    /// built from.
    resource: ResourceRef,
}

/// A metric's `resource.id` as its METRICS row carries it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResourceRef {
    /// No resource: an empty prefix.
    Absent,
    Id(u16),
    /// A resource that cannot be read: the metric's points are rejected with
    /// this tag, [`MALFORMED_RESOURCE`] for a `resource` column or `id` child
    /// of the wrong type, [`UNRECOGNIZED_ID_ENCODING`] for an `id` child whose
    /// encoding is not recognized.
    Unreadable(&'static str),
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
    /// The AnyValue `type` discriminant, or `None` when this row's discriminant
    /// is null (validity bit unset). A null discriminant is out of contract and
    /// [`raw_cell`] routes it through the complex/rejected fallback rather than
    /// reading whatever byte the buffer still holds.
    ty: Option<u8>,
    str_val: Option<&'b str>,
    bool_val: Option<bool>,
    int_val: Option<i64>,
    double_val: Option<f64>,
}

/// One decoded `NUMBER_DP_EXEMPLARS` or `HISTOGRAM_DP_EXEMPLARS` row, with its
/// `parent_id` already resolved by [`group_exemplars_by_parent_id`] and its
/// attributes still to be joined from the matching `*_DP_EXEMPLAR_ATTRS` table
/// by `id`. The two tables have identical columns in the data model, so one
/// decoder serves both.
///
/// `ts_ns` and `value` are `Option` because both columns are optional in the
/// table (otap-spec.md section 5.3.6). A row with no value is what OTLP itself
/// calls an invalid exemplar, so it is dropped and counted rather than stored
/// with a made-up value; a row with no timestamp has nothing to place it in a
/// cap window or in a segment's event-time range, so it goes the same way.
struct FlatExemplar {
    id: Option<u32>,
    ts_ns: Option<i64>,
    value: Option<f64>,
    trace_id: [u8; 16],
    span_id: [u8; 8],
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

type GroupOutcome = Result<(LabelSet, SeriesId), Rejection>;

/// Memoization cache for the series-grouping step: per metric id, maps a
/// canonicalized (sorted-by-key) attribute set to its already-computed
/// `(LabelSet, SeriesId)` or rejection, so BLAKE3 canonicalization runs
/// once per distinct combination per batch instead of once per point.
///
/// A key is built once per run: consecutive points of one metric with equal
/// attribute rows, as [`project_flat`] counts them. A later point of the run
/// reuses the run's outcome without copying its attributes again, so the keys
/// and sets held never exceed what the projection charged.
#[derive(Default)]
struct GroupCache<'a> {
    keys: HashMap<u16, HashMap<Vec<(String, RawCell)>, usize>>,
    outcomes: Vec<GroupOutcome>,
    /// Per metric id: the current run's attribute rows, and its outcome once
    /// resolved.
    runs: HashMap<u16, (&'a [u32], Option<usize>)>,
}

impl<'a> GroupCache<'a> {
    /// Track `range` as the latest counted point of `metric_id`, starting a new
    /// run unless its attributes equal the previous point's.
    fn enter_run(&mut self, metric_id: u16, range: &'a [u32], attrs: &[FlatAttr<'_>]) {
        if !has_label_form(range, attrs) {
            return;
        }
        let outcome = match self.runs.get(&metric_id) {
            Some(&(prev, outcome)) if same_flat_attrs(prev, range, attrs) => outcome,
            _ => None,
        };
        self.runs.insert(metric_id, (range, outcome));
    }

    /// The outcome for this point's attributes: the run's, the cached one for
    /// an equal key, or `build`'s result for a new key.
    fn resolve(
        &mut self,
        metric_id: u16,
        range: &'a [u32],
        attrs: &[FlatAttr<'_>],
        build: impl FnOnce(&[(String, RawCell)]) -> GroupOutcome,
    ) -> &GroupOutcome {
        let run = self.runs.get(&metric_id).and_then(|&(_, outcome)| outcome);
        let idx = match run {
            Some(idx) => idx,
            None => {
                let mut key: Vec<(String, RawCell)> = range
                    .iter()
                    .map(|&i| {
                        let a = &attrs[i as usize];
                        (a.key.to_string(), raw_cell(a))
                    })
                    .collect();
                key.sort_by(|a, b| a.0.cmp(&b.0));
                let keys = self.keys.entry(metric_id).or_default();
                let idx = match keys.get(&key) {
                    Some(&idx) => idx,
                    None => {
                        let idx = self.outcomes.len();
                        self.outcomes.push(build(&key));
                        keys.insert(key, idx);
                        idx
                    }
                };
                if let Some(entry) = self.runs.get_mut(&metric_id) {
                    entry.1 = Some(idx);
                }
                idx
            }
        };
        &self.outcomes[idx]
    }

    /// Heap bytes the cached keys and label sets hold. A set's slots are its
    /// labels plus the empty values [`build_group`] reserved and dropped.
    #[cfg(test)]
    fn held_bytes(&self) -> usize {
        self.keys
            .values()
            .flat_map(|keys| keys.iter())
            .map(|(key, &idx)| {
                let strings: usize = key
                    .iter()
                    .map(|(k, v)| {
                        k.capacity()
                            + match v {
                                RawCell::Str(s) => s.capacity(),
                                _ => 0,
                            }
                    })
                    .sum();
                let key_bytes = key.capacity() * std::mem::size_of::<(String, RawCell)>() + strings;
                let empties = key
                    .iter()
                    .filter(|(_, v)| matches!(v, RawCell::Str(s) if s.is_empty()))
                    .count();
                let set_bytes = match &self.outcomes[idx] {
                    Ok((set, _)) => held::labels(set.len() + empties, set.iter()),
                    Err(_) => 0,
                };
                key_bytes + set_bytes
            })
            .sum()
    }
}

/// Whether every attribute in `range` has a label form, the condition under
/// which [`project_flat`] counts a point.
fn has_label_form(range: &[u32], attrs: &[FlatAttr<'_>]) -> bool {
    range
        .iter()
        .all(|&i| resource_value_label_len(resource_attr_value(&attrs[i as usize])).is_some())
}

fn dense_size_for(root_ids: &[Option<RootIds>], parent_ids: impl Iterator<Item = u16>) -> usize {
    let mut max_id: u32 = 0;
    for ids in root_ids.iter().flatten() {
        for &v in &ids.ids {
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
) -> Vec<Option<RootIds>> {
    root_batches
        .iter()
        .map(|rb| {
            let ids = column_as::<UInt16Array>(rb, "id")?;
            let ids = decode_id_column_u16(rb, "id", ids, rejected)?;
            let resource_ids = decode_root_resource_ids(rb);
            Some(RootIds { ids, resource_ids })
        })
        .collect()
}

/// One METRICS batch's decoded `id` column and, per row, the `resource.id` it
/// joins to `RESOURCE_ATTRS` by.
struct RootIds {
    ids: Vec<u16>,
    resource_ids: Vec<ResourceRef>,
}

/// Decode the METRICS `resource.id` column (otap-spec.md section 5.3.1: a
/// `UInt16` child of the `resource` struct, DELTA by default, its encoding
/// declared on the child field). A row carries no resource when the `resource`
/// column, its `id` child, the struct slot or the id slot is absent or null.
/// Every row is [`MALFORMED_RESOURCE`] when the column or child is present
/// with the wrong type. Null slots are left out of the delta running sum, as
/// [`decode_delta_u16`] leaves them out. An unrecognized encoding makes every
/// row that carries a resource [`UNRECOGNIZED_ID_ENCODING`], leaving the rows
/// with none readable.
fn decode_root_resource_ids(rb: &RecordBatch) -> Vec<ResourceRef> {
    let rows = rb.num_rows();
    let Some(column) = rb.column_by_name("resource") else {
        return vec![ResourceRef::Absent; rows];
    };
    let Some(resource) = column.as_any().downcast_ref::<StructArray>() else {
        return vec![ResourceRef::Unreadable(MALFORMED_RESOURCE); rows];
    };
    let Some(idx) = resource.fields().iter().position(|f| f.name() == "id") else {
        return vec![ResourceRef::Absent; rows];
    };
    let field = &resource.fields()[idx];
    let Some(ids) = resource.column(idx).as_any().downcast_ref::<UInt16Array>() else {
        return vec![ResourceRef::Unreadable(MALFORMED_RESOURCE); rows];
    };
    let present = |i: usize| !resource.is_null(i) && !ids.is_null(i);
    let encoding = field_id_encoding(field);
    let mut acc: u16 = 0;
    (0..rows)
        .map(|i| {
            if !present(i) {
                return ResourceRef::Absent;
            }
            match encoding {
                Ok(IdEncoding::Plain) => ResourceRef::Id(ids.value(i)),
                Ok(IdEncoding::Delta) => {
                    acc = acc.wrapping_add(ids.value(i));
                    ResourceRef::Id(acc)
                }
                Ok(IdEncoding::QuasiDelta) | Err(()) => {
                    ResourceRef::Unreadable(UNRECOGNIZED_ID_ENCODING)
                }
            }
        })
        .collect()
}

fn column_as<'b, T: 'static>(rb: &'b RecordBatch, name: &str) -> Option<&'b T> {
    rb.column_by_name(name)?.as_any().downcast_ref::<T>()
}

fn name_at(col: &Arc<dyn Array>, i: usize) -> Option<String> {
    StrCol::from_array(col.as_ref())?.get(i).map(str::to_string)
}

fn opt_u8(col: Option<&UInt8Array>, i: usize) -> Option<u8> {
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn opt_i32(col: Option<&Int32Array>, i: usize) -> Option<i32> {
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn opt_bool(col: Option<&BooleanArray>, i: usize) -> Option<bool> {
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
    root_ids: &[Option<RootIds>],
    dense_size: usize,
) -> Vec<Option<RootEntry>> {
    let mut entries: Vec<Option<RootEntry>> = (0..dense_size).map(|_| None).collect();
    for (rb, ids) in root_batches.iter().zip(root_ids) {
        let Some(RootIds { ids, resource_ids }) = ids else {
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
        let unit_col = rb.column_by_name("unit");
        // Optional like `unit`: a producer that omits the column (this crate's
        // own test encoder among them) yields an empty help rather than a
        // decode failure.
        let description_col = rb.column_by_name("description");

        for (i, &id) in ids.iter().enumerate() {
            let Some(name) = name_at(name_col, i) else {
                continue;
            };
            let unit = unit_col.and_then(|c| name_at(c, i)).unwrap_or_default();
            let description = description_col
                .and_then(|c| name_at(c, i))
                .unwrap_or_default();
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
                entries[id as usize] = Some(RootEntry {
                    name,
                    kind,
                    unit,
                    description,
                    resource: resource_ids.get(i).copied().unwrap_or(ResourceRef::Absent),
                });
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

/// Flatten a data-point or exemplar `*Attrs` table. A batch missing its
/// `parent_id`, `key` or `type` column is skipped; one with an unrecognized
/// `parent_id` encoding is skipped and counted as [`UNRECOGNIZED_ID_ENCODING`].
fn flatten_attrs<'b>(
    attr_batches: &[&'b RecordBatch],
    rejected: &mut Vec<Rejection>,
) -> Vec<FlatAttr<'b>> {
    let mut out = Vec::new();
    for rb in attr_batches {
        match flatten_attr_batch(rb, false, &mut out) {
            Ok(()) | Err(AttrBatchError::Unreadable) => {}
            Err(AttrBatchError::UnrecognizedEncoding) => {
                rejected.push(Rejection::UnsupportedMetricType {
                    metric_type: UNRECOGNIZED_ID_ENCODING,
                    count: rb.num_rows(),
                });
            }
        }
    }
    out
}

/// Flatten the `RESOURCE_ATTRS` table, or name why it cannot be read: a
/// non-empty batch missing its `parent_id`, `key` or `type` column, with a
/// null `parent_id` slot, or with a `str`, `bool`, `int` or `double` column of
/// a type this pass cannot read, is [`MALFORMED_RESOURCE`]; an
/// unrecognized `parent_id` encoding is [`UNRECOGNIZED_ID_ENCODING`]. Either
/// leaves no resource readable, so [`plan_resources`] rejects every point that
/// refers to one.
fn flatten_resource_attrs<'b>(
    attr_batches: &[&'b RecordBatch],
) -> Result<Vec<FlatAttr<'b>>, &'static str> {
    let mut out = Vec::new();
    for rb in attr_batches {
        if rb.num_rows() == 0 {
            continue;
        }
        match flatten_attr_batch(rb, true, &mut out) {
            Ok(()) => {}
            Err(AttrBatchError::Unreadable) => return Err(MALFORMED_RESOURCE),
            Err(AttrBatchError::UnrecognizedEncoding) => return Err(UNRECOGNIZED_ID_ENCODING),
        }
    }
    Ok(out)
}

enum AttrBatchError {
    Unreadable,
    UnrecognizedEncoding,
}

/// Append one `*Attrs` batch's rows to `out`. With `strict`, a null
/// `parent_id` slot, or a value column present with a type this pass cannot
/// read, makes the batch unreadable rather than parent 0 or an empty value.
fn flatten_attr_batch<'b>(
    rb: &'b RecordBatch,
    strict: bool,
    out: &mut Vec<FlatAttr<'b>>,
) -> Result<(), AttrBatchError> {
    let parents = ParentIds::from_column(rb, "parent_id").ok_or(AttrBatchError::Unreadable)?;
    let keys = StrCol::from_column(rb, "key").ok_or(AttrBatchError::Unreadable)?;
    let types = column_as::<UInt8Array>(rb, "type").ok_or(AttrBatchError::Unreadable)?;
    if strict && parents.has_null_slot() {
        return Err(AttrBatchError::Unreadable);
    }
    let strs = StrCol::from_column(rb, "str");
    let bools = column_as::<BooleanArray>(rb, "bool");
    let ints = IntCol::from_column(rb, "int");
    let doubles = column_as::<Float64Array>(rb, "double");
    if strict {
        let unreadable = |name: &str, read: bool| !read && rb.column_by_name(name).is_some();
        if unreadable("str", strs.is_some())
            || unreadable("bool", bools.is_some())
            || unreadable("int", ints.is_some())
            || unreadable("double", doubles.is_some())
        {
            return Err(AttrBatchError::Unreadable);
        }
    }
    // otap-spec.md section 6.4.3: `*Attrs.parent_id` defaults to
    // QUASI-DELTA (equality columns `type`, `key`, and the type's Active
    // Field value), not the DELTA default of the core id chain.
    let decoded_parents = match quasi_delta_default_encoding(rb, "parent_id") {
        Ok(IdEncoding::Plain) => parents.decode_plain(),
        Ok(IdEncoding::Delta) => parents.decode_delta(),
        Ok(IdEncoding::QuasiDelta) => decode_quasi_delta_attrs(rb, &parents),
        Err(()) => return Err(AttrBatchError::UnrecognizedEncoding),
    };
    out.reserve(decoded_parents.len());
    for (i, &parent_id) in decoded_parents.iter().enumerate() {
        out.push(FlatAttr {
            parent_id,
            // A null key reads as the empty string, as the plain column's
            // empty slot always did.
            key: keys.get(i).unwrap_or(""),
            // Read the discriminant through the null-checking `opt_u8`, like
            // the sibling value columns: a null `type` (validity bit unset,
            // even with a scalar byte still in the buffer) is out of
            // contract and must reach `raw_cell`'s rejection fallback, not
            // be read as that stale byte.
            ty: opt_u8(Some(types), i),
            str_val: strs.as_ref().and_then(|c| c.get(i)),
            bool_val: opt_bool(bools, i),
            int_val: ints.as_ref().and_then(|c| c.get(i)),
            double_val: opt_f64(doubles, i),
        });
    }
    Ok(())
}

fn raw_cell(a: &FlatAttr) -> RawCell {
    match a.ty {
        Some(ANY_VALUE_TYPE_EMPTY) => RawCell::Str(String::new()),
        Some(ANY_VALUE_TYPE_STRING) => RawCell::Str(a.str_val.unwrap_or("").to_string()),
        Some(ANY_VALUE_TYPE_BOOL) => RawCell::Bool(a.bool_val.unwrap_or(false)),
        Some(ANY_VALUE_TYPE_INT) => RawCell::Int(a.int_val.unwrap_or(0)),
        Some(ANY_VALUE_TYPE_DOUBLE) => RawCell::Double(a.double_val.unwrap_or(0.0).to_bits()),
        // A null `type` discriminant (`None`) is out of contract: a malformed
        // producer can unset the validity bit while leaving a scalar byte in
        // the buffer. Like the complex Bytes/Array/Map types it carries no
        // usable scalar value here, so it takes the same per-point
        // `ComplexAttributeValue` rejection (via [`raw_cell_value`]) rather than
        // being silently read as the buffer's stale scalar byte.
        None | Some(_) => RawCell::Complex,
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
    blocked: &[bool],
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
        // A blocked metric's points were already rejected with its resource,
        // which is all OTLP reports for a metric under a rejected resource.
        if count == 0 || blocked.get(id).copied().unwrap_or(false) {
            continue;
        }
        match root.get(id).and_then(|e| e.as_ref()) {
            None => unknown_count += count,
            Some(entry) => match &entry.kind {
                RootKind::Unsupported => unknown_count += count,
                RootKind::Gauge => {
                    if let Some(name) = process_metric_name(&entry.name, limits, count, rejected) {
                        let name = prometheus_family_name(
                            &name,
                            &entry.unit,
                            PromMetricKind::Gauge,
                            false,
                        );
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
                    // not on temporality. Reversing this would diverge the two
                    // ingest paths' rejection classes (ADR-0011 identical-
                    // rejection-class contract).
                    let Some(name) = process_metric_name(&entry.name, limits, count, rejected)
                    else {
                        continue;
                    };
                    if *temporality != AGGREGATION_TEMPORALITY_CUMULATIVE {
                        rejected.push(Rejection::UnsupportedTemporality { count });
                        continue;
                    }
                    let kind = if *is_monotonic {
                        PromMetricKind::Counter
                    } else {
                        PromMetricKind::Gauge
                    };
                    let name = prometheus_family_name(&name, &entry.unit, kind, *is_monotonic);
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
/// complex (array/map/bytes) value. Its output length is
/// [`resource_value_label_len`], which [`check_flat_attrs_in_input_order`]
/// checks before this builds anything.
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
/// rejection-class contract. The length bounds here
/// intentionally match [`push_checked`], which the canonical build applies to
/// the sorted set; because a set that passes in input order also passes in any
/// order, the sorted build only ever surfaces the order-independent
/// `DuplicateLabelName` / `OversizedSeriesComponent` rejections.
///
/// Decided from lengths, so a point it rejects allocates nothing.
fn check_flat_attrs_in_input_order(
    range: &[u32],
    attrs: &[FlatAttr<'_>],
    limits: &IngestLimits,
) -> Result<(), Rejection> {
    for &i in range {
        let a = &attrs[i as usize];
        let value_len = resource_value_label_len(resource_attr_value(a))
            .ok_or(Rejection::ComplexAttributeValue)?;
        // ADR-0038: an empty-valued label is dropped as absent by
        // `push_checked` before any length check, so this ordered validation
        // pass must skip it too, or an empty-valued label with an over-long
        // name would reject here while the canonical build silently drops it,
        // breaking the ADR-0011 identical-rejection-class contract.
        check_label_len(sanitized_label_name_len(a.key), value_len, limits)?;
    }
    Ok(())
}

/// Build the `(LabelSet, SeriesId)` for one distinct attribute-set group
/// under one metric; called at most once per distinct group thanks to the
/// caller's memoizing `HashMap` (see module docs).
fn build_group(
    raw: &[(String, RawCell)],
    metric_name: &str,
    prefix: &[Label],
    tenant: &TenantId,
    limits: &IngestLimits,
) -> Result<(LabelSet, SeriesId), Rejection> {
    let mut labels = Vec::with_capacity(prefix.len() + raw.len() + 1);
    labels.extend_from_slice(prefix);
    labels.push(kept_label(
        METRIC_NAME_LABEL.to_string(),
        metric_name.to_string(),
    ));
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
    // ADR-0038: an empty-valued label is treated as absent (Prometheus
    // convention, matching ravel-remote-write and ravel-otlp). Drop it before
    // the length checks, mirroring ravel_otlp::push_checked exactly, so both
    // OTLP and OTAP hand SeriesId::compute the same label set.
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
    labels.push(kept_label(name, value));
    Ok(())
}

/// Every label a kept set holds is built here or in [`push_checked`], so its
/// name and value carry no spare capacity and the projection's per-label
/// charge ([`label_bytes`]) bounds what it allocates. Mirrors
/// `ravel_otlp::normalize`'s private `kept_label`.
fn kept_label(mut name: String, mut value: String) -> Label {
    name.shrink_to_fit();
    value.shrink_to_fit();
    Label { name, value }
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

    /// Heap bytes the memos' copied sets hold.
    #[cfg(test)]
    fn held_bytes<'a>(memos: impl Iterator<Item = &'a SeriesIdMemo>) -> usize {
        memos
            .filter_map(|m| m.last.as_ref())
            .map(|(labels, _)| held::labels(labels.len(), labels.iter()))
            .sum()
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

/// Group `NUMBER_DP_EXEMPLARS` or `HISTOGRAM_DP_EXEMPLARS` rows by the
/// data-point id they belong to (OTAP carries exemplars in their own table
/// joined by `parent_id`; OTLP embeds them inline on the data point as
/// `dp.exemplars`). Rows keep their table order within a parent, which is what
/// lets the admission pass mirror OTLP's per-data-point ordering.
///
/// Called once per exemplar table; both tables have the same columns and the
/// same encoding rules, so this is one decoder rather than two.
///
/// Exemplars are carried, not just counted: ADR-0047 gives them an RSEG
/// section. The `parent_id` decode is QUASI-DELTA by default with
/// `int_value`/`double_value` as its equality columns (otap-spec.md section
/// 6.4.3), via the same [`decode_quasi_delta_column`] call.
fn group_exemplars_by_parent_id(
    batches: &[&RecordBatch],
    rejected: &mut Vec<Rejection>,
) -> HashMap<u32, Vec<FlatExemplar>> {
    let mut grouped: HashMap<u32, Vec<FlatExemplar>> = HashMap::new();
    for rb in batches {
        let Some(parents) = column_as::<UInt32Array>(rb, "parent_id").map(ParentIds::U32) else {
            continue;
        };
        let parents = &parents;
        // otap-spec.md section 6.4.3: `*DpExemplars.parent_id` defaults to
        // QUASI-DELTA with `int_value`/`double_value` as its equality columns.
        let Some(decoded_parents) =
            decode_quasi_delta_column(rb, parents, rejected, decode_quasi_delta_exemplars)
        else {
            continue;
        };
        // Section 5.3.6: `id` is DELTA by default like the rest of the id
        // chain, and optional. Without it, this table's rows cannot be joined
        // to `HISTOGRAM_DP_EXEMPLAR_ATTRS`, which references that id; the
        // exemplars themselves still decode, just with no attributes.
        //
        // A NULL slot in the `id` column means "this exemplar declares no id"
        // and must join to no attributes. Neither raw decode expresses that:
        // DELTA (`decode_delta_u32`) carries its accumulator through a null
        // slot, so the row would inherit the previous row's id; Plain reads
        // the null slot's raw buffer value. Both would misattribute one
        // exemplar's attributes to another. Keep the source column so the
        // per-row join below can force a null slot back to `None`, regardless
        // of encoding.
        let id_col = column_as::<UInt32Array>(rb, "id");
        let decoded_ids = id_col.and_then(|ids| decode_id_column_u32(rb, "id", ids, rejected));
        let times = column_as::<TimestampNanosecondArray>(rb, "time_unix_nano");
        let ints = column_as::<Int64Array>(rb, "int_value");
        let doubles = column_as::<Float64Array>(rb, "double_value");
        let span_ids = column_as::<FixedSizeBinaryArray>(rb, "span_id");
        let trace_ids = column_as::<FixedSizeBinaryArray>(rb, "trace_id");

        for (i, &parent_id) in decoded_parents.iter().enumerate() {
            grouped.entry(parent_id).or_default().push(FlatExemplar {
                // A null `id` slot yields `None` (no join key, no attributes)
                // under every encoding: check the source column's validity bit
                // rather than trusting the decoded value, which for a null slot
                // is either the carried DELTA accumulator or a stale Plain
                // buffer value. Two exemplars declaring the SAME id both join to
                // that id's attribute set: the id is the explicit join key, so a
                // duplicate is not misattribution, and each row independently
                // resolving its own declared id keeps that deterministic.
                id: match (id_col, decoded_ids.as_ref()) {
                    (Some(col), Some(decoded)) if !col.is_null(i) => decoded.get(i).copied(),
                    _ => None,
                },
                ts_ns: times.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
                // A double wins over an int when a malformed producer sets
                // both, matching how OTLP's exemplar carries exactly one arm
                // of a oneof and this crate reads `double_value` first
                // everywhere else.
                value: opt_f64(doubles, i).or_else(|| opt_i64(ints, i).map(|v| v as f64)),
                trace_id: fixed_size_id(trace_ids, i),
                span_id: fixed_size_id(span_ids, i),
            });
        }
    }
    grouped
}

/// Read a `FixedSizeBinary(N)` id cell into an `N`-byte array, treating a
/// null, a missing column, and a column whose width is not `N` identically:
/// all-zero, which [`ravel_types::Exemplar`] documents as "absent". A
/// wrong-width column is a malformed producer, never a reason to panic or to
/// drop the exemplar carrying it (mirrors `ravel_otlp::normalize`'s `parse_id`,
/// which treats any wrong-length trace or span id on the wire the same way).
fn fixed_size_id<const N: usize>(col: Option<&FixedSizeBinaryArray>, i: usize) -> [u8; N] {
    let Some(col) = col.filter(|c| !c.is_null(i)) else {
        return [0u8; N];
    };
    <[u8; N]>::try_from(col.value(i)).unwrap_or([0u8; N])
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
    blocked: &[bool],
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
        // A blocked metric's points were already rejected with its resource,
        // which is all OTLP reports for a metric under a rejected resource.
        if count == 0 || blocked.get(id).copied().unwrap_or(false) {
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
                    let name = prometheus_family_name(
                        &name,
                        &entry.unit,
                        PromMetricKind::Histogram,
                        false,
                    );
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
    blocked: &[bool],
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
        // A blocked metric's points were already rejected with its resource,
        // which is all OTLP reports for a metric under a rejected resource.
        if count == 0 || blocked.get(id).copied().unwrap_or(false) {
            continue;
        }
        match root.get(id).and_then(|e| e.as_ref()) {
            None => unknown_count += count,
            Some(entry) => match &entry.kind {
                RootKind::Summary => {
                    if let Some(name) = process_metric_name(&entry.name, limits, count, rejected) {
                        let name = prometheus_family_name(
                            &name,
                            &entry.unit,
                            PromMetricKind::Summary,
                            false,
                        );
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

/// Build the resource-plus-attribute label prefix shared by every series
/// exploded from one histogram/summary data point (everything but the
/// `__name__` label and the synthesized `le`/`quantile` label, which differ per
/// series). Mirrors `ravel_otlp::normalize`'s private
/// `build_explode_base_labels`.
fn build_base_labels(
    prefix: &[Label],
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
    let mut labels = Vec::with_capacity(prefix.len() + range.len());
    labels.extend_from_slice(prefix);
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
    labels.push(kept_label(
        METRIC_NAME_LABEL.to_string(),
        metric_name.to_string(),
    ));
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
        // ADR-0098: fresh Arc per exploded series, no sharing (see above).
        labels: Arc::new(label_set),
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
/// zero-weight informational rejections for dropped min/max fields and for
/// exemplars the admission cap turned away, since the point itself was
/// admitted. The exemplars come from [`group_exemplars_by_parent_id`] over the
/// `HISTOGRAM_DP_EXEMPLARS` table rather than an inline field, since OTAP
/// carries exemplars in their own table.
#[allow(clippy::too_many_arguments)]
fn explode_histogram_point(
    tenant: &TenantId,
    metric_name: &str,
    prefix: &[Label],
    dp: &FlatHistogramDp,
    range: &[u32],
    attrs: &[FlatAttr],
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    exemplars: &mut ExemplarAdmission<'_, '_>,
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
    // Last of the validity checks, and the one that has to precede
    // `build_base_labels`: everything below allocates per bucket, and the bound
    // list is the sender's to choose. Mirrors `ravel_otlp::explode_histogram`,
    // reading the same `max_histogram_buckets` value the OTLP surface reads.
    if dp.explicit_bounds.len() > limits.max_histogram_buckets {
        return Err(Rejection::TooManyHistogramBuckets {
            bounds: dp.explicit_bounds.len(),
            max: limits.max_histogram_buckets,
        });
    }

    let base_labels = build_base_labels(prefix, range, attrs, limits)?;
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

    // Bucket series occupy series[0..expected_buckets] regardless of whether
    // `_sum` was pushed after them, since it and `_count` are always appended
    // last. Mirrors `ravel_otlp::normalize`'s explode_histogram exactly.
    let bucket_series = &series[0..expected_buckets];
    exemplars.admit(
        dp.id,
        limits,
        |value| {
            let idx = exemplar_bucket_index(&dp.explicit_bounds, value);
            bucket_series[idx].series_id
        },
        &mut informational,
    );

    Ok((series, informational))
}

/// The index into a histogram's ordered bucket series (explicit bounds, then
/// `+Inf`) that `value` falls into: the first bound at or above `value`, or the
/// `+Inf` bucket (index `explicit_bounds.len()`) if none qualifies. Mirrors
/// `ravel_otlp::normalize`'s private function of the same name, including its
/// note that `<=` here is an ordering comparison against already-validated
/// finite bounds, not an equality test on a stored value, so it does not
/// conflict with the bit-pattern comparison rule.
fn exemplar_bucket_index(explicit_bounds: &[f64], value: f64) -> usize {
    explicit_bounds
        .iter()
        .position(|&bound| value <= bound)
        .unwrap_or(explicit_bounds.len())
}

/// The exemplar side of one normalize pass: the decoded exemplar rows grouped
/// by data-point id, the attrs table they join to, the caller's admission cap,
/// and where admitted exemplars go.
///
/// Bundled into one struct because [`explode_histogram_point`] would otherwise
/// take five more parameters for one concern, and because the cap and the
/// output vector must be threaded by `&mut` through every data point in the
/// batch: the cap's whole purpose is state that outlives one point (ADR-0047
/// decision 2).
///
/// One of these is built per exemplar source (`NUMBER_DP_EXEMPLARS` and
/// `HISTOGRAM_DP_EXEMPLARS`), each borrowing the same caller-owned cap in turn
/// and appending to the same output vector, so a gauge point and a histogram
/// bucket in one batch compete for the same per-series window.
struct ExemplarAdmission<'a, 'b> {
    rows: &'a HashMap<u32, Vec<FlatExemplar>>,
    attrs: &'a [FlatAttr<'b>],
    attr_order: &'a [u32],
    cap: &'a mut ExemplarCap,
    out: &'a mut Vec<NormalizedExemplar>,
}

impl ExemplarAdmission<'_, '_> {
    /// Offer data point `dp_id`'s exemplars to the cap, pushing the admitted
    /// ones onto `out` tagged with the series `series_id_for` resolves each
    /// value to: for a classic histogram, the bucket series whose `le` bound is
    /// the smallest at or above the exemplar's value, matching Prometheus's own
    /// convention and the OTLP path; for a gauge or sum, the data point's own
    /// series, since a number point has no buckets and so no `le` bound to
    /// resolve. Rows too malformed to carry (no value, no timestamp) and rows
    /// the cap turns away are counted into `informational` through the existing
    /// drop counter, so the count stays visible now that some exemplars are
    /// kept.
    ///
    /// Candidates are offered newest-first, since [`ExemplarCap::admit`] never
    /// retracts an admission it already handed back: that ordering is what
    /// makes decision 2's "keep the newest within each window" hold when one
    /// data point carries several exemplars for one window.
    fn admit<F>(
        &mut self,
        dp_id: u32,
        limits: &IngestLimits,
        series_id_for: F,
        informational: &mut Vec<Rejection>,
    ) where
        F: Fn(f64) -> SeriesId,
    {
        let Some(rows) = self.rows.get(&dp_id) else {
            return;
        };
        if rows.is_empty() {
            return;
        }

        let mut candidates = Vec::with_capacity(rows.len());
        let mut dropped = 0usize;
        for row in rows {
            match self.build_exemplar(row, limits) {
                Some(candidate) => candidates.push(candidate),
                None => dropped += 1,
            }
        }
        // Stable sort: candidates are built in wire (table) order, so among
        // exemplars sharing one `ts_ns` the first in wire order stays first and
        // is offered to the cap first, making the tie-break deterministic
        // rather than implementation-defined. Descending still holds
        // keep-the-newest.
        candidates.sort_by_key(|c| std::cmp::Reverse(c.ts_ns));

        for candidate in candidates {
            let series_id = series_id_for(f64::from_bits(candidate.value_bits));
            if self.cap.admit(series_id, candidate.ts_ns) {
                self.out.push(NormalizedExemplar {
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

    /// Count exemplar rows whose `parent_id` matches no data-point row in the
    /// table they belong to. Such a row has no series to attach to and no data
    /// point whose own rejection accounts for it, so without this it would be
    /// the one shape of exemplar that still vanishes silently.
    ///
    /// Rows whose parent data point exists but was rejected are deliberately
    /// NOT counted here: they are dropped along with their point, exactly as
    /// `ravel_otlp` drops the exemplars of a point it rejects (its
    /// `build_point` returns before reaching admission). Counting them would
    /// give the same logical input a rejection class OTLP does not produce,
    /// which ADR-0011 forbids. An orphan has no OTLP equivalent at all -- OTLP
    /// exemplars ride inline on their data point and cannot be orphaned -- so
    /// counting it cannot diverge the two paths on any input both can express.
    ///
    /// `dp_ids` is walked only when there are exemplar rows to account for, so
    /// a batch with no exemplars pays nothing for this.
    fn count_orphans(
        &mut self,
        dp_ids: impl Iterator<Item = u32>,
        informational: &mut Vec<Rejection>,
    ) {
        if self.rows.is_empty() {
            return;
        }
        let known: std::collections::HashSet<u32> = dp_ids.collect();
        let orphans: usize = self
            .rows
            .iter()
            .filter(|(parent_id, _)| !known.contains(parent_id))
            .map(|(_, rows)| rows.len())
            .sum();
        if orphans > 0 {
            informational.push(Rejection::HistogramExemplarsDropped { count: orphans });
        }
    }

    /// Turn one decoded row into the shared canonical shape (ADR-0047 decision
    /// 1), joining its attributes from the matching `*_DP_EXEMPLAR_ATTRS` by the
    /// row's own `id`, or `None` if the row carries no value or no timestamp.
    ///
    /// An attribute whose value is complex (map/slice/bytes) becomes an
    /// empty-valued label rather than rejecting the exemplar, matching what
    /// `ravel_otlp::normalize`'s `decode_exemplar` does with an unparsable
    /// filtered-attribute value: filtered attributes are informational context,
    /// never series identity.
    fn build_exemplar(&self, row: &FlatExemplar, limits: &IngestLimits) -> Option<Exemplar> {
        let ts_ns = row.ts_ns?;
        let value = row.value?;

        let attr_range = row
            .id
            .map(|id| attr_range_for(self.attrs, self.attr_order, id))
            .unwrap_or(&[]);
        let filtered_attributes = attr_range
            .iter()
            .take(limits.max_attributes_per_point)
            .map(|&i| {
                let attr = &self.attrs[i as usize];
                Label {
                    name: sanitize_label_name(attr.key),
                    value: raw_cell_value(&raw_cell(attr)).unwrap_or_default(),
                }
            })
            .collect();

        Some(Exemplar {
            ts_ns,
            value_bits: value.to_bits(),
            trace_id: row.trace_id,
            span_id: row.span_id,
            filtered_attributes,
        })
    }
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
    prefix: &[Label],
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
    // Same place as `ravel_otlp::explode_summary`: after the per-quantile
    // validity checks and before `build_base_labels`, since everything below
    // allocates one series per quantile.
    if dp.quantiles.len() > limits.max_summary_quantiles {
        return Err(Rejection::TooManySummaryQuantiles {
            quantiles: dp.quantiles.len(),
            max: limits.max_summary_quantiles,
        });
    }

    let base_labels = build_base_labels(prefix, range, attrs, limits)?;
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Run ravel-otap's real metadata path for a single gauge metric carrying
    /// `unit` and return the unit word it stores. Drives [`build_metadata`], the
    /// same function `normalize_impl` calls, so the word returned here is the
    /// one an ingested OTAP metric would carry, not a re-derived value.
    fn otap_stored_unit_word(unit: &str) -> String {
        let root = vec![Some(RootEntry {
            name: "m".to_string(),
            kind: RootKind::Gauge,
            unit: unit.to_string(),
            description: String::new(),
            resource: ResourceRef::Absent,
        })];
        let decisions = vec![Some(MetricDecision {
            name: "m".to_string(),
            is_sum: false,
            is_monotonic: false,
        })];
        let meta = build_metadata(&root, &decisions, &[None], &[None], &[true]);
        meta.into_iter()
            .next()
            .expect("a produced metric yields one metadata tuple")
            .unit
    }

    /// The unit word ravel-otap stores must be exactly the one ravel-otlp's
    /// canonical `metadata_unit_word` produces: the two surfaces write the same
    /// durable record, so they must agree on every shape the mapping handles.
    ///
    /// This compares ravel-otap's real-path output against ravel-otlp's function
    /// directly (first assert), which catches a private mirror reappearing in
    /// this crate and drifting, and pins ravel-otlp's canonical output to the
    /// expected Prometheus word (second assert), so an edit to that single table
    /// cannot silently change what ravel-otap stores. Covers the annotated,
    /// per-unit, mapped-word, unmapped, and empty shapes.
    #[test]
    fn otap_metadata_unit_word_matches_ravel_otlp_canonical() {
        // (raw unit, the word ravel-otlp's canonical table currently produces
        // for a gauge).
        let cases = [
            ("{packets}", ""),            // annotation-only, stripped to empty
            ("By/s", "bytes_per_second"), // per-unit compound
            ("By", "bytes"),              // simple unit needing a metadata word
            ("furlong", "furlong"),       // no mapping, raw passthrough
            ("", ""),                     // empty
        ];
        for (unit, pinned) in cases {
            let canonical = ravel_otlp::normalize::metadata_unit_word(unit, PromMetricKind::Gauge);
            assert_eq!(
                otap_stored_unit_word(unit),
                canonical,
                "ravel-otap must store ravel-otlp's canonical word for {unit:?}"
            );
            assert_eq!(
                canonical, pinned,
                "ravel-otlp unit table changed for {unit:?}"
            );
        }
    }

    /// The METRICS `metric_type`/`is_monotonic` pair maps to the same Prometheus
    /// kind OTLP's `data` oneof does.
    #[test]
    fn metadata_kind_follows_the_otlp_oneof_rule() {
        assert_eq!(metadata_kind(&RootKind::Gauge), PromMetricKind::Gauge);
        assert_eq!(
            metadata_kind(&RootKind::Sum {
                temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
                is_monotonic: true,
            }),
            PromMetricKind::Counter
        );
        assert_eq!(
            metadata_kind(&RootKind::Sum {
                temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
                is_monotonic: false,
            }),
            PromMetricKind::Gauge
        );
        assert_eq!(
            metadata_kind(&RootKind::Histogram {
                temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
            }),
            PromMetricKind::Histogram
        );
        assert_eq!(metadata_kind(&RootKind::Summary), PromMetricKind::Summary);
        assert_eq!(
            metadata_kind(&RootKind::Unsupported),
            PromMetricKind::Unknown
        );
    }

    /// Deduplication by family name, first occurrence winning, and "no point, no
    /// metadata".
    #[test]
    fn build_metadata_dedups_by_family_and_skips_unproduced() {
        let root = vec![
            Some(RootEntry {
                name: "requests".to_string(),
                kind: RootKind::Sum {
                    temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
                    is_monotonic: true,
                },
                unit: "By".to_string(),
                description: "first help".to_string(),
                resource: ResourceRef::Absent,
            }),
            Some(RootEntry {
                name: "requests".to_string(),
                kind: RootKind::Sum {
                    temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
                    is_monotonic: true,
                },
                unit: "By".to_string(),
                description: "second help".to_string(),
                resource: ResourceRef::Absent,
            }),
            Some(RootEntry {
                name: "silent".to_string(),
                kind: RootKind::Gauge,
                unit: String::new(),
                description: "never stored".to_string(),
                resource: ResourceRef::Absent,
            }),
        ];
        let decisions = vec![
            Some(MetricDecision {
                name: "requests_bytes_total".to_string(),
                is_sum: true,
                is_monotonic: true,
            }),
            Some(MetricDecision {
                name: "requests_bytes_total".to_string(),
                is_sum: true,
                is_monotonic: true,
            }),
            Some(MetricDecision {
                name: "silent".to_string(),
                is_sum: false,
                is_monotonic: false,
            }),
        ];
        let empty_hist: Vec<Option<HistogramDecision>> = vec![None, None, None];
        let empty_summary: Vec<Option<SummaryDecision>> = vec![None, None, None];

        let metadata = build_metadata(
            &root,
            &decisions,
            &empty_hist,
            &empty_summary,
            // The third metric produced no admitted point.
            &[true, true, false],
        );

        assert_eq!(
            metadata,
            vec![MetricMetadata {
                family_name: "requests_bytes_total".to_string(),
                kind: PromMetricKind::Counter,
                help: "first help".to_string(),
                unit: "bytes".to_string(),
            }]
        );
    }

    use crate::encode::{
        AttrRow, AttrValue, DataPointRow, HistogramMetricRow, HistogramPointRow, MetricKind,
        MetricRow, MetricsStreamEncoder, ResourceAssignment, SummaryMetricRow, SummaryPointRow,
    };
    use crate::proto::experimental::arrow::v1::BatchArrowRecords;
    use crate::stream::{StreamConfig, StreamState};
    use arrow::array::{ArrayRef, Int64Builder, StringDictionaryBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use proptest::prelude::*;

    const TS: i64 = 1_700_000_000_000_000_000;

    fn str_attr(key: &str, value: &str) -> AttrRow {
        AttrRow {
            key: key.to_string(),
            value: AttrValue::Str(value.to_string()),
        }
    }

    fn gauge(name: &str, attrs: Vec<AttrRow>) -> MetricRow {
        MetricRow {
            name: name.to_string(),
            kind: MetricKind::Gauge,
            data_points: vec![DataPointRow {
                time_unix_nano: TS,
                value: 1.0,
                flags: 0,
                attrs,
                exemplars: vec![],
            }],
        }
    }

    fn encode(
        encoder: &mut MetricsStreamEncoder,
        metrics: &[MetricRow],
        resources: &[Vec<AttrRow>],
        assigned: &[Option<usize>],
    ) -> BatchArrowRecords {
        encoder
            .encode_batch_resources(
                1,
                metrics,
                &[],
                &[],
                &[],
                &[],
                &[],
                &ResourceAssignment {
                    resources,
                    metrics: assigned,
                    ..ResourceAssignment::default()
                },
            )
            .expect("encode")
    }

    fn run(batch: &DecodedBatch, limits: &IngestLimits) -> NormalizeOutput {
        normalize_decoded(&TenantId::new("acme"), batch, limits, TS)
    }

    /// Each point's labels as sorted `(name, value)` pairs, keyed by `__name__`.
    fn labels_by_name(out: &NormalizeOutput) -> BTreeMap<String, Vec<(String, String)>> {
        out.points
            .iter()
            .map(|p| {
                let pairs: Vec<(String, String)> = p
                    .labels
                    .iter()
                    .map(|l| (l.name.clone(), l.value.clone()))
                    .collect();
                let name = p.labels.get(METRIC_NAME_LABEL).unwrap_or("").to_string();
                (name, pairs)
            })
            .collect()
    }

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    /// The root `resource.id` travels DELTA-encoded (no metadata), and ids are
    /// scoped to one batch: the same id names a different resource in the next.
    #[test]
    fn delta_encoded_resource_ids_resolve_per_batch() {
        let limits = IngestLimits::default();
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let mut state = StreamState::new(StreamConfig::default());
        let resources = vec![
            vec![str_attr("service.name", "a")],
            vec![str_attr("service.name", "b")],
        ];
        let metrics = vec![
            gauge("m0", vec![]),
            gauge("m1", vec![]),
            gauge("m2", vec![]),
        ];
        let first = state
            .decode(encode(
                &mut encoder,
                &metrics,
                &resources,
                &[Some(0), Some(1), Some(0)],
            ))
            .expect("decode");

        let root = payloads_of(&first, ArrowPayloadType::UnivariateMetrics)[0];
        let ids = root
            .column_by_name("resource")
            .and_then(|c| c.as_any().downcast_ref::<StructArray>())
            .and_then(|s| s.column_by_name("id"))
            .and_then(|c| c.as_any().downcast_ref::<UInt16Array>())
            .expect("resource.id column");
        assert_eq!(ids.values().to_vec(), vec![0, 1, u16::MAX], "wire is delta");

        let out = run(&first, &limits);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        let got = labels_by_name(&out);
        assert_eq!(got["m0"], pairs(&[("__name__", "m0"), ("job", "a")]));
        assert_eq!(got["m1"], pairs(&[("__name__", "m1"), ("job", "b")]));
        assert_eq!(got["m2"], pairs(&[("__name__", "m2"), ("job", "a")]));

        let second = state
            .decode(encode(
                &mut encoder,
                &[gauge("m0", vec![])],
                &[vec![str_attr("service.name", "c")]],
                &[Some(0)],
            ))
            .expect("decode");
        let got = labels_by_name(&run(&second, &limits));
        assert_eq!(got["m0"], pairs(&[("__name__", "m0"), ("job", "c")]));
    }

    /// A data-point attrs table with every column dictionary-encoded (u32
    /// `parent_id` as `Dictionary<UInt16, UInt32>`, `key` with `UInt8` keys,
    /// `str` and `int` with `UInt16` keys) yields the same labels as the plain
    /// encoding of the same rows. The encoder's `RESOURCE_ATTRS` already sends
    /// dictionary `key`/`str`, so the resource side is covered too.
    #[test]
    fn dictionary_encoded_keys_and_values_decode() {
        let limits = IngestLimits::default();
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let mut state = StreamState::new(StreamConfig::default());
        let metrics = vec![gauge(
            "m",
            vec![
                str_attr("host", "h1"),
                AttrRow {
                    key: "code".to_string(),
                    value: AttrValue::Int(200),
                },
            ],
        )];
        let mut batch = state
            .decode(encode(
                &mut encoder,
                &metrics,
                &[vec![str_attr("service.name", "svc")]],
                &[Some(0)],
            ))
            .expect("decode");
        let plain = labels_by_name(&run(&batch, &limits));
        assert_eq!(
            plain["m"],
            pairs(&[
                ("__name__", "m"),
                ("code", "200"),
                ("host", "h1"),
                ("job", "svc")
            ])
        );

        let original = payloads_of(&batch, ArrowPayloadType::NumberDpAttrs)[0].clone();
        let parent = original
            .column_by_name("parent_id")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
            .expect("plain parent_id")
            .value(0);

        let parent_ids: ArrayRef = Arc::new(
            DictionaryArray::<UInt16Type>::try_new(
                UInt16Array::from(vec![0, 0]),
                Arc::new(UInt32Array::from(vec![parent])),
            )
            .expect("parent dict"),
        );
        let mut keys = StringDictionaryBuilder::<UInt8Type>::new();
        keys.append_value("host");
        keys.append_value("code");
        let mut strs = StringDictionaryBuilder::<UInt16Type>::new();
        strs.append_value("h1");
        strs.append_null();
        let mut int_values = Int64Builder::new();
        int_values.append_value(200);
        let ints: ArrayRef = Arc::new(
            DictionaryArray::<UInt16Type>::try_new(
                UInt16Array::from(vec![None, Some(0)]),
                Arc::new(int_values.finish()),
            )
            .expect("int dict"),
        );
        let parent_field = original
            .schema()
            .field_with_name("parent_id")
            .expect("parent field")
            .clone();
        let dict = |k: DataType, v: DataType| DataType::Dictionary(Box::new(k), Box::new(v));
        let schema = Schema::new(vec![
            parent_field.with_data_type(dict(DataType::UInt16, DataType::UInt32)),
            Field::new("key", dict(DataType::UInt8, DataType::Utf8), false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", dict(DataType::UInt16, DataType::Utf8), true),
            Field::new("int", dict(DataType::UInt16, DataType::Int64), true),
        ]);
        let dictionary_attrs = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                parent_ids,
                Arc::new(keys.finish()),
                Arc::new(UInt8Array::from(vec![
                    ANY_VALUE_TYPE_STRING,
                    ANY_VALUE_TYPE_INT,
                ])),
                Arc::new(strs.finish()),
                ints,
            ],
        )
        .expect("dictionary attrs batch");
        for (ty, rb) in &mut batch.payloads {
            if *ty == ArrowPayloadType::NumberDpAttrs {
                *rb = dictionary_attrs.clone();
            }
        }
        let out = run(&batch, &limits);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(labels_by_name(&out), plain);
    }

    /// A metric whose `resource` struct is null gets no resource labels, and
    /// the same series id as a batch carrying no resources at all.
    #[test]
    fn null_resource_id_is_an_empty_prefix() {
        let limits = IngestLimits::default();
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let mut state = StreamState::new(StreamConfig::default());
        let metrics = vec![
            gauge("m0", vec![]),
            gauge("m1", vec![str_attr("host", "h")]),
        ];
        let batch = state
            .decode(encode(
                &mut encoder,
                &metrics,
                &[vec![str_attr("service.name", "a")]],
                &[Some(0), None],
            ))
            .expect("decode");
        let out = run(&batch, &limits);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        let got = labels_by_name(&out);
        assert_eq!(got["m0"], pairs(&[("__name__", "m0"), ("job", "a")]));
        assert_eq!(got["m1"], pairs(&[("__name__", "m1"), ("host", "h")]));

        let mut bare_encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let bare = StreamState::new(StreamConfig::default())
            .decode(
                bare_encoder
                    .encode_batch(1, &[gauge("m1", vec![str_attr("host", "h")])])
                    .expect("encode"),
            )
            .expect("decode");
        let bare_out = run(&bare, &limits);
        let id_of = |o: &NormalizeOutput, name: &str| {
            o.points
                .iter()
                .find(|p| p.labels.get(METRIC_NAME_LABEL) == Some(name))
                .map(|p| p.series_id)
                .expect("point present")
        };
        assert_eq!(id_of(&out, "m1"), id_of(&bare_out, "m1"));
    }

    /// `RESOURCE_ATTRS` rows whose `parent_id` no metric references are never
    /// admitted: a complex value or an over-limit attribute count on them
    /// rejects nothing.
    #[test]
    fn orphan_resource_attr_rows_are_ignored() {
        let limits = IngestLimits {
            max_resource_attributes: 1,
            ..IngestLimits::default()
        };
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let mut state = StreamState::new(StreamConfig::default());
        let resources = vec![
            vec![str_attr("service.name", "a")],
            vec![
                str_attr("service.name", "b"),
                AttrRow {
                    key: "blob".to_string(),
                    value: AttrValue::Complex(ANY_VALUE_TYPE_MAP),
                },
            ],
        ];
        let batch = state
            .decode(encode(
                &mut encoder,
                &[gauge("m0", vec![])],
                &resources,
                &[Some(0)],
            ))
            .expect("decode");
        assert_eq!(
            payloads_of(&batch, ArrowPayloadType::ResourceAttrs)[0].num_rows(),
            3
        );
        let out = run(&batch, &limits);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(
            labels_by_name(&out)["m0"],
            pairs(&[("__name__", "m0"), ("job", "a")])
        );
    }

    /// A complex or overlong value rejects the points under its resource only
    /// on a key the builder reads; on any other key it is a dropped attribute.
    #[test]
    fn resource_value_limits_apply_only_to_read_keys() {
        let limits = IngestLimits {
            max_label_value_len: 4,
            ..IngestLimits::default()
        };
        let long = "x".repeat(8);
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let mut state = StreamState::new(StreamConfig::default());
        let resources = vec![
            vec![
                str_attr("service.name", "a"),
                AttrRow {
                    key: "blob".to_string(),
                    value: AttrValue::Complex(ANY_VALUE_TYPE_MAP),
                },
                str_attr("other", &long),
            ],
            vec![AttrRow {
                key: "service.name".to_string(),
                value: AttrValue::Complex(ANY_VALUE_TYPE_MAP),
            }],
            vec![str_attr("service.name", &long)],
        ];
        let batch = state
            .decode(encode(
                &mut encoder,
                &[
                    gauge("m0", vec![]),
                    gauge("m1", vec![]),
                    gauge("m2", vec![]),
                ],
                &resources,
                &[Some(0), Some(1), Some(2)],
            ))
            .expect("decode");
        let out = run(&batch, &limits);
        assert_eq!(
            labels_by_name(&out),
            BTreeMap::from([("m0".to_string(), pairs(&[("__name__", "m0"), ("job", "a")]))])
        );
        let mut rejected = out.rejected.clone();
        rejected.sort_by_key(|r| format!("{r:?}"));
        assert_eq!(
            rejected,
            vec![
                Rejection::Grouped {
                    reason: Box::new(Rejection::ComplexAttributeValue),
                    count: 1,
                },
                Rejection::Grouped {
                    reason: Box::new(Rejection::LabelValueTooLong { len: 8, max: 4 }),
                    count: 1,
                },
                Rejection::ResourceAttributesDropped { count: 2 },
            ]
        );
    }

    /// otap-spec.md section 5.5.1's AnyValue discriminants, which pcommon's
    /// `ValueType` and the Go encoder share. The tables in sections 5.4.1 and
    /// 5.4.2 of the vendored spec list a different order; 5.5.1 is the one the
    /// reference implementation follows.
    #[test]
    fn attribute_type_constants_match_the_spec() {
        assert_eq!(ANY_VALUE_TYPE_EMPTY, 0);
        assert_eq!(ANY_VALUE_TYPE_STRING, 1);
        assert_eq!(ANY_VALUE_TYPE_INT, 2);
        assert_eq!(ANY_VALUE_TYPE_DOUBLE, 3);
        assert_eq!(ANY_VALUE_TYPE_BOOL, 4);
        assert_eq!(ANY_VALUE_TYPE_MAP, 5);
        assert_eq!(ANY_VALUE_TYPE_SLICE, 6);
        assert_eq!(ANY_VALUE_TYPE_BYTES, 7);
    }

    /// A request whose projected resolved-label bytes exceed the bound is
    /// rejected whole, with one `ResolvedLabelBytesExceeded` carrying every
    /// point; one byte more of budget admits all of it. The projection covers
    /// the bytes of the labels the admitted points keep, plus the label sets,
    /// cache keys and resource prefixes held while they were built.
    #[test]
    fn otap_request_over_label_byte_bound_is_rejected_whole() {
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let mut state = StreamState::new(StreamConfig::default());
        let batch = state
            .decode(encode(
                &mut encoder,
                &[
                    gauge("m0", vec![str_attr("host", "h1")]),
                    gauge("m1", vec![str_attr("host", "h2")]),
                ],
                &[vec![str_attr("service.name", "a")]],
                &[Some(0), Some(0)],
            ))
            .expect("decode");
        let projected = project_resolved_label_bytes(&batch, &IngestLimits::default()).get();

        let tight = IngestLimits {
            max_resolved_label_bytes_per_request: projected - 1,
            ..IngestLimits::default()
        };
        let out = run(&batch, &tight);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![Rejection::ResolvedLabelBytesExceeded {
                projected,
                max: projected - 1,
                count: 2,
            }]
        );

        let exact = IngestLimits {
            max_resolved_label_bytes_per_request: projected,
            ..IngestLimits::default()
        };
        held::take();
        let out = run(&batch, &exact);
        let held = held::take();
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 2);
        let kept: usize = out
            .points
            .iter()
            .flat_map(|p| p.labels.iter())
            .map(|l| label_bytes(l.name.len(), l.value.len()))
            .sum();
        assert!(projected >= kept, "projected {projected} < kept {kept}");
        let output = output_label_bytes(&out);
        assert!(held > 0, "the build held its sets, keys and prefix");
        assert!(
            projected >= held + output,
            "projected {projected} < held {held} + output {output}"
        );
    }

    /// Heap bytes of every admitted point's own label set.
    fn output_label_bytes(out: &NormalizeOutput) -> usize {
        out.points
            .iter()
            .map(|p| held::labels(p.labels.len(), p.labels.iter()))
            .sum()
    }

    /// `rb` with column `name` replaced by `column`, keeping the field's
    /// metadata.
    fn with_column(rb: &RecordBatch, name: &str, column: ArrayRef) -> RecordBatch {
        let schema = rb.schema();
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (field, existing) in schema.fields().iter().zip(rb.columns()) {
            if field.name() == name {
                fields.push(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(column.data_type().clone())
                        .with_nullable(true),
                );
                columns.push(column.clone());
            } else {
                fields.push(field.as_ref().clone());
                columns.push(existing.clone());
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("rebuilt batch")
    }

    fn map_payload(
        batch: &mut DecodedBatch,
        payload: ArrowPayloadType,
        f: impl Fn(&RecordBatch) -> RecordBatch,
    ) {
        let mut seen = false;
        for (ty, rb) in &mut batch.payloads {
            if *ty == payload {
                *rb = f(rb);
                seen = true;
            }
        }
        assert!(seen, "batch carries a {payload:?} payload");
    }

    /// `m0` under a resource with `service.name=a` and `m1` under none, one
    /// point each.
    fn resource_and_bare() -> DecodedBatch {
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        StreamState::new(StreamConfig::default())
            .decode(encode(
                &mut encoder,
                &[
                    gauge("m0", vec![]),
                    gauge("m1", vec![str_attr("host", "h")]),
                ],
                &[vec![
                    str_attr("service.name", "a"),
                    str_attr("service.instance.id", "i"),
                ]],
                &[Some(0), None],
            ))
            .expect("decode")
    }

    fn malformed(metric_type: &'static str, count: usize) -> Vec<Rejection> {
        vec![Rejection::UnsupportedMetricType { metric_type, count }]
    }

    /// Exponential-histogram points are rejected on their own and are not in
    /// a resource's point count: two of them under `m0`'s over-cap resource
    /// leave that rejection at `m0`'s one gauge point, where OTLP counts 3.
    #[test]
    fn exp_histogram_points_are_not_in_the_resource_count() {
        let limits = IngestLimits {
            max_resource_attributes: 1,
            ..IngestLimits::default()
        };
        let mut batch = resource_and_bare();
        let exp_points = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "parent_id",
                DataType::UInt16,
                false,
            )])),
            vec![Arc::new(UInt16Array::from(vec![0u16, 0]))],
        )
        .expect("exp histogram points");
        batch
            .payloads
            .push((ArrowPayloadType::ExpHistogramDataPoints, exp_points));
        let out = run(&batch, &limits);
        assert_eq!(
            labels_by_name(&out).keys().collect::<Vec<_>>(),
            ["m1"],
            "{:?}",
            out.rejected
        );
        let mut rejected = out.rejected.clone();
        rejected.sort_by_key(|r| format!("{r:?}"));
        assert_eq!(
            rejected,
            vec![
                Rejection::TooManyResourceAttributes { count: 1, max: 1 },
                Rejection::UnsupportedMetricType {
                    metric_type: "exponential_histogram",
                    count: 2,
                },
            ]
        );
    }

    /// A METRICS `resource` column, or its `id` child, of the wrong type
    /// leaves no row's resource readable, so every point is rejected as
    /// `malformed_resource` rather than written without resource labels.
    #[test]
    fn wrong_typed_resource_column_rejects_every_point() {
        let limits = IngestLimits::default();
        let not_a_struct: ArrayRef = Arc::new(Int32Array::from(vec![0, 0]));
        let string_id: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("id", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["0", "1"])) as ArrayRef,
        )]));
        for column in [not_a_struct, string_id] {
            let mut batch = resource_and_bare();
            map_payload(&mut batch, ArrowPayloadType::UnivariateMetrics, |rb| {
                with_column(rb, "resource", column.clone())
            });
            let out = run(&batch, &limits);
            assert!(out.points.is_empty(), "{:?}", labels_by_name(&out));
            assert_eq!(out.rejected, malformed(MALFORMED_RESOURCE, 2));
        }
    }

    /// A `RESOURCE_ATTRS` batch whose `parent_id`, `key`, `type` or value
    /// column is present and cannot be read rejects every point under a
    /// resource as `malformed_resource`, rather than reading the values as
    /// empty; a point under no resource is unaffected.
    #[test]
    fn unreadable_resource_attrs_reject_points_under_a_resource() {
        let limits = IngestLimits::default();
        let unreadable: [(&str, ArrayRef); 9] = [
            ("parent_id", Arc::new(StringArray::from(vec!["0", "0"]))),
            ("key", Arc::new(Int64Array::from(vec![0, 0]))),
            ("type", Arc::new(StringArray::from(vec!["1", "1"]))),
            (
                "str",
                Arc::new(arrow::array::LargeStringArray::from(vec!["a", "i"])),
            ),
            (
                "str",
                Arc::new(arrow::array::StringViewArray::from(vec!["a", "i"])),
            ),
            (
                "str",
                Arc::new(
                    DictionaryArray::<UInt16Type>::try_new(
                        UInt16Array::from(vec![0, 0]),
                        Arc::new(Int64Array::from(vec![1])),
                    )
                    .expect("non-string str dict"),
                ),
            ),
            (
                "int",
                Arc::new(
                    DictionaryArray::<UInt8Type>::try_new(
                        UInt8Array::from(vec![None, None]),
                        Arc::new(Int32Array::from(vec![7])),
                    )
                    .expect("non-Int64 int dict"),
                ),
            ),
            ("bool", Arc::new(Int32Array::from(vec![None, None]))),
            (
                "double",
                Arc::new(arrow::array::Float32Array::from(vec![None, None])),
            ),
        ];
        for (name, column) in unreadable {
            let shape = format!("{name}: {}", column.data_type());
            let mut batch = resource_and_bare();
            map_payload(&mut batch, ArrowPayloadType::ResourceAttrs, |rb| {
                with_column(rb, name, column.clone())
            });
            let out = run(&batch, &limits);
            assert_eq!(out.rejected, malformed(MALFORMED_RESOURCE, 1), "{shape}");
            let got = labels_by_name(&out);
            assert_eq!(got.len(), 1, "{shape}: {got:?}");
            assert_eq!(got["m1"], pairs(&[("__name__", "m1"), ("host", "h")]));
        }
    }

    /// A `RESOURCE_ATTRS` `parent_id` declaring an encoding this pass does not
    /// recognize rejects every point under a resource as
    /// `unrecognized_id_encoding`, never guessed as plain or as no resource.
    #[test]
    fn unrecognized_resource_attrs_encoding_rejects_points_under_a_resource() {
        let limits = IngestLimits::default();
        let mut batch = resource_and_bare();
        map_payload(&mut batch, ArrowPayloadType::ResourceAttrs, |rb| {
            let schema = rb.schema();
            let fields: Vec<Field> = schema
                .fields()
                .iter()
                .map(|f| {
                    let mut f = f.as_ref().clone();
                    if f.name() == "parent_id" {
                        let mut metadata = f.metadata().clone();
                        metadata.insert("encoding".to_string(), "bogus".to_string());
                        f = f.with_metadata(metadata);
                    }
                    f
                })
                .collect();
            RecordBatch::try_new(Arc::new(Schema::new(fields)), rb.columns().to_vec())
                .expect("rebuilt batch")
        });
        let out = run(&batch, &limits);
        assert_eq!(out.rejected, malformed(UNRECOGNIZED_ID_ENCODING, 1));
        let got = labels_by_name(&out);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got["m1"], pairs(&[("__name__", "m1"), ("host", "h")]));
    }

    /// A dictionary-encoded `RESOURCE_ATTRS` `parent_id` reads like the plain
    /// column, and one with a null slot is `malformed_resource` instead of
    /// parent 0.
    #[test]
    fn null_dictionary_resource_parent_is_rejected() {
        let limits = IngestLimits::default();
        let dictionary = |null_slot: bool| {
            move |rb: &RecordBatch| {
                let plain = rb
                    .column_by_name("parent_id")
                    .and_then(|c| c.as_any().downcast_ref::<UInt16Array>())
                    .expect("plain u16 parent_id");
                let values: Vec<u32> = plain.values().iter().map(|&v| u32::from(v)).collect();
                let keys: Vec<Option<u8>> = (0..values.len())
                    .map(|i| (!(null_slot && i == 1)).then_some(i as u8))
                    .collect();
                let column: ArrayRef = Arc::new(
                    DictionaryArray::<UInt8Type>::try_new(
                        UInt8Array::from(keys),
                        Arc::new(UInt32Array::from(values)),
                    )
                    .expect("parent dict"),
                );
                with_column(rb, "parent_id", column)
            }
        };

        let mut batch = resource_and_bare();
        map_payload(
            &mut batch,
            ArrowPayloadType::ResourceAttrs,
            dictionary(false),
        );
        let out = run(&batch, &limits);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(
            labels_by_name(&out)["m0"],
            pairs(&[("__name__", "m0"), ("instance", "i"), ("job", "a")])
        );

        let mut batch = resource_and_bare();
        map_payload(
            &mut batch,
            ArrowPayloadType::ResourceAttrs,
            dictionary(true),
        );
        let out = run(&batch, &limits);
        assert_eq!(out.rejected, malformed(MALFORMED_RESOURCE, 1));
        assert_eq!(labels_by_name(&out).len(), 1);
    }

    /// `field` with its `encoding` metadata set to `encoding`.
    fn with_encoding(field: &Field, encoding: &str) -> Field {
        let mut metadata = field.metadata().clone();
        metadata.insert("encoding".to_string(), encoding.to_string());
        field.clone().with_metadata(metadata)
    }

    /// A null slot in a plain `UInt16` `RESOURCE_ATTRS.parent_id` is
    /// `malformed_resource` under every encoding, rather than the buffer value
    /// (plain) or the previous row's parent (delta, quasi-delta).
    #[test]
    fn null_plain_resource_parent_is_rejected() {
        let limits = IngestLimits::default();
        for encoding in ["plain", "delta", "quasidelta"] {
            let mut batch = resource_and_bare();
            map_payload(&mut batch, ArrowPayloadType::ResourceAttrs, |rb| {
                let rb = with_column(
                    rb,
                    "parent_id",
                    Arc::new(UInt16Array::from(vec![Some(0), None])),
                );
                let schema = rb.schema();
                let fields: Vec<Field> = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        if f.name() == "parent_id" {
                            with_encoding(f, encoding)
                        } else {
                            f.as_ref().clone()
                        }
                    })
                    .collect();
                RecordBatch::try_new(Arc::new(Schema::new(fields)), rb.columns().to_vec())
                    .expect("rebuilt batch")
            });
            let out = run(&batch, &limits);
            assert_eq!(out.rejected, malformed(MALFORMED_RESOURCE, 1), "{encoding}");
            let got = labels_by_name(&out);
            assert_eq!(got.len(), 1, "{encoding}: {got:?}");
            assert_eq!(got["m1"], pairs(&[("__name__", "m1"), ("host", "h")]));
        }
    }

    /// An unrecognized encoding on the METRICS `resource.id` child rejects the
    /// points under a resource once, counted in points, and keeps the metric
    /// with no resource.
    #[test]
    fn unrecognized_resource_id_encoding_rejects_points_under_a_resource() {
        let limits = IngestLimits::default();
        let mut m0 = gauge("m0", vec![]);
        m0.data_points = (0..3)
            .map(|i| DataPointRow {
                time_unix_nano: TS + i,
                value: 1.0,
                flags: 0,
                attrs: vec![],
                exemplars: vec![],
            })
            .collect();
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let mut batch = StreamState::new(StreamConfig::default())
            .decode(encode(
                &mut encoder,
                &[
                    m0,
                    gauge("m1", vec![str_attr("host", "h")]),
                    gauge("m2", vec![]),
                ],
                &[vec![str_attr("service.name", "a")]],
                &[Some(0), None, Some(0)],
            ))
            .expect("decode");
        map_payload(&mut batch, ArrowPayloadType::UnivariateMetrics, |rb| {
            let resource = rb
                .column_by_name("resource")
                .and_then(|c| c.as_any().downcast_ref::<StructArray>())
                .expect("resource struct");
            let fields: Vec<Field> = resource
                .fields()
                .iter()
                .map(|f| with_encoding(f, "bogus"))
                .collect();
            let column: ArrayRef = Arc::new(
                StructArray::try_new(
                    fields.into(),
                    resource.columns().to_vec(),
                    resource.nulls().cloned(),
                )
                .expect("resource struct"),
            );
            with_column(rb, "resource", column)
        });
        let out = run(&batch, &limits);
        assert_eq!(out.rejected, malformed(UNRECOGNIZED_ID_ENCODING, 4));
        assert_eq!(out.points.len(), 1);
        assert_eq!(
            labels_by_name(&out)["m1"],
            pairs(&[("__name__", "m1"), ("host", "h")])
        );
    }

    fn short_str() -> impl Strategy<Value = String> {
        prop_oneof![Just(String::new()), "[a-z]{1,3}", "[a-z]{20,40}"]
    }

    fn attr_row() -> impl Strategy<Value = AttrRow> {
        let key = prop_oneof![
            Just("a"),
            Just("b"),
            Just("c"),
            Just("job"),
            Just("host.name")
        ];
        let value = prop_oneof![
            3 => short_str().prop_map(AttrValue::Str),
            1 => (0i64..3).prop_map(AttrValue::Int),
            1 => any::<bool>().prop_map(AttrValue::Bool),
        ];
        (key, value).prop_map(|(key, value)| AttrRow {
            key: key.to_string(),
            value,
        })
    }

    fn resource_rows() -> impl Strategy<Value = Vec<AttrRow>> {
        let key = prop_oneof![
            Just("service.name"),
            Just("service.namespace"),
            Just("service.instance.id"),
            Just("host.name"),
            Just("other"),
        ];
        proptest::collection::vec(
            (key, short_str()).prop_map(|(key, v)| str_attr(key, &v)),
            0..5,
        )
    }

    /// The kind of a generated metric: a gauge, or a classic histogram or
    /// summary with this many bounds or quantiles per point.
    #[derive(Clone, Copy, Debug)]
    enum ShapeKind {
        Gauge,
        Histogram(usize),
        Summary(usize),
    }

    const SHAPE_BOUNDS: [f64; 4] = [0.25, 10.0, 1234.5678, 1e21];
    const SHAPE_QUANTILES: [f64; 3] = [0.5, 0.9, 0.999];

    /// Per metric: its points' attribute rows, its resource slot, and its kind.
    type MetricShape = (Vec<Vec<AttrRow>>, Option<usize>, ShapeKind);

    fn metric_shapes() -> impl Strategy<Value = (Vec<Vec<AttrRow>>, Vec<MetricShape>)> {
        let point = proptest::collection::vec(attr_row(), 0..4);
        let kind = prop_oneof![
            Just(ShapeKind::Gauge),
            (0..=SHAPE_BOUNDS.len()).prop_map(ShapeKind::Histogram),
            (0..=SHAPE_QUANTILES.len()).prop_map(ShapeKind::Summary),
        ];
        let metric = (
            proptest::collection::vec(point, 1..6),
            proptest::option::of(0usize..3),
            kind,
        );
        (
            proptest::collection::vec(resource_rows(), 3),
            proptest::collection::vec(metric, 1..4),
        )
    }

    /// Encode `shapes` under `resources`, each metric in its kind's table.
    fn encode_shapes(resources: &[Vec<AttrRow>], shapes: &[MetricShape]) -> DecodedBatch {
        let mut metrics = Vec::new();
        let mut histograms = Vec::new();
        let mut summaries = Vec::new();
        let (mut metric_slots, mut histogram_slots, mut summary_slots) =
            (Vec::new(), Vec::new(), Vec::new());
        for (m, (points, resource, kind)) in shapes.iter().enumerate() {
            let name = format!("m{m}");
            match *kind {
                ShapeKind::Gauge => {
                    metrics.push(MetricRow {
                        name,
                        kind: MetricKind::Gauge,
                        data_points: points
                            .iter()
                            .map(|attrs| DataPointRow {
                                time_unix_nano: TS,
                                value: 1.0,
                                flags: 0,
                                attrs: attrs.clone(),
                                exemplars: vec![],
                            })
                            .collect(),
                    });
                    metric_slots.push(*resource);
                }
                ShapeKind::Histogram(bounds) => {
                    histograms.push(HistogramMetricRow {
                        name,
                        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
                        data_points: points
                            .iter()
                            .map(|attrs| HistogramPointRow {
                                time_unix_nano: TS,
                                count: bounds as u64 + 1,
                                sum: Some(1.0),
                                bucket_counts: vec![1; bounds + 1],
                                explicit_bounds: SHAPE_BOUNDS[..bounds].to_vec(),
                                flags: 0,
                                min: None,
                                max: None,
                                exemplars: vec![],
                                attrs: attrs.clone(),
                            })
                            .collect(),
                    });
                    histogram_slots.push(*resource);
                }
                ShapeKind::Summary(quantiles) => {
                    summaries.push(SummaryMetricRow {
                        name,
                        data_points: points
                            .iter()
                            .map(|attrs| SummaryPointRow {
                                time_unix_nano: TS,
                                count: 1,
                                sum: 1.0,
                                quantiles: SHAPE_QUANTILES[..quantiles]
                                    .iter()
                                    .map(|&q| (q, 1.0))
                                    .collect(),
                                flags: 0,
                                attrs: attrs.clone(),
                            })
                            .collect(),
                    });
                    summary_slots.push(*resource);
                }
            }
        }
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let records = encoder
            .encode_batch_resources(
                1,
                &metrics,
                &[],
                &histograms,
                &[],
                &summaries,
                &[],
                &ResourceAssignment {
                    resources,
                    metrics: &metric_slots,
                    histograms: &histogram_slots,
                    summaries: &summary_slots,
                },
            )
            .expect("encode");
        StreamState::new(StreamConfig::default())
            .decode(records)
            .expect("decode")
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The projection covers every byte the build holds at once: the
        /// admitted points' label sets, plus the resource prefixes and
        /// whichever phase holds more, the group cache's keys and sets or the
        /// histogram and summary memos' copied sets, measured from their real
        /// capacities.
        #[test]
        fn projection_covers_held_and_output_label_bytes(
            (resources, shapes) in metric_shapes(),
        ) {
            let limits = IngestLimits {
                resource_attribute_allowlist: vec!["host.name".to_string()],
                ..IngestLimits::default()
            };
            let batch = encode_shapes(&resources, &shapes);

            let projected = project_resolved_label_bytes(&batch, &limits).get();
            held::take();
            let out = run(&batch, &limits);
            let held = held::take();
            let output = output_label_bytes(&out);
            prop_assert!(
                projected >= held + output,
                "projected {} < held {} + output {}",
                projected,
                held,
                output
            );
        }

        /// The length-only plan rejects a resource exactly when the builder
        /// does, with the same reason, and its byte figure covers the prefix
        /// the builder returns.
        #[test]
        fn resource_plan_agrees_with_the_builder(
            rows in proptest::collection::vec(
                (
                    prop_oneof![
                        Just("service.name"),
                        Just("service.namespace"),
                        Just("service.instance.id"),
                        Just("host.name"),
                        Just("h\u{e9}st"),
                    ],
                    prop_oneof![
                        3 => "[a-z]{0,12}".prop_map(Some),
                        1 => Just(None),
                    ],
                ),
                0..6,
            ),
        ) {
            let limits = IngestLimits {
                resource_attribute_allowlist: vec!["host.name".to_string(), "h\u{e9}st".to_string()],
                max_label_value_len: 8,
                max_label_name_len: 4,
                ..IngestLimits::default()
            };
            let pairs: Vec<(&str, ResourceAttrValue<'_>)> = rows
                .iter()
                .map(|(k, v)| {
                    let value = v.as_deref().map_or(ResourceAttrValue::Complex, ResourceAttrValue::Str);
                    (*k, value)
                })
                .collect();
            let mut built = Vec::new();
            let labels = admit_resource_labels(&pairs, 3, &limits, &mut built);
            match check_resource_labels(&pairs, &limits) {
                Err(reason) => {
                    prop_assert!(labels.is_none());
                    prop_assert_eq!(
                        built,
                        vec![Rejection::Grouped { reason: Box::new(reason), count: 3 }]
                    );
                }
                Ok(()) => {
                    let labels = labels.expect("the plan admitted it, so must the builder");
                    let actual: usize = labels
                        .iter()
                        .map(|l| label_bytes(l.name.len(), l.value.len()))
                        .sum();
                    let planned = resource_prefix_bytes(&pairs, &limits).unwrap_or(0);
                    prop_assert!(planned >= actual, "planned {} < built {}", planned, actual);
                }
            }
        }
    }
}
