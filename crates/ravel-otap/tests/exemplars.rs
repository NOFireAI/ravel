//! Exemplar admission on the OTAP path (ADR-0047 decisions 1 and 2, issue
//! #473): the `HISTOGRAM_DP_EXEMPLARS` table's own columns are carried into
//! `ravel_types::Exemplar` values, capped at one per series per window by the
//! caller's `ExemplarCap`, and whatever the cap turns away keeps incrementing
//! the pre-existing `HistogramExemplarsDropped` counter.
//!
//! Most cases go through the test encoder, so they exercise the real Arrow IPC
//! round trip. The ones a fixed encoder schema cannot express (a wrong-width
//! `trace_id` column, a missing `time_unix_nano` column) build their
//! `RecordBatch` by hand, the same way `differential.rs` does.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{
    BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use proptest::prelude::*;

use ravel_otap::encode::{
    AttrRow, AttrValue, ExemplarRow, ExemplarValue, HistogramMetricRow, HistogramPointRow,
    MetricsStreamEncoder,
};
use ravel_otap::normalize::{
    AGGREGATION_TEMPORALITY_CUMULATIVE, METRIC_TYPE_HISTOGRAM, normalize_decoded,
    normalize_decoded_with_exemplars,
};
use ravel_otap::proto::experimental::arrow::v1::ArrowPayloadType;
use ravel_otap::stream::{DecodedBatch, StreamConfig, StreamState};
use ravel_otlp::normalize::MetricsNormalizeResult;
use ravel_otlp::{IngestLimits, Rejection};
use ravel_types::{ExemplarCap, Label, SeriesId, TenantId};

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;

const TRACE_ID: [u8; 16] = [
    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
];
const SPAN_ID: [u8; 8] = [0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10];

fn tenant() -> TenantId {
    TenantId::new("acme")
}

/// One cumulative histogram metric with a single data point carrying
/// `exemplars`. `explicit_bounds` empty means one `+Inf` bucket, so every
/// exemplar attaches to the same bucket series and the per-series cap is what
/// decides admission rather than the bucket split.
fn histogram_with(bounds: Vec<f64>, exemplars: Vec<ExemplarRow>) -> Vec<HistogramMetricRow> {
    vec![HistogramMetricRow {
        name: "latency".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        data_points: vec![HistogramPointRow {
            time_unix_nano: INGEST_TS_NS,
            count: 4,
            sum: Some(4.0),
            bucket_counts: vec![1; bounds.len() + 1],
            explicit_bounds: bounds,
            flags: 0,
            min: None,
            max: None,
            exemplars,
            attrs: vec![],
        }],
    }]
}

fn exemplar_row(ts_ns: i64, value: f64, trace_id: Option<[u8; 16]>) -> ExemplarRow {
    ExemplarRow {
        time_unix_nano: ts_ns,
        value: ExemplarValue::Double(value),
        trace_id,
        span_id: None,
        attrs: vec![],
    }
}

/// Encode `histograms`, run them through the real IPC decode, and normalize
/// with the caller's cap. A fresh encoder per call keeps each test's schema
/// generation independent, exactly as `differential.rs` does.
fn normalize(histograms: &[HistogramMetricRow], cap: &mut ExemplarCap) -> MetricsNormalizeResult {
    let mut encoder = MetricsStreamEncoder::new("exemplars").expect("new encoder");
    let raw = encoder
        .encode_batch_ext(0, &[], histograms, &[])
        .expect("encode batch");
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw).expect("decode batch");
    normalize_decoded_with_exemplars(
        &tenant(),
        &decoded,
        &IngestLimits::default(),
        INGEST_TS_NS,
        cap,
    )
}

fn dropped_counts(rejected: &[Rejection]) -> Vec<usize> {
    rejected
        .iter()
        .filter_map(|r| match r {
            Rejection::HistogramExemplarsDropped { count } => Some(*count),
            _ => None,
        })
        .collect()
}

#[test]
fn exemplar_with_well_formed_ids_is_carried_with_them_decoded_byte_for_byte() {
    let mut cap = ExemplarCap::default();
    let result = normalize(
        &histogram_with(
            vec![],
            vec![ExemplarRow {
                time_unix_nano: INGEST_TS_NS,
                value: ExemplarValue::Double(2.5),
                trace_id: Some(TRACE_ID),
                span_id: Some(SPAN_ID),
                attrs: vec![AttrRow {
                    key: "http.status".to_string(),
                    value: AttrValue::Int(500),
                }],
            }],
        ),
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 1);
    let carried = &result.exemplars[0];
    assert_eq!(carried.exemplar.trace_id, TRACE_ID);
    assert_eq!(carried.exemplar.span_id, SPAN_ID);
    assert_eq!(carried.exemplar.ts_ns, INGEST_TS_NS);
    assert_eq!(carried.exemplar.value_bits, 2.5f64.to_bits());
    // Exemplar attributes come from the HISTOGRAM_DP_EXEMPLAR_ATTRS table,
    // joined by the exemplar's own id, and are sanitized like any other
    // attribute name.
    assert_eq!(
        carried.exemplar.filtered_attributes,
        vec![Label {
            name: "http_status".to_string(),
            value: "500".to_string(),
        }]
    );
    // A bounds-less histogram explodes into `_bucket{le="+Inf"}`, `_sum`, and
    // `_count`; the exemplar attaches to the bucket series, which is first.
    assert_eq!(carried.series_id, result.output.points[0].series_id);
    assert!(dropped_counts(&result.output.rejected).is_empty());
}

#[test]
fn two_exemplars_for_one_series_in_one_window_admit_the_newest_and_count_one_dropped() {
    let mut cap = ExemplarCap::default();
    let result = normalize(
        &histogram_with(
            vec![],
            vec![
                exemplar_row(INGEST_TS_NS, 1.0, Some(TRACE_ID)),
                exemplar_row(INGEST_TS_NS + 1_000_000_000, 2.0, None),
            ],
        ),
        &mut cap,
    );

    // One second apart, so both land in the same 10 s window on the same
    // (single-bucket) series: the cap keeps the newest.
    assert_eq!(result.exemplars.len(), 1);
    assert_eq!(
        result.exemplars[0].exemplar.ts_ns,
        INGEST_TS_NS + 1_000_000_000
    );
    assert_eq!(dropped_counts(&result.output.rejected), vec![1]);
}

#[test]
fn two_exemplars_for_one_series_in_different_windows_are_both_admitted() {
    let mut cap = ExemplarCap::default();
    let window = ExemplarCap::DEFAULT_WINDOW_NS;
    // Anchor on a window boundary so the two timestamps cannot share one
    // window regardless of where INGEST_TS_NS itself falls.
    let base = INGEST_TS_NS - INGEST_TS_NS.rem_euclid(window);
    let result = normalize(
        &histogram_with(
            vec![],
            vec![
                exemplar_row(base, 1.0, None),
                exemplar_row(base + window, 2.0, None),
            ],
        ),
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 2);
    let mut carried: Vec<i64> = result.exemplars.iter().map(|e| e.exemplar.ts_ns).collect();
    carried.sort_unstable();
    assert_eq!(carried, vec![base, base + window]);
    assert!(dropped_counts(&result.output.rejected).is_empty());
}

#[test]
fn absent_trace_and_span_ids_read_back_all_zero_with_the_exemplar_intact() {
    let mut cap = ExemplarCap::default();
    let result = normalize(
        &histogram_with(vec![], vec![exemplar_row(INGEST_TS_NS, 3.5, None)]),
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 1);
    let carried = &result.exemplars[0].exemplar;
    assert_eq!(carried.trace_id, [0u8; 16]);
    assert_eq!(carried.span_id, [0u8; 8]);
    assert_eq!(carried.ts_ns, INGEST_TS_NS);
    assert_eq!(carried.value_bits, 3.5f64.to_bits());
    assert!(dropped_counts(&result.output.rejected).is_empty());
}

#[test]
fn an_exemplar_with_no_value_column_set_is_dropped_and_counted() {
    let mut cap = ExemplarCap::default();
    let result = normalize(
        &histogram_with(
            vec![],
            vec![ExemplarRow {
                time_unix_nano: INGEST_TS_NS,
                value: ExemplarValue::Absent,
                trace_id: Some(TRACE_ID),
                span_id: None,
                attrs: vec![],
            }],
        ),
        &mut cap,
    );

    // OTLP itself calls an exemplar with no value invalid, and the OTLP path
    // drops-and-counts it; this mirrors that rather than storing a made-up
    // value.
    assert!(result.exemplars.is_empty());
    assert_eq!(dropped_counts(&result.output.rejected), vec![1]);
}

#[test]
fn an_int_valued_exemplar_is_carried_as_the_equivalent_f64() {
    let mut cap = ExemplarCap::default();
    let result = normalize(
        &histogram_with(
            vec![],
            vec![ExemplarRow {
                time_unix_nano: INGEST_TS_NS,
                value: ExemplarValue::Int(7),
                trace_id: None,
                span_id: None,
                attrs: vec![],
            }],
        ),
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 1);
    assert_eq!(result.exemplars[0].exemplar.value_bits, 7.0f64.to_bits());
}

#[test]
fn exemplar_attaches_to_the_bucket_series_matching_its_value() {
    let mut cap = ExemplarCap::default();
    // Bounds 1 and 10: value 5 belongs to the `le="10"` bucket, the smallest
    // bound at or above it, matching Prometheus's own convention and the OTLP
    // path.
    let result = normalize(
        &histogram_with(
            vec![1.0, 10.0],
            vec![exemplar_row(INGEST_TS_NS, 5.0, Some(TRACE_ID))],
        ),
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 1);
    let expected = expect_bucket_series_id("10");
    assert_eq!(result.exemplars[0].series_id, expected);
    // Sanity: that id really is one of the admitted points, and not the
    // `le="1"` bucket next to it.
    assert!(result.output.points.iter().any(|p| p.series_id == expected));
    assert_ne!(expected, expect_bucket_series_id("1"));
}

fn expect_bucket_series_id(le: &str) -> SeriesId {
    let labels = vec![Label {
        name: "le".to_string(),
        value: le.to_string(),
    }];
    SeriesId::compute(
        &tenant(),
        "latency_bucket",
        &ravel_types::LabelSet::new(
            labels
                .into_iter()
                .chain(std::iter::once(Label {
                    name: ravel_types::METRIC_NAME_LABEL.to_string(),
                    value: "latency_bucket".to_string(),
                }))
                .collect(),
        )
        .expect("valid label set"),
    )
    .expect("series id")
}

#[test]
fn the_cap_spans_batches_when_the_caller_owns_it() {
    let mut cap = ExemplarCap::default();
    let first = normalize(
        &histogram_with(vec![], vec![exemplar_row(INGEST_TS_NS, 1.0, None)]),
        &mut cap,
    );
    // A separate batch, same series, same window: only a cap that outlives one
    // batch can see the collision, which is why it is the caller's (ADR-0047
    // decision 2).
    let second = normalize(
        &histogram_with(
            vec![],
            vec![exemplar_row(INGEST_TS_NS + 1_000_000, 2.0, None)],
        ),
        &mut cap,
    );

    assert_eq!(first.exemplars.len(), 1);
    assert!(second.exemplars.is_empty());
    assert_eq!(dropped_counts(&second.output.rejected), vec![1]);
}

#[test]
fn the_legacy_entry_point_still_reports_a_cap_based_drop_count() {
    let histograms = histogram_with(
        vec![],
        vec![
            exemplar_row(INGEST_TS_NS, 1.0, Some(TRACE_ID)),
            exemplar_row(INGEST_TS_NS + 1, 2.0, None),
        ],
    );
    let mut encoder = MetricsStreamEncoder::new("exemplars-legacy").expect("new encoder");
    let raw = encoder
        .encode_batch_ext(0, &[], &histograms, &[])
        .expect("encode batch");
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw).expect("decode batch");
    let out = normalize_decoded(&tenant(), &decoded, &IngestLimits::default(), INGEST_TS_NS);

    // Two same-window exemplars, one admitted (and then discarded, since this
    // entry point has nowhere to put it) and one turned away by the cap.
    assert_eq!(dropped_counts(&out.rejected), vec![1]);
}

// --- hand-built batches: shapes the encoder's fixed schema cannot express ---

fn histogram_root_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        plain_field("id", DataType::UInt16),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("aggregation_temporality", DataType::Int32, true),
        Field::new("is_monotonic", DataType::Boolean, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt16Array::from(vec![0u16])),
            Arc::new(UInt8Array::from(vec![METRIC_TYPE_HISTOGRAM])),
            Arc::new(StringArray::from(vec!["latency"])),
            Arc::new(Int32Array::from(vec![Some(
                AGGREGATION_TEMPORALITY_CUMULATIVE,
            )])),
            Arc::new(BooleanArray::from(vec![None::<bool>])),
        ],
    )
    .expect("build root batch")
}

/// One histogram data point, id 0, a single `+Inf` bucket.
fn histogram_dp_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        plain_field("id", DataType::UInt32),
        plain_field("parent_id", DataType::UInt16),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("count", DataType::UInt64, false),
        Field::new(
            "bucket_counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        ),
        Field::new(
            "explicit_bounds",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        ),
    ]);
    let buckets = ListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt64, true)),
        OffsetBuffer::from_lengths([1usize]),
        Arc::new(UInt64Array::from(vec![1u64])),
        None,
    )
    .expect("build bucket list");
    // No explicit bounds: one `+Inf` bucket, so every exemplar on this point
    // attaches to the same bucket series.
    let bounds = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        OffsetBuffer::from_lengths([0usize]),
        Arc::new(Float64Array::from(Vec::<f64>::new())),
        None,
    )
    .expect("build bounds list");
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(vec![0u32])),
            Arc::new(UInt16Array::from(vec![0u16])),
            Arc::new(TimestampNanosecondArray::from(vec![INGEST_TS_NS])),
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(buckets),
            Arc::new(bounds),
        ],
    )
    .expect("build histogram dp batch")
}

fn plain_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false).with_metadata(
        [("encoding".to_string(), "plain".to_string())]
            .into_iter()
            .collect(),
    )
}

/// An exemplar batch whose `trace_id` column is `FixedSizeBinary(width)`:
/// `width` other than 16 is a malformed producer, not something this pass may
/// panic on.
fn exemplar_batch_with_trace_id_width(width: i32) -> RecordBatch {
    let schema = Schema::new(vec![
        plain_field("parent_id", DataType::UInt32),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("double_value", DataType::Float64, true),
        Field::new("trace_id", DataType::FixedSizeBinary(width), true),
    ]);
    let bytes = vec![0xabu8; width as usize];
    let trace_ids =
        FixedSizeBinaryArray::try_from_sparse_iter_with_size([Some(bytes)].into_iter(), width)
            .expect("build trace id column");
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(vec![0u32])),
            Arc::new(TimestampNanosecondArray::from(vec![INGEST_TS_NS])),
            Arc::new(Float64Array::from(vec![Some(1.5)])),
            Arc::new(trace_ids),
        ],
    )
    .expect("build exemplar batch")
}

fn normalize_custom(
    payloads: Vec<(ArrowPayloadType, RecordBatch)>,
    cap: &mut ExemplarCap,
) -> MetricsNormalizeResult {
    let decoded = DecodedBatch {
        batch_id: 0,
        payloads,
    };
    normalize_decoded_with_exemplars(
        &tenant(),
        &decoded,
        &IngestLimits::default(),
        INGEST_TS_NS,
        cap,
    )
}

#[test]
fn a_wrong_width_trace_id_column_yields_an_all_zero_id_and_keeps_the_exemplar() {
    let mut cap = ExemplarCap::default();
    let result = normalize_custom(
        vec![
            (ArrowPayloadType::UnivariateMetrics, histogram_root_batch()),
            (ArrowPayloadType::HistogramDataPoints, histogram_dp_batch()),
            (
                ArrowPayloadType::HistogramDpExemplars,
                exemplar_batch_with_trace_id_width(8),
            ),
        ],
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 1);
    let carried = &result.exemplars[0].exemplar;
    // Wrong width means "absent", the same convention a wrong-length trace id
    // on the OTLP wire takes; the rest of the exemplar is intact and nothing
    // is rejected over it.
    assert_eq!(carried.trace_id, [0u8; 16]);
    assert_eq!(carried.ts_ns, INGEST_TS_NS);
    assert_eq!(carried.value_bits, 1.5f64.to_bits());
    assert!(dropped_counts(&result.output.rejected).is_empty());
}

#[test]
fn a_correct_width_trace_id_column_still_decodes_after_the_wrong_width_case() {
    let mut cap = ExemplarCap::default();
    let result = normalize_custom(
        vec![
            (ArrowPayloadType::UnivariateMetrics, histogram_root_batch()),
            (ArrowPayloadType::HistogramDataPoints, histogram_dp_batch()),
            (
                ArrowPayloadType::HistogramDpExemplars,
                exemplar_batch_with_trace_id_width(16),
            ),
        ],
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 1);
    assert_eq!(result.exemplars[0].exemplar.trace_id, [0xabu8; 16]);
}

/// An exemplar table with no `time_unix_nano` column at all: the column is
/// optional in the data model, and without it there is nothing to place the
/// exemplar in a cap window, so it is dropped and counted rather than stored
/// at a guessed time.
#[test]
fn an_exemplar_with_no_timestamp_column_is_dropped_and_counted() {
    let schema = Schema::new(vec![
        plain_field("parent_id", DataType::UInt32),
        Field::new("double_value", DataType::Float64, true),
    ]);
    let exemplars = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(vec![0u32])),
            Arc::new(Float64Array::from(vec![Some(1.5)])),
        ],
    )
    .expect("build exemplar batch");

    let mut cap = ExemplarCap::default();
    let result = normalize_custom(
        vec![
            (ArrowPayloadType::UnivariateMetrics, histogram_root_batch()),
            (ArrowPayloadType::HistogramDataPoints, histogram_dp_batch()),
            (ArrowPayloadType::HistogramDpExemplars, exemplars),
        ],
        &mut cap,
    );

    assert!(result.exemplars.is_empty());
    assert_eq!(dropped_counts(&result.output.rejected), vec![1]);
}

/// A field with no `encoding` metadata at all, so the `id`/`parent_id`
/// encoding falls to its spec default: DELTA on the core id chain (of which
/// `HISTOGRAM_DP_EXEMPLARS.id` is one).
fn default_encoding_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

/// A `HISTOGRAM_DP_EXEMPLARS` batch of two rows, both `parent_id = 0`, with an
/// explicit `id` column and per-row timestamps. `id_encoding` names the field
/// metadata for the `id` column (`None` = no metadata, so the DELTA default).
fn exemplar_batch_with_ids(
    ids: Vec<Option<u32>>,
    times: Vec<i64>,
    values: Vec<f64>,
    id_encoding: Option<&str>,
) -> RecordBatch {
    let id_field = match id_encoding {
        // Nullable field (the `id` column is optional), carrying the requested
        // encoding metadata.
        Some(enc) => default_encoding_field("id", DataType::UInt32).with_metadata(
            [("encoding".to_string(), enc.to_string())]
                .into_iter()
                .collect(),
        ),
        None => default_encoding_field("id", DataType::UInt32),
    };
    let schema = Schema::new(vec![
        plain_field("parent_id", DataType::UInt32),
        id_field,
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("double_value", DataType::Float64, true),
    ]);
    let n = ids.len();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(vec![0u32; n])),
            Arc::new(UInt32Array::from(ids)),
            Arc::new(TimestampNanosecondArray::from(times)),
            Arc::new(Float64Array::from(
                values.into_iter().map(Some).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build exemplar batch with ids")
}

/// A `HISTOGRAM_DP_EXEMPLAR_ATTRS` batch: one int-valued attribute per row,
/// joined to the exemplar whose `id` equals the row's `parent_id`. `parent_id`
/// is `plain`-encoded so the join key is literal.
fn exemplar_attrs_batch(rows: Vec<(u32, &str, i64)>) -> RecordBatch {
    let schema = Schema::new(vec![
        plain_field("parent_id", DataType::UInt32),
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::UInt8, true),
        Field::new("int", DataType::Int64, true),
    ]);
    let parents: Vec<u32> = rows.iter().map(|(p, _, _)| *p).collect();
    let keys: Vec<&str> = rows.iter().map(|(_, k, _)| *k).collect();
    let types: Vec<u8> = rows.iter().map(|_| 2u8).collect(); // ANY_VALUE_TYPE_INT
    let ints: Vec<i64> = rows.iter().map(|(_, _, v)| *v).collect();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(parents)),
            Arc::new(StringArray::from(keys)),
            Arc::new(UInt8Array::from(types)),
            Arc::new(Int64Array::from(ints)),
        ],
    )
    .expect("build exemplar attrs batch")
}

/// Return the carried exemplar (there must be exactly one) whose value matches
/// `value`, so a test can name a specific row regardless of admission order.
fn carried_with_value(result: &MetricsNormalizeResult, value: f64) -> &ravel_types::Exemplar {
    let matches: Vec<_> = result
        .exemplars
        .iter()
        .filter(|e| e.exemplar.value_bits == value.to_bits())
        .collect();
    assert_eq!(matches.len(), 1, "exactly one exemplar with value {value}");
    &matches[0].exemplar
}

/// FINDING 1: a null `id` slot must join to no attributes. Under DELTA (the
/// spec default for the id chain), `decode_delta_u32` carries its accumulator
/// through a null slot, so without the validity-bit guard the second exemplar
/// would inherit the first's id 7 and therefore its `http.status=500`. The two
/// exemplars sit in different cap windows so both are admitted.
#[test]
fn a_null_id_slot_joins_to_no_attributes_under_delta_encoding() {
    let window = ExemplarCap::DEFAULT_WINDOW_NS;
    let base = INGEST_TS_NS - INGEST_TS_NS.rem_euclid(window);
    let mut cap = ExemplarCap::default();
    let result = normalize_custom(
        vec![
            (ArrowPayloadType::UnivariateMetrics, histogram_root_batch()),
            (ArrowPayloadType::HistogramDataPoints, histogram_dp_batch()),
            (
                ArrowPayloadType::HistogramDpExemplars,
                // Row 0: id 7, value 1.0. Row 1: null id (DELTA carries acc=7
                // forward here), value 2.0.
                exemplar_batch_with_ids(
                    vec![Some(7), None],
                    vec![base, base + window],
                    vec![1.0, 2.0],
                    None,
                ),
            ),
            (
                ArrowPayloadType::HistogramDpExemplarAttrs,
                exemplar_attrs_batch(vec![(7, "http.status", 500)]),
            ),
        ],
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 2);
    // The id-7 exemplar carries the attribute.
    assert_eq!(
        carried_with_value(&result, 1.0).filtered_attributes,
        vec![Label {
            name: "http_status".to_string(),
            value: "500".to_string(),
        }]
    );
    // The null-id exemplar carries none: it declared no join key.
    assert!(
        carried_with_value(&result, 2.0)
            .filtered_attributes
            .is_empty()
    );
    assert!(dropped_counts(&result.output.rejected).is_empty());
}

/// FINDING 1, Plain arm: a null slot must yield "no id" here too. A Plain
/// decode reads `values()` straight from the backing buffer and ignores the
/// validity bitmap; arrow zero-fills a null slot, so the buggy decode reads
/// id 0 for the null row. Row 0 declares id 0 (which has an attribute), so
/// without the validity-bit guard the null row would inherit id 0's
/// `http.status=500`. With the guard it joins to nothing.
#[test]
fn a_null_id_slot_joins_to_no_attributes_under_plain_encoding() {
    let window = ExemplarCap::DEFAULT_WINDOW_NS;
    let base = INGEST_TS_NS - INGEST_TS_NS.rem_euclid(window);
    let mut cap = ExemplarCap::default();
    let result = normalize_custom(
        vec![
            (ArrowPayloadType::UnivariateMetrics, histogram_root_batch()),
            (ArrowPayloadType::HistogramDataPoints, histogram_dp_batch()),
            (
                ArrowPayloadType::HistogramDpExemplars,
                // Row 0: id 0, value 1.0. Row 1: null id (Plain reads the
                // zero-filled buffer slot as 0), value 2.0.
                exemplar_batch_with_ids(
                    vec![Some(0), None],
                    vec![base, base + window],
                    vec![1.0, 2.0],
                    Some("plain"),
                ),
            ),
            (
                ArrowPayloadType::HistogramDpExemplarAttrs,
                exemplar_attrs_batch(vec![(0, "http.status", 500)]),
            ),
        ],
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 2);
    // Row 0 declared id 0, so it carries the attribute.
    assert_eq!(
        carried_with_value(&result, 1.0).filtered_attributes,
        vec![Label {
            name: "http_status".to_string(),
            value: "500".to_string(),
        }]
    );
    // The null-id row declared no join key, so it carries none, even though the
    // zero-filled buffer slot would otherwise read as id 0.
    assert!(
        carried_with_value(&result, 2.0)
            .filtered_attributes
            .is_empty()
    );
}

/// FINDING 1, duplicate-id behavior: two rows declaring the SAME id both join
/// to that id's attribute set. The id is the explicit join key, so a duplicate
/// is not misattribution: each row independently resolves its own declared id,
/// which is deterministic. Both carry `http.status=500`.
#[test]
fn duplicate_ids_each_join_to_that_ids_attributes() {
    let window = ExemplarCap::DEFAULT_WINDOW_NS;
    let base = INGEST_TS_NS - INGEST_TS_NS.rem_euclid(window);
    let mut cap = ExemplarCap::default();
    let result = normalize_custom(
        vec![
            (ArrowPayloadType::UnivariateMetrics, histogram_root_batch()),
            (ArrowPayloadType::HistogramDataPoints, histogram_dp_batch()),
            (
                ArrowPayloadType::HistogramDpExemplars,
                exemplar_batch_with_ids(
                    vec![Some(7), Some(7)],
                    vec![base, base + window],
                    vec![1.0, 2.0],
                    Some("plain"),
                ),
            ),
            (
                ArrowPayloadType::HistogramDpExemplarAttrs,
                exemplar_attrs_batch(vec![(7, "http.status", 500)]),
            ),
        ],
        &mut cap,
    );

    assert_eq!(result.exemplars.len(), 2);
    let expected = vec![Label {
        name: "http_status".to_string(),
        value: "500".to_string(),
    }];
    assert_eq!(
        carried_with_value(&result, 1.0).filtered_attributes,
        expected
    );
    assert_eq!(
        carried_with_value(&result, 2.0).filtered_attributes,
        expected
    );
}

proptest! {
    /// Arbitrary exemplar input never panics, and every exemplar row is
    /// accounted for exactly once: carried, or counted by the drop counter.
    #[test]
    fn arbitrary_exemplars_never_panic_and_are_always_accounted(
        rows in prop::collection::vec(
            (
                any::<i64>(),
                any::<f64>(),
                proptest::option::of(any::<[u8; 16]>()),
                proptest::option::of(any::<[u8; 8]>()),
                any::<bool>(),
            ),
            0..6,
        ),
        bounds in prop::collection::vec(1.0f64..100.0, 0..3),
    ) {
        let mut sorted_bounds = bounds;
        sorted_bounds.sort_by(f64::total_cmp);
        sorted_bounds.dedup_by(|a, b| a == b);

        let total = rows.len();
        let exemplars: Vec<ExemplarRow> = rows
            .into_iter()
            .map(|(ts_ns, value, trace_id, span_id, int_valued)| ExemplarRow {
                time_unix_nano: ts_ns,
                value: if int_valued {
                    ExemplarValue::Int(value as i64)
                } else {
                    ExemplarValue::Double(value)
                },
                trace_id,
                span_id,
                attrs: vec![],
            })
            .collect();

        let mut cap = ExemplarCap::default();
        let result = normalize(&histogram_with(sorted_bounds, exemplars), &mut cap);
        let dropped: usize = dropped_counts(&result.output.rejected).iter().sum();
        prop_assert_eq!(result.exemplars.len() + dropped, total);
    }
}
