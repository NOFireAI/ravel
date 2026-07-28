//! Minimal OTAP encoder for a synthetic METRICS payload set.
//!
//! Used by this crate's tests, and intended for later benches: it is the
//! test-side counterpart to `stream.rs`'s decoder, not a production
//! exporter. It emits exactly the subset of the OTAP metrics data model
//! (proto/otel-arrow/docs/data_model.md) that `StreamState` decodes and
//! that the columnar normalizer (`normalize.rs`) consumes:
//!
//! - `UNIVARIATE_METRICS` (root): `id` (UInt16), `metric_type` (UInt8, see
//!   [`normalize::METRIC_TYPE_GAUGE`](crate::normalize::METRIC_TYPE_GAUGE)
//!   and friends), `name` (Dictionary<UInt8, Utf8>), `aggregation_temporality`
//!   (Int32, nullable, meaningful only for `metric_type == METRIC_TYPE_SUM`),
//!   `is_monotonic` (Boolean, nullable, same condition). The data model
//!   marks these last two "optional" as nullable columns on the shared
//!   METRICS table, not as omitted columns -- unlike the AnyValue arms
//!   below, a `Gauge` row simply carries nulls in them.
//! - `NUMBER_DATA_POINTS`: `id` (UInt32, populated whenever the point has
//!   attrs, so `NUMBER_DP_ATTRS` has something to reference), `parent_id`
//!   (UInt16, FK to the root table's `id`), `time_unix_nano`
//!   (Timestamp(Nanosecond)), `double_value` (Float64).
//! - `NUMBER_DP_ATTRS`: `parent_id` (UInt32, FK to `NUMBER_DATA_POINTS.id`),
//!   `key` (Utf8), `type` (UInt8, an AnyValue discriminant per otap-spec.md
//!   section 5.5.1: 1=String, 2=Bool, 3=Int, 4=Double; `AttrValue::Complex`
//!   uses 6=Array purely as a marker with no populated value column, to
//!   exercise the normalizer's `ComplexAttributeValue` rejection path),
//!   `str`/`bool`/`int`/`double` (nullable, one populated per row per its
//!   `type`). A real exporter would omit whichever of these four columns no
//!   row in a given schema generation ever populates (otap-spec.md section
//!   4.2's adaptive-schema rule); this encoder always includes all four
//!   because one Arrow IPC schema is fixed for the life of a `schema_id`
//!   and our tests exercise every AnyValue shape across calls on the same
//!   stream. The normalizer must not infer a value's shape from column
//!   presence alone -- only `type` is authoritative.
//!
//! All `id`/`parent_id` columns are emitted as plain absolute values (no
//! delta or quasi-delta transport encoding, see otap-spec.md section 6.4):
//! `StreamState` does not decode those transforms, so encoding otherwise
//! would produce data Part 1 cannot round-trip. Every such column carries
//! field metadata `("encoding", "plain")` to say so explicitly on the wire,
//! matching the spec's own vocabulary for the encodings it allows.
//!
//! `NUMBER_DATA_POINTS` and `NUMBER_DP_ATTRS` payloads are omitted from a
//! batch entirely when they would have zero rows, per otap-spec.md section
//! 3.1 ("`arrow_payloads` SHOULD omit payloads with 0 rows").
//!
//! Root and data-point/attrs tables get independent `schema_id`s (each is
//! its own Arrow IPC stream, see otap-spec.md section 4.4) and are held
//! open across calls to `encode_batch`, so schema and dictionaries are only
//! ever sent once per `MetricsStreamEncoder` instance -- exactly like a
//! real OTAP exporter reusing one gRPC stream. Schema ids are fixed,
//! human-readable strings tagged with a caller-chosen `stream_version`
//! rather than the hash in otap-spec.md Appendix E: that algorithm is a
//! SHOULD, not a MUST, and generating a schema reset (a new
//! `MetricsStreamEncoder`, i.e. a new `stream_version`) is the only thing
//! that actually needs to be deterministic here.

use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, StringDictionaryBuilder, StructArray, TimestampNanosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit, UInt8Type};
use arrow_ipc::writer::StreamWriter;

use crate::normalize::{
    ANY_VALUE_TYPE_ARRAY, ANY_VALUE_TYPE_BOOL, ANY_VALUE_TYPE_DOUBLE, ANY_VALUE_TYPE_INT,
    ANY_VALUE_TYPE_STRING, METRIC_TYPE_GAUGE, METRIC_TYPE_HISTOGRAM, METRIC_TYPE_SUM,
    METRIC_TYPE_SUMMARY,
};
use crate::proto::experimental::arrow::v1::{ArrowPayload, ArrowPayloadType, BatchArrowRecords};

/// A data-point attribute's value, mirroring the AnyValue arms this encoder
/// can emit (see module docs for the `type` discriminant mapping). `Complex`
/// stands in for the Array/KVList/Bytes arms, none of which this encoder
/// gives a real column: it exists only to produce a `type` discriminant the
/// normalizer must reject.
pub enum AttrValue {
    Str(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    Complex,
}

/// A single data-point attribute.
pub struct AttrRow {
    pub key: String,
    pub value: AttrValue,
}

/// One data point on a metric.
pub struct DataPointRow {
    pub time_unix_nano: i64,
    pub value: f64,
    pub attrs: Vec<AttrRow>,
}

/// A metric's kind and, for `Sum`, the fields the METRICS table carries
/// alongside it (otap-spec.md's `aggregation_temporality` and
/// `is_monotonic` columns; `temporality` uses the same ordinals as OTLP's
/// `AggregationTemporality` enum: 0=Unspecified, 1=Delta, 2=Cumulative).
pub enum MetricKind {
    Gauge,
    Sum {
        temporality: i32,
        is_monotonic: bool,
    },
}

/// One metric and its data points, as fed to [`MetricsStreamEncoder`].
pub struct MetricRow {
    pub name: String,
    pub kind: MetricKind,
    pub data_points: Vec<DataPointRow>,
}

/// One `HISTOGRAM_DATA_POINTS` row (ADR-0016 phase B2). `exemplar_count`
/// stands in for `HISTOGRAM_DP_EXEMPLARS` rows joined by parent_id: this
/// encoder emits that many placeholder exemplar rows for the point rather
/// than taking real exemplar payloads, since the normalizer only ever counts
/// them (ADR-0016 drops exemplars entirely).
pub struct HistogramPointRow {
    pub time_unix_nano: i64,
    pub count: u64,
    pub sum: Option<f64>,
    pub bucket_counts: Vec<u64>,
    pub explicit_bounds: Vec<f64>,
    pub flags: u32,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub exemplar_count: usize,
    pub attrs: Vec<AttrRow>,
}

/// A Histogram metric and its data points. `temporality` uses the same
/// ordinals as [`MetricKind::Sum`]; unlike Sum, only cumulative temporality
/// is meaningful downstream (ADR-0016), but this encoder emits whatever the
/// caller passes so tests can exercise the rejection path.
pub struct HistogramMetricRow {
    pub name: String,
    pub temporality: i32,
    pub data_points: Vec<HistogramPointRow>,
}

/// One `SUMMARY_DATA_POINTS` row. Summaries carry no temporality field in
/// OTLP or OTAP.
pub struct SummaryPointRow {
    pub time_unix_nano: i64,
    pub count: u64,
    pub sum: f64,
    pub quantiles: Vec<(f64, f64)>,
    pub flags: u32,
    pub attrs: Vec<AttrRow>,
}

/// A Summary metric and its data points.
pub struct SummaryMetricRow {
    pub name: String,
    pub data_points: Vec<SummaryPointRow>,
}

/// A `std::io::Write` sink backed by a shared, drainable buffer. Lets an
/// `arrow_ipc::writer::StreamWriter` stay alive (so it never re-emits a
/// Schema message) while we pull out exactly the bytes written since the
/// last drain, to package as one `ArrowPayload.record`.
#[derive(Clone, Default)]
struct SharedBuf(Rc<RefCell<Vec<u8>>>);

impl Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SharedBuf {
    fn drain(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0.borrow_mut())
    }
}

fn plain_encoding_field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(
        [("encoding".to_string(), "plain".to_string())]
            .into_iter()
            .collect(),
    )
}

fn root_schema() -> Schema {
    Schema::new(vec![
        plain_encoding_field("id", DataType::UInt16, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new(
            "name",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("aggregation_temporality", DataType::Int32, true),
        Field::new("is_monotonic", DataType::Boolean, true),
    ])
}

fn data_points_schema() -> Schema {
    Schema::new(vec![
        plain_encoding_field("id", DataType::UInt32, false),
        plain_encoding_field("parent_id", DataType::UInt16, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("double_value", DataType::Float64, false),
    ])
}

fn attrs_schema() -> Schema {
    Schema::new(vec![
        plain_encoding_field("parent_id", DataType::UInt32, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::UInt8, false),
        Field::new("str", DataType::Utf8, true),
        Field::new("bool", DataType::Boolean, true),
        Field::new("int", DataType::Int64, true),
        Field::new("double", DataType::Float64, true),
    ])
}

fn u64_list_item_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::UInt64, true))
}

fn f64_list_item_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Float64, true))
}

fn quantile_value_fields() -> Fields {
    Fields::from(vec![
        Field::new("quantile", DataType::Float64, false),
        Field::new("value", DataType::Float64, false),
    ])
}

fn quantile_list_item_field() -> Arc<Field> {
    Arc::new(Field::new(
        "item",
        DataType::Struct(quantile_value_fields()),
        false,
    ))
}

fn histogram_data_points_schema() -> Schema {
    Schema::new(vec![
        plain_encoding_field("id", DataType::UInt32, false),
        plain_encoding_field("parent_id", DataType::UInt16, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, true),
        Field::new("bucket_counts", DataType::List(u64_list_item_field()), true),
        Field::new(
            "explicit_bounds",
            DataType::List(f64_list_item_field()),
            true,
        ),
        Field::new("flags", DataType::UInt32, false),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
    ])
}

fn summary_data_points_schema() -> Schema {
    Schema::new(vec![
        plain_encoding_field("id", DataType::UInt32, false),
        plain_encoding_field("parent_id", DataType::UInt16, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("quantile", DataType::List(quantile_list_item_field()), true),
        Field::new("flags", DataType::UInt32, false),
    ])
}

/// Only `parent_id` matters here: ADR-0016 drops exemplars entirely, so the
/// normalizer only ever counts rows per histogram data-point id
/// (`count_by_parent_id`), never reads an exemplar's own fields.
fn histogram_exemplars_schema() -> Schema {
    Schema::new(vec![plain_encoding_field(
        "parent_id",
        DataType::UInt32,
        false,
    )])
}

fn build_u64_list_array(lengths: &[usize], values: Vec<u64>) -> Result<ListArray, EncodeError> {
    let offsets = OffsetBuffer::from_lengths(lengths.iter().copied());
    let values_array: ArrayRef = Arc::new(UInt64Array::from(values));
    Ok(ListArray::try_new(
        u64_list_item_field(),
        offsets,
        values_array,
        None,
    )?)
}

fn build_f64_list_array(lengths: &[usize], values: Vec<f64>) -> Result<ListArray, EncodeError> {
    let offsets = OffsetBuffer::from_lengths(lengths.iter().copied());
    let values_array: ArrayRef = Arc::new(Float64Array::from(values));
    Ok(ListArray::try_new(
        f64_list_item_field(),
        offsets,
        values_array,
        None,
    )?)
}

fn build_quantile_list_array(
    lengths: &[usize],
    quantiles: Vec<f64>,
    values: Vec<f64>,
) -> Result<ListArray, EncodeError> {
    let offsets = OffsetBuffer::from_lengths(lengths.iter().copied());
    let struct_array = StructArray::new(
        quantile_value_fields(),
        vec![
            Arc::new(Float64Array::from(quantiles)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
        None,
    );
    Ok(ListArray::try_new(
        quantile_list_item_field(),
        offsets,
        Arc::new(struct_array),
        None,
    )?)
}

/// Accumulates `NUMBER_DP_ATTRS`/`HISTOGRAM_DP_ATTRS`/`SUMMARY_DP_ATTRS` rows
/// column-by-column; factored out since all three attribute tables share one
/// schema ([`attrs_schema`]) and one per-row encoding rule (see module docs
/// on the `type` discriminant).
#[derive(Default)]
struct AttrColumnBuilder {
    parent_ids: Vec<u32>,
    keys: Vec<String>,
    types: Vec<u8>,
    strs: Vec<Option<String>>,
    bools: Vec<Option<bool>>,
    ints: Vec<Option<i64>>,
    doubles: Vec<Option<f64>>,
}

impl AttrColumnBuilder {
    fn push(&mut self, parent_id: u32, attr: &AttrRow) {
        self.parent_ids.push(parent_id);
        self.keys.push(attr.key.clone());
        let (ty, str_v, bool_v, int_v, double_v) = match &attr.value {
            AttrValue::Str(s) => (ANY_VALUE_TYPE_STRING, Some(s.clone()), None, None, None),
            AttrValue::Bool(b) => (ANY_VALUE_TYPE_BOOL, None, Some(*b), None, None),
            AttrValue::Int(i) => (ANY_VALUE_TYPE_INT, None, None, Some(*i), None),
            AttrValue::Double(d) => (ANY_VALUE_TYPE_DOUBLE, None, None, None, Some(*d)),
            AttrValue::Complex => (ANY_VALUE_TYPE_ARRAY, None, None, None, None),
        };
        self.types.push(ty);
        self.strs.push(str_v);
        self.bools.push(bool_v);
        self.ints.push(int_v);
        self.doubles.push(double_v);
    }

    fn into_batch(self) -> Result<RecordBatch, EncodeError> {
        Ok(RecordBatch::try_new(
            Arc::new(attrs_schema()),
            vec![
                Arc::new(UInt32Array::from(self.parent_ids)),
                Arc::new(StringArray::from(self.keys)),
                Arc::new(UInt8Array::from(self.types)),
                Arc::new(StringArray::from(self.strs)),
                Arc::new(BooleanArray::from(self.bools)),
                Arc::new(Int64Array::from(self.ints)),
                Arc::new(Float64Array::from(self.doubles)),
            ],
        )?)
    }
}

/// A persistent per-payload-type Arrow IPC stream writer: one Arrow schema
/// (and its dictionaries), written once, followed by any number of record
/// batches.
struct PayloadStream {
    schema_id: String,
    payload_type: ArrowPayloadType,
    buf: SharedBuf,
    writer: StreamWriter<SharedBuf>,
}

impl PayloadStream {
    fn try_new(
        payload_type: ArrowPayloadType,
        schema_id: String,
        schema: &Schema,
    ) -> Result<Self, arrow::error::ArrowError> {
        let buf = SharedBuf::default();
        let writer = StreamWriter::try_new(buf.clone(), schema)?;
        Ok(Self {
            schema_id,
            payload_type,
            buf,
            writer,
        })
    }

    /// Write `batch` and package everything emitted since construction (or
    /// the last call) -- schema bytes on the very first call, dictionary
    /// deltas and the record batch on every call -- as one `ArrowPayload`,
    /// zstd-compressed. Returns `None` for an empty batch, matching OTAP's
    /// "omit payloads with 0 rows" rule.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<Option<ArrowPayload>, EncodeError> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        self.writer.write(batch)?;
        let raw = self.buf.drain();
        let compressed = zstd::stream::encode_all(raw.as_slice(), 0)
            .map_err(|e| EncodeError::Compression(e.to_string()))?;
        Ok(Some(ArrowPayload {
            schema_id: self.schema_id.clone(),
            r#type: self.payload_type as i32,
            record: compressed.into(),
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("zstd compression failed: {0}")]
    Compression(String),
}

/// Encodes [`MetricRow`]s into `BatchArrowRecords` messages for one OTAP
/// gRPC stream, holding one Arrow IPC stream per payload type open across
/// calls. See the module docs for exactly which columns are emitted.
pub struct MetricsStreamEncoder {
    root: PayloadStream,
    data_points: PayloadStream,
    attrs: PayloadStream,
    hist_data_points: PayloadStream,
    hist_attrs: PayloadStream,
    hist_exemplars: PayloadStream,
    summary_data_points: PayloadStream,
    summary_attrs: PayloadStream,
    next_metric_id: u16,
    next_dp_id: u32,
    next_hist_dp_id: u32,
    next_summary_dp_id: u32,
}

impl MetricsStreamEncoder {
    /// `stream_version` identifies this encoder's schema generation: use a
    /// new value to simulate an OTAP schema reset (a new `schema_id`) on
    /// the same gRPC stream.
    pub fn new(stream_version: &str) -> Result<Self, EncodeError> {
        Ok(Self {
            root: PayloadStream::try_new(
                ArrowPayloadType::UnivariateMetrics,
                format!("ravel-otap-metrics-root-{stream_version}"),
                &root_schema(),
            )?,
            data_points: PayloadStream::try_new(
                ArrowPayloadType::NumberDataPoints,
                format!("ravel-otap-number-dp-{stream_version}"),
                &data_points_schema(),
            )?,
            attrs: PayloadStream::try_new(
                ArrowPayloadType::NumberDpAttrs,
                format!("ravel-otap-number-dp-attrs-{stream_version}"),
                &attrs_schema(),
            )?,
            hist_data_points: PayloadStream::try_new(
                ArrowPayloadType::HistogramDataPoints,
                format!("ravel-otap-histogram-dp-{stream_version}"),
                &histogram_data_points_schema(),
            )?,
            hist_attrs: PayloadStream::try_new(
                ArrowPayloadType::HistogramDpAttrs,
                format!("ravel-otap-histogram-dp-attrs-{stream_version}"),
                &attrs_schema(),
            )?,
            hist_exemplars: PayloadStream::try_new(
                ArrowPayloadType::HistogramDpExemplars,
                format!("ravel-otap-histogram-dp-exemplars-{stream_version}"),
                &histogram_exemplars_schema(),
            )?,
            summary_data_points: PayloadStream::try_new(
                ArrowPayloadType::SummaryDataPoints,
                format!("ravel-otap-summary-dp-{stream_version}"),
                &summary_data_points_schema(),
            )?,
            summary_attrs: PayloadStream::try_new(
                ArrowPayloadType::SummaryDpAttrs,
                format!("ravel-otap-summary-dp-attrs-{stream_version}"),
                &attrs_schema(),
            )?,
            next_metric_id: 0,
            next_dp_id: 0,
            next_hist_dp_id: 0,
            next_summary_dp_id: 0,
        })
    }

    /// Encode `metrics` as one `BatchArrowRecords` with the given
    /// `batch_id`. Thin wrapper over [`Self::encode_batch_ext`] with no
    /// histogram or summary metrics, kept so existing callers and their
    /// signature don't need to change for B2.
    pub fn encode_batch(
        &mut self,
        batch_id: i64,
        metrics: &[MetricRow],
    ) -> Result<BatchArrowRecords, EncodeError> {
        self.encode_batch_ext(batch_id, metrics, &[], &[])
    }

    /// Encode `metrics`, `histograms`, and `summaries` as one
    /// `BatchArrowRecords` with the given `batch_id`. All root metric ids
    /// share one `next_metric_id` counter and one `UNIVARIATE_METRICS`
    /// table, matching otap-spec.md's single root table for every metric
    /// type. Data-point ids are assigned sequentially across calls,
    /// mimicking a real exporter reusing one stream; histogram and summary
    /// data points use their own id counters since they're joined by their
    /// own `*_DP_ATTRS`/`*_DP_EXEMPLARS` tables, never `NUMBER_DP_ATTRS`.
    pub fn encode_batch_ext(
        &mut self,
        batch_id: i64,
        metrics: &[MetricRow],
        histograms: &[HistogramMetricRow],
        summaries: &[SummaryMetricRow],
    ) -> Result<BatchArrowRecords, EncodeError> {
        let mut root_ids = Vec::new();
        let mut root_types = Vec::new();
        let mut root_names = Vec::new();
        let mut root_temporalities: Vec<Option<i32>> = Vec::new();
        let mut root_monotonic: Vec<Option<bool>> = Vec::new();

        let mut dp_ids = Vec::new();
        let mut dp_parent_ids = Vec::new();
        let mut dp_times = Vec::new();
        let mut dp_values = Vec::new();
        let mut attr_builder = AttrColumnBuilder::default();

        for metric in metrics {
            let metric_id = self.next_metric_id;
            self.next_metric_id = self.next_metric_id.wrapping_add(1);
            root_ids.push(metric_id);
            root_names.push(metric.name.clone());
            match metric.kind {
                MetricKind::Gauge => {
                    root_types.push(METRIC_TYPE_GAUGE);
                    root_temporalities.push(None);
                    root_monotonic.push(None);
                }
                MetricKind::Sum {
                    temporality,
                    is_monotonic,
                } => {
                    root_types.push(METRIC_TYPE_SUM);
                    root_temporalities.push(Some(temporality));
                    root_monotonic.push(Some(is_monotonic));
                }
            }

            for dp in &metric.data_points {
                let dp_id = self.next_dp_id;
                self.next_dp_id = self.next_dp_id.wrapping_add(1);
                dp_ids.push(dp_id);
                dp_parent_ids.push(metric_id);
                dp_times.push(dp.time_unix_nano);
                dp_values.push(dp.value);

                for attr in &dp.attrs {
                    attr_builder.push(dp_id, attr);
                }
            }
        }

        let mut hist_dp_ids = Vec::new();
        let mut hist_dp_parent_ids = Vec::new();
        let mut hist_dp_times = Vec::new();
        let mut hist_dp_counts = Vec::new();
        let mut hist_dp_sums: Vec<Option<f64>> = Vec::new();
        let mut hist_dp_bucket_lengths = Vec::new();
        let mut hist_dp_bucket_values: Vec<u64> = Vec::new();
        let mut hist_dp_bound_lengths = Vec::new();
        let mut hist_dp_bound_values: Vec<f64> = Vec::new();
        let mut hist_dp_flags = Vec::new();
        let mut hist_dp_mins: Vec<Option<f64>> = Vec::new();
        let mut hist_dp_maxs: Vec<Option<f64>> = Vec::new();
        let mut hist_attr_builder = AttrColumnBuilder::default();
        let mut hist_exemplar_parent_ids: Vec<u32> = Vec::new();

        for metric in histograms {
            let metric_id = self.next_metric_id;
            self.next_metric_id = self.next_metric_id.wrapping_add(1);
            root_ids.push(metric_id);
            root_types.push(METRIC_TYPE_HISTOGRAM);
            root_names.push(metric.name.clone());
            root_temporalities.push(Some(metric.temporality));
            root_monotonic.push(None);

            for dp in &metric.data_points {
                let dp_id = self.next_hist_dp_id;
                self.next_hist_dp_id = self.next_hist_dp_id.wrapping_add(1);
                hist_dp_ids.push(dp_id);
                hist_dp_parent_ids.push(metric_id);
                hist_dp_times.push(dp.time_unix_nano);
                hist_dp_counts.push(dp.count);
                hist_dp_sums.push(dp.sum);
                hist_dp_bucket_lengths.push(dp.bucket_counts.len());
                hist_dp_bucket_values.extend_from_slice(&dp.bucket_counts);
                hist_dp_bound_lengths.push(dp.explicit_bounds.len());
                hist_dp_bound_values.extend_from_slice(&dp.explicit_bounds);
                hist_dp_flags.push(dp.flags);
                hist_dp_mins.push(dp.min);
                hist_dp_maxs.push(dp.max);

                for attr in &dp.attrs {
                    hist_attr_builder.push(dp_id, attr);
                }
                hist_exemplar_parent_ids.extend(std::iter::repeat_n(dp_id, dp.exemplar_count));
            }
        }

        let mut summary_dp_ids = Vec::new();
        let mut summary_dp_parent_ids = Vec::new();
        let mut summary_dp_times = Vec::new();
        let mut summary_dp_counts = Vec::new();
        let mut summary_dp_sums = Vec::new();
        let mut summary_dp_quantile_lengths = Vec::new();
        let mut summary_dp_quantiles: Vec<f64> = Vec::new();
        let mut summary_dp_values: Vec<f64> = Vec::new();
        let mut summary_dp_flags = Vec::new();
        let mut summary_attr_builder = AttrColumnBuilder::default();

        for metric in summaries {
            let metric_id = self.next_metric_id;
            self.next_metric_id = self.next_metric_id.wrapping_add(1);
            root_ids.push(metric_id);
            root_types.push(METRIC_TYPE_SUMMARY);
            root_names.push(metric.name.clone());
            root_temporalities.push(None);
            root_monotonic.push(None);

            for dp in &metric.data_points {
                let dp_id = self.next_summary_dp_id;
                self.next_summary_dp_id = self.next_summary_dp_id.wrapping_add(1);
                summary_dp_ids.push(dp_id);
                summary_dp_parent_ids.push(metric_id);
                summary_dp_times.push(dp.time_unix_nano);
                summary_dp_counts.push(dp.count);
                summary_dp_sums.push(dp.sum);
                summary_dp_quantile_lengths.push(dp.quantiles.len());
                for (quantile, value) in &dp.quantiles {
                    summary_dp_quantiles.push(*quantile);
                    summary_dp_values.push(*value);
                }
                summary_dp_flags.push(dp.flags);

                for attr in &dp.attrs {
                    summary_attr_builder.push(dp_id, attr);
                }
            }
        }

        let mut name_builder = StringDictionaryBuilder::<UInt8Type>::new();
        for name in &root_names {
            name_builder.append(name)?;
        }
        let root_batch = RecordBatch::try_new(
            Arc::new(root_schema()),
            vec![
                Arc::new(UInt16Array::from(root_ids)),
                Arc::new(UInt8Array::from(root_types)),
                Arc::new(name_builder.finish()),
                Arc::new(Int32Array::from(root_temporalities)),
                Arc::new(BooleanArray::from(root_monotonic)),
            ],
        )?;

        let dp_batch = RecordBatch::try_new(
            Arc::new(data_points_schema()),
            vec![
                Arc::new(UInt32Array::from(dp_ids)),
                Arc::new(UInt16Array::from(dp_parent_ids)),
                Arc::new(TimestampNanosecondArray::from(dp_times)),
                Arc::new(Float64Array::from(dp_values)),
            ],
        )?;
        let attrs_batch = attr_builder.into_batch()?;

        let hist_dp_batch = RecordBatch::try_new(
            Arc::new(histogram_data_points_schema()),
            vec![
                Arc::new(UInt32Array::from(hist_dp_ids)),
                Arc::new(UInt16Array::from(hist_dp_parent_ids)),
                Arc::new(TimestampNanosecondArray::from(hist_dp_times)),
                Arc::new(UInt64Array::from(hist_dp_counts)),
                Arc::new(Float64Array::from(hist_dp_sums)),
                Arc::new(build_u64_list_array(
                    &hist_dp_bucket_lengths,
                    hist_dp_bucket_values,
                )?),
                Arc::new(build_f64_list_array(
                    &hist_dp_bound_lengths,
                    hist_dp_bound_values,
                )?),
                Arc::new(UInt32Array::from(hist_dp_flags)),
                Arc::new(Float64Array::from(hist_dp_mins)),
                Arc::new(Float64Array::from(hist_dp_maxs)),
            ],
        )?;
        let hist_attrs_batch = hist_attr_builder.into_batch()?;
        let hist_exemplars_batch = RecordBatch::try_new(
            Arc::new(histogram_exemplars_schema()),
            vec![Arc::new(UInt32Array::from(hist_exemplar_parent_ids))],
        )?;

        let summary_dp_batch = RecordBatch::try_new(
            Arc::new(summary_data_points_schema()),
            vec![
                Arc::new(UInt32Array::from(summary_dp_ids)),
                Arc::new(UInt16Array::from(summary_dp_parent_ids)),
                Arc::new(TimestampNanosecondArray::from(summary_dp_times)),
                Arc::new(UInt64Array::from(summary_dp_counts)),
                Arc::new(Float64Array::from(summary_dp_sums)),
                Arc::new(build_quantile_list_array(
                    &summary_dp_quantile_lengths,
                    summary_dp_quantiles,
                    summary_dp_values,
                )?),
                Arc::new(UInt32Array::from(summary_dp_flags)),
            ],
        )?;
        let summary_attrs_batch = summary_attr_builder.into_batch()?;

        let mut arrow_payloads = Vec::new();
        if let Some(p) = self.root.write_batch(&root_batch)? {
            arrow_payloads.push(p);
        }
        if let Some(p) = self.data_points.write_batch(&dp_batch)? {
            arrow_payloads.push(p);
        }
        if let Some(p) = self.attrs.write_batch(&attrs_batch)? {
            arrow_payloads.push(p);
        }
        if let Some(p) = self.hist_data_points.write_batch(&hist_dp_batch)? {
            arrow_payloads.push(p);
        }
        if let Some(p) = self.hist_attrs.write_batch(&hist_attrs_batch)? {
            arrow_payloads.push(p);
        }
        if let Some(p) = self.hist_exemplars.write_batch(&hist_exemplars_batch)? {
            arrow_payloads.push(p);
        }
        if let Some(p) = self.summary_data_points.write_batch(&summary_dp_batch)? {
            arrow_payloads.push(p);
        }
        if let Some(p) = self.summary_attrs.write_batch(&summary_attrs_batch)? {
            arrow_payloads.push(p);
        }

        Ok(BatchArrowRecords {
            batch_id,
            arrow_payloads,
            headers: Vec::new(),
        })
    }
}
