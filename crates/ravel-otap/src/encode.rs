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
    BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StringDictionaryBuilder, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, UInt8Type};
use arrow_ipc::writer::StreamWriter;

use crate::normalize::{
    ANY_VALUE_TYPE_ARRAY, ANY_VALUE_TYPE_BOOL, ANY_VALUE_TYPE_DOUBLE, ANY_VALUE_TYPE_INT,
    ANY_VALUE_TYPE_STRING, METRIC_TYPE_GAUGE, METRIC_TYPE_SUM,
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
    next_metric_id: u16,
    next_dp_id: u32,
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
            next_metric_id: 0,
            next_dp_id: 0,
        })
    }

    /// Encode `metrics` as one `BatchArrowRecords` with the given
    /// `batch_id`. Metric and data-point ids are assigned sequentially
    /// across calls, mimicking a real exporter reusing one stream.
    pub fn encode_batch(
        &mut self,
        batch_id: i64,
        metrics: &[MetricRow],
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

        let mut attr_parent_ids = Vec::new();
        let mut attr_keys = Vec::new();
        let mut attr_types = Vec::new();
        let mut attr_strs: Vec<Option<String>> = Vec::new();
        let mut attr_bools: Vec<Option<bool>> = Vec::new();
        let mut attr_ints: Vec<Option<i64>> = Vec::new();
        let mut attr_doubles: Vec<Option<f64>> = Vec::new();

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
                    attr_parent_ids.push(dp_id);
                    attr_keys.push(attr.key.clone());
                    let (ty, str_v, bool_v, int_v, double_v) = match &attr.value {
                        AttrValue::Str(s) => {
                            (ANY_VALUE_TYPE_STRING, Some(s.clone()), None, None, None)
                        }
                        AttrValue::Bool(b) => (ANY_VALUE_TYPE_BOOL, None, Some(*b), None, None),
                        AttrValue::Int(i) => (ANY_VALUE_TYPE_INT, None, None, Some(*i), None),
                        AttrValue::Double(d) => (ANY_VALUE_TYPE_DOUBLE, None, None, None, Some(*d)),
                        AttrValue::Complex => (ANY_VALUE_TYPE_ARRAY, None, None, None, None),
                    };
                    attr_types.push(ty);
                    attr_strs.push(str_v);
                    attr_bools.push(bool_v);
                    attr_ints.push(int_v);
                    attr_doubles.push(double_v);
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

        let attrs_batch = RecordBatch::try_new(
            Arc::new(attrs_schema()),
            vec![
                Arc::new(UInt32Array::from(attr_parent_ids)),
                Arc::new(StringArray::from(attr_keys)),
                Arc::new(UInt8Array::from(attr_types)),
                Arc::new(StringArray::from(attr_strs)),
                Arc::new(BooleanArray::from(attr_bools)),
                Arc::new(Int64Array::from(attr_ints)),
                Arc::new(Float64Array::from(attr_doubles)),
            ],
        )?;

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

        Ok(BatchArrowRecords {
            batch_id,
            arrow_payloads,
            headers: Vec::new(),
        })
    }
}
