//! Differential harness for ADR-0109's columnar bulk-load fast path (#606).
//!
//! It loads one bounded Parquet sample twice through the *same* in-process
//! `LogIngestRouter`: once as `Vec<NormalizedLogRecord>` through
//! [`LogIngestRouter::write`] (the pre-ADR row path, kept as the differential
//! reference), and once as a [`ColumnarLogBatch`] through
//! [`LogIngestRouter::write_columnar`] (the fast path). The only thing that
//! differs between the two runs is the write-path input shape, so the wall and
//! CPU delta between them isolates the per-row pivot the fast path removes
//! (`RlogWriter::push` gathers each dynamic column per row; `push_columnar`
//! stages it from contiguous per-column data).
//!
//! Scope, stated so the number cannot be over-read (this is why the harness
//! exists at all, #606):
//!
//! - **Local, bounded sample. Not the ClickBench reference figure.** The
//!   corpus is a small synthetic ClickBench-shaped table built in this process,
//!   not the full `hits.parquet` on the c6a.4xlarge/S3 reference box. Every
//!   figure this harness produces is a local differential, never the reference
//!   result.
//! - **Measures the epic WITHOUT ADR-0109 decision 3 contributing.** The
//!   columnar batch is built through [`ColumnarLogBatch::from_records`], which
//!   does not attach dictionaries, so the dictionary-preserving column and
//!   dictionary-aware bloom path (#660) never engages -- exactly as it fails to
//!   engage on ClickBench-shaped plain-`BYTE_ARRAY` Parquet. Decision 8's
//!   arithmetic counted those savings; this harness does not see them.
//! - **In-memory store.** The router writes to a [`MemoryStore`]. S3 latency,
//!   multi-shard fan-out scaling, and real PUT round trips are invisible here;
//!   the CRC32C over the in-memory PUT is the only object-store work timed.
//!
//! Report-only: this crate never changes library behavior, it only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use ravel_ingest::{
    Clock, IngestConfig, LogIngestRouter, SystemClock, WriteMode,
};
use ravel_logseg::{ColumnarLogBatch, LogRecord, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::TenantId;

/// Ack deadline for each Strict write. Generous: the in-memory store never
/// blocks, but a loaded host can still stretch a flush.
const WRITE_ACK_DEADLINE: Duration = Duration::from_secs(120);

/// The fixed resource+scope identity every corpus record shares. A single
/// stream keeps every row on one shard, so the row-vs-columnar differential is
/// the write-path pivot alone and not a difference in shard fan-out.
const RESOURCE_ATTRS: &[(&str, &str)] =
    &[("service.name", "clickbench-local"), ("dataset", "hits-sample")];

/// Which write path a measurement drove. The row path
/// ([`LogIngestRouter::write`]) is the differential reference; the columnar
/// path ([`LogIngestRouter::write_columnar`]) is ADR-0109's fast path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadPath {
    Row,
    Columnar,
}

impl LoadPath {
    pub fn label(self) -> &'static str {
        match self {
            LoadPath::Row => "row",
            LoadPath::Columnar => "columnar",
        }
    }
}

/// A harness failure. The router's own errors carry rich structure; here they
/// are flattened to a string, since a bench does not act on the classification.
#[derive(Debug, thiserror::Error)]
pub enum CompareError {
    #[error("parquet: {0}")]
    Parquet(String),
    #[error("{path} write failed: {cause}")]
    Write { path: &'static str, cause: String },
}

/// One path's measured result over the whole corpus.
#[derive(Debug, Clone)]
pub struct PathReport {
    pub path: LoadPath,
    pub rows_processed: u64,
    pub objects_written: usize,
    /// Batches driven through [`LogIngestRouter::write`]. Nonzero only on the
    /// row path. This and `columnar_batches` are the reachability signals that
    /// prove the comparison ran two *different* paths rather than measuring one
    /// path twice: each counter is incremented in the same match arm that calls
    /// its router method, so a flip of the method leaves the counter behind.
    pub row_batches: u64,
    /// Batches driven through [`LogIngestRouter::write_columnar`]. Nonzero only
    /// on the columnar path.
    pub columnar_batches: u64,
    /// Wall time of the timed region (the write loop only; corpus decode and
    /// per-batch input construction happen before it).
    pub wall: Duration,
    /// Process CPU (user + system) consumed across the timed region, sampled
    /// from `/proc/self/stat`. Process-wide, so it includes the tokio runtime's
    /// worker threads; both paths pay the same runtime overhead, so their
    /// difference remains the pivot. `None` when the reading is unavailable
    /// (non-Linux, or an unparseable stat line).
    pub cpu: Option<Duration>,
}

/// The two paths' reports plus the shared input description.
#[derive(Debug, Clone)]
pub struct CompareReport {
    pub row: PathReport,
    pub columnar: PathReport,
    pub corpus_rows: usize,
    /// Total Parquet columns (ts + body + attribute columns).
    pub columns: usize,
    pub attr_columns: usize,
    pub shards: u32,
    pub batch_rows: usize,
    pub parquet_bytes: usize,
}

impl CompareReport {
    /// Row-path wall divided by columnar-path wall.
    pub fn wall_speedup(&self) -> f64 {
        self.columnar.wall.as_secs_f64().max(f64::MIN_POSITIVE).recip()
            * self.row.wall.as_secs_f64()
    }

    /// The pivot's share of the row-path *write* CPU, as a fraction:
    /// `(cpu_row - cpu_columnar) / cpu_row`. `None` when either CPU reading is
    /// missing. This is the write-path share, not the end-to-end load share:
    /// the full bulk load also pays Parquet decode on both paths, which this
    /// differential excludes, so the pivot's share of total load CPU is lower.
    pub fn pivot_cpu_share(&self) -> Option<f64> {
        let row = self.row.cpu?.as_secs_f64();
        let col = self.columnar.cpu?.as_secs_f64();
        if row <= 0.0 {
            return None;
        }
        Some((row - col) / row)
    }
}

// ------------------------------------------------------------------ corpus

/// Column layout of the synthetic corpus: `int_cols` `Int64` attribute columns
/// and `str_cols` `Utf8` attribute columns, plus `ts` and `body`. ClickBench's
/// `hits` is ~105 columns, integer-heavy with a string minority; the defaults
/// mirror that shape and land in the wide regime where `wide_gather` shows the
/// per-row pivot dominating the block encode.
#[derive(Debug, Clone, Copy)]
pub struct CorpusShape {
    pub rows: usize,
    pub int_cols: usize,
    pub str_cols: usize,
}

impl Default for CorpusShape {
    fn default() -> Self {
        CorpusShape {
            rows: 50_000,
            int_cols: 60,
            str_cols: 40,
        }
    }
}

fn corpus_schema(shape: CorpusShape) -> SchemaRef {
    let mut fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ];
    for i in 0..shape.int_cols {
        fields.push(Field::new(format!("int_{i:02}"), DataType::Int64, false));
    }
    for i in 0..shape.str_cols {
        fields.push(Field::new(format!("str_{i:02}"), DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

/// Builds one deterministic ClickBench-shaped `RecordBatch`. Values are derived
/// from the row and column indices (no RNG), so the corpus is byte-stable
/// across runs and hosts. String columns repeat over a small alphabet per
/// column, the shape a dictionary-encoded categorical export takes.
fn corpus_batch(shape: CorpusShape, schema: &SchemaRef) -> RecordBatch {
    let n = shape.rows;
    let ts: Vec<i64> = (0..n).map(|r| 1_700_000_000_000_000_000 + r as i64 * 1_000_000).collect();
    let body: Vec<String> = (0..n)
        .map(|r| format!("request {} served in {}ms", r, r % 250))
        .collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(2 + shape.int_cols + shape.str_cols);
    columns.push(Arc::new(Int64Array::from(ts)));
    columns.push(Arc::new(StringArray::from(body)));

    for c in 0..shape.int_cols {
        let col: Vec<i64> = (0..n).map(|r| (r as i64).wrapping_mul(31).wrapping_add(c as i64)).collect();
        columns.push(Arc::new(Int64Array::from(col)));
    }
    for c in 0..shape.str_cols {
        // A small per-column alphabet (16 distinct values) so the column reads
        // like a low-cardinality categorical, the ClickBench-typical shape.
        let col: Vec<String> = (0..n).map(|r| format!("s{c:02}_{}", (r + c) % 16)).collect();
        columns.push(Arc::new(StringArray::from(col)));
    }

    RecordBatch::try_new(Arc::clone(schema), columns).expect("build corpus batch")
}

/// Encodes the corpus to an in-memory Parquet buffer with the default writer
/// properties (plain `BYTE_ARRAY` for the string columns, so the read-back
/// carries no Arrow `Dictionary` type and #660's dictionary path stays off).
pub fn clickbench_shaped_parquet(shape: CorpusShape) -> Vec<u8> {
    let schema = corpus_schema(shape);
    let batch = corpus_batch(shape, &schema);
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    buf
}

/// Reads `parquet_bytes` back through the real async-capable Parquet reader and
/// maps each row to a [`NormalizedLogRecord`]: `ts`/`body` to their fields, and
/// every attribute column to one per-record attribute (`Int64` to
/// [`AttrValue::I64`], `Utf8` to [`AttrValue::Str`]). Every record shares the
/// [`RESOURCE_ATTRS`] stream identity. This decode runs once and feeds both
/// paths, so it is not part of the differential.
pub fn decode_corpus(parquet_bytes: &[u8]) -> Result<Vec<NormalizedLogRecord>, CompareError> {
    let bytes = bytes::Bytes::copy_from_slice(parquet_bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| CompareError::Parquet(e.to_string()))?;
    let reader = builder.build().map_err(|e| CompareError::Parquet(e.to_string()))?;

    let res: Vec<(String, AttrValue)> = RESOURCE_ATTRS
        .iter()
        .map(|(k, v)| ((*k).to_string(), AttrValue::Str((*v).to_string())))
        .collect();
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    let stream_id = log_stream_id(&res, "bench", "", &scope_attrs);
    let stream_attrs = stream_attrs_bytes(&res, "bench", "", &scope_attrs);

    let mut records = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| CompareError::Parquet(e.to_string()))?;
        let schema = batch.schema();
        let ts = batch
            .column_by_name("ts")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| CompareError::Parquet("missing Int64 `ts` column".into()))?;
        let body = batch
            .column_by_name("body")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| CompareError::Parquet("missing Utf8 `body` column".into()))?;

        // Pre-resolve the attribute columns once per batch.
        enum Col<'a> {
            I64(&'a Int64Array),
            Str(&'a StringArray),
        }
        let mut attr_cols: Vec<(String, Col)> = Vec::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            if name == "ts" || name == "body" {
                continue;
            }
            let arr = batch.column(idx);
            let col = match arr.data_type() {
                DataType::Int64 => Col::I64(
                    arr.as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| CompareError::Parquet(format!("{name}: not Int64")))?,
                ),
                DataType::Utf8 => Col::Str(
                    arr.as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| CompareError::Parquet(format!("{name}: not Utf8")))?,
                ),
                other => {
                    return Err(CompareError::Parquet(format!(
                        "{name}: unexpected column type {other:?}"
                    )));
                }
            };
            attr_cols.push((name.clone(), col));
        }

        for row in 0..batch.num_rows() {
            let mut attrs: Vec<(String, AttrValue)> = Vec::with_capacity(attr_cols.len());
            for (name, col) in &attr_cols {
                let v = match col {
                    Col::I64(a) => AttrValue::I64(a.value(row)),
                    Col::Str(a) => AttrValue::Str(a.value(row).to_string()),
                };
                attrs.push((name.clone(), v));
            }
            let ts_ns = ts.value(row);
            records.push(NormalizedLogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns,
                observed_ts_ns: ts_ns,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: body.value(row).to_string(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs,
            });
        }
    }
    Ok(records)
}

// ------------------------------------------------------------------ measure

/// A field-for-field rename of a [`NormalizedLogRecord`] into the
/// [`LogRecord`] shape [`ColumnarLogBatch::from_records`] consumes.
fn to_logrecord(r: &NormalizedLogRecord) -> LogRecord {
    LogRecord {
        stream_id: r.stream_id,
        stream_attrs: r.stream_attrs.clone(),
        ts_ns: r.ts_ns,
        observed_ts_ns: r.observed_ts_ns,
        severity_num: r.severity_num,
        severity_text: r.severity_text.clone(),
        body: r.body.clone(),
        trace_id: r.trace_id,
        span_id: r.span_id,
        flags: r.flags,
        attrs: r.attrs.clone(),
    }
}

/// One prefetched batch's input, already in the shape its path's router method
/// consumes. Both variants are built *before* the timed region: the row vec is
/// cloned and the columnar batch is pivoted by `from_records` up front, exactly
/// as the shipping loader builds its columnar batch in the decode task, off the
/// flush critical path. So the timed region is the router call alone.
enum Built {
    Row(Vec<NormalizedLogRecord>),
    Columnar(Box<ColumnarLogBatch>),
}

fn config(shards: u32) -> IngestConfig {
    // `target_bytes: 1` flushes each write as one RLOG object inside the size
    // trigger, so every batch is one object with no lingering buffer, and flush
    // timing stays off the wall clock. Same config the CLI loader uses.
    IngestConfig {
        shard_count: shards,
        target_bytes: 1,
        ..IngestConfig::default()
    }
}

/// Runs the whole corpus through one path and returns its measured report.
///
/// The per-batch input is built for `path` before the timed region; the timed
/// region is the write loop. `row_batches`/`columnar_batches` are each bumped
/// in the same match arm that calls the path's router method -- the reachability
/// signal that proves this run drove the path it claims to.
async fn measure_path(
    path: LoadPath,
    records: &[NormalizedLogRecord],
    shards: u32,
    batch_rows: usize,
) -> Result<PathReport, CompareError> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let router = LogIngestRouter::new(config(shards), Arc::clone(&store), clock);
    let tenant = TenantId::new("clickbench-local");

    // Build every batch's input up front (outside the timed region). This is
    // the decisive line the acceptance test's flip targets: selecting `Row`
    // here for the columnar path makes the columnar run measure the row path.
    let batches: Vec<Built> = records
        .chunks(batch_rows.max(1))
        .map(|chunk| match path {
            LoadPath::Row => Built::Row(chunk.to_vec()),
            LoadPath::Columnar => {
                let logrecs: Vec<LogRecord> = chunk.iter().map(to_logrecord).collect();
                Built::Columnar(Box::new(ColumnarLogBatch::from_records(&logrecs)))
            }
        })
        .collect();

    let mut report = PathReport {
        path,
        rows_processed: 0,
        objects_written: 0,
        row_batches: 0,
        columnar_batches: 0,
        wall: Duration::ZERO,
        cpu: None,
    };

    let cpu_start = process_cpu();
    let wall_start = Instant::now();
    for built in batches {
        let receipt = match built {
            Built::Row(recs) => {
                report.row_batches += 1;
                let n = recs.len() as u64;
                let r = router
                    .write(tenant.clone(), recs, WriteMode::Strict, WRITE_ACK_DEADLINE)
                    .await
                    .map_err(|e| CompareError::Write {
                        path: "row",
                        cause: e.to_string(),
                    })?;
                report.rows_processed += n;
                r
            }
            Built::Columnar(batch) => {
                report.columnar_batches += 1;
                let n = batch.num_rows as u64;
                let r = router
                    .write_columnar(tenant.clone(), *batch, WriteMode::Strict, WRITE_ACK_DEADLINE)
                    .await
                    .map_err(|e| CompareError::Write {
                        path: "columnar",
                        cause: e.to_string(),
                    })?;
                report.rows_processed += n;
                r
            }
        };
        report.objects_written += receipt.tokens.len();
    }
    // Strict acks already guarantee durability; this is a defensive no-op.
    router.flush_all().await;
    report.wall = wall_start.elapsed();
    report.cpu = match (cpu_start, process_cpu()) {
        (Some(a), Some(b)) => b.checked_sub(a),
        _ => None,
    };
    Ok(report)
}

/// Loads the same decoded corpus through both paths and returns both reports.
/// Runs the row path first, then the columnar path, each on its own fresh
/// router and store so neither warms the other's caches.
pub async fn compare(
    records: &[NormalizedLogRecord],
    shape: CorpusShape,
    shards: u32,
    batch_rows: usize,
    parquet_bytes: usize,
) -> Result<CompareReport, CompareError> {
    let row = measure_path(LoadPath::Row, records, shards, batch_rows).await?;
    let columnar = measure_path(LoadPath::Columnar, records, shards, batch_rows).await?;
    Ok(CompareReport {
        row,
        columnar,
        corpus_rows: records.len(),
        columns: 2 + shape.int_cols + shape.str_cols,
        attr_columns: shape.int_cols + shape.str_cols,
        shards,
        batch_rows,
        parquet_bytes,
    })
}

/// Convenience: build the sample, decode it, and run the comparison.
pub async fn run(
    shape: CorpusShape,
    shards: u32,
    batch_rows: usize,
) -> Result<CompareReport, CompareError> {
    let parquet = clickbench_shaped_parquet(shape);
    let records = decode_corpus(&parquet)?;
    compare(&records, shape, shards, batch_rows, parquet.len()).await
}

/// Process CPU time (user + system) from `/proc/self/stat`, or `None` off Linux
/// or on an unparseable line. Fields 14 (`utime`) and 15 (`stime`) are in clock
/// ticks; Linux fixes `USER_HZ` at 100, so one tick is 10ms.
fn process_cpu() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // The `comm` field is parenthesized and may contain spaces or ')', so split
    // after the last ')': the remaining fields start at `state` (field 3).
    let rest = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // utime is field 14 -> index 11 after `state`; stime is field 15 -> 12.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    const USER_HZ: u64 = 100;
    Some(Duration::from_nanos(
        (utime + stime).saturating_mul(1_000_000_000 / USER_HZ),
    ))
}
