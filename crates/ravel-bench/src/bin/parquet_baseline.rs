//! Write-side Parquet baseline harness (GitHub issue #97 phase a).
//!
//! From the identical generated workloads the RSEG benches use, this bin
//! encodes the same logical content four ways and reports encode wall
//! (median of >=5 in-memory writes) and stored object bytes, same run and
//! same host:
//!
//!   1. Parquet, rows sorted by (series_id bytes, ts).
//!   2. Parquet, rows in generator input order (unsorted).
//!   3. RSEG v1 (`SegmentWriter::write`).
//!   4. RSEG v2 (`SegmentWriter::write_v2`).
//!
//! Parquet schema: series_id FixedSizeBinary(16), ts Int64, value Float64,
//! and one Dictionary(Int32, Utf8) column per label. Writer properties:
//! zstd(3), BYTE_STREAM_SPLIT on value, DELTA_BINARY_PACKED on ts,
//! dictionary encoding on labels, page/offset index enabled, 100k-row row
//! groups.
//!
//! Report-only: this bin never changes library behavior, it only measures
//! it. Gated on the `parquet-baseline` feature so the default build never
//! compiles arrow/parquet (see Cargo.toml). Phase b (read-side GET
//! accounting on MinIO) is a separate later task and is not started here.
//!
//! Run: `cargo run -p ravel-bench --release --features parquet-baseline \
//!   --bin parquet_baseline`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, Float64Array, Int64Array, StringDictionaryBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{build_segment, build_segment_v2};
use ravel_types::{LabelSet, Sample, SeriesId};

/// Median-of-N runs, plus enough runs to be defensible. The task asks for
/// at least 5.
const RUNS: usize = 5;
const ROW_GROUP_ROWS: usize = 100_000;

type RawSeries = (SeriesId, LabelSet, Vec<Sample>);

fn main() {
    print_header();

    // Shape 1: 100k series, 200k total samples (2/series), many_small. This
    // is the `segment_encode` / `profile_hotspots` encode shape, the
    // high-cardinality wall docs/rseg-v2-plan.md section 1 measures.
    let many_small = WorkloadConfig {
        series_count: 100_000,
        samples_per_series: 2,
        cardinality: CardinalityProfile::many_small(100_000),
        ..Default::default()
    };
    run_shape(
        "100k-series many_small, 200k samples (2/series)",
        &many_small,
    );

    // Shape 2: the 500-series ingest shape. ingest_bench at 500 series /
    // 20000 pts/s / 8s ingests 160k points, i.e. 320 samples/series, and
    // leaves the default many_small(100) cardinality; this reproduces that
    // logical content.
    let ingest = WorkloadConfig {
        series_count: 500,
        samples_per_series: 320,
        ..Default::default()
    };
    run_shape(
        "500-series ingest shape, 160k samples (320/series)",
        &ingest,
    );
}

fn print_header() {
    println!("== parquet write-side baseline (issue #97 phase a) ==");
    println!("  runs per measurement : {RUNS} (median reported)");
    println!("  arrow                : {}", arrow_version());
    println!("  parquet              : {}", parquet_version());
    println!();
}

fn arrow_version() -> &'static str {
    // Reported for provenance; the exact locked version is in Cargo.lock.
    "59 (workspace pin, same as ravel-otap)"
}

fn parquet_version() -> &'static str {
    "59 (pinned lockstep to arrow)"
}

fn run_shape(label: &str, config: &WorkloadConfig) {
    println!("-- shape: {label} --");
    let raw = generate_raw(config).expect("generate workload");

    let label_names = collect_label_names(&raw);
    let total_rows: usize = raw.iter().map(|(_, _, s)| s.len()).sum();
    println!(
        "  series: {}  rows: {}  label columns: {} {:?}",
        raw.len(),
        total_rows,
        label_names.len(),
        label_names
    );

    let schema = build_schema(&label_names);

    // Sorted-by-(series_id, ts) and input-order row layouts. Both are built
    // once, outside the timed region: only the parquet write itself is timed.
    let metas: Vec<SeriesMeta> = raw
        .iter()
        .map(|(id, labels, _)| SeriesMeta {
            id: id.0,
            label_values: label_values(labels, &label_names),
        })
        .collect();

    let mut input_order: Vec<Row> = Vec::with_capacity(total_rows);
    for (sidx, (_, _, samples)) in raw.iter().enumerate() {
        for s in samples {
            input_order.push(Row {
                series: sidx as u32,
                ts: s.ts_ns,
                value: s.value,
            });
        }
    }

    let mut sorted = input_order.clone();
    sorted.sort_by(|a, b| {
        metas[a.series as usize]
            .id
            .cmp(&metas[b.series as usize].id)
            .then_with(|| a.ts.cmp(&b.ts))
    });

    let sorted_batch = build_batch(&schema, &sorted, &metas, label_names.len());
    let unsorted_batch = build_batch(&schema, &input_order, &metas, label_names.len());

    let (pq_sorted_ms, pq_sorted_bytes) =
        median_write(|| write_parquet(Arc::clone(&schema), &sorted_batch));
    let (pq_unsorted_ms, pq_unsorted_bytes) =
        median_write(|| write_parquet(Arc::clone(&schema), &unsorted_batch));

    // RSEG v1/v2 from the same logical content. build_segment consumes its
    // input, so pre-clone RUNS copies outside the timed region (the
    // in-timed-region clone bias fixed by commit d605400, BENCHMARKS.md).
    let (rseg_v1_ms, rseg_v1_bytes) = median_rseg(&raw, build_segment);
    let (rseg_v2_ms, rseg_v2_bytes) = median_rseg(&raw, build_segment_v2);

    println!("  encoder            median ms     object bytes");
    print_row("parquet sorted", pq_sorted_ms, pq_sorted_bytes);
    print_row("parquet unsorted", pq_unsorted_ms, pq_unsorted_bytes);
    print_row("rseg v1", rseg_v1_ms, rseg_v1_bytes);
    print_row("rseg v2", rseg_v2_ms, rseg_v2_bytes);
    println!();
}

fn print_row(name: &str, ms: f64, bytes: usize) {
    println!("  {name:<18} {ms:>10.3}   {bytes:>14}");
}

// ------------------------------------------------------------------- rows

#[derive(Clone)]
struct Row {
    series: u32,
    ts: i64,
    value: f64,
}

struct SeriesMeta {
    id: [u8; 16],
    /// Aligned to the shared, sorted label-name list.
    label_values: Vec<String>,
}

fn collect_label_names(raw: &[RawSeries]) -> Vec<String> {
    let mut names: BTreeSet<String> = BTreeSet::new();
    for (_, labels, _) in raw {
        for l in labels.iter() {
            names.insert(l.name.clone());
        }
    }
    names.into_iter().collect()
}

fn label_values(labels: &LabelSet, names: &[String]) -> Vec<String> {
    names
        .iter()
        .map(|n| {
            labels
                .iter()
                .find(|l| &l.name == n)
                .map(|l| l.value.clone())
                .unwrap_or_default()
        })
        .collect()
}

// ------------------------------------------------------------------ arrow

fn build_schema(label_names: &[String]) -> SchemaRef {
    let mut fields = vec![
        Field::new("series_id", DataType::FixedSizeBinary(16), false),
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ];
    for name in label_names {
        fields.push(Field::new(
            name,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ));
    }
    Arc::new(Schema::new(fields))
}

fn build_batch(
    schema: &SchemaRef,
    rows: &[Row],
    metas: &[SeriesMeta],
    label_count: usize,
) -> RecordBatch {
    let mut sid = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut ts: Vec<i64> = Vec::with_capacity(rows.len());
    let mut val: Vec<f64> = Vec::with_capacity(rows.len());
    let mut labels: Vec<StringDictionaryBuilder<Int32Type>> = (0..label_count)
        .map(|_| StringDictionaryBuilder::new())
        .collect();

    for row in rows {
        let meta = &metas[row.series as usize];
        sid.append_value(meta.id).expect("append series_id");
        ts.push(row.ts);
        val.push(row.value);
        for (col, builder) in labels.iter_mut().enumerate() {
            builder.append_value(&meta.label_values[col]);
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(3 + label_count);
    columns.push(Arc::new(sid.finish()));
    columns.push(Arc::new(Int64Array::from(ts)));
    columns.push(Arc::new(Float64Array::from(val)));
    for mut builder in labels {
        columns.push(Arc::new(builder.finish()));
    }

    RecordBatch::try_new(Arc::clone(schema), columns).expect("build record batch")
}

fn writer_props() -> WriterProperties {
    let value = ColumnPath::from("value".to_string());
    let ts = ColumnPath::from("ts".to_string());
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3"),
        ))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        // Page index (column index + offset index) is written when
        // page-level statistics are enabled.
        .set_statistics_enabled(EnabledStatistics::Page)
        // value: BYTE_STREAM_SPLIT requires dictionary off for the column.
        .set_column_dictionary_enabled(value.clone(), false)
        .set_column_encoding(value, Encoding::BYTE_STREAM_SPLIT)
        // ts: DELTA_BINARY_PACKED, dictionary off.
        .set_column_dictionary_enabled(ts.clone(), false)
        .set_column_encoding(ts, Encoding::DELTA_BINARY_PACKED)
        // Labels keep the global default (dictionary on -> RLE_DICTIONARY).
        .build()
}

/// Writes `batch` to an in-memory sink and returns the byte length.
fn write_parquet(schema: SchemaRef, batch: &RecordBatch) -> usize {
    let mut buf: Vec<u8> = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema, Some(writer_props())).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
    buf.len()
}

// ------------------------------------------------------------------ timing

/// Median wall (ms) over [`RUNS`] writes and the (deterministic) byte size.
fn median_write(mut f: impl FnMut() -> usize) -> (f64, usize) {
    let mut times = Vec::with_capacity(RUNS);
    let mut bytes = 0usize;
    for _ in 0..RUNS {
        let start = Instant::now();
        bytes = f();
        times.push(start.elapsed().as_secs_f64() * 1e3);
    }
    (median(&mut times), bytes)
}

/// Median RSEG encode wall (ms) and object bytes. Clones are prepared
/// outside the timed region so the clone cost is not charged to the encode.
fn median_rseg(
    raw: &[RawSeries],
    build: fn(Vec<RawSeries>) -> ravel_segment::WrittenSegment,
) -> (f64, usize) {
    let mut clones: Vec<Vec<RawSeries>> = (0..RUNS).map(|_| raw.to_vec()).collect();
    let mut times = Vec::with_capacity(RUNS);
    let mut bytes = 0usize;
    for input in clones.drain(..) {
        let start = Instant::now();
        let written = build(input);
        times.push(start.elapsed().as_secs_f64() * 1e3);
        bytes = written.bytes.len();
    }
    (median(&mut times), bytes)
}

fn median(times: &mut [f64]) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).expect("no NaN timings"));
    times[times.len() / 2]
}
