//! RLOG encode throughput: `RlogWriter` push+finish over a fixed record
//! budget, swept across stream cardinality. Mirrors
//! `ravel-bench/benches/segment_encode.rs`'s series-cardinality sweep, with
//! log streams standing in for metric series: a `1_stream` shape (one stream
//! absorbing every record) versus a `many_streams` shape (cardinality spread
//! thin), at three record-count totals.
//!
//! The `ingest_shape` group encodes one ingest flush of the ADR-3492 benchmark
//! shape (`common/ingest_shape.rs`) eight ways: the row writer and the
//! columnar writer, each under both page choices, each finished serially and
//! with its column pages encoded on [`THREADS`] threads (ADR-3492 decision 7).
//! The columnar arms time `ColumnarLogBatch::try_from_records` per push batch
//! too, the work `rows_to_batch` does at ingest. A serial arm's timing is CPU
//! time; a threaded arm's is wall time. Before timing, each flush size's
//! stored object bytes are printed per arm, and the run fails unless the two
//! writers and the threaded encode agree byte for byte under each choice and
//! `Estimated` stores at most [`MAX_ESTIMATED_GROWTH`] more than `Exhaustive`
//! (ADR-3492, exit criteria).
#![allow(clippy::expect_used)]

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use ravel_logseg::{ColumnarLogBatch, LogRecord, PageChoice, RlogWriter};

#[path = "common/mod.rs"]
mod support;
use support::{bench_config, bench_identity, build_corpus};

#[path = "common/ingest_shape.rs"]
mod ingest_shape;
use ingest_shape::{PUSH_BATCH, ingest_records};

#[path = "common/page_pool.rs"]
mod page_pool;
use page_pool::ScopedPool;

/// Threads the threaded arms encode column pages on.
const THREADS: usize = 4;

/// Record-count totals for the sweep. Kept well under the 8192-record block
/// target's multi-block territory but large enough to amortize per-object
/// fixed costs (footer, directories); 200_000 mirrors segment_encode's
/// default budget, scaled down for logseg's heavier per-record encode.
const RECORD_TOTALS: [usize; 2] = [2_000, 20_000];

/// `(name, records_per_stream)` shapes. `1_stream` puts every record on one
/// stream (stream_count = 1); `many_streams` fixes 20 records per stream, a
/// cardinality dense enough to give STREAM_DIR and per-stream block-range
/// bookkeeping real work without degenerating to one record per stream.
const STREAM_SHAPES: [(&str, usize); 2] = [("1_stream", usize::MAX), ("many_streams", 20)];

fn bench_logseg_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("logseg_encode");
    for &total_records in &RECORD_TOTALS {
        for &(shape_name, records_per_stream) in &STREAM_SHAPES {
            let records_per_stream = records_per_stream.min(total_records);
            let stream_count = (total_records / records_per_stream).max(1);
            let corpus = build_corpus(stream_count, records_per_stream);
            let actual_records = corpus.len();
            group.throughput(Throughput::Elements(actual_records as u64));
            group.bench_function(format!("{total_records}_records_{shape_name}"), |b| {
                b.iter_batched(
                    || corpus.clone(),
                    |records| {
                        let mut w = RlogWriter::new(bench_config(), bench_identity());
                        for r in records {
                            w.push(r).expect("push");
                        }
                        w.finish().expect("finish")
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

/// Records per ingest flush.
const FLUSH_SIZES: [usize; 2] = [7_500, 30_000];

const CHOICES: [(&str, PageChoice); 2] = [
    ("exhaustive", PageChoice::Exhaustive),
    ("estimated", PageChoice::Estimated),
];

/// The most `Estimated` objects may store beyond `Exhaustive` ones.
const MAX_ESTIMATED_GROWTH: f64 = 0.10;

fn writer(choice: PageChoice) -> RlogWriter {
    let cfg = ravel_logseg::RlogConfig {
        page_choice: choice,
        ..bench_config()
    };
    RlogWriter::new(cfg, bench_identity())
}

/// Finishes `w` serially, or with its pages encoded on `pool`.
fn finish(w: RlogWriter, pool: Option<&mut ScopedPool>) -> Vec<u8> {
    match pool {
        Some(pool) => w.finish_on(pool).expect("finish_on"),
        None => w.finish().expect("finish"),
    }
}

fn encode_rows(
    records: Vec<LogRecord>,
    choice: PageChoice,
    pool: Option<&mut ScopedPool>,
) -> Vec<u8> {
    let mut w = writer(choice);
    for r in records {
        w.push(r).expect("push");
    }
    finish(w, pool)
}

fn encode_columnar(
    records: Vec<LogRecord>,
    choice: PageChoice,
    pool: Option<&mut ScopedPool>,
) -> Vec<u8> {
    let mut w = writer(choice);
    for batch in records.chunks(PUSH_BATCH) {
        let batch = ColumnarLogBatch::try_from_records(batch).expect("batch");
        w.push_columnar(batch).expect("push_columnar");
    }
    finish(w, pool)
}

/// Prints each arm's stored object bytes for a flush and fails the run when
/// the writers disagree or `Estimated` grows past its bound.
fn check_sizes(flush: usize, records: &[LogRecord]) {
    let mut sizes = [0usize; 2];
    for (k, &(name, choice)) in CHOICES.iter().enumerate() {
        let row = encode_rows(records.to_vec(), choice, None);
        let columnar = encode_columnar(records.to_vec(), choice, None);
        assert!(
            row == columnar,
            "{flush} records, {name}: row and columnar bytes differ"
        );
        let mut pool = ScopedPool::new(THREADS);
        let row_threaded = encode_rows(records.to_vec(), choice, Some(&mut pool));
        let columnar_threaded = encode_columnar(records.to_vec(), choice, Some(&mut pool));
        assert!(
            row_threaded == row && columnar_threaded == row,
            "{flush} records, {name}: threaded page encode bytes differ from finish"
        );
        assert!(
            pool.split_calls > 0 && pool.out_of_order_calls > 0,
            "{flush} records, {name}: no block was split and returned out of order"
        );
        println!(
            "ingest_shape/{flush} stored_bytes row_{name}={} columnar_{name}={}",
            row.len(),
            columnar.len()
        );
        sizes[k] = row.len();
    }
    let growth = sizes[1] as f64 / sizes[0] as f64 - 1.0;
    println!(
        "ingest_shape/{flush} estimated_vs_exhaustive={:+.3}%",
        growth * 100.0
    );
    assert!(
        growth <= MAX_ESTIMATED_GROWTH,
        "{flush} records: Estimated grows {growth:.4}"
    );
}

fn bench_ingest_shape(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_shape");
    group.sample_size(10);
    for &flush in &FLUSH_SIZES {
        let records = ingest_records(flush);
        check_sizes(flush, &records);
        group.throughput(Throughput::Elements(flush as u64));
        for &(name, choice) in &CHOICES {
            group.bench_function(format!("{flush}/row_{name}"), |b| {
                b.iter_batched(
                    || records.clone(),
                    |records| encode_rows(records, choice, None),
                    BatchSize::LargeInput,
                );
            });
            group.bench_function(format!("{flush}/columnar_{name}"), |b| {
                b.iter_batched(
                    || records.clone(),
                    |records| encode_columnar(records, choice, None),
                    BatchSize::LargeInput,
                );
            });
            group.bench_function(format!("{flush}/row_{name}_{THREADS}threads"), |b| {
                let mut pool = ScopedPool::new(THREADS);
                b.iter_batched(
                    || records.clone(),
                    |records| encode_rows(records, choice, Some(&mut pool)),
                    BatchSize::LargeInput,
                );
            });
            group.bench_function(format!("{flush}/columnar_{name}_{THREADS}threads"), |b| {
                let mut pool = ScopedPool::new(THREADS);
                b.iter_batched(
                    || records.clone(),
                    |records| encode_columnar(records, choice, Some(&mut pool)),
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_logseg_encode, bench_ingest_shape);
criterion_main!(benches);
