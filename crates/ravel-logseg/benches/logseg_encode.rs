//! RLOG encode throughput: `RlogWriter` push+finish over a fixed record
//! budget, swept across stream cardinality. Mirrors
//! `ravel-bench/benches/segment_encode.rs`'s series-cardinality sweep, with
//! log streams standing in for metric series: a `1_stream` shape (one stream
//! absorbing every record) versus a `many_streams` shape (cardinality spread
//! thin), at three record-count totals.
#![allow(clippy::expect_used)]

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use ravel_logseg::RlogWriter;

#[path = "common/mod.rs"]
mod support;
use support::{bench_config, bench_identity, build_corpus};

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

criterion_group!(benches, bench_logseg_encode);
criterion_main!(benches);
