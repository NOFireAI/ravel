//! RLOG decode+scan throughput: `RlogReader::scan` over a pre-built object
//! (issue #267). The object is built once via `RlogWriter` in an untimed
//! setup step (mirroring `segment_encode.rs`'s `iter_batched` setup/measure
//! split); only `scan` itself is timed. Two predicates show the
//! pruning-vs-full-scan difference: `Predicate::And(vec![])` (matches every
//! record, no pruning) and a selective `HasWord` on a rare planted token
//! (`support::make_record` plants "needle" on 1-in-97 records), which the
//! skip index and bloom should prune down to a small fraction of blocks.
#![allow(clippy::expect_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_logseg::{FieldSel, Predicate, RlogReader, RlogWriter};

#[path = "common/mod.rs"]
mod support;
use support::{bench_config, bench_identity, build_corpus};

/// Small enough for `block_target_records` (default 8192) to still yield many
/// blocks (records // 8192 ~= 24), so the skip index and bloom pruning have
/// real block-selection work to do; large enough to amortize per-scan
/// fixed costs.
const STREAM_COUNT: usize = 200;
const RECORDS_PER_STREAM: usize = 1_000;

fn build_object() -> Vec<u8> {
    let corpus = build_corpus(STREAM_COUNT, RECORDS_PER_STREAM);
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in corpus {
        w.push(r).expect("push");
    }
    w.finish().expect("finish")
}

fn bench_logseg_scan(c: &mut Criterion) {
    let object = build_object();
    let cfg = bench_config();
    let total_records = (STREAM_COUNT * RECORDS_PER_STREAM) as u64;

    let mut group = c.benchmark_group("logseg_scan");
    group.throughput(Throughput::Elements(total_records));

    group.bench_function("unfiltered_full_scan", |b| {
        b.iter(|| {
            let reader = RlogReader::new(&object, &cfg).expect("open");
            let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
            assert_eq!(rows.len() as u64, total_records);
        });
    });

    group.bench_function("selective_hasword_needle", |b| {
        b.iter(|| {
            let reader = RlogReader::new(&object, &cfg).expect("open");
            let (rows, _stats) = reader
                .scan(&Predicate::HasWord {
                    field: FieldSel::Body,
                    word: "needle".into(),
                })
                .expect("scan");
            assert!(!rows.is_empty());
        });
    });

    group.finish();
}

criterion_group!(benches, bench_logseg_scan);
criterion_main!(benches);
