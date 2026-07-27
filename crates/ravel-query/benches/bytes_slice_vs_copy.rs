//! Isolates the `FetchedRegions::slice` fix (docs/reviews/2026-07-27
//! -arrow-datafusion-plan-review.md finding F1): `Bytes::slice` (refcounted,
//! O(1)) versus the old `Bytes::copy_from_slice` (memcpy, O(len)) for
//! carving a page-sized region out of a suffix-fetch-sized buffer. Uses
//! `bytes::Bytes` directly rather than the private `FetchedRegions` type,
//! since the operation being measured is exactly the one line that changed.
#![allow(clippy::expect_used)]

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

/// Default suffix-fetch size (`DEFAULT_SUFFIX_LEN` in `fetcher.rs`).
const BUFFER_LEN: usize = 64 * 1024;
/// A typical TS or VAL page size for a few dozen samples.
const REGION_LEN: usize = 512;

fn bench_bytes_slice_vs_copy(c: &mut Criterion) {
    let buffer = Bytes::from(vec![0xABu8; BUFFER_LEN]);
    let start = 1024;
    let end = start + REGION_LEN;

    let mut group = c.benchmark_group("bytes_slice_vs_copy");
    group.throughput(Throughput::Bytes(REGION_LEN as u64));

    group.bench_function("bytes_slice_refcounted", |b| {
        b.iter(|| {
            let region = buffer.slice(start..end);
            std::hint::black_box(&region);
        });
    });

    group.bench_function("copy_from_slice", |b| {
        b.iter(|| {
            let region = Bytes::copy_from_slice(&buffer[start..end]);
            std::hint::black_box(&region);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_bytes_slice_vs_copy);
criterion_main!(benches);
