//! Per-page decode cost of `page::read_page` on many small zstd pages, the
//! shape where fixed per-call costs (decompression context setup) dominate
//! over the decompression itself. Pages are built once, untimed; one
//! iteration decodes every page, so ns/page is the reported time divided by
//! `PAGES`.
#![allow(clippy::expect_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_logseg::encoding::Enc;
use ravel_logseg::page::{COMP_ZSTD, DEFAULT_MAX_UNCOMP, PageDesc, write_page};

const PAGES: usize = 1_024;
/// Encoded bytes per page: above the 512-byte compression floor, small
/// enough that one decode is a few microseconds.
const PAGE_BYTES: usize = 2_048;

fn build_pages() -> Vec<(Vec<u8>, PageDesc)> {
    (0..PAGES)
        .map(|p| {
            let encoded: Vec<u8> = (0..PAGE_BYTES)
                .map(|i| ((i / 7) as u8).wrapping_add(p as u8))
                .collect();
            let mut out = Vec::new();
            let desc = write_page(&mut out, 0, Enc::Plain, &encoded, 3);
            assert_eq!(desc.comp, COMP_ZSTD, "bench pages must be zstd-compressed");
            (out, desc)
        })
        .collect()
}

fn bench_page_decode(c: &mut Criterion) {
    let pages = build_pages();
    let mut group = c.benchmark_group("page_decode");
    group.throughput(Throughput::Elements(PAGES as u64));
    group.bench_function("small_zstd_pages", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for (stored, desc) in &pages {
                let decoded = ravel_logseg::page::read_page(stored, desc, DEFAULT_MAX_UNCOMP)
                    .expect("decode");
                total += decoded.len();
            }
            assert_eq!(total, PAGES * PAGE_BYTES);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_page_decode);
criterion_main!(benches);
