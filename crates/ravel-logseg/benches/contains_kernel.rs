//! The contains arm's per-cell byte search (issue #2865): the
//! `memchr::memmem` finder the kernel builds once per arm
//! (`ravel_logseg::reader::ContainsNeedle`) against the per-position
//! `windows` scan it replaced, over 8,192 URL-like cells of 128 bytes with the
//! needle absent, at the end of every cell, and early in every cell.
//!
//! Each case is one pass over all 8,192 cells, and criterion's throughput is in
//! cells, so ns per cell is the reciprocal of the reported elem/s. Before
//! timing, both searches are checked to give the expected hit count on every
//! case, so a corpus that accidentally holds the needle fails rather than
//! timing the wrong thing.

#![allow(clippy::expect_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use ravel_logseg::reader::ContainsNeedle;

const CELLS: usize = 8192;
const CELL_LEN: usize = 128;
const NEEDLE: &[u8] = b"google";
/// Lowercase letters less `g`, so the needle's first byte never occurs by
/// chance, plus URL punctuation.
const ALPHABET: &[u8] = b"abcdefhijklmnopqrstuvwxyz0123456789/.-_?=&";

/// The search the contains kernel ran before the finder: one comparison per
/// byte position.
fn windows_contains(hay: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle)
}

/// `CELLS` deterministic URL-like cells of `CELL_LEN` bytes, with the needle
/// written at byte `at` of every cell when it is `Some`.
fn corpus(at: Option<usize>) -> Vec<Vec<u8>> {
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..CELLS)
        .map(|_| {
            let mut cell: Vec<u8> = b"https://".to_vec();
            while cell.len() < CELL_LEN {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                cell.push(ALPHABET[(state >> 33) as usize % ALPHABET.len()]);
            }
            if let Some(at) = at {
                cell[at..at + NEEDLE.len()].copy_from_slice(NEEDLE);
            }
            cell
        })
        .collect()
}

fn bench(c: &mut Criterion) {
    let finder = ContainsNeedle::new(NEEDLE);
    let cases = [
        ("absent", None, 0),
        ("end", Some(CELL_LEN - NEEDLE.len()), CELLS),
        ("early", Some(8), CELLS),
    ];
    let mut group = c.benchmark_group("contains_kernel");
    group.throughput(Throughput::Elements(CELLS as u64));
    for (name, at, hits) in cases {
        let cells = corpus(at);
        assert!(cells.iter().all(|c| c.len() == CELL_LEN));
        let by_windows = cells.iter().filter(|c| windows_contains(c, NEEDLE)).count();
        let by_finder = cells.iter().filter(|c| finder.is_in(c)).count();
        assert_eq!(by_windows, hits, "{name}: windows hit count");
        assert_eq!(by_finder, hits, "{name}: finder hit count");

        group.bench_with_input(BenchmarkId::new("windows", name), &cells, |b, cells| {
            b.iter(|| {
                cells
                    .iter()
                    .filter(|c| windows_contains(black_box(c), black_box(NEEDLE)))
                    .count()
            });
        });
        group.bench_with_input(BenchmarkId::new("finder", name), &cells, |b, cells| {
            b.iter(|| {
                cells
                    .iter()
                    .filter(|c| black_box(&finder).is_in(black_box(c)))
                    .count()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
