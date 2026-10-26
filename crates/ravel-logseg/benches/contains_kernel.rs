//! The contains arm's byte search (issues #2865 and #3295): the
//! `memchr::memmem` finder the kernel builds once per arm
//! (`ravel_logseg::reader::ContainsNeedle`) against the per-position
//! `windows` scan it replaced, and the finder called once per cell against one
//! search over the cells laid end to end (`ContainsNeedle::is_in_each`). The
//! buffer search is timed three ways: over a buffer built beforehand
//! (`buffer`, what a page decoded to one contiguous buffer would cost), and
//! with the copy of every cell into the buffer inside the timed pass, as the
//! kernel would have to do over today's one-allocation-per-cell plain pages,
//! in one buffer (`copy_buffer`) or in 16 KiB chunks (`chunk_buffer`). The
//! corpus is 8,192 URL-like cells of 128 bytes and 4,096 of 32 bytes, with the
//! needle absent, at the end of every cell, and early in every cell.
//!
//! Each case is one pass over all cells, and criterion's throughput is in
//! cells, so ns per cell is the reciprocal of the reported elem/s. Before
//! timing, every search is checked to give the expected hit count on every
//! case, so a corpus that accidentally holds the needle fails rather than
//! timing the wrong thing.

#![allow(clippy::expect_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use ravel_logseg::reader::ContainsNeedle;

const NEEDLE: &[u8] = b"google";
/// Lowercase letters less `g`, so the needle's first byte never occurs by
/// chance, plus URL punctuation.
const ALPHABET: &[u8] = b"abcdefhijklmnopqrstuvwxyz0123456789/.-_?=&";
/// The `chunk_buffer` lane's buffer size, small enough to stay in L1.
const CHUNK_BYTES: usize = 16 * 1024;

/// The search the contains kernel ran before the finder: one comparison per
/// byte position.
fn windows_contains(hay: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle)
}

/// `count` deterministic URL-like cells of `len` bytes, with the needle written
/// at byte `at` of every cell when it is `Some`.
fn corpus(count: usize, len: usize, at: Option<usize>) -> Vec<Vec<u8>> {
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..count)
        .map(|_| {
            let mut cell: Vec<u8> = b"https://".to_vec();
            while cell.len() < len {
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

/// `cells` laid end to end, with each cell's end offset: the layout
/// `is_in_each` searches. Clears both first so the buffers are reused.
fn lay_out(cells: &[Vec<u8>], bytes: &mut Vec<u8>, ends: &mut Vec<usize>) {
    bytes.clear();
    ends.clear();
    for c in cells {
        bytes.extend_from_slice(c);
        ends.push(bytes.len());
    }
}

fn bench(c: &mut Criterion) {
    let finder = ContainsNeedle::new(NEEDLE);
    let cases = [
        ("absent", 8192, 128, None),
        ("end", 8192, 128, Some(128 - NEEDLE.len())),
        ("early", 8192, 128, Some(8)),
        ("absent_32b", 4096, 32, None),
        ("end_32b", 4096, 32, Some(32 - NEEDLE.len())),
        ("early_32b", 4096, 32, Some(8)),
    ];
    let mut group = c.benchmark_group("contains_kernel");
    for (name, count, len, at) in cases {
        let hits = if at.is_some() { count } else { 0 };
        let cells = corpus(count, len, at);
        assert!(cells.iter().all(|c| c.len() == len));
        let (mut bytes, mut ends, mut verdicts) = (Vec::new(), Vec::new(), Vec::new());
        lay_out(&cells, &mut bytes, &mut ends);
        finder.is_in_each(&bytes, &ends, &mut verdicts);
        let by_windows = cells.iter().filter(|c| windows_contains(c, NEEDLE)).count();
        let by_finder = cells.iter().filter(|c| finder.is_in(c)).count();
        let by_buffer = verdicts.iter().filter(|&&v| v).count();
        assert_eq!(by_windows, hits, "{name}: windows hit count");
        assert_eq!(by_finder, hits, "{name}: finder hit count");
        assert_eq!(by_buffer, hits, "{name}: buffer hit count");
        assert_eq!(verdicts.len(), count, "{name}: one verdict per cell");

        group.throughput(Throughput::Elements(count as u64));
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
        group.bench_with_input(BenchmarkId::new("buffer", name), &cells, |b, _| {
            b.iter(|| {
                black_box(&finder).is_in_each(black_box(&bytes), &ends, &mut verdicts);
                verdicts.iter().filter(|&&v| v).count()
            });
        });
        group.bench_with_input(BenchmarkId::new("copy_buffer", name), &cells, |b, cells| {
            let (mut bytes, mut ends) = (Vec::new(), Vec::new());
            b.iter(|| {
                lay_out(black_box(cells), &mut bytes, &mut ends);
                black_box(&finder).is_in_each(&bytes, &ends, &mut verdicts);
                verdicts.iter().filter(|&&v| v).count()
            });
        });
        group.bench_with_input(
            BenchmarkId::new("chunk_buffer", name),
            &cells,
            |b, cells| {
                let (mut bytes, mut ends) = (Vec::new(), Vec::new());
                b.iter(|| {
                    let mut n = 0;
                    for chunk in black_box(cells).chunks(CHUNK_BYTES / len) {
                        lay_out(chunk, &mut bytes, &mut ends);
                        black_box(&finder).is_in_each(&bytes, &ends, &mut verdicts);
                        n += verdicts.iter().filter(|&&v| v).count();
                    }
                    n
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
