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
//!
//! The `contains_kernel_block` group times the whole kernel call (issue #3295)
//! on one writer-built, decoded block of 8,192 rows: `URL` cells of 40 to 160
//! bytes, the needle in about 1% of them, rows over 4 streams none of which
//! carries `URL`, and no other column for the key. `plain` stores a value per
//! row (a plain page); `dict` draws the values from 2,000 (the first block of
//! four sharing a row group dictionary, the shape the writer picks for them);
//! `attrs_raw` is `plain` with an overflowing attribute, not `URL`, on 5% of
//! rows. Beside each `kernel` case, `search` runs the needle over the cells the
//! kernel tests, alone, so `kernel - search` is the kernel's work outside the
//! search. Throughput is in rows. The corpus shape, the rows kept, and the
//! cells tested are checked before timing.

#![allow(clippy::expect_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::collections::HashSet;
use std::hint::black_box;

use ravel_logseg::reader::{ContainsKernelBench, ContainsKernelShape, ContainsNeedle};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_logseg::stream_attrs_bytes;
use ravel_types::logstream::log_stream_id;

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

/// Streams per block in the block corpus; none carries `URL`.
const STREAMS_PER_BLOCK: usize = 4;
/// Distinct `URL` values in the dictionary case.
const DICT_VALUES: usize = 2000;
/// Blocks in the dictionary case's row group.
const DICT_BLOCKS: usize = 4;
/// One row in this many carries the overflowing attribute in the `attrs_raw`
/// case.
const ATTRS_RAW_EVERY: usize = 20;

/// The next draw of a deterministic LCG.
fn next(state: &mut u64) -> usize {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    (*state >> 33) as usize
}

/// `n` URL-like values of 40 to 160 bytes; value `i` holds the needle when
/// `i % 100 == 7`, 1% of them.
fn urls(n: usize) -> Vec<String> {
    let mut state: u64 = 0x2545_f491_4f6c_dd1d;
    (0..n)
        .map(|i| {
            let len = 40 + next(&mut state) % 121;
            let mut cell: Vec<u8> = b"https://".to_vec();
            while cell.len() < len {
                cell.push(ALPHABET[next(&mut state) % ALPHABET.len()]);
            }
            if i % 100 == 7 {
                let at = 8 + next(&mut state) % (len - 8 - NEEDLE.len());
                cell[at..at + NEEDLE.len()].copy_from_slice(NEEDLE);
            }
            String::from_utf8(cell).expect("ascii")
        })
        .collect()
}

fn stream(n: usize) -> (LogStreamId, Vec<u8>) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc-{n}")),
    )];
    (
        log_stream_id(&resource, "scope", "1", &[]),
        stream_attrs_bytes(&resource, "scope", "1", &[]),
    )
}

/// `blocks` blocks of `CELLS` rows in one row group, row `i` on stream
/// `i % (4 * blocks)` with `URL` `values[pick(i)]`, and with the attribute `zz`
/// (overflowing into `attrs_raw`) on every row `i % every == 0` when
/// `attrs_raw_every` is set.
fn block_object(
    values: &[String],
    pick: impl Fn(usize) -> usize,
    blocks: usize,
    attrs_raw_every: Option<usize>,
) -> (Vec<u8>, RlogConfig) {
    let cfg = RlogConfig {
        block_target_records: CELLS,
        group_target_blocks: blocks,
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 1,
        writer_id: [9u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let streams: Vec<_> = (0..STREAMS_PER_BLOCK * blocks).map(stream).collect();
    let mut w = RlogWriter::new(cfg, identity);
    for i in 0..CELLS * blocks {
        let (stream_id, stream_attrs) = streams[i % streams.len()].clone();
        let mut attrs = vec![("URL".to_string(), AttrValue::Str(values[pick(i)].clone()))];
        if attrs_raw_every.is_some_and(|every| i % every == 0) {
            attrs.push(("zz".to_string(), AttrValue::Str(format!("z{i}"))));
        }
        w.push(LogRecord {
            stream_id,
            stream_attrs,
            ts_ns: i as i64,
            observed_ts_ns: i as i64,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "b".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        })
        .expect("push");
    }
    (w.finish().expect("finish"), cfg)
}

fn bench_block(c: &mut Criterion) {
    let needle = std::str::from_utf8(NEEDLE).expect("ascii");
    let plain = urls(CELLS);
    let pooled = urls(DICT_VALUES);
    let draws: Vec<usize> = {
        let mut state: u64 = 0x5851_f42d_4c95_7f2d;
        (0..CELLS * DICT_BLOCKS)
            .map(|_| next(&mut state) % DICT_VALUES)
            .collect()
    };
    let cases = [
        ("plain", block_object(&plain, |i| i, 1, None), None, 0),
        (
            "dict",
            block_object(&pooled, |i| draws[i], DICT_BLOCKS, None),
            Some(DICT_VALUES),
            0,
        ),
        (
            "attrs_raw",
            block_object(&plain, |i| i, 1, Some(ATTRS_RAW_EVERY)),
            None,
            CELLS.div_ceil(ATTRS_RAW_EVERY),
        ),
    ];
    let mut group = c.benchmark_group("contains_kernel_block");
    group.throughput(Throughput::Elements(CELLS as u64));
    for (name, (object, cfg), dict_entries, attrs_raw_cells) in cases {
        let k = ContainsKernelBench::new(&object, &cfg, "URL", needle).expect("open");
        assert_eq!(
            k.shape(),
            ContainsKernelShape {
                rows: CELLS,
                streams: STREAMS_PER_BLOCK,
                dict_entries,
                attrs_raw_cells,
                others: 0,
            },
            "{name}: corpus shape"
        );
        let cells = k.key_cells();
        assert!(cells.iter().all(Option::is_some), "{name}: every row has a URL");
        let hits = cells
            .iter()
            .flatten()
            .filter(|c| windows_contains(c, NEEDLE))
            .count();
        assert!(
            (CELLS / 200..CELLS / 50).contains(&hits),
            "{name}: {hits} hits, about 1% wanted"
        );
        let distinct: HashSet<&[u8]> = cells.iter().flatten().map(Vec::as_slice).collect();
        let tested_want = match dict_entries {
            Some(_) => {
                assert_eq!(k.dict_keys().len(), distinct.len(), "{name}: entries used");
                assert!(
                    distinct.len() > DICT_VALUES * 9 / 10,
                    "{name}: {} of {DICT_VALUES} entries used",
                    distinct.len()
                );
                distinct.len()
            }
            None => CELLS,
        };
        let (kept, tested) = k.run().expect("kernel");
        assert_eq!(kept, hits, "{name}: rows kept");
        assert_eq!(tested, tested_want as u64, "{name}: cells tested");
        let (searched_hits, searched) = k.search_only();
        assert_eq!(searched, tested, "{name}: search covers the kernel's tests");
        let searched_want = match dict_entries {
            Some(_) => distinct.iter().filter(|c| windows_contains(c, NEEDLE)).count(),
            None => hits,
        };
        assert_eq!(searched_hits, searched_want, "{name}: search hits");

        group.bench_function(BenchmarkId::new("kernel", name), |b| {
            b.iter(|| black_box(&k).run().expect("kernel"));
        });
        group.bench_function(BenchmarkId::new("search", name), |b| {
            b.iter(|| black_box(&k).search_only());
        });
    }
    group.finish();
}

criterion_group!(benches, bench, bench_block);
criterion_main!(benches);
