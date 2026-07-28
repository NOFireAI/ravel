//! Within-segment selective-read GET accounting (GitHub issue #167).
//!
//! Measures what a point lookup, a 10-series multi-lookup, and a full scan
//! each cost -- in GET requests and bytes transferred -- for three RSEG
//! layouts of the *same* logical workload:
//!
//!   (a) status quo v2  (`SegmentWriter::write_v2`)
//!   (b) SERIES_IDX only (additive sparse id index; still a valid v2 object)
//!   (c) SERIES_IDX + chunked meta (SERIES_META re-laid as per-chunk zstd
//!       frames; prototype, non-additive)
//!
//! at 500 / 10k / 100k series (`many_small`, 2 samples/series), the shapes the
//! #97 read-path accounting used.
//!
//! GET count and bytes are backend-independent, so -- exactly as the #97
//! harness argues -- they are the deliverable regardless of backend. Rather
//! than spin up an async object store, this bin meters an in-memory object
//! directly: every metered fetch records one GET and the bytes it returned,
//! the same accounting `CountingBackend::record_get` performs. The lookups are
//! keyed by series id (what the sparse index accelerates): a point lookup is a
//! binary search plus a few range-GETs, and the 10-series pattern is ten such
//! id lookups that share one SERIES_IDX fetch.
//!
//! Report-only: never changes library behavior. Not gated on any feature (no
//! arrow/parquet), so it builds and its correctness test runs under the
//! default `cargo test -p ravel-bench`.
//!
//! Run: `cargo run -p ravel-bench --release --bin selective_read_accounting`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{
    SERIES_IDS, SERIES_IDX, SERIES_META, SERIES_META_CHUNKS, TS_PAGES, VAL_PAGES, build_segment_v2,
    build_segment_v2_experimental,
};
use ravel_segment::Footer;
use ravel_segment::experiment_api::{
    DEFAULT_STRIDE as K, ExperimentFlags, PageLoc, decode_chunk_page_location,
    decode_page_locations_v2, decompress_chunk_frame, find_index_in_window, parse_series_idx,
};
use ravel_segment::{FooterOutcome, ReaderLimits};

/// RSEG reader-protocol suffix probe (docs/segment-format.md step 2), same
/// 64 KiB the #97 harness used.
const SUFFIX_PROBE: u64 = 64 * 1024;
/// Series counts (many_small, 2 samples/series).
const SHAPES: [usize; 3] = [500, 10_000, 100_000];
/// Decision-rule thresholds (issue #167): point-lookup bytes at 100k must drop
/// below this, and (c)'s object may grow by at most this fraction.
const POINT_LOOKUP_TARGET_BYTES: u64 = 400 * 1024;
const MAX_OBJECT_GROWTH: f64 = 0.02;

/// In-memory GET meter: mirrors `CountingBackend::record_get` (one GET, plus
/// the bytes it returned) without a real object store.
#[derive(Default, Clone, Copy)]
struct Meter {
    gets: u64,
    bytes: u64,
}

impl Meter {
    fn range<'a>(&mut self, obj: &'a [u8], off: u64, len: u64) -> &'a [u8] {
        let start = off as usize;
        let end = start + len as usize;
        self.gets += 1;
        self.bytes += (end - start) as u64;
        &obj[start..end]
    }

    fn suffix<'a>(&mut self, obj: &'a [u8], n: u64) -> &'a [u8] {
        let n = (n as usize).min(obj.len());
        self.gets += 1;
        self.bytes += n as u64;
        &obj[obj.len() - n..]
    }

    fn full<'a>(&mut self, obj: &'a [u8]) -> &'a [u8] {
        self.gets += 1;
        self.bytes += obj.len() as u64;
        obj
    }
}

fn main() {
    println!("== within-segment selective-read GET accounting (issue #167) ==");
    println!("  sparse/chunk stride K : {K}");
    println!("  suffix probe          : {SUFFIX_PROBE} bytes");
    println!("  lookups keyed by series id (binary search + range-GETs)");
    println!();

    let mut hundred_k: Option<WriteCosts> = None;
    for &n in &SHAPES {
        let costs = run_shape(n);
        if n == 100_000 {
            hundred_k = Some(costs);
        }
        println!();
    }

    if let Some(c) = hundred_k {
        score_decision(&c);
    }
}

struct WriteCosts {
    a_total: u64,
    b_total: u64,
    c_total: u64,
    series_idx_bytes_b: u64,
    series_idx_bytes_c: u64,
    meta_whole: u64,
    meta_chunks: u64,
    point_c_bytes: u64,
}

fn run_shape(n: usize) -> WriteCosts {
    let config = WorkloadConfig {
        series_count: n,
        samples_per_series: 2,
        cardinality: CardinalityProfile::many_small(n),
        ..Default::default()
    };
    let raw = generate_raw(&config).expect("generate workload");
    let series_count = raw.len();

    let a = build_segment_v2(raw.clone()).bytes;
    let b = build_segment_v2_experimental(raw.clone(), ExperimentFlags::series_idx_only()).bytes;
    let c =
        build_segment_v2_experimental(raw, ExperimentFlags::series_idx_and_chunked_meta()).bytes;

    println!("-- shape: {series_count} series, many_small, 2 samples/series --");
    println!(
        "  object bytes   : (a) {}  (b) {}  (c) {}",
        a.len(),
        b.len(),
        c.len()
    );

    let fa = footer_of(&a);
    let fb = footer_of(&b);
    let fc = footer_of(&c);

    let ids = all_ids(&a, &fa);
    let point = series_count / 2;
    let multi: Vec<usize> = (0..10)
        .map(|k| (k * series_count / 10).min(series_count.saturating_sub(1)))
        .collect();

    // Patterns per format.
    let a_point = measure_status_quo(&a, &fa, &[ids[point]]);
    let b_point = measure_series_idx_only(&b, &fb, &[ids[point]]);
    let c_point = measure_chunked(&c, &fc, &[ids[point]]);

    let a_multi = measure_status_quo(&a, &fa, &collect(&ids, &multi));
    let b_multi = measure_series_idx_only(&b, &fb, &collect(&ids, &multi));
    let c_multi = measure_chunked(&c, &fc, &collect(&ids, &multi));

    let a_scan = measure_full_scan(&a);
    let b_scan = measure_full_scan(&b);
    let c_scan = measure_full_scan(&c);

    print_table_header();
    print_row("a: point lookup", "status-quo", a_point);
    print_row("", "series-idx", b_point);
    print_row("", "idx+chunked", c_point);
    print_row("b: 10-series", "status-quo", a_multi);
    print_row("", "series-idx", b_multi);
    print_row("", "idx+chunked", c_multi);
    print_row("c: full scan", "status-quo", a_scan);
    print_row("", "series-idx", b_scan);
    print_row("", "idx+chunked", c_scan);

    // Write-side costs.
    let series_idx_bytes_b = section_len(&fb, SERIES_IDX);
    let series_idx_bytes_c = section_len(&fc, SERIES_IDX);
    let meta_whole = section_len(&fa, SERIES_META);
    let meta_chunks = section_len(&fc, SERIES_META_CHUNKS);
    let a_total = a.len() as u64;
    let c_total = c.len() as u64;
    println!(
        "  write-side     : SERIES_IDX (b) {} B ({:.3}% of obj), (c) {} B",
        series_idx_bytes_b,
        pct(series_idx_bytes_b, a_total),
        series_idx_bytes_c
    );
    println!(
        "  meta stored    : whole(a) {} B  chunked(c) {} B  loss +{} B ({:+.2}% of obj)",
        meta_whole,
        meta_chunks,
        meta_chunks as i64 - meta_whole as i64,
        (meta_chunks as f64 - meta_whole as f64) / a_total as f64 * 100.0,
    );
    println!(
        "  object growth  : (b) {:+.2}%   (c) {:+.2}%",
        (b.len() as f64 - a_total as f64) / a_total as f64 * 100.0,
        (c_total as f64 - a_total as f64) / a_total as f64 * 100.0,
    );

    WriteCosts {
        a_total,
        b_total: b.len() as u64,
        c_total,
        series_idx_bytes_b,
        series_idx_bytes_c,
        meta_whole,
        meta_chunks,
        point_c_bytes: c_point.bytes,
    }
}

fn collect(ids: &[[u8; 16]], idxs: &[usize]) -> Vec<[u8; 16]> {
    idxs.iter().map(|&i| ids[i]).collect()
}

// ----------------------------------------------------------- measured paths

/// (a) status quo: whole SERIES_IDS + whole SERIES_META, then two page GETs per
/// target. The whole catalog is fetched once, no matter how many targets.
fn measure_status_quo(obj: &[u8], footer: &Footer, targets: &[[u8; 16]]) -> Meter {
    let mut m = Meter::default();
    let footer = fetch_footer(&mut m, obj, footer);
    let ids_stored = get_section(&mut m, obj, &footer, SERIES_IDS);
    let meta_stored = get_section(&mut m, obj, &footer, SERIES_META);
    let locs = decode_page_locations_v2(
        &footer,
        meta_stored,
        section_len(&footer, TS_PAGES),
        section_len(&footer, VAL_PAGES),
        ReaderLimits::default(),
    )
    .expect("decode page locations");
    let count = (ids_stored.len() - 4) / 16;
    for t in targets {
        let idx = find_index_in_window(&ids_stored[4..4 + count * 16], 0, t)
            .expect("search")
            .expect("present id");
        fetch_pages(&mut m, obj, &footer, locs[idx as usize]);
    }
    m
}

/// (b) SERIES_IDX only: sparse-index fetch, one SERIES_IDS window per target,
/// but still the whole SERIES_META (meta is not chunked in this variant).
fn measure_series_idx_only(obj: &[u8], footer: &Footer, targets: &[[u8; 16]]) -> Meter {
    let mut m = Meter::default();
    let footer = fetch_footer(&mut m, obj, footer);
    let idx = parse_series_idx(get_section(&mut m, obj, &footer, SERIES_IDX)).expect("series_idx");
    let meta_stored = get_section(&mut m, obj, &footer, SERIES_META);
    let locs = decode_page_locations_v2(
        &footer,
        meta_stored,
        section_len(&footer, TS_PAGES),
        section_len(&footer, VAL_PAGES),
        ReaderLimits::default(),
    )
    .expect("decode page locations");
    let (ids_off, _) = section_range(&footer, SERIES_IDS);
    for t in targets {
        let window = idx.locate(t).expect("window");
        let win = m.range(obj, ids_off + window.section_offset, window.len);
        let abs = find_index_in_window(win, window.first_index, t)
            .expect("search")
            .expect("present id");
        fetch_pages(&mut m, obj, &footer, locs[abs as usize]);
    }
    m
}

/// (c) SERIES_IDX + chunked meta: sparse-index fetch, then per target one
/// SERIES_IDS window, one meta chunk frame, and two page GETs.
fn measure_chunked(obj: &[u8], footer: &Footer, targets: &[[u8; 16]]) -> Meter {
    let mut m = Meter::default();
    let footer = fetch_footer(&mut m, obj, footer);
    let idx = parse_series_idx(get_section(&mut m, obj, &footer, SERIES_IDX)).expect("series_idx");
    let (ids_off, _) = section_range(&footer, SERIES_IDS);
    let (chunks_off, _) = section_range(&footer, SERIES_META_CHUNKS);
    for t in targets {
        let window = idx.locate(t).expect("window");
        let win = m.range(obj, ids_off + window.section_offset, window.len);
        let abs = find_index_in_window(win, window.first_index, t)
            .expect("search")
            .expect("present id");
        let cl = idx.chunk_for(abs).expect("chunk");
        let frame_stored = m.range(obj, chunks_off + cl.frame_offset, cl.frame_stored_len);
        let frame = decompress_chunk_frame(
            frame_stored,
            cl.frame_uncompressed_len,
            ReaderLimits::default(),
        )
        .expect("frame");
        let loc = decode_chunk_page_location(&frame, cl.row_in_chunk, footer.min_event_ts_ns)
            .expect("chunk row");
        fetch_pages(&mut m, obj, &footer, loc);
    }
    m
}

/// Full scan: one whole-object GET (identical operation for every format; the
/// only difference is object size).
fn measure_full_scan(obj: &[u8]) -> Meter {
    let mut m = Meter::default();
    let _ = m.full(obj);
    m
}

fn fetch_pages(m: &mut Meter, obj: &[u8], footer: &Footer, loc: PageLoc) {
    let (ts_off, _) = section_range(footer, TS_PAGES);
    let (val_off, _) = section_range(footer, VAL_PAGES);
    let _ = m.range(obj, ts_off + loc.ts_page.0, loc.ts_page.1);
    let _ = m.range(obj, val_off + loc.val_page.0, loc.val_page.1);
}

fn fetch_footer(m: &mut Meter, obj: &[u8], _hint: &Footer) -> Footer {
    let suffix = m.suffix(obj, SUFFIX_PROBE);
    match ravel_segment::experiment_api::parse_footer_from_suffix_unvalidated(
        suffix,
        obj.len() as u64,
    )
    .expect("parse footer")
    {
        FooterOutcome::Ready(loc) => loc.footer,
        FooterOutcome::NeedRange { offset, len } => {
            let more = m.range(obj, offset, len);
            match ravel_segment::experiment_api::parse_footer_from_suffix_unvalidated(
                more,
                obj.len() as u64,
            )
            .expect("parse footer 2")
            {
                FooterOutcome::Ready(loc) => loc.footer,
                FooterOutcome::NeedRange { .. } => panic!("footer not covered"),
            }
        }
    }
}

fn get_section<'a>(m: &mut Meter, obj: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
    let (off, len) = section_range(footer, kind);
    m.range(obj, off, len)
}

// --------------------------------------------------------------- utilities

fn footer_of(obj: &[u8]) -> Footer {
    ravel_segment::experiment_api::parse_footer_unvalidated(obj).expect("parse footer")
}

fn section_range(footer: &Footer, kind: u32) -> (u64, u64) {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .unwrap_or_else(|| panic!("section kind {kind}"));
    (s.offset, s.len)
}

fn section_len(footer: &Footer, kind: u32) -> u64 {
    footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .map(|s| s.len)
        .unwrap_or(0)
}

fn all_ids(obj: &[u8], footer: &Footer) -> Vec<[u8; 16]> {
    let (off, len) = section_range(footer, SERIES_IDS);
    let bytes = &obj[off as usize..(off + len) as usize];
    let count = (bytes.len() - 4) / 16;
    (0..count)
        .map(|i| {
            let s = 4 + i * 16;
            bytes[s..s + 16].try_into().expect("16 bytes")
        })
        .collect()
}

fn pct(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64 * 100.0
    }
}

fn print_table_header() {
    println!(
        "  {:<16} {:<12} {:>6} {:>12}",
        "pattern", "format", "GETs", "bytes"
    );
}

fn print_row(pattern: &str, format: &str, m: Meter) {
    println!(
        "  {:<16} {:<12} {:>6} {:>12}",
        pattern, format, m.gets, m.bytes
    );
}

fn score_decision(c: &WriteCosts) {
    let point_ok = c.point_c_bytes < POINT_LOOKUP_TARGET_BYTES;
    let growth = (c.c_total as f64 - c.a_total as f64) / c.a_total as f64;
    let growth_ok = growth < MAX_OBJECT_GROWTH;
    let _ = (
        c.b_total,
        c.series_idx_bytes_b,
        c.series_idx_bytes_c,
        c.meta_whole,
        c.meta_chunks,
    );
    println!("== decision rule (issue #167), at 100k series ==");
    println!(
        "  point-lookup bytes (c): {} B  < {} B ? {}",
        c.point_c_bytes,
        POINT_LOOKUP_TARGET_BYTES,
        verdict(point_ok)
    );
    println!(
        "  object growth (c):      {:+.2}%  < {:.0}% ? {}",
        growth * 100.0,
        MAX_OBJECT_GROWTH * 100.0,
        verdict(growth_ok)
    );
    println!(
        "  => decision rule: {}",
        if point_ok && growth_ok {
            "MET"
        } else {
            "NOT MET"
        }
    );
}

fn verdict(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chunked point lookup fetches far fewer bytes than status quo (the
    /// metered path runs end to end, so its `decode_*` calls also spot-check
    /// correctness). At 10k series the whole SERIES_META already dwarfs the
    /// fixed 64 KiB suffix probe, so the gap is clear; below a few thousand
    /// series the suffix probe dominates and the index cannot help, which is
    /// itself a reported finding, not a regression.
    #[test]
    fn chunked_point_lookup_is_much_smaller_than_status_quo() {
        let n = 10_000;
        let config = WorkloadConfig {
            series_count: n,
            samples_per_series: 2,
            cardinality: CardinalityProfile::many_small(n),
            ..Default::default()
        };
        let raw = generate_raw(&config).expect("generate");
        let a = build_segment_v2(raw.clone()).bytes;
        let c = build_segment_v2_experimental(raw, ExperimentFlags::series_idx_and_chunked_meta())
            .bytes;
        let fa = footer_of(&a);
        let fc = footer_of(&c);
        let ids = all_ids(&a, &fa);
        let target = ids[n / 2];

        let a_point = measure_status_quo(&a, &fa, &[target]);
        let c_point = measure_chunked(&c, &fc, &[target]);
        assert!(
            c_point.bytes * 3 < a_point.bytes,
            "chunked point lookup ({} B) should be well under status quo ({} B)",
            c_point.bytes,
            a_point.bytes
        );
    }
}
