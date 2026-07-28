//! Within-segment selective-read GET accounting for RSEG v5 (ADR-0026,
//! issue #176; supersedes the #167 prototype bin).
//!
//! Measures what a point lookup, a 10-series multi-lookup, and a full scan
//! each cost -- in GET requests and bytes transferred -- for two RSEG layouts
//! of the *same* logical workload:
//!
//!   legacy: v4 whole-catalog read (`SegmentWriter::write_v4`); a selective
//!           read fetches the whole SERIES_IDS + SERIES_META catalog.
//!   sparse: v5 (`SegmentWriter::write_v5`); at or above the 4096-series
//!           threshold a selective read fetches SERIES_IDX, one crc-verified
//!           SERIES_IDS window, and one crc-verified meta chunk. Below the
//!           threshold v5 carries the v4-shaped whole catalog and the sparse
//!           path falls back to the whole-catalog read.
//!
//! at 500 / 10k / 100k series (`many_small`, 2 samples/series), the shapes the
//! #97/#167 read-path accounting used. GET count and bytes are
//! backend-independent, so -- as the #97 harness argues -- they are the
//! deliverable regardless of backend; this bin meters an in-memory object
//! directly, one GET and its returned bytes per metered fetch.
//!
//! Report-only: never changes library behavior. Runs under the default
//! `cargo test -p ravel-bench` via its correctness test.
//!
//! Run: `cargo run -p ravel-bench --release --bin selective_read_accounting`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{
    SERIES_IDS, SERIES_IDX, SERIES_META, SERIES_META_CHUNKS, TS_PAGES, VAL_PAGES, build_segment_v4,
    build_segment_v5,
};
use ravel_segment::{
    Footer, FooterOutcome, ReaderLimits, RunEntry, V5_STRIDE, decode_catalog_v4, decode_chunk_runs,
    find_index_in_window, parse_footer, parse_series_idx, verify_and_decompress_chunk_frame,
    verify_id_window,
};

/// RSEG reader-protocol suffix probe (docs/segment-format.md step 2), same
/// 64 KiB the #97 harness used.
const SUFFIX_PROBE: u64 = 64 * 1024;
/// Series counts (many_small, 2 samples/series).
const SHAPES: [usize; 3] = [500, 10_000, 100_000];
/// Decision rule (ADR-0026), at 100k: point-lookup bytes drop into the
/// low-100KB range, and the sparse object grows by under 1%.
const POINT_LOOKUP_TARGET_BYTES: u64 = 200 * 1024;
const MAX_OBJECT_GROWTH: f64 = 0.01;

/// In-memory GET meter: one GET, plus the bytes it returned.
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
    println!("== within-segment selective-read GET accounting (RSEG v5, ADR-0026) ==");
    println!("  sparse/chunk stride K : {V5_STRIDE}");
    println!("  suffix probe          : {SUFFIX_PROBE} bytes");
    println!("  lookups keyed by series id (binary search + crc-verified range-GETs)");
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
    legacy_total: u64,
    sparse_total: u64,
    series_idx_bytes: u64,
    meta_whole: u64,
    meta_chunks: u64,
    point_sparse_bytes: u64,
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

    let legacy = build_segment_v4(raw.clone()).bytes;
    let sparse = build_segment_v5(raw).bytes;

    let fl = footer_of(&legacy);
    let fs = footer_of(&sparse);
    let has_sparse = fs.sections.iter().any(|s| s.kind == SERIES_IDX);

    println!("-- shape: {series_count} series, many_small, 2 samples/series --");
    println!(
        "  object bytes   : legacy(v4) {}  sparse(v5) {}{}",
        legacy.len(),
        sparse.len(),
        if has_sparse {
            ""
        } else {
            "  (below threshold: no sparse sections)"
        }
    );

    let ids = all_ids(&legacy, &fl);
    let point = series_count / 2;
    let multi: Vec<usize> = (0..10)
        .map(|k| (k * series_count / 10).min(series_count.saturating_sub(1)))
        .collect();

    let l_point = measure_legacy(&legacy, &fl, &[ids[point]]);
    let s_point = measure_sparse(&sparse, &fs, has_sparse, &[ids[point]]);
    let l_multi = measure_legacy(&legacy, &fl, &collect(&ids, &multi));
    let s_multi = measure_sparse(&sparse, &fs, has_sparse, &collect(&ids, &multi));
    let l_scan = measure_full_scan(&legacy);
    let s_scan = measure_full_scan(&sparse);

    print_table_header();
    print_row("point lookup", "legacy(v4)", l_point);
    print_row("", "sparse(v5)", s_point);
    print_row("10-series", "legacy(v4)", l_multi);
    print_row("", "sparse(v5)", s_multi);
    print_row("full scan", "legacy(v4)", l_scan);
    print_row("", "sparse(v5)", s_scan);

    let series_idx_bytes = section_len(&fs, SERIES_IDX);
    let meta_whole = section_len(&fl, SERIES_META);
    let meta_chunks = section_len(&fs, SERIES_META_CHUNKS);
    let legacy_total = legacy.len() as u64;
    let sparse_total = sparse.len() as u64;
    println!(
        "  write-side     : SERIES_IDX {} B ({:.3}% of obj)",
        series_idx_bytes,
        pct(series_idx_bytes, legacy_total),
    );
    println!(
        "  meta stored    : whole {} B  chunked {} B  loss {:+} B ({:+.3}% of obj)",
        meta_whole,
        meta_chunks,
        meta_chunks as i64 - meta_whole as i64,
        (meta_chunks as f64 - meta_whole as f64) / legacy_total as f64 * 100.0,
    );
    println!(
        "  object growth  : {:+.3}%  (sparse over legacy)",
        (sparse_total as f64 - legacy_total as f64) / legacy_total as f64 * 100.0,
    );

    WriteCosts {
        legacy_total,
        sparse_total,
        series_idx_bytes,
        meta_whole,
        meta_chunks,
        point_sparse_bytes: s_point.bytes,
    }
}

fn collect(ids: &[[u8; 16]], idxs: &[usize]) -> Vec<[u8; 16]> {
    idxs.iter().map(|&i| ids[i]).collect()
}

// ----------------------------------------------------------- measured paths

/// Legacy v4: whole LABEL_DICT + SERIES_IDS + SERIES_META fetched once, then
/// two page GETs per target. The catalog fetch dominates and does not shrink
/// with the number of targets.
fn measure_legacy(obj: &[u8], footer: &Footer, targets: &[[u8; 16]]) -> Meter {
    let mut m = Meter::default();
    let footer = fetch_footer(&mut m, obj, footer);
    let dict = get_section(&mut m, obj, &footer, 1);
    let ids = get_section(&mut m, obj, &footer, SERIES_IDS);
    let meta = get_section(&mut m, obj, &footer, SERIES_META);
    let entries = decode_catalog_v4(&footer, dict, ids, meta, ReaderLimits::default())
        .expect("decode v4 catalog");
    for t in targets {
        let entry = entries
            .iter()
            .find(|e| &e.entry.series_id.0 == t)
            .expect("present id");
        fetch_run_pages(&mut m, obj, &footer, &entry.runs[0]);
    }
    m
}

/// Sparse v5: SERIES_IDX fetched once, then per target one crc-verified
/// SERIES_IDS window, one crc-verified meta chunk frame, and two page GETs.
/// Below the threshold (no SERIES_IDX) this falls back to the legacy path.
fn measure_sparse(obj: &[u8], footer: &Footer, has_sparse: bool, targets: &[[u8; 16]]) -> Meter {
    if !has_sparse {
        return measure_legacy(obj, footer, targets);
    }
    let mut m = Meter::default();
    let footer = fetch_footer(&mut m, obj, footer);
    let idx = parse_series_idx(get_section(&mut m, obj, &footer, SERIES_IDX)).expect("series_idx");
    let (ids_off, _) = section_range(&footer, SERIES_IDS);
    let (chunks_off, _) = section_range(&footer, SERIES_META_CHUNKS);
    for t in targets {
        let window = idx.locate(t).expect("window");
        let win = m.range(obj, ids_off + window.section_offset, window.len);
        verify_id_window(win, &window).expect("window crc");
        let abs = find_index_in_window(win, window.first_index, t)
            .expect("search")
            .expect("present id");
        let cl = idx.chunk_for(abs).expect("chunk");
        let stored = m.range(obj, chunks_off + cl.frame_offset, cl.frame_stored_len);
        let frame =
            verify_and_decompress_chunk_frame(stored, &cl, ReaderLimits::default()).expect("frame");
        let runs = decode_chunk_runs(&frame, cl.row_in_chunk, &footer).expect("chunk runs");
        fetch_run_pages(&mut m, obj, &footer, &runs[0]);
    }
    m
}

/// Full scan: one whole-object GET (identical operation for both layouts; only
/// the object size differs).
fn measure_full_scan(obj: &[u8]) -> Meter {
    let mut m = Meter::default();
    let _ = m.full(obj);
    m
}

fn fetch_run_pages(m: &mut Meter, obj: &[u8], footer: &Footer, run: &RunEntry) {
    let (ts_off, _) = section_range(footer, TS_PAGES);
    let (val_off, _) = section_range(footer, VAL_PAGES);
    let _ = m.range(obj, ts_off + run.ts_page.0, run.ts_page.1);
    let _ = m.range(obj, val_off + run.val_page.0, run.val_page.1);
}

fn fetch_footer(m: &mut Meter, obj: &[u8], _hint: &Footer) -> Footer {
    let suffix = m.suffix(obj, SUFFIX_PROBE);
    match parse_footer(obj.len() as u64, suffix).expect("parse footer") {
        FooterOutcome::Ready(loc) => loc.footer,
        FooterOutcome::NeedRange { offset, len } => {
            let more = m.range(obj, offset, len);
            match parse_footer(obj.len() as u64, more).expect("parse footer 2") {
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
    match parse_footer(obj.len() as u64, obj).expect("parse footer") {
        FooterOutcome::Ready(loc) => loc.footer,
        FooterOutcome::NeedRange { .. } => panic!("whole object covers footer"),
    }
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
        "  {:<14} {:<12} {:>6} {:>12}",
        "pattern", "layout", "GETs", "bytes"
    );
}

fn print_row(pattern: &str, layout: &str, m: Meter) {
    println!(
        "  {:<14} {:<12} {:>6} {:>12}",
        pattern, layout, m.gets, m.bytes
    );
}

fn score_decision(c: &WriteCosts) {
    let point_ok = c.point_sparse_bytes < POINT_LOOKUP_TARGET_BYTES;
    let growth = (c.sparse_total as f64 - c.legacy_total as f64) / c.legacy_total as f64;
    let growth_ok = growth < MAX_OBJECT_GROWTH;
    let _ = (c.series_idx_bytes, c.meta_whole, c.meta_chunks);
    println!("== decision rule (ADR-0026), at 100k series ==");
    println!(
        "  point-lookup bytes (sparse): {} B  < {} B ? {}",
        c.point_sparse_bytes,
        POINT_LOOKUP_TARGET_BYTES,
        verdict(point_ok)
    );
    println!(
        "  object growth (sparse):      {:+.3}%  < {:.0}% ? {}",
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

    /// The sparse point lookup fetches far fewer bytes than the legacy
    /// whole-catalog read (and the metered path runs end to end, so its
    /// `decode_*`/crc calls also spot-check correctness). At 10k series the
    /// whole SERIES_META already dwarfs the fixed 64 KiB suffix probe, so the
    /// gap is clear.
    #[test]
    fn sparse_point_lookup_is_much_smaller_than_legacy() {
        let n = 10_000;
        let config = WorkloadConfig {
            series_count: n,
            samples_per_series: 2,
            cardinality: CardinalityProfile::many_small(n),
            ..Default::default()
        };
        let raw = generate_raw(&config).expect("generate");
        let legacy = build_segment_v4(raw.clone()).bytes;
        let sparse = build_segment_v5(raw).bytes;
        let fl = footer_of(&legacy);
        let fs = footer_of(&sparse);
        assert!(
            fs.sections.iter().any(|s| s.kind == SERIES_IDX),
            "10k object must carry sparse sections"
        );
        let ids = all_ids(&legacy, &fl);
        let target = ids[n / 2];
        let l = measure_legacy(&legacy, &fl, &[target]);
        let s = measure_sparse(&sparse, &fs, true, &[target]);
        assert!(
            s.bytes * 3 < l.bytes,
            "sparse point lookup ({} B) should be well under legacy ({} B)",
            s.bytes,
            l.bytes,
        );
    }
}
