//! Stored-bytes report for the RSEG v2 columnar catalog gates
//! (docs/rseg-v2-plan.md sec 1.2/sec 8, issue #93).
//!
//! Builds v1 and v2 segments from the *same* logical workload and prints,
//! per workload shape:
//!
//! - v1 `SERIES_TABLE` raw (uncompressed) bytes;
//! - v2 `SERIES_IDS` raw bytes and `SERIES_META` raw bytes;
//! - the v2/v1 catalog ratio against the `<= 60%` stored-bytes gate;
//! - total object bytes for both against the `<= v1 + 1%` gate;
//! - `LABEL_DICT` compressed bytes (zstd -3, matching the writer's
//!   `ZSTD_LEVEL`); both versions emit sorted order since #146, so the
//!   ratio (plan section 3.3 fallback trigger at > 10%) sits at parity.
//!
//! Two shapes are reported: the shared generator's `many_small` shape
//! (5 label pairs/series, identical to the `segment_encode` bench), and a
//! 15-labels-per-series shape. Both shapes and the byte accounting live in
//! `ravel_bench::section_accounting`, shared with the `catalog_byte_gates`
//! test (issue #166) so neither copy of the logic can drift.
//!
//! Run: `cargo run -p ravel-bench --release --bin section_bytes_report`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::section_accounting::{
    CatalogBytes, Raw, footer_of, measure_catalog, raw_len, shape_many_small, shape_wide_15,
};
use ravel_bench::segment_support::{LABEL_DICT, build_segment, build_segment_v2, section_bytes};

const SERIES: usize = 100_000;
const TOTAL_SAMPLES: usize = 200_000;

fn report(shape: &str, raw: Raw) {
    let v1 = build_segment(raw.clone());
    let v2 = build_segment_v2(raw);
    let m: CatalogBytes = measure_catalog(&v1, &v2);

    let f1 = footer_of(&v1);
    let f2 = footer_of(&v2);

    println!("== {shape} ({SERIES} series, {TOTAL_SAMPLES} samples) ==");
    println!(
        "  series in segment (v1/v2):       {} / {}",
        m.v1_series_count, m.columnar_series_count
    );
    println!();
    println!(
        "  v1 SERIES_TABLE raw bytes:       {}",
        m.v1_series_table_raw
    );
    println!("  v2 SERIES_IDS raw bytes:         {}", m.series_ids_raw);
    println!("  v2 SERIES_META raw bytes:        {}", m.series_meta_raw);
    println!("  v2 catalog raw (IDS+META):       {}", m.catalog_raw);
    println!(
        "  catalog ratio v2/v1:             {:.4} ({:.2}%)  gate <=60%: {}",
        m.catalog_ratio,
        m.catalog_ratio * 100.0,
        verdict(m.catalog_ratio <= 0.60),
    );
    println!();
    println!("  v1 total object bytes:           {}", m.v1_total);
    println!("  v2 total object bytes:           {}", m.columnar_total);
    println!(
        "  total ratio v2/v1:               {:.4} ({:+.2}%)  gate <=v1+1%: {}",
        m.total_ratio,
        (m.total_ratio - 1.0) * 100.0,
        verdict(m.total_ratio <= 1.01),
    );
    println!();
    println!(
        "  LABEL_DICT raw bytes (v1/v2):    {} / {}",
        raw_len(&f1, LABEL_DICT),
        raw_len(&f2, LABEL_DICT)
    );
    println!(
        "  LABEL_DICT zstd -3 v1 (sorted):  {}",
        m.v1_label_dict_zstd3
    );
    println!(
        "  LABEL_DICT zstd -3 v2 (sorted):  {}",
        m.columnar_label_dict_zstd3
    );
    println!(
        "  LABEL_DICT ratio v2/v1:          {:.4} ({:+.2}%)  <=+1%: {}",
        m.label_dict_ratio,
        (m.label_dict_ratio - 1.0) * 100.0,
        verdict(m.label_dict_ratio <= 1.01),
    );
    println!(
        "  writer-stored LABEL_DICT (v1/v2):{} / {}  (equals zstd -3 above: writer policy)",
        section_bytes(&v1.bytes, &f1, LABEL_DICT).len(),
        section_bytes(&v2.bytes, &f2, LABEL_DICT).len(),
    );
    println!();
}

fn verdict(met: bool) -> &'static str {
    if met { "MET" } else { "NOT MET" }
}

fn main() {
    println!("# RSEG v2 stored-bytes report (issue #93, docs/rseg-v2-plan.md sec 1.2/sec 8)");
    println!(
        "# gates: catalog raw <=60% of v1 SERIES_TABLE; object bytes <=v1+1%; LABEL_DICT delta recorded"
    );
    println!();
    report(
        "many_small (5 label pairs/series)",
        shape_many_small(SERIES, TOTAL_SAMPLES),
    );
    report(
        "wide (15 labels/series, inline)",
        shape_wide_15(SERIES, TOTAL_SAMPLES),
    );
}
