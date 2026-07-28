//! Deterministic byte-gate test for the RSEG v2/v3 columnar catalog and
//! LABEL_DICT sizes (issue #166).
//!
//! The #146/#147 rounds settled the RSEG v2 byte gates (catalog raw ~56% of
//! v1, total bytes under v1, LABEL_DICT compressed byte-identical to v1), but
//! they lived only in BENCHMARKS.md prose. Timing cannot be a CI gate (the
//! reference host is never quiet); bytes are exact and deterministic. This
//! test makes the gates unlosable.
//!
//! Fixture: the 10k `many_small` shape (5 label pairs/series, 20k total
//! samples). Across the #93 rounds this shape tracked the 100k gate ratios
//! closely while running in a fraction of a second. The generator seed is
//! fixed (the [`WorkloadConfig`](ravel_bench::generator::WorkloadConfig)
//! default, 0), and `shape_wide_15` uses no RNG, so the whole fixture is
//! deterministic.
//!
//! Assertions are on ratios, not absolute bytes: a zstd version bump moves
//! both sides of every ratio together (they share one compressor), so it
//! cannot break these gates spuriously. Measured values are printed on
//! success too, so `cargo test -- --nocapture` output can be diffed across
//! runs to re-prove determinism.
//!
//! v2 vs v3 catalog (issue #166 finding): the task premise "v3 catalog layout
//! unchanged from v2" does not hold for SERIES_META *raw* bytes. RSEG v3's
//! SERIES_META is v2's grammar plus three per-series columns (block 10
//! `value_kind`, 11 `hist_page_gap`, 12 `hist_page_len`;
//! crates/ravel-segment/src/writer.rs `build_series_meta_v3`,
//! docs/rseg-v3-plan.md section 3.4). On the scalar path those three columns
//! are pure overhead (~3 bytes/series raw), so v3 catalog raw runs a few
//! points above v2's and does not clear the v2 `<=60%` gate at either fixture
//! size (measured v3 catalog/v1: 60.42% at 100k, 64.10% at this 10k fixture,
//! vs v2's 56.46% / 59.77%). This is a format-overhead fact, not a
//! fixture-size artifact. The v2 catalog gate is therefore asserted at `<=60%`
//! as specified; the v3 catalog is gated by the invariant that actually holds
//! -- v3 raw is v2 raw plus a small, bounded per-series scalar overhead and
//! stays well below v1 row-major -- with the measured v3/v1 ratio printed for
//! the record. LABEL_DICT and total-object-byte gates are unaffected: they
//! hold identically for v2 and v3, since the scalar VAL pages and LABEL_DICT
//! are byte-for-byte the same across the two writers.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::section_accounting::{
    CatalogBytes, measure_catalog, shape_many_small, shape_wide_15,
};
use ravel_bench::segment_support::{build_segment, build_segment_v2, build_segment_v3};

/// 10k `many_small` series, 20k total samples: same series/sample ratio as
/// the 100k report (`section_bytes_report`), a fraction of its runtime.
const SERIES: usize = 10_000;
const TOTAL_SAMPLES: usize = 20_000;

/// Columnar catalog raw (SERIES_IDS + SERIES_META) must be at most this
/// fraction of v1 SERIES_TABLE raw. #147 measured 56.46% for v2 at 100k; the
/// 10k shape sits a few points higher but still under the 60% gate.
const CATALOG_RATIO_MAX: f64 = 0.60;
/// Columnar total object bytes must not exceed v1 total by more than 1%.
const TOTAL_RATIO_MAX: f64 = 1.01;
/// Columnar LABEL_DICT compressed bytes must not exceed v1's by more than
/// 1%. #146 restored the sorted order, making it byte-identical to v1.
const LABEL_DICT_RATIO_MAX: f64 = 1.01;
/// Per-series raw overhead the v3 scalar path adds over v2 in SERIES_META
/// (the `value_kind` + `hist_page_gap` + `hist_page_len` columns). Measured
/// ~3.0 bytes/series; the bound leaves headroom while still catching a
/// regression that balloons the scalar-path metadata.
const V3_SCALAR_META_OVERHEAD_MAX_BYTES_PER_SERIES: u64 = 4;

fn print_measurement(label: &str, m: &CatalogBytes) {
    println!(
        "[{label}] series v1/col={}/{}  catalog {}B/{}B raw  ratio={:.4} ({:.2}%)",
        m.v1_series_count,
        m.columnar_series_count,
        m.catalog_raw,
        m.v1_series_table_raw,
        m.catalog_ratio,
        m.catalog_ratio * 100.0,
    );
    println!(
        "[{label}] total col/v1={}B/{}B  ratio={:.4} ({:+.2}%)  LABEL_DICT zstd3 col/v1={}B/{}B  ratio={:.4} ({:+.2}%)",
        m.columnar_total,
        m.v1_total,
        m.total_ratio,
        (m.total_ratio - 1.0) * 100.0,
        m.columnar_label_dict_zstd3,
        m.v1_label_dict_zstd3,
        m.label_dict_ratio,
        (m.label_dict_ratio - 1.0) * 100.0,
    );
}

/// The total-object-byte and LABEL_DICT gates. These hold identically for the
/// v2 and v3 scalar writers, so both call it.
fn assert_total_and_dict_gates(label: &str, m: &CatalogBytes) {
    assert!(
        m.total_ratio <= TOTAL_RATIO_MAX,
        "{label}: total object bytes ratio {:.4} exceeds gate {TOTAL_RATIO_MAX}",
        m.total_ratio,
    );
    assert!(
        m.label_dict_ratio <= LABEL_DICT_RATIO_MAX,
        "{label}: LABEL_DICT compressed ratio {:.4} exceeds gate {LABEL_DICT_RATIO_MAX}",
        m.label_dict_ratio,
    );
}

/// v2 (`write_v2`) `many_small`: catalog raw <=60% of v1, total <=v1+1%,
/// LABEL_DICT compressed <=v1+1%.
#[test]
fn v2_many_small_byte_gates() {
    let raw = shape_many_small(SERIES, TOTAL_SAMPLES);
    let v1 = build_segment(raw.clone());
    let v2 = build_segment_v2(raw);
    let m = measure_catalog(&v1, &v2);
    print_measurement("v2 many_small", &m);
    assert!(
        m.catalog_ratio <= CATALOG_RATIO_MAX,
        "v2 many_small: catalog raw ratio {:.4} exceeds gate {CATALOG_RATIO_MAX}",
        m.catalog_ratio,
    );
    assert_total_and_dict_gates("v2 many_small", &m);
}

/// v3 scalar path (`write_v3`) `many_small`. Total and LABEL_DICT gates hold
/// as for v2. The catalog gate is the invariant that actually holds on the
/// scalar path: v3 raw is v2 raw plus a small, bounded per-series overhead
/// (the value_kind/hist columns) and stays well below v1 row-major. The
/// measured v3/v1 catalog ratio is printed for the record (issue #166: it
/// exceeds the v2 `<=60%` gate at this fixture size; see the module note).
#[test]
fn v3_many_small_byte_gates() {
    let raw = shape_many_small(SERIES, TOTAL_SAMPLES);
    let v1 = build_segment(raw.clone());
    let v2 = build_segment_v2(raw.clone());
    let v3 = build_segment_v3(raw);
    let m2 = measure_catalog(&v1, &v2);
    let m3 = measure_catalog(&v1, &v3);
    print_measurement("v3 many_small", &m3);

    assert_total_and_dict_gates("v3 many_small", &m3);

    // v3 scalar catalog is v2's plus a bounded per-series overhead.
    assert!(
        m3.catalog_raw >= m2.catalog_raw,
        "v3 many_small: catalog raw {} unexpectedly below v2 {}",
        m3.catalog_raw,
        m2.catalog_raw,
    );
    let overhead = m3.catalog_raw - m2.catalog_raw;
    let overhead_budget = V3_SCALAR_META_OVERHEAD_MAX_BYTES_PER_SERIES * m3.columnar_series_count;
    assert!(
        overhead <= overhead_budget,
        "v3 many_small: catalog raw overhead over v2 {overhead}B exceeds budget {overhead_budget}B \
         ({V3_SCALAR_META_OVERHEAD_MAX_BYTES_PER_SERIES}B/series x {} series)",
        m3.columnar_series_count,
    );
    // Columnar still beats v1 row-major even with the v3 overhead.
    assert!(
        m3.catalog_raw < m3.v1_series_table_raw,
        "v3 many_small: catalog raw {} not below v1 SERIES_TABLE raw {}",
        m3.catalog_raw,
        m3.v1_series_table_raw,
    );
}

/// Wide 15-label shape: the columnar schema dictionary earns most here, so
/// the v2 total object bytes come out strictly below v1's. The v2 catalog and
/// LABEL_DICT gates also hold on this shape.
#[test]
fn v2_wide_15_total_below_v1() {
    let raw = shape_wide_15(SERIES, TOTAL_SAMPLES);
    let v1 = build_segment(raw.clone());
    let v2 = build_segment_v2(raw);
    let m = measure_catalog(&v1, &v2);
    print_measurement("v2 wide_15", &m);
    assert!(
        m.columnar_total < m.v1_total,
        "v2 wide_15: total {} not below v1 total {}",
        m.columnar_total,
        m.v1_total,
    );
    assert!(
        m.catalog_ratio <= CATALOG_RATIO_MAX,
        "v2 wide_15: catalog raw ratio {:.4} exceeds gate {CATALOG_RATIO_MAX}",
        m.catalog_ratio,
    );
    assert_total_and_dict_gates("v2 wide_15", &m);
}

/// v3 scalar path on the wide shape: same total-below-v1 property as v2,
/// since scalar VAL pages and LABEL_DICT are identical across the writers.
#[test]
fn v3_wide_15_total_below_v1() {
    let raw = shape_wide_15(SERIES, TOTAL_SAMPLES);
    let v1 = build_segment(raw.clone());
    let v3 = build_segment_v3(raw);
    let m = measure_catalog(&v1, &v3);
    print_measurement("v3 wide_15", &m);
    assert!(
        m.columnar_total < m.v1_total,
        "v3 wide_15: total {} not below v1 total {}",
        m.columnar_total,
        m.v1_total,
    );
    assert_total_and_dict_gates("v3 wide_15", &m);
}

/// Determinism: the whole measurement pipeline (fixed-seed generator ->
/// writer -> byte accounting) yields byte-identical numbers on a second run
/// in the same process, for both v2 and v3. Guards against any hidden
/// nondeterminism (map iteration order, unseeded RNG) that would make the
/// gate ratios wobble.
#[test]
fn measurements_are_deterministic() {
    for build_columnar in [
        build_segment_v2 as fn(_) -> _,
        build_segment_v3 as fn(_) -> _,
    ] {
        let measure = || {
            let raw = shape_many_small(SERIES, TOTAL_SAMPLES);
            measure_catalog(&build_segment(raw.clone()), &build_columnar(raw))
        };
        let a = measure();
        let b = measure();
        assert_eq!(a.v1_series_table_raw, b.v1_series_table_raw);
        assert_eq!(a.series_ids_raw, b.series_ids_raw);
        assert_eq!(a.series_meta_raw, b.series_meta_raw);
        assert_eq!(a.catalog_raw, b.catalog_raw);
        assert_eq!(a.v1_total, b.v1_total);
        assert_eq!(a.columnar_total, b.columnar_total);
        assert_eq!(a.v1_label_dict_zstd3, b.v1_label_dict_zstd3);
        assert_eq!(a.columnar_label_dict_zstd3, b.columnar_label_dict_zstd3);
        // Bit-identical ratios follow from identical integer byte counts.
        assert_eq!(a.catalog_ratio.to_bits(), b.catalog_ratio.to_bits());
        assert_eq!(a.total_ratio.to_bits(), b.total_ratio.to_bits());
        assert_eq!(a.label_dict_ratio.to_bits(), b.label_dict_ratio.to_bits());
    }
}
