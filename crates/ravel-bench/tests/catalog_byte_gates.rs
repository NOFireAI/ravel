//! Deterministic byte-gate test for the RSEG v5 sparse catalog (issue #166,
//! #176; re-anchored by ADR-0027).
//!
//! ADR-0027 retired the v1-v4 writers, so the old v1-relative catalog/total/
//! LABEL_DICT ratio gates (which needed a v1 baseline object to compare
//! against) are gone. What remains is the gate expressed entirely within a
//! single v5 object: the sparse SERIES_IDX overhead as a fraction of the
//! object. Timing cannot be a CI gate (the reference host is never quiet);
//! bytes are exact and deterministic. This makes the gate unlosable.
//!
//! Fixture: the 10k `many_small` shape (5 label pairs/series, 20k total
//! samples), at or above the 4096-series sparse-emission threshold. The
//! generator seed is fixed (the [`WorkloadConfig`] default, 0), so the whole
//! fixture is deterministic. Measured values are printed on success too, so
//! `cargo test -- --nocapture` output can be diffed across runs.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::section_accounting::shape_many_small;
use ravel_bench::segment_support::{SERIES_IDX, SERIES_META, SERIES_META_CHUNKS, build_segment_v5};
use ravel_segment::{ReaderLimits, open_from_full};

/// 10k `many_small` series, 20k total samples: >= the 4096 sparse-emission
/// threshold, a fraction of the 100k report's runtime.
const SERIES: usize = 10_000;
const TOTAL_SAMPLES: usize = 20_000;

/// The v5 sparse SERIES_IDX must cost under this fraction of the object at the
/// 10k shape (ADR-0026: write-side total +0.48% at 100k; the 10k shape sits a
/// little higher but well under 1%).
const V5_SPARSE_OVERHEAD_RATIO_MAX: f64 = 0.01;

fn section_len(bytes: &[u8], kind: u32) -> u64 {
    let loc = open_from_full(bytes, ReaderLimits::default()).expect("open");
    loc.footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .map(|s| s.len)
        .unwrap_or(0)
}

/// v5 sparse byte gate: at the 10k `many_small` shape the sparse SERIES_IDX
/// costs under 1% of object bytes, expressed as a ratio within the same v5
/// object (no removed-version baseline). The measured numbers are printed for
/// the record.
#[test]
fn v5_sparse_sections_under_one_percent_of_object() {
    let raw = shape_many_small(SERIES, TOTAL_SAMPLES);
    let v5 = build_segment_v5(raw);
    let v5_total = v5.bytes.len() as u64;

    let series_idx = section_len(&v5.bytes, SERIES_IDX);
    let meta_chunks = section_len(&v5.bytes, SERIES_META_CHUNKS);
    assert!(
        series_idx > 0,
        "SERIES_IDX must be present at the 10k shape"
    );
    assert!(
        meta_chunks > 0,
        "SERIES_META_CHUNKS must be present at the 10k shape"
    );
    assert_eq!(
        section_len(&v5.bytes, SERIES_META),
        0,
        "v5 sparse object drops the whole SERIES_META"
    );

    let ratio = series_idx as f64 / v5_total as f64;
    println!(
        "[v5 sparse 10k] v5={v5_total}B  SERIES_IDX={series_idx}B ({:.4}% of obj)  \
         SERIES_META_CHUNKS={meta_chunks}B",
        ratio * 100.0,
    );
    assert!(
        ratio < V5_SPARSE_OVERHEAD_RATIO_MAX,
        "v5 SERIES_IDX overhead ratio {ratio:.4} exceeds gate {V5_SPARSE_OVERHEAD_RATIO_MAX}",
    );
}

/// Determinism: the fixed-seed generator -> v5 writer pipeline yields
/// byte-identical section sizes on a second run in the same process. Guards
/// against hidden nondeterminism (map iteration order, unseeded RNG) that
/// would make the gate ratio wobble.
#[test]
fn v5_measurements_are_deterministic() {
    let measure = || {
        let raw = shape_many_small(SERIES, TOTAL_SAMPLES);
        let v5 = build_segment_v5(raw);
        (
            v5.bytes.len() as u64,
            section_len(&v5.bytes, SERIES_IDX),
            section_len(&v5.bytes, SERIES_META_CHUNKS),
        )
    };
    assert_eq!(measure(), measure());
}
