//! Deterministic byte-gate test for the RSEG v5 sparse catalog (ADR-0027).
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

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::section_accounting::shape_many_small;
use ravel_bench::segment_support::{
    LABEL_DICT, SERIES_IDS, SERIES_IDX, SERIES_META, SERIES_META_CHUNKS, TS_PAGES, VAL_PAGES,
    bench_bounds, bench_identity, bench_meta, build_segment_v5,
};
use ravel_segment::{
    ReaderLimits, RunInputV4, RunInputV7, SampleProvenance, SegmentWriter, SeriesInputV4,
    SeriesInputV7, SeriesValues, WrittenSegment, encode_run_v4, open_from_full,
};
use ravel_types::{LabelSet, Sample, SeriesId};

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

// --- run-fragmentation byte gate (issue #312, ADR-0092) -----------------
//
// The gate above builds one run of two samples per series, so it measures the
// per-series identity floor and cannot see run fragmentation. This gate builds
// the SAME sample data two ways through `SegmentWriter::write_v5` and pins that
// the fragmented layout costs materially more per sample than the merged one,
// which is the whole premise of ADR-0092's run-merging L1.

/// Fragmentation fixture: 500 series, 240 samples each, 15 s spacing,
/// millisecond-resolution timestamps with +/-200 ms jitter. 500 series is
/// below the 4096 sparse-emission threshold, so both layouts emit a plain
/// SERIES_META (not SERIES_META_CHUNKS), matching ADR-0092's measured table.
const FRAG_SERIES: usize = 500;
const FRAG_SAMPLES_PER_SERIES: usize = 240;
const FRAG_INTERVAL_NS: i64 = 15_000_000_000;
const FRAG_JITTER_NS: i64 = 200_000_000;
const MS_NS: i64 = 1_000_000;

/// A flush is modelled 2 s apart (ADR-0076 `max_flush_delay`), so the
/// fragmented layout's per-run provenance columns carry realistic distinct
/// values rather than an all-zero column that would understate its cost.
const FRAG_FLUSH_SPACING_NS: i64 = 2_000_000_000;

/// One deterministic workload: 500 series x 240 samples, timestamps snapped to
/// millisecond resolution and sorted ascending. Fixed seed, no wall-clock
/// time. Both layouts consume this identical data, so the comparison is
/// apples-to-apples.
fn fragmentation_workload() -> Vec<(SeriesId, LabelSet, Vec<Sample>)> {
    let config = WorkloadConfig {
        series_count: FRAG_SERIES,
        samples_per_series: FRAG_SAMPLES_PER_SERIES,
        interval_ns: FRAG_INTERVAL_NS,
        jitter_ns: FRAG_JITTER_NS,
        cardinality: CardinalityProfile::many_small(FRAG_SERIES),
        ..Default::default()
    };
    let mut raw = generate_raw(&config).expect("generate fragmentation workload");
    for (_, _, samples) in &mut raw {
        for s in samples.iter_mut() {
            // Snap to millisecond resolution.
            s.ts_ns = (s.ts_ns / MS_NS) * MS_NS;
        }
        samples.sort_by_key(|s| s.ts_ns);
    }
    raw
}

/// Merged layout: one run of N samples per series (one L0 flush's worth,
/// carrying one provenance triple).
fn build_merged(raw: &[(SeriesId, LabelSet, Vec<Sample>)]) -> WrittenSegment {
    let inputs: Vec<SeriesInputV4> = raw
        .iter()
        .map(|(series_id, labels, samples)| {
            let run = encode_run_v4(
                series_id,
                1_700_000_000_000_000_000,
                0,
                0,
                &SeriesValues::Scalar(samples.clone()),
            )
            .expect("encode merged run");
            SeriesInputV4 {
                series_id: *series_id,
                labels: labels.clone(),
                runs: vec![run],
            }
        })
        .collect();
    SegmentWriter::write_v5(inputs, bench_identity(), bench_bounds(), bench_meta())
        .expect("write merged v5")
}

/// Fragmented layout: N runs of one sample per series, each modelling a
/// separate L0 flush with its own provenance. Every one-sample value page
/// falls back to raw f64 and every one-sample timestamp page is an absolute
/// varint, exactly the regime ADR-0092 measures.
fn build_fragmented(raw: &[(SeriesId, LabelSet, Vec<Sample>)]) -> WrittenSegment {
    let inputs: Vec<SeriesInputV4> = raw
        .iter()
        .map(|(series_id, labels, samples)| {
            let runs: Vec<RunInputV4> = samples
                .iter()
                .enumerate()
                .map(|(k, sample)| {
                    encode_run_v4(
                        series_id,
                        1_700_000_000_000_000_000 + k as i64 * FRAG_FLUSH_SPACING_NS,
                        0,
                        k as u64,
                        &SeriesValues::Scalar(vec![*sample]),
                    )
                    .expect("encode fragmented run")
                })
                .collect();
            SeriesInputV4 {
                series_id: *series_id,
                labels: labels.clone(),
                runs,
            }
        })
        .collect();
    SegmentWriter::write_v5(inputs, bench_identity(), bench_bounds(), bench_meta())
        .expect("write fragmented v5")
}

/// The run-merged L1 layout the SHIPPING compactor actually produces (ADR-0092
/// decision 1, issue #315): one run of N samples per series, carrying the
/// per-sample dedup provenance column that a merge of N distinct L0 flushes
/// requires. Each sample keeps the flush it came from: `created_unix_ns` and
/// `writer_seq` vary per flush, `in_page_index` is 0 (each fragmented flush held
/// one sample). This is `build_merged` plus the real provenance cost, so its
/// bytes-per-sample is the epic's actual result, not the no-provenance floor.
fn build_merged_with_provenance(raw: &[(SeriesId, LabelSet, Vec<Sample>)]) -> WrittenSegment {
    let base = 1_700_000_000_000_000_000i64;
    let inputs: Vec<SeriesInputV7> = raw
        .iter()
        .map(|(series_id, labels, samples)| {
            let run = encode_run_v4(
                series_id,
                base,
                0,
                0,
                &SeriesValues::Scalar(samples.clone()),
            )
            .expect("encode merged run");
            let column: Vec<SampleProvenance> = samples
                .iter()
                .enumerate()
                .map(|(k, _)| SampleProvenance {
                    created_unix_ns: base + k as i64 * FRAG_FLUSH_SPACING_NS,
                    writer_epoch: 0,
                    writer_seq: k as u64,
                    in_page_index: 0,
                })
                .collect();
            SeriesInputV7 {
                series_id: *series_id,
                labels: labels.clone(),
                runs: vec![RunInputV7 {
                    run,
                    provenance: Some(column),
                }],
            }
        })
        .collect();
    SegmentWriter::write_v7_with_provenance(
        inputs,
        bench_identity(),
        bench_bounds(),
        bench_meta(),
        Vec::new(),
    )
    .expect("write merged v7 with provenance")
}

/// The five per-section byte-per-sample figures ADR-0092 reports, plus the
/// object total.
struct SectionSplit {
    label_dict: f64,
    series_ids: f64,
    series_meta: f64,
    ts_pages: f64,
    val_pages: f64,
    total: f64,
}

fn section_split(seg: &WrittenSegment, total_samples: usize) -> SectionSplit {
    let n = total_samples as f64;
    let per = |kind: u32| section_len(&seg.bytes, kind) as f64 / n;
    // At 500 series the layout is non-sparse: SERIES_META (kind 6) carries the
    // run-major columns; SERIES_META_CHUNKS (kind 9) is absent (len 0).
    let series_meta = per(SERIES_META) + per(SERIES_META_CHUNKS);
    SectionSplit {
        label_dict: per(LABEL_DICT),
        series_ids: per(SERIES_IDS),
        series_meta,
        ts_pages: per(TS_PAGES),
        val_pages: per(VAL_PAGES),
        total: seg.bytes.len() as f64 / n,
    }
}

fn print_split(name: &str, s: &SectionSplit) {
    println!(
        "[fragmentation {name}] total={:.2} B/sample  \
         LABEL_DICT={:.2} SERIES_IDS={:.2} SERIES_META={:.2} TS_PAGES={:.2} VAL_PAGES={:.2}",
        s.total, s.label_dict, s.series_ids, s.series_meta, s.ts_pages, s.val_pages,
    );
}

/// Pinned lower bound on `fragmented_total / merged_total`. Measured on this
/// tree (RSEG v7, #314) the ratio is 2.987: fragmented 26.52 B/sample against
/// merged 8.88 (see the printed splits). That is down from the 3.126 #312
/// measured and the 3.68 ADR-0092's table reports, because the page-codec wins
/// in this train (ALP, GCD-delta-FOR, GCD timestamps, first-ts-as-delta, the
/// single-sample no-pad rule) shrank the fragmented regime's per-run pages more
/// than the merged one. Re-derived here from the 2.987 measured value using the
/// same ~20%-below-measured margin policy #312 used to pin 2.5 against its then
/// 3.126, which lands at 2.4. The gate flags a regression in the fragmentation
/// penalty without being brittle to codec-level byte wobble; it is not a no-op,
/// and a superseded pin (2.5 was pinned against a measurement two page-codec
/// generations old) is exactly the drift this epic keeps finding.
const FRAG_RATIO_MIN: f64 = 2.4;

#[test]
fn fragmented_multi_run_shape_costs_more_than_merged() {
    let raw = fragmentation_workload();
    let total_samples: usize = raw.iter().map(|(_, _, s)| s.len()).sum();
    assert_eq!(total_samples, FRAG_SERIES * FRAG_SAMPLES_PER_SERIES);

    let merged = build_merged(&raw);
    let fragmented = build_fragmented(&raw);

    let merged_split = section_split(&merged, total_samples);
    let fragmented_split = section_split(&fragmented, total_samples);

    // Both splits printed on success so the numbers can be diffed across runs.
    print_split("merged", &merged_split);
    print_split("fragmented", &fragmented_split);

    // Sanity: at 500 series both layouts are non-sparse (plain SERIES_META,
    // no chunked catalog).
    assert_eq!(
        section_len(&merged.bytes, SERIES_META_CHUNKS),
        0,
        "500 series is below the sparse threshold: no SERIES_META_CHUNKS"
    );

    let ratio = fragmented_split.total / merged_split.total;
    println!(
        "[fragmentation] fragmented/merged total ratio = {ratio:.3} (gate >= {FRAG_RATIO_MIN})"
    );

    assert!(
        ratio >= FRAG_RATIO_MIN,
        "fragmented layout ({:.2} B/sample) must exceed merged ({:.2} B/sample) by >= {FRAG_RATIO_MIN}x, got {ratio:.3}x",
        fragmented_split.total,
        merged_split.total,
    );
}

/// Pinned lower bound on `fragmented_total / merged_with_provenance_total`,
/// the epic's headline result: a run-merged L1 object carrying the per-sample
/// provenance columns still costs materially less per sample than the fragmented
/// inputs it replaces. Measured on this tree the merged-with-provenance shape is
/// 8.88 B/sample against the fragmented 26.52, a ratio of 2.986. The per-sample
/// provenance cost is negligible here: 320 bytes over 120,000 samples (~0.003
/// B/sample), because the four columns are constant/arithmetic sequences that
/// `encode_i64` and zstd flatten almost to nothing on a regular flush cadence.
/// That is far below ADR-0092's ~5 B/sample estimate, which assumed the
/// columns did not compress. The gate is pinned ~20% below the measured ratio,
/// matching the `FRAG_RATIO_MIN` margin policy, so it flags a real regression in
/// the merge win without wobbling on codec-level byte drift.
const MERGED_WITH_PROV_RATIO_MIN: f64 = 2.4;

#[test]
fn run_merged_l1_with_provenance_costs_materially_less_than_inputs() {
    let raw = fragmentation_workload();
    let total_samples: usize = raw.iter().map(|(_, _, s)| s.len()).sum();
    assert_eq!(total_samples, FRAG_SERIES * FRAG_SAMPLES_PER_SERIES);

    let fragmented = build_fragmented(&raw);
    let merged_no_prov = build_merged(&raw);
    let merged_with_prov = build_merged_with_provenance(&raw);

    let fragmented_split = section_split(&fragmented, total_samples);
    let merged_no_prov_split = section_split(&merged_no_prov, total_samples);
    let merged_with_prov_split = section_split(&merged_with_prov, total_samples);

    print_split("fragmented (inputs)", &fragmented_split);
    print_split("merged (no provenance)", &merged_no_prov_split);
    print_split(
        "merged + per-sample provenance (shipping L1)",
        &merged_with_prov_split,
    );

    let provenance_cost = merged_with_prov_split.total - merged_no_prov_split.total;
    println!(
        "[fragmentation] exact object bytes: merged_no_prov={} merged_with_prov={} delta={} \
         over {total_samples} samples",
        merged_no_prov.bytes.len(),
        merged_with_prov.bytes.len(),
        merged_with_prov.bytes.len() as i64 - merged_no_prov.bytes.len() as i64,
    );
    // The provenance-carrying object must actually differ from the no-provenance
    // one: the columns are present, just highly compressible (constant/arithmetic
    // sequences under encode_i64 inside zstd), so their per-sample cost is small,
    // not zero-because-dropped.
    assert!(
        merged_with_prov.bytes.len() > merged_no_prov.bytes.len(),
        "the provenance columns must add bytes; equal size means they were dropped"
    );
    let ratio = fragmented_split.total / merged_with_prov_split.total;
    println!(
        "[fragmentation] merged+provenance = {:.2} B/sample (per-sample provenance cost = {:.2} \
         B/sample); fragmented/merged+prov ratio = {ratio:.3} (gate >= {MERGED_WITH_PROV_RATIO_MIN})",
        merged_with_prov_split.total, provenance_cost,
    );

    assert!(
        ratio >= MERGED_WITH_PROV_RATIO_MIN,
        "run-merged L1 with provenance ({:.2} B/sample) must cost materially less than the \
         fragmented inputs ({:.2} B/sample): ratio {ratio:.3} < {MERGED_WITH_PROV_RATIO_MIN}",
        merged_with_prov_split.total,
        fragmented_split.total,
    );
}

/// Determinism: the fixed-seed workload -> both writers yields byte-identical
/// object sizes on a second run, so the pinned ratio never wobbles.
#[test]
fn fragmentation_measurements_are_deterministic() {
    let measure = || {
        let raw = fragmentation_workload();
        (
            build_merged(&raw).bytes.len(),
            build_fragmented(&raw).bytes.len(),
        )
    };
    assert_eq!(measure(), measure());
}
