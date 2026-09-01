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
//! fixture is deterministic. The per-shape numbers are pinned by the
//! committed golden rather than printed; regenerate it with
//! `REGEN_CATALOG_BYTE_GATES=1`.
//!
//! This file also carries the run-fragmentation gate (issue #312, ADR-0092)
//! and, on top of it, the honest per-shape byte gate (issue #370). The
//! fragmentation gate historically measured only the generator's default
//! full-mantissa float, which no integer-model value codec can compress, then
//! quoted the result as the format's cost. The #370 gate re-points the same
//! 500x240 fixture at realistic value shapes (integer counter with resets,
//! integer gauge, two-decimal gauge, shared with the codec bake-off) and
//! commits the exact per-section split, the value encoding each shape lands
//! on, and a second scrape interval as a regenerable golden
//! (`catalog_byte_gates_golden.txt`); the full-entropy float stays as an
//! explicitly labelled control.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::section_accounting::shape_many_small;
use ravel_bench::segment_support::{
    SERIES_IDX, SERIES_META, SERIES_META_CHUNKS, VAL_PAGES, bench_bounds, bench_identity,
    bench_meta, build_segment_v5,
};
use ravel_bench::value_shapes::{ValueShape, value_stream};
use ravel_segment::{
    ReaderLimits, RunInputV4, RunInputV7, SampleProvenance, SegmentWriter, SeriesInputV4,
    SeriesInputV7, SeriesValues, WrittenSegment, decode_catalog_v5, encode_run_v4, open_from_full,
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
/// apples-to-apples. Values keep the generator's default full-entropy
/// counter/gauge floats: this is the historical control fixture (issue #370),
/// the one that produces 8.88 B/sample merged because no integer-model codec
/// can compress a full-mantissa f64.
fn fragmentation_workload() -> Vec<(SeriesId, LabelSet, Vec<Sample>)> {
    fragmentation_workload_at(FRAG_INTERVAL_NS)
}

/// [`fragmentation_workload`] at an arbitrary scrape interval, so the same
/// fixture can be measured at 15 s and at a second cadence (issue #370
/// deliverable 5). Only `interval_ns` moves; series count, sample count,
/// jitter, resolution, and seed are fixed.
fn fragmentation_workload_at(interval_ns: i64) -> Vec<(SeriesId, LabelSet, Vec<Sample>)> {
    let config = WorkloadConfig {
        series_count: FRAG_SERIES,
        samples_per_series: FRAG_SAMPLES_PER_SERIES,
        interval_ns,
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

/// The control fixture's timestamp shape with the value column replaced by a
/// realistic metric shape ([`ValueShape`], shared with the codec bake-off).
/// Timestamps are untouched, so a shape's TS_PAGES cost matches the control's
/// and only VAL_PAGES (and its dependent codec choice) moves. Each series gets
/// its own value walk (salt = series index), so the zstd-compressed VAL
/// section cannot collapse 500 identical streams into an unrealistic figure.
fn shaped_workload(shape: ValueShape, interval_ns: i64) -> Vec<(SeriesId, LabelSet, Vec<Sample>)> {
    let mut raw = fragmentation_workload_at(interval_ns);
    for (idx, (_, _, samples)) in raw.iter_mut().enumerate() {
        let values = value_stream(shape, samples.len(), idx as u64);
        for (s, v) in samples.iter_mut().zip(values) {
            s.value = v;
        }
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

/// Total samples in the fragmentation fixture, the denominator of every
/// bytes-per-sample figure below.
const FRAG_TOTAL_SAMPLES: usize = FRAG_SERIES * FRAG_SAMPLES_PER_SERIES;

/// A full byte breakdown of one written object: every footer section by kind,
/// plus the residual. Sections + residual sum exactly to the object.
///
/// The old five-part `section_split` did not sum: it divided the whole object
/// length for the total but summed only five named sections for the parts, so
/// the footer, the 16-byte trailer, and inter-section alignment padding were an
/// unaccounted remainder (issue #370 deliverable 4). This view names that
/// remainder as `residual`, and [`Breakdown::assert_sums`] pins the identity.
struct Breakdown {
    object_bytes: u64,
    total_samples: usize,
    /// (kind, len) for every section the footer lists, sorted by kind.
    sections: Vec<(u32, u64)>,
    /// `object_bytes - sum(section lens)`: footer + 16-byte trailer +
    /// inter-section alignment padding.
    residual: u64,
}

impl Breakdown {
    fn of(seg: &WrittenSegment, total_samples: usize) -> Self {
        let loc = open_from_full(&seg.bytes, ReaderLimits::default()).expect("open");
        let mut sections: Vec<(u32, u64)> = loc
            .footer
            .sections
            .iter()
            .map(|s| (s.kind, s.len))
            .collect();
        sections.sort_by_key(|(k, _)| *k);
        let object_bytes = seg.bytes.len() as u64;
        let section_sum: u64 = sections.iter().map(|(_, l)| *l).sum();
        let residual = object_bytes
            .checked_sub(section_sum)
            .expect("section bytes cannot exceed the object");
        Breakdown {
            object_bytes,
            total_samples,
            sections,
            residual,
        }
    }

    fn per_sample(&self, bytes: u64) -> f64 {
        bytes as f64 / self.total_samples as f64
    }

    fn total_per_sample(&self) -> f64 {
        self.per_sample(self.object_bytes)
    }

    fn section_bytes(&self, kind: u32) -> u64 {
        self.sections
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, l)| *l)
            .unwrap_or(0)
    }

    /// The named sections account for all but a small fixed remainder of the
    /// object.
    ///
    /// Sections-plus-residual equalling the object is not asserted here
    /// because `residual` is defined as that difference, so the equality is an
    /// identity for any input. The golden is what pins it: it emits every
    /// section line and the residual, and an exact match proves they close.
    ///
    /// What is falsifiable, and what this checks, is that the remainder stays
    /// small. The residual is meant to be footer plus the 16-byte trailer plus
    /// inter-section alignment padding, a few hundred bytes. A new section
    /// that nobody added to `SECTION_KINDS` would land here instead and blow
    /// the bound.
    fn assert_sums(&self) {
        const MAX_RESIDUAL_BYTES: u64 = 4096;
        assert!(
            self.residual <= MAX_RESIDUAL_BYTES,
            "residual {} exceeds {MAX_RESIDUAL_BYTES} bytes for a {}-byte object: \
             a section is likely missing from the breakdown's section list",
            self.residual,
            self.object_bytes,
        );
    }
}

fn section_name(kind: u32) -> &'static str {
    match kind {
        1 => "LABEL_DICT",
        2 => "SERIES_TABLE",
        3 => "TS_PAGES",
        4 => "VAL_PAGES",
        5 => "SERIES_IDS",
        6 => "SERIES_META",
        7 => "HIST_PAGES",
        8 => "SERIES_IDX",
        9 => "SERIES_META_CHUNKS",
        10 => "EXEMPLARS",
        _ => "UNKNOWN",
    }
}

/// Value page encoding tag names (docs/segment-format.md, `page_enc`). The
/// point of the honest fixture is to make these fire: on the random-float
/// control every page is VAL_RAW_F64, and the integer-model codecs
/// (VAL_ALP, VAL_GCD_DELTA_FOR) cannot engage by construction.
fn val_enc_name(tag: u8) -> &'static str {
    match tag {
        16 => "VAL_GORILLA",
        17 => "VAL_RAW_F64",
        18 => "VAL_ALP",
        19 => "VAL_GCD_DELTA_FOR",
        _ => "VAL_UNKNOWN",
    }
}

/// Histogram of the value-page encoding tag over every run's VAL page. One
/// entry per run (fragmented: 240 per series; merged: 1 per series). This is
/// what confirms or refutes "the integer-model codecs never fire on the
/// fixture".
fn val_enc_histogram(seg: &WrittenSegment) -> BTreeMap<u8, usize> {
    let limits = ReaderLimits::default();
    let loc = open_from_full(&seg.bytes, limits).expect("open");
    let entries = decode_catalog_v5(&loc.footer, &seg.bytes, limits).expect("decode catalog");
    let val_sec = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == VAL_PAGES)
        .expect("VAL_PAGES present");
    let mut hist = BTreeMap::new();
    for e in &entries {
        for run in &e.runs {
            let (off, len) = run.val_page;
            if len == 0 {
                continue;
            }
            let tag = seg.bytes[(val_sec.offset + off) as usize];
            *hist.entry(tag).or_insert(0) += 1;
        }
    }
    hist
}

/// Pinned lower bound on `fragmented_total / merged_total`. Measured on this
/// tree (RSEG v7, #314) the ratio is 2.987 on the random-float control:
/// fragmented 26.52 B/sample against merged 8.88. That is down from the 3.126
/// #312 measured and the 3.68 ADR-0092's table reports, because the page-codec
/// wins in this train (ALP, GCD-delta-FOR, GCD timestamps, first-ts-as-delta,
/// the single-sample no-pad rule) shrank the fragmented regime's per-run pages
/// more than the merged one. Re-derived from the 2.987 measured value using the
/// same ~20%-below-measured margin policy #312 used to pin 2.5 against its then
/// 3.126, which lands at 2.4. The gate flags a regression in the fragmentation
/// penalty without being brittle to codec-level byte wobble.
const FRAG_RATIO_MIN: f64 = 2.4;

#[test]
fn fragmented_multi_run_shape_costs_more_than_merged() {
    let raw = fragmentation_workload();
    let total_samples: usize = raw.iter().map(|(_, _, s)| s.len()).sum();
    assert_eq!(total_samples, FRAG_TOTAL_SAMPLES);

    let merged = Breakdown::of(&build_merged(&raw), total_samples);
    let fragmented = Breakdown::of(&build_fragmented(&raw), total_samples);
    merged.assert_sums();
    fragmented.assert_sums();

    // Sanity: at 500 series both layouts are non-sparse (plain SERIES_META,
    // no chunked catalog).
    assert_eq!(
        merged.section_bytes(SERIES_META_CHUNKS),
        0,
        "500 series is below the sparse threshold: no SERIES_META_CHUNKS"
    );

    let ratio = fragmented.total_per_sample() / merged.total_per_sample();
    assert!(
        ratio >= FRAG_RATIO_MIN,
        "fragmented layout ({:.2} B/sample) must exceed merged ({:.2} B/sample) by >= {FRAG_RATIO_MIN}x, got {ratio:.3}x",
        fragmented.total_per_sample(),
        merged.total_per_sample(),
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
/// matching the `FRAG_RATIO_MIN` margin policy.
const MERGED_WITH_PROV_RATIO_MIN: f64 = 2.4;

#[test]
fn run_merged_l1_with_provenance_costs_materially_less_than_inputs() {
    let raw = fragmentation_workload();
    let total_samples: usize = raw.iter().map(|(_, _, s)| s.len()).sum();
    assert_eq!(total_samples, FRAG_TOTAL_SAMPLES);

    let fragmented = build_fragmented(&raw);
    let merged_no_prov = build_merged(&raw);
    let merged_with_prov = build_merged_with_provenance(&raw);

    let fragmented_b = Breakdown::of(&fragmented, total_samples);
    let merged_with_prov_b = Breakdown::of(&merged_with_prov, total_samples);

    // The provenance-carrying object must actually differ from the no-provenance
    // one: the columns are present, just highly compressible (constant/arithmetic
    // sequences under encode_i64 inside zstd), so their per-sample cost is small,
    // not zero-because-dropped.
    assert!(
        merged_with_prov.bytes.len() > merged_no_prov.bytes.len(),
        "the provenance columns must add bytes; equal size means they were dropped"
    );
    let ratio = fragmented_b.total_per_sample() / merged_with_prov_b.total_per_sample();
    assert!(
        ratio >= MERGED_WITH_PROV_RATIO_MIN,
        "run-merged L1 with provenance ({:.2} B/sample) must cost materially less than the \
         fragmented inputs ({:.2} B/sample): ratio {ratio:.3} < {MERGED_WITH_PROV_RATIO_MIN}",
        merged_with_prov_b.total_per_sample(),
        fragmented_b.total_per_sample(),
    );
}

/// Determinism: the fixed-seed workload -> both writers yields byte-identical
/// object sizes on a second run, so the pinned numbers never wobble.
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

// --- honest per-shape byte gate + committed golden (issue #370) ---------
//
// The gate above measures only the random-float control: the generator's
// default full-mantissa counter/gauge floats, which no integer-model value
// codec can compress. Real metrics are integer counters, integer gauges, and
// low-precision decimals. This section measures those realistic shapes on the
// SAME 500x240 fixture (only the value column changes) and commits the exact
// per-section split, the value encoding each shape lands on, and a second
// scrape interval, as a regenerable golden.

/// One value shape the golden measures. The control is the historical fixture
/// (generator-default floats, the 8.88 anchor); the realistic shapes overwrite
/// only the value column of that same fixture.
#[derive(Clone, Copy)]
enum GateShape {
    RandomFloatControl,
    Realistic(ValueShape),
}

impl GateShape {
    fn label(self) -> &'static str {
        match self {
            GateShape::RandomFloatControl => "random_float(ctrl)",
            GateShape::Realistic(s) => s.label(),
        }
    }

    fn workload(self, interval_ns: i64) -> Vec<(SeriesId, LabelSet, Vec<Sample>)> {
        match self {
            GateShape::RandomFloatControl => fragmentation_workload_at(interval_ns),
            GateShape::Realistic(s) => shaped_workload(s, interval_ns),
        }
    }
}

/// The shapes measured: an integer counter with resets, an integer gauge, a
/// two-decimal gauge, and the full-entropy random float kept as an explicitly
/// labelled control (issue #370 deliverable 1).
const GATE_SHAPES: [GateShape; 4] = [
    GateShape::Realistic(ValueShape::CounterIntResets),
    GateShape::Realistic(ValueShape::GaugeInt),
    GateShape::Realistic(ValueShape::GaugeDec2),
    GateShape::RandomFloatControl,
];

/// The layouts measured per shape: the fragmented inputs, the merged object
/// without provenance, and the shipping merged object with per-sample
/// provenance.
#[derive(Clone, Copy)]
enum Layout {
    Fragmented,
    MergedNoProv,
    MergedWithProv,
}

impl Layout {
    fn tag(self) -> &'static str {
        match self {
            Layout::Fragmented => "fragmented",
            Layout::MergedNoProv => "merged_no_prov",
            Layout::MergedWithProv => "merged_with_prov",
        }
    }

    fn build(self, raw: &[(SeriesId, LabelSet, Vec<Sample>)]) -> WrittenSegment {
        match self {
            Layout::Fragmented => build_fragmented(raw),
            Layout::MergedNoProv => build_merged(raw),
            Layout::MergedWithProv => build_merged_with_provenance(raw),
        }
    }
}

const GATE_LAYOUTS: [Layout; 3] = [
    Layout::Fragmented,
    Layout::MergedNoProv,
    Layout::MergedWithProv,
];

/// The two scrape intervals: the fixture's 15 s and a 60 s second point
/// (issue #370 deliverable 5). No measurement at any interval other than 15 s
/// existed before.
const GATE_INTERVALS: [(&str, i64); 2] = [("15s", FRAG_INTERVAL_NS), ("60s", 60_000_000_000)];

/// One measured cell, retained so rendering and the assertions run over a
/// single build pass.
struct GateMeasurement {
    interval: &'static str,
    shape: &'static str,
    layout: &'static str,
    breakdown: Breakdown,
    val_enc: BTreeMap<u8, usize>,
}

/// Build every (interval, shape, layout) cell once.
fn measure_gate() -> Vec<GateMeasurement> {
    let mut out = Vec::new();
    for (interval, interval_ns) in GATE_INTERVALS {
        for shape in GATE_SHAPES {
            let raw = shape.workload(interval_ns);
            let total_samples: usize = raw.iter().map(|(_, _, s)| s.len()).sum();
            assert_eq!(total_samples, FRAG_TOTAL_SAMPLES);
            for layout in GATE_LAYOUTS {
                let seg = layout.build(&raw);
                let breakdown = Breakdown::of(&seg, total_samples);
                breakdown.assert_sums();
                out.push(GateMeasurement {
                    interval,
                    shape: shape.label(),
                    layout: layout.tag(),
                    val_enc: val_enc_histogram(&seg),
                    breakdown,
                });
            }
        }
    }
    out
}

/// Render one measurement as fully-qualified lines, so a golden mismatch on any
/// single line names the interval, shape, layout, and section it belongs to.
/// bytes are exact; B/sample = bytes / total_samples.
fn render_measurement(m: &GateMeasurement, out: &mut String) {
    let q = format!("{} {} {}", m.interval, m.shape, m.layout);
    let b = &m.breakdown;
    let _ = writeln!(out, "{q} object_bytes {}", b.object_bytes);
    let _ = writeln!(out, "{q} total_per_sample {:.4}", b.total_per_sample());
    for (kind, len) in &b.sections {
        let _ = writeln!(
            out,
            "{q} section {} {} {:.4}",
            section_name(*kind),
            len,
            b.per_sample(*len),
        );
    }
    let _ = writeln!(
        out,
        "{q} residual {} {:.4}",
        b.residual,
        b.per_sample(b.residual),
    );
    for (tag, count) in &m.val_enc {
        let _ = writeln!(out, "{q} valenc {} {}", val_enc_name(*tag), count);
    }
}

fn render_golden(measurements: &[GateMeasurement]) -> String {
    let mut out = String::new();
    out.push_str(GOLDEN_HEADER);
    for m in measurements {
        render_measurement(m, &mut out);
    }
    out
}

const GOLDEN_HEADER: &str = "\
# Catalog byte-gate golden (issue #370). Committed, exact, deterministic.
#
# Regenerate after any INTENDED codec or format change:
#   REGEN_CATALOG_BYTE_GATES=1 cargo test -p ravel-bench --test catalog_byte_gates
#
# Fixture: 500 series x 240 samples, 15s/60s spacing, ms resolution, 200ms
# jitter, seed 0. Only the value column changes across shapes; timestamps are
# identical, so a shape moves VAL_PAGES (and its codec choice) and nothing else
# on the value side. bytes are exact and deterministic; timing is not, so only
# bytes are asserted (see the file header).
#
# Each line is fully qualified: <interval> <shape> <layout> <metric...>.
# 'section <NAME> <bytes> <B/sample>' lines plus the 'residual' line sum
# exactly to 'object_bytes' (residual = footer + 16-byte trailer + inter-section
# alignment padding). 'valenc <ENC> <count>' is the value-page encoding tag over
# every run's VAL page (one per run): it shows which codec actually fired.
";

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("catalog_byte_gates_golden.txt")
}

/// The committed golden: the exact per-section split (with residual), the value
/// encoding each shape lands on, and a second scrape interval, for four value
/// shapes. This is the honest bytes-per-sample record issue #370 asks for; the
/// random-float control is the only shape that stays on VAL_RAW_F64 and near
/// 8.88 merged, and the realistic shapes land well below it.
#[test]
fn catalog_byte_gates_golden() {
    let measurements = measure_gate();
    let rendered = render_golden(&measurements);
    let path = golden_path();

    // An empty value must not regenerate: `REGEN_CATALOG_BYTE_GATES= cargo test`
    // would otherwise rewrite the golden instead of asserting against it, and
    // report a pass.
    if std::env::var("REGEN_CATALOG_BYTE_GATES").is_ok_and(|v| !v.is_empty()) {
        std::fs::write(&path, &rendered).expect("write golden");
        return;
    }

    let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {}: {e}. Regenerate with \
             REGEN_CATALOG_BYTE_GATES=1 cargo test -p ravel-bench --test catalog_byte_gates",
            path.display()
        )
    });

    if committed != rendered {
        for (i, (c, r)) in committed.lines().zip(rendered.lines()).enumerate() {
            assert_eq!(
                c,
                r,
                "catalog byte-gate golden mismatch at line {}. Regenerate INTENDED \
                 changes with REGEN_CATALOG_BYTE_GATES=1 cargo test -p ravel-bench \
                 --test catalog_byte_gates",
                i + 1,
            );
        }
        assert_eq!(
            committed.lines().count(),
            rendered.lines().count(),
            "catalog byte-gate golden line count differs. Regenerate with \
             REGEN_CATALOG_BYTE_GATES=1 cargo test -p ravel-bench --test catalog_byte_gates",
        );
        // Reaching here means the two differ as strings but match line for line
        // and in line count, so the difference is trailing-newline or
        // line-ending only. Fail rather than fall out of the branch reporting a
        // pass on a golden that did not match.
        panic!(
            "catalog byte-gate golden differs only in trailing whitespace or line \
             endings. Regenerate with REGEN_CATALOG_BYTE_GATES=1 cargo test \
             -p ravel-bench --test catalog_byte_gates"
        );
    }
}

/// Per-shape honesty gate (issue #370 deliverable 2): every realistic shape's
/// merged-with-provenance cost lands materially below the random-float
/// control's, because the integer-model codecs fire on realistic values and
/// cannot on a full-mantissa float. This is the claim the golden's numbers
/// back, asserted directly so a regression that makes the realistic shapes
/// stop compressing fails a named gate, not only the golden diff.
#[test]
fn realistic_shapes_beat_the_random_float_control() {
    let total_samples = FRAG_TOTAL_SAMPLES;
    let control = Breakdown::of(
        &build_merged_with_provenance(&GateShape::RandomFloatControl.workload(FRAG_INTERVAL_NS)),
        total_samples,
    )
    .total_per_sample();

    for shape in [
        ValueShape::CounterIntResets,
        ValueShape::GaugeInt,
        ValueShape::GaugeDec2,
    ] {
        let raw = shaped_workload(shape, FRAG_INTERVAL_NS);
        let merged = Breakdown::of(&build_merged_with_provenance(&raw), total_samples);
        assert!(
            merged.total_per_sample() < control,
            "realistic shape {} merged cost {:.2} B/sample must be below the random-float \
             control {:.2} B/sample; if it is not, the integer-model value codecs stopped firing",
            shape.label(),
            merged.total_per_sample(),
            control,
        );
    }
}
