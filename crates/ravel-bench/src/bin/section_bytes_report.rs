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
//!   `ZSTD_LEVEL`) under v1's sorted ordering vs v2's first-occurrence
//!   ordering, and the delta (plan section 3.3 fallback trigger at > 10%).
//!
//! Two shapes are reported: the shared generator's `many_small` shape
//! (5 label pairs/series, identical to the `segment_encode` bench), and a
//! 15-labels-per-series shape constructed inline in this bin. The 15-label
//! shape is built here and NOT added to `ravel_bench::generator` on purpose:
//! a concurrent task owns generator.rs, so this bin stays self-contained.
//!
//! Run: `cargo run -p ravel-bench --release --bin section_bytes_report`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{
    LABEL_DICT, SERIES_IDS, SERIES_META, SERIES_TABLE, build_segment, build_segment_v2,
    section_bytes,
};
use ravel_segment::{Footer, ReaderLimits, WrittenSegment, open_from_full};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

const SERIES: usize = 100_000;
const TOTAL_SAMPLES: usize = 200_000;
/// zstd level the writer uses for LABEL_DICT/SERIES_META
/// (crates/ravel-segment/src/format.rs `ZSTD_LEVEL`).
const ZSTD_LEVEL: i32 = 3;

type Raw = Vec<(SeriesId, LabelSet, Vec<Sample>)>;

fn footer_of(seg: &WrittenSegment) -> Footer {
    open_from_full(&seg.bytes, ReaderLimits::default())
        .expect("open segment")
        .footer
}

/// Uncompressed (raw) length recorded for `kind` in the footer.
fn raw_len(footer: &Footer, kind: u32) -> u64 {
    footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present")
        .uncompressed_len
}

/// zstd -3 compressed size of a section's raw bytes, recomputed here from
/// the section's decompressed content so v1 (sorted) and v2
/// (first-occurrence) LABEL_DICT are compressed under one identical policy.
fn zstd3_of_section(seg: &WrittenSegment, footer: &Footer, kind: u32) -> usize {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    let stored = section_bytes(&seg.bytes, footer, kind);
    let raw = zstd::bulk::decompress(stored, section.uncompressed_len as usize).expect("zstd");
    zstd::bulk::compress(&raw, ZSTD_LEVEL).expect("zstd").len()
}

fn report(shape: &str, raw: Raw) {
    let v1 = build_segment(raw.clone());
    let v2 = build_segment_v2(raw);
    let f1 = footer_of(&v1);
    let f2 = footer_of(&v2);

    let table_raw = raw_len(&f1, SERIES_TABLE);
    let ids_raw = raw_len(&f2, SERIES_IDS);
    let meta_raw = raw_len(&f2, SERIES_META);
    let v2_catalog = ids_raw + meta_raw;
    let catalog_ratio = v2_catalog as f64 / table_raw as f64;

    let v1_total = v1.bytes.len() as u64;
    let v2_total = v2.bytes.len() as u64;
    let total_ratio = v2_total as f64 / v1_total as f64;

    let dict_v1 = zstd3_of_section(&v1, &f1, LABEL_DICT);
    let dict_v2 = zstd3_of_section(&v2, &f2, LABEL_DICT);
    let dict_delta = dict_v2 as i64 - dict_v1 as i64;
    let dict_delta_pct = dict_delta as f64 / dict_v1 as f64 * 100.0;

    println!("== {shape} ({SERIES} series, {TOTAL_SAMPLES} samples) ==");
    println!(
        "  series in segment (v1/v2):       {} / {}",
        f1.series_count, f2.series_count
    );
    println!();
    println!("  v1 SERIES_TABLE raw bytes:       {table_raw}");
    println!("  v2 SERIES_IDS raw bytes:         {ids_raw}");
    println!("  v2 SERIES_META raw bytes:        {meta_raw}");
    println!("  v2 catalog raw (IDS+META):       {v2_catalog}");
    println!(
        "  catalog ratio v2/v1:             {:.4} ({:.2}%)  gate <=60%: {}",
        catalog_ratio,
        catalog_ratio * 100.0,
        verdict(catalog_ratio <= 0.60),
    );
    println!();
    println!("  v1 total object bytes:           {v1_total}");
    println!("  v2 total object bytes:           {v2_total}");
    println!(
        "  total ratio v2/v1:               {:.4} ({:+.2}%)  gate <=v1+1%: {}",
        total_ratio,
        (total_ratio - 1.0) * 100.0,
        verdict(v2_total <= v1_total + v1_total / 100),
    );
    println!();
    println!(
        "  LABEL_DICT raw bytes (v1/v2):    {} / {}",
        raw_len(&f1, LABEL_DICT),
        raw_len(&f2, LABEL_DICT)
    );
    println!("  LABEL_DICT zstd -3 v1 (sorted):  {dict_v1}");
    println!("  LABEL_DICT zstd -3 v2 (sorted):  {dict_v2}");
    println!(
        "  LABEL_DICT delta v2-v1:          {dict_delta:+} bytes ({dict_delta_pct:+.2}%)  <=+10%: {}",
        verdict(dict_delta_pct <= 10.0),
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

/// The shared generator's `many_small` shape (5 label pairs/series),
/// identical config to the `segment_encode` bench.
fn shape_many_small() -> Raw {
    let config = WorkloadConfig {
        series_count: SERIES,
        samples_per_series: (TOTAL_SAMPLES / SERIES).max(1),
        cardinality: CardinalityProfile::many_small(SERIES),
        ..Default::default()
    };
    generate_raw(&config).expect("generate")
}

/// A 15-labels-per-series shape built inline (not via the shared
/// generator). Every series carries the same 15 label names
/// (`label_000..label_014`) with per-series-unique values plus a unique
/// `series_idx`, so all series collapse to a single v2 schema: this is the
/// production-shaped case (plan sec 1.2/3.4: 10-20 labels/series) where the
/// schema dictionary earns the most.
fn shape_wide_15() -> Raw {
    let tenant = TenantId::new("bench-tenant");
    let start_ts_ns: i64 = 1_700_000_000_000_000_000;
    let interval_ns: i64 = 1_000_000_000;
    let samples_per_series = (TOTAL_SAMPLES / SERIES).max(1);
    let mut out: Raw = Vec::with_capacity(SERIES);
    for i in 0..SERIES {
        let mut labels: Vec<Label> = (0..15)
            .map(|j| Label {
                name: format!("label_{j:03}"),
                value: format!("v{i}_{j}"),
            })
            .collect();
        labels.push(Label {
            name: "series_idx".to_string(),
            value: i.to_string(),
        });
        let labels = LabelSet::new(labels).expect("labels");
        let series_id = SeriesId::compute(&tenant, "bench_gauge", &labels).expect("compute");
        let mut ts = start_ts_ns;
        let mut samples = Vec::with_capacity(samples_per_series);
        for k in 0..samples_per_series {
            ts += interval_ns;
            samples.push(Sample {
                ts_ns: ts,
                value: i as f64 + k as f64 * 0.5,
            });
        }
        out.push((series_id, labels, samples));
    }
    out
}

fn main() {
    println!("# RSEG v2 stored-bytes report (issue #93, docs/rseg-v2-plan.md sec 1.2/sec 8)");
    println!(
        "# gates: catalog raw <=60% of v1 SERIES_TABLE; object bytes <=v1+1%; LABEL_DICT delta recorded"
    );
    println!();
    report("many_small (5 label pairs/series)", shape_many_small());
    report("wide (15 labels/series, inline)", shape_wide_15());
}
