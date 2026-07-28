//! Shared stored-bytes accounting for the RSEG columnar catalog gates
//! (docs/rseg-v2-plan.md sec 1.2/sec 8, issues #93, #146, #147, #166).
//!
//! One logical workload is encoded twice: once as a v1 segment (row-major
//! `SERIES_TABLE`) and once as a columnar segment (v2 `write_v2` or the v3
//! scalar path `write_v3`, both emitting `SERIES_IDS` + `SERIES_META` +
//! `LABEL_DICT`). [`measure_catalog`] compares the two and returns the
//! byte counts and the three gate ratios the byte-gate test asserts:
//!
//! - catalog raw (`SERIES_IDS` + `SERIES_META`) vs v1 `SERIES_TABLE` raw;
//! - total object bytes, columnar vs v1;
//! - `LABEL_DICT` compressed (zstd -3, matching the writer's `ZSTD_LEVEL`)
//!   for v1 and the columnar writers; all emit sorted order since #146.
//!
//! Both the `section_bytes_report` bin and the `catalog_byte_gates` test
//! consume this module, so the accounting logic lives here once rather than
//! being copied into each. Ratios are reported (not absolute bytes) so a
//! zstd version bump cannot break a caller spuriously: both sides share one
//! compressor.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use crate::segment_support::{LABEL_DICT, SERIES_IDS, SERIES_META, SERIES_TABLE, section_bytes};
use ravel_segment::{Footer, ReaderLimits, WrittenSegment, open_from_full};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

/// zstd level the writer uses for LABEL_DICT/SERIES_META
/// (crates/ravel-segment/src/format.rs `ZSTD_LEVEL`).
pub const ZSTD_LEVEL: i32 = 3;

/// Raw `(SeriesId, LabelSet, Vec<Sample>)` workload consumed by the segment
/// builders in [`crate::segment_support`].
pub type Raw = Vec<(SeriesId, LabelSet, Vec<Sample>)>;

/// Opens `seg` and returns its footer.
pub fn footer_of(seg: &WrittenSegment) -> Footer {
    open_from_full(&seg.bytes, ReaderLimits::default())
        .expect("open segment")
        .footer
}

/// Uncompressed (raw) length recorded for `kind` in the footer.
pub fn raw_len(footer: &Footer, kind: u32) -> u64 {
    footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present")
        .uncompressed_len
}

/// zstd -3 compressed size of a section's raw bytes, recomputed here from
/// the section's decompressed content so the v1 and columnar LABEL_DICT are
/// compressed under one identical policy.
pub fn zstd3_of_section(seg: &WrittenSegment, footer: &Footer, kind: u32) -> usize {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    let stored = section_bytes(&seg.bytes, footer, kind);
    let raw = zstd::bulk::decompress(stored, section.uncompressed_len as usize).expect("zstd");
    zstd::bulk::compress(&raw, ZSTD_LEVEL).expect("zstd").len()
}

/// Measured stored-byte accounting for one v1-vs-columnar comparison. All
/// byte counts are printed by callers on success; the ratios are what the
/// byte-gate test asserts. Deterministic: the same workload always yields
/// the same values.
#[derive(Debug, Clone)]
pub struct CatalogBytes {
    pub v1_series_count: u64,
    pub columnar_series_count: u64,
    /// v1 `SERIES_TABLE` raw (uncompressed) bytes.
    pub v1_series_table_raw: u64,
    pub series_ids_raw: u64,
    pub series_meta_raw: u64,
    /// Columnar catalog raw bytes: `SERIES_IDS` + `SERIES_META`.
    pub catalog_raw: u64,
    /// `catalog_raw` / `v1_series_table_raw`.
    pub catalog_ratio: f64,
    pub v1_total: u64,
    pub columnar_total: u64,
    /// `columnar_total` / `v1_total`.
    pub total_ratio: f64,
    pub v1_label_dict_zstd3: usize,
    pub columnar_label_dict_zstd3: usize,
    /// `columnar_label_dict_zstd3` / `v1_label_dict_zstd3`.
    pub label_dict_ratio: f64,
}

/// Compares a v1 segment against a columnar one (v2 or v3 scalar) built from
/// the same logical workload and returns the byte accounting. Both segments
/// must be encodings of the same series set; the caller owns building them
/// (via [`crate::segment_support::build_segment`] and its `_v2`/`_v3`
/// counterparts).
pub fn measure_catalog(v1: &WrittenSegment, columnar: &WrittenSegment) -> CatalogBytes {
    let f1 = footer_of(v1);
    let fc = footer_of(columnar);

    let v1_series_table_raw = raw_len(&f1, SERIES_TABLE);
    let series_ids_raw = raw_len(&fc, SERIES_IDS);
    let series_meta_raw = raw_len(&fc, SERIES_META);
    let catalog_raw = series_ids_raw + series_meta_raw;
    let catalog_ratio = catalog_raw as f64 / v1_series_table_raw as f64;

    let v1_total = v1.bytes.len() as u64;
    let columnar_total = columnar.bytes.len() as u64;
    let total_ratio = columnar_total as f64 / v1_total as f64;

    let v1_label_dict_zstd3 = zstd3_of_section(v1, &f1, LABEL_DICT);
    let columnar_label_dict_zstd3 = zstd3_of_section(columnar, &fc, LABEL_DICT);
    let label_dict_ratio = columnar_label_dict_zstd3 as f64 / v1_label_dict_zstd3 as f64;

    CatalogBytes {
        v1_series_count: f1.series_count,
        columnar_series_count: fc.series_count,
        v1_series_table_raw,
        series_ids_raw,
        series_meta_raw,
        catalog_raw,
        catalog_ratio,
        v1_total,
        columnar_total,
        total_ratio,
        v1_label_dict_zstd3,
        columnar_label_dict_zstd3,
        label_dict_ratio,
    }
}

/// The shared generator's `many_small` shape (5 label pairs/series),
/// identical config to the `segment_encode` bench. `total_samples` is spread
/// evenly across `series` (at least one sample each). Seed is the generator
/// default (0), so the workload is fixed.
pub fn shape_many_small(series: usize, total_samples: usize) -> Raw {
    let config = WorkloadConfig {
        series_count: series,
        samples_per_series: (total_samples / series.max(1)).max(1),
        cardinality: CardinalityProfile::many_small(series),
        ..Default::default()
    };
    generate_raw(&config).expect("generate")
}

/// A 15-labels-per-series shape built inline (not via the shared
/// generator). Every series carries the same 15 label names
/// (`label_000..label_014`) with per-series-unique values plus a unique
/// `series_idx`, so all series collapse to a single columnar schema: this is
/// the production-shaped case (plan sec 1.2/3.4: 10-20 labels/series) where
/// the schema dictionary earns the most. Fully deterministic (no RNG).
pub fn shape_wide_15(series: usize, total_samples: usize) -> Raw {
    let tenant = TenantId::new("bench-tenant");
    let start_ts_ns: i64 = 1_700_000_000_000_000_000;
    let interval_ns: i64 = 1_000_000_000;
    let samples_per_series = (total_samples / series.max(1)).max(1);
    let mut out: Raw = Vec::with_capacity(series);
    for i in 0..series {
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
