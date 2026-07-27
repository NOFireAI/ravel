//! Lazy catalog matcher cost versus per-segment cardinality (issue #100).
//!
//! A selective query pays two catalog costs before it ever touches a value
//! page: it fetches the catalog sections, then scans them to resolve an
//! equality matcher to the matching series. The lazy path
//! (`decode_catalog_matching` / `decode_catalog_matching_v2`, issue #16 and
//! ADR-0014) still scans the full series table to find matches even though it
//! only materializes `LabelSet`s for the matched entries, so that scan grows
//! with per-segment series count regardless of how few series match. This
//! bench measures how that scan scales as one segment's series count grows
//! {10_000, 100_000, 1_000_000} while the matcher stays at ~1% selectivity,
//! for both RSEG v1 and v2.
//!
//! The matcher path decodes only the catalog (LABEL_DICT plus SERIES_TABLE in
//! v1, LABEL_DICT plus SERIES_IDS plus SERIES_META in v2); it never reads
//! TS/VAL pages. Samples per series therefore do not affect the measured path
//! and are kept small so the 1M-series segment fits in host memory.
//!
//! Alongside each size this bench prints, once and outside any timing loop,
//! the stored byte size of those catalog sections, since that is the object
//! fetch a real query pays before the scan runs.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{
    LABEL_DICT, SERIES_IDS, SERIES_META, SERIES_TABLE, build_segment, build_segment_v2,
    decode_matching_entries, decode_matching_entries_v2, section_bytes,
};
use std::hint::black_box;

/// Number of distinct base label sets. Matching one set's value selects
/// exactly `series_count / DISTINCT_GROUPS` series; with 100 groups that is
/// 1% of any size divisible by 100 (all sizes below are).
const DISTINCT_GROUPS: usize = 100;
/// Label pairs per base set, matching the selective-decode bench shape.
const LABELS_PER_SET: usize = 6;
/// The catalog matcher path never decodes value pages, so this only affects
/// segment build memory, not the measured scan. Kept small to fit 1M series.
const SAMPLES_PER_SERIES: usize = 8;
/// One equality matcher against the first base set (`base_idx == 0`).
const MATCHER: (&str, &str) = ("label_000", "v0_0");
/// Per-segment series counts to sweep.
const SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];

/// Series selected by [`MATCHER`] at a given per-segment series count.
fn matched_count(series_count: usize) -> usize {
    series_count / DISTINCT_GROUPS
}

/// Generous upper bound on resident bytes needed to build and hold one size's
/// segments: the raw workload, its clone (v1 consumes one, v2 the other), and
/// both encoded objects. Used only to cap the sweep on a memory-limited host,
/// so it errs high (~2 KiB per series at [`SAMPLES_PER_SERIES`]).
fn estimate_peak_bytes(series_count: usize) -> u64 {
    (series_count as u64).saturating_mul(2_048)
}

/// Parses `MemAvailable` (in bytes) from `/proc/meminfo` contents.
fn parse_mem_available_bytes(meminfo: &str) -> Option<u64> {
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

/// Host `MemAvailable` in bytes, or `None` if `/proc/meminfo` is unreadable.
fn host_available_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| parse_mem_available_bytes(&s))
}

/// Whether a size's estimated peak fits under available memory. With no
/// memory reading available, assume it fits (do not silently cap).
fn size_fits(series_count: usize, available: Option<u64>) -> bool {
    match available {
        Some(avail) => estimate_peak_bytes(series_count) <= avail,
        None => true,
    }
}

fn bench_matcher_scaling(c: &mut Criterion) {
    let available = host_available_bytes();
    println!(
        "# matcher_scaling: lazy catalog matcher cost vs per-segment cardinality (issue #100)"
    );
    println!(
        "# matcher={MATCHER:?} selectivity=~1% ({DISTINCT_GROUPS} distinct groups, {LABELS_PER_SET} labels/set)"
    );
    println!(
        "# samples_per_series={SAMPLES_PER_SERIES} (matcher path decodes catalog only, never value pages)"
    );
    match available {
        Some(b) => println!(
            "# MemAvailable={b} bytes ({:.2} GiB)",
            b as f64 / (1u64 << 30) as f64
        ),
        None => println!("# MemAvailable=unknown (/proc/meminfo unreadable)"),
    }

    let mut group = c.benchmark_group("matcher_scaling");
    for &size in SIZES.iter() {
        if !size_fits(size, available) {
            println!(
                "# CAP size={size}: estimated peak {} bytes exceeds MemAvailable {}; largest feasible size is the last one built above (reason: host memory)",
                estimate_peak_bytes(size),
                available.map_or_else(|| "unknown".to_string(), |b| b.to_string()),
            );
            continue;
        }

        let config = WorkloadConfig {
            series_count: size,
            samples_per_series: SAMPLES_PER_SERIES,
            cardinality: CardinalityProfile {
                distinct_sets: DISTINCT_GROUPS,
                labels_per_set: LABELS_PER_SET,
            },
            ..Default::default()
        };
        let raw = generate_raw(&config).expect("generate workload");
        let bytes_v1 = build_segment(raw.clone()).bytes;
        let bytes_v2 = build_segment_v2(raw).bytes;

        // Stored catalog bytes and matched-count sanity, printed once outside
        // any timing loop. Reusing the returned footers avoids reopening.
        let (footer_v1, entries_v1) = decode_matching_entries(&bytes_v1, &[MATCHER]);
        let (footer_v2, entries_v2) = decode_matching_entries_v2(&bytes_v2, &[MATCHER]);
        let matched = matched_count(size);
        assert_eq!(entries_v1.len(), matched, "v1 matched count at size {size}");
        assert_eq!(entries_v2.len(), matched, "v2 matched count at size {size}");

        let dict_v1 = section_bytes(&bytes_v1, &footer_v1, LABEL_DICT).len();
        let table_v1 = section_bytes(&bytes_v1, &footer_v1, SERIES_TABLE).len();
        let dict_v2 = section_bytes(&bytes_v2, &footer_v2, LABEL_DICT).len();
        let ids_v2 = section_bytes(&bytes_v2, &footer_v2, SERIES_IDS).len();
        let meta_v2 = section_bytes(&bytes_v2, &footer_v2, SERIES_META).len();
        println!(
            "# size={size} matched={matched} v1_stored_bytes[LABEL_DICT={dict_v1} SERIES_TABLE={table_v1} total={}] v2_stored_bytes[LABEL_DICT={dict_v2} SERIES_IDS={ids_v2} SERIES_META={meta_v2} total={}]",
            dict_v1 + table_v1,
            dict_v2 + ids_v2 + meta_v2,
        );

        // Throughput is series scanned per second: the matcher scan visits
        // every series regardless of how few match, so `size` is the work.
        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(format!("lazy_v1/{size}"), |b| {
            b.iter(|| {
                let (_footer, entries) = decode_matching_entries(&bytes_v1, &[MATCHER]);
                black_box(entries.len())
            });
        });
        group.bench_function(format!("lazy_v2/{size}"), |b| {
            b.iter(|| {
                let (_footer, entries) = decode_matching_entries_v2(&bytes_v2, &[MATCHER]);
                black_box(entries.len())
            });
        });
        // `bytes_v1`/`bytes_v2` drop here before the next (larger) size builds.
    }
    group.finish();
}

criterion_group!(benches, bench_matcher_scaling);
criterion_main!(benches);

// These `#[test]` fns cover the bench's pure helper logic. They run under
// `cargo test -p ravel-bench --benches` (criterion's test mode builds this
// target with a test harness); a plain `cargo test` does not build benches.
// Calls are `super::`-qualified rather than glob-imported so nothing dangles
// as an unused import in the non-test compile clippy performs, where the
// `#[test]` fns themselves are stripped.
#[cfg(test)]
mod tests {
    #[test]
    fn matched_count_is_one_percent() {
        assert_eq!(super::matched_count(10_000), 100);
        assert_eq!(super::matched_count(100_000), 1_000);
        assert_eq!(super::matched_count(1_000_000), 10_000);
    }

    #[test]
    fn estimate_peak_grows_with_size() {
        assert!(super::estimate_peak_bytes(100_000) > super::estimate_peak_bytes(10_000));
        assert!(super::estimate_peak_bytes(1_000_000) > super::estimate_peak_bytes(100_000));
        // No overflow at the top of the sweep.
        assert_eq!(super::estimate_peak_bytes(1_000_000), 2_048_000_000);
    }

    #[test]
    fn parse_mem_available_reads_kb_as_bytes() {
        let meminfo = "MemTotal:       16330144 kB\nMemFree:  1000 kB\nMemAvailable:    8290816 kB\nBuffers: 5 kB\n";
        assert_eq!(
            super::parse_mem_available_bytes(meminfo),
            Some(8_290_816 * 1024)
        );
    }

    #[test]
    fn parse_mem_available_absent_is_none() {
        assert_eq!(super::parse_mem_available_bytes("MemTotal: 100 kB\n"), None);
        assert_eq!(super::parse_mem_available_bytes(""), None);
    }

    #[test]
    fn size_fits_caps_only_when_over_budget() {
        // Fits: estimate (20_480_000) under budget.
        assert!(super::size_fits(10_000, Some(1_000_000_000)));
        // Does not fit: 1M estimate (2_048_000_000) over a 1 GiB budget.
        assert!(!super::size_fits(1_000_000, Some(1_073_741_824)));
        // Unknown memory never caps.
        assert!(super::size_fits(1_000_000, None));
    }
}
