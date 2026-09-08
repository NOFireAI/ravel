//! Measures the real encoded RSEG v3 size of the representative
//! native-histogram shape as a genuine encoded-byte number.
//!
//! Builds one segment over the histogram workload via
//! `SegmentWriter::write_histograms` and reports, per record (one histogram
//! sample per series in the representative shape):
//!   - total object bytes / record (whole segment: catalog, labels, footer,
//!     all pages), and
//!   - HIST_PAGES section bytes / record (just the histogram value pages).
//!
//! Not a criterion bench (it measures size, not time); run it directly to
//! obtain the encoded-size numbers.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_histograms};
use ravel_bench::segment_support::{bench_bounds, bench_identity};
use ravel_segment::{ReaderLimits, SegmentWriter, open_from_full};

/// HIST_PAGES section kind (docs/segment-format.md RSEG v3 amendment;
/// `ravel_segment::format::section_kind::HIST_PAGES`). Named here like the
/// other section kinds `segment_support` pins, so this bin does not depend on
/// the `format` module's visibility.
const HIST_PAGES: u32 = 7;

fn measure(series_count: usize) {
    let config = WorkloadConfig {
        series_count,
        cardinality: CardinalityProfile::many_small(series_count),
        ..Default::default()
    };
    let inputs = generate_histograms(&config).expect("generate histogram workload");
    // One histogram sample per series in the section 8 shape, so record count
    // equals series count.
    let records = inputs.len() as u64;
    let written =
        SegmentWriter::write_histograms(inputs, bench_identity(), bench_bounds()).expect("encode");

    let total_bytes = written.bytes.len() as u64;
    let loc =
        open_from_full(written.bytes.as_ref(), ReaderLimits::default()).expect("open segment");
    let hist_bytes: u64 = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == HIST_PAGES)
        .map(|s| s.len)
        .expect("HIST_PAGES section present");

    let total_per_record = total_bytes as f64 / records as f64;
    let hist_per_record = hist_bytes as f64 / records as f64;

    println!("series_count       : {series_count}");
    println!("records            : {records}");
    println!("total object bytes : {total_bytes}");
    println!("HIST_PAGES bytes   : {hist_bytes}");
    println!("total bytes/record : {total_per_record:.2}");
    println!("HIST bytes/record  : {hist_per_record:.2}");
    println!();
}

fn main() {
    println!("RSEG v3 native-histogram encoded size (representative shape)");
    println!("30 buckets/side, 4 spans/side, integer counts, has_sum, 1 sample/series");
    println!();
    for &series_count in &[100usize, 10_000] {
        measure(series_count);
    }
}
