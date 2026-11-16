//! Allocation + wall-clock profile for `SegmentWriter::write`. Criterion's
//! `segment_encode` bench reports throughput but not allocator traffic, and
//! the bench harness must not change; this is a separate, report-only bin
//! using `stats_alloc`'s instrumented global allocator to capture allocation
//! counts and bytes alongside wall time.
//! `stats_alloc`'s `unsafe impl GlobalAlloc` lives entirely inside that
//! crate; no `unsafe` appears in this file.
//!
//! Not wired into `cargo bench`; run directly:
//!   cargo run -p ravel-bench --release --bin segment_alloc_profile
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Instant;

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{bench_bounds, bench_identity};
use ravel_segment::{SegmentWriter, SeriesInput};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const TOTAL_SAMPLES: usize = 200_000;
const ITERATIONS: usize = 20;

fn series_inputs(series_count: usize) -> Vec<SeriesInput> {
    let samples_per_series = (TOTAL_SAMPLES / series_count).max(1);
    let config = WorkloadConfig {
        series_count,
        samples_per_series,
        cardinality: CardinalityProfile::many_small(series_count),
        ..Default::default()
    };
    generate_raw(&config)
        .expect("generate workload")
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect()
}

fn clone_inputs(inputs: &[SeriesInput]) -> Vec<SeriesInput> {
    inputs
        .iter()
        .map(|s| SeriesInput {
            series_id: s.series_id,
            labels: s.labels.clone(),
            samples: s.samples.clone(),
        })
        .collect()
}

fn profile_one(series_count: usize) {
    let inputs = series_inputs(series_count);

    let mut total_nanos: u128 = 0;
    let mut total_allocs: usize = 0;
    let mut total_bytes: usize = 0;

    for _ in 0..ITERATIONS {
        // Cloning the fixture is outside the measured region so only the
        // writer's own allocations count.
        let series = clone_inputs(&inputs);
        let mut region = Region::new(GLOBAL);
        let start = Instant::now();
        let written =
            SegmentWriter::write(series, bench_identity(), bench_bounds()).expect("encode");
        let elapsed = start.elapsed();
        let stats = region.change_and_reset();
        std::hint::black_box(&written);
        total_nanos += elapsed.as_nanos();
        total_allocs += stats.allocations;
        total_bytes += stats.bytes_allocated;
    }

    let avg_nanos = total_nanos / ITERATIONS as u128;
    let avg_allocs = total_allocs / ITERATIONS;
    let avg_bytes = total_bytes / ITERATIONS;
    println!(
        "{series_count:>7} series | avg {avg_nanos:>10} ns | avg {avg_allocs:>6} allocs | avg {avg_bytes:>10} bytes allocated"
    );
}

fn main() {
    println!(
        "SegmentWriter::write allocation profile ({ITERATIONS} iterations/case, {TOTAL_SAMPLES} total samples)"
    );
    for &series_count in &[100usize, 10_000, 100_000] {
        profile_one(series_count);
    }
}
