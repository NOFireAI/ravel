//! Deterministic `#[ignore]`d measurement probes for the encode benchmark's
//! timed region. Not part of the gate run; execute with `--ignored`:
//!   cargo test -p ravel-bench --test encode_bench_clone_exclusion -- --ignored --nocapture
//!
//! benches/segment_encode.rs clones the fixture in an iter_batched setup
//! closure so the reported time is encode-only. These probes quantify the
//! clone that the setup closure keeps out of the timed region
//! (`clone_is_in_encode_timed_region`) and confirm that excluding it lowers
//! the measured time (`iter_batched_excludes_clone`), i.e. why the setup
//! closure exists.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Instant;

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{bench_bounds, bench_identity};
use ravel_segment::{SegmentWriter, SeriesInput};

const TOTAL_SAMPLES: usize = 200_000;

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

/// a12-F01: before the fix, the `segment_encode` criterion bench ran
/// `clone_inputs` inside its timed `b.iter` closure, so the reported encode
/// throughput included the fixture clone. This quantifies that the clone is a
/// non-trivial fraction of the writer's own time at high cardinality (where the
/// clone deep-copies one `LabelSet` per series) — the contamination the fix
/// removed from the timed region, which was confounding the per-series-overhead
/// comparison the bench isolates. The bench now uses `b.iter_batched` with the
/// clone in the setup closure (as src/bin/segment_alloc_profile.rs does).
/// Magnitude is host-sensitive; the structural point is not.
#[test]
#[ignore = "measurement probe; run with --ignored"]
fn clone_is_in_encode_timed_region() {
    // 100k series / 200k samples: the same high-cardinality case as the
    // `segment_encode/100000_series` bench point.
    let inputs = series_inputs(100_000);

    // Warm caches / allocator, then take a best-of-a-few for each phase to
    // damp host noise without depending on absolute numbers.
    let mut clone_ns = u128::MAX;
    let mut write_ns = u128::MAX;
    for _ in 0..3 {
        let start = Instant::now();
        let cloned = clone_inputs(&inputs);
        clone_ns = clone_ns.min(start.elapsed().as_nanos());
        std::hint::black_box(&cloned);

        let series = clone_inputs(&inputs);
        let start = Instant::now();
        let written = SegmentWriter::write(series, bench_identity(), bench_bounds())
            .expect("encode bench segment");
        write_ns = write_ns.min(start.elapsed().as_nanos());
        std::hint::black_box(&written);
    }

    let clone_fraction = clone_ns as f64 / (clone_ns + write_ns) as f64;
    println!(
        "clone {:.2} ms, write {:.2} ms, clone is {:.1}% of the criterion timed region",
        clone_ns as f64 / 1e6,
        write_ns as f64 / 1e6,
        clone_fraction * 100.0
    );

    // The defect: the clone is charged to "encode throughput". Assert it is a
    // measurable fraction rather than rounding noise. The threshold is kept
    // conservative (>= 0.5%) so it holds under a debug `cargo test` build,
    // where the writer is far slower and the clone's relative share shrinks;
    // in a `--release` run on aarch64 it was ~12-13%, which the printed line
    // above shows directly.
    assert!(
        clone_fraction >= 0.005,
        "clone should be a measurable fraction of the timed region, got {:.2}%",
        clone_fraction * 100.0
    );
}

/// Companion: excluding the clone from the measured region reports the
/// writer's own time, which is meaningfully lower than clone+write.
#[test]
#[ignore = "measurement probe; run with --ignored"]
fn iter_batched_excludes_clone() {
    let inputs = series_inputs(100_000);

    // What the pre-fix bench timed: clone + write, per iteration.
    let mut current_ns = u128::MAX;
    for _ in 0..3 {
        let start = Instant::now();
        let series = clone_inputs(&inputs);
        let written =
            SegmentWriter::write(series, bench_identity(), bench_bounds()).expect("encode");
        current_ns = current_ns.min(start.elapsed().as_nanos());
        std::hint::black_box(&written);
    }

    // What iter_batched now times: write only (clone in the setup closure,
    // untimed).
    let mut fixed_ns = u128::MAX;
    for _ in 0..3 {
        let series = clone_inputs(&inputs); // setup, untimed
        let start = Instant::now();
        let written =
            SegmentWriter::write(series, bench_identity(), bench_bounds()).expect("encode");
        fixed_ns = fixed_ns.min(start.elapsed().as_nanos());
        std::hint::black_box(&written);
    }

    println!(
        "a12-F01: current(clone+write) {:.2} ms vs fixed(write-only) {:.2} ms",
        current_ns as f64 / 1e6,
        fixed_ns as f64 / 1e6
    );
    assert!(
        fixed_ns < current_ns,
        "excluding the clone must lower the measured time"
    );
}
