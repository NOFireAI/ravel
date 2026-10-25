//! Peak live heap and allocation count of the metrics compaction grouping
//! stage (issue #3254), one untimed run per arm on the `large` geometry.
//!
//! The workspace forbids `unsafe_code`, so allocation counting goes through
//! `stats_alloc`'s instrumented global allocator, which counts every call
//! with no off switch. That is why this is a separate binary from the
//! criterion bench `compaction_grouping`: its timings run on the plain
//! system allocator.
//!
//! Live bytes are sampled after every insert, after arm C's key sort, and
//! after every consumed group, so `peak_live_sampled` is the largest live
//! figure seen between operations. `peak_live_bound` adds, per operation, the
//! bytes it allocated and its net realloc growth to the live figure before
//! it, which bounds a transient inside one operation (a hash table resize
//! holds the old and the new table at once; a doubling `Vec` realloc may
//! hold both blocks). Figures are requested sizes, before allocator
//! rounding, and exclude the fixture, which is built before measuring.
//!
//! Usage: `cargo bench -p ravel-maintain --bench compaction_grouping_alloc`,
//! optionally followed by `-- <geometry>` (default `large`). Exits non-zero
//! when an arm's checksum disagrees with arm A, an arm leaves bytes live
//! after it returns, or the sample count is not exactly the number of
//! operations the geometry implies.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::process::ExitCode;

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[path = "common/grouping.rs"]
mod grouping;

use grouping::{arm_label, distinct_series, fixture, geometry, run_arm};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn live(s: &Stats) -> isize {
    s.bytes_allocated as isize - s.bytes_deallocated as isize
}

struct Measured {
    checksum: u64,
    samples: usize,
    peak_live_sampled: isize,
    peak_live_bound: isize,
    allocations: usize,
    reallocations: usize,
    live_after: isize,
}

fn measure(arm: char, inputs: &[Vec<ravel_maintain::read::SeriesPlan>]) -> Measured {
    let region = Region::new(GLOBAL);
    let mut prev = region.change();
    let mut samples = 0usize;
    let mut peak_live_sampled = 0isize;
    let mut peak_live_bound = 0isize;
    let mut tick = || {
        let now = region.change();
        let allocated_in_op = now.bytes_allocated as isize - prev.bytes_allocated as isize;
        let realloc_growth_in_op = (now.bytes_reallocated - prev.bytes_reallocated).max(0);
        peak_live_bound = peak_live_bound.max(live(&prev) + allocated_in_op + realloc_growth_in_op);
        peak_live_sampled = peak_live_sampled.max(live(&now));
        prev = now;
        samples += 1;
    };
    let checksum = run_arm(arm, inputs, &mut tick);
    let end = region.change();
    Measured {
        checksum,
        samples,
        peak_live_sampled,
        peak_live_bound,
        allocations: end.allocations,
        reallocations: end.reallocations,
        live_after: live(&end),
    }
}

fn main() -> ExitCode {
    let name = std::env::args()
        .skip(1)
        .find(|a| !a.starts_with('-'))
        .unwrap_or_else(|| "large".to_string());
    // Under `cargo test --all-targets` the binary runs without `--bench`; a
    // small geometry keeps that smoke run fast while still checking the arms.
    let name = if std::env::args().any(|a| a == "--bench") {
        name
    } else {
        "small".to_string()
    };
    let g = geometry(&name);
    let inputs = fixture(g);
    let groups = distinct_series(g);
    let contributions = g.inputs * g.series;
    println!(
        "compaction_grouping_alloc geometry={} inputs={} series_per_input={} \
         repeated_pct={} contributions={contributions} groups={groups} seed={:#x}",
        g.name,
        g.inputs,
        g.series,
        g.repeated_pct,
        grouping::SEED
    );

    let mut failed = false;
    let mut reference = None;
    for arm in ['A', 'B', 'C'] {
        let m = measure(arm, &inputs);
        let label = arm_label(arm).expect("fixed arm letters");
        println!(
            "arm={label} peak_live_sampled={} peak_live_bound={} allocations={} \
             reallocations={} live_after={} samples={} checksum={:#x}",
            m.peak_live_sampled,
            m.peak_live_bound,
            m.allocations,
            m.reallocations,
            m.live_after,
            m.samples,
            m.checksum
        );
        // One tick per insert and per group, plus arm C's tick after its sort.
        let expected_samples = contributions + groups + usize::from(arm == 'C');
        if m.samples != expected_samples {
            eprintln!(
                "FAIL arm={label}: {} samples, expected {expected_samples}",
                m.samples
            );
            failed = true;
        }
        if m.live_after != 0 {
            eprintln!(
                "FAIL arm={label}: {} bytes still live after the arm returned",
                m.live_after
            );
            failed = true;
        }
        match reference {
            None => reference = Some(m.checksum),
            Some(r) if r != m.checksum => {
                eprintln!("FAIL arm={label}: checksum differs from arm A");
                failed = true;
            }
            Some(_) => {}
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
