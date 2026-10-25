//! Peak live heap and allocation count of the RLOG writer's POSTINGS
//! accumulation (issue #3254, candidate 2), one untimed run per arm on the
//! `large` postings geometry.
//!
//! Same method as `compaction_grouping_alloc`: `stats_alloc`'s instrumented
//! global allocator (the workspace forbids `unsafe_code`), live bytes sampled
//! after every insert and after each field's encoding-boundary conversion, and
//! a per-operation transient bound, so the bound charges at most one field's
//! conversion at a time. Figures are requested sizes and exclude the
//! fixture, which is built before measuring.
//!
//! Usage: `cargo bench -p ravel-maintain --bench rlog_postings_alloc`,
//! optionally followed by `-- <geometry>` (default `large`). Exits non-zero
//! when an arm's checksum disagrees with arm A, an arm leaves bytes live
//! after it returns, the sample count is not exactly one per insert plus one
//! per field, or, on `large`, arm A's peak live bytes fall outside the
//! pre-registered band.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::process::ExitCode;

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[path = "common/postings.rs"]
mod postings;

use postings::{INDEXED, Value, arm_label, fixture, geometry, run_arm};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Arm A's pre-registered peak live band on `large`, in bytes (0.3 to 3 MB; the
/// postings pre-registration in docs/internal/bbtree-ordered-maps-3254.md).
const A_LARGE_PEAK_BAND: std::ops::RangeInclusive<isize> = 300_000..=3_000_000;

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

fn measure(arm: char, rows: &[Vec<(u32, Value)>]) -> Measured {
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
    let checksum = run_arm(arm, rows, &mut tick);
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
    let rows = fixture(g);
    let inserts = rows.len() * INDEXED.len();
    println!(
        "rlog_postings_alloc geometry={} blocks={} rows={} inserts={inserts} \
         service_terms={} namespace_terms={} seed={:#x}",
        g.name,
        g.blocks,
        rows.len(),
        g.service_terms,
        g.namespace_terms,
        postings::SEED
    );

    let mut failed = false;
    let mut reference = None;
    let mut a_peaks = Vec::new();
    for arm in ['A', 'C'] {
        let m = measure(arm, &rows);
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
        // No geometry reaches the distinct cap, so every insert and every
        // field's conversion ticks.
        let expected_samples = inserts + INDEXED.len();
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
        if arm == 'A' {
            a_peaks.push(m.peak_live_sampled);
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
    // The band was registered for `large` only; on that geometry A's peak
    // must be present exactly once and inside it.
    if g.name == "large" {
        let lo = A_LARGE_PEAK_BAND.start();
        let hi = A_LARGE_PEAK_BAND.end();
        match a_peaks.as_slice() {
            [peak] => {
                let verdict = if A_LARGE_PEAK_BAND.contains(peak) {
                    "PASS"
                } else {
                    failed = true;
                    "MISS"
                };
                println!(
                    "band arm=A geometry=large peak_live_sampled={peak} band={lo}..={hi} {verdict}"
                );
            }
            other => {
                println!(
                    "band arm=A geometry=large peak_live_sampled present {} times, expected 1 MISS",
                    other.len()
                );
                failed = true;
            }
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
