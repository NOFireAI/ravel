//! Per-record cost of the declared-column stat stamp, before and after #1135,
//! on the 104-declared-column shape in `common/stamp_shape.rs`.
//!
//! Both sides start from the same pre-split occurrence lists and run only the
//! stamp, not the record encode around it. The old path is the reference copy
//! in `common/stamp_shape.rs`; the new one is `StampScratch::finish` through
//! `writer::stamp_probe`.
//!
//! This bench is not a criterion harness even though the crate has criterion:
//! criterion reports its measurement to its own output directory rather than
//! to the caller, and this bench has to fail when the figure it prints leaves
//! its band. It times the two paths itself, prints per-record nanoseconds for
//! each, and exits non-zero unless the new path is at least
//! [`REQUIRED_SPEEDUP`] times faster on this shape.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::process::ExitCode;
use std::time::Instant;

use ravel_logseg::writer::stamp_probe::StampProbe;

#[path = "common/stamp_shape.rs"]
mod stamp_shape;

use stamp_shape::{DECLARED_COLUMNS, reference_setup, shape};

/// Records per timed round.
const ROUND: usize = 1_000;
/// Timed rounds per path. The fastest round is reported: it is the round least
/// disturbed by whatever else the host was doing.
const ROUNDS: usize = 20;
/// Rounds run before timing starts, so neither path pays for a cold cache or
/// for growing its reusable buffers inside a timed round.
const WARMUP_ROUNDS: usize = 3;
/// The band this bench asserts, from issue #1135: the one-pass slot-keyed
/// stamp must cost at most half of what the name-keyed rescan cost per record
/// on this shape.
const REQUIRED_SPEEDUP: f64 = 2.0;

fn main() -> ExitCode {
    let shape = shape(ROUND);
    let reference = reference_setup(&shape);
    let mut probe = StampProbe::new(
        &shape.columns,
        &shape.indexed_fields,
        &shape.stream_attrs,
        &shape.records,
    );
    assert_eq!(probe.len(), ROUND);
    assert_eq!(probe.tracked_names().len(), DECLARED_COLUMNS + 2);

    // Same inputs, same answer: a speedup on a path that stopped producing the
    // old output would not be a speedup.
    for i in 0..probe.len() {
        let rec = probe.record(i).expect("record in range");
        let (want_indexed, want_stat) = reference.stamp(rec);
        probe.stamp(i);
        let (got_indexed, got_stat) = probe.outputs();
        assert_eq!(
            got_indexed,
            want_indexed.as_slice(),
            "postings terms, record {i}"
        );
        assert_eq!(
            got_stat,
            want_stat.as_slice(),
            "numstat winners, record {i}"
        );
    }

    let before = time_ns(|| {
        for i in 0..ROUND {
            let rec = probe.record(i).expect("record in range");
            black_box(reference.stamp(rec));
        }
    });
    let after = time_ns(|| {
        for i in 0..ROUND {
            probe.stamp(i);
            black_box(probe.outputs());
        }
    });

    let speedup = before / after;
    println!("shape: {DECLARED_COLUMNS} declared columns, {ROUND} records, fastest of {ROUNDS}");
    println!("before (name-keyed rescan):  {before:.1} ns/record");
    println!("after  (slot-keyed one pass): {after:.1} ns/record");
    println!("speedup: {speedup:.2}x (required at least {REQUIRED_SPEEDUP:.2}x)");

    if speedup < REQUIRED_SPEEDUP {
        eprintln!(
            "stamp_declared: {speedup:.2}x is below the required {REQUIRED_SPEEDUP:.2}x on this shape"
        );
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// Fastest per-record nanoseconds over [`ROUNDS`] rounds of [`ROUND`] records.
fn time_ns(mut round: impl FnMut()) -> f64 {
    for _ in 0..WARMUP_ROUNDS {
        round();
    }
    let mut best = f64::INFINITY;
    for _ in 0..ROUNDS {
        let t0 = Instant::now();
        round();
        let ns = t0.elapsed().as_nanos() as f64 / ROUND as f64;
        best = best.min(ns);
    }
    best
}
