//! Allocation pin for the declared-column stat stamp (issue #1135).
//!
//! The stamp runs once per record on the bulk load path. Before #1135 it built
//! a `BTreeMap` per record, rescanned the record's occurrence list once per
//! tracked slot, cloned every winner's name into a `String`, and encoded every
//! overflow occurrence to canonical bytes: 492 allocations, 14 reallocations
//! and 71_935 bytes per record on the shape below. Now it writes into scratch
//! that lives on the writer and is cleared rather than rebuilt, so a
//! steady-state record allocates nothing at all, and this test pins that at
//! exactly zero rather than at a threshold.
//!
//! This file contains EXACTLY ONE test on purpose: it installs the instrumented
//! global allocator, which counts every allocation in the process, so a second
//! test running concurrently in the same binary would pollute the count
//! (`cargo test` runs a binary's tests on threads). The workspace forbids
//! `unsafe_code`, so a hand-rolled counting allocator will not compile;
//! `stats_alloc`'s instrumented allocator is used instead, as in
//! `columnar_writer_memory.rs`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

use ravel_logseg::writer::stamp_probe::StampProbe;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[path = "../benches/common/stamp_shape.rs"]
mod stamp_shape;

use stamp_shape::{DECLARED_COLUMNS, reference_setup, shape};

/// Records stamped under the counter.
const MEASURED: usize = 1_000;
/// Records stamped first, so the scratch buffers have reached the size this
/// shape needs before the counter starts. Growing a reusable buffer is a
/// one-off cost, not a per-record one.
const WARMUP: usize = 10;

#[test]
fn stamp_path_allocates_nothing_per_record() {
    let shape = shape(WARMUP + MEASURED);
    let reference = reference_setup(&shape);
    let mut probe = StampProbe::new(
        &shape.columns,
        &shape.indexed_fields,
        &shape.stream_attrs,
        &shape.records,
    );
    assert_eq!(probe.len(), WARMUP + MEASURED);
    assert_eq!(probe.tracked_names().len(), DECLARED_COLUMNS + 2);

    // The pin is only worth anything if the path it pins still stamps what the
    // pre-#1135 path stamped, so check the first record's output before
    // measuring. This part allocates freely; it is outside the counted region.
    let rec = probe.record(0).expect("record 0");
    let (want_indexed, want_stat) = reference.stamp(rec);
    probe.stamp(0);
    let (got_indexed, got_stat) = probe.outputs();
    assert_eq!(got_indexed, want_indexed.as_slice());
    assert_eq!(got_stat, want_stat.as_slice());
    // Not a vacuous comparison: this shape stamps a term for every fourth
    // declared column and a winner for every numeric one.
    assert_eq!(got_indexed.len(), 26);
    // One short of every tracked name: `col_042`'s winner is its overflow
    // occurrence, whose type has no declared column, so it contributes no
    // NumStat entry (and the row counts as a null in that column).
    assert_eq!(got_stat.len(), DECLARED_COLUMNS + 1);

    for i in 0..WARMUP {
        probe.stamp(i);
    }

    let region = Region::new(GLOBAL);
    for i in WARMUP..WARMUP + MEASURED {
        probe.stamp(i);
    }
    let stats = region.change();

    assert_eq!(
        stats.allocations, 0,
        "stamping {MEASURED} records allocated {} times ({} bytes)",
        stats.allocations, stats.bytes_allocated
    );
    assert_eq!(stats.reallocations, 0);
}
