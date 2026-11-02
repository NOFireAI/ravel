//! Allocation pin for the per-row columnar occurrence buffer on the flush
//! path (issue #3643, ADR-3492 2026-10-31 baseline amendment, cut (b)).
//!
//! `writer.rs`'s block loop used to start every row's in-budget-column buffer
//! from `Vec::new()`, so a row with many present columns regrew it from
//! scratch every time. It is now seeded with the previous row's final
//! length, which is free on a block of same-shaped rows and still bounded on
//! a block of differently-shaped ones -- unlike sizing it from the batch's
//! total column count, which would reserve columns-times-rows on a wide
//! sparse batch (the comment above the loop in `writer.rs` bounds the
//! block's peak at one copy of one block's cells, #682).
//!
//! This file contains EXACTLY ONE test on purpose: it installs the
//! instrumented global allocator, which counts every allocation in the
//! process, so a second test running concurrently in the same binary (`cargo
//! test` runs a binary's tests on threads) would pollute the count. As in
//! `stamp_alloc_pin.rs`, `stats_alloc`'s instrumented allocator stands in for
//! a hand-rolled one, which `unsafe_code` forbids.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

use ravel_logseg::{
    AttrValue, ColumnarLogBatch, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter,
    stream_attrs_bytes,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0u8; 16],
        shard: 0,
        writer_id: [0u8; 16],
        writer_epoch: 0,
        writer_seq: 0,
    }
}

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

// --- arm 1: uniform rows ----------------------------------------------------

/// Dynamic columns every uniform record carries: present on every row, so a
/// correctly-seeded buffer needs exactly one allocation per row and zero
/// reallocations past the block's first row.
const UNIFORM_STRS: usize = 10;
const UNIFORM_INTS: usize = 10;

fn uniform_record(i: u64, stream_attrs: &[u8]) -> LogRecord {
    let mut attrs = Vec::with_capacity(UNIFORM_STRS + UNIFORM_INTS);
    for c in 0..UNIFORM_STRS {
        attrs.push((format!("s{c}"), AttrValue::Str(format!("v{c}"))));
    }
    for c in 0..UNIFORM_INTS {
        attrs.push((format!("n{c}"), AttrValue::I64((i * 7 + c as u64) as i64)));
    }
    LogRecord {
        stream_id: sid(0),
        stream_attrs: stream_attrs.to_vec(),
        ts_ns: 1_700_000_000_000_000_000 + i as i64 * 1_000,
        observed_ts_ns: 1_700_000_000_000_000_000 + i as i64 * 1_000,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "a uniform row body long enough to look like a real log line".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// One flush of `rows` uniform records through the columnar path, under one
/// block (`rows` is always far below
/// `RlogConfig::default().block_target_records`). Only `finish`, where the
/// block loop runs, is under the counter: building the records and the batch
/// is setup, not the flush.
fn flush_uniform(rows: u64) -> stats_alloc::Stats {
    let stream_attrs = stream_attrs_bytes(
        &[("service.name".to_string(), AttrValue::Str("svc".to_string()))],
        "scope",
        "1",
        &[],
    );
    let records: Vec<LogRecord> = (0..rows).map(|i| uniform_record(i, &stream_attrs)).collect();
    let batch = ColumnarLogBatch::try_from_records(&records).expect("batch");
    let mut w = RlogWriter::new(RlogConfig::default(), identity());
    w.push_columnar(batch).expect("push_columnar");
    let region = Region::new(GLOBAL);
    w.finish().expect("finish");
    region.change()
}

/// Reallocations per additional uniform row, taken between two row counts at
/// one block size so that the writer's and batch's own one-time setup
/// (independent of row count) cancels out of the difference. Pinned to the
/// exact figure measured on this change: a per-row buffer that regrows from
/// `Vec::new()` reallocates several times per row (one per capacity
/// doubling), which this bound catches.
const UNIFORM_ROWS_SMALL: u64 = 64;
const UNIFORM_ROWS_LARGE: u64 = 256;
/// Upper bound on `reallocations(LARGE) - reallocations(SMALL)`, the exact
/// figure measured on this change (`cargo test -p ravel-logseg --test
/// flush_regrowth_alloc_pin -- --nocapture`). The remaining reallocations are
/// other per-row structures the flush grows (string interning, NumStat,
/// bloom) that this change does not touch; restoring `Vec::new()` for the
/// per-row buffer this change fixed raises the figure to 1023.
const MAX_UNIFORM_REALLOC_DELTA: isize = 447;

#[test]
fn flush_regrowth_alloc_pin() {
    // Warm up allocator bookkeeping (lazy statics, thread-local caches) before
    // either measured run, so neither pays a first-use cost the other does
    // not.
    let _ = flush_uniform(8);

    let small = flush_uniform(UNIFORM_ROWS_SMALL);
    let large = flush_uniform(UNIFORM_ROWS_LARGE);
    let delta_reallocs = large.reallocations as isize - small.reallocations as isize;
    eprintln!(
        "uniform rows: reallocations(small={UNIFORM_ROWS_SMALL})={}, \
         reallocations(large={UNIFORM_ROWS_LARGE})={}, delta={delta_reallocs}",
        small.reallocations, large.reallocations
    );
    assert!(
        delta_reallocs <= MAX_UNIFORM_REALLOC_DELTA,
        "flushing {} more uniform rows cost {delta_reallocs} more reallocations, \
         want at most {MAX_UNIFORM_REALLOC_DELTA}: the per-row buffer is regrowing \
         from scratch instead of reusing the previous row's length as a capacity hint",
        UNIFORM_ROWS_LARGE - UNIFORM_ROWS_SMALL
    );

    // --- arm 2: wide sparse batch -------------------------------------------
    let sparse = flush_sparse(SPARSE_ROWS);
    eprintln!(
        "wide sparse batch: bytes_allocated={}, base commit figure={BASE_SPARSE_BYTES}",
        sparse.bytes_allocated
    );
    let max_sparse_bytes = BASE_SPARSE_BYTES + BASE_SPARSE_BYTES / 10;
    assert!(
        sparse.bytes_allocated <= max_sparse_bytes,
        "wide sparse flush allocated {} bytes, more than 10% over the {BASE_SPARSE_BYTES} \
         bytes recorded on the base commit ({max_sparse_bytes} allowed): the per-row buffer \
         looks like it is sized from the batch's total column count instead of the \
         previous row's length",
        sparse.bytes_allocated
    );
}

// --- arm 2: wide sparse batch ------------------------------------------------

/// Distinct dynamic columns the batch spreads its attributes over.
const SPARSE_COLUMNS: u64 = 2_000;
/// Columns present on each row.
const SPARSE_PRESENT: u64 = 3;
/// Rows in the sparse flush: enough that the batch's union of attribute names
/// reaches [`SPARSE_COLUMNS`].
const SPARSE_ROWS: u64 = 4_000;

/// Bytes `flush_sparse(SPARSE_ROWS)` allocated on the base commit (before
/// #3643, `cols` in `writer.rs` starting from `Vec::new()`), measured the
/// same way as the assertion below (`cargo test -p ravel-logseg --test
/// flush_regrowth_alloc_pin -- --nocapture`). Most of this total comes from
/// per-column block bookkeeping unrelated to the two vectors this change
/// touches (every one of `SPARSE_COLUMNS` declared columns does O(1) work
/// per row whether or not it is present on that row), so this bound exists
/// to catch a sizing regression big enough to show through that unrelated
/// cost, not to isolate the two target vectors' own bytes.
const BASE_SPARSE_BYTES: usize = 662_649_397;

fn sparse_record(i: u64, stream_attrs: &[u8]) -> LogRecord {
    let mut attrs = Vec::with_capacity(SPARSE_PRESENT as usize);
    for p in 0..SPARSE_PRESENT {
        let col = (i * SPARSE_PRESENT + p) % SPARSE_COLUMNS;
        attrs.push((format!("wide_{col:04}"), AttrValue::I64(i as i64)));
    }
    LogRecord {
        stream_id: sid(1),
        stream_attrs: stream_attrs.to_vec(),
        ts_ns: 1_700_000_000_000_000_000 + i as i64 * 1_000,
        observed_ts_ns: 1_700_000_000_000_000_000 + i as i64 * 1_000,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "a sparse row body long enough to look like a real log line".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn flush_sparse(rows: u64) -> stats_alloc::Stats {
    let stream_attrs = stream_attrs_bytes(
        &[("service.name".to_string(), AttrValue::Str("svc".to_string()))],
        "scope",
        "1",
        &[],
    );
    let records: Vec<LogRecord> = (0..rows).map(|i| sparse_record(i, &stream_attrs)).collect();
    let batch = ColumnarLogBatch::try_from_records(&records).expect("batch");
    assert!(
        (batch.dyn_columns.len() as u64) >= SPARSE_COLUMNS - 1,
        "the generated batch only reached {} distinct columns, want close to {SPARSE_COLUMNS}",
        batch.dyn_columns.len()
    );
    let cfg = RlogConfig {
        max_dynamic_columns: SPARSE_COLUMNS as usize + 64,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    w.push_columnar(batch).expect("push_columnar");
    let region = Region::new(GLOBAL);
    w.finish().expect("finish");
    region.change()
}
