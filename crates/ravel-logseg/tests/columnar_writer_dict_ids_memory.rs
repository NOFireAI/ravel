//! Peak-memory bound on the row-group dictionary decision's id vectors
//! (issue #2144).
//!
//! Until a row group flushes, the writer holds a `u32` id per present value of
//! every string column still in the dictionary decision. Those ids are charged
//! to the same budget as the distinct value bytes (`block_max_bytes`), so a
//! group of many fully-present, low-cardinality string columns drops columns
//! from the decision once the budget is spent instead of holding every
//! column's ids for the whole group.
//!
//! This file contains EXACTLY ONE test on purpose: it installs the instrumented
//! global allocator and samples peak live bytes from a sidecar thread, so a
//! second test allocating in the same process would pollute the figure. See
//! `columnar_writer_memory.rs` for the measurement method.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use ravel_logseg::footer::{kind, open};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrValue, Bitmap, ColumnarLogBatch, DynColumn, FieldType, LogStreamId, ObjectIdentity,
    RlogConfig, RlogWriter, VarBytes, read_section, stream_attrs_bytes,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Fully-present string columns, each over sixteen 3-byte values.
const COLUMNS: usize = 128;
const BLOCK_ROWS: usize = 1024;
const GROUP_BLOCKS: usize = 32;
const ROWS: usize = BLOCK_ROWS * GROUP_BLOCKS;
/// `block_max_bytes`, and so the dictionary budget. A row's block estimate is
/// 40 + 8 (body) + 4 (severity_text) + 128 * (3 + 2) = 692 bytes, so a
/// 1024-row block estimates 708,608 bytes and blocks stay record-governed.
const BUDGET: usize = 1 << 20;
/// One block's ids across every string column: 1024 * 128 * 4 bytes. The
/// budget is checked after a block is folded, so it can be passed by this much.
const BLOCK_IDS: usize = BLOCK_ROWS * COLUMNS * 4;
/// The group's ids across every string column had none been dropped: 16 MiB,
/// sixteen times the budget.
const GROUP_IDS: usize = ROWS * COLUMNS * 4;

fn batch() -> ColumnarLogBatch {
    let mut batch = ColumnarLogBatch::new();
    batch.num_rows = ROWS;
    let mut body = VarBytes::new();
    let mut sev = VarBytes::new();
    for row in 0..ROWS {
        batch.ts_ns.push(row as i64);
        batch.observed_ts_ns.push(row as i64);
        batch.severity_num.push(9);
        batch.flags.push(0);
        body.push(b"log line");
        sev.push(b"INFO");
        batch.trace_id_validity.push(false);
        batch.span_id_validity.push(false);
        batch.stream_refs.push(0);
    }
    batch.body = body;
    batch.severity_text = sev;
    batch.stream_ids.push(LogStreamId([7u8; 16]));
    batch.stream_attrs.push(stream_attrs_bytes(
        &[("service.name".into(), AttrValue::Str("svc".into()))],
        "scope",
        "1.0",
        &[],
    ));
    for j in 0..COLUMNS {
        let mut validity = Bitmap::new();
        let mut cells = Vec::with_capacity(ROWS);
        for row in 0..ROWS {
            validity.push(true);
            cells.push(AttrValue::Str(format!("s{:02}", (row * 7 + j) % 16)));
        }
        batch.dyn_columns.push(DynColumn {
            name: format!("attr_{j:03}"),
            field_type: FieldType::Str,
            cells,
            validity,
        });
    }
    batch.residual_attrs = vec![Vec::new(); ROWS];
    batch
}

fn cfg(group_target_blocks: usize) -> RlogConfig {
    RlogConfig {
        block_target_records: BLOCK_ROWS,
        block_max_bytes: BUDGET,
        group_target_blocks,
        ..RlogConfig::default()
    }
}

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Runs `f`, sampling live allocated bytes from a spinning sidecar thread, and
/// returns `f`'s result plus the peak live-byte delta observed during the call.
fn measure_peak<R, F: FnOnce() -> R>(f: F) -> (R, usize) {
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicUsize::new(0));
    let base = INSTRUMENTED_SYSTEM.stats();
    let s2 = stop.clone();
    let p2 = peak.clone();
    let handle = thread::spawn(move || {
        loop {
            let cur = INSTRUMENTED_SYSTEM.stats();
            let live = (cur.bytes_allocated as i64 - base.bytes_allocated as i64)
                - (cur.bytes_deallocated as i64 - base.bytes_deallocated as i64);
            if live > 0 {
                p2.fetch_max(live as usize, Ordering::Relaxed);
            }
            if s2.load(Ordering::Relaxed) {
                break;
            }
            std::hint::spin_loop();
        }
    });
    let r = f();
    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();
    (r, peak.load(Ordering::Relaxed))
}

/// The object's row groups' block counts and how many chunks store a row-group
/// dictionary.
fn group_shape(object: &[u8]) -> (Vec<u32>, usize) {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open");
    let raw = read_section(object, ftr.section(kind::PAGE_DIR).expect("PAGE_DIR"), &cfg)
        .expect("read PAGE_DIR");
    let dir = PageDir::decode(&raw).expect("decode PAGE_DIR");
    let blocks = dir.groups.iter().map(|g| g.block_count).collect();
    let dicts = dir
        .groups
        .iter()
        .flat_map(|g| &g.chunks)
        .filter(|c| c.dict_page().is_some())
        .count();
    (blocks, dicts)
}

fn build(group_target_blocks: usize) -> (Vec<u8>, usize) {
    let batch = batch();
    measure_peak(|| {
        let mut w = RlogWriter::new(cfg(group_target_blocks), identity());
        w.push_columnar(batch).expect("push");
        w.finish().expect("finish")
    })
}

/// The documented bound: a 32-block row group costs over the same data in
/// one-block groups at most its buffered pages (no more than the object's own
/// bytes), the dictionary budget, and one block's ids past it. Everything else
/// both builds hold alike. With the ids uncharged, the 32-block group holds all
/// 16 MiB of them, over four times this bound.
#[test]
fn dictionary_ids_are_bounded_by_the_budget() {
    let (grouped, peak_grouped) = build(GROUP_BLOCKS);
    let (single, peak_single) = build(1);

    let (blocks, dicts_grouped) = group_shape(&grouped);
    assert_eq!(blocks, vec![GROUP_BLOCKS as u32]);
    // A column still in the decision at the flush is charged 32 * 1024 * 4 =
    // 131,072 bytes of ids plus its distinct bytes (48 for an attribute, 8 for
    // body, 4 for severity_text), so eight of the 130 string columns already
    // take 1,048,876 bytes, past the budget: seven keep a dictionary. With the
    // ids uncharged all 130 do.
    assert_eq!(dicts_grouped, 7);
    assert_eq!(group_shape(&single).0, vec![1; GROUP_BLOCKS]);

    let bound = peak_single + grouped.len() + BUDGET + BLOCK_IDS;
    eprintln!(
        "peak(group of 32)={peak_grouped} peak(group of 1)={peak_single} \
         object={} bound={bound} dict chunks={dicts_grouped} group ids={GROUP_IDS}",
        grouped.len(),
    );
    assert!(
        peak_grouped <= bound,
        "peak {peak_grouped} over the bound {bound}: peak(group of 1)={peak_single}, \
         object={}, budget={BUDGET}, one block's ids={BLOCK_IDS}",
        grouped.len(),
    );
}
