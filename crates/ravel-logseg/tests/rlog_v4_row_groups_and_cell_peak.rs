//! The two merged behaviours of the ADR-0699 port, proven on ONE object:
//! v4's row groups (#699) and #682's one-block-at-a-time cell materialization
//! coexist, and neither silently regresses into the other's cost.
//!
//! - Row groups (v4): the object's PAGE_DIR partitions its blocks into
//!   `ceil(blocks / group_target_blocks)` groups. This is a property of the
//!   produced bytes, so it is exact, not a `> 0` check. Setting the writer's
//!   `group_target_blocks` to 0 (the src/writer.rs `Layout::V4` zero-target
//!   path folds it to one block per group) turns this into `blocks` groups and
//!   flips the group-count assertion red.
//! - Per-block cells (#682): `build_object_columnar` materializes one block's
//!   cells at a time, so the finish-time working set is bounded by ONE block's
//!   payload and does not grow with the object. Restoring the pre-#682
//!   batch-wide materialization (the per-block loop
//!   `for (blk_idx, span) in spans.iter().enumerate()` in
//!   `RlogWriter::build_object_columnar`, src/writer.rs) makes the peak scale
//!   with the whole object and flips both peak assertions red: the absolute
//!   `peak <= K x one_block_payload` bound (the object here is 16x one block)
//!   and the object-independence ratio.
//!
//! Peak live bytes are sampled from a sidecar thread over `stats_alloc`'s
//! instrumented allocator, the same technique (and the same one-test-per-binary
//! constraint) as `tests/columnar_writer_memory.rs`: a second test allocating
//! in this process would pollute the figure. `unsafe` is denied workspace-wide,
//! so a hand-rolled counting allocator will not compile.

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

const NUM_COLUMNS: usize = 32;
const BLOCK_RECORDS: usize = 1024;
const GROUP_TARGET_BLOCKS: usize = 4;

/// Config with a small block and a small row group, so a modest object still
/// spans several row groups and one block's working set is a fixed constant.
fn cfg() -> RlogConfig {
    RlogConfig {
        block_target_records: BLOCK_RECORDS,
        group_target_blocks: GROUP_TARGET_BLOCKS,
        block_max_bytes: 1 << 30,
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

/// A deterministic pool of variable-length strings (8..=64 bytes).
fn string_pool() -> Vec<Vec<u8>> {
    (0..128u32)
        .map(|i| {
            let len = 8 + (i as usize % 57);
            (0..len)
                .map(|j| b'a' + ((i as u8 + j as u8) % 26))
                .collect()
        })
        .collect()
}

/// Builds a synthetic columnar batch of `n` rows by `NUM_COLUMNS` mixed-type
/// dynamic columns, and returns it with `P`, the total resolved cell payload in
/// bytes (string cells count their length, numeric cells 8).
fn build_batch(n: usize) -> (ColumnarLogBatch, u64) {
    let pool = string_pool();
    let mut batch = ColumnarLogBatch::new();
    batch.num_rows = n;

    let mut body = VarBytes::new();
    let mut sev = VarBytes::new();
    for row in 0..n {
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

    let sid = LogStreamId([7u8; 16]);
    batch.stream_ids.push(sid);
    batch.stream_attrs.push(stream_attrs_bytes(
        &[("service.name".into(), AttrValue::Str("svc".into()))],
        "scope",
        "1.0",
        &[],
    ));

    let mut p: u64 = 0;
    for j in 0..NUM_COLUMNS {
        let kind = j % 4;
        let field_type = match kind {
            0 => FieldType::Str,
            1 => FieldType::I64,
            2 => FieldType::Bool,
            _ => FieldType::F64,
        };
        let mut validity = Bitmap::new();
        let mut cells: Vec<AttrValue> = Vec::new();
        for row in 0..n {
            let present = (row * 31 + j * 7) % 10 != 0;
            validity.push(present);
            if !present {
                continue;
            }
            let value = match kind {
                0 => {
                    let s = &pool[(row * 7 + j * 13) % pool.len()];
                    p += s.len() as u64;
                    AttrValue::Str(String::from_utf8(s.clone()).unwrap())
                }
                1 => {
                    p += 8;
                    AttrValue::I64(row as i64 * 1000 + j as i64)
                }
                2 => {
                    p += 8;
                    AttrValue::Bool((row + j) % 2 == 0)
                }
                _ => {
                    p += 8;
                    AttrValue::F64(row as f64 * 0.5 + j as f64)
                }
            };
            cells.push(value);
        }
        batch.dyn_columns.push(DynColumn {
            name: format!("attr_{j:03}"),
            field_type,
            cells,
            validity,
        });
    }
    batch.residual_attrs = vec![Vec::new(); n];
    (batch, p)
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

/// Builds one version-4 object of `blocks` blocks through the columnar writer,
/// returning `(object bytes, P, P per block, finish-time peak, PAGE_DIR group
/// count)`.
fn build(blocks: usize) -> (Vec<u8>, u64, u64, usize, usize) {
    let (batch, p) = build_batch(blocks * BLOCK_RECORDS);
    let (bytes, peak) = measure_peak(|| {
        let mut w = RlogWriter::new(cfg(), identity());
        w.push_columnar(batch).expect("push");
        w.finish().expect("finish")
    });

    // A present PAGE_DIR section is the version-4 discriminant (absent in v3).
    let ftr = open(&bytes).expect("open");
    let raw = read_section(
        &bytes,
        ftr.section(kind::PAGE_DIR).expect("PAGE_DIR"),
        &cfg(),
    )
    .expect("read page_dir");
    let dir = PageDir::decode(&raw).expect("decode page_dir");
    assert_eq!(
        dir.block_count(),
        blocks as u64,
        "block count as configured"
    );
    let groups = dir.groups.len();
    (bytes, p, p / blocks as u64, peak, groups)
}

/// One object, both merged behaviours pinned; a second, larger object pins that
/// the per-block peak does not grow with the object.
///
/// `K_PEAK_OVER_BLOCK` and `MAX_OBJECT_PEAK_RATIO` are generous but real
/// multiples: the object below is 16 (then 32) blocks, so a batch-wide
/// materialization would put the peak at roughly `blocks x` one block's payload
/// (about 16x and a 2x ratio), an order of magnitude past both bounds.
#[test]
fn row_group_count_and_per_block_cell_peak_hold_together() {
    // 16 blocks at a 4-block group => 4 row groups ("several"); 32 => 8.
    const BLOCKS_A: usize = 16;
    const BLOCKS_B: usize = 32;

    // One block's cells materialize into occurrence lists and per-plan value
    // pages whose overhead (enum tags, Vec headers) is a small constant times
    // the raw payload (measured ~21x). Batch-wide materialization multiplies
    // that by the 16-block count (~340x one block's payload), so this bound is
    // the decisive catcher for that regression: green well above the per-block
    // measurement, red by an order of magnitude under batch-wide.
    const K_PEAK_OVER_BLOCK: u64 = 48;
    // The dominant term (one block's cells) is fixed; a residual whole-object
    // term (the ts/body pointer and dict arrays #682 left batch-wide) still
    // scales, so 2x the object moves the peak ~1.4x, not ~2x. Batch-wide
    // materialization would push this to ~2x, but the absolute bound above is
    // what makes that regression fail loudly; this is the corroborating check.
    const MAX_OBJECT_PEAK_RATIO: f64 = 2.0;

    let (_a, p_a, p_block_a, peak_a, groups_a) = build(BLOCKS_A);
    let (_b, _p_b, _p_block_b, peak_b, groups_b) = build(BLOCKS_B);

    eprintln!(
        "blocks_a={BLOCKS_A} groups_a={groups_a} P_a={p_a} P_block={p_block_a} \
         peak_a={peak_a} (K={:.2}); blocks_b={BLOCKS_B} groups_b={groups_b} \
         peak_b={peak_b} ratio={:.3}",
        peak_a as f64 / p_block_a as f64,
        peak_b as f64 / peak_a as f64,
    );

    // v4 row groups: the count is exactly ceil(blocks / group_target_blocks).
    // Flip red by setting `group_target_blocks` to 0 in `cfg()`.
    let expect = |blocks: usize| blocks.div_ceil(GROUP_TARGET_BLOCKS);
    assert_eq!(
        groups_a,
        expect(BLOCKS_A),
        "PAGE_DIR must partition {BLOCKS_A} blocks into ceil/{GROUP_TARGET_BLOCKS} groups"
    );
    assert_eq!(
        groups_b,
        expect(BLOCKS_B),
        "PAGE_DIR must partition {BLOCKS_B} blocks into ceil/{GROUP_TARGET_BLOCKS} groups"
    );

    // #682 per-block cells: the finish-time peak is bounded by one block's
    // payload, not the object's. Flip red by restoring batch-wide
    // materialization in `RlogWriter::build_object_columnar`.
    let bound = K_PEAK_OVER_BLOCK * p_block_a;
    assert!(
        (peak_a as u64) <= bound,
        "finish peak {peak_a} exceeds K x one-block payload: K={K_PEAK_OVER_BLOCK}, \
         one_block={p_block_a}, bound={bound} (measured K={:.2})",
        peak_a as f64 / p_block_a as f64,
    );

    // And it does not grow with the object: 2x the blocks, ~1x the peak.
    let ratio = peak_b as f64 / peak_a as f64;
    assert!(
        ratio <= MAX_OBJECT_PEAK_RATIO,
        "finish peak scaled with the object: peak({BLOCKS_A})={peak_a}, \
         peak({BLOCKS_B})={peak_b}, ratio={ratio:.3} > {MAX_OBJECT_PEAK_RATIO}"
    );
}
