//! Peak-memory and byte-identity regression tests for
//! `RlogWriter::build_object_columnar` (issue #682).
//!
//! Before the fix the columnar writer resolved the whole batch into dense
//! per-column value/stat arrays plus per-row occurrence and merged-view lists,
//! so peak live memory grew with `--batch-rows`: ~4.0 GB at 65_536 rows on the
//! synthetic batch below (a 131_072-row, 16-shard load did not fit a 31 GB
//! host). The fix materializes one block's cells at a time, so the peak is
//! bounded by one block and is independent of the batch size, without changing
//! a single output byte.
//!
//! This file contains EXACTLY ONE test on purpose: it installs the instrumented
//! global allocator and measures peak live bytes with a sidecar sampler thread,
//! so a second test allocating in the same process would pollute the figure
//! (`cargo test` runs a binary's tests on threads; nextest runs each in its own
//! process). The workspace forbids `unsafe_code`, so a hand-rolled counting
//! allocator will not compile; `stats_alloc`'s instrumented allocator (already
//! used by the ravel-sql/ravel-otlp allocation tests) is used instead, and peak
//! (max live) is derived by sampling it, since it exposes only cumulative
//! counters.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use ravel_logseg::{
    AttrValue, Bitmap, ColumnarLogBatch, DynColumn, FieldType, LogStreamId, ObjectIdentity,
    RlogConfig, RlogWriter, VarBytes, stream_attrs_bytes,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const NUM_COLUMNS: usize = 105;

/// BLAKE3 of the synthetic object at each batch size, computed against the
/// PRE-FIX writer (before this change) and unchanged by every step of the fix.
/// A mismatch means the fix altered an output byte, which it must never do.
const HASH_8K: &str = "c13ffbb6ebb2f8aacd29302267f5a3c052ddc61c8332735d2afd7fc782c81071";
const HASH_64K: &str = "0da2d9e1bf96a9c5ff9a79de9ec037dde78255ed2ef741887bd2f0243750017a";

/// Peak-live-bytes bound as a multiple of the total cell payload `P`. Measured
/// K = peak(65536) / P = 1.865 after the fix; rounded up to the next 0.5. The
/// pre-fix writer measured K ~= 44 (peak ~4.0 GB against P ~90 MB), so this
/// bound is red before the fix by more than an order of magnitude.
const K_PEAK_OVER_P: f64 = 2.0;

/// Peak-memory ratio between the two batch sizes. The fix makes the peak
/// independent of batch size (measured 1.16); the pre-fix writer's peak is
/// linear in batch size, so this ratio was ~7 (8x the rows, ~8x the peak).
const MAX_BATCH_PEAK_RATIO: f64 = 2.0;

/// A deterministic pool of variable-length strings (8..=64 bytes). Low
/// cardinality so the compressed BLOCKS section stays small relative to one
/// block's materialized working set, which is what the batch-size-independence
/// bound measures; `P` still counts every cell's full byte length, so the
/// magnitude bound stays meaningful.
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
/// dynamic columns (i64/str/bool/f64 cycled), strings 8..64 bytes, ~10% nulls,
/// one stream, deterministically. Returns the batch and `P`, the total resolved
/// cell payload in bytes (string/bytes cells count their length, numeric cells
/// 8), computed from the batch itself.
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

fn cfg() -> RlogConfig {
    RlogConfig {
        // Record-count governed, uniform 8192-row blocks at both batch sizes,
        // so one block's working set is the same constant regardless of the
        // batch size; block_max_bytes is lifted out of the way.
        block_target_records: 8192,
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

/// Runs `f`, sampling live allocated bytes from a spinning sidecar thread, and
/// returns `f`'s result plus the peak live-byte delta observed during the call.
/// The peak is a plateau (the block working set is held across each block's
/// zstd encode), so the sampler catches the same maximum deterministically.
fn measure_peak<R, F: FnOnce() -> R>(f: F) -> (R, usize) {
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicUsize::new(0));
    let base = INSTRUMENTED_SYSTEM.stats();
    let s2 = stop.clone();
    let p2 = peak.clone();
    let handle = thread::spawn(move || {
        loop {
            // Live bytes = allocated - deallocated. stats_alloc already folds a
            // realloc's growth into bytes_allocated (and a shrink into
            // bytes_deallocated), so bytes_reallocated must NOT be added here or
            // every Vec growth would count twice.
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

/// Builds one object through the columnar writer, returning its bytes, `P`, and
/// the peak live bytes during `finish`.
fn build_object(n: usize) -> (Vec<u8>, u64, usize) {
    let (batch, p) = build_batch(n);
    let (bytes, peak) = measure_peak(|| {
        let mut w = RlogWriter::new(cfg(), identity());
        w.push_columnar(batch).expect("push");
        w.finish().expect("finish")
    });
    (bytes, p, peak)
}

#[test]
fn columnar_writer_peak_is_bounded_and_batch_independent() {
    let (obj8k, _p8k, peak8k) = build_object(8_192);
    let (obj64k, p64k, peak64k) = build_object(65_536);

    // Byte identity: the fix must not change any output byte.
    assert_eq!(
        blake3::hash(&obj8k).to_hex().as_str(),
        HASH_8K,
        "columnar object bytes changed at 8192 rows"
    );
    assert_eq!(
        blake3::hash(&obj64k).to_hex().as_str(),
        HASH_64K,
        "columnar object bytes changed at 65536 rows"
    );

    // Magnitude: peak live bytes at 65536 rows are within K * P. Pinned
    // proportionally to the payload, not to a flat floor: the pre-fix writer's
    // dense whole-batch arrays put this at ~44 * P.
    let bound = (K_PEAK_OVER_P * p64k as f64) as usize;
    assert!(
        peak64k <= bound,
        "peak {peak64k} exceeds K*P: K={K_PEAK_OVER_P}, P={p64k}, bound={bound} \
         (measured K={:.3})",
        peak64k as f64 / p64k as f64,
    );

    // Batch-size independence (what the per-block materialization exists for):
    // 8x the rows must not cost anywhere near 8x the peak. Pre-fix this ratio
    // is ~7 (linear in batch size).
    let ratio = peak64k as f64 / peak8k as f64;
    assert!(
        ratio <= MAX_BATCH_PEAK_RATIO,
        "peak ratio {ratio:.3} exceeds {MAX_BATCH_PEAK_RATIO}: \
         peak(8192)={peak8k}, peak(65536)={peak64k}",
    );

    eprintln!(
        "peak(8192)={peak8k} peak(65536)={peak64k} P(65536)={p64k} \
         K={:.3} ratio={ratio:.3}",
        peak64k as f64 / p64k as f64,
    );
}
