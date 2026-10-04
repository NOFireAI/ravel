//! Peak-memory regression test for the routed build path (ADR-2467 decision 1,
//! issue #2564): `RlogWriter::build` folds row input into a `ColumnarLogBatch`
//! and explicitly `drop(records)`s the raw `Vec<LogRecord>` buffer before
//! `validate()`/`build_object_columnar` run (`writer.rs`, in `build`, directly
//! after the fold). A writer that kept `self.records` alive across the whole
//! build would hold the row buffer, the folded batch, and the per-block
//! materialization working set simultaneously; this measures that the routed
//! path's peak stays close to the columnar path's peak (which never allocates
//! a row buffer at all), rather than growing by roughly the row buffer's own
//! size on top of it.
//!
//! This file contains EXACTLY ONE test on purpose, for the same reason as
//! `columnar_writer_memory.rs`: it installs the instrumented global allocator
//! and measures peak live bytes with a sidecar sampler thread, so a second
//! test allocating in the same process would pollute the figure (`cargo test`
//! runs a binary's tests on threads; nextest runs each in its own process).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use ravel_logseg::{
    AttrValue, ColumnarLogBatch, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter,
    stream_attrs_bytes,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const NUM_ROWS: usize = 65_536;
const BODY_LEN: usize = 256;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [3u8; 16],
        shard: 0,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn cfg() -> RlogConfig {
    RlogConfig {
        block_target_records: 8_192,
        block_max_bytes: 1 << 30,
        ..RlogConfig::default()
    }
}

fn body(row: usize) -> String {
    (0..BODY_LEN)
        .map(|j| (b'a' + ((row + j) % 26) as u8) as char)
        .collect()
}

fn attrs(row: usize) -> Vec<(String, AttrValue)> {
    vec![("idx".to_string(), AttrValue::I64(row as i64))]
}

/// `NUM_ROWS` row-shaped records, one stream, deterministic bodies/attrs.
fn build_records() -> Vec<LogRecord> {
    let blob = stream_attrs_bytes(
        &[(
            "service.name".to_string(),
            AttrValue::Str("svc".to_string()),
        )],
        "scope",
        "1",
        &[],
    );
    (0..NUM_ROWS)
        .map(|row| LogRecord {
            stream_id: LogStreamId([9u8; 16]),
            stream_attrs: blob.clone(),
            ts_ns: row as i64,
            observed_ts_ns: row as i64,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: body(row),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs(row),
        })
        .collect()
}

/// Folds `records` into one [`ColumnarLogBatch`] via the same production
/// function `RlogWriter::build` uses (`ColumnarLogBatch::from_records`,
/// through `fold_records`), so the batch this produces is guaranteed
/// byte-identical to what the routed path builds from the same rows: this
/// test measures memory, not correctness, and a hand-rolled parallel batch
/// constructor would risk a spurious mismatch unrelated to the property under
/// test.
fn build_batch(records: &[LogRecord]) -> ColumnarLogBatch {
    ColumnarLogBatch::from_records(records)
}

/// Runs `f`, sampling live allocated bytes from a spinning sidecar thread, and
/// returns `f`'s result plus the peak live-byte delta observed during the
/// call. Mirrors `columnar_writer_memory.rs`'s `measure_peak`.
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

/// Peak-live-bytes ratio of the routed path (`push` per record + `finish`)
/// over the columnar path (`push_columnar` + `finish`) on equivalent data.
/// Measured 1.385 on this fixture (NUM_ROWS=65536, BODY_LEN=256). Reproduced
/// the regression this guards against by moving `records` into a binding that
/// outlives `RlogWriter::build`'s fold instead of `drop`ping it there: ratio
/// rose to 2.271 (routed_peak=41,850,460 vs an unchanged columnar_peak of
/// 18,430,357), which this bound catches.
const MAX_ROUTED_OVER_COLUMNAR_RATIO: f64 = 2.0;

#[test]
fn routed_path_drops_records_before_building() {
    let records = build_records();
    let (routed_bytes, routed_peak) = measure_peak(|| {
        let mut w = RlogWriter::new(cfg(), identity());
        for r in records {
            w.push(r).expect("push");
        }
        w.finish().expect("finish")
    });

    // Built, and the source records dropped, before `measure_peak` takes its
    // base snapshot: the batch's own allocation does not count against the
    // columnar path's measured delta, matching how it is already resident
    // (not freshly built inside the measured window) on the routed path's
    // `finish` call once `drop(records)` has run.
    let records2 = build_records();
    let batch = build_batch(&records2);
    drop(records2);
    let (columnar_bytes, columnar_peak) = measure_peak(|| {
        let mut w = RlogWriter::new(cfg(), identity());
        w.push_columnar(batch).expect("push_columnar");
        w.finish().expect("finish")
    });

    // The two paths encode the same rows, so the output must still match
    // (belt-and-braces: the byte-identity property has its own dedicated test
    // in row_reference_acceptance.rs; this file exists to measure memory, not
    // to re-assert correctness).
    assert_eq!(routed_bytes, columnar_bytes);

    let ratio = routed_peak as f64 / columnar_peak as f64;
    assert!(
        ratio <= MAX_ROUTED_OVER_COLUMNAR_RATIO,
        "routed/columnar peak ratio {ratio:.3} exceeds {MAX_ROUTED_OVER_COLUMNAR_RATIO}: \
         routed_peak={routed_peak}, columnar_peak={columnar_peak} -- the row buffer may be \
         staying alive past the fold (missing drop(records) in RlogWriter::build)",
    );

    eprintln!("routed_peak={routed_peak} columnar_peak={columnar_peak} ratio={ratio:.3}",);
}
