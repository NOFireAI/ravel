//! Allocation upper bound for the logs scan's columnar batch building (ADR-0099
//! decision 2, issue #415).
//!
//! This file contains EXACTLY ONE test on purpose. The measurement is a
//! `stats_alloc::Region` around the global allocator, so any other thread
//! allocating while it is open would land in the count; `cargo test` and
//! nextest both run test functions concurrently, so a second test in this
//! binary would make the figure non-deterministic. The scan itself is driven on
//! a current-thread tokio runtime for the same reason. This mirrors
//! `tests/scan_batch_allocations.rs`, which bounds the metrics scan.
//!
//! The assertion is an upper bound per emitted 8192-row batch, not an equality:
//! an unrelated small change must not fail it, but a return to per-record work
//! on this path (rebuilding a `LogRecord` and calling `merged_attrs`, i.e. the
//! row path this fast path exists to skip) blows through it by orders of
//! magnitude.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{LogsScanExec, logs_schema_with_declared};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// `LogsScanExec::BATCH_ROWS`, which is also the default RLOG block target, so
/// each written block decodes to exactly one full output batch.
const BATCH_ROWS: usize = 8192;
/// 30 full batches: 30 blocks of one full batch each, at the default block
/// target.
const RECORDS: usize = BATCH_ROWS * 30;

/// Allocations one 8192-row batch may cost on the columnar fast path, over a
/// numeric fixed-column projection (`ts`, `observed_ts`, `severity_num`,
/// `flags`). Each is an i64-backed column the block decodes into one contiguous
/// buffer, so the fast path builds each output batch from a bounded number of
/// Arrow buffers (one `Vec` gather plus the arrow array per column) and does no
/// per-record work at all: allocations per batch are O(columns), not O(rows).
///
/// The projection is numeric on purpose. `body` and `severity_text` are stored
/// as `Vec<Option<Vec<u8>>>` in `ravel_logseg`'s `DecodedBlock`, one owned
/// allocation per cell, so decoding either costs ~1 allocation per row *before*
/// any batch is built -- a property of the block decode both scan paths share,
/// not of the fast path this bound exists to characterize. Projecting them would
/// swamp the batch-building figure with decode allocations and measure the wrong
/// thing; the metrics scan's allocation test bounds only numeric columns for the
/// same reason.
///
/// A regression that reintroduced per-record work on this path (rebuilding a
/// `LogRecord` and calling `merged_attrs` per row, i.e. the row path) would be
/// three orders of magnitude above this bound. Measured at 51 on this fixture;
/// the bound is set with headroom over that.
const MAX_ALLOCS_PER_BATCH: usize = 80;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// One record with a resource attribute and one dynamic attribute. The fast
/// path touches neither; they are present so a regression to the row path pays
/// their full `merged_attrs` cost and trips the bound.
fn record(i: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc-{}", i % 8)),
    )];
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: i as i64,
        observed_ts_ns: i as i64,
        severity_num: (i % 24) as u8,
        severity_text: "INFO".to_string(),
        body: format!("request {} completed", i % 997),
        trace_id: Some([(i % 251) as u8; 16]),
        span_id: Some([(i % 251) as u8; 8]),
        flags: i as u32,
        attrs: vec![("user_id".to_string(), AttrValue::Str(format!("u{i}")))],
    }
}

/// One RLOG object, `RECORDS` records, default block target: `RECORDS /
/// BATCH_ROWS` full blocks, each a single spill-free block the fast path
/// consumes.
async fn fixture() -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>) {
    let store = MemoryStore::new();
    let mut w = RlogWriter::new(RlogConfig::default(), identity());
    for i in 0..RECORDS {
        w.push(record(i)).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    let key = "logs/alloc.rlog";
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    let seg = SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: 0,
        max_event_ts_ns: (RECORDS - 1) as i64,
        ingest_hour_bucket: 0,
        sample_count: RECORDS as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
    };
    (Arc::new(store), vec![seg])
}

/// Drain the columnar fast path (fixed-column projection, no erasure), returning
/// (batches, rows, columnar_batches, rowpath_batches).
async fn drain(
    store: Arc<dyn ObjectStoreBackend>,
    segments: &[SegmentRef],
) -> (usize, usize, usize, usize) {
    let scan = LogsScanExec::new(
        TENANT,
        LogSegmentFetcher::new(store),
        segments,
        1,
        i64::MIN,
        i64::MAX,
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        // ts, observed_ts, severity_num, flags: numeric fixed columns, so the
        // query is columnar-eligible and no per-cell string decode is charged.
        Some(&vec![0usize, 1, 2, 7]),
        QueryAccounting::new(),
        logs_schema_with_declared(&[]),
        Arc::new(Vec::new()),
    )
    .expect("build scan");
    let mut stream = scan
        .execute(0, Arc::new(TaskContext::default()))
        .expect("execute scan");
    let mut batches = 0;
    let mut rows = 0;
    while let Some(next) = stream.next().await {
        let batch = next.expect("batch");
        batches += 1;
        rows += batch.num_rows();
    }
    let metrics = scan.metrics().expect("metrics");
    let count = |name: &str| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
    (
        batches,
        rows,
        count("columnar_batches"),
        count("rowpath_batches"),
    )
}

#[test]
fn logs_scan_allocations_per_batch_stay_bounded() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (store, segments) = rt.block_on(fixture());

    // One untimed warm run outside the region: the first scan of a process
    // initializes DataFusion's lazily-built state, and those allocations belong
    // to no batch.
    let warm = rt.block_on(drain(Arc::clone(&store), &segments));
    assert_eq!(warm.1, RECORDS, "the warm run must emit every record");
    assert_eq!(warm.3, 0, "the warm run must take the columnar fast path");
    assert!(warm.2 > 0, "the warm run must build columnar batches");

    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let (batches, rows, columnar, rowpath) = rt.block_on(drain(store, &segments));
    let stats = region.change();

    assert_eq!(rows, RECORDS, "every written record must be emitted");
    assert_eq!(batches, 30, "the fixture must emit 30 full batches");
    assert_eq!(
        rowpath, 0,
        "the measured run must not fall back to the row path"
    );
    assert_eq!(
        columnar, batches,
        "every measured batch must come from the columnar fast path"
    );

    let per_batch = stats.allocations / batches;
    let bytes_per_batch = stats.bytes_allocated / batches;
    eprintln!(
        "logs_scan_allocations: {rows} rows in {batches} batches, \
         {} allocations ({} bytes) total, {per_batch} allocations/batch, \
         {bytes_per_batch} bytes/batch",
        stats.allocations, stats.bytes_allocated,
    );
    assert!(
        per_batch <= MAX_ALLOCS_PER_BATCH,
        "batch building must stay columnar: {per_batch} allocations per \
         {BATCH_ROWS}-row batch exceeds the {MAX_ALLOCS_PER_BATCH} bound \
         ({} allocations over {batches} batches)",
        stats.allocations,
    );
}
