//! Allocation upper bound for the metrics scan's batch building (ADR-0099
//! decision 7).
//!
//! This file contains EXACTLY ONE test on purpose. The measurement is a
//! `stats_alloc::Region` around the global allocator, so any other thread
//! allocating while it is open would land in the count; `cargo test` and
//! nextest both run test functions concurrently, so a second test in this
//! binary would make the figure non-deterministic. The scan itself is driven
//! on a current-thread tokio runtime for the same reason.
//!
//! The assertion is an upper bound per emitted 8192-row batch, not an
//! equality: an unrelated small change must not fail it, but a return to
//! per-sample allocation (the deleted `ScanRow` explode, or a gather that
//! builds a row struct) blows through it by orders of magnitude.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::RavelTableProvider;
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// `RsegScanExec::BATCH_ROWS`.
const BATCH_ROWS: usize = 8192;
/// 30 full batches from one series in one segment.
const SAMPLES: i64 = 8192 * 30;

/// Allocations one 8192-row batch may cost. The columnar path allocates a
/// bounded number of buffers per batch (the gathered or adopted ts/value
/// columns, the series_id buffer, three provenance columns, the in-page
/// column, the labels dictionary's keys and its one map entry per series),
/// each of them one or two allocations plus growth. 8192 samples per batch
/// means a per-sample allocation would be at least three orders of magnitude
/// above this bound. Measured at 74 on this fixture.
const MAX_ALLOCS_PER_BATCH: usize = 90;

/// Bytes one 8192-row batch may allocate. This is the bound that separates the
/// columnar path from the row path: the deleted `ScanRow` explode allocated
/// 64 bytes per sample for the run plus a full copy of every column per batch,
/// and measured 2.26 MB per batch on this fixture against the columnar path's
/// 0.79 MB. Both figures include the one segment's fetch and decode amortized
/// over the 30 batches, which is why this is a bound with headroom rather than
/// a ratio.
const MAX_BYTES_PER_BATCH: usize = 1_200_000;

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series_id_for(metric: &str) -> [u8; 16] {
    let tenant = TenantId::new("t".to_string());
    SeriesId::compute(&tenant, metric, &labels_for(metric))
        .expect("series id")
        .0
}

/// One segment, one series, `SAMPLES` samples: the shape whose batches sit
/// contiguously inside a single run.
async fn fixture() -> (Arc<dyn ObjectStoreBackend>, Snapshot) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let inputs = vec![SeriesInput {
        series_id: SeriesId(series_id_for("m")),
        labels: labels_for("m"),
        samples: (0..SAMPLES)
            .map(|i| Sample {
                ts_ns: i,
                value: i as f64,
            })
            .collect(),
    }];
    let writer_id = Uuid::from_u128(7);
    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    let key = "t/metrics/seg-0.rseg";
    store
        .put(key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment");
    let snapshot = Snapshot {
        segments: vec![SegmentRef {
            data_object_key: key.to_string(),
            object_size: written.bytes.len() as u64,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            ingest_hour_bucket: 0,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            shard: 0,
            content_hash: written.summary.blake3,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 1,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
        }],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (store, snapshot)
}

/// Drain the scan node itself (the pre-dedup fragment's leaf), returning
/// (batches, rows).
async fn drain(store: Arc<dyn ObjectStoreBackend>, snapshot: Snapshot) -> (usize, usize) {
    let segments = snapshot.segments.clone();
    let provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        SegmentFetcher::new(store),
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    let fragment = provider.worker_fragment(1, &segments).expect("fragment");
    let children = fragment.children();
    let scan = Arc::clone(children.first().expect("scan under the merge"));
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
    (batches, rows)
}

#[test]
fn scan_allocations_per_batch_stay_bounded() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (store, snapshot) = rt.block_on(fixture());

    // One untimed warm run outside the region: the first scan of a process
    // initializes DataFusion's lazily-built state, and those allocations
    // belong to no batch.
    let warm = rt.block_on(drain(Arc::clone(&store), snapshot.clone()));
    assert_eq!(warm.1, SAMPLES as usize, "the warm run must emit every row");

    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let (batches, rows) = rt.block_on(drain(store, snapshot));
    let stats = region.change();

    assert_eq!(
        rows, SAMPLES as usize,
        "every written sample must be emitted"
    );
    assert_eq!(batches, 30, "the fixture must emit 30 full batches");

    let per_batch = stats.allocations / batches;
    let bytes_per_batch = stats.bytes_allocated / batches;
    eprintln!(
        "scan_batch_allocations: {rows} rows in {batches} batches, \
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
    assert!(
        bytes_per_batch <= MAX_BYTES_PER_BATCH,
        "batch building must not copy the samples again: {bytes_per_batch} \
         bytes per {BATCH_ROWS}-row batch exceeds the {MAX_BYTES_PER_BATCH} \
         bound ({} bytes over {batches} batches)",
        stats.bytes_allocated,
    );
}
