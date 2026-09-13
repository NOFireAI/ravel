//! Peak allocation bound for `RsegDedupExec::finalize`'s per-row labels
//! compaction (`src/dedup.rs`'s call to `crate::labels::compact_labels`
//! ahead of `flush`'s own compaction; issue #1582 fix-round finding at
//! `src/dedup.rs:233`).
//!
//! This file contains EXACTLY ONE test on purpose, following
//! `tests/scan_batch_allocations.rs`: the measurement is a `stats_alloc::Region`
//! around the global allocator, so a second test running concurrently in this
//! binary (`cargo test`/nextest run test functions concurrently within one
//! binary) would land its own allocations in the count. The pipeline is
//! driven on a current-thread tokio runtime for the same reason. Valid under
//! a debug (`cargo test`) or release build alike, single-threaded within its
//! own process, which is guaranteed here because this file compiles to its
//! own test binary and holds no other `#[test]`.
//!
//! The corpus is `varied_corpus(10000, 1)`: 10,000 distinct series, one
//! sample each, none of them true (series, ts) duplicates, split across 500
//! small segments (`write_fixture_many_small_segments`) of 20 series each.
//! Each segment gets its own locally-built labels dictionary, and each
//! segment's own scan output (20 rows) sits far under `RsegDedupExec::
//! FLUSH_ROWS` (1024), so `poll_next`'s outer loop keeps folding winners from
//! many different segments -- each with a distinct, non-pointer-equal
//! dictionary -- into the same accumulator before a flush is due. Without
//! `finalize`'s per-row `compact_labels` call, each accumulated one-row slice
//! still points at its *whole* source segment's dictionary, so `flush`'s
//! `concat_batches` (which shares a dictionary only on pointer equality
//! between inputs) appends one full per-segment dictionary per accumulated
//! row instead of one shared, already-bounded one; with the call in place,
//! every slice already carries at most one entry, so the transient payload
//! scales with the row count times one entry rather than the row count times
//! a whole segment's dictionary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::collect;
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

const TENANT: TenantHash = TenantHash([11u8; 16]);

/// Bytes the whole scan -> merge -> dedup pipeline may allocate to answer
/// `SELECT * FROM samples` over the 10,000-series/500-segment corpus
/// described above. Measured on this fixture: with `finalize`'s per-row
/// `compact_labels` call in place, 201,280,657 bytes; with that call
/// deleted (leaving `flush`'s own compaction as the only guard),
/// 387,118,145 bytes, 1.92x more. This bound sits about a third above the
/// fixed measurement and a third below the broken one, so it is decisive
/// against allocator noise (the fixed run has headroom to grow) without
/// being a bare `> 0` or a restatement of either measured number.
const MAX_BYTES: usize = 260_000_000;

/// A corpus of `count` series, each with a distinct multi-label set and one
/// label unique to it, plus a shared and a per-series-absent label so the
/// shapes vary. `samples_each` samples per series, strictly ts-ascending.
/// Same shape as `tests/labels_dict_compaction.rs`'s `varied_corpus`.
fn varied_corpus(count: usize, samples_each: usize) -> Vec<(LabelSet, Vec<(i64, f64)>)> {
    (0..count)
        .map(|s| {
            let mut pairs = vec![
                ("__name__".to_string(), "http_requests".to_string()),
                ("job".to_string(), "api".to_string()),
                ("host".to_string(), format!("host-{s}")),
            ];
            if s % 2 == 0 {
                pairs.push(("region".to_string(), format!("r{}", s % 3)));
            }
            let labels = LabelSet::new(
                pairs
                    .into_iter()
                    .map(|(name, value)| Label { name, value })
                    .collect(),
            )
            .expect("valid labels");
            let samples = (0..samples_each)
                .map(|t| (t as i64 + 1, (s * 1000 + t) as f64))
                .collect();
            (labels, samples)
        })
        .collect()
}

/// Split `series` (all distinct, no true duplicates) across `segments`
/// equal-sized segments, each its own `SegmentRef` with its own locally-built
/// labels dictionary. Small per-segment row counts keep each segment's own
/// scan output under `RsegDedupExec::FLUSH_ROWS`, so several segments' worth
/// of winners -- each from a different dictionary -- must accumulate in one
/// flush's `concat_batches` rather than one segment alone crossing the flush
/// threshold by itself.
async fn write_fixture_many_small_segments(
    series: &[(LabelSet, Vec<(i64, f64)>)],
    segments: usize,
) -> (Arc<dyn ObjectStoreBackend>, Snapshot) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("t".to_string());
    let chunk = series.len().div_ceil(segments);
    let mut refs = Vec::with_capacity(segments);
    for (seq, chunk_series) in series.chunks(chunk).enumerate() {
        let base = seq * chunk;
        let inputs: Vec<SeriesInput> = chunk_series
            .iter()
            .enumerate()
            .map(|(j, (labels, samples))| {
                let metric = format!("m{}", base + j);
                SeriesInput {
                    series_id: SeriesId::compute(&tenant, &metric, labels).expect("series id"),
                    labels: labels.clone(),
                    samples: samples
                        .iter()
                        .map(|(ts_ns, value)| Sample {
                            ts_ns: *ts_ns,
                            value: *value,
                        })
                        .collect(),
                }
            })
            .collect();

        let identity = SegmentIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: Uuid::from_u128(seq as u128 + 1).to_string(),
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
        let key = format!("t/metrics/seg-{seq}.rseg");
        store
            .put(&key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put segment");
        refs.push(SegmentRef {
            data_object_key: key,
            object_size: written.bytes.len() as u64,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            ingest_hour_bucket: 0,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            shard: 0,
            content_hash: written.summary.blake3,
            writer_id: Uuid::from_u128(seq as u128 + 1),
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
            created_unix_ns: seq as i64 + 1,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
            declared_column_stats: Default::default(),
        });
    }
    let snapshot = Snapshot {
        segments: refs,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (store, snapshot)
}

/// Run the scan -> merge -> dedup pipeline against an already-written
/// fixture (the segment write itself is not part of what this test bounds
/// and must stay outside the timed region).
async fn run_pipeline(store: Arc<dyn ObjectStoreBackend>, snapshot: Snapshot) -> usize {
    let fetcher = SegmentFetcher::new(store);
    let provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        fetcher,
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    let plan = provider.plan(1).expect("build plan");
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn finalize_labels_compaction_bounds_peak_allocation() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let series = varied_corpus(10000, 1);
    let (store, snapshot) = rt.block_on(write_fixture_many_small_segments(&series, 500));

    // One untimed warm run outside the region: the first scan of a process
    // initializes DataFusion's lazily-built state, and those allocations
    // belong to no query.
    let warm_rows = rt.block_on(run_pipeline(Arc::clone(&store), snapshot.clone()));
    assert_eq!(
        warm_rows, 10000,
        "the warm run must emit one winner per series"
    );

    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let rows = rt.block_on(run_pipeline(store, snapshot));
    let stats = region.change();

    assert_eq!(rows, 10000, "every group must dedup to its one winner");
    eprintln!(
        "finalize_labels_compaction_bounds_peak_allocation: {rows} rows, \
         {} allocations, {} bytes allocated",
        stats.allocations, stats.bytes_allocated,
    );
    assert!(
        stats.bytes_allocated <= MAX_BYTES,
        "scan -> merge -> dedup over the 10,000-series/500-segment corpus \
         allocated {} bytes, exceeding the {MAX_BYTES} bound; this guards \
         RsegDedupExec::finalize's per-row labels compaction (src/dedup.rs) \
         against being deleted",
        stats.bytes_allocated,
    );
}
