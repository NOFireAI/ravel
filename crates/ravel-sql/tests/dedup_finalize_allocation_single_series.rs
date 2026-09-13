//! Peak allocation for `RsegDedupExec::finalize`'s per-row labels compaction
//! (`src/dedup.rs`) on the worst case for that compaction: one series with
//! many samples. Issue #1582 measurement round.
//!
//! Unlike `tests/dedup_finalize_allocation.rs`'s many-small-segments corpus,
//! every row here shares the *same* series and so the *same* one-entry
//! dictionary key within any given upstream scan batch (`RsegScanExec`'s
//! `BATCH_ROWS` = 8192, `src/scan.rs`): the per-row `compact_labels` rebuild
//! buys nothing (there is only ever one dictionary entry to begin with) but
//! still pays a `MapBuilder` rebuild on every one of the ~20,000 winner rows.
//! This is exactly the shape `finalize`'s labels memo (`LabelsMemo` in
//! `src/dedup.rs`) targets: consecutive winner rows here share both the
//! source dictionary (by pointer, within one scan batch) and the key (always
//! 0, since there is only one series), so the memo should turn all but a
//! handful of those ~20,000 rebuilds into a cheap `Arc::clone`.
//!
//! Same one-test-per-binary / current-thread-runtime constraints as
//! `tests/dedup_finalize_allocation.rs`; see that file's header for why.

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

const TENANT: TenantHash = TenantHash([12u8; 16]);
const SAMPLES: usize = 20_000;

/// Bytes the whole scan -> merge -> dedup pipeline may allocate to answer
/// `SELECT * FROM samples` over one series with 20,000 samples in one
/// segment. Measured on this fixture (debug build, cargo test default
/// profile, the same profile as `tests/dedup_finalize_allocation.rs`):
///
///   - `finalize`'s `compact_labels` call deleted:       65,194,710 bytes
///   - as committed pre-memo (issue #1582 round 1):     345,644,574 bytes
///   - as committed today, with the memo (this round):   65,236,104 bytes
///
/// The memo recovers essentially all of this corpus's regression: 345.6M
/// down to 65.2M, within 42,000 bytes (0.06%) of the 65.19M floor with no
/// compaction at all, because every row here shares one series' single
/// dictionary entry, exactly the shape the memo targets. The bound sits at
/// roughly 1.4x the memoized figure (headroom for allocator noise) and less
/// than 1/3 of the pre-memo figure, so it stays decisive against a memo
/// regression (silently no longer firing, or being removed) without being
/// so tight that unrelated allocator jitter trips it.
const MAX_BYTES: usize = 90_000_000;

fn one_series_corpus(samples_each: usize) -> Vec<(LabelSet, Vec<(i64, f64)>)> {
    let labels = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: "http_requests".to_string(),
        },
        Label {
            name: "job".to_string(),
            value: "api".to_string(),
        },
        Label {
            name: "host".to_string(),
            value: "host-0".to_string(),
        },
    ])
    .expect("valid labels");
    let samples = (0..samples_each)
        .map(|t| (t as i64 + 1, t as f64))
        .collect();
    vec![(labels, samples)]
}

/// Write `series` into one segment (unlike
/// `tests/dedup_finalize_allocation.rs`'s many-segment split, everything
/// here is one series in one segment, so scan/merge draws every row from one
/// locally-built dictionary).
async fn write_fixture_one_segment(
    series: &[(LabelSet, Vec<(i64, f64)>)],
) -> (Arc<dyn ObjectStoreBackend>, Snapshot) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("t".to_string());
    let inputs: Vec<SeriesInput> = series
        .iter()
        .enumerate()
        .map(|(j, (labels, samples))| {
            let metric = format!("m{j}");
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
        writer_id: Uuid::from_u128(1).to_string(),
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
    let seg_ref = SegmentRef {
        data_object_key: key.to_string(),
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: 0,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard: 0,
        content_hash: written.summary.blake3,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 1,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
        declared_column_stats: Default::default(),
    };
    let snapshot = Snapshot {
        segments: vec![seg_ref],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (store, snapshot)
}

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
fn finalize_labels_compaction_on_single_series_many_samples() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let series = one_series_corpus(SAMPLES);
    let (store, snapshot) = rt.block_on(write_fixture_one_segment(&series));

    let warm_rows = rt.block_on(run_pipeline(Arc::clone(&store), snapshot.clone()));
    assert_eq!(
        warm_rows, SAMPLES,
        "the warm run must emit one winner row per sample"
    );

    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let rows = rt.block_on(run_pipeline(store, snapshot));
    let stats = region.change();

    assert_eq!(
        rows, SAMPLES,
        "every sample is a distinct (series, ts), so none dedup away"
    );
    eprintln!(
        "finalize_labels_compaction_on_single_series_many_samples: {rows} rows, \
         {} allocations, {} bytes allocated",
        stats.allocations, stats.bytes_allocated,
    );
    assert!(
        stats.bytes_allocated <= MAX_BYTES,
        "scan -> merge -> dedup over one series x {SAMPLES} samples allocated \
         {} bytes, exceeding the {MAX_BYTES} bound (measured as committed, \
         with the finalize labels memo in place); this guards the memo \
         against silently no longer firing on the shape it targets",
        stats.bytes_allocated,
    );
}
