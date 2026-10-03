//! Allocation-churn figures (cumulative bytes allocated over the run, via
//! `stats_alloc`; not peak resident bytes) for `RsegDedupExec`'s
//! labels-dictionary handling (`src/dedup.rs`, `DedupStream::flush`) on the
//! shape its deferral round (issue #1582) exists for: many distinct series
//! in one segment, so every row shares one dictionary pointer but no two
//! rows share a key.
//!
//! Unlike `tests/dedup_finalize_allocation.rs`'s many-small-segments corpus
//! or `tests/dedup_finalize_allocation_single_series.rs`'s one-series
//! corpus, this corpus is the shape the labels memo (`LabelsMemo` in
//! `src/dedup.rs`) cannot help at all: every row is a different series, so
//! the dictionary key differs every row even though every row's dictionary
//! *pointer* is the same one segment-wide dictionary. An earlier round of
//! this operator compacted every row's labels down to one entry
//! unconditionally in `finalize`, which on this corpus paid a `MapBuilder`
//! rebuild on all 5,000 winner rows for nothing: every row already shared
//! one dictionary pointer, so `concat_batches` would have shared it for free
//! (see `crate::labels::compact_labels` in `ravel-sql` for why). `flush`'s
//! `out_multi_dict` tracking (see its docs) answers "does this flush window
//! actually need real compaction" before doing any, and skips the rebuild
//! entirely here.
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
use ravel_query::{EngineConfig, PhaseAccounting, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::RavelTableProvider;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const TENANT: TenantHash = TenantHash([13u8; 16]);
const SERIES: usize = 5_000;

/// Allocation churn (cumulative bytes allocated, not peak resident bytes)
/// the whole scan -> merge -> dedup pipeline may accumulate answering
/// `SELECT * FROM samples` over 5,000 distinct series, one sample each, in
/// one segment (so one shared labels dictionary throughout). Measured on
/// this fixture (debug build, cargo test default profile):
///
///   - compaction skipped/deleted outright:                29,948,424 bytes allocated
///   - compacting every row unconditionally (pre-deferral): 100,683,176 bytes allocated
///   - as committed today, deferred to `flush`:              29,566,211 bytes allocated
///
/// This is the corpus the deferral exists for: unconditional per-row
/// compaction cost 3.36x more than doing nothing, and the memo could not
/// help at all (every row is a different series, so the dictionary key never
/// repeats) -- only skipping the rebuild when it provably buys nothing does.
/// The current figure lands at the no-compaction floor, as expected. The
/// bound sits at roughly 1.5x the current figure (headroom for allocator
/// noise) and well under half the unconditional-compaction figure, so it
/// stays decisive against the skip silently no longer firing.
const MAX_CHURN_BYTES: usize = 45_000_000;

/// A corpus of `count` series, each with a distinct multi-label set and one
/// label unique to it, plus a shared and a per-series-absent label so the
/// shapes vary. One sample per series. Same shape as
/// `tests/dedup_finalize_allocation.rs`'s `varied_corpus`.
fn varied_corpus(count: usize) -> Vec<(LabelSet, Vec<(i64, f64)>)> {
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
            (labels, vec![(1i64, s as f64)])
        })
        .collect()
}

/// Write all of `series` into one segment, so scan/merge draws every row
/// from one locally-built labels dictionary.
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
        PhaseAccounting::new(),
    );
    let plan = provider.plan(1).expect("build plan");
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn finalize_labels_compaction_on_many_series_one_segment() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let series = varied_corpus(SERIES);
    let (store, snapshot) = rt.block_on(write_fixture_one_segment(&series));

    // One untimed warm run outside the region: the first scan of a process
    // initializes DataFusion's lazily-built state, and those allocations
    // belong to no query.
    let warm_rows = rt.block_on(run_pipeline(Arc::clone(&store), snapshot.clone()));
    assert_eq!(
        warm_rows, SERIES,
        "the warm run must emit one winner per series"
    );

    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let rows = rt.block_on(run_pipeline(store, snapshot));
    let stats = region.change();

    assert_eq!(rows, SERIES, "every series is a distinct group");
    eprintln!(
        "finalize_labels_compaction_on_many_series_one_segment: {rows} rows, \
         {} allocations, {} bytes allocated",
        stats.allocations, stats.bytes_allocated,
    );
    assert!(
        stats.bytes_allocated <= MAX_CHURN_BYTES,
        "scan -> merge -> dedup over 5,000 series in one segment allocated \
         {} bytes (churn), exceeding the {MAX_CHURN_BYTES} churn bound; this \
         guards RsegDedupExec::flush's out_multi_dict skip (src/dedup.rs) \
         against silently no longer firing on the shape it targets",
        stats.bytes_allocated,
    );
}
