//! Nested subquery evaluation is interruptible at the query deadline: the
//! evaluator yields at cross-level cost-budget checkpoints, so `QueryEngine`'s
//! `tokio::time::timeout` wrapper (engine.rs) can interrupt a runaway nested
//! evaluation rather than only firing after it finishes.
//!
//! `ravel_promql::Evaluator` checks a wall-clock deadline
//! (`with_deadline`/`QueryWindow::check_deadline`) between subquery grid
//! steps, and `QueryEngine::{instant,range}_with_stats` derive that deadline
//! from their own `Duration` parameter and pass it into the evaluator before
//! the `tokio::time::timeout` wrapper ever polls it. This test proves the
//! integration actually works end to end through `QueryEngine::range`, not
//! just inside `ravel-promql`'s own unit tests.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{EngineConfig, QueryEngine, QueryError};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{CommitToken, Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const SAMPLE_TS_NS: i64 = 1_000 * NS;

/// Writes one real RSEG segment carrying a single `up` sample and publishes
/// its commit record onto `store`, returning the read-your-write token.
/// Mirrors `deadline_cancels_fetch.rs`'s `publish_one_segment` helper.
async fn publish_up_segment(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
) -> CommitToken {
    let writer_id = Uuid::from_u128(11);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: "up".to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, "up", &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample {
            ts_ns: SAMPLE_TS_NS,
            value: 1.0,
        }],
    };
    let written: WrittenSegment =
        SegmentWriter::write(vec![input], identity, bounds).expect("write segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 42,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish")
}

/// `max_over_time(up[1000s:1s])` over a 1000-step outer range (1s step)
/// re-evaluates its own ~1000-point inner subquery from scratch at every
/// outer step: ~1,000,000 evaluation points total, comfortably under both
/// the per-node `max_range_points` cap and the shared cross-level budget
/// (default 1,000,000), so nothing rejects it outright -- measured in
/// isolation (`ravel-promql`'s own `short_deadline_cancels_a_long_running_
/// nested_subquery_evaluation` test, same debug build) at just over 1.5s to
/// run to completion.
///
/// With a 20ms deadline, `QueryEngine::range`'s evaluator must notice and
/// stop long before that -- not after running the full computation and
/// only then reporting a timeout. `elapsed` is what tells the two apart: a
/// regression that dropped the evaluator's own deadline check would still
/// eventually return `DeadlineExceeded` (via the outer `tokio::time::
/// timeout`), but only after the full ~1.5s of synchronous work, since
/// nothing inside that work would ever yield or check the deadline itself.
#[tokio::test]
async fn short_deadline_cancels_nested_subquery_reevaluation_through_the_engine() {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();

    let store = MemoryStore::new();
    let token = publish_up_segment(&store, &tenant_id, tenant_hash).await;
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, backend, EngineConfig::default());

    let start_ms = 1_000_000;
    let end_ms = start_ms + 999_000;
    let step_ms = 1_000;

    let start = Instant::now();
    let result = engine
        .range(
            tenant_hash,
            "max_over_time(up[1000s:1s])",
            start_ms,
            end_ms,
            step_ms,
            &[token],
            SAMPLE_TS_NS,
            Duration::from_millis(20),
        )
        .await;
    let elapsed = start.elapsed();

    assert!(
        matches!(result, Err(QueryError::DeadlineExceeded { .. })),
        "expected DeadlineExceeded, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "the query must be cancelled soon after the 20ms deadline, not after \
         running the full ~1,000,000-point computation (over 1.5s in this \
         same debug build); took {elapsed:?}"
    );
}
