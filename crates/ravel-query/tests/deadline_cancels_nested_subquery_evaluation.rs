//! A nested-subquery query's deadline surfaces through `QueryEngine::range`:
//! when the evaluation cannot proceed, `QueryEngine`'s `tokio::time::timeout`
//! wrapper (engine.rs) ends the call with `QueryError::DeadlineExceeded`
//! rather than hanging.
//!
//! `ravel_promql::Evaluator` checks a wall-clock deadline
//! (`with_deadline`/`QueryWindow::check_deadline`) between subquery grid steps,
//! and `QueryEngine::{instant,range}_with_stats` derive that deadline from
//! their own `Duration` parameter and pass it into the evaluator before the
//! `tokio::time::timeout` wrapper ever polls it. That the evaluator yields
//! *mid-computation* at cost-budget checkpoints is `ravel-promql`'s own unit
//! test (`short_deadline_cancels_a_long_running_nested_subquery_evaluation`);
//! this test proves the deadline is wired end to end through
//! `QueryEngine::range`.
//!
//! Determinism (#706): the earlier version raced a 20ms wall deadline against
//! a deliberately heavy `max_over_time(up[1000s:1s])` (~1,000,000 evaluation
//! points, ~1.5s in this debug build) and asserted the query returned in under
//! 500ms. Under load that is a coin flip: on a 16-core host with 16 sibling
//! nextest binaries running, the assertion failed 2 of 2 in a full
//! `scripts/gates.sh` run yet passed 3 of 3 when the binary ran alone. The
//! outcome depended on how much CPU the box could spare, not on the engine.
//!
//! The fix removes the host-speed assumption entirely. The one segment the
//! query must read is parked inside a [`FaultStore`] hold gate
//! (`hold(Op::Get, "rseg", Always)`), so the segment data-object GET never
//! completes; commit-record reads (snapshot resolution) pass straight through,
//! so the query reaches the fetch and then cannot make progress. Nothing can
//! finish the evaluation, so the deadline is the only thing that can end the
//! wait, and it does regardless of how loaded the host is. The gate's held
//! count is the observable evidence that the read was parked (and, being
//! left held, cancelled) when the deadline fired -- the deterministic
//! replacement for the old timing assertion. Mirrors the parked-read pattern
//! in `deadline_cancels_fetch.rs` and `crates/ravel-sql/tests/deadline.rs`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
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

/// `max_over_time(up[100s:1s])` is a nested subquery: `QueryEngine::range`
/// re-evaluates the inner `up[100s:1s]` grid at each outer step. The query
/// needs the `up` segment, so it issues one data-object GET -- and that GET is
/// parked forever by the hold gate. With the read unable to complete, the
/// evaluation cannot make progress, so the deadline is the only thing that can
/// end the wait; the query must surface `DeadlineExceeded`.
///
/// The evidence that the read was genuinely parked (and, being left held,
/// cancelled by the deadline drop) is `gate.held_count() == 1`: the segment
/// GET registered inside the gate and was never released. Removing the gate
/// (see the module doc) lets the read complete, the small query finishes well
/// under the deadline, and the `DeadlineExceeded` assertion fails -- so this
/// test cannot pass vacuously. Refs: #706.
#[tokio::test]
async fn short_deadline_cancels_nested_subquery_reevaluation_through_the_engine() {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();

    // Publish onto a plain store, then wrap it so the segment data-object read
    // can be parked. Publishing through the wrapper first would park the
    // publish itself.
    let inner = MemoryStore::new();
    let token = publish_up_segment(&inner, &tenant_id, tenant_hash).await;

    let fault_store = FaultStore::new(inner, FaultPlan::default());
    // Park every segment data-object GET (keys carry `rseg`); commit-record
    // reads that resolve the snapshot pass straight through, so the query
    // reaches the fetch and then cannot proceed.
    let gate = fault_store.hold(Op::Get, Some("rseg".to_string()), Occurrence::Always);
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(fault_store);

    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, backend, EngineConfig::default());

    // A small outer range: when the read is NOT parked the whole query runs in
    // a couple of milliseconds, far under the deadline, so the red state (gate
    // removed) returns `Ok` and the error-variant assertion fails.
    let start_ms = 1_000_000;
    let end_ms = start_ms + 10_000;
    let step_ms = 1_000;

    let result = engine
        .range(
            tenant_hash,
            "max_over_time(up[100s:1s])",
            start_ms,
            end_ms,
            step_ms,
            &[token],
            SAMPLE_TS_NS,
            // Realistic once nothing can complete under it: the parked read
            // guarantees the deadline is what ends the wait, not the host's
            // speed.
            Duration::from_millis(50),
        )
        .await;

    assert!(
        matches!(result, Err(QueryError::DeadlineExceeded { .. })),
        "expected DeadlineExceeded, got {result:?}"
    );
    assert_eq!(
        gate.held_count(),
        1,
        "the nested subquery's segment read must have been parked in the gate \
         when the deadline fired: the deadline was the only thing that could \
         end the wait"
    );
}
