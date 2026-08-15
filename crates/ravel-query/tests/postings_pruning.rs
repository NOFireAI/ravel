//! Postings-based segment
//! pruning at query time, applied only to snapshot-sourced segments behind
//! an equality `__name__` matcher, and the `QueryStats` counters it feeds.
//!
//! Every test folds real RSEG segments into a snapshot first (`now_at_seal`
//! puts the ingest hour at or below the watermark), so pruning is actually
//! reachable: an unfolded, listing-only snapshot is structurally immune to
//! it and would make these tests tautological.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, decode_head,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine, QueryError};
use ravel_segment::{
    IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V6, WrittenSegment,
};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// First instant at which `hour` is sealed (past its flush lifetime, clock
/// skew allowance, and fold safety margin), mirroring
/// `postings_exactness.rs` and `snapshot_resolve.rs`.
fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn tenant(id: &str) -> TenantId {
    TenantId::new(id.to_string())
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series_input(tenant_id: &TenantId, metric: &str, ts_ns: i64, value: f64) -> SeriesInput {
    let label_set = labels_for(metric);
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value }],
    }
}

/// Writes one real RSEG v6 segment and publishes its commit record,
/// mirroring `postings_exactness.rs`'s `publish_real_segment`. ADR-0027 left
/// v6 the only version; the trailing `_use_v2` flag is retained (ignored) so
/// the many call sites need not change.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    writer_id: Uuid,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    _use_v2: bool,
    inputs: Vec<SeriesInput>,
) {
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written: WrittenSegment =
        SegmentWriter::write(inputs, identity, bounds).expect("write v6 segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V6),
        created_unix_ns: 0,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

fn catalog(store: Arc<dyn ObjectStoreBackend>) -> Catalog {
    Catalog::new(store, CatalogConfig::default()).expect("catalog")
}

/// Sorted `(metric_name, value_bits)` pairs of an instant vector, for
/// order-independent, exact (bit-level) comparisons.
fn vector_bits(value: &Value) -> Vec<(String, u64)> {
    match value {
        Value::Vector(v) => {
            let mut out: Vec<(String, u64)> = v
                .iter()
                .map(|s| {
                    let name = s.labels.get(METRIC_NAME_LABEL).unwrap_or("").to_string();
                    (name, s.value.to_bits())
                })
                .collect();
            out.sort();
            out
        }
        other => panic!("expected instant vector, got {other:?}"),
    }
}

/// Pruned (equality `__name__`) and bypassed (regex `__name__`) queries over
/// the same fold must return bit-identical results, across both RSEG
/// formats and a genuine cross-segment duplicate sample (same series,
/// different segments, one v1 one v2): pruning may only ever narrow which
/// segments are *fetched*, never change which sample wins dedup.
#[tokio::test]
async fn pruned_and_bypassed_queries_are_bit_identical_across_formats_and_duplicates() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 5_000u32;
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;
    let t_ms = ts / 1_000_000;

    // Seg A (v1, writer_seq 1) and seg B (v2, writer_seq 2) carry the SAME
    // series (same metric, same labels -> same series id) at the SAME
    // timestamp: a genuine cross-format, cross-segment duplicate sample.
    // Total order picks the higher writer_seq, so B's value must win.
    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "target_metric", ts, 1.0)],
    )
    .await;
    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        2,
        hour,
        true,
        vec![series_input(&tid, "target_metric", ts, 2.0)],
    )
    .await;
    // Seg C: unrelated metric only. Must be pruned for the equality query,
    // fetched for the bypassed one.
    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "other_metric", ts, 99.0)],
    )
    .await;

    let catalog = catalog(store.clone());
    let report = catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    assert!(
        report.postings_built,
        "postings must build for this test to exercise pruning at all"
    );

    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());

    let (pruned_value, pruned_stats) = engine
        .instant_with_stats(th, "target_metric", t_ms, &[], now, Duration::from_secs(5))
        .await
        .expect("equality query");
    assert_eq!(pruned_stats.segments_fetched, 2, "seg A and seg B only");
    assert_eq!(pruned_stats.segments_pruned, 1, "seg C pruned");

    let (bypassed_value, bypassed_stats) = engine
        .instant_with_stats(
            th,
            "{__name__=~\"target_metric\"}",
            t_ms,
            &[],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect("regex-bypassed query");
    assert_eq!(
        bypassed_stats.segments_fetched, 3,
        "regex matcher bypasses pruning: all three segments fetched"
    );
    assert_eq!(bypassed_stats.segments_pruned, 0);

    let expected = vec![("target_metric".to_string(), 2.0f64.to_bits())];
    assert_eq!(vector_bits(&pruned_value), expected);
    assert_eq!(vector_bits(&bypassed_value), expected);
}

/// A negative `__name__` matcher must bypass pruning entirely, even though
/// a segment lacking the compared-against name legitimately exists (so
/// pruning-by-absence could otherwise seem to "work" here by accident).
#[tokio::test]
async fn negative_name_matcher_bypasses_pruning() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 5_001u32;
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;
    let t_ms = ts / 1_000_000;

    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "target_metric", ts, 1.0)],
    )
    .await;
    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "other_metric", ts, 2.0)],
    )
    .await;

    let catalog = catalog(store.clone());
    catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");

    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    // A second, non-empty matcher is required alongside the negative
    // `__name__` matcher: a `!=` matcher alone also matches a missing
    // label, so promql-parser rejects it as "must contain at least one
    // non-empty matcher" (mirrors real Prometheus selector validation).
    // Irrelevant to what it matches here, since this test only asserts
    // segment counters, not query results.
    let (_value, stats) = engine
        .instant_with_stats(
            th,
            "{__name__!=\"other_metric\",unused=\"x\"}",
            t_ms,
            &[],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect("negative-matcher query");
    assert_eq!(
        stats.segments_pruned, 0,
        "negative __name__ matcher must bypass pruning"
    );
    assert_eq!(stats.segments_fetched, 2);
}

/// A selector with no `__name__` matcher at all must bypass pruning.
#[tokio::test]
async fn absent_name_matcher_bypasses_pruning() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 5_002u32;
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;
    let t_ms = ts / 1_000_000;

    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "target_metric", ts, 1.0)],
    )
    .await;
    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "other_metric", ts, 2.0)],
    )
    .await;

    let catalog = catalog(store.clone());
    catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");

    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    // No `__name__` matcher at all: matches by label alone, or nothing.
    let (_value, stats) = engine
        .instant_with_stats(
            th,
            "{instance=\"anything\"}",
            t_ms,
            &[],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect("no-name-matcher query");
    assert_eq!(
        stats.segments_pruned, 0,
        "a selector without a __name__ matcher must bypass pruning"
    );
    assert_eq!(stats.segments_fetched, 2);
}

/// Missing postings (object GET returns NotFound) must degrade to no
/// pruning, never fail the query.
#[tokio::test]
async fn missing_postings_degrade_to_no_pruning() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 5_003u32;
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;
    let t_ms = ts / 1_000_000;

    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "target_metric", ts, 1.0)],
    )
    .await;
    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "other_metric", ts, 2.0)],
    )
    .await;

    let catalog = catalog(store.clone());
    catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");

    let head_bytes = store
        .get(&head_key(&th, Signal::Metrics), GetRange::Full)
        .await
        .expect("get head");
    let head = decode_head(&head_bytes.data).expect("decode head");
    let postings_key = head.postings.expect("postings ref present").key;
    store.delete(&postings_key).await.expect("delete postings");

    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    let (value, stats) = engine
        .instant_with_stats(th, "target_metric", t_ms, &[], now, Duration::from_secs(5))
        .await
        .expect("query must still succeed with postings missing");
    assert_eq!(
        stats.segments_pruned, 0,
        "missing postings must degrade to no pruning, not fail"
    );
    assert_eq!(stats.segments_fetched, 2, "both segments resolved unpruned");
    assert_eq!(
        vector_bits(&value),
        vec![("target_metric".to_string(), 1.0f64.to_bits())]
    );
}

/// Corrupt postings bytes (fails decode, or its CRC/hash checks) must
/// degrade to no pruning, never fail the query or return wrong data.
#[tokio::test]
async fn corrupt_postings_degrade_to_no_pruning() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 5_004u32;
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;
    let t_ms = ts / 1_000_000;

    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "target_metric", ts, 1.0)],
    )
    .await;
    publish_segment(
        store.as_ref(),
        th,
        Uuid::new_v4(),
        1,
        hour,
        false,
        vec![series_input(&tid, "other_metric", ts, 2.0)],
    )
    .await;

    let catalog = catalog(store.clone());
    catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");

    let head_bytes = store
        .get(&head_key(&th, Signal::Metrics), GetRange::Full)
        .await
        .expect("get head");
    let head = decode_head(&head_bytes.data).expect("decode head");
    let postings_key = head.postings.expect("postings ref present").key;

    let mut corrupt = store
        .get(&postings_key, GetRange::Full)
        .await
        .expect("get postings")
        .data
        .to_vec();
    // Flip a byte well past the fixed header (magic/version/lengths), deep
    // enough in the body to corrupt content without accidentally producing
    // another validly-shaped (but wrong) header.
    let flip_at = corrupt.len() - 1;
    corrupt[flip_at] ^= 0xFF;
    store
        .put(&postings_key, corrupt.into(), PutOptions::default())
        .await
        .expect("overwrite postings with corrupt bytes");

    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    let (value, stats) = engine
        .instant_with_stats(th, "target_metric", t_ms, &[], now, Duration::from_secs(5))
        .await
        .expect("query must still succeed with postings corrupt");
    assert_eq!(
        stats.segments_pruned, 0,
        "corrupt postings must degrade to no pruning, not fail"
    );
    assert_eq!(stats.segments_fetched, 2, "both segments resolved unpruned");
    assert_eq!(
        vector_bits(&value),
        vec![("target_metric".to_string(), 1.0f64.to_bits())]
    );
}

/// `max_segments` is enforced against the post-pruning segment count: an
/// equality `__name__` query that prunes below the budget must succeed even
/// when the unpruned (bypassed) snapshot would have exceeded it.
#[tokio::test]
async fn pruning_rescues_a_query_from_the_segment_budget() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 5_005u32;
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;
    let t_ms = ts / 1_000_000;

    // Six segments, six distinct metrics, one of them "target_metric".
    let metrics = [
        "target_metric",
        "metric_b",
        "metric_c",
        "metric_d",
        "metric_e",
        "metric_f",
    ];
    for (i, metric) in metrics.iter().enumerate() {
        publish_segment(
            store.as_ref(),
            th,
            Uuid::new_v4(),
            1,
            hour,
            false,
            vec![series_input(&tid, metric, ts, i as f64)],
        )
        .await;
    }

    let catalog = catalog(store.clone());
    catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");

    let config = EngineConfig {
        max_segments: 3,
        ..EngineConfig::default()
    };
    let engine = QueryEngine::new(Arc::new(catalog), store, config);

    // Bypassed (regex): all 6 segments resolve, unpruned, over budget.
    let err = engine
        .instant_with_stats(
            th,
            "{__name__=~\".+\"}",
            t_ms,
            &[],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect_err("6 unpruned segments must exceed a max_segments of 3");
    match err {
        QueryError::TooManySegments { count, max } => {
            assert_eq!(count, 6);
            assert_eq!(max, 3);
        }
        other => panic!("expected TooManySegments, got {other:?}"),
    }

    // Equality: prunes down to the 1 matching segment, well under budget.
    let (value, stats) = engine
        .instant_with_stats(th, "target_metric", t_ms, &[], now, Duration::from_secs(5))
        .await
        .expect("pruned query must succeed under the same budget");
    assert_eq!(stats.segments_fetched, 1);
    assert_eq!(stats.segments_pruned, 5);
    assert_eq!(
        vector_bits(&value),
        vec![("target_metric".to_string(), 0.0f64.to_bits())]
    );
}
