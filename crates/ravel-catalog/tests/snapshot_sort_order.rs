//! Regression test for a4-F01 (issue #52): the catalog snapshot segment sort
//! must be a deterministic total order.
//!
//! Adapted from the audit reproducer
//! (branch audit/repro-a4, crates/ravel-catalog/tests/audit_a4_repro.rs,
//! `a4_f01_snapshot_sort_key_is_not_a_total_order`), with the `#[ignore]`
//! removed and the assertion inverted: the reproducer asserted the tie was
//! observable; this asserts the fixed order is total and process-independent.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig, SegmentRef};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

fn tenant() -> TenantHash {
    TenantHash([0xab; 16])
}

/// Publish one segment with an explicit `writer_id` so the caller controls
/// which identity components collide. Distinct bytes per writer give a
/// distinct content hash and data key, i.e. genuinely different segments that
/// nonetheless share the provenance sort components.
#[allow(clippy::too_many_arguments)]
async fn publish_segment_with_writer(
    store: &MemoryStore,
    writer_id: Uuid,
    shard: u32,
    writer_epoch: u64,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
    min_event_ts_ns: i64,
    max_event_ts_ns: i64,
) -> CommitRecord {
    let payload = format!("segment-{shard}-{writer_id}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch,
        writer_seq,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    rec
}

/// a4-F01: the snapshot segment order must be a total order over distinct
/// segments and reproducible across independent resolves.
///
/// Two same-shard segments written by different writers tie on every
/// provenance component (`created_unix_ns`, `writer_epoch`, `writer_seq`).
/// Before the fix the sort key `(created_unix_ns, writer_epoch, writer_seq,
/// shard)` also tied, so `sort_by_key` (stable) left them in `HashMap`
/// iteration order, which the std `RandomState` reseeds per map: repeated
/// resolves (each building a fresh `HashMap`) could return the two segments
/// in different orders. After the fix the sort key appends `writer_id`, a
/// per-segment identity component, so the order is determined entirely by the
/// segments and is identical on every resolve and across processes.
#[tokio::test]
async fn a4_f01_snapshot_order_is_a_deterministic_total_order() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            ..Default::default()
        },
    )
    .expect("catalog");

    let now = 500_000 * NS_PER_HOUR;
    let hour = 500_000u32;
    // Same shard, epoch, seq, and created_unix_ns; only the writer_id differs.
    // seq is monotonic only per (writer_id, epoch, shard) and epoch is
    // unix-seconds, so this collision is reachable in production.
    let writer_a = Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
    let writer_b = Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);
    let epoch = 1u64;
    let seq = 1u64;

    publish_segment_with_writer(&store, writer_a, 0, epoch, seq, hour, now, now - 100, now).await;
    publish_segment_with_writer(&store, writer_b, 0, epoch, seq, hour, now, now - 100, now).await;

    let range = TimeRange {
        start_ns: now - 1_000,
        end_ns: now,
    };

    // The production sort key. No two distinct segments may compare equal
    // under it (total order), and it must not reference arrival/insertion or
    // map iteration order.
    let sort_key = |s: &SegmentRef| {
        (
            s.created_unix_ns,
            s.writer_epoch,
            s.writer_seq,
            s.shard,
            s.writer_id,
        )
    };

    // Resolve many times. Each resolve builds a fresh HashMap whose iteration
    // order the std RandomState reseeds, so a non-total sort would surface
    // both orders here; a total order pins one order on every resolve.
    let mut expected_order: Option<Vec<String>> = None;
    for _ in 0..64 {
        let snapshot = catalog
            .resolve(&tenant(), Signal::Metrics, range, &[], now)
            .await
            .expect("resolve");
        assert_eq!(snapshot.segments.len(), 2, "both segments must be visible");

        let s0 = &snapshot.segments[0];
        let s1 = &snapshot.segments[1];
        assert_ne!(
            s0.data_object_key, s1.data_object_key,
            "distinct writers produce distinct segments"
        );
        // No two distinct segments compare equal under the sort key.
        assert_ne!(
            sort_key(s0),
            sort_key(s1),
            "sort key must be a total order over distinct segments"
        );

        let order: Vec<String> = snapshot
            .segments
            .iter()
            .map(|s| s.data_object_key.clone())
            .collect();
        match &expected_order {
            None => expected_order = Some(order),
            Some(first) => assert_eq!(
                &order, first,
                "repeated resolves of one logical set must yield identical order"
            ),
        }
    }

    // The tiebreak is writer_id, ascending: writer_a (0x1111...) sorts before
    // writer_b (0x2222...). This pins the direction of the total order.
    let final_snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(final_snapshot.segments[0].writer_id, writer_a);
    assert_eq!(final_snapshot.segments[1].writer_id, writer_b);
}
