//! In-process tests for `ravel-cli maintain` (P8): the subcommands drive
//! ravel-maintain against a shared MemoryStore. These exercise the CLI glue
//! and its output paths on an empty store (the compaction/sweep/retention
//! decision logic itself is tested in ravel-maintain); the decode tests pin
//! the proto field printing.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use ravel_cli::maintain::{
    SignalArg, audit_versions, compact, decode_compaction_record, decode_retention_tombstone,
    status, sweep, verify_custody,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, RetentionTombstone,
};
use ravel_segment::VERSION_V6;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

fn store() -> Arc<dyn ObjectStoreBackend> {
    Arc::new(MemoryStore::new())
}

/// Publish one L0 metrics segment (data object + commit record) and return the
/// content-addressed data-object key, so a test can later corrupt the object
/// at that key. Content is `seg-<shard>-<seq>`; its blake3 is what the key's
/// hash16 embeds, which is exactly what `verify-custody` re-derives.
async fn publish_l0(
    store: &MemoryStore,
    tenant: &str,
    shard: u32,
    seq: u64,
    created_unix_ns: i64,
) -> String {
    let tenant_hash = TenantId::new(tenant).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / 3_600_000_000_000).expect("fits u32");
    let payload = format!("seg-{shard}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id: Uuid::new_v4(),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_unix_ns - 1_000,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
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
    data_key
}

/// Seed one metrics compaction record (shard 0, hour 100) with a single L1
/// part written at its reconstructed key, plus the given input identities. The
/// L1 part's content matches its key's hash16 (so the part itself is never an
/// anomaly); whether each input object exists is up to the caller.
async fn seed_compaction(store: &MemoryStore, tenant: &str, inputs: &[(Uuid, u64, u64)]) {
    let tenant_hash = TenantId::new(tenant).hash();
    let part_payload = b"l1-part-content".to_vec();
    let part_hash = *blake3::hash(&part_payload).as_bytes();
    let part = CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0u8; 16],
        content_hash: part_hash.to_vec(),
        object_size: part_payload.len() as u64,
        sample_count: 1,
        series_count: 1,
        run_count: 1,
        min_event_ts_ns: 100,
        max_event_ts_ns: 200,
        segment_format_version: u32::from(VERSION_V6),
    };
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
        shard: 0,
        ingest_hour_bucket: 100,
        level: 1,
        inputs: inputs
            .iter()
            .map(|(id, epoch, seq)| CompactionInputIdentity {
                writer_id: id.to_string(),
                writer_epoch: *epoch,
                writer_seq: *seq,
            })
            .collect(),
        input_set_hash: vec![7u8; 32],
        parts: vec![part.clone()],
        created_unix_ns: 999,
    };
    let part_key = keys::reconstruct_l1_part_key(&record, &part).expect("part key");
    store
        .put(&part_key, Bytes::from(part_payload), PutOptions::default())
        .await
        .expect("put l1 part");
    let record_key = keys::compaction_record_key_for(&record).expect("record key");
    store
        .put(
            &record_key,
            Bytes::from(record.encode_to_vec()),
            PutOptions::default(),
        )
        .await
        .expect("put compaction record");
}

/// Put an L0 data object at the content-addressed key for `content_hash`, but
/// store `stored_content` as its bytes. When `stored_content` does not hash to
/// `content_hash`, the object's content no longer matches the hash16 its key
/// embeds: exactly the post-write corruption `verify-custody` must catch.
async fn put_l0_object_at(
    store: &MemoryStore,
    tenant: &str,
    shard: u32,
    identity: (Uuid, u64, u64),
    content_hash: [u8; 32],
    stored_content: &[u8],
) {
    let tenant_hash = TenantId::new(tenant).hash();
    let key = keys::data_key(
        &tenant_hash,
        Signal::Metrics,
        shard,
        identity.0,
        identity.1,
        identity.2,
        &content_hash,
    )
    .expect("data key");
    store
        .put(
            &key,
            Bytes::copy_from_slice(stored_content),
            PutOptions::default(),
        )
        .await
        .expect("put l0 object");
}

#[tokio::test]
async fn compact_empty_bucket_is_below_min() {
    // Hour 0 is long sealed; an empty bucket has zero inputs, below the
    // min-inputs trigger, so a dry run reports it and writes nothing.
    compact(store(), "acme", SignalArg::Metrics, 0, 0, true)
        .await
        .expect("compact dry-run runs");
}

#[tokio::test]
async fn sweep_empty_shard_dry_run_is_clean() {
    sweep(store(), "acme", SignalArg::Logs, 0, true)
        .await
        .expect("sweep dry-run runs");
}

#[tokio::test]
async fn status_empty_bucket_is_clean() {
    status(store(), "acme", SignalArg::Metrics, 0, 0)
        .await
        .expect("status runs");
}

#[tokio::test]
async fn audit_versions_empty_store_finds_no_anomaly() {
    audit_versions(store(), "acme", 4)
        .await
        .expect("audit over an empty store reports no live objects and no anomaly");
}

#[tokio::test]
async fn verify_custody_empty_store_is_clean() {
    verify_custody(store(), "acme", 4)
        .await
        .expect("empty store has no live objects and no anomaly");
}

#[tokio::test]
async fn verify_custody_clean_store_verifies_every_object() {
    let store = Arc::new(MemoryStore::new());
    publish_l0(&store, "acme", 0, 1, 100 * 3_600_000_000_000).await;
    publish_l0(&store, "acme", 0, 2, 100 * 3_600_000_000_000).await;
    // A compaction record whose L1 part is present and matches, with an input
    // that was legitimately swept (no L0 object seeded for it).
    seed_compaction(&store, "acme", &[(Uuid::new_v4(), 1, 1)]).await;

    verify_custody(store as Arc<dyn ObjectStoreBackend>, "acme", 1)
        .await
        .expect("a clean store passes custody verification");
}

#[tokio::test]
async fn verify_custody_catches_a_corrupted_data_object() {
    let store = Arc::new(MemoryStore::new());
    let data_key = publish_l0(&store, "acme", 0, 1, 100 * 3_600_000_000_000).await;
    // Overwrite the object's bytes so its content no longer hashes to the
    // hash16 embedded in its key: post-write corruption.
    store
        .put(
            &data_key,
            Bytes::from_static(b"corrupted-after-write"),
            PutOptions::default(),
        )
        .await
        .expect("overwrite the data object");

    let err = verify_custody(store as Arc<dyn ObjectStoreBackend>, "acme", 1)
        .await
        .expect_err("a corrupted data object must fail verification");
    assert!(
        err.to_string().contains("content-hash mismatch"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn verify_custody_treats_a_legitimately_swept_input_as_no_anomaly() {
    let store = Arc::new(MemoryStore::new());
    // A compaction record referencing an input identity for which no L0 object
    // exists: the sweeper reclaimed it past its protection horizon. The L1
    // part is present and matches. This must not be an anomaly.
    seed_compaction(&store, "acme", &[(Uuid::new_v4(), 1, 1)]).await;

    verify_custody(store as Arc<dyn ObjectStoreBackend>, "acme", 1)
        .await
        .expect("a swept (missing) compaction input is expected, not an anomaly");
}

#[tokio::test]
async fn verify_custody_catches_a_compaction_input_with_a_mismatched_hash() {
    let store = Arc::new(MemoryStore::new());
    let identity = (Uuid::new_v4(), 1, 1);
    // The input object exists but its content does not match the hash16 its
    // key embeds (the key is derived from `good`, the stored bytes are not).
    let good = b"good-input-content";
    let content_hash = *blake3::hash(good).as_bytes();
    put_l0_object_at(&store, "acme", 0, identity, content_hash, b"corrupted").await;
    seed_compaction(&store, "acme", &[identity]).await;

    let err = verify_custody(store as Arc<dyn ObjectStoreBackend>, "acme", 1)
        .await
        .expect_err("an existing-but-mismatched input must fail verification");
    assert!(
        err.to_string().contains("content-hash mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn decode_compaction_record_prints_fields() {
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: vec![0u8; 32],
        signal: 1,
        shard: 3,
        ingest_hour_bucket: 42,
        level: 1,
        inputs: vec![CompactionInputIdentity {
            writer_id: "00000000-0000-0000-0000-000000000001".to_string(),
            writer_epoch: 10,
            writer_seq: 1,
        }],
        input_set_hash: vec![0xabu8; 32],
        parts: vec![CompactionPart {
            part_index: 0,
            first_series_id: vec![1u8; 16],
            last_series_id: vec![2u8; 16],
            content_hash: vec![3u8; 32],
            object_size: 1234,
            sample_count: 5,
            series_count: 2,
            run_count: 3,
            min_event_ts_ns: 100,
            max_event_ts_ns: 200,
            segment_format_version: u32::from(VERSION_V6),
        }],
        created_unix_ns: 999,
    };
    decode_compaction_record(&record.encode_to_vec()).expect("decode + print");
}

#[test]
fn decode_retention_tombstone_prints_fields() {
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: vec![0u8; 32],
        signal: 2,
        shard: 1,
        ingest_hour_bucket: 7,
        retired_at_ns: 555,
        retention_window_ns: 2_592_000_000_000_000,
        record_count_observed: 12,
    };
    decode_retention_tombstone(&tombstone.encode_to_vec()).expect("decode + print");
}
