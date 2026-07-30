//! In-process tests for `ravel-cli maintain` (P8): the subcommands drive
//! ravel-maintain against a shared MemoryStore. These exercise the CLI glue
//! and its output paths on an empty store (the compaction/sweep/retention
//! decision logic itself is tested in ravel-maintain); the decode tests pin
//! the proto field printing.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use prost::Message;
use ravel_cli::maintain::{
    SignalArg, audit_versions, compact, decode_compaction_record, decode_retention_tombstone,
    status, sweep,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, RetentionTombstone,
};

fn store() -> Arc<dyn ObjectStoreBackend> {
    Arc::new(MemoryStore::new())
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
            segment_format_version: 5,
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
