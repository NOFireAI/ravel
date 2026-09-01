//! Determinism: the same inputs, compacted twice, produce the same part
//! bytes, the same object keys, and the same record bytes (the
//! record CreateIfAbsent is the serialization point, but canonical orders are
//! still specified so identical builds converge on identical keys and the
//! race costs nothing).
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use prost::Message;
use ravel_maintain::{CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

fn specs() -> Vec<InputSpec> {
    vec![
        InputSpec::new(
            Uuid::from_u128(42),
            5,
            1,
            vec![
                raw_series("m", &[("k", "b")], &[(1_000, 1.0), (5_000, -0.0)]),
                raw_series("m", &[("k", "a")], &[(2_000, f64::NAN)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(7),
            5,
            2,
            vec![raw_series("m", &[("k", "a")], &[(3_000, 2.5)])],
        ),
        InputSpec::new(
            Uuid::from_u128(7),
            5,
            1,
            vec![raw_series("m", &[("k", "c")], &[(4_000, 9.0)])],
        ),
    ]
}

async fn compact_once() -> (String, Vec<u8>, Vec<(String, Vec<u8>)>) {
    let store = MemoryStore::new();
    for s in &specs() {
        seed_input(&store, s).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("compact");

    // The record key, encoded record bytes, and every (part key, part bytes).
    use ravel_commit::keys;
    use ravel_object_store::list_all;
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    let record_key = list_all(&store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .find(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .expect("record key");
    let record_bytes = get_full(&store, &record_key).await.to_vec();
    let record =
        ravel_proto::commit::v1::CompactionRecord::decode(record_bytes.as_slice()).unwrap();
    let mut parts = Vec::new();
    for p in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, p).unwrap();
        let bytes = get_full(&store, &key).await.to_vec();
        parts.push((key, bytes));
    }
    (record_key, record_bytes, parts)
}

#[tokio::test]
async fn same_inputs_same_bytes_and_keys() {
    let (rk_a, rb_a, parts_a) = compact_once().await;
    let (rk_b, rb_b, parts_b) = compact_once().await;

    assert_eq!(rk_a, rk_b, "record key deterministic");
    assert_eq!(rb_a, rb_b, "record bytes deterministic");
    assert_eq!(parts_a.len(), parts_b.len());
    for ((ka, ba), (kb, bb)) in parts_a.iter().zip(parts_b.iter()) {
        assert_eq!(ka, kb, "part key deterministic");
        assert_eq!(ba, bb, "part bytes deterministic (verbatim page copy)");
    }
    assert!(!parts_a.is_empty());
}
