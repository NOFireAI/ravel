//! Roundtrip and determinism property tests for the snapshot part envelope
//! and HEAD record codec (docs/metric-index-plan.md 3.1, 3.2, P1 tests).

#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_catalog::{PartLimits, decode_head, decode_part, encode_head, encode_part};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};

fn entry_key(e: &SnapshotEntry) -> (u32, u32, Vec<u8>, u64, u64) {
    (
        e.ingest_hour_bucket,
        e.shard,
        e.writer_id.clone(),
        e.writer_epoch,
        e.writer_seq,
    )
}

fn arb_entry(watermark_hour: u32) -> impl Strategy<Value = SnapshotEntry> {
    (
        (
            0..=watermark_hour,
            0u32..8,
            prop::collection::vec(any::<u8>(), 16),
            any::<u64>(),
            any::<u64>(),
            prop::collection::vec(any::<u8>(), 32),
            any::<u64>(),
        ),
        (
            any::<i64>(),
            any::<i64>(),
            any::<u64>(),
            any::<u64>(),
            0u32..4,
            any::<i64>(),
        ),
    )
        .prop_map(
            |(
                (
                    ingest_hour_bucket,
                    shard,
                    writer_id,
                    writer_epoch,
                    writer_seq,
                    content_hash,
                    object_size,
                ),
                (
                    min_event_ts_ns,
                    max_event_ts_ns,
                    sample_count,
                    series_count,
                    segment_format_version,
                    created_unix_ns,
                ),
            )| SnapshotEntry {
                level: 0,
                shard,
                ingest_hour_bucket,
                writer_id,
                writer_epoch,
                writer_seq,
                content_hash,
                object_size,
                min_event_ts_ns,
                max_event_ts_ns,
                sample_count,
                series_count,
                segment_format_version,
                created_unix_ns,
            },
        )
}

fn watermark_and_entries() -> impl Strategy<Value = (u32, Vec<SnapshotEntry>)> {
    (0u32..1000).prop_flat_map(|watermark_hour| {
        prop::collection::vec(arb_entry(watermark_hour), 0..40).prop_map(move |mut entries| {
            entries.sort_by_key(entry_key);
            entries.dedup_by_key(|e| entry_key(e));
            (watermark_hour, entries)
        })
    })
}

proptest! {
    #[test]
    fn part_roundtrip((watermark_hour, entries) in watermark_and_entries()) {
        let tenant_hash = [0x11u8; 16];
        let encoded = encode_part(tenant_hash, 1, 8, watermark_hour, &entries).expect("encode");
        let decoded = decode_part(&encoded, &PartLimits::default()).expect("decode");

        prop_assert_eq!(decoded.header.format_version, 1);
        prop_assert_eq!(decoded.header.tenant_hash, tenant_hash.to_vec());
        prop_assert_eq!(decoded.header.signal, 1);
        prop_assert_eq!(decoded.header.shard_count, 8);
        prop_assert_eq!(decoded.header.watermark_hour, watermark_hour);
        prop_assert_eq!(decoded.header.entry_count, entries.len() as u64);
        prop_assert_eq!(decoded.entries, entries);
    }

    #[test]
    fn part_encode_is_deterministic((watermark_hour, entries) in watermark_and_entries()) {
        let tenant_hash = [0x22u8; 16];
        let a = encode_part(tenant_hash, 1, 8, watermark_hour, &entries).expect("encode a");
        let b = encode_part(tenant_hash, 1, 8, watermark_hour, &entries).expect("encode b");
        prop_assert_eq!(a, b);
    }
}

fn arb_part_ref() -> impl Strategy<Value = SnapshotPartRef> {
    (
        "[a-z0-9/]{1,40}",
        prop::collection::vec(any::<u8>(), 32),
        any::<u64>(),
        any::<u64>(),
        0u32..1000,
    )
        .prop_map(
            |(key, blake3, size, entry_count, watermark_hour)| SnapshotPartRef {
                key,
                blake3,
                size,
                entry_count,
                watermark_hour,
            },
        )
}

fn arb_head() -> impl Strategy<Value = SnapshotHead> {
    (
        prop::collection::vec(arb_part_ref(), 1..5),
        prop::collection::vec(any::<u8>(), 16),
        any::<i64>(),
    )
        .prop_map(|(parts, folder_id, created_unix_ns)| {
            let watermark_hour = parts.iter().map(|p| p.watermark_hour).max().unwrap_or(0);
            SnapshotHead {
                format_version: 1,
                tenant_hash: vec![0x33u8; 16],
                signal: 1,
                shard_count: 8,
                watermark_hour,
                parts,
                folder_id,
                created_unix_ns,
            }
        })
}

proptest! {
    #[test]
    fn head_roundtrip(head in arb_head()) {
        let encoded = encode_head(&head).expect("encode");
        let decoded = decode_head(&encoded).expect("decode");
        prop_assert_eq!(decoded, head);
    }

    #[test]
    fn head_encode_is_deterministic(head in arb_head()) {
        let a = encode_head(&head).expect("encode a");
        let b = encode_head(&head).expect("encode b");
        prop_assert_eq!(a, b);
    }
}
