//! Roundtrip and determinism property tests for the snapshot part envelope
//! and HEAD record codec.

#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_catalog::{
    NamePostings, PartLimits, PostingsLimits, decode_head, decode_part, decode_postings,
    encode_head, encode_part, encode_postings,
};
use ravel_proto::catalog::v1::{
    SnapshotColumnStatsRef, SnapshotEntry, SnapshotHead, SnapshotPartRef, SnapshotPostingsRef,
};

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

fn arb_postings_ref(
    parts: &[SnapshotPartRef],
) -> impl Strategy<Value = Option<SnapshotPostingsRef>> + use<> {
    let part_blake3: Vec<Vec<u8>> = parts.iter().map(|p| p.blake3.clone()).collect();
    prop_oneof![
        Just(None),
        (
            "[a-z0-9/]{1,40}",
            prop::collection::vec(any::<u8>(), 32),
            any::<u64>(),
            any::<u32>(),
        )
            .prop_map(move |(key, blake3, size, name_count)| {
                Some(SnapshotPostingsRef {
                    key,
                    blake3,
                    size,
                    name_count,
                    part_blake3: part_blake3.clone(),
                })
            }),
    ]
}

fn arb_column_stats_ref(
    parts: &[SnapshotPartRef],
) -> impl Strategy<Value = Option<SnapshotColumnStatsRef>> + use<> {
    let part_blake3: Vec<Vec<u8>> = parts.iter().map(|p| p.blake3.clone()).collect();
    prop_oneof![
        Just(None),
        (
            "[a-z0-9/]{1,40}",
            prop::collection::vec(any::<u8>(), 32),
            any::<u64>(),
            any::<u32>(),
        )
            .prop_map(move |(key, blake3, size, segment_count)| {
                Some(SnapshotColumnStatsRef {
                    key,
                    blake3,
                    size,
                    segment_count,
                    part_blake3: part_blake3.clone(),
                })
            }),
    ]
}

fn arb_head() -> impl Strategy<Value = SnapshotHead> {
    // Each raw part carries a (gap, span) plus its arbitrary content fields.
    // `arb_head` folds the gaps/spans into ascending, strictly-disjoint hour
    // ranges ([min_hour, watermark_hour], each min_hour > the previous
    // part's watermark_hour) so a multi-part head satisfies `validate_head`'s
    // ordering, disjointness, and per-part-sanity rules.
    (
        prop::collection::vec(
            (
                0u32..5,
                0u32..10,
                "[a-z0-9/]{1,40}",
                prop::collection::vec(any::<u8>(), 32),
                any::<u64>(),
                any::<u64>(),
            ),
            1..5,
        ),
        prop::collection::vec(any::<u8>(), 16),
        any::<i64>(),
    )
        .prop_flat_map(|(raw_parts, folder_id, created_unix_ns)| {
            let mut cursor = 0u32;
            let parts: Vec<SnapshotPartRef> = raw_parts
                .into_iter()
                .map(|(gap, span, key, blake3, size, entry_count)| {
                    let min_hour = cursor + gap;
                    let watermark_hour = min_hour + span;
                    cursor = watermark_hour + 1;
                    SnapshotPartRef {
                        key,
                        blake3,
                        size,
                        entry_count,
                        watermark_hour,
                        min_hour,
                    }
                })
                .collect();
            (arb_postings_ref(&parts), arb_column_stats_ref(&parts)).prop_map(
                move |(postings, column_stats)| {
                    let watermark_hour = parts.iter().map(|p| p.watermark_hour).max().unwrap_or(0);
                    SnapshotHead {
                        format_version: 1,
                        tenant_hash: vec![0x33u8; 16],
                        signal: 1,
                        shard_count: 8,
                        watermark_hour,
                        parts: parts.clone(),
                        postings,
                        folder_id: folder_id.clone(),
                        created_unix_ns,
                        shard_generation_count: 1,
                        column_stats,
                        column_stats_part: None,
                    }
                },
            )
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

fn watermark_and_names() -> impl Strategy<Value = (u64, Vec<NamePostings>)> {
    (1u64..50).prop_flat_map(|entry_count| {
        prop::collection::btree_map(
            "[a-z]{1,8}",
            prop::collection::btree_set(0..entry_count, 0..6),
            0..8,
        )
        .prop_map(move |map| {
            let names = map
                .into_iter()
                .map(|(name, ordinals)| NamePostings {
                    name,
                    ordinals: ordinals.into_iter().collect(),
                })
                .collect();
            (entry_count, names)
        })
    })
}

proptest! {
    #[test]
    fn postings_roundtrip((entry_count, names) in watermark_and_names()) {
        let tenant_hash = [0x11u8; 16];
        let part_blake3 = [[0x22u8; 32]];
        let encoded = encode_postings(tenant_hash, 1, &part_blake3, entry_count, &names)
            .expect("encode");
        let decoded = decode_postings(&encoded, &PostingsLimits::default(), &part_blake3)
            .expect("decode");

        prop_assert_eq!(decoded.header.format_version, 1);
        prop_assert_eq!(decoded.header.tenant_hash, tenant_hash.to_vec());
        prop_assert_eq!(decoded.header.signal, 1);
        prop_assert_eq!(decoded.header.entry_count, entry_count);
        prop_assert_eq!(decoded.header.name_count, names.len() as u32);
        prop_assert_eq!(decoded.names, names);
    }

    #[test]
    fn postings_encode_is_deterministic((entry_count, names) in watermark_and_names()) {
        let tenant_hash = [0x22u8; 16];
        let part_blake3 = [[0x33u8; 32]];
        let a = encode_postings(tenant_hash, 1, &part_blake3, entry_count, &names)
            .expect("encode a");
        let b = encode_postings(tenant_hash, 1, &part_blake3, entry_count, &names)
            .expect("encode b");
        prop_assert_eq!(a, b);
    }
}

/// A part mixing an L0 entry (16-byte writer_id) and an L1 compaction-part
/// entry (level 1, 32-byte writer_id holding the input_set_hash) encodes and
/// decodes byte-for-byte, proving the level-dependent field-length rule
/// accepts both shapes in one part.
#[test]
fn mixed_l0_l1_part_roundtrips() {
    let l0 = SnapshotEntry {
        level: 0,
        shard: 0,
        ingest_hour_bucket: 5,
        writer_id: vec![0xAA; 16],
        writer_epoch: 1,
        writer_seq: 1,
        content_hash: vec![0xBB; 32],
        object_size: 100,
        min_event_ts_ns: 0,
        max_event_ts_ns: 100,
        sample_count: 1,
        series_count: 1,
        segment_format_version: 5,
        created_unix_ns: 1_000,
    };
    let l1 = SnapshotEntry {
        level: 1,
        shard: 0,
        ingest_hour_bucket: 5,
        // L1: writer_id carries the 32-byte input_set_hash, writer_epoch the
        // part_index.
        writer_id: vec![0xCC; 32],
        writer_epoch: 0,
        writer_seq: 0,
        content_hash: vec![0xDD; 32],
        object_size: 4096,
        min_event_ts_ns: 0,
        max_event_ts_ns: 100,
        sample_count: 10,
        series_count: 2,
        segment_format_version: 5,
        created_unix_ns: 2_000,
    };
    // Sorted by (hour, shard, writer_id bytes, ...): the 16-byte L0 writer_id
    // sorts before the 32-byte L1 one.
    let entries = vec![l0, l1];
    let encoded = encode_part([0x11; 16], 1, 8, 5, &entries).expect("encode");
    let decoded = decode_part(&encoded, &PartLimits::default()).expect("decode");
    assert_eq!(decoded.entries, entries);
}
