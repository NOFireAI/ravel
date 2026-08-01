//! RSPAN determinism: the same L0 `.rspan` inputs, compacted twice from
//! scratch, produce the same compaction record bytes, the same part object
//! keys, and the same part bytes (the span analogue of `rlog_determinism.rs`
//! and `determinism.rs`).
//!
//! This is the test that exercises the merge's `(trace_id, start_ts)` tie-break.
//! The `SpanCodec` groups records by trace_id and pushes them into the writer in
//! canonical input order; the writer stable-sorts by `(trace_id, start_ts)`, so
//! when two spans across different inputs share the same `(trace_id, start_ts)`
//! the tie is broken by the order the inputs were visited, which is their
//! canonical order. If that order were not deterministic, the two runs' blocks
//! would differ and the part bytes would diverge; asserting byte-identical
//! output across two independent runs pins the behaviour that crash-recovery
//! convergence depends on (plan §3.4).
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::{sealed_now_ns, seed_rspan_input, span_record, spans_bucket};
use prost::Message;
use ravel_commit::keys;
use ravel_maintain::{CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_rspan::SpanRecord;
use uuid::Uuid;

const EPOCH: u64 = 10;

/// Three L0 inputs seeded so several spans across *different* inputs share the
/// same `(trace_id, start_ts)` pair, forcing the merge's stable tie-break to
/// matter:
///
/// - trace 0, start 1000 is carried by all three inputs (span ids 1, 2, 3) --
///   three records that tie on `(trace_id, start_ts)`;
/// - trace 1, start 1500 is carried by inputs A and B -- two more that tie.
///
/// No dedup happens (distinct span ids are distinct records), so all of them
/// survive into the L1 output and their relative order is decided purely by the
/// tie-break. Distinct writer ids/seqs give the inputs a canonical order.
fn seed_specs() -> Vec<(Uuid, u64, Vec<SpanRecord>)> {
    vec![
        (
            Uuid::from_u128(42),
            1,
            vec![
                span_record(0, 1, 1000, 1100),
                span_record(0, 4, 2000, 2100),
                span_record(1, 1, 1500, 1600),
            ],
        ),
        (
            Uuid::from_u128(7),
            2,
            vec![span_record(0, 2, 1000, 1100), span_record(1, 2, 1500, 1600)],
        ),
        (
            Uuid::from_u128(7),
            1,
            vec![span_record(0, 3, 1000, 1100), span_record(2, 1, 500, 600)],
        ),
    ]
}

/// Compact a fresh store seeded from [`seed_specs`] and return the compaction
/// record key, its encoded bytes, and every `(part key, part bytes)`.
async fn compact_once() -> (String, Vec<u8>, Vec<(String, Vec<u8>)>) {
    let store = MemoryStore::new();
    for (writer_id, seq, records) in seed_specs() {
        seed_rspan_input(&store, writer_id, EPOCH, seq, &records).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = spans_bucket();
    compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("compact");

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
    let record_bytes = store
        .get(&record_key, GetRange::Full)
        .await
        .unwrap()
        .data
        .to_vec();
    let record =
        ravel_proto::commit::v1::CompactionRecord::decode(record_bytes.as_slice()).unwrap();
    let mut parts = Vec::new();
    for p in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, p).unwrap();
        let bytes = store.get(&key, GetRange::Full).await.unwrap().data.to_vec();
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
        assert_eq!(
            ba, bb,
            "part bytes deterministic (stable (trace_id, start_ts) tie-break over canonical input order)"
        );
    }
    assert!(!parts_a.is_empty());
}
