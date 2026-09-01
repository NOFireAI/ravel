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
//! convergence depends on.
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

/// v3 coverage (ADR-0054): merge two inputs with disjoint service
/// names and span names, then prove the compacted output's BLOOM section and
/// `service_name` column were rebuilt from the merged union. A bloom or column
/// copied from one input would answer/decode correctly for only that input's
/// records; this seeds distinct services per input so a copy shows up as a
/// wrong membership answer or a wrong decoded value.
#[tokio::test]
async fn v3_bloom_and_service_name_column_rebuilt_from_union() {
    use common::{rspan_bloom_survives, span_record_with_service, span_service_name};
    use ravel_object_store::memory::MemoryStore;
    use ravel_rspan::{COL_NAME, COL_SERVICE_NAME, RspanConfig, RspanReader, SpanQuery};

    let store = MemoryStore::new();
    let a = vec![
        span_record_with_service(0, 0, 10, 20, "read", "checkout"),
        span_record_with_service(1, 0, 11, 21, "read", "checkout"),
    ];
    let b = vec![
        span_record_with_service(2, 0, 12, 22, "write", "payments"),
        span_record_with_service(3, 0, 13, 23, "write", "payments"),
    ];
    seed_rspan_input(&store, Uuid::from_u128(1), EPOCH, 1, &a).await;
    seed_rspan_input(&store, Uuid::from_u128(2), EPOCH, 2, &b).await;

    let clock = FixedClock::new(sealed_now_ns());
    let bucket = spans_bucket();
    compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("compact");

    // Read the single compaction record and its one part.
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    let rec_key = list_all(&store, &prefix)
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
    let rec_bytes = store.get(&rec_key, GetRange::Full).await.unwrap().data;
    let record = ravel_proto::commit::v1::CompactionRecord::decode(rec_bytes.as_ref()).unwrap();
    assert_eq!(record.parts.len(), 1, "small corpus fits one part");
    let part_key = keys::reconstruct_l1_part_key(&record, &record.parts[0]).unwrap();
    let part = store.get(&part_key, GetRange::Full).await.unwrap().data;

    // Both inputs' service and span names are provably present in the rebuilt
    // bloom, not just the last-merged input's.
    for service in ["checkout", "payments"] {
        assert!(
            rspan_bloom_survives(part.as_ref(), COL_SERVICE_NAME, service),
            "merged bloom must answer membership for service {service}"
        );
    }
    for name in ["read", "write"] {
        assert!(
            rspan_bloom_survives(part.as_ref(), COL_NAME, name),
            "merged bloom must answer membership for span name {name}"
        );
    }
    // Negative controls: tokens in neither input are proven absent.
    assert!(
        !rspan_bloom_survives(part.as_ref(), COL_SERVICE_NAME, "billing"),
        "absent service must be pruned by the rebuilt bloom"
    );
    assert!(
        !rspan_bloom_survives(part.as_ref(), COL_NAME, "delete"),
        "absent span name must be pruned by the rebuilt bloom"
    );

    // The service_name column decodes each input's value post-merge.
    let cfg = RspanConfig::default();
    let reader = RspanReader::new(part.as_ref(), &cfg).expect("open part");
    let (rows, _) = reader
        .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
        .expect("scan");
    assert_eq!(rows.len(), 4, "all four merged spans present");
    for r in &rows {
        let want = if r.trace_id == [0u8; 16] || r.trace_id == [1u8; 16] {
            "checkout"
        } else {
            "payments"
        };
        assert_eq!(
            span_service_name(r),
            Some(want),
            "service_name column decodes per input for trace {:?}",
            r.trace_id
        );
    }
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
