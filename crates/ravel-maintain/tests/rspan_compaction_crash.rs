//! Compaction-specific crash coverage for the RSPAN codec: the "crash after
//! parts written, before the record" case, exercised over a spans bucket.
//!
//! The signal-generic sweeper crash matrix (`sweep_crash_matrix.rs`) already
//! runs every sweeper row over a spans fixture, and the pipeline around the
//! codec (seal/trigger gating, `input_set_hash`, the record's `CreateIfAbsent`
//! publish/converge protocol, the abandonment deadline) is signal-agnostic and
//! proven for metrics in `crash_matrix.rs`. What is codec-specific for spans is
//! that its L1 parts are content-addressed and deterministic, so a re-run after
//! a mid-publish crash reuses the same part keys via `CreateIfAbsent` and then
//! publishes a record naming exactly those parts. This test pins that for spans
//! and verifies the re-published output decodes back to the full input union.
//!
//! (The metrics crash matrix asserts equivalence by decoding RSEG samples, a
//! path with no spans analogue; rather than clone that whole harness for a
//! third format, this focused test asserts the same convergence directly on the
//! RSPAN decode -- the deliberate choice the issue's test note allows.)
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::{sealed_now_ns, seed_rspan_input, span_record, spans_bucket};
use ravel_commit::keys;
use ravel_maintain::{Bucket, CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_rspan::{RspanConfig, RspanReader, SpanQuery, SpanRecord};
use uuid::Uuid;

const EPOCH: u64 = 10;

/// Two compactable L0 span inputs whose union spans three traces.
async fn seed_two(store: &dyn ObjectStoreBackend) -> (Bucket, Vec<SpanRecord>) {
    let a = vec![span_record(0, 0, 10, 20), span_record(1, 0, 5, 9)];
    let b = vec![span_record(0, 1, 15, 18), span_record(2, 0, 1, 2)];
    seed_rspan_input(store, Uuid::from_u128(1), EPOCH, 1, &a).await;
    seed_rspan_input(store, Uuid::from_u128(2), EPOCH, 2, &b).await;
    let mut all = a;
    all.extend(b);
    (spans_bucket(), all)
}

/// An order-independent canonical key for a span (attrs folded to a sorted map).
type Canon = ([u8; 16], [u8; 8], i64, i64, Vec<(String, String)>);
fn canon(r: &SpanRecord) -> Canon {
    let attrs: std::collections::BTreeMap<String, String> = r.attrs.iter().cloned().collect();
    (
        r.trace_id,
        r.span_id,
        r.start_ts_ns,
        r.end_ts_ns,
        attrs.into_iter().collect(),
    )
}
fn canon_multiset(records: &[SpanRecord]) -> Vec<Canon> {
    let mut v: Vec<Canon> = records.iter().map(canon).collect();
    v.sort();
    v
}

/// Decode every span across every L1 part the bucket's compaction record names.
async fn read_l1_union(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> Vec<SpanRecord> {
    use prost::Message;
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    let rec_key = list_all(store, &prefix)
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
        .expect("compaction record present");
    let bytes = store.get(&rec_key, GetRange::Full).await.unwrap().data;
    let record = ravel_proto::commit::v1::CompactionRecord::decode(bytes.as_ref()).unwrap();
    let cfg = RspanConfig::default();
    let mut out = Vec::new();
    for p in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, p).unwrap();
        let obj = store.get(&key, GetRange::Full).await.unwrap().data;
        let reader = RspanReader::new(obj.as_ref(), &cfg).expect("open l1 part");
        let (rows, _) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan l1 part");
        out.extend(rows);
    }
    out
}

async fn compaction_record_present(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> bool {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    list_all(store, &prefix).await.unwrap().iter().any(|m| {
        matches!(
            keys::partition_bucket_entry(&m.key),
            Ok(keys::BucketEntry::CompactionRecord(_))
        )
    })
}

/// Rows 2/3 over spans: a crash after the parts are written but before the
/// record. The record-less parts are invisible (no record references them); the
/// re-run reuses their content-addressed keys via `CreateIfAbsent` and then
/// publishes. The record-PUT fault fires once, and the re-published output
/// decodes to the full input union.
#[tokio::test]
async fn row2_3_crash_after_parts_before_record_spans() {
    let inner = MemoryStore::new();
    let (bucket, expected) = seed_two(&inner).await;
    // The compaction record key filename is `l1.<hash>.cmt`; L1 part keys never
    // contain the literal "l1." (their dir segment is "/l1/"). So this rule
    // targets only the record PUT.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Timeout)
            .with_key_contains("l1.")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store = FaultStore::new(inner, plan);
    let clock = FixedClock::new(sealed_now_ns());

    let first = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket).await;
    assert!(first.is_err(), "record PUT fault must fail the run");
    assert_eq!(store.fault_count(Op::Put, FaultKind::Timeout), 1);
    assert!(!compaction_record_present(&store, &bucket).await);

    let second = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("re-run reuses part keys and publishes");
    assert!(matches!(second, CompactionOutcome::Compacted { .. }));
    assert!(compaction_record_present(&store, &bucket).await);

    let l1 = read_l1_union(&store, &bucket).await;
    assert_eq!(
        canon_multiset(&l1),
        canon_multiset(&expected),
        "re-published L1 output equals the full input span union"
    );
}

/// v3 coverage over the crash path (ADR-0054): the re-published
/// part after a mid-publish crash is not merely record-complete, its BLOOM
/// section and `service_name` column are correct too. Seeds two inputs with
/// disjoint service names, so a bloom or column copied from a single input
/// (rather than rebuilt from the merged union) shows up as a wrong membership
/// answer or a wrong decoded value on the rebuilt part.
#[tokio::test]
async fn row2_3_rebuilds_bloom_and_service_name_column_after_crash() {
    use common::{rspan_bloom_survives, span_record_with_service, span_service_name};
    use prost::Message;
    use ravel_rspan::{COL_NAME, COL_SERVICE_NAME};

    let inner = MemoryStore::new();
    let bucket = spans_bucket();
    let a = vec![
        span_record_with_service(0, 0, 10, 20, "read", "checkout"),
        span_record_with_service(1, 0, 11, 21, "read", "checkout"),
    ];
    let b = vec![
        span_record_with_service(2, 0, 12, 22, "write", "payments"),
        span_record_with_service(3, 0, 13, 23, "write", "payments"),
    ];
    seed_rspan_input(&inner, Uuid::from_u128(1), EPOCH, 1, &a).await;
    seed_rspan_input(&inner, Uuid::from_u128(2), EPOCH, 2, &b).await;

    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Timeout)
            .with_key_contains("l1.")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store = FaultStore::new(inner, plan);
    let clock = FixedClock::new(sealed_now_ns());

    let first = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket).await;
    assert!(first.is_err(), "record PUT fault must fail the run");
    let second = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("re-run reuses part keys and publishes");
    assert!(matches!(second, CompactionOutcome::Compacted { .. }));

    // Fetch the re-published part.
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
        .expect("compaction record present");
    let rec_bytes = store.get(&rec_key, GetRange::Full).await.unwrap().data;
    let record = ravel_proto::commit::v1::CompactionRecord::decode(rec_bytes.as_ref()).unwrap();
    assert_eq!(record.parts.len(), 1, "small corpus fits one part");
    let part_key = keys::reconstruct_l1_part_key(&record, &record.parts[0]).unwrap();
    let part = store.get(&part_key, GetRange::Full).await.unwrap().data;

    // The rebuilt-after-crash v3 output carries a correct bloom over both
    // inputs' tokens and proves absence for tokens in neither.
    for service in ["checkout", "payments"] {
        assert!(
            rspan_bloom_survives(part.as_ref(), COL_SERVICE_NAME, service),
            "rebuilt bloom must answer membership for service {service}"
        );
    }
    for name in ["read", "write"] {
        assert!(
            rspan_bloom_survives(part.as_ref(), COL_NAME, name),
            "rebuilt bloom must answer membership for span name {name}"
        );
    }
    assert!(
        !rspan_bloom_survives(part.as_ref(), COL_SERVICE_NAME, "billing"),
        "absent service must be pruned by the rebuilt bloom"
    );

    // The service_name column decodes each input's value on the re-published
    // part.
    let l1 = read_l1_union(&store, &bucket).await;
    assert_eq!(l1.len(), 4, "all four merged spans present");
    for r in &l1 {
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
