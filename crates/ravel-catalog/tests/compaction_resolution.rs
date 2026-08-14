//! Resolver integration tests for L0-to-L1 compaction: partition bucket listings by
//! shape, include compaction parts, exclude input-listed L0s, raise the
//! interlock metric on unlisted postdating L0s, token fallback, and the
//! FaultStore crash-recovery coverage.
//!
//! Resolution never fetches L0 data or L1 part objects (it only reads
//! records and reconstructs keys), so these tests build commit and
//! compaction records directly and never need real segment bytes; the
//! full fetch-and-decode differential proof lives in ravel-query's
//! `differential_compaction` test against real RSEG v6 L1 parts.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{keys, signal};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
};
use ravel_segment::VERSION_V6;
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;

fn tenant() -> TenantHash {
    TenantHash([0xab; 16])
}

fn config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        ..Default::default()
    }
}

/// Build a self-consistent L0 commit record. No segment bytes: resolution
/// reads only the record.
fn l0_record(
    writer_id: Uuid,
    shard: u32,
    seq: u64,
    hour: u32,
    created_unix_ns: i64,
    min_event_ts_ns: i64,
    max_event_ts_ns: i64,
) -> CommitRecord {
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: 100,
        content_hash: [seq as u8 ^ 0x5a; 32],
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V6),
        created_unix_ns,
        ingest_hour_bucket: hour,
    })
    .expect("valid record")
}

async fn put_l0(store: &dyn ObjectStoreBackend, record: &CommitRecord) {
    let key = keys::commit_key_for_record(record).expect("commit key");
    store
        .put(&key, record::encode(record), PutOptions::create_if_absent())
        .await
        .expect("put commit record");
}

fn part(part_index: u32, min_event_ts_ns: i64, max_event_ts_ns: i64, seed: u8) -> CompactionPart {
    CompactionPart {
        part_index,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: 10,
        series_count: 2,
        run_count: 3,
        min_event_ts_ns,
        max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V6),
    }
}

/// Build and PUT a compaction record naming `inputs`, at its own
/// reconstructed key. `seed` makes distinct records get distinct
/// `input_set_hash` (and thus distinct keys) within one bucket.
async fn put_compaction_record(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    hour: u32,
    inputs: &[&CommitRecord],
    parts: Vec<CompactionPart>,
    created_unix_ns: i64,
    seed: &[u8],
) -> (CompactionRecord, String) {
    let input_set_hash = *blake3::hash(seed).as_bytes();
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        shard,
        ingest_hour_bucket: hour,
        level: 1,
        inputs: inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect(),
        input_set_hash: input_set_hash.to_vec(),
        parts,
        created_unix_ns,
    };
    let hash16: String = input_set_hash[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let key = keys::compaction_record_key(&tenant(), Signal::Metrics, shard, hour, &hash16)
        .expect("record key");
    store
        .put(
            &key,
            record.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
    (record, key)
}

async fn put_tombstone(store: &dyn ObjectStoreBackend, shard: u32, hour: u32) {
    let key = keys::retention_tombstone_key(&tenant(), Signal::Metrics, shard, hour)
        .expect("tombstone key");
    store
        .put(
            &key,
            bytes::Bytes::from_static(b"tmb"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put tombstone");
}

/// A query range covering the whole of `HOUR`, plus `now_ns` in that hour so
/// the listing window includes it.
fn hour_range_and_now() -> (TimeRange, i64) {
    let start = i64::from(HOUR) * NS_PER_HOUR;
    let now = start + 30 * 60_000_000_000;
    (
        TimeRange {
            start_ns: start,
            end_ns: start + NS_PER_HOUR,
        },
        now,
    )
}

fn l1_keys(snapshot: &ravel_catalog::Snapshot) -> Vec<String> {
    snapshot
        .segments
        .iter()
        .filter(|s| matches!(s.level, SegmentLevel::L1 { .. }))
        .map(|s| s.data_object_key.clone())
        .collect()
}

#[tokio::test]
async fn compaction_record_includes_parts_and_excludes_input_l0s() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    let b = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;
    put_compaction_record(
        store.as_ref(),
        0,
        HOUR,
        &[&a, &b],
        vec![part(0, start, start + 100, 1)],
        now,
        b"set-1",
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");

    // Exactly the one L1 part; both L0 inputs excluded.
    assert_eq!(snapshot.segments.len(), 1);
    assert!(matches!(
        snapshot.segments[0].level,
        SegmentLevel::L1 { .. }
    ));
    assert_eq!(catalog.interlock_violations(), 0);
}

#[tokio::test]
async fn part_event_bounds_filter_against_query_range() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let start = i64::from(HOUR) * NS_PER_HOUR;
    let now = start + 30 * 60_000_000_000;
    // Query only the first minute of the hour.
    let range = TimeRange {
        start_ns: start,
        end_ns: start + 60_000_000_000,
    };
    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    put_compaction_record(
        store.as_ref(),
        0,
        HOUR,
        &[&a],
        vec![
            // In range.
            part(0, start, start + 100, 1),
            // Late in the hour, outside the query range.
            part(
                1,
                start + 40 * 60_000_000_000,
                start + 50 * 60_000_000_000,
                2,
            ),
        ],
        now,
        b"set-1",
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(l1_keys(&snapshot).len(), 1, "only the in-range part");
}

#[tokio::test]
async fn unlisted_l0_included_and_interlock_metric_only_when_postdating() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let listed = l0_record(Uuid::new_v4(), 0, 1, HOUR, start + 100, start, start + 100);
    // An unlisted L0 that predates the record: included, no metric.
    let predates = l0_record(Uuid::new_v4(), 0, 1, HOUR, start + 100, start, start + 100);
    // An unlisted L0 that postdates the record (interlock breach): included,
    // metric raised.
    let postdates = l0_record(Uuid::new_v4(), 0, 1, HOUR, start + 300, start, start + 100);
    put_l0(store.as_ref(), &listed).await;
    put_l0(store.as_ref(), &predates).await;
    put_l0(store.as_ref(), &postdates).await;
    put_compaction_record(
        store.as_ref(),
        0,
        HOUR,
        &[&listed],
        vec![part(0, start, start + 100, 1)],
        start + 200,
        b"set-1",
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");

    // 1 part + the two unlisted L0s; the listed input is excluded.
    assert_eq!(snapshot.segments.len(), 3);
    assert_eq!(l1_keys(&snapshot).len(), 1);
    let l0_count = snapshot
        .segments
        .iter()
        .filter(|s| matches!(s.level, SegmentLevel::L0))
        .count();
    assert_eq!(l0_count, 2);
    // Only the postdating one trips the interlock metric.
    assert_eq!(catalog.interlock_violations(), 1);
}

#[tokio::test]
async fn tombstoned_bucket_contributes_nothing() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    put_compaction_record(
        store.as_ref(),
        0,
        HOUR,
        &[&a],
        vec![part(0, start, start + 100, 1)],
        now,
        b"set-1",
    )
    .await;
    put_tombstone(store.as_ref(), 0, HOUR).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert!(snapshot.segments.is_empty());
}

#[tokio::test]
async fn two_records_with_different_input_set_hash_include_both_and_alarm() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    let b = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;
    put_compaction_record(
        store.as_ref(),
        0,
        HOUR,
        &[&a],
        vec![part(0, start, start + 100, 1)],
        now,
        b"set-1",
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        0,
        HOUR,
        &[&b],
        vec![part(0, start, start + 100, 2)],
        now,
        b"set-2",
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    // Both parts sets; every L0 is named by one record, so none survive.
    assert_eq!(l1_keys(&snapshot).len(), 2);
    assert_eq!(snapshot.segments.len(), 2);
    assert_eq!(catalog.compaction_input_set_conflicts(), 1);
}

#[tokio::test]
async fn mixed_level_snapshot_sort_is_deterministic() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    // An unlisted L0 (survives) plus a compaction record with two parts.
    let unlisted = l0_record(Uuid::new_v4(), 0, 9, HOUR, start + 100, start, start + 100);
    let input = l0_record(Uuid::new_v4(), 0, 1, HOUR, start + 100, start, start + 100);
    put_l0(store.as_ref(), &unlisted).await;
    put_l0(store.as_ref(), &input).await;
    put_compaction_record(
        store.as_ref(),
        0,
        HOUR,
        &[&input],
        vec![
            part(0, start, start + 100, 7),
            part(1, start, start + 100, 3),
        ],
        start + 100,
        b"set-1",
    )
    .await;

    let first = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve 1");
    let second = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve 2");
    let order1: Vec<_> = first
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    let order2: Vec<_> = second
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    assert_eq!(order1, order2, "sort is a deterministic total order");
    assert_eq!(first.segments.len(), 3);
}

#[tokio::test]
async fn unknown_bucket_entry_shape_is_a_fail_loud_error() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let prefix = keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, HOUR).unwrap();
    // A key in the bucket that matches none of the three known shapes.
    store
        .put(
            &format!("{prefix}garbage.blob"),
            bytes::Bytes::from_static(b"x"),
            PutOptions::create_if_absent(),
        )
        .await
        .unwrap();

    let err = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect_err("unknown shape must fail loud");
    assert!(matches!(err, ravel_catalog::CatalogError::Key(_)));
}

// --- Token fallback (docs/catalog-and-mvcc.md step 5) ---

/// Build a token for a skewed bucket outside the listing window, so the only
/// path that touches the token's commit key is the exact-token resolve.
fn skewed_token(record: &CommitRecord) -> (CommitToken, TimeRange, i64) {
    let now = i64::from(HOUR) * NS_PER_HOUR + 30 * 60_000_000_000;
    let range = TimeRange {
        start_ns: now - NS_PER_HOUR,
        end_ns: now,
    };
    let token = record::token_for(record).expect("token");
    (token, range, now)
}

#[tokio::test]
async fn token_fallback_serves_via_l1_when_commit_record_swept() {
    // Crash row 12: token GET NotFound post-sweep -> fallback finds the token
    // in an input list -> serve via L1. FaultStore makes the commit-record GET
    // return NotFound (the record was swept); its counter proves the fault
    // fired, not merely that resolution happened to take the fallback.
    let skewed_bucket = HOUR - 20;
    let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
    let input = l0_record(
        Uuid::new_v4(),
        0,
        1,
        skewed_bucket,
        skewed_created,
        skewed_created,
        skewed_created + 1_000,
    );
    let token_key = keys::commit_key_for_token(
        &tenant(),
        Signal::Metrics,
        &record::token_for(&input).unwrap(),
    )
    .unwrap();

    let inner = MemoryStore::new();
    // The commit record was swept: never stored. The compaction record naming
    // it is present.
    put_compaction_record(
        &inner,
        0,
        skewed_bucket,
        &[&input],
        vec![part(0, skewed_created, skewed_created + 1_000, 1)],
        skewed_created + 5_000,
        b"set-1",
    )
    .await;

    // Always-NotFound on the token's commit key models the swept record with a
    // countable fault.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(token_key)
            .with_occurrence(Occurrence::Always),
    );
    let store = Arc::new(FaultStore::new(inner, plan));
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

    let (token, range, now) = skewed_token(&input);
    let snapshot = catalog
        .resolve(
            &tenant(),
            Signal::Metrics,
            range,
            std::slice::from_ref(&token),
            now,
        )
        .await
        .expect("token served via L1");

    assert_eq!(l1_keys(&snapshot).len(), 1);
    // Two probes (one propagation retry) both NotFound before the fallback ran.
    assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 2);
}

#[tokio::test]
async fn token_fallback_tombstone_satisfied_with_zero_segments() {
    let skewed_bucket = HOUR - 20;
    let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
    let input = l0_record(
        Uuid::new_v4(),
        0,
        1,
        skewed_bucket,
        skewed_created,
        skewed_created,
        skewed_created + 1_000,
    );
    let store = Arc::new(MemoryStore::new());
    // No commit record (retired); a tombstone for the bucket.
    put_tombstone(store.as_ref(), 0, skewed_bucket).await;

    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (token, range, now) = skewed_token(&input);
    let snapshot = catalog
        .resolve(
            &tenant(),
            Signal::Metrics,
            range,
            std::slice::from_ref(&token),
            now,
        )
        .await
        .expect("tombstone satisfies token with zero segments");
    assert!(snapshot.segments.is_empty());
}

#[tokio::test]
async fn token_fallback_unsatisfiable_when_not_in_any_input_list() {
    let skewed_bucket = HOUR - 20;
    let skewed_created = i64::from(skewed_bucket) * NS_PER_HOUR + 10 * 60_000_000_000;
    let input = l0_record(
        Uuid::new_v4(),
        0,
        1,
        skewed_bucket,
        skewed_created,
        skewed_created,
        skewed_created + 1_000,
    );
    // A different writer's compaction record that does NOT name the token.
    let other = l0_record(
        Uuid::new_v4(),
        0,
        1,
        skewed_bucket,
        skewed_created,
        skewed_created,
        skewed_created + 1_000,
    );
    let store = Arc::new(MemoryStore::new());
    put_compaction_record(
        store.as_ref(),
        0,
        skewed_bucket,
        &[&other],
        vec![part(0, skewed_created, skewed_created + 1_000, 1)],
        skewed_created + 5_000,
        b"set-1",
    )
    .await;

    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (token, range, now) = skewed_token(&input);
    let err = catalog
        .resolve(
            &tenant(),
            Signal::Metrics,
            range,
            std::slice::from_ref(&token),
            now,
        )
        .await
        .expect_err("token neither in an input list nor tombstoned");
    assert!(matches!(
        err,
        ravel_catalog::CatalogError::UnsatisfiableToken { .. }
    ));
}

// --- Crash rows 5 and 6 ---

#[tokio::test]
async fn row5_l1_and_inputs_both_visible_do_not_double_count() {
    // Row 5: after the record PUT, before the sweep, the L1 parts AND the L0
    // inputs are all visible. The resolver excludes the listed inputs
    // immediately, so overlap harmlessness holds (no double count). The Delete
    // fault models the sweep crashing before it could remove an input; its
    // counter proves the crash was injected.
    let inner = MemoryStore::new();
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;
    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    let b = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(&inner, &a).await;
    put_l0(&inner, &b).await;
    put_compaction_record(
        &inner,
        0,
        HOUR,
        &[&a, &b],
        vec![part(0, start, start + 100, 1)],
        now,
        b"set-1",
    )
    .await;

    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Delete, ScriptedFault::Permanent("sweep crashed".into()))
            .with_occurrence(Occurrence::Always),
    );
    let store = Arc::new(FaultStore::new(inner, plan));
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

    // A sweeper begins deleting an input and crashes (the delete faults).
    let a_key = keys::commit_key_for_record(&a).unwrap();
    assert!(store.delete(&a_key).await.is_err());
    assert_eq!(store.fault_count(Op::Delete, FaultKind::Permanent), 1);

    // With inputs still present, resolution returns exactly the L1 part.
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(snapshot.segments.len(), 1);
    assert!(matches!(
        snapshot.segments[0].level,
        SegmentLevel::L1 { .. }
    ));
}

#[tokio::test]
async fn row6_parts_without_record_are_ignored_across_list_pages() {
    // Row 6: a resolver listing mid-publish can see parts but no record. Parts
    // live under the l1/ prefix, never the c/ bucket the resolver LISTs, so
    // they are ignored and the inputs resolve normally as L0. Exercised with a
    // tiny listing page size so the bucket paginates (the "pagination dedup by
    // key" contract).
    let store = Arc::new(MemoryStore::with_page_size(2));
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let inputs: Vec<CommitRecord> = (1..=5)
        .map(|seq| l0_record(Uuid::new_v4(), 0, seq, HOUR, now, start, start + 100))
        .collect();
    for r in &inputs {
        put_l0(store.as_ref(), r).await;
    }
    // A part object physically present under l1/ but NO compaction record yet.
    let part_key = keys::l1_part_key(
        &tenant(),
        Signal::Metrics,
        0,
        HOUR,
        "0011223344556677",
        0,
        "8899aabbccddeeff",
    )
    .unwrap();
    store
        .put(
            &part_key,
            bytes::Bytes::from_static(b"partbytes"),
            PutOptions::create_if_absent(),
        )
        .await
        .unwrap();

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    // All five L0 inputs resolve; the record-less part is invisible.
    assert_eq!(snapshot.segments.len(), 5);
    assert!(l1_keys(&snapshot).is_empty());
}

#[tokio::test]
async fn row6_mid_list_fault_is_surfaced_not_silently_partial() {
    // Row 6 safety: a store fault mid-LIST is surfaced (fail-loud), never
    // silently turned into a partial (record-less or input-less) snapshot. The
    // List fault counter proves the fault fired.
    let inner = MemoryStore::new();
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;
    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(&inner, &a).await;
    put_compaction_record(
        &inner,
        0,
        HOUR,
        &[&a],
        vec![part(0, start, start + 100, 1)],
        now,
        b"set-1",
    )
    .await;

    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::List, ScriptedFault::Transient("list blip".into()))
            .with_occurrence(Occurrence::Always),
    );
    let store = Arc::new(FaultStore::new(inner, plan));
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

    let err = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect_err("a mid-LIST fault must surface, never a partial snapshot");
    assert!(matches!(err, ravel_catalog::CatalogError::Store(_)));
    assert!(store.fault_count(Op::List, FaultKind::Transient) >= 1);
}
