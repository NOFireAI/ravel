//! Resolver integration tests for selective erasure (ADR-0064): the
//! per-resolve `del/` LIST that attaches pending `.dreq` predicates to the
//! snapshot (the immediate query-time exclusion / visibility bound), and the
//! `RewriteRecord` supersession that excludes rewritten inputs and superseded
//! predecessor parts through the SAME exclusion mechanism a `CompactionRecord`
//! already uses.
//!
//! Mirrors `compaction_resolution.rs`: resolution reads only records and
//! reconstructs keys, so these tests build commit, compaction, rewrite, and
//! erasure-request records directly and never need real segment bytes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use prost::Message;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, SegmentLevel,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
    ErasurePredicateMatcher, ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash, TimeRange};
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
        segment_format_version: u32::from(VERSION_V7),
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
        segment_format_version: u32::from(VERSION_V7),
    }
}

/// Build and PUT a compaction record naming `inputs`, at its own reconstructed
/// key. Returns the record and its key.
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
    let key = keys::compaction_record_key_for(&record).expect("record key");
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

/// Build a `RewriteRecord`. Exactly one of `inputs` (raw L0 identities) or
/// `superseded_key` (a whole predecessor record) is set (ADR-0064 amendment);
/// the canonical `input_set_hash` is computed the same way the writer does, so
/// the record survives `decode_rewrite`'s recomputation check.
fn build_rewrite(
    shard: u32,
    hour: u32,
    inputs: &[&CommitRecord],
    superseded_key: Option<&str>,
    parts: Vec<CompactionPart>,
    request_ids: &[Uuid],
    created_unix_ns: i64,
) -> RewriteRecord {
    let mut input_ids: Vec<CompactionInputIdentity> = inputs
        .iter()
        .map(|r| CompactionInputIdentity {
            writer_id: r.writer_id.clone(),
            writer_epoch: r.writer_epoch,
            writer_seq: r.writer_seq,
        })
        .collect();
    input_ids.sort_by(|a, b| {
        (a.writer_id.as_str(), a.writer_epoch, a.writer_seq).cmp(&(
            b.writer_id.as_str(),
            b.writer_epoch,
            b.writer_seq,
        ))
    });
    let mut sorted_ids: Vec<String> = request_ids.iter().map(|u| u.to_string()).collect();
    sorted_ids.sort();
    let input_set_hash =
        erasure::compute_rewrite_input_set_hash(&input_ids, superseded_key, &sorted_ids).to_vec();
    let drops: Vec<RewriteDrop> = request_ids
        .iter()
        .map(|u| RewriteDrop {
            request_id: u.to_string(),
            dropped_count: 1,
        })
        .collect();
    RewriteRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        shard,
        ingest_hour_bucket: hour,
        inputs: input_ids,
        input_set_hash,
        parts,
        drops,
        created_unix_ns,
        superseded_record_key: superseded_key.unwrap_or("").to_string(),
    }
}

async fn put_rewrite(store: &dyn ObjectStoreBackend, record: &RewriteRecord) -> String {
    let key = keys::rewrite_record_key_for(record).expect("rewrite key");
    store
        .put(
            &key,
            erasure::encode_rewrite(record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put rewrite record");
    key
}

/// Build and PUT an erasure request (`.dreq`) for one subject predicate.
async fn put_dreq(
    store: &dyn ObjectStoreBackend,
    request_id: Uuid,
    label: &str,
    value: &str,
    created_unix_ns: i64,
) -> ErasureRequest {
    let record = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns,
        predicate: vec![ErasurePredicateMatcher {
            key: label.to_string(),
            value: value.to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    let key = keys::erasure_request_key(&tenant(), Signal::Metrics, request_id).expect("dreq key");
    store
        .put(
            &key,
            erasure::encode_request(&record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
    record
}

/// A `.done` completion record object (PII-free), which a resolve must skip.
async fn put_done(store: &dyn ObjectStoreBackend, request_id: Uuid) {
    let key =
        keys::erasure_completion_key(&tenant(), Signal::Metrics, request_id).expect("done key");
    // The resolve classifies by key shape and skips `.done` before decoding, so
    // opaque bytes suffice for this fixture.
    store
        .put(
            &key,
            bytes::Bytes::from_static(b"done"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put done");
}

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

// --- del/ listing and pending-predicate attachment (ADR-0064 decision 2) ---

#[tokio::test]
async fn no_del_directory_returns_empty_pending_at_one_list() {
    // An empty window (query start far past `now`) makes resolve skip the whole
    // bucket fan-out, so the ONLY store request it issues is the single `del/`
    // LIST. That LIST is empty (no erasure), so `pending_erasure` is empty and
    // exactly one store request was made -- the "one LIST, nothing more" bound.
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let now = i64::from(HOUR) * NS_PER_HOUR;
    let range = TimeRange {
        start_ns: i64::from(HOUR + 10) * NS_PER_HOUR,
        end_ns: i64::from(HOUR + 11) * NS_PER_HOUR,
    };
    let accounting = QueryAccounting::new();
    let snapshot = catalog
        .resolve_with_accounting(&tenant(), Signal::Metrics, range, &[], now, &accounting)
        .await
        .expect("resolve");
    assert!(snapshot.segments.is_empty());
    assert!(snapshot.pending_erasure.is_empty());
    assert_eq!(
        accounting.snapshot().total_s3_requests(),
        1,
        "an empty del/ scan costs exactly one LIST and nothing more"
    );
    assert!(
        catalog.estimated_catalog_requests(range, now) >= accounting.snapshot().total_s3_requests(),
        "an empty-window estimate must still be a true upper envelope of the del/ LIST resolve \
         issues unconditionally"
    );
}

#[tokio::test]
async fn one_pending_dreq_is_attached_to_the_snapshot() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let rid = Uuid::new_v4();
    let request = put_dreq(store.as_ref(), rid, "user_id", "u123", now - 10).await;
    // A `.done` for an unrelated request must be skipped, not attached.
    put_done(store.as_ref(), Uuid::new_v4()).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(snapshot.pending_erasure.len(), 1);
    assert_eq!(snapshot.pending_erasure[0], request);
}

#[tokio::test]
async fn visibility_bound_predicate_attached_before_any_rewrite() {
    // The core visibility bound: a `.dreq` durable before a resolve is attached
    // by the very next resolve, with no rewrite record anywhere in the bucket.
    // The live L0 segment still resolves (attachment does not remove segments --
    // filtering is the scan layer's job); only the predicate rides along.
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;
    let seg = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &seg).await;
    let rid = Uuid::new_v4();
    let request = put_dreq(store.as_ref(), rid, "client.address", "10.0.0.1", now - 5).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(
        snapshot.segments.len(),
        1,
        "the segment is not yet rewritten"
    );
    assert_eq!(snapshot.pending_erasure, vec![request]);
}

// --- RewriteRecord supersession (ADR-0064 decision 3) ---

#[tokio::test]
async fn rewrite_with_inputs_excludes_named_l0s_and_includes_its_parts() {
    // Mirror of `compaction_record_includes_parts_and_excludes_input_l0s`, for a
    // rewrite record naming raw L0 inputs directly.
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    let b = l0_record(Uuid::new_v4(), 0, 2, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;
    let rw = build_rewrite(
        0,
        HOUR,
        &[&a, &b],
        None,
        vec![part(0, start, start + 100, 1)],
        &[Uuid::new_v4()],
        now,
    );
    put_rewrite(store.as_ref(), &rw).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(
        snapshot.segments.len(),
        1,
        "only the one rewrite output part"
    );
    assert!(matches!(
        snapshot.segments[0].level,
        SegmentLevel::L1 { .. }
    ));
    assert_eq!(catalog.interlock_violations(), 0);
}

#[tokio::test]
async fn rewrite_superseding_compaction_hides_predecessor_parts_and_inputs() {
    // The recursive-chase case one level deep: a rewrite names a live
    // CompactionRecord via `superseded_record_key`. The compaction's parts must
    // be hidden (superseded as a whole) and its L0 inputs excluded; only the
    // rewrite's own output parts survive.
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    let b = l0_record(Uuid::new_v4(), 0, 2, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;
    // Live compaction record with a part that still contains the erased subject.
    let (_comp, comp_key) = put_compaction_record(
        store.as_ref(),
        0,
        HOUR,
        &[&a, &b],
        vec![part(0, start, start + 100, 0xC0)],
        now,
        b"comp-set",
    )
    .await;
    // The rewrite supersedes the whole compaction record and emits an erased part.
    let rw = build_rewrite(
        0,
        HOUR,
        &[],
        Some(&comp_key),
        vec![part(0, start, start + 100, 0x0E)],
        &[Uuid::new_v4()],
        now + 1,
    );
    let rw_key = put_rewrite(store.as_ref(), &rw).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    // Exactly the rewrite's own output part; the compaction's part is gone.
    let expected_part = keys::reconstruct_rewrite_part_key(&rw, &rw.parts[0]).unwrap();
    assert_eq!(l1_keys(&snapshot), vec![expected_part]);
    assert_eq!(snapshot.segments.len(), 1);
    assert!(
        !rw_key.is_empty() && snapshot.segments[0].data_object_key != comp_key,
        "the superseded compaction part must not resurface"
    );
}

#[tokio::test]
async fn rewrite_chain_two_levels_resolves_through_both() {
    // A rewrite superseding an earlier rewrite that itself names raw L0 inputs
    // (a bucket erased twice, ADR-0064 amendment). Only the newest rewrite's
    // parts survive; the older rewrite's parts and the L0 inputs are excluded.
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    let rw1 = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![part(0, start, start + 100, 0x11)],
        &[Uuid::new_v4()],
        now,
    );
    let rw1_key = put_rewrite(store.as_ref(), &rw1).await;
    let rw2 = build_rewrite(
        0,
        HOUR,
        &[],
        Some(&rw1_key),
        vec![part(0, start, start + 100, 0x22)],
        &[Uuid::new_v4()],
        now + 1,
    );
    put_rewrite(store.as_ref(), &rw2).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    let expected_part = keys::reconstruct_rewrite_part_key(&rw2, &rw2.parts[0]).unwrap();
    assert_eq!(l1_keys(&snapshot), vec![expected_part]);
    assert_eq!(snapshot.segments.len(), 1);
}

#[tokio::test]
async fn rewrite_parts_appear_as_live_l1_entries() {
    // A rewrite with two output parts, no compaction predecessor: both parts
    // fold into the snapshot as L1-equivalent entries.
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    let rw = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![
            part(0, start, start + 100, 0x01),
            part(1, start, start + 100, 0x02),
        ],
        &[Uuid::new_v4()],
        now,
    );
    put_rewrite(store.as_ref(), &rw).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(l1_keys(&snapshot).len(), 2);
    assert!(
        snapshot
            .segments
            .iter()
            .all(|s| matches!(s.level, SegmentLevel::L1 { .. }))
    );
}

#[tokio::test]
async fn tampered_superseded_record_key_is_a_typed_error_never_a_panic() {
    // A rewrite whose `superseded_record_key` names a well-formed compaction
    // key for a DIFFERENT bucket (a different ingest hour). `decode_rewrite`
    // rejects the bucket mismatch on load, so resolve fails with a typed error,
    // never a panic or a silently-wrong snapshot.
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let wrong_bucket_key =
        keys::compaction_record_key(&tenant(), Signal::Metrics, 0, HOUR - 1, "0011223344556677")
            .expect("build a well-formed compaction key for a different hour");
    let rw = build_rewrite(
        0,
        HOUR,
        &[],
        Some(&wrong_bucket_key),
        vec![],
        &[Uuid::new_v4()],
        now,
    );
    put_rewrite(store.as_ref(), &rw).await;

    let err = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect_err("a tampered superseded_record_key must be a typed error");
    assert!(matches!(
        err,
        ravel_catalog::CatalogError::RewriteRecordDecode { .. }
    ));
}

// --- Fold recognizes rewrite records (ADR-0064; the dangerous warn-skip) ---

const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// `now_ns` at which ingest hour `hour` has just sealed under default margins.
fn now_at_seal(hour: u32) -> i64 {
    i64::from(hour + 1) * NS_PER_HOUR + MARGIN_NS + 1
}

#[tokio::test]
async fn fold_recognizes_rewrite_records_and_matches_resolve() {
    // A fold over a bucket holding a rewrite record must NOT warn-and-skip the
    // `rw.` key (which would ignore its supersession and resurrect the erased
    // L0 into the folded snapshot). Proven two ways: `layout_drift_count == 0`
    // (a skip would increment it), and a before/after-fold differential --
    // resolve returns the identical segment set whether served by listing or
    // from the part the fold wrote.
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let now = now_at_seal(HOUR);
    let start = i64::from(HOUR) * NS_PER_HOUR;
    let range = TimeRange {
        start_ns: start,
        end_ns: now,
    };

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, start + 1, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    let rw = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![part(0, start, start + 100, 0x42)],
        &[Uuid::new_v4()],
        start + 2,
    );
    put_rewrite(store.as_ref(), &rw).await;

    let before = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve before fold");
    // The rewrite's part is live; the L0 input is excluded.
    assert_eq!(l1_keys(&before).len(), 1);
    assert_eq!(before.segments.len(), 1);

    let report = catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    assert_eq!(report.watermark_hour, Some(HOUR));
    assert_eq!(
        report.layout_drift_count, 0,
        "the rewrite record must be recognized, never warn-and-skipped"
    );

    let after = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve after fold");
    let order = |s: &ravel_catalog::Snapshot| -> Vec<String> {
        s.segments
            .iter()
            .map(|x| x.data_object_key.clone())
            .collect()
    };
    assert_eq!(
        order(&before),
        order(&after),
        "fold reproduces the resolver's rewrite supersession exactly"
    );
}

/// The scenario the original Stage 4 checkpoint proved was broken: a rewrite
/// record publishes into a bucket that was ALREADY sealed and folded by a
/// PAST fold (ADR-0064 §3.1 scopes the rewrite pass to sealed buckets by
/// construction, so this is the ordinary case, not an edge case). Without a
/// reconcile trigger on `RewriteRecord`, the incremental fold path would
/// never re-list that bucket, the folded snapshot would keep serving the
/// pre-erasure L0 indefinitely, and the input object stays physically
/// present (no sweep has run yet) so there is no NotFound-driven re-resolve
/// to force a refresh either.
///
/// Proves the fix two ways: (a) the second fold is genuinely incremental
/// (`rebuilt == false`) yet still picks up the rewrite, and (b) the
/// erased-and-rewritten hour resolves with ZERO store requests beyond the
/// unconditional del/ LIST -- i.e. purely from the folded snapshot, with no
/// live bucket listing to fall back on and mask a fold bug.
#[tokio::test]
async fn reconcile_picks_up_a_rewrite_published_into_an_already_folded_bucket() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let start = i64::from(HOUR) * NS_PER_HOUR;

    // Seed and seal HOUR with a plain L0, and HOUR+1 as the tail so the
    // first fold has more than one bucket (matching the pattern established
    // fold reconcile tests use elsewhere in this crate).
    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, start + 1, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    let tail = l0_record(
        Uuid::new_v4(),
        0,
        1,
        HOUR + 1,
        start + NS_PER_HOUR + 1,
        start + NS_PER_HOUR,
        start + NS_PER_HOUR + 100,
    );
    put_l0(store.as_ref(), &tail).await;

    let now_1 = now_at_seal(HOUR + 1);
    let first = catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
        .await
        .expect("first fold");
    assert_eq!(first.watermark_hour, Some(HOUR + 1));

    // Confirm HOUR's L0 is live and unrewritten before the rewrite lands.
    let pre_rewrite = catalog
        .resolve(
            &tenant(),
            Signal::Metrics,
            TimeRange {
                start_ns: start,
                end_ns: start + 100,
            },
            &[],
            now_1,
        )
        .await
        .expect("resolve before rewrite");
    assert_eq!(pre_rewrite.segments.len(), 1);

    // The rewrite pass runs AFTER the first fold, into the now-sealed,
    // already-folded HOUR bucket -- exactly ADR-0064 §3.1's scope.
    let rw = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![part(0, start, start + 100, 0x77)],
        &[Uuid::new_v4()],
        start + 2,
    );
    put_rewrite(store.as_ref(), &rw).await;

    // A second, later fold: HOUR is now strictly behind the old watermark, so
    // only the reconcile pass (not the incremental listing range) can pick up
    // the rewrite.
    let now_2 = now_at_seal(HOUR + 3);
    let second = catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
        .await
        .expect("second fold");
    assert!(
        !second.rebuilt,
        "must be a genuine incremental+reconcile fold, not a full rebuild that would \
         trivially re-derive every hour and mask the reconcile-trigger bug"
    );
    assert!(!second.no_op);

    // Resolve HOUR again, from a range the snapshot watermark now fully
    // covers (HOUR < HOUR+3), so this MUST be served purely from the folded
    // snapshot window path (bounded HEAD GET + part GET(s) + the
    // unconditional del/ LIST) with no live per-bucket listing, which would
    // scale with data rather than stay inside the fixed envelope.
    let hour_range = TimeRange {
        start_ns: start,
        end_ns: start + 100,
    };
    let accounting = QueryAccounting::new();
    let post_rewrite = catalog
        .resolve_with_accounting(
            &tenant(),
            Signal::Metrics,
            hour_range,
            &[],
            now_2,
            &accounting,
        )
        .await
        .expect("resolve after rewrite and reconcile");

    assert!(
        accounting.snapshot().total_s3_requests()
            <= catalog.estimated_catalog_requests(hour_range, now_2),
        "must stay inside the snapshot-window envelope (no live per-bucket LIST fallback, \
         which would prove this test is only passing via the resolver's own live-listing \
         supersession rather than a real fold/reconcile fix): got {}, envelope {}",
        accounting.snapshot().total_s3_requests(),
        catalog.estimated_catalog_requests(hour_range, now_2)
    );
    assert_eq!(
        post_rewrite.segments.len(),
        1,
        "the rewrite's output part must be live"
    );
    assert_ne!(
        post_rewrite.segments[0].content_hash.as_slice(),
        a.content_hash.as_slice(),
        "the erased L0's content must not still be served: it must be the rewrite's part, \
         not the pre-erasure input"
    );
}

/// The absent-predecessor path in `resolve_rewrite_supersession`: a rewrite
/// names a `superseded_record_key` for a compaction record that is no longer
/// present in the bucket's live listing (already swept). The chase must stop
/// cleanly (never error, never hang) -- and, critically, must not silently
/// under-exclude: this test only proves the "never hang/error" half, since a
/// genuinely absent predecessor by construction carries no inputs this
/// resolve could discover here. The real safety net for the case the absent
/// predecessor's OWN inputs are somehow still live (a sweep-ordering anomaly)
/// is stated as a requirement on the rewrite pass's completion verification, not
/// something this resolver-only task can close.
#[tokio::test]
async fn rewrite_naming_an_absent_predecessor_resolves_cleanly() {
    let (range, now) = hour_range_and_now();
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

    // A rewrite naming a compaction record key that was never published (or
    // has since been swept) in this bucket.
    let absent_key =
        keys::compaction_record_key(&tenant(), Signal::Metrics, 0, HOUR, "0000000000000000")
            .expect("valid key shape");
    let rw = build_rewrite(
        0,
        HOUR,
        &[],
        Some(&absent_key),
        vec![part(0, range.start_ns, range.start_ns + 100, 0x11)],
        &[Uuid::new_v4()],
        range.start_ns + 1,
    );
    put_rewrite(store.as_ref(), &rw).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve must not error on an absent predecessor");
    assert_eq!(
        snapshot.segments.len(),
        1,
        "the rewrite's own output part is still live and included"
    );
}

/// Two live (non-superseding) rewrite records in one bucket must alarm via
/// [`Catalog::rewrite_sibling_conflicts`], not pass silently as ordinary
/// overlap the way two compaction records would.
#[tokio::test]
async fn sibling_rewrites_raise_the_conflict_counter() {
    let (range, now) = hour_range_and_now();
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

    let a = l0_record(
        Uuid::new_v4(),
        0,
        1,
        HOUR,
        range.start_ns,
        range.start_ns,
        range.start_ns + 1,
    );
    let b = l0_record(
        Uuid::new_v4(),
        0,
        2,
        HOUR,
        range.start_ns,
        range.start_ns,
        range.start_ns + 1,
    );
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;

    let rw_a = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![part(0, range.start_ns, range.start_ns + 100, 0x21)],
        &[Uuid::new_v4()],
        range.start_ns + 1,
    );
    let rw_b = build_rewrite(
        0,
        HOUR,
        &[&b],
        None,
        vec![part(0, range.start_ns, range.start_ns + 100, 0x22)],
        &[Uuid::new_v4()],
        range.start_ns + 2,
    );
    put_rewrite(store.as_ref(), &rw_a).await;
    put_rewrite(store.as_ref(), &rw_b).await;

    assert_eq!(catalog.rewrite_sibling_conflicts(), 0);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve must not error, only alarm");
    // Both rewrites' parts are still served (harmless-overlap posture), but
    // the conflict must be visible.
    assert_eq!(snapshot.segments.len(), 2);
    assert_eq!(
        catalog.rewrite_sibling_conflicts(),
        1,
        "two live sibling rewrites over one bucket must raise the alarm, not pass silently"
    );
}

// --- Stage 4 re-review additions: sibling-alarm precision and part-key
// --- correctness on the reconcile path.

/// A rewrite chain (rw2 supersedes rw1) is exactly ONE live rewrite, not two
/// siblings. The conflict alarm must not false-positive on it, or the alarm
/// becomes noise the moment any bucket is erased twice -- the case ADR-0064's
/// amendment explicitly designs for.
#[tokio::test]
async fn a_rewrite_chain_is_not_a_sibling_conflict() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    let rw1 = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![part(0, start, start + 100, 0x31)],
        &[Uuid::new_v4()],
        now,
    );
    let rw1_key = put_rewrite(store.as_ref(), &rw1).await;
    let rw2 = build_rewrite(
        0,
        HOUR,
        &[],
        Some(&rw1_key),
        vec![part(0, start, start + 100, 0x32)],
        &[Uuid::new_v4()],
        now + 1,
    );
    put_rewrite(store.as_ref(), &rw2).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(snapshot.segments.len(), 1);
    assert_eq!(
        catalog.rewrite_sibling_conflicts(),
        0,
        "a superseded predecessor is not a live sibling; the alarm must not fire"
    );
}

/// Three rewrites: rw2 supersedes rw1, and rw3 is independent. Exactly TWO
/// live, non-superseding rewrites remain, so the bucket is a genuine sibling
/// conflict and must alarm once -- proving the counter reads live records
/// rather than raw record count.
#[tokio::test]
async fn a_chain_plus_an_independent_rewrite_is_one_sibling_conflict() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();
    let start = range.start_ns;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, now, start, start + 100);
    let b = l0_record(Uuid::new_v4(), 0, 2, HOUR, now, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;

    let rw1 = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![part(0, start, start + 100, 0x41)],
        &[Uuid::new_v4()],
        now,
    );
    let rw1_key = put_rewrite(store.as_ref(), &rw1).await;
    let rw2 = build_rewrite(
        0,
        HOUR,
        &[],
        Some(&rw1_key),
        vec![part(0, start, start + 100, 0x42)],
        &[Uuid::new_v4()],
        now + 1,
    );
    put_rewrite(store.as_ref(), &rw2).await;
    let rw3 = build_rewrite(
        0,
        HOUR,
        &[&b],
        None,
        vec![part(0, start, start + 100, 0x43)],
        &[Uuid::new_v4()],
        now + 2,
    );
    put_rewrite(store.as_ref(), &rw3).await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    let mut got = l1_keys(&snapshot);
    got.sort();
    let mut want = vec![
        keys::reconstruct_rewrite_part_key(&rw2, &rw2.parts[0]).unwrap(),
        keys::reconstruct_rewrite_part_key(&rw3, &rw3.parts[0]).unwrap(),
    ];
    want.sort();
    assert_eq!(got, want, "only the two live rewrites' parts are served");
    assert_eq!(
        snapshot.segments.len(),
        2,
        "both L0 inputs are excluded; only the two live rewrite parts remain"
    );
    assert_eq!(
        catalog.rewrite_sibling_conflicts(),
        1,
        "two live non-superseding rewrites is exactly one bucket conflict"
    );
}

/// The reconcile path must reconstruct the rewrite output part's REAL object
/// key, not merely some entry that happens to carry the right content hash.
/// A folded `SnapshotEntry` carries only (level, shard, hour, writer_id =
/// input_set_hash, writer_epoch = part_index, content_hash), so a fold that
/// built a structurally plausible entry with the wrong identity fields would
/// resolve to a key no object lives at, and every assertion in
/// `reconcile_picks_up_a_rewrite_published_into_an_already_folded_bucket`
/// would still pass.
#[tokio::test]
async fn reconciled_rewrite_part_resolves_to_the_real_part_key() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let start = i64::from(HOUR) * NS_PER_HOUR;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, start + 1, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    let tail = l0_record(
        Uuid::new_v4(),
        0,
        1,
        HOUR + 1,
        start + NS_PER_HOUR + 1,
        start + NS_PER_HOUR,
        start + NS_PER_HOUR + 100,
    );
    put_l0(store.as_ref(), &tail).await;

    let now_1 = now_at_seal(HOUR + 1);
    catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
        .await
        .expect("first fold");

    let rw = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![part(0, start, start + 100, 0x78)],
        &[Uuid::new_v4()],
        start + 2,
    );
    put_rewrite(store.as_ref(), &rw).await;

    let now_2 = now_at_seal(HOUR + 3);
    let second = catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
        .await
        .expect("second fold");
    assert!(!second.rebuilt);

    let snapshot = catalog
        .resolve(
            &tenant(),
            Signal::Metrics,
            TimeRange {
                start_ns: start,
                end_ns: start + 100,
            },
            &[],
            now_2,
        )
        .await
        .expect("resolve");
    let expected = keys::reconstruct_rewrite_part_key(&rw, &rw.parts[0]).unwrap();
    assert_eq!(
        l1_keys(&snapshot),
        vec![expected],
        "the folded entry must reconstruct the rewrite's own part key"
    );
    let l0_key = keys::verify_object_key(&a).unwrap();
    assert!(
        !snapshot
            .segments
            .iter()
            .any(|s| s.data_object_key == l0_key),
        "the pre-erasure L0 object key must not be reachable from the folded snapshot"
    );
}

/// The sibling-rewrite alarm must fire on the FOLD path, not only on the live
/// per-bucket listing path. A query for an hour the folded snapshot covers
/// never calls `Catalog::process_bucket` at all, and per ADR-0064 section 3.1
/// that is the ORDINARY case for a rewrite (it always targets an
/// already-sealed, already-folded bucket). An alarm wired only into
/// `process_bucket` would therefore be silent for exactly the case it exists
/// to catch -- the same "correct logic on the path that does not run" shape as
/// the reconcile-trigger bug above.
#[tokio::test]
async fn sibling_rewrites_alarm_on_the_folded_snapshot_path_too() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let start = i64::from(HOUR) * NS_PER_HOUR;

    let a = l0_record(Uuid::new_v4(), 0, 1, HOUR, start + 1, start, start + 100);
    let b = l0_record(Uuid::new_v4(), 0, 2, HOUR, start + 1, start, start + 100);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;
    let tail = l0_record(
        Uuid::new_v4(),
        0,
        1,
        HOUR + 1,
        start + NS_PER_HOUR + 1,
        start + NS_PER_HOUR,
        start + NS_PER_HOUR + 100,
    );
    put_l0(store.as_ref(), &tail).await;

    let now_1 = now_at_seal(HOUR + 1);
    catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
        .await
        .expect("first fold");

    // Two live, non-superseding rewrites land in the sealed bucket.
    let rw_a = build_rewrite(
        0,
        HOUR,
        &[&a],
        None,
        vec![part(0, start, start + 100, 0x51)],
        &[Uuid::new_v4()],
        start + 2,
    );
    let rw_b = build_rewrite(
        0,
        HOUR,
        &[&b],
        None,
        vec![part(0, start, start + 100, 0x52)],
        &[Uuid::new_v4()],
        start + 3,
    );
    put_rewrite(store.as_ref(), &rw_a).await;
    put_rewrite(store.as_ref(), &rw_b).await;

    let now_2 = now_at_seal(HOUR + 3);
    catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
        .await
        .expect("second fold");

    let snapshot = catalog
        .resolve(
            &tenant(),
            Signal::Metrics,
            TimeRange {
                start_ns: start,
                end_ns: start + 100,
            },
            &[],
            now_2,
        )
        .await
        .expect("resolve");

    // Both siblings' parts are served (the ADR-sanctioned posture: correctness
    // still rests on the section 2 query-time filter), but the conflict must
    // not be silent -- the fold is the only observer in this scenario.
    assert_eq!(snapshot.segments.len(), 2);
    assert!(
        catalog.rewrite_sibling_conflicts() >= 1,
        "the fold must raise the sibling-rewrite alarm; a resolve served from the folded \
         snapshot never reaches process_bucket, so nothing else would"
    );
}
