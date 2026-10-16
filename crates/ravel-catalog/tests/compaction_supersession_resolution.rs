//! Resolution honours a version 2 compaction record (ADR-0066 force 2
//! amendment, items 2, 3 and 5).
//!
//! A version 2 record names, in `superseded_record_key`, the compaction record
//! it re-encodes. Its inputs are the predecessor's, so left to the overlap
//! rule alone the two would form one component and the tie-break could keep
//! the predecessor. The selector excludes every record a present version 2
//! record names, following chains of them, before any component is formed,
//! and refuses a version 2 record whose inputs differ from its present
//! predecessor's as a typed error. And when a live rewrite record and a version 2 record both supersede the
//! same predecessor, the rewrite wins: the version 2 record's parts re-encode
//! the pre-erasure data.
//!
//! Every test writes records directly to a `MemoryStore` and drives the real
//! `Catalog` (resolve, the token fallback, or the fold), which is the path
//! every query takes. No segment bytes are needed: resolution reads only
//! records. Each predecessor is given the all-zero `input_set_hash` where the
//! test needs the pre-supersession tie-break to pick it, and the all-`0xff`
//! hash where the test needs the tie-break to pick its successor, so a build
//! without the rule serves the wrong record rather than the right one by luck.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_catalog::{
    Catalog, CatalogConfig, CatalogError, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, SegmentLevel,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord, RewriteDrop,
    RewriteRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn tenant() -> TenantHash {
    TenantHash([0xab; 16])
}

fn catalog(store: &Arc<MemoryStore>) -> Catalog {
    Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            ..Default::default()
        },
    )
    .expect("catalog")
}

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

/// A self-consistent L0 commit record in `HOUR`. Never written to the store:
/// the tests need only its identity (and, for the token fallback, its token).
fn l0_record(seq: u64) -> CommitRecord {
    let start = hour_start();
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard: 0,
        writer_id: Uuid::from_u128(0x5eed_0000 + u128::from(seq)),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: 100,
        content_hash: [seq as u8 ^ 0x5a; 32],
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: start,
        max_event_ts_ns: start + 100,
        min_ingest_ts_ns: start,
        max_ingest_ts_ns: start + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: start,
        ingest_hour_bucket: HOUR,
    })
    .expect("valid record")
}

fn identity(record: &CommitRecord) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: record.writer_id.clone(),
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
    }
}

/// One part inside the hour. `seed` makes each record's part key distinct.
fn part(seed: u8) -> CompactionPart {
    let start = hour_start();
    CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: 10,
        series_count: 2,
        run_count: 3,
        min_event_ts_ns: start,
        max_event_ts_ns: start + 100,
        segment_format_version: u32::from(VERSION_V7),
        declared_column_stats: Vec::new(),
    }
}

async fn put_compaction(store: &dyn ObjectStoreBackend, record: &CompactionRecord) -> String {
    let key = keys::compaction_record_key_for(record).expect("record key");
    store
        .put(
            &key,
            record::encode_compaction(record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
    key
}

/// A version 1 compaction record over `inputs` whose `input_set_hash` is
/// `hash_byte` repeated, so a test decides where it falls in the overlap
/// tie-break (version 1 decoding does not recompute the hash).
fn version_1(inputs: &[&CommitRecord], hash_byte: u8, seed: u8) -> CompactionRecord {
    CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        level: 1,
        inputs: inputs.iter().map(|r| identity(r)).collect(),
        input_set_hash: vec![hash_byte; 32],
        parts: vec![part(seed)],
        created_unix_ns: hour_start() + 1_000,
        superseded_record_key: String::new(),
    }
}

/// A valid version 2 record re-encoding `predecessor` (at `predecessor_key`):
/// the predecessor's inputs verbatim, its own parts, and the version 2 hash
/// over those inputs and the key, which decoding recomputes.
fn version_2(predecessor: &CompactionRecord, predecessor_key: &str, seed: u8) -> CompactionRecord {
    CompactionRecord {
        format_version: 2,
        input_set_hash: erasure::compute_superseding_compaction_input_set_hash(
            &predecessor.inputs,
            predecessor_key,
        )
        .to_vec(),
        parts: vec![part(seed)],
        created_unix_ns: predecessor.created_unix_ns + 1_000,
        superseded_record_key: predecessor_key.to_string(),
        ..predecessor.clone()
    }
}

/// A rewrite record superseding the whole record at `superseded_key`, applying
/// one erasure request, with one output part.
async fn put_rewrite(
    store: &dyn ObjectStoreBackend,
    superseded_key: &str,
    seed: u8,
) -> RewriteRecord {
    let request_id = Uuid::from_u128(0xe7a5e).to_string();
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        inputs: Vec::new(),
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &[],
            Some(superseded_key),
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        parts: vec![part(seed)],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: hour_start() + 5_000,
        superseded_record_key: superseded_key.to_string(),
    };
    let key = keys::rewrite_record_key_for(&record).expect("rewrite key");
    store
        .put(
            &key,
            erasure::encode_rewrite(&record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put rewrite record");
    record
}

fn part_key(record: &CompactionRecord) -> String {
    keys::reconstruct_l1_part_key(record, &record.parts[0]).expect("part key")
}

fn rewrite_part_key(record: &RewriteRecord) -> String {
    keys::reconstruct_rewrite_part_key(record, &record.parts[0]).expect("rewrite part key")
}

fn l1_keys(snapshot: &ravel_catalog::Snapshot) -> Vec<String> {
    let mut out: Vec<String> = snapshot
        .segments
        .iter()
        .filter(|s| matches!(s.level, SegmentLevel::L1 { .. }))
        .map(|s| s.data_object_key.clone())
        .collect();
    out.sort();
    out
}

/// A listing resolve over the hour, before it seals.
fn live_window() -> (TimeRange, i64) {
    let start = hour_start();
    (
        TimeRange {
            start_ns: start,
            end_ns: start + NS_PER_HOUR,
        },
        start + 30 * 60_000_000_000,
    )
}

/// A window and `now` at which the fold seals `HOUR`.
fn sealed_window() -> (TimeRange, i64) {
    let start = hour_start();
    let now = i64::from(HOUR + 1) * NS_PER_HOUR + MARGIN_NS + 1;
    (
        TimeRange {
            start_ns: start,
            end_ns: now,
        },
        now,
    )
}

/// C1 over `[a, b]` with the all-zero hash, and a version 2 C2 naming C1.
async fn predecessor_and_successor(
    store: &dyn ObjectStoreBackend,
) -> (CompactionRecord, CompactionRecord) {
    let (a, b) = (l0_record(1), l0_record(2));
    let c1 = version_1(&[&a, &b], 0x00, 0xc1);
    let c1_key = put_compaction(store, &c1).await;
    let c2 = version_2(&c1, &c1_key, 0xc2);
    put_compaction(store, &c2).await;
    (c1, c2)
}

/// Resolve returns the version 2 record's parts, not its predecessor's.
///
/// Flipped line: `let superseded = superseded_by_version_2_records(records)?;`
/// in `select_authoritative_compaction_records` (catalog.rs), replaced by an
/// empty set. C1 and C2 then share every input, form one component, and the
/// all-zero hash makes C1 the winner, so the snapshot names C1's part. The
/// interlock counter assertion has its own line: the
/// `!selection.superseded().contains(k)` filter on `input_set_hashes` in
/// `process_bucket`; without it the counter reads 1.
#[tokio::test]
async fn resolve_serves_the_version_2_record_not_its_predecessor() {
    let store = Arc::new(MemoryStore::new());
    let (_c1, c2) = predecessor_and_successor(store.as_ref()).await;
    let (range, now) = live_window();

    let catalog = catalog(&store);
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(l1_keys(&snapshot), vec![part_key(&c2)]);
    assert_eq!(snapshot.segments.len(), 1);
    assert_eq!(
        catalog.compaction_input_set_conflicts(),
        0,
        "a predecessor beside its successor is not an interlock breach"
    );
}

/// The token fallback serves a token its predecessor covered from the
/// version 2 record's parts.
///
/// Flipped line: the `selection.is_excluded(ckey)` skip in
/// `resolve_min_token_fallback`'s compaction loop (catalog.rs). C1 covers the
/// token and is listed first by key, so the fallback adds C1's part to the C2
/// part the bucket listing already resolved, and the snapshot names both.
#[tokio::test]
async fn token_fallback_serves_the_version_2_record_not_its_predecessor() {
    let store = Arc::new(MemoryStore::new());
    let (c1, c2) = predecessor_and_successor(store.as_ref()).await;
    assert!(
        keys::compaction_record_key_for(&c1).unwrap()
            < keys::compaction_record_key_for(&c2).unwrap(),
        "C1's all-zero hash lists it first, so the fallback meets it before C2"
    );
    let token = record::token_for(&l0_record(1)).expect("token");
    let (range, now) = live_window();

    let snapshot = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[token], now)
        .await
        .expect("resolve");
    assert_eq!(l1_keys(&snapshot), vec![part_key(&c2)]);
}

/// The fold classifies the bucket from the version 2 record. After the fold
/// both records are deleted, so the resolve that follows can only be served
/// by what the fold wrote.
///
/// Flipped line: the same selector line as
/// `resolve_serves_the_version_2_record_not_its_predecessor`; the fold then
/// writes C1's part into the snapshot.
#[tokio::test]
async fn fold_classifies_the_bucket_from_the_version_2_record() {
    let store = Arc::new(MemoryStore::new());
    let (c1, c2) = predecessor_and_successor(store.as_ref()).await;
    let (range, now) = sealed_window();

    let report = catalog(&store)
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    assert_eq!(
        report.watermark_hour,
        Some(HOUR),
        "the fold sealed the hour"
    );

    for record in [&c1, &c2] {
        store
            .delete(&keys::compaction_record_key_for(record).unwrap())
            .await
            .expect("delete record");
    }
    let snapshot = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve from the folded snapshot");
    assert_eq!(l1_keys(&snapshot), vec![part_key(&c2)]);
    assert_eq!(snapshot.segments.len(), 1);
}

/// A chain of three (C3 supersedes C2 supersedes C1) resolves to its head.
///
/// Flipped line: the same selector line; all three then form one component
/// and the all-zero hash makes C1 the winner.
#[tokio::test]
async fn a_three_link_chain_resolves_to_its_head() {
    let store = Arc::new(MemoryStore::new());
    let (c1, c2) = predecessor_and_successor(store.as_ref()).await;
    let c2_key = keys::compaction_record_key_for(&c2).unwrap();
    let c3 = version_2(&c2, &c2_key, 0xc3);
    put_compaction(store.as_ref(), &c3).await;
    let (range, now) = live_window();

    let snapshot = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(l1_keys(&snapshot), vec![part_key(&c3)]);
    assert_ne!(part_key(&c1), part_key(&c3));
}

/// A version 2 record whose predecessor is not present excludes nothing: it is
/// served on its own, like any record without a conflict.
#[tokio::test]
async fn a_version_2_record_naming_an_absent_key_excludes_nothing() {
    let store = Arc::new(MemoryStore::new());
    let (a, b, c) = (l0_record(1), l0_record(2), l0_record(3));
    let swept = version_1(&[&a, &b], 0x00, 0xc1);
    let swept_key = keys::compaction_record_key_for(&swept).unwrap();
    let c2 = version_2(&swept, &swept_key, 0xc2);
    put_compaction(store.as_ref(), &c2).await;
    let other = version_1(&[&c], 0x11, 0xd1);
    put_compaction(store.as_ref(), &other).await;
    let (range, now) = live_window();

    let snapshot = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    let mut expected = vec![part_key(&c2), part_key(&other)];
    expected.sort();
    assert_eq!(l1_keys(&snapshot), expected);
}

/// C1 over `[a, b]` and a version 2 C2 naming C1 but carrying `[a, b, c]`: a
/// record a writer copying its predecessor's inputs would never produce, and
/// one decoding cannot refuse, since the version 2 hash is over C2's own
/// inputs. Returns C1's key and C2's key.
async fn mismatched_successor(store: &dyn ObjectStoreBackend) -> (String, String) {
    let (a, b, c) = (l0_record(1), l0_record(2), l0_record(3));
    let c1 = version_1(&[&a, &b], 0x00, 0xc1);
    let c1_key = put_compaction(store, &c1).await;
    let mut c2 = version_2(&c1, &c1_key, 0xc2);
    c2.inputs.push(identity(&c));
    c2.input_set_hash =
        erasure::compute_superseding_compaction_input_set_hash(&c2.inputs, &c1_key).to_vec();
    let c2_key = put_compaction(store, &c2).await;
    (c1_key, c2_key)
}

fn is_input_mismatch(err: &CatalogError, c1_key: &str, c2_key: &str) -> bool {
    matches!(
        err,
        CatalogError::CompactionSupersessionInputMismatch { key, superseded_key }
            if key == c2_key && superseded_key == c1_key
    )
}

/// A version 2 record whose inputs differ from its present predecessor's is a
/// typed error on resolve and on the fold, never an exclusion of the
/// predecessor.
///
/// Flipped line: `check_version_2_inputs(records)?;` in
/// `superseded_by_version_2_records` (catalog.rs). Without it C1 is excluded,
/// the resolve serves C2's part, and the fold seals the hour.
#[tokio::test]
async fn a_version_2_record_with_other_inputs_is_a_typed_error() {
    let store = Arc::new(MemoryStore::new());
    let (c1_key, c2_key) = mismatched_successor(store.as_ref()).await;

    let (range, now) = live_window();
    let err = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect_err("resolve must refuse the bucket");
    assert!(is_input_mismatch(&err, &c1_key, &c2_key), "{err:?}");

    let (_, now) = sealed_window();
    let err = catalog(&store)
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect_err("the fold must refuse the bucket");
    assert!(is_input_mismatch(&err, &c1_key, &c2_key), "{err:?}");
}

/// The same mismatch with a live rewrite naming C1 is the same error, although
/// dominance drops C2 before the selector sees it.
///
/// Flipped line: `check_version_2_inputs(compaction_records)?;` in
/// `erasure_dominated_compaction_records` (catalog.rs). Without it C2 is
/// dominated and dropped, C1 is rewrite-superseded, and the resolve succeeds
/// serving the rewrite's part.
#[tokio::test]
async fn a_dominated_version_2_record_with_other_inputs_is_a_typed_error() {
    let store = Arc::new(MemoryStore::new());
    let (c1_key, c2_key) = mismatched_successor(store.as_ref()).await;
    put_rewrite(store.as_ref(), &c1_key, 0x0e).await;
    let (range, now) = live_window();

    let err = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect_err("resolve must refuse the bucket");
    assert!(is_input_mismatch(&err, &c1_key, &c2_key), "{err:?}");
}

/// C1 over `[a, b]` with the all-`0xff` hash, a version 2 C2 naming C1, and a
/// live rewrite R superseding C1. Without the rewrite, the overlap tie-break
/// alone would keep C2.
async fn dominated_bucket(
    store: &dyn ObjectStoreBackend,
) -> (CompactionRecord, CompactionRecord, RewriteRecord) {
    let (a, b) = (l0_record(1), l0_record(2));
    let c1 = version_1(&[&a, &b], 0xff, 0xc1);
    let c1_key = put_compaction(store, &c1).await;
    let c2 = version_2(&c1, &c1_key, 0xc2);
    put_compaction(store, &c2).await;
    let r = put_rewrite(store, &c1_key, 0x0e).await;
    (c1, c2, r)
}

/// A rewrite and a version 2 record both naming C1 resolve to the rewrite's
/// parts only.
///
/// Flipped line: `erasure_dominated_compaction_records` (catalog.rs) returning
/// an empty set. C2 is then authoritative and not rewrite-superseded, so its
/// re-encode of the pre-erasure data is served beside the rewrite's part.
#[tokio::test]
async fn resolve_prefers_the_rewrite_over_a_dominated_version_2_record() {
    let store = Arc::new(MemoryStore::new());
    let (_c1, _c2, r) = dominated_bucket(store.as_ref()).await;
    let (range, now) = live_window();

    let snapshot = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(l1_keys(&snapshot), vec![rewrite_part_key(&r)]);
    assert_eq!(snapshot.segments.len(), 1);
}

/// The token fallback serves a token C1 covered from the rewrite, not from the
/// dominated version 2 record that also covers it.
///
/// Flipped line: the same dominance function. The fallback's compaction loop
/// then finds C2 live and covering the token and serves C2's part before the
/// rewrite loop runs.
#[tokio::test]
async fn token_fallback_prefers_the_rewrite_over_a_dominated_version_2_record() {
    let store = Arc::new(MemoryStore::new());
    let (_c1, _c2, r) = dominated_bucket(store.as_ref()).await;
    let token = record::token_for(&l0_record(2)).expect("token");
    let (range, now) = live_window();

    let snapshot = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[token], now)
        .await
        .expect("resolve");
    assert_eq!(l1_keys(&snapshot), vec![rewrite_part_key(&r)]);
}

/// The fold of a dominated bucket names the rewrite's part only, read back
/// after every record is deleted so only the fold's output can answer.
///
/// Flipped line: the same dominance function; the fold then also writes C2's
/// part.
#[tokio::test]
async fn fold_prefers_the_rewrite_over_a_dominated_version_2_record() {
    let store = Arc::new(MemoryStore::new());
    let (c1, c2, r) = dominated_bucket(store.as_ref()).await;
    let (range, now) = sealed_window();

    catalog(&store)
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    for key in [
        keys::compaction_record_key_for(&c1).unwrap(),
        keys::compaction_record_key_for(&c2).unwrap(),
        keys::rewrite_record_key_for(&r).unwrap(),
    ] {
        store.delete(&key).await.expect("delete record");
    }
    let snapshot = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve from the folded snapshot");
    assert_eq!(l1_keys(&snapshot), vec![rewrite_part_key(&r)]);
}

/// Dominance does not need the predecessor present: once a sweep has removed
/// C1, the rewrite still names it and C2 stays excluded.
#[tokio::test]
async fn dominance_outlives_the_swept_predecessor() {
    let store = Arc::new(MemoryStore::new());
    let (c1, _c2, r) = dominated_bucket(store.as_ref()).await;
    store
        .delete(&keys::compaction_record_key_for(&c1).unwrap())
        .await
        .expect("delete predecessor");
    let (range, now) = live_window();

    let snapshot = catalog(&store)
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    assert_eq!(l1_keys(&snapshot), vec![rewrite_part_key(&r)]);
}
