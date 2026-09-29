//! Maintenance over a bucket holding a version 2 compaction record (ADR-0066
//! force 2 amendment, items 2 to 6).
//!
//! A version 2 record names, in `superseded_record_key`, the compaction record
//! it re-encodes. The shared selector excludes that predecessor, and a version
//! 2 record whose predecessor a live rewrite record supersedes is dropped
//! before the selector runs. The erasure completion gate and `migrate` follow
//! both rules, as the resolver does. A version 2 record whose inputs differ
//! from its present predecessor's is a typed error for all of them. The sweep
//! reclaims a superseded predecessor and its parts as a chain group entered
//! from the version 2 record, under that record's horizon, the HEAD
//! reachability gate and legal hold, and reclaims an erasure-dominated version
//! 2 record with its rewrite's chain group.
//!
//! Every fixture writes records directly to a `MemoryStore` (or a `FaultStore`
//! over one) and drives the production entries (`sweep_superseded`,
//! `sweep_unreferenced_parts`, `bucket_erasure_completion`,
//! `count_below_target`, `migrate::largest_overlap_component`, a catalog
//! resolve). Each test's doc comment names the line whose change it catches.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use ravel_commit::{erasure, keys, record, signal};
use ravel_maintain::migrate::largest_overlap_component;
use ravel_maintain::{
    Bucket, CompactorConfig, FixedClock, LeaseCheck, NoLeases, PendingErasureRequest,
    SupersededSweepOutcome, bucket_erasure_completion, count_below_target, sweep_superseded,
    sweep_unreferenced_parts,
};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, ErasurePredicateMatcher,
    ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_types::{Signal, TimeRange};
use uuid::Uuid;

fn hour_ns() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// Past every record's protection horizon, so a deleting sweep pass is not
/// gated on age.
fn past_horizon_ns() -> i64 {
    hour_ns() + cfg().protection_horizon_ns + 10 * NS_PER_HOUR
}

async fn all_keys(store: &dyn ObjectStoreBackend) -> BTreeSet<String> {
    list_all(store, "t/")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

fn input(writer: u128, seq: u64) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: Uuid::from_u128(writer).to_string(),
        writer_epoch: 1,
        writer_seq: seq,
    }
}

/// A part covering `[min_ts, max_ts]`; `seed` keeps part keys distinct.
fn part(seed: u8, min_ts: i64, max_ts: i64) -> CompactionPart {
    CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: 2,
        series_count: 1,
        run_count: 1,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Vec::new(),
    }
}

/// A version 1 record in `bucket` whose `input_set_hash` is `hash_byte`
/// repeated, so a test decides where it falls in the overlap tie-break.
fn version_1(
    bucket: &Bucket,
    inputs: Vec<CompactionInputIdentity>,
    hash_byte: u8,
    part: CompactionPart,
) -> CompactionRecord {
    CompactionRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        level: 1,
        inputs,
        input_set_hash: vec![hash_byte; 32],
        parts: vec![part],
        created_unix_ns: hour_ns(),
        superseded_record_key: String::new(),
    }
}

/// A valid version 2 record naming `predecessor_key` over `inputs` (the
/// predecessor's own inputs, unless a test deliberately departs from them).
fn version_2(
    predecessor: &CompactionRecord,
    predecessor_key: &str,
    inputs: Vec<CompactionInputIdentity>,
    part: CompactionPart,
) -> CompactionRecord {
    CompactionRecord {
        format_version: 2,
        input_set_hash: erasure::compute_superseding_compaction_input_set_hash(
            &inputs,
            predecessor_key,
        )
        .to_vec(),
        inputs,
        parts: vec![part],
        created_unix_ns: predecessor.created_unix_ns + 1_000,
        superseded_record_key: predecessor_key.to_string(),
        ..predecessor.clone()
    }
}

/// PUT a compaction record and an object at each of its part keys. Returns the
/// record key.
async fn put_compaction(store: &dyn ObjectStoreBackend, record: &CompactionRecord) -> String {
    for p in &record.parts {
        let part_key = keys::reconstruct_l1_part_key(record, p).expect("part key");
        store
            .put(
                &part_key,
                bytes::Bytes::from_static(b"l1-part"),
                PutOptions::default(),
            )
            .await
            .expect("put part object");
    }
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

/// PUT a rewrite record superseding the whole record at `superseded_key` and
/// applying `request_id`, plus an object at its part key. Returns the record.
async fn put_rewrite(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    superseded_key: &str,
    request_id: Uuid,
    part: CompactionPart,
) -> RewriteRecord {
    let request_id = request_id.to_string();
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        inputs: Vec::new(),
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &[],
            Some(superseded_key),
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        parts: vec![part],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: hour_ns() + 5_000,
        superseded_record_key: superseded_key.to_string(),
    };
    let part_key = keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part");
    store
        .put(
            &part_key,
            bytes::Bytes::from_static(b"rw-part"),
            PutOptions::default(),
        )
        .await
        .expect("put rewrite part");
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

async fn data_key_of(store: &dyn ObjectStoreBackend, commit_key: &str) -> String {
    let bytes = get_full(store, commit_key).await;
    let rec = record::decode(&bytes).expect("decode commit record");
    keys::reconstruct_data_key(&rec).expect("data key")
}

/// Rule 2 then rule 3 past every horizon, with no HEAD, so nothing is held.
/// Returns the keys the two passes deleted.
async fn sweep_everything(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> BTreeSet<String> {
    let before = all_keys(store).await;
    let clock = FixedClock::new(past_horizon_ns());
    let outcome = sweep_superseded(
        store,
        &clock,
        &cfg(),
        &NoLeases,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
    )
    .await
    .expect("rule 2");
    assert_eq!(outcome.held_by_snapshot, 0);
    assert_eq!(outcome.held_by_unreadable_head, 0);
    sweep_unreferenced_parts(
        store,
        &clock,
        &cfg(),
        &NoLeases,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
    )
    .await
    .expect("rule 3");
    let after = all_keys(store).await;
    before.difference(&after).cloned().collect()
}

fn part_key(record: &CompactionRecord) -> String {
    keys::reconstruct_l1_part_key(record, &record.parts[0]).unwrap()
}

/// Rule 2 alone at `now_ns` under `lease`.
async fn sweep_at(
    store: &dyn ObjectStoreBackend,
    b: &Bucket,
    now_ns: i64,
    lease: &dyn LeaseCheck,
) -> SupersededSweepOutcome {
    sweep_superseded(
        store,
        &FixedClock::new(now_ns),
        &cfg(),
        lease,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 2")
}

/// Every key present, less the catalog objects a test put there itself.
async fn bucket_keys(store: &dyn ObjectStoreBackend) -> BTreeSet<String> {
    all_keys(store)
        .await
        .into_iter()
        .filter(|k| !k.contains("/catalog/"))
        .collect()
}

/// C1 and a version 2 C2 naming it, in the logs bucket, with no raw L0 input
/// left: they were swept long before a version 2 record is written.
async fn seed_predecessor_pair(
    store: &dyn ObjectStoreBackend,
) -> (CompactionRecord, String, CompactionRecord, String) {
    let b = logs_bucket();
    let base = hour_ns();
    let c1 = version_1(
        &b,
        vec![input(0xA1, 1), input(0xA2, 2)],
        0x00,
        part(0xc1, base, base + 10),
    );
    let c1_key = put_compaction(store, &c1).await;
    let c2 = version_2(&c1, &c1_key, c1.inputs.clone(), part(0xc2, base, base + 10));
    let c2_key = put_compaction(store, &c2).await;
    (c1, c1_key, c2, c2_key)
}

/// The level-1 snapshot entry a fold writes for `record`'s first part.
fn l1_entry(record: &CompactionRecord) -> SnapshotEntry {
    let p = &record.parts[0];
    SnapshotEntry {
        level: 1,
        shard: record.shard,
        ingest_hour_bucket: record.ingest_hour_bucket,
        writer_id: record.input_set_hash.clone(),
        writer_epoch: u64::from(p.part_index),
        writer_seq: 0,
        content_hash: p.content_hash.clone(),
        object_size: p.object_size,
        min_event_ts_ns: p.min_event_ts_ns,
        max_event_ts_ns: p.max_event_ts_ns,
        sample_count: p.sample_count,
        series_count: p.series_count,
        segment_format_version: p.segment_format_version,
        created_unix_ns: record.created_unix_ns,
        declared_column_stats: Vec::new(),
    }
}

/// The level-0 snapshot entry a fold writes for the raw input at `commit_key`.
async fn l0_entry(store: &dyn ObjectStoreBackend, commit_key: &str) -> SnapshotEntry {
    let rec = record::decode(&get_full(store, commit_key).await).expect("decode commit record");
    SnapshotEntry {
        level: 0,
        shard: rec.shard,
        ingest_hour_bucket: rec.ingest_hour_bucket,
        writer_id: Uuid::parse_str(&rec.writer_id).unwrap().as_bytes().to_vec(),
        writer_epoch: rec.writer_epoch,
        writer_seq: rec.writer_seq,
        content_hash: rec.content_hash.clone(),
        object_size: 1,
        min_event_ts_ns: rec.min_event_ts_ns,
        max_event_ts_ns: rec.max_event_ts_ns,
        sample_count: 1,
        series_count: 1,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 0,
        declared_column_stats: Vec::new(),
    }
}

/// PUT a catalog HEAD whose one snapshot part holds `entries`, the way a fold
/// that ran before a successor record was written names what it served.
async fn put_head_with(store: &dyn ObjectStoreBackend, entries: Vec<SnapshotEntry>) {
    let signal_id = signal::to_proto(Signal::Logs) as u32;
    let entry_count = entries.len() as u64;
    let bytes = ravel_catalog::encode_part(tenant_hash().0, signal_id, SHARD + 1, HOUR, &entries)
        .expect("snapshot part encodes");
    let prefix = format!(
        "t/{}/catalog/{}",
        tenant_hash().to_hex(),
        Signal::Logs.key_prefix()
    );
    let part_key = format!("{prefix}/snap/test-part.csnap");
    store
        .put(
            &part_key,
            bytes::Bytes::from(bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put snapshot part");
    let head = SnapshotHead {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal_id,
        shard_count: SHARD + 1,
        watermark_hour: HOUR,
        parts: vec![SnapshotPartRef {
            key: part_key,
            blake3: blake3::hash(&bytes).as_bytes().to_vec(),
            size: bytes.len() as u64,
            entry_count,
            watermark_hour: HOUR,
            min_hour: 0,
            column_stats: None,
        }],
        folder_id: vec![0u8; 16],
        created_unix_ns: 0,
        postings: None,
        shard_generation_count: 1,
    };
    store
        .put(
            &format!("{prefix}/HEAD"),
            bytes::Bytes::from(ravel_catalog::encode_head(&head).expect("HEAD encodes")),
            PutOptions::default(),
        )
        .await
        .expect("put HEAD");
}

/// A [`LeaseCheck`] protecting one exact key, standing in for a legal hold.
struct HoldKey(String);

impl LeaseCheck for HoldKey {
    fn is_protected(&self, key: &str) -> bool {
        key == self.0
    }
}

/// C1 and a version 2 C2 naming it, swept one nanosecond before C2's
/// protection horizon passes: nothing is deleted, although C1's own horizon
/// passed long before. The chain group is gated on the record that superseded
/// C1, since a query pinned before C2 was written may still read C1's part.
///
/// This passes on the code before the change too, which never reclaimed C1.
/// Flipped line: the `now < record.created_unix_ns.saturating_add(config
/// .protection_horizon_ns)` gate in rule 2's compaction arm (sweep.rs), which
/// the version 2 chain group sits behind. Without it C1 and its part go.
#[tokio::test]
async fn sweep_keeps_a_superseded_predecessor_inside_the_version_2_horizon() {
    let store = MemoryStore::new();
    let b = logs_bucket();
    let (c1, _, c2, _) = seed_predecessor_pair(&store).await;
    let before = bucket_keys(&store).await;
    let now = c2.created_unix_ns + cfg().protection_horizon_ns - 1;
    assert!(now >= c1.created_unix_ns + cfg().protection_horizon_ns);

    let outcome = sweep_at(&store, &b, now, &NoLeases).await;
    assert_eq!(outcome, SupersededSweepOutcome::default());
    assert_eq!(bucket_keys(&store).await, before);
}

/// C1 and a version 2 C2 naming it, swept past C2's horizon with no HEAD:
/// C1's part and then C1 are deleted, one record and one data object, and C2
/// with its part stays. A second pass finds nothing more.
///
/// Fails on the code before the change, which deleted nothing here. Flipped
/// line: `if deleting && version_2.heads.contains(key)` in rule 2's compaction
/// arm (sweep.rs); without the version 2 chain group C1 and its part survive.
#[tokio::test]
async fn sweep_reclaims_a_superseded_predecessor_past_the_horizon() {
    let store = MemoryStore::new();
    let b = logs_bucket();
    let (c1, c1_key, c2, c2_key) = seed_predecessor_pair(&store).await;

    let outcome = sweep_at(&store, &b, past_horizon_ns(), &NoLeases).await;
    assert_eq!((outcome.records_deleted, outcome.data_deleted), (1, 1));
    assert_eq!(outcome.held(), 0);
    assert_eq!(
        bucket_keys(&store).await,
        BTreeSet::from([c2_key.clone(), part_key(&c2)])
    );
    let again = sweep_at(&store, &b, past_horizon_ns(), &NoLeases).await;
    assert_eq!((again.records_deleted, again.data_deleted), (0, 0));
    assert!(!bucket_keys(&store).await.contains(&c1_key));
    assert!(!bucket_keys(&store).await.contains(&part_key(&c1)));
}

/// The chain group deletes parts first and records after: a delete of C1's
/// record that fails leaves C1's part already gone and C1 still present, never
/// the reverse.
///
/// Fails on the code before the change, where no delete is issued and the
/// part survives. Flipped line: the phase C loop over `group.data_keys`
/// running before the loop over `group.chain_record_keys` (sweep.rs).
#[tokio::test]
async fn sweep_deletes_a_predecessors_parts_before_its_record() {
    let mem = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let (c1, c1_key, _, _) = seed_predecessor_pair(mem.as_ref()).await;
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::Delete, ScriptedFault::Timeout).with_key_contains(&c1_key));
    let store = FaultStore::new(mem.clone(), plan);

    let err = sweep_superseded(
        &store,
        &FixedClock::new(past_horizon_ns()),
        &cfg(),
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await;
    assert!(err.is_err(), "the failed record delete fails the pass");
    assert_eq!(store.fault_count(Op::Delete, FaultKind::Timeout), 1);
    let left = bucket_keys(mem.as_ref()).await;
    assert!(!left.contains(&part_key(&c1)), "the part went first");
    assert!(left.contains(&c1_key), "the record outlives its part");
}

/// C1 and a version 2 C2 naming it, past C2's horizon, with a HEAD that still
/// names C1's part: nothing of C1 is deleted, and the pass counts C1's record
/// and part as held by the snapshot.
///
/// Fails on the code before the change, which counted nothing held (it
/// gathered no group). Flipped line: the `reach.object_gate(...)` match in
/// phase B (sweep.rs) answering `Clear`; C1 and its part are then deleted.
#[tokio::test]
async fn sweep_holds_a_predecessor_a_head_still_names() {
    let store = MemoryStore::new();
    let b = logs_bucket();
    let (c1, _, _, _) = seed_predecessor_pair(&store).await;
    put_head_with(&store, vec![l1_entry(&c1)]).await;
    let before = bucket_keys(&store).await;

    let outcome = sweep_at(&store, &b, past_horizon_ns(), &NoLeases).await;
    assert_eq!((outcome.records_deleted, outcome.data_deleted), (0, 0));
    assert_eq!(outcome.held_by_snapshot, 2, "C1's record and its part");
    assert_eq!(outcome.held_by_unreadable_head, 0);
    assert_eq!(bucket_keys(&store).await, before);
}

/// C1 and a version 2 C2 naming it, past C2's horizon, with a legal hold on
/// C1's part: the whole group is held, so C1's record stays too.
///
/// Fails on the code before the change, which held no group. Flipped line:
/// `if let Some(protected) = group.protected_key(lease)` in phase B
/// (sweep.rs); without it C1 and its part are deleted.
#[tokio::test]
async fn sweep_holds_a_predecessor_under_a_legal_hold() {
    let store = MemoryStore::new();
    let b = logs_bucket();
    let (c1, _, _, _) = seed_predecessor_pair(&store).await;
    let before = bucket_keys(&store).await;

    let hold = HoldKey(part_key(&c1));
    let outcome = sweep_at(&store, &b, past_horizon_ns(), &hold).await;
    assert_eq!((outcome.records_deleted, outcome.data_deleted), (0, 0));
    assert_eq!(outcome.chain_groups_held_by_legal_hold, 1);
    assert_eq!(bucket_keys(&store).await, before);
}

/// A version 2 record naming a key that is not present reclaims nothing: C2
/// alone, past its horizon.
#[tokio::test]
async fn sweep_reclaims_nothing_for_an_absent_predecessor() {
    let store = MemoryStore::new();
    let b = logs_bucket();
    let (_, c1_key, _, _) = seed_predecessor_pair(&store).await;
    store.delete(&c1_key).await.expect("drop C1");
    let before = bucket_keys(&store).await;

    let outcome = sweep_at(&store, &b, past_horizon_ns(), &NoLeases).await;
    assert_eq!(outcome, SupersededSweepOutcome::default());
    assert_eq!(bucket_keys(&store).await, before);
}

/// C3 over C2 over C1, all version 2 above C1, past every horizon: one chain
/// group entered from C3 reclaims C2 and C1 with their parts, two records and
/// two data objects, and C3 stays.
///
/// Fails on the code before the change, which deleted nothing. Flipped line:
/// `cursor = link.superseded_record_key()` in `gather_superseded_chain`
/// (sweep.rs) returning `None` for a compaction link; the walk then stops at
/// C2 and C1 is left, superseded by C2, with its part.
#[tokio::test]
async fn sweep_reclaims_a_three_link_version_2_chain() {
    let store = MemoryStore::new();
    let b = logs_bucket();
    let base = hour_ns();
    let (_, _, c2, c2_key) = seed_predecessor_pair(&store).await;
    let c3 = version_2(&c2, &c2_key, c2.inputs.clone(), part(0xc3, base, base + 10));
    let c3_key = put_compaction(&store, &c3).await;

    let outcome = sweep_at(&store, &b, past_horizon_ns(), &NoLeases).await;
    assert_eq!((outcome.records_deleted, outcome.data_deleted), (2, 2));
    assert_eq!(
        bucket_keys(&store).await,
        BTreeSet::from([c3_key, part_key(&c3)])
    );
}

/// The superseded raw L0 inputs of a bucket holding C1 and C2 are deleted
/// exactly as for a bucket holding C2 alone (its predecessor already gone):
/// neither loses an input before any horizon, and past every horizon both lose
/// the same two inputs, each once. The C1 bucket also loses C1 and its part,
/// and nothing else: the chain group entered from C2 holds no raw input.
///
/// Fails on the code before the change on the C1 bucket's key set, which kept
/// C1. Flipped line: `matches!(self, ChainEntry::Rewrite)` in
/// `ChainEntry::gathers_raw_l0_inputs` (sweep.rs) answering `true` for a
/// version 2 entry; the chain group then gathers C1's inputs a second time and
/// the C1 bucket's pass counts five records and five data objects.
#[tokio::test]
async fn sweep_deletes_the_same_raw_inputs_with_or_without_the_predecessor() {
    let base = hour_ns();
    let b = logs_bucket();
    let mut deleted_inputs: Vec<BTreeSet<String>> = Vec::new();
    for keep_predecessor in [true, false] {
        let store = MemoryStore::new();
        let inputs = seed_inputs(&store, &[(0x91, 1), (0x92, 2)]).await;
        let c1 = version_1(
            &b,
            vec![input(0x91, 1), input(0x92, 2)],
            0x00,
            part(0xc1, base, base + 10_000),
        );
        let c1_key = put_compaction(&store, &c1).await;
        let c2 = version_2(
            &c1,
            &c1_key,
            c1.inputs.clone(),
            part(0xc2, base, base + 10_000),
        );
        let c2_key = put_compaction(&store, &c2).await;
        if !keep_predecessor {
            store.delete(&c1_key).await.expect("drop C1");
            store.delete(&part_key(&c1)).await.expect("drop C1's part");
        }
        let input_keys: BTreeSet<String> = inputs
            .iter()
            .flat_map(|(commit, data)| [commit.clone(), data.clone()])
            .collect();

        let young = c1.created_unix_ns + cfg().protection_horizon_ns - 1;
        let before = bucket_keys(&store).await;
        assert_eq!(
            sweep_at(&store, &b, young, &NoLeases).await,
            SupersededSweepOutcome::default()
        );
        assert_eq!(bucket_keys(&store).await, before);

        let outcome = sweep_at(&store, &b, past_horizon_ns(), &NoLeases).await;
        let deleted: BTreeSet<String> = before
            .difference(&bucket_keys(&store).await)
            .cloned()
            .collect();
        let mut expected = input_keys.clone();
        if keep_predecessor {
            expected.extend([c1_key.clone(), part_key(&c1)]);
            assert_eq!((outcome.records_deleted, outcome.data_deleted), (3, 3));
        } else {
            assert_eq!((outcome.records_deleted, outcome.data_deleted), (2, 2));
        }
        assert_eq!(deleted, expected);
        assert_eq!(
            bucket_keys(&store).await,
            BTreeSet::from([c2_key, part_key(&c2)])
        );
        deleted_inputs.push(deleted.intersection(&input_keys).cloned().collect());
    }
    assert_eq!(deleted_inputs[0], deleted_inputs[1]);
}

/// A HEAD naming a raw L0 input holds that input whether or not the
/// predecessor is present, and the predecessor's chain group, which holds no
/// raw input, is not held by it.
///
/// Fails on the code before the change, which left C1 and its part. Flipped
/// line: as for
/// [`sweep_deletes_the_same_raw_inputs_with_or_without_the_predecessor`]; a
/// chain group holding the raw inputs is held with them and C1 survives.
#[tokio::test]
async fn sweep_holds_a_head_named_raw_input_apart_from_the_predecessor() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let inputs = seed_inputs(store.as_ref(), &[(0x95, 1)]).await;
    let c1 = version_1(
        &b,
        vec![input(0x95, 1)],
        0x00,
        part(0xc1, base, base + 10_000),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(
        &c1,
        &c1_key,
        c1.inputs.clone(),
        part(0xc2, base, base + 10_000),
    );
    put_compaction(store.as_ref(), &c2).await;
    // A HEAD folded before either record names the raw input.
    let entry = l0_entry(store.as_ref(), &inputs[0].0).await;
    put_head_with(store.as_ref(), vec![entry]).await;

    let outcome = sweep_at(store.as_ref(), &b, past_horizon_ns(), &NoLeases).await;
    let left = bucket_keys(store.as_ref()).await;
    assert!(left.contains(&inputs[0].0) && left.contains(&inputs[0].1));
    assert_eq!(outcome.held_by_snapshot, 2, "the input's commit and data");
    assert!(!left.contains(&c1_key) && !left.contains(&part_key(&c1)));
    assert_eq!((outcome.records_deleted, outcome.data_deleted), (1, 1));
}

/// A live rewrite R and a version 2 C2 both naming C1, with C1's raw inputs
/// still present, swept past every horizon. The erasure-dominated C2 joins R's
/// chain group: C2 and its part go with C1, its part and the raw inputs, each
/// once, and R with its part stays.
///
/// Fails on the code before the change, which left C2 and its part (and
/// gathered the raw inputs from C2's own entry a second time). Flipped line:
/// `version_2.join_dominated(group, &compactions)?` in rule 2's rewrite arm
/// (sweep.rs); without it C2 and its part survive, named by no record, with
/// parts that re-encode the pre-erasure C1.
#[tokio::test]
async fn sweep_reclaims_an_erasure_dominated_version_2_record_with_its_rewrite() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let inputs = seed_inputs(store.as_ref(), &[(0xB1, 1), (0xB2, 2)]).await;
    let c1 = version_1(
        &b,
        vec![input(0xB1, 1), input(0xB2, 2)],
        0xff,
        part(0xc1, base, base + 10_000),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(
        &c1,
        &c1_key,
        c1.inputs.clone(),
        part(0xc2, base, base + 10_000),
    );
    let c2_key = put_compaction(store.as_ref(), &c2).await;
    let r = put_rewrite(
        store.as_ref(),
        &b,
        &c1_key,
        Uuid::from_u128(0xd0),
        part(0x0e, base, base + 10_000),
    )
    .await;
    let before = bucket_keys(store.as_ref()).await;

    let outcome = sweep_at(store.as_ref(), &b, past_horizon_ns(), &NoLeases).await;
    // Two raw commits and C1, C2; two raw data objects and two parts.
    assert_eq!((outcome.records_deleted, outcome.data_deleted), (4, 4));
    let after = bucket_keys(store.as_ref()).await;
    let deleted: BTreeSet<String> = before.difference(&after).cloned().collect();
    let mut expected: BTreeSet<String> = inputs
        .iter()
        .flat_map(|(commit, data)| [commit.clone(), data.clone()])
        .collect();
    expected.extend([c1_key, part_key(&c1), c2_key, part_key(&c2)]);
    assert_eq!(deleted, expected);
    assert_eq!(
        after,
        BTreeSet::from([
            keys::rewrite_record_key_for(&r).unwrap(),
            keys::reconstruct_rewrite_part_key(&r, &r.parts[0]).unwrap(),
        ])
    );
}

/// The dominated C2 is under R's reachability gate as a member of R's group: a
/// HEAD still naming C2's part (folded while C2 was authoritative, before R
/// was written) holds C1 and R's whole group with it, and every request R
/// applied stays held.
///
/// Fails on the code before the change, which deleted C1 and its part and held
/// nothing. Flipped line: as for
/// [`sweep_reclaims_an_erasure_dominated_version_2_record_with_its_rewrite`];
/// without the join C2's part is outside the group and the gate clears it.
#[tokio::test]
async fn sweep_holds_a_rewrite_group_while_a_head_names_a_dominated_part() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let c1 = version_1(&b, vec![input(0xB5, 1)], 0xff, part(0xc1, base, base + 10));
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(&c1, &c1_key, c1.inputs.clone(), part(0xc2, base, base + 10));
    put_compaction(store.as_ref(), &c2).await;
    let request = Uuid::from_u128(0xd5);
    put_rewrite(
        store.as_ref(),
        &b,
        &c1_key,
        request,
        part(0x0e, base, base + 10),
    )
    .await;
    put_head_with(store.as_ref(), vec![l1_entry(&c2)]).await;
    let before = bucket_keys(store.as_ref()).await;

    let outcome = sweep_at(store.as_ref(), &b, past_horizon_ns(), &NoLeases).await;
    assert_eq!((outcome.records_deleted, outcome.data_deleted), (0, 0));
    assert_eq!(outcome.held_by_snapshot, 4, "C1, C2 and their parts");
    assert_eq!(
        outcome.held_request_ids,
        BTreeSet::from([request.to_string()])
    );
    assert_eq!(bucket_keys(store.as_ref()).await, before);
}

/// Seed one raw L0 input per `(writer, seq)` in the logs bucket, returning
/// each one's commit key and data key.
async fn seed_inputs(
    store: &dyn ObjectStoreBackend,
    inputs: &[(u128, u64)],
) -> Vec<(String, String)> {
    let base = hour_ns();
    let mut commits = Vec::new();
    for &(writer, seq) in inputs {
        let commit = seed_rlog_input(
            store,
            Uuid::from_u128(writer),
            1,
            seq,
            &[log_record(1, base + 1_000 * seq as i64, "row")],
        )
        .await;
        let data = data_key_of(store, &commit).await;
        commits.push((commit, data));
    }
    commits
}

/// The sweep's guard: an input is superseded only where an authoritative
/// record names it both with version 2 supersession honoured and with it
/// ignored. C1 names `[a, b]` with the all-zero hash, C2 (version 2, naming
/// C1) re-encodes the same `[a, b]`, and a version 1 D names `[b, c]` with a
/// hash between C1's and C2's. All three sets are the same size, so the hash
/// decides. Ignored, the three form one component, C1 wins, and the
/// authoritative inputs are `a` and `b`; honoured, C1 is excluded, D beats C2,
/// and they are `b` and `c`. Only `b` is in both, so only `b` goes, together
/// with C1 and its part, which C2's chain group reclaims without touching a
/// raw input.
///
/// Flipped lines, in `AuthoritativeInputs::from_records` (sweep.rs): keeping
/// only the ignored view (the sweep before resolution honoured version 2
/// records) deletes `a`, which the resolver now serves as a raw L0 because no
/// authoritative record names it; keeping only the honoured view deletes `c`
/// while C1 is still present, which a node that ignores version 2
/// supersession still serves as a raw L0. `ChainEntry::gathers_raw_l0_inputs`
/// answering `true` for a version 2 entry deletes `a` with C1's group.
#[tokio::test]
async fn sweep_supersedes_only_inputs_both_views_name() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let commits = seed_inputs(store.as_ref(), &[(0xC1, 1), (0xC2, 2), (0xC3, 3)]).await;
    let c1 = version_1(
        &b,
        vec![input(0xC1, 1), input(0xC2, 2)],
        0x00,
        part(0xc1, base, base + 10_000),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(
        &c1,
        &c1_key,
        c1.inputs.clone(),
        part(0xc2, base, base + 10_000),
    );
    put_compaction(store.as_ref(), &c2).await;
    let d = version_1(
        &b,
        vec![input(0xC2, 2), input(0xC3, 3)],
        0x01,
        part(0xd1, base, base + 10_000),
    );
    assert!(
        d.input_set_hash < c2.input_set_hash,
        "D must beat C2 in the honoured view"
    );
    put_compaction(store.as_ref(), &d).await;

    let deleted = sweep_everything(store.as_ref(), &b).await;
    let (b_commit, b_data) = commits[1].clone();
    assert_eq!(
        deleted,
        BTreeSet::from([b_commit, b_data, c1_key, part_key(&c1)])
    );

    // With C1 gone the two views agree: D beats C2 in both, so `c`, which D's
    // part serves, goes on the next pass, and `a`, which only the losing C2
    // names, is still served raw and stays.
    let deleted = sweep_everything(store.as_ref(), &b).await;
    let (c_commit, c_data) = commits[2].clone();
    assert_eq!(deleted, BTreeSet::from([c_commit, c_data]));
    let (a_commit, a_data) = commits[0].clone();
    let left = all_keys(store.as_ref()).await;
    assert!(left.contains(&a_commit) && left.contains(&a_data));
}

/// A version 2 C2 naming C1 but carrying `[a, b, c]` where C1 carries
/// `[a, b]` is a typed error for every resolution of the bucket, so the sweep
/// treats none of its inputs as superseded and deletes nothing.
///
/// Flipped line: `check_version_2_inputs(records)?;` in
/// `superseded_by_version_2_records` (catalog.rs). Without it C2 is
/// authoritative in both views (honoured, C1 is excluded; ignored, C2's larger
/// set wins), and the sweep deletes all three inputs.
#[tokio::test]
async fn sweep_deletes_nothing_for_a_version_2_record_with_other_inputs() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    seed_inputs(store.as_ref(), &[(0x61, 1), (0x62, 2), (0x63, 3)]).await;
    let c1 = version_1(
        &b,
        vec![input(0x61, 1), input(0x62, 2)],
        0x00,
        part(0xc1, base, base + 10_000),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(
        &c1,
        &c1_key,
        vec![input(0x61, 1), input(0x62, 2), input(0x63, 3)],
        part(0xc2, base, base + 10_000),
    );
    put_compaction(store.as_ref(), &c2).await;

    let deleted = sweep_everything(store.as_ref(), &b).await;
    assert_eq!(deleted, BTreeSet::new());
}

/// The data-object keys a snapshot resolve over the logs bucket's hour serves.
async fn served_keys(store: &Arc<MemoryStore>, now_ns: i64) -> BTreeSet<String> {
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = ravel_catalog::Catalog::new(
        dyn_store,
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    let base = hour_ns();
    catalog
        .resolve(
            &tenant_hash(),
            Signal::Logs,
            TimeRange {
                start_ns: base,
                end_ns: base + NS_PER_HOUR,
            },
            &[],
            now_ns,
        )
        .await
        .expect("resolve")
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect()
}

/// A rewrite R naming a version 2 C2 that names C1, swept past every horizon:
/// R's chain group runs through C2 to C1, so both records, both parts and the
/// raw L0 inputs below them go, and a resolve afterwards serves R's part and
/// nothing else.
///
/// Flipped line: `ChainLink::Compaction(r) => r.superseded_record_key
/// .is_empty()` in `ChainLink::names_raw_l0_inputs` (sweep.rs), restored to
/// `ChainLink::Compaction(_) => true`. The walk then ends at C2: C1 and its
/// part survive, and C1, named by no present record once C2 is gone, is served
/// again with its pre-erasure part beside R's.
#[tokio::test]
async fn sweep_follows_a_rewrite_chain_through_a_version_2_link() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let inputs = seed_inputs(store.as_ref(), &[(0x81, 1), (0x82, 2)]).await;
    let c1 = version_1(
        &b,
        vec![input(0x81, 1), input(0x82, 2)],
        0x00,
        part(0xc1, base, base + 10_000),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(
        &c1,
        &c1_key,
        c1.inputs.clone(),
        part(0xc2, base, base + 10_000),
    );
    let c2_key = put_compaction(store.as_ref(), &c2).await;
    let r = put_rewrite(
        store.as_ref(),
        &b,
        &c2_key,
        Uuid::from_u128(0xd2),
        part(0x0e, base, base + 10_000),
    )
    .await;

    let deleted = sweep_everything(store.as_ref(), &b).await;
    let mut expected: BTreeSet<String> = inputs
        .iter()
        .flat_map(|(commit, data)| [commit.clone(), data.clone()])
        .collect();
    expected.extend([
        c1_key,
        keys::reconstruct_l1_part_key(&c1, &c1.parts[0]).unwrap(),
        c2_key,
        keys::reconstruct_l1_part_key(&c2, &c2.parts[0]).unwrap(),
    ]);
    assert_eq!(deleted, expected);
    let r_part = keys::reconstruct_rewrite_part_key(&r, &r.parts[0]).unwrap();
    assert_eq!(
        all_keys(store.as_ref()).await,
        BTreeSet::from([keys::rewrite_record_key_for(&r).unwrap(), r_part.clone()])
    );
    assert_eq!(
        served_keys(&store, past_horizon_ns()).await,
        BTreeSet::from([r_part])
    );
}

fn pending_request(b: &Bucket, request_id: Uuid) -> Vec<PendingErasureRequest> {
    let base = hour_ns();
    vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&b.tenant_hash, b.signal, request_id)
            .expect("dreq key"),
        request: ErasureRequest {
            format_version: 1,
            tenant_hash: b.tenant_hash.0.to_vec(),
            signal: signal::to_proto(b.signal) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: base,
            predicate: vec![ErasurePredicateMatcher {
                key: "service.name".to_string(),
                value: "svc1".to_string(),
            }],
            window_start_ns: base,
            window_end_ns: base + 20_000,
            reason: String::new(),
        },
    }]
}

/// The erasure completion gate: a rewrite R applying the request supersedes
/// C1, and a version 2 C2 re-encodes C1 with its part inside the request
/// window. C2 is dominated, so nothing live serves the subject and the bucket
/// does not block the request.
///
/// Flipped line: `erasure_dominated_compaction_records` (catalog.rs) returning
/// an empty set. C2 is then authoritative and live, its part overlaps the
/// window, and the request stays blocked.
#[tokio::test]
async fn erasure_completion_ignores_a_dominated_version_2_record() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let request_id = Uuid::from_u128(0x0e45);
    let c1 = version_1(
        &b,
        vec![input(0xE1, 1)],
        0xff,
        part(0xc1, base + 1_000, base + 9_000),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(
        &c1,
        &c1_key,
        c1.inputs.clone(),
        part(0xc2, base + 1_000, base + 9_000),
    );
    put_compaction(store.as_ref(), &c2).await;
    put_rewrite(
        store.as_ref(),
        &b,
        &c1_key,
        request_id,
        part(0x0e, base + 1_000, base + 9_000),
    )
    .await;

    let clock = FixedClock::new(sealed_now_ns());
    let pending = pending_request(&b, request_id);
    let completion =
        bucket_erasure_completion(store.as_ref(), &clock, &cfg(), &NoLeases, &b, &pending)
            .await
            .expect("completion");
    assert!(completion.blocked.is_empty(), "{:?}", completion.blocked);
    assert!(!completion.unresolved);
}

/// `migrate` counts a bucket holding a predecessor and the version 2 record
/// that re-encodes it as one record in its overlap component, and two
/// genuinely overlapping version 1 records as two.
///
/// Flipped line: `let superseded = superseded_by_version_2_records(records)?;`
/// in `select_authoritative_compaction_records` (catalog.rs), replaced by an
/// empty set. C1 and C2 then share every input and form one component of two.
#[tokio::test]
async fn migrate_counts_a_predecessor_and_its_successor_as_one_record() {
    let b = logs_bucket();
    let base = hour_ns();
    let c1 = version_1(
        &b,
        vec![input(0xF1, 1), input(0xF2, 2)],
        0x00,
        part(0xc1, base, base + 10),
    );
    let c1_key = keys::compaction_record_key_for(&c1).unwrap();
    let c2 = version_2(&c1, &c1_key, c1.inputs.clone(), part(0xc2, base, base + 10));
    let c2_key = keys::compaction_record_key_for(&c2).unwrap();
    let records = vec![(c1_key, c1.clone()), (c2_key, c2)];
    assert_eq!(largest_overlap_component(&records).expect("count"), 1);

    let rival = version_1(&b, vec![input(0xF2, 2)], 0x33, part(0xd1, base, base + 10));
    let rival_key = keys::compaction_record_key_for(&rival).unwrap();
    let c1_key = keys::compaction_record_key_for(&c1).unwrap();
    let contested = vec![(c1_key, c1), (rival_key, rival)];
    assert_eq!(largest_overlap_component(&contested).expect("count"), 2);
}

/// `migrate`'s re-audit refuses a bucket the resolver refuses. R supersedes C1
/// over `[a, b]`; the version 2 C2 names C1 but carries `[a, b, c]`. The
/// re-audit fails with the typed error rather than counting `c` either way.
///
/// Flipped line: `check_version_2_inputs(compaction_records)?;` in
/// `erasure_dominated_compaction_records` (catalog.rs). The re-audit drops the
/// dominated C2 before the selector runs, so without it the selector never
/// sees C2 and the re-audit reports `c` as a live below-target L0.
#[tokio::test]
async fn migrate_reaudit_refuses_a_version_2_record_with_other_inputs() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    seed_inputs(store.as_ref(), &[(0x71, 1), (0x72, 2), (0x73, 3)]).await;
    let c1 = version_1(
        &b,
        vec![input(0x71, 1), input(0x72, 2)],
        0x00,
        part(0xc1, base, base + 10_000),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(
        &c1,
        &c1_key,
        vec![input(0x71, 1), input(0x72, 2), input(0x73, 3)],
        part(0xc2, base, base + 10_000),
    );
    let c2_key = put_compaction(store.as_ref(), &c2).await;
    put_rewrite(
        store.as_ref(),
        &b,
        &c1_key,
        Uuid::from_u128(0xd1),
        part(0x0e, base, base + 10_000),
    )
    .await;

    let target = u32::from(ravel_logseg::footer::VERSION) + 1;
    let err = count_below_target(store.as_ref(), &b.tenant_hash, b.signal, SHARD + 1, target)
        .await
        .expect_err("the re-audit must refuse the bucket");
    let message = err.to_string();
    assert!(
        message.contains("names a different input set")
            && message.contains(&c2_key)
            && message.contains(&c1_key),
        "{message}"
    );
}
