//! Maintenance over a bucket holding a version 2 compaction record (ADR-0066
//! force 2 amendment, items 2 to 5).
//!
//! A version 2 record names, in `superseded_record_key`, the compaction record
//! it re-encodes. The shared selector excludes that predecessor, and a version
//! 2 record whose predecessor a live rewrite record supersedes is dropped
//! before the selector runs. The erasure completion gate and `migrate` follow
//! both rules, as the resolver does. A version 2 record whose inputs differ
//! from its present predecessor's is a typed error for all of them. The sweep
//! does not reclaim a superseded predecessor yet: that needs horizon,
//! reachability and hold rules of its own, so until then it deletes nothing it
//! did not delete before.
//!
//! Every fixture writes records directly to a `MemoryStore` and drives the
//! production entries (`sweep_superseded`, `sweep_unreferenced_parts`,
//! `bucket_erasure_completion`, `count_below_target`,
//! `migrate::largest_overlap_component`). Each test's doc comment names the
//! line whose change it catches.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use ravel_commit::{erasure, keys, record, signal};
use ravel_maintain::migrate::largest_overlap_component;
use ravel_maintain::{
    Bucket, CompactorConfig, FixedClock, NoLeases, PendingErasureRequest,
    bucket_erasure_completion, count_below_target, sweep_superseded, sweep_unreferenced_parts,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
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

/// A bucket holding C1 and a version 2 C2 naming C1, swept past every horizon,
/// loses no record and no part: neither the superseded predecessor nor either
/// record's parts are reclaimed yet. The inputs were swept long ago, as they
/// are by the time a version 2 record is written, so nothing else is in play.
///
/// This pins the sweep's behaviour as it was before resolution honoured the
/// record, and passes on that code too; the guard it relies on is pinned by
/// `sweep_supersedes_only_inputs_both_views_name` below.
#[tokio::test]
async fn sweep_reclaims_nothing_of_a_superseded_predecessor() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let c1 = version_1(
        &b,
        vec![input(0xA1, 1), input(0xA2, 2)],
        0x00,
        part(0xc1, base, base + 10),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(&c1, &c1_key, c1.inputs.clone(), part(0xc2, base, base + 10));
    put_compaction(store.as_ref(), &c2).await;

    let deleted = sweep_everything(store.as_ref(), &b).await;
    assert_eq!(deleted, BTreeSet::new());
    let after = all_keys(store.as_ref()).await;
    assert_eq!(after.len(), 4, "two records and two parts survive");
}

/// A live rewrite R and a version 2 C2 both naming C1, swept past every
/// horizon. The sweep deletes exactly what it deleted before resolution
/// honoured version 2 records: R's chain group, which is C1 and C1's part.
/// The erasure-dominated C2 and its part survive, and so do R and its part.
///
/// This passes on the code before resolution honoured the record as well:
/// the sweep gives C2 no rule of its own yet.
#[tokio::test]
async fn sweep_keeps_an_erasure_dominated_version_2_record() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let c1 = version_1(
        &b,
        vec![input(0xB1, 1), input(0xB2, 2)],
        0xff,
        part(0xc1, base, base + 10),
    );
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(&c1, &c1_key, c1.inputs.clone(), part(0xc2, base, base + 10));
    let c2_key = put_compaction(store.as_ref(), &c2).await;
    let r = put_rewrite(
        store.as_ref(),
        &b,
        &c1_key,
        Uuid::from_u128(0xd0),
        part(0x0e, base, base + 10),
    )
    .await;

    let deleted = sweep_everything(store.as_ref(), &b).await;
    let c1_part = keys::reconstruct_l1_part_key(&c1, &c1.parts[0]).unwrap();
    assert_eq!(deleted, BTreeSet::from([c1_key, c1_part]));
    let after = all_keys(store.as_ref()).await;
    assert_eq!(
        after,
        BTreeSet::from([
            c2_key,
            keys::reconstruct_l1_part_key(&c2, &c2.parts[0]).unwrap(),
            keys::rewrite_record_key_for(&r).unwrap(),
            keys::reconstruct_rewrite_part_key(&r, &r.parts[0]).unwrap(),
        ])
    );
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
/// and they are `b` and `c`. Only `b` is in both, so only `b` goes.
///
/// Flipped lines, in `AuthoritativeInputs::from_records` (sweep.rs): keeping
/// only the ignored view (the sweep before resolution honoured version 2
/// records) deletes `a`, which the resolver now serves as a raw L0 because no
/// authoritative record names it; keeping only the honoured view deletes `c`,
/// which a node that predates the rule still serves as a raw L0.
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
    assert_eq!(deleted, BTreeSet::from([b_commit, b_data]));
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
