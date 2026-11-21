//! Rule 6 keeps an erasure request's `.dreq` while a compaction record that
//! conflicts with a live rewrite applying the request is live (#4560).
//!
//! The conflict is the third-stall ordering of #4449: a late L0 makes a
//! compaction cover more inputs than the erasure rewrite planned over, both
//! records publish, and the compaction's version 1 record shares inputs with
//! the rewrite without one rewrite covering all of them. Its parts are served
//! beside the rewrite's and can carry the erased rows, so the request's
//! query-time filter has to outlive the protection horizon for as long as the
//! record is live, and no longer.
//!
//! Every fixture writes records directly to a `MemoryStore` and drives the
//! production entries (`sweep_superseded`, `sweep_erasure_requests`) on an
//! injected clock. Each test's doc comment names the line whose change it
//! catches.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;

use common::*;
use ravel_commit::{erasure, keys, record, signal};
use ravel_maintain::{
    Bucket, CompactorConfig, FixedClock, HeldBucket, NoLeases, SupersededSweepOutcome,
    sweep_erasure_requests, sweep_superseded,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, ErasureCompletion,
    ErasurePredicateMatcher, ErasureRequest, RewriteDrop, RewriteRecord,
};
use uuid::Uuid;

fn hour_ns() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

/// The pinned-query window (ADR-1133) zeroed, so rule 2 sees its other rules
/// alone.
fn cfg() -> CompactorConfig {
    CompactorConfig {
        max_query_duration_ns: 0,
        head_cache_ttl_ns: 0,
        clock_skew_allowance_ns: 0,
        ..CompactorConfig::default()
    }
}

/// Past every record's protection horizon and every completion's.
fn past_horizon_ns() -> i64 {
    hour_ns() + cfg().protection_horizon_ns + 10 * NS_PER_HOUR
}

/// The erased request Q and an unrelated completed request U.
const Q: u128 = 0x51;
const U: u128 = 0x55;

/// Every key present, less unnamed-since markers (ADR-1133).
async fn all_keys(store: &dyn ObjectStoreBackend) -> BTreeSet<String> {
    list_all(store, "t/")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .filter(|key| !key.contains("/maint/unn/"))
        .collect()
}

fn input(writer: u128, seq: u64) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: Uuid::from_u128(writer).to_string(),
        writer_epoch: 1,
        writer_seq: seq,
    }
}

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

/// The raw L0 inputs `A`, `B` and the late `X`.
fn input_a() -> CompactionInputIdentity {
    input(0xA1, 1)
}
fn input_b() -> CompactionInputIdentity {
    input(0xA2, 2)
}
fn input_x() -> CompactionInputIdentity {
    input(0xA3, 3)
}

async fn seed_inputs(store: &dyn ObjectStoreBackend) -> BTreeSet<String> {
    let base = hour_ns();
    let mut keys_out = BTreeSet::new();
    for (writer, seq) in [(0xA1u128, 1u64), (0xA2, 2), (0xA3, 3)] {
        let commit = seed_rlog_input(
            store,
            Uuid::from_u128(writer),
            1,
            seq,
            &[log_record(1, base + 1_000 * seq as i64, "row")],
        )
        .await;
        let bytes = get_full(store, &commit).await;
        let rec = record::decode(&bytes).expect("decode commit record");
        keys_out.insert(keys::reconstruct_data_key(&rec).expect("data key"));
        keys_out.insert(commit);
    }
    keys_out
}

/// A version 1 compaction record over `inputs`, created `created_ns`.
fn version_1(
    bucket: &Bucket,
    inputs: Vec<CompactionInputIdentity>,
    hash_byte: u8,
    seed: u8,
    created_ns: i64,
) -> CompactionRecord {
    let base = hour_ns();
    CompactionRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        level: 1,
        inputs,
        input_set_hash: vec![hash_byte; 32],
        parts: vec![part(seed, base, base + 10_000)],
        created_unix_ns: created_ns,
        superseded_record_key: String::new(),
    }
}

/// PUT a compaction record and an object at its part key. Returns the record
/// key and the part key.
async fn put_compaction(
    store: &dyn ObjectStoreBackend,
    record: &CompactionRecord,
) -> (String, String) {
    let part_key = keys::reconstruct_l1_part_key(record, &record.parts[0]).expect("part key");
    store
        .put(
            &part_key,
            bytes::Bytes::from_static(b"l1-part"),
            PutOptions::default(),
        )
        .await
        .expect("put part object");
    let key = keys::compaction_record_key_for(record).expect("record key");
    store
        .put(
            &key,
            record::encode_compaction(record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
    (key, part_key)
}

/// PUT a rewrite record over the raw L0 `inputs` applying `request_id`, and an
/// object at its part key. Returns the record key and the part key.
async fn put_raw_rewrite(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    inputs: Vec<CompactionInputIdentity>,
    request_id: Uuid,
    seed: u8,
    created_ns: i64,
) -> (String, String) {
    let base = hour_ns();
    let request_id = request_id.to_string();
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &inputs,
            None,
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        inputs,
        parts: vec![part(seed, base, base + 10_000)],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: created_ns,
        superseded_record_key: String::new(),
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
    (key, part_key)
}

/// PUT a `.dreq` for `id` and a `.done` completed at the bucket's hour, so the
/// request is past its horizon at [`past_horizon_ns`]. Returns the `.dreq` key.
async fn put_completed_request(store: &dyn ObjectStoreBackend, b: &Bucket, id: Uuid) -> String {
    let base = hour_ns();
    let request_key = keys::erasure_request_key(&b.tenant_hash, b.signal, id).expect("dreq key");
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: b.tenant_hash.0.to_vec(),
        signal: signal::to_proto(b.signal) as i32,
        request_id: id.to_string(),
        created_unix_ns: base,
        predicate: vec![ErasurePredicateMatcher {
            key: "service.name".to_string(),
            value: "svc1".to_string(),
        }],
        window_start_ns: base,
        window_end_ns: base + 20_000,
        reason: String::new(),
    };
    store
        .put(
            &request_key,
            erasure::encode_request(&request),
            PutOptions::default(),
        )
        .await
        .expect("put .dreq");
    let completion = ErasureCompletion {
        format_version: 1,
        tenant_hash: b.tenant_hash.0.to_vec(),
        signal: signal::to_proto(b.signal) as i32,
        request_id: id.to_string(),
        predicate_hash: vec![0x11; 32],
        bucket_drops: Vec::new(),
        requested_unix_ns: 0,
        completed_unix_ns: base,
        deferral_cause: 0,
    };
    store
        .put(
            &keys::erasure_completion_key(&b.tenant_hash, b.signal, id).expect("done key"),
            erasure::encode_completion(&completion),
            PutOptions::default(),
        )
        .await
        .expect("put .done");
    request_key
}

/// Rule 2's deleting pass past every horizon, with no HEAD.
async fn rule_2(store: &dyn ObjectStoreBackend, b: &Bucket) -> SupersededSweepOutcome {
    sweep_superseded(
        store,
        &FixedClock::new(past_horizon_ns()),
        &cfg(),
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 2")
}

/// Rule 6 past every horizon: `(deleted, kept, held_by_superseded_inputs)`.
async fn rule_6(store: &dyn ObjectStoreBackend, b: &Bucket) -> (usize, usize, usize) {
    let outcome = sweep_erasure_requests(
        store,
        &FixedClock::new(past_horizon_ns()),
        &cfg(),
        &NoLeases,
        &b.tenant_hash,
        b.signal,
    )
    .await
    .expect("rule 6");
    (
        outcome.deleted,
        outcome.kept,
        outcome.held_by_superseded_inputs,
    )
}

struct Conflict {
    b: Bucket,
    inputs: BTreeSet<String>,
    rewrite: (String, String),
    c: CompactionRecord,
    c_keys: (String, String),
    q_dreq: String,
    u_dreq: String,
}

/// The third-stall ordering: L0s A, B and the late X; a live rewrite R over
/// {A, B} applying Q; a version 1 C over {A, B, X} published after R. Q and an
/// unrelated U are complete and past their horizon.
async fn seed_conflict(store: &dyn ObjectStoreBackend) -> Conflict {
    let b = logs_bucket();
    let base = hour_ns();
    let inputs = seed_inputs(store).await;
    let rewrite = put_raw_rewrite(
        store,
        &b,
        vec![input_a(), input_b()],
        Uuid::from_u128(Q),
        0x4e,
        base + 5_000,
    )
    .await;
    let c = version_1(
        &b,
        vec![input_a(), input_b(), input_x()],
        0x44,
        0xc4,
        base + 6_000,
    );
    let c_keys = put_compaction(store, &c).await;
    let q_dreq = put_completed_request(store, &b, Uuid::from_u128(Q)).await;
    let u_dreq = put_completed_request(store, &b, Uuid::from_u128(U)).await;
    Conflict {
        b,
        inputs,
        rewrite,
        c,
        c_keys,
        q_dreq,
        u_dreq,
    }
}

/// The server tick's order, rule 2 then rule 6, past every horizon: rule 2
/// reclaims the three raw L0 inputs (R and C supersede them) and leaves R, C
/// and their parts; its holds name no request, yet it reports Q as applied by
/// a rewrite C conflicts with. Rule 6 keeps Q's `.dreq`, counted as held, and
/// retires the unrelated U's.
///
/// Flipped line: `if holds.conflict_request_ids.contains(&request_id_s)` in
/// `sweep_erasure_requests` (sweep.rs) made `if false && ..`. Rule 6 then
/// returns `(2, 0, 0)`, deleting Q's `.dreq` while C's parts are served.
#[tokio::test]
async fn rule_6_keeps_a_dreq_while_a_conflicting_compaction_record_is_live() {
    let store = MemoryStore::new();
    let s = seed_conflict(&store).await;
    let before = all_keys(&store).await;

    let outcome = rule_2(&store, &s.b).await;
    assert_eq!(
        outcome.conflict_request_ids,
        BTreeSet::from([Uuid::from_u128(Q).to_string()])
    );
    assert!(outcome.held_request_ids.is_empty(), "{outcome:?}");
    assert!(outcome.held_truncated_buckets.is_empty(), "{outcome:?}");
    let after_rule_2 = all_keys(&store).await;
    let deleted: BTreeSet<String> = before.difference(&after_rule_2).cloned().collect();
    assert_eq!(deleted, s.inputs);
    for key in [&s.c_keys.0, &s.c_keys.1, &s.rewrite.0, &s.rewrite.1] {
        assert!(after_rule_2.contains(key), "{key} was deleted");
    }

    assert_eq!(rule_6(&store, &s.b).await, (1, 1, 1));
    let after = all_keys(&store).await;
    assert!(after.contains(&s.q_dreq), "Q's .dreq was deleted");
    assert!(!after.contains(&s.u_dreq), "U's .dreq was kept");
    let rule_6_deleted: BTreeSet<String> = after_rule_2.difference(&after).cloned().collect();
    assert_eq!(rule_6_deleted, BTreeSet::from([s.u_dreq.clone()]));
}

/// Release by deletion: once C's record and part are gone (an operator, or a
/// later sweep), the next rule 6 pass retires Q's `.dreq`. Nothing persisted
/// the earlier keep. A version 1 D over an input R does not name stays in the
/// bucket throughout, so the bucket still has a compaction record to resolve
/// after C is gone.
///
/// Fails against a keep that outlives the conflict. Flipped line: in
/// `Version2Groups::from_records` (sweep.rs), `chain_request_ids` called for
/// every rewrite in the bucket instead of each `dominance.conflicts` pair, so
/// the keep follows the live rewrite rather than the conflict. The
/// `conflict_request_ids` assertion after the delete then fails with Q in it.
#[tokio::test]
async fn a_kept_dreq_is_released_once_the_conflicting_record_is_deleted() {
    let store = MemoryStore::new();
    let s = seed_conflict(&store).await;
    let d = version_1(&s.b, vec![input(0xA4, 4)], 0x22, 0xd4, hour_ns() + 6_000);
    let d_keys = put_compaction(&store, &d).await;
    rule_2(&store, &s.b).await;
    assert_eq!(rule_6(&store, &s.b).await, (1, 1, 1));

    store.delete(&s.c_keys.1).await.expect("delete C's part");
    store.delete(&s.c_keys.0).await.expect("delete C's record");
    let outcome = rule_2(&store, &s.b).await;
    assert!(outcome.conflict_request_ids.is_empty(), "{outcome:?}");

    assert_eq!(rule_6(&store, &s.b).await, (1, 0, 0));
    let after = all_keys(&store).await;
    assert!(!after.contains(&s.q_dreq));
    assert!(after.contains(&d_keys.0) && after.contains(&d_keys.1));
}

/// Release by dominance: a later live rewrite R2 over {A, B, X} covers every
/// input of C, so C is dominated rather than conflicting, and rule 6's own
/// observation, with no deleting pass in between, retires Q's `.dreq` while
/// C's record is still present.
///
/// Flipped line: as for
/// [`a_kept_dreq_is_released_once_the_conflicting_record_is_deleted`]; R is
/// still live, so the keep that follows it returns `(0, 1, 1)`.
#[tokio::test]
async fn a_kept_dreq_is_released_once_a_later_rewrite_dominates_the_record() {
    let store = MemoryStore::new();
    let s = seed_conflict(&store).await;
    rule_2(&store, &s.b).await;
    assert_eq!(rule_6(&store, &s.b).await, (1, 1, 1));

    put_raw_rewrite(
        &store,
        &s.b,
        vec![input_a(), input_b(), input_x()],
        Uuid::from_u128(0x52),
        0x4f,
        hour_ns() + 7_000,
    )
    .await;
    assert_eq!(rule_6(&store, &s.b).await, (1, 0, 0));
    let after = all_keys(&store).await;
    assert!(!after.contains(&s.q_dreq));
    assert!(after.contains(&s.c_keys.0), "C was deleted, not dominated");
}

/// A version 2 record M re-encoding C (a `migrate` record) carries C's inputs
/// and is not dominated, so it conflicts with R too. Rule 2 reclaims C under
/// M's chain group, and with C gone only M's conflict is left: rule 6 still
/// keeps Q's `.dreq`.
///
/// Flipped line: the version 2 conflict loop at the end of
/// `ravel_catalog::erasure_dominance` (catalog.rs) removed. With C gone the
/// bucket reports no conflict, and rule 6 returns `(2, 0, 0)`.
#[tokio::test]
async fn rule_6_keeps_a_dreq_while_a_conflicting_version_2_record_is_live() {
    let store = MemoryStore::new();
    let s = seed_conflict(&store).await;
    let m = CompactionRecord {
        format_version: 2,
        input_set_hash: erasure::compute_superseding_compaction_input_set_hash(
            &s.c.inputs,
            &s.c_keys.0,
        )
        .to_vec(),
        parts: vec![part(0xc5, hour_ns(), hour_ns() + 10_000)],
        created_unix_ns: s.c.created_unix_ns + 1_000,
        superseded_record_key: s.c_keys.0.clone(),
        ..s.c.clone()
    };
    let m_keys = put_compaction(&store, &m).await;

    rule_2(&store, &s.b).await;
    let after_rule_2 = all_keys(&store).await;
    assert!(!after_rule_2.contains(&s.c_keys.0), "C was not reclaimed");
    assert!(
        !after_rule_2.contains(&s.c_keys.1),
        "C's part was not reclaimed"
    );
    assert!(after_rule_2.contains(&m_keys.0) && after_rule_2.contains(&m_keys.1));

    assert_eq!(rule_6(&store, &s.b).await, (1, 1, 1));
    let after = all_keys(&store).await;
    assert!(after.contains(&s.q_dreq), "Q's .dreq was deleted");
    assert!(!after.contains(&s.u_dreq), "U's .dreq was kept");
}

/// An erasure request Z applied by a rewrite over R.
const Z: u128 = 0x5a;

/// The reclaimed-predecessor ordering: R (over {A, B}, applying Q) is
/// superseded by a live R2 that applies only Z, and C over {A, B, X}
/// conflicts with R2 through R's inputs. Rule 2 reclaims R's record and part
/// under R2's group once R's horizon passes; the fixture deletes them as that
/// pass would. R2 now resolves to no inputs at all, so C is reported as
/// neither dominated nor conflicting, yet its parts still carry Q's and Z's
/// erased rows. Rule 6 keeps every candidate while the bucket stands, and
/// releases all three once C is gone.
///
/// Fails without the truncated-chain check in `Version2Groups::from_records`
/// (sweep.rs): the `conflict_truncated_buckets` assertion finds an empty set,
/// and rule 6 then returns `(3, 0, 0)`, deleting Q's `.dreq` while C's parts
/// are served.
#[tokio::test]
async fn rule_6_keeps_every_dreq_while_a_conflicting_rewrite_chain_is_truncated() {
    let store = MemoryStore::new();
    let s = seed_conflict(&store).await;
    let r2 = put_superseding_rewrite(
        &store,
        &s.b,
        &s.rewrite.0,
        Uuid::from_u128(Z),
        0x4d,
        hour_ns() + 5_500,
    )
    .await;
    let z_dreq = put_completed_request(&store, &s.b, Uuid::from_u128(Z)).await;
    store.delete(&s.rewrite.1).await.expect("delete R's part");
    store.delete(&s.rewrite.0).await.expect("delete R's record");

    let outcome = rule_2(&store, &s.b).await;
    assert_eq!(
        outcome.conflict_truncated_buckets,
        BTreeSet::from([HeldBucket {
            shard: s.b.shard,
            ingest_hour_bucket: s.b.ingest_hour_bucket,
        }]),
        "{outcome:?}"
    );
    assert!(outcome.conflict_request_ids.is_empty(), "{outcome:?}");
    assert!(outcome.held_request_ids.is_empty(), "{outcome:?}");
    assert!(outcome.held_truncated_buckets.is_empty(), "{outcome:?}");
    let after_rule_2 = all_keys(&store).await;
    for key in [&s.c_keys.0, &s.c_keys.1, &r2.0, &r2.1] {
        assert!(after_rule_2.contains(key), "{key} was deleted");
    }

    assert_eq!(rule_6(&store, &s.b).await, (0, 3, 3));
    let after = all_keys(&store).await;
    for key in [&s.q_dreq, &z_dreq, &s.u_dreq] {
        assert!(after.contains(key), "{key} was deleted");
    }

    store.delete(&s.c_keys.1).await.expect("delete C's part");
    store.delete(&s.c_keys.0).await.expect("delete C's record");
    let outcome = rule_2(&store, &s.b).await;
    assert!(outcome.conflict_truncated_buckets.is_empty(), "{outcome:?}");
    assert_eq!(rule_6(&store, &s.b).await, (3, 0, 0));
}

/// The third-stall conflict in a bucket whose version 2 supersession does
/// not resolve: a chain of 65 version 2 records over an unrelated input E,
/// one past the catalog's supersession depth bound, so
/// `select_authoritative_compaction_records` fails for the bucket after
/// `erasure_dominance` has already reported C's conflict with R. (A cycle
/// cannot be written: a version 2 record's key hashes the key it names.)
/// Rule 6 keeps Q's `.dreq` and retires U's.
///
/// Fails with the conflict loop placed after the selection's error arm in
/// `Version2Groups::from_records` (sweep.rs): the arm continues past it, the
/// `conflict_request_ids` assertion finds an empty set, and rule 6 returns
/// `(2, 0, 0)`.
#[tokio::test]
async fn rule_6_keeps_a_conflict_dreq_when_the_bucket_selection_fails() {
    let store = MemoryStore::new();
    let s = seed_conflict(&store).await;
    let base = version_1(&s.b, vec![input(0xA5, 5)], 0x71, 0xe1, hour_ns() + 7_000);
    let (mut named, _) = put_compaction(&store, &base).await;
    for i in 0..65u8 {
        let link = CompactionRecord {
            format_version: 2,
            input_set_hash: erasure::compute_superseding_compaction_input_set_hash(
                &base.inputs,
                &named,
            )
            .to_vec(),
            parts: vec![part(i, hour_ns(), hour_ns() + 10_000)],
            created_unix_ns: base.created_unix_ns + 1 + i64::from(i),
            superseded_record_key: named.clone(),
            ..base.clone()
        };
        named = put_compaction(&store, &link).await.0;
    }

    let outcome = rule_2(&store, &s.b).await;
    assert_eq!(
        outcome.conflict_request_ids,
        BTreeSet::from([Uuid::from_u128(Q).to_string()]),
        "{outcome:?}"
    );
    assert!(outcome.conflict_truncated_buckets.is_empty(), "{outcome:?}");
    let after_rule_2 = all_keys(&store).await;
    for key in [&s.c_keys.0, &s.c_keys.1, &s.rewrite.0, &s.rewrite.1] {
        assert!(after_rule_2.contains(key), "{key} was deleted");
    }

    assert_eq!(rule_6(&store, &s.b).await, (1, 1, 1));
    let after = all_keys(&store).await;
    assert!(after.contains(&s.q_dreq), "Q's .dreq was deleted");
    assert!(!after.contains(&s.u_dreq), "U's .dreq was kept");
}

/// The third-stall conflict in a bucket where `erasure_dominance` itself
/// fails: a version 2 record names a version 1 record over E but carries a
/// different input set, which the catalog refuses
/// (`CompactionSupersessionInputMismatch`). The bucket's conflicts are then
/// unknown, so rule 2 reports it as truncated and rule 6 keeps every `.dreq`.
///
/// Fails with the `conflict_truncated` insert in the dominance error arm of
/// the superseded-input sweep (sweep.rs) removed: the truncated-bucket
/// assertion finds an empty set, and rule 6 returns `(2, 0, 0)`.
#[tokio::test]
async fn rule_6_keeps_every_dreq_when_the_bucket_dominance_fails() {
    let store = MemoryStore::new();
    let s = seed_conflict(&store).await;
    let base = version_1(&s.b, vec![input(0xA5, 5)], 0x71, 0xe1, hour_ns() + 7_000);
    let (named, _) = put_compaction(&store, &base).await;
    let other_inputs = vec![input(0xA6, 6)];
    let link = CompactionRecord {
        format_version: 2,
        input_set_hash: erasure::compute_superseding_compaction_input_set_hash(
            &other_inputs,
            &named,
        )
        .to_vec(),
        inputs: other_inputs,
        parts: vec![part(0x72, hour_ns(), hour_ns() + 10_000)],
        created_unix_ns: base.created_unix_ns + 1,
        superseded_record_key: named.clone(),
        ..base.clone()
    };
    put_compaction(&store, &link).await;

    let outcome = rule_2(&store, &s.b).await;
    assert_eq!(
        outcome.conflict_truncated_buckets,
        BTreeSet::from([HeldBucket {
            shard: s.b.shard,
            ingest_hour_bucket: s.b.ingest_hour_bucket,
        }]),
        "{outcome:?}"
    );
    let after_rule_2 = all_keys(&store).await;
    for key in [&s.c_keys.0, &s.c_keys.1, &s.rewrite.0, &s.rewrite.1] {
        assert!(after_rule_2.contains(key), "{key} was deleted");
    }

    assert_eq!(rule_6(&store, &s.b).await, (0, 2, 2));
    let after = all_keys(&store).await;
    for key in [&s.q_dreq, &s.u_dreq] {
        assert!(after.contains(key), "{key} was deleted");
    }
}

/// PUT a rewrite record superseding the record at `superseded` and applying
/// `request_id`, and an object at its part key. Returns the record key and
/// the part key.
async fn put_superseding_rewrite(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    superseded: &str,
    request_id: Uuid,
    seed: u8,
    created_ns: i64,
) -> (String, String) {
    let base = hour_ns();
    let request_id = request_id.to_string();
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &[],
            Some(superseded),
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        inputs: Vec::new(),
        parts: vec![part(seed, base, base + 10_000)],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: created_ns,
        superseded_record_key: superseded.to_string(),
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
    (key, part_key)
}
