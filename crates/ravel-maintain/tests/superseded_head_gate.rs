//! The superseded-input sweep's HEAD-reachability gate (ADR-0020 delete
//! blocker, applied to rule 2).
//!
//! A selective-erasure rewrite record can land in any sealed hour, including
//! one the fold's fixed reconcile window and its retention-frontier band both
//! miss. The snapshot part covering that hour then keeps naming the
//! pre-rewrite inputs. Deleting those inputs on the protection horizon alone
//! makes every query over that hour fail closed until the fold catches up, so
//! the sweep asks the same question retention asks before deleting: does the
//! live catalog HEAD still name this object?
//!
//! Every fixture here uses a rewrite in an hour 100 hours behind the fold
//! watermark, well outside the default 26-hour reconcile window and with no
//! retirement frontier configured, so the second fold provably does not
//! refresh it. Each test names the line whose flip breaks it, per the repo's
//! prove-the-test discipline.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use ravel_commit::keys::{self, BucketEntry};
use ravel_commit::{erasure, signal};
use ravel_maintain::{
    Bucket, CompactorConfig, ErasureRequestSweepOutcome, ErasureRewriteOutcome, FixedClock,
    MaintainMemo, NoLeases, PendingErasureRequest, SupersededSweepOutcome, erasure_rewrite_bucket,
    sweep_erasure_requests, sweep_superseded,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    ErasureBucketDrop, ErasureCompletion, ErasurePredicateMatcher, ErasureRequest,
};
use ravel_types::Signal;
use uuid::Uuid;

/// The rewritten hour: 100 hours behind the hour the fold watermark lands on,
/// so it is outside the default `fold_reconcile_window_hours` (26).
const OLD_HOUR: u32 = HOUR - 100;
/// A recent hour that keeps the fold watermark far ahead of [`OLD_HOUR`].
const RECENT_HOUR: u32 = HOUR;

const REQUEST_SEED: u128 = 0x0E45;

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

fn old_bucket() -> Bucket {
    bucket_at(OLD_HOUR)
}

fn request_id() -> Uuid {
    Uuid::from_u128(REQUEST_SEED)
}

/// The `n`th distinct erasure request id; `request_id_n(0)` is
/// [`request_id`]. Each one becomes its own rewrite generation.
fn request_id_n(n: u128) -> Uuid {
    Uuid::from_u128(REQUEST_SEED + n)
}

fn head_key() -> String {
    format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash().to_hex(),
        Signal::Metrics.key_prefix()
    )
}

/// Two L0 inputs in [`OLD_HOUR`] (both carrying the erasure subject) and one in
/// [`RECENT_HOUR`] that holds the fold watermark far ahead of them. Returns the
/// old hour's two commit keys, in seed order.
async fn seed_two_hours(store: &dyn ObjectStoreBackend) -> [String; 2] {
    let old_ns = i64::from(OLD_HOUR) * NS_PER_HOUR;
    let first = seed_input(
        store,
        &InputSpec::new_at(
            OLD_HOUR,
            Uuid::from_u128(0xA1),
            1,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(old_ns + 1_000, 1.0)]),
                raw_series("victim", &[("k", "b")], &[(old_ns + 2_000, 5.0)]),
            ],
        ),
    )
    .await;
    let second = seed_input(
        store,
        &InputSpec::new_at(
            OLD_HOUR,
            Uuid::from_u128(0xA2),
            1,
            2,
            vec![raw_series(
                "victim",
                &[("k", "b")],
                &[(old_ns + 3_000, 3.0)],
            )],
        ),
    )
    .await;
    let recent_ns = i64::from(RECENT_HOUR) * NS_PER_HOUR;
    seed_input(
        store,
        &InputSpec::new_at(
            RECENT_HOUR,
            Uuid::from_u128(0xB1),
            1,
            1,
            vec![raw_series(
                "keep",
                &[("k", "c")],
                &[(recent_ns + 1_000, 2.0)],
            )],
        ),
    )
    .await;
    [first, second]
}

/// A windowless erasure request matching every series named `victim`.
fn pending_request() -> PendingErasureRequest {
    pending_request_for(request_id(), "victim")
}

/// A windowless erasure request under `id` matching every series named
/// `series`.
fn pending_request_for(id: Uuid, series: &str) -> PendingErasureRequest {
    PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, id)
            .expect("dreq key"),
        request: erasure_request_for(id, series),
    }
}

fn erasure_request() -> ErasureRequest {
    erasure_request_for(request_id(), "victim")
}

fn erasure_request_for(id: Uuid, series: &str) -> ErasureRequest {
    ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: id.to_string(),
        created_unix_ns: 0,
        predicate: vec![ErasurePredicateMatcher {
            key: "__name__".to_string(),
            value: series.to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    }
}

/// Publish a rewrite record into [`OLD_HOUR`], superseding both of its raw-L0
/// inputs.
async fn run_rewrite(store: &dyn ObjectStoreBackend, clock: &FixedClock) {
    run_rewrite_with(store, clock, &[pending_request()]).await
}

/// Publish one rewrite generation into [`OLD_HOUR`] applying `pending`. The
/// first call supersedes the bucket's raw L0 inputs; each later call with a
/// request id no live generation has applied yet supersedes the generation
/// before it, which is how these fixtures grow a supersession chain.
async fn run_rewrite_with(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    pending: &[PendingErasureRequest],
) {
    let mut memo = MaintainMemo::with_default_interval();
    let outcome = erasure_rewrite_bucket(
        store,
        clock,
        &cfg(),
        &NoLeases,
        &old_bucket(),
        pending,
        &mut memo,
    )
    .await
    .expect("rewrite");
    assert!(
        matches!(outcome, ErasureRewriteOutcome::Rewritten { .. }),
        "the rewrite must publish a record, got {outcome:?}"
    );
}

/// Fold a HEAD for the metrics signal at `now_ns`. `reconcile_window_hours`
/// widens the fold's fixed reconcile window; the default (26) leaves
/// [`OLD_HOUR`] unreconciled forever, which is the defect this suite pins.
async fn fold_head(
    store: &Arc<MemoryStore>,
    now_ns: i64,
    folder: u128,
    reconcile_window_hours: Option<u32>,
) {
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let mut config = ravel_catalog::CatalogConfig {
        shard_count: SHARD + 1,
        ..Default::default()
    };
    if let Some(hours) = reconcile_window_hours {
        config.fold_reconcile_window_hours = hours;
    }
    let catalog = ravel_catalog::Catalog::new(dyn_store, config).expect("catalog");
    catalog
        .fold(
            &tenant_hash(),
            Signal::Metrics,
            Uuid::from_u128(folder),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold publishes a HEAD");
}

/// Every data-object key the current HEAD names for `hour`, reconstructed from
/// the snapshot entries independently of the sweep's own mapping: a level-0
/// entry carries a 16-byte `writer_id`, a level-1 entry carries its parent
/// record's 32-byte `input_set_hash` there and the `part_index` in
/// `writer_epoch`.
async fn head_named_data_keys(store: &dyn ObjectStoreBackend, hour: u32) -> BTreeSet<String> {
    let got = get_full(store, &head_key()).await;
    let head = ravel_catalog::decode_head(got.as_ref()).expect("HEAD decodes");
    let limits = ravel_catalog::PartLimits {
        max_snapshot_part_bytes: ravel_catalog::DEFAULT_MAX_SNAPSHOT_PART_BYTES,
    };
    let mut out = BTreeSet::new();
    for part_ref in &head.parts {
        let bytes = get_full(store, &part_ref.key).await;
        let part = ravel_catalog::decode_part(bytes.as_ref(), &limits).expect("part decodes");
        for entry in &part.entries {
            if entry.ingest_hour_bucket != hour {
                continue;
            }
            let content_hash: [u8; 32] = entry.content_hash.as_slice().try_into().unwrap();
            let key = if entry.level == 0 {
                let writer_id: [u8; 16] = entry.writer_id.as_slice().try_into().unwrap();
                keys::data_key(
                    &tenant_hash(),
                    Signal::Metrics,
                    entry.shard,
                    Uuid::from_bytes(writer_id),
                    entry.writer_epoch,
                    entry.writer_seq,
                    &content_hash,
                )
                .unwrap()
            } else {
                let input_set_hash: [u8; 32] = entry.writer_id.as_slice().try_into().unwrap();
                keys::l1_part_key(
                    &tenant_hash(),
                    Signal::Metrics,
                    entry.shard,
                    entry.ingest_hour_bucket,
                    &hex::encode(&input_set_hash[..8]),
                    u32::try_from(entry.writer_epoch).unwrap(),
                    &hex::encode(&content_hash[..8]),
                )
                .unwrap()
            };
            out.insert(key);
        }
    }
    out
}

/// The L0 data-object keys physically present for `bucket`'s shard, restricted
/// to the ones `expected` names (L0 keys carry no hour component, so the other
/// hour's object is filtered out by membership, never by prefix).
async fn present_keys(
    store: &dyn ObjectStoreBackend,
    candidates: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for key in candidates {
        if store.head(key).await.is_ok() {
            out.insert(key.clone());
        }
    }
    out
}

/// The old hour's superseded input data keys, read off the commit records the
/// fixture seeded (so the expected set never restates the sweep's arithmetic).
async fn seeded_input_data_keys(
    store: &dyn ObjectStoreBackend,
    commit_keys: &[String; 2],
) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for key in commit_keys {
        let bytes = get_full(store, key).await;
        let record = ravel_commit::record::decode(&bytes).expect("commit record decodes");
        out.insert(keys::reconstruct_data_key(&record).expect("data key"));
    }
    out
}

async fn sweep(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
) -> ravel_maintain::Result<SupersededSweepOutcome> {
    let b = old_bucket();
    sweep_superseded(
        store,
        clock,
        &cfg(),
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
}

/// The rewrite record's own key in the old bucket.
async fn rewrite_record_key(store: &dyn ObjectStoreBackend) -> String {
    let b = old_bucket();
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .unwrap();
    for meta in list_all(store, &prefix).await.unwrap() {
        if matches!(
            keys::partition_bucket_entry(&meta.key),
            Ok(BucketEntry::RewriteRecord(_))
        ) {
            return meta.key;
        }
    }
    panic!("no rewrite record in the old bucket");
}

/// The old bucket's rewrite record keys ordered oldest generation first: the
/// one that superseded the raw L0 inputs, then each record that superseded the
/// one before it. Reconstructed from the `superseded_record_key` pointers, so
/// it never restates the sweep's own walk.
async fn rewrite_chain(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let b = old_bucket();
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .unwrap();
    let mut records = Vec::new();
    for meta in list_all(store, &prefix).await.unwrap() {
        if matches!(
            keys::partition_bucket_entry(&meta.key),
            Ok(BucketEntry::RewriteRecord(_))
        ) {
            let bytes = get_full(store, &meta.key).await;
            let record = erasure::decode_rewrite(&bytes).expect("rewrite record decodes");
            records.push((meta.key, record));
        }
    }
    let mut current = records
        .iter()
        .find(|(_, r)| !r.inputs.is_empty())
        .expect("one generation supersedes the raw L0 inputs")
        .0
        .clone();
    let mut chain = Vec::new();
    loop {
        chain.push(current.clone());
        match records.iter().find(|(_, r)| r.superseded_record_key == current) {
            Some((key, _)) => current = key.clone(),
            None => return chain,
        }
    }
}

/// The L1 part keys the rewrite record at `key` names.
async fn rewrite_part_keys(store: &dyn ObjectStoreBackend, key: &str) -> BTreeSet<String> {
    let bytes = get_full(store, key).await;
    let record = erasure::decode_rewrite(&bytes).expect("rewrite record decodes");
    record
        .parts
        .iter()
        .map(|part| keys::reconstruct_rewrite_part_key(&record, part).expect("part key"))
        .collect()
}

/// Seed a `.dreq` for `id` over `series` and its `.done` completion, anchored
/// at `completed` and naming [`OLD_HOUR`] as the one bucket it touched.
async fn seed_dreq_and_done_for(
    store: &dyn ObjectStoreBackend,
    id: Uuid,
    series: &str,
    completed: i64,
) -> (String, String) {
    let dreq_key = keys::erasure_request_key(&tenant_hash(), Signal::Metrics, id).unwrap();
    store
        .put(
            &dreq_key,
            erasure::encode_request(&erasure_request_for(id, series)),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let completion = ErasureCompletion {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: id.to_string(),
        predicate_hash: vec![0x11; 32],
        bucket_drops: vec![ErasureBucketDrop {
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard: SHARD,
            ingest_hour_bucket: OLD_HOUR,
            dropped_count: 2,
        }],
        requested_unix_ns: 0,
        completed_unix_ns: completed,
        deferral_cause: 0,
    };
    let done_key = keys::erasure_completion_key(&tenant_hash(), Signal::Metrics, id).unwrap();
    store
        .put(
            &done_key,
            erasure::encode_completion(&completion),
            PutOptions::default(),
        )
        .await
        .unwrap();
    (dreq_key, done_key)
}

async fn sweep_dreq(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
) -> ravel_maintain::Result<ErasureRequestSweepOutcome> {
    sweep_erasure_requests(
        store,
        clock,
        &cfg(),
        &NoLeases,
        &tenant_hash(),
        Signal::Metrics,
    )
    .await
}

fn past_horizon(created: i64) -> i64 {
    created + cfg().protection_horizon_ns + 1
}

// --- (a) the defect: a HEAD-named input is never deleted --------------------

/// A rewrite in an hour the fold's reconcile window never reaches leaves the
/// published snapshot naming the pre-rewrite inputs. Past the protection
/// horizon the sweep must delete none of them: the exact delete count is zero,
/// and the exact set of surviving input keys is the set the HEAD-named part
/// still references.
///
/// Also pins the gate's request shape: two catalog GETs for the whole pass
/// (one HEAD, one covering part), regardless of how many input groups it gates.
///
/// Flip-line proof: replace the `reach.object_gate(...)` match in
/// `sweep_superseded_impl` with `SnapshotGate::Clear` (or delete the match):
/// the pass then deletes the four inputs, the `Op::Delete` fault rule below
/// fires, and the `.expect` on the sweep panics.
#[tokio::test]
async fn head_named_superseded_inputs_are_held_not_deleted() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let commit_keys = seed_two_hours(mem.as_ref()).await;
    let input_data_keys = seeded_input_data_keys(mem.as_ref(), &commit_keys).await;

    // 1. Fold: the snapshot names the old hour's two L0 inputs.
    fold_head(&mem, created, 1, None).await;
    // 2. Rewrite the old hour, superseding both inputs.
    run_rewrite(mem.as_ref(), &clock).await;
    // 3. Fold again three hours later. The watermark advances, but the old
    //    hour is 100 hours behind it and no retirement frontier is configured,
    //    so neither the fixed window nor the frontier band re-lists it: HEAD
    //    still names the pre-rewrite inputs.
    fold_head(&mem, created + 3 * NS_PER_HOUR, 2, None).await;

    let named = head_named_data_keys(mem.as_ref(), OLD_HOUR).await;
    assert_eq!(
        named, input_data_keys,
        "the stale snapshot still names exactly the two pre-rewrite inputs"
    );
    let head_bytes = get_full(mem.as_ref(), &head_key()).await;
    let head = ravel_catalog::decode_head(head_bytes.as_ref()).expect("HEAD decodes");
    assert_eq!(head.parts.len(), 1, "single-part HEAD on this fixture");

    // Any delete at all faults the pass, so "zero deletes" is proven by the
    // pass succeeding, not only by the returned counters. The third catalog
    // GET faults too: the gate reads HEAD once and the one covering part once
    // per pass, never per input.
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::Delete, ScriptedFault::Timeout))
        .with_rule(
            Rule::new(Op::Get, ScriptedFault::Timeout)
                .with_key_contains("/catalog/")
                .with_occurrence(Occurrence::Nth(3)),
        );
    let store = FaultStore::new(mem.clone(), plan);

    clock.set(past_horizon(created));
    let outcome = sweep(&store, &clock)
        .await
        .expect("the gate holds every input, so no delete and no third catalog GET happen");

    assert_eq!(outcome.records_deleted, 0, "no record deleted");
    assert_eq!(outcome.data_deleted, 0, "no data object deleted");
    assert_eq!(
        outcome.held_by_snapshot, 4,
        "two commit records and two data objects held, reported as Named"
    );
    assert_eq!(outcome.held_by_unreadable_head, 0);
    assert_eq!(
        store.fault_count(Op::Delete, FaultKind::Timeout),
        0,
        "the sweep issued exactly zero deletes"
    );
    assert_eq!(
        store.fault_count(Op::Get, FaultKind::Timeout),
        0,
        "exactly two catalog GETs per pass: one HEAD, one covering part"
    );

    // The exact surviving set: every key the HEAD-named part references, and
    // both commit records that resolve them.
    assert_eq!(
        present_keys(mem.as_ref(), &input_data_keys).await,
        named,
        "the objects left in the store are exactly the ones HEAD names"
    );
    for key in &commit_keys {
        assert!(
            mem.head(key).await.is_ok(),
            "the commit record resolving a held input survives with it"
        );
    }
}

// --- (b) the gate delays, never prevents ------------------------------------

/// Once the fold has reconciled the hour, the snapshot names the rewrite's
/// output part instead of the pre-rewrite inputs, and the very next sweep
/// deletes exactly those inputs: two records, two data objects, nothing held.
///
/// Flip-line proof: make `SnapshotReachability::object_gate` return
/// `SnapshotGate::Blocked(SnapshotBlock::Named)` unconditionally (or drop the
/// `objects.contains(&named)` test so every entry blocks): the inputs are then
/// held forever and the `records_deleted == 2` assertion fails.
#[tokio::test]
async fn reconciled_hour_lets_the_sweep_delete_exactly_the_superseded_inputs() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let commit_keys = seed_two_hours(mem.as_ref()).await;
    let input_data_keys = seeded_input_data_keys(mem.as_ref(), &commit_keys).await;

    fold_head(&mem, created, 1, None).await;
    run_rewrite(mem.as_ref(), &clock).await;
    // A reconcile window wide enough to reach 100 hours back: the fold now
    // observes the late rewrite record and republishes the hour.
    fold_head(&mem, created + 3 * NS_PER_HOUR, 2, Some(200)).await;

    let named = head_named_data_keys(mem.as_ref(), OLD_HOUR).await;
    assert!(
        named.is_disjoint(&input_data_keys),
        "the reconciled snapshot names none of the pre-rewrite inputs"
    );
    assert_eq!(
        named.len(),
        1,
        "it names exactly the rewrite's single output part"
    );

    clock.set(past_horizon(created));
    let outcome = sweep(mem.as_ref(), &clock).await.expect("sweep");
    assert_eq!(outcome.records_deleted, 2, "both input commit records gone");
    assert_eq!(outcome.data_deleted, 2, "both input data objects gone");
    assert_eq!(outcome.held_by_snapshot, 0);
    assert_eq!(outcome.held_by_unreadable_head, 0);

    assert_eq!(
        present_keys(mem.as_ref(), &input_data_keys).await,
        BTreeSet::new(),
        "exactly the superseded input data objects were deleted"
    );
    for key in &commit_keys {
        assert!(
            mem.head(key).await.is_err(),
            "the superseded input's commit record is gone too"
        );
    }
    for key in &named {
        assert!(
            mem.head(key).await.is_ok(),
            "the live rewrite output part survives"
        );
    }
}

// --- (c) unreadable HEAD blocks the whole pass fail-closed ------------------

/// A HEAD that is present but unreadable blocks every group in the pass:
/// non-reachability cannot be proven from data that cannot be read. Nothing is
/// deleted and the hold is reported under the `Unreadable` reason, not the
/// ordinary `Named` one.
///
/// Flip-line proof: map the `decode_head` error in
/// `SnapshotReachability::ensure_head` to `HeadLoad::Absent` instead of
/// `HeadLoad::Unreadable`: the gate then clears, the four inputs are deleted,
/// the `Op::Delete` fault fires, and the `.expect` on the sweep panics.
#[tokio::test]
async fn unreadable_head_blocks_the_whole_pass_fail_closed() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let commit_keys = seed_two_hours(mem.as_ref()).await;
    let input_data_keys = seeded_input_data_keys(mem.as_ref(), &commit_keys).await;

    fold_head(&mem, created, 1, None).await;
    run_rewrite(mem.as_ref(), &clock).await;

    // Every byte of the HEAD object is flipped on read: present, undecodable.
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::Get, ScriptedFault::CorruptRange).with_key_contains(head_key()))
        .with_rule(Rule::new(Op::Delete, ScriptedFault::Timeout));
    let store = FaultStore::new(mem.clone(), plan);

    clock.set(past_horizon(created));
    let outcome = sweep(&store, &clock)
        .await
        .expect("an unreadable HEAD holds, it does not error the pass");

    assert_eq!(
        store.fault_count(Op::Get, FaultKind::CorruptRange),
        1,
        "exactly one HEAD GET per pass, and it was corrupted"
    );
    assert_eq!(outcome.records_deleted, 0);
    assert_eq!(outcome.data_deleted, 0);
    assert_eq!(
        outcome.held_by_unreadable_head, 4,
        "all four objects held under the Unreadable reason"
    );
    assert_eq!(outcome.held_by_snapshot, 0);
    assert_eq!(
        store.fault_count(Op::Delete, FaultKind::Timeout),
        0,
        "the sweep issued exactly zero deletes"
    );
    assert_eq!(
        present_keys(mem.as_ref(), &input_data_keys).await.len(),
        2,
        "both input data objects survive"
    );
}

// --- (d) absent HEAD clears the gate ---------------------------------------

/// With no HEAD at all there is no snapshot naming anything, so the gate
/// clears and the sweep deletes on the horizon exactly as it always did
/// (ADR-0020: the catalog index is a pure optimization).
///
/// Flip-line proof: map `StoreError::NotFound` in
/// `SnapshotReachability::ensure_head` to `HeadLoad::Unreadable` instead of
/// `HeadLoad::Absent`: nothing is deleted and the `records_deleted == 2`
/// assertion fails.
#[tokio::test]
async fn absent_head_clears_the_gate_and_the_pass_deletes_as_before() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let commit_keys = seed_two_hours(mem.as_ref()).await;
    let input_data_keys = seeded_input_data_keys(mem.as_ref(), &commit_keys).await;

    // Deliberately no fold: HEAD is absent.
    run_rewrite(mem.as_ref(), &clock).await;
    assert!(mem.head(&head_key()).await.is_err(), "no HEAD was folded");

    clock.set(past_horizon(created));
    let outcome = sweep(mem.as_ref(), &clock).await.expect("sweep");
    assert_eq!(outcome.records_deleted, 2);
    assert_eq!(outcome.data_deleted, 2);
    assert_eq!(outcome.held_by_snapshot, 0);
    assert_eq!(outcome.held_by_unreadable_head, 0);
    assert_eq!(
        present_keys(mem.as_ref(), &input_data_keys).await,
        BTreeSet::new()
    );
}

// --- (e) the two horizons, cross-checked ------------------------------------

/// Seed the request object and its completion record. `completed` anchors the
/// `.dreq` removal horizon; `bucket_drops` names the one bucket this request's
/// rewrite touched.
async fn seed_dreq_and_done(store: &dyn ObjectStoreBackend, completed: i64) -> (String, String) {
    let dreq_key =
        keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id()).unwrap();
    store
        .put(
            &dreq_key,
            erasure::encode_request(&erasure_request()),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let completion = ErasureCompletion {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id().to_string(),
        predicate_hash: vec![0x11; 32],
        bucket_drops: vec![ErasureBucketDrop {
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard: SHARD,
            ingest_hour_bucket: OLD_HOUR,
            dropped_count: 2,
        }],
        requested_unix_ns: 0,
        completed_unix_ns: completed,
        deferral_cause: 0,
    };
    let done_key =
        keys::erasure_completion_key(&tenant_hash(), Signal::Metrics, request_id()).unwrap();
    store
        .put(
            &done_key,
            erasure::encode_completion(&completion),
            PutOptions::default(),
        )
        .await
        .unwrap();
    (dreq_key, done_key)
}

/// The two horizons are tied only by both being `protection_horizon_ns`; the
/// invariant they exist to provide is an ordering: the query-time exclusion
/// filter must outlive every pre-rewrite input a snapshot can still resolve.
/// This drives both sweeps in the maintenance tick's own order (superseded
/// inputs first, then `.dreq` removal) across a clock advance, and asserts
/// after every single sweep call that the `.dreq` is still present whenever any
/// superseded input is.
///
/// Flip-line proof, two lines, one per direction. Remove the
/// `superseded_inputs_outstanding` guard in `sweep_erasure_requests` and the
/// `.dreq` disappears at its horizon while the held inputs are still resolvable
/// from the stale snapshot, failing the ordering assertion at step 3. Replace
/// the `reach.object_gate(...)` match in `sweep_superseded_impl` with
/// `SnapshotGate::Clear` and the inputs are deleted at step 3 while the
/// snapshot still names them, failing the "inputs still present" assertion
/// there.
#[tokio::test]
async fn dreq_outlives_every_superseded_input_its_rewrite_left_resolvable() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let commit_keys = seed_two_hours(mem.as_ref()).await;
    let input_data_keys = seeded_input_data_keys(mem.as_ref(), &commit_keys).await;

    fold_head(&mem, created, 1, None).await;
    run_rewrite(mem.as_ref(), &clock).await;
    fold_head(&mem, created + 3 * NS_PER_HOUR, 2, None).await;
    let rw_key = rewrite_record_key(mem.as_ref()).await;

    // The completion lands at the same instant the rewrite did, so the two
    // horizons coincide exactly: this is the tightest ordering the pair admits.
    let (dreq_key, done_key) = seed_dreq_and_done(mem.as_ref(), created).await;

    // After every sweep call: if any superseded input is still in the store,
    // the request that excludes it must still be there too.
    async fn assert_ordering(
        store: &dyn ObjectStoreBackend,
        dreq_key: &str,
        inputs: &BTreeSet<String>,
        step: &str,
    ) {
        let remaining = present_keys(store, inputs).await.len();
        if remaining > 0 {
            assert!(
                store.head(dreq_key).await.is_ok(),
                "{step}: {remaining} superseded input(s) still resolvable, so the .dreq must \
                 still be excluding them"
            );
        }
    }

    let horizon = created + cfg().protection_horizon_ns;
    for (step, now) in [("pre-horizon", created), ("one ns early", horizon - 1)] {
        clock.set(now);
        let out = sweep(mem.as_ref(), &clock).await.expect("superseded sweep");
        assert_eq!(
            (out.records_deleted, out.data_deleted),
            (0, 0),
            "{step}: nothing collectable yet"
        );
        assert_ordering(mem.as_ref(), &dreq_key, &input_data_keys, step).await;

        let dreq = sweep_erasure_requests(
            mem.as_ref(),
            &clock,
            &cfg(),
            &NoLeases,
            &tenant_hash(),
            Signal::Metrics,
        )
        .await
        .expect("dreq sweep");
        assert_eq!(dreq.deleted, 0, "{step}: the .dreq horizon has not elapsed");
        assert_ordering(mem.as_ref(), &dreq_key, &input_data_keys, step).await;
    }

    // Step 3: both horizons have elapsed, but the stale snapshot still names
    // the inputs. The sweep holds them, and the .dreq is held with them.
    clock.set(horizon);
    let out = sweep(mem.as_ref(), &clock).await.expect("superseded sweep");
    assert_eq!(out.held_by_snapshot, 4, "at the horizon: all four held");
    assert_eq!((out.records_deleted, out.data_deleted), (0, 0));
    assert_eq!(
        present_keys(mem.as_ref(), &input_data_keys).await.len(),
        2,
        "the inputs the stale snapshot names are still present"
    );
    assert_ordering(mem.as_ref(), &dreq_key, &input_data_keys, "at the horizon").await;

    let dreq = sweep_erasure_requests(
        mem.as_ref(),
        &clock,
        &cfg(),
        &NoLeases,
        &tenant_hash(),
        Signal::Metrics,
    )
    .await
    .expect("dreq sweep");
    assert_eq!(dreq.deleted, 0, "the .dreq outlives the held inputs");
    assert_eq!(dreq.kept, 1);
    assert_eq!(
        dreq.held_by_superseded_inputs, 1,
        "held for the stated reason, not merely by the horizon"
    );
    assert_ordering(mem.as_ref(), &dreq_key, &input_data_keys, "at the horizon").await;
    assert!(
        mem.head(&rw_key).await.is_ok(),
        "the rewrite record that supersedes them is untouched"
    );

    // Step 4: the fold reconciles the hour. The gate clears, the inputs go,
    // and only then does the .dreq become removable.
    fold_head(&mem, horizon + 3 * NS_PER_HOUR, 3, Some(200)).await;
    let out = sweep(mem.as_ref(), &clock).await.expect("superseded sweep");
    assert_eq!((out.records_deleted, out.data_deleted), (2, 2));
    assert_eq!(
        present_keys(mem.as_ref(), &input_data_keys).await,
        BTreeSet::new(),
        "every superseded input is physically gone"
    );
    assert_ordering(mem.as_ref(), &dreq_key, &input_data_keys, "after reconcile").await;

    let dreq = sweep_erasure_requests(
        mem.as_ref(),
        &clock,
        &cfg(),
        &NoLeases,
        &tenant_hash(),
        Signal::Metrics,
    )
    .await
    .expect("dreq sweep");
    assert_eq!(dreq.deleted, 1, "the .dreq is removed once, and only now");
    assert_eq!(dreq.held_by_superseded_inputs, 0);
    assert!(mem.head(&dreq_key).await.is_err(), ".dreq physically gone");
    assert!(
        mem.head(&done_key).await.is_ok(),
        ".done is permanent audit evidence, never swept"
    );
}

// --- (f) the erasure-invariant hole: supersession chains --------------------
//
// A rewrite record can itself be superseded by a later rewrite applying a
// different request. Gating the later generation on its predecessor's OUTPUT
// parts alone clears while a stale HEAD names the raw inputs at the end of the
// chain, so the predecessor's record disappears and the request it applied is
// no longer discoverable from any record. The `.dreq` guard then retires the
// query-time exclusion filter while the raw inputs it erased the subject out of
// are still present and HEAD-resolvable. Two rules close it: a rewrite record
// outlives every input it superseded, and a `.dreq` outlives every input any
// rewrite in its supersession chain superseded.

/// The pieces of a supersession-chain fixture the assertions need.
struct ChainFixture {
    commit_keys: [String; 2],
    input_data_keys: BTreeSet<String>,
    /// Rewrite record keys, oldest generation first.
    chain: Vec<String>,
}

/// Seed `generations` rewrite generations over [`OLD_HOUR`]'s two raw L0
/// inputs. The oldest applies [`request_id`] over `victim` (the only request
/// that erases anything here); each later one applies a fresh request id over a
/// series name no input carries, so it republishes its predecessor's surviving
/// rows and grows the chain without changing what is erased.
///
/// `reconcile` is the second fold's reconcile window: `None` leaves the hour
/// unreconciled, so HEAD still names the raw inputs.
async fn seed_chain(
    mem: &Arc<MemoryStore>,
    clock: &FixedClock,
    created: i64,
    generations: u128,
    reconcile: Option<u32>,
) -> ChainFixture {
    let commit_keys = seed_two_hours(mem.as_ref()).await;
    let input_data_keys = seeded_input_data_keys(mem.as_ref(), &commit_keys).await;
    fold_head(mem, created, 1, None).await;
    run_rewrite_with(mem.as_ref(), clock, &[pending_request()]).await;
    for n in 1..generations {
        run_rewrite_with(
            mem.as_ref(),
            clock,
            &[pending_request_for(request_id_n(n), &format!("absent{n}"))],
        )
        .await;
    }
    fold_head(mem, created + 3 * NS_PER_HOUR, 2, reconcile).await;
    let chain = rewrite_chain(mem.as_ref()).await;
    assert_eq!(
        u128::try_from(chain.len()).unwrap(),
        generations,
        "one rewrite generation per applied request"
    );
    ChainFixture {
        commit_keys,
        input_data_keys,
        chain,
    }
}

/// The reviewer's scenario. R1 applies X and supersedes the two raw L0 inputs
/// carrying `victim`; R2 applies Y and supersedes R1; HEAD is never re-folded
/// within reach of the hour, so it still names the raw inputs.
///
/// Past the horizon the sweep must delete nothing: the chain is one group, and
/// a HEAD naming anything in it holds all of it, R1's own record included. The
/// `.dreq` guard must then find X by walking the chain from R2 back through R1,
/// and hold the filter.
///
/// Also pins the guard's request shape on this fixture: two LISTs (`del/` and
/// the bucket's commit prefix) and five GETs (the `.done`, R1's and R2's
/// records, and the two input commit records) for the whole pass, with the
/// chain walk reusing the records the pass already read.
///
/// Flip-line proof: in `sweep_superseded_impl`, drop the
/// `superseded_by_present.contains(key)` skip and gate each generation from its
/// own entry again (equivalently, replace `gather_superseded_chain` with a
/// gather of the predecessor's own record and parts). R1's record is then
/// deleted while its raw inputs are held, the `records_deleted == 0` assertion
/// fails, and the `.dreq` assertions fail with it.
#[tokio::test]
async fn superseded_predecessor_and_its_dreq_outlive_head_named_raw_inputs() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let fixture = seed_chain(&mem, &clock, created, 2, None).await;
    let r1 = fixture.chain[0].clone();
    let r2 = fixture.chain[1].clone();
    let r1_parts = rewrite_part_keys(mem.as_ref(), &r1).await;
    assert_eq!(r1_parts.len(), 1, "R1 published exactly one part");

    assert_eq!(
        head_named_data_keys(mem.as_ref(), OLD_HOUR).await,
        fixture.input_data_keys,
        "the stale snapshot still names exactly the two pre-rewrite inputs"
    );

    let (dreq_x, _) = seed_dreq_and_done_for(mem.as_ref(), request_id(), "victim", created).await;

    clock.set(past_horizon(created));
    let out = sweep(mem.as_ref(), &clock).await.expect("superseded sweep");
    assert_eq!(
        out.records_deleted, 0,
        "no record deleted: R1 is part of the held chain group, not its own gate"
    );
    assert_eq!(out.data_deleted, 0, "no data object deleted");
    assert_eq!(
        out.held_by_snapshot, 6,
        "one group of six: two input commit records, two input data objects, R1's single \
         part, and R1's own record"
    );
    assert_eq!(out.held_by_unreadable_head, 0);

    // Exact surviving key sets.
    assert_eq!(
        present_keys(mem.as_ref(), &fixture.input_data_keys).await,
        fixture.input_data_keys,
        "both victim inputs still present"
    );
    let mut records: BTreeSet<String> = fixture.commit_keys.iter().cloned().collect();
    records.insert(r1.clone());
    records.insert(r2.clone());
    assert_eq!(
        present_keys(mem.as_ref(), &records).await,
        records,
        "R1's record survives with the inputs it superseded, and so does R2"
    );
    assert_eq!(
        present_keys(mem.as_ref(), &r1_parts).await,
        r1_parts,
        "R1's parts survive with its record"
    );

    // The guard's request shape is asserted, not printed: the sixth GET and the
    // third LIST of the pass both fault, so the pass succeeding proves the
    // counts.
    let plan = FaultPlan::empty()
        .with_rule(
            Rule::new(Op::Get, ScriptedFault::Timeout).with_occurrence(Occurrence::Nth(6)),
        )
        .with_rule(
            Rule::new(Op::List, ScriptedFault::Timeout).with_occurrence(Occurrence::Nth(3)),
        );
    let store = FaultStore::new(mem.clone(), plan);
    let dreq = sweep_dreq(&store, &clock)
        .await
        .expect("the chain walk reads each record once, so no sixth GET and no third LIST");
    assert_eq!(
        dreq.deleted, 0,
        ".dreq_X outlives the inputs R1 erased the subject out of"
    );
    assert_eq!(dreq.kept, 1);
    assert_eq!(
        dreq.held_by_superseded_inputs, 1,
        "held for the stated reason: X is found by walking R2 -> R1, whose inputs are present"
    );
    assert_eq!(
        store.fault_count(Op::Get, FaultKind::Timeout),
        0,
        "exactly five GETs: the .done, R1's and R2's records, and the two input commit records"
    );
    assert_eq!(
        store.fault_count(Op::List, FaultKind::Timeout),
        0,
        "exactly two LISTs: the del/ prefix and the touched bucket's commit prefix"
    );
    assert!(mem.head(&dreq_x).await.is_ok(), ".dreq_X physically present");
}

/// The same chain, once the fold has reconciled the hour: the gate clears and
/// the whole chain is collected in the one order the invariant allows. The
/// superseded inputs go first and R1's record goes last, proven by faulting
/// exactly R1's record delete and observing what is already gone at that
/// instant. A second, clean pass finishes R1, and only then does the `.dreq`
/// sweep retire the filter.
///
/// Flip-line proof: move the `chain_record_keys` delete loop in
/// `sweep_superseded_impl` ahead of the `data_keys` loop. R1's record is then
/// deleted before the inputs it superseded, and the "already gone at the fault"
/// assertions fail.
#[tokio::test]
async fn reconciled_chain_deletes_the_superseded_inputs_before_the_record_that_erased_them() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let fixture = seed_chain(&mem, &clock, created, 2, Some(200)).await;
    let r1 = fixture.chain[0].clone();
    let r2 = fixture.chain[1].clone();
    let r1_parts = rewrite_part_keys(mem.as_ref(), &r1).await;
    let r2_parts = rewrite_part_keys(mem.as_ref(), &r2).await;

    let named = head_named_data_keys(mem.as_ref(), OLD_HOUR).await;
    assert_eq!(
        named, r2_parts,
        "the reconciled snapshot names exactly the live generation's parts"
    );

    let (dreq_x, done_x) =
        seed_dreq_and_done_for(mem.as_ref(), request_id(), "victim", created).await;

    // Step 1: fault exactly R1's record delete. Everything the invariant
    // requires to be gone before it must already be gone.
    clock.set(past_horizon(created));
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Delete, ScriptedFault::Timeout).with_key_contains(r1.clone()),
    );
    let store = FaultStore::new(mem.clone(), plan);
    let err = sweep(&store, &clock).await;
    assert!(
        err.is_err(),
        "the faulted delete of R1's record surfaces as a pass error"
    );
    assert_eq!(
        store.fault_count(Op::Delete, FaultKind::Timeout),
        1,
        "exactly one delete was aimed at R1's record"
    );
    assert_eq!(
        present_keys(mem.as_ref(), &fixture.input_data_keys).await,
        BTreeSet::new(),
        "every input R1 superseded is already gone when its record's delete is issued"
    );
    for key in &fixture.commit_keys {
        assert!(
            mem.head(key).await.is_err(),
            "the superseded input's commit record went before R1's record too"
        );
    }
    assert_eq!(
        present_keys(mem.as_ref(), &r1_parts).await,
        BTreeSet::new(),
        "R1's own parts went before R1's record"
    );
    assert!(
        mem.head(&r1).await.is_ok(),
        "R1's record is still there: its delete is the one that faulted"
    );
    assert!(
        mem.head(&dreq_x).await.is_ok(),
        "the filter is still live while R1's record is"
    );

    // Step 2: a clean pass finishes the job. The group's inputs are already
    // gone, so it is R1's record and the parts it names, re-issued idempotently.
    let out = sweep(mem.as_ref(), &clock).await.expect("superseded sweep");
    assert_eq!(out.records_deleted, 1, "R1's record, and nothing else");
    assert_eq!(
        out.data_deleted, 1,
        "R1's single part, deleted again now that the pass reaches past the fault"
    );
    assert_eq!((out.held_by_snapshot, out.held_by_unreadable_head), (0, 0));
    assert!(mem.head(&r1).await.is_err(), "R1's record is gone");
    assert!(mem.head(&r2).await.is_ok(), "the live generation survives");
    assert_eq!(
        present_keys(mem.as_ref(), &r2_parts).await,
        r2_parts,
        "the live generation's parts survive"
    );

    // Step 3: only with every superseded input gone does the filter retire.
    let dreq = sweep_dreq(mem.as_ref(), &clock).await.expect("dreq sweep");
    assert_eq!(dreq.deleted, 1, ".dreq_X is removed once, and only now");
    assert_eq!(dreq.kept, 0);
    assert_eq!(dreq.held_by_superseded_inputs, 0);
    assert!(mem.head(&dreq_x).await.is_err(), ".dreq_X physically gone");
    assert!(
        mem.head(&done_x).await.is_ok(),
        ".done is permanent audit evidence, never swept"
    );
}

/// Three generations: R1 applies X, R2 applies Y and supersedes R1, R3 applies
/// Z and supersedes R2. Only R1's raw inputs are HEAD-named, and that holds
/// every record in the chain and every one of the three `.dreq` objects: each
/// superseded generation's parts still carry whatever the generation above it
/// erased, so the same rule covers Y and Z without a special case.
///
/// Flip-line proof: stop the walk in `gather_superseded_chain` after one
/// generation (`break` instead of following `superseded_record_key`). R1 and its
/// inputs then fall outside the group R3's entry gates, the chain is collected
/// from the top down, and the `records_deleted == 0` and
/// `held_by_superseded_inputs == 3` assertions both fail.
#[tokio::test]
async fn three_deep_chain_holds_every_generation_and_every_request() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let fixture = seed_chain(&mem, &clock, created, 3, None).await;
    let r1_parts = rewrite_part_keys(mem.as_ref(), &fixture.chain[0]).await;
    let r2_parts = rewrite_part_keys(mem.as_ref(), &fixture.chain[1]).await;
    assert_eq!(r1_parts.len(), 1);
    assert_eq!(r2_parts.len(), 1);

    assert_eq!(
        head_named_data_keys(mem.as_ref(), OLD_HOUR).await,
        fixture.input_data_keys,
        "only R1's raw inputs are HEAD-named"
    );

    let mut dreqs = BTreeSet::new();
    for n in 0..3u128 {
        let series = if n == 0 {
            "victim".to_string()
        } else {
            format!("absent{n}")
        };
        let (dreq, _) =
            seed_dreq_and_done_for(mem.as_ref(), request_id_n(n), &series, created).await;
        dreqs.insert(dreq);
    }
    assert_eq!(dreqs.len(), 3, "three distinct requests");

    clock.set(past_horizon(created));
    let out = sweep(mem.as_ref(), &clock).await.expect("superseded sweep");
    assert_eq!(
        (out.records_deleted, out.data_deleted),
        (0, 0),
        "no record in the chain is deleted while R1's inputs are present"
    );
    assert_eq!(
        out.held_by_snapshot, 8,
        "one group of eight: two input commit records, two input data objects, R1's and R2's \
         one part each, and R1's and R2's own records"
    );
    assert_eq!(out.held_by_unreadable_head, 0);

    let mut survivors: BTreeSet<String> = fixture.chain.iter().cloned().collect();
    survivors.extend(fixture.commit_keys.iter().cloned());
    survivors.extend(fixture.input_data_keys.iter().cloned());
    survivors.extend(r1_parts.iter().cloned());
    survivors.extend(r2_parts.iter().cloned());
    assert_eq!(
        present_keys(mem.as_ref(), &survivors).await,
        survivors,
        "every generation, its parts, and the raw inputs all survive"
    );

    let dreq = sweep_dreq(mem.as_ref(), &clock).await.expect("dreq sweep");
    assert_eq!(dreq.deleted, 0, "no filter retires");
    assert_eq!(dreq.kept, 3);
    assert_eq!(
        dreq.held_by_superseded_inputs, 3,
        "X because R1's raw inputs are present, Y and Z because a generation each superseded \
         is still present"
    );
    assert_eq!(
        present_keys(mem.as_ref(), &dreqs).await,
        dreqs,
        "all three .dreq objects physically present"
    );
}

/// The guard on its own, with deliverable 1's ordering removed by hand: R1's
/// record is deleted directly, as an older sweep would have left it, while its
/// raw inputs stay. The chain from R2 is then cut at a missing record, so X is
/// named nowhere, and the guard must still hold `.dreq_X` rather than infer
/// from "no record mentions X" that nothing is outstanding.
///
/// The hold terminates: once every object the missing generation could have
/// superseded is gone, the same guard retires the filter on the next pass.
///
/// Flip-line proof: delete the `walk.truncated` arm of the decision in
/// `superseded_inputs_outstanding` (keep only the
/// `walk.request_ids.contains(request_id)` branch). The cut chain then reports
/// nothing outstanding and the `deleted == 0` assertion fails.
#[tokio::test]
async fn dreq_guard_holds_when_the_record_that_applied_the_request_is_already_gone() {
    let mem = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let fixture = seed_chain(&mem, &clock, created, 2, None).await;
    let r1 = fixture.chain[0].clone();
    let r1_parts = rewrite_part_keys(mem.as_ref(), &r1).await;
    let (dreq_x, _) = seed_dreq_and_done_for(mem.as_ref(), request_id(), "victim", created).await;

    // An older sweep's leftover: the record that applied X is gone, the inputs
    // it superseded are not.
    mem.delete(&r1).await.expect("delete R1's record");
    assert_eq!(
        present_keys(mem.as_ref(), &fixture.input_data_keys).await,
        fixture.input_data_keys,
        "the inputs R1 superseded are still present"
    );

    clock.set(past_horizon(created));
    let dreq = sweep_dreq(mem.as_ref(), &clock).await.expect("dreq sweep");
    assert_eq!(
        dreq.deleted, 0,
        "the chain from R2 is cut at the missing record and the bucket still holds raw inputs, \
         so the filter stays even though no record names X"
    );
    assert_eq!(dreq.kept, 1);
    assert_eq!(dreq.held_by_superseded_inputs, 1);
    assert!(mem.head(&dreq_x).await.is_ok(), ".dreq_X physically present");

    // Remove everything the missing generation could have superseded.
    for key in fixture
        .commit_keys
        .iter()
        .chain(fixture.input_data_keys.iter())
        .chain(r1_parts.iter())
    {
        mem.delete(key).await.expect("delete a residual object");
    }
    let dreq = sweep_dreq(mem.as_ref(), &clock).await.expect("dreq sweep");
    assert_eq!(dreq.deleted, 1, "the hold terminates on the next pass");
    assert_eq!(dreq.kept, 0);
    assert_eq!(dreq.held_by_superseded_inputs, 0);
    assert!(mem.head(&dreq_x).await.is_err(), ".dreq_X physically gone");
}
