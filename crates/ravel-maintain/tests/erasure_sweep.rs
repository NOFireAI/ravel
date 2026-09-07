//! The physical-removal half of selective subject erasure.
//!
//! Proves, against a `MemoryStore` oracle with an injected clock, that the
//! sweep additions honor ADR-0064's deletion guarantees:
//!
//! - a rewrite's superseded inputs (raw-L0 identities, or a whole superseded
//!   predecessor record plus its pre-rewrite L1 parts) are deleted by the
//!   existing superseded-input sweep, but only after `protection_horizon` and
//!   only when the `LegalHoldCheck` passes;
//! - a `.dreq` is removed only once its `.done` exists AND
//!   `now >= done.completed_unix_ns + protection_horizon` AND no legal hold
//!   covers it, never a nanosecond early, and the `.done` is never deleted.
//!
//! Each test names the line whose flip breaks it, per the repo's
//! prove-the-test discipline.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::{BTreeMap, HashSet};

use common::*;
use prost::Message;
use ravel_commit::keys::{self, BucketEntry};
use ravel_commit::{erasure, record, signal};
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::erasure_rewrite::{ERASURE_REWRITE_DEADLINE_NS, erasure_seal_wait_bound_ns};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, ErasureRewriteOutcome, FixedClock, LeaseCheck,
    LegalHoldCheck, MaintainMemo, NoLeases, PendingErasureRequest, PublishOutcome, RetentionConfig,
    RetentionOutcome, RetentionPolicy, SupersededSweepOutcome, bucket_erasure_completion,
    compact_bucket, erasure_rewrite_bucket, pending_erasure_requests, retention_sweep_bucket,
    shard_hold_scopes, sweep_erasure_requests, sweep_superseded, write_hold_set,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    CompactionRecord, ErasureCompletion, ErasurePredicateMatcher, ErasureRequest, RewriteRecord,
};
use ravel_types::Signal;
use uuid::Uuid;

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// A `now_ns` comfortably past a record's supersession horizon.
fn past_horizon(created_ns: i64) -> i64 {
    created_ns
        .saturating_add(cfg().protection_horizon_ns)
        .saturating_add(NS_PER_HOUR)
}

/// A `LeaseCheck` protecting exactly one key prefix, counting consultations so
/// a test can prove the hold was actually consulted, not merely configured.
/// `LeaseCheck: Send + Sync` forces a thread-safe counter.
struct HoldPrefixSync {
    prefix: String,
    consulted: std::sync::atomic::AtomicUsize,
}
impl LeaseCheck for HoldPrefixSync {
    fn is_protected(&self, key: &str) -> bool {
        self.consulted
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        key.starts_with(&self.prefix)
    }
}

fn metrics_specs() -> Vec<InputSpec> {
    vec![
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("victim", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("victim", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ]
}

/// A windowless erasure request matching every series named `metric`,
/// acknowledged at time 0.
fn pending_request(seed: u128, metric: &str) -> PendingErasureRequest {
    pending_request_at(seed, metric, 0)
}

/// A windowless erasure request matching every series named `metric`,
/// acknowledged at `created_unix_ns` -- the instant that fixes the request's
/// scope (ADR-0064 decision 1).
fn pending_request_at(seed: u128, metric: &str, created_unix_ns: i64) -> PendingErasureRequest {
    let request_id = Uuid::from_u128(seed);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns,
        predicate: vec![ErasurePredicateMatcher {
            key: "__name__".to_string(),
            value: metric.to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request,
    }
}

async fn run_rewrite(store: &dyn ObjectStoreBackend, clock: &FixedClock) -> ErasureRewriteOutcome {
    let mut memo = MaintainMemo::with_default_interval();
    erasure_rewrite_bucket(
        store,
        clock,
        &cfg(),
        &NoLeases,
        &bucket(),
        &[pending_request(42, "victim")],
        &mut memo,
    )
    .await
    .expect("rewrite")
}

async fn sweep(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    lease: &dyn LeaseCheck,
) -> (usize, usize) {
    let out = sweep_full(store, clock, lease).await;
    (out.records_deleted, out.data_deleted)
}

async fn sweep_full(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    lease: &dyn LeaseCheck,
) -> SupersededSweepOutcome {
    let b = bucket();
    sweep_superseded(
        store,
        clock,
        &cfg(),
        lease,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("sweep_superseded")
}

fn l0_data_prefix(b: &Bucket) -> String {
    format!(
        "t/{}/{}/l0/{:04}/",
        b.tenant_hash.to_hex(),
        b.signal.key_prefix(),
        b.shard
    )
}

async fn l0_data_count(store: &dyn ObjectStoreBackend, b: &Bucket) -> usize {
    list_all(store, &l0_data_prefix(b)).await.unwrap().len()
}

/// Find the single compaction or rewrite record key in the test bucket.
async fn find_record_key(store: &dyn ObjectStoreBackend, want_rewrite: bool) -> String {
    let b = bucket();
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .unwrap();
    for m in list_all(store, &prefix).await.unwrap() {
        match keys::partition_bucket_entry(&m.key) {
            Ok(BucketEntry::RewriteRecord(_)) if want_rewrite => return m.key,
            Ok(BucketEntry::CompactionRecord(_)) if !want_rewrite => return m.key,
            _ => {}
        }
    }
    panic!("expected record not found (want_rewrite={want_rewrite})");
}

async fn compaction_part_keys(store: &dyn ObjectStoreBackend, key: &str) -> Vec<String> {
    let got = get_full(store, key).await;
    let r = CompactionRecord::decode(got.as_ref()).unwrap();
    r.parts
        .iter()
        .map(|p| keys::reconstruct_l1_part_key(&r, p).unwrap())
        .collect()
}

async fn rewrite_part_keys(store: &dyn ObjectStoreBackend, key: &str) -> Vec<String> {
    let got = get_full(store, key).await;
    let r = RewriteRecord::decode(got.as_ref()).unwrap();
    r.parts
        .iter()
        .map(|p| keys::reconstruct_rewrite_part_key(&r, p).unwrap())
        .collect()
}

async fn present(store: &dyn ObjectStoreBackend, key: &str) -> bool {
    store.head(key).await.is_ok()
}

// --- Deliverable 1: rewrite-superseded inputs -------------------------------

/// A raw-L0 rewrite's superseded L0 inputs are deleted by the existing
/// superseded-input sweep, but only after `protection_horizon`.
///
/// Flip-line proof: in `sweep_superseded_impl`, changing the `RewriteRecord`
/// arm's horizon gate to unconditionally skip (or dropping the
/// `BucketEntry::RewriteRecord` arm back to `continue`) leaves the L0 data
/// objects in place, failing the `l0_data_count == 0` assertion after the
/// horizon.
#[tokio::test]
async fn rewrite_raw_l0_inputs_swept_only_after_horizon() {
    let store = MemoryStore::new();
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    for spec in metrics_specs() {
        seed_input(&store, &spec).await;
    }
    let b = bucket();
    assert_eq!(l0_data_count(&store, &b).await, 2, "two L0 inputs seeded");

    let outcome = run_rewrite(&store, &clock).await;
    assert!(
        matches!(outcome, ErasureRewriteOutcome::Rewritten { .. }),
        "rewrite publishes a record, got {outcome:?}"
    );
    let rw_key = find_record_key(&store, true).await;
    let rw_parts = rewrite_part_keys(&store, &rw_key).await;

    // Before the horizon: the rewrite record superseded the inputs, but they
    // are not yet collectable.
    let (recs, data) = sweep(&store, &clock, &NoLeases).await;
    assert_eq!((recs, data), (0, 0), "nothing swept before the horizon");
    assert_eq!(
        l0_data_count(&store, &b).await,
        2,
        "L0 inputs still present before the horizon"
    );

    // At/after the horizon: the L0 inputs are gone, the rewrite record and its
    // output part survive (they are the live data, superseded by nothing).
    clock.set(past_horizon(created));
    let (recs, data) = sweep(&store, &clock, &NoLeases).await;
    assert!(recs > 0 && data > 0, "records and data swept after horizon");
    assert_eq!(
        l0_data_count(&store, &b).await,
        0,
        "L0 inputs physically gone after the horizon"
    );
    assert!(
        present(&store, &rw_key).await,
        "live rewrite record survives"
    );
    for p in &rw_parts {
        assert!(
            present(&store, p).await,
            "live rewrite output part survives"
        );
    }
}

/// A legal hold over the shard's key prefixes blocks the rewrite-superseded
/// sweep even past the horizon, where an unheld sweep would have deleted the
/// inputs. Proven against a no-hold control that does delete them, and by
/// asserting the hold was actually consulted.
///
/// Flip-line proof: dropping the `group.protected_key(lease)` skip that
/// `sweep_superseded_impl` applies to a whole chain group before gating it
/// makes the held inputs vanish, failing the `l0_data_count == 2` assertion.
#[tokio::test]
async fn rewrite_superseded_inputs_survive_under_legal_hold() {
    let created = sealed_now_ns();

    // Control: no hold, inputs are swept after the horizon.
    let control = MemoryStore::new();
    let clock = FixedClock::new(created);
    for spec in metrics_specs() {
        seed_input(&control, &spec).await;
    }
    run_rewrite(&control, &clock).await;
    clock.set(past_horizon(created));
    let (_r, control_data) = sweep(&control, &clock, &NoLeases).await;
    assert!(control_data > 0, "control sweeps the inputs");
    assert_eq!(l0_data_count(&control, &bucket()).await, 0);

    // Held: the same sweep leaves the inputs in place.
    let held = MemoryStore::new();
    let clock = FixedClock::new(created);
    for spec in metrics_specs() {
        seed_input(&held, &spec).await;
    }
    run_rewrite(&held, &clock).await;
    let b = bucket();
    for (i, scope) in shard_hold_scopes(&b.tenant_hash, b.signal, b.shard)
        .unwrap()
        .iter()
        .enumerate()
    {
        write_hold_set(
            &held,
            &b.tenant_hash,
            Uuid::from_u128(200 + i as u128),
            created,
            scope,
            "litigation hold",
        )
        .await
        .unwrap();
    }
    let lease = LegalHoldCheck::refresh(&held, &b.tenant_hash)
        .await
        .unwrap();
    assert!(!lease.is_empty(), "hold snapshot is active");

    clock.set(past_horizon(created));
    let (recs, data) = sweep(&held, &clock, &lease).await;
    assert_eq!(
        (recs, data),
        (0, 0),
        "a held bucket sheds nothing, even past the horizon"
    );
    assert_eq!(
        l0_data_count(&held, &b).await,
        2,
        "held L0 inputs survive the sweep"
    );
}

/// A rewrite that superseded a whole prior compaction record (via
/// `superseded_record_key`) sheds that predecessor record AND its pre-rewrite
/// L1 parts -- the parts holding the un-erased subject that rule 3 cannot
/// collect while the predecessor record still references them -- after the
/// horizon, while the rewrite's own output parts survive.
///
/// Flip-line proof: replacing the `superseded_record_key` arm's
/// `gather_superseded_chain` call with `Ok(Vec::new())` leaves the predecessor
/// compaction record and its L1 parts in place, failing the
/// `!present(comp_key)` / `!present(comp_part)` assertions after the horizon.
#[tokio::test]
async fn rewrite_predecessor_record_and_parts_swept_after_horizon() {
    let store = MemoryStore::new();
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    for spec in metrics_specs() {
        seed_input(&store, &spec).await;
    }

    // Compact first, so the rewrite's live input set is an L1 compaction
    // record (the `superseded_record_key` case), not raw L0.
    let outcome = compact_bucket(&store, &clock, &cfg(), &bucket())
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    let comp_key = find_record_key(&store, false).await;
    let comp_parts = compaction_part_keys(&store, &comp_key).await;
    assert!(!comp_parts.is_empty(), "compaction produced L1 parts");

    let outcome = run_rewrite(&store, &clock).await;
    assert!(
        matches!(outcome, ErasureRewriteOutcome::Rewritten { .. }),
        "rewrite over the compacted bucket publishes, got {outcome:?}"
    );
    let rw_key = find_record_key(&store, true).await;
    let rw_parts = rewrite_part_keys(&store, &rw_key).await;

    // Before the horizon: predecessor record and its parts still present.
    sweep(&store, &clock, &NoLeases).await;
    assert!(
        present(&store, &comp_key).await,
        "predecessor present pre-horizon"
    );
    for p in &comp_parts {
        assert!(
            present(&store, p).await,
            "predecessor part present pre-horizon"
        );
    }

    // After the horizon: predecessor record and every pre-rewrite L1 part gone;
    // the live rewrite record and its output parts survive.
    clock.set(past_horizon(created));
    sweep(&store, &clock, &NoLeases).await;
    assert!(
        !present(&store, &comp_key).await,
        "superseded compaction record physically gone after horizon"
    );
    for p in &comp_parts {
        assert!(
            !present(&store, p).await,
            "pre-rewrite L1 part physically gone after horizon"
        );
    }
    assert!(
        present(&store, &rw_key).await,
        "live rewrite record survives"
    );
    for p in &rw_parts {
        assert!(
            present(&store, p).await,
            "live rewrite output part survives"
        );
    }
}

// --- Deliverable 2: .dreq removal (rule 6) ----------------------------------

/// Seed a `.dreq` object (its body is never decoded by the sweep, only the key
/// is parsed) and optionally a `.done` completion whose `completed_unix_ns` is
/// `completed`. Returns the `.dreq` and `.done` keys.
async fn seed_dreq_done(
    store: &dyn ObjectStoreBackend,
    seed: u128,
    completed: Option<i64>,
) -> (String, Option<String>) {
    let request_id = Uuid::from_u128(seed);
    let dreq_key = keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id).unwrap();
    let request = pending_request(seed, "victim").request;
    store
        .put(
            &dreq_key,
            erasure::encode_request(&request),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let done_key = if let Some(completed_unix_ns) = completed {
        let completion = ErasureCompletion {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            request_id: request_id.to_string(),
            predicate_hash: vec![0x11; 32],
            bucket_drops: vec![],
            requested_unix_ns: 0,
            completed_unix_ns,
            deferral_cause: 0,
        };
        let key =
            keys::erasure_completion_key(&tenant_hash(), Signal::Metrics, request_id).unwrap();
        store
            .put(
                &key,
                erasure::encode_completion(&completion),
                PutOptions::default(),
            )
            .await
            .unwrap();
        Some(key)
    } else {
        None
    };
    (dreq_key, done_key)
}

async fn sweep_dreq(
    store: &dyn ObjectStoreBackend,
    now: i64,
    lease: &dyn LeaseCheck,
) -> ravel_maintain::ErasureRequestSweepOutcome {
    sweep_erasure_requests(
        store,
        &FixedClock::new(now),
        &cfg(),
        lease,
        &tenant_hash(),
        Signal::Metrics,
    )
    .await
    .expect("sweep_erasure_requests")
}

/// A `.dreq` with no `.done` is never removed: the erasure is not verified
/// complete, so its query-time exclusion filter must stay live.
///
/// Flip-line proof: making rule 6 delete a `.dreq` whose `done_completed_ns`
/// lookup returns `None` (treating "no completion" as removable) deletes the
/// request here, failing the `deleted == 0` assertion.
#[tokio::test]
async fn dreq_without_done_is_never_removed() {
    let store = MemoryStore::new();
    let (dreq_key, _) = seed_dreq_done(&store, 1, None).await;
    let out = sweep_dreq(&store, i64::MAX / 2, &NoLeases).await;
    assert_eq!(out.deleted, 0, "no .done -> never removed");
    assert_eq!(out.kept, 1);
    assert!(present(&store, &dreq_key).await, ".dreq still present");
}

/// A `.dreq` is removed exactly at `done.completed_unix_ns + protection_horizon`
/// and NOT a nanosecond earlier, and the `.done` is never deleted.
///
/// Flip-line proof: changing rule 6's gate from `now < completed + horizon` to
/// `now <= completed + horizon` (an off-by-one) would delete the `.dreq` one
/// nanosecond early, failing the `deleted == 0` assertion at the boundary
/// minus one.
#[tokio::test]
async fn dreq_removed_at_horizon_boundary_not_a_nanosecond_early() {
    let completed = sealed_now_ns();
    let horizon = cfg().protection_horizon_ns;
    let boundary = completed + horizon;

    // One nanosecond early: kept.
    let store = MemoryStore::new();
    let (dreq_key, done_key) = seed_dreq_done(&store, 2, Some(completed)).await;
    let done_key = done_key.unwrap();
    let out = sweep_dreq(&store, boundary - 1, &NoLeases).await;
    assert_eq!(
        out.deleted, 0,
        "not removed a nanosecond before the horizon"
    );
    assert!(
        present(&store, &dreq_key).await,
        ".dreq survives pre-boundary"
    );

    // Exactly at the boundary: removed; the .done is permanent.
    let out = sweep_dreq(&store, boundary, &NoLeases).await;
    assert_eq!(out.deleted, 1, "removed exactly at the horizon boundary");
    assert!(!present(&store, &dreq_key).await, ".dreq physically gone");
    assert!(
        present(&store, &done_key).await,
        ".done is permanent, never swept"
    );
}

/// A legal hold over the `del/` keyspace blocks `.dreq` removal even past the
/// horizon; the hold is consulted.
///
/// Flip-line proof: dropping the `lease.is_protected(dreq_key)` guard in rule 6
/// deletes the held `.dreq`, failing the `deleted == 0` assertion.
#[tokio::test]
async fn held_dreq_survives_past_horizon() {
    let completed = sealed_now_ns();
    let store = MemoryStore::new();
    let (dreq_key, _) = seed_dreq_done(&store, 3, Some(completed)).await;

    let hold = HoldPrefixSync {
        prefix: keys::del_prefix(&tenant_hash(), Signal::Metrics),
        consulted: std::sync::atomic::AtomicUsize::new(0),
    };
    let out = sweep_dreq(&store, past_horizon(completed), &hold).await;
    assert_eq!(out.deleted, 0, "held .dreq survives past the horizon");
    assert_eq!(out.kept, 1);
    assert!(present(&store, &dreq_key).await);
    assert!(
        hold.consulted.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the legal-hold check was actually consulted"
    );
}

/// DreqSweep / DreqSweepRespectsLegalHold (lifecycle traceability row 27): a
/// `.dreq` past its own horizon, with a valid `.done`, and never itself
/// protected by any hold, is still kept when the rewrite's own superseded
/// input chain is legally held -- distinct from [`held_dreq_survives_past_horizon`]
/// above, where the hold covers the `del/` key directly. Here the hold covers
/// only the shard's data/commit/L1 prefixes ([`shard_hold_scopes`]), which
/// never overlap `del/`, so `lease.is_protected(dreq_key)` is false and the
/// only path that can keep the request is rule 6 observing rule 2's own
/// chain-level hold (`chain_groups_held_by_legal_hold`) through
/// `held_by_superseded_inputs`.
///
/// Flip-line proof: in `sweep_erasure_requests_inner`, replace `let held =
/// holds.request_ids.contains(&request_id_s) || !holds.truncated_buckets.is_empty();`
/// with `let held = false;`: the held case then deletes the `.dreq` exactly
/// like the control, failing `assert_eq!(held_out.deleted, 0, ...)` below.
#[tokio::test]
async fn dreq_sweep_keeps_request_markers_whose_chain_is_legally_held() {
    let created = sealed_now_ns();
    let b = bucket();

    // Control: no hold anywhere. The rewrite's superseded chain is freely
    // clearable, so rule 6 deletes the completed, past-horizon `.dreq`.
    let control = MemoryStore::new();
    let clock = FixedClock::new(created);
    for spec in metrics_specs() {
        seed_input(&control, &spec).await;
    }
    run_rewrite(&control, &clock).await;
    let (control_dreq_key, _) = seed_dreq_done(&control, 42, Some(created)).await;
    let control_out = sweep_dreq(&control, past_horizon(created), &NoLeases).await;
    assert_eq!(
        control_out.deleted, 1,
        "control: nothing holds the chain, the marker is removed"
    );
    assert_eq!(control_out.held_by_superseded_inputs, 0);
    assert!(!present(&control, &control_dreq_key).await);

    // Held: a legal hold over the shard's real key prefixes (not `del/`)
    // holds the whole supersession chain the rewrite produced. The `.dreq`'s
    // own key is untouched by the hold, its `.done` is valid, and it is well
    // past its horizon -- every rule-6 gate other than the chain-hold
    // observation says "delete this."
    let held = MemoryStore::new();
    let clock = FixedClock::new(created);
    for spec in metrics_specs() {
        seed_input(&held, &spec).await;
    }
    run_rewrite(&held, &clock).await;
    let (held_dreq_key, _) = seed_dreq_done(&held, 42, Some(created)).await;
    for (i, scope) in shard_hold_scopes(&b.tenant_hash, b.signal, b.shard)
        .unwrap()
        .iter()
        .enumerate()
    {
        write_hold_set(
            &held,
            &b.tenant_hash,
            Uuid::from_u128(300 + i as u128),
            created,
            scope,
            "litigation hold",
        )
        .await
        .unwrap();
    }
    let lease = LegalHoldCheck::refresh(&held, &b.tenant_hash)
        .await
        .unwrap();
    assert!(
        !lease.is_protected(&held_dreq_key),
        "shard_hold_scopes never covers del/, so the dreq key itself is not held"
    );

    let held_out = sweep_dreq(&held, past_horizon(created), &lease).await;
    assert_eq!(
        held_out.deleted, 0,
        "the held chain keeps the marker even though the dreq key itself is unheld"
    );
    assert_eq!(
        held_out.held_by_superseded_inputs, 1,
        "held specifically via the chain observation, not the dreq-key-held path"
    );
    assert!(present(&held, &held_dreq_key).await, ".dreq survives");
}

/// A completion with a zero `completed_unix_ns` is a fail-safe keep: a zero
/// anchor would collapse the horizon gate to always-past.
///
/// Flip-line proof: removing the `completed_ns == 0` fail-safe guard in rule 6
/// makes `now < 0 + horizon` false for any large `now`, deleting the request
/// and failing the `deleted == 0` assertion.
#[tokio::test]
async fn zero_completion_timestamp_keeps_dreq() {
    let store = MemoryStore::new();
    let (dreq_key, _) = seed_dreq_done(&store, 4, Some(0)).await;
    let out = sweep_dreq(&store, i64::MAX / 2, &NoLeases).await;
    assert_eq!(out.deleted, 0, "zero completion anchor is not a valid gate");
    assert!(present(&store, &dreq_key).await);
}

/// CompleteErasure / CompletionRespectsLegalHold (lifecycle traceability row
/// 25): a legal hold covering the bucket blocks `bucket_erasure_completion`
/// unconditionally, checked before the served-set read. Isolated from the
/// served-set branch: the seeded input's event-time window does not overlap
/// the pending request's window at all, so without a hold the served-set
/// check alone already reports the bucket complete (`blocked` empty,
/// `unresolved` false); the flip to `unresolved: true` under the hold is
/// caused solely by the legal-hold gate, not by anything the served-set
/// branch would have found.
///
/// Flip-line proof: in `bucket_erasure_completion`, delete the `if
/// bucket_is_held(&listing, lease) { out.unresolved = true; return Ok(out); }`
/// block. Completion then falls through to the served-set check, which (given
/// the non-overlapping window) reports `unresolved: false`, failing
/// `assert!(held.unresolved, ...)` below.
#[tokio::test]
async fn erasure_completion_refuses_while_a_legal_hold_covers_the_bucket() {
    let store = MemoryStore::new();
    let spec = InputSpec::new(
        Uuid::from_u128(0x51),
        1,
        1,
        vec![raw_series(
            "victim",
            &[("k", "a")],
            &[(1_000, 1.0), (2_000, 2.0)],
        )],
    );
    seed_input(&store, &spec).await;

    let request_id = Uuid::from_u128(0x52);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 0,
        predicate: vec![ErasurePredicateMatcher {
            key: "__name__".to_string(),
            value: "victim".to_string(),
        }],
        // Strictly after the seeded input's [1_000, 2_000] event-time range,
        // so `bucket_may_overlap` is false regardless of the hold.
        window_start_ns: 1_000_000,
        window_end_ns: 2_000_000,
        reason: String::new(),
    };
    let pending = vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request,
    }];

    let clock = FixedClock::new(sealed_now_ns());
    let config = cfg();

    // Control: with no hold, the non-overlapping window alone already means
    // the served-set check reports the bucket complete.
    let control =
        bucket_erasure_completion(&store, &clock, &config, &NoLeases, &bucket(), &pending)
            .await
            .expect("completion without hold");
    assert!(
        control.blocked.is_empty(),
        "the request window does not overlap the seeded input"
    );
    assert!(
        !control.unresolved,
        "control: the served-set check alone would allow completion"
    );

    let scopes = shard_hold_scopes(&tenant_hash(), Signal::Metrics, SHARD).expect("shard scopes");
    for (i, scope) in scopes.iter().enumerate() {
        write_hold_set(
            &store,
            &tenant_hash(),
            Uuid::from_u128(200 + i as u128),
            0,
            scope,
            "litigation hold",
        )
        .await
        .expect("set hold");
    }
    let lease = LegalHoldCheck::refresh(&store, &tenant_hash())
        .await
        .expect("refresh");

    let held = bucket_erasure_completion(&store, &clock, &config, &lease, &bucket(), &pending)
        .await
        .expect("completion under hold");
    assert!(
        held.blocked.is_empty(),
        "the hold gate returns before the served-set check ever populates blocked"
    );
    assert!(
        held.unresolved,
        "a bucket under legal hold stays unresolved regardless of the served-set outcome"
    );
}

/// A retention config whose window for the test tenant is exactly the floor,
/// so the tiny-timestamp fixtures (events at ts 1_000-3_000, `now` at
/// [`sealed_now_ns`]) are always expired.
fn retention_at_floor(config: &CompactorConfig) -> RetentionConfig {
    let floor = config.retention_floor_ns(DEFAULT_MAX_INGEST_LAG_NS);
    RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: vec![(TENANT.to_string(), floor)],
        },
        config,
        DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("valid retention config")
}

/// PerformRewrite / tombstone guard (lifecycle traceability row 29): once a
/// bucket carries a retention tombstone, `erasure_rewrite_bucket` refuses to
/// run at all -- checked against a fresh listing at rewrite time, not a
/// cached decision. Isolated from the other refusal outcomes: the control
/// runs the identical seeded bucket and pending request with no tombstone and
/// gets `Rewritten`, so the flip to `Tombstoned` is caused solely by the
/// tombstone-listing check, not by `NotSealed`/`NoApplicableRequests`/`Held`.
///
/// Flip-line proof: in `erasure_rewrite_bucket`, delete the `if
/// listing.tombstone_key.is_some() { return Ok(ErasureRewriteOutcome::Tombstoned); }`
/// block. The tombstoned case then falls through to the live-input resolution
/// and rewrites the (physically already-swept) bucket instead of refusing,
/// failing the `matches!(outcome, ErasureRewriteOutcome::Tombstoned)` assertion
/// below.
#[tokio::test]
async fn erasure_rewrite_refuses_a_tombstoned_bucket_with_the_tombstoned_outcome() {
    let created = sealed_now_ns();
    let config = cfg();

    // Control: the same seeded bucket and pending request, no tombstone. The
    // rewrite proceeds normally, proving the fixture is otherwise rewritable.
    let control = MemoryStore::new();
    let clock = FixedClock::new(created);
    for spec in metrics_specs() {
        seed_input(&control, &spec).await;
    }
    let control_outcome = run_rewrite(&control, &clock).await;
    assert!(
        matches!(control_outcome, ErasureRewriteOutcome::Rewritten { .. }),
        "control: an untombstoned bucket rewrites normally, got {control_outcome:?}"
    );

    // Tombstoned: retention expires and tombstones the bucket before the
    // rewrite pass ever runs.
    let store = MemoryStore::new();
    for spec in metrics_specs() {
        seed_input(&store, &spec).await;
    }
    let b = bucket();
    let retention = retention_at_floor(&config);
    let retention_outcome =
        retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &b)
            .await
            .expect("retention pass");
    assert_eq!(
        retention_outcome,
        RetentionOutcome::Tombstoned,
        "the tiny-timestamp fixture is expired under the floor window"
    );

    let mut memo = MaintainMemo::with_default_interval();
    let outcome = erasure_rewrite_bucket(
        &store,
        &clock,
        &config,
        &NoLeases,
        &b,
        &[pending_request(42, "victim")],
        &mut memo,
    )
    .await
    .expect("rewrite over a tombstoned bucket");
    assert!(
        matches!(outcome, ErasureRewriteOutcome::Tombstoned),
        "a tombstoned bucket refuses the rewrite entirely, got {outcome:?}"
    );
}

// --- Issue #1290: completion waits for a bucket open at acknowledgement -----
//
// `run_erasure_pass` (the driver that turns a `BucketErasureCompletion` into a
// `.done`) lives in `services/ravel-server/src/maintain.rs`, which is private
// to that binary. `erasure_tick` below mirrors it against the same public
// ravel-maintain entry points the server calls, in the same order, so these
// tests drive the changed gate end to end (pending -> `.done`) from this
// crate's own test suite.

/// The instant these tests acknowledge their erasure request: half an hour
/// into [`HOUR`], so [`bucket`] is the request's own still-open ingest hour.
fn ack_inside_hour() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR + 1_800_000_000_000
}

/// The instant [`bucket`] seals: its end plus the seal margin
/// (`max_flush_lifetime + clock_skew_allowance`).
fn hour_seals_at() -> i64 {
    bucket().end_ns() + cfg().seal_margin_ns()
}

/// Seed a durable `.dreq` acknowledged at `created_unix_ns` and return the
/// pending request it decodes back to, so a test can drive both the direct
/// per-bucket calls and [`erasure_tick`] (which re-lists the `.dreq`s itself,
/// exactly as the server does).
async fn seed_dreq_at(
    store: &dyn ObjectStoreBackend,
    seed: u128,
    metric: &str,
    created_unix_ns: i64,
) -> PendingErasureRequest {
    let pending = pending_request_at(seed, metric, created_unix_ns);
    store
        .put(
            &pending.request_key,
            erasure::encode_request(&pending.request),
            PutOptions::default(),
        )
        .await
        .expect("put .dreq");
    pending
}

/// What one mirrored maintenance tick observed.
#[derive(Debug, Default)]
struct ErasureTick {
    /// The union of every bucket's `blocked` set.
    blocked: HashSet<String>,
    /// Buckets the rewrite pass left for a later pass because they are still
    /// open (`ErasureRewriteOutcome::NotSealed`).
    not_sealed: usize,
    /// A bucket in scope was not brought up to date, so no `.done` is written.
    deferred: bool,
    /// `.done` records this tick actually created.
    done_written: usize,
}

/// The ingest hours a scan discovers for `shard`, mirroring the server's
/// `list_erasure_scan_hours` (a delimited LIST over the commit-shard prefix).
async fn scan_hours(store: &dyn ObjectStoreBackend, shard: u32) -> Vec<u32> {
    let prefix =
        keys::commit_shard_prefix(&tenant_hash(), Signal::Metrics, shard).expect("shard prefix");
    let listed = store.list_delimited(&prefix).await.expect("list delimited");
    let mut hours: Vec<u32> = listed
        .common_prefixes
        .iter()
        .map(|common| {
            let rest = common
                .strip_prefix(&prefix)
                .and_then(|r| r.strip_suffix('/'))
                .unwrap_or("");
            keys::parse_ingest_hour_string(rest).expect("hour segment")
        })
        .collect();
    hours.sort_unstable();
    hours
}

/// Write one request's `.done`, mirroring the server's
/// `write_erasure_completion`: `CreateIfAbsent`, `completed_unix_ns` clamped
/// forward to the request time, and validated before it reaches the store.
/// Returns whether this call created the record.
async fn write_done(store: &dyn ObjectStoreBackend, now_ns: i64, request: &ErasureRequest) -> bool {
    let completion = ErasureCompletion {
        format_version: erasure::FORMAT_VERSION,
        tenant_hash: request.tenant_hash.clone(),
        signal: request.signal,
        request_id: request.request_id.clone(),
        // The server derives this from the predicate so the permanent record
        // carries no plaintext; nothing in the completion gate reads it.
        predicate_hash: vec![0x11; 32],
        bucket_drops: vec![],
        requested_unix_ns: request.created_unix_ns,
        completed_unix_ns: now_ns.max(request.created_unix_ns),
        deferral_cause: 0,
    };
    erasure::validate_completion(&completion).expect("valid completion");
    let key = keys::erasure_completion_key_for(&completion).expect("done key");
    match store
        .put(
            &key,
            erasure::encode_completion(&completion),
            PutOptions::create_if_absent(),
        )
        .await
    {
        Ok(_) => true,
        Err(ravel_object_store::StoreError::AlreadyExists) => false,
        Err(err) => panic!("put .done: {err}"),
    }
}

/// One `(tenant, metrics)` erasure pass at `now_ns`, mirroring the server's
/// `run_erasure_pass`: list the pending `.dreq`s, rewrite every discovered
/// bucket, run the catalog completion gate over every bucket regardless of the
/// rewrite outcome, then write a `.done` for each request no bucket blocked --
/// and only when nothing deferred.
///
/// Unlike the server this panics on a store or rewrite error instead of
/// deferring: against a `MemoryStore` oracle an error is a bug in the test
/// fixture, and the server's defer-on-error path is covered by its own tests.
async fn erasure_tick(store: &dyn ObjectStoreBackend, now_ns: i64) -> ErasureTick {
    let clock = FixedClock::new(now_ns);
    let config = cfg();
    let mut memo = MaintainMemo::with_default_interval();
    let mut tick = ErasureTick::default();

    let pending = pending_erasure_requests(store, &tenant_hash(), Signal::Metrics)
        .await
        .expect("list pending requests");
    if pending.is_empty() {
        return tick;
    }

    for hour in scan_hours(store, SHARD).await {
        let b = bucket_at(hour);
        match erasure_rewrite_bucket(store, &clock, &config, &NoLeases, &b, &pending, &mut memo)
            .await
            .expect("rewrite bucket")
        {
            ErasureRewriteOutcome::Rewritten { publish, .. } => {
                if matches!(publish, PublishOutcome::Abandoned) {
                    tick.deferred = true;
                }
            }
            ErasureRewriteOutcome::NotSealed => tick.not_sealed += 1,
            ErasureRewriteOutcome::Held => tick.deferred = true,
            ErasureRewriteOutcome::AlreadyApplied
            | ErasureRewriteOutcome::NoApplicableRequests
            | ErasureRewriteOutcome::Tombstoned => {}
        }

        let completion = bucket_erasure_completion(store, &clock, &config, &NoLeases, &b, &pending)
            .await
            .expect("completion gate");
        if completion.unresolved {
            tick.deferred = true;
        }
        tick.blocked.extend(completion.blocked);
    }

    if tick.deferred {
        return tick;
    }
    for entry in &pending {
        if tick.blocked.contains(&entry.request.request_id) {
            continue;
        }
        if write_done(store, now_ns, &entry.request).await {
            tick.done_written += 1;
        }
    }
    tick
}

/// Every object under the tenant's erasure prefix (`.dreq`s and `.done`s).
async fn del_object_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let prefix = keys::del_prefix(&tenant_hash(), Signal::Metrics);
    let mut keys: Vec<String> = list_all(store, &prefix)
        .await
        .expect("list erasure prefix")
        .into_iter()
        .map(|m| m.key)
        .collect();
    keys.sort();
    keys
}

async fn done_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    del_object_keys(store)
        .await
        .into_iter()
        .filter(|k| k.ends_with(".done"))
        .collect()
}

/// The single `.done` record in the store, decoded.
async fn only_completion(store: &dyn ObjectStoreBackend) -> ErasureCompletion {
    let keys = done_keys(store).await;
    assert_eq!(keys.len(), 1, "expected exactly one .done, got {keys:?}");
    erasure::decode_completion(&get_full(store, &keys[0]).await).expect("decode .done")
}

/// Every series and sample an object serves, keyed by series id.
async fn object_samples(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> BTreeMap<[u8; 16], Vec<(i64, u64)>> {
    let mut out = read_scalar_samples(&get_full(store, key).await);
    for v in out.values_mut() {
        v.sort();
    }
    out
}

/// The `(commit records, rewrite records)` a bucket's listing holds.
async fn bucket_record_keys(store: &dyn ObjectStoreBackend, b: &Bucket) -> (Vec<String>, usize) {
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .expect("bucket prefix");
    let mut commits = Vec::new();
    let mut rewrites = 0usize;
    for meta in list_all(store, &prefix).await.expect("list bucket") {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(_)) => commits.push(meta.key),
            Ok(BucketEntry::RewriteRecord(_)) => rewrites += 1,
            _ => {}
        }
    }
    commits.sort();
    (commits, rewrites)
}

/// A request acknowledged while its own ingest hour is still open does NOT
/// complete before that hour seals: the rewrite pass defers the open bucket to
/// a later pass, and completion honors the deferral instead of overtaking it.
/// Without this, the `.done` lands, the request stops being pending, no later
/// pass ever rewrites the hour, and the subject's records are served again the
/// moment the query-time filter retires (issue #1290).
///
/// Flip-line proof: in `bucket_erasure_completion`, restore the bare early
/// return in the unsealed branch (`if !bucket.is_sealed(clock.now_ns(),
/// config) { return Ok(out); }`, dropping the `bucket_in_scope_at_ack` loop
/// that inserts into `out.blocked`). The open bucket then blocks nothing, the
/// tick writes the `.done` at the acknowledgement instant, and the
/// `tick.blocked` and `done_keys` assertions below fail.
#[tokio::test]
async fn completion_waits_for_a_bucket_still_open_at_acknowledgement() {
    let store = MemoryStore::new();
    for spec in metrics_specs() {
        seed_input(&store, &spec).await;
    }
    let ack = ack_inside_hour();
    let pending = seed_dreq_at(&store, 0x1290, "victim", ack).await;
    let request_id = pending.request.request_id.clone();

    // The premise: at the acknowledgement the request's own hour is open, so
    // the rewrite pass defers it (ADR-0064 decision 3 point 1).
    let clock = FixedClock::new(ack);
    let mut memo = MaintainMemo::with_default_interval();
    let outcome = erasure_rewrite_bucket(
        &store,
        &clock,
        &cfg(),
        &NoLeases,
        &bucket(),
        std::slice::from_ref(&pending),
        &mut memo,
    )
    .await
    .expect("rewrite at the acknowledgement");
    assert_eq!(
        outcome,
        ErasureRewriteOutcome::NotSealed,
        "the request's own ingest hour is still open at the acknowledgement"
    );

    let tick = erasure_tick(&store, ack).await;
    assert_eq!(
        tick.not_sealed, 1,
        "exactly one discovered bucket, and it is unsealed"
    );
    assert_eq!(
        tick.blocked,
        HashSet::from([request_id.clone()]),
        "the open bucket blocks exactly the request whose scope covered it at the ack"
    );
    assert_eq!(tick.done_written, 0, "no completion is written this tick");
    assert_eq!(
        done_keys(&store).await,
        Vec::<String>::new(),
        "no .done object exists"
    );
    assert_eq!(
        del_object_keys(&store).await,
        vec![pending.request_key.clone()],
        "the erasure prefix holds the .dreq and nothing else"
    );
    assert_eq!(
        pending_erasure_requests(&store, &tenant_hash(), Signal::Metrics)
            .await
            .expect("list pending")
            .len(),
        1,
        "the request is still pending, so a later pass revisits it"
    );
}

/// Once the hour that was open at the acknowledgement seals, a later pass
/// rewrites it and the request completes: exactly one `.done`, and the bucket
/// serves exactly the surviving records.
///
/// Flip-line proof: same flipped line as
/// [`completion_waits_for_a_bucket_still_open_at_acknowledgement`] -- with the
/// bare early return restored, the pre-seal tick writes the `.done`, so the
/// `done_keys` assertion before the seal fails (and the request is no longer
/// pending when the hour seals, which is the resurrection this test's second
/// half pins shut).
#[tokio::test]
async fn completion_lands_once_the_open_bucket_seals_and_is_rewritten() {
    let store = MemoryStore::new();
    for spec in metrics_specs() {
        seed_input(&store, &spec).await;
    }
    let ack = ack_inside_hour();
    let pending = seed_dreq_at(&store, 0x1291, "victim", ack).await;
    let request_id = pending.request.request_id.clone();
    let seal_ns = hour_seals_at();

    // One nanosecond before the seal: still deferred, still no completion.
    let early = erasure_tick(&store, seal_ns - 1).await;
    assert_eq!(
        early.blocked,
        HashSet::from([request_id.clone()]),
        "a nanosecond before the seal the bucket is still open"
    );
    assert_eq!(
        done_keys(&store).await,
        Vec::<String>::new(),
        "no .done a nanosecond before the seal"
    );
    assert_eq!(
        del_object_keys(&store).await,
        vec![pending.request_key.clone()],
        "the .dreq alone before the seal"
    );

    // At the seal: the rewrite lands and the request completes.
    let sealed = erasure_tick(&store, seal_ns).await;
    assert!(
        sealed.blocked.is_empty(),
        "a sealed and rewritten bucket blocks nothing, got {:?}",
        sealed.blocked
    );
    assert_eq!(sealed.done_written, 1, "the request completes at the seal");
    let done_key = keys::erasure_completion_key(
        &tenant_hash(),
        Signal::Metrics,
        Uuid::parse_str(&request_id).expect("request uuid"),
    )
    .expect("done key");
    assert_eq!(
        done_keys(&store).await,
        vec![done_key.clone()],
        "exactly one .done, for exactly this request"
    );
    let mut expected_prefix_keys = vec![done_key.clone(), pending.request_key.clone()];
    expected_prefix_keys.sort();
    assert_eq!(
        del_object_keys(&store).await,
        expected_prefix_keys,
        "the erasure prefix holds the .dreq and its .done, nothing more"
    );

    // The rewritten bucket serves exactly the survivors, sample for sample.
    let rewrite_key = find_record_key(&store, true).await;
    let parts = rewrite_part_keys(&store, &rewrite_key).await;
    assert_eq!(parts.len(), 1, "one rewrite output part, got {parts:?}");
    let served = object_samples(&store, &parts[0]).await;
    let keep = raw_series("keep", &[("k", "a")], &[]);
    let victim = raw_series("victim", &[("k", "b")], &[]);
    assert_eq!(
        served,
        BTreeMap::from([(
            keep.0.0,
            vec![(1_000, 1.0f64.to_bits()), (2_000, 2.0f64.to_bits())]
        )]),
        "the rewrite output serves exactly the surviving records"
    );
    assert!(
        !served.contains_key(&victim.0.0),
        "the erased subject is gone from the hour that was open at the ack"
    );

    // A later pass is a no-op: the request is complete, not re-completed.
    let again = erasure_tick(&store, seal_ns + NS_PER_HOUR).await;
    assert_eq!(
        again.done_written, 0,
        "a completed request is no longer pending"
    );
    assert_eq!(
        done_keys(&store).await,
        vec![done_key],
        "still exactly one .done"
    );
}

/// Records ingested AFTER the acknowledgement are out of scope: they are not
/// erased, and they do not hold completion open. This is what bounds the wait
/// -- a continuously-ingesting tenant completes, because each new ingest hour
/// opened after the ack, not before it.
///
/// Flip-line proof: make `bucket_in_scope_at_ack` return `true`
/// unconditionally (drop the `bucket.start_ns() <= created + skew` test). The
/// still-open next hour then blocks the request forever, so `done_keys` is
/// empty and `assert_eq!(tick.done_written, 1)` below fails.
#[tokio::test]
async fn records_ingested_after_the_acknowledgement_neither_erase_nor_hold_completion() {
    let store = MemoryStore::new();
    for spec in metrics_specs() {
        seed_input(&store, &spec).await;
    }
    let ack = ack_inside_hour();
    let pending = seed_dreq_at(&store, 0x1292, "victim", ack).await;

    // Post-ack ingest: a predicate-matching record lands in the NEXT ingest
    // hour, which opened after the ack.
    let post_ack = InputSpec::new_at(
        HOUR + 1,
        Uuid::from_u128(0x1292),
        11,
        1,
        vec![raw_series("victim", &[("k", "b")], &[(4_000, 4.0)])],
    );
    seed_input(&store, &post_ack).await;
    let later = bucket_at(HOUR + 1);

    let seal_ns = hour_seals_at();
    assert!(
        !later.is_sealed(seal_ns, &cfg()),
        "the post-ack hour is still open when the acked hour seals, so this \
         test exercises the unsealed branch"
    );
    let completion = bucket_erasure_completion(
        &store,
        &FixedClock::new(seal_ns),
        &cfg(),
        &NoLeases,
        &later,
        std::slice::from_ref(&pending),
    )
    .await
    .expect("completion gate over the post-ack bucket");
    assert!(
        completion.blocked.is_empty(),
        "a bucket whose hour opened after the ack holds no in-scope records, \
         so it blocks nothing, got {:?}",
        completion.blocked
    );

    let tick = erasure_tick(&store, seal_ns).await;
    assert!(
        tick.blocked.is_empty(),
        "nothing in scope is left unrewritten, got {:?}",
        tick.blocked
    );
    assert_eq!(
        tick.not_sealed, 1,
        "exactly one unsealed bucket: the post-ack hour"
    );
    assert_eq!(
        tick.done_written, 1,
        "the post-ack hour does not hold completion open"
    );
    assert_eq!(done_keys(&store).await.len(), 1, "exactly one .done");

    // The post-ack record itself is untouched.
    let (commits, rewrites) = bucket_record_keys(&store, &later).await;
    assert_eq!(
        commits.len(),
        1,
        "the post-ack bucket holds exactly its one commit record, got {commits:?}"
    );
    assert_eq!(rewrites, 0, "no rewrite record touched the post-ack bucket");
    let record = record::decode(&get_full(&store, &commits[0]).await).expect("decode commit");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    let victim = raw_series("victim", &[("k", "b")], &[]);
    assert_eq!(
        object_samples(&store, &data_key).await,
        BTreeMap::from([(victim.0.0, vec![(4_000, 4.0f64.to_bits())])]),
        "the post-ack record survives the erasure sample for sample"
    );
}

/// The wait the fix introduces is bounded, and the bound sits far inside the
/// documented `erasure_rewrite_deadline`, so a request waiting for its open
/// hour to seal is never a stuck request: it is not alarming, it is working.
/// The bound composes the same terms as the ADR-0019 §5 retention floor
/// (`max_ingest_lag + one bucket span + seal margin`), which is why it is
/// asserted equal to it rather than restated as a literal.
///
/// Flip-line proof: with the bare early return restored in
/// `bucket_erasure_completion`'s unsealed branch, the request completes at the
/// acknowledgement tick, so the `done_keys` assertion before the seal fails
/// and the measured wait is never taken.
#[tokio::test]
async fn a_bounded_completion_wait_stays_inside_the_erasure_rewrite_deadline() {
    let store = MemoryStore::new();
    for spec in metrics_specs() {
        seed_input(&store, &spec).await;
    }
    let ack = ack_inside_hour();
    seed_dreq_at(&store, 0x1293, "victim", ack).await;

    let at_ack = erasure_tick(&store, ack).await;
    assert_eq!(at_ack.done_written, 0, "the wait starts at the ack");
    assert_eq!(
        done_keys(&store).await,
        Vec::<String>::new(),
        "no .done at the ack"
    );

    let seal_ns = hour_seals_at();
    let at_seal = erasure_tick(&store, seal_ns).await;
    assert_eq!(at_seal.done_written, 1, "the wait ends at the seal");

    // The wait this fixture actually drove, read back off the durable record.
    let completion = only_completion(&store).await;
    assert_eq!(
        completion.requested_unix_ns, ack,
        "the .done anchors on the acknowledgement"
    );
    let observed = completion.completed_unix_ns - completion.requested_unix_ns;
    assert_eq!(
        observed, 5_700_000_000_000,
        "1h35m: the 30 minutes left of the open hour plus the 1h5m seal margin"
    );

    let bound = erasure_seal_wait_bound_ns(&cfg(), DEFAULT_MAX_INGEST_LAG_NS);
    assert_eq!(
        bound,
        4 * NS_PER_HOUR + 300_000_000_000,
        "4h05m with default config: 2h max ingest lag + 1h bucket span + 1h5m seal margin"
    );
    assert_eq!(
        bound,
        cfg().retention_floor_ns(DEFAULT_MAX_INGEST_LAG_NS),
        "the wait bound composes the same terms as the ADR-0019 section 5 retention floor"
    );
    assert!(
        observed <= bound,
        "the driven wait {observed} exceeds the bound {bound}"
    );

    assert_eq!(
        ERASURE_REWRITE_DEADLINE_NS,
        72 * NS_PER_HOUR,
        "the documented erasure_rewrite_deadline is 72h"
    );
    assert!(
        bound < ERASURE_REWRITE_DEADLINE_NS,
        "the worst-case seal wait {bound} must stay inside the {ERASURE_REWRITE_DEADLINE_NS} \
         deadline, or a request that is merely waiting would alarm as stuck"
    );
}
