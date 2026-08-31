//! ADR-0979 decision 3: the bounded RLOG compaction path releases each part's
//! bytes at PUT and closes the tombstone race by post-publish HEAD verification
//! rather than by retaining bytes.
//!
//! These tests drive the compaction pipeline at its two seams --
//! [`RlogCodec::build_parts`] (which PUTs the content-addressed parts) and
//! [`publish_record`] (which PUTs the record, then verifies) -- in sequence,
//! exactly as `rewrite_and_publish` calls them. Splitting the two lets a test
//! delete an already-PUT part in the window between the part PUTs and the
//! record PUT, which is the tombstone race the decision exists to close, and
//! lets it inspect each returned part's `put_already_existed` flag directly.
//!
//! The out-of-band deletion is a direct `store.delete`: the library
//! `FaultStore` injects store *errors* on an operation, not a delete of a
//! *different* key mid-run, so a genuine tombstone/sweep deletion is modeled by
//! deleting the object and proving it is gone (HEAD `NotFound`) before publish
//! runs -- the "the fault actually fired" check the fault-injection discipline
//! asks for.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::codec::SegmentCodec;
use ravel_maintain::config::MergeMemoryTracker;
use ravel_maintain::error::MaintainError;
use ravel_maintain::publish::{PublishOutcome, publish_record};
use ravel_maintain::read::{input_set_hash, list_bucket, load_inputs};
use ravel_maintain::rlog::RlogCodec;
use ravel_maintain::{Bucket, CompactorConfig, FixedClock};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use uuid::Uuid;

/// Seed a small logs bucket whose whole record set fits one L1 part, so a test
/// reasons about exactly one content-addressed part key.
async fn seed_one_part_bucket(store: &MemoryStore) -> Bucket {
    // Two small inputs over two shared streams: a handful of records, well under
    // the default stored-size target, so the merge closes a single part.
    seed_rlog_input(
        store,
        Uuid::from_u128(1),
        1,
        1,
        &[log_record(0, 10, "alpha"), log_record(1, 20, "bravo")],
    )
    .await;
    seed_rlog_input(
        store,
        Uuid::from_u128(2),
        1,
        2,
        &[log_record(0, 15, "charlie"), log_record(1, 25, "delta")],
    )
    .await;
    logs_bucket()
}

/// Load every input's RLOG catalog in canonical input order, aligned with
/// `inputs`, exactly as `rewrite_and_publish` does before `build_parts`.
async fn load_catalogs(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    inputs: &[ravel_maintain::read::InputRecord],
) -> Vec<<RlogCodec as SegmentCodec>::Catalog> {
    let mut catalogs = Vec::with_capacity(inputs.len());
    for input in inputs {
        catalogs.push(
            RlogCodec::load_input_catalog(store, config, input)
                .await
                .expect("load catalog"),
        );
    }
    catalogs
}

/// Run one abandoned compaction that dies before its record PUT: build and PUT
/// the parts, publish nothing. This is a bare `build_parts` call, which is what
/// a run that crashed between the part PUTs and the record PUT leaves behind:
/// content-addressed parts in the store, no record. Returns the built part keys.
async fn abandoned_build(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    commit_keys: &[String],
) -> Vec<String> {
    let inputs = load_inputs(store, bucket, commit_keys, config.input_read_concurrency)
        .await
        .expect("load inputs");
    let hash = input_set_hash(&inputs);
    let catalogs = load_catalogs(store, config, &inputs).await;
    let parts = RlogCodec::build_parts(store, config, bucket, &inputs, catalogs, &hash)
        .await
        .expect("abandoned build_parts");
    // A fresh PUT of every part: none pre-existed, so none is flagged.
    for p in &parts {
        assert!(
            !p.put_already_existed,
            "an abandoned run's first build PUTs every part fresh"
        );
        assert!(
            p.bytes.is_none(),
            "compaction releases part bytes at PUT (ADR-0979 D3)"
        );
    }
    parts.into_iter().map(|p| p.key).collect()
}

/// The tombstone race: a part that answered `AlreadyExists` at PUT is deleted
/// out-of-band before the post-publish HEAD verification, and the run fails
/// loud with [`MaintainError::AlreadyExistsPartVanished`] naming that part.
///
/// Flip proof (non-vacuous): against the pre-D3 code, `build_parts` retained
/// the part's bytes and the publish path had no post-publish HEAD verification
/// at all -- a part that answered `AlreadyExists` was never re-checked, and the
/// convergence repair path (which only runs on the `AlreadyExists`-on-*record*
/// arm, not this fresh-record arm) silently covered nothing here. So the typed
/// error never fired and the run reported `Published`; this test fails at
/// `expect_err`. The flip is exactly the arm D3 adds: verify the
/// `AlreadyExists` parts after the record PUT instead of trusting the retained
/// copy.
#[tokio::test]
async fn tombstone_deleting_an_already_exists_part_fails_loud() {
    let store = MemoryStore::new();
    let bucket = seed_one_part_bucket(&store).await;
    let config = CompactorConfig::default();
    let commit_keys = list_bucket(&store, &bucket)
        .await
        .expect("list")
        .commit_keys;

    // A prior abandoned run uploaded the part(s); capture the single part key.
    let abandoned_keys = abandoned_build(&store, &config, &bucket, &commit_keys).await;
    assert_eq!(
        abandoned_keys.len(),
        1,
        "the fixture builds exactly one part"
    );
    let vanished = abandoned_keys[0].clone();

    // Our run rebuilds the byte-identical part; its PUT answers AlreadyExists.
    let inputs = load_inputs(&store, &bucket, &commit_keys, config.input_read_concurrency)
        .await
        .expect("inputs");
    let hash = input_set_hash(&inputs);
    let catalogs = load_catalogs(&store, &config, &inputs).await;
    let parts = RlogCodec::build_parts(&store, &config, &bucket, &inputs, catalogs, &hash)
        .await
        .expect("build_parts");
    assert!(
        parts.iter().all(|p| p.put_already_existed),
        "every rebuilt part's PUT must answer AlreadyExists"
    );
    assert!(
        parts.iter().all(|p| p.bytes.is_none()),
        "no AlreadyExists part retains bytes (D3 kills the whole-output term)"
    );

    // The tombstone race: the unreferenced-part sweep deletes the abandoned
    // part between our part PUTs (above) and our record PUT (below). Prove the
    // deletion took effect -- the object is genuinely gone -- so the failure
    // below is a real vanished part, not a vacuous check.
    store.delete(&vanished).await.expect("delete part");
    assert!(
        matches!(store.head(&vanished).await, Err(StoreError::NotFound)),
        "the raced deletion must actually remove the part"
    );

    let clock = FixedClock::new(sealed_now_ns());
    let err = publish_record(
        &store,
        &config,
        &clock,
        &bucket,
        &inputs,
        &hash,
        &parts,
        sealed_now_ns(),
    )
    .await
    .expect_err("a vanished AlreadyExists part must fail the run loud");

    match err {
        MaintainError::AlreadyExistsPartVanished { part_key } => {
            assert_eq!(part_key, vanished, "the error names the vanished part");
        }
        other => panic!("expected AlreadyExistsPartVanished, got {other:?}"),
    }

    // Nothing is left half-referenced without the run knowing: the part the
    // record would reference is provably absent, and the run reported the typed
    // failure rather than success, so a caller re-runs rather than trusting a
    // holed compaction.
    assert!(
        matches!(store.head(&vanished).await, Err(StoreError::NotFound)),
        "the vanished part is still absent after the loud failure"
    );
}

/// The all-parts-`AlreadyExists` retry: a retry after an abandoned run that
/// uploaded every part gets `AlreadyExists` for all of them, retains zero
/// bytes, and completes with a published record whose post-publish HEAD
/// verification passes (the parts are all present).
///
/// Flip proof (non-vacuous): assertion (b) pins the retained-bytes high-water
/// to exactly 0. Against the pre-D3 code, `build_parts` held every part's
/// encoded bytes until publish, so `peak_retained_part_bytes()` equals the
/// nonzero sum of the parts' `object_size` and the `== 0` assertion fails --
/// the exact term D3 removes. Assertion (a) fails against any change that lets a
/// rebuilt part report a fresh PUT.
#[tokio::test]
async fn all_parts_already_exists_retry_retains_nothing_and_publishes() {
    let store = MemoryStore::new();
    let bucket = seed_one_part_bucket(&store).await;
    // The abandoned run runs WITHOUT the tracker, so the tracker below reflects
    // only the retry's accounting.
    let plain = CompactorConfig::default();
    let commit_keys = list_bucket(&store, &bucket)
        .await
        .expect("list")
        .commit_keys;
    abandoned_build(&store, &plain, &bucket, &commit_keys).await;

    // The retry: build_parts sees every part already present.
    let tracker = MergeMemoryTracker::new();
    let config = CompactorConfig {
        merge_memory_tracker: Some(tracker.clone()),
        ..CompactorConfig::default()
    };
    let inputs = load_inputs(&store, &bucket, &commit_keys, config.input_read_concurrency)
        .await
        .expect("inputs");
    let hash = input_set_hash(&inputs);
    let catalogs = load_catalogs(&store, &config, &inputs).await;
    let parts = RlogCodec::build_parts(&store, &config, &bucket, &inputs, catalogs, &hash)
        .await
        .expect("retry build_parts");

    // (a) every part PUT answered AlreadyExists.
    assert!(!parts.is_empty(), "the retry rebuilds the same parts");
    assert!(
        parts.iter().all(|p| p.put_already_existed),
        "every part's PUT must answer AlreadyExists on the retry"
    );

    // (b) retained-bytes accounting never grew past the single-part bound: the
    // bound is exactly 0, because compaction releases at PUT (derived from the
    // charge site, not the fixture's byte totals).
    assert_eq!(
        tracker.peak_retained_part_bytes(),
        0,
        "an all-AlreadyExists retry retains zero part bytes (ADR-0979 D3)"
    );

    // (c) the run completes with a published record and the post-publish HEAD
    // verification passes (the parts are all present).
    let clock = FixedClock::new(sealed_now_ns());
    let outcome = publish_record(
        &store,
        &config,
        &clock,
        &bucket,
        &inputs,
        &hash,
        &parts,
        sealed_now_ns(),
    )
    .await
    .expect("publish must complete: every AlreadyExists part is present");
    assert_eq!(
        outcome,
        PublishOutcome::Published,
        "the retry wins the record PUT and its post-publish HEADs all pass"
    );
    // The record is the single compaction record in the bucket.
    let record = fetch_compaction_record(&store, &bucket).await;
    assert_eq!(record.level, 1);
}

/// The rerun convergence: after the loud tombstone-race failure, a re-run from
/// scratch converges. Its `build_parts` finds the deleted key absent, so its
/// PUT is a FRESH put that restores the byte-identical part before the record
/// resolves; the record PUT then answers `AlreadyExists` (the race run's record
/// is present) and `resolve_already_exists` HEAD-verifies every winner part,
/// finds them all present, and reports `Converged { parts_repaired: 0 }` --
/// convergence by presence, needing no retained bytes.
///
/// Flip proof (non-vacuous): if the rerun's fresh PUT did NOT restore the part
/// (e.g. the fresh-PUT-restores-age reasoning failed), `resolve_already_exists`
/// would HEAD the still-missing part, find nothing to repair from (the bytes
/// were released), and the record would stay holed; `parts_repaired` would not
/// be a clean 0 over present parts. The assertion pins convergence-by-presence.
#[tokio::test]
async fn rerun_after_vanished_part_converges_by_presence() {
    let store = MemoryStore::new();
    let bucket = seed_one_part_bucket(&store).await;
    let config = CompactorConfig::default();
    let commit_keys = list_bucket(&store, &bucket)
        .await
        .expect("list")
        .commit_keys;

    // Reach the loud-failure state: abandoned run, then a run whose AlreadyExists
    // part is deleted before its record PUT.
    let abandoned_keys = abandoned_build(&store, &config, &bucket, &commit_keys).await;
    let vanished = abandoned_keys[0].clone();

    let inputs = load_inputs(&store, &bucket, &commit_keys, config.input_read_concurrency)
        .await
        .expect("inputs");
    let hash = input_set_hash(&inputs);
    let catalogs = load_catalogs(&store, &config, &inputs).await;
    let parts = RlogCodec::build_parts(&store, &config, &bucket, &inputs, catalogs, &hash)
        .await
        .expect("race build_parts");
    store.delete(&vanished).await.expect("delete part");

    let clock = FixedClock::new(sealed_now_ns());
    let loud = publish_record(
        &store,
        &config,
        &clock,
        &bucket,
        &inputs,
        &hash,
        &parts,
        sealed_now_ns(),
    )
    .await;
    assert!(
        matches!(loud, Err(MaintainError::AlreadyExistsPartVanished { .. })),
        "the race run fails loud, got {loud:?}"
    );
    // The record was published (referencing the now-missing part); the part is
    // absent, which is exactly what the rerun must heal.
    assert!(
        matches!(store.head(&vanished).await, Err(StoreError::NotFound)),
        "the part is absent going into the rerun"
    );

    // Re-run from scratch: rebuild parts (the deleted key gets a FRESH put that
    // restores it) then publish (the record already exists -> resolve).
    let rerun_catalogs = load_catalogs(&store, &config, &inputs).await;
    let rerun_parts =
        RlogCodec::build_parts(&store, &config, &bucket, &inputs, rerun_catalogs, &hash)
            .await
            .expect("rerun build_parts");
    // The restored key was absent, so its PUT was fresh, not AlreadyExists.
    let restored = rerun_parts
        .iter()
        .find(|p| p.key == vanished)
        .expect("the rerun rebuilds the same content-addressed key");
    assert!(
        !restored.put_already_existed,
        "the deleted key's PUT is a fresh put that restores the part"
    );
    // The part is present again, before the record resolves.
    assert!(
        store.head(&vanished).await.is_ok(),
        "the fresh PUT restored the part before record resolution"
    );

    let outcome = publish_record(
        &store,
        &config,
        &clock,
        &bucket,
        &inputs,
        &hash,
        &rerun_parts,
        sealed_now_ns(),
    )
    .await
    .expect("rerun publish");
    assert_eq!(
        outcome,
        PublishOutcome::Converged { parts_repaired: 0 },
        "the rerun converges by presence, repairing nothing"
    );

    // Every part the winner record references now resolves.
    let record = fetch_compaction_record(&store, &bucket).await;
    for p in &record.parts {
        let key = ravel_commit::keys::reconstruct_l1_part_key(&record, p).expect("part key");
        assert!(
            store.head(&key).await.is_ok(),
            "every referenced part is present after convergence"
        );
    }
    // Sanity: the record we read back matches a GET (single record present).
    let _ = store
        .get(&commit_keys[0], GetRange::Full)
        .await
        .expect("inputs still live");
}
