//! Suite F (MARKER): every `check_marker` case against a real
//! [`MemoryStore`], plus the read-error-refuses-with-fault-counter case
//! against a [`FaultStore`]. These integration tests exercise only
//! `ravel-ipcrypt`'s public surface (`check_marker`, `CheckOutcome`,
//! `CheckError`, `MarkerError`), unlike the crate-internal unit tests beside
//! `marker.rs` itself, which also cover `read_marker`/`create_marker`
//! directly.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use prost::Message;
use ravel_ipcrypt::fingerprint::EMPTY_POLICY_FINGERPRINT;
use ravel_ipcrypt::marker::{
    CheckError, CheckOutcome, IPCRYPT_POLICY_KEY, MarkerError, check_marker, read_marker,
    replace_marker,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreError};
use ravel_proto::sys::v1 as sysproto;

#[tokio::test]
async fn no_marker_and_empty_local_proceeds_without_writing() {
    let store = MemoryStore::new();
    let outcome = check_marker(&store, EMPTY_POLICY_FINGERPRINT)
        .await
        .expect("ok");
    assert_eq!(outcome, CheckOutcome::ProceededNoMarker);

    // Nothing was written: a second call still sees no marker and still
    // proceeds, rather than finding something this call should not have
    // created.
    let outcome_again = check_marker(&store, EMPTY_POLICY_FINGERPRINT)
        .await
        .expect("ok");
    assert_eq!(outcome_again, CheckOutcome::ProceededNoMarker);
}

#[tokio::test]
async fn no_marker_and_nonempty_local_creates_it() {
    let store = MemoryStore::new();
    let fp = [11u8; 32];
    let outcome = check_marker(&store, fp).await.expect("ok");
    assert_eq!(outcome, CheckOutcome::Created);

    // The marker now exists and a later call with the same fingerprint
    // matches it rather than creating a second one.
    let outcome_again = check_marker(&store, fp).await.expect("ok");
    assert_eq!(outcome_again, CheckOutcome::Matched);
}

#[tokio::test]
async fn present_marker_matching_local_fingerprint_matches() {
    let store = MemoryStore::new();
    let fp = [22u8; 32];
    check_marker(&store, fp).await.expect("first call creates");
    let outcome = check_marker(&store, fp).await.expect("ok");
    assert_eq!(outcome, CheckOutcome::Matched);
}

#[tokio::test]
async fn present_marker_disagreeing_with_local_fingerprint_refuses() {
    let store = MemoryStore::new();
    let stored_fp = [33u8; 32];
    let local_fp = [44u8; 32];
    check_marker(&store, stored_fp)
        .await
        .expect("first call creates");

    let err = check_marker(&store, local_fp).await.unwrap_err();
    match err {
        CheckError::Mismatch { stored, local } => {
            assert_eq!(stored, hex::encode(stored_fp));
            assert_eq!(local, hex::encode(local_fp));
        }
        other => panic!("expected Mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn empty_local_fingerprint_against_a_present_marker_is_also_a_mismatch() {
    // The empty-policy sentinel is not special-cased on the "marker already
    // present" path: a deployment that previously had a non-empty policy and
    // now has none still needs an explicit re-key of the marker.
    let store = MemoryStore::new();
    let stored_fp = [55u8; 32];
    check_marker(&store, stored_fp)
        .await
        .expect("first call creates");

    let err = check_marker(&store, EMPTY_POLICY_FINGERPRINT)
        .await
        .unwrap_err();
    assert!(matches!(err, CheckError::Mismatch { .. }));
}

#[tokio::test]
async fn decode_failure_refuses() {
    let store = MemoryStore::new();
    store
        .put(
            ravel_ipcrypt::marker::IPCRYPT_POLICY_KEY,
            b"this is not a valid IpcryptPolicyMarker protobuf"
                .to_vec()
                .into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("seed a corrupt marker object");

    let err = check_marker(&store, [1u8; 32]).await.unwrap_err();
    assert!(matches!(err, CheckError::Marker(MarkerError::Decode(_))));
}

#[tokio::test]
async fn unknown_format_version_refuses() {
    let store = MemoryStore::new();
    let bytes = sysproto::IpcryptPolicyMarker {
        format_version: 7,
        fingerprint: vec![9u8; 32],
    }
    .encode_to_vec();
    store
        .put(
            ravel_ipcrypt::marker::IPCRYPT_POLICY_KEY,
            bytes.into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("seed a future-versioned marker object");

    let err = check_marker(&store, [1u8; 32]).await.unwrap_err();
    assert!(matches!(
        err,
        CheckError::Marker(MarkerError::UnknownVersion { got: 7 })
    ));
}

/// Builds a store where a winner's marker is already present in the inner
/// [`MemoryStore`], wrapped so the caller's own `check_marker` call actually
/// takes the lost-create-race branch: its first `get` reports not-found (so
/// it attempts to create the marker) and its `put` fails as a
/// conditional-write rejection (so it falls through to the
/// re-read-and-compare arm), rather than the real backend's natural
/// `AlreadyExists` short-circuiting the scenario before that branch ever
/// runs.
async fn lost_race_store(winner_fp: [u8; 32]) -> FaultStore<MemoryStore> {
    let inner = MemoryStore::new();
    check_marker(&inner, winner_fp)
        .await
        .expect("winner creates");

    let plan = FaultPlan::empty()
        .with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(IPCRYPT_POLICY_KEY)
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_passthrough(),
        )
        .with_rule(Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite));
    FaultStore::new(inner, plan)
}

#[tokio::test]
async fn lost_create_race_against_a_matching_winner_proceeds() {
    // A real race (two processes both seeing no marker and both racing
    // `CreateIfAbsent`) ends with the loser's `create_marker` call failing
    // with `AlreadyExists` and falling through to a re-read-and-compare
    // against whichever fingerprint the winner wrote. The scripted faults
    // force `check_marker` through exactly that branch instead of relying on
    // the real backend's natural conflict behavior.
    let fp = [66u8; 32];
    let store = lost_race_store(fp).await;

    let outcome = check_marker(&store, fp)
        .await
        .expect("loser re-reads and matches");
    assert_eq!(outcome, CheckOutcome::Matched);
    assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1
    );
}

#[tokio::test]
async fn lost_create_race_against_a_mismatching_winner_refuses_naming_both_fingerprints() {
    let winner_fp = [66u8; 32];
    let local_fp = [67u8; 32];
    let store = lost_race_store(winner_fp).await;

    let err = check_marker(&store, local_fp).await.unwrap_err();
    match err {
        CheckError::Mismatch { stored, local } => {
            assert_eq!(stored, hex::encode(winner_fp));
            assert_eq!(local, hex::encode(local_fp));
        }
        other => panic!("expected Mismatch, got {other:?}"),
    }
    assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1
    );
}

#[tokio::test]
async fn read_error_refuses_and_is_distinguishable_from_absent() {
    // Distinguishing test (suite F): an implementation that treated a
    // transient store failure the same as "no marker present" would
    // proceed (or wrongly attempt a create) here instead of refusing. The
    // fault counter proves the fault actually fired, so this is not a
    // vacuous pass.
    let plan = FaultPlan::empty().with_rule(Rule::new(
        Op::Get,
        ScriptedFault::Transient("injected failure".to_string()),
    ));
    let store = FaultStore::new(MemoryStore::new(), plan);

    let err = check_marker(&store, [1u8; 32]).await.unwrap_err();
    assert!(matches!(
        err,
        CheckError::Marker(MarkerError::Store(StoreError::Transient(_)))
    ));
    assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
}

#[tokio::test]
async fn public_read_marker_returns_fingerprint_and_the_version_replace_marker_needs() {
    // Item 4: `read_marker` is public so a caller like `ravel-cli ipcrypt
    // set-policy` can obtain the current Version for its CAS replace without
    // reaching into crate-internal test code.
    let store = MemoryStore::new();
    let fp = [77u8; 32];
    check_marker(&store, fp).await.expect("creates marker");

    let (stored, version) = read_marker(&store)
        .await
        .expect("ok")
        .expect("marker present after create");
    assert_eq!(stored.fingerprint, fp);

    let new_fp = [88u8; 32];
    replace_marker(&store, new_fp, version)
        .await
        .expect("replace with the version read_marker returned succeeds");

    let (stored_after, _) = read_marker(&store)
        .await
        .expect("ok")
        .expect("marker still present");
    assert_eq!(stored_after.fingerprint, new_fp);
}

#[tokio::test]
async fn replace_marker_with_stale_version_fails() {
    let store = MemoryStore::new();
    let fp = [99u8; 32];
    check_marker(&store, fp).await.expect("creates marker");

    let (_, stale_version) = read_marker(&store)
        .await
        .expect("ok")
        .expect("marker present");

    // Someone else replaces the marker first, advancing its version.
    replace_marker(&store, [100u8; 32], stale_version.clone())
        .await
        .expect("first replace succeeds");

    // Retrying with the now-stale version must fail, not silently overwrite.
    let err = replace_marker(&store, [101u8; 32], stale_version)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::PreconditionFailed));
}
