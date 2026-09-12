//! In-process coverage for `ravel-cli store qualify` (ADR-0050 section 6).
//! Same in-process-against-a-shared-`MemoryStore` pattern as
//! `tests/catalog.rs`: a subprocess-per-invocation test could not observe
//! whether a second `qualify` call left an existing `sys/qualification`
//! record untouched, since each subprocess would start from an empty store.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_cli::qualify::{self, QUALIFICATION_KEY};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};

#[tokio::test]
async fn qualify_records_a_pass_against_a_conforming_backend() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    qualify::qualify(store.clone(), "memory".to_string(), "run-1")
        .await
        .expect("a conforming backend must qualify");

    let outcome = store
        .get(QUALIFICATION_KEY, GetRange::Full)
        .await
        .expect("sys/qualification must be written on a pass");
    let record: qualify::QualificationRecord =
        serde_json::from_slice(&outcome.data).expect("record must be valid JSON");
    assert_eq!(record.backend_identity, "memory");
    assert!(!record.passed_properties.is_empty());
}

#[tokio::test]
async fn a_second_qualify_run_does_not_overwrite_the_existing_record() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    qualify::qualify(store.clone(), "memory".to_string(), "run-1")
        .await
        .expect("first run qualifies");
    let first = store
        .get(QUALIFICATION_KEY, GetRange::Full)
        .await
        .expect("record present after first run");

    // A distinct scratch run-id proves the second run actually re-executed
    // the suite (not skipped), yet the durable record must be left alone.
    qualify::qualify(store.clone(), "memory".to_string(), "run-2")
        .await
        .expect("second run also qualifies and must not error");
    let second = store
        .get(QUALIFICATION_KEY, GetRange::Full)
        .await
        .expect("record still present after second run");

    assert_eq!(first.data, second.data, "qualification is once per bucket");
}

/// A record recorded under an older suite version is re-recorded (not left
/// stale) when `qualify` re-runs against the same bucket. Without this,
/// bumping `CONFORMANCE_SUITE_VERSION` would deadlock every already-qualified
/// bucket: `ravel-server` refuses a below-floor record, and `CreateIfAbsent`
/// cannot clear it. The re-recorded object carries the current suite version
/// and the full probed-property set.
#[tokio::test]
async fn qualify_re_records_over_a_stale_suite_version() {
    use bytes::Bytes;
    use ravel_object_store::PutOptions;
    use ravel_object_store::conformance::CONFORMANCE_SUITE_VERSION;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    // A bucket qualified before the probe set grew carries a below-floor record.
    let stale = qualify::QualificationRecord {
        suite_version: CONFORMANCE_SUITE_VERSION - 1,
        backend_identity: "memory".to_string(),
        qualified_unix_ns: 1,
        passed_properties: vec!["conditional_write_create_if_absent".to_string()],
    };
    store
        .put(
            QUALIFICATION_KEY,
            Bytes::from(serde_json::to_vec(&stale).expect("encode stale record")),
            PutOptions::default(),
        )
        .await
        .expect("seed a stale-version record");

    qualify::qualify(store.clone(), "memory".to_string(), "run-1")
        .await
        .expect("re-qualification upgrades a stale record instead of erroring");

    let outcome = store
        .get(QUALIFICATION_KEY, GetRange::Full)
        .await
        .expect("record present after re-qualification");
    let record: qualify::QualificationRecord =
        serde_json::from_slice(&outcome.data).expect("record must be valid JSON");
    assert_eq!(
        record.suite_version, CONFORMANCE_SUITE_VERSION,
        "a below-floor record is re-recorded at the current suite version"
    );
    assert_eq!(
        record.passed_properties.len(),
        8,
        "the upgraded record lists all eight probed properties"
    );
}

/// The re-record path guards its overwrite with `CasVersion`, so a concurrent
/// `qualify` from a newer binary that installs a higher-version record between
/// our read and our write is never silently downgraded (issue #1302,
/// deliverable 2). The interleaving is driven, not asserted: a hold gate parks
/// this run at its CAS write after it has read the stale record, a competing
/// higher-version write lands while it is parked, and on release the CAS write
/// fails its precondition, the run re-reads, and leaves the newer record
/// untouched. Flipping the guarded `PutMode::CasVersion` back to an
/// unconditional `Overwrite` makes the released write clobber the newer record,
/// dropping the surviving `suite_version` from CONFORMANCE_SUITE_VERSION + 1 to
/// CONFORMANCE_SUITE_VERSION and failing the final assertion.
#[tokio::test]
async fn a_concurrent_newer_record_is_not_downgraded_during_re_record() {
    use bytes::Bytes;
    use ravel_object_store::PutOptions;
    use ravel_object_store::conformance::CONFORMANCE_SUITE_VERSION;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;

    let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));

    // A bucket qualified before the probe set grew carries a below-floor record,
    // which the re-record path reads and decides to overwrite.
    let stale = qualify::QualificationRecord {
        suite_version: CONFORMANCE_SUITE_VERSION - 1,
        backend_identity: "memory".to_string(),
        qualified_unix_ns: 1,
        passed_properties: vec!["conditional_write_create_if_absent".to_string()],
    };
    fault
        .put(
            QUALIFICATION_KEY,
            Bytes::from(serde_json::to_vec(&stale).expect("encode stale record")),
            PutOptions::default(),
        )
        .await
        .expect("seed a stale-version record before the gate is registered");

    // Hold the SECOND put on sys/qualification: the CAS re-record write. The
    // first put on that key is qualify's own CreateIfAbsent attempt (fails
    // AlreadyExists against the seed); the conformance suite's own puts live
    // under the `sys/qualify/<run>/` scratch prefix and never match this key.
    let gate = fault.hold(
        Op::Put,
        Some(QUALIFICATION_KEY.to_string()),
        Occurrence::Nth(2),
    );

    let run_store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let run =
        tokio::spawn(
            async move { qualify::qualify(run_store, "memory".to_string(), "run-1").await },
        );

    // The run has read the stale record and is parked at the CAS write. Exactly
    // one call is held: the CAS re-record put, and nothing else.
    gate.wait_until_held(1).await;
    assert_eq!(
        gate.held_count(),
        1,
        "exactly the CAS re-record put is held, once"
    );

    // A concurrent qualify from a NEWER binary installs a higher-version record
    // while our run is parked. This is the write the parked CAS must not clobber.
    let newer = qualify::QualificationRecord {
        suite_version: CONFORMANCE_SUITE_VERSION + 1,
        backend_identity: "memory".to_string(),
        qualified_unix_ns: 2,
        passed_properties: vec!["conditional_write_create_if_absent".to_string()],
    };
    fault
        .put(
            QUALIFICATION_KEY,
            Bytes::from(serde_json::to_vec(&newer).expect("encode newer record")),
            PutOptions::default(),
        )
        .await
        .expect("a newer binary records a higher-version pass");

    // Release the held CAS write. Its precondition (the stale version) no longer
    // holds, so it fails PreconditionFailed; the run re-reads, sees the newer
    // record, and leaves it untouched, returning Ok as a no-op.
    let ids = gate.held();
    assert_eq!(ids.len(), 1, "one held call to release");
    assert!(gate.release(ids[0]), "the held CAS put is released");

    run.await
        .expect("join the qualify task")
        .expect("the run succeeds as a no-op without downgrading");
    assert_eq!(
        gate.held_count(),
        0,
        "the held CAS put fired and released exactly once"
    );

    let outcome = fault
        .get(QUALIFICATION_KEY, GetRange::Full)
        .await
        .expect("record present after the race");
    let record: qualify::QualificationRecord =
        serde_json::from_slice(&outcome.data).expect("record must be valid JSON");
    assert_eq!(
        record.suite_version,
        CONFORMANCE_SUITE_VERSION + 1,
        "the higher-version record survives; the older run did not downgrade it"
    );
}
