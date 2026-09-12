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
