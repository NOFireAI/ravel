//! In-process coverage for `ravel-cli store qualify` (ADR-0050 section 6,
//! issue #478). Same in-process-against-a-shared-`MemoryStore` pattern as
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
