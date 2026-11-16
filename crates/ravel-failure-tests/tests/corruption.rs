//! Corrupt stored bytes must fail a query with a typed error, never return
//! wrong data (docs/segment-format.md; docs/consistency-model.md
//! "Corruption").
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{catalog, engine, publish_segment, series_input, tenant};
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_query::{FetchError, QueryError};
use ravel_segment::SegmentError;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flipping the trailer's final byte (the RSEG magic) deterministically
/// fails footer parsing with `SegmentError::BadMagic`, regardless of
/// internal layout details -- and the query must surface that as a typed
/// error, not as missing or wrong series data.
#[tokio::test]
async fn corrupt_footer_fails_query_with_typed_error_never_wrong_data() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let ts = BASE_NS - NS_PER_MIN;
    let hour_bucket = u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "footer_corrupt_metric",
        &[],
        &[(ts, 1.0)],
    )];
    let (_, data_key) = publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour_bucket,
        BASE_NS,
        series,
    )
    .await;

    let existing = store
        .get(&data_key, GetRange::Full)
        .await
        .expect("get object");
    let mut corrupted = existing.data.to_vec();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xFF;
    store
        .put(&data_key, Bytes::from(corrupted), PutOptions::default())
        .await
        .expect("overwrite with corrupted trailer");

    let cat = catalog(Arc::clone(&store), 1);
    let eng = engine(store, cat);
    let err = eng
        .instant(
            tid.hash(),
            "footer_corrupt_metric",
            ts / 1_000_000,
            &[],
            BASE_NS,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a corrupted footer must never resolve to a value");
    assert!(
        matches!(
            err,
            QueryError::Fetch(FetchError::Corrupt {
                source: SegmentError::BadMagic,
                ..
            })
        ),
        "expected Fetch(Corrupt(BadMagic)), got {err:?}"
    );
}

/// A store-level corruption on every byte read back (modeling bit rot or a
/// bad read on the wire, via `FaultStore`'s `CorruptRange`) must also fail
/// the query with a typed error rather than returning corrupted values.
#[tokio::test]
async fn corrupt_read_via_fault_store_fails_query_with_typed_error_never_wrong_data() {
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::Get, ScriptedFault::CorruptRange).with_key_contains("/l0/"));
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let tid = tenant("acme");
    let ts = BASE_NS - NS_PER_MIN;
    let hour_bucket = u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket");

    let series = vec![series_input(&tid, "page_corrupt_metric", &[], &[(ts, 1.0)])];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour_bucket,
        BASE_NS,
        series,
    )
    .await;

    let cat = catalog(Arc::clone(&store), 1);
    let eng = engine(store, cat);
    let err = eng
        .instant(
            tid.hash(),
            "page_corrupt_metric",
            ts / 1_000_000,
            &[],
            BASE_NS,
            Duration::from_secs(5),
        )
        .await
        .expect_err("every read of a corrupted object must never resolve to a value");
    assert!(
        matches!(err, QueryError::Fetch(FetchError::Corrupt { .. })),
        "expected Fetch(Corrupt(..)), got {err:?}"
    );
    assert!(
        fault_store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::CorruptRange) >= 1,
        "corruption must have actually fired on the read path"
    );
}
