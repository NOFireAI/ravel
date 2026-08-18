//! Completion-ordering tests built on `FaultStore`'s hold-and-release gate
//! (ADR-0059 decision 5). Each test drives real concurrent in-flight
//! operations, holds them inside the store, and releases them in a controlled
//! order distinct from submission, to exercise the two places completion order
//! is contract-relevant but was never driven before: multipart part completion
//! (docs/object-store-contract.md, "Multipart upload") and cross-shard
//! commit-record visibility (each shard's flush is independent;
//! docs/catalog-and-mvcc.md). This closes a testing-capability gap: no
//! production path today depends on object-store completion order (ADR-0059,
//! rejected alternative "Treat S1-14 as closing a live correctness bug").
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{catalog, engine, expect_vector, publish_segment, series_input, tenant};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, MULTIPART_MIN_PART_SIZE, ObjectStoreBackend};
use ravel_types::TenantHash;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Two multipart parts, held at completion and released in the reverse of
/// submission order, still assemble byte-correctly: part order is fixed at
/// submission, not by which completion the store observes first. This is the
/// first test to actually drive the "parts may complete out of submission
/// order" assumption the object-store contract has always stated.
#[tokio::test]
async fn multipart_parts_completing_out_of_submission_order_assemble_correctly() {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let key = "t/acme/m/l0/0000/multipart-seg";

    // Hold every part completion for this multipart key.
    let gate = store.hold(Op::Put, Some(key.to_string()), Occurrence::Always);

    // Two distinct parts: a full-size non-final part (the 5 MiB minimum) and a
    // short final tail. Distinct fill bytes so a reversed assembly would be
    // detectably wrong.
    let head = vec![0xAAu8; MULTIPART_MIN_PART_SIZE];
    let tail = vec![0xBBu8; 4096];
    let mut expected = head.clone();
    expected.extend_from_slice(&tail);

    let mut upload = store.put_multipart(key).await.expect("start multipart");
    upload
        .put_part(Bytes::from(head.clone()), None)
        .await
        .expect("submit part 0");
    upload
        .put_part(Bytes::from(tail.clone()), None)
        .await
        .expect("submit part 1");

    // `complete()` arms one gate check per submitted part, concurrently, then
    // assembles once every completion is released. It borrows the store, so it
    // cannot be spawned onto a 'static task; `join!` drives it alongside the
    // release-control future on this task instead.
    let complete = upload.complete();
    let control = async {
        gate.wait_until_held(2).await;
        let ids = gate.held();
        assert_eq!(ids.len(), 2, "both part completions are held at once");
        // Release the final part's completion before the first part's: the
        // reverse of submission order.
        assert!(gate.release(ids[1]));
        assert!(gate.release(ids[0]));
    };
    let (result, ()) = tokio::join!(complete, control);
    result.expect("multipart complete after out-of-order release");
    assert_eq!(gate.held_count(), 0);

    // The assembled object is byte-correct: assembly followed submission order,
    // not the reversed completion order.
    let got = store
        .get(key, GetRange::Full)
        .await
        .expect("get assembled object");
    assert_eq!(got.data.len(), expected.len());
    assert_eq!(
        &got.data[..],
        &expected[..],
        "assembled bytes must follow submission order, not completion order"
    );
}

/// Resolve one metric's instant value through a fresh catalog + engine over the
/// current store state (the fold re-lists commit records live, so each call
/// reflects present visibility). `None` when the metric resolves to no series.
async fn value_of(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    metric: &str,
    ts: i64,
) -> Option<f64> {
    let cat = catalog(Arc::clone(store), 2);
    let eng = engine(Arc::clone(store), cat);
    let (value, _coverage) = eng
        .instant(
            tenant_hash,
            metric,
            ts / 1_000_000,
            &[],
            BASE_NS,
            Duration::from_secs(5),
        )
        .await
        .expect("query");
    let vector = expect_vector(value);
    match vector.len() {
        0 => None,
        _ => Some(vector[0].value),
    }
}

/// Two shards flush for the same tenant and hour. Shard 1 commits fully; shard
/// 0's commit-record PUT is held in flight. A query spanning the window sees the
/// visible shard correctly and does not see the held shard until its commit
/// record lands, then sees both once released. This is per-shard visibility
/// independence (each shard's flush is genuinely independent, ADR-0059), driven
/// by a real held commit-record PUT, not a sequential approximation.
#[tokio::test]
async fn cross_shard_commit_visibility_is_per_shard_independent() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let tid = tenant("acme");
    let ts = BASE_NS - NS_PER_MIN;
    let hour = u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket");

    // Shard 1 flushes fully and becomes visible first (no gate armed yet).
    publish_segment(
        store.as_ref(),
        tid.hash(),
        1,
        Uuid::new_v4(),
        1,
        1,
        hour,
        BASE_NS,
        vec![series_input(&tid, "cross_shard_s1", &[], &[(ts, 11.0)])],
    )
    .await;

    // Shard 0's commit-record PUT is held in flight. The commit key carries
    // `/c/0000/`; the data-object PUT (already issued before the commit PUT)
    // carries `/l0/0000/`, so only the commit record is held here.
    let gate = fault_store.hold(Op::Put, Some("/c/0000/".to_string()), Occurrence::Always);
    let flush0 = publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour,
        BASE_NS,
        vec![series_input(&tid, "cross_shard_s0", &[], &[(ts, 22.0)])],
    );
    let control = async {
        gate.wait_until_held(1).await;
        assert_eq!(gate.held_count(), 1, "shard 0's commit-record PUT is held");
        // Shard 1 is fully visible; shard 0 is not, its commit record unwritten.
        assert_eq!(
            value_of(&store, tid.hash(), "cross_shard_s1", ts).await,
            Some(11.0),
            "the already-committed shard must resolve correctly while shard 0 is held"
        );
        assert_eq!(
            value_of(&store, tid.hash(), "cross_shard_s0", ts).await,
            None,
            "a query must not see shard 0 before its commit record becomes visible"
        );
        let ids = gate.held();
        assert!(gate.release(ids[0]));
    };
    tokio::join!(flush0, control);

    // Both shards now visible: the delayed shard resolves correctly and did not
    // corrupt the shard that was already visible.
    assert_eq!(gate.held_count(), 0);
    assert_eq!(
        value_of(&store, tid.hash(), "cross_shard_s1", ts).await,
        Some(11.0)
    );
    assert_eq!(
        value_of(&store, tid.hash(), "cross_shard_s0", ts).await,
        Some(22.0)
    );
}
