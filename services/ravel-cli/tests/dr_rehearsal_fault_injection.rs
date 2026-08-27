//! Proof, for issue #814, that the DR rehearsal's reconciliation stage
//! actually catches the two faults it claims to catch (the third,
//! canary-query error, is proven at the shell level in
//! `scripts/dr-rehearsal/`, against a real single-process `ravel-server
//! --store memory` run, because it needs a running query surface rather
//! than a library call).
//!
//! Each test seeds a real object into a shared `MemoryStore` (the same
//! in-process pattern `tests/reconstruct.rs` and `tests/maintain.rs` use for
//! the identical reason: a subprocess-per-invocation store is empty every
//! time), then asserts the reconciliation call returns `Err` and that the
//! error text names the anomaly. A rehearsal workflow that only exercises a
//! clean store is not evidence it catches anything: this is that evidence.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::maintain::{SignalArg as MaintainSignalArg, verify_custody};
use ravel_cli::reconstruct::reconstruct;
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::NewCommitRecord;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

/// Publish one real L0 metrics segment (data object + commit record), the
/// same helper shape `tests/maintain.rs::publish_l0` uses, so the record
/// this test later strands is otherwise entirely legitimate.
async fn publish_l0(store: &MemoryStore, tenant: &str, shard: u32, seq: u64, created_unix_ns: i64) {
    let tenant_hash = TenantId::new(tenant).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / 3_600_000_000_000).expect("fits u32");
    let payload = format!("dr-rehearsal-seg-{shard}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = ravel_commit::record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id: Uuid::new_v4(),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_unix_ns - 1_000,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Fault 1 (dangling commit record): a replica restore that lost the L0
/// object a commit record still points at (a partial or racing copy) must
/// fail `verify-custody`'s "missing-and-unexpected" check, not go unnoticed
/// until a query silently returns less data than committed.
#[tokio::test]
async fn dr_rehearsal_fails_on_dangling_commit_record() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "acme";
    let tenant_hash = TenantId::new(tenant).hash();
    let shard = 0u32;
    let created_unix_ns = 495_734i64 * 3_600_000_000_000;

    publish_l0(&store, tenant, shard, 1, created_unix_ns).await;

    // Recover the record's data key the same way verify-custody does, then
    // strand the record by deleting the object it points at: the restore
    // copy that lost one object while its commit record survived.
    let prefix = keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, shard)
        .expect("commit shard prefix");
    let metas = ravel_object_store::list_all(store.as_ref(), &prefix)
        .await
        .expect("list commit records");
    let commit_meta = metas
        .into_iter()
        .find(|m| {
            matches!(
                keys::partition_bucket_entry(&m.key),
                Ok(keys::BucketEntry::CommitRecord(_))
            )
        })
        .expect("published commit record present");
    let got = store
        .get(&commit_meta.key, GetRange::Full)
        .await
        .expect("read commit record");
    let record = ravel_commit::record::decode(&got.data).expect("decode commit record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    store.delete(&data_key).await.expect("strand the record");

    let err = verify_custody(store as Arc<dyn ObjectStoreBackend>, tenant, 4, false)
        .await
        .expect_err("verify-custody must fail on a dangling commit record");
    let message = err.to_string();
    assert!(
        message.contains("anomal"),
        "error must name the anomaly, got: {message}"
    );
}

/// Fault 2 (missing/unreconstructable data): a restore copy that carries an
/// L0 object with no commit record and a footer too corrupt to rebuild one
/// from (a torn upload, not merely a lost record) must fail `commit
/// reconstruct`, not report a clean 0-candidates-failed run.
#[tokio::test]
async fn dr_rehearsal_fails_on_unreconstructable_data_object() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "acme";
    let tenant_hash = TenantId::new(tenant).hash();
    let shard = 0u32;
    let writer_id = Uuid::new_v4();

    // A record-less object at a well-formed L0 key (so it is picked up as a
    // reconstruction candidate) whose bytes are not a valid RSEG footer at
    // all: the torn-upload case, distinct from a merely-missing record.
    let garbage = Bytes::from_static(b"not a valid rseg footer, torn upload");
    let content_hash = *blake3::hash(&garbage).as_bytes();
    let data_key = keys::data_key(
        &tenant_hash,
        Signal::Metrics,
        shard,
        writer_id,
        1,
        1,
        &content_hash,
    )
    .expect("data key");
    store
        .put(&data_key, garbage, PutOptions::default())
        .await
        .expect("seed corrupt data object");

    let err = reconstruct(
        store as Arc<dyn ObjectStoreBackend>,
        tenant,
        MaintainSignalArg::Metrics,
        shard,
    )
    .await
    .expect_err("commit reconstruct must fail on an unreconstructable data object");
    let message = err.to_string();
    assert!(
        message.contains("candidate(s) failed"),
        "error must report the failed candidate, got: {message}"
    );
}
