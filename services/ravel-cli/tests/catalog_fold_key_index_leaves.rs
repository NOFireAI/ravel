//! `ravel-cli catalog fold --key-index-leaves` (ADR-2707 decision 4): the
//! flag reaches the catalog the fold runs with, and without it the fold reads
//! no KEY_IDX section and writes no leaf.
//!
//! Driven in-process against one `MemoryStore` per case, for the reason
//! `tests/catalog_signal.rs` gives.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

const TENANT: &str = "cli-fold-key-index-leaves";

/// Old enough that the default seal margin seals its hour whatever minute the
/// test runs in (the same constant as `tests/catalog_signal.rs`).
const SEALED_AGE_NS: i64 = 3 * NS_PER_HOUR;

/// Publishes one RSPAN object, which always carries a KEY_IDX section over
/// `trace_id`, with one span per trace id.
async fn publish_spans(store: &MemoryStore, now_ns: i64) {
    let tenant_hash = TenantId::new(TENANT).hash();
    let writer_id = Uuid::from_u128(1);
    let mut writer = RspanWriter::new(
        RspanConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    let base_ts = now_ns - SEALED_AGE_NS;
    for i in 0..3u8 {
        writer.push(SpanRecord {
            trace_id: [i + 1; 16],
            span_id: [i + 1; 8],
            parent_span_id: None,
            name: "op".to_string(),
            start_ts_ns: base_ts + i64::from(i),
            end_ts_ns: base_ts + i64::from(i) + 10,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        });
    }
    let bytes = Bytes::from(writer.finish().expect("finish"));
    let commit = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: 3,
        series_count: 1,
        min_event_ts_ns: base_ts,
        max_event_ts_ns: base_ts + 100,
        min_ingest_ts_ns: base_ts,
        max_ingest_ts_ns: base_ts + 100,
        segment_format_version: 1,
        created_unix_ns: base_ts,
        ingest_hour_bucket: u32::try_from(base_ts.div_euclid(NS_PER_HOUR)).expect("fits u32"),
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&commit).expect("data key");
    publish::put_data_object(store, &data_key, bytes)
        .await
        .expect("put data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish");
}

async fn kidx_objects(store: &MemoryStore) -> usize {
    let prefix = format!(
        "t/{}/catalog/{}/",
        TenantId::new(TENANT).hash().to_hex(),
        Signal::Spans.key_prefix()
    );
    list_all(store, &prefix)
        .await
        .expect("list the catalog prefix")
        .iter()
        .filter(|meta| meta.key.ends_with(".kidx"))
        .count()
}

async fn fold_spans(key_index_leaves: bool) -> (ravel_catalog::FoldReport, usize) {
    let now = ravel_cli::now_ns().expect("system clock readable");
    let store = Arc::new(MemoryStore::new());
    publish_spans(&store, now).await;
    let (report, _printed) = catalog::fold_with_writers_stopped(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        TENANT,
        1,
        SignalArg::Spans,
        None,
        false,
        key_index_leaves,
        now,
        true,
    )
    .await
    .expect("fold succeeds");
    assert_eq!(report.entry_count, 1, "the spans object is folded");
    (report, kidx_objects(&store).await)
}

/// Without the flag a spans fold, whose objects always carry a KEY_IDX
/// section, issues no section read and writes no leaf; with it, the same
/// fold reads the section and publishes one leaf.
#[tokio::test]
async fn the_key_index_leaves_flag_reaches_the_fold() {
    let (off, off_leaves) = fold_spans(false).await;
    assert_eq!(off.key_index_ranged_gets, 0, "off: no KEY_IDX read");
    assert_eq!(off.key_index_leaves_written, 0, "off: no leaf");
    assert_eq!(off_leaves, 0, "off: no .kidx object");

    let (on, on_leaves) = fold_spans(true).await;
    assert!(on.key_index_ranged_gets > 0, "on: the section is read");
    assert_eq!(on.key_index_leaves_written, 1, "on: one leaf");
    assert_eq!(on_leaves, 1, "on: one .kidx object");
}
