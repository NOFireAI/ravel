//! `ravel-cli catalog fold` report completeness (#1598): the human-readable
//! report must print every `FoldReport` field, `--json` must serialize the
//! whole struct, and `put_requests` must count every PUT the fold issues.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

/// Same margin as `tests/catalog.rs`: clears the default 1h20m seal margin
/// regardless of which minute of the hour the test runs in.
const SEALED_AGE_NS: i64 = 3 * 60 * 60 * 1_000_000_000;

const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

fn now_ns() -> i64 {
    ravel_cli::now_ns().expect("system clock readable")
}

async fn publish_segment(
    store: &MemoryStore,
    tenant: &str,
    shard: u32,
    seq: u64,
    created_unix_ns: i64,
) {
    let tenant_hash = TenantId::new(tenant).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / 3_600_000_000_000).expect("fits u32");
    let payload = format!("seg-{shard}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
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

/// Deliverable 1 (#1598): the pre-change human-readable report printed 13 of
/// `FoldReport`'s 23 fields and silently omitted the rest, including
/// `column_stats_dictionaries_dropped`. Every field must now appear.
///
/// Prove-the-test: delete any one of the `push_str` lines this test checks
/// for in `render_fold_report` (`services/ravel-cli/src/catalog.rs`) and the
/// matching assertion below fails.
#[tokio::test]
async fn fold_human_report_prints_every_fold_report_field() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-report-fields";
    let created = now_ns() - SEALED_AGE_NS;
    publish_segment(&store, tenant, 0, 1, created).await;

    let (_report, printed) = catalog::fold(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        1,
        SignalArg::Metrics,
        None,
        now_ns(),
        false,
    )
    .await
    .expect("fold succeeds");

    // Fields the pre-change report already printed.
    for field in [
        "no_op:",
        "rebuilt:",
        "previous_watermark_hour:",
        "watermark_hour:",
        "seal_margin:",
        "buckets_folded:",
        "entry_count:",
        "part_bytes:",
        "list_requests:",
        "get_requests:",
        "put_requests:",
    ] {
        assert!(
            printed.contains(field),
            "already-printed field {field} missing from report:\n{printed}"
        );
    }
    // Fields the pre-change report silently omitted.
    for field in [
        "parts_total:",
        "parts_reused:",
        "postings_built:",
        "postings_bytes:",
        "column_stats_built:",
        "column_stats_bytes:",
        "column_stats_part_built:",
        "column_stats_part_bytes:",
        "column_stats_part_objects_built:",
        "column_stats_dictionaries_dropped:",
        "layout_drift_count:",
        "frontier_hours_reconciled:",
        "frontier_hours_deferred:",
    ] {
        assert!(
            printed.contains(field),
            "previously-omitted field {field} missing from report:\n{printed}"
        );
    }
}

/// Deliverable 1's `--json` flag: the whole `FoldReport` round-trips through
/// `serde_json`, including a field the human report used to omit.
#[tokio::test]
async fn fold_json_report_serializes_the_whole_fold_report() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-report-json";
    let created = now_ns() - SEALED_AGE_NS;
    publish_segment(&store, tenant, 0, 1, created).await;

    let (report, printed) = catalog::fold(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        1,
        SignalArg::Metrics,
        None,
        now_ns(),
        true,
    )
    .await
    .expect("fold succeeds");

    // First line is the store-selection header (every render function's
    // first line, human or JSON); the rest is the JSON body.
    let body = printed
        .split_once('\n')
        .map(|(_, rest)| rest)
        .expect("header line present");
    let value: serde_json::Value = serde_json::from_str(body).expect("valid JSON");
    assert_eq!(
        value.get("column_stats_dictionaries_dropped"),
        Some(&serde_json::json!(report.column_stats_dictionaries_dropped)),
        "JSON must carry a field the human report used to omit, got:\n{body}"
    );
    assert_eq!(
        value.get("put_requests"),
        Some(&serde_json::json!(report.put_requests))
    );
}

/// Deliverable 2 (#1598): `put_requests` must count every PUT the fold
/// issues. `publish_segment`'s payload is not a real encoded segment (it is
/// the literal bytes `seg-0-1`), so `fetch_segment_names` fails to decode it
/// and `build_postings` returns `None` (`crates/ravel-catalog/src/fold.rs`
/// lines 2596-2610): no postings object is built, and this fixture also
/// declares no typed columns, so no column-statistics object is built either
/// (ADR-0850/ADR-1413 gate on `typed_attr_columns` being non-empty). That
/// leaves exactly two PUTs: the fresh tail part (`.csnap`) and the HEAD CAS
/// write.
///
/// Prove-the-test: in `render_fold_report`
/// (`services/ravel-cli/src/catalog.rs`), or by reverting the `fold.rs` fix
/// that counts a PUT before matching its outcome, `put_requests` would read
/// something other than `2` and this assertion fails.
#[tokio::test]
async fn fold_put_requests_counts_exactly_the_objects_this_fold_writes() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "cli-fold-put-requests";
    let created = now_ns() - SEALED_AGE_NS;
    publish_segment(&store, tenant, 0, 1, created).await;

    let (report, printed) = catalog::fold(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        1,
        SignalArg::Metrics,
        None,
        now_ns(),
        false,
    )
    .await
    .expect("fold succeeds");

    assert!(
        !report.postings_built,
        "this fixture's payload cannot decode as a real segment, so no postings object is built: {report:?}"
    );
    assert!(
        !report.column_stats_built
            && !report.column_stats_part_built
            && report.column_stats_part_objects_built == 0,
        "fixture declares no typed columns, so no column-stats object is built: {report:?}"
    );
    assert_eq!(
        report.parts_total - report.parts_reused,
        1,
        "exactly one fresh tail part is written"
    );
    assert_eq!(
        report.put_requests, 2,
        "put_requests must count exactly the part PUT and the HEAD PUT this fold issues:\n{printed}"
    );
}
