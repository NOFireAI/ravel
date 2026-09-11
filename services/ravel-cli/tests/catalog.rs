//! In-process coverage for `ravel-cli catalog fold/inspect/verify`.
//! Run in-process against a single
//! shared `MemoryStore` rather than as `segment_inspect.rs`'s subprocess
//! pattern: each subprocess invocation of the binary gets its own empty
//! `MemoryStore`, so a chained fold -> inspect -> verify scenario cannot be
//! built that way without a persistent S3/MinIO backend, unavailable in this
//! environment. `ravel_cli`'s lib target exists for exactly this reason.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{DefaultedMemoryEmptyWalk, StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{
    SnapshotColumnStatsPartRef, SnapshotColumnStatsRef, SnapshotHead, SnapshotPartRef,
};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

/// Default seal margins sum to 1h20m (max_flush_lifetime 1h +
/// clock_skew_allowance 5m + fold_safety_margin 15m), but hour-bucket
/// quantization means anything under ~2h20m can
/// land on the wrong side of `sealed_watermark_hour`'s boundary depending on
/// which minute of the hour the test happens to run in. 3h clears that
/// margin with room to spare (matches `services/ravel-server/tests/fold_e2e.rs`'s
/// `SEALED_AGE`).
const SEALED_AGE_NS: i64 = 3 * 60 * 60 * 1_000_000_000;

/// Every fixture here builds its own `MemoryStore`, which is the explicit
/// `--store memory` case (issue #1024): the reports carry `store: memory` and
/// an empty result stays a success, unlike a walk on the defaulted store.
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
    let record = record::build(NewCommitRecord {
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
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish");
}

#[tokio::test]
async fn fold_then_inspect_then_verify_round_trips_cleanly() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "acme";
    let created = now_ns() - SEALED_AGE_NS;
    publish_segment(&store, tenant, 0, 1, created).await;
    publish_segment(&store, tenant, 0, 2, created).await;

    catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
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

    catalog::inspect(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Metrics,
    )
    .await
    .expect("inspect succeeds against a freshly folded HEAD");

    catalog::verify(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Metrics,
    )
    .await
    .expect("verify finds no divergence right after a fold");
}

#[tokio::test]
async fn verify_fails_when_a_sealed_record_is_missing_from_the_snapshot() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "acme";
    let created = now_ns() - SEALED_AGE_NS;
    publish_segment(&store, tenant, 0, 1, created).await;

    catalog::fold(
        store.clone() as Arc<dyn ObjectStoreBackend>,
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

    // Published after the fold ran, so it is sealed but not yet folded in:
    // the snapshot under-counts relative to the sealed commit history.
    publish_segment(&store, tenant, 0, 2, created).await;

    let err = catalog::verify(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Metrics,
    )
    .await
    .expect_err("verify must fail when a sealed record is missing from the snapshot");
    assert!(
        err.to_string().contains("missing"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn inspect_and_verify_report_cleanly_with_no_head_yet() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = "acme";

    catalog::inspect(store.clone(), MEMORY, tenant, SignalArg::Metrics)
        .await
        .expect("inspect on an absent HEAD reports rather than errors");
    catalog::verify(store.clone(), MEMORY, tenant, SignalArg::Metrics)
        .await
        .expect("verify on an absent HEAD reports rather than errors");
}

/// Issue #1024 for `catalog fold`, the other command the incident names: with
/// `--store` omitted the fold runs against the empty in-process store, and
/// rather than publishing an empty HEAD and reporting `buckets_folded: 0` at
/// exit 0 it refuses with the typed error, naming the tenant and the remedy.
///
/// Non-vacuity (prove-the-test): delete the `require_tenant_data_present` call
/// in `catalog::fold` and this returns `Ok` with `buckets_folded == 0`, exactly
/// the shape the incident reported; the HEAD assertion below then also fails,
/// because the refused fold no longer leaves the catalog untouched.
#[tokio::test]
async fn fold_on_the_defaulted_memory_store_refuses_instead_of_sealing_nothing() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = "acme";

    let err = catalog::fold(
        store.clone(),
        StoreSelection::defaulted_memory(),
        tenant,
        1,
        SignalArg::Metrics,
        None,
        now_ns(),
        false,
    )
    .await
    .expect_err("a fold over an unchosen empty store must refuse");

    let typed = err
        .downcast_ref::<DefaultedMemoryEmptyWalk>()
        .expect("typed DefaultedMemoryEmptyWalk");
    assert_eq!(
        *typed,
        DefaultedMemoryEmptyWalk {
            command: "catalog fold",
            tenant: tenant.to_string(),
            searched: "objects",
        }
    );
    assert!(
        err.to_string().contains("--store s3"),
        "the error must name the remedy: {err}"
    );

    let listed = store.list("t/", None).await.expect("list").objects;
    assert!(
        listed.is_empty(),
        "the refusal happens before any catalog write; found {listed:?}"
    );
}

#[tokio::test]
async fn inspect_rejects_a_corrupt_head() {
    let store = Arc::new(MemoryStore::new());
    let tenant_hash = TenantId::new("acme").hash();
    let key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );
    store
        .put(
            &key,
            Bytes::from_static(b"not a valid HEAD"),
            PutOptions::default(),
        )
        .await
        .expect("seed a corrupt HEAD object");

    let err = catalog::inspect(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        SignalArg::Metrics,
    )
    .await
    .expect_err("corrupt HEAD must be a typed error, not a panic or silent success");
    assert!(
        err.to_string().contains("corrupt"),
        "unexpected error: {err}"
    );
}

/// A part fetch failure partway through the parts loop must not discard the
/// report already rendered: an operator inspecting a damaged catalog needs
/// the HEAD fields and every earlier part's line, not just the one error for
/// the part that failed.
#[tokio::test]
async fn inspect_preserves_partial_output_when_a_part_fetch_fails() {
    let store = Arc::new(MemoryStore::new());
    let tenant_hash = TenantId::new("acme").hash();
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );

    let present_key = format!(
        "t/{}/catalog/{}/part/present",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );
    let present_bytes =
        ravel_catalog::encode_part(tenant_hash.0, Signal::Metrics as u32, 1, 0, &[])
            .expect("encode a valid empty part");
    store
        .put(
            &present_key,
            Bytes::from(present_bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("seed the present part");

    let missing_key = format!(
        "t/{}/catalog/{}/part/missing",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );

    let head = SnapshotHead {
        format_version: ravel_catalog::HEAD_FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: Signal::Metrics as u32,
        shard_count: 1,
        watermark_hour: 1,
        parts: vec![
            SnapshotPartRef {
                key: present_key.clone(),
                blake3: blake3::hash(&present_bytes).as_bytes().to_vec(),
                size: present_bytes.len() as u64,
                entry_count: 0,
                watermark_hour: 0,
                min_hour: 0,
                column_stats: None,
            },
            SnapshotPartRef {
                key: missing_key.clone(),
                blake3: vec![0u8; 32],
                size: 0,
                entry_count: 0,
                watermark_hour: 1,
                min_hour: 1,
                column_stats: None,
            },
        ],
        folder_id: Uuid::new_v4().into_bytes().to_vec(),
        created_unix_ns: now_ns(),
        ..Default::default()
    };
    let head_bytes = ravel_catalog::encode_head(&head).expect("encode a valid multi-part head");
    store
        .put(&head_key, Bytes::from(head_bytes), PutOptions::default())
        .await
        .expect("seed the head");

    let mut out = String::new();
    let err = catalog::render_inspect(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        SignalArg::Metrics,
        &mut out,
    )
    .await
    .expect_err("a missing part must surface as a typed error");
    assert!(
        err.to_string().contains("missing"),
        "unexpected error: {err}"
    );

    assert!(
        out.contains("format_version"),
        "partial report lost the HEAD fields: {out}"
    );
    assert!(
        out.contains(&present_key),
        "partial report lost the successfully-fetched part that preceded the failure: {out}"
    );
    // Every LISTED part carries a field-7 line, including the one whose
    // object could not be fetched. `part_ref.column_stats` comes from the
    // HEAD protobuf already in hand, so it does not depend on the fetch, and
    // omitting the line for a failing part would reintroduce exactly the
    // omitted-line-versus-unset-field ambiguity the ABSENT marker exists to
    // remove, on the parts most worth inspecting.
    //
    // Prove-the-test: move the `column_stats (field 7)` push_str in
    // `render_inspect` back below the `store.get(...)?` and this reads 1.
    assert_eq!(
        out.matches("column_stats (field 7):").count(),
        2,
        "both listed parts must carry a field-7 line, including the one whose \
         fetch failed: {out}"
    );
    assert!(
        out.contains(&missing_key),
        "the failing part must still be listed: {out}"
    );
}

/// Deliverable 3 (#1598): HEAD field 11 (`SnapshotColumnStatsRef`, ADR-0850),
/// HEAD field 13 (`SnapshotColumnStatsPartRef`, ADR-0942), and per-part field
/// 7 (`SnapshotColumnStatsPartRef`, ADR-1413) must each print their key and
/// size when set. Only the present direction is covered here; the sibling
/// test below covers absent, because a test that only covers "present"
/// cannot detect the omitted-line-vs-unset-field ambiguity that motivated
/// this deliverable.
///
/// Prove-the-test: delete the `column_stats (field 11)` (or `field 13`, or
/// `field 7`) `push_str` line from `render_inspect`
/// (`services/ravel-cli/src/catalog.rs`) and the matching assertion below
/// fails.
#[tokio::test]
async fn inspect_prints_column_stats_refs_when_present() {
    let store = Arc::new(MemoryStore::new());
    let tenant_hash = TenantId::new("acme").hash();
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );
    let part_key = format!(
        "t/{}/catalog/{}/part/present",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );
    let part_bytes = ravel_catalog::encode_part(tenant_hash.0, Signal::Metrics as u32, 1, 0, &[])
        .expect("encode a valid empty part");
    store
        .put(
            &part_key,
            Bytes::from(part_bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("seed the part");

    let field11_key = "t/acme/catalog/metrics/idx/present-v1.cstat".to_string();
    let field13_key = "t/acme/catalog/metrics/idx/present-v2.cstat".to_string();
    let field7_key = "t/acme/catalog/metrics/idx/present-v3.cstat".to_string();

    let head = SnapshotHead {
        format_version: ravel_catalog::HEAD_FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: Signal::Metrics as u32,
        shard_count: 1,
        watermark_hour: 1,
        parts: vec![SnapshotPartRef {
            key: part_key.clone(),
            blake3: blake3::hash(&part_bytes).as_bytes().to_vec(),
            size: part_bytes.len() as u64,
            entry_count: 0,
            watermark_hour: 1,
            min_hour: 0,
            column_stats: Some(SnapshotColumnStatsPartRef {
                key: field7_key.clone(),
                blake3: vec![7u8; 32],
                size: 700,
                segment_count: 1,
                part_blake3: vec![],
            }),
        }],
        folder_id: Uuid::new_v4().into_bytes().to_vec(),
        created_unix_ns: now_ns(),
        column_stats: Some(SnapshotColumnStatsRef {
            key: field11_key.clone(),
            blake3: vec![11u8; 32],
            size: 1100,
            segment_count: 1,
            part_blake3: vec![blake3::hash(&part_bytes).as_bytes().to_vec()],
        }),
        column_stats_part: Some(SnapshotColumnStatsPartRef {
            key: field13_key.clone(),
            blake3: vec![13u8; 32],
            size: 1300,
            segment_count: 1,
            part_blake3: vec![],
        }),
        ..Default::default()
    };
    let head_bytes = ravel_catalog::encode_head(&head).expect("encode a valid head");
    store
        .put(&head_key, Bytes::from(head_bytes), PutOptions::default())
        .await
        .expect("seed the head");

    let mut out = String::new();
    catalog::render_inspect(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        SignalArg::Metrics,
        &mut out,
    )
    .await
    .expect("inspect succeeds");

    assert!(
        out.contains(&format!(
            "column_stats (field 11): key={field11_key} size=1100"
        )),
        "HEAD field 11 ref not printed: {out}"
    );
    assert!(
        out.contains(&format!(
            "column_stats_part (field 13): key={field13_key} size=1300"
        )),
        "HEAD field 13 ref not printed: {out}"
    );
    assert!(
        out.contains(&format!(
            "column_stats (field 7): key={field7_key} size=700"
        )),
        "per-part field 7 ref not printed: {out}"
    );
}

/// Deliverable 3's absent direction: when field 11, field 13, and the
/// per-part field 7 are all unset, the report prints an explicit `ABSENT`
/// marker for each rather than omitting the line. An omitted line and an
/// unset field are otherwise indistinguishable to a reader, which is exactly
/// the ambiguity that hid a real defect (#1598).
#[tokio::test]
async fn inspect_prints_absent_marker_for_unset_column_stats_refs() {
    let store = Arc::new(MemoryStore::new());
    let tenant_hash = TenantId::new("acme").hash();
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );
    let part_key = format!(
        "t/{}/catalog/{}/part/present",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );
    let part_bytes = ravel_catalog::encode_part(tenant_hash.0, Signal::Metrics as u32, 1, 0, &[])
        .expect("encode a valid empty part");
    store
        .put(
            &part_key,
            Bytes::from(part_bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("seed the part");

    let head = SnapshotHead {
        format_version: ravel_catalog::HEAD_FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: Signal::Metrics as u32,
        shard_count: 1,
        watermark_hour: 1,
        parts: vec![SnapshotPartRef {
            key: part_key.clone(),
            blake3: blake3::hash(&part_bytes).as_bytes().to_vec(),
            size: part_bytes.len() as u64,
            entry_count: 0,
            watermark_hour: 1,
            min_hour: 0,
            column_stats: None,
        }],
        folder_id: Uuid::new_v4().into_bytes().to_vec(),
        created_unix_ns: now_ns(),
        column_stats: None,
        column_stats_part: None,
        ..Default::default()
    };
    let head_bytes = ravel_catalog::encode_head(&head).expect("encode a valid head");
    store
        .put(&head_key, Bytes::from(head_bytes), PutOptions::default())
        .await
        .expect("seed the head");

    let mut out = String::new();
    catalog::render_inspect(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        SignalArg::Metrics,
        &mut out,
    )
    .await
    .expect("inspect succeeds");

    assert!(
        out.contains("column_stats (field 11): ABSENT"),
        "HEAD field 11 must print an explicit ABSENT marker: {out}"
    );
    assert!(
        out.contains("column_stats_part (field 13): ABSENT"),
        "HEAD field 13 must print an explicit ABSENT marker: {out}"
    );
    assert!(
        out.contains("column_stats (field 7): ABSENT"),
        "per-part field 7 must print an explicit ABSENT marker: {out}"
    );
}
