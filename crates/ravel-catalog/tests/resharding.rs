//! Integration tests for generation-versioned resharding on the read side
//! (ADR-0052 sections 4 and 5). Drives `Catalog::resolve`
//! and `Catalog::fold` through the public API against a durable provisioning
//! record carrying a shard-generation history, proving the epic's acceptance
//! property at the unit/integration level: a query spanning a reshard's
//! activation returns complete results from both the old and new shard ranges.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::{
    AbsentPolicy, Catalog, CatalogConfig, CatalogError, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, append_generation, decode_head,
    encode_head, validate_or_adopt,
};
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{SnapshotHead, SnapshotPartRef};
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn tenant() -> TenantHash {
    TenantHash([0x5a; 16])
}

fn config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        ..Default::default()
    }
}

fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn head_key(tenant: &TenantHash) -> String {
    format!(
        "t/{}/catalog/{}/HEAD",
        tenant.to_hex(),
        Signal::Metrics.key_prefix()
    )
}

/// Enforcement-enabled catalog whose configured `shard_count` equals
/// generation 0's count (the scalar the record records), so the startup
/// provisioning check matches.
fn enforcing_catalog(store: Arc<dyn ObjectStoreBackend>, gen0_count: u32) -> Catalog {
    Catalog::new(store, config(gen0_count))
        .expect("catalog")
        .with_provisioning_enforcement()
}

/// Seed a two-generation provisioning record: generation 0 at `gen0_count`
/// (activation hour 0) and generation 1 at `gen1_count` activating at
/// `activation_hour`. Uses the real write path (`validate_or_adopt` then
/// `append_generation`), so the record is exactly what a live reshard writes.
async fn seed_two_generations(
    store: &dyn ObjectStoreBackend,
    gen0_count: u32,
    gen1_count: u32,
    activation_hour: u32,
) {
    validate_or_adopt(
        store,
        &tenant(),
        Signal::Metrics,
        gen0_count,
        0,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("create generation 0");
    // append_generation requires the activation strictly in the future
    // relative to its own `now`; place `now` one hour before it.
    let append_now_ns = i64::from(activation_hour - 1) * NS_PER_HOUR;
    append_generation(
        store,
        &tenant(),
        Signal::Metrics,
        gen1_count,
        activation_hour,
        append_now_ns,
    )
    .await
    .expect("append generation 1");
}

#[allow(clippy::too_many_arguments)]
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    seq: u64,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
) -> String {
    let writer_id = Uuid::new_v4();
    let payload = format!("seg-{shard}-{writer_id}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id,
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
    data_key
}

fn mid_hour(hour: u32) -> i64 {
    i64::from(hour) * NS_PER_HOUR + 1_000_000_000
}

/// The epic's acceptance property (unit/integration level): with an increase
/// (generation 0 count 2, generation 1 count 4), a query spanning the
/// activation returns segments from both the old shard range (shard 1, only
/// valid under count 2) and the new shard range (shard 3, only valid under
/// count 4). Under the old static `0..shard_count` fan-out the shard-3 segment
/// would be silently dropped; the per-hour `scan_count` finds it.
#[tokio::test]
async fn resolve_across_increase_returns_both_shard_ranges() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let old_hour = 500_000u32;
    let activation = 500_002u32;
    let new_hour = 500_003u32;
    seed_two_generations(store.as_ref(), 2, 4, activation).await;

    // Old-range write: shard 1, before the activation.
    let old_key = publish_segment(store.as_ref(), 1, 1, old_hour, mid_hour(old_hour)).await;
    // New-range write: shard 3, at/after the activation (only reachable once
    // the count grew to 4).
    let new_key = publish_segment(store.as_ref(), 3, 1, new_hour, mid_hour(new_hour)).await;

    let catalog = enforcing_catalog(store.clone(), 2);
    let now = now_at_seal(new_hour);
    let range = TimeRange {
        start_ns: i64::from(old_hour) * NS_PER_HOUR,
        end_ns: now,
    };
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve across the reshard boundary");
    let keys: Vec<&str> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.as_str())
        .collect();
    assert!(
        keys.contains(&old_key.as_str()),
        "old-range (shard 1) segment must be resolved"
    );
    assert!(
        keys.contains(&new_key.as_str()),
        "new-range (shard 3) segment must be resolved -- the generation-aware \
         scan set, not the static count-2 fan-out, is what finds it"
    );
    assert_eq!(snapshot.segments.len(), 2);
}

/// A decrease (generation 0 count 4, generation 1 count 2) with a straggler
/// write routed under the old, larger count into an early hour of the new
/// generation, inside the slack window. The straggler lands in shard 3 (only
/// valid under count 4) at the activation hour; `scan_count`'s slack keeps
/// count 4 in the scan set for `S` hours, so the straggler is still found.
#[tokio::test]
async fn resolve_across_decrease_finds_straggler_in_slack_window() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let activation = 500_010u32;
    seed_two_generations(store.as_ref(), 4, 2, activation).await;

    // A straggler routed under the retiring count 4 into the activation hour
    // itself (within the slack window): shard 3, hour == activation.
    let straggler_key =
        publish_segment(store.as_ref(), 3, 1, activation, mid_hour(activation)).await;
    // A normal new-generation write at the same hour under count 2: shard 1.
    let new_key = publish_segment(store.as_ref(), 1, 1, activation, mid_hour(activation)).await;

    let catalog = enforcing_catalog(store.clone(), 4);
    let now = now_at_seal(activation);
    let range = TimeRange {
        start_ns: i64::from(activation) * NS_PER_HOUR,
        end_ns: now,
    };
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve across the decrease boundary");
    let keys: Vec<&str> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.as_str())
        .collect();
    assert!(
        keys.contains(&straggler_key.as_str()),
        "the straggler in shard 3 (old count) inside the slack window must be found"
    );
    assert!(
        keys.contains(&new_key.as_str()),
        "the new-count write is found"
    );
    assert_eq!(snapshot.segments.len(), 2);
}

/// A fold writes `SnapshotHead.shard_count` as the fan-out ceiling at fold
/// time and `shard_generation_count` as the number of generations known then
/// (ADR-0052 section 5). After an increase to count 4 activated before the
/// folded watermark, the ceiling is 4 and the generation count is 2.
#[tokio::test]
async fn fold_records_ceiling_and_generation_count() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let activation = 500_000u32;
    let watermark = 500_005u32;
    seed_two_generations(store.as_ref(), 2, 4, activation).await;

    // Data under both generations, all sealed by the fold's `now`.
    publish_segment(store.as_ref(), 1, 1, 499_999, mid_hour(499_999)).await;
    publish_segment(store.as_ref(), 3, 1, watermark, mid_hour(watermark)).await;

    let catalog = enforcing_catalog(store.clone(), 2);
    let report = catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_at_seal(watermark),
            &[],
        )
        .await
        .expect("fold");
    assert!(!report.no_op);

    let got = store
        .get(&head_key(&tenant()), GetRange::Full)
        .await
        .expect("HEAD present");
    let head = decode_head(&got.data).expect("decode HEAD");
    assert_eq!(
        head.shard_count, 4,
        "fold-time fan-out ceiling is the larger generation's count"
    );
    assert_eq!(
        head.shard_generation_count, 2,
        "two generations were known at fold time"
    );
}

/// A pre-reshard HEAD (a lower `shard_generation_count` than the reader now
/// knows) is accepted, not rejected as a `shard_count` mismatch: Phase 1
/// listing covers the newer hours it lacks. Proven end to end: fold under a
/// single generation, then reshard, then resolve a window spanning both the
/// snapshot's hours and a new-generation hour, and confirm the resolve
/// succeeds and returns both the folded old data and the new-range segment.
#[tokio::test]
async fn pre_reshard_head_is_accepted_after_a_later_reshard() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let folded_hour = 500_000u32;

    // Generation 0 only, then fold: HEAD carries shard_generation_count = 1.
    validate_or_adopt(
        store.as_ref(),
        &tenant(),
        Signal::Metrics,
        2,
        0,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("create generation 0");
    let old_key = publish_segment(store.as_ref(), 1, 1, folded_hour, mid_hour(folded_hour)).await;
    let fold_catalog = enforcing_catalog(store.clone(), 2);
    fold_catalog
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_at_seal(folded_hour),
            &[],
        )
        .await
        .expect("fold under a single generation");
    let head_before = decode_head(
        &store
            .get(&head_key(&tenant()), GetRange::Full)
            .await
            .expect("HEAD present")
            .data,
    )
    .expect("decode");
    assert_eq!(head_before.shard_generation_count, 1, "pre-reshard head");

    // Now reshard to count 4, activating after the folded watermark, and write
    // a new-range segment in a later hour.
    let activation = 500_002u32;
    let new_hour = 500_003u32;
    append_generation(
        store.as_ref(),
        &tenant(),
        Signal::Metrics,
        4,
        activation,
        i64::from(activation - 1) * NS_PER_HOUR,
    )
    .await
    .expect("append generation 1");
    let new_key = publish_segment(store.as_ref(), 3, 1, new_hour, mid_hour(new_hour)).await;

    // A fresh reader (no cached HEAD) that now knows two generations.
    let catalog = enforcing_catalog(store.clone(), 2);
    let now = now_at_seal(new_hour);
    let range = TimeRange {
        start_ns: i64::from(folded_hour) * NS_PER_HOUR,
        end_ns: now,
    };
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("a pre-reshard HEAD must be accepted, never a shard_count hard error");
    let keys: Vec<&str> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.as_str())
        .collect();
    assert!(
        keys.contains(&old_key.as_str()),
        "the folded old-range segment is served from the pre-reshard snapshot"
    );
    assert!(
        keys.contains(&new_key.as_str()),
        "the new-range segment in a hour above the watermark is served by listing"
    );
}

/// A HEAD claiming more generations than the reader's provisioning record
/// knows, whose `shard_count` also fails to match the reader's ceiling, must
/// fail closed with a `shard_count` `FieldMismatch` after the single record
/// re-read -- never be served from a possibly-stale view that could
/// under-scan (ADR-0052 section 5).
#[tokio::test]
async fn head_ahead_of_reader_fails_closed() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let watermark = 500_000u32;

    // Reader knows only generation 0 at count 2.
    validate_or_adopt(
        store.as_ref(),
        &tenant(),
        Signal::Metrics,
        2,
        0,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("create generation 0");

    // A hand-written HEAD claiming two generations and a bogus ceiling of 99.
    // Validation rejects it before any part is loaded, so a dummy part ref is
    // enough to pass the envelope's structural checks.
    let head = SnapshotHead {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as u32,
        shard_count: 99,
        watermark_hour: watermark,
        parts: vec![SnapshotPartRef {
            key: "t/x/catalog/m/snap/unused.aaaa.csnap".to_string(),
            blake3: vec![0x11; 32],
            size: 1,
            entry_count: 0,
            watermark_hour: watermark,
            min_hour: 0,
        }],
        postings: None,
        folder_id: vec![0x22; 16],
        created_unix_ns: 0,
        shard_generation_count: 2,
    };
    store
        .put(
            &head_key(&tenant()),
            Bytes::from(encode_head(&head).expect("encode head")),
            PutOptions::default(),
        )
        .await
        .expect("put head");

    let catalog = enforcing_catalog(store.clone(), 2);
    let now = now_at_seal(watermark);
    let range = TimeRange {
        start_ns: i64::from(watermark) * NS_PER_HOUR,
        end_ns: now,
    };
    let err = catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect_err("a HEAD ahead of the reader with a wrong ceiling must fail closed");
    match err {
        CatalogError::FieldMismatch { field, actual, .. } => {
            assert_eq!(field, "shard_count");
            assert_eq!(actual, "99");
        }
        other => panic!("expected FieldMismatch shard_count, got {other:?}"),
    }
}
