//! ADR-0052 section 4 + ADR-0056: the prefix-scan traversal path
//! must derive its shard-index domain from the generation history, exactly as
//! the per-bucket path does, so a shard-count reshard is resolved completely on
//! either path.
//!
//! The gap this file closes: `Catalog::list_window_by_prefix` used to loop
//! `0..self.config.shard_count` (the current, static count). On a shard-count
//! DECREASE the retiring larger generation's higher shard indices stay in scope
//! for `DEFAULT_SCAN_SLACK_HOURS` past the successor's activation (the read-side
//! scan rule, `scan_count`); the prefix path never scanned them, so a straggler
//! commit record routed under the old count into an early successor hour was
//! silently dropped. The per-bucket path already got this right via
//! `scan_count` per hour; these tests extend the differential property to
//! the prefix path and pin the decrease case it used to miss.
//!
//! Enforcement must be on: only then is the durable generation history read
//! (a non-enforcing catalog treats `shard_count` as a static config and scans
//! the single implicit generation 0). The traversal path is forced by config,
//! as in `resolve_prefix_traversal.rs`: `prefix_list_crossover_requests = 0`
//! pins the prefix scan, `u64::MAX` pins the per-bucket loop; both run over the
//! same corpus and are compared.
//!
//! The fixed prefix path is a strict correctness superset of the per-bucket
//! path: `max_scan_count_over_range` takes the max shard count seen across the
//! whole queried range, so the prefix path can over-scan shard indices that no
//! hour in range actually needs, but it never under-scans one a per-bucket hour
//! would have listed. The two paths' results converge on the same segment set;
//! only their LIST cost can differ.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::{AbsentPolicy, Catalog, CatalogConfig, append_generation, validate_or_adopt};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

fn tenant() -> TenantHash {
    TenantHash([0x5a; 16])
}

/// Enforcement-enabled catalog forced onto the prefix scan (crossover 0), with
/// an unbounded runtime ceiling. `gen0_count` is generation 0's count; the
/// caller sets `CatalogConfig.shard_count` to it by convention (ADR-0082
/// no longer requires the two to agree -- the enforced check only requires
/// the record to be present and decodable, and routing comes from the
/// generation history regardless of this catalog's configured value).
fn prefix_enforcing(store: Arc<dyn ObjectStoreBackend>, gen0_count: u32) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count: gen0_count,
            prefix_list_crossover_requests: 0,
            max_catalog_list_requests: u64::MAX,
            ..Default::default()
        },
    )
    .expect("catalog")
    .with_provisioning_enforcement()
}

/// Enforcement-enabled catalog forced onto the per-bucket loop (crossover
/// `u64::MAX`), the differential control.
fn per_bucket_enforcing(store: Arc<dyn ObjectStoreBackend>, gen0_count: u32) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count: gen0_count,
            prefix_list_crossover_requests: u64::MAX,
            max_catalog_list_requests: u64::MAX,
            ..Default::default()
        },
    )
    .expect("catalog")
    .with_provisioning_enforcement()
}

/// Seed a two-generation provisioning record via the real write path, so it is
/// exactly what a live reshard writes: generation 0 at `gen0_count` (activation
/// hour 0) and generation 1 at `gen1_count` activating at `activation_hour`.
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

/// Seed a three-generation provisioning record: generation 0 at `gen0_count`
/// (hour 0), generation 1 at `gen1_count` activating at `gen1_activation`,
/// generation 2 at `gen2_count` activating at `gen2_activation`.
///
/// These helpers set `CatalogConfig.shard_count` to generation 0's count by
/// convention (see `prefix_enforcing`), so a two-generation decrease alone can
/// never expose a static-`shard_count` bug: whichever count generation 0 has
/// is also the configured value, and if generation 0 is the larger of the two
/// counts the old static bound already covered the retiring generation by
/// coincidence. A middle generation wider than generation 0 is the only shape
/// that exposes it: `gen0_count < gen1_count`, `gen2_count <= gen0_count`, so
/// the configured value (`gen0_count`) is narrower than the retiring
/// generation 1's count and the bug is real, not accidentally masked.
async fn seed_three_generations(
    store: &dyn ObjectStoreBackend,
    gen0_count: u32,
    gen1_count: u32,
    gen1_activation: u32,
    gen2_count: u32,
    gen2_activation: u32,
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
    append_generation(
        store,
        &tenant(),
        Signal::Metrics,
        gen1_count,
        gen1_activation,
        i64::from(gen1_activation - 1) * NS_PER_HOUR,
    )
    .await
    .expect("append generation 1");
    append_generation(
        store,
        &tenant(),
        Signal::Metrics,
        gen2_count,
        gen2_activation,
        i64::from(gen2_activation - 1) * NS_PER_HOUR,
    )
    .await
    .expect("append generation 2");
}

/// Publish one L0 commit record (and its data object) in `(shard,
/// ingest_hour_bucket)` with the given event ts, returning its data-object key.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    ingest_hour_bucket: u32,
    event_ts_ns: i64,
) -> String {
    let writer_id = Uuid::new_v4();
    let payload = format!("seg-{shard}-{writer_id}-{ingest_hour_bucket}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: event_ts_ns,
        max_event_ts_ns: event_ts_ns,
        min_ingest_ts_ns: event_ts_ns,
        max_ingest_ts_ns: event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: event_ts_ns,
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
    i64::from(hour) * NS_PER_HOUR + 60_000_000_000
}

fn data_keys(snap: &ravel_catalog::Snapshot) -> Vec<String> {
    let mut keys: Vec<String> = snap
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    keys.sort();
    keys
}

/// A shard-count DECREASE through a wider MIDDLE generation: generation 0 at
/// count 4 (hour 0), generation 1 at count 8 (hour 100), generation 2 back
/// down to count 4 (hour 200). Enforcement pins the configured `shard_count`
/// to generation 0's count (4), so the bug this test pins is invisible to any
/// two-generation shape where generation 0 already holds the larger count --
/// only a middle generation wider than generation 0 exposes the static
/// `0..config.shard_count` bound the prefix path used to have. A straggler
/// commit record is routed under the retiring count-8 generation into shard 6
/// at hour 200, inside generation 1's `S`-hour slack window past generation
/// 2's activation. Resolved through the prefix path, the straggler is found
/// only because the shard bound is now the generation-aware union scan set
/// (`max_scan_count_over_range`), not the static configured count 4. The
/// per-bucket control returns the byte-identical segment set. This test fails
/// on the unfixed parent commit (`f3ea506`) and passes once the prefix path
/// consults generation history (`94a3f5e`).
#[tokio::test]
async fn prefix_decrease_finds_straggler_in_retiring_shard_and_matches_per_bucket() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    seed_three_generations(store.as_ref(), 4, 8, 100, 4, 200).await;

    // Straggler routed under the retiring count-8 generation into shard 6
    // (only valid under count 8) at hour 200, inside generation 1's slack
    // window past generation 2's activation.
    let straggler_key = publish_segment(store.as_ref(), 6, 200, mid_hour(200)).await;
    // A normal generation-2 write at the same hour under count 4: shard 1.
    let new_key = publish_segment(store.as_ref(), 1, 200, mid_hour(200)).await;
    // A generation-1 write in an earlier hour, shard 7 (only valid under 8).
    let old_key = publish_segment(store.as_ref(), 7, 150, mid_hour(150)).await;

    // Window wide enough to include every bucket above and route to the
    // prefix path (crossover forced to 0). `now` seals hour 200.
    let now_ns = 202 * NS_PER_HOUR;
    let range = TimeRange {
        start_ns: 0,
        end_ns: now_ns,
    };

    let prefix = prefix_enforcing(store.clone(), 4);
    let per_bucket = per_bucket_enforcing(store.clone(), 4);
    let snap_px = prefix
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("prefix resolve across the decrease boundary");
    let snap_pb = per_bucket
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("per-bucket resolve across the decrease boundary");

    assert_eq!(
        snap_px, snap_pb,
        "prefix and per-bucket paths must return byte-identical snapshots \
         across a shard-count decrease"
    );

    let got = data_keys(&snap_px);
    assert!(
        got.contains(&straggler_key),
        "the straggler in retiring shard 6 inside the slack window must be found \
         by the prefix path: got {got:?}"
    );
    assert!(got.contains(&new_key), "the new-count write is found");
    assert!(
        got.contains(&old_key),
        "the pre-decrease write in retiring shard 7 is found"
    );
    assert_eq!(
        snap_px.segments.len(),
        3,
        "exactly the three planted records"
    );
}

/// A shard-count INCREASE (generation 0 count 4, generation 1 count 8)
/// through the prefix path. Unlike the decrease case, this one already fails
/// on the unfixed parent commit (`f3ea506`): the old static `0..shard_count`
/// bound scans only 0..4, so the new-range write in shard 6 is unconditionally
/// dropped by the prefix path on every increase, not just inside a slack
/// window. This test is the regression pin for that half of the bug; the
/// decrease test above pins the slack-window-scoped half.
#[tokio::test]
async fn prefix_increase_returns_both_shard_ranges() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let activation = 10u32;
    seed_two_generations(store.as_ref(), 4, 8, activation).await;

    // Old-range write: shard 1, before the activation (valid under count 4).
    let old_key =
        publish_segment(store.as_ref(), 1, activation - 3, mid_hour(activation - 3)).await;
    // New-range write: shard 6, at/after the activation (only reachable once
    // the count grew to 8).
    let new_key = publish_segment(store.as_ref(), 6, activation, mid_hour(activation)).await;

    let now_ns = i64::from(activation + 1) * NS_PER_HOUR + NS_PER_HOUR;
    let range = TimeRange {
        start_ns: 0,
        end_ns: now_ns,
    };

    let prefix = prefix_enforcing(store.clone(), 4);
    let per_bucket = per_bucket_enforcing(store.clone(), 4);
    let snap_px = prefix
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("prefix resolve across the increase boundary");
    let snap_pb = per_bucket
        .resolve(&tenant(), Signal::Metrics, range, &[], now_ns)
        .await
        .expect("per-bucket resolve across the increase boundary");

    assert_eq!(
        snap_px, snap_pb,
        "prefix and per-bucket paths must agree across a shard-count increase"
    );
    let got = data_keys(&snap_px);
    assert!(
        got.contains(&old_key),
        "old-range (shard 1) segment resolved"
    );
    assert!(
        got.contains(&new_key),
        "new-range (shard 6) segment resolved: the generation-aware union scan \
         set, not the static count-4 fan-out, is what finds it"
    );
    assert_eq!(snap_px.segments.len(), 2);
}
