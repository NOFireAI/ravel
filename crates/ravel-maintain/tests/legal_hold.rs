//! Legal hold over the sweeper's `LeaseCheck` seam (ADR-0042 decision 2, issue
//! #368). Builds the same superseded-sweep scenario the crash-matrix suite
//! uses, then proves an active hold blocks the delete, a later clear releases
//! it, overlapping holds fold to latest-record-wins, and a tenant with no holds
//! sweeps byte-for-byte identically to [`NoLeases`].
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_commit::keys;
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, LeaseCheck, LegalHoldCheck, NoLeases,
    SweepReport, compact_bucket, sweep_shard, write_hold_clear, write_hold_set,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use uuid::Uuid;

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// Two compactable metrics L0 inputs (the crash-matrix fixture shape).
fn metrics_specs() -> Vec<InputSpec> {
    vec![
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("m", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("m", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "a")], &[(3_000, 3.0)])],
        ),
    ]
}

/// Seed two compactable L0 inputs and compact them, leaving the L0 inputs in
/// place (compaction never deletes; the sweep does). Returns the bucket.
async fn seed_and_compact(store: &dyn ObjectStoreBackend, clock: &FixedClock) -> Bucket {
    for spec in metrics_specs() {
        seed_input(store, &spec).await;
    }
    let bucket = bucket();
    let outcome = compact_bucket(store, clock, &cfg(), &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    bucket
}

/// A `now_ns` comfortably past the compaction record's supersession horizon.
fn past_horizon(created_ns: i64) -> i64 {
    created_ns
        .saturating_add(cfg().protection_horizon_ns)
        .saturating_add(NS_PER_HOUR)
}

/// The `t/<tenant_hex>/m/l0/<shard>/` prefix covering the bucket's L0 data
/// objects: the scope a hold on this shard's metrics inputs covers.
fn l0_data_prefix(bucket: &Bucket) -> String {
    format!(
        "t/{}/{}/l0/{:04}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        bucket.shard
    )
}

async fn l0_data_count(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> usize {
    list_all(store, &l0_data_prefix(bucket))
        .await
        .unwrap()
        .len()
}

/// Assert the compaction record and every L1 part it names are still present
/// (HEAD, no decode).
async fn assert_l1_intact(store: &dyn ObjectStoreBackend, bucket: &Bucket) {
    let record = fetch_compaction_record(store, bucket).await;
    assert!(!record.parts.is_empty());
    for part in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, part).unwrap();
        store.head(&key).await.expect("L1 part still present");
    }
}

async fn run_sweep(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    lease: &dyn ravel_maintain::LeaseCheck,
    bucket: &Bucket,
) -> SweepReport {
    sweep_shard(
        store,
        clock,
        &cfg(),
        lease,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
    )
    .await
    .expect("sweep")
}

/// A hold set over the L0 data prefix blocks the superseded sweep from deleting
/// those data objects, where an unheld sweep would have removed them.
#[tokio::test]
async fn active_hold_blocks_delete_that_would_otherwise_happen() {
    // Control: no hold, the data objects are swept.
    let control = MemoryStore::new();
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = seed_and_compact(&control, &clock).await;
    clock.set(past_horizon(created));
    let report = run_sweep(&control, &clock, &NoLeases, &bucket).await;
    assert!(report.superseded_data_deleted > 0, "control sweeps data");
    assert_eq!(
        l0_data_count(&control, &bucket).await,
        0,
        "control: data gone"
    );

    // With an active hold over the L0 data prefix, the same sweep leaves the
    // held data objects in place (and the L1 output is untouched either way).
    let held = MemoryStore::new();
    let clock = FixedClock::new(created);
    let bucket = seed_and_compact(&held, &clock).await;
    let before = l0_data_count(&held, &bucket).await;
    assert!(before > 0);

    write_hold_set(
        &held,
        &bucket.tenant_hash,
        Uuid::from_u128(100),
        created,
        &l0_data_prefix(&bucket),
        "litigation hold #42",
    )
    .await
    .expect("set hold");

    let lease = LegalHoldCheck::refresh(&held, &bucket.tenant_hash)
        .await
        .expect("refresh");
    assert_eq!(lease.active_prefixes(), [l0_data_prefix(&bucket)]);

    clock.set(past_horizon(created));
    run_sweep(&held, &clock, &lease, &bucket).await;
    assert_eq!(
        l0_data_count(&held, &bucket).await,
        before,
        "held data objects survive the sweep"
    );
    assert_l1_intact(&held, &bucket).await;
}

/// Clearing the hold (a new clear record) lets the next pass delete the key the
/// prior pass protected.
#[tokio::test]
async fn cleared_hold_lets_the_next_pass_delete() {
    let store = MemoryStore::new();
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = seed_and_compact(&store, &clock).await;
    let scope = l0_data_prefix(&bucket);

    write_hold_set(
        &store,
        &bucket.tenant_hash,
        Uuid::from_u128(100),
        created,
        &scope,
        "hold",
    )
    .await
    .unwrap();

    // First pass under the hold: data survives.
    let lease = LegalHoldCheck::refresh(&store, &bucket.tenant_hash)
        .await
        .unwrap();
    clock.set(past_horizon(created));
    run_sweep(&store, &clock, &lease, &bucket).await;
    assert!(
        l0_data_count(&store, &bucket).await > 0,
        "held during first pass"
    );

    // A later clear record releases the scope; re-fold and re-sweep.
    write_hold_clear(
        &store,
        &bucket.tenant_hash,
        Uuid::from_u128(101),
        created + 1,
        &scope,
    )
    .await
    .unwrap();
    let lease = LegalHoldCheck::refresh(&store, &bucket.tenant_hash)
        .await
        .unwrap();
    assert!(lease.is_empty(), "clear folded to no active holds");

    run_sweep(&store, &clock, &lease, &bucket).await;
    assert_eq!(
        l0_data_count(&store, &bucket).await,
        0,
        "the cleared key can now be deleted"
    );
    assert_l1_intact(&store, &bucket).await;
}

/// Two overlapping/conflicting holds on the same scope resolve by
/// latest-record-wins (ADR-0040 fold rule): a set then a later clear leaves the
/// scope unheld; the reverse leaves it held.
#[tokio::test]
async fn conflicting_holds_resolve_by_latest_record() {
    let store = MemoryStore::new();
    let tenant = tenant_hash();
    let scope_a = format!("t/{}/m/l0/0007/", tenant.to_hex());
    let scope_b = format!("t/{}/l/l0/0007/", tenant.to_hex());

    // scope_a: set@100 then clear@200 -> unheld.
    write_hold_set(&store, &tenant, Uuid::from_u128(1), 100, &scope_a, "x")
        .await
        .unwrap();
    write_hold_clear(&store, &tenant, Uuid::from_u128(2), 200, &scope_a)
        .await
        .unwrap();
    // scope_b: clear@100 then set@200 -> held.
    write_hold_clear(&store, &tenant, Uuid::from_u128(3), 100, &scope_b)
        .await
        .unwrap();
    write_hold_set(&store, &tenant, Uuid::from_u128(4), 200, &scope_b, "y")
        .await
        .unwrap();

    let lease = LegalHoldCheck::refresh(&store, &tenant).await.unwrap();
    assert_eq!(lease.active_prefixes(), std::slice::from_ref(&scope_b));
    assert!(lease.is_protected(&format!("{scope_b}obj")));
    assert!(!lease.is_protected(&format!("{scope_a}obj")));
}

/// A tenant with no configured holds sweeps byte-for-byte identically to
/// [`NoLeases`]: same report and same surviving object set.
#[tokio::test]
async fn empty_holds_equals_no_leases() {
    let created = sealed_now_ns();

    let no_leases_store = MemoryStore::new();
    let clock = FixedClock::new(created);
    let bucket = seed_and_compact(&no_leases_store, &clock).await;
    clock.set(past_horizon(created));
    let report_no_leases = run_sweep(&no_leases_store, &clock, &NoLeases, &bucket).await;
    let remaining_no_leases = all_keys(&no_leases_store).await;

    let empty_store = MemoryStore::new();
    let clock = FixedClock::new(created);
    let bucket = seed_and_compact(&empty_store, &clock).await;
    // Refresh against a tenant that never set a hold: an empty snapshot.
    let lease = LegalHoldCheck::refresh(&empty_store, &bucket.tenant_hash)
        .await
        .unwrap();
    assert!(lease.is_empty());
    clock.set(past_horizon(created));
    let report_empty = run_sweep(&empty_store, &clock, &lease, &bucket).await;
    let remaining_empty = all_keys(&empty_store).await;

    assert_eq!(report_no_leases, report_empty, "identical sweep reports");
    assert_eq!(
        remaining_no_leases, remaining_empty,
        "identical surviving object set"
    );
}

/// Every object key under the tenant prefix, sorted.
async fn all_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let prefix = format!("t/{}/", tenant_hash().to_hex());
    let mut keys: Vec<String> = list_all(store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .collect();
    keys.sort();
    keys
}
