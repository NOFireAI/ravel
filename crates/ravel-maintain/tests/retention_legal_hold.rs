//! Legal hold over the physical retention sweep (issue #1697): a hold on any
//! part of a tombstoned bucket must stop the whole sweep, not let it delete
//! the rest.
//!
//! The bug this pins is not "held bytes were deleted". It is the opposite
//! shape: the held data objects survived while the commit records that name
//! them, the L1 parts beside them, and the tombstone that excludes the bucket
//! were deleted around them, leaving bytes nothing can read and nothing can
//! ever sweep. So every test here asserts the exact surviving key set under the
//! tenant prefix, byte for byte against the set present before the sweep, not a
//! count and not the survival of the held keys alone.
//!
//! The fixture is a compacted bucket so all four object classes the sweep
//! deletes are really present (L0 commit records, a compaction record, L0 data
//! objects, L1 parts) plus the tombstone. The rewrite-record class is covered
//! by the `sweep_delete_keys` unit test in `src/retention.rs`, which drives the
//! delete set directly.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;

use std::collections::BTreeSet;
use std::sync::Mutex;

use ravel_commit::keys;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::retention::held_by_lease_buckets_total;
use ravel_maintain::{
    Bucket, Clock, CompactionOutcome, CompactorConfig, FixedClock, LeaseCheck, NoLeases,
    RetentionConfig, RetentionOutcome, RetentionPolicy, compact_bucket, retention_sweep_bucket,
};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use uuid::Uuid;

/// Serializes the tests that assert on the process-wide held-bucket counter, so
/// each one's before/after delta covers only its own sweep. Every test in this
/// binary runs a sweep, so all of them take it (the same rule
/// `retention_version_window.rs` follows for its own counter).
static COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// A retention config whose window for the test tenant is exactly the floor, so
/// the tiny-timestamp fixtures are always expired.
fn retention_at_floor(config: &CompactorConfig) -> RetentionConfig {
    let floor = config.retention_floor_ns(DEFAULT_MAX_INGEST_LAG_NS);
    RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: vec![(TENANT.to_string(), floor)],
        },
        config,
        DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("valid retention config")
}

/// A [`LeaseCheck`] protecting every key under any of a fixed set of prefixes,
/// recording every key it was asked about. The prefix match is
/// `LegalHoldCheck`'s own rule, so a scope here behaves exactly as a real hold
/// record carrying the same scope string would.
#[derive(Default)]
struct RecordingHold {
    prefixes: Vec<String>,
    asked: Mutex<Vec<String>>,
}

impl RecordingHold {
    fn new(prefixes: &[String]) -> Self {
        RecordingHold {
            prefixes: prefixes.to_vec(),
            asked: Mutex::new(Vec::new()),
        }
    }

    fn asked(&self) -> BTreeSet<String> {
        self.asked
            .lock()
            .expect("asked lock")
            .iter()
            .cloned()
            .collect()
    }
}

impl LeaseCheck for RecordingHold {
    fn is_protected(&self, key: &str) -> bool {
        self.asked.lock().expect("asked lock").push(key.to_string());
        self.prefixes.iter().any(|p| key.starts_with(p.as_str()))
    }
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

/// Seed two L0 inputs and compact them, so the bucket holds every object class
/// the physical sweep deletes: L0 commit records, a compaction record, L0 data
/// objects and L1 parts. Compaction never deletes its inputs; the sweep does.
/// Returns the bucket and its L0 commit-record keys.
async fn seed_compacted_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
) -> (Bucket, Vec<String>) {
    let mut commit_keys = Vec::new();
    for spec in metrics_specs() {
        commit_keys.push(seed_input(store, &spec).await);
    }
    let bucket = bucket();
    let outcome = compact_bucket(store, clock, &cfg(), &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    (bucket, commit_keys)
}

async fn all_keys(store: &dyn ObjectStoreBackend) -> BTreeSet<String> {
    list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.key)
        .collect()
}

/// Tombstone the bucket, then return the clock advanced past the protection
/// horizon and the exact key set present at that moment.
async fn tombstone_and_arm(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    clock: &FixedClock,
    config: &CompactorConfig,
    retention: &RetentionConfig,
) -> BTreeSet<String> {
    let created = clock.now_ns();
    let out = retention_sweep_bucket(store, clock, config, retention, &NoLeases, bucket)
        .await
        .expect("tombstone pass");
    assert_eq!(out, RetentionOutcome::Tombstoned);
    clock.set(created + config.protection_horizon_ns + 1);
    all_keys(store).await
}

/// The `t/<tenant_hex>/m/l0/<shard>/` prefix: the L0 data objects alone, one of
/// the three prefixes `shard_hold_scopes` returns for this shard.
fn l0_data_prefix(bucket: &Bucket) -> String {
    format!(
        "t/{}/{}/l0/{:04}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        bucket.shard
    )
}

/// Every L1 part key the bucket's compaction record names.
async fn l1_part_keys(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> Vec<String> {
    let record = fetch_compaction_record(store, bucket).await;
    assert!(!record.parts.is_empty(), "the fixture really compacted");
    record
        .parts
        .iter()
        .map(|part| keys::reconstruct_l1_part_key(&record, part).expect("part key"))
        .collect()
}

/// Control: with nothing held, the horizon-elapsed sweep retires the bucket.
/// Without this, every test below could pass on a build that never sweeps.
#[tokio::test]
async fn an_unheld_bucket_is_swept_and_counts_no_hold() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let clock = FixedClock::new(sealed_now_ns());
    let (bucket, _commit_keys) = seed_compacted_bucket(&store, &clock).await;
    let armed = tombstone_and_arm(&store, &bucket, &clock, &config, &retention).await;
    assert!(!armed.is_empty(), "the fixture really seeded objects");

    let before = held_by_lease_buckets_total();
    let swept = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(swept, RetentionOutcome::Swept);
    assert_eq!(
        all_keys(&store).await,
        BTreeSet::new(),
        "an expired, unheld bucket is fully retired"
    );
    assert_eq!(
        held_by_lease_buckets_total() - before,
        0,
        "no bucket was held"
    );
}

/// A hold over the L0 data prefix alone, which is one of the three prefixes
/// `shard_hold_scopes` returns, blocks the entire retention sweep.
///
/// Before the gate this returned `Swept`: the L0 data objects survived under
/// the hold while the commit records naming them, the compaction record, the L1
/// parts and the tombstone were all deleted around them.
#[tokio::test]
async fn a_hold_on_the_l0_prefix_alone_blocks_the_whole_retention_sweep() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let clock = FixedClock::new(sealed_now_ns());
    let (bucket, commit_keys) = seed_compacted_bucket(&store, &clock).await;
    let armed = tombstone_and_arm(&store, &bucket, &clock, &config, &retention).await;

    let hold = RecordingHold::new(&[l0_data_prefix(&bucket)]);
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &hold, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(
        out,
        RetentionOutcome::SweptPartial,
        "a hold on part of the bucket parks the whole retirement"
    );
    let after = all_keys(&store).await;
    assert_eq!(
        after, armed,
        "the key set is byte-identical to the pre-sweep set: every commit record, every data \
         object, every L1 part and the tombstone survive"
    );
    for key in &commit_keys {
        assert!(
            after.contains(key),
            "the commit record naming held data survives: {key}"
        );
    }
    let tombstone = keys::retention_tombstone_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .expect("tombstone key");
    assert!(
        after.contains(&tombstone),
        "the tombstone survives, so the bucket stays excluded and a later pass can finish"
    );
}

/// The mirror: a hold on a single commit-record key, holding no data object at
/// all, blocks the sweep just as completely. A gate that only consulted the
/// data-object classes would pass this and delete the held record.
#[tokio::test]
async fn a_hold_on_one_commit_record_alone_blocks_the_whole_retention_sweep() {
    let _guard = COUNTER_LOCK.lock().await;
    let store = MemoryStore::new();
    let config = cfg();
    let retention = retention_at_floor(&config);
    let clock = FixedClock::new(sealed_now_ns());
    let (bucket, commit_keys) = seed_compacted_bucket(&store, &clock).await;
    let armed = tombstone_and_arm(&store, &bucket, &clock, &config, &retention).await;

    // One exact commit-record key, not a prefix: the narrowest possible hold.
    let hold = RecordingHold::new(&[commit_keys[0].clone()]);
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &hold, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(out, RetentionOutcome::SweptPartial);
    assert_eq!(
        all_keys(&store).await,
        armed,
        "one held commit record parks the whole bucket, data objects included"
    );
}

/// The counter reads exactly one held bucket after a held sweep, and exactly
/// zero after an unheld sweep of an identical bucket. Pins that it counts
/// buckets per declining pass, not keys and not passes in general.
#[tokio::test]
async fn the_counter_counts_one_per_held_bucket() {
    let _guard = COUNTER_LOCK.lock().await;
    let config = cfg();
    let retention = retention_at_floor(&config);

    let held_store = MemoryStore::new();
    let clock = FixedClock::new(sealed_now_ns());
    let (bucket, _commit_keys) = seed_compacted_bucket(&held_store, &clock).await;
    tombstone_and_arm(&held_store, &bucket, &clock, &config, &retention).await;
    // The hold covers all three shard prefixes, so many keys are protected in
    // one pass: the counter must still move by one.
    let hold = RecordingHold::new(&[format!("t/{}/", bucket.tenant_hash.to_hex())]);

    let before = held_by_lease_buckets_total();
    let out = retention_sweep_bucket(&held_store, &clock, &config, &retention, &hold, &bucket)
        .await
        .expect("sweep pass");
    assert_eq!(out, RetentionOutcome::SweptPartial);
    assert_eq!(
        held_by_lease_buckets_total() - before,
        1,
        "one held bucket counts one, however many of its keys are protected"
    );

    let unheld_store = MemoryStore::new();
    let clock = FixedClock::new(sealed_now_ns());
    let (bucket, _commit_keys) = seed_compacted_bucket(&unheld_store, &clock).await;
    tombstone_and_arm(&unheld_store, &bucket, &clock, &config, &retention).await;

    let before = held_by_lease_buckets_total();
    let out = retention_sweep_bucket(
        &unheld_store,
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("sweep pass");
    assert_eq!(out, RetentionOutcome::Swept);
    assert_eq!(
        held_by_lease_buckets_total() - before,
        0,
        "an identical bucket swept unheld counts nothing"
    );
}

/// The gate asks the `LeaseCheck` about every key class the sweep would delete,
/// and reaches no delete at all when one is held.
///
/// A `FaultStore` arms a fault on every `Delete`, so any delete the gate failed
/// to prevent is observable as a fired fault rather than only as a missing
/// object. The control half proves the fault is really armed and really
/// reachable: the same store and the same plan, swept with `NoLeases`, fires it
/// on the first delete. Without that half, "the fault did not fire" would be
/// satisfied by a plan that could never fire.
#[tokio::test]
async fn the_gate_asks_about_every_key_class_and_attempts_no_delete() {
    let _guard = COUNTER_LOCK.lock().await;
    let config = cfg();
    let retention = retention_at_floor(&config);

    let plan = FaultPlan::empty().with_rule(Rule::new(Op::Delete, ScriptedFault::Timeout));
    let store = FaultStore::new(MemoryStore::new(), plan);
    let clock = FixedClock::new(sealed_now_ns());
    let (bucket, commit_keys) = seed_compacted_bucket(&store, &clock).await;
    let data = data_keys_of(&store, &commit_keys).await;
    let l1 = l1_part_keys(&store, &bucket).await;
    let compaction_record = compaction_record_key(&store, &bucket).await;
    let tombstone = keys::retention_tombstone_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .expect("tombstone key");
    let armed = tombstone_and_arm(&store, &bucket, &clock, &config, &retention).await;

    let hold = RecordingHold::new(&[l0_data_prefix(&bucket)]);
    let out = retention_sweep_bucket(&store, &clock, &config, &retention, &hold, &bucket)
        .await
        .expect("sweep pass");

    assert_eq!(out, RetentionOutcome::SweptPartial);
    assert_eq!(
        store.fault_count(Op::Delete, FaultKind::Timeout),
        0,
        "the gate refused before any delete was attempted"
    );
    assert_eq!(all_keys(&store).await, armed);

    // Every class of key the sweep deletes was offered to the check: L0 commit
    // records, the compaction record, L0 data objects, L1 parts, and the
    // tombstone.
    let asked = hold.asked();
    for key in commit_keys
        .iter()
        .chain(std::iter::once(&compaction_record))
        .chain(data.iter())
        .chain(l1.iter())
        .chain(std::iter::once(&tombstone))
    {
        assert!(asked.contains(key), "the check was never asked about {key}");
    }

    // Control: the same armed plan, nothing held, and the first delete faults.
    let control = FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(Rule::new(Op::Delete, ScriptedFault::Timeout)),
    );
    let clock = FixedClock::new(sealed_now_ns());
    let (bucket, _commit_keys) = seed_compacted_bucket(&control, &clock).await;
    tombstone_and_arm(&control, &bucket, &clock, &config, &retention).await;
    retention_sweep_bucket(&control, &clock, &config, &retention, &NoLeases, &bucket)
        .await
        .expect_err("the armed delete fault fails the unheld sweep");
    assert_eq!(
        control.fault_count(Op::Delete, FaultKind::Timeout),
        1,
        "the delete fault is armed and reachable: an unheld sweep fires it"
    );
}

/// The L0 data-object key each commit record names, in commit-key order.
async fn data_keys_of(store: &dyn ObjectStoreBackend, commit_keys: &[String]) -> Vec<String> {
    use ravel_commit::record;
    use ravel_object_store::GetRange;
    let mut out = Vec::new();
    for key in commit_keys {
        let got = store
            .get(key, GetRange::Full)
            .await
            .expect("commit record present");
        let rec = record::decode(&got.data).expect("commit record decodes");
        out.push(keys::reconstruct_data_key(&rec).expect("data key"));
    }
    out
}

/// The bucket's single compaction-record key.
async fn compaction_record_key(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> String {
    let record = fetch_compaction_record(store, bucket).await;
    keys::compaction_record_key_for(&record).expect("compaction record key")
}
