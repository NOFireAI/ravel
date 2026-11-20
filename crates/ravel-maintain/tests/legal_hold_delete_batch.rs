//! Every delete batch re-reads the legal-hold set before its first delete and
//! judges its deletes from that read (ADR-0042, the delete-batch re-read
//! amendment, #4501). These tests drive the superseded-input sweep, whose batch
//! is one chain group, and the retention physical sweep, whose batch is one
//! bucket, with a hold written between the pass's snapshot and a batch, and
//! with a hold re-read that fails.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use ravel_commit::keys;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::{
    AUDIT_HOLD_SHARD, Bucket, CompactionOutcome, CompactorConfig, FixedClock, LegalHoldCheck,
    NoLeases, RetentionConfig, RetentionOutcome, RetentionPolicy, SweepReport, compact_bucket,
    retention_sweep_bucket, shard_hold_scopes, sweep_shard, write_hold_set,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use uuid::Uuid;

fn cfg() -> CompactorConfig {
    CompactorConfig {
        max_query_duration_ns: 0,
        head_cache_ttl_ns: 0,
        clock_skew_allowance_ns: 0,
        ..CompactorConfig::default()
    }
}

const SECOND_HOUR: u32 = HOUR + 1;

/// A clock past the seal margin of both [`HOUR`] and [`SECOND_HOUR`].
fn sealed_ns() -> i64 {
    sealed_now_ns() + NS_PER_HOUR
}

/// Seed two compactable inputs into `hour`'s bucket and compact them, so the
/// shard holds one superseded chain group for the hour.
async fn seed_and_compact_at(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    hour: u32,
    writer: u128,
) -> Bucket {
    for seq in [1, 2] {
        let spec = InputSpec::new_at(
            hour,
            Uuid::from_u128(writer),
            10,
            seq,
            vec![raw_series(
                "m",
                &[("k", "a")],
                &[(seq as i64 * 1_000, seq as f64)],
            )],
        );
        seed_input(store, &spec).await;
    }
    let bucket = bucket_at(hour);
    let outcome = compact_bucket(store, clock, &cfg(), &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    bucket
}

/// Two compacted buckets in one shard, one per hour: four superseded inputs,
/// each its own chain group.
async fn two_groups(store: &dyn ObjectStoreBackend, clock: &FixedClock) {
    seed_and_compact_at(store, clock, HOUR, 0xa1).await;
    seed_and_compact_at(store, clock, SECOND_HOUR, 0xb2).await;
}

fn past_horizon() -> i64 {
    sealed_ns() + cfg().protection_horizon_ns + NS_PER_HOUR
}

/// Every key under the shard's three hold scopes, by scope: L0 data, commit
/// prefix, L1 parts.
async fn shard_keys(store: &dyn ObjectStoreBackend) -> [BTreeSet<String>; 3] {
    let scopes =
        shard_hold_scopes(&tenant_hash(), ravel_types::Signal::Metrics, SHARD).expect("scopes");
    let mut out: [BTreeSet<String>; 3] = Default::default();
    for (i, scope) in scopes.iter().enumerate() {
        out[i] = list_all(store, scope)
            .await
            .expect("list")
            .into_iter()
            .map(|m| m.key)
            .filter(|k| !k.contains("/maint/unn/"))
            .collect();
    }
    out
}

/// The commit-prefix keys of one hour's bucket.
async fn hour_keys(store: &dyn ObjectStoreBackend, hour: u32) -> BTreeSet<String> {
    let prefix =
        keys::commit_shard_hour_prefix(&tenant_hash(), ravel_types::Signal::Metrics, SHARD, hour)
            .expect("prefix");
    list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

async fn hold_the_shard(store: &dyn ObjectStoreBackend, id: u128) {
    for (i, scope) in shard_hold_scopes(&tenant_hash(), ravel_types::Signal::Metrics, SHARD)
        .expect("scopes")
        .iter()
        .enumerate()
    {
        write_hold_set(
            store,
            &tenant_hash(),
            Uuid::from_u128(id + i as u128),
            sealed_ns(),
            scope,
            "hold written mid-pass",
        )
        .await
        .expect("set hold");
    }
}

async fn sweep(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    lease: &LegalHoldCheck,
) -> SweepReport {
    sweep_shard(
        store,
        clock,
        &cfg(),
        lease,
        &tenant_hash(),
        ravel_types::Signal::Metrics,
        SHARD,
    )
    .await
    .expect("sweep")
}

/// Control: with no hold the pass deletes all four chain groups.
#[tokio::test]
async fn with_no_hold_every_chain_group_is_deleted() {
    let store = MemoryStore::new();
    let clock = FixedClock::new(sealed_ns());
    two_groups(&store, &clock).await;
    let snapshot = LegalHoldCheck::refresh(&store, &tenant_hash())
        .await
        .expect("refresh");
    clock.set(past_horizon());
    let report = sweep(&store, &clock, &snapshot).await;
    assert_eq!(
        (
            report.superseded_records_deleted,
            report.superseded_data_deleted,
            report.superseded_groups_held_by_legal_hold
        ),
        (4, 4, 0),
        "{report:?}"
    );
}

/// A hold written while the first chain group's first delete is in flight is
/// honored by every later group, whose own re-reads come after it. Each
/// compacted input is its own chain group (its commit record and data object),
/// so the shard holds four. The first group, which read before the hold, is
/// deleted whole (the residual); the other three are held. A pass that re-read
/// once for all its groups would delete all four.
#[tokio::test]
async fn a_hold_written_between_two_chain_groups_holds_the_later_groups() {
    let mem = Arc::new(MemoryStore::new());
    let clock = FixedClock::new(sealed_ns());
    two_groups(mem.as_ref(), &clock).await;
    let snapshot = LegalHoldCheck::refresh(mem.as_ref(), &tenant_hash())
        .await
        .expect("refresh");
    assert!(snapshot.is_empty());
    let store = FaultStore::new(Arc::clone(&mem), FaultPlan::empty());
    let gate = store.hold(Op::Delete, None, Occurrence::Nth(1));
    let mut commits_before = hour_keys(mem.as_ref(), HOUR).await;
    commits_before.extend(hour_keys(mem.as_ref(), SECOND_HOUR).await);
    let [l0_before, _, _] = shard_keys(mem.as_ref()).await;
    assert_eq!(l0_before.len(), 4);
    clock.set(past_horizon());

    let driver = async {
        gate.wait_until_held(1).await;
        let held = gate.held_details();
        assert_eq!(held.len(), 1);
        hold_the_shard(mem.as_ref(), 0x4501).await;
        gate.release(held[0].0);
        held[0].2.clone()
    };
    let (report, first_delete) = tokio::join!(sweep(&store, &clock, &snapshot), driver);

    assert_eq!(
        (
            report.superseded_records_deleted,
            report.superseded_data_deleted,
            report.superseded_groups_held_by_legal_hold
        ),
        (1, 1, 3),
        "the first group deletes its record and data object, the three later groups are \
         held: {report:?}"
    );
    let mut commits_after = hour_keys(mem.as_ref(), HOUR).await;
    commits_after.extend(hour_keys(mem.as_ref(), SECOND_HOUR).await);
    let mut expected = commits_before.clone();
    assert!(
        expected.remove(&first_delete),
        "the first delete is a commit record"
    );
    assert_eq!(
        commits_after, expected,
        "only the first group's record is gone"
    );
    let [l0_after, _, _] = shard_keys(mem.as_ref()).await;
    assert_eq!(
        l0_after.len(),
        3,
        "only the first group's data object is gone"
    );
}

/// A group whose hold re-read fails deletes nothing and is counted once; the
/// later groups' healthy re-reads let them delete.
#[tokio::test]
async fn a_failed_re_read_holds_its_chain_group_and_the_next_group_proceeds() {
    let mem = Arc::new(MemoryStore::new());
    let clock = FixedClock::new(sealed_ns());
    two_groups(mem.as_ref(), &clock).await;
    // The tick's snapshot is read before the fault is armed.
    let snapshot = LegalHoldCheck::refresh(mem.as_ref(), &tenant_hash())
        .await
        .expect("refresh");
    let source = Arc::clone(snapshot.source().expect("bound"));
    let hold_prefix =
        keys::commit_shard_prefix(&tenant_hash(), ravel_types::Signal::Audit, AUDIT_HOLD_SHARD)
            .expect("prefix");
    let store = FaultStore::new(
        Arc::clone(&mem),
        FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Transient("hold shard down".into()))
                .with_key_contains(hold_prefix)
                .with_occurrence(Occurrence::Nth(1)),
        ),
    );
    let [l0_before, _, _] = shard_keys(mem.as_ref()).await;
    clock.set(past_horizon());

    let report = sweep(&store, &clock, &snapshot).await;

    assert_eq!(
        store.fault_count(Op::List, FaultKind::Transient),
        1,
        "the first group's re-read faulted"
    );
    assert_eq!(
        source.reread_failures(),
        1,
        "one failed batch, counted once"
    );
    assert_eq!(
        (
            report.superseded_records_deleted,
            report.superseded_data_deleted,
            report.superseded_groups_held_by_legal_hold
        ),
        (3, 3, 0),
        "the failed group deletes nothing, the three healthy ones delete their record and data \
         object each: {report:?}"
    );
    let [l0_after, _, _] = shard_keys(mem.as_ref()).await;
    assert_eq!((l0_before.len(), l0_after.len()), (4, 1));
}

// --- retention: one bucket is one batch ------------------------------------

fn retention(config: &CompactorConfig) -> RetentionConfig {
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

/// Two compacted, tombstoned buckets in one shard with the clock past their
/// protection horizon.
async fn two_tombstoned_buckets(store: &dyn ObjectStoreBackend) -> (FixedClock, Bucket, Bucket) {
    let clock = FixedClock::new(sealed_ns());
    let a = seed_and_compact_at(store, &clock, HOUR, 0xa1).await;
    let b = seed_and_compact_at(store, &clock, SECOND_HOUR, 0xb2).await;
    let config = cfg();
    let retention = retention(&config);
    for bucket in [&a, &b] {
        let out = retention_sweep_bucket(store, &clock, &config, &retention, &NoLeases, bucket)
            .await
            .expect("tombstone pass");
        assert_eq!(out, RetentionOutcome::Tombstoned);
    }
    clock.set(sealed_ns() + config.protection_horizon_ns + 1);
    (clock, a, b)
}

async fn sweep_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    lease: &LegalHoldCheck,
    bucket: &Bucket,
) -> RetentionOutcome {
    let config = cfg();
    retention_sweep_bucket(store, clock, &config, &retention(&config), lease, bucket)
        .await
        .expect("sweep")
}

/// One snapshot for the pass, two buckets: the first is swept, the hold is
/// written, and the second bucket's own re-read holds it. A pass that re-read
/// once would sweep both; one that judged from the snapshot would sweep both.
#[tokio::test]
async fn a_hold_written_between_two_retention_buckets_holds_the_second() {
    let store = MemoryStore::new();
    let (clock, a, b) = two_tombstoned_buckets(&store).await;
    let snapshot = LegalHoldCheck::refresh(&store, &tenant_hash())
        .await
        .expect("refresh");
    let b_before = hour_keys(&store, b.ingest_hour_bucket).await;
    assert_eq!(
        b_before.len(),
        4,
        "two records, a compaction record and a tombstone"
    );

    assert_eq!(
        sweep_bucket(&store, &clock, &snapshot, &a).await,
        RetentionOutcome::Swept
    );
    assert!(hour_keys(&store, a.ingest_hour_bucket).await.is_empty());
    hold_the_shard(&store, 0x4502).await;

    assert_eq!(
        sweep_bucket(&store, &clock, &snapshot, &b).await,
        RetentionOutcome::SweptPartial,
        "the second bucket's re-read sees the hold"
    );
    assert_eq!(
        hour_keys(&store, b.ingest_hour_bucket).await,
        b_before,
        "nothing deleted"
    );
    let [l0, _, _] = shard_keys(&store).await;
    assert_eq!(l0.len(), 2, "the held bucket's two data objects remain");
}

/// A bucket whose re-read fails deletes nothing and keeps its tombstone, the
/// failure is counted once, and the next bucket's healthy re-read sweeps it.
#[tokio::test]
async fn a_failed_re_read_holds_its_retention_bucket_and_the_next_bucket_proceeds() {
    let mem = Arc::new(MemoryStore::new());
    let (clock, a, b) = two_tombstoned_buckets(mem.as_ref()).await;
    let snapshot = LegalHoldCheck::refresh(mem.as_ref(), &tenant_hash())
        .await
        .expect("refresh");
    let source = Arc::clone(snapshot.source().expect("bound"));
    let hold_prefix =
        keys::commit_shard_prefix(&tenant_hash(), ravel_types::Signal::Audit, AUDIT_HOLD_SHARD)
            .expect("prefix");
    let store = FaultStore::new(
        Arc::clone(&mem),
        FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Transient("hold shard down".into()))
                .with_key_contains(hold_prefix)
                .with_occurrence(Occurrence::Nth(1)),
        ),
    );
    let a_before = hour_keys(mem.as_ref(), a.ingest_hour_bucket).await;

    assert_eq!(
        sweep_bucket(&store, &clock, &snapshot, &a).await,
        RetentionOutcome::SweptPartial
    );
    assert_eq!(store.fault_count(Op::List, FaultKind::Transient), 1);
    assert_eq!(source.reread_failures(), 1);
    assert_eq!(
        hour_keys(mem.as_ref(), a.ingest_hour_bucket).await,
        a_before,
        "nothing deleted"
    );

    assert_eq!(
        sweep_bucket(&store, &clock, &snapshot, &b).await,
        RetentionOutcome::Swept
    );
    assert!(
        hour_keys(mem.as_ref(), b.ingest_hour_bucket)
            .await
            .is_empty()
    );
    assert_eq!(
        source.reread_failures(),
        1,
        "the healthy batch counts nothing"
    );
}

// --- unreferenced parts: the pass's part deletes are one batch -------------

/// A stray `l1/` part no compaction record names, past the age gate, is kept
/// when a hold over the shard is written after the pass's snapshot. Control:
/// with no hold it is deleted.
#[tokio::test]
async fn a_hold_placed_after_the_snapshot_keeps_an_unreferenced_part() {
    for hold in [false, true] {
        let store = MemoryStore::new();
        let clock = FixedClock::new(sealed_ns());
        let bucket = seed_and_compact_at(&store, &clock, HOUR, 0xa1).await;
        let record = fetch_compaction_record(&store, &bucket).await;
        let stray_key = keys::l1_part_key(
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
            bucket.ingest_hour_bucket,
            &hex::encode(&record.input_set_hash[..8]),
            9,
            &hex::encode([0xEEu8; 8]),
        )
        .unwrap();
        store
            .put(
                &stray_key,
                bytes::Bytes::from_static(b"stray-l1-part"),
                ravel_object_store::PutOptions::default(),
            )
            .await
            .unwrap();
        let snapshot = LegalHoldCheck::refresh(&store, &tenant_hash())
            .await
            .expect("refresh");
        if hold {
            hold_the_shard(&store, 0x4503).await;
        }
        let config = cfg();
        clock.set(config.unreferenced_part_age_gate_ns() + 1);
        let deleted = ravel_maintain::sweep_unreferenced_parts(
            &store,
            &clock,
            &config,
            &snapshot,
            &tenant_hash(),
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("sweep");
        assert_eq!(deleted, usize::from(!hold), "hold={hold}");
        assert_eq!(store.head(&stray_key).await.is_ok(), hold, "hold={hold}");
    }
}
