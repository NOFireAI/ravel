//! Retention tests (ADR-0019): the
//! under-retention impossibility floor, tombstone irreversibility, the
//! compactor-racing-tombstone ordering interlock, partial-sweep crash
//! convergence, and the config validation floor. The convergence/interlock
//! tests run once over an RSEG (metrics) fixture and once over an RLOG (logs)
//! fixture through one seeding helper.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use proptest::prelude::*;
use ravel_commit::keys;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::retention::{is_expired, max_event_ts};
use ravel_maintain::scan::scan_and_maintain;
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, NoLeases, RetentionConfig,
    RetentionConfigError, RetentionOutcome, RetentionPolicy, SnapshotBlock, compact_bucket,
    maintain_bucket, retention_sweep_bucket,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{CommitRecord, Signal as ProtoSignal};
use std::sync::Arc;
use uuid::Uuid;

use bytes::Bytes;
use ravel_maintain::Clock;
use ravel_types::{Signal, TimeRange};

#[derive(Debug, Clone, Copy)]
enum Sig {
    Metrics,
    Logs,
}

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

fn metrics_specs() -> Vec<InputSpec> {
    vec![
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![raw_series(
                "m",
                &[("k", "a")],
                &[(1_000, 1.0), (2_000, 2.0)],
            )],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ]
}

/// Seed two L0 inputs for `sig` (uncompacted) and return the bucket.
async fn seed_two(store: &dyn ObjectStoreBackend, sig: Sig) -> Bucket {
    match sig {
        Sig::Metrics => {
            for s in metrics_specs() {
                seed_input(store, &s).await;
            }
            bucket()
        }
        Sig::Logs => seed_rlog_two_inputs(store).await,
    }
}

/// A retention config whose window for the test tenant is exactly the floor,
/// so the tiny-timestamp fixtures (events near ts 0, `now` in the far future)
/// are always expired.
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

async fn bucket_is_empty(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> bool {
    let commit_prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    let commit_empty = list_all(store, &commit_prefix).await.unwrap().is_empty();
    let l1_prefix = format!(
        "t/{}/{}/l1/{:04}/{}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        bucket.shard,
        keys::ingest_hour_string(bucket.ingest_hour_bucket),
    );
    let l1_empty = list_all(store, &l1_prefix).await.unwrap().is_empty();
    commit_empty && l1_empty
}

async fn has_tombstone(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> bool {
    let key = keys::retention_tombstone_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    store.head(&key).await.is_ok()
}

// --- Config validation floor (ADR-0019 §5) ---------------------------------

#[test]
fn retention_config_floor_boundary() {
    let config = cfg();
    let lag = DEFAULT_MAX_INGEST_LAG_NS;
    let floor = config.retention_floor_ns(lag);

    // Exactly at the floor: accepted.
    let at = RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: vec![(TENANT.to_string(), floor)],
        },
        &config,
        lag,
    )
    .expect("floor is accepted");
    assert_eq!(at.window_for(&tenant_hash()), Some(floor));
    assert_eq!(at.floor_ns(), floor);

    // One ns below the floor: rejected.
    let below = RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: vec![(TENANT.to_string(), floor - 1)],
        },
        &config,
        lag,
    );
    assert!(matches!(
        below,
        Err(RetentionConfigError::BelowFloor { .. })
    ));

    // A default below the floor is rejected too.
    let bad_default = RetentionConfig::from_policy(
        RetentionPolicy {
            default: Some(floor - 1),
            tenants: vec![],
        },
        &config,
        lag,
    );
    assert!(matches!(
        bad_default,
        Err(RetentionConfigError::BelowFloor {
            tenant,
            ..
        }) if tenant == "default"
    ));

    // A default at the floor applies to any tenant with no override.
    let ok_default = RetentionConfig::from_policy(
        RetentionPolicy {
            default: Some(floor),
            tenants: vec![],
        },
        &config,
        lag,
    )
    .expect("default at floor accepted");
    assert_eq!(ok_default.window_for(&tenant_hash()), Some(floor));

    // No policy at all: no retention for anyone.
    let none = RetentionConfig::default();
    assert_eq!(none.window_for(&tenant_hash()), None);
}

// --- Under-retention impossibility (ADR-0019 decision 1) -------------------

fn commit_with_max_event(max_event_ts_ns: i64) -> CommitRecord {
    CommitRecord {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: ProtoSignal::Metrics as i32,
        shard: SHARD,
        writer_id: Uuid::nil().to_string(),
        writer_epoch: 0,
        writer_seq: 0,
        object_key: String::new(),
        object_size: 0,
        content_hash: vec![0; 32],
        sample_count: 0,
        series_count: 0,
        min_event_ts_ns: max_event_ts_ns.min(0),
        max_event_ts_ns,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
        segment_format_version: 1,
        created_unix_ns: 0,
        ingest_hour_bucket: 0,
        declared_column_stats: Vec::new(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// The impossibility floor (ADR-0019 decision 1): across a range of `R`,
    /// `now`, and event timestamps, a bucket holding any sample younger than
    /// `R` (event ts > now - R) is never expired, and an expired bucket's
    /// every sample is strictly older than `R`.
    #[test]
    fn no_sample_younger_than_r_is_ever_excluded(
        now in 1_000_000_000i64..4_000_000_000_000_000_000,
        r in 1i64..1_000_000_000_000,
        events in prop::collection::vec(0i64..4_000_000_000_000_000_000, 1..8),
    ) {
        let commits: Vec<CommitRecord> =
            events.iter().map(|&ts| commit_with_max_event(ts)).collect();
        let max = max_event_ts(&commits, &[]);
        let expired = is_expired(max, now, r);
        let threshold = now.saturating_sub(r);

        let has_young = events.iter().any(|&ts| ts > threshold);
        if has_young {
            prop_assert!(!expired, "a sample younger than R must keep the bucket live");
        }
        if expired {
            for &ts in &events {
                prop_assert!(ts < threshold, "expired bucket has only samples older than R");
            }
        }
    }
}

// --- Tombstone irreversibility (ADR-0019 decision 2) -----------------------

/// Raising a tenant's retention window after a bucket is tombstoned never
/// resurrects it: the tombstone, once durable, is honored regardless of the
/// (now larger) `R`.
#[tokio::test]
async fn tombstone_irreversible_when_r_is_raised() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_two(&store, sig).await;
        let config = cfg();

        // Expire and tombstone the bucket.
        let small = retention_at_floor(&config);
        let out = retention_sweep_bucket(&store, &clock, &config, &small, &NoLeases, &bucket)
            .await
            .expect("retention pass");
        assert_eq!(out, RetentionOutcome::Tombstoned);
        assert!(has_tombstone(&store, &bucket).await);

        // Raise R to a value so large the bucket would not be expired under it
        // (now - R <= max_event). The tombstone must still be honored.
        let huge_r = created; // now - created == 0 <= any event ts
        let raised = RetentionConfig::from_policy(
            RetentionPolicy {
                default: None,
                tenants: vec![(TENANT.to_string(), huge_r)],
            },
            &config,
            DEFAULT_MAX_INGEST_LAG_NS,
        )
        .expect("raised config");

        // Before the horizon: still tombstoned, not resurrected to live.
        let still = retention_sweep_bucket(&store, &clock, &config, &raised, &NoLeases, &bucket)
            .await
            .expect("retention pass");
        assert_eq!(still, RetentionOutcome::Tombstoned);
        assert!(has_tombstone(&store, &bucket).await);

        // Past the horizon: the tombstone is honored and the bucket is swept,
        // despite the raised R that would have kept a fresh bucket alive.
        clock.set(created + config.protection_horizon_ns + 1);
        let swept = retention_sweep_bucket(&store, &clock, &config, &raised, &NoLeases, &bucket)
            .await
            .expect("sweep pass");
        assert_eq!(swept, RetentionOutcome::Swept);
        assert!(!has_tombstone(&store, &bucket).await);
        assert!(
            bucket_is_empty(&store, &bucket).await,
            "records and parts gone"
        );
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
}

// --- Compactor racing tombstone: the ordering interlock (decision 6) -------

/// Both retention and compaction are eligible at once. The driver ordering
/// (retention first) wins: the bucket is tombstoned and never compacted. The
/// second defense (compact_bucket declining when it lists a tombstone) is
/// asserted too.
#[tokio::test]
async fn compactor_racing_tombstone_retention_wins() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_two(&store, sig).await;
        let config = cfg();
        let retention = retention_at_floor(&config);

        // Driver ordering: retention runs first, so compaction never runs.
        let (r, c) = maintain_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
            .await
            .expect("maintain");
        assert_eq!(r, RetentionOutcome::Tombstoned);
        assert!(c.is_none(), "an expired bucket must not be compacted");
        assert!(has_tombstone(&store, &bucket).await);
        // No compaction record was written.
        assert!(no_compaction_record(&store, &bucket).await);

        // Second defense (ADR-0019 decision 6, "efficiency measure only"):
        // calling compact_bucket directly on the tombstoned bucket declines.
        let direct = compact_bucket(&store, &clock, &config, &bucket)
            .await
            .expect("compact declines");
        assert_eq!(direct, CompactionOutcome::Tombstoned);
        assert!(no_compaction_record(&store, &bucket).await);
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
}

async fn no_compaction_record(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> bool {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    !list_all(store, &prefix).await.unwrap().iter().any(|m| {
        matches!(
            keys::partition_bucket_entry(&m.key),
            Ok(keys::BucketEntry::CompactionRecord(_))
        )
    })
}

// --- Partial-sweep crash convergence (ADR-0019 decision 4) -----------------

/// A crash during the physical sweep (here the tombstone's own delete faults
/// after everything else is gone) leaves the tombstone in place; the next pass
/// finishes the job and deletes the tombstone only once the bucket verifies
/// empty. Runs over a compacted bucket so every deletion phase (L0 records,
/// compaction record, L0 data, L1 parts, tombstone) is exercised.
#[tokio::test]
async fn partial_sweep_crash_then_converges() {
    async fn run(sig: Sig) {
        let inner = MemoryStore::new();
        // The tombstone delete (key ends in "retire.tmb") faults once, after
        // the records/data/parts are already deleted and the bucket verifies
        // empty.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Delete, ScriptedFault::Timeout)
                .with_key_contains("retire.tmb")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_two(&store, sig).await;
        let config = cfg();

        // Compact first so the bucket holds L0 records, a compaction record,
        // and L1 parts, then tombstone it.
        let compacted = compact_bucket(&store, &clock, &config, &bucket)
            .await
            .expect("compact");
        assert!(matches!(compacted, CompactionOutcome::Compacted { .. }));
        let retention = retention_at_floor(&config);
        let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
            .await
            .expect("tombstone pass");
        assert_eq!(out, RetentionOutcome::Tombstoned);

        // Past the horizon: the physical sweep deletes everything, then the
        // tombstone delete faults.
        clock.set(created + config.protection_horizon_ns + 1);
        let err =
            retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket).await;
        assert!(err.is_err(), "the tombstone delete fault aborts the sweep");
        assert_eq!(store.fault_count(Op::Delete, FaultKind::Timeout), 1);
        assert!(
            has_tombstone(&store, &bucket).await,
            "tombstone survives the crash, so exclusion still holds"
        );

        // Re-run: the fault is spent; the bucket already verifies empty of
        // records and parts, so the tombstone is deleted and the bucket is
        // fully retired.
        let swept = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
            .await
            .expect("converging sweep");
        assert_eq!(swept, RetentionOutcome::Swept);
        assert!(!has_tombstone(&store, &bucket).await);
        assert!(bucket_is_empty(&store, &bucket).await);
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
}

/// A crash during the physical sweep's L1-part delete (the step whose set of
/// keys the pre-fix code reconstructed from the compaction records it had
/// already deleted) still converges. The compaction records are deleted before
/// the L1 delete, so the pre-fix code re-listed an empty compaction-record set
/// on the next pass, computed no L1 keys, and left the physical `l1/` objects
/// orphaned forever (SweptPartial on every subsequent pass, tombstone stuck).
/// The fix discovers L1 keys by a fresh LIST of the bucket's `l1/` prefix, so
/// the next pass deletes whatever is physically present regardless of the
/// compaction record's fate and reaches `Swept`.
#[tokio::test]
async fn l1_delete_crash_then_converges() {
    async fn run(sig: Sig) {
        let inner = MemoryStore::new();
        // The first L1-part delete (key contains "/l1/") faults once, after the
        // commit records, compaction records, and L0 data are already deleted.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Delete, ScriptedFault::Timeout)
                .with_key_contains("/l1/")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_two(&store, sig).await;
        let config = cfg();

        // Compact so the bucket holds L0 records, a compaction record, and L1
        // parts, then tombstone it.
        let compacted = compact_bucket(&store, &clock, &config, &bucket)
            .await
            .expect("compact");
        assert!(matches!(compacted, CompactionOutcome::Compacted { .. }));
        let retention = retention_at_floor(&config);
        let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
            .await
            .expect("tombstone pass");
        assert_eq!(out, RetentionOutcome::Tombstoned);

        // Past the horizon: the physical sweep deletes records, compaction
        // records, and L0 data, then the first L1-part delete faults and aborts
        // the pass. The tombstone (deleted last) survives.
        clock.set(created + config.protection_horizon_ns + 1);
        let err =
            retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket).await;
        assert!(err.is_err(), "the L1-part delete fault aborts the sweep");
        assert_eq!(store.fault_count(Op::Delete, FaultKind::Timeout), 1);
        assert!(
            has_tombstone(&store, &bucket).await,
            "tombstone survives the crash, so exclusion still holds"
        );
        // The compaction record is already gone, yet the physical l1/ objects
        // remain: exactly the orphaned-part state the pre-fix code could not
        // recover from.
        assert!(
            !bucket_is_empty(&store, &bucket).await,
            "L1 parts remain after the faulted pass"
        );

        // Re-run: the fault is spent. The fix re-LISTs the l1/ prefix directly
        // (no surviving compaction record needed), deletes the orphaned parts,
        // verifies the bucket empty, and deletes the tombstone: fully retired.
        let swept = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
            .await
            .expect("converging sweep");
        assert_eq!(swept, RetentionOutcome::Swept);
        assert!(!has_tombstone(&store, &bucket).await);
        assert!(
            bucket_is_empty(&store, &bucket).await,
            "records and parts gone, l1/ prefix verified empty"
        );
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
}

// --- Full retention lifecycle over a non-compacted bucket ------------------

/// End to end over a never-compacted bucket: sealed and expired -> tombstoned
/// -> (before horizon) still tombstoned -> (after horizon) swept empty. This
/// is the ADR-0019 "retention of never-compacted L0 buckets needs no
/// compactor" path.
#[tokio::test]
async fn retention_lifecycle_uncompacted_bucket() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_two(&store, sig).await;
        let config = cfg();
        let retention = retention_at_floor(&config);

        // Not expired under a window larger than the data's age? Use a window
        // so large the bucket is live, to confirm NotExpired first.
        let live = RetentionConfig::from_policy(
            RetentionPolicy {
                default: None,
                tenants: vec![(TENANT.to_string(), created)],
            },
            &config,
            DEFAULT_MAX_INGEST_LAG_NS,
        )
        .unwrap();
        let not_expired =
            retention_sweep_bucket(&store, &clock, &config, &live, &NoLeases, &bucket)
                .await
                .expect("live pass");
        assert_eq!(not_expired, RetentionOutcome::NotExpired);
        assert!(!has_tombstone(&store, &bucket).await);

        // Now expired: tombstone written.
        let out = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
            .await
            .expect("tombstone pass");
        assert_eq!(out, RetentionOutcome::Tombstoned);

        // Before the horizon: still tombstoned, nothing deleted.
        let before =
            retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
                .await
                .expect("pre-horizon pass");
        assert_eq!(before, RetentionOutcome::Tombstoned);
        assert!(!bucket_is_empty(&store, &bucket).await);

        // After the horizon: swept empty.
        clock.set(created + config.protection_horizon_ns + 1);
        let swept = retention_sweep_bucket(&store, &clock, &config, &retention, &NoLeases, &bucket)
            .await
            .expect("sweep pass");
        assert_eq!(swept, RetentionOutcome::Swept);
        assert!(bucket_is_empty(&store, &bucket).await);
        assert!(!has_tombstone(&store, &bucket).await);
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
}

/// No retention window configured -> the pass is a no-op.
#[tokio::test]
async fn no_policy_is_a_noop() {
    let store = MemoryStore::new();
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = seed_two(&store, Sig::Metrics).await;
    let out = retention_sweep_bucket(
        &store,
        &clock,
        &cfg(),
        &RetentionConfig::default(),
        &NoLeases,
        &bucket,
    )
    .await
    .expect("no-op pass");
    assert_eq!(out, RetentionOutcome::NoPolicy);
    assert!(!has_tombstone(&store, &bucket).await);
    assert!(!bucket_is_empty(&store, &bucket).await);
}

// --- HEAD-reachability delete blocker (ADR-0020) ---------------------------

/// The catalog HEAD key for the shared test tenant/signal (frozen key layout).
fn head_key(signal: Signal) -> String {
    format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash().to_hex(),
        signal.key_prefix()
    )
}

/// Fold the seeded commit layout into a catalog HEAD naming the seeded
/// bucket's segments. `shard_count = SHARD + 1` so the fold's scan set covers
/// the fixture shard `SHARD`. `default_retention_ns` is the caller-resolved
/// deployment-wide retention default (from `RetentionConfig::window_for`) the
/// production fold loop passes into `Catalog::fold`; `None` for the tests that
/// rely on the durable `TenantConfig.retention_ns` override instead.
async fn fold_head(
    store: &Arc<MemoryStore>,
    signal: Signal,
    now_ns: i64,
    default_retention_ns: Option<i64>,
) {
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = ravel_catalog::Catalog::new(
        dyn_store,
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    catalog
        .fold(
            &tenant_hash(),
            signal,
            Uuid::new_v4(),
            now_ns,
            &[],
            default_retention_ns,
        )
        .await
        .expect("fold builds a HEAD naming the seeded bucket");
}

/// Write the tenant's durable retention window so the fold's frontier reconcile
/// has a frontier to derive (deliverable 1 half of the e2e).
async fn write_tenant_retention(store: &dyn ObjectStoreBackend, retention_ns: i64) {
    let cfg = ravel_catalog::TenantConfig {
        retention_ns: Some(retention_ns),
        ..ravel_catalog::TenantConfig::new(ravel_catalog::TenantLifecycleState::Active)
    };
    ravel_catalog::set_tenant_config(store, &tenant_hash(), &cfg, 1)
        .await
        .expect("write tenant retention config");
}

/// Deliverable 3, case 1: the physical sweep is BLOCKED when the live HEAD
/// snapshot still names an object inside the bucket. Nothing is deleted, the
/// `BlockedBySnapshot(Named)` outcome is returned, and the tombstone is left in
/// place. To watch this FAIL against pre-fix code, replace the HEAD gate at the
/// top of `physical_sweep` with `SnapshotGate::Clear` (or delete it): the sweep
/// then deletes the bucket's objects while the snapshot still names them.
#[tokio::test]
async fn sweep_blocked_when_head_names_bucket() {
    let store = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = seed_two(store.as_ref(), Sig::Metrics).await;
    let config = cfg();

    // A HEAD naming the bucket's segments.
    fold_head(&store, Signal::Metrics, created, None).await;
    assert!(store.head(&head_key(Signal::Metrics)).await.is_ok());

    let retention = retention_at_floor(&config);
    let tombstoned = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("tombstone pass");
    assert_eq!(tombstoned, RetentionOutcome::Tombstoned);

    // Past the horizon: the sweep would delete, but HEAD still names the bucket.
    clock.set(created + config.protection_horizon_ns + 1);
    let blocked = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("gate pass");
    assert_eq!(
        blocked,
        RetentionOutcome::BlockedBySnapshot(SnapshotBlock::Named),
        "a HEAD-referenced snapshot blocks the physical delete"
    );
    assert!(
        has_tombstone(store.as_ref(), &bucket).await,
        "the tombstone is left in place so exclusion still holds"
    );
    assert!(
        !bucket_is_empty(store.as_ref(), &bucket).await,
        "nothing is deleted while the HEAD snapshot names the bucket"
    );
}

/// Deliverable 3, case 2: an ABSENT HEAD is not a block. With no snapshot
/// naming anything, the sweep proceeds to completion (ADR-0020: the index is a
/// pure optimization; a missing HEAD degrades to listing).
#[tokio::test]
async fn sweep_proceeds_when_head_absent() {
    let store = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = seed_two(store.as_ref(), Sig::Metrics).await;
    let config = cfg();

    // Deliberately NO fold: HEAD is absent.
    assert!(store.head(&head_key(Signal::Metrics)).await.is_err());

    let retention = retention_at_floor(&config);
    let tombstoned = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("tombstone");
    assert_eq!(tombstoned, RetentionOutcome::Tombstoned);

    clock.set(created + config.protection_horizon_ns + 1);
    let swept = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("sweep");
    assert_eq!(
        swept,
        RetentionOutcome::Swept,
        "an absent HEAD does not block; the sweep completes"
    );
    assert!(bucket_is_empty(store.as_ref(), &bucket).await);
    assert!(!has_tombstone(store.as_ref(), &bucket).await);
}

/// Deliverable 3, case 3: a HEAD present but UNDECODABLE fails closed. The
/// sweep is blocked with `BlockedBySnapshot(Unreadable)` and nothing is
/// deleted: non-reachability cannot be proven from a HEAD that cannot be read.
/// To watch this FAIL, make `ensure_head` map a decode error to
/// `HeadLoad::Absent` instead of `HeadLoad::Unreadable`.
#[tokio::test]
async fn sweep_blocked_fail_closed_when_head_undecodable() {
    let store = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = seed_two(store.as_ref(), Sig::Metrics).await;
    let config = cfg();

    // A present but garbage HEAD object.
    store
        .put(
            &head_key(Signal::Metrics),
            Bytes::from_static(b"not a valid HEAD record"),
            PutOptions::default(),
        )
        .await
        .expect("put garbage HEAD");

    let retention = retention_at_floor(&config);
    let tombstoned = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("tombstone");
    assert_eq!(tombstoned, RetentionOutcome::Tombstoned);

    clock.set(created + config.protection_horizon_ns + 1);
    let blocked = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("gate");
    assert_eq!(
        blocked,
        RetentionOutcome::BlockedBySnapshot(SnapshotBlock::Unreadable),
        "an unreadable HEAD blocks the delete fail-closed"
    );
    assert!(
        !bucket_is_empty(store.as_ref(), &bucket).await,
        "fail-closed: nothing deleted when HEAD cannot be read"
    );
    assert!(has_tombstone(store.as_ref(), &bucket).await);
}

/// Deliverable 4 (interleaving): the two serialized fold/sweep orderings. A
/// sweep whose gate reads the PRE-fold HEAD (which still names the bucket) must
/// not delete the bucket's objects; only after a later fold drops the bucket
/// does the sweep proceed. This covers both orderings a genuine concurrent
/// fold+sweep collapses to, since the gate reads one atomic HEAD version: the
/// pre-fold version (blocks) or the post-fold version (bucket already dropped,
/// safe to delete).
#[tokio::test]
async fn sweep_respects_pre_fold_head_then_deletes_after_fold_drops_bucket() {
    let store = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = seed_two(store.as_ref(), Sig::Metrics).await;
    let config = cfg();

    // Pre-fold HEAD names the bucket.
    fold_head(&store, Signal::Metrics, created, None).await;
    let retention = retention_at_floor(&config);
    retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("tombstone");
    clock.set(created + config.protection_horizon_ns + 1);

    // Ordering A: the sweep's gate reads the pre-fold HEAD (still names the
    // bucket) -> blocked, nothing deleted.
    let blocked = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("gate");
    assert_eq!(
        blocked,
        RetentionOutcome::BlockedBySnapshot(SnapshotBlock::Named)
    );
    assert!(
        !bucket_is_empty(store.as_ref(), &bucket).await,
        "the pre-fold HEAD named the bucket, so nothing was deleted"
    );

    // The fold completes and drops the tombstoned bucket from the snapshot.
    fold_head(&store, Signal::Metrics, clock.now_ns(), None).await;

    // Ordering B: the sweep's gate now reads the post-fold HEAD (bucket
    // dropped) -> proceeds and deletes.
    let swept = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("sweep");
    assert_eq!(swept, RetentionOutcome::Swept);
    assert!(bucket_is_empty(store.as_ref(), &bucket).await);
}

/// The epic exit criterion: retention of an hour whose tombstone lands FAR
/// behind the fold watermark (outside the fixed reconcile window) never leaves
/// the snapshot naming a deleted object. The fold's retention-frontier
/// reconcile drops the out-of-window hour, then the sweep deletes it; a
/// resolve spanning the retired hour returns only live segments, never a
/// reference to a deleted object (the catalog-layer condition that produces
/// `SnapshotInvalidated` / 503 downstream), and it stays true across repeated
/// resolves.
///
/// To watch this FAIL against pre-fix code, disable the fold's frontier
/// reconcile (`frontier_reconcile_max_hours: 0`) AND the sweep's HEAD gate
/// (`SnapshotGate::Clear`): the out-of-window hour is never dropped, the sweep
/// deletes its objects while HEAD still names them, and the resolved snapshot
/// then names a deleted object.
#[tokio::test]
async fn retention_of_out_of_window_hour_never_leaves_snapshot_naming_deleted_objects() {
    let store = Arc::new(MemoryStore::new());
    let config = cfg();

    // Two hours: an old hour retention will retire, and a recent hour at the
    // watermark that survives. The gap (100 hours) puts the old hour far
    // outside the fixed 26h reconcile window behind the watermark.
    let recent_hour = HOUR;
    let old_hour = HOUR - 100;

    // Consistent event timestamps at each hour (so a resolve by event time
    // matches the ingest hour), via the seed helper's sample timestamps.
    seed_input(
        store.as_ref(),
        &InputSpec::new_at(
            old_hour,
            Uuid::from_u128(0xA1),
            1,
            1,
            vec![raw_series(
                "m",
                &[("k", "old")],
                &[(i64::from(old_hour) * NS_PER_HOUR + 1_000, 1.0)],
            )],
        ),
    )
    .await;
    seed_input(
        store.as_ref(),
        &InputSpec::new_at(
            recent_hour,
            Uuid::from_u128(0xB2),
            1,
            1,
            vec![raw_series(
                "m",
                &[("k", "recent")],
                &[(i64::from(recent_hour) * NS_PER_HOUR + 1_000, 2.0)],
            )],
        ),
    )
    .await;

    // Retention window 90h, matched between the sweep (RetentionConfig) and the
    // fold (durable TenantConfig.retention_ns), so both agree on the frontier.
    let retention_window_ns = 90 * NS_PER_HOUR;
    let retention = RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: vec![(TENANT.to_string(), retention_window_ns)],
        },
        &config,
        DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("retention config");
    write_tenant_retention(store.as_ref(), retention_window_ns).await;

    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let old_bucket = bucket_at(old_hour);

    // 1. Fold: the snapshot names both hours.
    fold_head(&store, Signal::Metrics, created, None).await;

    // 2. Retire the old hour (tombstone written far behind the watermark).
    let tombstoned = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &old_bucket,
    )
    .await
    .expect("tombstone");
    assert_eq!(tombstoned, RetentionOutcome::Tombstoned);

    // 3. Fold again, a few hours later so the watermark advances and the
    //    reconcile pass runs (a fold at the same instant seals no new hour and
    //    is a no-op): the retention-frontier reconcile observes the
    //    out-of-window tombstone and drops the old hour from the snapshot.
    fold_head(&store, Signal::Metrics, created + 3 * NS_PER_HOUR, None).await;

    // 4. Past the protection horizon, run the sweep to completion.
    clock.set(created + config.protection_horizon_ns + 1);
    let mut outcome = RetentionOutcome::Tombstoned;
    for _ in 0..8 {
        outcome = retention_sweep_bucket(
            store.as_ref(),
            &clock,
            &config,
            &retention,
            &NoLeases,
            &old_bucket,
        )
        .await
        .expect("sweep pass");
        if outcome == RetentionOutcome::Swept {
            break;
        }
    }
    assert_eq!(
        outcome,
        RetentionOutcome::Swept,
        "the sweep completes once the fold has dropped the out-of-window hour"
    );
    assert!(bucket_is_empty(store.as_ref(), &old_bucket).await);

    // 5. Resolve a window spanning the retired hour, repeatedly. The resolve
    //    must never name a deleted object (no SnapshotInvalidated) and must
    //    exclude the retired hour.
    let resolver = ravel_catalog::Catalog::new(
        {
            let s: Arc<dyn ObjectStoreBackend> = store.clone();
            s
        },
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("resolver catalog");
    let range = TimeRange {
        start_ns: i64::from(old_hour) * NS_PER_HOUR,
        end_ns: (i64::from(recent_hour) + 1) * NS_PER_HOUR,
    };
    for _ in 0..3 {
        let snapshot = resolver
            .resolve(&tenant_hash(), Signal::Metrics, range, &[], clock.now_ns())
            .await
            .expect("resolve must never return a 5xx-shaped error for the retired range");
        for seg in &snapshot.segments {
            assert_ne!(
                seg.ingest_hour_bucket, old_hour,
                "the retired hour must not be named by any resolved segment"
            );
            assert!(
                store.head(&seg.data_object_key).await.is_ok(),
                "a resolved segment names a DELETED object ({}): this is the \
                 SnapshotInvalidated condition the fix must prevent",
                seg.data_object_key
            );
        }
        assert!(
            snapshot
                .segments
                .iter()
                .any(|s| s.ingest_hour_bucket == recent_hour),
            "the surviving recent hour is still resolvable"
        );
    }
}

/// ADR-0078 acceptance test: a deployment configured with ONLY a
/// `RetentionConfig` deployment default (the CLI-flag path: `--retention-default`
/// / `--retention-tenant`) and NO durable `TenantConfig.retention_ns` write must
/// still get the fold's retention-frontier reconcile pass, so the physical sweep
/// completes the delete. This is the exact gap #134 closes: before the fix, the
/// fold read only `TenantConfig.retention_ns` (never written by the running
/// server), so `tenant_retention_ns` was always `None`, the frontier reconcile
/// never ran, HEAD kept naming the retired hour, and the sweep stayed blocked
/// forever.
///
/// It mirrors `retention_of_out_of_window_hour_never_leaves_snapshot_naming_
/// deleted_objects` and drives retention through the real `Catalog::fold`
/// (via `fold_head`) and the real `retention_sweep_bucket`, but resolves the
/// window from the same `RetentionConfig::window_for` the production fold loop
/// uses (`services/ravel-server/src/fold.rs`) and passes it as the new
/// deployment-default parameter -- with no `write_tenant_retention` call at all.
///
/// To watch this FAIL against pre-fix code, restore the old resolution in
/// `crates/ravel-catalog/src/fold.rs` (`Ok(None) => None` and the `Err` arm
/// returning `None`, ignoring `default_retention_ns`): with no TenantConfig
/// record present, `tenant_retention_ns` is `None`, the frontier reconcile is
/// skipped, the old hour is never dropped from the snapshot, the sweep stays
/// `BlockedBySnapshot`, and the final `RetentionOutcome::Swept` assertion fails.
#[tokio::test]
async fn deployment_default_retention_drives_frontier_reconcile_and_sweep() {
    let store = Arc::new(MemoryStore::new());
    let config = cfg();

    // Same two-hour layout as the TenantConfig-driven e2e: an old hour far
    // behind the watermark (outside the fixed 26h reconcile window) that
    // retention retires, and a recent hour at the watermark that survives.
    let recent_hour = HOUR;
    let old_hour = HOUR - 100;

    seed_input(
        store.as_ref(),
        &InputSpec::new_at(
            old_hour,
            Uuid::from_u128(0xA1),
            1,
            1,
            vec![raw_series(
                "m",
                &[("k", "old")],
                &[(i64::from(old_hour) * NS_PER_HOUR + 1_000, 1.0)],
            )],
        ),
    )
    .await;
    seed_input(
        store.as_ref(),
        &InputSpec::new_at(
            recent_hour,
            Uuid::from_u128(0xB2),
            1,
            1,
            vec![raw_series(
                "m",
                &[("k", "recent")],
                &[(i64::from(recent_hour) * NS_PER_HOUR + 1_000, 2.0)],
            )],
        ),
    )
    .await;

    // The ONLY retention source: a deployment-wide default (the CLI
    // `--retention-default` path). No `write_tenant_retention` / TenantConfig
    // record is written, so the fold's frontier reconcile can only run if it
    // honors this deployment default (the #134 fix).
    let retention_window_ns = 90 * NS_PER_HOUR;
    let retention = RetentionConfig::from_policy(
        RetentionPolicy {
            default: Some(retention_window_ns),
            tenants: vec![],
        },
        &config,
        DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("retention config");
    // The value the production fold loop resolves per tenant and hands to
    // `Catalog::fold` as its new deployment-default parameter.
    let default_retention_ns = retention.window_for(&tenant_hash());
    assert_eq!(default_retention_ns, Some(retention_window_ns));

    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let old_bucket = bucket_at(old_hour);

    // 1. Fold: the snapshot names both hours.
    fold_head(&store, Signal::Metrics, created, default_retention_ns).await;

    // 2. Retire the old hour (tombstone written far behind the watermark).
    let tombstoned = retention_sweep_bucket(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        &old_bucket,
    )
    .await
    .expect("tombstone");
    assert_eq!(tombstoned, RetentionOutcome::Tombstoned);

    // 3. Fold again a few hours later so the watermark advances and the
    //    reconcile pass runs. With the fix, the frontier reconcile derived from
    //    the deployment default observes the out-of-window tombstone and drops
    //    the old hour from the snapshot.
    fold_head(
        &store,
        Signal::Metrics,
        created + 3 * NS_PER_HOUR,
        default_retention_ns,
    )
    .await;

    // 4. Past the protection horizon, run the sweep to completion. It can only
    //    reach `Swept` if step 3 dropped the old hour from the HEAD snapshot.
    clock.set(created + config.protection_horizon_ns + 1);
    let mut outcome = RetentionOutcome::Tombstoned;
    for _ in 0..8 {
        outcome = retention_sweep_bucket(
            store.as_ref(),
            &clock,
            &config,
            &retention,
            &NoLeases,
            &old_bucket,
        )
        .await
        .expect("sweep pass");
        if outcome == RetentionOutcome::Swept {
            break;
        }
    }
    assert_eq!(
        outcome,
        RetentionOutcome::Swept,
        "with only a RetentionConfig deployment default (no TenantConfig write), \
         the fold's frontier reconcile must still drop the retired hour so the \
         sweep completes -- this is the #134 fix"
    );
    assert!(bucket_is_empty(store.as_ref(), &old_bucket).await);

    // 5. A resolve spanning the retired hour never names a deleted object and
    //    excludes the retired hour, while the recent hour stays resolvable.
    let resolver = ravel_catalog::Catalog::new(
        {
            let s: Arc<dyn ObjectStoreBackend> = store.clone();
            s
        },
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("resolver catalog");
    let range = TimeRange {
        start_ns: i64::from(old_hour) * NS_PER_HOUR,
        end_ns: (i64::from(recent_hour) + 1) * NS_PER_HOUR,
    };
    let snapshot = resolver
        .resolve(&tenant_hash(), Signal::Metrics, range, &[], clock.now_ns())
        .await
        .expect("resolve must never return a 5xx-shaped error for the retired range");
    for seg in &snapshot.segments {
        assert_ne!(
            seg.ingest_hour_bucket, old_hour,
            "the retired hour must not be named by any resolved segment"
        );
        assert!(
            store.head(&seg.data_object_key).await.is_ok(),
            "a resolved segment names a DELETED object ({}): this is the \
             SnapshotInvalidated condition the fix must prevent",
            seg.data_object_key
        );
    }
    assert!(
        snapshot
            .segments
            .iter()
            .any(|s| s.ingest_hour_bucket == recent_hour),
        "the surviving recent hour is still resolvable"
    );
}

/// Deliverable 5 (counters): the blocked-by-snapshot outcome is observable
/// through `MaintainReport::blocked_by_snapshot`, driven end to end through the
/// production `scan_and_maintain` pass over the shard.
#[tokio::test]
async fn scan_reports_blocked_by_snapshot_counter() {
    let store = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    seed_two(store.as_ref(), Sig::Metrics).await;
    let config = cfg();
    let retention = retention_at_floor(&config);

    // A HEAD naming the bucket: the sweep will block on it.
    fold_head(&store, Signal::Metrics, created, None).await;

    // Pass 1: tombstone the expired bucket.
    let r1 = scan_and_maintain(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan pass 1");
    assert_eq!(r1.blocked_by_snapshot, 0);

    // Pass 2, past the horizon: the physical sweep is blocked by the HEAD.
    clock.set(created + config.protection_horizon_ns + 1);
    let r2 = scan_and_maintain(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan pass 2");
    assert_eq!(
        r2.blocked_by_snapshot, 1,
        "the HEAD-reachability block is counted on MaintainReport"
    );
    assert_eq!(r2.blocked_by_unreadable_head, 0);
}

/// Deliverable 5 (counters): the fail-closed undecodable-HEAD block is
/// observable through `MaintainReport::blocked_by_unreadable_head`, distinct
/// from the named-block counter.
#[tokio::test]
async fn scan_reports_blocked_by_unreadable_head_counter() {
    let store = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    seed_two(store.as_ref(), Sig::Metrics).await;
    let config = cfg();
    let retention = retention_at_floor(&config);

    // A present but garbage HEAD: the sweep blocks fail-closed.
    store
        .put(
            &head_key(Signal::Metrics),
            Bytes::from_static(b"garbage HEAD"),
            PutOptions::default(),
        )
        .await
        .expect("put garbage HEAD");

    // Pass 1: tombstone.
    scan_and_maintain(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan pass 1");

    // Pass 2, past the horizon: fail-closed block, counted distinctly.
    clock.set(created + config.protection_horizon_ns + 1);
    let r2 = scan_and_maintain(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan pass 2");
    assert_eq!(
        r2.blocked_by_unreadable_head, 1,
        "the fail-closed unreadable-HEAD block is counted distinctly"
    );
    assert_eq!(r2.blocked_by_snapshot, 0);
}
