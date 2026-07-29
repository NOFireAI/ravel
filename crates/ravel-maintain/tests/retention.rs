//! Retention tests (ADR-0019, docs/compaction-retention-plan.md §6): the
//! under-retention impossibility floor, tombstone irreversibility, the
//! compactor-racing-tombstone ordering interlock, partial-sweep crash
//! convergence, and the config validation floor. The convergence/interlock
//! tests run once over an RSEG (metrics) fixture and once over an RLOG (logs)
//! fixture through one seeding helper, per the issue text.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use proptest::prelude::*;
use ravel_commit::keys;
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::retention::{is_expired, max_event_ts};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, NoLeases, RetentionConfig,
    RetentionConfigError, RetentionOutcome, RetentionPolicy, compact_bucket, maintain_bucket,
    retention_sweep_bucket,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_proto::commit::v1::{CommitRecord, Signal as ProtoSignal};
use uuid::Uuid;

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
