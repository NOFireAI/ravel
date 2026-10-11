//! The writer policy over recorded format floors (ADR-1746 decisions 4 and 5)
//! on the ingest flush and on every maintenance run that writes parts.
//!
//! Each case writes the tenant's provisioning record and raises its floor
//! with the catalog's own functions, one above the version the build writes,
//! then counts the store's PUTs around the run: a refused run makes none.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use ravel_catalog::{
    AbsentPolicy, FloorBasis, FloorEvidence, FloorObservation, classify_floor, raise_format_floor,
    read_floors_from_store, validate_or_adopt,
};
use ravel_commit::{keys, signal};
use ravel_ingest::{
    BelowFormatFloor, FormatFamily, FormatFloorMetrics, FormatFloorPolicy, IngestConfig,
    IngestRouter, PendingAckBudget, SystemClock, WriteError, WriteMode,
};
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, FixedClock, MaintainError, MaintainMemo, MigrateBudget,
    NoLeases, PendingErasureRequest, RequestLedger, RetentionConfig, RetentionOutcome,
    census_family, compact_bucket, erasure_rewrite_bucket, maintain_bucket,
    migrate_bucket_format, migrate_family,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend};
use ravel_otlp::NormalizedPoint;
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

/// Shard count of the provisioning record: wide enough for [`SHARD`].
const SHARDS: u32 = 8;

fn rseg_writing() -> u32 {
    FormatFamily::Rseg.writable_version()
}

/// The refusal a run writing RSEG at the build's version gets from a floor
/// one above it.
fn refused() -> BelowFormatFloor {
    BelowFormatFloor {
        family: FormatFamily::Rseg,
        floor: rseg_writing() + 1,
        writing: rseg_writing(),
    }
}

fn basis() -> FloorBasis {
    FloorBasis {
        observed_entries: 0,
        observed_newest_created_unix_ns: 0,
        observed_shards: 1,
    }
}

async fn provision(store: &dyn ObjectStoreBackend, tenant: &TenantId, shards: u32) {
    validate_or_adopt(
        store,
        &tenant.hash(),
        Signal::Metrics,
        shards,
        1,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
}

async fn raise_rseg_floor(store: &dyn ObjectStoreBackend, tenant: &TenantId, floor: u32) {
    raise_format_floor(
        store,
        &tenant.hash(),
        Signal::Metrics,
        FormatFamily::Rseg.as_str(),
        floor,
        basis(),
        "test",
        2,
    )
    .await
    .expect("raise floor");
}

fn puts(store: &InstrumentedStore<MemoryStore>) -> u64 {
    store.metrics().snapshot().put.calls
}

/// The two-input metrics bucket the common fixtures build, in a store whose
/// floor for the tenant is one above the build's RSEG version.
async fn floored_bucket() -> InstrumentedStore<MemoryStore> {
    let store = InstrumentedStore::new(MemoryStore::new());
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_000, 1.0)]),
                raw_series("victim", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("victim", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ] {
        seed_input(&store, &spec).await;
    }
    let acme = TenantId::new(TENANT);
    provision(&store, &acme, SHARDS).await;
    raise_rseg_floor(&store, &acme, rseg_writing() + 1).await;
    store
}

fn config_with(policy: FormatFloorPolicy) -> CompactorConfig {
    CompactorConfig {
        format_floor: Some(policy),
        ..CompactorConfig::default()
    }
}

fn refusing() -> (Arc<FormatFloorMetrics>, CompactorConfig) {
    let metrics = Arc::new(FormatFloorMetrics::default());
    let config = config_with(FormatFloorPolicy::new(false, Arc::clone(&metrics)));
    (metrics, config)
}

fn point(tenant: &TenantId, host: &str) -> NormalizedPoint {
    let labels = LabelSet::new(vec![
        Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "cpu_usage".to_string(),
        },
        Label {
            name: "host".to_string(),
            value: host.to_string(),
        },
    ])
    .expect("labels");
    NormalizedPoint {
        series_id: SeriesId::compute(tenant, "cpu_usage", &labels).expect("series id"),
        labels: Arc::new(labels),
        sample: Sample {
            ts_ns: 1_000,
            value: 1.0,
        },
        is_monotonic_sum: false,
    }
}

fn router(store: &Arc<InstrumentedStore<MemoryStore>>, policy: FormatFloorPolicy) -> IngestRouter {
    let store: Arc<dyn ObjectStoreBackend> = store.clone();
    IngestRouter::new(
        IngestConfig {
            shard_count: 1,
            max_flush_delay: Duration::from_secs(3600),
            max_flush_delay_idle: Duration::from_secs(3600),
            ..IngestConfig::default()
        },
        store,
        Signal::Metrics,
        Arc::new(SystemClock),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_format_floor_policy(policy)
}

/// What `ravel-cli maintain audit-versions` classifies the tenant's RSEG
/// floor as: the same census and classification it runs.
async fn audit_rseg_floor(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> FloorEvidence {
    let floors = read_floors_from_store(store, &tenant.hash(), Signal::Metrics)
        .await
        .expect("read floors")
        .expect("a record");
    let floor = floors.last().expect("one floor");
    let census = census_family(store, &tenant.hash(), Signal::Metrics, 1)
        .await
        .expect("census");
    let observed = FloorObservation {
        live_below_floor: census.live_below(floor.floor_version) as u64,
        newest_created_unix_ns: census.newest_live_created_unix_ns,
        scan_shards: 1,
    };
    classify_floor(floor, &observed)
}

/// The issue's acceptance test. A flush into a bucket whose RSEG floor is one
/// above the writer's version is refused with `BelowFormatFloor`, counted, and
/// PUTs nothing, and the strict write is answered with the refusal. Under the
/// override the same write lands, the override counter moves by one, and the
/// audit that classified the floor `Current` before now finds it
/// `Contradicted`.
#[tokio::test]
async fn flush_into_a_bucket_whose_floor_exceeds_the_writer_is_refused_and_counted() {
    let store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let acme = TenantId::new("floor-acme");
    provision(store.as_ref(), &acme, 1).await;
    raise_rseg_floor(store.as_ref(), &acme, rseg_writing() + 1).await;
    assert_eq!(
        audit_rseg_floor(store.as_ref(), &acme).await,
        FloorEvidence::Current
    );
    let metrics = Arc::new(FormatFloorMetrics::default());

    let refusing = router(&store, FormatFloorPolicy::new(false, Arc::clone(&metrics)));
    let before = puts(&store);
    let err = refusing
        .write(
            acme.clone(),
            vec![point(&acme, "a")],
            WriteMode::StrictArrival,
            Duration::from_secs(60),
        )
        .await
        .expect_err("the strict write is answered with the refusal");
    assert!(
        matches!(err, WriteError::BelowFormatFloor(r) if r == refused()),
        "{err:?}"
    );
    assert_eq!(metrics.refused(FormatFamily::Rseg), 1, "the refusal is counted");
    assert_eq!(puts(&store), before, "nothing is PUT");
    refusing.shutdown().await;
    assert_eq!(puts(&store), before, "nothing is PUT at shutdown either");
    assert_eq!(
        audit_rseg_floor(store.as_ref(), &acme).await,
        FloorEvidence::Current
    );

    let overriding = router(&store, FormatFloorPolicy::new(true, Arc::clone(&metrics)));
    let receipt = overriding
        .write(
            acme.clone(),
            vec![point(&acme, "a")],
            WriteMode::StrictArrival,
            Duration::from_secs(60),
        )
        .await
        .expect("the override lands the write");
    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(metrics.writes_below(FormatFamily::Rseg), 1);
    assert_eq!(metrics.refused(FormatFamily::Rseg), 1);
    assert_eq!(puts(&store), before + 2, "one data object, one commit record");
    overriding.shutdown().await;
    assert_eq!(
        audit_rseg_floor(store.as_ref(), &acme).await,
        FloorEvidence::Contradicted
    );
}

/// A compaction into a bucket whose floor is above its output version is
/// refused before the claim, the catalog reads and every PUT, with its one
/// floor read counted in the ledger's own phase.
#[tokio::test]
async fn compaction_below_the_floor_is_refused_before_any_put() {
    let store = floored_bucket().await;
    let (metrics, config) = refusing();
    let ledger = RequestLedger::new();
    let config = CompactorConfig {
        request_ledger: Some(ledger.clone()),
        ..config
    };
    let clock = FixedClock::new(sealed_now_ns());
    let before = puts(&store);

    let err = compact_bucket(&store, &clock, &config, &bucket())
        .await
        .expect_err("refused below the floor");

    assert!(
        matches!(err, MaintainError::BelowFormatFloor(r) if r == refused()),
        "{err:?}"
    );
    assert_eq!(puts(&store), before, "nothing is PUT");
    assert_eq!(metrics.refused(FormatFamily::Rseg), 1);
    let report = ledger.report();
    assert_eq!(report.floor_read.requests, 1);
    assert_eq!(report.coordinate.requests, 0, "no claim was taken");
    assert_eq!(report.catalog_read.requests, 0, "no catalog was read");
}

/// The maintenance pass over the same bucket keeps going: the refused
/// compaction is no compaction, and the retention evaluation stands.
#[tokio::test]
async fn a_refused_compaction_does_not_stop_the_maintenance_pass() {
    let store = floored_bucket().await;
    let (metrics, config) = refusing();
    let clock = FixedClock::new(sealed_now_ns());
    let before = puts(&store);

    let (retention, compaction) = maintain_bucket(
        &store,
        &clock,
        &config,
        &RetentionConfig::default(),
        &NoLeases,
        &bucket(),
    )
    .await
    .expect("the pass carries on past the refusal");

    assert_eq!(retention, RetentionOutcome::NoPolicy);
    assert!(compaction.is_none(), "{compaction:?}");
    assert_eq!(puts(&store), before);
    assert_eq!(metrics.refused(FormatFamily::Rseg), 1);
}

/// Under the override the compaction publishes and is counted once.
#[tokio::test]
async fn the_override_lets_a_compaction_write_below_the_floor() {
    let store = floored_bucket().await;
    let metrics = Arc::new(FormatFloorMetrics::default());
    let config = config_with(FormatFloorPolicy::new(true, Arc::clone(&metrics)));
    let clock = FixedClock::new(sealed_now_ns());

    let outcome = compact_bucket(&store, &clock, &config, &bucket())
        .await
        .expect("the override compacts");

    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    assert_eq!(metrics.writes_below(FormatFamily::Rseg), 1);
    assert_eq!(metrics.refused(FormatFamily::Rseg), 0);
}

/// An erasure rewrite is refused before its claim, its build and every PUT;
/// the request stays pending.
#[tokio::test]
async fn an_erasure_rewrite_below_the_floor_is_refused_before_any_put() {
    let store = floored_bucket().await;
    let (metrics, config) = refusing();
    let clock = FixedClock::new(sealed_now_ns());
    let request_id = Uuid::from_u128(0x1746);
    let pending = vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request: ErasureRequest {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: "__name__".to_string(),
                value: "victim".to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        },
    }];
    let mut memo = MaintainMemo::with_default_interval();
    let before = puts(&store);

    let err = erasure_rewrite_bucket(
        &store,
        &clock,
        &config,
        &NoLeases,
        &bucket(),
        &pending,
        &mut memo,
    )
    .await
    .expect_err("refused below the floor");

    assert!(
        matches!(err, MaintainError::BelowFormatFloor(r) if r == refused()),
        "{err:?}"
    );
    assert_eq!(puts(&store), before, "nothing is PUT");
    assert_eq!(metrics.refused(FormatFamily::Rseg), 1);
}

/// A bucket migration is refused before its claim and every PUT.
#[tokio::test]
async fn a_migration_below_the_floor_is_refused_before_any_put() {
    let store = floored_bucket().await;
    let (metrics, config) = refusing();
    let clock = FixedClock::new(sealed_now_ns());
    let before = puts(&store);

    let err = migrate_bucket_format(&store, &clock, &config, &bucket(), rseg_writing() + 1)
        .await
        .expect_err("refused below the floor");

    assert!(
        matches!(err, MaintainError::BelowFormatFloor(r) if r == refused()),
        "{err:?}"
    );
    assert_eq!(puts(&store), before, "nothing is PUT");
    assert_eq!(metrics.refused(FormatFamily::Rseg), 1);
}

/// `migrate_family` checks the floor even when its caller installed no
/// policy, as `ravel-cli maintain migrate` does not.
#[tokio::test]
async fn a_family_migration_checks_the_floor_with_no_policy_installed() {
    let store = floored_bucket().await;
    let config = CompactorConfig::default();
    assert!(config.format_floor.is_none());
    let clock = FixedClock::new(sealed_now_ns());
    let before = puts(&store);

    let err = migrate_family(
        &store,
        &clock,
        &config,
        tenant_hash(),
        Signal::Metrics,
        FormatFamily::Rseg.as_str(),
        rseg_writing() + 1,
        SHARDS,
        MigrateBudget::unlimited(),
        "test",
    )
    .await
    .expect_err("refused below the floor");

    assert!(
        matches!(err, MaintainError::BelowFormatFloor(r) if r == refused()),
        "{err:?}"
    );
    assert_eq!(puts(&store), before, "nothing is PUT");
}
