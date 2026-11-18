//! The fresh-store `sys/compat` bootstrap of a retired-reader release
//! (ADR-4233 decision 7, fresh stores).
//!
//! Each test drives `startup_retry::run_startup_store_gates`, the part of
//! `main`'s startup store checks that runs the retired-reader gate, the
//! bootstrap and tenancy pinning, with a test-only non-empty retired set:
//! `RETIRED_READERS` is empty in this build, so the gate never reads the
//! store there. The rest of the startup checks install the process-global
//! tenant-hash scheme, so a second start in one process stops before them.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use ravel_catalog::compat::{
    COMPAT_KEY, CompatKeys, FAMILY_RSEG, KNOWN_FAMILIES, create_compat, read_compat,
};
use ravel_maintain::DEFAULT_PUBLISH_LIFETIME_NS;
use ravel_maintain::compat_init::{InitConfig, baseline_record, baseline_version};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, GateHandle, Occurrence, Op, Rule, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_server::Mode;
use ravel_server::compat_gate::{
    Bootstrap, CertifiedFamily, CompatGateError, GateOutcome, RetiredReader, bootstrap_fresh_store,
    bootstrap_record,
};
use ravel_server::config::StoreKind;
use ravel_server::startup_retry::{
    StartupClock, StartupRetry, StartupStoreGates, StartupStoreInputs, run_startup_store_gates,
};
use ravel_types::ConfiguredScheme;

const KEY: [u8; 32] = [0x5C; 32];
const TENANCY_KEY: &str = "sys/tenancy";
const NOW_NS: i64 = 1_800_000_000_000_000_000;
const HOUR_NS: i64 = 3_600_000_000_000;

/// rseg's reader retired below this build's baseline write version, so a
/// fresh store's own writes are readable.
fn retired() -> Vec<RetiredReader> {
    vec![RetiredReader {
        family: FAMILY_RSEG,
        oldest_readable: baseline_version(FAMILY_RSEG).expect("rseg baseline"),
    }]
}

fn keyed() -> ConfiguredScheme {
    ConfiguredScheme::Keyed(Box::new(KEY))
}

fn keys() -> CompatKeys {
    CompatKeys::from_file_keys(&[KEY]).expect("key")
}

/// One start's gates under `retry`, at `now_ns`.
async fn start_with(
    retry: &StartupRetry,
    store: &Arc<dyn ObjectStoreBackend>,
    tenancy: ConfiguredScheme,
    retired: &[RetiredReader],
    now_ns: i64,
) -> anyhow::Result<StartupStoreGates> {
    let inputs = StartupStoreInputs {
        store,
        store_kind: StoreKind::Memory,
        backend_identity: None,
        require_bucket_protection: false,
        store_s3: None,
        tenancy,
        retired_readers: retired,
        tenant_kms: None,
        shard_count: 1,
        mode: Mode::All,
    };
    run_startup_store_gates(retry, &inputs, &|| now_ns).await
}

async fn start(
    store: &Arc<dyn ObjectStoreBackend>,
    tenancy: ConfiguredScheme,
    now_ns: i64,
) -> anyhow::Result<StartupStoreGates> {
    start_with(
        &StartupRetry::disabled(),
        store,
        tenancy,
        &retired(),
        now_ns,
    )
    .await
}

fn refusal(result: anyhow::Result<StartupStoreGates>) -> anyhow::Error {
    match result {
        Ok(gates) => panic!("expected a refusal, started with {gates:?}"),
        Err(err) => err,
    }
}

async fn every_object(store: &dyn ObjectStoreBackend) -> Vec<(String, String)> {
    let mut objects = Vec::new();
    let mut page = None;
    loop {
        let listed = store.list("", page).await.expect("list");
        objects.extend(
            listed
                .objects
                .into_iter()
                .map(|meta| (meta.key, format!("{:?}", meta.version))),
        );
        match listed.next {
            Some(next) => page = Some(next),
            None => return objects,
        }
    }
}

async fn stored_compat(store: &dyn ObjectStoreBackend) -> sysproto::CompatRecord {
    read_compat(store, &keys())
        .await
        .expect("read sys/compat")
        .expect("sys/compat exists")
        .0
}

/// The record a bootstrap at `census_ns` must leave: compat init's record on
/// an empty store, plus rseg's empty-census certificate and its watermark.
fn assert_bootstrapped(record: &sysproto::CompatRecord, census_ns: i64) {
    let init = baseline_record(
        &InitConfig {
            baseline_release_major: 0,
            baseline_release_minor: 34,
            max_publish_lifetime_ns: DEFAULT_PUBLISH_LIFETIME_NS,
        },
        &BTreeMap::new(),
    )
    .expect("init record");
    let without_certificates = sysproto::CompatRecord {
        format_version: 0,
        mac: Vec::new(),
        families: record
            .families
            .iter()
            .map(|family| sysproto::CompatFamily {
                certificate: None,
                late_writes_verified_through_unix_ns: 0,
                ..family.clone()
            })
            .collect(),
        ..record.clone()
    };
    assert_eq!(
        without_certificates, init,
        "compat init's empty-store record"
    );
    assert_eq!(record.compat_epoch, 0);
    assert_eq!(
        (record.baseline_release_major, record.baseline_release_minor),
        (0, 34)
    );
    assert_eq!(record.max_publish_lifetime_ns, DEFAULT_PUBLISH_LIFETIME_NS);
    assert!(record.adopted.is_empty());
    assert_eq!(record.families.len(), KNOWN_FAMILIES.len());
    for family in &record.families {
        assert_eq!(
            family.active_write_version,
            baseline_version(&family.family).expect("baseline"),
            "{}",
            family.family
        );
        assert_eq!(family.activated_epoch, 0, "{}", family.family);
        assert!(!family.contradicted, "{}", family.family);
        if family.family != FAMILY_RSEG {
            assert_eq!(family.certificate, None, "{}", family.family);
            assert_eq!(family.late_writes_verified_through_unix_ns, 0);
        }
    }
    let rseg = record
        .families
        .iter()
        .find(|family| family.family == FAMILY_RSEG)
        .expect("rseg entry");
    assert_eq!(
        rseg.certificate,
        Some(sysproto::Certificate {
            version: baseline_version(FAMILY_RSEG).expect("baseline"),
            certified_unix_ns: census_ns,
            member_count: 0,
            newest_created_unix_ns: 0,
            buckets_scanned: 0,
            tenant_set_digest: blake3::Hasher::new().finalize().as_bytes().to_vec(),
        })
    );
    assert_eq!(rseg.late_writes_verified_through_unix_ns, census_ns + 1);
}

fn certified_at(census_ns: i64) -> GateOutcome {
    GateOutcome::Certified {
        families: vec![CertifiedFamily {
            family: FAMILY_RSEG,
            certificate_version: baseline_version(FAMILY_RSEG).expect("baseline"),
            watermark_unix_ns: census_ns + 1,
            watermark_stale: false,
        }],
    }
}

/// The first start of a retired-reader release on an empty keyed store
/// creates `sys/compat` at the baseline versions, so the second start is
/// certified instead of refused for `sys/tenancy` without `sys/compat`.
#[tokio::test]
async fn a_second_start_on_a_store_the_first_start_found_empty_is_not_refused() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let first = start(&store, keyed(), NOW_NS).await.expect("first start");
    assert!(store.head(TENANCY_KEY).await.is_ok());
    let second = start(&store, keyed(), NOW_NS + HOUR_NS)
        .await
        .expect("second start");

    assert_eq!(first.retired_reader_gate, GateOutcome::Fresh);
    assert_eq!(first.compat_bootstrap, Some(Bootstrap::Created));
    assert_eq!(second.retired_reader_gate, certified_at(NOW_NS));
    assert_eq!(second.compat_bootstrap, None);
    assert_bootstrapped(&stored_compat(store.as_ref()).await, NOW_NS);
}

/// `sys/compat` is the first object a fresh start writes: a start whose
/// create fails leaves the store empty, so the next start is fresh again
/// rather than refused for `sys/tenancy` without `sys/compat`.
#[tokio::test]
async fn a_start_whose_compat_create_fails_leaves_the_store_fresh() {
    let fault = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan {
            sequences: vec![
                Sequence::new(Op::Put)
                    .with_key_contains(COMPAT_KEY)
                    .then_fault(ScriptedFault::Permanent("denied".to_string()))
                    .then_passthrough(),
            ],
            ..FaultPlan::empty()
        },
    ));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();

    let first = start(&store, keyed(), NOW_NS).await;
    let after_first = every_object(fault.inner()).await;
    let retried = start(&store, keyed(), NOW_NS + HOUR_NS)
        .await
        .expect("the next start finds the store fresh");

    let err = refusal(first);
    assert!(
        matches!(
            err.downcast_ref::<CompatGateError>(),
            Some(CompatGateError::BootstrapCreate(_))
        ),
        "{err:?}"
    );
    assert_eq!(fault.fault_count(Op::Put, FaultKind::Permanent), 1);
    assert_eq!(after_first, Vec::new());
    assert_eq!(retried.retired_reader_gate, GateOutcome::Fresh);
    assert_eq!(retried.compat_bootstrap, Some(Bootstrap::Created));

    let third = start(&store, keyed(), NOW_NS + 2 * HOUR_NS)
        .await
        .expect("third start");
    assert_eq!(third.retired_reader_gate, certified_at(NOW_NS + HOUR_NS));
}

/// Waits until `n` creates of `sys/compat` are held, failing rather than
/// hanging when fewer ever arrive.
async fn creates_held(held: &GateHandle, n: usize) {
    tokio::time::timeout(Duration::from_secs(30), held.wait_until_held(n))
        .await
        .unwrap_or_else(|_| panic!("{n} sys/compat creates never arrived"));
}

/// Two fresh starts racing on one empty store both start: one creates
/// `sys/compat`, the other's create-only write loses, leaves the winner's
/// record untouched, and starts on it.
#[tokio::test]
async fn two_concurrent_fresh_starts_both_start_on_one_sys_compat() {
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let held = fault.hold(Op::Put, Some(COMPAT_KEY.to_string()), Occurrence::Always);
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();

    let censuses = [NOW_NS, NOW_NS + 5];
    let starts = censuses.map(|census| {
        let store = Arc::clone(&store);
        tokio::spawn(async move { start(&store, keyed(), census).await })
    });
    // Both starts found the store fresh and reached their create.
    creates_held(&held, 2).await;
    for (id, _, key) in held.held_details() {
        assert_eq!(key, COMPAT_KEY);
        assert!(held.release(id));
    }
    let mut outcomes = Vec::new();
    for (census, started) in censuses.into_iter().zip(starts) {
        let gates = started.await.expect("join").expect("both start");
        assert_eq!(gates.retired_reader_gate, GateOutcome::Fresh);
        outcomes.push((census, gates.compat_bootstrap.expect("bootstrapped")));
    }
    let created: Vec<i64> = outcomes
        .iter()
        .filter(|(_, outcome)| *outcome == Bootstrap::Created)
        .map(|(census, _)| *census)
        .collect();
    let existing = outcomes
        .iter()
        .filter(|(_, outcome)| *outcome == Bootstrap::Existing)
        .count();
    assert_eq!(created.len(), 1, "{outcomes:?}");
    assert_eq!(existing, 1, "{outcomes:?}");

    let objects = every_object(fault.inner()).await;
    assert_eq!(
        objects.iter().filter(|(key, _)| key == COMPAT_KEY).count(),
        1
    );
    assert_bootstrapped(&stored_compat(fault.inner()).await, created[0]);

    let next = start(&store, keyed(), NOW_NS + HOUR_NS)
        .await
        .expect("next start");
    assert_eq!(next.retired_reader_gate, certified_at(created[0]));
}

/// A lost create starts only on a record the gate accepts: one written by
/// another process without a certificate refuses, and stays as written.
#[tokio::test]
async fn a_lost_create_against_an_uncertified_record_refuses_and_leaves_it() {
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let held = fault.hold(Op::Put, Some(COMPAT_KEY.to_string()), Occurrence::Always);
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();

    let starting = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { start(&store, keyed(), NOW_NS).await })
    };
    creates_held(&held, 1).await;
    let mut uncertified = bootstrap_record(&retired(), NOW_NS).expect("record");
    for family in &mut uncertified.families {
        family.certificate = None;
        family.late_writes_verified_through_unix_ns = 0;
    }
    create_compat(fault.inner(), &keys(), uncertified)
        .await
        .expect("the other process's record");
    let before = every_object(fault.inner()).await;
    for (id, _, _) in held.held_details() {
        assert!(held.release(id));
    }

    let err = refusal(starting.await.expect("join"));
    assert!(
        matches!(
            err.downcast_ref::<CompatGateError>(),
            Some(CompatGateError::CertificateAbsent {
                family: FAMILY_RSEG,
                ..
            })
        ),
        "{err:?}"
    );
    assert_eq!(every_object(fault.inner()).await, before);
    assert!(matches!(
        fault.inner().head(TENANCY_KEY).await,
        Err(StoreError::NotFound)
    ));
}

/// A create reported as lost whose record cannot then be read refuses.
#[tokio::test]
async fn a_lost_create_with_nothing_to_read_refuses() {
    let fault = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains(COMPAT_KEY),
        ),
    ));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let err = refusal(start(&store, keyed(), NOW_NS).await);
    assert!(
        matches!(
            err.downcast_ref::<CompatGateError>(),
            Some(CompatGateError::BootstrapVanished)
        ),
        "{err:?}"
    );
    assert_eq!(
        fault.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1
    );
    assert_eq!(every_object(fault.inner()).await, Vec::new());
}

/// Virtual time: a retry sleep returns at once.
#[derive(Default)]
struct InstantClock {
    now: Mutex<Duration>,
}

#[async_trait::async_trait]
impl StartupClock for InstantClock {
    fn elapsed(&self) -> Duration {
        *self.now.lock().unwrap()
    }

    async fn sleep(&self, duration: Duration) {
        *self.now.lock().unwrap() += duration;
    }

    async fn attempt_timer(&self, _duration: Duration) {
        std::future::pending::<()>().await;
    }
}

/// A create that landed but whose acknowledgement was lost is retried; the
/// retry finds the record, accepts it, and starts.
#[tokio::test]
async fn an_acknowledgement_lost_create_is_retried_and_starts() {
    let fault = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan {
            sequences: vec![
                Sequence::new(Op::Put)
                    .with_key_contains(COMPAT_KEY)
                    .then_fault(ScriptedFault::DuplicateDelivery)
                    .then_passthrough(),
            ],
            ..FaultPlan::empty()
        },
    ));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let retry = StartupRetry::new(
        Arc::new(InstantClock::default()),
        Duration::from_secs(60),
        1,
    );
    let gates = start_with(&retry, &store, keyed(), &retired(), NOW_NS)
        .await
        .expect("starts");
    assert_eq!(gates.compat_bootstrap, Some(Bootstrap::Existing));
    assert_eq!(fault.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_bootstrapped(&stored_compat(fault.inner()).await, NOW_NS);
}

/// An unkeyed fresh store behaves as it did before the bootstrap: the first
/// start writes no `sys/compat` and pins `sys/tenancy`, so the next start of
/// the same release is refused for `sys/tenancy` without `sys/compat`.
#[tokio::test]
async fn an_unkeyed_fresh_store_gets_no_sys_compat() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let first = start(&store, ConfiguredScheme::Unkeyed, NOW_NS)
        .await
        .expect("first start");
    assert_eq!(first.retired_reader_gate, GateOutcome::Fresh);
    assert_eq!(first.compat_bootstrap, Some(Bootstrap::Unkeyed));
    let objects = every_object(store.as_ref()).await;
    assert_eq!(
        objects
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec![TENANCY_KEY]
    );

    let err = refusal(start(&store, ConfiguredScheme::Unkeyed, NOW_NS + HOUR_NS).await);
    assert!(
        matches!(
            err.downcast_ref::<CompatGateError>(),
            Some(CompatGateError::TenancyWithoutCompat)
        ),
        "{err:?}"
    );
    assert_eq!(every_object(store.as_ref()).await, objects);
}

/// A store holding `t/` data and neither marker is not fresh: the start is
/// refused and nothing is written.
#[tokio::test]
async fn a_store_with_tenant_data_and_no_markers_is_refused_and_untouched() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    store
        .put(
            "t/acme/m/commits/00000001",
            Bytes::from_static(b"commit"),
            PutOptions::default(),
        )
        .await
        .expect("put tenant data");
    let before = every_object(store.as_ref()).await;
    let err = refusal(start(&store, keyed(), NOW_NS).await);
    assert!(
        matches!(
            err.downcast_ref::<CompatGateError>(),
            Some(CompatGateError::TenantDataWithoutMarkers)
        ),
        "{err:?}"
    );
    assert_eq!(every_object(store.as_ref()).await, before);
    assert_eq!(before.len(), 1);
}

/// Without a retired reader the bootstrap sends no request, keyed or not, so
/// a build with an empty `RETIRED_READERS` writes nothing new.
#[tokio::test]
async fn the_bootstrap_sends_no_request_without_a_retired_reader_or_a_key() {
    let plan = FaultPlan {
        rules: [Op::Head, Op::Get, Op::Put, Op::List]
            .into_iter()
            .map(|op| Rule::new(op, ScriptedFault::Timeout))
            .collect(),
        ..FaultPlan::empty()
    };
    let store = FaultStore::new(MemoryStore::new(), plan);
    assert!(ravel_server::compat_gate::RETIRED_READERS.is_empty());
    assert_eq!(
        bootstrap_fresh_store(
            &store,
            Some(&KEY),
            ravel_server::compat_gate::RETIRED_READERS,
            NOW_NS
        )
        .await
        .expect("no-op"),
        Bootstrap::NoRetiredReader
    );
    assert_eq!(
        bootstrap_fresh_store(&store, None, &retired(), NOW_NS)
            .await
            .expect("no-op"),
        Bootstrap::Unkeyed
    );
    assert!(store.counters_snapshot().is_empty());
    assert_eq!(every_object(store.inner()).await, Vec::new());
}

/// A start that stopped after its create and before pinning `sys/tenancy`
/// leaves a store the next start reads as adopted and starts on.
#[tokio::test]
async fn a_start_stopped_before_tenancy_pinning_leaves_an_adopted_store() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    assert_eq!(
        bootstrap_fresh_store(store.as_ref(), Some(&KEY), &retired(), NOW_NS)
            .await
            .expect("create"),
        Bootstrap::Created
    );
    assert!(matches!(
        store.head(TENANCY_KEY).await,
        Err(StoreError::NotFound)
    ));
    let next = start(&store, keyed(), NOW_NS + HOUR_NS)
        .await
        .expect("next start");
    assert_eq!(next.retired_reader_gate, certified_at(NOW_NS));
    assert!(store.head(TENANCY_KEY).await.is_ok());
}

/// A release that reads nothing at its own baseline write version cannot
/// certify a fresh store: it refuses before writing.
#[tokio::test]
async fn a_record_the_gate_would_refuse_is_never_written() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let baseline = baseline_version(FAMILY_RSEG).expect("baseline");
    let above_baseline = [RetiredReader {
        family: FAMILY_RSEG,
        oldest_readable: baseline + 1,
    }];
    let err = refusal(
        start_with(
            &StartupRetry::disabled(),
            &store,
            keyed(),
            &above_baseline,
            NOW_NS,
        )
        .await,
    );
    assert!(
        matches!(
            err.downcast_ref::<CompatGateError>(),
            Some(CompatGateError::CertificateBelow { certified, oldest_readable, .. })
                if *certified == baseline && *oldest_readable == baseline + 1
        ),
        "{err:?}"
    );
    assert_eq!(every_object(store.as_ref()).await, Vec::new());
}
