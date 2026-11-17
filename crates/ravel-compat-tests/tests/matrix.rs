//! The executable compatibility matrix (ADR-4233 decision 9).
//!
//! Each `#[tokio::test]` below pins one row of the ADR's matrix (decision 9's
//! table and decision 5's rollback table) with the real library write/read
//! paths against a `MemoryStore` (or a `FaultStore` where a row needs a
//! failure). A row the current build cannot express end to end is written as a
//! test that asserts the typed refusal the ADR names, never a skipped test.
//!
//! Rows pinned elsewhere, not duplicated here:
//!   - the two-version compaction/migrate rows ("corpus at N, drill activation,
//!     migrate"; a run writing N+1 compacts an input at N+1; force-2 lifts
//!     parts) are pinned by `crates/ravel-maintain/tests/format_drill.rs`,
//!     which the compat-tests CI lane runs under `--features format-drill`;
//!   - the teardown-flush census and late-write rows are pinned by
//!     `tests/certify_flags_teardown_pair.rs` in this crate;
//!   - the frozen-corpus "written by R, read by HEAD" row is pinned by the
//!     corpus test in this crate (`tests/corpus.rs`).
//!
//! Reader fail-closed on an out-of-window version (decision 4, and the Flight
//! SQL scan row) is pinned by `ravel-segment`'s `reader_version_window` tests
//! and the end-to-end test; the product query/Flight surfaces are out of this
//! crate's scope. See the task report for the rows deferred to product-crate
//! coverage and the product gaps that block them (no `ravel-cli compat`
//! subcommand yet; the attestation flag ships as `writer_roles_can_read_compat`
//! rather than `--pre-baseline-processes-terminated`; the fold postings build
//! still refuses a drill-version segment).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use prost::Message;
use ravel_catalog::compat::{
    COMPAT_FORMAT_VERSION, CompatClock, CompatKeys, CompatView, FAMILY_RSEG, activate_family,
    create_compat,
};
use ravel_catalog::{
    AbsentPolicy, CompatRefusal, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
    ProvisioningError, provisioning_key, raise_compat_fence, validate_or_adopt,
    validate_or_adopt_fenced,
};
use ravel_maintain::{
    ActivateError, ActivateRequest, CertifyRefusal, CertifyReport, CertifyTimings, CompatInitError,
    InitConfig, InitOutcome, WritableVersions, activate, certify, init_compat,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{Signal, TenantHash, TenantId};

const SECOND_NS: i64 = 1_000_000_000;
const HOUR_NS: i64 = 3_600 * SECOND_NS;
/// A fixed base instant well inside the representable range.
const T0: i64 = 1_800_000_000 * SECOND_NS;
/// RSEG's baseline version; the drill version is one above it.
const N: u32 = 7;
const N_PLUS_1: u32 = 8;

#[derive(Debug)]
struct ManualClock(AtomicI64);

impl ManualClock {
    fn new(ns: i64) -> Self {
        ManualClock(AtomicI64::new(ns))
    }
}

impl CompatClock for ManualClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn keys() -> CompatKeys {
    CompatKeys::from_file_keys(&[[4; 32]]).expect("one key")
}

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn hash() -> TenantHash {
    tenant().hash()
}

/// A two-key listing page, so every census and walk pages.
fn store() -> Arc<MemoryStore> {
    Arc::new(MemoryStore::with_page_size(2))
}

/// `sys/compat` present with RSEG at `active`, nothing else activated.
async fn write_compat(store: &dyn ObjectStoreBackend, active: u32) {
    create_compat(
        store,
        &keys(),
        sysproto::CompatRecord {
            format_version: COMPAT_FORMAT_VERSION,
            baseline_release_major: 0,
            baseline_release_minor: 34,
            families: vec![sysproto::CompatFamily {
                family: FAMILY_RSEG.to_string(),
                active_write_version: active,
                ..Default::default()
            }],
            max_publish_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
}

/// A view over `store`, refreshed once at `T0`.
async fn fresh_view(store: Arc<dyn ObjectStoreBackend>) -> CompatView {
    let view = CompatView::new(store, keys(), Arc::new(ManualClock::new(T0)));
    view.refresh().await.expect("refresh");
    view
}

async fn provision_v3(store: &dyn ObjectStoreBackend) {
    validate_or_adopt(
        store,
        &hash(),
        Signal::Metrics,
        1,
        T0 - HOUR_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
}

async fn read_prov(store: &dyn ObjectStoreBackend) -> sysproto::ProvisioningRecord {
    let bytes = store
        .get(&provisioning_key(&hash(), Signal::Metrics), GetRange::Full)
        .await
        .expect("get prov")
        .data;
    sysproto::ProvisioningRecord::decode(bytes.as_ref()).expect("decode prov")
}

fn init_config() -> InitConfig {
    InitConfig {
        baseline_release_major: 0,
        baseline_release_minor: 34,
        max_publish_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
    }
}

fn activate_request(version: u32, writable_attested: bool) -> ActivateRequest {
    ActivateRequest {
        family: FAMILY_RSEG.to_string(),
        version,
        activated_by: "matrix test".to_string(),
        writer_roles_can_read_compat: writable_attested,
        alert_rule_tenants: Vec::new(),
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
        resume_from: None,
    }
}

fn writable_n_and_n1() -> WritableVersions {
    WritableVersions::current().with_family(FAMILY_RSEG, vec![N, N_PLUS_1])
}

// === Version resolution: mixed nodes and rollback (decision 5; matrix rows
// "mixed nodes before/after activation", "rollback before activation") ========

/// Before activation, both an N+1-capable build and an N-only build resolve to
/// N for the family: every object written is version N (matrix row "mixed nodes
/// N-only and N+1, before activation", and decision 5 row "B+1 installed, F not
/// activated, roll back to B").
#[tokio::test]
async fn mixed_nodes_before_activation_both_write_n() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let view = fresh_view(store.clone()).await;

    let capable = view
        .write_policy(
            T0,
            FAMILY_RSEG,
            &[N as u16, N_PLUS_1 as u16],
            N as u16,
            None,
        )
        .expect("capable build writes");
    let n_only = view
        .write_policy(T0, FAMILY_RSEG, &[N as u16], N as u16, None)
        .expect("N-only build writes");

    assert_eq!(capable, N as u16, "capable build emits N before activation");
    assert_eq!(n_only, N as u16, "N-only build emits N before activation");
}

/// After activation, the N+1-capable build resolves to the active version N+1.
#[tokio::test]
async fn n_plus_one_node_writes_active_after_activation() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    activate_family(store.as_ref(), &keys(), FAMILY_RSEG, N_PLUS_1, "t", T0)
        .await
        .expect("activate");
    let view = fresh_view(store.clone()).await;

    let v = view
        .write_policy(
            T0,
            FAMILY_RSEG,
            &[N as u16, N_PLUS_1 as u16],
            N as u16,
            None,
        )
        .expect("capable build writes the active version");
    assert_eq!(v, N_PLUS_1 as u16);
}

/// After activation, an N-only build refuses the family's publishes with the
/// typed `ActiveVersionNotWritable`, and writes nothing (matrix row "mixed
/// nodes, after activation"; decision 5 row "F activated and N+1 objects exist,
/// roll back to B").
#[tokio::test]
async fn n_only_node_refuses_the_family_after_activation() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    activate_family(store.as_ref(), &keys(), FAMILY_RSEG, N_PLUS_1, "t", T0)
        .await
        .expect("activate");
    let view = fresh_view(store.clone()).await;

    let refusal = view
        .write_policy(T0, FAMILY_RSEG, &[N as u16], N as u16, None)
        .expect_err("N-only build refuses");
    assert!(
        matches!(
            refusal,
            CompatRefusal::ActiveVersionNotWritable { ref family, active: 8, .. }
                if family == FAMILY_RSEG
        ),
        "{refusal:?}"
    );
}

/// A writer whose last successful read is older than the hard horizon refuses
/// to publish with `ViewStale`: the stale writer stops past `H` (matrix row
/// "stale sys/compat view ... stale writer emits only the older version and
/// stops past H").
#[tokio::test]
async fn a_stale_view_refuses_to_publish_past_the_horizon() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let view = fresh_view(store.clone()).await;

    // Fresh at T0, but a day later with no further refresh the view is stale.
    let refusal = view
        .write_policy(
            T0 + 24 * HOUR_NS,
            FAMILY_RSEG,
            &[N as u16, N_PLUS_1 as u16],
            N as u16,
            None,
        )
        .expect_err("stale view refuses");
    assert!(
        matches!(refusal, CompatRefusal::ViewStale { .. }),
        "{refusal:?}"
    );
}

/// A process that finds `sys/compat` absent while a bucket's provisioning
/// record carries a `compat_epoch` refuses that bucket's publishes with
/// `CompatRecordMissing`: a deleted or rolled-back `sys/compat` is never
/// treated as "baseline" (matrix row "sys/compat deleted after activation:
/// publishes refused with CompatRecordMissing").
#[tokio::test]
async fn absent_compat_with_a_fenced_bucket_refuses_record_missing() {
    let store = store();
    // No sys/compat is ever written. The view refreshes against the absence.
    let view = fresh_view(store.clone()).await;
    let fenced_bucket = sysproto::ProvisioningRecord {
        format_version: 4,
        compat_epoch: 1,
        ..Default::default()
    };

    let refusal = view
        .write_policy(
            T0,
            FAMILY_RSEG,
            &[N as u16, N_PLUS_1 as u16],
            N as u16,
            Some(&fenced_bucket),
        )
        .expect_err("absent compat over a fenced bucket refuses");
    assert!(
        matches!(refusal, CompatRefusal::CompatRecordMissing { .. }),
        "{refusal:?}"
    );
}

// === Activation is durable and monotone (decision 3) =========================

/// `activate` refuses unless `sys/compat` exists: it is run only after `init`
/// (decision 3, "it refuses unless sys/compat exists").
#[tokio::test]
async fn activate_refuses_without_init() {
    let store = store();
    let clock = ManualClock::new(T0);
    let err = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable_n_and_n1(),
        &activate_request(N_PLUS_1, true),
    )
    .await
    .expect_err("no sys/compat");
    assert!(matches!(err, ActivateError::RecordAbsent), "{err:?}");
}

/// `activate` never lowers or re-sets a version: a request at the current
/// active version is refused `NotAboveActive`.
#[tokio::test]
async fn activate_refuses_a_target_not_above_active() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let clock = ManualClock::new(T0);
    let err = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable_n_and_n1(),
        &activate_request(N, true),
    )
    .await
    .expect_err("not above active");
    assert!(
        matches!(
            err,
            ActivateError::NotAboveActive {
                active: 7,
                requested: 7,
                ..
            }
        ),
        "{err:?}"
    );
}

/// `activate` moves exactly one version: a jump of two is refused `VersionJump`.
#[tokio::test]
async fn activate_refuses_a_version_jump() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let clock = ManualClock::new(T0);
    let writable = WritableVersions::current().with_family(FAMILY_RSEG, vec![N, N_PLUS_1, 9]);
    let err = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable,
        &activate_request(9, true),
    )
    .await
    .expect_err("version jump");
    assert!(
        matches!(
            err,
            ActivateError::VersionJump {
                active: 7,
                requested: 9,
                ..
            }
        ),
        "{err:?}"
    );
}

/// The external prerequisite is attested at activation: without the operator's
/// attestation (the flag the ADR amendment names
/// `--pre-baseline-processes-terminated`; it ships today as the
/// `writer_roles_can_read_compat` field) `activate` refuses (decision 3,
/// Consequences "compat activate must refuse without the operator's
/// attestation").
#[tokio::test]
async fn activate_refuses_without_the_attestation() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let clock = ManualClock::new(T0);
    let err = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable_n_and_n1(),
        &activate_request(N_PLUS_1, false),
    )
    .await
    .expect_err("no attestation");
    assert!(
        matches!(err, ActivateError::WriterRolesCannotReadCompat),
        "{err:?}"
    );
}

/// A successful activation raises the version and completes the fence walk,
/// raising the provisioned bucket's fence (decision 3/4: `compat activate`
/// fences every existing provisioned bucket and records
/// `fence_complete_unix_ns`).
#[tokio::test]
async fn activate_completes_the_fence_walk() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    provision_v3(store.as_ref()).await;
    let clock = ManualClock::new(T0);

    let report = activate(
        store.as_ref(),
        Some(&keys()),
        &clock,
        &writable_n_and_n1(),
        &activate_request(N_PLUS_1, true),
    )
    .await
    .expect("activate");

    assert_eq!(report.active_write_version, N_PLUS_1);
    assert!(report.walk.complete, "{:?}", report.walk);
    assert!(
        report.walk.fence_complete_unix_ns.is_some(),
        "{:?}",
        report.walk
    );
    assert_eq!(report.walk.buckets_raised, 1, "{:?}", report.walk);
    // The bucket's provisioning record is now fenced at version 4.
    let prov = read_prov(store.as_ref()).await;
    assert_eq!(prov.format_version, 4, "{prov:?}");
    assert!(prov.compat_epoch >= 1, "{prov:?}");
}

// === init adopts a clean store (decision 1; matrix rows for `compat init`) ====

/// `init` over a clean store creates `sys/compat` at the baseline versions.
#[tokio::test]
async fn init_adopts_a_clean_store() {
    let store = store();
    let clock = ManualClock::new(T0);
    let report = init_compat(store.as_ref(), Some(&keys()), &clock, &init_config())
        .await
        .expect("init");
    assert_eq!(report.outcome, InitOutcome::Created);
    assert!(report.census.is_some(), "a census ran");
    // The RSEG family is adopted at its baseline.
    let rseg = report
        .record
        .families
        .iter()
        .find(|f| f.family == FAMILY_RSEG)
        .expect("rseg entry");
    assert_eq!(rseg.active_write_version, N);
}

/// `init` is idempotent on the record it would write: a second run finds the
/// record already present and writes nothing.
#[tokio::test]
async fn init_is_idempotent() {
    let store = store();
    let clock = ManualClock::new(T0);
    init_compat(store.as_ref(), Some(&keys()), &clock, &init_config())
        .await
        .expect("first init");
    let again = init_compat(store.as_ref(), Some(&keys()), &clock, &init_config())
        .await
        .expect("second init");
    assert_eq!(again.outcome, InitOutcome::AlreadyInitialized);
    assert!(again.census.is_none(), "no census on the second run");
}

/// `init` refuses a store whose provisioning record carries a `compat_epoch`
/// but has no `sys/compat`: the record was deleted or rolled back and must be
/// restored from bucket versioning, never re-created at the baseline (matrix
/// row "sys/compat deleted after activation: compat init refuses").
#[tokio::test]
async fn init_refuses_a_fenced_store_missing_compat() {
    let store = store();
    provision_v3(store.as_ref()).await;
    raise_compat_fence(store.as_ref(), &hash(), Signal::Metrics, 1, T0)
        .await
        .expect("fence the bucket");
    let clock = ManualClock::new(T0);
    let err = init_compat(store.as_ref(), Some(&keys()), &clock, &init_config())
        .await
        .expect_err("fenced store refuses init");
    assert!(
        matches!(
            err,
            CompatInitError::ProvisioningFenced {
                compat_epoch: 1,
                ..
            }
        ),
        "{err:?}"
    );
}

/// `init` fails closed on an incomplete listing rather than shortening the
/// population (matrix row "incomplete listing: certify/migrate refuse"; here at
/// the `init` census, which lists strictly).
#[tokio::test]
async fn init_refuses_on_an_incomplete_listing() {
    let inner = store();
    let store = FaultStore::new(
        inner,
        FaultPlan::empty().with_rule(Rule::new(
            Op::List,
            ScriptedFault::Transient("list down".into()),
        )),
    );
    let clock = ManualClock::new(T0);
    let err = init_compat(&store, Some(&keys()), &clock, &init_config())
        .await
        .expect_err("listing failure refuses");
    assert!(matches!(err, CompatInitError::List(_)), "{err:?}");
}

// === Provisioning-record fencing (decision 4; matrix rows for fenced/unfenced
// provisioning records and the born-fenced tenant) ===========================

/// A tenant onboarded after activation is born fenced: its record is written
/// directly at version 4 with the current epoch and `compat_fenced_unix_ns = 0`
/// (no pre-baseline process ever saw it at version 3), so the `certify` wait is
/// not restarted (matrix row "a tenant onboarded after activation").
#[tokio::test]
async fn a_tenant_onboarded_after_activation_is_born_fenced() {
    let store = store();
    validate_or_adopt_fenced(
        store.as_ref(),
        &hash(),
        Signal::Metrics,
        1,
        T0,
        AbsentPolicy::CreateFromConfig,
        Some(std::num::NonZeroU64::new(3).unwrap()),
    )
    .await
    .expect("born-fenced provision");

    let prov = read_prov(store.as_ref()).await;
    assert_eq!(prov.format_version, 4, "born at version 4: {prov:?}");
    assert_eq!(prov.compat_epoch, 3, "{prov:?}");
    assert_eq!(
        prov.compat_fenced_unix_ns, 0,
        "born fenced stamps no time: {prov:?}"
    );
}

/// A fenced (version 4) provisioning record keeps fields 8 and 9 through a
/// reshard and a floor raise by the baseline (matrix row "fenced (version 4)
/// provisioning record: reshard and floor raise by the baseline").
#[tokio::test]
async fn a_fenced_v4_record_survives_a_reshard() {
    let store = store();
    provision_v3(store.as_ref()).await;
    raise_compat_fence(store.as_ref(), &hash(), Signal::Metrics, 2, T0)
        .await
        .expect("fence");
    let fenced = read_prov(store.as_ref()).await;
    assert_eq!(fenced.format_version, 4);
    assert_eq!(fenced.compat_epoch, 2);
    let fenced_at = fenced.compat_fenced_unix_ns;
    assert_eq!(fenced_at, T0, "first fence stamps the time");

    // Reshard: append one generation with a different count, activation strictly
    // in the future.
    let now_hour = u32::try_from((T0 + HOUR_NS).div_euclid(HOUR_NS)).unwrap();
    ravel_catalog::append_generation(
        store.as_ref(),
        &hash(),
        Signal::Metrics,
        2,
        now_hour + 10,
        T0 + HOUR_NS,
    )
    .await
    .expect("reshard");

    let after = read_prov(store.as_ref()).await;
    assert_eq!(
        after.format_version, 4,
        "reshard keeps version 4: {after:?}"
    );
    assert_eq!(after.compat_epoch, 2, "keeps the epoch: {after:?}");
    assert_eq!(
        after.compat_fenced_unix_ns, fenced_at,
        "keeps the fence time: {after:?}"
    );
}

/// An unfenced (version 3) provisioning record reshards and stays at version 3,
/// adding neither compat field, so the pre-baseline fence never fires on an
/// unfenced bucket (matrix row "unfenced (version 3) provisioning record").
#[tokio::test]
async fn an_unfenced_v3_record_reshards_at_v3() {
    let store = store();
    provision_v3(store.as_ref()).await;
    let before = read_prov(store.as_ref()).await;
    assert_eq!(before.format_version, 3);
    assert_eq!(before.compat_epoch, 0);

    let now_hour = u32::try_from((T0 + HOUR_NS).div_euclid(HOUR_NS)).unwrap();
    ravel_catalog::append_generation(
        store.as_ref(),
        &hash(),
        Signal::Metrics,
        2,
        now_hour + 10,
        T0 + HOUR_NS,
    )
    .await
    .expect("reshard");

    let after = read_prov(store.as_ref()).await;
    assert_eq!(after.format_version, 3, "stays version 3: {after:?}");
    assert_eq!(after.compat_epoch, 0, "no epoch: {after:?}");
    assert_eq!(after.compat_fenced_unix_ns, 0, "no fence time: {after:?}");
}

/// A provisioning record above the rewrite ceiling (version 4) is refused
/// rather than rewritten through an older field set (matrix row tail "a record
/// above 4 is refused").
#[tokio::test]
async fn a_provisioning_record_above_v4_is_refused() {
    let store = store();
    provision_v3(store.as_ref()).await;
    raise_compat_fence(store.as_ref(), &hash(), Signal::Metrics, 1, T0)
        .await
        .expect("fence to v4");
    // Forge a version-5 record in place, reusing the real record's shape.
    let mut rec = read_prov(store.as_ref()).await;
    rec.format_version = 5;
    store
        .put(
            &provisioning_key(&hash(), Signal::Metrics),
            rec.encode_to_vec().into(),
            ravel_object_store::PutOptions::default(),
        )
        .await
        .expect("put forged v5");

    let err = raise_compat_fence(store.as_ref(), &hash(), Signal::Metrics, 2, T0 + HOUR_NS)
        .await
        .expect_err("v5 record is refused");
    assert!(
        matches!(
            err,
            ProvisioningError::RefusingToRewriteNewerRecord { got: 5, .. }
        ),
        "{err:?}"
    );
}

// === certify preconditions (decision 7) ======================================

/// `certify` applies only to the class A families and refuses any other with
/// `NotClassAFamily` (decision 7, "it applies to the class A families only, and
/// refuses any other").
#[tokio::test]
async fn certify_refuses_a_non_class_a_family() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let report = certify(store.as_ref(), &keys(), T0, "csnap", 1, timings(), None)
        .await
        .expect("certify runs");
    assert!(
        matches!(
            report,
            CertifyReport::Refused(CertifyRefusal::NotClassAFamily { .. })
        ),
        "{report:?}"
    );
}

/// `certify` refuses a target above the family's active version with
/// `TargetAboveActive` (decision 7 preconditions, "F's active version is at
/// least N+1").
#[tokio::test]
async fn certify_refuses_a_target_above_active() {
    let store = store();
    write_compat(store.as_ref(), N).await;
    let report = certify(
        store.as_ref(),
        &keys(),
        T0,
        FAMILY_RSEG,
        N_PLUS_1,
        timings(),
        None,
    )
    .await
    .expect("certify runs");
    assert!(
        matches!(
            report,
            CertifyReport::Refused(CertifyRefusal::TargetAboveActive {
                target: 8,
                active: 7
            })
        ),
        "{report:?}"
    );
}

/// `certify` refuses when `sys/compat` is absent with `CompatRecordAbsent`.
#[tokio::test]
async fn certify_refuses_without_a_compat_record() {
    let store = store();
    let report = certify(
        store.as_ref(),
        &keys(),
        T0,
        FAMILY_RSEG,
        N_PLUS_1,
        timings(),
        None,
    )
    .await
    .expect("certify runs");
    assert!(
        matches!(
            report,
            CertifyReport::Refused(CertifyRefusal::CompatRecordAbsent)
        ),
        "{report:?}"
    );
}

fn timings() -> CertifyTimings {
    CertifyTimings {
        t_req_ns: 60 * SECOND_NS,
        max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    }
}
