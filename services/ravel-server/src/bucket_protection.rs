//! Bucket-protection startup gate (ADR-0072 decision 3, ADR-1727 decision 5).
//!
//! `--require-bucket-protection` (default off) reads the bucket's
//! protection report ([`BucketProtectionReport`], one entry per condition of
//! docs/object-store-contract.md's "Required bucket configuration" section)
//! once at startup and turns it into a fail-closed check:
//!
//! - Fatal, refusing to start: `object-lock` failed (Object Lock disabled on
//!   the bucket), `abort-multipart` failed (no enabled
//!   `AbortIncompleteMultipartUpload` rule of 7 days or less covering the
//!   data), `no-foreign-rule` failed (another expiration or transition rule
//!   targets `t/` or `sys/`), and `noncurrent-expiration` failed while
//!   `versioning` passed (versioning on without a noncurrent-version
//!   expiration rule). The server has no expected `E_v`, so it does not check
//!   a covering rule's `NoncurrentDays` against one; the condition still
//!   fails on a covering rule that also sets `NewerNoncurrentVersions`, on
//!   covering rules that disagree on `NoncurrentDays`, and on a rule over part
//!   of `t/` that expires noncurrent versions sooner than they agree on.
//! - Any other failed condition is counted and logged, not fatal.
//! - `delete-marker-replication` and `object-retention` are checked only by
//!   `ravel-cli store verify-protection`. The server has no replication or
//!   retention expectation, never asks for either, and neither counts toward
//!   the gauges.
//! - An unknown condition (no API for the call, an access denial, or a
//!   response the reader cannot parse) never refuses: it logs one warning and
//!   raises the `ravel_bucket_protection_unknown` gauge.
//! - The bucket-configuration read is bounded by [`STARTUP_DEADLINE`]. A read
//!   that has not finished by then makes every checked condition unknown, so
//!   control-plane GETs that stall warn and start instead of holding startup,
//!   together with the read-cache warm-up's own bound, past the operator's
//!   earliest liveness restart. The bound covers this read
//!   only: the `sys/qualification` read that runs before it goes through the
//!   retrying store path and is bounded only by the store's own request
//!   timeout and retries, so an endpoint that stalls every request holds
//!   startup there first.
//!
//! Each check sets three gauges: `ravel_bucket_protection_conditions_failed`
//! and `ravel_bucket_protection_conditions_unknown` count the checked
//! conditions observed failed and unknown, and
//! `ravel_bucket_protection_unknown` is 1 whenever the unknown count is
//! nonzero. All three stay 0 when the flag is off.
//!
//! # Which store the gate reads
//!
//! Only `S3Store` answers affirmatively. The `BucketControlPlane` impl on the
//! `dyn ObjectStoreBackend` trait object reports every condition unknown, so
//! [`enforce_at_startup_on`], the function `main.rs` calls, takes the concrete
//! base `S3Store` that `store::build_store` returns beside the wrapped handles
//! when the backend is S3, and falls back to the trait object for every other
//! backend. [`enforce`] is generic over the source so tests can also drive it
//! with fixture reports.
//!
//! # Enforcement stays at the bucket/IAM layer
//!
//! Every request the report issues is a read-only GET; nothing here
//! configures Object Lock or lifecycle policy. This gate only makes a
//! silently-unprotected production deployment impossible to start.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::conformance::{
    BucketControlPlane, BucketProtectionParams, BucketProtectionReport, ConditionState,
    ProtectionConditionId, probe_bucket_protection,
};
use ravel_object_store::s3::S3Store;

/// Bound on the whole startup read of the protection report. Its GETs run
/// before any listener binds, one after another, each bounded only by the
/// store's request timeout (20 s by default). The operator's liveness probe
/// restarts a pod on its third consecutive failure, which lands between about
/// 25 s and 35 s after the pod starts depending on the probe's tick phase.
/// The read-cache warm-up also runs before the main HTTP listener binds and
/// is bounded by its own 10 s, so 10 s here leaves at least 5 s of the
/// earliest restart for the rest of startup.
pub const STARTUP_DEADLINE: Duration = Duration::from_secs(10);

/// Count of checked conditions the last [`enforce`] call observed failed.
/// Process-global, matching the other single-source, no-label `/metrics`
/// values this crate renders directly in `metrics::render`.
static CONDITIONS_FAILED: AtomicU64 = AtomicU64::new(0);

/// Count of checked conditions the last [`enforce`] call observed unknown.
static CONDITIONS_UNKNOWN: AtomicU64 = AtomicU64::new(0);

/// The three bucket-protection gauges as `/metrics` renders them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BucketProtectionGauges {
    /// `ravel_bucket_protection_unknown`: 1 when `conditions_unknown` is
    /// nonzero, else 0.
    pub unknown: u64,
    /// `ravel_bucket_protection_conditions_failed`.
    pub conditions_failed: u64,
    /// `ravel_bucket_protection_conditions_unknown`.
    pub conditions_unknown: u64,
}

/// The gauge values from the last [`enforce`] call, all 0 when the flag is off
/// and `enforce` never ran.
pub fn bucket_protection_gauges() -> BucketProtectionGauges {
    let conditions_unknown = CONDITIONS_UNKNOWN.load(Ordering::Relaxed);
    BucketProtectionGauges {
        unknown: u64::from(conditions_unknown > 0),
        conditions_failed: CONDITIONS_FAILED.load(Ordering::Relaxed),
        conditions_unknown,
    }
}

/// The `ravel_bucket_protection_unknown` gauge source: 1 if the last
/// [`enforce`] call observed any checked condition unknown, 0 otherwise,
/// including when the flag is off and `enforce` never ran.
pub fn bucket_protection_unknown() -> u64 {
    bucket_protection_gauges().unknown
}

fn set_gauges(outcome: BucketProtectionOutcome) {
    CONDITIONS_FAILED.store(outcome.conditions_failed, Ordering::Relaxed);
    CONDITIONS_UNKNOWN.store(outcome.conditions_unknown, Ordering::Relaxed);
}

/// Whether the startup check evaluates `id` at all. The server has no
/// replication or retention expectation, so the report leaves those two
/// conditions unknown by construction; counting them would hold the unknown
/// gauge at 1 on every compliant bucket.
fn checked_in_process(id: ProtectionConditionId) -> bool {
    match id {
        ProtectionConditionId::Versioning
        | ProtectionConditionId::NoncurrentExpiration
        | ProtectionConditionId::ExpiredDeleteMarker
        | ProtectionConditionId::AbortMultipart
        | ProtectionConditionId::RuleScope
        | ProtectionConditionId::NoForeignRule
        | ProtectionConditionId::ObjectLock => true,
        ProtectionConditionId::DeleteMarkerReplication | ProtectionConditionId::ObjectRetention => {
            false
        }
    }
}

/// Whether a failed `id` refuses to start. `versioning_on` is whether the
/// `versioning` condition passed: noncurrent-version expiration is required
/// only on a versioned bucket.
fn fatal_when_failed(id: ProtectionConditionId, versioning_on: bool) -> bool {
    match id {
        ProtectionConditionId::NoncurrentExpiration => versioning_on,
        ProtectionConditionId::AbortMultipart
        | ProtectionConditionId::NoForeignRule
        | ProtectionConditionId::ObjectLock => true,
        ProtectionConditionId::Versioning
        | ProtectionConditionId::ExpiredDeleteMarker
        | ProtectionConditionId::RuleScope
        | ProtectionConditionId::DeleteMarkerReplication
        | ProtectionConditionId::ObjectRetention => false,
    }
}

/// One condition whose failure refuses to start, with the report's detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FatalCondition {
    pub id: ProtectionConditionId,
    pub detail: String,
}

fn list_fatal(fatal: &[FatalCondition]) -> String {
    fatal
        .iter()
        .map(|condition| format!("{}: {}", condition.id.id(), condition.detail))
        .collect::<Vec<_>>()
        .join("; ")
}

/// A typed bucket-protection-gate failure. Unknown conditions are
/// deliberately not a cause, since unknown is a warn-and-continue outcome.
#[derive(Debug, thiserror::Error)]
pub enum BucketProtectionError {
    #[error(
        "the bucket's protection configuration violates docs/object-store-contract.md's \
         \"Required bucket configuration\" section ({}). Configure the bucket, then restart, or \
         run without --require-bucket-protection for a non-production deployment.",
        list_fatal(.fatal)
    )]
    Refused {
        /// Every fatal condition observed failed, in report order.
        fatal: Vec<FatalCondition>,
        /// The gauge values the refusing check set.
        outcome: BucketProtectionOutcome,
    },
}

/// The counts a non-refusing [`enforce`] call observed, returned so a caller
/// (and a test) can read them directly rather than solely through the
/// process-global gauges, which race under parallel test execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BucketProtectionOutcome {
    /// Checked conditions observed failed, none of them fatal.
    pub conditions_failed: u64,
    /// Checked conditions observed unknown.
    pub conditions_unknown: u64,
}

impl BucketProtectionOutcome {
    /// Whether any checked condition was unknown, which is when a zero
    /// `conditions_failed` is not evidence that the bucket passes the
    /// conditions the server checks.
    pub fn is_unknown(&self) -> bool {
        self.conditions_unknown > 0
    }
}

fn ids_where(report: &BucketProtectionReport, pick: fn(&ConditionState) -> bool) -> String {
    report
        .conditions
        .iter()
        .filter(|entry| checked_in_process(entry.id) && pick(&entry.state))
        .map(|entry| format!("{} ({})", entry.id.id(), entry.state.detail()))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Enforce the bucket-protection gate against `source`, within
/// [`STARTUP_DEADLINE`].
///
/// Always sets the gauges as a side effect, on a refusal too, so `/metrics`
/// reflects the latest call. Read-only: the report is built from GETs alone
/// (three for `S3Store` under the server's parameters: `?versioning`,
/// `?lifecycle`, `?object-lock`; none for a source that cannot answer), so
/// this is safe to run before any listener binds.
pub async fn enforce<S>(source: &S) -> Result<BucketProtectionOutcome, BucketProtectionError>
where
    S: BucketControlPlane + ?Sized,
{
    enforce_within(source, STARTUP_DEADLINE).await
}

/// Every condition unknown, for a report read that missed its deadline. The
/// report is assembled only once all of its GETs have answered, so no
/// condition has been read when the deadline fires.
fn timed_out_report(deadline: Duration) -> BucketProtectionReport {
    let detail = format!(
        "the bucket-protection read did not finish within {} ms",
        deadline.as_millis()
    );
    BucketProtectionReport::from_states(
        ProtectionConditionId::ALL
            .iter()
            .map(|id| (*id, ConditionState::Unknown(detail.clone()))),
    )
}

/// [`enforce`] with an explicit bound on the report read. A read that has not
/// finished within `deadline` is abandoned and every checked condition counts
/// as unknown, which warns and starts.
pub async fn enforce_within<S>(
    source: &S,
    deadline: Duration,
) -> Result<BucketProtectionOutcome, BucketProtectionError>
where
    S: BucketControlPlane + ?Sized,
{
    let params = BucketProtectionParams::default();
    let read = probe_bucket_protection(source, &params);
    let report = match tokio::time::timeout(deadline, read).await {
        Ok(report) => report,
        Err(_) => timed_out_report(deadline),
    };
    let checked = || {
        report
            .conditions
            .iter()
            .filter(|entry| checked_in_process(entry.id))
    };
    let count = |pick: fn(&ConditionState) -> bool| checked().filter(|e| pick(&e.state)).count();
    let outcome = BucketProtectionOutcome {
        conditions_failed: count(ConditionState::is_fail) as u64,
        conditions_unknown: count(ConditionState::is_unknown) as u64,
    };
    set_gauges(outcome);

    let versioning_on = report
        .state(ProtectionConditionId::Versioning)
        .is_some_and(ConditionState::is_pass);
    let fatal: Vec<FatalCondition> = checked()
        .filter_map(|entry| match &entry.state {
            ConditionState::Fail(detail) if fatal_when_failed(entry.id, versioning_on) => {
                Some(FatalCondition {
                    id: entry.id,
                    detail: detail.clone(),
                })
            }
            _ => None,
        })
        .collect();
    if !fatal.is_empty() {
        return Err(BucketProtectionError::Refused { fatal, outcome });
    }

    if outcome.conditions_failed > 0 {
        tracing::warn!(
            failed = %ids_where(&report, ConditionState::is_fail),
            "--require-bucket-protection: the bucket fails bucket-protection conditions that do \
             not refuse startup; fix them at the bucket layer. Starting anyway \
             (ravel_bucket_protection_conditions_failed={}).",
            outcome.conditions_failed
        );
    }
    if outcome.is_unknown() {
        tracing::warn!(
            unknown = %ids_where(&report, ConditionState::is_unknown),
            "--require-bucket-protection: the platform cannot confirm these bucket-protection \
             conditions for this backend. Confirm them out of band. Starting anyway \
             (ravel_bucket_protection_unknown=1, ravel_bucket_protection_conditions_unknown={}).",
            outcome.conditions_unknown
        );
    }
    Ok(outcome)
}

/// Run [`enforce`] only when `required` (the `--require-bucket-protection`
/// flag) is set, returning `None` when the flag is off so the caller can see
/// the gate was skipped rather than confusing that with a clean result. With
/// `required` false this sends no request and never sets the gauges.
pub async fn enforce_if_required<S>(
    required: bool,
    source: &S,
) -> Result<Option<BucketProtectionOutcome>, BucketProtectionError>
where
    S: BucketControlPlane + ?Sized,
{
    if !required {
        return Ok(None);
    }
    enforce(source).await.map(Some)
}

/// [`enforce_if_required`] wrapped in the `.context(...)` the binary reports a
/// refusal with.
pub async fn enforce_at_startup<S>(
    required: bool,
    source: &S,
) -> anyhow::Result<Option<BucketProtectionOutcome>>
where
    S: BucketControlPlane + ?Sized,
{
    enforce_if_required(required, source)
        .await
        .context("bucket-protection contract check failed; refusing to start")
}

/// The call `main.rs` makes with what `store::build_store` returned: the
/// concrete base `S3Store` when the backend is S3 (`s3`), else the wrapped
/// `store`, which reports every condition unknown.
pub async fn enforce_at_startup_on(
    required: bool,
    s3: Option<&S3Store>,
    store: &dyn ObjectStoreBackend,
) -> anyhow::Result<Option<BucketProtectionOutcome>> {
    match s3 {
        Some(s3) => enforce_at_startup(required, s3).await,
        None => enforce_at_startup(required, store).await,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::LazyLock;

    use tokio::sync::Mutex;

    use super::*;

    /// Serializes every test that calls [`enforce`] or reads the gauges: they
    /// are process-global and `cargo test` runs tests on parallel threads.
    /// A `tokio::sync::Mutex` because the guard is held across an `.await`.
    static GAUGE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn reset_gauges() {
        set_gauges(BucketProtectionOutcome::default());
    }

    /// A source that serves a fixed report, bypassing `ObjectStoreBackend`.
    struct Fixture(BucketProtectionReport);

    #[async_trait::async_trait]
    impl BucketControlPlane for Fixture {
        async fn bucket_protection_report(
            &self,
            _params: &BucketProtectionParams,
        ) -> BucketProtectionReport {
            self.0.clone()
        }
    }

    /// Every condition passing, then each `overrides` entry applied.
    fn fixture(overrides: &[(ProtectionConditionId, ConditionState)]) -> Fixture {
        Fixture(BucketProtectionReport::from_states(
            ProtectionConditionId::ALL
                .iter()
                .map(|id| (*id, ConditionState::Pass))
                .chain(overrides.iter().cloned()),
        ))
    }

    fn fail(detail: &str) -> ConditionState {
        ConditionState::Fail(detail.to_string())
    }

    fn unknown(detail: &str) -> ConditionState {
        ConditionState::Unknown(detail.to_string())
    }

    fn gauges(
        unknown: u64,
        conditions_failed: u64,
        conditions_unknown: u64,
    ) -> BucketProtectionGauges {
        BucketProtectionGauges {
            unknown,
            conditions_failed,
            conditions_unknown,
        }
    }

    /// Asserts `source` refuses naming exactly `expected`, and pins the gauges.
    async fn assert_refuses(
        source: &Fixture,
        expected: ProtectionConditionId,
        expected_gauges: BucketProtectionGauges,
    ) {
        let err = enforce(source)
            .await
            .expect_err("a fatal condition must refuse to start");
        let BucketProtectionError::Refused { fatal, .. } = &err;
        let ids: Vec<ProtectionConditionId> = fatal.iter().map(|c| c.id).collect();
        assert_eq!(ids, vec![expected], "{err}");
        assert!(err.to_string().contains(expected.id()), "{err}");
        assert_eq!(bucket_protection_gauges(), expected_gauges);
    }

    /// Flipped line: the `ProtectionConditionId::ObjectLock` arm of
    /// `fatal_when_failed`.
    #[tokio::test]
    async fn refuses_when_object_lock_disabled() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[(ProtectionConditionId::ObjectLock, fail("lock off"))]);
        assert_refuses(&source, ProtectionConditionId::ObjectLock, gauges(0, 1, 0)).await;
    }

    /// Flipped line: the `NoncurrentExpiration => versioning_on` arm of
    /// `fatal_when_failed`.
    #[tokio::test]
    async fn refuses_on_versioning_without_noncurrent_expiration() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[(
            ProtectionConditionId::NoncurrentExpiration,
            fail("no noncurrent rule"),
        )]);
        assert_refuses(
            &source,
            ProtectionConditionId::NoncurrentExpiration,
            gauges(0, 1, 0),
        )
        .await;
    }

    /// Flipped line: the `ProtectionConditionId::AbortMultipart` arm of
    /// `fatal_when_failed`.
    #[tokio::test]
    async fn refuses_when_abort_multipart_absent() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[(ProtectionConditionId::AbortMultipart, fail("no abort rule"))]);
        assert_refuses(
            &source,
            ProtectionConditionId::AbortMultipart,
            gauges(0, 1, 0),
        )
        .await;
    }

    /// Flipped line: the `ProtectionConditionId::NoForeignRule` arm of
    /// `fatal_when_failed`.
    #[tokio::test]
    async fn refuses_on_a_foreign_rule() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[(
            ProtectionConditionId::NoForeignRule,
            fail("rule expire-all expires t/"),
        )]);
        assert_refuses(
            &source,
            ProtectionConditionId::NoForeignRule,
            gauges(0, 1, 0),
        )
        .await;
    }

    /// A refusal still counts the unknown conditions it saw, so the gauges of
    /// a refusing check are the whole observation, not just the fatal part.
    #[tokio::test]
    async fn refusal_sets_both_counts() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[
            (ProtectionConditionId::ObjectLock, fail("lock off")),
            (
                ProtectionConditionId::RuleScope,
                unknown("sixteen-rule union incomplete"),
            ),
        ]);
        assert_refuses(&source, ProtectionConditionId::ObjectLock, gauges(1, 1, 1)).await;
    }

    /// Flipped line: `NoncurrentExpiration => versioning_on`. With versioning
    /// off there are no noncurrent versions to expire, so a missing rule is
    /// counted and does not refuse; making the arm `true` refuses here.
    #[tokio::test]
    async fn missing_noncurrent_expiration_on_an_unversioned_bucket_does_not_refuse() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[
            (
                ProtectionConditionId::Versioning,
                fail("versioning suspended"),
            ),
            (
                ProtectionConditionId::NoncurrentExpiration,
                fail("no noncurrent rule"),
            ),
        ]);
        let outcome = enforce(&source)
            .await
            .expect("not fatal without versioning");
        assert_eq!(
            outcome,
            BucketProtectionOutcome {
                conditions_failed: 2,
                conditions_unknown: 0,
            }
        );
        assert_eq!(bucket_protection_gauges(), gauges(0, 2, 0));
    }

    /// Flipped line: the `ExpiredDeleteMarker` entry in the `false` arm of
    /// `fatal_when_failed`. A failed condition outside the fatal set starts,
    /// and is counted.
    #[tokio::test]
    async fn non_fatal_fail_starts_and_is_counted() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[(
            ProtectionConditionId::ExpiredDeleteMarker,
            fail("no delete-marker cleanup"),
        )]);
        let outcome = enforce(&source).await.expect("not a fatal condition");
        assert_eq!(
            outcome,
            BucketProtectionOutcome {
                conditions_failed: 1,
                conditions_unknown: 0,
            }
        );
        assert_eq!(bucket_protection_gauges(), gauges(0, 1, 0));
    }

    /// Flipped line: the `false` arm of `checked_in_process`. The server never
    /// asks for replication or retention, so even a report that fails them is
    /// neither fatal nor counted.
    #[tokio::test]
    async fn cli_only_conditions_never_refuse_and_are_not_counted() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[
            (
                ProtectionConditionId::DeleteMarkerReplication,
                fail("delete markers not replicated"),
            ),
            (ProtectionConditionId::ObjectRetention, fail("no retention")),
        ]);
        let outcome = enforce(&source)
            .await
            .expect("CLI-only conditions never refuse");
        assert_eq!(outcome, BucketProtectionOutcome::default());
        assert_eq!(bucket_protection_gauges(), gauges(0, 0, 0));
    }

    /// Flipped line: `conditions_unknown: count(ConditionState::is_unknown)`
    /// in `enforce`. Unknown warns and sets all three gauges.
    #[tokio::test]
    async fn unknown_warns_and_sets_the_gauges() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let source = fixture(&[
            (ProtectionConditionId::ObjectLock, unknown("access denied")),
            (ProtectionConditionId::RuleScope, unknown("narrower union")),
            (
                ProtectionConditionId::DeleteMarkerReplication,
                unknown("not expected"),
            ),
        ]);
        let outcome = enforce(&source).await.expect("unknown must not refuse");
        assert_eq!(
            outcome,
            BucketProtectionOutcome {
                conditions_failed: 0,
                conditions_unknown: 2,
            }
        );
        assert!(outcome.is_unknown());
        assert_eq!(bucket_protection_gauges(), gauges(1, 0, 2));
    }

    /// A store held only as `dyn ObjectStoreBackend` answers every condition
    /// unknown: all seven checked conditions count.
    #[tokio::test]
    async fn the_trait_object_reports_every_checked_condition_unknown() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let store: std::sync::Arc<dyn ObjectStoreBackend> =
            std::sync::Arc::new(ravel_object_store::memory::MemoryStore::new());
        let outcome = enforce(store.as_ref())
            .await
            .expect("unknown must not refuse");
        assert_eq!(
            outcome,
            BucketProtectionOutcome {
                conditions_failed: 0,
                conditions_unknown: 7,
            }
        );
        assert_eq!(bucket_protection_gauges(), gauges(1, 0, 7));
    }

    #[tokio::test]
    async fn clean_when_compliant() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        set_gauges(BucketProtectionOutcome {
            conditions_failed: 3,
            conditions_unknown: 3,
        });
        let outcome = enforce(&fixture(&[]))
            .await
            .expect("a compliant bucket starts");
        assert_eq!(outcome, BucketProtectionOutcome::default());
        assert_eq!(bucket_protection_gauges(), gauges(0, 0, 0));
    }

    /// Flipped line: `enforce_if_required`'s `if !required { return Ok(None) }`.
    /// Without it the fatal fixture refuses and the gauges move.
    #[tokio::test]
    async fn default_off_does_not_gate_or_set_the_gauges() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        reset_gauges();
        let source = fixture(&[
            (ProtectionConditionId::ObjectLock, fail("lock off")),
            (ProtectionConditionId::RuleScope, unknown("narrower union")),
        ]);
        let outcome = enforce_if_required(false, &source)
            .await
            .expect("the flag being off must never refuse to start");
        assert_eq!(outcome, None);
        assert_eq!(bucket_protection_gauges(), gauges(0, 0, 0));
    }

    /// Bodies a versioned, locked bucket with one compliant lifecycle rule
    /// over the whole bucket answers with.
    fn fake_bucket_body(subresource: &str) -> &'static str {
        match subresource {
            "versioning" => {
                "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
            }
            "lifecycle" => {
                "<LifecycleConfiguration><Rule><ID>ravel</ID><Status>Enabled</Status><Filter/>\
                 <NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays>\
                 </NoncurrentVersionExpiration>\
                 <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>\
                 <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation>\
                 </AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"
            }
            "object-lock" => {
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                 </ObjectLockConfiguration>"
            }
            other => panic!("unexpected subresource {other:?}"),
        }
    }

    /// An `S3Store` on the fake endpoint at `addr`, counting into `metrics`,
    /// with the default request timeout.
    fn s3_store_at(
        addr: std::net::SocketAddr,
        metrics: std::sync::Arc<ravel_object_store::StoreMetrics>,
    ) -> S3Store {
        S3Store::with_metrics(
            ravel_object_store::s3::S3Config {
                bucket: "ravel-test".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some(format!("http://{addr}")),
                access_key_id: "test".to_string(),
                secret_access_key: "test".to_string(),
                allow_http: true,
                force_path_style: true,
                kms_key_id: None,
                session_token: None,
                credentials_file: None,
                auth: Default::default(),
                instance_metadata_endpoint: None,
            },
            metrics,
        )
        .expect("store")
    }

    /// Flipped line: `enforce_within`'s `tokio::time::timeout(deadline, read)`.
    /// The endpoint accepts every connection and never answers, so each GET
    /// would otherwise wait out the store's 20 s request timeout, three in a
    /// row. Under the deadline the check gives up after the first GET, counts
    /// all seven checked conditions unknown, and starts.
    #[tokio::test]
    async fn a_stalled_endpoint_starts_within_the_deadline_with_every_condition_unknown() {
        use std::sync::Arc;

        use ravel_object_store::StoreMetrics;
        use ravel_object_store::instrument::ControlPlaneMetricsSnapshot;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let metrics: Arc<StoreMetrics> = Arc::default();
        let store = s3_store_at(addr, Arc::clone(&metrics));

        let _guard = GAUGE_TEST_LOCK.lock().await;
        let deadline = Duration::from_millis(500);
        let outcome = enforce_within(&store, deadline)
            .await
            .expect("a stalled read must warn and start, not refuse");
        assert_eq!(
            outcome,
            BucketProtectionOutcome {
                conditions_failed: 0,
                conditions_unknown: 7,
            }
        );
        assert_eq!(bucket_protection_gauges(), gauges(1, 0, 7));
        assert_eq!(
            metrics.control_plane(),
            ControlPlaneMetricsSnapshot {
                requests: 1,
                calls: 0,
                response_bytes: 0,
            }
        );
    }

    #[test]
    fn the_startup_deadline_leaves_room_before_the_earliest_liveness_restart() {
        // Mirrors the liveness `Probe` that `probes_on` builds in
        // services/ravel-operator/src/reconcile.rs, whose values are literals
        // there: `initial_delay_seconds: Some(5)`, `period_seconds: Some(10)`,
        // `failure_threshold: Some(3)`. The first probe runs at the initial
        // delay or up to one period later, and the kubelet restarts on the
        // third consecutive failure, so the earliest restart is the initial
        // delay plus two periods.
        let initial_delay_seconds = 5;
        let period_seconds = 10;
        let failure_threshold = 3;
        let earliest_restart =
            Duration::from_secs(initial_delay_seconds + (failure_threshold - 1) * period_seconds);
        assert_eq!(earliest_restart, Duration::from_secs(25));
        assert_eq!(STARTUP_DEADLINE, Duration::from_secs(10));
        // The read-cache warm-up also runs before the main HTTP listener
        // binds, which the liveness probe targets unless
        // `dedicated_health_port` is set, and the read cache is on by default.
        let bounded = STARTUP_DEADLINE + crate::cache_warm::WARM_DEADLINE;
        assert_eq!(bounded, Duration::from_secs(20));
        assert!(bounded < earliest_restart);
        // The warm-up is counted at its full bound above. The remainder
        // covers the rest of startup up to the main HTTP listener bind: the
        // qualification read on a store that answers, the tenancy and GC
        // reads, the warm-up's return after its deadline cancels it, and the
        // bind itself.
        assert!(earliest_restart - bounded >= Duration::from_secs(5));
    }

    /// Flipped line: `enforce`'s single `probe_bucket_protection` call. One
    /// startup check is one report, three GETs (versioning, lifecycle,
    /// object-lock), and a compliant bucket leaves every gauge at 0.
    #[tokio::test]
    async fn one_startup_check_reads_the_bucket_once() {
        use std::sync::Arc;

        use ravel_object_store::StoreMetrics;
        use ravel_object_store::instrument::ControlPlaneMetricsSnapshot;

        let served = Arc::new(AtomicU64::new(0));
        let served_bytes = Arc::new(AtomicU64::new(0));
        let (requests, bytes) = (Arc::clone(&served), Arc::clone(&served_bytes));
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
            let (requests, bytes) = (Arc::clone(&requests), Arc::clone(&bytes));
            async move {
                let query = uri.query().unwrap_or("");
                let subresource = query.split(['=', '&']).next().unwrap_or("");
                let body = fake_bucket_body(subresource);
                requests.fetch_add(1, Ordering::Relaxed);
                bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                body
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let metrics: Arc<StoreMetrics> = Arc::default();
        let store = s3_store_at(addr, Arc::clone(&metrics));

        let _guard = GAUGE_TEST_LOCK.lock().await;
        let outcome = enforce(&store)
            .await
            .expect("a compliant bucket must start cleanly");
        assert_eq!(outcome, BucketProtectionOutcome::default());
        assert_eq!(bucket_protection_gauges(), gauges(0, 0, 0));
        assert_eq!(served.load(Ordering::Relaxed), 3);
        assert_eq!(
            metrics.control_plane(),
            ControlPlaneMetricsSnapshot {
                requests: 3,
                calls: 3,
                response_bytes: served_bytes.load(Ordering::Relaxed),
            }
        );
        assert_eq!(
            metrics.snapshot(),
            ravel_object_store::StoreMetricsSnapshot::default()
        );
    }
}
