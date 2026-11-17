//! Bounded retry of the store reads startup refuses on (issue #4325).
//!
//! Every gate that reads the store before a listener binds (qualification,
//! tenancy pinning, tenant key epochs, provisioning validation, `sys/gc`, the
//! IPCrypt policy marker) used to refuse to start on the first store error. A
//! store that is still coming up, or a single dropped connection, then exited
//! the process even though nothing about the deployment was wrong.
//!
//! [`StartupRetry::run`] runs one such read and retries it only while it fails
//! with [`StoreError::Transient`], found by type in the error's source chain,
//! never by its text. The wait between attempts is capped exponential backoff
//! with jitter, and retrying stops at one deadline shared by every site and
//! measured from process start (`--startup-store-deadline`). Every other error
//! refuses on the attempt that returned it, with the message it had before
//! this module existed. Time and sleep come from a [`StartupClock`], so tests
//! drive the deadline without waiting.
//!
//! No listener is bound while these reads run, so `/readyz` cannot report
//! ready during a retry: the main HTTP listener binds only after every site
//! here has passed, and the `--listen-health` listener, which binds earlier,
//! answers 503 until `start` returns.

use std::error::Error as StdError;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_object_store::{KmsRoutingStore, ObjectStoreBackend, StoreError};
use ravel_types::TenantHash;

use crate::config::{Mode, StoreKind};
use crate::tenancy::{ResolvedTenancy, TenancyError};
use crate::tenant_kms::{TenantKmsConfig, TenantKmsError};

/// Default for `--startup-store-deadline`. The Kubernetes operator gives a
/// ravel-server pod no startup probe and a liveness probe of
/// `initialDelaySeconds: 5`, `periodSeconds: 10`, `failureThreshold: 3`, so
/// the earliest restart lands 25 s after the container starts. 15 s stops
/// retrying 10 s before that, which leaves the last attempt and the exit time
/// to finish, so the process reports its own refusal instead of being killed.
pub const DEFAULT_STARTUP_STORE_DEADLINE: Duration = Duration::from_secs(15);

/// First backoff before jitter; doubles per attempt up to [`MAX_BACKOFF`].
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
/// Backoff cap before jitter.
const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// Site names, as they appear in the retry log line and the deadline error.
pub const SITE_QUALIFICATION: &str = "sys/qualification";
pub const SITE_TENANCY: &str = "sys/tenancy";
pub const SITE_TENANT_KMS: &str = "tenant key-epoch records (t/<tenant_hash>/enc)";
pub const SITE_PROVISIONING: &str = "shard_count provisioning records";
pub const SITE_GC_CONFIG: &str = "sys/gc";
pub const SITE_IPCRYPT_MARKER: &str = "sys/ipcrypt/policy";

/// Elapsed time since process start and a sleep, injected so a test drives
/// the deadline without waiting.
#[async_trait::async_trait]
pub trait StartupClock: Send + Sync {
    /// Time since process start.
    fn elapsed(&self) -> Duration;
    /// Wait `duration`.
    async fn sleep(&self, duration: Duration);
}

/// The production clock: tokio's monotonic clock, started when the process
/// constructs it at the top of `main`.
pub struct TokioStartupClock {
    started: tokio::time::Instant,
}

impl TokioStartupClock {
    pub fn starting_now() -> Self {
        TokioStartupClock {
            started: tokio::time::Instant::now(),
        }
    }
}

#[async_trait::async_trait]
impl StartupClock for TokioStartupClock {
    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// How a startup store read failed for good.
#[derive(Debug)]
pub enum StartupReadFailure<E> {
    /// A non-transient error: refused on the attempt that returned it.
    Refused(E),
    /// Every attempt failed transiently and the deadline passed.
    DeadlinePassed {
        site: &'static str,
        attempts: u32,
        deadline: Duration,
        last: E,
    },
}

impl<E> StartupReadFailure<E> {
    /// The refusal `main` reports. `refuse` builds the error the site has
    /// always refused with; a deadline refusal wraps that error for the last
    /// attempt in a context naming the site, the deadline and the attempts.
    pub fn into_anyhow(self, refuse: impl FnOnce(E) -> anyhow::Error) -> anyhow::Error {
        match self {
            StartupReadFailure::Refused(err) => refuse(err),
            StartupReadFailure::DeadlinePassed {
                site,
                attempts,
                deadline,
                last,
            } => refuse(last).context(format!(
                "startup store read of {site} still failed with a transient store error when \
                 the startup deadline of {} (--startup-store-deadline) passed after {attempts} \
                 attempts; refusing to start",
                humantime::format_duration(deadline)
            )),
        }
    }
}

/// The retry policy every startup store read shares: one clock started at
/// process start, one deadline, one jitter stream.
#[derive(Clone)]
pub struct StartupRetry {
    clock: Arc<dyn StartupClock>,
    deadline: Duration,
    rng: Arc<AtomicU64>,
}

impl StartupRetry {
    /// `deadline` is measured on `clock` from its zero, which the production
    /// clock places at process start. A zero `deadline` turns retry off: the
    /// first error of any kind refuses, unchanged. `seed` feeds the jitter.
    pub fn new(clock: Arc<dyn StartupClock>, deadline: Duration, seed: u64) -> Self {
        StartupRetry {
            clock,
            deadline,
            rng: Arc::new(AtomicU64::new(seed)),
        }
    }

    /// No retry: the first error refuses. The policy of every `start` variant
    /// other than the one the binary calls.
    pub fn disabled() -> Self {
        StartupRetry::new(
            Arc::new(TokioStartupClock::starting_now()),
            Duration::ZERO,
            0,
        )
    }

    pub fn deadline(&self) -> Duration {
        self.deadline
    }

    /// Run `op` until it succeeds, fails with an error `is_transient` rejects,
    /// or fails transiently once the deadline has passed. Each retry logs one
    /// warning naming `site`, the attempt number and the error. The sleep
    /// before a retry never runs past the deadline, so the last attempt
    /// starts at the deadline at the latest.
    pub async fn run<T, E, F, Fut>(
        &self,
        site: &'static str,
        mut op: F,
        is_transient: impl Fn(&E) -> bool,
    ) -> Result<T, StartupReadFailure<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: Display,
    {
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            let err = match op().await {
                Ok(value) => {
                    if attempt > 1 {
                        tracing::info!(
                            site,
                            attempts = attempt,
                            "startup store read succeeded after transient failures"
                        );
                    }
                    return Ok(value);
                }
                Err(err) => err,
            };
            if self.deadline.is_zero() || !is_transient(&err) {
                return Err(StartupReadFailure::Refused(err));
            }
            let elapsed = self.clock.elapsed();
            let Some(remaining) = self.deadline.checked_sub(elapsed).filter(|r| !r.is_zero())
            else {
                return Err(StartupReadFailure::DeadlinePassed {
                    site,
                    attempts: attempt,
                    deadline: self.deadline,
                    last: err,
                });
            };
            let delay = self.backoff(attempt).min(remaining);
            tracing::warn!(
                site,
                attempt,
                error = %err,
                retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                deadline_ms = u64::try_from(self.deadline.as_millis()).unwrap_or(u64::MAX),
                "startup store read failed with a transient store error; retrying"
            );
            self.clock.sleep(delay).await;
        }
    }

    /// Equal jitter over a capped exponential: half the step, plus a uniform
    /// draw over the other half, so retries from replicas that failed together
    /// spread out but never collapse to zero.
    fn backoff(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let step = INITIAL_BACKOFF
            .saturating_mul(1u32 << shift)
            .min(MAX_BACKOFF);
        let half = step / 2;
        let span = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        let draw = self.next_random() % span.saturating_add(1);
        half + Duration::from_nanos(draw)
    }

    /// splitmix64 over a shared counter.
    fn next_random(&self) -> u64 {
        let mut z = self
            .rng
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Whether a store error is one startup retries.
pub fn store_error_is_transient(err: &StoreError) -> bool {
    matches!(err, StoreError::Transient(_))
}

/// Whether the first [`StoreError`] in `err`'s source chain, `err` included,
/// is [`StoreError::Transient`]. An error with no store error in its chain is
/// not transient.
pub fn chain_is_transient(err: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(e) = current {
        if let Some(store) = e.downcast_ref::<StoreError>() {
            return store_error_is_transient(store);
        }
        current = e.source();
    }
    false
}

/// `TenantKmsError` holds its `KeyEpochError` in a field that is not a
/// source, so the chain walk starts there.
fn tenant_kms_is_transient(err: &TenantKmsError) -> bool {
    match err {
        TenantKmsError::ReadEpochs { error, .. } | TenantKmsError::RecordEpoch { error, .. } => {
            chain_is_transient(error)
        }
        _ => false,
    }
}

/// The store-backend qualification gate (ADR-0050 section 6), retried.
pub async fn enforce_qualification(
    retry: &StartupRetry,
    store: &dyn ObjectStoreBackend,
    store_kind: StoreKind,
    configured_identity: Option<&str>,
) -> anyhow::Result<()> {
    retry
        .run(
            SITE_QUALIFICATION,
            || crate::qualification::enforce(store, store_kind, configured_identity),
            |err| chain_is_transient(err),
        )
        .await
        .map_err(|failure| {
            failure.into_anyhow(|err| {
                anyhow::Error::new(err).context(
                    "store backend is not qualified (sys/qualification); refusing to start",
                )
            })
        })
}

/// Tenant-hash scheme resolution and marker bootstrap (ADR-0050 section 3),
/// retried. The marker write is create-if-absent and a retry starts from the
/// read, so a write that landed before an ambiguous failure is found by the
/// next attempt and validated like any present marker.
pub async fn resolve_tenancy(
    retry: &StartupRetry,
    store: &dyn ObjectStoreBackend,
    configured: ravel_types::ConfiguredScheme,
    now_ns: i64,
) -> anyhow::Result<ResolvedTenancy> {
    retry
        .run(
            SITE_TENANCY,
            || crate::tenancy::resolve_and_pin(store, configured.clone(), now_ns),
            |err: &TenancyError| chain_is_transient(err),
        )
        .await
        .map_err(|failure| {
            failure.into_anyhow(|err| {
                anyhow::Error::new(err)
                    .context("failed to resolve the bucket's tenant-hash scheme (sys/tenancy)")
            })
        })
}

/// Per-tenant SSE-KMS key registration and key-epoch bootstrap (ADR-0062
/// decision 1b), retried. The epoch writes are compare-and-swap and a retry
/// re-reads the record first: an epoch that landed before an ambiguous
/// failure reads back as the current key (nothing to write) or as an
/// unfinished bootstrap (epoch 1 appended), and registering a key twice
/// overwrites it with the same value.
pub async fn configure_tenant_kms(
    retry: &StartupRetry,
    kms: &KmsRoutingStore,
    store: &dyn ObjectStoreBackend,
    config: &TenantKmsConfig,
    now_ns: i64,
) -> anyhow::Result<()> {
    retry
        .run(
            SITE_TENANT_KMS,
            || crate::tenant_kms::configure_tenant_kms(kms, store, config, now_ns),
            tenant_kms_is_transient,
        )
        .await
        .map_err(|failure| {
            failure.into_anyhow(|err| {
                anyhow::Error::new(err)
                    .context("failed to configure per-tenant SSE-KMS routing (--tenant-kms-config)")
            })
        })
}

/// Durable `shard_count` validation for the statically known tenants
/// (ADR-0050 section 5), retried. An adoption write is create-if-absent and
/// race-safe, and a retry reads the record first, so a record that landed
/// before an ambiguous failure is validated as present.
pub async fn validate_static_provisioning(
    retry: &StartupRetry,
    store: &dyn ObjectStoreBackend,
    static_tenants: &[TenantHash],
    shard_count: u32,
    mode: Mode,
    now_ns: i64,
) -> anyhow::Result<()> {
    retry
        .run(
            SITE_PROVISIONING,
            || {
                crate::provisioning::validate_static_provisioning(
                    store,
                    static_tenants,
                    shard_count,
                    mode,
                    now_ns,
                )
            },
            |err| chain_is_transient(err),
        )
        .await
        .map_err(|failure| {
            failure.into_anyhow(|err| {
                anyhow::Error::new(err).context(
                    "shard_count provisioning validation failed for a statically-known tenant",
                )
            })
        })
}

/// Virtual time and fault plans shared by the startup-site tests here and in
/// `gc_config` and `ipcrypt`.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod test_support {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use ravel_object_store::fault::{FaultPlan, Occurrence, Op, Rule, ScriptedFault, Sequence};

    pub(crate) use super::StartupClock;
    use super::StartupRetry;

    /// Virtual time: `sleep` advances it at once and records the request.
    #[derive(Default)]
    pub(crate) struct ManualClock {
        now: Mutex<Duration>,
        sleeps: Mutex<Vec<Duration>>,
    }

    impl ManualClock {
        pub(crate) fn sleeps(&self) -> Vec<Duration> {
            self.sleeps.lock().expect("sleeps lock").clone()
        }
    }

    #[async_trait::async_trait]
    impl StartupClock for ManualClock {
        fn elapsed(&self) -> Duration {
            *self.now.lock().expect("clock lock")
        }

        async fn sleep(&self, duration: Duration) {
            *self.now.lock().expect("clock lock") += duration;
            self.sleeps.lock().expect("sleeps lock").push(duration);
        }
    }

    pub(crate) const DEADLINE: Duration = Duration::from_secs(15);

    pub(crate) fn retry_on(clock: Arc<ManualClock>) -> StartupRetry {
        StartupRetry::new(clock, DEADLINE, 7)
    }

    /// Fails transiently `failures` times on (`op`, `key`), then passes through.
    pub(crate) fn transient_then_ok(op: Op, key: &str, failures: usize) -> FaultPlan {
        let mut sequence = Sequence::new(op).with_key_contains(key);
        for i in 0..failures {
            sequence = sequence.then_fault(ScriptedFault::Transient(format!("blip {i}")));
        }
        FaultPlan::empty().with_sequence(sequence.then_passthrough())
    }

    /// Fails transiently on every (`op`, `key`) call.
    pub(crate) fn always_transient(op: Op, key: &str) -> FaultPlan {
        FaultPlan::empty().with_rule(
            Rule::new(op, ScriptedFault::Transient("store still down".into()))
                .with_key_contains(key)
                .with_occurrence(Occurrence::Always),
        )
    }

    /// Fails with a permanent error on every (`op`, `key`) call.
    pub(crate) fn always_permanent(op: Op, key: &str) -> FaultPlan {
        FaultPlan::empty().with_rule(
            Rule::new(op, ScriptedFault::Permanent("bad request".into()))
                .with_key_contains(key)
                .with_occurrence(Occurrence::Always),
        )
    }

    /// The text `into_anyhow` gives a deadline refusal, for `site` after
    /// `attempts`.
    pub(crate) fn assert_deadline_refusal(err: &anyhow::Error, site: &str, attempts: u64) {
        let message = err.to_string();
        assert!(attempts > 1, "retried before giving up: {attempts}");
        assert!(
            message.starts_with(&format!("startup store read of {site} ")),
            "{message}"
        );
        assert!(
            message.contains(&format!("passed after {attempts} attempts")),
            "{message}"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_object_store::conformance::{
        CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY, QualificationRecord,
    };
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, PutOptions};
    use ravel_types::{ConfiguredScheme, TenantHashScheme, TenantId};

    use super::test_support::*;
    use super::*;

    async fn seed_qualification(store: &dyn ObjectStoreBackend) {
        let record = QualificationRecord {
            suite_version: CONFORMANCE_SUITE_VERSION,
            backend_identity: "s3://ravel-test".to_string(),
            qualified_unix_ns: 1,
            passed_properties: vec!["conditional_write_create_if_absent".to_string()],
        };
        store
            .put(
                QUALIFICATION_KEY,
                Bytes::from(serde_json::to_vec(&record).expect("encode")),
                PutOptions::default(),
            )
            .await
            .expect("seed record");
    }

    // --- the helper itself ---

    #[tokio::test]
    async fn backoff_doubles_to_the_cap_with_jitter_inside_each_step() {
        let clock = Arc::new(ManualClock::default());
        let retry = retry_on(clock.clone());
        let mut calls = 0u32;
        let result: Result<(), _> = retry
            .run(
                "test",
                || {
                    calls += 1;
                    async { Err(StoreError::Transient("down".into())) }
                },
                store_error_is_transient,
            )
            .await;
        assert!(matches!(
            result,
            Err(StartupReadFailure::DeadlinePassed { .. })
        ));
        let sleeps = clock.sleeps();
        for (i, sleep) in sleeps.iter().enumerate() {
            let step = INITIAL_BACKOFF
                .saturating_mul(1u32 << i.min(16))
                .min(MAX_BACKOFF);
            let last = i + 1 == sleeps.len();
            assert!(
                *sleep <= step && (last || *sleep >= step / 2),
                "sleep {i} = {sleep:?} outside [{:?}, {step:?}]",
                step / 2
            );
        }
        assert_eq!(sleeps.iter().sum::<Duration>(), DEADLINE);
        assert_eq!(calls as usize, sleeps.len() + 1);
    }

    #[tokio::test]
    async fn zero_deadline_refuses_a_transient_error_at_once() {
        let clock = Arc::new(ManualClock::default());
        let retry = StartupRetry::new(clock.clone(), Duration::ZERO, 1);
        let mut calls = 0u32;
        let result: Result<(), _> = retry
            .run(
                "test",
                || {
                    calls += 1;
                    async { Err(StoreError::Transient("down".into())) }
                },
                store_error_is_transient,
            )
            .await;
        assert!(matches!(result, Err(StartupReadFailure::Refused(_))));
        assert_eq!(calls, 1);
        assert!(clock.sleeps().is_empty());
    }

    #[test]
    fn the_deadline_flag_defaults_to_15s_and_accepts_zero() {
        use crate::config::Cli;
        let cli = Cli::parse_validated_from(["ravel-server"]).expect("default config validates");
        assert_eq!(
            cli.startup_checks.startup_store_deadline,
            DEFAULT_STARTUP_STORE_DEADLINE
        );
        assert_eq!(DEFAULT_STARTUP_STORE_DEADLINE, Duration::from_secs(15));
        let cli = Cli::parse_validated_from(["ravel-server", "--startup-store-deadline", "0s"])
            .expect("zero turns retry off");
        assert_eq!(cli.startup_checks.startup_store_deadline, Duration::ZERO);
        let err = Cli::parse_validated_from(["ravel-server", "--startup-store-deadline", "soon"])
            .expect_err("an unparseable duration is rejected");
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn classification_is_by_type_through_sources() {
        let tenancy = TenancyError::Store(StoreError::Transient("x".into()));
        assert!(chain_is_transient(&tenancy));
        assert!(!chain_is_transient(&TenancyError::CorruptMarker));
        let throttled = TenancyError::Store(StoreError::Throttled { retry_after_ms: 1 });
        assert!(!chain_is_transient(&throttled));
        // The text names the variant; only the type counts.
        let text = std::io::Error::other("transient error: x");
        assert!(!chain_is_transient(&text));
        let kms = TenantKmsError::ReadEpochs {
            tenant: "acme".into(),
            error: ravel_catalog::KeyEpochError::Store {
                key: "t/x/enc".into(),
                source: StoreError::Transient("x".into()),
            },
        };
        assert!(tenant_kms_is_transient(&kms));
        assert!(!tenant_kms_is_transient(&TenantKmsError::EmptyTenantId));
    }

    // --- qualification ---

    fn faulted(plan: FaultPlan) -> FaultStore<MemoryStore> {
        FaultStore::new(MemoryStore::new(), plan)
    }

    #[tokio::test]
    async fn qualification_retries_transient_failures_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(Op::Get, QUALIFICATION_KEY, 3));
        seed_qualification(store.inner()).await;
        enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect("starts once the store answers");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 3);
        // Three faults plus the passthrough: four attempts.
        assert_eq!(store.sequence_progress(0), 4);
        assert_eq!(clock.sleeps().len(), 3);
    }

    #[tokio::test]
    async fn qualification_transient_past_the_deadline_refuses_naming_the_site() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_transient(Op::Get, QUALIFICATION_KEY));
        seed_qualification(store.inner()).await;
        let err = enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect_err("refuses once the deadline passes");
        let attempts = store.fault_count(Op::Get, FaultKind::Transient);
        assert_deadline_refusal(&err, SITE_QUALIFICATION, attempts);
        assert_eq!(clock.elapsed(), DEADLINE);
        assert!(format!("{err:#}").contains("store backend is not qualified"));
    }

    #[tokio::test]
    async fn qualification_non_transient_error_refuses_on_the_first_attempt() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_permanent(Op::Get, QUALIFICATION_KEY));
        seed_qualification(store.inner()).await;
        let err = enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect_err("refuses at once");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
        assert_eq!(
            err.to_string(),
            "store backend is not qualified (sys/qualification); refusing to start"
        );
        assert!(clock.sleeps().is_empty());
    }

    #[tokio::test]
    async fn qualification_absent_record_still_refuses_at_once() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(FaultPlan::empty());
        let err = enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect_err("absent is a refusal, not a retry");
        assert!(matches!(
            err.downcast_ref::<crate::qualification::QualificationError>(),
            Some(crate::qualification::QualificationError::Absent)
        ));
        assert!(clock.sleeps().is_empty());
    }

    // --- tenancy ---

    #[tokio::test]
    async fn tenancy_retries_a_transient_marker_read_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(
            Op::Get,
            crate::tenancy::TENANCY_MARKER_KEY,
            2,
        ));
        let resolved = resolve_tenancy(
            &retry_on(clock.clone()),
            &store,
            ConfiguredScheme::Unkeyed,
            1,
        )
        .await
        .expect("starts once the store answers");
        assert!(matches!(resolved.scheme, TenantHashScheme::V1Unkeyed));
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 2);
        assert_eq!(store.sequence_progress(0), 3);
    }

    #[tokio::test]
    async fn tenancy_retries_a_transient_marker_write_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(
            Op::Put,
            crate::tenancy::TENANCY_MARKER_KEY,
            2,
        ));
        resolve_tenancy(
            &retry_on(clock.clone()),
            &store,
            ConfiguredScheme::Unkeyed,
            1,
        )
        .await
        .expect("starts once the write lands");
        assert_eq!(store.fault_count(Op::Put, FaultKind::Transient), 2);
        assert_eq!(store.sequence_progress(0), 3);
        store
            .inner()
            .get(crate::tenancy::TENANCY_MARKER_KEY, GetRange::Full)
            .await
            .expect("the marker landed");
    }

    #[tokio::test]
    async fn tenancy_transient_past_the_deadline_refuses_naming_the_site() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_transient(
            Op::Get,
            crate::tenancy::TENANCY_MARKER_KEY,
        ));
        let err = resolve_tenancy(
            &retry_on(clock.clone()),
            &store,
            ConfiguredScheme::Unkeyed,
            1,
        )
        .await
        .expect_err("refuses once the deadline passes");
        let attempts = store.fault_count(Op::Get, FaultKind::Transient);
        assert_deadline_refusal(&err, SITE_TENANCY, attempts);
    }

    #[tokio::test]
    async fn tenancy_non_transient_error_refuses_on_the_first_attempt() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_permanent(
            Op::Get,
            crate::tenancy::TENANCY_MARKER_KEY,
        ));
        let err = resolve_tenancy(
            &retry_on(clock.clone()),
            &store,
            ConfiguredScheme::Unkeyed,
            1,
        )
        .await
        .expect_err("refuses at once");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
        assert_eq!(
            err.to_string(),
            "failed to resolve the bucket's tenant-hash scheme (sys/tenancy)"
        );
        assert!(clock.sleeps().is_empty());
    }

    // --- provisioning ---

    fn tenants() -> Vec<TenantHash> {
        vec![TenantId::new("acme").hash()]
    }

    #[tokio::test]
    async fn provisioning_retries_a_transient_listing_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(Op::List, "/s/l0/", 3));
        validate_static_provisioning(
            &retry_on(clock.clone()),
            &store,
            &tenants(),
            1,
            Mode::All,
            1,
        )
        .await
        .expect("starts once the listing answers");
        assert_eq!(store.fault_count(Op::List, FaultKind::Transient), 3);
        assert_eq!(store.sequence_progress(0), 4);
    }

    #[tokio::test]
    async fn provisioning_transient_past_the_deadline_refuses_naming_the_site() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_transient(Op::List, "/s/l0/"));
        let err = validate_static_provisioning(
            &retry_on(clock.clone()),
            &store,
            &tenants(),
            1,
            Mode::All,
            1,
        )
        .await
        .expect_err("refuses once the deadline passes");
        let attempts = store.fault_count(Op::List, FaultKind::Transient);
        assert_deadline_refusal(&err, SITE_PROVISIONING, attempts);
    }

    #[tokio::test]
    async fn provisioning_non_transient_error_refuses_on_the_first_attempt() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_permanent(Op::List, "/s/l0/"));
        let err = validate_static_provisioning(
            &retry_on(clock.clone()),
            &store,
            &tenants(),
            1,
            Mode::All,
            1,
        )
        .await
        .expect_err("refuses at once");
        assert_eq!(store.fault_count(Op::List, FaultKind::Permanent), 1);
        assert_eq!(
            err.to_string(),
            "shard_count provisioning validation failed for a statically-known tenant"
        );
        assert!(clock.sleeps().is_empty());
    }

    // --- tenant KMS ---

    fn kms_config() -> TenantKmsConfig {
        crate::tenant_kms::parse_tenant_kms_config(
            "[tenants]\nacme = \"arn:aws:kms:us-east-1:111122223333:key/acme\"\n",
        )
        .expect("valid config")
    }

    fn kms_store() -> KmsRoutingStore {
        KmsRoutingStore::new(
            Arc::new(MemoryStore::new()),
            ravel_object_store::s3::S3Config {
                bucket: "ravel-test".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some("http://localhost:0".to_string()),
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
            ravel_object_store::s3::S3HttpConfig::default(),
            Arc::new(ravel_object_store::StoreMetrics::default()),
        )
    }

    #[tokio::test]
    async fn tenant_kms_retries_a_transient_epoch_read_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(Op::Get, "/enc", 2));
        configure_tenant_kms(
            &retry_on(clock.clone()),
            &kms_store(),
            &store,
            &kms_config(),
            1,
        )
        .await
        .expect("starts once the store answers");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 2);
        assert_eq!(store.sequence_progress(0), 3);
    }

    #[tokio::test]
    async fn tenant_kms_transient_past_the_deadline_refuses_naming_the_site() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_transient(Op::Get, "/enc"));
        let err = configure_tenant_kms(
            &retry_on(clock.clone()),
            &kms_store(),
            &store,
            &kms_config(),
            1,
        )
        .await
        .expect_err("refuses once the deadline passes");
        let attempts = store.fault_count(Op::Get, FaultKind::Transient);
        assert_deadline_refusal(&err, SITE_TENANT_KMS, attempts);
    }

    #[tokio::test]
    async fn tenant_kms_non_transient_error_refuses_on_the_first_attempt() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_permanent(Op::Get, "/enc"));
        let err = configure_tenant_kms(
            &retry_on(clock.clone()),
            &kms_store(),
            &store,
            &kms_config(),
            1,
        )
        .await
        .expect_err("refuses at once");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
        assert_eq!(
            err.to_string(),
            "failed to configure per-tenant SSE-KMS routing (--tenant-kms-config)"
        );
        assert!(clock.sleeps().is_empty());
    }
}
