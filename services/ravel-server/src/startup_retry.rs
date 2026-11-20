//! Bounded retry of the store reads startup refuses on (issue #4325).
//!
//! Every gate that reads the store before a listener binds (qualification,
//! the retired-reader gate, tenancy pinning, tenant key epochs, provisioning
//! validation, `sys/gc`, the IPCrypt policy marker) used to refuse to start on
//! the first store error, even one the store contract marks retryable.
//!
//! [`StartupRetry::run`] runs one such read and retries it only while it fails
//! with a [`StoreError`] found by type in the error's source chain, never by
//! its text, that is either transient ([`StoreError::is_retryable`] accepts it:
//! `Transient`, `Timeout` or `Throttled`) or a [`StoreError::ClockSkewed`]
//! refusal. Runtime callers never retry a clock-skew refusal; startup does,
//! because a process that has just started (after a VM resume or an NTP step,
//! say) can sign with a clock the store has not caught up with yet, and a
//! restart only repeats that.
//! The wait between attempts is capped exponential backoff with jitter, and
//! one deadline shared by every site and measured from process start
//! (`--startup-store-deadline`) bounds the retries and each retry: a retry
//! runs for at most the time left before the deadline or
//! [`MIN_STARTUP_ATTEMPT`], whichever is longer, and is cut off if it is still
//! running then. The first attempt at a site is not bounded here, so a slow
//! but healthy store is not refused; the store's own request timeout and
//! retries end it. Every other error refuses on the attempt that returned it,
//! with the message it had before this module existed. Time, sleep and the
//! retry bound come from a [`StartupClock`], so tests drive the deadline
//! without waiting.
//!
//! `/readyz` cannot report ready during a retry: the main HTTP listener binds
//! only after every site here has passed, and the `--listen-health` listener,
//! which binds after [`run_startup_store_checks`] returns, answers 503 until
//! `start` returns.

use std::error::Error as StdError;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_maintain::GcConfigValues;
use ravel_object_store::s3::S3Store;
use ravel_object_store::{KmsRoutingStore, ObjectStoreBackend, StoreError};
use ravel_types::{ConfiguredScheme, TenantHash};

use crate::compat_gate::{CompatGateError, GateOutcome, RetiredReader};
use crate::config::{Mode, StoreKind};
use crate::tenancy::{ResolvedTenancy, TenancyError};
use crate::tenant_kms::{TenantKmsConfig, TenantKmsError};

/// Default for `--startup-store-deadline`. The Kubernetes operator gives a
/// ravel-server pod no startup probe and a liveness probe of
/// `initialDelaySeconds: 5`, `periodSeconds: 10`, `failureThreshold: 3`, so
/// the earliest restart lands 25 s after the container starts. A read whose
/// first attempt fails transiently before the deadline ends by the deadline
/// plus [`MIN_STARTUP_ATTEMPT`], 20 s, 5 s before that restart. A first
/// attempt is not bounded here, and the other pre-listener phases add their
/// own bounds, so the startup as a whole can outlast that restart:
/// docs/guides/operations/deployment.md says what to set.
pub const DEFAULT_STARTUP_STORE_DEADLINE: Duration = Duration::from_secs(15);

/// The shortest bound any retry gets. A retry runs for the time left before
/// the deadline or this, whichever is longer, so a healthy retry that starts
/// just before the deadline is not cut off.
pub const MIN_STARTUP_ATTEMPT: Duration = Duration::from_secs(5);

/// First backoff before jitter; doubles per attempt up to [`MAX_BACKOFF`].
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
/// Backoff cap before jitter.
const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// Site names, as they appear in the retry log line and the deadline error.
pub const SITE_QUALIFICATION: &str = "sys/qualification";
pub const SITE_RETIRED_READER_GATE: &str = "the retired-reader gate (sys/compat, sys/tenancy)";
pub const SITE_TENANCY: &str = "sys/tenancy";
pub const SITE_TENANT_KMS: &str = "tenant key-epoch records (t/<tenant_hash>/enc)";
pub const SITE_PROVISIONING: &str = "shard_count provisioning records";
pub const SITE_GC_CONFIG: &str = "sys/gc";
pub const SITE_IPCRYPT_MARKER: &str = "sys/ipcrypt/policy";

/// Elapsed time since process start, the wait between attempts and the bound
/// on one retry, injected so a test drives the deadline without waiting.
#[async_trait::async_trait]
pub trait StartupClock: Send + Sync {
    /// Time since process start.
    fn elapsed(&self) -> Duration;
    /// The wait between two attempts.
    async fn sleep(&self, duration: Duration);
    /// Completes once `duration` has passed; a retry still running then is
    /// cut off.
    async fn attempt_timer(&self, duration: Duration);
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

    async fn attempt_timer(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// How a startup store read failed for good.
#[derive(Debug)]
pub enum StartupReadFailure<E> {
    /// A non-transient error: refused on the attempt that returned it.
    Refused(E),
    /// Every attempt failed with an error startup retries, and the deadline
    /// has passed or the next wait would reach it. `cause` is why `last` was
    /// retried.
    DeadlinePassed {
        site: &'static str,
        attempts: u32,
        deadline: Duration,
        cause: RetryCause,
        last: E,
    },
    /// Retry `attempts` was still running when its bound, `bound`, ran out.
    /// `last` is the retried error of the attempt before it, and `cause` why
    /// it was retried.
    CutOff {
        site: &'static str,
        attempts: u32,
        deadline: Duration,
        bound: Duration,
        cause: RetryCause,
        last: E,
    },
}

/// Why startup retries a failed store read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryCause {
    /// A store error [`StoreError::is_retryable`] accepts.
    Transient,
    /// A [`StoreError::ClockSkewed`] refusal: the store rejected the request's
    /// signing time.
    ClockSkew,
}

impl RetryCause {
    /// The failure as the refusal message names it.
    fn failure_text(self) -> &'static str {
        match self {
            RetryCause::Transient => "a transient store error",
            RetryCause::ClockSkew => {
                "a clock-skew refusal (the store rejected the request clock, S3 \
                 RequestTimeTooSkewed; check time sync between this host and the store)"
            }
        }
    }
}

fn attempts_text(attempts: u32) -> String {
    if attempts == 1 {
        "1 attempt".to_string()
    } else {
        format!("{attempts} attempts")
    }
}

impl<E> StartupReadFailure<E> {
    /// The refusal `main` reports. `refuse` builds the error the site has
    /// always refused with; a deadline refusal wraps that error for the last
    /// failed attempt in a context naming the site, the deadline and the
    /// attempts.
    pub fn into_anyhow(self, refuse: impl FnOnce(E) -> anyhow::Error) -> anyhow::Error {
        match self {
            StartupReadFailure::Refused(err) => refuse(err),
            StartupReadFailure::DeadlinePassed {
                site,
                attempts,
                deadline,
                cause,
                last,
            } => refuse(last).context(format!(
                "startup store read of {site} still failed with {} after {}, and the startup \
                 deadline of {} (--startup-store-deadline) has passed or would pass before \
                 another attempt; refusing to start",
                cause.failure_text(),
                attempts_text(attempts),
                humantime::format_duration(deadline)
            )),
            StartupReadFailure::CutOff {
                site,
                attempts,
                deadline,
                bound,
                cause,
                last,
            } => refuse(last).context(format!(
                "startup store read of {site} was cut off after attempt {attempts} ran for {}, \
                 its bound under the startup deadline of {} (--startup-store-deadline): the time \
                 left before the deadline, and never less than {}; the attempt before it failed \
                 with {}; refusing to start",
                humantime::format_duration(bound),
                humantime::format_duration(deadline),
                humantime::format_duration(MIN_STARTUP_ATTEMPT),
                cause.failure_text()
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
    /// clock places at process start. A zero `deadline` turns retry, and with it
    /// the retry bound, off: the first error of any kind refuses, unchanged.
    /// `seed` feeds the jitter.
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

    /// Time left before the deadline, `None` once it has passed.
    fn remaining(&self) -> Option<Duration> {
        self.deadline
            .checked_sub(self.clock.elapsed())
            .filter(|remaining| !remaining.is_zero())
    }

    /// The bound on a retry starting now: the time left before the deadline
    /// or [`MIN_STARTUP_ATTEMPT`], whichever is longer.
    fn attempt_bound(&self) -> Duration {
        self.remaining()
            .unwrap_or(Duration::ZERO)
            .max(MIN_STARTUP_ATTEMPT)
    }

    /// Run `op` until it succeeds, fails with an error `retry_cause` gives no
    /// [`RetryCause`] for, or the deadline ends it. Each retry logs one warning naming `site`, the
    /// attempt number and the error.
    ///
    /// The first attempt is not bounded here; only the store's own request
    /// timeout and retries end it. Each retry is bounded by the time left
    /// before the deadline or [`MIN_STARTUP_ATTEMPT`], whichever is longer,
    /// and is cut off if it is still running then; a cut-off retry counts as
    /// one attempt. A retry starts only when its wait ends before the
    /// deadline, so once an attempt has failed transiently the read ends at
    /// that failure or by the deadline plus [`MIN_STARTUP_ATTEMPT`], whichever
    /// is later. A site whose first attempt fails with a retried error after
    /// the deadline has passed gets no retry and refuses.
    pub async fn run<T, E, F, Fut>(
        &self,
        site: &'static str,
        mut op: F,
        retry_cause: impl Fn(&E) -> Option<RetryCause>,
    ) -> Result<T, StartupReadFailure<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: Display,
    {
        let mut attempt: u32 = 0;
        let mut last: Option<(RetryCause, E)> = None;
        loop {
            attempt = attempt.saturating_add(1);
            let outcome = match last.take() {
                None => op().await,
                Some((cause, previous)) => {
                    // A wait that overran the deadline: the retry does not
                    // start.
                    if self.remaining().is_none() {
                        return Err(StartupReadFailure::DeadlinePassed {
                            site,
                            attempts: attempt.saturating_sub(1),
                            deadline: self.deadline,
                            cause,
                            last: previous,
                        });
                    }
                    let bound = self.attempt_bound();
                    let attempt_future = op();
                    tokio::pin!(attempt_future);
                    let timer = self.clock.attempt_timer(bound);
                    tokio::select! {
                        biased;
                        outcome = &mut attempt_future => outcome,
                        () = timer => {
                            tracing::warn!(
                                site,
                                attempt,
                                bound_ms = u64::try_from(bound.as_millis()).unwrap_or(u64::MAX),
                                deadline_ms = u64::try_from(self.deadline.as_millis())
                                    .unwrap_or(u64::MAX),
                                "startup store read cut off at the end of its attempt bound"
                            );
                            return Err(StartupReadFailure::CutOff {
                                site,
                                attempts: attempt,
                                deadline: self.deadline,
                                bound,
                                cause,
                                last: previous,
                            });
                        }
                    }
                }
            };
            let err = match outcome {
                Ok(value) => {
                    if attempt > 1 {
                        tracing::info!(
                            site,
                            attempts = attempt,
                            "startup store read succeeded after retried failures"
                        );
                    }
                    return Ok(value);
                }
                Err(err) => err,
            };
            let cause = match retry_cause(&err) {
                Some(cause) if !self.deadline.is_zero() => cause,
                _ => return Err(StartupReadFailure::Refused(err)),
            };
            let elapsed = self.clock.elapsed();
            let delay = self.backoff(attempt);
            match self.remaining() {
                Some(remaining) if delay < remaining => {}
                _ => {
                    return Err(StartupReadFailure::DeadlinePassed {
                        site,
                        attempts: attempt,
                        deadline: self.deadline,
                        cause,
                        last: err,
                    });
                }
            }
            let retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
            let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
            let deadline_ms = u64::try_from(self.deadline.as_millis()).unwrap_or(u64::MAX);
            match cause {
                RetryCause::Transient => tracing::warn!(
                    site,
                    attempt,
                    error = %err,
                    retry_in_ms,
                    elapsed_ms,
                    deadline_ms,
                    "startup store read failed with a transient store error; retrying"
                ),
                RetryCause::ClockSkew => tracing::warn!(
                    site,
                    attempt,
                    error = %err,
                    retry_in_ms,
                    elapsed_ms,
                    deadline_ms,
                    "startup store read was refused for clock skew (the store rejected the \
                     request clock); retrying until the startup deadline, check time sync"
                ),
            }
            last = Some((cause, err));
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

/// Why startup retries a store error, `None` when it does not: the kinds
/// [`StoreError::is_retryable`] accepts (`Transient`, `Timeout` and
/// `Throttled`) are [`RetryCause::Transient`], and
/// [`StoreError::ClockSkewed`] is [`RetryCause::ClockSkew`].
pub fn store_error_retry_cause(err: &StoreError) -> Option<RetryCause> {
    if err.is_retryable() {
        Some(RetryCause::Transient)
    } else if matches!(err, StoreError::ClockSkewed(_)) {
        Some(RetryCause::ClockSkew)
    } else {
        None
    }
}

/// Why startup retries the first [`StoreError`] in `err`'s source chain, `err`
/// included ([`store_error_retry_cause`]). An error with no store error in its
/// chain is not retried.
pub fn chain_retry_cause(err: &(dyn StdError + 'static)) -> Option<RetryCause> {
    let mut current = Some(err);
    while let Some(e) = current {
        if let Some(store) = e.downcast_ref::<StoreError>() {
            return store_error_retry_cause(store);
        }
        current = e.source();
    }
    None
}

/// `TenantKmsError` holds its `KeyEpochError` in a field that is not a
/// source, so the chain walk starts there.
fn tenant_kms_retry_cause(err: &TenantKmsError) -> Option<RetryCause> {
    match err {
        TenantKmsError::ReadEpochs { error, .. } | TenantKmsError::RecordEpoch { error, .. } => {
            chain_retry_cause(error)
        }
        _ => None,
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
            |err| chain_retry_cause(err),
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

/// The retired-reader gate (ADR-4233 decision 7), retried. The gate is
/// read-only, so a retry repeats its reads from the start. A store error the
/// gate wraps, in [`CompatGateError::Store`] or through
/// [`CompatGateError::CompatUnreadable`], retries when it is transient; every
/// refusal the gate decides from what it read refuses on the first attempt.
/// It sends no request while `retired` is empty. The deployment key comes
/// from the configured scheme, as `--tenant-hash-key-file` gives it.
pub async fn enforce_retired_reader_gate(
    retry: &StartupRetry,
    store: &dyn ObjectStoreBackend,
    configured: &ConfiguredScheme,
    retired: &[RetiredReader],
    now_ns: i64,
) -> anyhow::Result<GateOutcome> {
    let deployment_key = match configured {
        ConfiguredScheme::Keyed(key) => Some(key.as_ref()),
        ConfiguredScheme::Unkeyed | ConfiguredScheme::Unspecified => None,
    };
    let outcome = retry
        .run(
            SITE_RETIRED_READER_GATE,
            || crate::compat_gate::enforce(store, deployment_key, retired, now_ns),
            |err: &CompatGateError| chain_retry_cause(err),
        )
        .await
        .map_err(|failure| {
            failure.into_anyhow(|err| {
                anyhow::Error::new(err).context("retired-reader gate (sys/compat) refused startup")
            })
        })?;
    tracing::debug!(outcome = ?outcome, "retired-reader gate passed");
    Ok(outcome)
}

/// Tenant-hash scheme resolution and marker bootstrap (ADR-0050 section 3),
/// retried. The marker write is create-if-absent and a retry starts from the
/// read, so a write that landed before an ambiguous failure is found by the
/// next attempt and validated like any present marker.
///
/// That re-read does not log the pre-ADR adoption warning or count
/// `V1_UNKEYED_ADOPTIONS`: a present marker cannot tell this process's lost
/// write from one a concurrent process landed, and `resolve_and_pin` skips
/// both on its `AlreadyExists` race path for the same reason. An adoption
/// whose acknowledgement was lost therefore goes unlogged and uncounted here.
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
            |err: &TenancyError| chain_retry_cause(err),
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
            tenant_kms_retry_cause,
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
/// As with the tenancy marker, that re-read does not log the adoption line.
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
            |err| chain_retry_cause(err),
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

/// What [`run_startup_store_checks`] reads its configuration from.
pub struct StartupStoreInputs<'a> {
    /// The foreground store every check reads.
    pub store: &'a Arc<dyn ObjectStoreBackend>,
    pub store_kind: StoreKind,
    /// The bucket identity the qualification record must name, if any.
    pub backend_identity: Option<&'a str>,
    /// `--require-bucket-protection`.
    pub require_bucket_protection: bool,
    /// The concrete S3 store the bucket-protection read goes through.
    pub store_s3: Option<&'a S3Store>,
    pub tenancy: ConfiguredScheme,
    /// The readers this build retired, which the retired-reader gate checks:
    /// `compat_gate::RETIRED_READERS` in `main`.
    pub retired_readers: &'a [RetiredReader],
    /// The KMS routing store and `--tenant-kms-config`, when that flag set
    /// up per-tenant SSE-KMS routing.
    pub tenant_kms: Option<(&'a KmsRoutingStore, &'a TenantKmsConfig)>,
    /// `--shards`.
    pub shard_count: u32,
    pub mode: Mode,
}

/// What the startup store checks resolved.
pub struct StartupStoreState {
    /// Why the retired-reader gate let the process start.
    pub retired_reader_gate: GateOutcome,
    pub tenancy: ResolvedTenancy,
    /// The statically known tenants, hashed under the installed scheme.
    pub static_tenants: Vec<TenantHash>,
    pub gc: GcConfigValues,
}

/// Every startup store check `main` makes before a listener binds, in order:
/// qualification, the bucket-protection gate, the retired-reader gate (before
/// tenancy pinning, which writes `sys/tenancy` on a fresh store, because the
/// gate tells a fresh store from an unadopted one by that marker's absence),
/// tenancy pinning (and the scheme install), tenant key epochs, provisioning
/// validation for the tenants `static_tenants` returns once the scheme is
/// installed, and the `sys/gc` bootstrap. Every check but the bucket-protection gate, which has its own
/// bound, runs under `retry`. The IPCrypt marker check is the one site left,
/// inside `start`. `now_unix_ns` stamps the records the checks write.
pub async fn run_startup_store_checks(
    retry: &StartupRetry,
    inputs: StartupStoreInputs<'_>,
    static_tenants: impl FnOnce() -> Vec<TenantHash>,
    now_unix_ns: impl Fn() -> i64,
) -> anyhow::Result<StartupStoreState> {
    let store = inputs.store;
    enforce_qualification(
        retry,
        store.as_ref(),
        inputs.store_kind,
        inputs.backend_identity,
    )
    .await?;
    crate::bucket_protection::enforce_at_startup_on(
        inputs.require_bucket_protection,
        inputs.store_s3,
        store.as_ref(),
    )
    .await?;
    let retired_reader_gate = enforce_retired_reader_gate(
        retry,
        store.as_ref(),
        &inputs.tenancy,
        inputs.retired_readers,
        now_unix_ns(),
    )
    .await?;
    let tenancy = resolve_tenancy(retry, store.as_ref(), inputs.tenancy, now_unix_ns()).await?;
    ravel_types::install_tenant_hash_scheme(tenancy.scheme.clone())
        .map_err(|_| anyhow::anyhow!("tenant-hash scheme was already installed"))?;
    if let Some((kms, config)) = inputs.tenant_kms {
        configure_tenant_kms(retry, kms, store.as_ref(), config, now_unix_ns()).await?;
    }
    let static_tenants = static_tenants();
    validate_static_provisioning(
        retry,
        store.as_ref(),
        &static_tenants,
        inputs.shard_count,
        inputs.mode,
        now_unix_ns(),
    )
    .await?;
    let gc = crate::gc_config::bootstrap_at_startup(
        Arc::clone(store),
        now_unix_ns(),
        inputs.mode,
        retry,
    )
    .await?;
    Ok(StartupStoreState {
        retired_reader_gate,
        tenancy,
        static_tenants,
        gc,
    })
}

/// Virtual time and fault plans shared by the startup-site tests here and in
/// `gc_config` and `ipcrypt`.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod test_support {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use ravel_object_store::fault::{FaultPlan, Occurrence, Op, Rule, ScriptedFault, Sequence};
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload,
        ObjectMeta, ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome,
        StoreError,
    };

    use super::{StartupClock, StartupRetry};

    /// Answers the `get`s of `key` with scripted store errors, in order, then
    /// passes them through; every other call passes through. `FaultStore` has
    /// no scripted `AccessDenied` or `ClockSkewed`, which this exists for.
    pub(crate) struct ScriptedRefusals {
        inner: Arc<dyn ObjectStoreBackend>,
        key: String,
        refusals: Mutex<VecDeque<StoreError>>,
        always: Option<fn() -> StoreError>,
        gets: Mutex<u64>,
    }

    impl ScriptedRefusals {
        /// `refusals` in order, then the real answer.
        pub(crate) fn then_ok(
            inner: Arc<dyn ObjectStoreBackend>,
            key: &str,
            refusals: impl IntoIterator<Item = StoreError>,
        ) -> Self {
            ScriptedRefusals {
                inner,
                key: key.to_string(),
                refusals: Mutex::new(refusals.into_iter().collect()),
                always: None,
                gets: Mutex::new(0),
            }
        }

        /// `refusal()` on every `get` of `key`.
        pub(crate) fn always(
            inner: Arc<dyn ObjectStoreBackend>,
            key: &str,
            refusal: fn() -> StoreError,
        ) -> Self {
            ScriptedRefusals {
                always: Some(refusal),
                ..ScriptedRefusals::then_ok(inner, key, [])
            }
        }

        /// The `get`s of `key` so far, refused or not.
        pub(crate) fn gets(&self) -> u64 {
            *self.gets.lock().expect("gets lock")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for ScriptedRefusals {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            if key == self.key {
                *self.gets.lock().expect("gets lock") += 1;
                if let Some(refusal) = self.always {
                    return Err(refusal());
                }
                if let Some(refusal) = self.refusals.lock().expect("refusals lock").pop_front() {
                    return Err(refusal);
                }
            }
            self.inner.get(key, range).await
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &Pin,
        ) -> Result<PinnedRead, StoreError> {
            self.inner.get_pinned(key, range, pin).await
        }

        async fn get_with_pin(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<PinnedRead, StoreError> {
            self.inner.get_with_pin(key, range).await
        }

        async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
            self.inner.pin_of(key).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
            self.inner.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// A 403 `RequestTimeTooSkewed`, as the S3 adapter reports it.
    pub(crate) fn clock_skewed() -> StoreError {
        StoreError::ClockSkewed(
            "k: Server returned non-2xx status code: 403 Forbidden: \
             <Error><Code>RequestTimeTooSkewed</Code></Error>"
                .into(),
        )
    }

    /// A 403 `AccessDenied` whose text names the clock-skew code, so only a
    /// classification by text could take it for a clock-skew refusal.
    pub(crate) fn denied_naming_clock_skew() -> StoreError {
        StoreError::AccessDenied(
            "k: Server returned non-2xx status code: 403 Forbidden: \
             <Error><Code>AccessDenied</Code><Message>RequestTimeTooSkewed</Message></Error>"
                .into(),
        )
    }

    /// Virtual time: `sleep` advances it at once and records the request.
    /// `attempt_timer` yields once, so an attempt that is ready when first
    /// polled wins, then advances the time by the whole bound and records
    /// the cut-off.
    #[derive(Default)]
    pub(crate) struct ManualClock {
        now: Mutex<Duration>,
        sleeps: Mutex<Vec<Duration>>,
        cut_offs: Mutex<Vec<Duration>>,
        overshoot: Mutex<Duration>,
    }

    impl ManualClock {
        pub(crate) fn sleeps(&self) -> Vec<Duration> {
            self.sleeps.lock().expect("sleeps lock").clone()
        }

        /// The bounds of the attempts the timer cut off.
        pub(crate) fn cut_offs(&self) -> Vec<Duration> {
            self.cut_offs.lock().expect("cut-offs lock").clone()
        }

        /// Make every later sleep end `by` past the time it asked for.
        pub(crate) fn overshoot_sleeps_by(&self, by: Duration) {
            *self.overshoot.lock().expect("overshoot lock") = by;
        }

        /// Move the virtual time forward without a recorded sleep.
        pub(crate) fn advance(&self, by: Duration) {
            *self.now.lock().expect("clock lock") += by;
        }

        /// The virtual time the sleeps have advanced to.
        pub(crate) fn virtual_now(&self) -> Duration {
            *self.now.lock().expect("clock lock")
        }
    }

    #[async_trait::async_trait]
    impl StartupClock for ManualClock {
        fn elapsed(&self) -> Duration {
            *self.now.lock().expect("clock lock")
        }

        async fn sleep(&self, duration: Duration) {
            let overshoot = *self.overshoot.lock().expect("overshoot lock");
            *self.now.lock().expect("clock lock") += duration + overshoot;
            self.sleeps.lock().expect("sleeps lock").push(duration);
        }

        async fn attempt_timer(&self, duration: Duration) {
            tokio::task::yield_now().await;
            *self.now.lock().expect("clock lock") += duration;
            self.cut_offs.lock().expect("cut-offs lock").push(duration);
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

    /// The first `put` on `key` lands but answers `Transient` (a lost
    /// acknowledgement); later puts pass through.
    pub(crate) fn ack_lost_then_ok(key: &str) -> FaultPlan {
        FaultPlan::empty().with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains(key)
                .then_fault(ScriptedFault::DuplicateDelivery)
                .then_passthrough(),
        )
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

    /// Fails `failures` times on (`op`, `key`) with `fault`, then passes
    /// through.
    pub(crate) fn fault_then_ok(
        op: Op,
        key: &str,
        fault: ScriptedFault,
        failures: usize,
    ) -> FaultPlan {
        let mut sequence = Sequence::new(op).with_key_contains(key);
        for _ in 0..failures {
            sequence = sequence.then_fault(fault.clone());
        }
        FaultPlan::empty().with_sequence(sequence.then_passthrough())
    }

    /// The text `into_anyhow` gives a deadline refusal for a clock-skew
    /// refusal, for `site` after `attempts`.
    pub(crate) fn assert_clock_skew_deadline_refusal(
        err: &anyhow::Error,
        site: &str,
        attempts: u64,
    ) {
        assert!(attempts > 1, "retried before giving up: {attempts}");
        assert_eq!(
            err.to_string(),
            format!(
                "startup store read of {site} still failed with a clock-skew refusal (the \
                 store rejected the request clock, S3 RequestTimeTooSkewed; check time sync \
                 between this host and the store) after {attempts} attempts, and the startup \
                 deadline of 15s (--startup-store-deadline) has passed or would pass before \
                 another attempt; refusing to start"
            )
        );
    }

    /// The text `into_anyhow` gives a deadline refusal, for `site` after
    /// `attempts`.
    pub(crate) fn assert_deadline_refusal(err: &anyhow::Error, site: &str, attempts: u64) {
        let message = err.to_string();
        assert!(attempts > 1, "retried before giving up: {attempts}");
        assert_eq!(
            message,
            format!(
                "startup store read of {site} still failed with a transient store error after \
                 {attempts} attempts, and the startup deadline of 15s (--startup-store-deadline) \
                 has passed or would pass before another attempt; refusing to start"
            )
        );
    }

    /// The text `into_anyhow` gives an attempt cut off after `bound`.
    pub(crate) fn assert_cut_off_refusal(
        err: &anyhow::Error,
        site: &str,
        attempts: u64,
        bound: Duration,
    ) {
        assert_eq!(
            err.to_string(),
            format!(
                "startup store read of {site} was cut off after attempt {attempts} ran for {}, \
                 its bound under the startup deadline of 15s (--startup-store-deadline): the \
                 time left before the deadline, and never less than 5s; the attempt before it \
                 failed with a transient store error; refusing to start",
                humantime::format_duration(bound)
            )
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_catalog::compat::CompatError;
    use ravel_object_store::conformance::{
        CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY, QualificationRecord,
    };
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, ScriptedFault};
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
                store_error_retry_cause,
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
            assert!(
                *sleep <= step && *sleep >= step / 2,
                "sleep {i} = {sleep:?} outside [{:?}, {step:?}]",
                step / 2
            );
        }
        // No wait reaches the deadline; the one refused would have, and no
        // wait is longer than the cap.
        let slept = sleeps.iter().sum::<Duration>();
        assert!(slept < DEADLINE, "{slept:?}");
        assert!(DEADLINE - slept <= MAX_BACKOFF, "{slept:?}");
        assert_eq!(clock.virtual_now(), slept);
        assert_eq!(calls as usize, sleeps.len() + 1);
        assert!(clock.cut_offs().is_empty());
    }

    /// A healthy first attempt is not bounded: a store that answers 21 s
    /// after the read starts, past the deadline plus `MIN_STARTUP_ATTEMPT`,
    /// starts. On tokio's paused clock, so the store delay and any bound race
    /// in virtual time.
    #[tokio::test(start_paused = true)]
    async fn a_slow_healthy_first_attempt_is_not_cut_off() {
        let retry = StartupRetry::new(Arc::new(TokioStartupClock::starting_now()), DEADLINE, 7);
        let started = tokio::time::Instant::now();
        let answer_after = DEADLINE + MIN_STARTUP_ATTEMPT + Duration::from_secs(1);
        let store = faulted(FaultPlan::empty());
        seed_qualification(store.inner()).await;
        let gate = store.hold(
            Op::Get,
            Some(QUALIFICATION_KEY.to_string()),
            ravel_object_store::fault::Occurrence::Always,
        );
        let releaser = release_after(gate.clone(), answer_after);
        enforce_qualification(&retry, &store, StoreKind::S3, None)
            .await
            .expect("a first attempt answering 21 s after it starts is not cut off");
        releaser.await.expect("releaser task");
        assert_eq!(gate.held_count(), 0);
        // hygiene-allow: wall-clock -- tokio::time::Instant under start_paused is the paused virtual clock the code under test also reads
        assert_eq!(
            started.elapsed(),
            answer_after,
            "the read ran its full 21 s"
        );
    }

    /// A transient failure, then a retry that hangs: the retry is bounded by
    /// the time left, counts as the second attempt, and the refusal carries
    /// the first attempt's error.
    #[tokio::test]
    async fn a_hung_retry_is_cut_off_by_the_time_left() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(Op::Get, QUALIFICATION_KEY, 1));
        seed_qualification(store.inner()).await;
        let gate = store.hold(
            Op::Get,
            Some(QUALIFICATION_KEY.to_string()),
            ravel_object_store::fault::Occurrence::Nth(2),
        );
        let err = enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect_err("the hung retry refuses at the deadline");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
        assert_eq!(gate.held_count(), 1);
        assert_cut_off_refusal(&err, SITE_QUALIFICATION, 2, DEADLINE - clock.sleeps()[0]);
        assert!(
            format!("{err:#}").contains("store backend is not qualified"),
            "{err:#}"
        );
        let sleeps = clock.sleeps();
        assert_eq!(sleeps.len(), 1);
        assert_eq!(clock.cut_offs(), vec![DEADLINE - sleeps[0]]);
        assert_eq!(clock.virtual_now(), DEADLINE);
    }

    /// A wait that ends past the deadline starts no retry, bounded or not.
    #[tokio::test]
    async fn a_wait_that_overruns_the_deadline_starts_no_retry() {
        let clock = Arc::new(ManualClock::default());
        clock.overshoot_sleeps_by(DEADLINE);
        let mut calls = 0u32;
        let result: Result<(), _> = retry_on(clock.clone())
            .run(
                "test",
                || {
                    calls += 1;
                    async { Err(StoreError::Transient("down".into())) }
                },
                store_error_retry_cause,
            )
            .await;
        assert!(
            matches!(
                result,
                Err(StartupReadFailure::DeadlinePassed { attempts: 1, .. })
            ),
            "{result:?}"
        );
        assert_eq!(calls, 1);
        assert_eq!(clock.sleeps().len(), 1);
    }

    /// A site first reached after the deadline gets one attempt and no retry.
    #[tokio::test]
    async fn a_first_attempt_after_the_deadline_runs_once_without_retry() {
        let clock = Arc::new(ManualClock::default());
        clock.advance(DEADLINE + Duration::from_secs(1));
        let store = faulted(always_transient(Op::Get, QUALIFICATION_KEY));
        seed_qualification(store.inner()).await;
        let err = enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect_err("no retry past the deadline");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
        assert!(
            err.to_string()
                .contains("after 1 attempt, and the startup deadline"),
            "{err}"
        );
        assert!(clock.sleeps().is_empty());
        assert!(clock.cut_offs().is_empty());
    }

    /// A healthy read that starts 1 s before the deadline and takes 3 s is
    /// not cut off when the 1 s left runs out, and starts. On tokio's paused
    /// clock, so the store delay and any bound race in virtual time.
    #[tokio::test(start_paused = true)]
    async fn a_healthy_read_started_just_before_the_deadline_is_not_cut_off() {
        let retry = StartupRetry::new(Arc::new(TokioStartupClock::starting_now()), DEADLINE, 7);
        let started = tokio::time::Instant::now();
        tokio::time::advance(DEADLINE - Duration::from_secs(1)).await;
        let store = faulted(FaultPlan::empty());
        seed_qualification(store.inner()).await;
        let gate = store.hold(
            Op::Get,
            Some(QUALIFICATION_KEY.to_string()),
            ravel_object_store::fault::Occurrence::Always,
        );
        let releaser = {
            let gate = gate.clone();
            tokio::spawn(async move {
                gate.wait_until_held(1).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
                let held = gate.held();
                assert_eq!(held.len(), 1);
                assert!(gate.release(held[0]));
            })
        };
        enforce_qualification(&retry, &store, StoreKind::S3, None)
            .await
            .expect("a read answering 3 s after it starts is not cut off");
        releaser.await.expect("releaser task");
        assert_eq!(gate.held_count(), 0);
        // hygiene-allow: wall-clock -- tokio::time::Instant under start_paused is the paused virtual clock the code under test also reads
        assert_eq!(
            started.elapsed(),
            DEADLINE + Duration::from_secs(2),
            "the read ran its full 3 s"
        );
    }

    /// A site first reached after the deadline runs its first attempt
    /// unbounded: a store that answers 10 s after the attempt starts, past
    /// `MIN_STARTUP_ATTEMPT`, starts.
    #[tokio::test(start_paused = true)]
    async fn a_slow_healthy_first_attempt_after_the_deadline_is_not_cut_off() {
        let retry = StartupRetry::new(Arc::new(TokioStartupClock::starting_now()), DEADLINE, 7);
        let started = tokio::time::Instant::now();
        let reached = DEADLINE + Duration::from_secs(1);
        tokio::time::advance(reached).await;
        let answer_after = MIN_STARTUP_ATTEMPT * 2;
        let store = faulted(FaultPlan::empty());
        seed_qualification(store.inner()).await;
        let gate = store.hold(
            Op::Get,
            Some(QUALIFICATION_KEY.to_string()),
            ravel_object_store::fault::Occurrence::Always,
        );
        let releaser = release_after(gate.clone(), answer_after);
        enforce_qualification(&retry, &store, StoreKind::S3, None)
            .await
            .expect("a first attempt after the deadline is not cut off");
        releaser.await.expect("releaser task");
        assert_eq!(gate.held_count(), 0);
        // hygiene-allow: wall-clock -- tokio::time::Instant under start_paused is the paused virtual clock the code under test also reads
        assert_eq!(started.elapsed(), reached + answer_after);
    }

    /// A read that fails transiently 1 s before the deadline and whose retry
    /// never answers: the retry is bounded by `MIN_STARTUP_ATTEMPT`, not by
    /// the time left, and refuses 5 s after it starts, carrying the first
    /// attempt's error.
    #[tokio::test(start_paused = true)]
    async fn a_hung_retry_near_the_deadline_is_cut_off_after_the_minimum() {
        let retry = StartupRetry::new(Arc::new(TokioStartupClock::starting_now()), DEADLINE, 7);
        let started = tokio::time::Instant::now();
        let reached = DEADLINE - Duration::from_secs(1);
        tokio::time::advance(reached).await;
        let store = faulted(transient_then_ok(Op::Get, QUALIFICATION_KEY, 1));
        seed_qualification(store.inner()).await;
        let gate = store.hold(
            Op::Get,
            Some(QUALIFICATION_KEY.to_string()),
            ravel_object_store::fault::Occurrence::Nth(2),
        );
        let err = tokio::time::timeout(
            Duration::from_secs(600),
            enforce_qualification(&retry, &store, StoreKind::S3, None),
        )
        .await
        .expect("a retry is bounded")
        .expect_err("a hung retry refuses");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
        assert_eq!(gate.held_count(), 1, "the retry reached the store");
        assert_cut_off_refusal(&err, SITE_QUALIFICATION, 2, MIN_STARTUP_ATTEMPT);
        assert!(
            format!("{err:#}").contains("store backend is not qualified"),
            "{err:#}"
        );
        // The one wait is the first backoff step, jittered into [50, 100] ms.
        let waited = started.elapsed() - reached - MIN_STARTUP_ATTEMPT;
        // hygiene-allow: wall-clock -- tokio::time::Instant under start_paused is the paused virtual clock the code under test also reads
        assert!(
            waited >= INITIAL_BACKOFF / 2 && waited <= INITIAL_BACKOFF,
            "{waited:?}"
        );
    }

    /// Releases the one read `gate` holds `after` it is first held.
    fn release_after(
        gate: ravel_object_store::fault::GateHandle,
        after: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            gate.wait_until_held(1).await;
            tokio::time::sleep(after).await;
            let held = gate.held();
            assert_eq!(held.len(), 1);
            assert!(gate.release(held[0]));
        })
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
                store_error_retry_cause,
            )
            .await;
        assert!(matches!(result, Err(StartupReadFailure::Refused(_))));
        assert_eq!(calls, 1);
        assert!(clock.sleeps().is_empty());
    }

    #[derive(Clone)]
    struct CapturedLog(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn each_retry_logs_one_warning_with_site_attempt_and_error() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = CapturedLog(buffer.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let clock = Arc::new(ManualClock::default());
        let mut calls = 0u32;
        retry_on(clock)
            .run(
                "sys/example",
                || {
                    calls += 1;
                    let failing = calls <= 2;
                    let call = calls;
                    async move {
                        if failing {
                            Err(StoreError::Transient(format!("blip {call}")))
                        } else {
                            Ok(())
                        }
                    }
                },
                store_error_retry_cause,
            )
            .await
            .expect("succeeds on the third attempt");

        let logged = String::from_utf8(buffer.lock().expect("log lock").clone()).expect("utf-8");
        let warnings: Vec<&str> = logged.lines().filter(|l| l.contains("WARN")).collect();
        assert_eq!(warnings.len(), 2, "{logged}");
        for (i, line) in warnings.iter().enumerate() {
            let attempt = i + 1;
            assert!(line.contains("site=\"sys/example\""), "{line}");
            assert!(line.contains(&format!("attempt={attempt}")), "{line}");
            assert!(
                line.contains(&format!("error=transient error: blip {attempt}")),
                "{line}"
            );
        }
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
        let transient = Some(RetryCause::Transient);
        let tenancy = TenancyError::Store(StoreError::Transient("x".into()));
        assert_eq!(chain_retry_cause(&tenancy), transient);
        assert_eq!(chain_retry_cause(&TenancyError::CorruptMarker), None);
        // Every kind `StoreError::is_retryable` accepts retries.
        let throttled = TenancyError::Store(StoreError::Throttled { retry_after_ms: 1 });
        assert_eq!(chain_retry_cause(&throttled), transient);
        assert_eq!(
            chain_retry_cause(&TenancyError::Store(StoreError::Timeout)),
            transient
        );
        let wrapped =
            anyhow::Error::new(TenancyError::Store(StoreError::Timeout)).context("outer context");
        assert_eq!(chain_retry_cause(wrapped.as_ref()), transient);
        assert_eq!(
            chain_retry_cause(&TenancyError::Store(StoreError::AccessDenied("no".into()))),
            None
        );
        assert_eq!(
            chain_retry_cause(&TenancyError::Store(StoreError::NotFound)),
            None
        );
        // A clock-skew refusal retries at startup only: at runtime it is not
        // retryable.
        assert!(!clock_skewed().is_retryable());
        let skewed = anyhow::Error::new(TenancyError::Store(clock_skewed())).context("outer");
        assert_eq!(
            chain_retry_cause(skewed.as_ref()),
            Some(RetryCause::ClockSkew)
        );
        // The text names the variant or the code; only the type counts.
        let text = std::io::Error::other("transient error: x");
        assert_eq!(chain_retry_cause(&text), None);
        let text = std::io::Error::other("403 RequestTimeTooSkewed (clock skew)");
        assert_eq!(chain_retry_cause(&text), None);
        assert_eq!(
            chain_retry_cause(&TenancyError::Store(denied_naming_clock_skew())),
            None
        );
        let kms = TenantKmsError::ReadEpochs {
            tenant: "acme".into(),
            error: ravel_catalog::KeyEpochError::Store {
                key: "t/x/enc".into(),
                source: StoreError::Transient("x".into()),
            },
        };
        assert_eq!(tenant_kms_retry_cause(&kms), transient);
        assert_eq!(
            tenant_kms_retry_cause(&TenantKmsError::EmptyTenantId),
            None
        );
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
    async fn qualification_retries_a_store_timeout_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(fault_then_ok(
            Op::Get,
            QUALIFICATION_KEY,
            ScriptedFault::Timeout,
            2,
        ));
        seed_qualification(store.inner()).await;
        enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect("starts once the store answers");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Timeout), 2);
        assert_eq!(store.sequence_progress(0), 3);
        assert_eq!(clock.sleeps().len(), 2);
    }

    #[tokio::test]
    async fn qualification_retries_a_throttled_store_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(fault_then_ok(
            Op::Get,
            QUALIFICATION_KEY,
            ScriptedFault::Throttled {
                retry_after_ms: 1000,
            },
            2,
        ));
        seed_qualification(store.inner()).await;
        enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect("starts once the store answers");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Throttled), 2);
        assert_eq!(store.sequence_progress(0), 3);
        assert_eq!(clock.sleeps().len(), 2);
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
        assert!(clock.virtual_now() < DEADLINE);
        assert!(DEADLINE - clock.virtual_now() <= MAX_BACKOFF);
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

    /// A store holding a qualification record.
    async fn qualified_memory() -> Arc<dyn ObjectStoreBackend> {
        let store = MemoryStore::new();
        seed_qualification(&store).await;
        Arc::new(store)
    }

    #[tokio::test]
    async fn qualification_retries_a_clock_skew_refusal_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = ScriptedRefusals::then_ok(
            qualified_memory().await,
            QUALIFICATION_KEY,
            [clock_skewed(), clock_skewed()],
        );
        enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect("starts once the store accepts the request clock");
        // Two refusals plus the read that passed: three attempts, two waits.
        assert_eq!(store.gets(), 3);
        assert_eq!(clock.sleeps().len(), 2);
        assert!(clock.virtual_now() < DEADLINE);
        // Startup retries it; runtime does not.
        assert!(!clock_skewed().is_retryable());
    }

    #[tokio::test]
    async fn qualification_clock_skew_past_the_deadline_refuses_naming_clock_skew() {
        let clock = Arc::new(ManualClock::default());
        let store = ScriptedRefusals::always(
            qualified_memory().await,
            QUALIFICATION_KEY,
            clock_skewed,
        );
        let err = enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect_err("refuses once the deadline passes");
        assert_clock_skew_deadline_refusal(&err, SITE_QUALIFICATION, store.gets());
        assert_eq!(clock.sleeps().len() as u64, store.gets() - 1);
        assert!(clock.virtual_now() < DEADLINE);
        assert!(DEADLINE - clock.virtual_now() <= MAX_BACKOFF);
        let chain = format!("{err:#}");
        assert!(chain.contains("store backend is not qualified"), "{chain}");
        assert!(chain.contains("RequestTimeTooSkewed"), "{chain}");
    }

    #[tokio::test]
    async fn qualification_access_denied_naming_the_clock_skew_code_refuses_at_once() {
        let clock = Arc::new(ManualClock::default());
        let store = ScriptedRefusals::always(
            qualified_memory().await,
            QUALIFICATION_KEY,
            denied_naming_clock_skew,
        );
        let err = enforce_qualification(&retry_on(clock.clone()), &store, StoreKind::S3, None)
            .await
            .expect_err("refuses at once");
        assert_eq!(store.gets(), 1);
        assert!(clock.sleeps().is_empty());
        assert_eq!(
            err.to_string(),
            "store backend is not qualified (sys/qualification); refusing to start"
        );
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

    // --- retired-reader gate ---

    const GATE_KEY: [u8; 32] = [0x2A; 32];
    const CERTIFIED_NS: i64 = 1_000_000_000_000;
    const GATE_NOW_NS: i64 = CERTIFIED_NS + 3_600_000_000_000;
    const RSEG_8: RetiredReader = RetiredReader {
        family: ravel_catalog::compat::FAMILY_RSEG,
        oldest_readable: 8,
    };
    const GATE_REFUSAL: &str = "retired-reader gate (sys/compat) refused startup";

    fn keyed() -> ConfiguredScheme {
        ConfiguredScheme::Keyed(Box::new(GATE_KEY))
    }

    /// An `rseg` entry with an optional version-8 certificate.
    fn rseg_entry(
        certified: bool,
        watermark_unix_ns: i64,
        contradicted: bool,
    ) -> ravel_proto::sys::v1::CompatFamily {
        ravel_proto::sys::v1::CompatFamily {
            family: ravel_catalog::compat::FAMILY_RSEG.to_string(),
            active_write_version: 8,
            certificate: certified.then(|| ravel_proto::sys::v1::Certificate {
                version: 8,
                certified_unix_ns: CERTIFIED_NS,
                member_count: 3,
                ..Default::default()
            }),
            late_writes_verified_through_unix_ns: watermark_unix_ns,
            contradicted,
            ..Default::default()
        }
    }

    async fn put_tenancy_marker(store: &dyn ObjectStoreBackend) {
        store
            .put(
                crate::tenancy::TENANCY_MARKER_KEY,
                Bytes::from_static(b"marker"),
                PutOptions::default(),
            )
            .await
            .expect("put sys/tenancy");
    }

    /// An adopted store: `sys/compat` holding `entry`, and `sys/tenancy`.
    async fn seed_compat(
        store: &dyn ObjectStoreBackend,
        entry: ravel_proto::sys::v1::CompatFamily,
    ) {
        ravel_catalog::compat::create_compat(
            store,
            &ravel_catalog::compat::CompatKeys::from_file_keys(&[GATE_KEY]).expect("key"),
            ravel_proto::sys::v1::CompatRecord {
                format_version: ravel_catalog::compat::COMPAT_FORMAT_VERSION,
                families: vec![entry],
                ..Default::default()
            },
        )
        .await
        .expect("create sys/compat");
        put_tenancy_marker(store).await;
    }

    #[tokio::test]
    async fn retired_reader_gate_retries_a_transient_compat_head_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(
            Op::Head,
            ravel_catalog::compat::COMPAT_KEY,
            2,
        ));
        let outcome = enforce_retired_reader_gate(
            &retry_on(clock.clone()),
            &store,
            &keyed(),
            &[RSEG_8],
            GATE_NOW_NS,
        )
        .await
        .expect("starts once the store answers");
        assert_eq!(outcome, crate::compat_gate::GateOutcome::Fresh);
        assert_eq!(store.fault_count(Op::Head, FaultKind::Transient), 2);
        assert_eq!(store.sequence_progress(0), 3);
        assert_eq!(clock.sleeps().len(), 2);
    }

    /// A transient error on the `sys/compat` GET reaches the gate as
    /// `CompatUnreadable(CompatError::Store { .. })`; the source-chain walk
    /// finds the store error under both and retries.
    #[tokio::test]
    async fn retired_reader_gate_retries_a_transient_compat_read_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(
            Op::Get,
            ravel_catalog::compat::COMPAT_KEY,
            2,
        ));
        seed_compat(store.inner(), rseg_entry(true, CERTIFIED_NS + 1, false)).await;
        let outcome = enforce_retired_reader_gate(
            &retry_on(clock.clone()),
            &store,
            &keyed(),
            &[RSEG_8],
            GATE_NOW_NS,
        )
        .await
        .expect("starts once the store answers");
        assert!(
            matches!(outcome, crate::compat_gate::GateOutcome::Certified { .. }),
            "{outcome:?}"
        );
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 2);
        assert_eq!(store.sequence_progress(0), 3);
        assert_eq!(clock.sleeps().len(), 2);
    }

    #[tokio::test]
    async fn retired_reader_gate_transient_past_the_deadline_refuses_naming_the_site() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_transient(
            Op::Head,
            ravel_catalog::compat::COMPAT_KEY,
        ));
        let err = enforce_retired_reader_gate(
            &retry_on(clock.clone()),
            &store,
            &keyed(),
            &[RSEG_8],
            GATE_NOW_NS,
        )
        .await
        .expect_err("refuses once the deadline passes");
        let attempts = store.fault_count(Op::Head, FaultKind::Transient);
        assert_deadline_refusal(&err, SITE_RETIRED_READER_GATE, attempts);
        assert!(format!("{err:#}").contains(GATE_REFUSAL));
    }

    #[tokio::test]
    async fn retired_reader_gate_non_transient_store_error_refuses_on_the_first_attempt() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(always_permanent(
            Op::Head,
            ravel_catalog::compat::COMPAT_KEY,
        ));
        let err = enforce_retired_reader_gate(
            &retry_on(clock.clone()),
            &store,
            &keyed(),
            &[RSEG_8],
            GATE_NOW_NS,
        )
        .await
        .expect_err("refuses at once");
        assert_eq!(store.fault_count(Op::Head, FaultKind::Permanent), 1);
        assert_eq!(err.to_string(), GATE_REFUSAL);
        assert!(matches!(
            err.downcast_ref::<CompatGateError>(),
            Some(CompatGateError::Store { .. })
        ));
        assert!(clock.sleeps().is_empty());
    }

    /// Every refusal the gate decides from what it read refuses on the first
    /// attempt, with the context `main` gave it before the retry existed.
    #[tokio::test]
    async fn retired_reader_gate_refusals_refuse_on_the_first_attempt() {
        type Check = fn(&CompatGateError) -> bool;
        let cases: [(
            &str,
            Option<ravel_proto::sys::v1::CompatFamily>,
            ConfiguredScheme,
            Check,
        ); 7] = [
            ("sys/tenancy without sys/compat", None, keyed(), |err| {
                matches!(err, CompatGateError::TenancyWithoutCompat)
            }),
            (
                "no certificate",
                Some(rseg_entry(false, CERTIFIED_NS + 1, false)),
                keyed(),
                |err| matches!(err, CompatGateError::CertificateAbsent { .. }),
            ),
            (
                "contradicted",
                Some(rseg_entry(true, CERTIFIED_NS + 1, true)),
                keyed(),
                |err| matches!(err, CompatGateError::Contradicted { .. }),
            ),
            (
                "watermark not past the census",
                Some(rseg_entry(true, CERTIFIED_NS, false)),
                keyed(),
                |err| matches!(err, CompatGateError::WatermarkNotPastCensus { .. }),
            ),
            (
                "unkeyed process",
                Some(rseg_entry(true, CERTIFIED_NS + 1, false)),
                ConfiguredScheme::Unkeyed,
                |err| matches!(err, CompatGateError::Unkeyed),
            ),
            (
                "no entry for the family",
                Some(ravel_proto::sys::v1::CompatFamily {
                    family: ravel_catalog::compat::FAMILY_RLOG.to_string(),
                    ..rseg_entry(true, CERTIFIED_NS + 1, false)
                }),
                keyed(),
                |err| matches!(err, CompatGateError::FamilyAbsent { .. }),
            ),
            (
                "certificate below the oldest readable version",
                Some(ravel_proto::sys::v1::CompatFamily {
                    certificate: Some(ravel_proto::sys::v1::Certificate {
                        version: 7,
                        certified_unix_ns: CERTIFIED_NS,
                        member_count: 3,
                        ..Default::default()
                    }),
                    ..rseg_entry(true, CERTIFIED_NS + 1, false)
                }),
                keyed(),
                |err| matches!(err, CompatGateError::CertificateBelow { .. }),
            ),
        ];
        for (name, entry, configured, check) in cases {
            let clock = Arc::new(ManualClock::default());
            let store = MemoryStore::new();
            match entry {
                Some(entry) => seed_compat(&store, entry).await,
                None => put_tenancy_marker(&store).await,
            }
            let err = enforce_retired_reader_gate(
                &retry_on(clock.clone()),
                &store,
                &configured,
                &[RSEG_8],
                GATE_NOW_NS,
            )
            .await
            .expect_err(name);
            assert_eq!(err.to_string(), GATE_REFUSAL, "{name}");
            let gate_err = err
                .downcast_ref::<CompatGateError>()
                .unwrap_or_else(|| panic!("{name}: {err:#}"));
            assert!(check(gate_err), "{name}: {gate_err:?}");
            assert!(clock.sleeps().is_empty(), "{name}: retried");
        }
    }

    /// A `sys/compat` the gate cannot read for a reason other than a
    /// transient store error refuses on the first attempt: one GET, no sleep.
    #[tokio::test]
    async fn retired_reader_gate_unreadable_compat_refuses_on_the_first_attempt() {
        type Check = fn(&CompatError) -> bool;
        let one_get = || {
            FaultPlan::empty().with_sequence(
                ravel_object_store::fault::Sequence::new(Op::Get)
                    .with_key_contains(ravel_catalog::compat::COMPAT_KEY)
                    .then_passthrough()
                    .then_passthrough(),
            )
        };
        let cases: [(&str, FaultPlan, bool, ConfiguredScheme, Check); 3] = [
            (
                "wrong deployment key",
                one_get(),
                true,
                ConfiguredScheme::Keyed(Box::new([0x55; 32])),
                |err| matches!(err, CompatError::BadMac { .. }),
            ),
            ("corrupt body", one_get(), false, keyed(), |err| {
                matches!(err, CompatError::Decode { .. })
            }),
            (
                "permanent store error on the GET",
                fault_then_ok(
                    Op::Get,
                    ravel_catalog::compat::COMPAT_KEY,
                    ScriptedFault::Permanent("bad request".into()),
                    2,
                ),
                true,
                keyed(),
                |err| matches!(err, CompatError::Store { .. }),
            ),
        ];
        for (name, plan, sealed, configured, check) in cases {
            let clock = Arc::new(ManualClock::default());
            let store = faulted(plan);
            if sealed {
                seed_compat(store.inner(), rseg_entry(true, CERTIFIED_NS + 1, false)).await;
            } else {
                store
                    .inner()
                    .put(
                        ravel_catalog::compat::COMPAT_KEY,
                        Bytes::from_static(b"not a protobuf"),
                        PutOptions::default(),
                    )
                    .await
                    .expect("put corrupt sys/compat");
                put_tenancy_marker(store.inner()).await;
            }
            let err = enforce_retired_reader_gate(
                &retry_on(clock.clone()),
                &store,
                &configured,
                &[RSEG_8],
                GATE_NOW_NS,
            )
            .await
            .expect_err(name);
            assert_eq!(err.to_string(), GATE_REFUSAL, "{name}");
            match err.downcast_ref::<CompatGateError>() {
                Some(CompatGateError::CompatUnreadable(cause)) => {
                    assert!(check(cause), "{name}: {cause:?}");
                }
                other => panic!("{name}: {other:?}"),
            }
            assert_eq!(store.sequence_progress(0), 1, "{name}: GETs of sys/compat");
            assert!(clock.sleeps().is_empty(), "{name}: retried");
        }
    }

    /// With `sys/compat` absent the gate HEADs `sys/tenancy`; a transient
    /// failure there retries the gate, which then starts on a fresh store.
    #[tokio::test]
    async fn retired_reader_gate_retries_a_transient_tenancy_head_then_starts() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(transient_then_ok(
            Op::Head,
            crate::tenancy::TENANCY_MARKER_KEY,
            2,
        ));
        let outcome = enforce_retired_reader_gate(
            &retry_on(clock.clone()),
            &store,
            &keyed(),
            &[RSEG_8],
            GATE_NOW_NS,
        )
        .await
        .expect("starts once the store answers");
        assert_eq!(outcome, crate::compat_gate::GateOutcome::Fresh);
        assert_eq!(store.fault_count(Op::Head, FaultKind::Transient), 2);
        assert_eq!(store.sequence_progress(0), 3);
        assert_eq!(clock.sleeps().len(), 2);
    }

    /// `sys/compat` deleted between the gate's HEAD and its GET: the gate
    /// HEADs `sys/tenancy` once and decides from it, without a retry.
    #[tokio::test]
    async fn retired_reader_gate_compat_deleted_before_the_get_decides_by_tenancy() {
        for with_tenancy in [true, false] {
            let clock = Arc::new(ManualClock::default());
            let store = faulted(
                FaultPlan::empty()
                    .with_sequence(
                        ravel_object_store::fault::Sequence::new(Op::Get)
                            .with_key_contains(ravel_catalog::compat::COMPAT_KEY)
                            .then_fault(ScriptedFault::NotFoundBlip)
                            .then_passthrough(),
                    )
                    .with_sequence(
                        ravel_object_store::fault::Sequence::new(Op::Head)
                            .with_key_contains(crate::tenancy::TENANCY_MARKER_KEY)
                            .then_passthrough()
                            .then_passthrough(),
                    ),
            );
            ravel_catalog::compat::create_compat(
                store.inner(),
                &ravel_catalog::compat::CompatKeys::from_file_keys(&[GATE_KEY]).expect("key"),
                ravel_proto::sys::v1::CompatRecord {
                    format_version: ravel_catalog::compat::COMPAT_FORMAT_VERSION,
                    families: vec![rseg_entry(true, CERTIFIED_NS + 1, false)],
                    ..Default::default()
                },
            )
            .await
            .expect("create sys/compat");
            if with_tenancy {
                put_tenancy_marker(store.inner()).await;
            }
            let result = enforce_retired_reader_gate(
                &retry_on(clock.clone()),
                &store,
                &keyed(),
                &[RSEG_8],
                GATE_NOW_NS,
            )
            .await;
            if with_tenancy {
                let err = result.expect_err("sys/tenancy without sys/compat");
                assert_eq!(err.to_string(), GATE_REFUSAL);
                assert!(matches!(
                    err.downcast_ref::<CompatGateError>(),
                    Some(CompatGateError::TenancyWithoutCompat)
                ));
            } else {
                assert_eq!(
                    result.expect("fresh"),
                    crate::compat_gate::GateOutcome::Fresh
                );
            }
            assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
            assert_eq!(store.sequence_progress(0), 1, "GETs of sys/compat");
            assert_eq!(store.sequence_progress(1), 1, "HEADs of sys/tenancy");
            assert!(clock.sleeps().is_empty(), "retried");
        }
    }

    /// With this build's empty retired set the gate sends no request, so a
    /// store that would fail every gate read is never asked.
    #[tokio::test]
    async fn retired_reader_gate_sends_no_request_for_an_empty_set() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(
            always_permanent(Op::Head, "sys/").with_rule(
                ravel_object_store::fault::Rule::new(
                    Op::Get,
                    ScriptedFault::Permanent("bad request".into()),
                )
                .with_key_contains("sys/")
                .with_occurrence(ravel_object_store::fault::Occurrence::Always),
            ),
        );
        assert!(crate::compat_gate::RETIRED_READERS.is_empty());
        let outcome = enforce_retired_reader_gate(
            &retry_on(clock.clone()),
            &store,
            &keyed(),
            crate::compat_gate::RETIRED_READERS,
            GATE_NOW_NS,
        )
        .await
        .expect("no retired reader, nothing read");
        assert_eq!(outcome, crate::compat_gate::GateOutcome::NoRetiredReader);
        assert_eq!(store.fault_count(Op::Head, FaultKind::Permanent), 0);
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 0);
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

    /// A marker write that landed but answered `Transient` is read back by
    /// the retry and validated; nothing is written a second time.
    #[tokio::test]
    async fn tenancy_reads_back_a_marker_whose_write_ack_was_lost() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(ack_lost_then_ok(crate::tenancy::TENANCY_MARKER_KEY));
        let resolved = resolve_tenancy(
            &retry_on(clock.clone()),
            &store,
            ConfiguredScheme::Unkeyed,
            1,
        )
        .await
        .expect("the retry finds the landed marker");
        assert!(matches!(resolved.scheme, TenantHashScheme::V1Unkeyed));
        assert_eq!(store.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
        // Only the faulted put ran: the passthrough step was never reached.
        assert_eq!(store.sequence_progress(0), 1);
        assert_eq!(clock.sleeps().len(), 1);
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

    /// An adoption record whose create landed but answered `Transient` is
    /// read back by the retry and validated as present; nothing is written a
    /// second time.
    #[tokio::test]
    async fn provisioning_reads_back_an_adoption_whose_write_ack_was_lost() {
        let clock = Arc::new(ManualClock::default());
        let tenant = TenantId::new("acme").hash();
        let hex = tenant.to_hex();
        let store = faulted(ack_lost_then_ok("/m/prov"));
        for key in [
            format!("t/{hex}/m/l0/0000/100/seg"),
            format!("t/{hex}/m/c/0000/100/rec.cmt"),
        ] {
            store
                .inner()
                .put(&key, Bytes::from_static(&[1]), PutOptions::default())
                .await
                .expect("seed pre-ADR data");
        }
        validate_static_provisioning(&retry_on(clock.clone()), &store, &[tenant], 4, Mode::All, 1)
            .await
            .expect("the retry finds the landed record");
        assert_eq!(store.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
        // Only the faulted put ran: the passthrough step was never reached.
        assert_eq!(store.sequence_progress(0), 1);
        assert_eq!(clock.sleeps().len(), 1);
        let record = store
            .inner()
            .get(
                &ravel_catalog::provisioning_key(&tenant, ravel_types::Signal::Metrics),
                GetRange::Full,
            )
            .await
            .expect("the adoption record landed");
        let record = <ravel_proto::sys::v1::ProvisioningRecord as prost::Message>::decode(
            record.data.as_ref(),
        )
        .expect("record decodes");
        assert_eq!(record.shard_count, 4);
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

    /// Epoch 0 lands but its acknowledgement is lost: the retry reads the
    /// unfinished bootstrap back and appends epoch 1 once, so the record ends
    /// with exactly the two epochs a clean start writes.
    #[tokio::test]
    async fn tenant_kms_completes_a_bootstrap_whose_first_write_ack_was_lost() {
        let clock = Arc::new(ManualClock::default());
        let store = faulted(ack_lost_then_ok("/enc"));
        configure_tenant_kms(
            &retry_on(clock.clone()),
            &kms_store(),
            &store,
            &kms_config(),
            1,
        )
        .await
        .expect("the retry completes the bootstrap");
        assert_eq!(store.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
        assert_eq!(store.sequence_progress(0), 2);
        let epochs = ravel_catalog::read_epochs_from_store(&store, &TenantId::new("acme").hash())
            .await
            .expect("read epochs")
            .expect("record present");
        let keys: Vec<(u32, &str)> = epochs
            .iter()
            .map(|e| (e.epoch, e.key_arn.as_str()))
            .collect();
        assert_eq!(
            keys,
            vec![(0, ""), (1, "arn:aws:kms:us-east-1:111122223333:key/acme")]
        );
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
