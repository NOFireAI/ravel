//! Server-side startup wiring for the durable GC-config object `sys/gc`
//! (ADR-0050 section 4, EC4).
//!
//! The object itself, its bootstrap/read/set, and the per-mode validators live
//! in [`ravel_maintain::gc_config`] (that crate owns the GC constraint and the
//! `CompactorConfig` the defaults derive from). This module is only the thin
//! server adapter: it bootstraps from this process's maintain defaults and
//! converts the `Duration`-typed process knobs (the query engine deadline, the
//! Flight ticket ceiling) into the nanoseconds the validators compare.
//!
//! The fresh-bucket property every startup path here depends on:
//! [`bootstrap`] writes `sys/gc` from the maintain defaults on a fresh
//! bucket rather than refusing because the object is absent, and a racing loser
//! re-reads the winner's object. So a fresh, never-bootstrapped bucket never
//! fails startup for any process whose credential may create `sys/gc`; only a
//! *present* object that a mode really violates refuses. Under per-role
//! credentials Gateway and Query may not create it, and their refused create
//! fails with [`bootstrap_failure_context`]'s start-order fix. The gateway,
//! query and maintain templates each list `sys/gc`, so on S3 an absent `sys/gc`
//! answers as missing and the bootstrap proceeds, and a refused read means the
//! credential lacks the read grant. Only under bucket policies that predate
//! those list grants is a GET of an absent key refused, so a fresh bucket can
//! surface as a refused read; that context names both readings, the missing
//! read grant first. Every other refusal names the missing grant, except a
//! clock-skew refusal (S3 `RequestTimeTooSkewed`), which names time sync.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use ravel_maintain::{CompactorConfig, GcAccessOp, GcConfigError, GcConfigValues};
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};

use crate::Mode;
use crate::startup_retry::{RetryCause, SITE_GC_CONFIG, StartupRetry, store_error_retry_cause};

/// A `Duration` as saturating `i64` nanoseconds, matching how every ns knob in
/// this workspace is compared. An absurd duration saturates to `i64::MAX`
/// rather than wrapping.
pub fn duration_to_ns(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// Bootstrap `sys/gc` on a fresh bucket (from this process's maintain defaults),
/// or read the durable object on a bootstrapped one. Never refuses on a merely
/// absent object; a concurrent bootstrap loser re-reads the winner's object.
pub async fn bootstrap(
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
) -> Result<GcConfigValues, GcConfigError> {
    ravel_maintain::bootstrap_gc_config(store, GcConfigValues::maintain_defaults(), now_ns).await
}

/// [`bootstrap`] as startup runs it: retried while it fails with a transient
/// store error (issue #4325) or a clock-skew refusal (issue #4521), and
/// refused otherwise, with [`CLOCK_SKEW_CONTEXT`] for a clock-skew refusal and
/// [`bootstrap_failure_context`] for anything else. The create is
/// create-if-absent and a retry starts from the read, so an object that landed
/// before an ambiguous failure reads back as present.
///
/// `GcConfigError` carries the store error as text, and reports a clock-skew
/// refusal as `AccessDenied`, so the store is wrapped in [`LastErrorStore`] to
/// learn the failing call's error by type.
pub async fn bootstrap_at_startup(
    store: Arc<dyn ObjectStoreBackend>,
    now_ns: i64,
    mode: Mode,
    retry: &StartupRetry,
) -> anyhow::Result<GcConfigValues> {
    let recording = LastErrorStore::new(store);
    retry
        .run(
            SITE_GC_CONFIG,
            || async {
                recording.forget();
                bootstrap(&recording, now_ns)
                    .await
                    .map_err(|error| GcAttemptError {
                        retry: match (&error, recording.last_failure_retry_cause()) {
                            (GcConfigError::Store(_), Some(RetryCause::Transient)) => {
                                Some(RetryCause::Transient)
                            }
                            (GcConfigError::AccessDenied { .. }, Some(RetryCause::ClockSkew)) => {
                                Some(RetryCause::ClockSkew)
                            }
                            _ => None,
                        },
                        error,
                    })
            },
            |err| err.retry,
        )
        .await
        .map_err(|failure| {
            failure.into_anyhow(|err| {
                let context = if err.retry == Some(RetryCause::ClockSkew) {
                    CLOCK_SKEW_CONTEXT.to_string()
                } else {
                    bootstrap_failure_context(&err.error, mode)
                };
                anyhow::Error::new(err.error).context(context)
            })
        })
}

/// The context startup attaches to a `sys/gc` read or create the store refused
/// for clock skew, in place of [`bootstrap_failure_context`]'s missing-grant
/// advice.
pub const CLOCK_SKEW_CONTEXT: &str = "the object store rejected this process's request clock \
     on sys/gc (S3 RequestTimeTooSkewed: the request's signing time is too far from the \
     store's clock), which is not a missing grant; check time sync (NTP) between this host \
     and the store. Startup retries this refusal until --startup-store-deadline";

/// One failed [`bootstrap`] attempt and why startup retries the store call
/// behind it ([`store_error_retry_cause`]), `None` when it does not.
struct GcAttemptError {
    error: GcConfigError,
    retry: Option<RetryCause>,
}

impl fmt::Display for GcAttemptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

/// Delegates every call and remembers why startup would retry the most recent
/// failing one ([`store_error_retry_cause`]).
struct LastErrorStore {
    inner: Arc<dyn ObjectStoreBackend>,
    last_failure: AtomicU8,
}

const NOT_RETRIED: u8 = 0;
const TRANSIENT: u8 = 1;
const CLOCK_SKEW: u8 = 2;

impl LastErrorStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Self {
        LastErrorStore {
            inner,
            last_failure: AtomicU8::new(NOT_RETRIED),
        }
    }

    fn forget(&self) {
        self.last_failure.store(NOT_RETRIED, Ordering::SeqCst);
    }

    fn last_failure_retry_cause(&self) -> Option<RetryCause> {
        match self.last_failure.load(Ordering::SeqCst) {
            TRANSIENT => Some(RetryCause::Transient),
            CLOCK_SKEW => Some(RetryCause::ClockSkew),
            _ => None,
        }
    }

    fn note<T>(&self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        if let Err(err) = &result {
            let code = match store_error_retry_cause(err) {
                Some(RetryCause::Transient) => TRANSIENT,
                Some(RetryCause::ClockSkew) => CLOCK_SKEW,
                None => NOT_RETRIED,
            };
            self.last_failure.store(code, Ordering::SeqCst);
        }
        result
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for LastErrorStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.note(self.inner.put(key, data, opts).await)
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.note(self.inner.get(key, range).await)
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.note(self.inner.get_pinned(key, range, pin).await)
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.note(self.inner.get_with_pin(key, range).await)
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.note(self.inner.pin_of(key).await)
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.note(self.inner.put_multipart(key).await)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.note(self.inner.head(key).await)
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.note(self.inner.list(prefix, page).await)
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.note(self.inner.list_after(prefix, start_after, page).await)
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.note(self.inner.list_delimited(prefix).await)
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.note(self.inner.delete(key).await)
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

/// The context startup attaches to a failed [`bootstrap`] in `mode`. A refused
/// access names the grant or start order that fixes it, chosen by which request
/// was refused and by whether this mode's role may create `sys/gc` (ADR-0055
/// section 4: only the Maintain and Admin roles may); every other failure keeps
/// the generic context.
pub fn bootstrap_failure_context(err: &GcConfigError, mode: Mode) -> String {
    match err {
        GcConfigError::AccessDenied {
            op: GcAccessOp::Read,
            ..
        } => "this process's object-store credential was refused GetObject on sys/gc. The \
             gateway, query and maintain templates each grant an s3:ListBucket naming sys/gc, \
             so under them an absent sys/gc reads as missing rather than refused, and \
             a refused read means this credential lacks GetObject on sys/gc, or kms:Decrypt \
             on the bucket's default key if the bucket uses SSE-KMS, and restarting will not \
             help until that grant is fixed. If the bucket's policies predate those list \
             grants, S3 refuses a GET of a key that does not exist rather than reporting it \
             missing, so on a fresh bucket this can instead mean sys/gc has not been created \
             yet: apply the current deploy/iam templates, or run `ravel-cli gc-config set` \
             under the Admin credential, with a protection horizon and grace equal to the \
             maintain process's --gc-protection-horizon and --gc-grace (its defaults when \
             unset) and a max query duration and max flush lifetime matching the \
             --gc-max-query-duration and --gc-max-flush-lifetime the processes run with, \
             then restart this process"
            .to_string(),
        GcConfigError::AccessDenied {
            op: GcAccessOp::Create,
            ..
        } => match mode {
            Mode::Gateway | Mode::Query => "sys/gc does not exist yet and this process's \
                 object-store credential was refused its create. Under per-role credentials \
                 only the Maintain and Admin roles create sys/gc: start the maintain process \
                 first, or run `ravel-cli gc-config set` under the Admin credential with a \
                 protection horizon and grace equal to the maintain process's \
                 --gc-protection-horizon and --gc-grace (its defaults when unset) and a max \
                 query duration and max flush lifetime matching the --gc-max-query-duration \
                 and --gc-max-flush-lifetime the processes run with, then restart this \
                 process. Under a shared credential the same refusal means this \
                 credential lacks PutObject on sys/gc or kms:GenerateDataKey on the bucket's \
                 default key"
                .to_string(),
            Mode::Maintain => "sys/gc does not exist yet and this process's object-store \
                 credential was refused its create. The Maintain role needs PutObject on \
                 sys/gc, which deploy/iam/maintain.json grants, plus kms:GenerateDataKey on the \
                 bucket's default key if the bucket uses SSE-KMS, which the shipped templates \
                 grant only on the tenant key"
                .to_string(),
            Mode::All => "sys/gc does not exist yet and this process's object-store \
                 credential was refused its create. An all-in-one process runs under one \
                 credential for every role, and that credential lacks PutObject on sys/gc, or \
                 kms:GenerateDataKey on the bucket's default key if the bucket uses SSE-KMS"
                .to_string(),
        },
        _ => "failed to bootstrap or read the durable GC config (sys/gc)".to_string(),
    }
}

/// Maintain-mode check: the running compactor's horizon and grace must EQUAL the
/// stored values (ADR-0050 section 4).
pub fn validate_maintain(
    stored: &GcConfigValues,
    compactor: &CompactorConfig,
) -> Result<(), GcConfigError> {
    ravel_maintain::validate_maintain(stored, compactor.protection_horizon_ns, compactor.grace_ns)
}

/// Query-mode check: the engine deadline must be `<=` the stored
/// `max_query_duration_ns` (ADR-0050 section 4).
pub fn validate_query(stored: &GcConfigValues, deadline: Duration) -> Result<(), GcConfigError> {
    ravel_maintain::validate_query_deadline(stored, duration_to_ns(deadline))
}

/// Query-mode check: the HEAD cache TTL the query process's catalog runs on
/// must be `<=` the stored `head_cache_ttl_ns` (ADR-1133 decision 4). On a
/// version 1 `sys/gc` the stored value is the compiled default.
pub fn validate_query_head_cache_ttl(
    stored: &GcConfigValues,
    catalog_config: &ravel_catalog::CatalogConfig,
) -> Result<(), GcConfigError> {
    ravel_maintain::validate_query_head_cache_ttl(stored, catalog_config.head_cache_ttl_ns)
}

/// Flight SQL check: the ticket-TTL ceiling must be `<=` the stored
/// `protection_horizon_ns` minus `grace_ns` (ADR-0050 section 4). The server
/// sources the ceiling from `sys/gc` (see [`flight_ceiling`]), so this passes by
/// construction and stands as the fail-closed guard against a hand-set ceiling.
pub fn validate_flight(stored: &GcConfigValues, ceiling: Duration) -> Result<(), GcConfigError> {
    ravel_maintain::validate_flight_ceiling(stored, duration_to_ns(ceiling))
}

/// The Flight ticket-TTL ceiling this deployment must use, sourced from the
/// durable `sys/gc` rather than a hardcoded default: `protection_horizon -
/// grace`. Wiring this into the Flight service is what makes the ticket ceiling
/// track the single durable GC authority (the flight_ticket.rs "the minting
/// caller ... owns the ceiling" note), instead of the conservative 24 h default
/// that predates `sys/gc`.
pub fn flight_ceiling(stored: &GcConfigValues) -> Duration {
    Duration::from_nanos(u64::try_from(stored.flight_ceiling_ns()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn denied(op: GcAccessOp) -> GcConfigError {
        GcConfigError::AccessDenied {
            op,
            detail: "sys/gc: AccessDenied".into(),
        }
    }

    #[test]
    fn refused_create_in_gateway_or_query_names_the_start_order_fix() {
        for mode in [Mode::Gateway, Mode::Query] {
            let msg = bootstrap_failure_context(&denied(GcAccessOp::Create), mode);
            assert!(msg.contains("start the maintain process first"), "{msg}");
            assert!(
                msg.contains("`ravel-cli gc-config set` under the Admin credential"),
                "{msg}"
            );
            assert!(
                msg.contains(
                    "equal to the maintain process's --gc-protection-horizon and --gc-grace"
                ),
                "{msg}"
            );
            assert!(
                msg.contains("only the Maintain and Admin roles create sys/gc"),
                "{msg}"
            );
            assert!(
                msg.contains(
                    "Under a shared credential the same refusal means this credential lacks \
                     PutObject on sys/gc or kms:GenerateDataKey"
                ),
                "{msg}"
            );
        }
    }

    #[test]
    fn refused_create_in_maintain_names_the_maintain_grant() {
        let msg = bootstrap_failure_context(&denied(GcAccessOp::Create), Mode::Maintain);
        assert!(
            msg.contains("The Maintain role needs PutObject on sys/gc"),
            "{msg}"
        );
        assert!(msg.contains("kms:GenerateDataKey"), "{msg}");
        assert!(
            msg.contains("the shipped templates grant only on the tenant key"),
            "{msg}"
        );
        assert!(
            !msg.to_lowercase().contains("start the maintain process"),
            "{msg}"
        );
        assert!(!msg.contains("gc-config set"), "{msg}");
    }

    #[test]
    fn refused_create_in_all_names_the_shared_credential() {
        let msg = bootstrap_failure_context(&denied(GcAccessOp::Create), Mode::All);
        assert!(
            msg.contains("one credential for every role, and that credential lacks PutObject"),
            "{msg}"
        );
        assert!(msg.contains("kms:GenerateDataKey"), "{msg}");
        assert!(!msg.contains("Maintain role"), "{msg}");
        assert!(!msg.contains("maintain.json"), "{msg}");
        assert!(
            !msg.to_lowercase().contains("start the maintain process"),
            "{msg}"
        );
        assert!(!msg.contains("gc-config set"), "{msg}");
    }

    #[test]
    fn refused_read_names_the_read_grant_first_and_the_predating_policy_case_in_every_mode() {
        for mode in [Mode::All, Mode::Gateway, Mode::Query, Mode::Maintain] {
            let msg = bootstrap_failure_context(&denied(GcAccessOp::Read), mode);
            assert!(msg.contains("refused GetObject on sys/gc"), "{msg}");
            let read_grant = msg
                .find("a refused read means this credential lacks GetObject on sys/gc")
                .unwrap_or_else(|| panic!("{msg}"));
            let predating = msg
                .find("If the bucket's policies predate those list grants")
                .unwrap_or_else(|| panic!("{msg}"));
            assert!(read_grant < predating, "{msg}");
            assert!(
                msg.contains("refuses a GET of a key that does not exist"),
                "{msg}"
            );
            assert!(!msg.contains("grant none covering sys/gc"), "{msg}");
            assert!(
                msg.contains("run `ravel-cli gc-config set` under the Admin credential"),
                "{msg}"
            );
            assert!(
                msg.contains(
                    "equal to the maintain process's --gc-protection-horizon and --gc-grace"
                ),
                "{msg}"
            );
            assert!(msg.contains("kms:Decrypt"), "{msg}");
            assert!(
                msg.contains("restarting will not help until that grant is fixed"),
                "{msg}"
            );
            assert!(
                !msg.to_lowercase().contains("start the maintain process"),
                "{msg}"
            );
        }
    }

    #[test]
    fn other_bootstrap_failures_keep_the_generic_context() {
        let msg = bootstrap_failure_context(&GcConfigError::Store("timeout".into()), Mode::Gateway);
        assert_eq!(
            msg,
            "failed to bootstrap or read the durable GC config (sys/gc)"
        );
    }

    mod startup {
        #![allow(clippy::expect_used)]

        use ravel_object_store::fault::{FaultKind, FaultStore, Op};
        use ravel_object_store::memory::MemoryStore;

        use super::super::*;
        use crate::startup_retry::test_support::*;

        const GC_KEY: &str = "sys/gc";

        fn faulted(plan: ravel_object_store::fault::FaultPlan) -> Arc<FaultStore<MemoryStore>> {
            Arc::new(FaultStore::new(MemoryStore::new(), plan))
        }

        #[tokio::test]
        async fn retries_a_transient_read_then_starts() {
            let clock = Arc::new(ManualClock::default());
            let store = faulted(transient_then_ok(Op::Get, GC_KEY, 3));
            let values = bootstrap_at_startup(store.clone(), 1, Mode::All, &retry_on(clock))
                .await
                .expect("starts once the store answers");
            assert_eq!(
                values.protection_horizon_ns,
                GcConfigValues::maintain_defaults().protection_horizon_ns
            );
            assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 3);
            assert_eq!(store.sequence_progress(0), 4);
        }

        /// `GcConfigError::Store` carries text only; the recording store still
        /// sees the throttled answer by type and the read retries.
        #[tokio::test]
        async fn retries_a_throttled_read_then_starts() {
            let clock = Arc::new(ManualClock::default());
            let store = faulted(fault_then_ok(
                Op::Get,
                GC_KEY,
                ravel_object_store::fault::ScriptedFault::Throttled {
                    retry_after_ms: 1000,
                },
                2,
            ));
            bootstrap_at_startup(store.clone(), 1, Mode::All, &retry_on(clock.clone()))
                .await
                .expect("starts once the store answers");
            assert_eq!(store.fault_count(Op::Get, FaultKind::Throttled), 2);
            assert_eq!(store.sequence_progress(0), 3);
            assert_eq!(clock.sleeps().len(), 2);
        }

        #[tokio::test]
        async fn retries_a_transient_create_then_starts() {
            let clock = Arc::new(ManualClock::default());
            let store = faulted(transient_then_ok(Op::Put, GC_KEY, 2));
            bootstrap_at_startup(store.clone(), 1, Mode::Maintain, &retry_on(clock))
                .await
                .expect("starts once the create lands");
            assert_eq!(store.fault_count(Op::Put, FaultKind::Transient), 2);
            assert_eq!(store.sequence_progress(0), 3);
        }

        /// A create that landed but answered `Transient` is read back by the
        /// retry; nothing is written a second time.
        #[tokio::test]
        async fn reads_back_an_object_whose_create_ack_was_lost() {
            let clock = Arc::new(ManualClock::default());
            let store = faulted(ack_lost_then_ok(GC_KEY));
            bootstrap_at_startup(store.clone(), 1, Mode::Maintain, &retry_on(clock.clone()))
                .await
                .expect("the retry finds the landed object");
            assert_eq!(store.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
            assert_eq!(store.sequence_progress(0), 1);
            assert_eq!(clock.sleeps().len(), 1);
        }

        #[tokio::test]
        async fn transient_past_the_deadline_refuses_naming_the_site() {
            let clock = Arc::new(ManualClock::default());
            let store = faulted(always_transient(Op::Get, GC_KEY));
            let err = bootstrap_at_startup(store.clone(), 1, Mode::All, &retry_on(clock.clone()))
                .await
                .expect_err("refuses once the deadline passes");
            let attempts = store.fault_count(Op::Get, FaultKind::Transient);
            assert_deadline_refusal(&err, SITE_GC_CONFIG, attempts);
            assert!(clock.virtual_now() < DEADLINE);
            assert!(
                format!("{err:#}")
                    .contains("failed to bootstrap or read the durable GC config (sys/gc)")
            );
        }

        #[tokio::test]
        async fn non_transient_error_refuses_on_the_first_attempt() {
            let clock = Arc::new(ManualClock::default());
            let store = faulted(always_permanent(Op::Get, GC_KEY));
            let err = bootstrap_at_startup(store.clone(), 1, Mode::All, &retry_on(clock.clone()))
                .await
                .expect_err("refuses at once");
            assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
            assert_eq!(
                err.to_string(),
                "failed to bootstrap or read the durable GC config (sys/gc)"
            );
            assert!(clock.sleeps().is_empty());
        }

        #[tokio::test]
        async fn retries_a_clock_skew_refusal_then_starts() {
            let clock = Arc::new(ManualClock::default());
            let store = Arc::new(ScriptedRefusals::then_ok(
                Arc::new(MemoryStore::new()),
                GC_KEY,
                [clock_skewed(), clock_skewed()],
            ));
            bootstrap_at_startup(store.clone(), 1, Mode::All, &retry_on(clock.clone()))
                .await
                .expect("starts once the store accepts the request clock");
            assert_eq!(store.gets(), 3);
            assert_eq!(clock.sleeps().len(), 2);
        }

        #[tokio::test]
        async fn clock_skew_past_the_deadline_refuses_naming_time_sync() {
            let clock = Arc::new(ManualClock::default());
            let store = Arc::new(ScriptedRefusals::always(
                Arc::new(MemoryStore::new()),
                GC_KEY,
                clock_skewed,
            ));
            let err = bootstrap_at_startup(store.clone(), 1, Mode::All, &retry_on(clock.clone()))
                .await
                .expect_err("refuses once the deadline passes");
            assert_clock_skew_deadline_refusal(&err, SITE_GC_CONFIG, store.gets());
            let chain = format!("{err:#}");
            assert!(chain.contains(CLOCK_SKEW_CONTEXT), "{chain}");
            assert!(!chain.contains("lacks GetObject"), "{chain}");
            assert!(!chain.contains("restarting will not help"), "{chain}");
        }

        /// An `AccessDenied` is a missing grant whatever its text says.
        #[tokio::test]
        async fn access_denied_naming_the_clock_skew_code_refuses_at_once() {
            let clock = Arc::new(ManualClock::default());
            let store = Arc::new(ScriptedRefusals::always(
                Arc::new(MemoryStore::new()),
                GC_KEY,
                denied_naming_clock_skew,
            ));
            let err = bootstrap_at_startup(store.clone(), 1, Mode::All, &retry_on(clock.clone()))
                .await
                .expect_err("refuses at once");
            assert_eq!(store.gets(), 1);
            assert!(clock.sleeps().is_empty());
            let chain = format!("{err:#}");
            assert!(!chain.contains(CLOCK_SKEW_CONTEXT), "{chain}");
            assert!(chain.contains("lacks GetObject on sys/gc"), "{chain}");
        }

        /// A transient failure on an earlier call does not make a later
        /// non-transient one retryable: the attempt is judged by the call
        /// that failed it.
        #[tokio::test]
        async fn a_permanent_create_after_a_transient_read_refuses_at_once() {
            let clock = Arc::new(ManualClock::default());
            let plan = transient_then_ok(Op::Get, GC_KEY, 1).with_rule(
                ravel_object_store::fault::Rule::new(
                    Op::Put,
                    ravel_object_store::fault::ScriptedFault::Permanent("bad request".into()),
                )
                .with_key_contains(GC_KEY),
            );
            let store = faulted(plan);
            bootstrap_at_startup(store.clone(), 1, Mode::All, &retry_on(clock.clone()))
                .await
                .expect_err("refuses on the permanent create");
            assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
            assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 1);
            assert_eq!(clock.sleeps().len(), 1);
        }
    }
}
