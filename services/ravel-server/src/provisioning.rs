//! Server-side wiring for the durable `shard_count` provisioning record
//! (ADR-0050 section 5, EC5). The record itself and the validate-or-adopt
//! decision live in `ravel_catalog::provisioning`; this module wires that one
//! shared function into the two consumers the server owns directly:
//!
//! - the ingest first-write path ([`ProvisioningRecordWriter`], threaded into
//!   every `IngestState` like the recovery-manifest writer), which pins the
//!   configured `shard_count` in the record as a tenant's data first lands, and
//! - the startup static-tenant check ([`validate_static_provisioning`]), which
//!   refuses to start only when a statically-known tenant's record is
//!   unreadable, has a structurally invalid generation history
//!   ([`ProvisioningError::CorruptGenerations`]), or when the configured value
//!   would hide data. In `query` mode the check lists committed data only
//!   (the commit prefix) and never adopts; it still refuses when the configured
//!   `shard_count` would hide committed data. A decodable record with a valid
//!   history whose recorded `shard_count` differs from the live `--shards`
//!   default is tolerated: routing uses the record's own generation history
//!   (ADR-0082).
//!
//! The catalog resolve consumer is wired in `ravel_catalog` itself
//! (`Catalog::with_provisioning_enforcement`); the maintain per-tenant loop is
//! wired in [`crate::maintain`]. All three go through
//! [`ravel_catalog::validate_or_adopt`].
//!
//! Fresh-deployment safety: a brand-new tenant with no
//! prior writes and no provisioning record must never fail startup. The startup
//! check uses [`AbsentPolicy::AdoptIfData`] in every mode but `query`, and
//! [`AbsentPolicy::RefuseIfCommittedDataHidden`] in `query`
//! ([`static_absent_policy`]); both return
//! [`ProvisioningCheck::FreshNoData`] for a (tenant, signal) with no record and
//! no data, so an operator-managed cluster that starts with zero data and
//! configured tenant tokens passes through cleanly; only data a lower
//! `shard_count` would hide, an unreadable record, or a record with a
//! structurally invalid generation history, refuses. A decodable record with
//! a valid history whose recorded `shard_count` differs from the live default
//! is tolerated (ADR-0082).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use ravel_catalog::compat::{CompatRefusal, CompatView};
use ravel_catalog::{
    AbsentPolicy, ProvisioningCheck, ProvisioningError, validate_or_adopt, validate_or_adopt_fenced,
};
use ravel_ingest::ReadBackoff;
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash, TenantId};

use crate::config::Mode;

/// The signals the server ingests and maintains, and therefore provisions a
/// `shard_count` record for. Mirrors `crate::maintain::MAINTAINED_SIGNALS` and
/// `crate::fold::FOLD_SIGNALS`; the startup check iterates every statically
/// known tenant across exactly these.
pub const PROVISIONED_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// Count of dynamic-tenant first-touch hard provisioning failures: an
/// unreadable record (`UnsupportedVersion`, `CorruptRecord`, `Decode`) or
/// pre-ADR data a lower value would hide (`AdoptionWouldHideData`), rendered at
/// `/metrics` as `ravel_provisioning_shard_count_mismatch_total`. A recorded
/// `shard_count` that merely differs from the live default is no longer a
/// failure (ADR-0082): that drift is tolerated and counted separately by
/// `ravel_catalog::shard_count_drift_count`. Process-global with a single
/// source and no labels, mirroring [`crate::tenancy::v1_unkeyed_adoption_count`].
static SHARD_COUNT_MISMATCHES: AtomicU64 = AtomicU64::new(0);

/// The dynamic-mismatch count for the `/metrics` renderer.
pub fn shard_count_mismatch_count() -> u64 {
    SHARD_COUNT_MISMATCHES.load(Ordering::Relaxed)
}

/// Whether a provisioning failure must fail the touch that raised it rather
/// than be logged-and-ignored. A `Store` error is the one fail-open case: it is
/// usually transient and retry-friendly, so a store blip does not fail the
/// request at this step. It is not always transient (a store that serves the
/// record with a failing checksum reports one on every read), which is why
/// the ingest router, not this check, refuses the tenant's writes while the
/// record cannot be read. A
/// recorded `shard_count` that differs from the live default is no longer a
/// failure at all (ADR-0082): it is tolerated and never reaches here. Every
/// remaining variant means the tenant's true `shard_count` is unknown or
/// adopting the live value would hide data -- an `AdoptionWouldHideData`
/// refusal, an unreadable record (`UnsupportedVersion`, `CorruptRecord`,
/// `Decode`), or a decodable record whose generation history fails its
/// structural invariants (`CorruptGenerations`) -- whose recorded
/// `shard_count` cannot be trusted. Proceeding with ingest under an unknown
/// `shard_count` is exactly the silent-data-hiding this record exists to
/// prevent (ADR-0050 section 5), so those all fail hard: the version guard
/// refuses "rather than misread a future record format", and a fail-open here
/// would defeat that guard's own stated purpose. `CorruptGenerations` must fail
/// hard for the same reason: `Catalog::enforce_provisioning_once` rejects the
/// identical record via the same `validate_or_adopt` call, so failing open
/// here would let the ingest router route a write the catalog refuses to
/// resolve.
fn is_hard_failure(err: &ProvisioningError) -> bool {
    matches!(
        err,
        ProvisioningError::AdoptionWouldHideData { .. }
            | ProvisioningError::UnsupportedVersion { .. }
            | ProvisioningError::CorruptRecord { .. }
            | ProvisioningError::Decode { .. }
            | ProvisioningError::CorruptGenerations { .. }
    )
}

/// A copy of a hard provisioning failure, for [`ProvisioningRecordWriter`] to
/// return again inside its read backoff window. `None` for any other variant.
/// `ProvisioningError` is not `Clone` (its `Store` variant carries a
/// `StoreError`), so the hard variants are rebuilt field by field.
fn copy_hard_failure(err: &ProvisioningError) -> Option<ProvisioningError> {
    Some(match err {
        ProvisioningError::AdoptionWouldHideData {
            tenant_hash,
            signal,
            configured,
            observed_shard,
        } => ProvisioningError::AdoptionWouldHideData {
            tenant_hash: tenant_hash.clone(),
            signal,
            configured: *configured,
            observed_shard: *observed_shard,
        },
        ProvisioningError::UnsupportedVersion { key, got, ceiling } => {
            ProvisioningError::UnsupportedVersion {
                key: key.clone(),
                got: *got,
                ceiling: *ceiling,
            }
        }
        ProvisioningError::CorruptRecord {
            key,
            field,
            expected,
            actual,
        } => ProvisioningError::CorruptRecord {
            key: key.clone(),
            field,
            expected: expected.clone(),
            actual: actual.clone(),
        },
        ProvisioningError::Decode { key, source } => ProvisioningError::Decode {
            key: key.clone(),
            source: source.clone(),
        },
        ProvisioningError::CorruptGenerations { key, defect } => {
            ProvisioningError::CorruptGenerations {
                key: key.clone(),
                defect: *defect,
            }
        }
        _ => return None,
    })
}

/// Why [`ProvisioningRecordWriter::ensure`] failed a first touch.
#[derive(Debug, thiserror::Error)]
pub enum FirstTouchError {
    /// A hard provisioning failure: a `shard_count` that would hide data, or
    /// a record whose true `shard_count` cannot be read. Not retryable.
    #[error(transparent)]
    Provisioning(#[from] ProvisioningError),
    /// The process's `sys/compat` view is stale, so the epoch a record
    /// created now must be born fenced at is unknown (ADR-4233 decision 4).
    /// Raised only when the record is absent; nothing is written. Retryable:
    /// it clears on the next successful refresh.
    #[error("first touch of {signal:?} refused, retry later: {refusal}")]
    CompatViewStale {
        signal: Signal,
        refusal: CompatRefusal,
    },
    /// `sys/compat` bounds every writer's publish lifetime below this
    /// process's ingest `max_flush_lifetime` (ADR-4233 decision 7), so the
    /// ingest router refuses the write at admission; the first touch refuses
    /// it first, rather than create a record for a write that is then
    /// refused. Raised only when the record is absent; nothing is written.
    /// Retryable, as the router's own refusal is.
    #[error(
        "first touch of {signal:?} refused, retry later: max_flush_lifetime is \
         {max_flush_lifetime_ns} ns, above the {max_publish_lifetime_ns} ns \
         max_publish_lifetime_ns in sys/compat (ADR-4233 decision 7)"
    )]
    PublishLifetimeAboveCompat {
        signal: Signal,
        max_flush_lifetime_ns: i64,
        max_publish_lifetime_ns: i64,
    },
}

impl FirstTouchError {
    /// Whether the transports answer it as a retryable `sys/compat` refusal
    /// (HTTP 503, gRPC `UNAVAILABLE`), as the ingest router answers the same
    /// refusals at admission, rather than as a provisioning failure. A stale
    /// view clears with no operator; a lifetime bound clears once
    /// `sys/compat`'s bound is raised.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            FirstTouchError::CompatViewStale { .. }
                | FirstTouchError::PublishLifetimeAboveCompat { .. }
        )
    }
}

/// Why a first touch may read a provisioning record but not create one.
enum CreateRefusal {
    Stale(CompatRefusal),
    LifetimeAbove {
        max_flush_lifetime_ns: i64,
        max_publish_lifetime_ns: i64,
    },
}

/// The data family a first touch of `signal` precedes, under which a refusal
/// is counted in `ravel_compat_publish_refused_total`.
fn first_touch_family(signal: Signal) -> Option<&'static str> {
    match signal {
        Signal::Metrics => Some(ravel_catalog::compat::FAMILY_RSEG),
        Signal::Logs => Some(ravel_catalog::compat::FAMILY_RLOG),
        Signal::Spans => Some(ravel_catalog::compat::FAMILY_RSPAN),
        Signal::Profiles | Signal::Alerts | Signal::Audit => None,
    }
}

/// Increment the process-global mismatch counter if `err` is a hard provisioning
/// failure. Shared by the consumers that catch a provisioning error off the
/// ingest hot path (the maintain per-tenant loop), so an alert keyed on the
/// counter fires for a maintain-only mismatch too, not just an ingest one.
pub(crate) fn note_provisioning_failure(err: &ProvisioningError) {
    if is_hard_failure(err) {
        SHARD_COUNT_MISMATCHES.fetch_add(1, Ordering::Relaxed);
    }
}

/// Writes and validates each (tenant, signal)'s provisioning record on the
/// tenant's first write for that signal in this process (ADR-0050 section 5),
/// mirroring [`crate::tenancy::RecoveryManifestWriter`]. A per-process
/// in-memory set makes every write after the first a cheap set hit; the
/// `CreateIfAbsent` in [`validate_or_adopt`] handles the cross-process race.
///
/// Built once per process from the configured `shard_count` and shared across
/// the metrics, logs, spans, and remote-write ingest paths, so the "seen"
/// set is shared and one tenant's first write across any transport provisions
/// the record once.
///
/// A (tenant, signal) whose last check failed, on a store error or a hard
/// failure, is not checked again until its read backoff window passes (issue
/// #3653): 1 s after the first failure, doubling on each consecutive one up to
/// 30 s, cleared by a success. Inside the window a store error's check returns
/// `Ok` and a hard failure's returns the same typed error again.
pub struct ProvisioningRecordWriter {
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    seen: Mutex<HashSet<(TenantHash, Signal)>>,
    read_backoff: Mutex<ReadBackoff<(TenantHash, Signal)>>,
    /// The hard failure of each backed-off key whose last check failed hard.
    hard_failures: Mutex<HashMap<(TenantHash, Signal), ProvisioningError>>,
    /// The process's `sys/compat` view. Once it has seen an activation
    /// (`compat_epoch > 0`), a record this writer creates is born fenced at
    /// that epoch (ADR-4233 decision 4).
    compat: Option<CompatView>,
    /// Notified when a first touch is refused for a stale view, so the
    /// process's refresh loop re-reads `sys/compat` early.
    compat_refresh_wake: Option<Arc<tokio::sync::Notify>>,
    /// The ingest routers' `max_flush_lifetime`: a record is not created
    /// while `sys/compat` bounds publishes below it.
    max_flush_lifetime: Option<std::time::Duration>,
}

impl ProvisioningRecordWriter {
    pub fn new(store: Arc<dyn ObjectStoreBackend>, shard_count: u32) -> Self {
        ProvisioningRecordWriter {
            store,
            shard_count,
            seen: Mutex::new(HashSet::new()),
            read_backoff: Mutex::new(ReadBackoff::new()),
            hard_failures: Mutex::new(HashMap::new()),
            compat: None,
            compat_refresh_wake: None,
            max_flush_lifetime: None,
        }
    }

    /// Notify `wake` each time a first touch is refused for a stale view.
    pub fn with_compat_refresh_wake(mut self, wake: Arc<tokio::sync::Notify>) -> Self {
        self.compat_refresh_wake = Some(wake);
        self
    }

    /// Refuse to create a record while `sys/compat`'s
    /// `max_publish_lifetime_ns` is below `max_flush_lifetime`, the lifetime
    /// the ingest routers this writer provisions for are built with: their
    /// admission refuses the write that follows (ADR-4233 decision 7), so the
    /// first touch refuses it first, with
    /// [`FirstTouchError::PublishLifetimeAboveCompat`]. A present record is
    /// read and adopted as usual.
    pub fn with_max_flush_lifetime(mut self, max_flush_lifetime: std::time::Duration) -> Self {
        self.max_flush_lifetime = Some(max_flush_lifetime);
        self
    }

    /// Create records born fenced once `view` has seen an activation: the
    /// first touch of a new (tenant, signal) after an activation writes its
    /// record at version 4 with the view's `compat_epoch` and
    /// `compat_fenced_unix_ns` 0 (ADR-4233 decision 4, conditional
    /// publication). Before any activation, and with `None`, a first touch
    /// creates the version-3 record it always has. Under a stale view a
    /// first touch that would create the record is refused,
    /// [`FirstTouchError::CompatViewStale`]; one that finds it present is not.
    pub fn with_compat_view(mut self, view: Option<CompatView>) -> Self {
        self.compat = view;
        self
    }

    /// The epoch a record created at `now_ns` is born fenced at: the view's
    /// seen `compat_epoch`, `None` before any activation and without a view.
    /// The view must be fresh under [`CompatView::check_fresh`] with a bucket
    /// `compat_epoch` of 0, since a new record has none: a stale view may
    /// have missed an activation, and a record created unfenced after one
    /// lets a writer publish at the baseline past the activation. The
    /// record's `max_publish_lifetime_ns`, when set, must not be below
    /// [`Self::with_max_flush_lifetime`]'s lifetime.
    fn born_fenced_epoch(
        &self,
        now_ns: i64,
    ) -> Result<Option<std::num::NonZeroU64>, CreateRefusal> {
        let Some(view) = &self.compat else {
            return Ok(None);
        };
        view.check_fresh(now_ns, 0).map_err(CreateRefusal::Stale)?;
        if let (Some(lifetime), Some(record)) = (self.max_flush_lifetime, view.last_record()) {
            let max_flush_lifetime_ns = i64::try_from(lifetime.as_nanos()).unwrap_or(i64::MAX);
            let max_publish_lifetime_ns = record.max_publish_lifetime_ns;
            if max_publish_lifetime_ns > 0 && max_flush_lifetime_ns > max_publish_lifetime_ns {
                return Err(CreateRefusal::LifetimeAbove {
                    max_flush_lifetime_ns,
                    max_publish_lifetime_ns,
                });
            }
        }
        Ok(std::num::NonZeroU64::new(view.seen_epoch()))
    }

    /// Ensure the (tenant, signal) provisioning record on a first write.
    ///
    /// On a hard provisioning failure (the dynamic-tenant first-touch case,
    /// ADR-0050 section 5) -- a `shard_count` disagreement, or an unreadable
    /// record whose true `shard_count` cannot be trusted -- increments the
    /// process-global mismatch counter and returns the typed error so the caller
    /// fails that one request; the process is never taken down for a single
    /// dynamic tenant. The (tenant, signal) then backs off: inside the window
    /// every request fails with the same typed error without a store call, and
    /// the counter is not incremented again until a read fails. Only a store
    /// error is logged and treated as success at
    /// this step: it is not the tenant's fault, and the ingest router, which
    /// re-reads the record before routing, refuses the tenant's writes for as
    /// long as it cannot read it. After a store error the (tenant, signal) is
    /// not checked again until its read backoff window passes; inside the
    /// window this returns `Ok` without a store call, and the WARN is logged
    /// once per failed check. A genuine success (created, adopted, or matched)
    /// is cached in `seen` and clears the backoff.
    ///
    /// Under a stale `sys/compat` view the record is read but never created:
    /// a present record is validated and adopted as on a fresh view, and an
    /// absent one is refused with [`FirstTouchError::CompatViewStale`],
    /// counted in `ravel_compat_publish_refused_total` under the signal's data
    /// family, and wakes the refresh loop ([`Self::with_compat_refresh_wake`]).
    /// A record `sys/compat`'s publish bound forbids this process's ingest
    /// lifetime ([`Self::with_max_flush_lifetime`]) is read the same way and
    /// never created: an absent one is refused with
    /// [`FirstTouchError::PublishLifetimeAboveCompat`], counted under
    /// `publish_lifetime_above_compat`. Neither refusal caches anything or
    /// starts a backoff, so the next request checks again.
    pub async fn ensure(
        &self,
        tenant_hash: &TenantHash,
        signal: Signal,
        now_ns: i64,
    ) -> Result<(), FirstTouchError> {
        let key = (*tenant_hash, signal);
        if self.seen.lock().contains(&key) {
            return Ok(());
        }
        if self
            .read_backoff
            .lock()
            .blocked_until(&key, now_ns)
            .is_some()
        {
            // Only hard failures are remembered, and every hard failure
            // copies, so a backed-off key with a remembered error never reads.
            match self.hard_failures.lock().get(&key) {
                Some(err) => {
                    if let Some(err) = copy_hard_failure(err) {
                        return Err(err.into());
                    }
                }
                None => return Ok(()),
            }
        }
        // A stale view leaves only the epoch of a record created now unknown,
        // and a lifetime bound refuses only the write that follows, so both
        // read without creating: a present record is validated and adopted
        // as on a fresh view, and an absent one is refused.
        let (absent_policy, born_fenced, refused) = match self.born_fenced_epoch(now_ns) {
            Ok(epoch) => (AbsentPolicy::CreateFromConfig, epoch, None),
            Err(refusal) => (AbsentPolicy::CheckOnly, None, Some(refusal)),
        };
        match validate_or_adopt_fenced(
            self.store.as_ref(),
            tenant_hash,
            signal,
            self.shard_count,
            now_ns,
            absent_policy,
            born_fenced,
        )
        .await
        {
            Ok(check) => {
                if let Some(refused) = refused
                    && !matches!(check, ProvisioningCheck::RecordPresent { .. })
                {
                    return Err(self.refuse_creation(tenant_hash, signal, refused));
                }
                self.seen.lock().insert(key);
                self.read_backoff.lock().clear(&key);
                self.hard_failures.lock().remove(&key);
                Ok(())
            }
            Err(err) if is_hard_failure(&err) => {
                SHARD_COUNT_MISMATCHES.fetch_add(1, Ordering::Relaxed);
                self.read_backoff.lock().record_failure(key, now_ns);
                if let Some(copy) = copy_hard_failure(&err) {
                    self.hard_failures.lock().insert(key, copy);
                }
                Err(err.into())
            }
            Err(err) => {
                let delay_ns = {
                    let mut read_backoff = self.read_backoff.lock();
                    // A store error inside a hard failure's window came from a
                    // concurrent read; the window keeps failing closed.
                    if read_backoff.blocked_until(&key, now_ns).is_none() {
                        self.hard_failures.lock().remove(&key);
                    }
                    read_backoff.record_failure(key, now_ns)
                };
                tracing::warn!(
                    %err,
                    tenant_hash = %tenant_hash.to_hex(),
                    signal = signal.key_prefix(),
                    retry_in_ms = delay_ns / 1_000_000,
                    "provisioning record check hit a store error; this request is not refused \
                     here, but the ingest router refuses this tenant's writes for this signal \
                     while it cannot read the record"
                );
                Ok(())
            }
        }
    }

    /// The error of a first touch that found no record and may not create
    /// one: counted in `ravel_compat_publish_refused_total` under the
    /// signal's data family, logged, and for a stale view, the refresh loop
    /// woken.
    fn refuse_creation(
        &self,
        tenant_hash: &TenantHash,
        signal: Signal,
        refused: CreateRefusal,
    ) -> FirstTouchError {
        match refused {
            CreateRefusal::Stale(refusal) => {
                if let Some(family) = first_touch_family(signal) {
                    crate::compat::record_publish_refusal(family, &refusal);
                }
                if let Some(wake) = &self.compat_refresh_wake {
                    wake.notify_one();
                }
                tracing::warn!(
                    %refusal,
                    tenant_hash = %tenant_hash.to_hex(),
                    signal = signal.key_prefix(),
                    "provisioning record creation refused: the sys/compat view is stale, \
                     so the epoch a new record is born fenced at is unknown; retried on \
                     the next request"
                );
                FirstTouchError::CompatViewStale { signal, refusal }
            }
            CreateRefusal::LifetimeAbove {
                max_flush_lifetime_ns,
                max_publish_lifetime_ns,
            } => {
                if let Some(family) = first_touch_family(signal) {
                    crate::compat::record_publish_refusal_reason(
                        family,
                        crate::compat::CompatRefusalReason::PublishLifetimeAboveCompat,
                    );
                }
                tracing::warn!(
                    tenant_hash = %tenant_hash.to_hex(),
                    signal = signal.key_prefix(),
                    max_flush_lifetime_ns,
                    max_publish_lifetime_ns,
                    "provisioning record creation refused: sys/compat bounds publishes \
                     below this process's ingest max_flush_lifetime, so the ingest router \
                     refuses the write (ADR-4233 decision 7)"
                );
                FirstTouchError::PublishLifetimeAboveCompat {
                    signal,
                    max_flush_lifetime_ns,
                    max_publish_lifetime_ns,
                }
            }
        }
    }
}

/// Best-effort provisioning-record write on an ingest request's tenant, called
/// from every server ingest handler before the write (ADR-0050 section 5), the
/// provisioning analogue of [`crate::tenancy::ensure_recovery_manifest`].
///
/// A no-op when `writer` is `None`. Returns the typed error on a hard
/// provisioning failure (a `shard_count` mismatch or an unreadable record), so
/// the handler fails that one request, and the retryable
/// [`FirstTouchError::CompatViewStale`] when the record is absent and a stale
/// `sys/compat` view leaves the epoch it would be born fenced at unknown; a
/// store error is logged inside
/// [`ProvisioningRecordWriter::ensure`] and reported as success here.
pub async fn ensure_provisioning_record(
    writer: &Option<Arc<ProvisioningRecordWriter>>,
    tenant: &TenantId,
    signal: Signal,
    now_ns: i64,
) -> Result<(), FirstTouchError> {
    match writer {
        Some(writer) => writer.ensure(&tenant.hash(), signal, now_ns).await,
        None => Ok(()),
    }
}

/// What the startup static-tenant check may do about an absent record in
/// `mode`. A `query` process serves committed data only: it validates a
/// present record and never adopts, and for an absent record it lists the
/// commit prefix and refuses when its configured count would hide committed
/// data, so the Query credential needs no provisioning write and no `l0/`
/// listing for this check (ADR-0055, prov write conditions amendment). Every
/// mode that runs ingest or maintenance adopts pre-ADR data, as those paths do
/// at runtime.
pub fn static_absent_policy(mode: Mode) -> AbsentPolicy {
    match mode {
        Mode::Query => AbsentPolicy::RefuseIfCommittedDataHidden,
        Mode::All | Mode::Gateway | Mode::Maintain => AbsentPolicy::AdoptIfData,
    }
}

/// Validate the configured `shard_count` for every statically-known tenant at
/// startup (ADR-0050 section 5), refusing to start on the first unreadable
/// record or adoption that would hide data.
/// The static tenant set is the union of `--tenant-token` (or
/// `--tenant-token-file`) and `--maintain-tenant` (already hashed), so an
/// OIDC/mTLS deployment with no static tenants (an empty set) has nothing to
/// validate here and every dynamic tenant is validated at first touch instead.
///
/// Runs under [`static_absent_policy`]`(mode)`. Under
/// [`AbsentPolicy::AdoptIfData`] a (tenant, signal) with no record and no data
/// passes through without refusing (the fresh-deployment case), a (tenant,
/// signal) with pre-ADR data is adopted once, and only an unreadable record or
/// pre-ADR data a lower value would hide refuses. Under
/// [`AbsentPolicy::RefuseIfCommittedDataHidden`] (Query mode) an absent record
/// lists only the commit prefix, refuses when committed data sits on a shard
/// index at or above `shard_count`, and otherwise passes without a write; a
/// present record is validated the same way. Either way a brand-new tenant
/// with no prior writes and no provisioning record does not fail startup.
pub async fn validate_static_provisioning(
    store: &dyn ObjectStoreBackend,
    static_tenants: &[TenantHash],
    shard_count: u32,
    mode: Mode,
    now_ns: i64,
) -> Result<(), ProvisioningError> {
    let absent_policy = static_absent_policy(mode);
    for tenant_hash in static_tenants {
        for signal in PROVISIONED_SIGNALS {
            let check = validate_or_adopt(
                store,
                tenant_hash,
                signal,
                shard_count,
                now_ns,
                absent_policy,
            )
            .await?;
            if matches!(check, ProvisioningCheck::Written) {
                tracing::info!(
                    tenant_hash = %tenant_hash.to_hex(),
                    signal = signal.key_prefix(),
                    shard_count,
                    "adopted pre-ADR data into a shard_count provisioning record at startup"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use prost::Message;
    use ravel_catalog::provisioning_key;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        GetRange, InstrumentedStore, ObjectStoreBackend, PutOptions, StoreError,
    };
    use ravel_proto::sys::v1 as sysproto;

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    async fn seed_record(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
        signal: Signal,
        shard_count: u32,
    ) {
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: match signal {
                Signal::Metrics => sysproto::Signal::Metrics,
                Signal::Logs => sysproto::Signal::Logs,
                Signal::Spans => sysproto::Signal::Spans,
                _ => sysproto::Signal::Unspecified,
            } as i32,
            shard_count,
            created_unix_ns: 1,
            generations: Vec::new(),
            format_floors: Vec::new(),
            compat_epoch: 0,
            compat_fenced_unix_ns: 0,
        };
        store
            .put(
                &provisioning_key(th, signal),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed record");
    }

    /// The fresh-deployment guarantee, made a test: a brand-new tenant with no prior
    /// writes and no provisioning record does not fail startup. This is the
    /// exact fresh-`ravel-operator`-cluster shape (configured tenant tokens,
    /// zero data).
    #[tokio::test]
    async fn fresh_tenant_with_no_prior_data_starts_cleanly() {
        for mode in ALL_MODES {
            let store = store();
            let statics = [TenantId::new("acme").hash(), TenantId::new("globex").hash()];
            validate_static_provisioning(store.as_ref(), &statics, 4, mode, 1_000)
                .await
                .unwrap_or_else(|e| {
                    panic!("{mode:?}: a fresh tenant with no record and no data must not fail startup: {e}")
                });
            // And nothing was written for a signal with no data.
            let got = store
                .get(
                    &provisioning_key(&statics[0], Signal::Metrics),
                    GetRange::Full,
                )
                .await;
            assert!(
                matches!(got, Err(StoreError::NotFound)),
                "{mode:?}: no record written"
            );
        }
    }

    /// ADR-0082: a statically-known tenant whose recorded `shard_count` (4)
    /// differs from the live `--shards` default (2) no longer refuses startup.
    /// Before this change `validate_static_provisioning` returned
    /// `ProvisioningError::ShardCountMismatch` and the `.expect(...)` below
    /// panicked; now the drift is tolerated and startup proceeds.
    #[tokio::test]
    async fn static_tenant_drift_does_not_refuse_startup() {
        for mode in ALL_MODES {
            let store = store();
            let th = TenantId::new("acme").hash();
            seed_record(store.as_ref(), &th, Signal::Metrics, 4).await;
            validate_static_provisioning(store.as_ref(), &[th], 2, mode, 1_000)
                .await
                .unwrap_or_else(|e| {
                    panic!("{mode:?}: a recorded shard_count above the live default is tolerated (ADR-0082): {e}")
                });
        }
    }

    const ALL_MODES: [Mode; 4] = [Mode::All, Mode::Gateway, Mode::Query, Mode::Maintain];

    /// A store holding pre-ADR metrics data for `th` on shard 0 (an L0 shard
    /// directory and a commit shard directory) and no provisioning record,
    /// wrapped in a counter so a test can assert which operations ran.
    async fn store_with_unprovisioned_data(th: &TenantHash) -> InstrumentedStore<MemoryStore> {
        let inner = MemoryStore::new();
        let hex = th.to_hex();
        for key in [
            format!("t/{hex}/m/l0/0000/100/seg"),
            format!("t/{hex}/m/c/0000/100/rec.cmt"),
        ] {
            inner
                .put(&key, vec![1].into(), PutOptions::default())
                .await
                .expect("seed shard data");
        }
        InstrumentedStore::new(inner)
    }

    /// The startup policy per mode: only a `query` process checks without
    /// adopting. Every other mode runs ingest or maintenance and adopts.
    #[test]
    fn only_query_mode_checks_without_adopting() {
        for mode in ALL_MODES {
            let expected = if mode == Mode::Query {
                AbsentPolicy::RefuseIfCommittedDataHidden
            } else {
                AbsentPolicy::AdoptIfData
            };
            assert_eq!(static_absent_policy(mode), expected, "{mode:?}");
        }
    }

    /// A store holding unprovisioned metrics data for `th` at `keys` (relative
    /// to `t/<hex>/m/`) with no provisioning record, shaped like the Query
    /// credential: any listing of an `l0/` prefix fails, as the Query template
    /// grants no `l0/` listing. Wrapped in a counter so a test can assert which
    /// operations ran.
    async fn query_store_with_unprovisioned_data(
        th: &TenantHash,
        keys: &[&str],
    ) -> InstrumentedStore<FaultStore<MemoryStore>> {
        let inner = MemoryStore::new();
        let hex = th.to_hex();
        for key in keys {
            inner
                .put(
                    &format!("t/{hex}/m/{key}"),
                    vec![1].into(),
                    PutOptions::default(),
                )
                .await
                .expect("seed shard data");
        }
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Permanent("query credential: no l0 listing".into()),
            )
            .with_key_contains("/l0/"),
        );
        InstrumentedStore::new(FaultStore::new(inner, plan))
    }

    /// A `query` process never adopts at startup: over a tenant with committed
    /// data in range and no record it lists only the commit prefix (one
    /// delimited listing per provisioned signal), never `l0/`, writes no record,
    /// and starts. `l0/` holds data on shard 9, above the configured count of
    /// 4, which the adopt path would refuse over; Query does not serve it.
    /// Fails with `Mode::Query => AbsentPolicy::AdoptIfData` in
    /// [`static_absent_policy`]: the adopt path lists `l0/`.
    #[tokio::test]
    async fn query_mode_startup_over_in_range_committed_data_lists_commits_and_writes_nothing() {
        let th = TenantId::new("acme").hash();
        let store = query_store_with_unprovisioned_data(
            &th,
            &[
                "c/0000/100/rec.cmt",
                "c/0003/100/rec.cmt",
                "l0/0009/100/seg",
            ],
        )
        .await;
        let before = store.metrics().snapshot();
        validate_static_provisioning(&store, &[th], 4, Mode::Query, 1_000)
            .await
            .expect("query-mode startup over in-range committed data must pass");
        let after = store.metrics().snapshot();
        assert_eq!(
            store.inner().fault_count(Op::List, FaultKind::Permanent),
            0,
            "query-mode startup must not list l0/"
        );
        assert_eq!(
            after.list_calls() - before.list_calls(),
            PROVISIONED_SIGNALS.len() as u64,
            "one commit-prefix listing per provisioned signal"
        );
        assert_eq!(
            after.put.calls - before.put.calls,
            0,
            "query-mode startup must not write"
        );
        assert_eq!(
            after.get.calls - before.get.calls,
            PROVISIONED_SIGNALS.len() as u64,
            "one record read per provisioned signal"
        );
        let got = store
            .get(&provisioning_key(&th, Signal::Metrics), GetRange::Full)
            .await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "no record written"
        );
    }

    /// ADR-0050 section 5 in Query mode: committed data on shard 4 with
    /// `--shards 4` and no record would be read through the implicit generation
    /// 0 of 4 shards, leaving shard 4 out of every query, so startup refuses.
    /// Fails with `Mode::Query => AbsentPolicy::CheckOnly` in
    /// [`static_absent_policy`]: that policy returns before any listing.
    #[tokio::test]
    async fn query_mode_startup_refuses_when_shards_would_hide_committed_data() {
        let th = TenantId::new("acme").hash();
        let store =
            query_store_with_unprovisioned_data(&th, &["c/0000/100/rec.cmt", "c/0004/100/rec.cmt"])
                .await;
        let before = store.metrics().snapshot();
        let err = validate_static_provisioning(&store, &[th], 4, Mode::Query, 1_000)
            .await
            .expect_err("committed data on shard 4 is hidden by --shards 4");
        assert!(
            matches!(
                err,
                ProvisioningError::AdoptionWouldHideData {
                    configured: 4,
                    observed_shard: 4,
                    ..
                }
            ),
            "got {err}"
        );
        let after = store.metrics().snapshot();
        assert_eq!(after.put.calls - before.put.calls, 0, "no write");
        assert_eq!(
            store.inner().fault_count(Op::List, FaultKind::Permanent),
            0,
            "no l0/ listing"
        );
    }

    /// Query mode still validates a present record: one that does not belong to
    /// this (tenant, signal) refuses startup exactly as in every other mode.
    /// (A record whose `shard_count` merely differs from `--shards` is drift,
    /// tolerated in every mode under ADR-0082:
    /// `static_tenant_drift_does_not_refuse_startup`.)
    #[tokio::test]
    async fn query_mode_startup_still_refuses_an_unreadable_record() {
        for mode in ALL_MODES {
            let store = store();
            let th = TenantId::new("acme").hash();
            let other = TenantId::new("globex").hash();
            let record = sysproto::ProvisioningRecord {
                format_version: 1,
                tenant_hash: other.0.to_vec(),
                signal: sysproto::Signal::Metrics as i32,
                shard_count: 4,
                created_unix_ns: 1,
                generations: Vec::new(),
                format_floors: Vec::new(),
                compat_epoch: 0,
                compat_fenced_unix_ns: 0,
            };
            store
                .put(
                    &provisioning_key(&th, Signal::Metrics),
                    record.encode_to_vec().into(),
                    PutOptions::default(),
                )
                .await
                .expect("seed record");
            let err = validate_static_provisioning(store.as_ref(), &[th], 4, mode, 1_000)
                .await
                .expect_err("a record naming another tenant must refuse startup");
            assert!(
                matches!(
                    err,
                    ProvisioningError::CorruptRecord {
                        field: "tenant_hash",
                        ..
                    }
                ),
                "{mode:?}: got {err}"
            );
        }
    }

    /// Every mode that runs ingest or maintenance still adopts pre-ADR data at
    /// startup: it lists the shard directories and creates the record.
    #[tokio::test]
    async fn adopting_modes_create_the_record_over_unprovisioned_data() {
        for mode in [Mode::All, Mode::Gateway, Mode::Maintain] {
            let th = TenantId::new("acme").hash();
            let store = store_with_unprovisioned_data(&th).await;
            let before = store.metrics().snapshot();
            validate_static_provisioning(&store, &[th], 4, mode, 1_000)
                .await
                .unwrap_or_else(|e| panic!("{mode:?}: adopt must succeed: {e}"));
            let after = store.metrics().snapshot();
            assert!(
                after.list_calls() > before.list_calls(),
                "{mode:?}: the adopt path lists the shard directories"
            );
            assert_eq!(
                after.put.calls - before.put.calls,
                1,
                "{mode:?}: exactly the metrics record is created"
            );
            let got = store
                .get(&provisioning_key(&th, Signal::Metrics), GetRange::Full)
                .await
                .unwrap_or_else(|e| panic!("{mode:?}: record adopted: {e}"));
            let record = sysproto::ProvisioningRecord::decode(got.data.as_ref())
                .expect("adopted record decodes");
            assert_eq!(record.shard_count, 4, "{mode:?}");
        }
    }

    /// ADR-0082 on the dynamic path: a first-touch drift (recorded 4, live
    /// default 2) succeeds rather than failing that one request. Before this
    /// change `ensure` returned `ProvisioningError::ShardCountMismatch` and the
    /// `.expect(...)` below panicked.
    #[tokio::test]
    async fn dynamic_first_touch_drift_succeeds() {
        let store = store();
        let drifted = TenantId::new("acme").hash();
        seed_record(store.as_ref(), &drifted, Signal::Metrics, 4).await;
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 2));

        writer
            .ensure(&drifted, Signal::Metrics, 1_000)
            .await
            .expect("a drifted dynamic tenant is tolerated on first touch (ADR-0082)");

        // A different tenant is unaffected: it provisions cleanly.
        let other = TenantId::new("globex").hash();
        writer
            .ensure(&other, Signal::Metrics, 1_000)
            .await
            .expect("an unrelated tenant provisions cleanly");
    }

    /// ADR-0082 maintain gate: the per-tenant loop's provisioning gate goes
    /// through `validate_or_adopt(.., AdoptIfData)`, exactly as
    /// [`crate::maintain`] calls it. A drifted record (recorded 4, live default
    /// 2) reports the tenant eligible (returns `RecordPresent`) rather than a
    /// hard failure that would skip its maintain tick. Before this change it
    /// returned `ProvisioningError::ShardCountMismatch` and the `.expect(...)`
    /// below panicked.
    #[tokio::test]
    async fn maintain_gate_tolerates_drift_reports_eligible() {
        let store = store();
        let th = TenantId::new("acme").hash();
        seed_record(store.as_ref(), &th, Signal::Metrics, 4).await;
        let check = validate_or_adopt(
            store.as_ref(),
            &th,
            Signal::Metrics,
            2,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect("the maintain gate tolerates drift and reports the tenant eligible (ADR-0082)");
        assert_eq!(
            check,
            ProvisioningCheck::RecordPresent {
                recorded_shard_count: 4
            }
        );
    }

    /// A future-version record on a dynamic tenant's
    /// first-touch path fails that request with a typed `UnsupportedVersion`
    /// error and increments the mismatch counter, rather than being logged and
    /// letting ingest proceed under an unknown shard_count (ADR-0050 section 5:
    /// the version guard refuses rather than misread a future record format).
    #[tokio::test]
    async fn dynamic_first_touch_unsupported_version_fails_request() {
        let store = store();
        let th = TenantId::new("acme").hash();
        let record = sysproto::ProvisioningRecord {
            format_version: 999,
            tenant_hash: th.0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 4,
            created_unix_ns: 1,
            generations: Vec::new(),
            format_floors: Vec::new(),
            compat_epoch: 0,
            compat_fenced_unix_ns: 0,
        };
        store
            .put(
                &provisioning_key(&th, Signal::Metrics),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed future-version record");
        let before = shard_count_mismatch_count();
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 4));
        let err = writer
            .ensure(&th, Signal::Metrics, 1_000)
            .await
            .expect_err("a future-version record must fail the request, not fail-open");
        assert!(
            matches!(
                err,
                FirstTouchError::Provisioning(ProvisioningError::UnsupportedVersion { .. })
            ),
            "got: {err}"
        );
        // Race-safe monotonic check (see the note in
        // `dynamic_first_touch_mismatch_fails_one_request_only`): the shared
        // global counter only grows, so a strict increase proves it fired here.
        assert!(
            shard_count_mismatch_count() > before,
            "a hard provisioning failure must increment the counter"
        );
    }

    /// An undecodable (corrupt) record on a dynamic
    /// tenant's first-touch path fails that request with a typed `Decode` error
    /// rather than being swallowed and letting ingest proceed under an unknown
    /// shard_count.
    #[tokio::test]
    async fn dynamic_first_touch_corrupt_record_fails_request() {
        let store = store();
        let th = TenantId::new("acme").hash();
        store
            .put(
                &provisioning_key(&th, Signal::Metrics),
                vec![0xFF, 0xFF, 0xFF, 0x07].into(),
                PutOptions::default(),
            )
            .await
            .expect("seed garbage record");
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 4));
        let err = writer
            .ensure(&th, Signal::Metrics, 1_000)
            .await
            .expect_err("a corrupt record must fail the request, not fail-open");
        assert!(
            matches!(
                err,
                FirstTouchError::Provisioning(ProvisioningError::Decode { .. })
            ),
            "got: {err}"
        );
    }

    /// A record with a corrupt generation history on a dynamic tenant's
    /// first-touch path must fail that request with a typed
    /// `CorruptGenerations` error and increment the mismatch counter, rather
    /// than being logged and letting ingest proceed and the router cache a
    /// tenant `Catalog::enforce_provisioning_once` would reject the same
    /// record for (split-brain between the ingest router's and the catalog's
    /// view of the record). Before the fix, `is_hard_failure` (this file, the
    /// `matches!` in `is_hard_failure` above) omitted
    /// `ProvisioningError::CorruptGenerations`, so `ensure` fell into the
    /// fail-open `Err(err) => ... Ok(())` arm and this `expect_err` panicked.
    #[tokio::test]
    async fn dynamic_first_touch_corrupt_generations_fails_request() {
        let store = store();
        let th = TenantId::new("acme").hash();
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 4,
            created_unix_ns: 1,
            // ScalarMismatch: generations[0].shard_count (8) != scalar shard_count (4).
            generations: vec![sysproto::ShardGeneration {
                generation: 0,
                shard_count: 8,
                activation_hour: 0,
                appended_unix_ns: 0,
            }],
            format_floors: Vec::new(),
            compat_epoch: 0,
            compat_fenced_unix_ns: 0,
        };
        store
            .put(
                &provisioning_key(&th, Signal::Metrics),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed corrupt-generations record");
        let before = shard_count_mismatch_count();
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 4));
        let err = writer
            .ensure(&th, Signal::Metrics, 1_000)
            .await
            .expect_err("a corrupt generation history must fail the request, not fail-open");
        assert!(
            matches!(
                err,
                FirstTouchError::Provisioning(ProvisioningError::CorruptGenerations { .. })
            ),
            "got: {err}"
        );
        assert!(
            shard_count_mismatch_count() > before,
            "a hard provisioning failure must increment the counter"
        );
    }

    /// A tenant's first write pins the record from config; a second write is a
    /// cheap seen-set hit.
    #[tokio::test]
    async fn first_write_pins_record_from_config() {
        let store = store();
        let th = TenantId::new("acme").hash();
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 4));
        writer
            .ensure(&th, Signal::Metrics, 1_000)
            .await
            .expect("first write");
        writer
            .ensure(&th, Signal::Metrics, 2_000)
            .await
            .expect("second is a no-op");
        let record = store
            .get(&provisioning_key(&th, Signal::Metrics), GetRange::Full)
            .await
            .expect("record present");
        let decoded = sysproto::ProvisioningRecord::decode(record.data.as_ref()).expect("decode");
        assert_eq!(decoded.shard_count, 4);
    }

    /// Issue #3653: after `ensure` fails open on a store error, requests for
    /// the same (tenant, signal) inside the backoff window return `Ok` with no
    /// store read; one after the window reads again, and a success clears the
    /// backoff and fills `seen`. Another signal of the same tenant is not
    /// backed off.
    #[tokio::test]
    async fn ensure_backs_off_after_a_store_error() {
        use ravel_object_store::fault::Sequence;

        const NS_PER_SEC: i64 = 1_000_000_000;
        let th = TenantId::new("acme").hash();
        let key = provisioning_key(&th, Signal::Metrics);
        let mut sequence = Sequence::new(Op::Get)
            .with_key_contains(key)
            .then_fault(ScriptedFault::Transient("unreadable".into()))
            .then_fault(ScriptedFault::Transient("unreadable".into()));
        for _ in 0..8 {
            sequence = sequence.then_passthrough();
        }
        let store = Arc::new(FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_sequence(sequence),
        ));
        let writer = ProvisioningRecordWriter::new(store.clone(), 4);
        let t0 = 1_000 * NS_PER_SEC;

        for _ in 0..5 {
            writer
                .ensure(&th, Signal::Metrics, t0)
                .await
                .expect("a store error fails open");
        }
        assert_eq!(store.sequence_progress(0), 1, "one read in the window");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
        writer
            .ensure(&th, Signal::Metrics, t0 + NS_PER_SEC - 1)
            .await
            .expect("fails open");
        assert_eq!(store.sequence_progress(0), 1);

        let t1 = t0 + NS_PER_SEC;
        for _ in 0..5 {
            writer
                .ensure(&th, Signal::Metrics, t1)
                .await
                .expect("fails open");
        }
        assert_eq!(store.sequence_progress(0), 2, "one read after the window");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 2);

        let t2 = t1 + 2 * NS_PER_SEC;
        writer
            .ensure(&th, Signal::Metrics, t2)
            .await
            .expect("the record reads back");
        assert_eq!(store.sequence_progress(0), 3);
        assert!(writer.seen.lock().contains(&(th, Signal::Metrics)));
        assert!(writer.read_backoff.lock().is_empty());
        writer.ensure(&th, Signal::Metrics, t2).await.expect("seen");
        assert_eq!(store.sequence_progress(0), 3, "a seen tenant reads nothing");
    }

    /// Issue #3653: a present-but-corrupt record fails every request, but
    /// requests for the same (tenant, signal) inside the backoff window fail
    /// with the same typed error and issue no GET. One after the window reads
    /// again, and a read that succeeds clears the remembered error.
    #[tokio::test]
    async fn ensure_backs_off_after_a_hard_failure() {
        use ravel_object_store::fault::Sequence;

        const NS_PER_SEC: i64 = 1_000_000_000;
        let th = TenantId::new("acme").hash();
        let key = provisioning_key(&th, Signal::Metrics);
        let mut sequence = Sequence::new(Op::Get).with_key_contains(key.clone());
        for _ in 0..16 {
            sequence = sequence.then_passthrough();
        }
        let store = Arc::new(FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_sequence(sequence),
        ));
        store
            .put(
                &key,
                vec![0xFF, 0xFF, 0xFF, 0x07].into(),
                PutOptions::default(),
            )
            .await
            .expect("seed garbage record");
        let writer = ProvisioningRecordWriter::new(store.clone(), 4);
        let t0 = 1_000 * NS_PER_SEC;

        for _ in 0..5 {
            let err = writer
                .ensure(&th, Signal::Metrics, t0)
                .await
                .expect_err("a corrupt record fails the request");
            assert!(
                matches!(
                    err,
                    FirstTouchError::Provisioning(ProvisioningError::Decode { .. })
                ),
                "got: {err}"
            );
        }
        assert_eq!(store.sequence_progress(0), 1, "one GET in the window");
        let err = writer
            .ensure(&th, Signal::Metrics, t0 + NS_PER_SEC - 1)
            .await
            .expect_err("still failing inside the window");
        assert!(
            matches!(
                err,
                FirstTouchError::Provisioning(ProvisioningError::Decode { .. })
            ),
            "got: {err}"
        );
        assert_eq!(store.sequence_progress(0), 1);

        let t1 = t0 + NS_PER_SEC;
        for _ in 0..3 {
            let err = writer
                .ensure(&th, Signal::Metrics, t1)
                .await
                .expect_err("a corrupt record fails the request");
            assert!(
                matches!(
                    err,
                    FirstTouchError::Provisioning(ProvisioningError::Decode { .. })
                ),
                "got: {err}"
            );
        }
        assert_eq!(store.sequence_progress(0), 2, "one GET after the window");

        seed_record(store.as_ref(), &th, Signal::Metrics, 4).await;
        let t2 = t1 + 2 * NS_PER_SEC;
        writer
            .ensure(&th, Signal::Metrics, t2)
            .await
            .expect("the repaired record reads back");
        assert_eq!(store.sequence_progress(0), 3);
        assert!(writer.seen.lock().contains(&(th, Signal::Metrics)));
        assert!(writer.read_backoff.lock().is_empty());
        assert!(writer.hard_failures.lock().is_empty());
        writer.ensure(&th, Signal::Metrics, t2).await.expect("seen");
        assert_eq!(store.sequence_progress(0), 3, "a seen tenant reads nothing");
    }

    struct FixedCompatClock(i64);

    impl ravel_catalog::compat::CompatClock for FixedCompatClock {
        fn now_unix_ns(&self) -> i64 {
            self.0
        }
    }

    /// A view over `store`, refreshed once, after `sys/compat` was created at
    /// store-wide epoch `epoch` (`None`: no record at all).
    async fn compat_view_at(
        store: &Arc<dyn ObjectStoreBackend>,
        epoch: Option<u64>,
    ) -> ravel_catalog::compat::CompatView {
        let keys = ravel_catalog::compat::CompatKeys::from_file_keys(&[[0x21; 32]]).expect("key");
        if let Some(epoch) = epoch {
            ravel_catalog::compat::create_compat(
                store.as_ref(),
                &keys,
                sysproto::CompatRecord {
                    compat_epoch: epoch,
                    ..Default::default()
                },
            )
            .await
            .expect("create sys/compat");
        }
        let view = ravel_catalog::compat::CompatView::new(
            Arc::clone(store),
            keys,
            Arc::new(FixedCompatClock(1_000)),
        );
        view.refresh().await.expect("refresh");
        view
    }

    async fn stored_record(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
    ) -> sysproto::ProvisioningRecord {
        let got = store
            .get(&provisioning_key(th, Signal::Logs), GetRange::Full)
            .await
            .expect("record written");
        sysproto::ProvisioningRecord::decode(got.data.as_ref()).expect("decodes")
    }

    /// After an activation (the view has seen `compat_epoch` 3), a first touch
    /// creates the record born fenced: version 4, `compat_epoch` 3,
    /// `compat_fenced_unix_ns` 0 (ADR-4233 decision 4).
    #[tokio::test]
    async fn first_touch_after_an_activation_is_born_fenced() {
        let store = store();
        let view = compat_view_at(&store, Some(3)).await;
        let th = TenantId::new("born-fenced").hash();
        let writer = ProvisioningRecordWriter::new(store.clone(), 4).with_compat_view(Some(view));
        writer
            .ensure(&th, Signal::Logs, 1_000)
            .await
            .expect("first touch");
        let record = stored_record(store.as_ref(), &th).await;
        assert_eq!(
            (
                record.format_version,
                record.compat_epoch,
                record.compat_fenced_unix_ns,
                record.shard_count,
            ),
            (ravel_catalog::PROVISIONING_FENCED_FORMAT_VERSION, 3, 0, 4)
        );
    }

    /// Before any activation, with a record at epoch 0, no record at all, or
    /// no view, a first touch creates the version-3 record it always has.
    #[tokio::test]
    async fn first_touch_before_an_activation_is_unfenced() {
        for epoch in [Some(0), None] {
            let store = store();
            let view = compat_view_at(&store, epoch).await;
            let th = TenantId::new("unfenced").hash();
            let writer =
                ProvisioningRecordWriter::new(store.clone(), 4).with_compat_view(Some(view));
            writer
                .ensure(&th, Signal::Logs, 1_000)
                .await
                .expect("first touch");
            let record = stored_record(store.as_ref(), &th).await;
            assert_eq!(
                (record.format_version, record.compat_epoch),
                (3, 0),
                "sys/compat epoch {epoch:?}"
            );
        }
        let store = store();
        let th = TenantId::new("no-view").hash();
        let writer = ProvisioningRecordWriter::new(store.clone(), 4).with_compat_view(None);
        writer
            .ensure(&th, Signal::Logs, 1_000)
            .await
            .expect("first touch");
        let record = stored_record(store.as_ref(), &th).await;
        assert_eq!((record.format_version, record.compat_epoch), (3, 0));
    }

    struct SteppedCompatClock(std::sync::atomic::AtomicI64);

    impl ravel_catalog::compat::CompatClock for SteppedCompatClock {
        fn now_unix_ns(&self) -> i64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn spans_view_stale_refusals() -> u64 {
        crate::compat::publish_refusals_snapshot()
            .into_iter()
            .find(|(family, reason, _)| {
                family.as_str() == ravel_catalog::compat::FAMILY_RSPAN
                    && *reason == crate::compat::CompatRefusalReason::ViewStale
            })
            .map(|(_, _, count)| count)
            .expect("the rspan view_stale series always renders")
    }

    async fn spans_record(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
    ) -> Result<sysproto::ProvisioningRecord, StoreError> {
        let got = store
            .get(&provisioning_key(th, Signal::Spans), GetRange::Full)
            .await?;
        Ok(sysproto::ProvisioningRecord::decode(got.data.as_ref()).expect("decodes"))
    }

    /// A view that read an activation (epoch 3) and then went a horizon
    /// without a read, while a second activation it never read moved the
    /// epoch to 4, refuses the first touch: typed, retryable, counted under
    /// `rspan`/`view_stale`, with no record written, nothing cached and no
    /// backoff, so the touch succeeds the moment a read makes the view fresh
    /// and the record is born fenced at the epoch that read saw. A view no
    /// read has succeeded for refuses the same way after one 5xx.
    #[tokio::test]
    async fn first_touch_refuses_under_a_stale_view_after_an_activation() {
        use ravel_catalog::compat::{COMPAT_KEY, COMPAT_VIEW_HORIZON_NS, CompatKeys};
        const T0: i64 = 1_700_000_000_000_000_000;
        let store = store();
        let keys = CompatKeys::from_file_keys(&[[0x21; 32]]).expect("key");
        ravel_catalog::compat::create_compat(
            store.as_ref(),
            &keys,
            sysproto::CompatRecord {
                compat_epoch: 3,
                ..Default::default()
            },
        )
        .await
        .expect("create sys/compat");
        let clock = Arc::new(SteppedCompatClock(std::sync::atomic::AtomicI64::new(T0)));
        let view = CompatView::new(Arc::clone(&store), keys.clone(), clock.clone());
        view.refresh().await.expect("refresh at T0");
        ravel_catalog::compat::update_compat(store.as_ref(), &keys, |record| {
            record.compat_epoch = 4;
            Ok(())
        })
        .await
        .expect("an activation the view does not read");

        let th = TenantId::new("stale-first-touch").hash();
        let wake = Arc::new(tokio::sync::Notify::new());
        let writer = ProvisioningRecordWriter::new(store.clone(), 4)
            .with_compat_view(Some(view.clone()))
            .with_compat_refresh_wake(Arc::clone(&wake));
        let woken = || futures::FutureExt::now_or_never(wake.notified()).is_some();
        let stale_at = T0 + COMPAT_VIEW_HORIZON_NS;
        let before = spans_view_stale_refusals();
        for attempt in 0..2 {
            let err = writer
                .ensure(&th, Signal::Spans, stale_at)
                .await
                .expect_err("a first touch under a stale view is refused");
            assert!(err.is_retryable(), "attempt {attempt}: {err}");
            match &err {
                FirstTouchError::CompatViewStale { signal, refusal } => assert_eq!(
                    (*signal, refusal),
                    (
                        Signal::Spans,
                        &CompatRefusal::ViewStale {
                            last_read_unix_ns: Some(T0),
                            now_unix_ns: stale_at,
                        }
                    ),
                    "attempt {attempt}"
                ),
                other => panic!("attempt {attempt}: expected CompatViewStale, got {other:?}"),
            }
            assert!(woken(), "attempt {attempt}: the refusal wakes the refresh");
            assert!(!woken(), "attempt {attempt}: one wake per refusal");
            assert!(
                matches!(
                    crate::traces_ingest::SpanIngestRequestError::from(err),
                    crate::traces_ingest::SpanIngestRequestError::CompatRefused(_)
                ),
                "attempt {attempt}: the transports map it to 503 / UNAVAILABLE"
            );
        }
        assert_eq!(spans_view_stale_refusals() - before, 2);
        assert!(
            matches!(
                spans_record(store.as_ref(), &th).await,
                Err(StoreError::NotFound)
            ),
            "a refused first touch writes no record"
        );
        assert!(!writer.seen.lock().contains(&(th, Signal::Spans)));
        assert!(writer.read_backoff.lock().is_empty());

        clock.0.store(stale_at, Ordering::Relaxed);
        view.refresh().await.expect("a fresh read");
        writer
            .ensure(&th, Signal::Spans, stale_at)
            .await
            .expect("the first touch once the view is fresh");
        let record = spans_record(store.as_ref(), &th)
            .await
            .expect("record written");
        assert_eq!(
            (
                record.format_version,
                record.compat_epoch,
                record.compat_fenced_unix_ns
            ),
            (ravel_catalog::PROVISIONING_FENCED_FORMAT_VERSION, 4, 0)
        );

        let faults = Arc::new(FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::Transient("503".into()))
                    .with_key_contains(COMPAT_KEY),
            ),
        ));
        let faulted: Arc<dyn ObjectStoreBackend> = faults.clone();
        let blind = CompatView::new(Arc::clone(&faulted), keys, clock.clone());
        assert!(blind.refresh().await.is_err());
        let writer =
            ProvisioningRecordWriter::new(faulted.clone(), 4).with_compat_view(Some(blind));
        let err = writer
            .ensure(&th, Signal::Spans, stale_at)
            .await
            .expect_err("a never-read view after a 5xx is stale");
        assert!(
            matches!(
                &err,
                FirstTouchError::CompatViewStale {
                    refusal: CompatRefusal::ViewStale {
                        last_read_unix_ns: None,
                        ..
                    },
                    ..
                }
            ),
            "{err:?}"
        );
        assert_eq!(faults.fault_count(Op::Get, FaultKind::Transient), 1);
        assert!(matches!(
            spans_record(faulted.as_ref(), &th).await,
            Err(StoreError::NotFound)
        ));
    }

    fn logs_lifetime_refusals() -> u64 {
        crate::compat::publish_refusals_snapshot()
            .into_iter()
            .find(|(family, reason, _)| {
                family.as_str() == ravel_catalog::compat::FAMILY_RLOG
                    && *reason == crate::compat::CompatRefusalReason::PublishLifetimeAboveCompat
            })
            .map(|(_, _, count)| count)
            .expect("the rlog publish_lifetime_above_compat series always renders")
    }

    /// #4362: on a store where `compat init` recorded a
    /// `max_publish_lifetime_ns` below the ingest routers' lifetime, with a
    /// fresh view, the router refuses the write at admission, so the first
    /// touch that precedes it refuses first and creates no record: typed,
    /// retryable (503 on the transports), counted under
    /// `rlog`/`publish_lifetime_above_compat`, nothing cached. A record that
    /// exists is adopted as usual, and a lifetime within the bound creates
    /// the record. A first touch that ignores the bound PUTs a record for a
    /// write the router then refuses.
    #[tokio::test]
    async fn first_touch_refuses_to_create_a_record_under_a_publish_bound_below_the_lifetime() {
        use ravel_catalog::compat::CompatKeys;
        const T0: i64 = 1_700_000_000_000_000_000;
        let store = store();
        let keys = CompatKeys::from_file_keys(&[[0x21; 32]]).expect("key");
        ravel_catalog::compat::create_compat(
            store.as_ref(),
            &keys,
            sysproto::CompatRecord {
                compat_epoch: 1,
                max_publish_lifetime_ns: 60_000_000_000,
                ..Default::default()
            },
        )
        .await
        .expect("compat init");
        let clock = Arc::new(SteppedCompatClock(std::sync::atomic::AtomicI64::new(T0)));
        let view = CompatView::new(Arc::clone(&store), keys, clock);
        view.refresh().await.expect("a fresh read");
        let writer = ProvisioningRecordWriter::new(store.clone(), 4)
            .with_compat_view(Some(view.clone()))
            .with_max_flush_lifetime(std::time::Duration::from_secs(3600));

        let th = TenantId::new("bounded-first-touch").hash();
        let before = logs_lifetime_refusals();
        let err = writer
            .ensure(&th, Signal::Logs, T0)
            .await
            .expect_err("the bound is below the lifetime");
        assert!(
            matches!(
                err,
                FirstTouchError::PublishLifetimeAboveCompat {
                    signal: Signal::Logs,
                    max_flush_lifetime_ns: 3_600_000_000_000,
                    max_publish_lifetime_ns: 60_000_000_000,
                }
            ),
            "{err:?}"
        );
        assert!(err.is_retryable());
        assert!(matches!(
            crate::logs_ingest::LogIngestRequestError::from(err),
            crate::logs_ingest::LogIngestRequestError::CompatRefused(_)
        ));
        assert_eq!(logs_lifetime_refusals() - before, 1);
        assert!(
            matches!(
                store
                    .get(&provisioning_key(&th, Signal::Logs), GetRange::Full)
                    .await,
                Err(StoreError::NotFound)
            ),
            "a refused first touch writes no record"
        );
        assert!(!writer.seen.lock().contains(&(th, Signal::Logs)));
        assert!(writer.read_backoff.lock().is_empty());

        let existing = TenantId::new("bounded-existing").hash();
        seed_record(store.as_ref(), &existing, Signal::Logs, 4).await;
        writer
            .ensure(&existing, Signal::Logs, T0)
            .await
            .expect("an existing record is adopted");
        assert!(writer.seen.lock().contains(&(existing, Signal::Logs)));

        let within = ProvisioningRecordWriter::new(store.clone(), 4)
            .with_compat_view(Some(view))
            .with_max_flush_lifetime(std::time::Duration::from_secs(60));
        within
            .ensure(&th, Signal::Logs, T0)
            .await
            .expect("a lifetime at the bound creates the record");
        assert_eq!(stored_record(store.as_ref(), &th).await.compat_epoch, 1);
        assert_eq!(logs_lifetime_refusals() - before, 1);
    }

    /// A view that read an activation and then went a horizon without a read
    /// leaves unknown only the epoch a record created now is born fenced at.
    /// The first touch of a bucket whose record exists reads it, validates
    /// it and caches it, as on a fresh view. The touch that would create a
    /// record, for a fresh bucket or one holding pre-ADR data, is refused and
    /// writes nothing.
    #[tokio::test]
    async fn a_stale_view_accepts_an_existing_record_and_refuses_a_new_one() {
        use ravel_catalog::compat::{COMPAT_VIEW_HORIZON_NS, CompatKeys};
        const T0: i64 = 1_700_000_000_000_000_000;
        let store = store();
        let keys = CompatKeys::from_file_keys(&[[0x21; 32]]).expect("key");
        ravel_catalog::compat::create_compat(
            store.as_ref(),
            &keys,
            sysproto::CompatRecord {
                compat_epoch: 3,
                ..Default::default()
            },
        )
        .await
        .expect("create sys/compat");
        let clock = Arc::new(SteppedCompatClock(std::sync::atomic::AtomicI64::new(T0)));
        let view = CompatView::new(Arc::clone(&store), keys, clock);
        view.refresh().await.expect("refresh at T0");
        let stale_at = T0 + COMPAT_VIEW_HORIZON_NS;
        assert!(view.check_fresh(stale_at, 0).is_err(), "the view is stale");

        let existing = TenantId::new("stale-existing").hash();
        seed_record(store.as_ref(), &existing, Signal::Spans, 4).await;
        let fresh = TenantId::new("stale-fresh").hash();
        let pre_adr = TenantId::new("stale-pre-adr").hash();
        let hex = pre_adr.to_hex();
        for key in [
            format!("t/{hex}/m/l0/0000/100/seg"),
            format!("t/{hex}/m/c/0000/100/rec.cmt"),
        ] {
            store
                .put(&key, vec![1].into(), PutOptions::default())
                .await
                .expect("seed shard data");
        }

        let writer = ProvisioningRecordWriter::new(store.clone(), 4).with_compat_view(Some(view));
        writer
            .ensure(&existing, Signal::Spans, stale_at)
            .await
            .expect("an existing record is adopted under a stale view");
        assert!(writer.seen.lock().contains(&(existing, Signal::Spans)));
        let record = spans_record(store.as_ref(), &existing)
            .await
            .expect("record kept");
        assert_eq!((record.format_version, record.compat_epoch), (1, 0));

        for (th, signal) in [(fresh, Signal::Spans), (pre_adr, Signal::Metrics)] {
            let err = writer
                .ensure(&th, signal, stale_at)
                .await
                .expect_err("creating a record under a stale view is refused");
            assert!(
                matches!(
                    &err,
                    FirstTouchError::CompatViewStale {
                        refusal: CompatRefusal::ViewStale {
                            last_read_unix_ns: Some(T0),
                            ..
                        },
                        ..
                    }
                ),
                "{signal:?}: {err:?}"
            );
            assert!(
                matches!(
                    store
                        .get(&provisioning_key(&th, signal), GetRange::Full)
                        .await,
                    Err(StoreError::NotFound)
                ),
                "{signal:?}: a refused creation writes no record"
            );
            assert!(!writer.seen.lock().contains(&(th, signal)));
        }
        assert!(writer.read_backoff.lock().is_empty());
    }
}
