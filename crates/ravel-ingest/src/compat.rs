//! The `sys/compat` activation policy as ingest applies it (ADR-4233
//! decisions 2 to 4).
//!
//! An [`IngestCompatPolicy`] holds the process's [`CompatView`] and, per
//! [`FormatFamily`], the versions this build writes and the family's baseline.
//! A router given one (`with_compat_policy`) applies it at three points:
//!
//! - Admission refuses a write the cached view already says cannot publish
//!   ([`CompatRefusal::ActiveVersionNotWritable`],
//!   [`CompatRefusal::CompatRecordMissing`]) before anything is buffered. A
//!   stale view is not refused here: the flush decides at permit grant.
//! - Permit grant picks the version a flush encodes at
//!   ([`CompatView::write_policy`]) and refuses the flush closed when the view
//!   is stale, the version is not writable, or the router is gone and no
//!   cached provisioning record is left to ask with: no PUT, its rows
//!   dropped, its byte charges released, its strict waiters acked with a
//!   retryable [`CompatPublishRefusal`].
//! - Every data-object, commit-record and fence attempt, retries included,
//!   asks [`CompatView::write_policy`] again with the bucket's cached
//!   provisioning record, and is refused when it refuses (a stale view, under
//!   the rule the view applies) or answers a version other than the granted
//!   one. Before the first PUT of an object above the family's baseline, the
//!   bucket's provisioning record is fenced at the view's epoch
//!   ([`ravel_catalog::raise_compat_fence`]).
//!
//! The format-floor checks at admission and at flush open compare the floor
//! with the version the policy names for the family
//! ([`FormatFamily::writable_version_under`]); a grant at another version is
//! checked against the floor again.
//!
//! A router given `None`, or none at all, writes each family's `CURRENT`
//! version with none of these checks, exactly as before the policy existed.
//! Every refusal counts once under its [`CompatRefusalReason`] in the
//! signal's ingest metrics snapshot (`compat_publish_refused`), a field
//! `ravel-server` does not yet render on `/metrics` (#4287).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use ravel_catalog::compat::{CompatClock, CompatRefusal, CompatView};
use ravel_catalog::{CompatFenceOutcome, ProvisioningError, raise_compat_fence};
use ravel_commit::rng::RngSource;
use ravel_object_store::ObjectStoreBackend;
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{Signal, TenantHash};

use crate::clock::Clock;
use crate::format_floor::FormatFamily;

/// Attempts at the bucket fence before a lost CAS race fails the flush.
const FENCE_ATTEMPTS: u32 = 3;

/// The longest jittered wait between two fence attempts.
const FENCE_BACKOFF_MAX: Duration = Duration::from_millis(50);

/// The versions a build writes for one family and the family's baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FamilyVersions {
    writable: Vec<u16>,
    baseline: u16,
}

impl FamilyVersions {
    fn of(family: FormatFamily) -> Self {
        let (writable, baseline) = match family {
            FormatFamily::Rseg => (
                ravel_segment::SegmentVersion::WINDOW
                    .iter()
                    .map(|v| v.number())
                    .collect(),
                ravel_segment::SegmentVersion::CURRENT.number(),
            ),
            FormatFamily::Rlog => {
                let supported = ravel_logseg::footer::SUPPORTED_VERSIONS;
                (
                    (supported.oldest()..=supported.newest())
                        .filter(|v| supported.contains(*v))
                        .collect(),
                    ravel_logseg::footer::WriteVersion::CURRENT.number(),
                )
            }
            FormatFamily::Rspan => {
                let supported = ravel_rspan::footer::SUPPORTED_VERSIONS;
                (
                    (supported.oldest()..=supported.newest())
                        .filter(|v| supported.contains(*v))
                        .collect(),
                    ravel_rspan::footer::WriteVersion::CURRENT.number(),
                )
            }
        };
        FamilyVersions { writable, baseline }
    }
}

/// The family string `sys/compat` records `family` under.
fn compat_family(family: FormatFamily) -> &'static str {
    match family {
        FormatFamily::Rseg => ravel_catalog::compat::FAMILY_RSEG,
        FormatFamily::Rlog => ravel_catalog::compat::FAMILY_RLOG,
        FormatFamily::Rspan => ravel_catalog::compat::FAMILY_RSPAN,
    }
}

fn family_index(family: FormatFamily) -> usize {
    match family {
        FormatFamily::Rseg => 0,
        FormatFamily::Rlog => 1,
        FormatFamily::Rspan => 2,
    }
}

/// The process's activation policy handle: one [`CompatView`], refreshed by
/// its owner, and the versions this build writes per family. Shared by `Arc`
/// between every router of the process.
pub struct IngestCompatPolicy {
    view: CompatView,
    clock: Arc<dyn CompatClock>,
    families: [FamilyVersions; 3],
}

impl std::fmt::Debug for IngestCompatPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestCompatPolicy")
            .field("view", &self.view)
            .field("families", &self.families)
            .finish()
    }
}

/// A process whose `max_flush_lifetime` is longer than the publish bound
/// `sys/compat` holds every writer to (ADR-4233 decision 7) refuses to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "max_flush_lifetime is {max_flush_lifetime_ns} ns, above the {max_publish_lifetime_ns} ns \
     max_publish_lifetime_ns in sys/compat (ADR-4233 decision 7): refusing to start"
)]
pub struct PublishLifetimeAboveCompat {
    pub max_flush_lifetime_ns: i64,
    pub max_publish_lifetime_ns: i64,
}

impl IngestCompatPolicy {
    /// A policy over `view`, read at `clock`, writing each family's reader
    /// window (`SegmentVersion::WINDOW`, the RLOG and RSPAN
    /// `SUPPORTED_VERSIONS`) with each crate's `CURRENT` version as baseline.
    pub fn new(view: CompatView, clock: Arc<dyn CompatClock>) -> Self {
        IngestCompatPolicy {
            view,
            clock,
            families: FormatFamily::ALL.map(FamilyVersions::of),
        }
    }

    /// Replace one family's writable set and baseline, so a test can build a
    /// second writable version without the `format-drill` feature.
    #[cfg(test)]
    pub(crate) fn with_family_versions(
        mut self,
        family: FormatFamily,
        writable: Vec<u16>,
        baseline: u16,
    ) -> Self {
        self.families[family_index(family)] = FamilyVersions { writable, baseline };
        self
    }

    /// The view the policy answers from.
    pub fn view(&self) -> &CompatView {
        &self.view
    }

    /// The versions of `family` this build writes.
    pub fn writable(&self, family: FormatFamily) -> &[u16] {
        &self.families[family_index(family)].writable
    }

    /// `family`'s baseline version.
    pub fn baseline(&self, family: FormatFamily) -> u16 {
        self.families[family_index(family)].baseline
    }

    fn now_unix_ns(&self) -> i64 {
        self.clock.now_unix_ns()
    }

    /// The version a `family` object written now encodes at, or why none may
    /// be published. `bucket_compat_epoch` is the cached provisioning
    /// record's `compat_epoch`, `None` when no record is known.
    pub fn write_version(
        &self,
        family: FormatFamily,
        bucket_compat_epoch: Option<u64>,
    ) -> Result<u16, CompatRefusal> {
        let versions = &self.families[family_index(family)];
        let bucket = bucket_compat_epoch.map(|compat_epoch| sysproto::ProvisioningRecord {
            compat_epoch,
            ..Default::default()
        });
        self.view.write_policy(
            self.now_unix_ns(),
            compat_family(family),
            &versions.writable,
            versions.baseline,
            bucket.as_ref(),
        )
    }

    /// The version the view names for `family`: its active version when this
    /// build writes it, else the family's baseline. Unlike
    /// [`Self::write_version`] it does not refuse on a stale or missing view.
    pub fn named_version(&self, family: FormatFamily) -> u16 {
        let versions = &self.families[family_index(family)];
        self.view
            .seen_family(compat_family(family))
            .and_then(|seen| u16::try_from(seen.active_write_version).ok())
            .filter(|active| versions.writable.contains(active))
            .unwrap_or(versions.baseline)
    }

    /// The admission check: [`Self::write_version`], except that a stale
    /// view admits, because the flush re-asks at permit grant.
    pub fn admit(
        &self,
        family: FormatFamily,
        bucket_compat_epoch: Option<u64>,
    ) -> Result<(), CompatRefusal> {
        match self.write_version(family, bucket_compat_epoch) {
            Ok(_) | Err(CompatRefusal::ViewStale { .. }) => Ok(()),
            Err(refusal) => Err(refusal),
        }
    }

    /// Refuses a `max_flush_lifetime` above the record's
    /// `max_publish_lifetime_ns`. Passes when the view holds no record or the
    /// record's bound is zero (unset).
    pub fn check_publish_lifetime(
        &self,
        max_flush_lifetime: Duration,
    ) -> Result<(), PublishLifetimeAboveCompat> {
        let Some(record) = self.view.last_record() else {
            return Ok(());
        };
        let max_publish_lifetime_ns = record.max_publish_lifetime_ns;
        let max_flush_lifetime_ns =
            i64::try_from(max_flush_lifetime.as_nanos()).unwrap_or(i64::MAX);
        if max_publish_lifetime_ns > 0 && max_flush_lifetime_ns > max_publish_lifetime_ns {
            return Err(PublishLifetimeAboveCompat {
                max_flush_lifetime_ns,
                max_publish_lifetime_ns,
            });
        }
        Ok(())
    }
}

/// The policy a router's switch and its shard actors share. Shard actors hold
/// it directly, so a flush still knows whether a policy applies after its
/// router's switch is dropped.
#[derive(Debug, Clone, Default)]
pub(crate) struct CompatSlot(Arc<RwLock<Option<Arc<IngestCompatPolicy>>>>);

impl CompatSlot {
    pub(crate) fn set(&self, policy: Option<Arc<IngestCompatPolicy>>) {
        *self.0.write().unwrap_or_else(|p| p.into_inner()) = policy;
    }

    pub(crate) fn get(&self) -> Option<Arc<IngestCompatPolicy>> {
        self.0.read().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

/// The cached provisioning-record `compat_epoch` of each tenant's bucket, as
/// a router's switch holds it.
pub(crate) trait BucketCompat: Send + Sync {
    /// The cached record's `compat_epoch`; `None` when no record is cached.
    fn bucket_compat_epoch(&self, tenant: TenantHash) -> Option<u64>;
    /// Raise the cached epoch to `compat_epoch` after a fence.
    fn note_compat_fence(&self, tenant: TenantHash, compat_epoch: u64);
}

/// Why a publish was refused: the field of [`CompatRefusalSnapshot`] that
/// counts it, and the name logged with the refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompatRefusalReason {
    ViewStale,
    ActiveVersionNotWritable,
    CompatRecordMissing,
    FenceFailed,
    ActiveVersionMoved,
    NoBucketView,
}

impl CompatRefusalReason {
    pub const ALL: [CompatRefusalReason; 6] = [
        CompatRefusalReason::ViewStale,
        CompatRefusalReason::ActiveVersionNotWritable,
        CompatRefusalReason::CompatRecordMissing,
        CompatRefusalReason::FenceFailed,
        CompatRefusalReason::ActiveVersionMoved,
        CompatRefusalReason::NoBucketView,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            CompatRefusalReason::ViewStale => "view_stale",
            CompatRefusalReason::ActiveVersionNotWritable => "active_version_not_writable",
            CompatRefusalReason::CompatRecordMissing => "compat_record_missing",
            CompatRefusalReason::FenceFailed => "fence_failed",
            CompatRefusalReason::ActiveVersionMoved => "active_version_moved",
            CompatRefusalReason::NoBucketView => "no_bucket_view",
        }
    }

    fn index(self) -> usize {
        match self {
            CompatRefusalReason::ViewStale => 0,
            CompatRefusalReason::ActiveVersionNotWritable => 1,
            CompatRefusalReason::CompatRecordMissing => 2,
            CompatRefusalReason::FenceFailed => 3,
            CompatRefusalReason::ActiveVersionMoved => 4,
            CompatRefusalReason::NoBucketView => 5,
        }
    }
}

/// A write or flush the activation policy refused. Nothing of it was
/// published.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CompatPublishRefusal {
    /// The view refused (ADR-4233 decision 2).
    #[error("refusing to publish a {family} object: {refusal} (ADR-4233)")]
    Policy {
        family: FormatFamily,
        refusal: CompatRefusal,
    },
    /// The bucket could not be fenced before the first object above the
    /// family's baseline (ADR-4233 decision 4).
    #[error(
        "refusing to publish a {family} object at version {version}: fencing the bucket at \
         compat_epoch {compat_epoch} failed: {cause} (ADR-4233 decision 4)"
    )]
    FenceFailed {
        family: FormatFamily,
        version: u16,
        compat_epoch: u64,
        cause: String,
    },
    /// The view's answer moved off the version the flush was granted while
    /// the flush was publishing (ADR-4233 decision 3).
    #[error(
        "refusing to publish a {family} object at version {granted}: the activation policy now \
         answers version {active} (ADR-4233 decision 3)"
    )]
    ActiveVersionMoved {
        family: FormatFamily,
        granted: u16,
        active: u16,
    },
    /// The flush's router is gone, so no cached provisioning record of its
    /// bucket is left to ask the policy with.
    #[error(
        "refusing to publish a {family} object: the router that held the bucket's provisioning \
         record is gone (ADR-4233)"
    )]
    NoBucketView { family: FormatFamily },
}

impl CompatPublishRefusal {
    pub fn reason(&self) -> CompatRefusalReason {
        match self {
            CompatPublishRefusal::Policy { refusal, .. } => match refusal {
                CompatRefusal::ViewStale { .. } => CompatRefusalReason::ViewStale,
                CompatRefusal::ActiveVersionNotWritable { .. } => {
                    CompatRefusalReason::ActiveVersionNotWritable
                }
                CompatRefusal::CompatRecordMissing { .. } => {
                    CompatRefusalReason::CompatRecordMissing
                }
            },
            CompatPublishRefusal::FenceFailed { .. } => CompatRefusalReason::FenceFailed,
            CompatPublishRefusal::ActiveVersionMoved { .. } => {
                CompatRefusalReason::ActiveVersionMoved
            }
            CompatPublishRefusal::NoBucketView { .. } => CompatRefusalReason::NoBucketView,
        }
    }

    /// Always true: the refused rows are valid, and a later write succeeds
    /// once a refresh, a rollout, a retried fence, a new grant or a restored
    /// `sys/compat` clears the refusal. [`CompatRefusal::is_retryable`] answers a
    /// different question, whether it clears without an operator.
    pub fn is_retryable(&self) -> bool {
        true
    }
}

/// Refusals by reason, one set per signal's ingest metrics.
#[derive(Debug, Default)]
pub struct CompatRefusalCounters {
    by_reason: [AtomicU64; 6],
}

/// Point-in-time copy of [`CompatRefusalCounters`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompatRefusalSnapshot {
    pub view_stale: u64,
    pub active_version_not_writable: u64,
    pub compat_record_missing: u64,
    pub fence_failed: u64,
    pub active_version_moved: u64,
    pub no_bucket_view: u64,
}

impl CompatRefusalSnapshot {
    pub fn get(&self, reason: CompatRefusalReason) -> u64 {
        match reason {
            CompatRefusalReason::ViewStale => self.view_stale,
            CompatRefusalReason::ActiveVersionNotWritable => self.active_version_not_writable,
            CompatRefusalReason::CompatRecordMissing => self.compat_record_missing,
            CompatRefusalReason::FenceFailed => self.fence_failed,
            CompatRefusalReason::ActiveVersionMoved => self.active_version_moved,
            CompatRefusalReason::NoBucketView => self.no_bucket_view,
        }
    }

    pub fn total(&self) -> u64 {
        CompatRefusalReason::ALL.iter().map(|r| self.get(*r)).sum()
    }
}

impl CompatRefusalCounters {
    pub(crate) fn record(&self, reason: CompatRefusalReason) {
        self.by_reason[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CompatRefusalSnapshot {
        let load = |r: CompatRefusalReason| self.by_reason[r.index()].load(Ordering::Relaxed);
        CompatRefusalSnapshot {
            view_stale: load(CompatRefusalReason::ViewStale),
            active_version_not_writable: load(CompatRefusalReason::ActiveVersionNotWritable),
            compat_record_missing: load(CompatRefusalReason::CompatRecordMissing),
            fence_failed: load(CompatRefusalReason::FenceFailed),
            active_version_moved: load(CompatRefusalReason::ActiveVersionMoved),
            no_bucket_view: load(CompatRefusalReason::NoBucketView),
        }
    }
}

/// What a granted flush publishes at, chosen once at permit grant.
pub(crate) struct PublishGrant {
    family: FormatFamily,
    version: u16,
    managed: Option<Managed>,
}

struct Managed {
    policy: Arc<IngestCompatPolicy>,
    tenant: TenantHash,
    /// The router's cache of the bucket's provisioning record, asked again
    /// before every attempt.
    bucket: Arc<dyn BucketCompat>,
    /// The version is above the family's baseline, so the bucket must be
    /// fenced before the first PUT.
    fence: bool,
}

impl PublishGrant {
    /// No policy installed: the family's `CURRENT` version, no checks.
    pub(crate) fn unmanaged(family: FormatFamily) -> Self {
        PublishGrant {
            family,
            version: FamilyVersions::of(family).baseline,
            managed: None,
        }
    }

    /// Ask `policy` at permit grant. `None` is [`Self::unmanaged`]. With a
    /// policy and no `bucket` (the router is gone), refused
    /// [`CompatPublishRefusal::NoBucketView`].
    pub(crate) fn at_grant(
        policy: Option<Arc<IngestCompatPolicy>>,
        family: FormatFamily,
        tenant: TenantHash,
        bucket: Option<Arc<dyn BucketCompat>>,
    ) -> Result<Self, CompatPublishRefusal> {
        let Some(policy) = policy else {
            return Ok(PublishGrant::unmanaged(family));
        };
        let Some(bucket) = bucket else {
            return Err(CompatPublishRefusal::NoBucketView { family });
        };
        let version = policy
            .write_version(family, bucket.bucket_compat_epoch(tenant))
            .map_err(|refusal| CompatPublishRefusal::Policy { family, refusal })?;
        let fence = version > policy.baseline(family);
        Ok(PublishGrant {
            family,
            version,
            managed: Some(Managed {
                policy,
                tenant,
                bucket,
                fence,
            }),
        })
    }

    pub(crate) fn version(&self) -> u16 {
        self.version
    }

    /// The refusal for a granted version the writer cannot encode.
    pub(crate) fn unencodable(&self) -> CompatPublishRefusal {
        let writable = self
            .managed
            .as_ref()
            .map(|m| m.policy.writable(self.family).to_vec())
            .unwrap_or_default();
        CompatPublishRefusal::Policy {
            family: self.family,
            refusal: CompatRefusal::ActiveVersionNotWritable {
                family: compat_family(self.family).to_string(),
                active: u32::from(self.version),
                writable,
            },
        }
    }

    /// The check before every PUT and fence attempt (ADR-4233 decision 3):
    /// the view is asked again, with the bucket's cached provisioning record,
    /// and the attempt is refused when it refuses or names a version other
    /// than the granted one.
    pub(crate) fn check_attempt(&self) -> Result<(), CompatPublishRefusal> {
        let Some(managed) = &self.managed else {
            return Ok(());
        };
        let active = managed
            .policy
            .write_version(
                self.family,
                managed.bucket.bucket_compat_epoch(managed.tenant),
            )
            .map_err(|refusal| CompatPublishRefusal::Policy {
                family: self.family,
                refusal,
            })?;
        if active != self.version {
            return Err(CompatPublishRefusal::ActiveVersionMoved {
                family: self.family,
                granted: self.version,
                active,
            });
        }
        Ok(())
    }

    /// Before the first PUT of an object above the baseline: raise the
    /// bucket's provisioning record to the view's epoch unless the cached
    /// record already carries it. Each fence attempt is refused like a PUT
    /// attempt, and bounded by `deadline_ns` on `clock`; a lost CAS race is
    /// retried after a wait `rng` jitters up to [`FENCE_BACKOFF_MAX`].
    pub(crate) async fn ensure_fenced(
        &self,
        store: &dyn ObjectStoreBackend,
        signal: Signal,
        clock: &dyn Clock,
        rng: &dyn RngSource,
        deadline_ns: i64,
    ) -> Result<(), CompatPublishRefusal> {
        let Some(Managed {
            policy,
            tenant,
            bucket,
            fence: true,
        }) = &self.managed
        else {
            return Ok(());
        };
        let tenant = *tenant;
        let compat_epoch = policy.view().seen_epoch();
        if bucket.bucket_compat_epoch(tenant).unwrap_or(0) >= compat_epoch {
            return Ok(());
        }
        let failed = |cause: String| CompatPublishRefusal::FenceFailed {
            family: self.family,
            version: self.version,
            compat_epoch,
            cause,
        };
        let elapsed = || failed("max_flush_lifetime elapsed".into());
        let mut attempt = 0;
        loop {
            attempt += 1;
            self.check_attempt()?;
            let raise =
                raise_compat_fence(store, &tenant, signal, compat_epoch, policy.now_unix_ns());
            match bounded(clock, deadline_ns, raise).await {
                Some(Ok(outcome)) => {
                    let fenced = match outcome {
                        CompatFenceOutcome::AlreadyFenced { compat_epoch } => compat_epoch,
                        CompatFenceOutcome::Raised { compat_epoch, .. } => compat_epoch,
                    };
                    bucket.note_compat_fence(tenant, fenced);
                    return Ok(());
                }
                Some(Err(ProvisioningError::FenceCasConflict { .. }))
                    if attempt < FENCE_ATTEMPTS =>
                {
                    let max_ms = u64::try_from(FENCE_BACKOFF_MAX.as_millis()).unwrap_or(u64::MAX);
                    let wait = Duration::from_millis(rng.jitter_ms(max_ms));
                    bounded(clock, deadline_ns, clock.sleep(wait))
                        .await
                        .ok_or_else(elapsed)?;
                }
                Some(Err(err)) => return Err(failed(err.to_string())),
                None => return Err(elapsed()),
            }
        }
    }
}

/// Why a flush's PUT retry loop stopped without storing its object.
pub(crate) enum PutAbort {
    /// Retry budget or `max_flush_lifetime` exhausted, or a terminal store
    /// error.
    Exhausted,
    /// The activation policy refused the next attempt.
    Refused(CompatPublishRefusal),
}

/// `fut` raced against `deadline_ns` on `clock`, as the shards bound their
/// own store calls.
async fn bounded<F: Future>(clock: &dyn Clock, deadline_ns: i64, fut: F) -> Option<F::Output> {
    let remaining_ns = deadline_ns.saturating_sub(clock.now_ns());
    if remaining_ns <= 0 {
        return None;
    }
    let remaining = Duration::from_nanos(u64::try_from(remaining_ns).unwrap_or(u64::MAX));
    tokio::select! {
        result = fut => Some(result),
        () = clock.sleep(remaining) => None,
    }
}

/// Logs one refused flush or write.
pub(crate) fn warn_publish_refused(
    signal: Signal,
    shard: Option<u32>,
    tenant: TenantHash,
    refusal: &CompatPublishRefusal,
) {
    tracing::warn!(
        signal = ?signal,
        shard,
        tenant_hash = %tenant.to_hex(),
        reason = refusal.reason().as_str(),
        error = %refusal,
        "refusing to publish: the sys/compat activation policy does not allow it; no commit \
         record was stored"
    );
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::atomic::AtomicI64;

    use ravel_catalog::CompatKeys;
    use ravel_catalog::compat::{FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, create_compat};
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    const BASE_NS: i64 = 1_700_000_000_000_000_000;

    struct FixedClock(AtomicI64);

    impl CompatClock for FixedClock {
        fn now_unix_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// A policy whose view read a `sys/compat` naming `rseg` the active RSEG
    /// version, with RSEG's writable set lowered to `[7]` over baseline 6.
    async fn policy(rseg: u32) -> IngestCompatPolicy {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let keys = || CompatKeys::from_file_keys(&[[5; 32]]).expect("one key");
        let family = |name: &str, version: u32| sysproto::CompatFamily {
            family: name.to_string(),
            active_write_version: version,
            ..Default::default()
        };
        create_compat(
            store.as_ref(),
            &keys(),
            sysproto::CompatRecord {
                compat_epoch: 1,
                families: vec![
                    family(FAMILY_RSEG, rseg),
                    family(FAMILY_RLOG, FormatFamily::Rlog.writable_version()),
                    family(FAMILY_RSPAN, FormatFamily::Rspan.writable_version()),
                ],
                ..Default::default()
            },
        )
        .await
        .expect("create sys/compat");
        let clock = Arc::new(FixedClock(AtomicI64::new(BASE_NS)));
        let view = CompatView::new(store, keys(), clock.clone());
        view.refresh().await.expect("read sys/compat");
        IngestCompatPolicy::new(view, clock).with_family_versions(FormatFamily::Rseg, vec![7], 6)
    }

    #[tokio::test]
    async fn the_writable_version_is_the_view_named_version_when_this_build_writes_it() {
        assert_eq!(
            FormatFamily::Rseg.writable_version_under(None),
            u32::from(ravel_segment::SegmentVersion::CURRENT.number())
        );

        let active = policy(7).await;
        assert_eq!(active.named_version(FormatFamily::Rseg), 7);
        assert_eq!(FormatFamily::Rseg.writable_version_under(Some(&active)), 7);

        let unwritable = policy(8).await;
        assert_eq!(unwritable.named_version(FormatFamily::Rseg), 6);
        assert_eq!(
            FormatFamily::Rseg.writable_version_under(Some(&unwritable)),
            6
        );
        assert_eq!(
            FormatFamily::Rlog.writable_version_under(Some(&unwritable)),
            FormatFamily::Rlog.writable_version()
        );
    }

    /// A grant asked with a policy installed and no bucket view (its router
    /// was dropped) is refused under `no_bucket_view`, never granted the
    /// unmanaged `CURRENT`; with no policy it is unmanaged.
    #[tokio::test]
    async fn a_grant_with_no_bucket_view_is_refused_typed() {
        let tenant = ravel_types::TenantId::new("acme").hash();
        let err = PublishGrant::at_grant(
            Some(Arc::new(policy(7).await)),
            FormatFamily::Rseg,
            tenant,
            None,
        )
        .err()
        .expect("no bucket view");
        assert!(
            matches!(
                err,
                CompatPublishRefusal::NoBucketView {
                    family: FormatFamily::Rseg
                }
            ),
            "{err:?}"
        );
        assert_eq!(err.reason(), CompatRefusalReason::NoBucketView);
        assert!(err.is_retryable());

        let grant =
            PublishGrant::at_grant(None, FormatFamily::Rseg, tenant, None).expect("unmanaged");
        assert!(grant.managed.is_none());
        assert_eq!(grant.check_attempt().ok(), Some(()));
    }

    #[test]
    fn each_refusal_reason_counts_under_its_own_label() {
        let labels: Vec<&str> = CompatRefusalReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect();
        assert_eq!(
            labels,
            [
                "view_stale",
                "active_version_not_writable",
                "compat_record_missing",
                "fence_failed",
                "active_version_moved",
                "no_bucket_view",
            ]
        );

        let counters = CompatRefusalCounters::default();
        for (n, reason) in CompatRefusalReason::ALL.into_iter().enumerate() {
            for _ in 0..=n {
                counters.record(reason);
            }
        }
        let snapshot = counters.snapshot();
        assert_eq!(
            snapshot,
            CompatRefusalSnapshot {
                view_stale: 1,
                active_version_not_writable: 2,
                compat_record_missing: 3,
                fence_failed: 4,
                active_version_moved: 5,
                no_bucket_view: 6,
            }
        );
        assert_eq!(snapshot.total(), 21);
    }
}
