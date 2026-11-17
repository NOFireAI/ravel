//! The `sys/compat` activation policy as ingest applies it (ADR-4233
//! decisions 2 to 4).
//!
//! An [`IngestCompatPolicy`] holds the process's [`CompatView`] and, per
//! [`FormatFamily`], the versions this build writes and the family's baseline.
//! A router given one (`with_compat_policy`) applies it at three points:
//!
//! - Admission refuses, before anything is buffered, a write the cached view
//!   says cannot publish now ([`CompatRefusal::ViewStale`],
//!   [`CompatRefusal::ActiveVersionNotWritable`],
//!   [`CompatRefusal::CompatRecordMissing`]) or whose router's
//!   `max_flush_lifetime` is above the record's `max_publish_lifetime_ns`
//!   ([`CompatPublishRefusal::PublishLifetimeAboveCompat`]). Every admission
//!   refusal is retryable and acknowledges nothing.
//! - Permit grant picks the version a flush encodes at
//!   ([`CompatView::write_policy`]). A stale view at grant does not drop the
//!   flush: it hands its permit back and asks again every
//!   [`STALE_VIEW_RETRY`], the last time at the deadline itself, until the
//!   view answers or the flush's `max_flush_lifetime` from its first grant
//!   elapses, and only then is the flush refused `view_stale`. Any other
//!   refusal at grant (a version not writable, a missing record after an
//!   activation, or a router gone with no cached provisioning record left to
//!   ask with) refuses the flush closed: no PUT, its rows dropped, its byte
//!   charges released, its strict waiters acked with a retryable
//!   [`CompatPublishRefusal`]. The record's `max_publish_lifetime_ns` is not
//!   checked at grant: it is never lowered (ADR-4233 decision 7), so a record
//!   whose bound is below the router's lifetime is one `compat init` wrote
//!   while the rows were buffered, and they publish inside the router's own
//!   `max_flush_lifetime`.
//! - Every data-object, commit-record and fence attempt, retries included,
//!   asks [`CompatView::write_policy`] again with the bucket's cached
//!   provisioning record. A stale view waits for a fresh one within the same
//!   deadline, as at grant; any other refusal, or an answer other than the
//!   granted version, refuses the attempt. Before the first PUT of an object
//!   above the family's baseline, the bucket's provisioning record is fenced
//!   at the view's epoch ([`ravel_catalog::raise_compat_fence`]).
//!
//! Every `view_stale` answer the policy gives wakes its refresh notifier
//! ([`IngestCompatPolicy::with_refresh_wake`]), so the process that owns the
//! view can re-read `sys/compat` before its next scheduled refresh.
//!
//! None of these refusals can fire on a store with no `sys/compat` record
//! whose view is blind or fresh: the view answers each family's baseline, and
//! with no record there is no lifetime bound to exceed.
//!
//! The format-floor checks at admission and at flush open compare the floor
//! with the version the policy names for the family
//! ([`FormatFamily::writable_version_under`]); a grant at another version is
//! checked against the floor again.
//!
//! A router given `None`, or none at all, writes each family's `CURRENT`
//! version with none of these checks, exactly as before the policy existed.
//! Every refusal counts once under its [`CompatRefusalReason`] in the
//! signal's ingest metrics snapshot (`compat_publish_refused`), which
//! `ravel-server` renders on `/metrics` as
//! `ravel_compat_publish_refused_total` (#4301).

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
use tokio::sync::Notify;

use crate::clock::Clock;
use crate::format_floor::FormatFamily;

/// Attempts at the bucket fence before a lost CAS race fails the flush.
pub(crate) const FENCE_ATTEMPTS: u32 = 3;

/// The longest jittered wait between two fence attempts.
const FENCE_BACKOFF_MAX: Duration = Duration::from_millis(50);

/// How long a flush that met a stale view waits before asking it again.
pub(crate) const STALE_VIEW_RETRY: Duration = Duration::from_secs(1);

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
    refresh_wake: Option<Arc<Notify>>,
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

impl PublishLifetimeAboveCompat {
    fn refusal(self, family: FormatFamily) -> CompatPublishRefusal {
        CompatPublishRefusal::PublishLifetimeAboveCompat {
            family,
            max_flush_lifetime_ns: self.max_flush_lifetime_ns,
            max_publish_lifetime_ns: self.max_publish_lifetime_ns,
        }
    }
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
            refresh_wake: None,
        }
    }

    /// Notify `wake` (one stored permit, `Notify::notify_one`) each time the
    /// policy answers `view_stale`, at admission, at grant or before an
    /// attempt, so the owner of the view can refresh it early instead of on
    /// its next scheduled tick. Rate limiting is the owner's.
    #[must_use]
    pub fn with_refresh_wake(mut self, wake: Arc<Notify>) -> Self {
        self.refresh_wake = Some(wake);
        self
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
        let answer = self.view.write_policy(
            self.now_unix_ns(),
            compat_family(family),
            &versions.writable,
            versions.baseline,
            bucket.as_ref(),
        );
        if let (Err(CompatRefusal::ViewStale { .. }), Some(wake)) = (&answer, &self.refresh_wake) {
            wake.notify_one();
        }
        answer
    }

    /// The version the view names for `family`: its active version when this
    /// build writes it, else the family's baseline. An active version this
    /// build does not write falls to the baseline here too, because this is a
    /// version-selection helper, not the publish gate: the compat write policy
    /// refuses such a write outright as
    /// [`CompatRefusal::ActiveVersionNotWritable`]
    /// ([`Self::write_version`]), so the baseline this names is never actually
    /// published. A format-floor check reading this version
    /// ([`FormatFamily::writable_version_under`]) therefore compares the floor
    /// against the baseline, which can surface as `BelowFormatFloor` even
    /// though the operative refusal on the write path is
    /// `ActiveVersionNotWritable`. Unlike [`Self::write_version`] it does not
    /// refuse on a stale or missing view.
    pub fn named_version(&self, family: FormatFamily) -> u16 {
        let versions = &self.families[family_index(family)];
        self.view
            .seen_family(compat_family(family))
            .and_then(|seen| u16::try_from(seen.active_write_version).ok())
            .filter(|active| versions.writable.contains(active))
            .unwrap_or(versions.baseline)
    }

    /// The admission check: [`Self::write_version`] refusing under the same
    /// conditions a grant would, a stale view included, then
    /// [`Self::check_publish_lifetime`] against the current record.
    pub fn admit(
        &self,
        family: FormatFamily,
        bucket_compat_epoch: Option<u64>,
        max_flush_lifetime: Duration,
    ) -> Result<(), CompatPublishRefusal> {
        self.write_version(family, bucket_compat_epoch)
            .map_err(|refusal| CompatPublishRefusal::Policy { family, refusal })?;
        self.check_publish_lifetime(max_flush_lifetime)
            .map_err(|above| above.refusal(family))
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
pub(crate) struct CompatSlot(Arc<RwLock<Option<InstalledPolicy>>>);

/// A policy as one router installed it: with the router's
/// `max_flush_lifetime`, which admission checks against the record's
/// `max_publish_lifetime_ns` as it stands then.
#[derive(Debug, Clone)]
pub(crate) struct InstalledPolicy {
    pub(crate) policy: Arc<IngestCompatPolicy>,
    pub(crate) max_flush_lifetime: Duration,
}

impl CompatSlot {
    pub(crate) fn set(
        &self,
        policy: Option<Arc<IngestCompatPolicy>>,
        max_flush_lifetime: Duration,
    ) {
        *self.0.write().unwrap_or_else(|p| p.into_inner()) = policy.map(|policy| InstalledPolicy {
            policy,
            max_flush_lifetime,
        });
    }

    pub(crate) fn get(&self) -> Option<Arc<IngestCompatPolicy>> {
        self.installed().map(|installed| installed.policy)
    }

    pub(crate) fn installed(&self) -> Option<InstalledPolicy> {
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
    PublishLifetimeAboveCompat,
}

impl CompatRefusalReason {
    pub const ALL: [CompatRefusalReason; 7] = [
        CompatRefusalReason::ViewStale,
        CompatRefusalReason::ActiveVersionNotWritable,
        CompatRefusalReason::CompatRecordMissing,
        CompatRefusalReason::FenceFailed,
        CompatRefusalReason::ActiveVersionMoved,
        CompatRefusalReason::NoBucketView,
        CompatRefusalReason::PublishLifetimeAboveCompat,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            CompatRefusalReason::ViewStale => "view_stale",
            CompatRefusalReason::ActiveVersionNotWritable => "active_version_not_writable",
            CompatRefusalReason::CompatRecordMissing => "compat_record_missing",
            CompatRefusalReason::FenceFailed => "fence_failed",
            CompatRefusalReason::ActiveVersionMoved => "active_version_moved",
            CompatRefusalReason::NoBucketView => "no_bucket_view",
            CompatRefusalReason::PublishLifetimeAboveCompat => "publish_lifetime_above_compat",
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
            CompatRefusalReason::PublishLifetimeAboveCompat => 6,
        }
    }
}

/// A write or flush the activation policy refused. No commit record is
/// published by the refused attempt; a data object already stored before the
/// refusal is left for orphan GC.
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
    /// The router's `max_flush_lifetime` is above the `max_publish_lifetime_ns`
    /// of the record the view holds now (ADR-4233 decision 7), checked at
    /// admission only.
    #[error(
        "refusing to publish a {family} object: max_flush_lifetime is {max_flush_lifetime_ns} \
         ns, above the {max_publish_lifetime_ns} ns max_publish_lifetime_ns in sys/compat \
         (ADR-4233 decision 7)"
    )]
    PublishLifetimeAboveCompat {
        family: FormatFamily,
        max_flush_lifetime_ns: i64,
        max_publish_lifetime_ns: i64,
    },
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
            CompatPublishRefusal::PublishLifetimeAboveCompat { .. } => {
                CompatRefusalReason::PublishLifetimeAboveCompat
            }
        }
    }

    /// The view is stale: a later refresh clears it with no operator, so a
    /// granted or queued flush waits for one instead of dropping its rows.
    pub fn is_view_stale(&self) -> bool {
        matches!(
            self,
            CompatPublishRefusal::Policy {
                refusal: CompatRefusal::ViewStale { .. },
                ..
            }
        )
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
    by_reason: [AtomicU64; 7],
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
    pub publish_lifetime_above_compat: u64,
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
            CompatRefusalReason::PublishLifetimeAboveCompat => self.publish_lifetime_above_compat,
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
            publish_lifetime_above_compat: load(CompatRefusalReason::PublishLifetimeAboveCompat),
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

    /// Ask `installed` at permit grant. `None` is [`Self::unmanaged`]. With a
    /// policy and no `bucket` (the router is gone), refused
    /// [`CompatPublishRefusal::NoBucketView`]. The record's
    /// `max_publish_lifetime_ns` is admission's check only: rows admitted
    /// before `compat init` recorded a bound below the router's
    /// `max_flush_lifetime` are acknowledged, and publish inside that
    /// lifetime instead of being dropped here.
    pub(crate) fn at_grant(
        installed: Option<InstalledPolicy>,
        family: FormatFamily,
        tenant: TenantHash,
        bucket: Option<Arc<dyn BucketCompat>>,
    ) -> Result<Self, CompatPublishRefusal> {
        let Some(InstalledPolicy { policy, .. }) = installed else {
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

    /// [`Self::check_attempt`], waiting out a stale view: asked again every
    /// [`STALE_VIEW_RETRY`] on `clock`, the last time at `deadline_ns`
    /// ([`stale_view_wait`]), until the view answers, and refused
    /// `view_stale` only when it is still stale at that last ask. Any other
    /// refusal returns at once.
    pub(crate) async fn attempt_when_fresh(
        &self,
        clock: &dyn Clock,
        deadline_ns: i64,
    ) -> Result<(), CompatPublishRefusal> {
        loop {
            match self.check_attempt() {
                Err(refused) if refused.is_view_stale() => {
                    if !stale_view_wait(clock, deadline_ns).await {
                        return Err(refused);
                    }
                }
                checked => return checked,
            }
        }
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
            self.attempt_when_fresh(clock, deadline_ns).await?;
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

/// The wait between two asks of a stale view: [`STALE_VIEW_RETRY`], cut short
/// at `deadline_ns` on `clock` so the view is asked once more at the deadline
/// itself. `false`, with no wait, once `deadline_ns` has passed: the caller
/// refuses `view_stale`.
pub(crate) async fn stale_view_wait(clock: &dyn Clock, deadline_ns: i64) -> bool {
    let remaining_ns = deadline_ns.saturating_sub(clock.now_ns());
    if remaining_ns <= 0 {
        return false;
    }
    let remaining = Duration::from_nanos(u64::try_from(remaining_ns).unwrap_or(u64::MAX));
    clock.sleep(STALE_VIEW_RETRY.min(remaining)).await;
    true
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
         record is published by the refused attempt, and a data object already stored before \
         the refusal is left for orphan GC"
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
        policy_and_clock(rseg).await.0
    }

    /// [`policy`] and the clock its view is read at.
    async fn policy_and_clock(rseg: u32) -> (IngestCompatPolicy, Arc<FixedClock>) {
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
        let policy = IngestCompatPolicy::new(view, clock.clone()).with_family_versions(
            FormatFamily::Rseg,
            vec![7],
            6,
        );
        (policy, clock)
    }

    /// A bucket whose cached provisioning record carries `compat_epoch` 1.
    struct FencedBucket;

    impl BucketCompat for FencedBucket {
        fn bucket_compat_epoch(&self, _tenant: TenantHash) -> Option<u64> {
            Some(1)
        }

        fn note_compat_fence(&self, _tenant: TenantHash, _compat_epoch: u64) {}
    }

    /// Whether `wake` holds a stored permit, taking it.
    fn woken(wake: &Notify) -> bool {
        futures::FutureExt::now_or_never(wake.notified()).is_some()
    }

    /// A flush waiting out a stale view before an attempt asks the view once
    /// more at its deadline, not one whole [`STALE_VIEW_RETRY`] before it:
    /// with 1.5 s left, the view is stale at 0 s and at 1 s and fresh again
    /// at 1.5 s, and the attempt goes ahead. A wait that ends at the deadline
    /// without asking refuses it `view_stale` at 1.5 s. With the view still
    /// stale at the deadline the attempt is refused `view_stale` then, and
    /// not before.
    #[tokio::test]
    async fn a_stale_view_wait_asks_once_more_at_the_deadline() {
        for recover in [true, false] {
            let (policy, compat_clock) = policy_and_clock(7).await;
            let view = policy.view().clone();
            let tenant = ravel_types::TenantId::new("acme").hash();
            let grant = PublishGrant::at_grant(
                Some(InstalledPolicy {
                    policy: Arc::new(policy),
                    max_flush_lifetime: Duration::from_secs(60),
                }),
                FormatFamily::Rseg,
                tenant,
                Some(Arc::new(FencedBucket)),
            )
            .ok()
            .expect("granted on a fresh view");
            compat_clock
                .0
                .fetch_add(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);
            assert!(grant.check_attempt().is_err_and(|r| r.is_view_stale()));

            let clock = crate::compat_signal_tests::ManualClock::new();
            let deadline_ns = clock.now_ns() + 1_500_000_000;
            let attempt = tokio::spawn({
                let clock = Arc::clone(&clock);
                async move { grant.attempt_when_fresh(clock.as_ref(), deadline_ns).await }
            });
            clock.advance_ns(1_000_000_000).await;
            assert!(!attempt.is_finished(), "recover={recover}: 1 s in");
            if recover {
                view.refresh().await.expect("read sys/compat");
            }
            clock.advance_ns(500_000_000).await;
            let outcome = tokio::time::timeout(Duration::from_secs(30), attempt)
                .await
                .expect("the attempt ends at the deadline")
                .expect("attempt task");
            if recover {
                assert_eq!(outcome, Ok(()));
            } else {
                assert!(
                    outcome.as_ref().is_err_and(|r| r.is_view_stale()),
                    "{outcome:?}"
                );
            }
        }
    }

    /// Every `view_stale` answer stores one wake permit on the policy's
    /// refresh notifier, at admission and before an attempt alike; a fresh
    /// answer, and a refusal other than `view_stale`, store none.
    #[tokio::test]
    async fn a_view_stale_answer_wakes_the_refresh() {
        let lifetime = Duration::from_secs(60);
        let wake = Arc::new(Notify::new());
        let (policy, compat_clock) = policy_and_clock(7).await;
        let policy = policy.with_refresh_wake(Arc::clone(&wake));
        assert_eq!(policy.admit(FormatFamily::Rseg, Some(1), lifetime), Ok(()));
        assert!(!woken(&wake), "a fresh view does not wake the refresh");

        compat_clock
            .0
            .fetch_add(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);
        let refused = policy
            .admit(FormatFamily::Rseg, Some(1), lifetime)
            .expect_err("stale");
        assert!(refused.is_view_stale(), "{refused:?}");
        assert!(woken(&wake), "admission's view_stale wakes the refresh");
        assert!(!woken(&wake), "one permit per answer, taken once");
        assert!(policy.write_version(FormatFamily::Rseg, Some(1)).is_err());
        assert!(woken(&wake), "an attempt's view_stale wakes it too");

        let wake = Arc::new(Notify::new());
        let unwritable = policy_and_clock(8)
            .await
            .0
            .with_refresh_wake(Arc::clone(&wake));
        let refused = unwritable
            .admit(FormatFamily::Rseg, Some(1), lifetime)
            .expect_err("not writable");
        assert_eq!(
            refused.reason(),
            CompatRefusalReason::ActiveVersionNotWritable
        );
        assert!(!woken(&wake), "only view_stale wakes the refresh");
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
            Some(InstalledPolicy {
                policy: Arc::new(policy(7).await),
                max_flush_lifetime: Duration::from_secs(60),
            }),
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
                "publish_lifetime_above_compat",
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
                publish_lifetime_above_compat: 7,
            }
        );
        assert_eq!(snapshot.total(), 28);
    }
}
