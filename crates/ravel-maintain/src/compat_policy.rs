//! The `sys/compat` write policy maintenance writers obey (ADR-4233
//! decisions 3, 4 and 7).
//!
//! A [`MaintainCompatPolicy`] holds the process's [`CompatView`] and, per
//! format family, the versions this build writes and the family's baseline,
//! all read from the format crates. Installed in
//! [`CompactorConfig::compat_policy`], it makes every maintenance writer:
//!
//! - fix the version it writes for each family it touches once, at the run
//!   start, before any claim, build or PUT ([`CompatRun`]); a view older than
//!   [`COMPAT_VIEW_HORIZON_NS`], an active version this build cannot write, or
//!   an absent `sys/compat` with an activation on record refuses the run;
//! - re-check the view's age before every PUT attempt of a data object or a
//!   record, retries included ([`CompatRun::before_put`]);
//! - raise the bucket's provisioning-record fence to the family's
//!   `activated_epoch` before its first PUT above the family baseline into a
//!   bucket that has a provisioning record (decision 4);
//! - refuse when its own publish lifetime exceeds the recorded `P`
//!   (`PublishLifetimeAboveCompat`, decision 7).
//!
//! Every refusal is counted under
//! `ravel_compat_publish_refused_total{family,reason}`
//! ([`compat_publish_refused_total`]). With no policy installed nothing here
//! runs and every writer emits its build's current version, as before.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use futures::lock::Mutex as AsyncMutex;
use prost::Message;
use ravel_catalog::compat::{
    COMPAT_VIEW_HORIZON_NS, CompatClock, CompatRefusal, CompatView, FAMILY_COMMIT_RECORD,
    FAMILY_COMPACTION_RECORD, FAMILY_REWRITE_RECORD, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN,
};
use ravel_catalog::{CompatFenceOutcome, provisioning_key, raise_compat_fence};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::request_ledger::{RequestPhase, note_get};

/// `P` when `sys/compat` records none: the compiled `max_flush_lifetime` and
/// the default `max_compaction_lifetime`, one hour (ADR-4233 decision 7).
pub const DEFAULT_PUBLISH_LIFETIME_NS: i64 = 3_600 * 1_000_000_000;

/// The versions this build writes for one family, and the family's baseline:
/// the version a writer emits while `sys/compat` names none for the family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyVersions {
    pub writable: Vec<u16>,
    pub baseline: u16,
}

impl FamilyVersions {
    /// The build's table for `family`, from the format crates; `None` for a
    /// family no maintenance writer emits.
    pub fn for_family(family: &str) -> Option<FamilyVersions> {
        match family {
            FAMILY_RSEG => Some(FamilyVersions {
                writable: ravel_segment::SUPPORTED_VERSIONS
                    .versions()
                    .map(ravel_segment::SegmentVersion::number)
                    .collect(),
                baseline: ravel_segment::SegmentVersion::CURRENT.number(),
            }),
            FAMILY_RLOG => {
                let window = ravel_logseg::footer::SUPPORTED_VERSIONS;
                Some(FamilyVersions {
                    writable: (window.oldest()..=window.newest()).collect(),
                    baseline: ravel_logseg::footer::WriteVersion::CURRENT.number(),
                })
            }
            FAMILY_RSPAN => {
                let window = ravel_rspan::footer::SUPPORTED_VERSIONS;
                Some(FamilyVersions {
                    writable: (window.oldest()..=window.newest()).collect(),
                    baseline: ravel_rspan::footer::WriteVersion::CURRENT.number(),
                })
            }
            FAMILY_COMPACTION_RECORD => Some(FamilyVersions {
                writable: (record_version(ravel_commit::record::COMPACTION_FORMAT_VERSION)
                    ..=record_version(ravel_commit::record::COMPACTION_SUPERSEDING_FORMAT_VERSION))
                    .collect(),
                baseline: record_version(ravel_commit::record::COMPACTION_FORMAT_VERSION),
            }),
            FAMILY_REWRITE_RECORD => Some(FamilyVersions::single(record_version(
                ravel_commit::erasure::FORMAT_VERSION,
            ))),
            FAMILY_COMMIT_RECORD => Some(FamilyVersions::single(record_version(
                ravel_commit::record::FORMAT_VERSION,
            ))),
            _ => None,
        }
    }

    fn single(version: u16) -> FamilyVersions {
        FamilyVersions {
            writable: vec![version],
            baseline: version,
        }
    }
}

/// A record format version as the `u16` the compat families compare in. The
/// record constants are `u32` but small; one that did not fit would match no
/// active version and so refuse rather than truncate.
fn record_version(version: u32) -> u16 {
    u16::try_from(version).unwrap_or(u16::MAX)
}

/// The maintenance families a policy knows by default.
const MAINTAIN_FAMILIES: [&str; 6] = [
    FAMILY_RSEG,
    FAMILY_RLOG,
    FAMILY_RSPAN,
    FAMILY_COMPACTION_RECORD,
    FAMILY_REWRITE_RECORD,
    FAMILY_COMMIT_RECORD,
];

/// The data family a signal's parts belong to; `None` for a signal no part
/// family covers.
pub fn part_family(signal: Signal) -> Option<&'static str> {
    match signal {
        Signal::Metrics => Some(FAMILY_RSEG),
        Signal::Logs | Signal::Alerts | Signal::Audit => Some(FAMILY_RLOG),
        Signal::Spans => Some(FAMILY_RSPAN),
        Signal::Profiles => None,
    }
}

/// The write policy handle maintenance writers consult. Built by the process
/// owner (server or CLI) around the view it refreshes; cheap to clone.
#[derive(Clone)]
pub struct MaintainCompatPolicy {
    view: CompatView,
    clock: Arc<dyn CompatClock>,
    families: Arc<BTreeMap<String, FamilyVersions>>,
}

impl std::fmt::Debug for MaintainCompatPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaintainCompatPolicy")
            .field("view", &self.view)
            .field("families", &self.families)
            .finish_non_exhaustive()
    }
}

impl MaintainCompatPolicy {
    /// A policy over `view`, judging staleness with `clock` (the clock the
    /// view stamps its reads with), and the build's own family table.
    pub fn new(view: CompatView, clock: Arc<dyn CompatClock>) -> Self {
        let families = MAINTAIN_FAMILIES
            .iter()
            .filter_map(|family| {
                FamilyVersions::for_family(family).map(|v| ((*family).to_string(), v))
            })
            .collect();
        MaintainCompatPolicy {
            view,
            clock,
            families: Arc::new(families),
        }
    }

    /// Replace one family's versions. For tests that need a build writing two
    /// versions without the `format-drill` feature.
    pub fn with_family(mut self, family: &str, versions: FamilyVersions) -> Self {
        Arc::make_mut(&mut self.families).insert(family.to_string(), versions);
        self
    }

    pub fn view(&self) -> &CompatView {
        &self.view
    }

    pub fn versions(&self, family: &str) -> Option<&FamilyVersions> {
        self.families.get(family)
    }

    pub fn now_unix_ns(&self) -> i64 {
        self.clock.now_unix_ns()
    }

    /// The recorded `P`, or [`DEFAULT_PUBLISH_LIFETIME_NS`] when the record
    /// is absent or carries zero.
    pub fn publish_lifetime_ns(&self) -> i64 {
        self.view
            .last_record()
            .map(|r| r.max_publish_lifetime_ns)
            .filter(|p| *p > 0)
            .unwrap_or(DEFAULT_PUBLISH_LIFETIME_NS)
    }

    /// The family's active write version as the view last saw it, or its
    /// baseline. No staleness check: for reporting and selection only.
    pub fn active_version(&self, family: &str) -> Option<u16> {
        let baseline = self.families.get(family)?.baseline;
        match self.view.seen_family(family) {
            Some(seen) => u16::try_from(seen.active_write_version).ok(),
            None => Some(baseline),
        }
    }

    /// Start a run of `run` writing `families` into a bucket identified by
    /// `bucket`, whose provisioning record is `record`. `lifetime_ns` is the
    /// longest the run may take from now to its last PUT; above the recorded
    /// `P` the run is refused. The refusal is counted and logged here.
    pub(crate) fn begin(
        &self,
        run: &'static str,
        families: &[&'static str],
        bucket: Option<(TenantHash, Signal)>,
        record: Option<&sysproto::ProvisioningRecord>,
        lifetime_ns: i64,
    ) -> Result<CompatRun> {
        let now = self.now_unix_ns();
        let mut versions = Vec::with_capacity(families.len());
        let mut fence_epoch = 0u64;
        for family in families {
            let (writable, baseline) = self
                .families
                .get(*family)
                .map_or((&[][..], 0), |v| (v.writable.as_slice(), v.baseline));
            let version = self
                .view
                .write_policy(now, family, writable, baseline, record)
                .map_err(|refusal| {
                    refuse(run, family, bucket.as_ref(), CompatRunRefusal::Compat(refusal))
                })?;
            if version > baseline {
                if let Some(seen) = self.view.seen_family(family) {
                    fence_epoch = fence_epoch.max(seen.activated_epoch);
                }
            }
            versions.push((*family, version));
        }
        let recorded = self
            .view
            .last_record()
            .map(|r| r.max_publish_lifetime_ns)
            .filter(|p| *p > 0);
        if let Some(record_ns) = recorded {
            if lifetime_ns > record_ns {
                let family = families.first().copied().unwrap_or_default();
                return Err(refuse(
                    run,
                    family,
                    bucket.as_ref(),
                    CompatRunRefusal::PublishLifetimeAboveCompat {
                        process_ns: lifetime_ns,
                        record_ns,
                    },
                ));
            }
        }
        let fence = match (bucket, record) {
            (Some((tenant_hash, signal)), Some(record)) if record.compat_epoch < fence_epoch => {
                Some(PendingFence {
                    tenant_hash,
                    signal,
                    compat_epoch: fence_epoch,
                    raised: AsyncMutex::new(false),
                })
            }
            _ => None,
        };
        Ok(CompatRun {
            inner: Arc::new(RunInner {
                policy: self.clone(),
                run,
                started_unix_ns: now,
                versions,
                bucket,
                fence,
            }),
        })
    }

    /// [`Self::begin`] for a writer into a signal with no provisioning record
    /// (audit, legal hold): no fence is ever raised.
    pub(crate) fn begin_unprovisioned(
        &self,
        run: &'static str,
        families: &[&'static str],
        lifetime_ns: i64,
    ) -> Result<CompatRun> {
        self.begin(run, families, None, None, lifetime_ns)
    }
}

/// Why a maintenance run refused to publish under the compat policy.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CompatRunRefusal {
    /// The view's own refusal: stale, unwritable active version, or an
    /// absent record with an activation on record.
    #[error(transparent)]
    Compat(CompatRefusal),
    /// The run's publish lifetime is above the `P` recorded in `sys/compat`.
    #[error(
        "publish lifetime {process_ns} ns is above the recorded max_publish_lifetime {record_ns} ns"
    )]
    PublishLifetimeAboveCompat { process_ns: i64, record_ns: i64 },
    /// A migrate target that is not the family's active write version.
    #[error("target version {target} is not the family's active write version {active}")]
    TargetNotActive { target: u32, active: u16 },
    /// A force-2 re-encode while `compaction-record` is not active at 2.
    #[error(
        "compaction record version 2 needs family compaction-record active at 2; active is {active}"
    )]
    CompactionRecordNotActive { active: u16 },
    /// Raising the bucket's provisioning-record fence failed; nothing above
    /// the baseline was written.
    #[error("raising the bucket's compat fence to epoch {compat_epoch} failed: {detail}")]
    FenceFailed { compat_epoch: u64, detail: String },
    /// `P` passed since the run start before the record PUT; the object is
    /// abandoned without its record.
    #[error("{elapsed_ns} ns since the run start is past the publish lifetime {lifetime_ns} ns")]
    PublishLifetimeExpired { elapsed_ns: i64, lifetime_ns: i64 },
}

impl CompatRunRefusal {
    /// The `reason` label of `ravel_compat_publish_refused_total`.
    pub fn reason(&self) -> &'static str {
        match self {
            CompatRunRefusal::Compat(CompatRefusal::ViewStale { .. }) => "view_stale",
            CompatRunRefusal::Compat(CompatRefusal::ActiveVersionNotWritable { .. }) => {
                "active_version_not_writable"
            }
            CompatRunRefusal::Compat(CompatRefusal::CompatRecordMissing { .. }) => {
                "compat_record_missing"
            }
            CompatRunRefusal::PublishLifetimeAboveCompat { .. } => "publish_lifetime_above_compat",
            CompatRunRefusal::TargetNotActive { .. } => "target_not_active",
            CompatRunRefusal::CompactionRecordNotActive { .. } => "compaction_record_not_active",
            CompatRunRefusal::FenceFailed { .. } => "fence_failed",
            CompatRunRefusal::PublishLifetimeExpired { .. } => "publish_lifetime_expired",
        }
    }
}

static REFUSED: LazyLock<Mutex<BTreeMap<(String, &'static str), u64>>> =
    LazyLock::new(Default::default);

/// Counter seam for `ravel_compat_publish_refused_total{family,reason}`:
/// maintenance publishes this process refused under the compat policy.
/// Process-wide and monotonic.
pub fn compat_publish_refused_total(family: &str, reason: &str) -> u64 {
    let map = REFUSED.lock().unwrap_or_else(PoisonError::into_inner);
    map.iter()
        .filter(|((f, r), _)| f == family && *r == reason)
        .map(|(_, n)| *n)
        .sum()
}

/// Every nonzero `(family, reason, count)` of
/// `ravel_compat_publish_refused_total`, for an exporter.
pub fn compat_publish_refused_snapshot() -> Vec<(String, &'static str, u64)> {
    let map = REFUSED.lock().unwrap_or_else(PoisonError::into_inner);
    map.iter()
        .map(|((family, reason), n)| (family.clone(), *reason, *n))
        .collect()
}

/// Count, log and wrap a refusal.
pub(crate) fn refuse(
    run: &'static str,
    family: &str,
    bucket: Option<&(TenantHash, Signal)>,
    refusal: CompatRunRefusal,
) -> MaintainError {
    {
        let mut map = REFUSED.lock().unwrap_or_else(PoisonError::into_inner);
        *map.entry((family.to_string(), refusal.reason()))
            .or_default() += 1;
    }
    tracing::warn!(
        run,
        family,
        reason = refusal.reason(),
        tenant_hash = bucket.map(|(t, _)| t.to_hex()),
        signal = ?bucket.map(|(_, s)| *s),
        error = %refusal,
        "maintenance: refusing to publish under the sys/compat write policy; nothing \
         further written (ADR-4233)"
    );
    MaintainError::CompatRefused {
        run,
        family: family.to_string(),
        refusal,
    }
}

/// The versions one run writes, fixed at its start, and the fence it owes
/// before its first PUT above a baseline. Cheap to clone; clones share the
/// fence state.
#[derive(Clone)]
pub struct CompatRun {
    inner: Arc<RunInner>,
}

struct RunInner {
    policy: MaintainCompatPolicy,
    run: &'static str,
    started_unix_ns: i64,
    versions: Vec<(&'static str, u16)>,
    bucket: Option<(TenantHash, Signal)>,
    fence: Option<PendingFence>,
}

struct PendingFence {
    tenant_hash: TenantHash,
    signal: Signal,
    compat_epoch: u64,
    raised: AsyncMutex<bool>,
}

impl std::fmt::Debug for CompatRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompatRun")
            .field("run", &self.inner.run)
            .field("started_unix_ns", &self.inner.started_unix_ns)
            .field("versions", &self.inner.versions)
            .field(
                "fence_epoch",
                &self.inner.fence.as_ref().map(|f| f.compat_epoch),
            )
            .finish()
    }
}

impl CompatRun {
    /// The version this run writes for `family`, if the run covers it.
    pub fn version(&self, family: &str) -> Option<u16> {
        self.inner
            .versions
            .iter()
            .find(|(f, _)| *f == family)
            .map(|(_, v)| *v)
    }

    pub fn started_unix_ns(&self) -> i64 {
        self.inner.started_unix_ns
    }

    pub fn policy(&self) -> &MaintainCompatPolicy {
        &self.inner.policy
    }

    /// The epoch this run raises the bucket's fence to before its first PUT,
    /// if it owes one.
    pub fn fence_epoch(&self) -> Option<u64> {
        self.inner.fence.as_ref().map(|f| f.compat_epoch)
    }

    fn primary_family(&self) -> &'static str {
        self.inner
            .versions
            .first()
            .map(|(f, _)| *f)
            .unwrap_or_default()
    }

    fn refuse(&self, refusal: CompatRunRefusal) -> MaintainError {
        refuse(
            self.inner.run,
            self.primary_family(),
            self.inner.bucket.as_ref(),
            refusal,
        )
    }

    /// Run before every PUT attempt of a data object or record, retries
    /// included: refuse when the view is at or past the horizon, then raise
    /// the bucket's fence if this run owes one and has not raised it yet.
    pub(crate) async fn before_put(&self, store: &dyn ObjectStoreBackend) -> Result<()> {
        let policy = &self.inner.policy;
        let now = policy.now_unix_ns();
        let last = policy.view.last_read_unix_ns();
        let fresh = last.is_some_and(|last| now.saturating_sub(last) < COMPAT_VIEW_HORIZON_NS);
        if !fresh {
            return Err(self.refuse(CompatRunRefusal::Compat(CompatRefusal::ViewStale {
                last_read_unix_ns: last,
                now_unix_ns: now,
            })));
        }
        let Some(fence) = self.inner.fence.as_ref() else {
            return Ok(());
        };
        let mut raised = fence.raised.lock().await;
        if *raised {
            return Ok(());
        }
        match raise_compat_fence(
            store,
            &fence.tenant_hash,
            fence.signal,
            fence.compat_epoch,
            now,
        )
        .await
        {
            Ok(outcome) => {
                if let CompatFenceOutcome::Raised { previous_epoch, .. } = outcome {
                    tracing::info!(
                        run = self.inner.run,
                        tenant_hash = %fence.tenant_hash.to_hex(),
                        signal = ?fence.signal,
                        previous_epoch,
                        compat_epoch = fence.compat_epoch,
                        "maintenance: raised the bucket's compat fence before its first \
                         publish above the baseline (ADR-4233 decision 4)"
                    );
                }
                *raised = true;
                Ok(())
            }
            Err(err) => Err(self.refuse(CompatRunRefusal::FenceFailed {
                compat_epoch: fence.compat_epoch,
                detail: err.to_string(),
            })),
        }
    }

    /// Run before a record PUT by a writer bound to `P` from its start (the
    /// audit and legal-hold writers): abandon once `P` has passed.
    pub(crate) fn before_record_put(&self) -> Result<()> {
        let policy = &self.inner.policy;
        let elapsed_ns = policy
            .now_unix_ns()
            .saturating_sub(self.inner.started_unix_ns);
        let lifetime_ns = policy.publish_lifetime_ns();
        if elapsed_ns >= lifetime_ns {
            return Err(self.refuse(CompatRunRefusal::PublishLifetimeExpired {
                elapsed_ns,
                lifetime_ns,
            }));
        }
        Ok(())
    }

    /// Refuse a run whose version choice is incompatible with what it was
    /// asked to write: counted and logged like every other refusal.
    pub(crate) fn refuse_with(&self, family: &str, refusal: CompatRunRefusal) -> MaintainError {
        refuse(self.inner.run, family, self.inner.bucket.as_ref(), refusal)
    }
}

/// [`CompatRun::before_put`] on an optional run; a no-op without one.
pub(crate) async fn before_put(
    compat: Option<&CompatRun>,
    store: &dyn ObjectStoreBackend,
) -> Result<()> {
    match compat {
        Some(run) => run.before_put(store).await,
        None => Ok(()),
    }
}

/// Start a run that writes `families` into `bucket`, before its claim: a
/// no-op returning `None` when `config` carries no policy. Reads the
/// bucket's provisioning record fresh, under [`RequestPhase::FloorRead`].
pub(crate) async fn begin_bucket_run(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    run: &'static str,
    families: &[&'static str],
) -> Result<Option<CompatRun>> {
    let Some(policy) = config.compat_policy.as_ref() else {
        return Ok(None);
    };
    if let Some(outer) = config.compat_run.as_ref()
        && families.iter().all(|family| outer.version(family).is_some())
    {
        return Ok(Some(outer.clone()));
    }
    let key = provisioning_key(&bucket.tenant_hash, bucket.signal);
    let got = store.get(&key, GetRange::Full).await;
    note_get(config.request_ledger.as_ref(), RequestPhase::FloorRead, &got);
    let record = match got {
        Ok(outcome) => Some(
            sysproto::ProvisioningRecord::decode(outcome.data.as_ref())
                .map_err(|err| MaintainError::FormatFloorRead(format!("{key}: {err}")))?,
        ),
        Err(StoreError::NotFound) => None,
        Err(err) => return Err(MaintainError::Store(err)),
    };
    policy
        .begin(
            run,
            families,
            Some((bucket.tenant_hash, bucket.signal)),
            record.as_ref(),
            config.max_compaction_lifetime_ns,
        )
        .map(Some)
}

/// [`begin_bucket_run`] for a writer of `bucket`'s part family alone, the
/// shape compaction uses.
pub(crate) async fn begin_part_run(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    run: &'static str,
) -> Result<Option<CompatRun>> {
    match part_family(bucket.signal) {
        Some(family) => begin_bucket_run(store, config, bucket, run, &[family]).await,
        None => Ok(None),
    }
}

/// Refuse a `migrate` of `family` toward `target` before its walk when a policy
/// is installed and `target` is not the family's active write version (ADR-4233
/// decision 6). Each bucket's run checks again before its claim.
pub(crate) fn require_active_target(
    config: &CompactorConfig,
    family: &str,
    target: u32,
) -> Result<()> {
    let Some(policy) = config.compat_policy.as_ref() else {
        return Ok(());
    };
    let (writable, baseline) = policy
        .versions(family)
        .map_or((&[][..], 0), |v| (v.writable.as_slice(), v.baseline));
    let active = policy
        .view
        .write_policy(policy.now_unix_ns(), family, writable, baseline, None)
        .map_err(|refusal| refuse("migration", family, None, CompatRunRefusal::Compat(refusal)))?;
    if u32::from(active) != target {
        return Err(refuse(
            "migration",
            family,
            None,
            CompatRunRefusal::TargetNotActive { target, active },
        ));
    }
    Ok(())
}

/// The version `config`'s run in flight writes for `family`; `None` when no
/// run was started under a policy.
pub(crate) fn run_version(config: &CompactorConfig, family: &str) -> Option<u16> {
    config.compat_run.as_ref().and_then(|run| run.version(family))
}

/// The RSEG version a compaction part is written at: the run's, or the
/// build's current one with no policy.
pub(crate) fn rseg_write_version(config: &CompactorConfig) -> Result<ravel_segment::SegmentVersion> {
    match run_version(config, FAMILY_RSEG) {
        None => Ok(ravel_segment::SegmentVersion::CURRENT),
        Some(v) => ravel_segment::SegmentVersion::from_number(v).ok_or_else(|| {
            MaintainError::Invariant(format!("the compat run chose RSEG version {v}, outside the window"))
        }),
    }
}

/// The RLOG version a part is written at: the run's, or the build's current
/// one with no policy.
pub(crate) fn rlog_write_version(
    config: &CompactorConfig,
) -> Result<ravel_logseg::footer::WriteVersion> {
    match run_version(config, FAMILY_RLOG) {
        None => Ok(ravel_logseg::footer::WriteVersion::CURRENT),
        Some(v) => ravel_logseg::footer::WriteVersion::new(v).map_err(|err| {
            MaintainError::Invariant(format!("the compat run chose RLOG version {v}: {err}"))
        }),
    }
}

/// The RSPAN version a part is written at: the run's, or the build's current
/// one with no policy.
pub(crate) fn rspan_write_version(
    config: &CompactorConfig,
) -> Result<ravel_rspan::footer::WriteVersion> {
    match run_version(config, FAMILY_RSPAN) {
        None => Ok(ravel_rspan::footer::WriteVersion::CURRENT),
        Some(v) => ravel_rspan::footer::WriteVersion::new(v).map_err(|err| {
            MaintainError::Invariant(format!("the compat run chose RSPAN version {v}: {err}"))
        }),
    }
}

/// Hazard H6 (ADR-4233 decision 4): refuse to re-encode an input whose reader
/// skipped section kinds this build does not know, since the re-encode would
/// drop them. The caller holds the bucket.
pub(crate) fn refuse_unknown_sections(
    object_key: &str,
    unknown_section_kinds: &[u32],
    recorded_version: Option<u32>,
) -> Result<()> {
    if unknown_section_kinds.is_empty() {
        return Ok(());
    }
    Err(MaintainError::UnknownSectionKinds {
        object_key: object_key.to_string(),
        kinds: unknown_section_kinds.to_vec(),
        recorded_version,
    })
}
