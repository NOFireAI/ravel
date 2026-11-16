//! `compat activate` (ADR-4233 decisions 3 and 4): raise one family's active
//! write version by exactly one, then fence every existing bucket eagerly.
//!
//! [`activate`] refuses before any write when the store is unkeyed, when the
//! operator has not attested that every writer role can read `sys/compat`,
//! when the version is not one this build writes, and when it is not exactly
//! one above the active version. The version and the store-wide
//! `compat_epoch` move together in one `sys/compat` CAS
//! ([`ravel_catalog::compat::activate_family`]); a lost race re-reads and
//! re-checks, so activations of different families compose and none lowers a
//! version.
//!
//! The fence walk then raises every provisioning record to the family's
//! `activated_epoch` and, for `rlog`, parks every discoverable tenant's alert
//! lease at it. Both steps are idempotent. The walk stops at its first
//! failure and returns a [`FenceCursor`] naming the item it could not finish;
//! `fence_complete_unix_ns` is written only after a walk with no failure. A
//! family whose fence is still pending resumes the walk on the next
//! `activate` instead of activating again, so a crash at any point converges.

use std::collections::{BTreeMap, BTreeSet};

use ravel_catalog::compat::{
    FAMILY_CATALOG_HEAD, FAMILY_CSNAP, FAMILY_CSTAT, FAMILY_KIDX, FAMILY_NPOST,
    FAMILY_RETENTION_TOMBSTONE, FAMILY_RLOG, KNOWN_FAMILIES, activate_family, read_compat,
    update_compat,
};
use ravel_catalog::{
    CompatClock, CompatError, CompatFenceOutcome, CompatKeys, ProvisioningError,
    fold_write_version, raise_compat_fence,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{Signal, TenantHash};

use crate::compat_init::{SIGNALS, StrictListError, list_tenants_strict};
use crate::compat_policy::FamilyVersions;

/// How many times [`activate`] re-reads and retries after losing a
/// `sys/compat` CAS race.
pub const ACTIVATE_MAX_ATTEMPTS: u32 = 8;

/// The versions this build writes, per family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableVersions {
    families: BTreeMap<String, Vec<u32>>,
}

impl WritableVersions {
    /// This build's table: the maintenance families from [`FamilyVersions`],
    /// the catalog families from the fold's write version for any signal,
    /// and the retention tombstone at its single format version.
    pub fn current() -> WritableVersions {
        let mut families = BTreeMap::new();
        for family in KNOWN_FAMILIES {
            let versions: BTreeSet<u32> = match family {
                FAMILY_CATALOG_HEAD | FAMILY_CSNAP | FAMILY_NPOST | FAMILY_CSTAT | FAMILY_KIDX => {
                    SIGNALS
                        .into_iter()
                        .filter_map(|s| fold_write_version(family, s))
                        .map(u32::from)
                        .collect()
                }
                FAMILY_RETENTION_TOMBSTONE => {
                    BTreeSet::from([ravel_commit::record::TOMBSTONE_FORMAT_VERSION])
                }
                _ => FamilyVersions::for_family(family)
                    .map(|v| v.writable.into_iter().map(u32::from).collect())
                    .unwrap_or_default(),
            };
            families.insert(family.to_string(), versions.into_iter().collect());
        }
        WritableVersions { families }
    }

    /// Replace one family's writable set.
    pub fn with_family(mut self, family: &str, versions: Vec<u32>) -> WritableVersions {
        self.families.insert(family.to_string(), versions);
        self
    }

    /// The writable set for `family`; empty for a family this build never
    /// writes.
    pub fn versions(&self, family: &str) -> &[u32] {
        self.families.get(family).map_or(&[], Vec::as_slice)
    }
}

/// One `compat activate` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateRequest {
    pub family: String,
    pub version: u32,
    /// Recorded as the family's `activated_by`.
    pub activated_by: String,
    /// The operator's attestation that every writer role's credentials can
    /// read `sys/compat` (ADR-4233 decision 3). `false` refuses.
    pub writer_roles_can_read_compat: bool,
    /// Tenants named by the alert rules file. Their leases are parked on an
    /// `rlog` activation even when no alert key exists for them yet.
    pub alert_rule_tenants: Vec<TenantHash>,
    /// Where an interrupted walk stopped. Ignored unless it names this
    /// family and the epoch being walked.
    pub resume_from: Option<FenceCursor>,
}

/// The fence walk item a resumed walk starts at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FencePosition {
    Provisioning { tenant: TenantHash, signal: Signal },
    Leases { tenant: TenantHash },
}

impl FencePosition {
    fn rank(self) -> (u8, TenantHash, usize) {
        match self {
            FencePosition::Provisioning { tenant, signal } => (
                0,
                tenant,
                SIGNALS.iter().position(|s| *s == signal).unwrap_or(0),
            ),
            FencePosition::Leases { tenant } => (1, tenant, 0),
        }
    }
}

/// An advisory resume point. Every walk step is idempotent, so a lost cursor
/// only costs re-reading items already fenced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceCursor {
    pub compat_epoch: u64,
    pub position: FencePosition,
}

/// Why a fence walk stopped.
#[derive(Debug, thiserror::Error)]
pub enum FenceFailure {
    #[error(transparent)]
    Listing(#[from] StrictListError),
    #[error("tenant {} signal {signal:?}: provisioning fence: {source}", tenant.to_hex())]
    Provisioning {
        tenant: TenantHash,
        signal: Signal,
        #[source]
        source: ProvisioningError,
    },
    #[error("tenant {}: alert lease: {source}", tenant.to_hex())]
    Lease {
        tenant: TenantHash,
        #[source]
        source: ravel_alerting::LeaseError,
    },
    #[error("recording fence_complete_unix_ns: {0}")]
    FenceComplete(#[source] CompatError),
}

/// What one fence walk did.
#[derive(Debug, Default)]
pub struct FenceWalkReport {
    /// The family's `activated_epoch`, the epoch every bucket and lease is
    /// raised to.
    pub compat_epoch: u64,
    pub tenants: u64,
    pub buckets_raised: u64,
    pub buckets_already_fenced: u64,
    /// (tenant, signal) pairs with no provisioning record.
    pub buckets_unprovisioned: u64,
    pub leases_created: u64,
    pub leases_replaced: u64,
    pub leases_already_parked: u64,
    /// Every item was fenced.
    pub complete: bool,
    /// The value this walk wrote to `fence_complete_unix_ns`; `None` when the
    /// walk failed or a later activation of the family superseded it.
    pub fence_complete_unix_ns: Option<i64>,
    /// Where to resume after a failure; `None` means from the start.
    pub resume_from: Option<FenceCursor>,
    pub failure: Option<FenceFailure>,
}

/// Whether [`activate`] activated a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivateOutcome {
    /// The family moved from `previous_version` to `version`.
    Activated { previous_version: u32, version: u32 },
    /// The family's previous activation had not finished its fence walk;
    /// that walk ran and nothing was activated.
    ResumedPendingWalk,
}

/// The result of an [`activate`] that wrote no refusal.
#[derive(Debug)]
pub struct ActivateReport {
    pub outcome: ActivateOutcome,
    pub family: String,
    /// The family's active write version after the call.
    pub active_write_version: u32,
    pub walk: FenceWalkReport,
}

/// Why [`activate`] refused. Every refusal is raised before any write.
#[derive(Debug, thiserror::Error)]
pub enum ActivateError {
    #[error(
        "no file keys configured: sys/compat is MACed under the file keys, so an unkeyed store cannot be activated"
    )]
    Unkeyed,
    #[error(
        "writer roles are not attested to read sys/compat; a writer that cannot read it would never see the activation"
    )]
    WriterRolesCannotReadCompat,
    #[error("sys/compat is absent; run compat init first")]
    RecordAbsent,
    #[error("family {family} has no entry in sys/compat")]
    FamilyNotInRecord { family: String },
    #[error("this build does not write {family} version {version}; it writes {writable:?}")]
    NotWritable {
        family: String,
        version: u32,
        writable: Vec<u32>,
    },
    #[error("{family} version {requested} is not above the active version {active}")]
    NotAboveActive {
        family: String,
        active: u32,
        requested: u32,
    },
    #[error("{family} version {requested} skips past active version {active} + 1")]
    VersionJump {
        family: String,
        active: u32,
        requested: u32,
    },
    #[error("sys/compat: {attempts} activation attempts each lost a CAS race")]
    Contended { attempts: u32 },
    #[error(transparent)]
    Compat(#[from] CompatError),
}

fn family_entry<'a>(
    record: &'a sysproto::CompatRecord,
    family: &str,
) -> Result<&'a sysproto::CompatFamily, ActivateError> {
    record
        .families
        .iter()
        .find(|f| f.family == family)
        .ok_or_else(|| ActivateError::FamilyNotInRecord {
            family: family.to_string(),
        })
}

/// The family's last activation has not recorded a finished fence walk.
fn fence_pending(entry: &sysproto::CompatFamily) -> bool {
    entry.activated_epoch > 0
        && (entry.fence_complete_unix_ns == 0
            || entry.fence_complete_unix_ns < entry.activated_unix_ns)
}

/// Activate `request.version` for `request.family`, or resume the family's
/// pending fence walk, then walk.
pub async fn activate(
    store: &dyn ObjectStoreBackend,
    keys: Option<&CompatKeys>,
    clock: &dyn CompatClock,
    writable: &WritableVersions,
    request: &ActivateRequest,
) -> Result<ActivateReport, ActivateError> {
    let keys = keys.ok_or(ActivateError::Unkeyed)?;
    if !request.writer_roles_can_read_compat {
        return Err(ActivateError::WriterRolesCannotReadCompat);
    }
    let family = request.family.as_str();
    let mut attempts = 0;
    let (outcome, entry) = loop {
        let Some((record, _)) = read_compat(store, keys).await? else {
            return Err(ActivateError::RecordAbsent);
        };
        let entry = family_entry(&record, family)?;
        if fence_pending(entry) {
            break (ActivateOutcome::ResumedPendingWalk, entry.clone());
        }
        let allowed = writable.versions(family);
        if !allowed.contains(&request.version) {
            return Err(ActivateError::NotWritable {
                family: family.to_string(),
                version: request.version,
                writable: allowed.to_vec(),
            });
        }
        let active = entry.active_write_version;
        if request.version <= active {
            return Err(ActivateError::NotAboveActive {
                family: family.to_string(),
                active,
                requested: request.version,
            });
        }
        if active.checked_add(1) != Some(request.version) {
            return Err(ActivateError::VersionJump {
                family: family.to_string(),
                active,
                requested: request.version,
            });
        }
        attempts += 1;
        match activate_family(
            store,
            keys,
            family,
            request.version,
            &request.activated_by,
            clock.now_unix_ns(),
        )
        .await
        {
            Ok((record, _)) => {
                let entry = family_entry(&record, family)?.clone();
                break (
                    ActivateOutcome::Activated {
                        previous_version: active,
                        version: request.version,
                    },
                    entry,
                );
            }
            Err(err) if err.is_retryable() && attempts < ACTIVATE_MAX_ATTEMPTS => {
                tracing::info!(family, attempts, error = %err, "compat activate: retrying");
            }
            Err(err) if err.is_retryable() => {
                return Err(ActivateError::Contended { attempts });
            }
            Err(err) => return Err(err.into()),
        }
    };

    let walk = fence_walk(store, keys, clock, family, &entry, request).await;
    Ok(ActivateReport {
        outcome,
        family: family.to_string(),
        active_write_version: entry.active_write_version,
        walk,
    })
}

/// Fence every provisioned bucket (and, for `rlog`, every discoverable alert
/// lease) at `entry.activated_epoch`, then record `fence_complete_unix_ns`.
async fn fence_walk(
    store: &dyn ObjectStoreBackend,
    keys: &CompatKeys,
    clock: &dyn CompatClock,
    family: &str,
    entry: &sysproto::CompatFamily,
    request: &ActivateRequest,
) -> FenceWalkReport {
    let epoch = entry.activated_epoch;
    let mut report = FenceWalkReport {
        compat_epoch: epoch,
        ..FenceWalkReport::default()
    };
    let resume = request
        .resume_from
        .filter(|c| c.compat_epoch == epoch)
        .map(|c| c.position);
    let before_resume = |pos: FencePosition| resume.is_some_and(|r| pos.rank() < r.rank());
    let fail = |report: &mut FenceWalkReport, at: Option<FencePosition>, failure: FenceFailure| {
        report.resume_from = at.or(resume).map(|position| FenceCursor {
            compat_epoch: epoch,
            position,
        });
        report.failure = Some(failure);
    };

    let tenants = match list_tenants_strict(store).await {
        Ok(tenants) => tenants,
        Err(err) => {
            fail(&mut report, None, err.into());
            return report;
        }
    };
    report.tenants = tenants.len() as u64;

    for tenant in &tenants {
        for signal in SIGNALS {
            let pos = FencePosition::Provisioning {
                tenant: *tenant,
                signal,
            };
            if before_resume(pos) {
                continue;
            }
            match raise_compat_fence(store, tenant, signal, epoch, clock.now_unix_ns()).await {
                Ok(CompatFenceOutcome::Raised { .. }) => report.buckets_raised += 1,
                Ok(CompatFenceOutcome::AlreadyFenced { .. }) => report.buckets_already_fenced += 1,
                Err(ProvisioningError::NoRecordToFence { .. }) => {
                    report.buckets_unprovisioned += 1;
                }
                Err(source) => {
                    let failure = FenceFailure::Provisioning {
                        tenant: *tenant,
                        signal,
                        source,
                    };
                    fail(&mut report, Some(pos), failure);
                    return report;
                }
            }
        }
    }

    if family == FAMILY_RLOG {
        let mut lease_tenants: BTreeSet<TenantHash> =
            request.alert_rule_tenants.iter().copied().collect();
        for tenant in &tenants {
            if before_resume(FencePosition::Leases { tenant: *tenant }) {
                continue;
            }
            match has_alert_prefix(store, tenant).await {
                Ok(true) => {
                    lease_tenants.insert(*tenant);
                }
                Ok(false) => {}
                Err(err) => {
                    fail(
                        &mut report,
                        Some(FencePosition::Leases { tenant: *tenant }),
                        err.into(),
                    );
                    return report;
                }
            }
        }
        for tenant in &lease_tenants {
            let pos = FencePosition::Leases { tenant: *tenant };
            if before_resume(pos) {
                continue;
            }
            match ravel_alerting::park_lease(store, tenant, epoch).await {
                Ok(ravel_alerting::ParkOutcome::Created) => report.leases_created += 1,
                Ok(ravel_alerting::ParkOutcome::Replaced) => report.leases_replaced += 1,
                Ok(ravel_alerting::ParkOutcome::AlreadyParked { .. }) => {
                    report.leases_already_parked += 1;
                }
                Err(source) => {
                    let failure = FenceFailure::Lease {
                        tenant: *tenant,
                        source,
                    };
                    fail(&mut report, Some(pos), failure);
                    return report;
                }
            }
        }
    }

    match record_fence_complete(store, keys, clock, family, epoch).await {
        Ok(written) => {
            report.complete = true;
            report.fence_complete_unix_ns = written;
        }
        Err(err) => {
            // Every item is fenced; a resumed walk re-reads them all and
            // retries only this write.
            fail(&mut report, None, FenceFailure::FenceComplete(err));
        }
    }
    report
}

/// Whether a strict listing of `t/<h>/` finds the alert prefix `t/<h>/a/`.
async fn has_alert_prefix(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<bool, StrictListError> {
    let prefix = format!("t/{}/", tenant.to_hex());
    let listing = store
        .list_delimited(&prefix)
        .await
        .map_err(|source| StrictListError::Store {
            prefix: prefix.clone(),
            source,
        })?;
    let unaddressable = listing
        .unaddressable
        .first()
        .map(|k| k.key.clone())
        .or_else(|| listing.unaddressable_prefixes.first().cloned());
    if let Some(key) = unaddressable {
        return Err(StrictListError::Unaddressable {
            prefix,
            key,
            count: (listing.unaddressable.len() + listing.unaddressable_prefixes.len()) as u64,
        });
    }
    let alerts = format!("{prefix}{}/", Signal::Alerts.key_prefix());
    Ok(listing.common_prefixes.contains(&alerts))
}

/// Set the family's `fence_complete_unix_ns` when its activation is still
/// the one at `epoch`. `Ok(None)` when a later activation superseded it.
async fn record_fence_complete(
    store: &dyn ObjectStoreBackend,
    keys: &CompatKeys,
    clock: &dyn CompatClock,
    family: &str,
    epoch: u64,
) -> Result<Option<i64>, CompatError> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let Some((record, _)) = read_compat(store, keys).await? else {
            return Err(CompatError::RecordAbsent {
                key: ravel_catalog::COMPAT_KEY.to_string(),
            });
        };
        let Some(entry) = record.families.iter().find(|f| f.family == family) else {
            return Err(CompatError::FamilyNotInRecord {
                family: family.to_string(),
            });
        };
        if entry.activated_epoch != epoch {
            return Ok(None);
        }
        if !fence_pending(entry) {
            return Ok(Some(entry.fence_complete_unix_ns));
        }
        let complete_ns = clock.now_unix_ns().max(entry.activated_unix_ns);
        let result = update_compat(store, keys, |record| {
            if let Some(entry) = record
                .families
                .iter_mut()
                .find(|f| f.family == family && f.activated_epoch == epoch)
            {
                entry.fence_complete_unix_ns = entry.fence_complete_unix_ns.max(complete_ns);
            }
            Ok(())
        })
        .await;
        match result {
            Ok(_) => return Ok(Some(complete_ns)),
            Err(err) if err.is_retryable() && attempts < ACTIVATE_MAX_ATTEMPTS => {}
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::compat_init::tests::{
        T0, TestClock, config, provision, seed_metrics, tenant, test_keys,
    };
    use crate::compat_init::{InitOutcome, init_compat};
    use bytes::Bytes;
    use ravel_alerting::{AlertLease, alert_lease_key};
    use ravel_catalog::COMPAT_KEY;
    use ravel_catalog::compat::FAMILY_RSEG;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, PutOptions};
    use std::sync::Arc;

    async fn initialized() -> Arc<MemoryStore> {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        // Tenant 1: metrics and logs provisioned, an unparked lease held by
        // an evaluator. Tenant 2: metrics only, no alert keys. Tenant 3: logs
        // only, alert state but no lease.
        seed_metrics(mem.as_ref(), tenant(1)).await;
        provision(mem.as_ref(), tenant(1), Signal::Logs).await;
        seed_metrics(mem.as_ref(), tenant(2)).await;
        provision(mem.as_ref(), tenant(3), Signal::Logs).await;
        let lease = AlertLease::unparked("evaluator-a".to_string(), T0 + 1_000, 0);
        mem.put(
            &alert_lease_key(&tenant(1)),
            lease.to_bytes().expect("lease bytes"),
            PutOptions::default(),
        )
        .await
        .expect("put lease");
        mem.put(
            &format!("t/{}/a/state/rule-1", tenant(3).to_hex()),
            Bytes::from_static(b"{}"),
            PutOptions::default(),
        )
        .await
        .expect("put alert state");
        let report = init_compat(mem.as_ref(), Some(&test_keys()), &TestClock(T0), &config())
            .await
            .expect("init");
        assert_eq!(report.outcome, InitOutcome::Created);
        mem
    }

    fn baseline(family: &str) -> u32 {
        crate::compat_init::baseline_version(family).expect("baseline")
    }

    fn writable_next(family: &str) -> WritableVersions {
        WritableVersions::current()
            .with_family(family, vec![baseline(family), baseline(family) + 1])
    }

    fn request(family: &str) -> ActivateRequest {
        ActivateRequest {
            family: family.to_string(),
            version: baseline(family) + 1,
            activated_by: "test".to_string(),
            writer_roles_can_read_compat: true,
            // Tenant 4 is named only by the rules file and has no keys.
            alert_rule_tenants: vec![tenant(4)],
            resume_from: None,
        }
    }

    async fn entry(store: &dyn ObjectStoreBackend, family: &str) -> sysproto::CompatFamily {
        let (record, _) = read_compat(store, &test_keys())
            .await
            .expect("read")
            .expect("present");
        family_entry(&record, family).expect("entry").clone()
    }

    /// Advances one nanosecond per reading, so a test can order the walk's
    /// writes by the times they stamped.
    struct StepClock(std::sync::atomic::AtomicI64);

    impl CompatClock for StepClock {
        fn now_unix_ns(&self) -> i64 {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }
    }

    async fn prov_record(
        store: &dyn ObjectStoreBackend,
        t: TenantHash,
        signal: Signal,
    ) -> sysproto::ProvisioningRecord {
        use prost::Message;
        let got = store
            .get(&ravel_catalog::provisioning_key(&t, signal), GetRange::Full)
            .await
            .expect("prov");
        sysproto::ProvisioningRecord::decode(got.data.as_ref()).expect("decode prov")
    }

    async fn prov_epoch(store: &dyn ObjectStoreBackend, t: TenantHash, signal: Signal) -> u64 {
        prov_record(store, t, signal).await.compat_epoch
    }

    async fn lease(store: &dyn ObjectStoreBackend, t: TenantHash) -> Option<AlertLease> {
        match store.get(&alert_lease_key(&t), GetRange::Full).await {
            Ok(got) => Some(AlertLease::from_slice(got.data.as_ref()).expect("lease")),
            Err(ravel_object_store::StoreError::NotFound) => None,
            Err(err) => panic!("lease get: {err}"),
        }
    }

    fn no_put_store(inner: Arc<MemoryStore>) -> FaultStore<Arc<MemoryStore>> {
        FaultStore::new(
            inner,
            FaultPlan::empty().with_rule(Rule::new(
                Op::Put,
                ScriptedFault::Permanent("refusal tests write nothing".into()),
            )),
        )
    }

    #[tokio::test]
    async fn activation_walk_fences_every_bucket_parks_absent_leases_then_sets_fence_complete() {
        let mem = initialized().await;
        let now = T0 + 10;

        let report = activate(
            mem.as_ref(),
            Some(&test_keys()),
            &StepClock(now.into()),
            &writable_next(FAMILY_RLOG),
            &request(FAMILY_RLOG),
        )
        .await
        .expect("activate");

        assert_eq!(
            report.outcome,
            ActivateOutcome::Activated {
                previous_version: baseline(FAMILY_RLOG),
                version: baseline(FAMILY_RLOG) + 1,
            }
        );
        let walk = &report.walk;
        assert!(walk.failure.is_none(), "{:?}", walk.failure);
        assert!(walk.complete);
        assert_eq!(walk.compat_epoch, 1);
        assert_eq!(walk.tenants, 3);
        // (1, metrics), (1, logs), (2, metrics), (3, logs).
        assert_eq!(walk.buckets_raised, 4);
        assert_eq!(walk.buckets_already_fenced, 0);
        assert_eq!(walk.buckets_unprovisioned, 3 * SIGNALS.len() as u64 - 4);
        // Tenant 1's lease replaced; tenants 3 (alert state) and 4 (rules
        // file) created parked; tenant 2 has no alert keys.
        assert_eq!(walk.leases_replaced, 1);
        assert_eq!(walk.leases_created, 2);
        assert_eq!(walk.leases_already_parked, 0);
        assert_eq!(walk.resume_from, None);
        let fence_complete = walk.fence_complete_unix_ns.expect("fence complete");

        // fence_complete is stamped after the last bucket fence.
        let mut last_fenced = 0;
        for (t, signal) in [
            (tenant(1), Signal::Metrics),
            (tenant(1), Signal::Logs),
            (tenant(2), Signal::Metrics),
            (tenant(3), Signal::Logs),
        ] {
            let prov = prov_record(mem.as_ref(), t, signal).await;
            assert_eq!(prov.compat_epoch, 1, "{t:?} {signal:?}");
            assert!(prov.compat_fenced_unix_ns > now, "{t:?} {signal:?}");
            last_fenced = last_fenced.max(prov.compat_fenced_unix_ns);
        }
        assert!(
            fence_complete > last_fenced,
            "fence_complete {fence_complete} not after the last fence {last_fenced}"
        );
        for t in [tenant(1), tenant(3), tenant(4)] {
            let parked = lease(mem.as_ref(), t).await.expect("lease present");
            assert!(parked.is_parked(), "{t:?}");
            assert_eq!(parked.compat_epoch, 1);
        }
        assert_eq!(lease(mem.as_ref(), tenant(2)).await, None);

        let rlog = entry(mem.as_ref(), FAMILY_RLOG).await;
        assert_eq!(rlog.active_write_version, baseline(FAMILY_RLOG) + 1);
        assert_eq!(rlog.activated_epoch, 1);
        assert_eq!(rlog.activated_unix_ns, now);
        assert_eq!(rlog.activated_by, "test");
        assert_eq!(rlog.fence_complete_unix_ns, fence_complete);
    }

    #[tokio::test]
    async fn non_rlog_activation_fences_buckets_and_leaves_leases() {
        let mem = initialized().await;
        let report = activate(
            mem.as_ref(),
            Some(&test_keys()),
            &TestClock(T0 + 10),
            &writable_next(FAMILY_RSEG),
            &request(FAMILY_RSEG),
        )
        .await
        .expect("activate");
        assert!(report.walk.complete);
        assert_eq!(report.walk.buckets_raised, 4);
        assert_eq!(report.walk.leases_created + report.walk.leases_replaced, 0);
        assert!(
            !lease(mem.as_ref(), tenant(1))
                .await
                .expect("lease")
                .is_parked()
        );
        assert_eq!(lease(mem.as_ref(), tenant(4)).await, None);
    }

    #[tokio::test]
    async fn crash_mid_walk_resumes_from_the_cursor_and_only_then_sets_fence_complete() {
        let mem = initialized().await;
        // The third provisioning write (tenant 2, metrics) fails.
        let store = FaultStore::new(
            mem.clone(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::Transient("prov down".into()))
                    .with_key_contains("/prov")
                    .with_occurrence(Occurrence::Nth(3)),
            ),
        );
        let first = activate(
            &store,
            Some(&test_keys()),
            &TestClock(T0 + 10),
            &writable_next(FAMILY_RLOG),
            &request(FAMILY_RLOG),
        )
        .await
        .expect("activate");

        assert_eq!(store.fault_count(Op::Put, FaultKind::Transient), 1);
        let walk = &first.walk;
        assert!(!walk.complete);
        assert!(
            matches!(
                walk.failure,
                Some(FenceFailure::Provisioning {
                    signal: Signal::Metrics,
                    ..
                })
            ),
            "{:?}",
            walk.failure
        );
        let cursor = walk.resume_from.expect("cursor");
        assert_eq!(
            cursor,
            FenceCursor {
                compat_epoch: 1,
                position: FencePosition::Provisioning {
                    tenant: tenant(2),
                    signal: Signal::Metrics,
                },
            }
        );
        assert_eq!(walk.buckets_raised, 2);
        assert_eq!(walk.fence_complete_unix_ns, None);
        assert_eq!(
            entry(mem.as_ref(), FAMILY_RLOG)
                .await
                .fence_complete_unix_ns,
            0
        );
        assert_eq!(
            prov_epoch(mem.as_ref(), tenant(2), Signal::Metrics).await,
            0
        );
        assert_eq!(lease(mem.as_ref(), tenant(4)).await, None);

        // A fresh process repeats the request with the cursor: the pending
        // walk resumes instead of activating a second time.
        let resumed = activate(
            &store,
            Some(&test_keys()),
            &TestClock(T0 + 20),
            &writable_next(FAMILY_RLOG),
            &ActivateRequest {
                resume_from: Some(cursor),
                ..request(FAMILY_RLOG)
            },
        )
        .await
        .expect("resume");
        assert_eq!(resumed.outcome, ActivateOutcome::ResumedPendingWalk);
        assert!(resumed.walk.complete, "{:?}", resumed.walk.failure);
        // Tenants 1's buckets sit before the cursor and are not revisited.
        assert_eq!(resumed.walk.buckets_raised, 2);
        assert_eq!(resumed.walk.buckets_already_fenced, 0);
        assert_eq!(resumed.walk.leases_created, 2);
        assert_eq!(resumed.walk.fence_complete_unix_ns, Some(T0 + 20));
        let rlog = entry(mem.as_ref(), FAMILY_RLOG).await;
        assert_eq!(rlog.active_write_version, baseline(FAMILY_RLOG) + 1);
        assert_eq!(rlog.activated_epoch, 1);
        assert_eq!(rlog.fence_complete_unix_ns, T0 + 20);
        assert_eq!(
            prov_epoch(mem.as_ref(), tenant(2), Signal::Metrics).await,
            1
        );
    }

    #[tokio::test]
    async fn lost_cursor_resume_rewalks_everything_idempotently() {
        let mem = initialized().await;
        let store = FaultStore::new(
            mem.clone(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Put, ScriptedFault::Transient("lease down".into()))
                    .with_key_contains("alert-lease")
                    .with_occurrence(Occurrence::Nth(1)),
            ),
        );
        let first = activate(
            &store,
            Some(&test_keys()),
            &TestClock(T0 + 10),
            &writable_next(FAMILY_RLOG),
            &request(FAMILY_RLOG),
        )
        .await
        .expect("activate");
        assert_eq!(store.fault_count(Op::Put, FaultKind::Transient), 1);
        assert!(matches!(
            first.walk.failure,
            Some(FenceFailure::Lease { .. })
        ));
        assert_eq!(
            entry(mem.as_ref(), FAMILY_RLOG)
                .await
                .fence_complete_unix_ns,
            0
        );

        let resumed = activate(
            &store,
            Some(&test_keys()),
            &TestClock(T0 + 20),
            &writable_next(FAMILY_RLOG),
            &request(FAMILY_RLOG),
        )
        .await
        .expect("resume");
        assert_eq!(resumed.outcome, ActivateOutcome::ResumedPendingWalk);
        assert!(resumed.walk.complete);
        assert_eq!(resumed.walk.buckets_raised, 0);
        assert_eq!(resumed.walk.buckets_already_fenced, 4);
        assert_eq!(
            resumed.walk.leases_created + resumed.walk.leases_replaced,
            3
        );
        assert_eq!(
            entry(mem.as_ref(), FAMILY_RLOG)
                .await
                .fence_complete_unix_ns,
            T0 + 20
        );
    }

    #[tokio::test]
    async fn unkeyed_store_refuses_before_any_write() {
        let mem = initialized().await;
        let store = no_put_store(mem.clone());
        let err = activate(
            &store,
            None,
            &TestClock(T0),
            &writable_next(FAMILY_RLOG),
            &request(FAMILY_RLOG),
        )
        .await
        .expect_err("unkeyed refuses");
        assert!(matches!(err, ActivateError::Unkeyed), "{err:?}");
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
        assert_eq!(entry(mem.as_ref(), FAMILY_RLOG).await.activated_epoch, 0);
    }

    #[tokio::test]
    async fn unattested_writer_roles_refuse_before_any_write() {
        let mem = initialized().await;
        let store = no_put_store(mem.clone());
        let err = activate(
            &store,
            Some(&test_keys()),
            &TestClock(T0),
            &writable_next(FAMILY_RLOG),
            &ActivateRequest {
                writer_roles_can_read_compat: false,
                ..request(FAMILY_RLOG)
            },
        )
        .await
        .expect_err("unattested refuses");
        assert!(
            matches!(err, ActivateError::WriterRolesCannotReadCompat),
            "{err:?}"
        );
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
    }

    #[tokio::test]
    async fn version_this_build_does_not_write_refuses() {
        let mem = initialized().await;
        let store = no_put_store(mem.clone());
        let err = activate(
            &store,
            Some(&test_keys()),
            &TestClock(T0),
            &WritableVersions::current().with_family(FAMILY_RLOG, vec![baseline(FAMILY_RLOG)]),
            &request(FAMILY_RLOG),
        )
        .await
        .expect_err("unwritable refuses");
        assert!(matches!(err, ActivateError::NotWritable { .. }), "{err:?}");
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
    }

    #[tokio::test]
    async fn activation_never_lowers_or_repeats_a_version() {
        let mem = initialized().await;
        let family = FAMILY_RLOG;
        let base = baseline(family);
        let writable = WritableVersions::current()
            .with_family(family, vec![base - 1, base, base + 1, base + 2]);
        let store = no_put_store(mem.clone());
        for (requested, lower) in [(base - 1, true), (base, true), (base + 2, false)] {
            let err = activate(
                &store,
                Some(&test_keys()),
                &TestClock(T0),
                &writable,
                &ActivateRequest {
                    version: requested,
                    ..request(family)
                },
            )
            .await
            .expect_err("refuses");
            if lower {
                assert!(
                    matches!(err, ActivateError::NotAboveActive { active, requested: r, .. } if active == base && r == requested),
                    "{err:?}"
                );
            } else {
                assert!(matches!(err, ActivateError::VersionJump { .. }), "{err:?}");
            }
        }
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
        let rlog = entry(mem.as_ref(), family).await;
        assert_eq!(rlog.active_write_version, base);
        assert_eq!(rlog.activated_epoch, 0);
    }

    #[tokio::test]
    async fn concurrent_activations_of_two_families_compose_through_cas_retry() {
        let mem = initialized().await;
        let store = FaultStore::new(mem.clone(), FaultPlan::empty());
        let gate = store.hold(Op::Put, Some(COMPAT_KEY.to_string()), Occurrence::Nth(1));
        let writable = writable_next(FAMILY_RSEG).with_family(
            FAMILY_RLOG,
            vec![baseline(FAMILY_RLOG), baseline(FAMILY_RLOG) + 1],
        );
        let keys = test_keys();

        // A reads the record at epoch 0 and is held at its CAS; B activates
        // rlog and finishes its walk; A's CAS then loses and A retries.
        let (clock_a, request_a) = (TestClock(T0 + 10), request(FAMILY_RSEG));
        let a = activate(&store, Some(&keys), &clock_a, &writable, &request_a);
        let b = async {
            gate.wait_until_held(1).await;
            let b = activate(
                &store,
                Some(&keys),
                &TestClock(T0 + 20),
                &writable,
                &request(FAMILY_RLOG),
            )
            .await;
            for id in gate.held() {
                assert!(gate.release(id));
            }
            b
        };
        let (a, b) = tokio::join!(a, b);
        let a = a.expect("rseg activation");
        let b = b.expect("rlog activation");

        assert!(a.walk.complete && b.walk.complete);
        assert_eq!(b.walk.compat_epoch, 1);
        assert_eq!(a.walk.compat_epoch, 2);
        let (record, _) = read_compat(mem.as_ref(), &keys)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(record.compat_epoch, 2);
        let rseg = family_entry(&record, FAMILY_RSEG).expect("rseg");
        let rlog = family_entry(&record, FAMILY_RLOG).expect("rlog");
        assert_eq!(rseg.active_write_version, baseline(FAMILY_RSEG) + 1);
        assert_eq!(rseg.activated_epoch, 2);
        assert_eq!(rlog.active_write_version, baseline(FAMILY_RLOG) + 1);
        assert_eq!(rlog.activated_epoch, 1);
        assert!(!fence_pending(rseg) && !fence_pending(rlog));
        // B's walk raised every bucket to 1, A's to 2.
        assert_eq!(
            prov_epoch(mem.as_ref(), tenant(1), Signal::Metrics).await,
            2
        );
    }

    #[tokio::test]
    async fn a_pending_walk_resumes_instead_of_activating_again() {
        let mem = initialized().await;
        let store = FaultStore::new(
            mem.clone(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::List, ScriptedFault::Transient("list down".into()))
                    .with_occurrence(Occurrence::Nth(1)),
            ),
        );
        let first = activate(
            &store,
            Some(&test_keys()),
            &TestClock(T0 + 10),
            &writable_next(FAMILY_RSEG),
            &request(FAMILY_RSEG),
        )
        .await
        .expect("activate");
        assert_eq!(store.fault_count(Op::List, FaultKind::Transient), 1);
        assert!(matches!(first.walk.failure, Some(FenceFailure::Listing(_))));
        assert_eq!(first.walk.resume_from, None);
        assert_eq!(
            entry(mem.as_ref(), FAMILY_RSEG)
                .await
                .fence_complete_unix_ns,
            0
        );

        // Asking for the next version while the walk is pending resumes it.
        let writable = WritableVersions::current().with_family(
            FAMILY_RSEG,
            vec![baseline(FAMILY_RSEG) + 1, baseline(FAMILY_RSEG) + 2],
        );
        let second = activate(
            &store,
            Some(&test_keys()),
            &TestClock(T0 + 20),
            &writable,
            &ActivateRequest {
                version: baseline(FAMILY_RSEG) + 2,
                ..request(FAMILY_RSEG)
            },
        )
        .await
        .expect("resume");
        assert_eq!(second.outcome, ActivateOutcome::ResumedPendingWalk);
        assert!(second.walk.complete);
        let rseg = entry(mem.as_ref(), FAMILY_RSEG).await;
        assert_eq!(rseg.active_write_version, baseline(FAMILY_RSEG) + 1);
        assert_eq!(rseg.activated_epoch, 1);
        assert_eq!(rseg.fence_complete_unix_ns, T0 + 20);
    }

    #[tokio::test]
    async fn absent_record_refuses() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        let err = activate(
            mem.as_ref(),
            Some(&test_keys()),
            &TestClock(T0),
            &writable_next(FAMILY_RLOG),
            &request(FAMILY_RLOG),
        )
        .await
        .expect_err("absent refuses");
        assert!(matches!(err, ActivateError::RecordAbsent), "{err:?}");
        assert!(mem.get(COMPAT_KEY, GetRange::Full).await.is_err());
    }
}
