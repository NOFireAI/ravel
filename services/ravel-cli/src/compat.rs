//! `ravel-cli compat {init,activate,certify,verify,status}`: the operator
//! surface of the retained-data compatibility contract (ADR-4233).
//!
//! Every subcommand is a thin wrapper over the `ravel-maintain` library
//! (`init_compat`, `activate`, `certify`, `late_write_pass`) and the
//! `ravel-catalog` `sys/compat` reader, adding only argument parsing, typed
//! exit codes, and a machine-readable `--json` rendering. The library owns
//! every durable decision; this module never computes a version, a fence or a
//! certificate itself.
//!
//! `sys/compat` is MAC-signed under the deployment key the global
//! `--tenant-hash-key-file` names, not the maintain heartbeat key (T11,
//! #4246). An unkeyed store therefore builds no compat record: `init` and
//! `activate` refuse it through the library's own typed `Unkeyed` error, and
//! `certify`, `verify` and `status` (which must verify the record's MAC to
//! read it) refuse at the CLI boundary with the same reason.

use std::path::Path;
use std::sync::Arc;

use ravel_catalog::compat::{
    CompatClock, CompatKeys, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, read_compat,
};
use ravel_maintain::{
    ActivateError, ActivateOutcome, ActivateRequest, CertifyRefusal, CertifyReport, CertifyTimings,
    CompatInitError, InitConfig, InitOutcome, WritableVersions, activate as lib_activate,
    certify as lib_certify, init_compat, late_write_pass,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_proto::sys::v1 as sysproto;
use ravel_types::TenantHash;
use serde_json::{Value, json};

/// Success: the command ran and nothing refused it.
pub const EXIT_OK: i32 = 0;
/// A precondition refused the command; nothing durable was written. Unkeyed
/// store, a missing attestation, an unwritable version, the certify time gate,
/// and every other library refusal land here.
pub const EXIT_REFUSED: i32 = 2;
/// A below-certified-version member or a late write contradicts the family:
/// `certify` returned `Contradicted`, or `verify` found late writes.
pub const EXIT_CONTRADICTED: i32 = 3;
/// The work is resumable but not finished: a `certify` run stopped on its
/// budget, or its certificate's finalization is still pending a late-write
/// pass.
pub const EXIT_PENDING: i32 = 4;

/// What one compat subcommand concluded: a typed exit code and the two
/// renderings the caller picks between with `--json`.
pub struct CompatOutcome {
    pub exit_code: i32,
    text: String,
    json: Value,
}

impl CompatOutcome {
    fn ok(text: String, json: Value) -> Self {
        CompatOutcome {
            exit_code: EXIT_OK,
            text,
            json,
        }
    }

    fn with_code(exit_code: i32, text: String, json: Value) -> Self {
        CompatOutcome {
            exit_code,
            text,
            json,
        }
    }

    /// A refusal: exit [`EXIT_REFUSED`], the reason in both renderings.
    fn refused(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        CompatOutcome {
            exit_code: EXIT_REFUSED,
            text: format!("refused: {reason}\n"),
            json: json!({ "outcome": "refused", "reason": reason }),
        }
    }

    /// Write the chosen rendering to stdout.
    pub fn emit(&self, as_json: bool) {
        if as_json {
            let rendered = serde_json::to_string_pretty(&self.json)
                .unwrap_or_else(|_| self.json.to_string());
            println!("{rendered}");
        } else {
            print!("{}", self.text);
        }
    }

    #[cfg(test)]
    fn json(&self) -> &Value {
        &self.json
    }
}

/// A fixed wall-clock reading, captured once per invocation. The library's
/// activation CAS loop and `init` both read the clock through this trait; a
/// single CLI invocation needs only one reading, and injecting it keeps the
/// tests deterministic (CLAUDE.md: time is injected, no `SystemTime::now()` in
/// logic under test).
struct FixedClock(i64);

impl CompatClock for FixedClock {
    fn now_unix_ns(&self) -> i64 {
        self.0
    }
}

/// Build the `sys/compat` MAC keys from the deployment key file the global
/// `--tenant-hash-key-file` names, or `None` when the store is unkeyed. The
/// same 32-byte deployment key doubles as the v2 tenant-hash key and the
/// `sys/compat` MAC key (T11, #4246), parsed through the shared loader.
pub fn load_compat_keys(key_file: Option<&Path>) -> anyhow::Result<Option<CompatKeys>> {
    let Some(path) = key_file else {
        return Ok(None);
    };
    let raw = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("could not read --tenant-hash-key-file {path:?}: {e}"))?;
    let key = crate::tenancy::parse_deployment_key(&raw)
        .map_err(|e| anyhow::anyhow!("invalid --tenant-hash-key-file {}: {e}", path.display()))?;
    let keys = CompatKeys::from_file_keys(&[key])
        .map_err(|e| anyhow::anyhow!("deriving the sys/compat key: {e}"))?;
    Ok(Some(keys))
}

/// The unkeyed-store refusal `certify`/`verify`/`status` raise before touching
/// the store, matching the wording of the library's own `Unkeyed` errors that
/// `init`/`activate` raise.
const UNKEYED_READ_REFUSAL: &str =
    "no --tenant-hash-key-file configured: sys/compat is MAC-signed under the deployment key, so \
     an unkeyed store carries no compat record to read";

fn is_class_a(family: &str) -> bool {
    family == FAMILY_RSEG || family == FAMILY_RLOG || family == FAMILY_RSPAN
}

// ---------------------------------------------------------------------------
// init
// ---------------------------------------------------------------------------

/// `compat init`: adopt the store at the baseline (ADR-4233 decision 1). Runs
/// the census and creates `sys/compat`, or returns the record already present
/// when it matches. Refuses an unkeyed store (typed `Unkeyed`), any object
/// outside the baseline read set, and a divergent existing record.
pub async fn init(
    store: Arc<dyn ObjectStoreBackend>,
    keys: Option<CompatKeys>,
    now_ns: i64,
    config: InitConfig,
) -> anyhow::Result<CompatOutcome> {
    let clock = FixedClock(now_ns);
    match init_compat(store.as_ref(), keys.as_ref(), &clock, &config).await {
        Ok(report) => Ok(render_init(&report)),
        Err(err) => classify_init_error(err),
    }
}

fn render_init(report: &ravel_maintain::InitReport) -> CompatOutcome {
    let outcome = match report.outcome {
        InitOutcome::Created => "created",
        InitOutcome::AlreadyInitialized => "already-initialized",
    };
    let record = &report.record;
    let mut text = String::new();
    text.push_str(&format!("compat init: {outcome}\n"));
    text.push_str(&format!(
        "baseline: v{}.{}\n",
        record.baseline_release_major, record.baseline_release_minor
    ));
    text.push_str(&format!("compat_epoch: {}\n", record.compat_epoch));
    text.push_str(&format!(
        "max_publish_lifetime_ns: {}\n",
        record.max_publish_lifetime_ns
    ));
    text.push_str("families:\n");
    for family in &record.families {
        text.push_str(&format!(
            "  {}: active_write_version={}\n",
            family.family, family.active_write_version
        ));
    }
    if let Some(census) = &report.census {
        text.push_str(&format!(
            "census: tenants={} objects_checked={} objects_ignored={} objects_vanished={}\n",
            census.tenants, census.objects_checked, census.objects_ignored, census.objects_vanished
        ));
    }
    if !record.adopted.is_empty() {
        text.push_str("adopted:\n");
        for adopted in &record.adopted {
            text.push_str(&format!(
                "  {} v{}: {}\n",
                adopted.family, adopted.version, adopted.count
            ));
        }
    }

    let families: Vec<Value> = record
        .families
        .iter()
        .map(|f| json!({ "family": f.family, "active_write_version": f.active_write_version }))
        .collect();
    let adopted: Vec<Value> = record
        .adopted
        .iter()
        .map(|a| json!({ "family": a.family, "version": a.version, "count": a.count }))
        .collect();
    let census = report.census.as_ref().map(|c| {
        json!({
            "tenants": c.tenants,
            "objects_checked": c.objects_checked,
            "objects_ignored": c.objects_ignored,
            "objects_vanished": c.objects_vanished,
        })
    });
    CompatOutcome::ok(
        text,
        json!({
            "outcome": outcome,
            "baseline_release_major": record.baseline_release_major,
            "baseline_release_minor": record.baseline_release_minor,
            "compat_epoch": record.compat_epoch,
            "max_publish_lifetime_ns": record.max_publish_lifetime_ns,
            "families": families,
            "adopted": adopted,
            "census": census,
        }),
    )
}

/// A census/create fault (a listing error, a GET failure, a MAC mismatch)
/// propagates as an error (exit 1); a refusal the operator must fix is a typed
/// [`EXIT_REFUSED`] outcome that writes nothing.
fn classify_init_error(err: CompatInitError) -> anyhow::Result<CompatOutcome> {
    match err {
        CompatInitError::Unkeyed
        | CompatInitError::InvalidPublishLifetime { .. }
        | CompatInitError::NoBaselineVersion { .. }
        | CompatInitError::Unreadable { .. }
        | CompatInitError::UnknownBucketEntry { .. }
        | CompatInitError::NoFamilyForSignal { .. }
        | CompatInitError::ProvisioningFenced { .. }
        | CompatInitError::OutsideReadSet { .. }
        | CompatInitError::RecordDiffers { .. } => Ok(CompatOutcome::refused(err.to_string())),
        // A store, listing or MAC fault: not operator-correctable here.
        CompatInitError::List(_) | CompatInitError::Get { .. } | CompatInitError::Compat(_) => {
            Err(anyhow::Error::new(err))
        }
    }
}

// ---------------------------------------------------------------------------
// activate
// ---------------------------------------------------------------------------

/// The CLI shape of one `compat activate` invocation.
pub struct ActivateArgs {
    pub family: String,
    pub version: u32,
    /// The operator attestation (ADR-4233 decision 4, prerequisite amendment):
    /// every pre-baseline (v0.33.x) process is terminated before the first
    /// activation above the baseline. Activation refuses without it.
    pub pre_baseline_processes_terminated: bool,
    /// The operator identity recorded in the family entry's `activated_by`.
    pub activated_by: String,
    /// Tenants named by the alert rules file whose leases are parked on an
    /// `rlog` activation even before they hold an alert key.
    pub alert_rule_tenants: Vec<TenantHash>,
    pub clock_skew_allowance_ns: i64,
}

/// The residual the `--pre-baseline-processes-terminated` attestation does not
/// close, stated in the refusal and in `--help` (ADR-4233 audit-retention and
/// prerequisite amendments): a pre-baseline maintain process that outlives the
/// rollout still deletes alert history and audit objects by age with no
/// version hold, and no fence or pass detects that deletion.
pub const PRE_BASELINE_RESIDUAL: &str =
    "a pre-baseline (v0.33.x) maintain process that outlives the rollout still deletes alert \
     history and audit objects by age with no version hold; no fence and no late-write pass \
     detects that deletion, so terminating every pre-baseline process is the only control";

/// `compat activate --family F --version N+1` (ADR-4233 decision 3). Refuses
/// unless `--pre-baseline-processes-terminated` is given, passes that
/// attestation into the library's `writer_roles_can_read_compat`, and records
/// it in the family entry's `activated_by`.
///
/// `writable` is this build's writable-version table; the dispatch passes
/// [`WritableVersions::current`]. The baseline build writes one version per
/// family, so on it every activation above the baseline refuses `NotWritable`;
/// a two-version release's `current()` admits the activation.
pub async fn activate(
    store: Arc<dyn ObjectStoreBackend>,
    keys: Option<CompatKeys>,
    now_ns: i64,
    writable: WritableVersions,
    args: ActivateArgs,
) -> anyhow::Result<CompatOutcome> {
    if !args.pre_baseline_processes_terminated {
        // Refuse before any read or write. The library would also refuse
        // (the attestation feeds `writer_roles_can_read_compat`), but the CLI
        // states the prerequisite and its residual in the operator's words.
        return Ok(CompatOutcome::refused(format!(
            "activate requires --pre-baseline-processes-terminated: ADR-4233 decision 4 rests the \
             guarantee for pre-baseline processes on terminating every one before the first \
             activation above the baseline. Residual even with the flag: {PRE_BASELINE_RESIDUAL}"
        )));
    }

    // The attestation is recorded in `activated_by`; no sys/compat field is
    // added for it (a new field would be a proto change needing its own ADR).
    let activated_by = format!(
        "{}; attests: pre-baseline-processes-terminated",
        args.activated_by
    );
    let request = ActivateRequest {
        family: args.family.clone(),
        version: args.version,
        activated_by,
        writer_roles_can_read_compat: args.pre_baseline_processes_terminated,
        alert_rule_tenants: args.alert_rule_tenants,
        clock_skew_allowance_ns: args.clock_skew_allowance_ns,
        resume_from: None,
    };
    let clock = FixedClock(now_ns);
    match lib_activate(store.as_ref(), keys.as_ref(), &clock, &writable, &request).await {
        Ok(report) => Ok(render_activate(&report)),
        Err(err) => classify_activate_error(err),
    }
}

fn render_activate(report: &ravel_maintain::ActivateReport) -> CompatOutcome {
    let outcome = match report.outcome {
        ActivateOutcome::Activated { .. } => "activated",
        ActivateOutcome::ResumedPendingWalk => "resumed-pending-walk",
    };
    let walk = &report.walk;
    let mut text = String::new();
    match report.outcome {
        ActivateOutcome::Activated {
            previous_version,
            version,
        } => text.push_str(&format!(
            "compat activate: {} {} -> {}\n",
            report.family, previous_version, version
        )),
        ActivateOutcome::ResumedPendingWalk => text.push_str(&format!(
            "compat activate: {} resumed a pending fence walk; nothing newly activated\n",
            report.family
        )),
    }
    text.push_str(&format!(
        "active_write_version: {}\n",
        report.active_write_version
    ));
    text.push_str(&format!(
        "fence walk: tenants={} buckets_raised={} buckets_already_fenced={} \
         buckets_unprovisioned={} leases_created={} leases_replaced={} leases_already_parked={} \
         complete={}\n",
        walk.tenants,
        walk.buckets_raised,
        walk.buckets_already_fenced,
        walk.buckets_unprovisioned,
        walk.leases_created,
        walk.leases_replaced,
        walk.leases_already_parked,
        walk.complete,
    ));
    if let Some(ahead) = report.stamp_ahead_of_clock_ns {
        text.push_str(&format!(
            "warning: activation stamped {ahead} ns past this clock, beyond the skew allowance\n"
        ));
    }
    if let Some(failure) = &walk.failure {
        text.push_str(&format!("fence walk incomplete: {failure}\n"));
    }

    // An incomplete fence walk blocks certification until it is resumed; a
    // completed activation whose walk finished is success.
    let exit_code = if walk.complete {
        EXIT_OK
    } else {
        EXIT_PENDING
    };
    let json = json!({
        "outcome": outcome,
        "family": report.family,
        "active_write_version": report.active_write_version,
        "stamp_ahead_of_clock_ns": report.stamp_ahead_of_clock_ns,
        "fence_walk": {
            "tenants": walk.tenants,
            "buckets_raised": walk.buckets_raised,
            "buckets_already_fenced": walk.buckets_already_fenced,
            "buckets_unprovisioned": walk.buckets_unprovisioned,
            "leases_created": walk.leases_created,
            "leases_replaced": walk.leases_replaced,
            "leases_already_parked": walk.leases_already_parked,
            "complete": walk.complete,
            "fence_complete_unix_ns": walk.fence_complete_unix_ns,
            "failure": walk.failure.as_ref().map(ToString::to_string),
        },
    });
    CompatOutcome::with_code(exit_code, text, json)
}

fn classify_activate_error(err: ActivateError) -> anyhow::Result<CompatOutcome> {
    match err {
        ActivateError::Unkeyed
        | ActivateError::WriterRolesCannotReadCompat
        | ActivateError::RecordAbsent
        | ActivateError::FamilyNotInRecord { .. }
        | ActivateError::NotWritable { .. }
        | ActivateError::NotAboveActive { .. }
        | ActivateError::VersionJump { .. } => Ok(CompatOutcome::refused(err.to_string())),
        // A CAS race that exhausted its attempts, or a store/decode fault: the
        // operator retries rather than fixing an input.
        ActivateError::Contended { .. } | ActivateError::Compat(_) => Err(anyhow::Error::new(err)),
    }
}

// ---------------------------------------------------------------------------
// certify
// ---------------------------------------------------------------------------

/// The CLI shape of one `compat certify` invocation.
pub struct CertifyArgs {
    pub family: String,
    pub version: u32,
    /// `--budget-records`: cap the `(tenant, signal, shard)` units one run
    /// covers. `None` is no cap. A budgeted run that stops leaves the
    /// resumable cursor and reports `incomplete`.
    pub budget_records: Option<usize>,
    pub max_flush_lifetime_ns: i64,
    pub t_req_ns: i64,
    pub clock_skew_allowance_ns: i64,
}

/// `compat certify --family F --version N+1` (ADR-4233 decision 7). Produces
/// the certificate a reader-retiring release accepts, or reports why it did
/// not: a precondition refusal (including the time gate), a contradiction, a
/// held member, a budget-stopped run, or a finalization still pending a
/// late-write pass.
pub async fn certify(
    store: Arc<dyn ObjectStoreBackend>,
    keys: Option<CompatKeys>,
    now_ns: i64,
    args: CertifyArgs,
) -> anyhow::Result<CompatOutcome> {
    let Some(keys) = keys else {
        return Ok(CompatOutcome::refused(UNKEYED_READ_REFUSAL));
    };
    let timings = CertifyTimings {
        t_req_ns: args.t_req_ns,
        max_flush_lifetime_ns: args.max_flush_lifetime_ns,
        clock_skew_allowance_ns: args.clock_skew_allowance_ns,
    };
    let report = lib_certify(
        store.as_ref(),
        &keys,
        now_ns,
        &args.family,
        args.version,
        timings,
        args.budget_records,
    )
    .await?;
    Ok(render_certify(&args.family, &report))
}

fn render_certify(family: &str, report: &CertifyReport) -> CompatOutcome {
    match report {
        CertifyReport::Certified {
            version,
            member_count,
            buckets_scanned,
            newest_created_unix_ns,
        } => {
            let text = format!(
                "compat certify: {family} v{version} certified\n\
                 member_count: {member_count}\n\
                 buckets_scanned: {buckets_scanned}\n\
                 newest_created_unix_ns: {newest_created_unix_ns}\n"
            );
            CompatOutcome::ok(
                text,
                json!({
                    "outcome": "certified",
                    "family": family,
                    "version": version,
                    "member_count": member_count,
                    "buckets_scanned": buckets_scanned,
                    "newest_created_unix_ns": newest_created_unix_ns,
                }),
            )
        }
        CertifyReport::Contradicted {
            member_key,
            found_version,
        } => CompatOutcome::with_code(
            EXIT_CONTRADICTED,
            format!(
                "compat certify: {family} contradicted by {member_key} at version {found_version}\n"
            ),
            json!({
                "outcome": "contradicted",
                "family": family,
                "member_key": member_key,
                "found_version": found_version,
            }),
        ),
        CertifyReport::HeldMember {
            member_key,
            version,
        } => CompatOutcome::with_code(
            EXIT_REFUSED,
            format!(
                "compat certify: {family} blocked by a member this build cannot read: \
                 {member_key} at version {version}\n"
            ),
            json!({
                "outcome": "held-member",
                "family": family,
                "member_key": member_key,
                "version": version,
            }),
        ),
        CertifyReport::Incomplete { units_examined } => CompatOutcome::with_code(
            EXIT_PENDING,
            format!(
                "compat certify: {family} incomplete, budget stopped after {units_examined} \
                 units; rerun to resume from the cursor\n"
            ),
            json!({
                "outcome": "incomplete",
                "family": family,
                "units_examined": units_examined,
            }),
        ),
        CertifyReport::FinalizationPending {
            version,
            certified_unix_ns,
            watermark_unix_ns,
        } => CompatOutcome::with_code(
            EXIT_PENDING,
            format!(
                "compat certify: {family} v{version} certificate written, finalization pending a \
                 late-write pass (certified_unix_ns={certified_unix_ns}, \
                 watermark_unix_ns={watermark_unix_ns})\n"
            ),
            json!({
                "outcome": "finalization-pending",
                "family": family,
                "version": version,
                "certified_unix_ns": certified_unix_ns,
                "watermark_unix_ns": watermark_unix_ns,
            }),
        ),
        CertifyReport::Refused(refusal) => CompatOutcome::refused(certify_refusal_reason(refusal)),
    }
}

fn certify_refusal_reason(refusal: &CertifyRefusal) -> String {
    match refusal {
        CertifyRefusal::NotClassAFamily { family } => {
            format!("{family} is not a class-A data family (rseg, rlog, rspan)")
        }
        CertifyRefusal::CompatRecordAbsent => {
            "sys/compat is absent; run compat init first".to_string()
        }
        CertifyRefusal::FamilyNotInRecord { family } => {
            format!("family {family} has no entry in sys/compat")
        }
        CertifyRefusal::PublishLifetimeAbsent => {
            "sys/compat records no max_publish_lifetime_ns (P); the time precondition needs it"
                .to_string()
        }
        CertifyRefusal::TargetAboveActive { target, active } => {
            format!("target version {target} is above the active write version {active}")
        }
        CertifyRefusal::FenceWalkIncomplete => {
            "the activation's fence walk has not finished; rerun compat activate to resume it"
                .to_string()
        }
        CertifyRefusal::TimeGate {
            now_ns,
            not_before_ns,
        } => format!(
            "the time precondition S is gated on has not passed: now={now_ns}, \
             not_before={not_before_ns}"
        ),
        CertifyRefusal::BucketRecentlyFenced {
            tenant_hash,
            signal,
            not_before_ns,
        } => format!(
            "tenant {tenant_hash} signal {signal:?} was fenced too recently for its G_pre; \
             not before {not_before_ns}"
        ),
        CertifyRefusal::BucketUnfenced {
            tenant_hash,
            signal,
            compat_epoch,
            activated_epoch,
        } => format!(
            "tenant {tenant_hash} signal {signal:?} is fenced at epoch {compat_epoch}, below the \
             activation epoch {activated_epoch}"
        ),
        CertifyRefusal::TenantSetChanged => {
            "the tenant set changed since the census; rerun certify".to_string()
        }
        CertifyRefusal::IncompleteListing {
            prefix,
            unaddressable,
        } => format!("incomplete listing under {prefix}: {unaddressable} unaddressable keys"),
        CertifyRefusal::UnexpectedShardEntry { key } => {
            format!("{key} is not a shard directory under its bucket prefix")
        }
        CertifyRefusal::TargetBelowCertificate { target, certified } => format!(
            "family already carries a certificate at version {certified}, above the target {target}"
        ),
        CertifyRefusal::AlreadyCertified { version } => {
            format!("family already carries an uncontradicted certificate at version {version}")
        }
        CertifyRefusal::MemberAbsent { member_key } => {
            format!("member {member_key} is absent or vanished mid-census")
        }
        CertifyRefusal::MemberCorrupt { member_key } => {
            format!("member {member_key} has a structurally corrupt trailer")
        }
        CertifyRefusal::StampTrailerMismatch {
            member_key,
            stamp,
            trailer,
        } => format!("member {member_key} stamps version {stamp} but its trailer reads {trailer}"),
        CertifyRefusal::CompatChangedDuringCensus => {
            "sys/compat changed during the census; a later run starts a fresh census".to_string()
        }
        CertifyRefusal::CompatRecordRaced => {
            "sys/compat was replaced during the certificate CAS; rerun to resume the cursor"
                .to_string()
        }
        CertifyRefusal::ConcurrentCertify => {
            "another certify run owns the cursor".to_string()
        }
    }
}

// ---------------------------------------------------------------------------
// verify / status
// ---------------------------------------------------------------------------

/// `compat verify` (ADR-4233 decision 7). Runs the late-write pass for the
/// class-A families that carry a certificate (or only `--family` when given),
/// then prints every family's active version, epoch, fence-complete time,
/// certificate, contradicted flag and watermark. Exits [`EXIT_CONTRADICTED`]
/// when any family is contradicted or a pass found a late write.
pub async fn verify(
    store: Arc<dyn ObjectStoreBackend>,
    keys: Option<CompatKeys>,
    now_ns: i64,
    family: Option<String>,
    clock_skew_allowance_ns: i64,
) -> anyhow::Result<CompatOutcome> {
    let Some(keys) = keys else {
        return Ok(CompatOutcome::refused(UNKEYED_READ_REFUSAL));
    };

    // Pick the class-A families to run the pass over: the one named, or every
    // class-A family that carries a certificate.
    let Some((record, _version)) = read_compat(store.as_ref(), &keys).await? else {
        return Ok(render_absent("verify"));
    };
    let targets: Vec<String> = match &family {
        Some(f) if is_class_a(f) => vec![f.clone()],
        Some(_) => Vec::new(),
        None => record
            .families
            .iter()
            .filter(|f| is_class_a(&f.family) && f.certificate.is_some())
            .map(|f| f.family.clone())
            .collect(),
    };

    let mut pass_findings: Vec<Value> = Vec::new();
    let mut found_late_write = false;
    for target in &targets {
        let report =
            late_write_pass(store.as_ref(), &keys, now_ns, target, clock_skew_allowance_ns).await?;
        if !report.late_writes.is_empty() {
            found_late_write = true;
        }
        pass_findings.push(json!({
            "family": target,
            "examined": report.examined,
            "certified": report.certified,
            "certificate_replaced": report.certificate_replaced,
            "watermark_unix_ns": report.watermark_unix_ns,
            "late_writes": report
                .late_writes
                .iter()
                .map(|lw| json!({ "record_key": lw.record_key, "version": lw.version }))
                .collect::<Vec<_>>(),
        }));
    }

    // Re-read so the printed snapshot reflects any `contradicted` the pass set.
    let Some((record, _version)) = read_compat(store.as_ref(), &keys).await? else {
        return Ok(render_absent("verify"));
    };
    let contradicted = record.families.iter().any(|f| f.contradicted);
    let exit_code = if found_late_write || contradicted {
        EXIT_CONTRADICTED
    } else {
        EXIT_OK
    };
    Ok(render_snapshot(
        "verify",
        &record,
        Some(pass_findings),
        exit_code,
    ))
}

/// `compat status` (ADR-4233 decision 3). A read-only snapshot of `sys/compat`:
/// every family's active version, epoch, fence-complete time, certificate,
/// contradicted flag and watermark. Runs no pass and always exits
/// [`EXIT_OK`].
pub async fn status(
    store: Arc<dyn ObjectStoreBackend>,
    keys: Option<CompatKeys>,
) -> anyhow::Result<CompatOutcome> {
    let Some(keys) = keys else {
        return Ok(CompatOutcome::refused(UNKEYED_READ_REFUSAL));
    };
    let Some((record, _version)) = read_compat(store.as_ref(), &keys).await? else {
        return Ok(render_absent("status"));
    };
    Ok(render_snapshot("status", &record, None, EXIT_OK))
}

fn render_absent(command: &str) -> CompatOutcome {
    CompatOutcome::ok(
        format!("compat {command}: sys/compat is absent; run compat init first\n"),
        json!({ "outcome": "absent" }),
    )
}

/// The per-family rendering shared by `verify` and `status`. `finalization
/// pending` is derived (a certificate present whose `certified_unix_ns` is
/// above the watermark), the same condition `certify` reports as
/// `FinalizationPending`.
fn render_snapshot(
    command: &str,
    record: &sysproto::CompatRecord,
    pass_findings: Option<Vec<Value>>,
    exit_code: i32,
) -> CompatOutcome {
    let mut text = String::new();
    text.push_str(&format!(
        "compat {command}: baseline v{}.{} compat_epoch={} max_publish_lifetime_ns={}\n",
        record.baseline_release_major,
        record.baseline_release_minor,
        record.compat_epoch,
        record.max_publish_lifetime_ns,
    ));
    let mut families_json = Vec::new();
    for family in &record.families {
        let (cert_text, cert_json) = match &family.certificate {
            Some(cert) => (
                format!(
                    "certificate=v{} (certified_unix_ns={}, member_count={}, buckets_scanned={})",
                    cert.version, cert.certified_unix_ns, cert.member_count, cert.buckets_scanned
                ),
                json!({
                    "version": cert.version,
                    "certified_unix_ns": cert.certified_unix_ns,
                    "member_count": cert.member_count,
                    "newest_created_unix_ns": cert.newest_created_unix_ns,
                    "buckets_scanned": cert.buckets_scanned,
                }),
            ),
            None => ("certificate=none".to_string(), Value::Null),
        };
        let finalization_pending = family.certificate.as_ref().is_some_and(|cert| {
            family.late_writes_verified_through_unix_ns < cert.certified_unix_ns
        });
        text.push_str(&format!(
            "  {}: active_write_version={} activated_epoch={} fence_complete_unix_ns={} \
             contradicted={} watermark_unix_ns={} {} finalization_pending={}\n",
            family.family,
            family.active_write_version,
            family.activated_epoch,
            family.fence_complete_unix_ns,
            family.contradicted,
            family.late_writes_verified_through_unix_ns,
            cert_text,
            finalization_pending,
        ));
        families_json.push(json!({
            "family": family.family,
            "active_write_version": family.active_write_version,
            "activated_epoch": family.activated_epoch,
            "activated_unix_ns": family.activated_unix_ns,
            "activated_by": family.activated_by,
            "fence_complete_unix_ns": family.fence_complete_unix_ns,
            "contradicted": family.contradicted,
            "watermark_unix_ns": family.late_writes_verified_through_unix_ns,
            "finalization_pending": finalization_pending,
            "certificate": cert_json,
        }));
    }

    let mut json = json!({
        "outcome": command,
        "baseline_release_major": record.baseline_release_major,
        "baseline_release_minor": record.baseline_release_minor,
        "compat_epoch": record.compat_epoch,
        "max_publish_lifetime_ns": record.max_publish_lifetime_ns,
        "families": families_json,
    });
    if let Some(findings) = pass_findings {
        json["late_write_passes"] = Value::Array(findings);
    }
    CompatOutcome::with_code(exit_code, text, json)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    const T0: i64 = 1_800_000_000_000_000_000;
    const HOUR_NS: i64 = 3_600 * 1_000_000_000;

    fn keys() -> CompatKeys {
        CompatKeys::from_file_keys(&[[7u8; 32]]).expect("one key")
    }

    fn init_config() -> InitConfig {
        InitConfig {
            baseline_release_major: 0,
            baseline_release_minor: 34,
            max_publish_lifetime_ns: HOUR_NS,
        }
    }

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    // ---- init ----

    #[tokio::test]
    async fn init_success_creates_record() {
        let store = store();
        let out = init(store.clone(), Some(keys()), T0, init_config())
            .await
            .expect("init runs");
        assert_eq!(out.exit_code, EXIT_OK);
        assert_eq!(out.json()["outcome"], "created");
        // The record is now present and names the rseg family.
        let families = out.json()["families"].as_array().expect("families array");
        assert!(
            families.iter().any(|f| f["family"] == FAMILY_RSEG),
            "families: {families:?}"
        );
    }

    #[tokio::test]
    async fn init_refuses_unkeyed_store() {
        let store = store();
        let out = init(store, None, T0, init_config())
            .await
            .expect("init returns a typed refusal, not an error");
        assert_eq!(out.exit_code, EXIT_REFUSED);
        assert_eq!(out.json()["outcome"], "refused");
        let reason = out.json()["reason"].as_str().expect("reason");
        assert!(
            reason.contains("unkeyed") || reason.contains("MACed"),
            "reason names the unkeyed cause: {reason}"
        );
    }

    #[tokio::test]
    async fn init_is_idempotent() {
        let store = store();
        init(store.clone(), Some(keys()), T0, init_config())
            .await
            .expect("first init");
        let out = init(store, Some(keys()), T0, init_config())
            .await
            .expect("second init");
        assert_eq!(out.exit_code, EXIT_OK);
        assert_eq!(out.json()["outcome"], "already-initialized");
    }

    // ---- activate ----

    fn activate_args(terminated: bool) -> ActivateArgs {
        ActivateArgs {
            family: FAMILY_RSEG.to_string(),
            version: 8,
            pre_baseline_processes_terminated: terminated,
            activated_by: "operator".to_string(),
            alert_rule_tenants: Vec::new(),
            clock_skew_allowance_ns: 60 * 1_000_000_000,
        }
    }

    /// This build writes one rseg version, so a real `current()` refuses the
    /// activation; a two-version release's table is simulated by widening the
    /// rseg writable set to {7, 8}.
    fn two_wide_rseg() -> WritableVersions {
        WritableVersions::current().with_family(FAMILY_RSEG, vec![7, 8])
    }

    #[tokio::test]
    async fn activate_refuses_without_attestation() {
        let store = store();
        init(store.clone(), Some(keys()), T0, init_config())
            .await
            .expect("init");
        let out = activate(
            store.clone(),
            Some(keys()),
            T0,
            two_wide_rseg(),
            activate_args(false),
        )
        .await
        .expect("activate returns a typed refusal");
        assert_eq!(out.exit_code, EXIT_REFUSED);
        let reason = out.json()["reason"].as_str().expect("reason");
        assert!(
            reason.contains("pre-baseline-processes-terminated"),
            "reason names the prerequisite flag: {reason}"
        );
        // Nothing was activated: rseg stays at the baseline.
        let st = status(store, Some(keys())).await.expect("status");
        let rseg = st.json()["families"]
            .as_array()
            .expect("families")
            .iter()
            .find(|f| f["family"] == FAMILY_RSEG)
            .expect("rseg entry")
            .clone();
        assert_eq!(rseg["active_write_version"], 7);
        assert_eq!(rseg["activated_epoch"], 0);
    }

    #[tokio::test]
    async fn activate_refuses_unwritable_version() {
        let store = store();
        init(store.clone(), Some(keys()), T0, init_config())
            .await
            .expect("init");
        // The default `current()` table does not write rseg 8.
        let out = activate(
            store,
            Some(keys()),
            T0,
            WritableVersions::current(),
            activate_args(true),
        )
        .await
        .expect("activate returns a typed refusal");
        assert_eq!(out.exit_code, EXIT_REFUSED);
        let reason = out.json()["reason"].as_str().expect("reason");
        assert!(
            reason.contains("does not write"),
            "reason names the unwritable version: {reason}"
        );
    }

    #[tokio::test]
    async fn activate_success_records_attestation_in_activated_by() {
        let store = store();
        init(store.clone(), Some(keys()), T0, init_config())
            .await
            .expect("init");
        let out = activate(
            store.clone(),
            Some(keys()),
            T0,
            two_wide_rseg(),
            activate_args(true),
        )
        .await
        .expect("activate runs");
        assert_eq!(out.exit_code, EXIT_OK, "json: {}", out.json());
        assert_eq!(out.json()["outcome"], "activated");
        assert_eq!(out.json()["active_write_version"], 8);
        assert_eq!(out.json()["fence_walk"]["complete"], true);
        // The attestation is recorded in activated_by.
        let st = status(store, Some(keys())).await.expect("status");
        let rseg = st.json()["families"]
            .as_array()
            .expect("families")
            .iter()
            .find(|f| f["family"] == FAMILY_RSEG)
            .expect("rseg")
            .clone();
        assert_eq!(rseg["active_write_version"], 8);
        let activated_by = rseg["activated_by"].as_str().expect("activated_by");
        assert!(
            activated_by.contains("pre-baseline-processes-terminated"),
            "activated_by records the attestation: {activated_by}"
        );
    }

    // ---- certify ----

    fn certify_args(version: u32) -> CertifyArgs {
        CertifyArgs {
            family: FAMILY_RSEG.to_string(),
            version,
            budget_records: None,
            max_flush_lifetime_ns: HOUR_NS,
            t_req_ns: 30 * 1_000_000_000,
            clock_skew_allowance_ns: 60 * 1_000_000_000,
        }
    }

    async fn init_and_activate(store: &Arc<dyn ObjectStoreBackend>) {
        init(store.clone(), Some(keys()), T0, init_config())
            .await
            .expect("init");
        activate(
            store.clone(),
            Some(keys()),
            T0,
            two_wide_rseg(),
            activate_args(true),
        )
        .await
        .expect("activate");
    }

    #[tokio::test]
    async fn certify_refuses_before_time_gate() {
        let store = store();
        init_and_activate(&store).await;
        // now == activation time: H + P + T_req has not elapsed.
        let out = certify(store, Some(keys()), T0, certify_args(8))
            .await
            .expect("certify runs");
        assert_eq!(out.exit_code, EXIT_REFUSED);
        let reason = out.json()["reason"].as_str().expect("reason");
        assert!(
            reason.contains("time precondition"),
            "reason names the time gate: {reason}"
        );
    }

    #[tokio::test]
    async fn certify_refuses_unkeyed_store() {
        let store = store();
        let out = certify(store, None, T0, certify_args(8))
            .await
            .expect("certify returns a typed refusal");
        assert_eq!(out.exit_code, EXIT_REFUSED);
        let reason = out.json()["reason"].as_str().expect("reason");
        assert!(reason.contains("unkeyed"), "reason: {reason}");
    }

    /// The full sequence through the CLI: init adopts, activate raises rseg to
    /// 8 and completes its fence walk, certify (after the time gate, over an
    /// empty corpus with nothing to migrate) writes the certificate, and
    /// verify reports it. Migration is the empty-corpus no-op here: the census
    /// finds no member below the target, which is the condition certify
    /// requires; a non-empty corpus's migration is the `maintain migrate` /
    /// server half of #4248.
    #[tokio::test]
    async fn compat_activate_migrate_certify_verify_round_trip() {
        let store = store();
        init_and_activate(&store).await;

        // Past the time gate: now = activation + 2h clears H (10m) + P (1h) +
        // T_req (30s) + skew (60s) with margin, and no provisioned bucket adds
        // a G_pre term on the empty corpus.
        let now_certify = T0 + 2 * HOUR_NS;
        let out = certify(store.clone(), Some(keys()), now_certify, certify_args(8))
            .await
            .expect("certify runs");
        assert_eq!(out.exit_code, EXIT_OK, "json: {}", out.json());
        assert_eq!(out.json()["outcome"], "certified");
        assert_eq!(out.json()["version"], 8);

        // verify runs the late-write pass and reports the certificate.
        let out = verify(
            store.clone(),
            Some(keys()),
            now_certify,
            Some(FAMILY_RSEG.to_string()),
            60 * 1_000_000_000,
        )
        .await
        .expect("verify runs");
        assert_eq!(out.exit_code, EXIT_OK, "json: {}", out.json());
        let rseg = out.json()["families"]
            .as_array()
            .expect("families")
            .iter()
            .find(|f| f["family"] == FAMILY_RSEG)
            .expect("rseg")
            .clone();
        assert_eq!(rseg["certificate"]["version"], 8);
        assert_eq!(rseg["contradicted"], false);
        // The pass advanced the watermark to its start, clearing the pending
        // finalization the bare certificate CAS leaves.
        assert_eq!(rseg["finalization_pending"], false, "rseg: {rseg}");
    }

    // ---- verify / status ----

    #[tokio::test]
    async fn status_refuses_unkeyed_store() {
        let store = store();
        let out = status(store, None).await.expect("status returns a refusal");
        assert_eq!(out.exit_code, EXIT_REFUSED);
    }

    #[tokio::test]
    async fn status_reports_absent_record() {
        let store = store();
        let out = status(store, Some(keys())).await.expect("status runs");
        assert_eq!(out.exit_code, EXIT_OK);
        assert_eq!(out.json()["outcome"], "absent");
    }

    #[tokio::test]
    async fn status_prints_every_family_field() {
        let store = store();
        init(store.clone(), Some(keys()), T0, init_config())
            .await
            .expect("init");
        let out = status(store, Some(keys())).await.expect("status runs");
        assert_eq!(out.exit_code, EXIT_OK);
        let families = out.json()["families"].as_array().expect("families");
        let rseg = families
            .iter()
            .find(|f| f["family"] == FAMILY_RSEG)
            .expect("rseg");
        // Every field the deliverable names is present.
        for key in [
            "active_write_version",
            "activated_epoch",
            "fence_complete_unix_ns",
            "contradicted",
            "watermark_unix_ns",
            "finalization_pending",
            "certificate",
        ] {
            assert!(rseg.get(key).is_some(), "missing {key} in {rseg}");
        }
    }
}
