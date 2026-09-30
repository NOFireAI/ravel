//! `ravel-cli store qualify` (ADR-0050 section 6): runs
//! `ravel_object_store::conformance`'s empirical suite against a configured
//! backend and, on a pass, records the outcome at `sys/qualification` via
//! `CreateIfAbsent` -- once per bucket at a given suite version. A record left
//! by an older suite version is the one exception: a re-run overwrites it, which
//! is the only way to clear `ravel-server`'s stale-record refusal. That
//! overwrite is guarded by `CasVersion` on the read version, so a concurrent
//! `qualify` from a newer binary can never be silently downgraded (ADR-1302).

use std::sync::Arc;

use bytes::Bytes;
use ravel_object_store::conformance::{
    BucketConfigProbe, BucketProbesSource, CONFORMANCE_SUITE_VERSION, LifecycleRuleStatus,
    bucket_config_alarms, probe_bucket_lock_and_config, run_conformance_suite,
};
use ravel_object_store::s3::UploadIntegrity;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError};

use crate::store::BuiltStore;

// The record and its key now live in `ravel-object-store` so `ravel-server`
// startup (ADR-0050 section 6) and this writer share one definition.
// Re-exported here so existing `ravel_cli::qualify::{QUALIFICATION_KEY,
// QualificationRecord}` call sites (and the in-process CLI test) keep resolving
// against the CLI's own module path.
pub use ravel_object_store::conformance::{QUALIFICATION_KEY, QualificationRecord};

/// Run the conformance suite against `store` under a fresh scratch prefix and
/// print each property's outcome. On a pass, writes [`QualificationRecord`]
/// to `sys/qualification`; if an equal-or-newer record is already there (a
/// prior qualifying run at this suite version), leaves it untouched and reports
/// it instead, and overwrites one written under an older suite version with the
/// current pass. Returns an error -- without writing anything -- if any
/// property fails, naming which one(s).
///
/// `list_page_size` must be the page size `store` was actually built with
/// (see `ravel-cli store qualify --list-page-size`): it is passed straight
/// through to `run_conformance_suite`, whose cross-page and ordering probes
/// size their key counts against it.
pub async fn qualify(
    store: Arc<dyn ObjectStoreBackend>,
    backend_identity: String,
    run_id: &str,
    list_page_size: usize,
) -> anyhow::Result<()> {
    qualify_built(
        BuiltStore::Other(store),
        backend_identity,
        run_id,
        list_page_size,
    )
    .await
}

/// [`qualify`] over a [`BuiltStore`]: when it holds the concrete S3 store,
/// the bucket probes read the bucket's configuration and the stored-checksum
/// echo check runs.
pub async fn qualify_built(
    built: BuiltStore,
    backend_identity: String,
    run_id: &str,
    list_page_size: usize,
) -> anyhow::Result<()> {
    let store = built.backend();
    let scratch_prefix = format!("sys/qualify/{run_id}/");
    let report = run_conformance_suite(store.as_ref(), &scratch_prefix, list_page_size).await;

    for result in &report.results {
        println!(
            "{:<40} {} {}",
            result.property.name(),
            if result.passed { "PASS" } else { "FAIL" },
            result.detail
        );
    }

    // Informational Object Lock / versioning probe (ADR-0055 section 3, citing
    // ADR-0042 decision 3). Printed unconditionally, before the pass/fail
    // decision, and clearly labeled informational and non-blocking: it never
    // affects whether qualification passes, what is recorded in
    // `sys/qualification`, or whether the server starts (ADR-0050 section 6 is
    // unaffected). An S3 store answers from the bucket's own configuration;
    // any other backend reports "unknown" through the `ObjectStoreBackend`
    // contract, which exposes no such query.
    //
    // Informational required-bucket-configuration report (ADR-0064 §7, S2-16,
    // S4-12): bucket versioning state and the status of the two sanctioned
    // lifecycle rules, plus any contract-violation alarms. Like the Object Lock
    // probe, it is informational-plus-alarming and never affects whether
    // qualification passes, what `sys/qualification` records, or whether the
    // server starts; `object_store` cannot enforce bucket policy, so Ravel
    // reports what it can observe (unknown through the trait contract) and
    // documents what it requires.
    for line in built_bucket_probe_lines(&built).await {
        println!("{line}");
    }

    let echo = checksum_echo(&built, &format!("{scratch_prefix}checksum-echo")).await;
    let (echo, echo_cleanup) = echo;
    println!("{}", echo.line());
    if let Some(note) = echo_cleanup {
        println!("{CHECKSUM_ECHO_LABEL:<40} {note}");
    }

    if !report.passed() {
        let failed_names: Vec<&str> = report.failures().map(|r| r.property.name()).collect();
        anyhow::bail!(
            "store qualification failed: {} does not satisfy the object store contract \
             (docs/object-store-contract.md); failing propert{}: {}",
            backend_identity,
            if failed_names.len() == 1 { "y" } else { "ies" },
            failed_names.join(", "),
        );
    }
    if let Some(failure) = echo.failure() {
        anyhow::bail!(
            "store qualification failed: {backend_identity} failed the stored-checksum echo \
             check: {failure}"
        );
    }

    let record = QualificationRecord {
        suite_version: CONFORMANCE_SUITE_VERSION,
        backend_identity: backend_identity.clone(),
        qualified_unix_ns: crate::now_ns()?,
        passed_properties: report
            .results
            .iter()
            .map(|r| r.property.name().to_string())
            .collect(),
    };
    let body = serde_json::to_vec_pretty(&record)
        .map_err(|err| anyhow::anyhow!("failed to encode qualification record: {err}"))?;

    match store
        .put(
            QUALIFICATION_KEY,
            Bytes::from(body),
            PutOptions::create_if_absent(),
        )
        .await
    {
        Ok(_) => {
            println!(
                "wrote {QUALIFICATION_KEY}: {backend_identity} qualified (suite v{})",
                CONFORMANCE_SUITE_VERSION
            );
            Ok(())
        }
        Err(StoreError::AlreadyExists) => {
            re_record_if_stale(store.as_ref(), &record, &backend_identity).await
        }
        Err(err) => Err(anyhow::anyhow!(
            "qualification passed but writing {QUALIFICATION_KEY} failed: {err}"
        )),
    }
}

/// Bound on re-record retries when a concurrent `qualify` keeps changing the
/// record between our read and our CAS write. Reached only when a peer wins the
/// race repeatedly with an equal-or-older suite version, which no single
/// production binary does (a peer at an equal-or-newer version ends the loop as
/// a no-op on the next read); the bound turns a pathological mixed-binary race
/// into a clear error instead of an operator-visible hang.
const MAX_RERECORD_ATTEMPTS: usize = 5;

/// Called when `CreateIfAbsent` reported the record already exists. Leaves an
/// equal-or-newer record untouched (the once-per-bucket-at-a-suite-version
/// no-op); when the stored record predates [`CONFORMANCE_SUITE_VERSION`] it was
/// never checked against the probes this build added, so `ravel-server` refuses
/// it as stale and this run must overwrite it (ADR-1302, superseding ADR-0050
/// section 6's write-once rule).
///
/// The overwrite is guarded by [`PutMode::CasVersion`] on the version read in
/// the same iteration, not an unconditional `Overwrite`: between the read and
/// the write a concurrent `qualify` from a newer binary could install a
/// higher-version record, and an unconditional overwrite would silently
/// downgrade it back to this run's older version, installing exactly the stale
/// record the startup gate accepts that this whole change exists to prevent.
/// On a `PreconditionFailed` the loop re-reads and re-decides: an
/// equal-or-newer record now present is left untouched, and only a genuinely
/// older one is overwritten again, bounded by [`MAX_RERECORD_ATTEMPTS`].
async fn re_record_if_stale(
    store: &dyn ObjectStoreBackend,
    record: &QualificationRecord,
    backend_identity: &str,
) -> anyhow::Result<()> {
    for _ in 0..MAX_RERECORD_ATTEMPTS {
        let existing_obj = store
            .get(QUALIFICATION_KEY, GetRange::Full)
            .await
            .map_err(|err| anyhow::anyhow!("failed to read existing {QUALIFICATION_KEY}: {err}"))?;
        let existing: QualificationRecord = serde_json::from_slice(&existing_obj.data)
            .map_err(|err| anyhow::anyhow!("{QUALIFICATION_KEY} is corrupt: {err}"))?;

        if existing.suite_version >= CONFORMANCE_SUITE_VERSION {
            println!(
                "{QUALIFICATION_KEY} already recorded for {} (suite v{}, qualified at unix_ns={}); \
                 not overwritten -- qualification is once per bucket at this suite version, per \
                 ADR-1302",
                existing.backend_identity, existing.suite_version, existing.qualified_unix_ns
            );
            return Ok(());
        }

        let refreshed = serde_json::to_vec_pretty(record)
            .map_err(|err| anyhow::anyhow!("failed to encode qualification record: {err}"))?;
        match store
            .put(
                QUALIFICATION_KEY,
                Bytes::from(refreshed),
                PutOptions {
                    mode: PutMode::CasVersion(existing_obj.version),
                    checksum: None,
                },
            )
            .await
        {
            Ok(_) => {
                println!(
                    "re-recorded {QUALIFICATION_KEY}: {backend_identity} re-qualified, upgrading \
                     the stored record from suite v{} to v{}",
                    existing.suite_version, CONFORMANCE_SUITE_VERSION
                );
                return Ok(());
            }
            // A concurrent writer changed the record between our read and this
            // write; re-read and re-decide rather than downgrade blindly.
            Err(StoreError::PreconditionFailed) => continue,
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "qualification passed but re-recording {QUALIFICATION_KEY} failed: {err}"
                ));
            }
        }
    }

    Err(anyhow::anyhow!(
        "qualification passed but {QUALIFICATION_KEY} is being rewritten concurrently by another \
         qualify run; retried {MAX_RERECORD_ATTEMPTS} times without a stable version. Re-run \
         `ravel-cli store qualify` once concurrent runs have stopped"
    ))
}

/// [`bucket_probe_lines`] from the concrete S3 store when `built` holds one,
/// else through the `dyn ObjectStoreBackend` impls.
pub async fn built_bucket_probe_lines(built: &BuiltStore) -> Vec<String> {
    match built {
        BuiltStore::S3 { store, .. } => bucket_probe_lines(store.as_ref()).await,
        BuiltStore::Other(store) => bucket_probe_lines(store.as_ref()).await,
    }
}

/// Label of the stored-checksum echo line in `store qualify`'s output.
pub const CHECKSUM_ECHO_LABEL: &str = "checksum/stored_echo";

/// The body the echo check PUTs.
const CHECKSUM_ECHO_BODY: &[u8] = b"ravel-cli store qualify stored-checksum echo probe";

/// Outcome of the stored-checksum echo check: whether the endpoint returned,
/// on a whole-object GET with checksum mode, the checksum it stored for a PUT
/// that carried one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChecksumEcho {
    /// The endpoint returned a stored checksum and it matched the body.
    Verified,
    /// The endpoint returned no stored checksum; reads are served unverified.
    NotReturned,
    /// The check did not run, for the stated reason.
    NotChecked(String),
    /// The endpoint returned a checksum that does not match the one sent, or
    /// the probe itself failed. A qualify failure.
    Failed(String),
}

impl ChecksumEcho {
    /// The one line `store qualify` prints for this outcome.
    pub fn line(&self) -> String {
        let text = match self {
            ChecksumEcho::Verified => "verified: the endpoint returned a stored checksum for \
                                      the crc64nvme probe PUT read back whole, and it matched \
                                      the body"
                .to_string(),
            ChecksumEcho::NotReturned => "not returned: the endpoint returned no stored checksum \
                                          for a crc64nvme PUT read back whole, so whole-object \
                                          reads are served unverified"
                .to_string(),
            ChecksumEcho::NotChecked(reason) => format!("not checked ({reason})"),
            ChecksumEcho::Failed(detail) => format!("FAIL {detail}"),
        };
        format!("{CHECKSUM_ECHO_LABEL:<40} {text}")
    }

    /// The failure this outcome carries, if it fails qualification.
    pub fn failure(&self) -> Option<&str> {
        match self {
            ChecksumEcho::Failed(detail) => Some(detail),
            ChecksumEcho::Verified | ChecksumEcho::NotReturned | ChecksumEcho::NotChecked(_) => {
                None
            }
        }
    }
}

/// PUT a probe object at `key` with the store's upload checksum, GET it whole
/// with checksum mode, and report whether the endpoint returned the checksum
/// it stored. The probe object is deleted whatever the outcome; a refused
/// delete is returned beside the outcome rather than failing it, because it
/// says nothing about the checksum.
///
/// The S3 adapter verifies a returned CRC-64/NVME checksum against the body it
/// received and fails the read as corrupted on a mismatch, and counts a read
/// that came back with no checksum on [`S3Store::get_unverified`]; this reads
/// that count around its own GET. A returned SHA-256 checksum is counted the
/// same way, so under `sha256` a missing checksum cannot be told from a
/// returned one and the check does not run.
///
/// [`S3Store::get_unverified`]: ravel_object_store::s3::S3Store::get_unverified
pub async fn checksum_echo(built: &BuiltStore, key: &str) -> (ChecksumEcho, Option<String>) {
    let BuiltStore::S3 { store, http } = built else {
        return (
            ChecksumEcho::NotChecked(
                "not an S3 store: no upload checksum is stored or returned".to_string(),
            ),
            None,
        );
    };
    let outcome = checksum_echo_probe(store, http, key).await;
    if matches!(outcome, ChecksumEcho::NotChecked(_)) {
        return (outcome, None);
    }
    let cleanup_error = match store.delete(key).await {
        Ok(()) => None,
        Err(err) => Some(format!(
            "note: the probe object {key} was left in place: {err}"
        )),
    };
    (outcome, cleanup_error)
}

async fn checksum_echo_probe(
    store: &ravel_object_store::s3::S3Store,
    http: &ravel_object_store::s3::S3HttpConfig,
    key: &str,
) -> ChecksumEcho {
    match http.upload_integrity {
        UploadIntegrity::Off => {
            return ChecksumEcho::NotChecked("upload integrity off".to_string());
        }
        UploadIntegrity::Sha256 => {
            return ChecksumEcho::NotChecked(
                "upload integrity sha256: a stored SHA-256 checksum is not recomputed on read, so \
                 its return cannot be observed"
                    .to_string(),
            );
        }
        UploadIntegrity::Crc64Nvme => {}
    }
    if !http.request_stored_checksum {
        return ChecksumEcho::NotChecked(
            "--s3-request-stored-checksum=false: the stored checksum is not requested".to_string(),
        );
    }

    let body = Bytes::from_static(CHECKSUM_ECHO_BODY);
    if let Err(err) = store.put(key, body.clone(), PutOptions::default()).await {
        return ChecksumEcho::Failed(format!("the probe PUT of {key} failed: {err}"));
    }
    let unverified_before = store.get_unverified();
    let read = store.get(key, GetRange::Full).await;
    let unverified_after = store.get_unverified();
    match read {
        Err(StoreError::Corrupted(detail)) => ChecksumEcho::Failed(format!(
            "the endpoint returned a checksum for {key} that does not match the crc64nvme \
             checksum sent with it: {detail}"
        )),
        Err(err) => ChecksumEcho::Failed(format!("the probe GET of {key} failed: {err}")),
        Ok(got) if got.data != body => ChecksumEcho::Failed(format!(
            "the probe GET of {key} returned {} bytes that differ from the {} bytes written",
            got.data.len(),
            body.len()
        )),
        Ok(_) if unverified_after == unverified_before => ChecksumEcho::Verified,
        Ok(_) => ChecksumEcho::NotReturned,
    }
}

/// Probe `source` once for both informational reports and render them: the
/// Object Lock / versioning line, then [`bucket_config_report_lines`]. One
/// probe call, so a source that reads the bucket for its answers (`S3Store`)
/// reads it once per qualify run rather than once per report.
pub async fn bucket_probe_lines<S: BucketProbesSource + ?Sized>(source: &S) -> Vec<String> {
    let probes = probe_bucket_lock_and_config(source).await;
    let mut lines = vec![format!(
        "{:<40} {} (informational, non-blocking) {}",
        "object_lock/versioning",
        probes.object_lock.status.name(),
        probes.object_lock.detail
    )];
    lines.extend(bucket_config_report_lines(&probes.bucket_config));
    lines
}

/// Render the informational required-bucket-configuration report for a
/// [`BucketConfigProbe`] (ADR-0064 §7): one line per observed setting (all
/// clearly labeled informational and non-blocking), followed by one line per
/// contract-violation alarm from [`bucket_config_alarms`]. A non-compliant rule
/// names its reason on its own line. Factored out of [`qualify`] so the
/// compliant/non-compliant reporting can be tested without a live versioned
/// bucket.
pub fn bucket_config_report_lines(probe: &BucketConfigProbe) -> Vec<String> {
    let rule_line = |label: &str, status: &LifecycleRuleStatus| match status {
        LifecycleRuleStatus::NonCompliant(reason) => format!(
            "{label:<40} {} (informational, non-blocking) {reason}",
            status.name()
        ),
        LifecycleRuleStatus::Present
        | LifecycleRuleStatus::Absent
        | LifecycleRuleStatus::Unknown => {
            format!(
                "{label:<40} {} (informational, non-blocking)",
                status.name()
            )
        }
    };
    let mut lines = vec![
        format!(
            "{:<40} {} (informational, non-blocking) {}",
            "bucket/versioning",
            probe.versioning.name(),
            probe.detail
        ),
        rule_line(
            "lifecycle/abort_incomplete_multipart",
            &probe.abort_incomplete_multipart_upload,
        ),
        rule_line(
            "lifecycle/noncurrent_version_expiration",
            &probe.noncurrent_version_expiration,
        ),
    ];
    for alarm in bucket_config_alarms(probe) {
        lines.push(format!("{:<40} {alarm}", "bucket/config"));
    }
    lines
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::conformance::{LifecycleRuleStatus, VersioningStatus};

    /// A compliant bucket configuration (ADR-0064 §7) produces the three
    /// informational lines and no alarm; a non-compliant one (versioning on
    /// with no noncurrent-version expiration rule) adds the named alarm line.
    /// This is the qualify-level report the operator sees, exercised for both
    /// configurations without a live versioned bucket.
    #[test]
    fn bucket_config_report_marks_compliant_vs_non_compliant() {
        let compliant = BucketConfigProbe {
            versioning: VersioningStatus::On,
            abort_incomplete_multipart_upload: LifecycleRuleStatus::Present,
            noncurrent_version_expiration: LifecycleRuleStatus::Present,
            detail: "fixture: compliant".to_string(),
        };
        let lines = bucket_config_report_lines(&compliant);
        assert_eq!(
            lines.len(),
            3,
            "a compliant bucket reports three informational lines and no alarm: {lines:?}"
        );
        assert!(lines.iter().all(|l| l.contains("informational")));
        assert!(
            !lines.iter().any(|l| l.contains("ALARM")),
            "a compliant bucket must not alarm: {lines:?}"
        );
        assert!(lines[0].contains("bucket/versioning"));
        assert!(lines[0].contains(" on "));

        let non_compliant = BucketConfigProbe {
            versioning: VersioningStatus::On,
            abort_incomplete_multipart_upload: LifecycleRuleStatus::Present,
            noncurrent_version_expiration: LifecycleRuleStatus::Absent,
            detail: "fixture: non-compliant".to_string(),
        };
        let lines = bucket_config_report_lines(&non_compliant);
        assert!(
            lines
                .iter()
                .any(|l| l.contains("ALARM") && l.contains("unsupported configuration")),
            "a versioned bucket without a noncurrent-version rule must alarm: {lines:?}"
        );
    }

    /// Bodies a versioned, locked bucket answers with, whose one covering
    /// lifecycle rule carries an abort window of 30 days.
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
                 <AbortIncompleteMultipartUpload><DaysAfterInitiation>30</DaysAfterInitiation>\
                 </AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"
            }
            "object-lock" => {
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                 </ObjectLockConfiguration>"
            }
            other => panic!("unexpected subresource {other:?}"),
        }
    }

    /// Stand up a fake S3 endpoint over [`fake_bucket_body`], returning its
    /// base URL and the count of requests and body bytes it served.
    async fn spawn_fake_bucket() -> (
        String,
        Arc<std::sync::atomic::AtomicU64>,
        Arc<std::sync::atomic::AtomicU64>,
    ) {
        use std::sync::atomic::{AtomicU64, Ordering};
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
        (format!("http://{addr}"), served, served_bytes)
    }

    /// One qualify run reads the bucket once for both informational reports:
    /// three control-plane GETs (versioning, lifecycle, object-lock), where
    /// probing the two reports separately costs six. The abort rule that
    /// covers t/ with a 30-day window prints `non-compliant` with its reason on
    /// its own line, and the NOTE names the same reason.
    #[tokio::test]
    async fn bucket_probe_lines_read_the_bucket_once_and_name_a_non_compliant_rule() {
        use std::sync::atomic::Ordering;

        use ravel_object_store::StoreMetrics;
        use ravel_object_store::conformance::{probe_bucket_config, probe_object_lock};
        use ravel_object_store::instrument::ControlPlaneMetricsSnapshot;
        use ravel_object_store::s3::{S3Config, S3Store};

        let (endpoint, served, served_bytes) = spawn_fake_bucket().await;
        let metrics: Arc<StoreMetrics> = Arc::default();
        let store = S3Store::with_metrics(
            S3Config {
                bucket: "ravel-test".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some(endpoint),
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
            Arc::clone(&metrics),
        )
        .expect("store");

        let lines = bucket_probe_lines(&store).await;
        let reason = "rule \"ravel\": AbortIncompleteMultipartUpload is 30 days, more than 7";
        assert_eq!(
            lines,
            vec![
                format!(
                    "{:<40} enabled (informational, non-blocking) Object Lock is enabled on the \
                     bucket (?object-lock)",
                    "object_lock/versioning"
                ),
                format!(
                    "{:<40} on (informational, non-blocking) derived from the ADR-1727 \
                     bucket-protection control plane (?versioning, ?lifecycle over signed \
                     read-only GETs)",
                    "bucket/versioning"
                ),
                format!(
                    "{:<40} non-compliant (informational, non-blocking) {reason}",
                    "lifecycle/abort_incomplete_multipart"
                ),
                format!(
                    "{:<40} present (informational, non-blocking)",
                    "lifecycle/noncurrent_version_expiration"
                ),
                format!(
                    "{:<40} NOTE: the REQUIRED AbortIncompleteMultipartUpload lifecycle rule (7 \
                     days or less) covers t/ but does not meet the contract: {reason} (ADR-0064 \
                     §7 point 3). Abandoned multipart uploads stay billable for longer than the \
                     contract allows. The NOTE prefix reflects the probe's limits, not an \
                     optional requirement.",
                    "bucket/config"
                ),
            ]
        );
        let bytes = served_bytes.load(Ordering::Relaxed);
        assert_eq!(served.load(Ordering::Relaxed), 3);
        assert_eq!(
            metrics.control_plane(),
            ControlPlaneMetricsSnapshot {
                requests: 3,
                calls: 3,
                response_bytes: bytes,
            }
        );
        assert_eq!(bytes, 569);

        // The two probes asked one at a time each run a whole report.
        probe_object_lock(&store).await;
        probe_bucket_config(&store).await;
        assert_eq!(served.load(Ordering::Relaxed), 3 + 6);
        assert_eq!(metrics.control_plane().requests, 3 + 6);
        assert_eq!(
            metrics.snapshot(),
            ravel_object_store::StoreMetricsSnapshot::default()
        );
    }

    /// `--store s3` parsed as an operator types it, against `endpoint`, with
    /// `extra` flags appended.
    fn s3_store(endpoint: &str, extra: &[&str]) -> BuiltStore {
        use clap::Parser;
        let mut argv = vec![
            "ravel-cli",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            endpoint,
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
        ];
        argv.extend_from_slice(extra);
        let args = crate::store::StoreArgs::try_parse_from(argv).expect("flags parse");
        crate::store::build_store_handle(&args, Some(ravel_object_store::s3::LIST_PAGE_SIZE))
            .expect("s3 builds")
    }

    fn bucket_answers() -> [(&'static str, axum::http::StatusCode, &'static str); 3] {
        use axum::http::StatusCode;
        [
            ("versioning", StatusCode::OK, fake_bucket_body("versioning")),
            ("lifecycle", StatusCode::OK, fake_bucket_body("lifecycle")),
            (
                "object-lock",
                StatusCode::OK,
                fake_bucket_body("object-lock"),
            ),
        ]
    }

    /// Issue #2197, CLI half: the store `store qualify` builds from `--store
    /// s3` keeps the concrete `S3Store`, so its bucket probe lines read the
    /// bucket (three control-plane GETs) and report `enabled` and `on`. The
    /// same store as `dyn ObjectStoreBackend` reads nothing and reports
    /// `unknown`, which is what qualify printed before.
    #[tokio::test]
    async fn qualify_probe_lines_read_the_bucket_through_the_concrete_s3_store() {
        use crate::fake_s3::{Echo, spawn};

        let (endpoint, fake) = spawn(Echo::Stored, &bucket_answers()).await;
        let built = s3_store(&endpoint, &[]);
        let lines = built_bucket_probe_lines(&built).await;
        assert_eq!(
            fake.control_plane(),
            vec!["versioning", "lifecycle", "object-lock"]
        );
        assert!(
            lines[0].starts_with(&format!("{:<40} enabled ", "object_lock/versioning")),
            "{lines:?}"
        );
        assert!(
            lines[1].starts_with(&format!("{:<40} on ", "bucket/versioning")),
            "{lines:?}"
        );

        let through_dyn = built_bucket_probe_lines(&BuiltStore::Other(built.backend())).await;
        assert!(
            through_dyn[0].starts_with(&format!("{:<40} unknown ", "object_lock/versioning")),
            "{through_dyn:?}"
        );
        assert!(
            through_dyn[1].starts_with(&format!("{:<40} unknown ", "bucket/versioning")),
            "{through_dyn:?}"
        );
        assert_eq!(fake.control_plane().len(), 3, "the dyn path reads nothing");
    }

    /// The echo check against an endpoint that returns the stored checksum,
    /// one that returns none, and one that returns a different one: verified,
    /// not returned, and a qualify failure. The probe PUT carries
    /// `x-amz-checksum-crc64nvme`, and the probe object is deleted each time.
    #[tokio::test]
    async fn checksum_echo_reports_verified_not_returned_and_mismatch() {
        use crate::fake_s3::{Echo, spawn};

        let key = "sys/qualify/run/checksum-echo";
        for (echo, expected) in [
            (Echo::Stored, ChecksumEcho::Verified),
            (Echo::Nothing, ChecksumEcho::NotReturned),
        ] {
            let (endpoint, fake) = spawn(echo, &[]).await;
            let (outcome, cleanup) = checksum_echo(&s3_store(&endpoint, &[]), key).await;
            assert_eq!(outcome, expected, "{echo:?}");
            assert_eq!(outcome.failure(), None);
            assert_eq!(cleanup, None);
            let puts = fake.puts();
            assert_eq!(puts.len(), 1, "{puts:?}");
            assert!(
                puts[0]
                    .checksum_headers
                    .iter()
                    .any(|(name, _)| name == "x-amz-checksum-crc64nvme"),
                "{puts:?}"
            );
            assert_eq!(fake.deletes(), vec![key.to_string()]);
            assert_eq!(fake.object_count(), 0, "the probe object is cleaned up");
        }

        assert_eq!(
            ChecksumEcho::Verified.line(),
            format!(
                "{:<40} verified: the endpoint returned a stored checksum for the crc64nvme \
                 probe PUT read back whole, and it matched the body",
                "checksum/stored_echo"
            )
        );
        assert_eq!(
            ChecksumEcho::NotReturned.line(),
            format!(
                "{:<40} not returned: the endpoint returned no stored checksum for a crc64nvme \
                 PUT read back whole, so whole-object reads are served unverified",
                "checksum/stored_echo"
            )
        );

        let (endpoint, fake) = spawn(Echo::Wrong, &[]).await;
        let (outcome, _) = checksum_echo(&s3_store(&endpoint, &[]), key).await;
        let failure = outcome
            .failure()
            .expect("a mismatched checksum fails qualify");
        assert!(
            failure.contains("does not match the crc64nvme checksum sent with it"),
            "{failure}"
        );
        assert!(outcome.line().contains(" FAIL "), "{}", outcome.line());
        assert_eq!(fake.deletes(), vec![key.to_string()]);
    }

    /// A credential that cannot delete the probe object leaves it in place:
    /// the outcome stands, and the note names the key.
    #[tokio::test]
    async fn checksum_echo_keeps_its_outcome_when_the_probe_cannot_be_deleted() {
        use crate::fake_s3::{Echo, spawn};

        let key = "sys/qualify/run/checksum-echo";
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        fake.refuse_deletes
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let (outcome, cleanup) = checksum_echo(&s3_store(&endpoint, &[]), key).await;
        assert_eq!(outcome, ChecksumEcho::Verified);
        let note = cleanup.expect("a refused delete is reported");
        assert!(
            note.starts_with(&format!("note: the probe object {key} was left in place: ")),
            "{note}"
        );
        assert_eq!(fake.object_count(), 1);
    }

    /// With upload integrity off, with the stored checksum not requested, or
    /// on a non-S3 backend, the check says why it did not run and sends
    /// nothing.
    #[tokio::test]
    async fn checksum_echo_is_not_checked_without_a_stored_checksum_to_ask_for() {
        use crate::fake_s3::{Echo, spawn};

        let key = "sys/qualify/run/checksum-echo";
        let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
        let (off, _) =
            checksum_echo(&s3_store(&endpoint, &["--s3-upload-integrity", "off"]), key).await;
        assert_eq!(
            off.line(),
            format!(
                "{:<40} not checked (upload integrity off)",
                "checksum/stored_echo"
            )
        );
        let (not_requested, _) = checksum_echo(
            &s3_store(&endpoint, &["--s3-request-stored-checksum=false"]),
            key,
        )
        .await;
        assert!(
            matches!(&not_requested, ChecksumEcho::NotChecked(reason)
                if reason.starts_with("--s3-request-stored-checksum=false")),
            "{not_requested:?}"
        );
        let (memory, _) = checksum_echo(
            &BuiltStore::Other(Arc::new(ravel_object_store::memory::MemoryStore::new())),
            key,
        )
        .await;
        assert!(matches!(memory, ChecksumEcho::NotChecked(_)), "{memory:?}");
        assert!(
            fake.puts().is_empty(),
            "nothing was sent: {:?}",
            fake.puts()
        );
    }
}
