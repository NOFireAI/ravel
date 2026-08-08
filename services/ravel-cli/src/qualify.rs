//! `ravel-cli store qualify` (ADR-0050 section 6, issue #478): runs
//! `ravel_object_store::conformance`'s empirical suite against a configured
//! backend and, on a pass, records the outcome at `sys/qualification` via
//! `CreateIfAbsent` -- once per bucket, never overwritten.

use std::sync::Arc;

use bytes::Bytes;
use ravel_object_store::conformance::{
    BucketConfigProbe, CONFORMANCE_SUITE_VERSION, bucket_config_alarms, probe_bucket_config,
    probe_object_lock, run_conformance_suite,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};

// The record and its key now live in `ravel-object-store` so `ravel-server`
// startup (ADR-0050 section 6, EC7) and this writer share one definition.
// Re-exported here so existing `ravel_cli::qualify::{QUALIFICATION_KEY,
// QualificationRecord}` call sites (and the in-process CLI test) keep resolving
// against the CLI's own module path.
pub use ravel_object_store::conformance::{QUALIFICATION_KEY, QualificationRecord};

/// Run the conformance suite against `store` under a fresh scratch prefix and
/// print each property's outcome. On a pass, writes [`QualificationRecord`]
/// to `sys/qualification`; if one is already there (a prior qualifying run),
/// leaves it untouched and reports it instead. Returns an error -- without
/// writing anything -- if any property fails, naming which one(s).
pub async fn qualify(
    store: Arc<dyn ObjectStoreBackend>,
    backend_identity: String,
    run_id: &str,
) -> anyhow::Result<()> {
    let scratch_prefix = format!("sys/qualify/{run_id}/");
    let report = run_conformance_suite(store.as_ref(), &scratch_prefix).await;

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
    // unaffected). Today it always reports "unknown" through the
    // `ObjectStoreBackend` contract, which exposes no such query.
    let object_lock = probe_object_lock(store.as_ref()).await;
    println!(
        "{:<40} {} (informational, non-blocking) {}",
        "object_lock/versioning",
        object_lock.status.name(),
        object_lock.detail
    );

    // Informational required-bucket-configuration report (ADR-0064 §7, S2-16,
    // S4-12): bucket versioning state and the presence/absence of the two
    // sanctioned lifecycle rules, plus any contract-violation alarms. Like the
    // Object Lock probe above, it is informational-plus-alarming and never
    // affects whether qualification passes, what `sys/qualification` records, or
    // whether the server starts; `object_store` cannot enforce bucket policy, so
    // Ravel reports what it can observe (unknown through the trait contract) and
    // documents what it requires.
    let bucket_config = probe_bucket_config(store.as_ref()).await;
    for line in bucket_config_report_lines(&bucket_config) {
        println!("{line}");
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
            let existing = store
                .get(QUALIFICATION_KEY, GetRange::Full)
                .await
                .map_err(|err| {
                    anyhow::anyhow!("failed to read existing {QUALIFICATION_KEY}: {err}")
                })?;
            let existing: QualificationRecord = serde_json::from_slice(&existing.data)
                .map_err(|err| anyhow::anyhow!("{QUALIFICATION_KEY} is corrupt: {err}"))?;
            println!(
                "{QUALIFICATION_KEY} already recorded for {} (suite v{}, qualified at unix_ns={}); \
                 not overwritten -- qualification is once per bucket, per ADR-0050 section 6",
                existing.backend_identity, existing.suite_version, existing.qualified_unix_ns
            );
            Ok(())
        }
        Err(err) => Err(anyhow::anyhow!(
            "qualification passed but writing {QUALIFICATION_KEY} failed: {err}"
        )),
    }
}

/// Render the informational required-bucket-configuration report for a
/// [`BucketConfigProbe`] (ADR-0064 §7): one line per observed setting (all
/// clearly labeled informational and non-blocking), followed by one line per
/// contract-violation alarm from [`bucket_config_alarms`]. Factored out of
/// [`qualify`] so the compliant/non-compliant reporting can be tested without a
/// live versioned bucket (the trait contract only ever reports `unknown`).
pub fn bucket_config_report_lines(probe: &BucketConfigProbe) -> Vec<String> {
    let mut lines = vec![
        format!(
            "{:<40} {} (informational, non-blocking) {}",
            "bucket/versioning",
            probe.versioning.name(),
            probe.detail
        ),
        format!(
            "{:<40} {} (informational, non-blocking)",
            "lifecycle/abort_incomplete_multipart",
            probe.abort_incomplete_multipart_upload.name(),
        ),
        format!(
            "{:<40} {} (informational, non-blocking)",
            "lifecycle/noncurrent_version_expiration",
            probe.noncurrent_version_expiration.name(),
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
}
