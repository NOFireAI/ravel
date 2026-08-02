//! `ravel-cli store qualify` (ADR-0050 section 6, issue #478): runs
//! `ravel_object_store::conformance`'s empirical suite against a configured
//! backend and, on a pass, records the outcome at `sys/qualification` via
//! `CreateIfAbsent` -- once per bucket, never overwritten.

use std::sync::Arc;

use bytes::Bytes;
use ravel_object_store::conformance::{CONFORMANCE_SUITE_VERSION, run_conformance_suite};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
use serde::{Deserialize, Serialize};

/// Root-prefix key for the durable qualification record (ADR-0050 section 6,
/// "New durable objects and key-layout entries": root prefix `sys/`).
pub const QUALIFICATION_KEY: &str = "sys/qualification";

/// Durable record written to [`QUALIFICATION_KEY`] on a passing run. JSON, not
/// protobuf: `proto/ravel/sys.proto` (which ADR-0050 section 6 names for the
/// eventual durable `sys/*` messages) is out of scope for this change --
/// see the final report for why this is a deliberate, flagged gap rather
/// than a silent one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualificationRecord {
    pub suite_version: u32,
    pub backend_identity: String,
    pub qualified_unix_ns: i64,
    pub passed_properties: Vec<String>,
}

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
