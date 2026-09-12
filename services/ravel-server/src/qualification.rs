//! Store-backend qualification startup gate (ADR-0050 section 6 enforcement,
//! EC7).
//!
//! A production store backend is qualified empirically, once per bucket, by
//! `ravel-cli store qualify`, which records the outcome at `sys/qualification`
//! (the durable [`QualificationRecord`], relocated into
//! [`ravel_object_store::conformance`] so this reader and that writer share one
//! definition). This module is the fail-closed reader every server process runs
//! at startup: it refuses to start on a production [`StoreKind`] when the record
//! is absent, or when its `suite_version` is below the running binary's
//! [`CONFORMANCE_SUITE_VERSION`] floor.
//!
//! This is the same shape as EC3's tenancy check and EC4's GC-config check:
//! read-only, no writes, runs once before any listener binds, in every mode
//! (qualification is a store-backend property every mode's correctness depends
//! on, so it is not scoped to a subset of modes the way GC-config validation
//! is). [`StoreKind::Memory`], the semantics oracle, is exempt.
//!
//! # No fresh-bucket exemption
//!
//! Unlike the tenancy marker (EC3) and the GC-config object (EC4), which a
//! fresh bucket bootstraps and continues past, an absent `sys/qualification`
//! record is *always* a refusal on a production store. There is no
//! bootstrap-and-continue path: the record is only ever created by a
//! deliberate `ravel-cli store qualify` run against a live backend. A fresh
//! production deployment must therefore run `store qualify` before the server
//! can start at all. This is intentional (ADR-0050 section 6, and
//! docs/guides/operations.md's startup-invariants section), not a bug to route
//! around: a never-qualified backend has never been shown to honor the
//! conditional-write and read-after-write guarantees Ravel's durability
//! depends on.

use ravel_object_store::conformance::{CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};

use crate::config::StoreKind;

/// A typed qualification-gate failure. Every variant refuses to start; none
/// warn and continue (ADR-0050's single rule). Absent and stale-version are
/// deliberately distinct, nameable conditions rather than one generic message,
/// so an operator sees whether to run `store qualify` for the first time or to
/// re-qualify an out-of-date backend.
#[derive(Debug, thiserror::Error)]
pub enum QualificationError {
    #[error(
        "store qualification record {QUALIFICATION_KEY} is absent: this backend has never been \
         qualified with `ravel-cli store qualify`. A production store must be qualified before \
         the server can start (ADR-0050 section 6); this is intentional, not a fresh-bucket \
         bootstrap path. Run `ravel-cli store qualify` against this bucket, then restart."
    )]
    Absent,
    #[error(
        "store qualification record {QUALIFICATION_KEY} was recorded under suite version \
         {record_version}, but this binary requires at least version {required_version}: the \
         backend was qualified by an older suite that did not exercise every property this build \
         depends on. Re-run `ravel-cli store qualify` with a current build, then restart."
    )]
    StaleVersion {
        record_version: u32,
        required_version: u32,
    },
    #[error("object store error reading {QUALIFICATION_KEY}: {0}")]
    Store(#[from] StoreError),
    #[error("{QUALIFICATION_KEY} is corrupt and could not be decoded as JSON: {0}")]
    Decode(String),
}

/// Outcome of comparing the recorded `backend_identity` against the identity
/// this process is configured for (ADR-0050 section 6). Separated from the
/// logging in [`enforce`] so the comparison rule can be asserted directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityCheck {
    /// No configured identity to compare against (the exempt memory store, or a
    /// caller that did not supply one). Nothing is checked.
    Skipped,
    /// The recorded identity matches the configured one.
    Matches,
    /// The recorded identity differs from the configured one. A warning, never
    /// a refusal: see [`enforce`].
    Mismatch,
}

/// Compare a record's `backend_identity` against the configured one. A missing
/// configured identity is [`IdentityCheck::Skipped`], not a mismatch: the
/// caller had nothing to compare, so silence is correct.
fn check_identity(recorded: &str, configured: Option<&str>) -> IdentityCheck {
    match configured {
        None => IdentityCheck::Skipped,
        Some(configured) if configured == recorded => IdentityCheck::Matches,
        Some(_) => IdentityCheck::Mismatch,
    }
}

/// Enforce the qualification gate for `store_kind` against the durable
/// `sys/qualification` record (ADR-0050 section 6). Returns `Ok(())` when the
/// store is exempt ([`StoreKind::Memory`]) or a current-version record is
/// present; refuses with a specific [`QualificationError`] otherwise.
///
/// `configured_identity` is the [`ravel_object_store::conformance::s3_backend_identity`]
/// string this process is configured for, or `None` for the exempt memory
/// store. When it is present and differs from the record's `backend_identity`,
/// [`enforce`] logs a warning and starts anyway. The record lives in the
/// bucket, so a replicated, restored, or migrated bucket carries a
/// qualification a different backend earned; a mismatch is the signal for that.
/// It is deliberately not a refusal: the identity is endpoint-derived (see
/// [`ravel_object_store::conformance::s3_backend_identity`]), so an endpoint
/// rename or an addressing-style switch changes it with no backend change, and
/// a hard refusal on that benign case would be an outage an operator then
/// disables, leaving the check protecting nothing. This reader never writes, so
/// it cannot re-qualify either; warning is the honest report, mirroring the
/// bucket-protection gate's `Unknown` handling.
///
/// Read-only: a single GET of the fixed record key, no write on any path, so it
/// is safe to run before any listener binds in every mode.
pub async fn enforce(
    store: &dyn ObjectStoreBackend,
    store_kind: StoreKind,
    configured_identity: Option<&str>,
) -> Result<(), QualificationError> {
    // The semantics oracle is exempt: MemoryStore *is* the reference behavior
    // the suite falsifies other backends against, so qualifying it against
    // itself would be circular (ADR-0050 section 6).
    if matches!(store_kind, StoreKind::Memory) {
        return Ok(());
    }

    let record = match store.get(QUALIFICATION_KEY, GetRange::Full).await {
        Ok(outcome) => outcome,
        // Absent is its own refusal, distinct from a stale record: there is no
        // bootstrap-and-continue path here (see the module docs).
        Err(StoreError::NotFound) => return Err(QualificationError::Absent),
        Err(err) => return Err(QualificationError::Store(err)),
    };

    let record: ravel_object_store::conformance::QualificationRecord =
        serde_json::from_slice(&record.data)
            .map_err(|err| QualificationError::Decode(err.to_string()))?;

    if record.suite_version < CONFORMANCE_SUITE_VERSION {
        return Err(QualificationError::StaleVersion {
            record_version: record.suite_version,
            required_version: CONFORMANCE_SUITE_VERSION,
        });
    }

    if check_identity(&record.backend_identity, configured_identity) == IdentityCheck::Mismatch {
        tracing::warn!(
            recorded_identity = %record.backend_identity,
            configured_identity = configured_identity.unwrap_or_default(),
            "sys/qualification was recorded against a different backend identity than this \
             process is configured for. After a benign endpoint rename or addressing-style \
             change this is expected, but on a replicated, restored, or migrated bucket it means \
             this backend was never qualified. Verify the backend and re-run `ravel-cli store \
             qualify` if it is genuinely a different store."
        );
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use ravel_object_store::conformance::QualificationRecord;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};

    use super::*;

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    async fn write_record(store: &dyn ObjectStoreBackend, suite_version: u32) {
        let record = QualificationRecord {
            suite_version,
            backend_identity: "s3://ravel-test".to_string(),
            qualified_unix_ns: 1,
            passed_properties: vec!["conditional_write_create_if_absent".to_string()],
        };
        let body = serde_json::to_vec(&record).expect("encode");
        store
            .put(QUALIFICATION_KEY, Bytes::from(body), PutOptions::default())
            .await
            .expect("seed record");
    }

    /// A production store kind with no `sys/qualification` record refuses
    /// startup with the distinct "absent" error, never the stale-version one.
    #[tokio::test]
    async fn absent_record_on_production_store_refuses() {
        let store = store();
        let err = enforce(store.as_ref(), StoreKind::S3, None)
            .await
            .expect_err("an absent record on a production store must refuse startup");
        assert!(
            matches!(err, QualificationError::Absent),
            "absent must be its own condition, got: {err}"
        );
    }

    /// A record recorded under an older suite version refuses with the distinct
    /// stale-version error (not collapsed into the absent case).
    #[tokio::test]
    async fn stale_version_record_refuses_distinctly() {
        let store = store();
        write_record(store.as_ref(), CONFORMANCE_SUITE_VERSION - 1).await;
        let err = enforce(store.as_ref(), StoreKind::S3, None)
            .await
            .expect_err("a below-floor suite_version must refuse startup");
        match err {
            QualificationError::StaleVersion {
                record_version,
                required_version,
            } => {
                assert_eq!(record_version, CONFORMANCE_SUITE_VERSION - 1);
                assert_eq!(required_version, CONFORMANCE_SUITE_VERSION);
            }
            other => panic!("expected a stale-version refusal, got: {other}"),
        }
    }

    /// A present, current-version record starts cleanly on a production store.
    #[tokio::test]
    async fn current_record_on_production_store_starts() {
        let store = store();
        write_record(store.as_ref(), CONFORMANCE_SUITE_VERSION).await;
        enforce(store.as_ref(), StoreKind::S3, None)
            .await
            .expect("a current-version record must start cleanly");
    }

    /// The oracle is exempt: `StoreKind::Memory` starts regardless of whether a
    /// record is present, so a dev/test process is never gated on a
    /// qualification run.
    #[tokio::test]
    async fn memory_store_is_exempt_with_or_without_a_record() {
        let store = store();
        enforce(store.as_ref(), StoreKind::Memory, None)
            .await
            .expect("memory is exempt even with no record");
        write_record(store.as_ref(), CONFORMANCE_SUITE_VERSION).await;
        enforce(store.as_ref(), StoreKind::Memory, None)
            .await
            .expect("memory is exempt with a record too");
    }

    /// A corrupt record is a typed decode refusal, never a panic or a
    /// start-anyway.
    #[tokio::test]
    async fn corrupt_record_refuses_typed() {
        let store = store();
        store
            .put(
                QUALIFICATION_KEY,
                Bytes::from_static(b"not json"),
                PutOptions::default(),
            )
            .await
            .expect("seed garbage");
        let err = enforce(store.as_ref(), StoreKind::S3, None)
            .await
            .expect_err("a corrupt record must refuse, not panic");
        assert!(matches!(err, QualificationError::Decode(_)), "got: {err}");
    }

    /// The identity comparison rule: a match, a mismatch, and the no-configured
    /// case are the three distinct outcomes.
    #[test]
    fn check_identity_distinguishes_match_mismatch_and_skip() {
        assert_eq!(
            check_identity("s3://ravel-test", Some("s3://ravel-test")),
            IdentityCheck::Matches
        );
        assert_eq!(
            check_identity("s3://ravel-test", Some("s3://other@host")),
            IdentityCheck::Mismatch
        );
        assert_eq!(
            check_identity("s3://ravel-test", None),
            IdentityCheck::Skipped
        );
    }

    /// A record whose `backend_identity` differs from the configured one is a
    /// warning, never a refusal: the server still starts. The identity is
    /// endpoint-derived, so a benign rename must not be an outage (D2).
    #[tokio::test]
    async fn mismatched_identity_warns_but_starts() {
        let store = store();
        write_record(store.as_ref(), CONFORMANCE_SUITE_VERSION).await;
        enforce(
            store.as_ref(),
            StoreKind::S3,
            Some("s3://a-different-bucket@elsewhere"),
        )
        .await
        .expect("an identity mismatch warns, it does not refuse startup");
    }

    /// A matching configured identity starts cleanly, same as the no-identity
    /// path, so supplying an identity never tightens the gate into a refusal.
    #[tokio::test]
    async fn matching_identity_starts() {
        let store = store();
        write_record(store.as_ref(), CONFORMANCE_SUITE_VERSION).await;
        enforce(store.as_ref(), StoreKind::S3, Some("s3://ravel-test"))
            .await
            .expect("a matching identity must start cleanly");
    }
}
