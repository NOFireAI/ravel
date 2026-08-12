//! Bucket-protection startup gate (ADR-0072 decision 3).
//!
//! `crates/ravel-object-store/src/conformance.rs` already probes a backend
//! for Object Lock / versioning status and required lifecycle configuration
//! (ADR-0055 section 3, ADR-0064 section 7), but those probes are
//! informational only: `ravel-cli store qualify` prints them and nothing
//! gates on the result. `--require-bucket-protection` (default off) turns
//! the same probes into a fail-closed startup check, documented in
//! docs/object-store-contract.md's "Required bucket configuration" section:
//!
//! - [`ObjectLockStatus::Disabled`], or an `"ALARM:"`-prefixed entry from
//!   `bucket_config_alarms` (the versioning-without-expiration
//!   misconfiguration), refuses to start.
//! - [`ObjectLockStatus::Unknown`] -- what every backend reachable only
//!   through `ObjectStoreBackend` reports today, since no adapter can
//!   actually answer this query -- logs one warning and sets the
//!   `ravel_bucket_protection_unknown` gauge to 1, so a fleet can alarm on it
//!   without being blocked by it.
//! - [`ObjectLockStatus::Enabled`] with no alarms starts clean.
//!
//! # Generic over the probe source, not `dyn ObjectStoreBackend`
//!
//! `ObjectLockProbeSource` and `BucketConfigProbeSource` are implemented
//! directly on the `dyn ObjectStoreBackend` trait object type (not via a
//! blanket generic impl), so that impl is the only one a value erased to
//! `Arc<dyn ObjectStoreBackend>` can ever reach, and it unconditionally
//! reports `Unknown`. [`enforce`] is therefore generic over `S:
//! ObjectLockProbeSource + BucketConfigProbeSource + ?Sized` rather than
//! taking `&dyn ObjectStoreBackend`: production monomorphizes it over `dyn
//! ObjectStoreBackend` (today, always `Unknown`, by design per ADR-0042
//! decision 3), while tests monomorphize it over small fixture structs that
//! implement both traits directly, bypassing `ObjectStoreBackend` entirely,
//! to exercise `Enabled`/`Disabled` as well. This is the same fixture
//! pattern conformance.rs's own tests use (`FixedLockSource`,
//! `FixedBucketConfig`).
//!
//! # Enforcement stays at the bucket/IAM layer
//!
//! Nothing here configures or verifies Object Lock or lifecycle policy
//! in-process; `object_store` 0.14 exposes no such API, and ADR-0042
//! decision 3 reserves a real in-process capability for its own
//! trait-extending ADR. This gate only makes a silently-unprotected
//! production deployment impossible to start.

use std::sync::atomic::{AtomicU64, Ordering};

use ravel_object_store::conformance::{
    BucketConfigProbeSource, ObjectLockProbeSource, ObjectLockStatus, bucket_config_alarms,
    probe_bucket_config, probe_object_lock,
};

/// Whether the most recent [`enforce`] call observed
/// [`ObjectLockStatus::Unknown`] (the case every production backend reports
/// today). Rendered at `/metrics` as the `ravel_bucket_protection_unknown`
/// gauge. Process-global, matching the established pattern for the other
/// single-source, no-label `/metrics` values this crate renders directly in
/// `metrics::render` (`crate::store_probe::store_reachable`).
static BUCKET_PROTECTION_UNKNOWN: AtomicU64 = AtomicU64::new(0);

/// The `ravel_bucket_protection_unknown` gauge source: 1 if the last
/// [`enforce`] call (with the flag on) observed [`ObjectLockStatus::Unknown`],
/// 0 otherwise, including when the flag is off and `enforce` never ran.
pub fn bucket_protection_unknown() -> u64 {
    BUCKET_PROTECTION_UNKNOWN.load(Ordering::Relaxed)
}

fn set_bucket_protection_unknown(unknown: bool) {
    BUCKET_PROTECTION_UNKNOWN.store(u64::from(unknown), Ordering::Relaxed);
}

/// A typed bucket-protection-gate failure. Every variant refuses to start;
/// `Unknown` is deliberately not a variant here, since it is a warn-and-
/// continue outcome, not a refusal (see [`enforce`]).
#[derive(Debug, thiserror::Error)]
pub enum BucketProtectionError {
    #[error(
        "the backend affirmatively reports neither Object Lock nor bucket versioning enabled \
         ({detail}). ADR-0072 decision 3 and docs/object-store-contract.md's \"Required bucket \
         configuration\" section require Object Lock (compliance mode) on sys/*, t/*/*/prov, \
         commit records t/*/*/c/*, and t/*/catalog/*/* HEAD history for a production \
         deployment. Configure the bucket, then restart, or run without \
         --require-bucket-protection for a non-production deployment."
    )]
    ObjectLockDisabled { detail: String },
    #[error(
        "the backend's bucket configuration violates ADR-0064 section 7: {alarm} Configure the \
         bucket, then restart, or run without --require-bucket-protection for a non-production \
         deployment."
    )]
    BucketConfigAlarm { alarm: String },
}

/// The outcome of a clean (non-refusing) [`enforce`] call, so a caller (and a
/// test) can assert on it directly rather than solely through the
/// process-global gauge, which races under parallel test execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketProtectionOutcome {
    /// Object Lock / versioning was affirmatively confirmed enabled, and no
    /// bucket-configuration alarm fired.
    Enabled,
    /// The backend could not answer (every backend reachable only through
    /// `ObjectStoreBackend` today); a warning was logged and the
    /// `ravel_bucket_protection_unknown` gauge was set to 1.
    Unknown,
}

/// Enforce the ADR-0072 decision 3 bucket-protection gate against `source`.
///
/// Always sets the `ravel_bucket_protection_unknown` gauge as a side effect
/// (0 on `Enabled`, 1 on `Unknown`) so `/metrics` reflects the latest call;
/// callers that also need the outcome without racing the gauge under
/// parallel tests should use the returned [`BucketProtectionOutcome`].
///
/// Read-only: both probes are GETs (or affirm nothing at all, in the
/// production `dyn ObjectStoreBackend` case), so this is safe to run before
/// any listener binds, mirroring [`crate::qualification::enforce`].
pub async fn enforce<S>(source: &S) -> Result<BucketProtectionOutcome, BucketProtectionError>
where
    S: ObjectLockProbeSource + BucketConfigProbeSource + ?Sized,
{
    let lock_probe = probe_object_lock(source).await;
    if lock_probe.status == ObjectLockStatus::Disabled {
        return Err(BucketProtectionError::ObjectLockDisabled {
            detail: lock_probe.detail,
        });
    }

    let config_probe = probe_bucket_config(source).await;
    for alarm in bucket_config_alarms(&config_probe) {
        if let Some(alarm) = alarm.strip_prefix("ALARM: ") {
            return Err(BucketProtectionError::BucketConfigAlarm {
                alarm: alarm.to_string(),
            });
        }
    }

    if lock_probe.status == ObjectLockStatus::Unknown {
        tracing::warn!(
            detail = %lock_probe.detail,
            "--require-bucket-protection: Object Lock / versioning status is unknown for this \
             backend; the platform cannot confirm the ADR-0072 decision 3 bucket-protection \
             contract is in place. Confirm it out of band. Starting anyway \
             (ravel_bucket_protection_unknown=1)."
        );
        set_bucket_protection_unknown(true);
        return Ok(BucketProtectionOutcome::Unknown);
    }

    set_bucket_protection_unknown(false);
    Ok(BucketProtectionOutcome::Enabled)
}

/// The single call site `main.rs` uses: run [`enforce`] only when `required`
/// (the `--require-bucket-protection` flag) is set, returning `None` when the
/// flag is off so the caller can see the gate was skipped rather than
/// confusing that with a clean `Enabled` result. With `required` false this
/// never touches either probe and never sets the gauge, so the default-off
/// path is byte-identical to startup before this gate existed.
pub async fn enforce_if_required<S>(
    required: bool,
    source: &S,
) -> Result<Option<BucketProtectionOutcome>, BucketProtectionError>
where
    S: ObjectLockProbeSource + BucketConfigProbeSource + ?Sized,
{
    if !required {
        return Ok(None);
    }
    enforce(source).await.map(Some)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::LazyLock;

    use ravel_object_store::conformance::{
        BucketConfigProbe, LifecycleRuleStatus, ObjectLockProbe, VersioningStatus,
    };
    use tokio::sync::Mutex;

    use super::*;

    /// Serializes every test that reads or writes
    /// `BUCKET_PROTECTION_UNKNOWN`: it is a process-global shared by every
    /// test in this binary, and `cargo test` runs tests in parallel threads
    /// by default, so two gauge-touching tests racing would let one test's
    /// assertion observe the other's write. Held for the gauge-sensitive
    /// portion of each such test; tests that never reach a gauge write (the
    /// `Disabled` and alarm refusals, which return before either
    /// `set_bucket_protection_unknown` call) do not need it. `tokio::sync::
    /// Mutex::new` is not `const`, so this needs `LazyLock` rather than a
    /// plain `static` initializer (and a `tokio::sync::Mutex`, not
    /// `std::sync::Mutex`, since the guard is held across an `.await`).
    static GAUGE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    /// A fixture implementing both probe-source traits directly, bypassing
    /// `ObjectStoreBackend` entirely, so it can report `Enabled`/`Disabled`
    /// as well as `Unknown` -- the same pattern conformance.rs's own tests
    /// use (`FixedLockSource`, `FixedBucketConfig`).
    struct Fixture {
        lock: ObjectLockProbe,
        config: BucketConfigProbe,
    }

    #[async_trait::async_trait]
    impl ObjectLockProbeSource for Fixture {
        async fn object_lock_status(&self) -> ObjectLockProbe {
            self.lock.clone()
        }
    }

    #[async_trait::async_trait]
    impl BucketConfigProbeSource for Fixture {
        async fn bucket_config(&self) -> BucketConfigProbe {
            self.config.clone()
        }
    }

    fn clean_config() -> BucketConfigProbe {
        BucketConfigProbe {
            versioning: VersioningStatus::Off,
            abort_incomplete_multipart_upload: LifecycleRuleStatus::Present,
            noncurrent_version_expiration: LifecycleRuleStatus::Unknown,
            detail: "fixture".to_string(),
        }
    }

    /// Flipped line for this test: `enforce`'s `if lock_probe.status ==
    /// ObjectLockStatus::Disabled { return Err(...) }` check. Removing that
    /// early return (or changing `Disabled` to a status this fixture never
    /// reports) makes this test observe `Ok(_)` instead of the expected
    /// `Err(ObjectLockDisabled)`.
    #[tokio::test]
    async fn startup_refuses_when_object_lock_disabled() {
        let fixture = Fixture {
            lock: ObjectLockProbe::disabled("fixture: no Object Lock, no versioning"),
            config: clean_config(),
        };
        let err = enforce(&fixture)
            .await
            .expect_err("Disabled must refuse to start");
        assert!(
            matches!(err, BucketProtectionError::ObjectLockDisabled { .. }),
            "expected ObjectLockDisabled, got: {err}"
        );
    }

    /// Flipped line: the `if probe.versioning == VersioningStatus::On &&
    /// probe.noncurrent_version_expiration == LifecycleRuleStatus::Absent`
    /// check inside `conformance::bucket_config_alarms`, surfaced here
    /// through the `"ALARM: "` branch of `enforce`'s alarm loop. Given a
    /// probe with versioning on and the expiration rule absent, that ALARM
    /// string is produced and `enforce` must turn it into a refusal; without
    /// the `strip_prefix("ALARM: ")` branch this would fall through to
    /// `Ok`.
    #[tokio::test]
    async fn refuses_on_versioning_without_expiration_alarm() {
        let fixture = Fixture {
            lock: ObjectLockProbe::enabled("fixture: Object Lock enabled"),
            config: BucketConfigProbe {
                versioning: VersioningStatus::On,
                abort_incomplete_multipart_upload: LifecycleRuleStatus::Present,
                noncurrent_version_expiration: LifecycleRuleStatus::Absent,
                detail: "fixture".to_string(),
            },
        };
        let err = enforce(&fixture)
            .await
            .expect_err("versioning on without expiration must refuse to start");
        assert!(
            matches!(err, BucketProtectionError::BucketConfigAlarm { .. }),
            "expected BucketConfigAlarm, got: {err}"
        );
    }

    /// Flipped line: `enforce`'s `if lock_probe.status ==
    /// ObjectLockStatus::Unknown { ... set_bucket_protection_unknown(true);
    /// return Ok(Unknown) }` branch. Changing that condition (or dropping
    /// the `set_bucket_protection_unknown(true)` call) makes this test see
    /// the gauge stay at 0, or the outcome stop being `Unknown`.
    #[tokio::test]
    async fn startup_warns_and_sets_gauge_on_unknown() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let fixture = Fixture {
            lock: ObjectLockProbe::unknown("fixture: no API to ask"),
            config: clean_config(),
        };
        let outcome = enforce(&fixture)
            .await
            .expect("Unknown must not refuse to start");
        assert_eq!(outcome, BucketProtectionOutcome::Unknown);
        assert_eq!(
            bucket_protection_unknown(),
            1,
            "the gauge must be set to 1 on Unknown"
        );
    }

    /// Flipped line: the final `set_bucket_protection_unknown(false); Ok(...
    /// Enabled)` fallthrough. If `Enabled` were instead routed through the
    /// `Unknown` branch (e.g. by weakening the `==` check to accept
    /// `Enabled` too), this test's gauge assertion would observe 1 instead
    /// of 0.
    #[tokio::test]
    async fn startup_clean_when_enabled() {
        let _guard = GAUGE_TEST_LOCK.lock().await;
        let fixture = Fixture {
            lock: ObjectLockProbe::enabled("fixture: Object Lock enabled"),
            config: clean_config(),
        };
        let outcome = enforce(&fixture)
            .await
            .expect("Enabled with no alarms must start cleanly");
        assert_eq!(outcome, BucketProtectionOutcome::Enabled);
        assert_eq!(
            bucket_protection_unknown(),
            0,
            "the gauge must be 0 on a clean Enabled start"
        );
    }

    /// Flipped line: `enforce_if_required`'s `if !required { return Ok(None)
    /// }` early return. Deleting it (always calling `enforce`) makes this
    /// test observe an `Err(ObjectLockDisabled)` instead of `Ok(None)`,
    /// since the fixture reports `Disabled` -- exactly the byte-identical-
    /// default-off behavior deliverable 2 requires: without the flag, a
    /// `Disabled` probe must still let the server start.
    #[tokio::test]
    async fn default_off_does_not_gate() {
        let fixture = Fixture {
            lock: ObjectLockProbe::disabled("fixture: no Object Lock, no versioning"),
            config: clean_config(),
        };
        let outcome = enforce_if_required(false, &fixture)
            .await
            .expect("the flag being off must never refuse to start, regardless of probe status");
        assert_eq!(
            outcome, None,
            "the flag being off must skip the gate entirely, not just avoid refusing"
        );
    }
}
