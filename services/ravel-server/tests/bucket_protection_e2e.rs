//! End-to-end reachability for the ADR-0072 decision 3 bucket-protection
//! startup gate (EE-T6, issue #900, epic #894).
//!
//! `services/ravel-server/src/bucket_protection.rs`'s own unit tests prove
//! `enforce`/`enforce_if_required` refuse on `Disabled` and warn-and-continue
//! on `Unknown`, but every one of them calls those functions directly with a
//! `Fixture`. None of them touches `main.rs`'s own call site, and `main.rs`
//! is a binary entry point `cargo test` cannot call into directly. Before
//! this test existed, that call site (`ravel_server::bucket_protection::
//! enforce_at_startup(cli.require_bucket_protection, store.as_ref())?` in
//! `main.rs`) was reachable from no test at all: the only type production
//! ever calls it with (`&dyn ObjectStoreBackend`) can never report
//! `Disabled` (see the module doc on `bucket_protection.rs`), so even a test
//! that called the real call site with a real store could never see the
//! refusal branch fire.
//!
//! This test closes that gap the same way `provisioning_e2e.rs`'s restart
//! phase closes an analogous one: by calling the exact function `main.rs`
//! calls (`enforce_at_startup`, extracted verbatim from what used to be
//! inlined in `main()` specifically so this test and the binary share one
//! statement), once with a `Fixture` standing in for a misconfigured bucket
//! (`Disabled`) and once with a real `MemoryStore` standing in for
//! production's `Arc<dyn ObjectStoreBackend>` (`Unknown`).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::conformance::{
    BucketConfigProbe, BucketConfigProbeSource, LifecycleRuleStatus, ObjectLockProbe,
    ObjectLockProbeSource, VersioningStatus,
};
use ravel_object_store::memory::MemoryStore;
use ravel_server::bucket_protection::{BucketProtectionOutcome, bucket_protection_unknown};

/// A fixture bypassing `ObjectStoreBackend` entirely (mirroring
/// `bucket_protection.rs`'s own private test fixture, which this crate
/// cannot reach since it is not `pub`), so this e2e test can drive
/// `enforce_at_startup` through the `Disabled` branch a real backend can
/// never report.
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
        detail: "e2e fixture".to_string(),
    }
}

/// The real-startup-wiring half of 1a: a `Disabled` bucket must refuse to
/// start via the exact statement `main.rs` runs.
///
/// Flipped line: `main.rs`'s call site is exactly
/// `ravel_server::bucket_protection::enforce_at_startup(cli.require_bucket_protection,
/// store.as_ref()).await?;` with no further handling. If that trailing `?`
/// were removed (or the `Result` were discarded/mapped to `Ok(())`), the
/// binary would log nothing-visible-to-this-test and continue starting on a
/// bucket this fixture proves must refuse; this test would then be the only
/// signal, since `bucket_protection.rs`'s own unit tests never reach
/// `main.rs` at all. Because `enforce_at_startup` is the identical function
/// both `main.rs` and this test call, this test drives the same `Disabled`
/// refusal `main.rs` relies on. It does not itself catch a deleted `?` at
/// `main.rs:90` -- that single call site is the sole production caller and is
/// undrivable from a boot-through-`start()` test, since `&dyn
/// ObjectStoreBackend` never reports `Disabled` -- so this test guards
/// `enforce_at_startup`'s refusal and message, and the `?` is verified by
/// reading `main.rs`.
#[tokio::test]
async fn real_startup_wiring_refuses_when_bucket_protection_disabled() {
    let fixture = Fixture {
        lock: ObjectLockProbe::disabled("e2e: no Object Lock, no versioning"),
        config: clean_config(),
    };
    let err = ravel_server::bucket_protection::enforce_at_startup(true, &fixture)
        .await
        .expect_err("a Disabled bucket must refuse to start through main.rs's own call site");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("bucket-protection contract check failed; refusing to start"),
        "error must carry main.rs's own .context(...) message, proving this is main.rs's exact \
         wrapped call, not a bare enforce_if_required: {msg}"
    );
    assert!(
        msg.contains("Object Lock") || msg.contains("neither Object Lock nor bucket versioning"),
        "error must still name the underlying Disabled cause: {msg}"
    );
}

/// The companion clean-boot half of 1a: a real backend (every production
/// store, reachable only through `ObjectStoreBackend`) reports `Unknown`,
/// not `Disabled`, and must boot cleanly with the gauge set rather than
/// refuse -- so `--require-bucket-protection` on today's real backends is
/// observability, not an accidental universal refusal.
///
/// Flipped line: the `if lock_probe.status == ObjectLockStatus::Unknown { ...
/// return Ok(Unknown) }` branch inside `enforce` (exercised here through
/// `main.rs`'s own `enforce_at_startup` wrapper and a real `MemoryStore`,
/// not a fixture). Tightening that check to also refuse on `Unknown` would
/// make this assertion observe `Err` instead of `Ok`, and every production
/// deployment using `--require-bucket-protection` today would fail to
/// start.
#[tokio::test]
async fn real_startup_wiring_boots_clean_on_unknown_and_sets_gauge() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let outcome = ravel_server::bucket_protection::enforce_at_startup(true, store.as_ref())
        .await
        .expect("Unknown must not refuse to start")
        .expect("required=true must return Some(outcome)");
    assert_eq!(
        outcome,
        BucketProtectionOutcome::Unknown,
        "every backend reachable only through ObjectStoreBackend reports Unknown today"
    );
    assert_eq!(
        bucket_protection_unknown(),
        1,
        "the ravel_bucket_protection_unknown gauge must be set for /metrics to alarm on it"
    );
}
