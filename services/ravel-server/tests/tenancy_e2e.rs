#![allow(clippy::expect_used, clippy::unwrap_used)]
//! End-to-end startup pinning for the deployment-keyed tenant hash
//! (ADR-0050 section 3, EC3). These drive
//! [`ravel_server::tenancy::resolve_and_pin`] directly against a
//! `MemoryStore`, which is where the startup refusals live: the real `main`
//! calls the same function and installs its resolved scheme. The scheme is
//! never installed into the process-global here (a `OnceLock` shared across
//! these tests), so each case asserts on the returned `ResolvedTenancy` or the
//! typed error rather than on `TenantId::hash()`.

use std::sync::Arc;

use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_server::tenancy::{
    ConfiguredScheme, ResolvedTenancy, TENANCY_MARKER_KEY, TenancyError, resolve_and_pin,
};
use ravel_types::{TenantHashScheme, TenantId};

fn store() -> Arc<dyn ObjectStoreBackend> {
    Arc::new(MemoryStore::new())
}

fn keyed(bytes: u8) -> ConfiguredScheme {
    ConfiguredScheme::Keyed(Box::new([bytes; 32]))
}

/// The named acceptance test: an existing `V2_KEYED` bucket keyed with
/// fingerprint A, a process configured with a different key (fingerprint B),
/// must refuse startup with a typed error that names the mismatch. Never a
/// generic panic, never a silent parallel namespace.
#[tokio::test]
async fn wrong_key_fingerprint_refuses_startup() {
    let store = store();
    // Bootstrap the bucket keyed with key A.
    let resolved = resolve_and_pin(store.as_ref(), keyed(0xAA), 1_000)
        .await
        .expect("fresh keyed bucket bootstraps");
    assert!(matches!(resolved.scheme, TenantHashScheme::V2Keyed { .. }));

    // A second process with a different key must be refused.
    let err = resolve_and_pin(store.as_ref(), keyed(0xBB), 2_000)
        .await
        .expect_err("a wrong key must refuse startup, not open a parallel namespace");
    match err {
        TenancyError::KeyFingerprintMismatch { expected, got } => {
            assert_ne!(expected, got, "the two fingerprints must actually differ");
            let msg = TenancyError::KeyFingerprintMismatch { expected, got }.to_string();
            assert!(
                msg.contains("fingerprint"),
                "error names the mismatch: {msg}"
            );
            assert!(
                msg.contains("refusing to start"),
                "error is a refusal: {msg}"
            );
        }
        other => panic!("expected a KeyFingerprintMismatch, got: {other}"),
    }
}

/// The correct key against the same marker starts and returns the keyed scheme.
#[tokio::test]
async fn matching_key_fingerprint_starts() {
    let store = store();
    resolve_and_pin(store.as_ref(), keyed(0xAA), 1_000)
        .await
        .expect("bootstrap");
    let resolved = resolve_and_pin(store.as_ref(), keyed(0xAA), 2_000)
        .await
        .expect("the matching key starts");
    assert!(matches!(resolved.scheme, TenantHashScheme::V2Keyed { .. }));
}

/// A fresh bucket with no flags at all refuses: keyed is the default, and it
/// needs a key. Never silent unkeyed operation.
#[tokio::test]
async fn fresh_bucket_without_flags_refuses() {
    let store = store();
    let err = resolve_and_pin(store.as_ref(), ConfiguredScheme::Unspecified, 1_000)
        .await
        .expect_err("a fresh bucket with no key and no opt-out must refuse startup");
    assert!(
        matches!(err, TenancyError::FreshBucketNeedsKey),
        "expected FreshBucketNeedsKey, got: {err}"
    );
}

/// A fresh bucket with `--tenant-hash-unkeyed` starts and writes a
/// `V1_UNKEYED` marker.
#[tokio::test]
async fn fresh_bucket_unkeyed_starts_and_writes_marker() {
    let store = store();
    let resolved = resolve_and_pin(store.as_ref(), ConfiguredScheme::Unkeyed, 1_000)
        .await
        .expect("explicit unkeyed opt-out starts");
    assert!(matches!(resolved.scheme, TenantHashScheme::V1Unkeyed));
    // The marker was written and pins the scheme for the next process.
    assert!(
        store
            .get(TENANCY_MARKER_KEY, ravel_object_store::GetRange::Full)
            .await
            .is_ok(),
        "an unkeyed marker must have been written"
    );
    // A later keyed process must now be refused (the choice is permanent).
    let err = resolve_and_pin(store.as_ref(), keyed(0xAA), 2_000)
        .await
        .expect_err("an unkeyed-pinned bucket refuses a keyed process");
    assert!(
        matches!(err, TenancyError::SchemeMismatch { .. }),
        "expected SchemeMismatch, got: {err}"
    );
}

/// A pre-ADR bucket (holds `t/` data, no marker) is adopted as `V1_UNKEYED`
/// with no flags, writes the marker, and every existing prefix still resolves
/// to exactly the same unkeyed hash: no re-key, no breakage.
#[tokio::test]
async fn pre_adr_bucket_adopts_unkeyed_and_preserves_hashes() {
    let store = store();

    // Seed the bucket with a real pre-ADR object under a v1-unkeyed prefix.
    // The prefix is computed with the frozen v1 derivation directly, exactly
    // as a pre-ADR binary would have written it.
    let tenant = TenantId::new("acme");
    let v1_hash = TenantHashScheme::V1Unkeyed.hash(&tenant);
    let seeded_key = format!("t/{}/m/l0/seed-object", v1_hash.to_hex());
    store
        .put(
            &seeded_key,
            bytes::Bytes::from_static(b"pre-adr-data"),
            PutOptions::default(),
        )
        .await
        .expect("seed pre-ADR data");

    // Adopt with no flags: keyed-by-default does NOT apply to a bucket that
    // already holds data; it is pinned unkeyed, permanently.
    let resolved = resolve_and_pin(store.as_ref(), ConfiguredScheme::Unspecified, 5_000)
        .await
        .expect("a pre-ADR bucket adopts unkeyed with no flags");
    assert!(matches!(resolved.scheme, TenantHashScheme::V1Unkeyed));

    // The resolved scheme reproduces the EXACT prefix the seeded object lives
    // under: existing data is still addressable. This is the "no re-key, no
    // breakage" guarantee, checked against a real pre-ADR key, not just the
    // marker write.
    let resolved_hash = resolved.scheme.hash(&tenant);
    assert_eq!(
        resolved_hash, v1_hash,
        "adoption must not move any existing tenant's prefix"
    );
    assert_eq!(
        format!("t/{}/m/l0/seed-object", resolved_hash.to_hex()),
        seeded_key,
        "the seeded object is still addressable under the resolved scheme"
    );

    // The marker now records V1_UNKEYED, so a later keyed process refuses.
    let err = resolve_and_pin(store.as_ref(), keyed(0xAA), 6_000)
        .await
        .expect_err("an adopted-unkeyed bucket refuses a keyed process");
    assert!(
        matches!(err, TenancyError::SchemeMismatch { .. }),
        "got: {err}"
    );
}

/// A pre-ADR bucket handed a key refuses: an existing unkeyed bucket cannot be
/// re-keyed (there is no migration path, by design).
#[tokio::test]
async fn pre_adr_bucket_with_key_refuses_rekey() {
    let store = store();
    let tenant = TenantId::new("acme");
    let v1_hash = TenantHashScheme::V1Unkeyed.hash(&tenant);
    store
        .put(
            &format!("t/{}/m/l0/seed", v1_hash.to_hex()),
            bytes::Bytes::from_static(b"data"),
            PutOptions::default(),
        )
        .await
        .expect("seed");
    let err = resolve_and_pin(store.as_ref(), keyed(0xAA), 1_000)
        .await
        .expect_err("keying an existing unkeyed bucket must refuse");
    assert!(
        matches!(err, TenancyError::PreExistingDataIsUnkeyed),
        "expected PreExistingDataIsUnkeyed, got: {err}"
    );
}

/// A keyed marker with no key configured refuses: the bucket is unreadable
/// without the deployment key.
#[tokio::test]
async fn keyed_bucket_without_key_refuses() {
    let store = store();
    resolve_and_pin(store.as_ref(), keyed(0xAA), 1_000)
        .await
        .expect("bootstrap keyed");
    let err = resolve_and_pin(store.as_ref(), ConfiguredScheme::Unspecified, 2_000)
        .await
        .expect_err("a keyed bucket with no key must refuse");
    assert!(matches!(err, TenancyError::KeyRequired), "got: {err}");
}

/// The keyed bootstrap retains the deployment key so the recovery-manifest
/// writer can derive its AEAD key (ADR-0050 section 3).
#[tokio::test]
async fn keyed_bootstrap_retains_deployment_key() {
    let store = store();
    let resolved: ResolvedTenancy = resolve_and_pin(store.as_ref(), keyed(0xAA), 1_000)
        .await
        .expect("bootstrap keyed");
    assert!(
        resolved.deployment_key.is_some(),
        "a keyed bucket must retain its deployment key for the recovery manifest"
    );
}
