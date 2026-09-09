//! Deployment-keyed tenant hash: the `sys/tenancy` marker, startup pinning,
//! and the per-tenant recovery manifest (ADR-0050 section 3).
//!
//! The tenant-hash derivation is pinned per bucket at the bucket's birth and
//! recorded in an immutable `sys/tenancy` object at the bucket root (not under
//! any tenant prefix, since its purpose is to establish the tenant prefixes).
//! Every process resolves that marker once at startup via [`resolve_and_pin`]
//! and installs the resulting [`ravel_types::TenantHashScheme`] process-wide.
//! A configured scheme or key that disagrees with the marker is a startup
//! refusal, never a silent parallel namespace: deploying the wrong key must
//! be a loud failed deploy, not a bucket that looks empty while new writes
//! land beside the old data.
//!
//! There is no re-key migration. A bucket's scheme is permanent; a deployment
//! that needs to change schemes starts a new bucket and drains into it
//! operationally (docs/guides/operations.md).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{
    MarkerScheme, SchemeResolutionError, TenantHash, TenantHashScheme, TenantId,
    resolve_scheme_against_marker, tenant_key_fingerprint,
};

/// The tenant-hash scheme a process was configured for, derived from its
/// startup flags before any object store is read (ADR-0050 section 3):
/// `--tenant-hash-key-file` implies [`ConfiguredScheme::Keyed`],
/// `--tenant-hash-unkeyed` implies [`ConfiguredScheme::Unkeyed`], and neither
/// flag is [`ConfiguredScheme::Unspecified`]. Re-exported from `ravel-types`,
/// where the fail-closed decision it feeds ([`resolve_scheme_against_marker`])
/// lives, so the server and the read-only CLI share one copy of that rule.
pub use ravel_types::ConfiguredScheme;

/// The bucket-root marker key (ADR-0050 section 3). Fixed, never under a
/// tenant prefix.
pub const TENANCY_MARKER_KEY: &str = "sys/tenancy";

/// Prefix every tenant's objects live under. A non-empty listing here means
/// the bucket already holds data, which decides fresh-bucket vs pre-ADR-bucket
/// adoption.
const TENANT_PREFIX: &str = "t/";

/// Key-derivation context for the recovery-manifest AEAD key (ADR-0050
/// section 3). Domain-separated from the hashing key and the fingerprint so
/// the same deployment key yields three independent secrets.
const RECOVERY_AEAD_CONTEXT: &str = "ravel-tenant-v2-recovery-aead";

/// Format version written into every marker and manifest this module emits.
const FORMAT_VERSION: u32 = 1;

/// The pinned tenancy of a bucket, resolved once at startup. `scheme` is
/// installed process-wide via [`ravel_types::install_tenant_hash_scheme`];
/// `deployment_key` is retained for keyed buckets so the recovery-manifest
/// writer can derive its AEAD key.
pub struct ResolvedTenancy {
    pub scheme: TenantHashScheme,
    pub deployment_key: Option<Box<[u8; 32]>>,
}

impl std::fmt::Debug for ResolvedTenancy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the deployment key: it must never reach a log or a test
        // failure message. Only its presence is shown.
        f.debug_struct("ResolvedTenancy")
            .field("scheme", &self.scheme)
            .field(
                "deployment_key",
                &self.deployment_key.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl ResolvedTenancy {
    fn unkeyed() -> Self {
        ResolvedTenancy {
            scheme: TenantHashScheme::V1Unkeyed,
            deployment_key: None,
        }
    }

    fn keyed(key: [u8; 32]) -> Self {
        ResolvedTenancy {
            scheme: TenantHashScheme::v2_from_deployment_key(&key),
            deployment_key: Some(Box::new(key)),
        }
    }
}

/// A typed startup-pinning failure. Every variant refuses to start; none warn
/// and continue (ADR-0050's single rule). The wrong-key case names both
/// fingerprints so an operator sees exactly which key is expected.
#[derive(Debug, thiserror::Error)]
pub enum TenancyError {
    #[error("object store error resolving sys/tenancy: {0}")]
    Store(#[from] StoreError),
    #[error("sys/tenancy is corrupt and could not be decoded: {0}")]
    MarkerDecode(#[from] prost::DecodeError),
    #[error(
        "sys/tenancy records an unknown or unspecified scheme value; refusing to start rather \
         than guess the tenant-hash derivation"
    )]
    CorruptMarker,
    #[error(
        "sys/tenancy declares format_version {got}, but this build only understands version \
         {FORMAT_VERSION}: refusing to start rather than misread a future marker format as v1"
    )]
    UnsupportedMarkerVersion { got: u32 },
    #[error(
        "sys/tenancy pins this bucket to {marker_scheme}, but this process is configured for \
         {configured_scheme}: refusing to start rather than open a parallel namespace"
    )]
    SchemeMismatch {
        marker_scheme: &'static str,
        configured_scheme: &'static str,
    },
    #[error(
        "sys/tenancy pins this bucket to v2-keyed, but no --tenant-hash-key-file was configured: \
         refusing to start (the bucket's prefixes are unreadable without the deployment key)"
    )]
    KeyRequired,
    #[error(
        "sys/tenancy pins this bucket to v2-keyed with key fingerprint {expected}, but the \
         configured --tenant-hash-key-file has fingerprint {got}: refusing to start rather than \
         open a parallel namespace under the wrong key"
    )]
    KeyFingerprintMismatch { expected: String, got: String },
    #[error(
        "this is a fresh bucket and the tenant hash is keyed by default, but no \
         --tenant-hash-key-file was configured: pass --tenant-hash-key-file to key the bucket, \
         or --tenant-hash-unkeyed to opt out permanently"
    )]
    FreshBucketNeedsKey,
    #[error(
        "this bucket already holds tenant data written under the unkeyed derivation, but a \
         --tenant-hash-key-file was configured: an existing bucket cannot be re-keyed \
         (ADR-0050 section 3). Drop the key file to run unkeyed, or start a new keyed bucket"
    )]
    PreExistingDataIsUnkeyed,
    #[error(
        "sys/tenancy was absent then present within one startup, but could not be re-read: a \
         concurrent bootstrap left the marker unreadable"
    )]
    MarkerVanished,
    #[error("recovery manifest error: {0}")]
    Recovery(#[from] RecoveryError),
}

/// Count of buckets this process adopted as `V1_UNKEYED` because they held
/// `t/` data but no `sys/tenancy` marker (a pre-ADR-0050 bucket). Rendered at
/// `/metrics` as `ravel_tenancy_v1_unkeyed_adoptions_total` so an operator
/// sees the one-time migration happened.
static V1_UNKEYED_ADOPTIONS: AtomicU64 = AtomicU64::new(0);

/// The adoption count for the `/metrics` renderer.
pub fn v1_unkeyed_adoption_count() -> u64 {
    V1_UNKEYED_ADOPTIONS.load(Ordering::Relaxed)
}

fn unkeyed_marker(now_ns: i64) -> sysproto::TenancyMarker {
    sysproto::TenancyMarker {
        format_version: FORMAT_VERSION,
        scheme: sysproto::TenantHashScheme::V1Unkeyed as i32,
        key_fingerprint: Vec::new(),
        created_unix_ns: now_ns,
    }
}

fn keyed_marker(deployment_key: &[u8; 32], now_ns: i64) -> sysproto::TenancyMarker {
    sysproto::TenancyMarker {
        format_version: FORMAT_VERSION,
        scheme: sysproto::TenantHashScheme::V2Keyed as i32,
        key_fingerprint: tenant_key_fingerprint(deployment_key).to_vec(),
        created_unix_ns: now_ns,
    }
}

async fn read_marker(
    store: &dyn ObjectStoreBackend,
) -> Result<Option<sysproto::TenancyMarker>, TenancyError> {
    match store.get(TENANCY_MARKER_KEY, GetRange::Full).await {
        Ok(outcome) => {
            let marker = sysproto::TenancyMarker::decode(outcome.data.as_ref())?;
            Ok(Some(marker))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(TenancyError::Store(err)),
    }
}

/// Write the marker once. Returns the raw store error so the caller can
/// distinguish the `AlreadyExists` race from a real failure.
async fn write_marker(
    store: &dyn ObjectStoreBackend,
    marker: &sysproto::TenancyMarker,
) -> Result<(), StoreError> {
    let bytes = marker.encode_to_vec();
    store
        .put(
            TENANCY_MARKER_KEY,
            bytes.into(),
            PutOptions::create_if_absent(),
        )
        .await
        .map(|_| ())
}

/// Cheap existence check: does the bucket hold any `t/` data at all? Only the
/// first listing page is inspected, since existence, not enumeration, is all
/// that decides fresh-bucket vs pre-ADR adoption.
async fn bucket_has_tenant_data(store: &dyn ObjectStoreBackend) -> Result<bool, TenancyError> {
    let page = store.list(TENANT_PREFIX, None).await?;
    // A non-empty first page means data exists. `next.is_some()` is cheap
    // insurance for a backend that could return an empty first page with a
    // continuation token: treat "there is another page" as data present too,
    // so a pre-ADR bucket is never misread as fresh (which would let a keyed
    // bootstrap split its namespace).
    Ok(!page.objects.is_empty() || page.next.is_some())
}

/// Validate a configured scheme against a present marker (ADR-0050 section 3,
/// priority 1). The marker is authoritative; the config must be compatible or
/// the process refuses to start.
fn validate_against_marker(
    marker: &sysproto::TenancyMarker,
    configured: &ConfiguredScheme,
) -> Result<ResolvedTenancy, TenancyError> {
    // A future marker format could carry different field semantics; refuse it
    // rather than misread it under the v1 layout (ADR-0050 section 3).
    if marker.format_version != FORMAT_VERSION {
        return Err(TenancyError::UnsupportedMarkerVersion {
            got: marker.format_version,
        });
    }
    // Decode the persisted enum into the proto-free `MarkerScheme` and delegate
    // the fail-closed decision to the single shared implementation in
    // `ravel-types`, so the server and the CLI cannot diverge on it. The rich
    // `TenancyError` messages this module's tests pin are preserved by mapping
    // the shared refusal back onto them.
    let marker_scheme = match sysproto::TenantHashScheme::try_from(marker.scheme)
        .map_err(|_| TenancyError::CorruptMarker)?
    {
        sysproto::TenantHashScheme::V1Unkeyed => MarkerScheme::V1Unkeyed,
        sysproto::TenantHashScheme::V2Keyed => MarkerScheme::V2Keyed,
        sysproto::TenantHashScheme::SchemeUnspecified => return Err(TenancyError::CorruptMarker),
    };
    match resolve_scheme_against_marker(marker_scheme, &marker.key_fingerprint, configured) {
        // A resolved success against a keyed marker is only reachable with a
        // `Keyed` config (the shared resolver refuses every other pairing), so
        // retaining the deployment key here for the recovery-manifest writer is
        // sound; any other success is the unkeyed derivation.
        Ok(_scheme) => Ok(match configured {
            ConfiguredScheme::Keyed(key) => ResolvedTenancy::keyed(**key),
            ConfiguredScheme::Unkeyed | ConfiguredScheme::Unspecified => ResolvedTenancy::unkeyed(),
        }),
        Err(SchemeResolutionError::SchemeMismatch {
            marker_scheme,
            configured_scheme,
        }) => Err(TenancyError::SchemeMismatch {
            marker_scheme,
            configured_scheme,
        }),
        Err(SchemeResolutionError::KeyRequired) => Err(TenancyError::KeyRequired),
        Err(SchemeResolutionError::KeyFingerprintMismatch { expected, got }) => {
            Err(TenancyError::KeyFingerprintMismatch { expected, got })
        }
    }
}

/// Resolve the bucket's tenant-hash scheme and pin it, in ADR-0050 section 3's
/// exact priority order:
///
/// 1. Marker present: the configured scheme and key fingerprint must match it,
///    or refuse to start (the single most important correctness property here).
/// 2. Marker absent, no `t/` data (a fresh bucket): write the marker from the
///    configured scheme. Keyed is the default; an unspecified config refuses.
/// 3. Marker absent, `t/` data present (a pre-ADR bucket): only the unkeyed
///    derivation ever existed, so write a `V1_UNKEYED` marker, count it, log
///    it, and continue. This is the entire migration story: no re-key.
///
/// The marker write is `CreateIfAbsent`; a concurrent bootstrap that wins the
/// race is handled by re-reading and validating against the now-present marker.
pub async fn resolve_and_pin(
    store: &dyn ObjectStoreBackend,
    configured: ConfiguredScheme,
    now_ns: i64,
) -> Result<ResolvedTenancy, TenancyError> {
    if let Some(marker) = read_marker(store).await? {
        return validate_against_marker(&marker, &configured);
    }

    let has_data = bucket_has_tenant_data(store).await?;

    enum Adoption {
        Fresh,
        PreExisting,
    }

    let (marker, resolved, adoption) = if has_data {
        // Pre-ADR bucket: unkeyed by construction. A key here would split the
        // namespace, so refuse; otherwise pin unkeyed permanently.
        match &configured {
            ConfiguredScheme::Keyed(_) => return Err(TenancyError::PreExistingDataIsUnkeyed),
            ConfiguredScheme::Unkeyed | ConfiguredScheme::Unspecified => (
                unkeyed_marker(now_ns),
                ResolvedTenancy::unkeyed(),
                Adoption::PreExisting,
            ),
        }
    } else {
        // Fresh bucket: write from the configured scheme; keyed by default.
        match &configured {
            ConfiguredScheme::Keyed(key) => (
                keyed_marker(key, now_ns),
                ResolvedTenancy::keyed(**key),
                Adoption::Fresh,
            ),
            ConfiguredScheme::Unkeyed => (
                unkeyed_marker(now_ns),
                ResolvedTenancy::unkeyed(),
                Adoption::Fresh,
            ),
            ConfiguredScheme::Unspecified => return Err(TenancyError::FreshBucketNeedsKey),
        }
    };

    match write_marker(store, &marker).await {
        Ok(()) => {
            if matches!(adoption, Adoption::PreExisting) {
                V1_UNKEYED_ADOPTIONS.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "bucket holds t/ data but had no sys/tenancy marker; pinned V1_UNKEYED \
                     (pre-ADR-0050 adoption, permanent). Existing prefixes are unchanged."
                );
            }
            Ok(resolved)
        }
        // A concurrent process wrote the marker first. Re-read and validate our
        // configuration against what actually landed, so a race can never let
        // an incompatible config through.
        Err(StoreError::AlreadyExists) => {
            let marker = read_marker(store)
                .await?
                .ok_or(TenancyError::MarkerVanished)?;
            validate_against_marker(&marker, &configured)
        }
        Err(err) => Err(TenancyError::Store(err)),
    }
}

// ---------------------------------------------------------------------------
// Recovery manifest (ADR-0050 section 3): sys/t/<tenant_hash>
// ---------------------------------------------------------------------------

/// The recovery-manifest key for one tenant. Under `sys/`, not the tenant's
/// own prefix, so the whole recovery set is discoverable with the deployment
/// key alone.
pub fn recovery_manifest_key(tenant_hash: &TenantHash) -> String {
    format!("sys/t/{}", tenant_hash.to_hex())
}

/// AES-256-GCM nonce length (96 bits).
const NONCE_LEN: usize = 12;

/// A typed recovery-manifest failure. Decrypting with the wrong key, or a
/// tampered manifest, is [`RecoveryError::Decrypt`], never a panic.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("recovery manifest could not be decoded: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error(
        "recovery manifest declares format_version {got}, but this build only understands \
         version {FORMAT_VERSION}"
    )]
    UnsupportedVersion { got: u32 },
    #[error("recovery manifest nonce is not {NONCE_LEN} bytes")]
    BadNonce,
    #[error("recovery manifest AEAD key setup failed")]
    KeySetup,
    #[error("recovery manifest could not be decrypted (wrong deployment key or tampered object)")]
    Decrypt,
    #[error("secure random generation failed")]
    Rng,
}

fn recovery_aead_key(deployment_key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key(RECOVERY_AEAD_CONTEXT, deployment_key)
}

/// Encrypt `tenant`'s id under the deployment key and encode the recovery
/// manifest object bytes. The `tenant_hash` is bound as AEAD associated data
/// so a manifest cannot be relocated to another tenant's key and still open.
pub fn seal_recovery_manifest(
    deployment_key: &[u8; 32],
    tenant: &TenantId,
    tenant_hash: &TenantHash,
    now_ns: i64,
) -> Result<Vec<u8>, RecoveryError> {
    use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
    use ring::rand::{SecureRandom, SystemRandom};

    let aead_key = recovery_aead_key(deployment_key);
    let key = LessSafeKey::new(
        UnboundKey::new(&AES_256_GCM, &aead_key).map_err(|_| RecoveryError::KeySetup)?,
    );

    let mut nonce_bytes = [0u8; NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| RecoveryError::Rng)?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);

    let mut in_out = tenant.as_str().as_bytes().to_vec();
    key.seal_in_place_append_tag(nonce, Aad::from(tenant_hash.0), &mut in_out)
        .map_err(|_| RecoveryError::Decrypt)?;

    let manifest = sysproto::TenantRecoveryManifest {
        format_version: FORMAT_VERSION,
        nonce: nonce_bytes.to_vec(),
        ciphertext: in_out,
        created_unix_ns: now_ns,
    };
    Ok(manifest.encode_to_vec())
}

/// Decode and decrypt a recovery manifest, recovering the original
/// [`TenantId`]. A wrong key, a tampered object, or a mismatched
/// `tenant_hash` all fail as [`RecoveryError::Decrypt`].
pub fn open_recovery_manifest(
    deployment_key: &[u8; 32],
    tenant_hash: &TenantHash,
    manifest_bytes: &[u8],
) -> Result<TenantId, RecoveryError> {
    use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};

    let manifest = sysproto::TenantRecoveryManifest::decode(manifest_bytes)?;
    if manifest.format_version != FORMAT_VERSION {
        return Err(RecoveryError::UnsupportedVersion {
            got: manifest.format_version,
        });
    }
    let nonce_bytes: [u8; NONCE_LEN] = manifest
        .nonce
        .as_slice()
        .try_into()
        .map_err(|_| RecoveryError::BadNonce)?;

    let aead_key = recovery_aead_key(deployment_key);
    let key = LessSafeKey::new(
        UnboundKey::new(&AES_256_GCM, &aead_key).map_err(|_| RecoveryError::KeySetup)?,
    );
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);

    let mut in_out = manifest.ciphertext;
    let plaintext = key
        .open_in_place(nonce, Aad::from(tenant_hash.0), &mut in_out)
        .map_err(|_| RecoveryError::Decrypt)?;
    let id = std::str::from_utf8(plaintext).map_err(|_| RecoveryError::Decrypt)?;
    Ok(TenantId::new(id))
}

/// Writes each keyed tenant's recovery manifest once, at the tenant's first
/// write in this process's lifetime (ADR-0050 section 3). A per-process
/// in-memory set avoids a LIST-before-every-write tax; the `CreateIfAbsent`
/// write handles the cross-process race, and an `AlreadyExists` is success.
///
/// Constructed only for keyed buckets. Unkeyed buckets never write a manifest:
/// an unkeyed prefix is trivially reversible, so there is nothing to recover.
pub struct RecoveryManifestWriter {
    store: Arc<dyn ObjectStoreBackend>,
    deployment_key: Box<[u8; 32]>,
    seen: Mutex<HashSet<TenantHash>>,
}

impl RecoveryManifestWriter {
    pub fn new(store: Arc<dyn ObjectStoreBackend>, deployment_key: Box<[u8; 32]>) -> Self {
        RecoveryManifestWriter {
            store,
            deployment_key,
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// Ensure `tenant`'s recovery manifest exists, cheaply. Idempotent within
    /// a process (the in-memory set) and across processes (`CreateIfAbsent`,
    /// where `AlreadyExists` means another writer got there first and is
    /// success). Any other store error is returned for the caller to log; it
    /// is not fatal to ingest, since the `sys/tenancy` fingerprint, not the
    /// manifest, is the primary wrong-key guard.
    pub async fn ensure(&self, tenant: &TenantId, now_ns: i64) -> Result<(), TenancyError> {
        // Derive the prefix from this writer's own deployment key rather than
        // the process-global `TenantId::hash()`. On a keyed bucket the two are
        // identical (the global scheme is v2 from this same key), but deriving
        // locally keeps the manifest key and its AEAD key rooted in one place
        // and removes any dependency on the global being installed first.
        let tenant_hash =
            TenantHashScheme::v2_from_deployment_key(&self.deployment_key).hash(tenant);
        if self.seen.lock().contains(&tenant_hash) {
            return Ok(());
        }

        let bytes = seal_recovery_manifest(&self.deployment_key, tenant, &tenant_hash, now_ns)?;
        let key = recovery_manifest_key(&tenant_hash);
        match self
            .store
            .put(&key, bytes.into(), PutOptions::create_if_absent())
            .await
        {
            Ok(_) | Err(StoreError::AlreadyExists) => {
                self.seen.lock().insert(tenant_hash);
                Ok(())
            }
            Err(err) => Err(TenancyError::Store(err)),
        }
    }
}

/// Best-effort recovery-manifest write on an ingest request's tenant, called
/// from every server ingest handler before the write (ADR-0050 section 3). A
/// keyed bucket's first write for a tenant this process has not seen writes
/// the tenant's `sys/t/<tenant_hash>` manifest so bucket-plus-key always
/// recovers the id-to-prefix mapping; subsequent writes are a cheap in-memory
/// set hit.
///
/// A no-op when `writer` is `None` (an unkeyed bucket needs no manifest). A
/// store failure is logged and ingest proceeds: the `sys/tenancy` fingerprint,
/// not the manifest, is the primary wrong-key guard, so a manifest write is
/// never on the durability-critical path.
pub async fn ensure_recovery_manifest(
    writer: &Option<Arc<RecoveryManifestWriter>>,
    tenant: &TenantId,
    now_ns: i64,
) {
    if let Some(writer) = writer
        && let Err(err) = writer.ensure(tenant, now_ns).await
    {
        tracing::warn!(
            %err,
            tenant = tenant.as_str(),
            "failed to write tenant recovery manifest; ingest continues"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    #[test]
    fn recovery_manifest_roundtrips_with_the_same_key() {
        let key = [3u8; 32];
        let tenant = TenantId::new("acme");
        let hash = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);
        let bytes = seal_recovery_manifest(&key, &tenant, &hash, 1_000).expect("seal must succeed");
        let recovered = open_recovery_manifest(&key, &hash, &bytes).expect("open must succeed");
        assert_eq!(recovered, tenant, "recovered id must equal the original");
    }

    #[test]
    fn recovery_manifest_wrong_key_is_a_typed_error() {
        let key = [3u8; 32];
        let wrong = [4u8; 32];
        let tenant = TenantId::new("acme");
        let hash = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);
        let bytes = seal_recovery_manifest(&key, &tenant, &hash, 1_000).expect("seal must succeed");
        let err = open_recovery_manifest(&wrong, &hash, &bytes)
            .expect_err("a wrong key must be a typed decrypt error, not a panic");
        assert!(matches!(err, RecoveryError::Decrypt), "got: {err}");
    }

    #[test]
    fn recovery_manifest_tampered_ciphertext_is_a_typed_error() {
        let key = [3u8; 32];
        let tenant = TenantId::new("acme");
        let hash = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);
        let bytes = seal_recovery_manifest(&key, &tenant, &hash, 1_000).expect("seal must succeed");
        // Decode, flip a byte inside the AEAD ciphertext (not the proto
        // framing), and re-encode: the AEAD tag must reject the tampered
        // ciphertext as a typed decrypt error, never a panic or a wrong id.
        let mut manifest =
            sysproto::TenantRecoveryManifest::decode(bytes.as_slice()).expect("decode");
        manifest.ciphertext[0] ^= 0xFF;
        let tampered = manifest.encode_to_vec();
        let err = open_recovery_manifest(&key, &hash, &tampered)
            .expect_err("a tampered ciphertext must be a typed error, not a panic");
        assert!(matches!(err, RecoveryError::Decrypt), "got: {err}");
    }

    #[test]
    fn recovery_manifest_wrong_tenant_hash_aad_is_a_typed_error() {
        let key = [3u8; 32];
        let tenant = TenantId::new("acme");
        let hash = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);
        let other = TenantHashScheme::v2_from_deployment_key(&key).hash(&TenantId::new("globex"));
        let bytes = seal_recovery_manifest(&key, &tenant, &hash, 1_000).expect("seal must succeed");
        let err = open_recovery_manifest(&key, &other, &bytes)
            .expect_err("a manifest opened under the wrong tenant_hash AAD must fail");
        assert!(matches!(err, RecoveryError::Decrypt), "got: {err}");
    }

    #[tokio::test]
    async fn writer_is_idempotent_and_create_if_absent() {
        let store = store();
        let key = Box::new([7u8; 32]);
        let writer = RecoveryManifestWriter::new(store.clone(), key.clone());
        let tenant = TenantId::new("acme");
        // `ensure` keys the manifest by the v2 derivation of the writer's own
        // deployment key, independent of any process-global scheme install.
        let hash = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);

        writer.ensure(&tenant, 1_000).await.expect("first write");
        writer
            .ensure(&tenant, 2_000)
            .await
            .expect("second is a no-op");

        let obj = store
            .get(&recovery_manifest_key(&hash), GetRange::Full)
            .await
            .expect("manifest object exists");
        let recovered = open_recovery_manifest(&key, &hash, &obj.data).expect("manifest decrypts");
        assert_eq!(recovered, tenant);
    }
}
