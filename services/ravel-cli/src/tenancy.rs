//! `ravel-cli tenancy show` (ADR-0050 section 3): decode and render the
//! bucket-root `sys/tenancy` marker, so an operator can see which tenant-hash
//! scheme a bucket is pinned to and, for a keyed bucket, its key fingerprint.
//!
//! Optionally takes a `--tenant-hash-key-file`; when given, its fingerprint is
//! derived through the same [`ravel_types::tenant_key_fingerprint`] the server
//! uses and compared against the marker, so an operator can confirm a key
//! matches a bucket before deploying it (the same check the server makes at
//! startup, run offline). The key file is read the same way the server reads
//! it: 64 hex characters or exactly 32 raw bytes.

use std::path::Path;
use std::sync::Arc;

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{
    ConfiguredScheme, MarkerScheme, TenantHashScheme, resolve_scheme_against_marker,
};

/// Format version this build understands for the `sys/tenancy` marker, matching
/// the server's own (ADR-0050 section 3). A future version is refused rather
/// than misread under the v1 layout.
const MARKER_FORMAT_VERSION: u32 = 1;

/// Build the [`ConfiguredScheme`] the CLI's global `--tenant-hash-key-file` /
/// `--tenant-hash-unkeyed` flags select, reading and validating the key file
/// the same way the server does. The two flags are mutually exclusive.
pub fn configured_scheme_from_flags(
    key_file: Option<&Path>,
    unkeyed: bool,
) -> anyhow::Result<ConfiguredScheme> {
    if key_file.is_some() && unkeyed {
        anyhow::bail!(
            "--tenant-hash-key-file and --tenant-hash-unkeyed are mutually exclusive: the first \
             keys the tenant hash, the second opts out of keying. Pass exactly one."
        );
    }
    if let Some(path) = key_file {
        let raw = std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("could not read --tenant-hash-key-file {path:?}: {e}"))?;
        let key = parse_deployment_key(&raw).map_err(|e| {
            anyhow::anyhow!("invalid --tenant-hash-key-file {}: {e}", path.display())
        })?;
        return Ok(ConfiguredScheme::Keyed(Box::new(key)));
    }
    if unkeyed {
        return Ok(ConfiguredScheme::Unkeyed);
    }
    Ok(ConfiguredScheme::Unspecified)
}

/// Resolve the tenant-hash scheme the CLI must hash tenants under, reading the
/// bucket's real `sys/tenancy` marker and validating the configured scheme
/// against it through the same fail-closed decision the server uses at startup
/// ([`resolve_scheme_against_marker`]). This is EC3/#566: without it, every
/// tenant-hashing CLI command silently used the v1-unkeyed default even on a
/// v2-keyed bucket, writing (for example) a legal hold under the wrong `t/`
/// prefix while the server's sweeper checked the right one.
///
/// The CLI never bootstraps a bucket, so it only decides the marker-present
/// case plus a read-only default for an absent marker: a keyed config against a
/// bucket with no marker is refused (the bucket is not pinned to that key),
/// while an unkeyed or unspecified config falls back to the v1 derivation (the
/// only scheme a pre-ADR, not-yet-adopted bucket ever used).
pub async fn resolve_scheme(
    store: &dyn ObjectStoreBackend,
    configured: ConfiguredScheme,
) -> anyhow::Result<TenantHashScheme> {
    let outcome = match store.get("sys/tenancy", GetRange::Full).await {
        Ok(outcome) => outcome,
        Err(StoreError::NotFound) => {
            return match configured {
                ConfiguredScheme::Keyed(_) => anyhow::bail!(
                    "--tenant-hash-key-file was given, but this bucket has no sys/tenancy marker \
                     (it is a pre-ADR-0050 or not-yet-bootstrapped bucket, which is unkeyed): \
                     refusing to hash tenants under a key this bucket is not pinned to. Drop the \
                     key file to inspect it unkeyed."
                ),
                ConfiguredScheme::Unkeyed | ConfiguredScheme::Unspecified => {
                    Ok(TenantHashScheme::V1Unkeyed)
                }
            };
        }
        Err(err) => anyhow::bail!("failed to fetch sys/tenancy: {err}"),
    };

    let marker = sysproto::TenancyMarker::decode(outcome.data.as_ref())
        .map_err(|err| anyhow::anyhow!("sys/tenancy failed to decode: {err}"))?;
    if marker.format_version != MARKER_FORMAT_VERSION {
        anyhow::bail!(
            "sys/tenancy declares format_version {}, but this build only understands version {}: \
             refusing to hash tenants rather than misread a future marker format",
            marker.format_version,
            MARKER_FORMAT_VERSION
        );
    }
    let marker_scheme = match sysproto::TenantHashScheme::try_from(marker.scheme).map_err(|_| {
        anyhow::anyhow!(
            "sys/tenancy records an unknown scheme value {}",
            marker.scheme
        )
    })? {
        sysproto::TenantHashScheme::V1Unkeyed => MarkerScheme::V1Unkeyed,
        sysproto::TenantHashScheme::V2Keyed => MarkerScheme::V2Keyed,
        sysproto::TenantHashScheme::SchemeUnspecified => {
            anyhow::bail!("sys/tenancy records SCHEME_UNSPECIFIED, which is never a valid marker")
        }
    };
    resolve_scheme_against_marker(marker_scheme, &marker.key_fingerprint, &configured)
        .map_err(|err| anyhow::anyhow!("{err}"))
}

/// `tenancy show`: GET `sys/tenancy`, decode it, and render the scheme and (for
/// a keyed bucket) the key fingerprint. A missing marker, a store error, or a
/// decode failure are all typed `Err` and never panic.
pub async fn show(
    store: Arc<dyn ObjectStoreBackend>,
    key_file: Option<&Path>,
) -> anyhow::Result<String> {
    let outcome = match store.get("sys/tenancy", GetRange::Full).await {
        Ok(outcome) => outcome,
        Err(StoreError::NotFound) => {
            anyhow::bail!(
                "no sys/tenancy marker in this bucket: it is either a fresh bucket not yet \
                 bootstrapped by a server, or a pre-ADR-0050 bucket a server has not yet adopted"
            )
        }
        Err(err) => anyhow::bail!("failed to fetch sys/tenancy: {err}"),
    };
    let marker = sysproto::TenancyMarker::decode(outcome.data.as_ref())
        .map_err(|err| anyhow::anyhow!("sys/tenancy failed to decode: {err}"))?;

    let scheme = sysproto::TenantHashScheme::try_from(marker.scheme).map_err(|_| {
        anyhow::anyhow!(
            "sys/tenancy records an unknown scheme value {}",
            marker.scheme
        )
    })?;

    let mut out = String::new();
    out.push_str(&format!("format_version: {}\n", marker.format_version));
    match scheme {
        sysproto::TenantHashScheme::V1Unkeyed => {
            out.push_str("scheme: v1-unkeyed\n");
        }
        sysproto::TenantHashScheme::V2Keyed => {
            out.push_str("scheme: v2-keyed\n");
            out.push_str(&format!(
                "key_fingerprint: {}\n",
                hex::encode(&marker.key_fingerprint)
            ));
        }
        sysproto::TenantHashScheme::SchemeUnspecified => {
            anyhow::bail!("sys/tenancy records SCHEME_UNSPECIFIED, which is never a valid marker");
        }
    }

    if let Some(path) = key_file {
        let raw = std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("could not read --tenant-hash-key-file {path:?}: {e}"))?;
        let key = parse_deployment_key(&raw).map_err(|e| {
            anyhow::anyhow!("invalid --tenant-hash-key-file {}: {e}", path.display())
        })?;
        let fp = ravel_types::tenant_key_fingerprint(&key);
        let fp_hex = hex::encode(fp);
        out.push_str(&format!("configured_key_fingerprint: {fp_hex}\n"));
        let matches = scheme == sysproto::TenantHashScheme::V2Keyed
            && marker.key_fingerprint.as_slice() == fp;
        out.push_str(&format!(
            "key_matches_marker: {}\n",
            if matches { "yes" } else { "no" }
        ));
    }

    Ok(out)
}

/// Parse a 32-byte deployment key from a key file's raw bytes: 64 hex
/// characters (whitespace-trimmed) or exactly 32 raw bytes. Mirrors the
/// server's own loader so `tenancy show` verifies the key the server would.
/// `pub(crate)` so `tenant_token` can parse the same deployment key file
/// format for `sys/auth` (the same 32-byte key doubles as the v2-keyed
/// tenant-hash key and the `sys/auth` token-hashing key).
pub(crate) fn parse_deployment_key(raw: &[u8]) -> anyhow::Result<[u8; 32]> {
    if let Ok(text) = std::str::from_utf8(raw) {
        let trimmed = text.trim();
        if trimmed.len() == 64 && trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
            let bytes =
                hex::decode(trimmed).map_err(|e| anyhow::anyhow!("key is not valid hex: {e}"))?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("hex key did not decode to 32 bytes"))?;
            return Ok(arr);
        }
    }
    if raw.len() == 32 {
        let mut key = [0u8; 32];
        key.copy_from_slice(raw);
        return Ok(key);
    }
    anyhow::bail!(
        "must contain a 32-byte deployment key: either 64 hex characters or exactly 32 raw \
         bytes (got {} bytes)",
        raw.len()
    );
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{PutMode, PutOptions};

    async fn put_marker(store: &dyn ObjectStoreBackend, marker: &sysproto::TenancyMarker) {
        store
            .put(
                "sys/tenancy",
                marker.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put marker");
    }

    #[tokio::test]
    async fn shows_unkeyed_scheme() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        put_marker(
            store.as_ref(),
            &sysproto::TenancyMarker {
                format_version: 1,
                scheme: sysproto::TenantHashScheme::V1Unkeyed as i32,
                key_fingerprint: Vec::new(),
                created_unix_ns: 1_000,
            },
        )
        .await;
        let report = show(store, None).await.expect("show succeeds");
        assert!(report.contains("scheme: v1-unkeyed"), "report: {report}");
    }

    #[tokio::test]
    async fn shows_keyed_scheme_and_fingerprint() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let key = [5u8; 32];
        let fp = ravel_types::tenant_key_fingerprint(&key);
        put_marker(
            store.as_ref(),
            &sysproto::TenancyMarker {
                format_version: 1,
                scheme: sysproto::TenantHashScheme::V2Keyed as i32,
                key_fingerprint: fp.to_vec(),
                created_unix_ns: 1_000,
            },
        )
        .await;
        let report = show(store, None).await.expect("show succeeds");
        assert!(report.contains("scheme: v2-keyed"), "report: {report}");
        assert!(
            report.contains(&format!("key_fingerprint: {}", hex::encode(fp))),
            "report: {report}"
        );
    }

    #[tokio::test]
    async fn missing_marker_is_a_typed_error() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let err = show(store, None)
            .await
            .expect_err("a missing marker must error, not print a false report");
        assert!(
            err.to_string().contains("no sys/tenancy marker"),
            "err: {err}"
        );
    }

    /// EC3/#566 finding 1: on a v2-keyed bucket, a CLI command run WITHOUT the
    /// key file must refuse rather than silently fall back to the v1-unkeyed
    /// default (which is what wrote holds/folds under the wrong `t/` prefix).
    #[tokio::test]
    async fn keyed_bucket_without_key_refuses() {
        use ravel_types::tenant_key_fingerprint;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let key = [0x11u8; 32];
        put_marker(
            store.as_ref(),
            &sysproto::TenancyMarker {
                format_version: 1,
                scheme: sysproto::TenantHashScheme::V2Keyed as i32,
                key_fingerprint: tenant_key_fingerprint(&key).to_vec(),
                created_unix_ns: 1_000,
            },
        )
        .await;

        let configured = configured_scheme_from_flags(None, false).expect("no flags is valid");
        let err = resolve_scheme(store.as_ref(), configured)
            .await
            .expect_err("a keyed bucket with no key must refuse, not default to v1-unkeyed");
        let msg = err.to_string();
        assert!(
            msg.contains("v2-keyed"),
            "err names the pinned scheme: {msg}"
        );
    }

    /// The same keyed bucket, run WITH the correct key, resolves the v2
    /// derivation and reproduces exactly the keyed prefix, not the v1 one.
    #[tokio::test]
    async fn keyed_bucket_with_correct_key_resolves_v2_prefix() {
        use ravel_types::{TenantId, tenant_key_fingerprint};

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let key = [0x22u8; 32];
        put_marker(
            store.as_ref(),
            &sysproto::TenancyMarker {
                format_version: 1,
                scheme: sysproto::TenantHashScheme::V2Keyed as i32,
                key_fingerprint: tenant_key_fingerprint(&key).to_vec(),
                created_unix_ns: 1_000,
            },
        )
        .await;

        let configured = ConfiguredScheme::Keyed(Box::new(key));
        let scheme = resolve_scheme(store.as_ref(), configured)
            .await
            .expect("the correct key resolves");

        let tenant = TenantId::new("acme");
        let v1 = TenantHashScheme::V1Unkeyed.hash(&tenant);
        let v2 = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);
        assert_eq!(scheme.hash(&tenant), v2, "resolves the keyed prefix");
        assert_ne!(scheme.hash(&tenant), v1, "not the silent v1 default");
    }

    /// The correct fail-closed reason distinguishes a wrong key from a missing
    /// one: a key whose fingerprint disagrees with the marker is refused.
    #[tokio::test]
    async fn keyed_bucket_with_wrong_key_refuses() {
        use ravel_types::tenant_key_fingerprint;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        put_marker(
            store.as_ref(),
            &sysproto::TenancyMarker {
                format_version: 1,
                scheme: sysproto::TenantHashScheme::V2Keyed as i32,
                key_fingerprint: tenant_key_fingerprint(&[0x33u8; 32]).to_vec(),
                created_unix_ns: 1_000,
            },
        )
        .await;

        let configured = ConfiguredScheme::Keyed(Box::new([0x44u8; 32]));
        let err = resolve_scheme(store.as_ref(), configured)
            .await
            .expect_err("a wrong key must refuse");
        assert!(
            err.to_string().contains("fingerprint"),
            "err names the fingerprint mismatch: {err}"
        );
    }

    /// The two flags cannot be combined.
    #[test]
    fn key_file_and_unkeyed_are_mutually_exclusive() {
        let err = configured_scheme_from_flags(Some(Path::new("/some/key")), true)
            .expect_err("both flags must be rejected");
        assert!(err.to_string().contains("mutually exclusive"), "err: {err}");
    }
}
