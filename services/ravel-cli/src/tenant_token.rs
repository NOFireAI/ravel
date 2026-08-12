//! `ravel-cli tenant token upsert|revoke|list` (ADR-0072 decision 4, #875): the
//! first shipped writer of the durable `sys/auth` bearer-token map
//! (`ravel_catalog::auth_token_map`).
//!
//! `upsert` and `revoke` need the bucket's 32-byte deployment key (the same
//! key that doubles as the v2-keyed tenant-hash key) to hash or address
//! entries; `revoke` removes by tenant name alone (`remove_tokens_by_tenant`),
//! so it needs no plaintext token. `list` never prints a raw token hash as if
//! it were a secret and never accepts or prints plaintext: it prints each
//! entry's tenant id and a short, clearly-labeled fingerprint (a prefix of the
//! token hash) only so a human can tell two entries for the same tenant
//! apart, alongside the map's own deployment-key fingerprint.

use std::path::Path;
use std::sync::Arc;

use ravel_catalog::{AuthSetOutcome, read_auth_map, remove_tokens_by_tenant, upsert_token};
use ravel_object_store::ObjectStoreBackend;

use crate::tenancy::parse_deployment_key;

/// Width of the per-entry token fingerprint `list` prints: a prefix of the
/// stored token hash, long enough to distinguish entries for a human, short
/// enough that it is never mistaken for (or usable as) the hash itself.
const TOKEN_FINGERPRINT_LEN: usize = 8;

/// Read and parse `--deployment-key-file`'s 32-byte deployment key.
fn load_deployment_key(path: &Path) -> anyhow::Result<[u8; 32]> {
    let raw = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("could not read --deployment-key-file {path:?}: {e}"))?;
    parse_deployment_key(&raw)
        .map_err(|e| anyhow::anyhow!("invalid --deployment-key-file {}: {e}", path.display()))
}

/// `tenant token upsert`: map `token` to `tenant_id` in `sys/auth`, hashing it
/// under the deployment key. The plaintext is hashed and dropped, never
/// persisted or printed.
pub async fn upsert(
    store: Arc<dyn ObjectStoreBackend>,
    deployment_key_file: &Path,
    token: &[u8],
    tenant_id: &str,
    now_ns: i64,
) -> anyhow::Result<()> {
    let key = load_deployment_key(deployment_key_file)?;
    match upsert_token(store.as_ref(), &key, token, tenant_id, now_ns).await? {
        AuthSetOutcome::Created => println!("sys/auth created; token mapped to {tenant_id}"),
        AuthSetOutcome::Updated => println!("sys/auth updated; token mapped to {tenant_id}"),
        AuthSetOutcome::Unchanged => println!("no change"),
    }
    Ok(())
}

/// `tenant token revoke`: remove every token mapped to `tenant_id`. Needs no
/// plaintext token: entries carry `tenant_id` in the clear, so this is a
/// direct `remove_tokens_by_tenant` call and is correct even when the caller
/// has never seen the tenant's tokens.
pub async fn revoke(
    store: Arc<dyn ObjectStoreBackend>,
    deployment_key_file: &Path,
    tenant_id: &str,
    now_ns: i64,
) -> anyhow::Result<()> {
    let key = load_deployment_key(deployment_key_file)?;
    match remove_tokens_by_tenant(store.as_ref(), &key, tenant_id, now_ns).await? {
        AuthSetOutcome::Updated => println!("revoked all tokens for tenant {tenant_id}"),
        AuthSetOutcome::Unchanged => {
            println!("no tokens were mapped to tenant {tenant_id}; nothing to revoke")
        }
        AuthSetOutcome::Created => unreachable!("remove never creates the object"),
    }
    Ok(())
}

/// `tenant token list`: print the deployment-key fingerprint and, per entry,
/// the tenant id and a short token fingerprint. Never prints a raw token
/// hash, and there is no plaintext to print.
pub async fn list(
    store: Arc<dyn ObjectStoreBackend>,
    deployment_key_file: &Path,
) -> anyhow::Result<String> {
    let key = load_deployment_key(deployment_key_file)?;
    let mut out = String::new();
    match read_auth_map(store.as_ref(), &key).await? {
        None => {
            out.push_str(
                "sys/auth is not present: no tokens have been provisioned in this bucket yet\n",
            );
        }
        Some((map, _version)) => {
            out.push_str(&format!(
                "deployment_key_fingerprint: {}\n",
                hex::encode(map.key_fingerprint)
            ));
            for entry in &map.entries {
                out.push_str(&format!(
                    "tenant_id: {}  token_fingerprint: {}\n",
                    entry.tenant_id,
                    hex::encode(&entry.token_hash[..TOKEN_FINGERPRINT_LEN])
                ));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    fn key_file() -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("create temp key file");
        f.write_all(&[0x77u8; 32]).expect("write key");
        f
    }

    /// Upsert, then list, then revoke, then list again: the full round trip
    /// against a `MemoryStore`.
    #[tokio::test]
    async fn upsert_list_revoke_round_trip() {
        let store = store();
        let kf = key_file();

        upsert(store.clone(), kf.path(), b"tok-a", "tenant-a", 1)
            .await
            .expect("upsert");
        let listing = list(store.clone(), kf.path()).await.expect("list");
        assert!(
            listing.contains("tenant_id: tenant-a"),
            "listing: {listing}"
        );
        assert!(
            !listing.contains("tok-a"),
            "plaintext must never appear in the listing: {listing}"
        );

        revoke(store.clone(), kf.path(), "tenant-a", 2)
            .await
            .expect("revoke");
        let listing = list(store.clone(), kf.path())
            .await
            .expect("list after revoke");
        assert!(
            !listing.contains("tenant_id: tenant-a"),
            "revoked tenant must not appear: {listing}"
        );
    }

    /// `list` never prints the full 32-byte token hash, only a short prefix.
    #[tokio::test]
    async fn list_never_prints_full_token_hash() {
        let store = store();
        let kf = key_file();
        upsert(store.clone(), kf.path(), b"secret-token", "tenant-a", 1)
            .await
            .expect("upsert");

        let key = load_deployment_key(kf.path()).expect("parse key");
        let full_hash = ravel_catalog::token_hash(&key, b"secret-token");
        let listing = list(store.clone(), kf.path()).await.expect("list");
        assert!(
            !listing.contains(&hex::encode(full_hash)),
            "the full hash must never appear in the listing: {listing}"
        );
        assert!(
            listing.contains(&hex::encode(&full_hash[..TOKEN_FINGERPRINT_LEN])),
            "a short fingerprint prefix is expected: {listing}"
        );
    }

    /// Revoking a tenant with no tokens is a clean no-op, not an error.
    #[tokio::test]
    async fn revoke_absent_tenant_is_a_no_op() {
        let store = store();
        let kf = key_file();
        revoke(store.clone(), kf.path(), "tenant-a", 1)
            .await
            .expect("revoke on empty bucket succeeds");
    }

    /// `list` on a fresh bucket reports absence rather than erroring.
    #[tokio::test]
    async fn list_on_unbootstrapped_bucket_reports_absence() {
        let store = store();
        let kf = key_file();
        let listing = list(store, kf.path()).await.expect("list on fresh bucket");
        assert!(listing.contains("not present"), "listing: {listing}");
    }
}
