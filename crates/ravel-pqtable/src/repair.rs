//! Removing manifest versions above [`MAX_MANIFEST_VERSION`] (ADR-2040,
//! version bound amendment).
//!
//! No writer creates such a version, so one was put by something else: the
//! Query credential's create-only grant admits any 20-digit version key.
//! Readers and the sweep already ignore it ([`crate::resolve::newest`],
//! [`crate::sweep::plan`]); this module lists a table's version keys, flags
//! the ones above the bound, and deletes exactly those.
//!
//! The repair runs under the Maintain credential, which lists and deletes
//! under `t/<tenant_hash>/pq/t/` and reads no manifest. Which versions are
//! flagged is therefore decided from the listing alone. [`describe`] reads a
//! version's body only to show who wrote it and with what statement, and
//! reports a read it could not make rather than failing.
//!
//! A key under the table's `v/` prefix whose 20 digits do not fit in a `u64`
//! is above the bound too, and is flagged. Any other key that is not a
//! manifest version is listed, never flagged and never deleted here.

use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, MANIFEST_SUFFIX, MAX_MANIFEST_VERSION, VERSION_WIDTH, manifest_prefix,
    parse_manifest_key,
};
use crate::manifest::{Manifest, decode_manifest};

#[derive(Debug, thiserror::Error)]
pub enum RepairError {
    #[error("object store error on {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    /// A key asked to be deleted that is not a version of this table above
    /// [`MAX_MANIFEST_VERSION`]. Nothing was deleted.
    #[error(
        "refusing to delete {key:?}: it is not a manifest version above the version bound \
         {bound}; only those are removed by this repair"
    )]
    NotFlagged { key: String, bound: u64 },
    #[error(transparent)]
    Key(#[from] KeyError),
}

/// What a listed key under a table's `v/` prefix names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedVersion {
    /// A manifest version of this table.
    Number(u64),
    /// Twenty decimal digits that do not fit in a `u64`: above any bound.
    Overflow { digits: String },
    /// Not a manifest version key of this table.
    NotAVersion { reason: String },
}

/// One key under a table's `v/` prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedEntry {
    pub key: String,
    pub version: ListedVersion,
    /// True exactly when the key names a version above
    /// [`MAX_MANIFEST_VERSION`]: the keys [`delete_flagged`] deletes.
    pub flagged: bool,
}

/// What [`describe`] could read of one listed version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Description {
    Manifest(Box<Manifest>),
    /// Listed, then gone before the read.
    Missing,
    /// The read failed, or the body is not a manifest this build decodes.
    Unreadable {
        reason: String,
    },
}

/// Classify `key`, which was listed under `prefix` (the table's `v/` prefix).
fn classify(tenant: &TenantHash, table: &str, prefix: &str, key: &str) -> ListedEntry {
    let entry = |version: ListedVersion, flagged: bool| ListedEntry {
        key: key.to_string(),
        version,
        flagged,
    };
    match parse_manifest_key(key) {
        Ok(parsed) if parsed.tenant_hash == *tenant && parsed.table == table => {
            let flagged = parsed.version > MAX_MANIFEST_VERSION;
            entry(ListedVersion::Number(parsed.version), flagged)
        }
        Ok(_) => entry(
            ListedVersion::NotAVersion {
                reason: "belongs to another tenant or table".into(),
            },
            false,
        ),
        Err(err) => {
            let digits = key
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_suffix(MANIFEST_SUFFIX));
            match digits {
                Some(d)
                    if d.len() == VERSION_WIDTH
                        && d.bytes().all(|b| b.is_ascii_digit())
                        && d.parse::<u64>().is_err() =>
                {
                    entry(
                        ListedVersion::Overflow {
                            digits: d.to_string(),
                        },
                        true,
                    )
                }
                _ => entry(
                    ListedVersion::NotAVersion {
                        reason: err.to_string(),
                    },
                    false,
                ),
            }
        }
    }
}

/// Every key under `table`'s `v/` prefix, in listing (ascending key) order,
/// each flagged when it names a version above [`MAX_MANIFEST_VERSION`]. One
/// paginated LIST; no manifest is read.
pub async fn list(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
) -> Result<Vec<ListedEntry>, RepairError> {
    let prefix = manifest_prefix(tenant, table)?;
    let mut listed = list_all(store, &prefix)
        .await
        .map_err(|source| RepairError::Store {
            key: prefix.clone(),
            source,
        })?;
    listed.sort_by(|a, b| a.key.cmp(&b.key));
    listed.dedup_by(|a, b| a.key == b.key);
    Ok(listed
        .iter()
        .map(|meta| classify(tenant, table, &prefix, &meta.key))
        .collect())
}

/// Read `entry`'s body for display. Never fails: a read the credential may
/// not make, or a body that does not decode, is [`Description::Unreadable`].
pub async fn describe(store: &dyn ObjectStoreBackend, entry: &ListedEntry) -> Description {
    match store.get(&entry.key, GetRange::Full).await {
        Ok(outcome) => match decode_manifest(&entry.key, &outcome.data) {
            Ok(manifest) => Description::Manifest(Box::new(manifest)),
            Err(err) => Description::Unreadable {
                reason: err.to_string(),
            },
        },
        Err(StoreError::NotFound) => Description::Missing,
        Err(err) => Description::Unreadable {
            reason: err.to_string(),
        },
    }
}

/// Delete `keys`, each of which must be a version of `table` above
/// [`MAX_MANIFEST_VERSION`]. Every key is checked before the first delete,
/// so one that is not flagged ([`RepairError::NotFlagged`]) deletes nothing.
/// A delete that fails stops the repair; the keys deleted before it are
/// gone and a later listing no longer shows them. Returns the deleted keys.
pub async fn delete_flagged(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    keys: &[String],
) -> Result<Vec<String>, RepairError> {
    let prefix = manifest_prefix(tenant, table)?;
    for key in keys {
        if !classify(tenant, table, &prefix, key).flagged {
            return Err(RepairError::NotFlagged {
                key: key.clone(),
                bound: MAX_MANIFEST_VERSION,
            });
        }
    }
    let mut deleted = Vec::with_capacity(keys.len());
    for key in keys {
        store
            .delete(key)
            .await
            .map_err(|source| RepairError::Store {
                key: key.clone(),
                source,
            })?;
        deleted.push(key.clone());
    }
    Ok(deleted)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_object_store::PutOptions;
    use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::keys::manifest_key;
    use crate::manifest::encode_manifest;
    use crate::test_util::{TENANT_A, TENANT_B, live_manifest};

    async fn put_version(store: &dyn ObjectStoreBackend, tenant: &TenantHash, version: u64) {
        let key = manifest_key(tenant, "hits", version).expect("key");
        let bytes = encode_manifest(tenant, &live_manifest("hits", version, &[1])).expect("encode");
        store
            .put(&key, Bytes::from(bytes), PutOptions::create_if_absent())
            .await
            .expect("put");
    }

    fn mkey(version: u64) -> String {
        manifest_key(&TENANT_A, "hits", version).expect("key")
    }

    fn overflow_key() -> String {
        format!(
            "{}99999999999999999999.pqm",
            manifest_prefix(&TENANT_A, "hits").expect("prefix")
        )
    }

    /// Versions 1, the bound, one above it and u64::MAX of `hits`, one
    /// overflowing key, version 7 of another tenant, and junk.
    async fn forged_store() -> MemoryStore {
        let store = MemoryStore::with_page_size(2);
        for v in [1, MAX_MANIFEST_VERSION, MAX_MANIFEST_VERSION + 1, u64::MAX] {
            put_version(&store, &TENANT_A, v).await;
        }
        put_version(&store, &TENANT_B, 7).await;
        for key in [
            overflow_key(),
            format!(
                "{}notes.txt",
                manifest_prefix(&TENANT_A, "hits").expect("prefix")
            ),
        ] {
            store
                .put(&key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
        }
        store
    }

    #[tokio::test]
    async fn list_flags_exactly_the_versions_above_the_bound() {
        let store = forged_store().await;
        let got = list(&store, &TENANT_A, "hits").await.expect("list");
        let summary: Vec<(String, bool)> = got.iter().map(|e| (e.key.clone(), e.flagged)).collect();
        let notes = format!(
            "{}notes.txt",
            manifest_prefix(&TENANT_A, "hits").expect("prefix")
        );
        assert_eq!(
            summary,
            vec![
                (mkey(1), false),
                (mkey(MAX_MANIFEST_VERSION), false),
                (mkey(MAX_MANIFEST_VERSION + 1), true),
                (mkey(u64::MAX), true),
                (overflow_key(), true),
                (notes, false),
            ]
        );
        assert_eq!(got[0].version, ListedVersion::Number(1));
        assert_eq!(
            got[4].version,
            ListedVersion::Overflow {
                digits: "99999999999999999999".into()
            }
        );
        assert!(matches!(got[5].version, ListedVersion::NotAVersion { .. }));
    }

    #[tokio::test]
    async fn describe_reads_the_writer_and_statement_and_reports_what_it_cannot_read() {
        let store = forged_store().await;
        let got = list(&store, &TENANT_A, "hits").await.expect("list");
        match describe(&store, &got[3]).await {
            Description::Manifest(m) => {
                assert_eq!(m.version, u64::MAX);
                assert_eq!(m.created_by, "test");
                assert_eq!(m.statement, format!("v{}", u64::MAX));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            describe(&store, &got[4]).await,
            Description::Unreadable { .. }
        ));
        store.delete(&got[0].key).await.expect("delete");
        assert_eq!(describe(&store, &got[0]).await, Description::Missing);
    }

    #[tokio::test]
    async fn delete_flagged_removes_exactly_the_flagged_keys() {
        let store = InstrumentedStore::new(forged_store().await);
        let flagged: Vec<String> = list(&store, &TENANT_A, "hits")
            .await
            .expect("list")
            .into_iter()
            .filter(|e| e.flagged)
            .map(|e| e.key)
            .collect();
        let deleted = delete_flagged(&store, &TENANT_A, "hits", &flagged)
            .await
            .expect("delete");
        assert_eq!(deleted, flagged);
        assert_eq!(store.metrics().snapshot().op(StoreOp::Delete).calls, 3);
        let left: Vec<String> = list(&store, &TENANT_A, "hits")
            .await
            .expect("list")
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(left.len(), 3, "{left:?}");
        assert_eq!(left[..2], [mkey(1), mkey(MAX_MANIFEST_VERSION)]);
        assert!(left[2].ends_with("notes.txt"));
        // The other tenant's version is untouched.
        store
            .head(&manifest_key(&TENANT_B, "hits", 7).expect("key"))
            .await
            .expect("tenant b");
    }

    #[tokio::test]
    async fn a_key_at_or_below_the_bound_is_refused_and_nothing_is_deleted() {
        let store = InstrumentedStore::new(forged_store().await);
        let notes = format!(
            "{}notes.txt",
            manifest_prefix(&TENANT_A, "hits").expect("prefix")
        );
        for refused in [
            mkey(MAX_MANIFEST_VERSION),
            mkey(1),
            notes,
            manifest_key(&TENANT_B, "hits", u64::MAX).expect("key"),
        ] {
            // A flagged key first: the refusal must come before any delete.
            let keys = vec![mkey(u64::MAX), refused.clone()];
            let got = delete_flagged(&store, &TENANT_A, "hits", &keys).await;
            assert!(
                matches!(
                    &got,
                    Err(RepairError::NotFlagged { key, bound })
                        if *key == refused && *bound == MAX_MANIFEST_VERSION
                ),
                "{got:?}"
            );
        }
        assert_eq!(store.metrics().snapshot().op(StoreOp::Delete).calls, 0);
        assert_eq!(
            list(&store, &TENANT_A, "hits").await.expect("list").len(),
            6
        );
    }
}
