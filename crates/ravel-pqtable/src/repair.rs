//! Removing forged manifest versions (ADR-2040, version bound amendment).
//!
//! No writer creates a version above [`MAX_MANIFEST_VERSION`] or a `.pqm` key
//! whose slot names no version, so one was put by something else: the
//! Query credential's create-only grant admits any 20-character version.
//! Readers and the sweep already skip both ([`crate::resolve::newest`],
//! [`crate::sweep::plan`]); [`list`] flags them and [`delete_flagged`]
//! deletes exactly those.
//!
//! A forged version at or below the bound cannot be told apart by its key. One
//! that is the table's highest is its newest, and one exactly at the bound
//! leaves the writer no next version, so every later DDL on the table is
//! refused. [`delete_version`] deletes one such version an operator has named,
//! having judged it forged from the DDL audit log (every statement the server
//! runs records `attempted` before it touches the store), and nothing else.
//!
//! The repair runs under the Maintain credential, which lists and deletes
//! under `t/<tenant_hash>/pq/t/` and reads no manifest. Which versions are
//! flagged is therefore decided from the listing alone. [`describe`] reads a
//! version's body only to show who wrote it and with what statement, and
//! reports a read it could not make rather than failing. Any other key that is
//! not a manifest version is listed, never flagged and never deleted here.

use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, ListedManifestKey, MAX_MANIFEST_VERSION, manifest_key, manifest_prefix,
    parse_listed_manifest_key,
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
    /// A key asked to be deleted that [`list`] does not flag. Nothing was
    /// deleted.
    #[error(
        "refusing to delete {key:?}: it is not a manifest version above the version bound \
         {bound} or a key naming no version; only those are removed by --delete"
    )]
    NotFlagged { key: String, bound: u64 },
    /// A version named for [`delete_version`] that is zero or above the
    /// bound. Nothing was listed or deleted.
    #[error(
        "refusing to delete version {version}: --delete-version takes a version from 1 to the \
         version bound {bound}; keys above the bound or naming no version are removed by --delete"
    )]
    VersionOutOfRange { version: u64, bound: u64 },
    /// A version named for [`delete_version`] whose key is not in the
    /// listing. Nothing was deleted.
    #[error("refusing to delete {key:?}: no such key is listed")]
    NotListed { key: String },
    #[error(transparent)]
    Key(#[from] KeyError),
}

/// What a listed key under a table's `v/` prefix names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedVersion {
    /// A manifest version of this table.
    Number(u64),
    /// A `.pqm` key whose `slot` names no version: the wrong length, an extra
    /// path segment, more than `u64::MAX`, zero, or not all decimal digits.
    Invalid { slot: String, reason: String },
    /// Not a manifest version key of this table.
    NotAVersion { reason: String },
}

/// One key under a table's `v/` prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedEntry {
    pub key: String,
    pub version: ListedVersion,
    /// True exactly when the key names a version above
    /// [`MAX_MANIFEST_VERSION`] or is [`ListedVersion::Invalid`]: the keys
    /// [`delete_flagged`] deletes.
    pub flagged: bool,
    /// When the store wrote the key, by the store's clock. Unlike a
    /// manifest's `created_unix_ns`, whoever put the key cannot choose it.
    pub last_modified_unix_ms: i64,
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

/// What `key`, listed under `table`'s `v/` prefix, names, and whether it is
/// flagged.
fn classify(tenant: &TenantHash, table: &str, key: &str) -> (ListedVersion, bool) {
    let foreign = || ListedVersion::NotAVersion {
        reason: "belongs to another tenant or table".into(),
    };
    match parse_listed_manifest_key(key) {
        Ok(ListedManifestKey::Version(parsed)) => {
            if parsed.tenant_hash == *tenant && parsed.table == table {
                let flagged = parsed.version > MAX_MANIFEST_VERSION;
                (ListedVersion::Number(parsed.version), flagged)
            } else {
                (foreign(), false)
            }
        }
        Ok(ListedManifestKey::InvalidVersion {
            tenant_hash,
            table: listed_table,
            slot,
            reason,
        }) => {
            if tenant_hash == *tenant && listed_table == table {
                let reason = reason.to_string();
                (ListedVersion::Invalid { slot, reason }, true)
            } else {
                (foreign(), false)
            }
        }
        Err(err) => (
            ListedVersion::NotAVersion {
                reason: err.to_string(),
            },
            false,
        ),
    }
}

/// Every key under `table`'s `v/` prefix, in listing (ascending key) order,
/// each flagged when it names a version above [`MAX_MANIFEST_VERSION`] or is
/// a `.pqm` key whose slot names no version. One paginated LIST; no manifest
/// is read.
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
        .into_iter()
        .map(|meta| {
            let (version, flagged) = classify(tenant, table, &meta.key);
            ListedEntry {
                key: meta.key,
                version,
                flagged,
                last_modified_unix_ms: meta.last_modified_unix_ms,
            }
        })
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

/// Delete `keys`, each of which must be one [`list`] flags for `table`.
/// Every key is checked before the first delete, so one that is not flagged
/// ([`RepairError::NotFlagged`]) deletes nothing. A delete that fails stops
/// the repair; the keys deleted before it are gone and a later listing no
/// longer shows them. Returns the deleted keys.
pub async fn delete_flagged(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    keys: &[String],
) -> Result<Vec<String>, RepairError> {
    manifest_prefix(tenant, table)?;
    for key in keys {
        if !classify(tenant, table, key).1 {
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

/// Delete exactly the key of `table`'s manifest `version`, which must be in
/// `1..=MAX_MANIFEST_VERSION` ([`RepairError::VersionOutOfRange`] otherwise,
/// before any store call) and listed ([`RepairError::NotListed`] otherwise).
/// For a version an operator has judged forged; nothing about the version is
/// checked beyond its range. Returns the deleted key.
pub async fn delete_version(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    version: u64,
) -> Result<String, RepairError> {
    if version == 0 || version > MAX_MANIFEST_VERSION {
        return Err(RepairError::VersionOutOfRange {
            version,
            bound: MAX_MANIFEST_VERSION,
        });
    }
    let key = manifest_key(tenant, table, version)?;
    // Listing the key itself costs one request however many versions the
    // table holds.
    let listed = list_all(store, &key)
        .await
        .map_err(|source| RepairError::Store {
            key: key.clone(),
            source,
        })?;
    if !listed.iter().any(|meta| meta.key == key) {
        return Err(RepairError::NotListed { key });
    }
    store
        .delete(&key)
        .await
        .map_err(|source| RepairError::Store {
            key: key.clone(),
            source,
        })?;
    Ok(key)
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

    const OVERFLOW: &str = "99999999999999999999";
    const ZERO: &str = "00000000000000000000";
    const NON_DIGIT: &str = "abcdefghijklmnopqrst";
    /// An extra path segment between the table's `v/` and a second `v/`, which
    /// the Query grant's `*` binds. Sorts after `notes.txt` (`q` > `n`).
    const NESTED: &str = "q/v/00000000000000000001";

    /// The `.pqm` key of `hits` whose 20 version characters are `slot`.
    fn invalid_key(slot: &str) -> String {
        format!(
            "{}{slot}.pqm",
            manifest_prefix(&TENANT_A, "hits").expect("prefix")
        )
    }

    fn notes_key() -> String {
        format!(
            "{}notes.txt",
            manifest_prefix(&TENANT_A, "hits").expect("prefix")
        )
    }

    /// Versions 1, the bound, one above it and u64::MAX of `hits`, keys whose
    /// version characters overflow a u64, are zero, are not digits and sit
    /// under an extra path segment, version 7 of another tenant, and junk.
    async fn forged_store() -> MemoryStore {
        let store = MemoryStore::with_page_size(2);
        for v in [1, MAX_MANIFEST_VERSION, MAX_MANIFEST_VERSION + 1, u64::MAX] {
            put_version(&store, &TENANT_A, v).await;
        }
        put_version(&store, &TENANT_B, 7).await;
        for key in [
            invalid_key(OVERFLOW),
            invalid_key(ZERO),
            invalid_key(NON_DIGIT),
            invalid_key(NESTED),
            notes_key(),
        ] {
            store
                .put(&key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
        }
        store
    }

    #[tokio::test]
    async fn list_flags_exactly_the_versions_above_the_bound_and_the_keys_naming_none() {
        let store = forged_store().await;
        store.set_clock_ms(1_234);
        put_version(&store, &TENANT_A, 2).await;
        let got = list(&store, &TENANT_A, "hits").await.expect("list");
        let summary: Vec<(String, bool)> = got.iter().map(|e| (e.key.clone(), e.flagged)).collect();
        assert_eq!(
            summary,
            vec![
                (invalid_key(ZERO), true),
                (mkey(1), false),
                (mkey(2), false),
                (mkey(MAX_MANIFEST_VERSION), false),
                (mkey(MAX_MANIFEST_VERSION + 1), true),
                (mkey(u64::MAX), true),
                (invalid_key(OVERFLOW), true),
                (invalid_key(NON_DIGIT), true),
                (notes_key(), false),
                (invalid_key(NESTED), true),
            ]
        );
        assert_eq!(got[1].version, ListedVersion::Number(1));
        for (entry, slot, reason) in [
            (&got[0], ZERO, "version is zero"),
            (&got[6], OVERFLOW, "version does not fit in a u64"),
            (&got[7], NON_DIGIT, "version is not 20 decimal digits"),
            (&got[9], NESTED, "version is not 20 decimal digits"),
        ] {
            assert_eq!(
                entry.version,
                ListedVersion::Invalid {
                    slot: slot.into(),
                    reason: reason.into()
                }
            );
        }
        assert!(matches!(got[8].version, ListedVersion::NotAVersion { .. }));
        // The store's own write time, not anything the manifest claims.
        assert_eq!(got[2].last_modified_unix_ms, 1_234);
        assert_eq!(got[1].last_modified_unix_ms, 0);
    }

    #[tokio::test]
    async fn describe_reads_the_writer_and_statement_and_reports_what_it_cannot_read() {
        let store = forged_store().await;
        let got = list(&store, &TENANT_A, "hits").await.expect("list");
        assert_eq!(got[4].key, mkey(u64::MAX));
        match describe(&store, &got[4]).await {
            Description::Manifest(m) => {
                assert_eq!(m.version, u64::MAX);
                assert_eq!(m.created_by, "test");
                assert_eq!(m.statement, format!("v{}", u64::MAX));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            describe(&store, &got[5]).await,
            Description::Unreadable { .. }
        ));
        store.delete(&got[1].key).await.expect("delete");
        assert_eq!(describe(&store, &got[1]).await, Description::Missing);
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
        assert_eq!(
            flagged,
            [
                invalid_key(ZERO),
                mkey(MAX_MANIFEST_VERSION + 1),
                mkey(u64::MAX),
                invalid_key(OVERFLOW),
                invalid_key(NON_DIGIT),
                invalid_key(NESTED),
            ]
        );
        assert_eq!(deleted, flagged);
        assert_eq!(store.metrics().snapshot().op(StoreOp::Delete).calls, 6);
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
        let tenant_b_prefix = manifest_prefix(&TENANT_B, "hits").expect("prefix");
        for refused in [
            mkey(MAX_MANIFEST_VERSION),
            mkey(1),
            notes_key(),
            manifest_key(&TENANT_B, "hits", u64::MAX).expect("key"),
            format!("{tenant_b_prefix}{ZERO}.pqm"),
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
            9
        );
    }

    fn keys_of(listed: Vec<ListedEntry>) -> Vec<String> {
        listed.into_iter().map(|e| e.key).collect()
    }

    #[tokio::test]
    async fn delete_version_deletes_exactly_the_named_key() {
        let store = InstrumentedStore::new(forged_store().await);
        for v in [2, 3] {
            put_version(store.inner(), &TENANT_A, v).await;
        }
        let before = keys_of(list(&store, &TENANT_A, "hits").await.expect("list"));
        let deleted = delete_version(&store, &TENANT_A, "hits", 2)
            .await
            .expect("delete");
        assert_eq!(deleted, mkey(2));
        assert_eq!(store.metrics().snapshot().op(StoreOp::Delete).calls, 1);
        let after = keys_of(list(&store, &TENANT_A, "hits").await.expect("list"));
        let expected: Vec<String> = before.into_iter().filter(|k| *k != mkey(2)).collect();
        assert_eq!(after, expected);
        assert!(after.contains(&mkey(1)) && after.contains(&mkey(3)));
        // The bound itself is a version an operator may name.
        assert_eq!(
            delete_version(&store, &TENANT_A, "hits", MAX_MANIFEST_VERSION)
                .await
                .expect("delete at the bound"),
            mkey(MAX_MANIFEST_VERSION)
        );
        store
            .head(&manifest_key(&TENANT_B, "hits", 7).expect("key"))
            .await
            .expect("tenant b");
    }

    #[tokio::test]
    async fn delete_version_refuses_zero_and_above_the_bound_before_any_store_call() {
        let store = InstrumentedStore::new(forged_store().await);
        for version in [0, MAX_MANIFEST_VERSION + 1, u64::MAX] {
            let got = delete_version(&store, &TENANT_A, "hits", version).await;
            assert!(
                matches!(
                    &got,
                    Err(RepairError::VersionOutOfRange { version: v, bound })
                        if *v == version && *bound == MAX_MANIFEST_VERSION
                ),
                "{got:?}"
            );
        }
        let snapshot = store.metrics().snapshot();
        assert_eq!(snapshot.op(StoreOp::Delete).calls, 0);
        assert_eq!(snapshot.op(StoreOp::List).calls, 0);
        // A version in range that is not there is refused, not reported
        // deleted.
        let got = delete_version(&store, &TENANT_A, "hits", 5).await;
        assert!(
            matches!(&got, Err(RepairError::NotListed { key }) if *key == mkey(5)),
            "{got:?}"
        );
        assert_eq!(store.metrics().snapshot().op(StoreOp::Delete).calls, 0);
        assert_eq!(
            list(&store, &TENANT_A, "hits").await.expect("list").len(),
            9
        );
    }

    /// A version put exactly at the bound leaves the writer no next version.
    /// Once an operator deletes it, the next statement numbers from the
    /// newest version beneath it.
    #[tokio::test]
    async fn a_table_wedged_at_the_bound_takes_ddl_again_once_that_version_is_deleted() {
        use std::collections::BTreeMap;

        use crate::clock::FixedClock;
        use crate::test_util::file_for;
        use crate::writer::{Intent, Outcome, WriteError, apply};

        let replace = || Intent::CreateOrReplace {
            location: "s3://customer/data/".into(),
            grant: "s3://customer/data".into(),
            files: vec![file_for(2)],
            options: BTreeMap::new(),
            created_by: "t".into(),
            statement: "s".into(),
        };
        let store = MemoryStore::new();
        put_version(&store, &TENANT_A, 1).await;
        put_version(&store, &TENANT_A, MAX_MANIFEST_VERSION).await;
        let clock = FixedClock::new(0);
        let got = apply(&store, &TENANT_A, "hits", replace(), &clock, 660_000).await;
        assert!(
            matches!(got, Err(WriteError::VersionAboveBound { .. })),
            "{got:?}"
        );
        assert_eq!(
            delete_version(&store, &TENANT_A, "hits", MAX_MANIFEST_VERSION)
                .await
                .expect("delete"),
            mkey(MAX_MANIFEST_VERSION)
        );
        assert_eq!(
            apply(&store, &TENANT_A, "hits", replace(), &clock, 660_000)
                .await
                .expect("apply"),
            Outcome::Committed { version: 2 }
        );
        assert_eq!(
            crate::resolve::versions(&store, &TENANT_A, "hits")
                .await
                .expect("versions"),
            vec![1, 2]
        );
    }
}
