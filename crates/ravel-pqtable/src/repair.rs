//! Removing forged manifest versions (ADR-2040, version bound amendment).
//!
//! No writer creates a version above [`MAX_MANIFEST_VERSION`] or a `.pqm` key
//! whose slot names no version, so one was put by something else: the
//! Query credential's create-only grant admits any 20-character version, and
//! its table wildcard also admits an extra path segment under a table's `v/`.
//! Readers and the sweep already skip both ([`crate::resolve::newest`],
//! [`crate::sweep::plan`]); [`list`] flags them and [`delete_flagged`]
//! deletes exactly those, except a flagged key the S3 adapter would send a
//! delete of to a different key ([`ListedEntry::undeletable`]), which it
//! skips and reports.
//!
//! A forged version at or below the bound cannot be told apart by its key. One
//! that is the table's highest is its newest, and one exactly at the bound
//! leaves the writer no next version, so every later DDL on the table is
//! refused. [`delete_version`] deletes one such version an operator has named,
//! having judged it forged from the DDL audit log (every statement the server
//! runs records `attempted` before it touches the store), and nothing else.
//!
//! The same grant's `*` also admits a `.pqm` key whose segment between
//! `pq/t/` and `/v/` is not a valid table name
//! ([`ListedManifestKey::InvalidTable`]). No table owns it, so no per-table
//! listing shows it; the tenant-wide listings skip it when the store lists
//! it. [`list_stray`] lists every such key of a tenant and [`delete_stray`]
//! deletes exactly those, except a key the S3 adapter would send a delete
//! of to a different key ([`StrayClass::Undeletable`]) and, unless asked, a
//! key that may be a table created before its name was reserved
//! ([`StrayClass::ReservedName`]). The S3 adapter cannot list a key holding
//! a control character, an empty segment or a `.` or `..` segment at all:
//! the listing that meets one fails, here and in the tenant-wide listings,
//! and only a delete of the exact key through an S3 tool removes it.
//!
//! The repair runs under the Maintain credential, which lists and deletes
//! under `t/<tenant_hash>/pq/t/` and reads no manifest. Which versions are
//! flagged is therefore decided from the listing alone. [`describe`] reads a
//! version's body only to show who wrote it and with what statement, and
//! reports a read it could not make rather than failing. Any other key that is
//! not a manifest version is listed, never flagged and never deleted here.

use ravel_object_store::{
    DrainStep, GetRange, MAX_LIST_PAGES, ObjectStoreBackend, StoreError, drain_pages,
};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, ListedManifestKey, MANIFEST_SUFFIX, MAX_MANIFEST_VERSION, VERSION_WIDTH,
    is_store_path, manifest_key, manifest_prefix, parse_listed_manifest_key, store_path,
    tenant_manifest_prefix,
};
use crate::manifest::{Manifest, decode_manifest};
use crate::names::IAM_GRANT_SEGMENTS;

#[derive(Debug, thiserror::Error)]
pub enum RepairError {
    #[error("object store error on {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    /// The delete of `key` failed, after `deleted`, the keys before it, were
    /// deleted.
    #[error(
        "object store error deleting {key:?}, after deleting {} key(s) before it: {source}",
        deleted.len()
    )]
    Delete {
        key: String,
        deleted: Vec<String>,
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
    /// A key asked to be deleted that [`list_stray`] does not list: a manifest
    /// key of a valid table, another tenant's key, or one of no manifest
    /// shape. Nothing was deleted.
    #[error(
        "refusing to delete {key:?}: it is not a key of this tenant whose segment between pq/t/ \
         and /v/ is not a valid table name; only those are removed by --stray --delete"
    )]
    NotStray { key: String },
    /// A key asked to be deleted that the S3 adapter would send the delete
    /// of to `path`, a different key ([`StrayClass::Undeletable`]). Nothing
    /// was deleted.
    #[error(
        "refusing to delete {key:?}: the store's path encoding sends a delete of it to {path:?}, \
         a different key; delete the exact key with the Maintain credential through an S3 tool"
    )]
    Undeletable { key: String, path: String },
    /// A key asked to be deleted that may be the manifest of a table created
    /// before its name was reserved ([`StrayClass::ReservedName`]), without
    /// the caller asking to include those. Nothing was deleted.
    #[error(
        "refusing to delete {key:?}: its table segment is a name reserved after tables could be \
         created, so it may be a manifest of such a table; pass --include-reserved-names to \
         delete it"
    )]
    ReservedName { key: String },
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
    /// [`delete_flagged`] deletes, unless [`ListedEntry::undeletable`].
    pub flagged: bool,
    /// True when the listing reported the key unaddressable: a delete of it
    /// would reach [`store_path`] of it, a different key, so Ravel cannot
    /// delete it, and [`delete_flagged`] skips it.
    pub undeletable: bool,
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
        Ok(ListedManifestKey::InvalidTable { .. }) => (foreign(), false),
        Err(err) => (
            ListedVersion::NotAVersion {
                reason: err.to_string(),
            },
            false,
        ),
    }
}

/// A key a listing returned, and whether the listing reported it
/// unaddressable.
struct ListedKey {
    key: String,
    last_modified_unix_ms: i64,
    unaddressable: bool,
}

/// Every key under `prefix` once, in ascending key order: the listing's
/// objects and every key a page reported unaddressable. The drain's own
/// [`ravel_object_store::Unaddressable`] keeps only a sample, so each page's
/// `unaddressable` keys are collected here instead.
async fn list_every_key(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> Result<Vec<ListedKey>, RepairError> {
    let mut listed = Vec::new();
    let skipped = std::sync::Mutex::new(Vec::new());
    drain_pages::<StoreError, _, _, _>(
        prefix,
        None,
        MAX_LIST_PAGES,
        |_, token| {
            let skipped = &skipped;
            async move {
                let page = store.list(prefix, token).await?;
                skipped
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .extend(page.unaddressable.iter().map(|skipped| ListedKey {
                        key: skipped.key.clone(),
                        last_modified_unix_ms: skipped.last_modified_unix_ms,
                        unaddressable: true,
                    }));
                Ok(page)
            }
        },
        |meta| {
            listed.push(ListedKey {
                key: meta.key,
                last_modified_unix_ms: meta.last_modified_unix_ms,
                unaddressable: false,
            });
            Ok(DrainStep::Continue)
        },
    )
    .await
    .map_err(|source| RepairError::Store {
        key: prefix.to_string(),
        source,
    })?;
    listed.extend(
        skipped
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    listed.sort_by(|a, b| a.key.cmp(&b.key));
    // A page boundary can re-deliver a key; an unaddressable key is never
    // also an object, so equal keys are repeats of one key.
    listed.dedup_by(|a, b| a.key == b.key);
    Ok(listed)
}

/// Every key under `table`'s `v/` prefix, in ascending key order, each
/// flagged when it names a version above [`MAX_MANIFEST_VERSION`] or is a
/// `.pqm` key whose slot names no version. A key the listing reports
/// unaddressable is listed too, as [`ListedEntry::undeletable`]. One
/// paginated LIST; no manifest is read.
pub async fn list(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
) -> Result<Vec<ListedEntry>, RepairError> {
    let prefix = manifest_prefix(tenant, table)?;
    Ok(list_every_key(store, &prefix)
        .await?
        .into_iter()
        .map(|listed| {
            let (version, flagged) = classify(tenant, table, &listed.key);
            ListedEntry {
                key: listed.key,
                version,
                flagged,
                undeletable: listed.unaddressable,
                last_modified_unix_ms: listed.last_modified_unix_ms,
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

/// What [`delete_flagged`] removed and what it left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlaggedDeletion {
    /// The flagged keys deleted, in the order given.
    pub deleted: Vec<String>,
    /// The flagged keys the S3 adapter would send a delete of to a different
    /// key, in the order given. None was sent a delete.
    pub undeletable: Vec<String>,
}

/// Delete `keys`, each of which must be one [`list`] flags for `table`,
/// skipping each the S3 adapter would send a delete of to a different key
/// ([`ListedEntry::undeletable`]): no delete reaches a key other than the one
/// flagged. Every key is checked before the first delete, so one that is not
/// flagged ([`RepairError::NotFlagged`]) deletes nothing. A delete that fails
/// stops the repair with [`RepairError::Delete`], naming the keys deleted
/// before it.
pub async fn delete_flagged(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    keys: &[String],
) -> Result<FlaggedDeletion, RepairError> {
    manifest_prefix(tenant, table)?;
    if let Some(key) = keys.iter().find(|key| !classify(tenant, table, key).1) {
        return Err(RepairError::NotFlagged {
            key: key.clone(),
            bound: MAX_MANIFEST_VERSION,
        });
    }
    let (deletable, undeletable): (Vec<String>, Vec<String>) =
        keys.iter().cloned().partition(|key| is_store_path(key));
    let deleted = delete_each(store, &deletable).await?;
    Ok(FlaggedDeletion {
        deleted,
        undeletable,
    })
}

/// Delete `keys` in order, stopping at the first delete that fails.
async fn delete_each(
    store: &dyn ObjectStoreBackend,
    keys: &[String],
) -> Result<Vec<String>, RepairError> {
    let mut deleted = Vec::with_capacity(keys.len());
    for key in keys {
        if let Err(source) = store.delete(key).await {
            return Err(RepairError::Delete {
                key: key.clone(),
                deleted,
                source,
            });
        }
        deleted.push(key.clone());
    }
    Ok(deleted)
}

/// A key of `tenant` whose segment between `pq/t/` and `/v/` is not a valid
/// table name ([`ListedManifestKey::InvalidTable`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrayEntry {
    pub key: String,
    /// When the store wrote the key, by the store's clock.
    pub last_modified_unix_ms: i64,
    pub class: StrayClass,
}

/// Whether [`delete_stray`] deletes a stray key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrayClass {
    /// Deleted.
    Deletable,
    /// `t/<tenant_hash>/pq/t/<segment>/v/<20 digits>.pqm` whose segment is
    /// one of [`IAM_GRANT_SEGMENTS`], reserved after tables could be created,
    /// so possibly a manifest of a table created before the name was
    /// reserved. Deleted only when the caller includes reserved names.
    ReservedName,
    /// The S3 adapter sends a delete of the key to [`store_path`] of it, a
    /// different key, so Ravel cannot delete it. Never deleted.
    Undeletable,
}

impl StrayClass {
    fn of(key: &str, rest: &str) -> Self {
        if !is_store_path(key) {
            StrayClass::Undeletable
        } else if reserved_after_tables_existed(rest) {
            StrayClass::ReservedName
        } else {
            StrayClass::Deletable
        }
    }
}

/// Whether `rest`, the key text after `pq/t/`, is `<segment>/v/<20
/// digits>.pqm` for a single segment [`crate::names::validate_table`] has
/// refused only since the IAM segment amendment. The built-in and signal
/// names have been refused since tables could first be created.
fn reserved_after_tables_existed(rest: &str) -> bool {
    let Some((segment, slot)) = rest
        .strip_suffix(MANIFEST_SUFFIX)
        .and_then(|stem| stem.split_once("/v/"))
    else {
        return false;
    };
    IAM_GRANT_SEGMENTS.contains(&segment)
        && slot.len() == VERSION_WIDTH
        && slot.bytes().all(|b| b.is_ascii_digit())
}

/// The class of `key` when it is a stray key of `tenant`, `None` otherwise.
fn stray_class(tenant: &TenantHash, key: &str) -> Option<StrayClass> {
    match parse_listed_manifest_key(key) {
        Ok(ListedManifestKey::InvalidTable { tenant_hash, rest }) if tenant_hash == *tenant => {
            Some(StrayClass::of(key, &rest))
        }
        _ => None,
    }
}

/// Every key under `t/<tenant_hash>/pq/t/` that is
/// [`ListedManifestKey::InvalidTable`], in ascending key order and with its
/// [`StrayClass`]: the keys the tenant-wide listings skip because no table
/// owns them. A key the listing reports unaddressable is listed too, as
/// [`StrayClass::Undeletable`]. Manifest keys of valid tables and keys of no
/// manifest shape are not listed. One paginated LIST of a prefix that ends at
/// a segment boundary; no key is read. On S3 a key under that prefix holding
/// a control character, an empty segment or a `.` or `..` segment fails the
/// listing with [`RepairError::Store`], as it fails the tenant-wide listings.
pub async fn list_stray(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<Vec<StrayEntry>, RepairError> {
    let prefix = tenant_manifest_prefix(tenant);
    Ok(list_every_key(store, &prefix)
        .await?
        .into_iter()
        .filter_map(|listed| {
            stray_class(tenant, &listed.key).map(|class| StrayEntry {
                key: listed.key,
                last_modified_unix_ms: listed.last_modified_unix_ms,
                class,
            })
        })
        .collect())
}

/// Delete `keys`, each of which must be one [`list_stray`] lists for
/// `tenant` as [`StrayClass::Deletable`], or as [`StrayClass::ReservedName`]
/// when `include_reserved_names` is set. Every key is classified before the
/// first delete, so a manifest key of a valid table, another tenant's key, or
/// any other key ([`RepairError::NotStray`]), a key the S3 adapter would
/// delete a different key for ([`RepairError::Undeletable`]), and an
/// excluded reserved name ([`RepairError::ReservedName`]) delete nothing. A
/// delete that fails stops the repair with [`RepairError::Delete`], naming
/// the keys deleted before it. Returns the deleted keys.
pub async fn delete_stray(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    keys: &[String],
    include_reserved_names: bool,
) -> Result<Vec<String>, RepairError> {
    for key in keys {
        match stray_class(tenant, key) {
            None => return Err(RepairError::NotStray { key: key.clone() }),
            Some(StrayClass::Undeletable) => {
                return Err(RepairError::Undeletable {
                    key: key.clone(),
                    path: store_path(key),
                });
            }
            Some(StrayClass::ReservedName) if !include_reserved_names => {
                return Err(RepairError::ReservedName { key: key.clone() });
            }
            Some(StrayClass::ReservedName | StrayClass::Deletable) => {}
        }
    }
    delete_each(store, keys).await
}

/// Whether the manifest key `key` is in the listing of `prefix`, the `v/`
/// prefix it sits under. The S3 adapter appends the delimiter to every list
/// prefix, so `key` itself cannot be the prefix; the listing starts just
/// before `key` instead, so no version that sorts before it is listed. Only
/// keys that extend `key`'s 20 digits with text sorting below `.pqm` lie
/// between the two: none on a table no one put such keys under, where this
/// is one request, and otherwise paged through up to [`MAX_LIST_PAGES`]
/// pages. The Maintain credential may list here but not read, so this is a
/// listing rather than a HEAD.
async fn is_listed(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
    key: &str,
) -> Result<bool, RepairError> {
    // Below `key`, and every key between the two starts with it.
    let start_after = key.strip_suffix(MANIFEST_SUFFIX).unwrap_or(prefix);
    let mut found = false;
    drain_pages::<StoreError, _, _, _>(
        prefix,
        Some(start_after),
        MAX_LIST_PAGES,
        |after, token| async move { store.list_after(prefix, after.as_deref(), token).await },
        |meta| {
            if meta.key.as_str() < key {
                return Ok(DrainStep::Continue);
            }
            found = meta.key == key;
            Ok(DrainStep::Stop)
        },
    )
    .await
    .map_err(|source| RepairError::Store {
        key: prefix.to_string(),
        source,
    })?;
    Ok(found)
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
    let prefix = manifest_prefix(tenant, table)?;
    if !is_listed(store, &prefix, &key).await? {
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
    use crate::test_util::{S3KeyStore, SegmentAlignedStore, TENANT_A, TENANT_B, live_manifest};

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
        assert_eq!(
            deleted,
            FlaggedDeletion {
                deleted: flagged,
                undeletable: Vec::new(),
            }
        );
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

    /// The third delete fails: the error names that key and the two deleted
    /// before it, and nothing after it is sent a delete.
    #[tokio::test]
    async fn a_failed_delete_names_the_keys_deleted_before_it() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };

        let store = FaultStore::new(
            InstrumentedStore::new(forged_store().await),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Delete,
                    ScriptedFault::Permanent("delete refused".into()),
                )
                .with_occurrence(Occurrence::Nth(3)),
            ),
        );
        let flagged: Vec<String> = list(&store, &TENANT_A, "hits")
            .await
            .expect("list")
            .into_iter()
            .filter(|e| e.flagged)
            .map(|e| e.key)
            .collect();
        let got = delete_flagged(&store, &TENANT_A, "hits", &flagged).await;
        match got {
            Err(RepairError::Delete {
                key,
                deleted,
                source: StoreError::Permanent(msg),
            }) => {
                assert_eq!(key, flagged[2]);
                assert_eq!(deleted, flagged[..2]);
                assert_eq!(msg, "delete refused");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(store.fault_count(Op::Delete, FaultKind::Permanent), 1);
        assert_eq!(
            store.inner().metrics().snapshot().op(StoreOp::Delete).calls,
            2
        );
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

    async fn segment_aligned_forged_store() -> SegmentAlignedStore {
        let store = SegmentAlignedStore::new(forged_store().await);
        for v in [2, 3] {
            put_version(store.inner.inner(), &TENANT_A, v).await;
        }
        store
    }

    /// [`SegmentAlignedStore`] behind the S3 adapter's key handling.
    type S3Store = S3KeyStore<SegmentAlignedStore>;

    /// The raw key space under an [`S3Store`], where a test puts what a
    /// credential could put with any S3 client.
    fn memory(store: &S3Store) -> &MemoryStore {
        store.inner.inner.inner()
    }

    fn deletes_sent(store: &S3Store) -> u64 {
        store
            .inner
            .inner
            .metrics()
            .snapshot()
            .op(StoreOp::Delete)
            .calls
    }

    /// The text after `t/<tenant_hash>/pq/t/` of keys the Query grant admits
    /// under a segment that is not a valid table name, which the S3 adapter
    /// lists and deletes: upper case, a nested path, and a name reserved
    /// since tables could first be created.
    const STRAYS: [&str; 3] = [
        "Hits/v/00000000000000000001.pqm",
        "a/b/v/00000000000000000001.pqm",
        "logs/v/00000000000000000001.pqm",
    ];

    /// Stray keys no request reaches, because `Path::from` percent-encodes a
    /// tilde, a percent sign and a non-ASCII character. A listing reports
    /// them unaddressable, never as objects.
    const UNDELETABLE: [&str; 3] = [
        "Hits~/v/00000000000000000001.pqm",
        "a%2Fb/v/00000000000000000001.pqm",
        "Hits/v/0000000000000000000\u{e9}.pqm",
    ];

    /// Well-formed manifest keys of tables named with words reserved after
    /// tables could be created.
    const RESERVED: [&str; 2] = [
        "l0/v/00000000000000000001.pqm",
        "u/v/00000000000000000007.pqm",
    ];

    /// Stray keys the S3 adapter cannot address, because `Path::from`
    /// rewrites them: control characters, an empty segment, a `.` and a `..`
    /// segment, and an empty table segment.
    const UNLISTABLE: [&str; 5] = [
        "Hits/v/\u{1b}[2J\u{7}xxxxxxxxxxxxxxx.pqm",
        "hits//v/00000000000000000003.pqm",
        "./v/00000000000000000001.pqm",
        "a/../v/00000000000000000001.pqm",
        "/v/00000000000000000001.pqm",
    ];

    fn stray_key(tenant: &TenantHash, rest: &str) -> String {
        format!("{}{rest}", tenant_manifest_prefix(tenant))
    }

    /// Tenant A's keys for `rests` in ascending key order.
    fn sorted(rests: &[&str]) -> Vec<String> {
        let mut keys: Vec<String> = rests.iter().map(|r| stray_key(&TENANT_A, r)).collect();
        keys.sort();
        keys
    }

    /// [`forged_store`] with versions 2 and 3, every [`STRAYS`],
    /// [`UNDELETABLE`] and [`RESERVED`] key of tenant A written at store
    /// time 5_000, one stray key of tenant B, and a key under an invalid
    /// table segment without the `.pqm` suffix, behind the S3 adapter's
    /// listing and key handling.
    async fn stray_store() -> S3Store {
        let store = S3KeyStore {
            inner: segment_aligned_forged_store().await,
        };
        let memory = memory(&store);
        memory.set_clock_ms(5_000);
        for key in STRAYS
            .iter()
            .chain(&UNDELETABLE)
            .chain(&RESERVED)
            .map(|rest| stray_key(&TENANT_A, rest))
            .chain([
                stray_key(&TENANT_B, STRAYS[0]),
                stray_key(&TENANT_A, "Hits/v/00000000000000000001.parquet"),
            ])
        {
            memory.insert_foreign(&key, Bytes::from_static(b"x"));
        }
        store
    }

    /// The keys under `prefix` that a listing of `store` reports
    /// unaddressable.
    async fn unaddressable_under(store: &dyn ObjectStoreBackend, prefix: &str) -> Vec<String> {
        ravel_object_store::list_all_reporting(store, prefix)
            .await
            .expect("list")
            .unaddressable
            .sample
            .into_iter()
            .map(|skipped| skipped.key)
            .collect()
    }

    /// The [`UNDELETABLE`] keys, which the listing reports unaddressable,
    /// are listed as [`StrayClass::Undeletable`].
    #[tokio::test]
    async fn list_stray_lists_exactly_the_keys_under_no_valid_table() {
        let store = stray_store().await;
        let got = list_stray(&store, &TENANT_A).await.expect("list");
        assert_eq!(
            unaddressable_under(&store, &tenant_manifest_prefix(&TENANT_A)).await,
            sorted(&UNDELETABLE)
        );
        let mut expected: Vec<StrayEntry> = [
            (&STRAYS[..], StrayClass::Deletable),
            (&UNDELETABLE[..], StrayClass::Undeletable),
            (&RESERVED[..], StrayClass::ReservedName),
        ]
        .into_iter()
        .flat_map(|(rests, class)| {
            sorted(rests).into_iter().map(move |key| StrayEntry {
                key,
                last_modified_unix_ms: 5_000,
                class,
            })
        })
        .collect();
        expected.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(got, expected);
        assert_eq!(
            list_stray(&store, &TENANT_B).await.expect("list"),
            vec![StrayEntry {
                key: stray_key(&TENANT_B, STRAYS[0]),
                last_modified_unix_ms: 5_000,
                class: StrayClass::Deletable,
            }]
        );
        assert_eq!(deletes_sent(&store), 0);
    }

    #[test]
    fn only_a_well_formed_key_under_a_name_reserved_after_tables_existed_is_reserved() {
        for segment in IAM_GRANT_SEGMENTS {
            let rest = format!("{segment}/v/00000000000000000001.pqm");
            assert_eq!(
                StrayClass::of(&stray_key(&TENANT_A, &rest), &rest),
                StrayClass::ReservedName,
                "{rest}"
            );
        }
        for rest in [
            // Reserved since tables could first be created.
            "logs/v/00000000000000000001.pqm",
            "samples/v/00000000000000000001.pqm",
            // Not a single reserved segment, or not a well-formed version.
            "x/l0/v/00000000000000000001.pqm",
            "l0/x/v/00000000000000000001.pqm",
            "L0/v/00000000000000000001.pqm",
            "l0/v/0000000000000000000x.pqm",
            "l0/v/0000000000000000001x.pqm",
        ] {
            assert_eq!(
                StrayClass::of(&stray_key(&TENANT_A, rest), rest),
                StrayClass::Deletable,
                "{rest}"
            );
        }
    }

    #[tokio::test]
    async fn delete_stray_deletes_exactly_the_deletable_stray_keys() {
        let store = stray_store().await;
        let hits_before = keys_of(list(&store, &TENANT_A, "hits").await.expect("list"));
        let deletable: Vec<String> = list_stray(&store, &TENANT_A)
            .await
            .expect("list")
            .into_iter()
            .filter(|e| e.class == StrayClass::Deletable)
            .map(|e| e.key)
            .collect();
        assert_eq!(deletable, sorted(&STRAYS));
        let deleted = delete_stray(&store, &TENANT_A, &deletable, false)
            .await
            .expect("delete");
        assert_eq!(deleted, sorted(&STRAYS));
        assert_eq!(store.inner.deletes(), sorted(&STRAYS));
        let stray_keys = |entries: Vec<StrayEntry>| -> Vec<String> {
            entries.into_iter().map(|e| e.key).collect()
        };
        let mut left: Vec<&str> = RESERVED.to_vec();
        left.extend(UNDELETABLE);
        assert_eq!(
            stray_keys(list_stray(&store, &TENANT_A).await.expect("list")),
            sorted(&left)
        );
        // With reserved names included, those go too.
        let deleted = delete_stray(&store, &TENANT_A, &sorted(&RESERVED), true)
            .await
            .expect("delete");
        assert_eq!(deleted, sorted(&RESERVED));
        assert_eq!(
            stray_keys(list_stray(&store, &TENANT_A).await.expect("list")),
            sorted(&UNDELETABLE)
        );
        // Every valid table's keys, the other tenant's stray key, the
        // foreign key and the unaddressable keys are still there.
        assert_eq!(
            unaddressable_under(&store, &tenant_manifest_prefix(&TENANT_A)).await,
            sorted(&UNDELETABLE)
        );
        assert_eq!(
            keys_of(list(&store, &TENANT_A, "hits").await.expect("list")),
            hits_before
        );
        for key in [
            stray_key(&TENANT_B, STRAYS[0]),
            stray_key(&TENANT_A, "Hits/v/00000000000000000001.parquet"),
        ] {
            store.head(&key).await.expect("untouched");
        }
    }

    #[tokio::test]
    async fn delete_stray_refuses_any_other_key_before_any_delete() {
        let store = stray_store().await;
        let refusals = [
            mkey(1),
            mkey(u64::MAX),
            invalid_key(ZERO),
            notes_key(),
            stray_key(&TENANT_B, STRAYS[0]),
            stray_key(&TENANT_A, "Hits/v/00000000000000000001.parquet"),
        ]
        .into_iter()
        .map(|key| (key, "NotStray"))
        .chain(sorted(&UNDELETABLE).into_iter().map(|k| (k, "Undeletable")))
        .chain(sorted(&RESERVED).into_iter().map(|k| (k, "ReservedName")));
        for (refused, kind) in refusals {
            // A deletable stray key first: the refusal must come before any
            // delete.
            let mut keys = sorted(&STRAYS);
            keys.push(refused.clone());
            let got = delete_stray(&store, &TENANT_A, &keys, false).await;
            let named = match &got {
                Err(RepairError::NotStray { key }) => ("NotStray", key),
                Err(RepairError::Undeletable { key, path }) => {
                    assert_eq!(*path, store_path(key));
                    assert_ne!(path, key);
                    ("Undeletable", key)
                }
                Err(RepairError::ReservedName { key }) => ("ReservedName", key),
                other => panic!("{other:?}"),
            };
            assert_eq!(named, (kind, &refused));
        }
        // Including reserved names never admits an undeletable key.
        let mut keys = sorted(&RESERVED);
        keys.push(stray_key(&TENANT_A, UNDELETABLE[0]));
        let got = delete_stray(&store, &TENANT_A, &keys, true).await;
        assert!(
            matches!(&got, Err(RepairError::Undeletable { key, .. }) if *key == keys[2]),
            "{got:?}"
        );
        assert!(store.inner.deletes().is_empty());
        assert_eq!(deletes_sent(&store), 0);
        assert_eq!(list_stray(&store, &TENANT_A).await.expect("list").len(), 8);
        assert_eq!(
            unaddressable_under(&store, &tenant_manifest_prefix(&TENANT_A)).await,
            sorted(&UNDELETABLE)
        );
    }

    /// `Path::from` drops the empty segment of `hits//v/<3>.pqm`, so the S3
    /// adapter would send its delete to version 3 of `hits`.
    #[tokio::test]
    async fn delete_stray_refuses_a_key_whose_delete_reaches_a_valid_version() {
        let store = stray_store().await;
        let doubled = stray_key(&TENANT_A, "hits//v/00000000000000000003.pqm");
        memory(&store).insert_foreign(&doubled, Bytes::from_static(b"x"));
        assert!(matches!(
            parse_listed_manifest_key(&doubled),
            Ok(ListedManifestKey::InvalidTable { .. })
        ));
        assert_eq!(store_path(&doubled), mkey(3));
        let got = delete_stray(&store, &TENANT_A, std::slice::from_ref(&doubled), true).await;
        assert!(
            matches!(&got, Err(RepairError::Undeletable { key, path })
                if *key == doubled && *path == mkey(3)),
            "{got:?}"
        );
        assert_eq!(deletes_sent(&store), 0);
        memory(&store).head(&mkey(3)).await.expect("version 3 kept");
        assert_eq!(
            unaddressable_under(memory(&store), &doubled).await,
            [doubled]
        );
    }

    /// A version slot of 20 tildes, which `Path::from` percent-encodes.
    const TILDES: &str = "~~~~~~~~~~~~~~~~~~~~";

    /// A key under `hits`'s own `v/` prefix that `Path::from` encodes is
    /// reported unaddressable by the listing and listed as undeletable.
    /// Named to `delete_flagged` beside the other flagged keys, it is never
    /// sent a delete and is reported, and every other flagged key is still
    /// deleted, each at its own key.
    #[tokio::test]
    async fn delete_flagged_skips_an_unaddressable_key_and_deletes_the_rest() {
        let store = S3KeyStore {
            inner: segment_aligned_forged_store().await,
        };
        let tilde = invalid_key(TILDES);
        memory(&store).insert_foreign(&tilde, Bytes::from_static(b"x"));
        assert_ne!(store_path(&tilde), tilde);
        let listed = list(&store, &TENANT_A, "hits").await.expect("list");
        let marked: Vec<(&str, bool)> = listed
            .iter()
            .filter(|e| e.flagged)
            .map(|e| (e.key.as_str(), e.undeletable))
            .collect();
        assert_eq!(
            marked,
            [
                (invalid_key(ZERO).as_str(), false),
                (mkey(MAX_MANIFEST_VERSION + 1).as_str(), false),
                (mkey(u64::MAX).as_str(), false),
                (invalid_key(OVERFLOW).as_str(), false),
                (invalid_key(NON_DIGIT).as_str(), false),
                (invalid_key(NESTED).as_str(), false),
                (tilde.as_str(), true),
            ]
        );
        let hits_prefix = manifest_prefix(&TENANT_A, "hits").expect("prefix");
        assert_eq!(
            unaddressable_under(&store, &hits_prefix).await,
            std::slice::from_ref(&tilde)
        );
        let rest: Vec<String> = listed
            .into_iter()
            .filter(|e| e.flagged && !e.undeletable)
            .map(|e| e.key)
            .collect();
        let mut flagged = rest.clone();
        flagged.push(tilde.clone());
        let got = delete_flagged(&store, &TENANT_A, "hits", &flagged).await;
        assert_eq!(
            got.expect("skips rather than refusing"),
            FlaggedDeletion {
                deleted: rest.clone(),
                undeletable: vec![tilde.clone()],
            }
        );
        assert_eq!(store.inner.deletes(), rest);
        assert_eq!(deletes_sent(&store), 6);
        let after = list(&store, &TENANT_A, "hits").await.expect("list");
        let flagged_after: Vec<(&str, bool)> = after
            .iter()
            .filter(|e| e.flagged)
            .map(|e| (e.key.as_str(), e.undeletable))
            .collect();
        assert_eq!(flagged_after, [(tilde.as_str(), true)]);
        assert_eq!(unaddressable_under(memory(&store), &tilde).await, [tilde]);
    }

    /// `Path::from` drops the empty segment of `hits/v//<3>.pqm`, a key that
    /// names no version of `hits`, so the S3 adapter would send its delete to
    /// version 3. `delete_flagged` skips it, so version 3 is kept.
    #[tokio::test]
    async fn delete_flagged_skips_a_flagged_key_whose_delete_reaches_a_valid_version() {
        let store = S3KeyStore {
            inner: segment_aligned_forged_store().await,
        };
        let doubled = invalid_key("/00000000000000000003");
        memory(&store).insert_foreign(&doubled, Bytes::from_static(b"x"));
        assert!(classify(&TENANT_A, "hits", &doubled).1);
        assert_eq!(store_path(&doubled), mkey(3));
        let got = delete_flagged(&store, &TENANT_A, "hits", std::slice::from_ref(&doubled)).await;
        assert_eq!(
            got.expect("skips rather than refusing"),
            FlaggedDeletion {
                deleted: Vec::new(),
                undeletable: vec![doubled.clone()],
            }
        );
        assert_eq!(store.inner.deletes(), Vec::<String>::new());
        assert_eq!(deletes_sent(&store), 0);
        memory(&store).head(&mkey(3)).await.expect("version 3 kept");
        // `MemoryStore` lists every key and reports this one unaddressable,
        // so it is listed flagged and undeletable.
        let listed = list(memory(&store), &TENANT_A, "hits").await.expect("list");
        let entry = listed.iter().find(|e| e.key == doubled);
        assert!(
            entry.is_some_and(|e| e.flagged && e.undeletable),
            "{listed:?}"
        );
        assert_eq!(
            unaddressable_under(memory(&store), &doubled).await,
            [doubled]
        );
    }

    /// Reports every page's unaddressable keys twice, as the S3 adapter
    /// lists an unaddressable page tail again on the next page.
    struct TwiceReported<S> {
        inner: S,
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for TwiceReported<S> {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, StoreError> {
            self.inner.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            let mut page = self.inner.list(prefix, page).await?;
            page.unaddressable.extend(page.unaddressable.clone());
            Ok(page)
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// The drain's summary keeps 16 unaddressable keys and counts a key
    /// reported twice in one page twice; `list` and `list_stray` name every
    /// one of 20 such keys exactly once, in key order.
    #[tokio::test]
    async fn every_unaddressable_key_is_listed_once_beyond_the_drain_sample() {
        let store = TwiceReported {
            inner: forged_store().await,
        };
        let tilde_keys: Vec<String> = (0..20)
            .map(|i| invalid_key(&format!("{i:02}{}", &TILDES[2..])))
            .collect();
        let stray_tilde_keys: Vec<String> = (0..20)
            .map(|i| stray_key(&TENANT_A, &format!("Hits~{i:02}/v/{ZERO}.pqm")))
            .collect();
        for key in tilde_keys.iter().chain(&stray_tilde_keys) {
            store.inner.insert_foreign(key, Bytes::from_static(b"x"));
        }
        let hits_prefix = manifest_prefix(&TENANT_A, "hits").expect("prefix");
        let reported = ravel_object_store::list_all_reporting(&store, &hits_prefix)
            .await
            .expect("list");
        assert_eq!(reported.unaddressable.sample.len(), 16);
        assert!(reported.unaddressable.count > 20);

        let undeletable: Vec<String> = list(&store, &TENANT_A, "hits")
            .await
            .expect("list")
            .into_iter()
            .filter(|e| e.undeletable)
            .map(|e| e.key)
            .collect();
        assert_eq!(undeletable, tilde_keys);
        let stray_undeletable: Vec<String> = list_stray(&store, &TENANT_A)
            .await
            .expect("list")
            .into_iter()
            .filter(|e| e.class == StrayClass::Undeletable)
            .map(|e| e.key)
            .collect();
        assert_eq!(stray_undeletable, stray_tilde_keys);
    }

    /// A stray key with a control character, an empty segment or a `.` or
    /// `..` segment is listed unaddressable by the S3 adapter, so
    /// [`list_stray`] names it as undeletable beside the other strays, and
    /// [`delete_stray`] refuses it.
    #[tokio::test]
    async fn a_key_the_s3_adapter_cannot_address_is_listed_as_undeletable() {
        for rest in UNLISTABLE {
            let store = stray_store().await;
            let key = stray_key(&TENANT_A, rest);
            memory(&store).insert_foreign(&key, Bytes::from_static(b"x"));
            let listed = list_stray(&store, &TENANT_A).await.expect("list");
            assert_eq!(
                listed
                    .iter()
                    .filter(|e| e.key == key)
                    .map(|e| e.class)
                    .collect::<Vec<_>>(),
                [StrayClass::Undeletable],
                "{rest:?}: {listed:?}"
            );
            assert!(
                matches!(
                    delete_stray(&store, &TENANT_A, std::slice::from_ref(&key), false).await,
                    Err(RepairError::Undeletable { .. })
                ),
                "{rest:?}"
            );
            assert_eq!(deletes_sent(&store), 0, "{rest:?}");
            assert!(
                unaddressable_under(memory(&store), &tenant_manifest_prefix(&TENANT_A))
                    .await
                    .contains(&key),
                "{rest:?}"
            );
        }
    }

    #[tokio::test]
    async fn the_wrapper_lists_by_whole_segment() {
        let store = segment_aligned_forged_store().await;
        assert!(
            ravel_object_store::list_all(&store, &mkey(2))
                .await
                .expect("list")
                .is_empty()
        );
        assert_eq!(
            list(&store, &TENANT_A, "hits").await.expect("list").len(),
            11
        );
    }

    #[tokio::test]
    async fn delete_version_finds_an_existing_version_on_a_segment_aligned_store() {
        let store = segment_aligned_forged_store().await;
        let before = keys_of(list(&store, &TENANT_A, "hits").await.expect("list"));
        let deleted = delete_version(&store, &TENANT_A, "hits", 2)
            .await
            .expect("delete");
        assert_eq!(deleted, mkey(2));
        assert_eq!(
            store.inner.metrics().snapshot().op(StoreOp::Delete).calls,
            1
        );
        let after = keys_of(list(&store, &TENANT_A, "hits").await.expect("list"));
        let expected: Vec<String> = before.into_iter().filter(|k| *k != mkey(2)).collect();
        assert_eq!(after, expected);
        // Version 1 sorts first and the bound among the highest: both are
        // found too.
        for version in [1, MAX_MANIFEST_VERSION] {
            assert_eq!(
                delete_version(&store, &TENANT_A, "hits", version)
                    .await
                    .expect("delete"),
                mkey(version)
            );
        }
        assert_eq!(
            store.inner.metrics().snapshot().op(StoreOp::Delete).calls,
            3
        );
    }

    #[tokio::test]
    async fn delete_version_of_a_missing_version_on_a_segment_aligned_store_is_not_listed() {
        let store = segment_aligned_forged_store().await;
        // 5 is missing between listed versions; 4294967295 sorts just below
        // the bound, which is listed.
        for version in [5, MAX_MANIFEST_VERSION - 1] {
            let got = delete_version(&store, &TENANT_A, "hits", version).await;
            assert!(
                matches!(&got, Err(RepairError::NotListed { key }) if *key == mkey(version)),
                "{got:?}"
            );
        }
        assert_eq!(
            store.inner.metrics().snapshot().op(StoreOp::Delete).calls,
            0
        );
        assert_eq!(
            list(&store, &TENANT_A, "hits").await.expect("list").len(),
            11
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
