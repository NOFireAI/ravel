//! Resolving a table's newest manifest (ADR-2040 D1, D2). There is no mutable
//! HEAD: a table's state is the highest version under its `v/` prefix. Ravel
//! stores no data objects of its own for a Parquet table, so resolving a table
//! never lists anything but that prefix.

use std::collections::BTreeMap;

use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, manifest_key, manifest_prefix, parse_manifest_key, tenant_manifest_prefix,
};
use crate::manifest::{Manifest, ManifestError, decode_manifest};

/// How many times [`newest`] re-lists when the version it listed is gone by
/// the time it is read.
pub const MAX_RESOLVE_ATTEMPTS: usize = 3;

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("object store error on {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    /// A key under a Parquet table prefix that is not the key shape that
    /// prefix holds, or that belongs to another tenant or table.
    #[error("unexpected object {key:?} under {prefix:?}: {reason}")]
    ForeignKey {
        key: String,
        prefix: String,
        reason: String,
    },
    #[error(
        "the newest listed version of {table:?} was deleted before it could be read, \
         {attempts} times in a row"
    )]
    Vanished { table: String, attempts: usize },
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
}

fn store_error(key: &str, source: StoreError) -> ResolveError {
    ResolveError::Store {
        key: key.to_string(),
        source,
    }
}

/// Every version number of `table`, ascending, from a paginated LIST of its
/// `v/` prefix.
pub async fn versions(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
) -> Result<Vec<u64>, ResolveError> {
    let prefix = manifest_prefix(tenant, table)?;
    let listed = list_all(store, &prefix)
        .await
        .map_err(|e| store_error(&prefix, e))?;
    let mut out = Vec::with_capacity(listed.len());
    for meta in listed {
        let parsed = parse_manifest_key(&meta.key).map_err(|e| ResolveError::ForeignKey {
            key: meta.key.clone(),
            prefix: prefix.clone(),
            reason: e.to_string(),
        })?;
        if parsed.tenant_hash != *tenant || parsed.table != table {
            return Err(ResolveError::ForeignKey {
                key: meta.key,
                prefix,
                reason: "belongs to another tenant or table".into(),
            });
        }
        out.push(parsed.version);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// Every table of `tenant` that has at least one manifest version, with that
/// table's version numbers ascending.
///
/// One LIST of `t/<tenant_hash>/pq/t/` answers for every table, so an
/// inspection of a whole tenant costs the same listing a sweep does rather
/// than one per table. A key under that prefix that is not a manifest key is
/// [`ResolveError::ForeignKey`], the same refusal [`versions`] makes.
pub async fn tables(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<BTreeMap<String, Vec<u64>>, ResolveError> {
    let prefix = tenant_manifest_prefix(tenant);
    let listed = list_all(store, &prefix)
        .await
        .map_err(|e| store_error(&prefix, e))?;
    let mut out: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for meta in listed {
        let parsed = parse_manifest_key(&meta.key).map_err(|e| ResolveError::ForeignKey {
            key: meta.key.clone(),
            prefix: prefix.clone(),
            reason: e.to_string(),
        })?;
        if parsed.tenant_hash != *tenant {
            return Err(ResolveError::ForeignKey {
                key: meta.key,
                prefix,
                reason: "belongs to another tenant".into(),
            });
        }
        out.entry(parsed.table).or_default().push(parsed.version);
    }
    for versions in out.values_mut() {
        versions.sort_unstable();
        versions.dedup();
    }
    Ok(out)
}

/// Read and decode one manifest version. `Ok(None)` when it does not exist.
pub async fn read_version(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
    version: u64,
) -> Result<Option<Manifest>, ResolveError> {
    let key = manifest_key(tenant, table, version)?;
    match store.get(&key, GetRange::Full).await {
        Ok(outcome) => Ok(Some(decode_manifest(&key, &outcome.data)?)),
        Err(StoreError::NotFound) => Ok(None),
        Err(e) => Err(store_error(&key, e)),
    }
}

/// The newest manifest version of `table`, or `None` if it has none. A
/// returned manifest may be a dropped one ([`Manifest::is_live`] is false):
/// the caller decides that a dropped table does not exist, and a writer needs
/// the dropped version's number to write the next one.
pub async fn newest(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
) -> Result<Option<Manifest>, ResolveError> {
    for _ in 0..MAX_RESOLVE_ATTEMPTS {
        let Some(&version) = versions(store, tenant, table).await?.last() else {
            return Ok(None);
        };
        if let Some(manifest) = read_version(store, tenant, table, version).await? {
            return Ok(Some(manifest));
        }
    }
    Err(ResolveError::Vanished {
        table: table.to_string(),
        attempts: MAX_RESOLVE_ATTEMPTS,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::manifest::encode_manifest;
    use crate::test_util::{CountingStore, TENANT_A, TENANT_B, live_manifest};

    async fn put_version(store: &MemoryStore, tenant: &TenantHash, version: u64) {
        let m = live_manifest("hits", version, &[version as u8]);
        let key = manifest_key(tenant, "hits", version).expect("key");
        let bytes = encode_manifest(tenant, &m).expect("encode");
        store
            .put(&key, Bytes::from(bytes), PutOptions::create_if_absent())
            .await
            .expect("put");
    }

    #[tokio::test]
    async fn newest_pages_through_every_version_and_reads_the_highest() {
        let store = MemoryStore::with_page_size(2);
        assert_eq!(
            newest(&store, &TENANT_A, "hits").await.expect("resolve"),
            None
        );
        for v in [1, 2, 3, 9, 10, 11, 12] {
            put_version(&store, &TENANT_A, v).await;
        }
        put_version(&store, &TENANT_B, 99).await;
        assert_eq!(
            versions(&store, &TENANT_A, "hits").await.expect("versions"),
            vec![1, 2, 3, 9, 10, 11, 12]
        );
        let got = newest(&store, &TENANT_A, "hits").await.expect("resolve");
        assert_eq!(got, Some(live_manifest("hits", 12, &[12])));
    }

    #[tokio::test]
    async fn a_newest_version_deleted_before_every_read_is_reported_as_vanished() {
        // A sweep deleting the version this resolve just listed, on every
        // attempt. Three versions and three deletions exhaust the bound, so
        // the resolve reports Vanished rather than an empty table.
        let inner = MemoryStore::new();
        for v in [1, 2, 3] {
            put_version(&inner, &TENANT_A, v).await;
        }
        let store = CountingStore::new(inner);
        store.delete_after_each_list((1..=MAX_RESOLVE_ATTEMPTS as u64).rev().map(|v| {
            manifest_key(&TENANT_A, "hits", v).expect("key")
        }));
        let got = newest(&store, &TENANT_A, "hits").await;
        assert!(
            matches!(
                got,
                Err(ResolveError::Vanished { ref table, attempts })
                    if table == "hits" && attempts == MAX_RESOLVE_ATTEMPTS
            ),
            "{got:?}"
        );
        assert_eq!(store.list_count(), MAX_RESOLVE_ATTEMPTS);
    }

    #[tokio::test]
    async fn a_newest_version_that_reappears_within_the_bound_resolves() {
        // Only the first listed newest is deleted, so the second attempt
        // reads version 2 and the resolve succeeds inside the bound.
        let inner = MemoryStore::new();
        for v in [1, 2, 3] {
            put_version(&inner, &TENANT_A, v).await;
        }
        let store = CountingStore::new(inner);
        store.delete_after_each_list([manifest_key(&TENANT_A, "hits", 3).expect("key")]);
        let got = newest(&store, &TENANT_A, "hits").await.expect("resolve");
        assert_eq!(got, Some(live_manifest("hits", 2, &[2])));
        assert_eq!(store.list_count(), 2);
    }

    #[tokio::test]
    async fn tables_groups_every_version_by_table_and_skips_other_tenants() {
        let store = MemoryStore::with_page_size(2);
        assert!(tables(&store, &TENANT_A).await.expect("tables").is_empty());
        for v in [1, 2, 10] {
            put_version(&store, &TENANT_A, v).await;
        }
        for v in [3, 4] {
            let m = live_manifest("clicks", v, &[v as u8]);
            let key = manifest_key(&TENANT_A, "clicks", v).expect("key");
            let bytes = encode_manifest(&TENANT_A, &m).expect("encode");
            store
                .put(&key, Bytes::from(bytes), PutOptions::create_if_absent())
                .await
                .expect("put");
        }
        put_version(&store, &TENANT_B, 99).await;
        let got = tables(&store, &TENANT_A).await.expect("tables");
        assert_eq!(
            got,
            BTreeMap::from([
                ("clicks".to_string(), vec![3, 4]),
                ("hits".to_string(), vec![1, 2, 10]),
            ])
        );
    }

    #[tokio::test]
    async fn a_foreign_key_under_a_table_prefix_is_refused() {
        let store = MemoryStore::new();
        put_version(&store, &TENANT_A, 1).await;
        let junk = format!(
            "{}notes.txt",
            manifest_prefix(&TENANT_A, "hits").expect("prefix")
        );
        store
            .put(&junk, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
        assert!(matches!(
            newest(&store, &TENANT_A, "hits").await,
            Err(ResolveError::ForeignKey { key, .. }) if key == junk
        ));
    }
}
