//! Resolving a table's newest manifest (ADR-2040 D1, D2). There is no mutable
//! HEAD: a table's state is the highest version under its `v/` prefix. Ravel
//! stores no data objects of its own for a Parquet table, so resolving a table
//! never lists anything but that prefix.
//!
//! A version above [`MAX_MANIFEST_VERSION`] is never a table's newest: no
//! writer creates one, so it was put by something else, and letting it win
//! would hand the table to whoever put it. [`newest`] resolves the highest
//! version at or below the bound instead, and reports the table once per
//! process as an [`AboveBoundVersions`] warning.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, MAX_MANIFEST_VERSION, manifest_key, manifest_prefix, parse_manifest_key,
    tenant_manifest_prefix,
};
use crate::manifest::{Manifest, ManifestError, decode_manifest};

/// A table whose listing holds manifest versions above
/// [`MAX_MANIFEST_VERSION`], which [`newest`] ignored. Logged once per table
/// per process; [`above_bound_resolves`] counts every listing that saw one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "Parquet table {table:?} of tenant {tenant_hash} has {count} manifest version(s) above the \
     version bound {bound} (highest {highest}); they are ignored, and \
     `ravel-cli parquet repair --tenant <tenant> --table {table} --delete` removes them"
)]
pub struct AboveBoundVersions {
    /// The tenant hash, as 32 lowercase hex characters.
    pub tenant_hash: String,
    pub table: String,
    pub count: usize,
    pub highest: u64,
    pub bound: u64,
}

/// Listings that saw a version above the bound, per (tenant hash, table).
static ABOVE_BOUND: Mutex<BTreeMap<(String, String), u64>> = Mutex::new(BTreeMap::new());

/// Count one listing of `warning.table` that saw versions above the bound.
/// Returns true the first time this process records that table, which is when
/// the caller logs it.
fn record_above_bound(warning: &AboveBoundVersions) -> bool {
    let mut seen = ABOVE_BOUND.lock().unwrap_or_else(PoisonError::into_inner);
    let count = seen
        .entry((warning.tenant_hash.clone(), warning.table.clone()))
        .or_insert(0);
    *count = count.saturating_add(1);
    *count == 1
}

/// How many listings of `table` in this process found a manifest version
/// above [`MAX_MANIFEST_VERSION`].
pub fn above_bound_resolves(tenant: &TenantHash, table: &str) -> u64 {
    let seen = ABOVE_BOUND.lock().unwrap_or_else(PoisonError::into_inner);
    seen.get(&(tenant.to_hex(), table.to_string()))
        .copied()
        .unwrap_or(0)
}

/// `versions` (ascending) up to and including [`MAX_MANIFEST_VERSION`], and
/// the rest.
pub fn split_at_bound(versions: &[u64]) -> (&[u64], &[u64]) {
    versions.split_at(versions.partition_point(|&v| v <= MAX_MANIFEST_VERSION))
}

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

impl ResolveError {
    /// True only for a manifest a newer build wrote; see
    /// [`ManifestError::is_newer_format_version`].
    ///
    /// Every variant is named, so a new one fails to compile until it is
    /// classified here. `Key` answers false for the whole wrapped `KeyError`,
    /// which carries no format version; a version-ceiling variant added to it
    /// must be delegated to here.
    pub fn is_newer_format_version(&self) -> bool {
        match self {
            ResolveError::Manifest(err) => err.is_newer_format_version(),
            ResolveError::Store { .. }
            | ResolveError::ForeignKey { .. }
            | ResolveError::Vanished { .. }
            | ResolveError::Key(_) => false,
        }
    }
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

/// The newest manifest version of `table` at or below
/// [`MAX_MANIFEST_VERSION`], or `None` if it has none. A returned manifest may
/// be a dropped one ([`Manifest::is_live`] is false): the caller decides that
/// a dropped table does not exist, and a writer needs the dropped version's
/// number to write the next one.
///
/// Versions above the bound are not read. Each listing that holds one is
/// counted ([`above_bound_resolves`]), and the first in this process for the
/// table is logged at `warn` as an [`AboveBoundVersions`].
pub async fn newest(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
) -> Result<Option<Manifest>, ResolveError> {
    for _ in 0..MAX_RESOLVE_ATTEMPTS {
        let listed = versions(store, tenant, table).await?;
        let (bounded, above) = split_at_bound(&listed);
        if let Some(&highest) = above.last() {
            let warning = AboveBoundVersions {
                tenant_hash: tenant.to_hex(),
                table: table.to_string(),
                count: above.len(),
                highest,
                bound: MAX_MANIFEST_VERSION,
            };
            if record_above_bound(&warning) {
                tracing::warn!(
                    tenant_hash = %warning.tenant_hash,
                    table = %warning.table,
                    highest = warning.highest,
                    "{warning}"
                );
            }
        }
        let Some(&version) = bounded.last() else {
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

    #[test]
    fn only_a_manifest_above_the_ceiling_is_newer() {
        let above = ResolveError::Manifest(ManifestError::UnsupportedVersion {
            key: "k".into(),
            got: 9,
            ceiling: 1,
        });
        assert!(above.is_newer_format_version());
        let below = ResolveError::Manifest(ManifestError::VersionBelowFloor {
            key: "k".into(),
            got: 0,
            floor: 1,
        });
        assert!(!below.is_newer_format_version());
        let vanished = ResolveError::Vanished {
            table: "hits".into(),
            attempts: 3,
        };
        assert!(!vanished.is_newer_format_version());
        let store = ResolveError::Store {
            key: "k".into(),
            source: StoreError::Timeout,
        };
        assert!(!store.is_newer_format_version());
    }

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

    /// Everything the `tracing` events of this thread format, from the moment
    /// the returned guard is set until it drops.
    fn capture_logs() -> (
        std::sync::Arc<Mutex<Vec<u8>>>,
        tracing::subscriber::DefaultGuard,
    ) {
        struct Sink(std::sync::Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let logs = std::sync::Arc::new(Mutex::new(Vec::new()));
        let sink = std::sync::Arc::clone(&logs);
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || Sink(std::sync::Arc::clone(&sink)))
            .finish();
        (logs, tracing::subscriber::set_default(subscriber))
    }

    #[tokio::test]
    async fn a_newest_version_above_the_bound_is_ignored_and_warned_once() {
        // A tenant no other test uses: the warning state is per process.
        const TENANT_C: TenantHash = TenantHash([0xc3; 16]);
        let (logs, _guard) = capture_logs();
        let store = MemoryStore::new();
        for v in [1, 2, MAX_MANIFEST_VERSION + 1] {
            put_version(&store, &TENANT_C, v).await;
        }
        // Not a manifest at all: resolving it would fail to decode, so the
        // resolve below proves the version above the bound is never read.
        store
            .put(
                &manifest_key(&TENANT_C, "hits", u64::MAX).expect("key"),
                Bytes::from_static(b"forged"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put");
        for _ in 0..3 {
            let got = newest(&store, &TENANT_C, "hits").await.expect("resolve");
            assert_eq!(got, Some(live_manifest("hits", 2, &[2])));
        }
        assert_eq!(above_bound_resolves(&TENANT_C, "hits"), 3);
        let text = String::from_utf8(logs.lock().expect("logs").clone()).expect("utf8");
        assert_eq!(text.matches("above the version bound").count(), 1, "{text}");
        assert!(text.contains("WARN"), "{text}");
        assert!(text.contains("\"hits\""), "{text}");
        assert!(text.contains(&u64::MAX.to_string()), "{text}");
        assert!(text.contains(&TENANT_C.to_hex()), "{text}");
    }

    #[tokio::test]
    async fn a_version_exactly_at_the_bound_is_the_newest() {
        const TENANT_D: TenantHash = TenantHash([0xd4; 16]);
        let store = MemoryStore::new();
        for v in [1, MAX_MANIFEST_VERSION] {
            put_version(&store, &TENANT_D, v).await;
        }
        let got = newest(&store, &TENANT_D, "hits").await.expect("resolve");
        assert_eq!(got.map(|m| m.version), Some(MAX_MANIFEST_VERSION));
        assert_eq!(above_bound_resolves(&TENANT_D, "hits"), 0);
    }

    #[tokio::test]
    async fn a_table_with_only_versions_above_the_bound_has_no_newest() {
        const TENANT_E: TenantHash = TenantHash([0xe5; 16]);
        let store = MemoryStore::new();
        put_version(&store, &TENANT_E, u64::MAX).await;
        assert_eq!(
            newest(&store, &TENANT_E, "hits").await.expect("resolve"),
            None
        );
        assert_eq!(above_bound_resolves(&TENANT_E, "hits"), 1);
    }

    #[test]
    fn split_at_bound_keeps_the_bound_itself_below() {
        let listed = [1, MAX_MANIFEST_VERSION, MAX_MANIFEST_VERSION + 1, u64::MAX];
        assert_eq!(
            split_at_bound(&listed),
            (
                &[1, MAX_MANIFEST_VERSION][..],
                &[MAX_MANIFEST_VERSION + 1, u64::MAX][..]
            )
        );
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
        store.delete_after_each_list(
            (1..=MAX_RESOLVE_ATTEMPTS as u64)
                .rev()
                .map(|v| manifest_key(&TENANT_A, "hits", v).expect("key")),
        );
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
        // The tenant-wide listing makes the same refusal.
        assert!(matches!(
            tables(&store, &TENANT_A).await,
            Err(ResolveError::ForeignKey { key, .. }) if key == junk
        ));
    }
}
