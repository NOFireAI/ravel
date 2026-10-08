//! Resolving a table's newest manifest (ADR-2040 D1, D2). There is no mutable
//! HEAD: a table's state is the highest version under its `v/` prefix. Ravel
//! stores no data objects of its own for a Parquet table, so resolving a table
//! never lists anything but that prefix.
//!
//! A version above [`MAX_MANIFEST_VERSION`] is never a table's newest: no
//! writer creates one, so it was put by something else, and letting it win
//! would hand the table to whoever put it. [`newest`] resolves the highest
//! version at or below the bound instead. A `.pqm` key under a table's `v/`
//! prefix whose slot names no version at all
//! ([`ListedManifestKey::InvalidVersion`]) is treated the same way: every
//! listing here, and the sweep's, skips it rather than failing when the store
//! lists it. The S3 adapter cannot list such a key holding a control
//! character, an empty segment or a `.` or `..` segment: [`versions`], and so
//! every resolve of that table, fails with [`ResolveError::Store`] instead,
//! as do the tenant-wide listings. Each listing that finds either kind is
//! counted, and the table is reported once per process as an
//! [`AboveBoundVersions`] warning.
//!
//! A key the Query grant admits whose segment between `pq/t/` and `/v/` is
//! not a valid table name ([`ListedManifestKey::InvalidTable`]) belongs to no
//! table, so no per-table listing sees it. The tenant-wide listings here and
//! in the sweep skip it the same way when the store lists it, count each
//! listing that finds one per tenant ([`invalid_table_listings`]) and report
//! the tenant once per process as an [`InvalidTableKeys`] warning. The S3
//! adapter cannot list such a key holding a control character, an empty
//! segment or a `.` or `..` segment: the listing fails with
//! [`ResolveError::Store`] instead.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_types::TenantHash;

use crate::keys::{
    KeyError, ListedManifestKey, MAX_MANIFEST_VERSION, VERSION_WIDTH, manifest_key,
    manifest_prefix, parse_listed_manifest_key, tenant_manifest_prefix,
};
use crate::manifest::{Manifest, ManifestError, decode_manifest};

/// A table whose listing holds manifest version keys no reader resolves:
/// versions above [`MAX_MANIFEST_VERSION`], or `.pqm` keys whose slot names no
/// version. Logged once per table per process;
/// [`above_bound_resolves`] counts every listing that saw one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "Parquet table {table:?} of tenant {tenant_hash} has {count} manifest version key(s) above \
     the version bound {bound} or naming no version (highest {highest:?}); they are ignored, \
     and `ravel-cli parquet repair --tenant <tenant> --table {table} --delete` removes them"
)]
pub struct AboveBoundVersions {
    /// The tenant hash, as 32 lowercase hex characters.
    pub tenant_hash: String,
    pub table: String,
    pub count: usize,
    /// The version characters (the text between `v/` and `.pqm`) of the
    /// highest such key in key order, as the key spells them. Chosen by whoever put the key, so print it
    /// escaped.
    pub highest: String,
    pub bound: u64,
}

/// A tenant whose manifest prefix holds keys the Query grant admits under a
/// segment that is not a valid table name
/// ([`ListedManifestKey::InvalidTable`]). Logged once per tenant per process;
/// [`invalid_table_listings`] counts every listing that saw one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "tenant {tenant_hash} has {count} manifest-shaped key(s) whose segment between pq/t/ and \
     /v/ is not a valid table name (first {first:?}); they are ignored, and \
     `ravel-cli parquet repair --tenant <tenant> --stray --delete` removes them"
)]
pub struct InvalidTableKeys {
    /// The tenant hash, as 32 lowercase hex characters.
    pub tenant_hash: String,
    pub count: usize,
    /// The first such key in key order, whole. Chosen by whoever put it, so
    /// print it escaped.
    pub first: String,
}

/// How many (tenant hash, table) pairs [`ABOVE_BOUND`] records. Past it, a
/// listing of a table not yet recorded is not counted, so the map cannot grow
/// with the number of tables someone forges keys under. The warning is then
/// rate limited to one in every [`ABOVE_BOUND_WARN_EVERY`] such listings: a
/// credential forging keys under more than this many table names otherwise
/// turns the once-per-table warning into one per resolve and controls the log
/// volume.
pub const ABOVE_BOUND_TABLES_MAX: usize = 4096;

/// Once [`ABOVE_BOUND`] is full, warn on one in every this many listings of a
/// table it has no room for. Bounds the log volume without hiding the
/// condition, which [`above_bound_resolves`] cannot report for such a table.
pub const ABOVE_BOUND_WARN_EVERY: u64 = 1024;

/// The per-process warning state: listings that saw a version key no reader
/// resolves, per (tenant hash, table), for at most [`ABOVE_BOUND_TABLES_MAX`]
/// pairs, and a saturating count of listings the full map had no room for.
/// `invalid_tables` and `invalid_tables_overflow` are the same per tenant hash
/// for [`ListedManifestKey::InvalidTable`] keys, under the same cap.
struct AboveBoundState {
    seen: BTreeMap<(String, String), u64>,
    overflow: u64,
    invalid_tables: BTreeMap<String, u64>,
    invalid_tables_overflow: u64,
}

static ABOVE_BOUND: Mutex<AboveBoundState> = Mutex::new(AboveBoundState {
    seen: BTreeMap::new(),
    overflow: 0,
    invalid_tables: BTreeMap::new(),
    invalid_tables_overflow: 0,
});

/// Count one listing of `key` in `seen`, which holds at most `cap` entries,
/// and in `overflow` once it is full. Returns true when the caller should log
/// it: the first time `key` is recorded, and on one in every `warn_every`
/// listings of a key the full map has no room for.
fn record_in<K: Ord>(
    seen: &mut BTreeMap<K, u64>,
    overflow: &mut u64,
    key: K,
    cap: usize,
    warn_every: u64,
) -> bool {
    if let Some(count) = seen.get_mut(&key) {
        *count = count.saturating_add(1);
        return false;
    }
    if seen.len() < cap {
        seen.insert(key, 1);
        return true;
    }
    let nth = *overflow;
    *overflow = overflow.saturating_add(1);
    nth.is_multiple_of(warn_every)
}

/// Count and, the first time, log one listing of `table` that found
/// `invalid` (the version characters of its
/// [`ListedManifestKey::InvalidVersion`] keys) or a version in `versions`
/// above [`MAX_MANIFEST_VERSION`]. Nothing when it found neither.
pub(crate) fn note_unresolvable(
    tenant: &TenantHash,
    table: &str,
    versions: &[u64],
    invalid: &[String],
) {
    let mut slots: Vec<String> = versions
        .iter()
        .filter(|&&v| v > MAX_MANIFEST_VERSION)
        .map(|v| format!("{v:0width$}", width = VERSION_WIDTH))
        .collect();
    slots.extend(invalid.iter().cloned());
    let Some(highest) = slots.iter().max().cloned() else {
        return;
    };
    let warning = AboveBoundVersions {
        tenant_hash: tenant.to_hex(),
        table: table.to_string(),
        count: slots.len(),
        highest,
        bound: MAX_MANIFEST_VERSION,
    };
    let log = {
        let mut state = ABOVE_BOUND.lock().unwrap_or_else(PoisonError::into_inner);
        let AboveBoundState { seen, overflow, .. } = &mut *state;
        record_in(
            seen,
            overflow,
            (warning.tenant_hash.clone(), warning.table.clone()),
            ABOVE_BOUND_TABLES_MAX,
            ABOVE_BOUND_WARN_EVERY,
        )
    };
    if log {
        tracing::warn!(
            tenant_hash = %warning.tenant_hash,
            table = %warning.table,
            highest = ?warning.highest,
            "{warning}"
        );
    }
}

/// How many listings of `table` in this process found a manifest version key
/// no reader resolves: a version above [`MAX_MANIFEST_VERSION`], or a key
/// whose slot names no version. Zero for a table first seen after
/// [`ABOVE_BOUND_TABLES_MAX`] others were recorded.
pub fn above_bound_resolves(tenant: &TenantHash, table: &str) -> u64 {
    let state = ABOVE_BOUND.lock().unwrap_or_else(PoisonError::into_inner);
    state
        .seen
        .get(&(tenant.to_hex(), table.to_string()))
        .copied()
        .unwrap_or(0)
}

/// Count and, the first time for `tenant`, log one tenant-wide listing that
/// found `keys` ([`ListedManifestKey::InvalidTable`] keys, ascending).
/// Nothing when `keys` is empty.
pub(crate) fn note_invalid_tables(tenant: &TenantHash, keys: &[String]) {
    let Some(first) = keys.first() else {
        return;
    };
    let warning = InvalidTableKeys {
        tenant_hash: tenant.to_hex(),
        count: keys.len(),
        first: first.clone(),
    };
    let log = {
        let mut state = ABOVE_BOUND.lock().unwrap_or_else(PoisonError::into_inner);
        let AboveBoundState {
            invalid_tables,
            invalid_tables_overflow,
            ..
        } = &mut *state;
        record_in(
            invalid_tables,
            invalid_tables_overflow,
            warning.tenant_hash.clone(),
            ABOVE_BOUND_TABLES_MAX,
            ABOVE_BOUND_WARN_EVERY,
        )
    };
    if log {
        tracing::warn!(
            tenant_hash = %warning.tenant_hash,
            first = ?warning.first,
            "{warning}"
        );
    }
}

/// How many tenant-wide listings of `tenant` in this process found a key the
/// Query grant admits under a segment that is not a valid table name
/// ([`ListedManifestKey::InvalidTable`]). Zero for a tenant first seen after
/// [`ABOVE_BOUND_TABLES_MAX`] others were recorded.
pub fn invalid_table_listings(tenant: &TenantHash) -> u64 {
    let state = ABOVE_BOUND.lock().unwrap_or_else(PoisonError::into_inner);
    state
        .invalid_tables
        .get(&tenant.to_hex())
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

/// What one listing found under a table's `v/` prefix.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableListing {
    /// Every manifest version, ascending, including any above
    /// [`MAX_MANIFEST_VERSION`].
    pub versions: Vec<u64>,
    /// The `.pqm` keys whose slot names no version
    /// ([`ListedManifestKey::InvalidVersion`]), ascending. No reader resolves
    /// them.
    pub invalid_keys: Vec<String>,
}

/// What one listing of a tenant's whole manifest prefix found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantListing {
    /// Every table with at least one key under its `v/` prefix.
    pub tables: BTreeMap<String, TableListing>,
    /// The keys whose segment between `pq/t/` and `/v/` is not a valid table
    /// name ([`ListedManifestKey::InvalidTable`]), whole and ascending. No
    /// reader resolves them and no table owns them.
    pub invalid_table_keys: Vec<String>,
}

/// Every key under `prefix`, grouped by table. `only_table` is the table a
/// per-table prefix belongs to, which every key must name. Each table with a
/// key no reader resolves is passed to [`note_unresolvable`], and the keys
/// under no valid table, which only a tenant-wide listing can see, to
/// [`note_invalid_tables`].
async fn list_grouped(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    prefix: &str,
    only_table: Option<&str>,
) -> Result<TenantListing, ResolveError> {
    let listed = list_all(store, prefix)
        .await
        .map_err(|e| store_error(prefix, e))?;
    let foreign = |key: String, reason: String| ResolveError::ForeignKey {
        key,
        prefix: prefix.to_string(),
        reason,
    };
    let mut out: BTreeMap<String, (TableListing, Vec<String>)> = BTreeMap::new();
    let mut invalid_table_keys = Vec::new();
    for meta in listed {
        let parsed = parse_listed_manifest_key(&meta.key)
            .map_err(|e| foreign(meta.key.clone(), e.to_string()))?;
        let (tenant_hash, table) = match &parsed {
            ListedManifestKey::Version(p) => (p.tenant_hash, p.table.as_str()),
            ListedManifestKey::InvalidVersion {
                tenant_hash, table, ..
            } => (*tenant_hash, table.as_str()),
            ListedManifestKey::InvalidTable { tenant_hash, .. } => {
                if *tenant_hash != *tenant || only_table.is_some() {
                    return Err(foreign(
                        meta.key,
                        "belongs to another tenant or table".into(),
                    ));
                }
                invalid_table_keys.push(meta.key);
                continue;
            }
        };
        if tenant_hash != *tenant || only_table.is_some_and(|t| t != table) {
            return Err(foreign(
                meta.key,
                "belongs to another tenant or table".into(),
            ));
        }
        let (listing, slots) = out.entry(table.to_string()).or_default();
        match parsed {
            ListedManifestKey::Version(p) => listing.versions.push(p.version),
            ListedManifestKey::InvalidVersion { slot, .. } => {
                listing.invalid_keys.push(meta.key);
                slots.push(slot);
            }
            ListedManifestKey::InvalidTable { .. } => {}
        }
    }
    invalid_table_keys.sort_unstable();
    invalid_table_keys.dedup();
    note_invalid_tables(tenant, &invalid_table_keys);
    let tables = out
        .into_iter()
        .map(|(table, (mut listing, mut slots))| {
            slots.sort_unstable();
            slots.dedup();
            listing.versions.sort_unstable();
            listing.versions.dedup();
            listing.invalid_keys.sort_unstable();
            listing.invalid_keys.dedup();
            note_unresolvable(tenant, &table, &listing.versions, &slots);
            (table, listing)
        })
        .collect();
    Ok(TenantListing {
        tables,
        invalid_table_keys,
    })
}

/// Every version number of `table`, ascending, from a paginated LIST of its
/// `v/` prefix, including any above [`MAX_MANIFEST_VERSION`]. A `.pqm` key
/// whose slot names no version ([`ListedManifestKey::InvalidVersion`]) is
/// skipped and counted ([`above_bound_resolves`]); any other key that is not a
/// version of this table is [`ResolveError::ForeignKey`]. On S3 such a key
/// holding a control character, an empty segment or a `.` or `..` segment
/// fails the listing with [`ResolveError::Store`].
pub async fn versions(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
) -> Result<Vec<u64>, ResolveError> {
    let prefix = manifest_prefix(tenant, table)?;
    let mut grouped = list_grouped(store, tenant, &prefix, Some(table)).await?;
    Ok(grouped.tables.remove(table).unwrap_or_default().versions)
}

/// Every table of `tenant` with at least one key under its `v/` prefix, what
/// that prefix holds, and the keys under no valid table.
///
/// One LIST of `t/<tenant_hash>/pq/t/` answers for every table, so an
/// inspection of a whole tenant costs the same listing a sweep does rather
/// than one per table. Keys whose slot names no version are skipped and
/// counted as in [`versions`]. Keys the Query grant admits under a segment
/// that is not a valid table name ([`ListedManifestKey::InvalidTable`]) are
/// skipped, and the listing is counted per tenant
/// ([`invalid_table_listings`]). Any other key under that prefix that is not
/// a manifest key is [`ResolveError::ForeignKey`]. On S3 a key under that
/// prefix holding a control character, an empty segment or a `.` or `..`
/// segment fails the listing with [`ResolveError::Store`].
pub async fn tenant_listing(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<TenantListing, ResolveError> {
    list_grouped(store, tenant, &tenant_manifest_prefix(tenant), None).await
}

/// The tables of [`tenant_listing`].
pub async fn table_listings(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<BTreeMap<String, TableListing>, ResolveError> {
    Ok(tenant_listing(store, tenant).await?.tables)
}

/// Every table of `tenant` that has at least one manifest version, with that
/// table's version numbers ascending: [`table_listings`] without the keys that
/// name no version.
pub async fn tables(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<BTreeMap<String, Vec<u64>>, ResolveError> {
    Ok(table_listings(store, tenant)
        .await?
        .into_iter()
        .filter(|(_, listing)| !listing.versions.is_empty())
        .map(|(table, listing)| (table, listing.versions))
        .collect())
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
/// Versions above the bound, and keys whose slot names no version,
/// are not read. Each listing that holds one is counted
/// ([`above_bound_resolves`]), and the first in this process for the table is
/// logged at `warn` as an [`AboveBoundVersions`].
pub async fn newest(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    table: &str,
) -> Result<Option<Manifest>, ResolveError> {
    for _ in 0..MAX_RESOLVE_ATTEMPTS {
        let listed = versions(store, tenant, table).await?;
        let (bounded, _above) = split_at_bound(&listed);
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
pub(crate) mod tests {
    use bytes::Bytes;
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::manifest::encode_manifest;
    use crate::test_util::{CountingStore, S3KeyStore, TENANT_A, TENANT_B, live_manifest};

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
    pub(crate) fn capture_logs() -> (
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

    /// The 20 version characters of a `.pqm` key that names no version: more
    /// than `u64::MAX`, zero, and not digits.
    pub(crate) const INVALID_SLOTS: [&str; 3] = [
        "99999999999999999999",
        "00000000000000000000",
        "abcdefghijklmnopqrst",
    ];

    /// Write `t/<tenant>/pq/t/hits/v/<slot>.pqm` with a body that is not a
    /// manifest, so any attempt to read it as one fails. The key is written
    /// raw, as any S3 client could, even when no store operation could.
    pub(crate) async fn put_invalid(store: &MemoryStore, tenant: &TenantHash, slot: &str) {
        let key = format!(
            "{}{slot}.pqm",
            manifest_prefix(tenant, "hits").expect("prefix")
        );
        store.insert_foreign(&key, Bytes::from_static(b"forged"));
    }

    /// How many times the above-bound warning names `slot` in `logs`.
    pub(crate) fn warnings_naming(logs: &Mutex<Vec<u8>>, slot: &str) -> usize {
        let text = String::from_utf8(logs.lock().expect("logs").clone()).expect("utf8");
        text.lines()
            .filter(|l| {
                l.contains("WARN")
                    && l.contains("above the version bound")
                    && l.contains(&format!("{slot:?}"))
            })
            .count()
    }

    #[tokio::test]
    async fn a_key_naming_no_version_is_skipped_by_versions_and_newest_and_counted() {
        for (i, slot) in INVALID_SLOTS.iter().enumerate() {
            let tenant = TenantHash([0x41 + i as u8; 16]);
            let (logs, _guard) = capture_logs();
            let store = MemoryStore::with_page_size(2);
            for v in [1, 2] {
                put_version(&store, &tenant, v).await;
            }
            put_invalid(&store, &tenant, slot).await;
            assert_eq!(
                versions(&store, &tenant, "hits").await.expect("versions"),
                vec![1, 2],
                "{slot}"
            );
            assert_eq!(
                newest(&store, &tenant, "hits").await.expect("resolve"),
                Some(live_manifest("hits", 2, &[2])),
                "{slot}"
            );
            assert_eq!(above_bound_resolves(&tenant, "hits"), 2, "{slot}");
            assert_eq!(warnings_naming(&logs, slot), 1, "{slot}");
        }
    }

    #[tokio::test]
    async fn a_key_naming_no_version_is_skipped_by_the_tenant_listing_and_counted() {
        for (i, slot) in INVALID_SLOTS.iter().enumerate() {
            let tenant = TenantHash([0x44 + i as u8; 16]);
            let (logs, _guard) = capture_logs();
            let store = MemoryStore::with_page_size(2);
            for v in [1, 2] {
                put_version(&store, &tenant, v).await;
            }
            put_invalid(&store, &tenant, slot).await;
            for _ in 0..2 {
                assert_eq!(
                    tables(&store, &tenant).await.expect("tables"),
                    BTreeMap::from([("hits".to_string(), vec![1, 2])]),
                    "{slot}"
                );
            }
            assert_eq!(above_bound_resolves(&tenant, "hits"), 2, "{slot}");
            assert_eq!(warnings_naming(&logs, slot), 1, "{slot}");
            let listings = table_listings(&store, &tenant).await.expect("listings");
            assert_eq!(
                listings.get("hits").map(|l| l.invalid_keys.clone()),
                Some(vec![format!(
                    "{}{slot}.pqm",
                    manifest_prefix(&tenant, "hits").expect("prefix")
                )])
            );
        }
    }

    #[tokio::test]
    async fn a_nested_key_under_a_table_v_prefix_is_skipped_by_every_listing() {
        // The Query grant's `*` binds `hits/v/q`, so this key sits under the
        // table's own `v/` prefix with an extra segment before a second `v/`.
        // Before #2430 it failed `versions`, `newest` and the tenant listing.
        const NESTED: &str = "q/v/00000000000000000001";
        const TENANT: TenantHash = TenantHash([0x5a; 16]);
        let (logs, _guard) = capture_logs();
        let store = MemoryStore::with_page_size(2);
        for v in [1, 2] {
            put_version(&store, &TENANT, v).await;
        }
        put_invalid(&store, &TENANT, NESTED).await;
        // The per-table listing yields the legitimate newest with no error.
        assert_eq!(
            versions(&store, &TENANT, "hits").await.expect("versions"),
            vec![1, 2]
        );
        assert_eq!(
            newest(&store, &TENANT, "hits").await.expect("resolve"),
            Some(live_manifest("hits", 2, &[2]))
        );
        // And so does the tenant-wide listing.
        assert_eq!(
            tables(&store, &TENANT).await.expect("tables"),
            BTreeMap::from([("hits".to_string(), vec![1, 2])])
        );
        assert_eq!(above_bound_resolves(&TENANT, "hits"), 3);
        assert_eq!(warnings_naming(&logs, NESTED), 1);
    }

    /// The text after `t/<tenant_hash>/pq/t/` of a key the Query grant admits
    /// under a segment that is not a valid table name: upper case, a reserved
    /// name, and a nested path.
    pub(crate) const STRAY_SHAPES: [&str; 3] = [
        "Hits/v/00000000000000000001.pqm",
        "logs/v/00000000000000000001.pqm",
        "a/b/v/00000000000000000001.pqm",
    ];

    /// Keys of the same kind that the S3 adapter cannot list, because
    /// `object_store`'s `Path::parse` refuses them: control characters, an
    /// empty segment, a `.` and a `..` segment, and an empty table segment.
    pub(crate) const UNLISTABLE_SHAPES: [&str; 5] = [
        "Hits/v/\u{1b}[2J\u{7}xxxxxxxxxxxxxxx.pqm",
        "hits//v/00000000000000000003.pqm",
        "./v/00000000000000000001.pqm",
        "a/../v/00000000000000000001.pqm",
        "/v/00000000000000000001.pqm",
    ];

    /// Whether `err` is the S3 adapter's error for a listed key
    /// `object_store` could not parse.
    pub(crate) fn is_unparsed_listing(err: &StoreError) -> bool {
        matches!(err, StoreError::Permanent(msg) if msg.starts_with("invalid path"))
    }

    /// Write `t/<tenant>/pq/t/<rest>` raw with a body that is not a
    /// manifest, and return the key.
    pub(crate) async fn put_stray(store: &MemoryStore, tenant: &TenantHash, rest: &str) -> String {
        let key = format!("{}{rest}", tenant_manifest_prefix(tenant));
        store.insert_foreign(&key, Bytes::from_static(b"forged"));
        key
    }

    /// How many times the invalid-table warning names `key`, escaped, in
    /// `logs`.
    pub(crate) fn stray_warnings_naming(logs: &Mutex<Vec<u8>>, key: &str) -> usize {
        let text = String::from_utf8(logs.lock().expect("logs").clone()).expect("utf8");
        text.lines()
            .filter(|l| {
                l.contains("WARN")
                    && l.contains("is not a valid table name")
                    && l.contains(&format!("{key:?}"))
            })
            .count()
    }

    #[tokio::test]
    async fn a_key_under_an_invalid_table_segment_is_skipped_by_the_tenant_listing() {
        for (i, rest) in STRAY_SHAPES.iter().enumerate() {
            let tenant = TenantHash([0x60 + i as u8; 16]);
            let (logs, _guard) = capture_logs();
            let store = S3KeyStore {
                inner: MemoryStore::with_page_size(2),
            };
            for v in [1, 2] {
                put_version(&store.inner, &tenant, v).await;
            }
            let stray = put_stray(&store.inner, &tenant, rest).await;
            for _ in 0..2 {
                assert_eq!(
                    tables(&store, &tenant).await.expect("tables"),
                    BTreeMap::from([("hits".to_string(), vec![1, 2])]),
                    "{rest}"
                );
            }
            assert_eq!(invalid_table_listings(&tenant), 2, "{rest}");
            assert_eq!(stray_warnings_naming(&logs, &stray), 1, "{rest}");
            // The healthy table has nothing of its own to report.
            assert_eq!(above_bound_resolves(&tenant, "hits"), 0, "{rest}");
            let listing = tenant_listing(&store, &tenant).await.expect("listing");
            assert_eq!(listing.invalid_table_keys, vec![stray], "{rest}");
            assert_eq!(listing.tables.len(), 1, "{rest}");
            // A named valid table never lists the stray key.
            assert_eq!(
                versions(&store, &tenant, "hits").await.expect("versions"),
                vec![1, 2],
                "{rest}"
            );
            assert_eq!(invalid_table_listings(&tenant), 3, "{rest}");
        }
    }

    /// The counter counts listings, not keys: one listing that finds two
    /// stray keys adds one, and its warning counts both.
    #[tokio::test]
    async fn the_invalid_table_counter_counts_listings_not_keys() {
        const TENANT: TenantHash = TenantHash([0x78; 16]);
        let (logs, _guard) = capture_logs();
        let store = S3KeyStore {
            inner: MemoryStore::new(),
        };
        put_version(&store.inner, &TENANT, 1).await;
        let first = put_stray(&store.inner, &TENANT, STRAY_SHAPES[0]).await;
        put_stray(&store.inner, &TENANT, STRAY_SHAPES[2]).await;
        let listing = tenant_listing(&store, &TENANT).await.expect("listing");
        assert_eq!(listing.invalid_table_keys.len(), 2);
        assert_eq!(invalid_table_listings(&TENANT), 1);
        tenant_listing(&store, &TENANT).await.expect("listing");
        assert_eq!(invalid_table_listings(&TENANT), 2);
        let text = String::from_utf8(logs.lock().expect("logs").clone()).expect("utf8");
        assert_eq!(stray_warnings_naming(&logs, &first), 1, "{text}");
        assert!(text.contains("has 2 manifest-shaped key(s)"), "{text}");
    }

    /// A stray key S3 lists can hold characters a terminal would act on or
    /// misread. `Path::from` encodes each of them, so the listing reports the
    /// key unaddressable rather than under an invalid table, and its warning
    /// prints it escaped.
    #[tokio::test]
    async fn an_unaddressable_stray_key_is_warned_about_escaped_and_not_as_an_invalid_table() {
        const TENANT: TenantHash = TenantHash([0x66; 16]);
        let rest = "Hits/v/\"\\xxxxxxxxxxxxxxxxxx.pqm";
        let (logs, _guard) = capture_logs();
        let store = S3KeyStore {
            inner: MemoryStore::new(),
        };
        put_version(&store.inner, &TENANT, 1).await;
        let stray = put_stray(&store.inner, &TENANT, rest).await;
        assert_eq!(
            tables(&store, &TENANT).await.expect("tables"),
            BTreeMap::from([("hits".to_string(), vec![1])])
        );
        let listing = tenant_listing(&store, &TENANT).await.expect("listing");
        assert!(listing.invalid_table_keys.is_empty());
        assert_eq!(invalid_table_listings(&TENANT), 0);
        let text = String::from_utf8(logs.lock().expect("logs").clone()).expect("utf8");
        assert!(!text.contains("v/\""), "{text}");
        assert_eq!(
            text.lines()
                .filter(|l| l.contains("WARN")
                    && l.contains("skipped key")
                    && l.contains(&format!("{stray:?}")))
                .count(),
            1,
            "{text}"
        );
        assert!(text.contains("v/\\\"\\\\x"), "{text}");
        assert_eq!(stray_warnings_naming(&logs, &stray), 0, "{text}");
    }

    /// On S3 a stray key `Path::parse` refuses fails the tenant-wide listing
    /// with the store error; `MemoryStore` reports it unaddressable.
    #[tokio::test]
    async fn a_stray_key_the_s3_adapter_cannot_list_fails_the_tenant_listing() {
        for (i, rest) in UNLISTABLE_SHAPES.iter().enumerate() {
            let tenant = TenantHash([0x70 + i as u8; 16]);
            let store = S3KeyStore {
                inner: MemoryStore::with_page_size(2),
            };
            for v in [1, 2] {
                put_version(&store.inner, &tenant, v).await;
            }
            let stray = put_stray(&store.inner, &tenant, rest).await;
            for got in [
                tables(&store, &tenant).await.map(drop),
                tenant_listing(&store, &tenant).await.map(drop),
            ] {
                assert!(
                    matches!(&got, Err(ResolveError::Store { key, source })
                        if *key == tenant_manifest_prefix(&tenant)
                            && is_unparsed_listing(source)),
                    "{rest:?}: {got:?}"
                );
            }
            assert_eq!(invalid_table_listings(&tenant), 0, "{rest:?}");
            // The table's own listing never meets it.
            assert_eq!(
                versions(&store, &tenant, "hits").await.expect("versions"),
                vec![1, 2],
                "{rest:?}"
            );
            let listing = tenant_listing(&store.inner, &tenant)
                .await
                .expect("listing");
            assert!(listing.invalid_table_keys.is_empty(), "{rest:?}");
            let reported = ravel_object_store::list_all_reporting(
                &store.inner,
                &tenant_manifest_prefix(&tenant),
            )
            .await
            .expect("list")
            .unaddressable
            .sample;
            assert_eq!(
                reported.iter().map(|k| &k.key).collect::<Vec<_>>(),
                [&stray],
                "{rest:?}"
            );
        }
    }

    /// On S3 a key under the table's own `v/` prefix whose slot names no
    /// version and that `Path::parse` refuses fails every listing that meets
    /// it, the table's own included; `MemoryStore` reports it unaddressable
    /// and skips it.
    #[tokio::test]
    async fn a_key_naming_no_version_the_s3_adapter_cannot_list_fails_every_listing() {
        for (i, slot) in [
            "\u{1b}[2J\u{7}xxxxxxxxxxxxxxx",
            "/00000000000000000003",
            "./00000000000000000001",
            "../00000000000000000001",
        ]
        .into_iter()
        .enumerate()
        {
            let tenant = TenantHash([0x90 + i as u8; 16]);
            let store = S3KeyStore {
                inner: MemoryStore::with_page_size(2),
            };
            for v in [1, 2] {
                put_version(&store.inner, &tenant, v).await;
            }
            put_invalid(&store.inner, &tenant, slot).await;
            let prefix = manifest_prefix(&tenant, "hits").expect("prefix");
            let got = versions(&store, &tenant, "hits").await;
            assert!(
                matches!(&got, Err(ResolveError::Store { key, source })
                    if *key == prefix && is_unparsed_listing(source)),
                "{slot:?}: {got:?}"
            );
            assert!(
                matches!(
                    newest(&store, &tenant, "hits").await,
                    Err(ResolveError::Store { .. })
                ),
                "{slot:?}"
            );
            assert!(
                matches!(
                    tables(&store, &tenant).await,
                    Err(ResolveError::Store { .. })
                ),
                "{slot:?}"
            );
            assert_eq!(
                versions(&store.inner, &tenant, "hits")
                    .await
                    .expect("versions"),
                vec![1, 2],
                "{slot:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_key_under_an_invalid_table_segment_without_the_suffix_is_still_foreign() {
        for rest in [
            "Hits/v/00000000000000000001.parquet",
            "a/b/v/00000000000000000001.pqm.tmp",
            "Hits/v/00000000000000000001",
            "Hits/v/0000000000000001.txt",
            "logs/notes.txt",
        ] {
            let store = MemoryStore::new();
            put_version(&store, &TENANT_A, 1).await;
            let junk = format!("{}{rest}", tenant_manifest_prefix(&TENANT_A));
            store
                .put(&junk, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
            assert!(
                matches!(
                    tables(&store, &TENANT_A).await,
                    Err(ResolveError::ForeignKey { ref key, .. }) if *key == junk
                ),
                "{rest}"
            );
        }
    }

    #[test]
    fn the_warning_map_stops_recording_at_its_cap_and_still_asks_for_a_warn() {
        let mut seen = BTreeMap::new();
        let mut overflow = 0;
        let key = |t: &str| ("tenant".to_string(), t.to_string());
        // `warn_every` of 1: every overflow listing still warns, isolating
        // the map-growth behaviour from the rate limit tested below.
        assert!(record_in(&mut seen, &mut overflow, key("a"), 2, 1));
        assert!(!record_in(&mut seen, &mut overflow, key("a"), 2, 1));
        assert!(record_in(&mut seen, &mut overflow, key("b"), 2, 1));
        // Full: a new table warns every time and is not recorded.
        assert!(record_in(&mut seen, &mut overflow, key("c"), 2, 1));
        assert!(record_in(&mut seen, &mut overflow, key("c"), 2, 1));
        assert_eq!(seen.len(), 2);
        assert_eq!(seen.get(&key("a")), Some(&2));
        assert_eq!(seen.get(&key("c")), None);
        // A recorded table keeps counting.
        assert!(!record_in(&mut seen, &mut overflow, key("b"), 2, 1));
        assert_eq!(seen.get(&key("b")), Some(&2));
        assert_eq!(overflow, 2, "both overflow listings of c counted");
    }

    #[test]
    fn a_full_map_rate_limits_the_overflow_warning_and_still_counts_every_listing() {
        let mut seen = BTreeMap::new();
        let mut overflow = 0;
        // A cap of 1, filled once, so every further distinct table takes the
        // overflow path.
        assert!(record_in(
            &mut seen,
            &mut overflow,
            ("t".to_string(), "recorded".to_string()),
            1,
            ABOVE_BOUND_WARN_EVERY
        ));
        // 4096 resolves of distinct unrecorded tables: one warn in every
        // `ABOVE_BOUND_WARN_EVERY`, so at most 4.
        let mut warns = 0;
        for i in 0..4096 {
            if record_in(
                &mut seen,
                &mut overflow,
                ("t".to_string(), format!("forged{i}")),
                1,
                ABOVE_BOUND_WARN_EVERY,
            ) {
                warns += 1;
            }
        }
        assert_eq!(warns, 4, "one warn per {ABOVE_BOUND_WARN_EVERY} of 4096");
        assert_eq!(overflow, 4096, "every overflow listing counted");
        assert_eq!(seen.len(), 1, "the map did not grow past its cap");
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
        // Only a key whose suffix is not `.pqm` directly under `v/` is
        // foreign; a `.pqm` key under the prefix whose slot names no version
        // (any length, nested or not) is skipped.
        let prefix = manifest_prefix(&TENANT_A, "hits").expect("prefix");
        for junk in [
            format!("{prefix}notes.txt"),
            format!("{prefix}00000000000000000001.parquet"),
        ] {
            let store = MemoryStore::new();
            put_version(&store, &TENANT_A, 1).await;
            store
                .put(&junk, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
            assert!(matches!(
                newest(&store, &TENANT_A, "hits").await,
                Err(ResolveError::ForeignKey { ref key, .. }) if *key == junk
            ));
            // The tenant-wide listing makes the same refusal.
            assert!(matches!(
                tables(&store, &TENANT_A).await,
                Err(ResolveError::ForeignKey { ref key, .. }) if *key == junk
            ));
        }
        // A key outside `v/` reaches only the tenant-wide listing.
        let store = MemoryStore::new();
        put_version(&store, &TENANT_A, 1).await;
        let outside = format!(
            "{}hits/x/00000000000000000002.pqm",
            tenant_manifest_prefix(&TENANT_A)
        );
        store
            .put(&outside, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
        assert!(matches!(
            tables(&store, &TENANT_A).await,
            Err(ResolveError::ForeignKey { ref key, .. }) if *key == outside
        ));
        assert_eq!(
            versions(&store, &TENANT_A, "hits").await.expect("versions"),
            vec![1]
        );
    }
}
