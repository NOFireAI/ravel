//! Parquet tables in a SQL query (ADR-2040 decisions D3, D4 and D6).
//!
//! A statement whose only tables are Parquet tables of the caller's tenant is
//! resolved here before its session is built: each table's newest live
//! manifest is read from Ravel's own store, every file the manifest names is
//! checked against the tenant's grants as they are now, and a
//! [`ParquetTableProvider`] is built per table. The session that plans the
//! statement then carries a [`SingleStoreRegistry`] answering only the
//! tenant's `ravel-pq://<tenant_hash>/` URL (crate::session).
//!
//! What a server wires in is [`ParquetSources`]: Ravel's store, the external
//! stores its credential profiles reach ([`ExternalStores`]), and the read
//! services the reader shares with the rest of the query path. An executor
//! without it treats every name that is not a signal table exactly as it did
//! before Parquet tables existed. With it but with no profiles configured, a
//! statement naming a Parquet table fails with
//! [`ParquetQueryError::NotConfigured`] after one LIST per name and no GET.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use ravel_object_store::external::{ExternalKind, ExternalProfile, ExternalStore, ProfileError};
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_parquet::{
    MetadataCache, ParquetReadError, ParquetTableError, ParquetTableProvider, ReadServices,
    TenantParquetStore,
};
use ravel_pqtable::grants::{self, GrantsError};
use ravel_pqtable::manifest::Manifest;
use ravel_pqtable::names::validate_table;
use ravel_pqtable::resolve::{self, ResolveError};
use ravel_query::{GetLimiter, PhaseAccounting, ReadCache};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};

use crate::error::{ErrorClass, MSG_CORRUPT, MSG_PLAN, MSG_UNAVAILABLE};

/// Bytes of decoded Parquet footers the process keeps, when the embedder does
/// not choose a bound.
pub const DEFAULT_PARQUET_METADATA_CACHE_BYTES: u64 = 64 << 20;

/// Why opening the store behind one (profile, bucket) failed.
#[derive(Debug, thiserror::Error)]
pub enum ExternalStoreError {
    #[error("no credential profile named {profile:?} is configured")]
    UnknownProfile { profile: String },
    /// The profile's store could not be opened. [`ProfileError`]'s `Display`
    /// names neither a secret nor where one is kept.
    #[error("opening bucket {bucket:?} through profile {profile:?}: {source}")]
    Open {
        profile: String,
        bucket: String,
        #[source]
        source: ProfileError,
    },
    /// The profile reaches `bucket` at the endpoint Ravel's own data bucket is
    /// configured at, and the names match (ADR-2040 D4). No Parquet table reads
    /// Ravel's bucket, whatever wrote the manifest naming the file.
    #[error("bucket {bucket:?} through profile {profile:?} is Ravel's own data bucket")]
    RavelBucket { profile: String, bucket: String },
}

/// Ravel's own data bucket as the server is configured to reach it: the S3
/// endpoint (`None` for AWS's regional endpoint) and the bucket name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RavelBucket {
    pub endpoint: Option<String>,
    pub bucket: String,
}

impl RavelBucket {
    /// Whether `profile` reaching `bucket` names this bucket by
    /// configuration: the same bucket name on the same service. Ravel's bucket
    /// reached under another name is refused when the grant is written, by
    /// the probe of ADR-2040 D1, not here.
    fn is_reached_by(&self, profile: &ExternalProfile, bucket: &str) -> bool {
        if self.bucket != bucket {
            return false;
        }
        let ravel = Service::of(self.endpoint.as_deref());
        match &profile.kind {
            ExternalKind::S3 { endpoint, .. } => Service::of(endpoint.as_deref()) == ravel,
            ExternalKind::Gcs { .. } => ravel == Service::Gcs,
            ExternalKind::Azure { .. } => false,
        }
    }
}

/// The service an S3 endpoint reaches: AWS for no endpoint or any
/// `amazonaws.com` host, GCS's S3 interoperability host, or any other host
/// and port, whatever the scheme and path.
#[derive(Debug, PartialEq, Eq)]
enum Service {
    Aws,
    Gcs,
    Host(String),
}

impl Service {
    fn of(endpoint: Option<&str>) -> Self {
        let Some(endpoint) = endpoint else {
            return Service::Aws;
        };
        let endpoint = endpoint.trim().to_ascii_lowercase();
        let authority = endpoint
            .split_once("://")
            .map_or(endpoint.as_str(), |(_, rest)| rest);
        let host = authority
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default()
            .rsplit('@')
            .next()
            .unwrap_or_default();
        let name = host.split(':').next().unwrap_or_default();
        if name == "amazonaws.com" || name.ends_with(".amazonaws.com") {
            Service::Aws
        } else if name == "storage.googleapis.com" {
            Service::Gcs
        } else {
            Service::Host(host.to_string())
        }
    }
}

/// The read-only stores Parquet files are read through, one per (credential
/// profile, bucket).
pub trait ExternalStores: Send + Sync {
    fn store(
        &self,
        profile: &str,
        bucket: &str,
    ) -> Result<Arc<dyn ObjectStoreBackend>, ExternalStoreError>;
}

/// [`ExternalStores`] over the profiles of a credential profile file: each
/// (profile, bucket) is opened with [`ExternalStore::open`] the first time a
/// query reads it, and the open store is kept for the process. A (profile,
/// bucket) that is Ravel's own data bucket ([`Self::refusing`]) is refused
/// before anything is opened.
pub struct ProfileStores {
    profiles: HashMap<String, ExternalProfile>,
    ravel: Option<RavelBucket>,
    opened: Mutex<HashMap<(String, String), Arc<dyn ObjectStoreBackend>>>,
}

impl ProfileStores {
    pub fn new(profiles: Vec<ExternalProfile>) -> Self {
        ProfileStores {
            profiles: profiles
                .into_iter()
                .map(|profile| (profile.name.clone(), profile))
                .collect(),
            ravel: None,
            opened: Mutex::new(HashMap::new()),
        }
    }

    /// Refuse, with [`ExternalStoreError::RavelBucket`], every (profile,
    /// bucket) that reaches `ravel`.
    pub fn refusing(mut self, ravel: RavelBucket) -> Self {
        self.ravel = Some(ravel);
        self
    }

    /// The configured profile names, sorted.
    pub fn profile_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.profiles.keys().cloned().collect();
        names.sort();
        names
    }
}

impl fmt::Debug for ProfileStores {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProfileStores")
            .field("profiles", &self.profile_names())
            .finish_non_exhaustive()
    }
}

impl ExternalStores for ProfileStores {
    fn store(
        &self,
        profile: &str,
        bucket: &str,
    ) -> Result<Arc<dyn ObjectStoreBackend>, ExternalStoreError> {
        let key = (profile.to_string(), bucket.to_string());
        let mut opened = self.opened.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(store) = opened.get(&key) {
            return Ok(Arc::clone(store));
        }
        let config =
            self.profiles
                .get(profile)
                .ok_or_else(|| ExternalStoreError::UnknownProfile {
                    profile: profile.to_string(),
                })?;
        if self
            .ravel
            .as_ref()
            .is_some_and(|ravel| ravel.is_reached_by(config, bucket))
        {
            return Err(ExternalStoreError::RavelBucket {
                profile: profile.to_string(),
                bucket: bucket.to_string(),
            });
        }
        let store =
            ExternalStore::open(config, bucket).map_err(|source| ExternalStoreError::Open {
                profile: profile.to_string(),
                bucket: bucket.to_string(),
                source,
            })?;
        opened.insert(key, Arc::clone(&store));
        Ok(store)
    }
}

/// [`ExternalStores`] from a fixed map of profile name to store, the store
/// serving every bucket of its profile. The seam an embedder or a test uses to
/// read Parquet files from stores it built itself.
#[derive(Default)]
pub struct ExternalStoreMap {
    stores: HashMap<String, Arc<dyn ObjectStoreBackend>>,
}

impl ExternalStoreMap {
    pub fn new(stores: HashMap<String, Arc<dyn ObjectStoreBackend>>) -> Self {
        ExternalStoreMap { stores }
    }
}

impl ExternalStores for ExternalStoreMap {
    fn store(
        &self,
        profile: &str,
        _bucket: &str,
    ) -> Result<Arc<dyn ObjectStoreBackend>, ExternalStoreError> {
        self.stores
            .get(profile)
            .cloned()
            .ok_or_else(|| ExternalStoreError::UnknownProfile {
                profile: profile.to_string(),
            })
    }
}

/// Everything the executor needs to read Parquet tables.
#[derive(Clone)]
pub struct ParquetSources {
    /// Ravel's own store, which holds the manifests and the grants record.
    ravel: Arc<dyn ObjectStoreBackend>,
    /// `None` when the server has no credential profile file: no Parquet
    /// table can be read.
    external: Option<Arc<dyn ExternalStores>>,
    services: ReadServices,
}

impl ParquetSources {
    /// `limiter` and `cache` are the process-wide GET limiter and read cache
    /// the signal-table fetchers use; `metadata_cache_bytes` bounds the
    /// decoded-footer cache this call creates for the process.
    pub fn new(
        ravel: Arc<dyn ObjectStoreBackend>,
        external: Option<Arc<dyn ExternalStores>>,
        limiter: Arc<GetLimiter>,
        cache: Option<ReadCache>,
        metadata_cache_bytes: u64,
    ) -> Self {
        ParquetSources {
            ravel,
            external,
            services: ReadServices {
                limiter,
                cache,
                metadata: Arc::new(MetadataCache::new(metadata_cache_bytes)),
            },
        }
    }

    /// Whether credential profiles are configured, so a Parquet table can be
    /// read at all.
    pub fn is_configured(&self) -> bool {
        self.external.is_some()
    }

    /// The stores Parquet files are read through; `None` when no credential
    /// profiles are configured.
    pub fn external_stores(&self) -> Option<&Arc<dyn ExternalStores>> {
        self.external.as_ref()
    }

    /// The decoded-footer cache, shared by every query this executor runs.
    pub fn metadata_cache(&self) -> &Arc<MetadataCache> {
        &self.services.metadata
    }

    fn resolve_store(&self, accounting: &QueryAccounting) -> ResolveStore {
        ResolveStore {
            inner: Arc::clone(&self.ravel),
            accounting: accounting.clone(),
        }
    }
}

impl fmt::Debug for ParquetSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetSources")
            .field("configured", &self.is_configured())
            .field("services", &self.services)
            .finish_non_exhaustive()
    }
}

/// The newest live manifest of each Parquet table a statement names, every
/// file of which lay inside one of the tenant's grants when it was resolved.
#[derive(Debug, Clone)]
pub(crate) struct ParquetResolution {
    pub(crate) manifests: Vec<Manifest>,
}

/// The tables of one Parquet session, and the store its registry answers with.
pub struct ParquetSession {
    pub(crate) tables: Vec<(String, Arc<dyn TableProvider>)>,
    pub(crate) store: Arc<TenantParquetStore>,
}

/// Why a statement over Parquet tables could not be answered.
#[derive(Debug, thiserror::Error)]
pub enum ParquetQueryError {
    /// The statement names a Parquet table of this tenant, and the server was
    /// started without a credential profile file, so no Parquet file can be
    /// read.
    #[error(
        "Parquet table {table} cannot be queried: this server has no credential profile file \
         (--parquet-profiles or RAVEL_PARQUET_PROFILES)"
    )]
    NotConfigured { table: String },
    /// A file of the table's newest manifest lies outside every location
    /// granted to the tenant now. Grants are checked when a query resolves the
    /// table, not only when the table was created.
    #[error(
        "Parquet table {table} names a file outside every location currently granted to this \
         tenant"
    )]
    LocationNotGranted { table: String },
    /// The store behind a profile the table's files name could not be opened.
    #[error("Parquet table {table}: {source}")]
    Store {
        table: String,
        #[source]
        source: ExternalStoreError,
    },
    /// The table's manifest could not be resolved.
    #[error("resolving Parquet table {table}: {source}")]
    Resolve {
        table: String,
        #[source]
        source: ResolveError,
    },
    /// The tenant's grants record could not be read.
    #[error("reading the Parquet location grants: {0}")]
    Grants(#[source] GrantsError),
    /// The table's provider could not be built from its manifest.
    #[error(transparent)]
    Table(#[from] ParquetTableError),
    /// A read of one of the table's files failed while the query ran.
    #[error(transparent)]
    Read(#[from] ParquetReadError),
    /// [`crate::SqlRequest::row_window`] names an event-time column, and a
    /// Parquet table has no column Ravel knows to be one.
    #[error(
        "a row window cannot be applied to a Parquet table, which has no event-time column; \
         filter on the table's own columns instead"
    )]
    RowWindowUnsupported,
}

impl ParquetQueryError {
    /// The client-visible class: 400 for a request this surface cannot take,
    /// 422 for a refusal or a permanent state a retry cannot change, 503 for
    /// a storage or integrity fault.
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            ParquetQueryError::RowWindowUnsupported => ErrorClass::BadRequest,
            ParquetQueryError::Read(read)
            | ParquetQueryError::Table(ParquetTableError::Read { source: read, .. }) => {
                match read {
                    ParquetReadError::FileChanged { .. } | ParquetReadError::FileMissing { .. } => {
                        ErrorClass::Unsupported
                    }
                    ParquetReadError::Store { .. }
                    | ParquetReadError::Corrupt { .. }
                    | ParquetReadError::LeaderLost { .. } => ErrorClass::Unavailable,
                }
            }
            ParquetQueryError::Resolve { .. } | ParquetQueryError::Grants(_) => {
                ErrorClass::Unavailable
            }
            ParquetQueryError::NotConfigured { .. }
            | ParquetQueryError::LocationNotGranted { .. }
            | ParquetQueryError::Store { .. }
            | ParquetQueryError::Table(_) => ErrorClass::Unsupported,
        }
    }

    /// The text a client may see. Refusals that name only the caller's own
    /// table, and a changed or missing file of the tenant's own bucket that
    /// the caller must act on, are returned verbatim; storage and decode
    /// detail is not.
    pub(crate) fn client_message(&self) -> String {
        match self {
            ParquetQueryError::NotConfigured { .. }
            | ParquetQueryError::LocationNotGranted { .. }
            | ParquetQueryError::RowWindowUnsupported => self.to_string(),
            ParquetQueryError::Read(read)
            | ParquetQueryError::Table(ParquetTableError::Read { source: read, .. }) => {
                match read {
                    ParquetReadError::FileChanged { .. } | ParquetReadError::FileMissing { .. } => {
                        read.to_string()
                    }
                    ParquetReadError::Corrupt { .. } => MSG_CORRUPT.to_string(),
                    ParquetReadError::Store { .. } | ParquetReadError::LeaderLost { .. } => {
                        MSG_UNAVAILABLE.to_string()
                    }
                }
            }
            ParquetQueryError::Table(
                err @ (ParquetTableError::Dropped { .. }
                | ParquetTableError::NoFiles { .. }
                | ParquetTableError::Option { .. }
                | ParquetTableError::Schema { .. }),
            ) => err.to_string(),
            ParquetQueryError::Table(ParquetTableError::Plan { .. }) => MSG_PLAN.to_string(),
            ParquetQueryError::Store {
                table,
                source: ExternalStoreError::RavelBucket { .. },
            } => format!(
                "Parquet table {table} names a file in Ravel's own data bucket, which no Parquet \
                 table may read"
            ),
            ParquetQueryError::Store { table, .. }
            | ParquetQueryError::Table(ParquetTableError::NoStore { table, .. }) => format!(
                "Parquet table {table} is read through a credential profile this server cannot \
                 open"
            ),
            ParquetQueryError::Resolve {
                source: ResolveError::Store { .. } | ResolveError::Vanished { .. },
                ..
            }
            | ParquetQueryError::Grants(GrantsError::Store { .. }) => MSG_UNAVAILABLE.to_string(),
            ParquetQueryError::Resolve { .. } | ParquetQueryError::Grants(_) => {
                MSG_CORRUPT.to_string()
            }
        }
    }
}

/// Every name in `names` that has at least one manifest version for `tenant`,
/// from one LIST per valid table name and no GET.
pub(crate) async fn names_with_versions(
    sources: &ParquetSources,
    tenant: &TenantHash,
    names: &BTreeSet<String>,
    accounting: &QueryAccounting,
) -> Result<Vec<String>, ParquetQueryError> {
    let store = sources.resolve_store(accounting);
    let mut found = Vec::new();
    for name in names {
        if validate_table(name).is_err() {
            continue;
        }
        let versions = resolve::versions(&store, tenant, name)
            .await
            .map_err(|source| ParquetQueryError::Resolve {
                table: name.clone(),
                source,
            })?;
        if !versions.is_empty() {
            found.push(name.clone());
        }
    }
    Ok(found)
}

/// Resolve the Parquet tables among `names`: the newest live manifest of each,
/// checked against the tenant's current grants. `None` when no name is a live
/// Parquet table of `tenant`.
///
/// Every file must lie inside a grant that exists now under the profile the
/// file is read through; the grant the manifest recorded is not consulted.
pub(crate) async fn resolve_tables(
    sources: &ParquetSources,
    tenant: &TenantHash,
    names: &BTreeSet<String>,
    accounting: &QueryAccounting,
) -> Result<Option<ParquetResolution>, ParquetQueryError> {
    let store = sources.resolve_store(accounting);
    let mut manifests = Vec::new();
    for name in names {
        if validate_table(name).is_err() {
            continue;
        }
        let newest = resolve::newest(&store, tenant, name)
            .await
            .map_err(|source| ParquetQueryError::Resolve {
                table: name.clone(),
                source,
            })?;
        if let Some(manifest) = newest.filter(Manifest::is_live) {
            manifests.push(manifest);
        }
    }
    let Some(first) = manifests.first() else {
        return Ok(None);
    };
    if !sources.is_configured() {
        return Err(ParquetQueryError::NotConfigured {
            table: first.table.clone(),
        });
    }
    let granted = grants::list(&store, tenant)
        .await
        .map_err(ParquetQueryError::Grants)?;
    for manifest in &manifests {
        let outside = manifest.files.iter().any(|file| {
            !granted
                .iter()
                .any(|grant| grants::contains_key(grant, &file.profile, &file.bucket, &file.key))
        });
        if outside {
            return Err(ParquetQueryError::LocationNotGranted {
                table: manifest.table.clone(),
            });
        }
    }
    Ok(Some(ParquetResolution { manifests }))
}

/// Build one provider per resolved table, reading each table's first footer
/// through the reader (charged to Probe), and the store the session's
/// registry answers with. `parallel` is ADR-2040 D6's file grouping.
pub(crate) async fn build_tables(
    sources: &ParquetSources,
    tenant: TenantHash,
    resolution: &ParquetResolution,
    accounting: &PhaseAccounting,
    parallel: bool,
) -> Result<Vec<(String, ParquetTableProvider)>, ParquetQueryError> {
    let Some(external) = &sources.external else {
        return Err(ParquetQueryError::NotConfigured {
            table: resolution
                .manifests
                .first()
                .map(|manifest| manifest.table.clone())
                .unwrap_or_default(),
        });
    };
    let mut tables = Vec::with_capacity(resolution.manifests.len());
    for manifest in &resolution.manifests {
        let mut stores: HashMap<(String, String), Arc<dyn ObjectStoreBackend>> = HashMap::new();
        for file in &manifest.files {
            let key = (file.profile.clone(), file.bucket.clone());
            if stores.contains_key(&key) {
                continue;
            }
            let store = external
                .store(&file.profile, &file.bucket)
                .map_err(|source| ParquetQueryError::Store {
                    table: manifest.table.clone(),
                    source,
                })?;
            stores.insert(key, store);
        }
        let provider = ParquetTableProvider::try_new(
            tenant,
            manifest,
            &stores,
            sources.services.clone(),
            accounting.clone(),
            parallel,
        )
        .await?;
        tables.push((manifest.table.clone(), provider));
    }
    Ok(tables)
}

/// The session for `tables`: each registered under its own name, and a
/// [`TenantParquetStore`] that knows every file of every manifest.
pub(crate) fn session_tables(
    tenant: TenantHash,
    resolution: &ParquetResolution,
    tables: Vec<(String, ParquetTableProvider)>,
) -> ParquetSession {
    let store = Arc::new(TenantParquetStore::new(tenant));
    for manifest in &resolution.manifests {
        store.add_manifest(manifest);
    }
    ParquetSession {
        tables: tables
            .into_iter()
            .map(|(name, provider)| (name, Arc::new(provider) as Arc<dyn TableProvider>))
            .collect(),
        store,
    }
}

/// The schemas of `tables`, for the classification plan.
pub(crate) fn schemas(tables: &[(String, ParquetTableProvider)]) -> Vec<(String, SchemaRef)> {
    tables
        .iter()
        .map(|(name, provider)| (name.clone(), provider.schema()))
        .collect()
}

/// The [`ParquetReadError`] somewhere in `err`'s source chain: a Parquet read
/// fails inside DataFusion's scan, which wraps it in parquet's and its own
/// error types.
pub(crate) fn read_error(err: &DataFusionError) -> Option<ParquetReadError> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(error) = current {
        if let Some(found) = error.downcast_ref::<ParquetReadError>() {
            return Some(found.clone());
        }
        current = error.source();
    }
    None
}

/// Ravel's own store as the Parquet resolve reads it: every LIST, GET and HEAD
/// is charged to the resolve phase's accounting. It never writes.
struct ResolveStore {
    inner: Arc<dyn ObjectStoreBackend>,
    accounting: QueryAccounting,
}

impl ResolveStore {
    fn refuse<T>(&self, operation: &str) -> Result<T, StoreError> {
        Err(StoreError::ReadOnly {
            operation: operation.to_string(),
            store: "the Parquet table resolve".to_string(),
        })
    }
}

#[async_trait]
impl ObjectStoreBackend for ResolveStore {
    async fn put(
        &self,
        key: &str,
        _data: Bytes,
        _opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.refuse(&format!("put of {key}"))
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.accounting.record_s3_request(AccountedOp::Get);
        let outcome = self.inner.get(key, range).await?;
        self.accounting
            .add_s3_bytes(AccountedOp::Get, outcome.data.len() as u64);
        Ok(outcome)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.accounting.record_s3_request(AccountedOp::Head);
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.accounting.record_s3_request(AccountedOp::List);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.accounting.record_s3_request(AccountedOp::List);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.refuse(&format!("delete of {key}"))
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// A static-key S3 profile reaching `endpoint`, its keys read from files
    /// under `dir` when `readable`, from a missing file otherwise.
    fn s3_profile(
        name: &str,
        endpoint: Option<&str>,
        dir: &std::path::Path,
        readable: bool,
    ) -> ExternalProfile {
        let key = dir.join(if readable { "key" } else { "missing" });
        if readable {
            std::fs::write(&key, "test-key\n").expect("write key");
        }
        let endpoint = endpoint.map_or("null".to_string(), |e| format!("{e:?}"));
        let json = format!(
            r#"[{{"name": {name:?}, "kind": "s3", "region": "us-east-1", "endpoint": {endpoint},
                 "allow_http": true, "force_path_style": true,
                 "credentials": {{"mode": "static",
                   "access_key_id": {{"from": "file", "path": {key:?}}},
                   "secret_access_key": {{"from": "file", "path": {key:?}}}}}}}]"#
        );
        ravel_object_store::external::load_profiles(&json)
            .expect("profile")
            .remove(0)
    }

    /// `ProfileStores` opens one store per (profile, bucket), the first time
    /// it is asked, and hands the same store back on every later request.
    #[tokio::test]
    async fn one_store_per_profile_and_bucket_reused_on_the_next_read() {
        let dir = tempfile::tempdir().expect("temp dir");
        let stores = ProfileStores::new(vec![
            s3_profile("lake", Some("http://127.0.0.1:9"), dir.path(), true),
            s3_profile("other", Some("http://127.0.0.1:9"), dir.path(), true),
        ]);
        let first = stores.store("lake", "a").expect("opens");
        let again = stores.store("lake", "a").expect("reused");
        assert!(
            Arc::ptr_eq(&first, &again),
            "the second read reuses the store"
        );
        let bucket = stores.store("lake", "b").expect("opens");
        assert!(
            !Arc::ptr_eq(&first, &bucket),
            "another bucket, another store"
        );
        let profile = stores.store("other", "a").expect("opens");
        assert!(
            !Arc::ptr_eq(&first, &profile),
            "another profile, another store"
        );
        assert!(matches!(
            stores.store("nobody", "a"),
            Err(ExternalStoreError::UnknownProfile { profile }) if profile == "nobody"
        ));
    }

    /// A profile whose secret cannot be read fails with a typed error that
    /// names the kind of source and not its path. The failure is not kept:
    /// once the secret file exists, the next request opens the store.
    #[tokio::test]
    async fn a_profile_whose_secret_cannot_be_read_fails_typed() {
        let dir = tempfile::tempdir().expect("temp dir");
        let stores = ProfileStores::new(vec![s3_profile(
            "lake",
            Some("http://127.0.0.1:9"),
            dir.path(),
            false,
        )]);
        let err = stores.store("lake", "a").err().expect("refused");
        assert!(
            matches!(
                &err,
                ExternalStoreError::Open {
                    profile,
                    bucket,
                    source: ProfileError::SecretUnavailable { kind: "file" },
                } if profile == "lake" && bucket == "a"
            ),
            "{err:?}"
        );
        assert!(!err.to_string().contains("missing"), "{err}");
        std::fs::write(dir.path().join("missing"), "test-key\n").expect("write key");
        stores
            .store("lake", "a")
            .expect("the next request opens the store");
    }

    /// ADR-2040 D4: a (profile, bucket) naming Ravel's own data bucket, the
    /// same bucket name on the same service, is refused before the store is
    /// opened, so even a profile whose secret is unreadable fails this way.
    /// The same profile's other buckets, and the same name on another
    /// endpoint, still open.
    #[tokio::test]
    async fn ravels_own_bucket_is_refused_before_anything_is_opened() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ravel = RavelBucket {
            endpoint: Some("http://127.0.0.1:9".to_string()),
            bucket: "ravel".to_string(),
        };
        let stores = ProfileStores::new(vec![
            s3_profile("lake", Some("HTTP://127.0.0.1:9/"), dir.path(), true),
            s3_profile("locked", Some("http://127.0.0.1:9"), dir.path(), false),
            s3_profile("elsewhere", Some("http://127.0.0.1:10"), dir.path(), true),
        ])
        .refusing(ravel);
        for profile in ["lake", "locked"] {
            let err = stores.store(profile, "ravel").err().expect("refused");
            assert!(
                matches!(&err, ExternalStoreError::RavelBucket { profile: p, bucket }
                    if p == profile && bucket == "ravel"),
                "{profile}: {err:?}"
            );
        }
        stores.store("lake", "lake").expect("another bucket opens");
        stores
            .store("elsewhere", "ravel")
            .expect("the same name on another endpoint opens");

        let err = ParquetQueryError::Store {
            table: "t".to_string(),
            source: ExternalStoreError::RavelBucket {
                profile: "lake".to_string(),
                bucket: "ravel".to_string(),
            },
        };
        assert_eq!(err.class(), ErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("Ravel's own data bucket"), "{message}");
        assert!(!message.contains("\"ravel\""), "{message}");
    }

    /// The service comparison: AWS for no endpoint or any `amazonaws.com`
    /// host, GCS's interoperability host for a GCS profile, and an Azure
    /// profile never matches an S3-configured bucket.
    #[tokio::test]
    async fn ravels_bucket_is_matched_by_service() {
        let dir = tempfile::tempdir().expect("temp dir");
        let aws = RavelBucket {
            endpoint: None,
            bucket: "ravel".to_string(),
        };
        let profile = s3_profile(
            "lake",
            Some("https://s3.eu-west-1.amazonaws.com"),
            dir.path(),
            true,
        );
        assert!(aws.is_reached_by(&profile, "ravel"));
        assert!(!aws.is_reached_by(&profile, "lake"));
        let local = s3_profile("local", Some("http://127.0.0.1:9"), dir.path(), true);
        assert!(!aws.is_reached_by(&local, "ravel"));

        let profiles = ravel_object_store::external::load_profiles(
            r#"[{"name": "g", "kind": "gcs", "credentials": {"mode": "application_default"}},
                {"name": "z", "kind": "azure", "account": "acct",
                 "credentials": {"mode": "sas_token", "token": {"from": "env", "name": "T"}}}]"#,
        )
        .expect("profiles");
        let gcs = RavelBucket {
            endpoint: Some("https://storage.googleapis.com".to_string()),
            bucket: "ravel".to_string(),
        };
        assert!(gcs.is_reached_by(&profiles[0], "ravel"));
        assert!(!aws.is_reached_by(&profiles[0], "ravel"));
        assert!(!gcs.is_reached_by(&profiles[1], "ravel"));
        assert!(!aws.is_reached_by(&profiles[1], "ravel"));
    }
}
