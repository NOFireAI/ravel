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
//! A Flight SQL ticket pins each table's manifest version instead
//! ([`ParquetPin`]): `DoGet` reads those manifest objects by version
//! ([`resolve_pinned_tables`]) and still checks the grants as they are then.
//!
//! What a server wires in is [`ParquetSources`]: Ravel's store, the external
//! stores its credential profiles reach ([`ExternalStores`]), and the read
//! services the reader shares with the rest of the query path. An executor
//! without it treats every name that is not a signal table exactly as it did
//! before Parquet tables existed. With it but with no profiles configured, a
//! statement naming a Parquet table fails with
//! [`ParquetQueryError::NotConfigured`] after one LIST per name, one GET of
//! the newest manifest of each name that has versions, and no file read.

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
    MetadataCache, ParquetReadError, ParquetTableError, ParquetTableProvider, ReadLimits,
    ReadServices, TenantParquetStore,
};
use ravel_pqtable::grants::{self, GrantsError};
use ravel_pqtable::keys::MAX_MANIFEST_VERSION;
use ravel_pqtable::manifest::Manifest;
use ravel_pqtable::names::validate_table;
use ravel_pqtable::resolve::{self, ResolveError};
use ravel_query::{GetLimiter, PhaseAccounting, ReadCache};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, CostEstimate, QueryAccounting};

use crate::error::{ErrorClass, MSG_CORRUPT, MSG_PLAN, MSG_UNAVAILABLE};

/// Bytes of decoded Parquet footers the process keeps, when the embedder does
/// not choose a bound.
pub const DEFAULT_PARQUET_METADATA_CACHE_BYTES: u64 = 64 << 20;

/// The most distinct base tables other than the five signal tables one
/// statement may name ([`crate::SqlError::TooManyTables`] past it).
///
/// Whether such a name is a Parquet table is a fact about the store, so each
/// one can cost a LIST of its manifest prefix, and a name that has versions a
/// GET of its newest manifest, issued one after another before the statement
/// plans. This bound caps those reads per statement. Sixteen
/// clears every join a dashboard or report realistically writes, while the
/// statement complexity guard alone would admit several hundred names.
pub const MAX_STATEMENT_TABLE_NAMES: usize = 16;

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
    /// The address `profile` reaches `bucket` at overlaps the address of
    /// Ravel's own data bucket (ADR-2040 D4): see [`RavelBucket`] for what is
    /// compared. Only configuration is compared; Ravel's bucket reached under
    /// another name is refused when the grant is written, by the probe of
    /// ADR-2040 D1.
    #[error("bucket {bucket:?} through profile {profile:?} is Ravel's own data bucket")]
    RavelBucket { profile: String, bucket: String },
}

/// Ravel's own data bucket as the server is configured to reach it: the S3
/// endpoint (`None` for AWS's regional endpoint), the region and the bucket
/// name. Ravel's store addresses its bucket path-style.
///
/// A (profile, bucket) reaches it when their bucket addresses overlap. A
/// bucket address is where the S3 client sends requests for the bucket: an
/// explicit endpoint as written when virtual-hosted, the endpoint with
/// `/<bucket>` appended when path-style, and AWS's regional endpoint for the
/// region when there is none; a GCS profile's is the bucket on GCS. Two
/// addresses overlap when they are on the same service (the same AWS
/// partition, GCS, or the same host and port, a missing port read as the
/// scheme's default) and one's bucket and path segments begin with the
/// other's. An AWS or GCS address's bucket is its host's bucket label or, for
/// the service host itself, its first path segment, so a virtual-hosted and a
/// path-style address of one bucket compare equal. The prefix rule matters for
/// an endpoint that names no bucket: a virtual-hosted profile at Ravel's own
/// host sends every key verbatim, so a key beginning with Ravel's bucket name
/// reads Ravel's bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RavelBucket {
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
}

impl RavelBucket {
    /// Whether `profile` reaching `bucket` addresses this bucket, or a
    /// location containing it, by configuration.
    fn is_reached_by(&self, profile: &ExternalProfile, bucket: &str) -> bool {
        let ravel = BucketAddress::s3(self.endpoint.as_deref(), &self.region, &self.bucket, true);
        let theirs = match &profile.kind {
            ExternalKind::S3 {
                endpoint,
                region,
                force_path_style,
                ..
            } => BucketAddress::s3(endpoint.as_deref(), region, bucket, *force_path_style),
            ExternalKind::Gcs { .. } => BucketAddress {
                service: Service::Gcs,
                segments: vec![bucket.to_string()],
            },
            ExternalKind::Azure { .. } => return false,
        };
        ravel.overlaps(&theirs)
    }
}

/// Where requests for one bucket go: the service and the leading path
/// segments every key is appended to.
#[derive(Debug, PartialEq, Eq)]
struct BucketAddress {
    service: Service,
    segments: Vec<String>,
}

/// The namespace a bucket address is resolved in. AWS bucket names are unique
/// within a partition, and GovCloud and China are partitions of their own.
#[derive(Debug, PartialEq, Eq)]
enum Service {
    Aws(AwsPartition),
    Gcs,
    /// Any other host, lowercased, with its port.
    Host(String, Option<u16>),
}

#[derive(Debug, PartialEq, Eq)]
enum AwsPartition {
    Commercial,
    GovCloud,
    China,
}

impl BucketAddress {
    /// The address of `bucket` through an S3 client configured with
    /// `endpoint`, `region` and `path_style`, following `object_store`'s
    /// `AmazonS3Builder`.
    fn s3(endpoint: Option<&str>, region: &str, bucket: &str, path_style: bool) -> Self {
        let url = match (endpoint, path_style) {
            (Some(endpoint), false) => endpoint.to_string(),
            (Some(endpoint), true) => format!("{}/{bucket}", endpoint.trim_end_matches('/')),
            (None, false) => format!("https://{bucket}.s3.{region}.amazonaws.com"),
            (None, true) => format!("https://s3.{region}.amazonaws.com/{bucket}"),
        };
        Self::of_url(&url)
    }

    fn of_url(url: &str) -> Self {
        let url = url.trim();
        let (scheme, rest) = match url.split_once("://") {
            Some((scheme, rest)) => (scheme.to_ascii_lowercase(), rest),
            None => (String::new(), url),
        };
        let at = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, path) = rest.split_at(at);
        let path = path.split(['?', '#']).next().unwrap_or_default();
        let mut segments: Vec<String> = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(str::to_string)
            .collect();
        let host_port = authority
            .rsplit('@')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let (host, port) = split_port(&host_port);
        let host = host.trim_end_matches('.');
        let port = port.or(match scheme.as_str() {
            "https" => Some(443),
            "http" => Some(80),
            _ => None,
        });

        let (labels, partition) = if let Some(labels) = host.strip_suffix(".amazonaws.com.cn") {
            (labels, Some(AwsPartition::China))
        } else if let Some(labels) = host.strip_suffix(".amazonaws.com") {
            (labels, Some(AwsPartition::Commercial))
        } else {
            (host, None)
        };
        if let Some(partition) = partition {
            let labels: Vec<&str> = labels.split('.').collect();
            if let Some(service) = labels
                .iter()
                .rposition(|label| *label == "s3" || label.starts_with("s3-"))
            {
                let partition = if labels[service..]
                    .iter()
                    .any(|label| label.contains("us-gov-"))
                {
                    AwsPartition::GovCloud
                } else {
                    partition
                };
                if service > 0 {
                    segments.insert(0, labels[..service].join("."));
                }
                return BucketAddress {
                    service: Service::Aws(partition),
                    segments,
                };
            }
        }
        if host == "storage.googleapis.com" {
            return BucketAddress {
                service: Service::Gcs,
                segments,
            };
        }
        if let Some(bucket) = host.strip_suffix(".storage.googleapis.com") {
            segments.insert(0, bucket.to_string());
            return BucketAddress {
                service: Service::Gcs,
                segments,
            };
        }
        BucketAddress {
            service: Service::Host(host.to_string(), port),
            segments,
        }
    }

    /// Whether the two addresses are on one service and one's segments begin
    /// with the other's.
    fn overlaps(&self, other: &BucketAddress) -> bool {
        let shorter = self.segments.len().min(other.segments.len());
        self.service == other.service && self.segments[..shorter] == other.segments[..shorter]
    }
}

/// `host_port` split into its host and its port, when it names one that
/// parses. An IPv6 literal keeps its brackets.
fn split_port(host_port: &str) -> (&str, Option<u16>) {
    let colon = if host_port.starts_with('[') {
        host_port
            .find(']')
            .and_then(|close| host_port[close..].find(':').map(|at| close + at))
    } else {
        host_port.rfind(':')
    };
    match colon {
        Some(at) => match host_port[at + 1..].parse() {
            Ok(port) => (&host_port[..at], Some(port)),
            Err(_) => (&host_port[..at], None),
        },
        None => (host_port, None),
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

    /// Ravel's own store, which holds the manifests and the grants record.
    /// Unlike [`ParquetSources::resolve_store`], this is unaccounted direct
    /// write access: `crate::ddl::execute_ddl` is the one caller, and grants
    /// reads and manifest writes are outside query-cost accounting (ADR-2040
    /// grants-and-DDL-cost amendment, 2026-10-01).
    pub(crate) fn ravel_store(&self) -> &Arc<dyn ObjectStoreBackend> {
        &self.ravel
    }

    /// The process-wide GET limiter the signal-table fetchers share, for
    /// `crate::ddl::execute_ddl`'s `snapshot_location` call.
    pub(crate) fn get_limiter(&self) -> &Arc<GetLimiter> {
        &self.services.limiter
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

/// The live manifest of each Parquet table a statement names, every file of
/// which lay inside one of the tenant's grants when it was resolved. The
/// manifests are the newest ones when the statement resolved them
/// ([`resolve_tables`]) and the pinned ones when a Flight ticket redeemed them
/// ([`resolve_pinned_tables`]).
#[derive(Debug, Clone)]
pub struct ParquetResolution {
    pub(crate) manifests: Vec<Manifest>,
}

impl ParquetResolution {
    /// The table name and manifest version of each resolved table: what a
    /// Flight ticket pins so that `DoGet` reads these manifest objects and no
    /// newer ones.
    pub fn pins(&self) -> Vec<ParquetPin> {
        self.manifests
            .iter()
            .map(|manifest| ParquetPin {
                table: manifest.table.clone(),
                version: manifest.version,
            })
            .collect()
    }
}

/// One Parquet table a Flight ticket pins: the table's name and the version of
/// the immutable manifest object (`ravel-pqtable`'s `v/<version>` key) that
/// `GetFlightInfo` resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetPin {
    pub table: String,
    pub version: u64,
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
    /// A Flight ticket pins a manifest version of the table that is no longer
    /// there, or is a drop. Reported to the client as
    /// [`crate::SqlError::SnapshotInvalidated`]: the pinned state is gone and
    /// no other version may stand in for it.
    #[error("the pinned manifest version {version} of Parquet table {table} is gone")]
    PinnedManifestGone { table: String, version: u64 },
}

impl ParquetQueryError {
    /// The client-visible class: 400 for a request this surface cannot take,
    /// 422 for a refusal or a permanent state a retry cannot change, 503 for
    /// a transient storage fault or a manifest or grants record above the
    /// version ceiling, 500 for an integrity fault or a store read
    /// that failed its checksum. It agrees with `client_message`: a variant
    /// answered `MSG_CORRUPT` there is `Internal` here.
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            ParquetQueryError::RowWindowUnsupported => ErrorClass::BadRequest,
            ParquetQueryError::Read(read)
            | ParquetQueryError::Table(ParquetTableError::Read { source: read, .. }) => {
                match read {
                    ParquetReadError::FileChanged { .. } | ParquetReadError::FileMissing { .. } => {
                        ErrorClass::Unsupported
                    }
                    // A data file is an external object Ravel did not write,
                    // and one stored without a checksum never fails as
                    // `Corrupted`; where one was stored, the mismatch follows
                    // the same rule as the manifest and grants reads below.
                    ParquetReadError::Store { source, .. }
                        if matches!(source.as_ref(), StoreError::Corrupted(_)) =>
                    {
                        ErrorClass::Internal
                    }
                    ParquetReadError::Corrupt { .. } => ErrorClass::Internal,
                    ParquetReadError::Store { .. } | ParquetReadError::LeaderLost { .. } => {
                        ErrorClass::Unavailable
                    }
                    // Unreachable in practice: `From<ParquetQueryError> for
                    // SqlError` (ravel-sql/src/error.rs) converts this to
                    // `SqlError::Fetch` before a boxed `SqlError::Parquet`
                    // ever reaches this `class()` call.
                    ParquetReadError::MemoryExhausted { .. } => ErrorClass::Unavailable,
                    // Unreachable in practice: the same `From` impl converts
                    // these to `SqlError::RequestBudgetExceeded` /
                    // `SqlError::TooManyBytesScanned` first.
                    ParquetReadError::RequestBudgetExceeded { .. }
                    | ParquetReadError::BytesBudgetExceeded { .. } => ErrorClass::Unsupported,
                }
            }
            // A checksum mismatch on a manifest or grants record Ravel wrote:
            // a retry reads the same bytes, as under `CatalogError::Store`.
            ParquetQueryError::Resolve {
                source:
                    ResolveError::Store {
                        source: StoreError::Corrupted(_),
                        ..
                    },
                ..
            }
            | ParquetQueryError::Grants(GrantsError::Store {
                source: StoreError::Corrupted(_),
                ..
            }) => ErrorClass::Internal,
            ParquetQueryError::Resolve {
                source: ResolveError::Store { .. } | ResolveError::Vanished { .. },
                ..
            }
            | ParquetQueryError::Grants(GrantsError::Store { .. })
            | ParquetQueryError::PinnedManifestGone { .. } => ErrorClass::Unavailable,
            // A record above the version ceiling is not corrupt (ADR-0066
            // decision 2): a peer on a newer build reads it. One below the
            // floor stays in the corrupt catch-all, as on the catalog surface.
            ParquetQueryError::Resolve { source, .. } if source.is_newer_format_version() => {
                ErrorClass::Unavailable
            }
            ParquetQueryError::Grants(err) if err.is_newer_format_version() => {
                ErrorClass::Unavailable
            }
            // Every other manifest or grants fault is answered MSG_CORRUPT by
            // `client_message`, so it is corrupt here too.
            ParquetQueryError::Resolve { .. } | ParquetQueryError::Grants(_) => {
                ErrorClass::Internal
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
                    // Unreachable in practice: see the note on the matching
                    // arm in `class()` above -- `From<ParquetQueryError> for
                    // SqlError` converts these before `client_message()` runs.
                    ParquetReadError::RequestBudgetExceeded { .. }
                    | ParquetReadError::BytesBudgetExceeded { .. } => read.to_string(),
                    ParquetReadError::Corrupt { .. } => MSG_CORRUPT.to_string(),
                    // The checksum split of the matching arm in `class()`.
                    ParquetReadError::Store { source, .. }
                        if matches!(source.as_ref(), StoreError::Corrupted(_)) =>
                    {
                        MSG_CORRUPT.to_string()
                    }
                    ParquetReadError::Store { .. } | ParquetReadError::LeaderLost { .. } => {
                        MSG_UNAVAILABLE.to_string()
                    }
                    // Unreachable in practice: same note as above.
                    ParquetReadError::MemoryExhausted { .. } => MSG_UNAVAILABLE.to_string(),
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
                source:
                    ResolveError::Store {
                        source: StoreError::Corrupted(_),
                        ..
                    },
                ..
            }
            | ParquetQueryError::Grants(GrantsError::Store {
                source: StoreError::Corrupted(_),
                ..
            }) => MSG_CORRUPT.to_string(),
            ParquetQueryError::Resolve {
                source: ResolveError::Store { .. } | ResolveError::Vanished { .. },
                ..
            }
            | ParquetQueryError::Grants(GrantsError::Store { .. }) => MSG_UNAVAILABLE.to_string(),
            ParquetQueryError::PinnedManifestGone { .. } => MSG_UNAVAILABLE.to_string(),
            ParquetQueryError::Resolve { source, .. } if source.is_newer_format_version() => {
                MSG_UNAVAILABLE.to_string()
            }
            ParquetQueryError::Grants(err) if err.is_newer_format_version() => {
                MSG_UNAVAILABLE.to_string()
            }
            ParquetQueryError::Resolve { .. } | ParquetQueryError::Grants(_) => {
                MSG_CORRUPT.to_string()
            }
        }
    }
}

/// The first name in `names` that is a live Parquet table of `tenant`, from
/// one LIST per valid table name up to and including it, plus one GET of the
/// newest manifest for each name that has manifest versions.
///
/// A name whose newest version is a drop is no table, exactly as
/// [`resolve_tables`] treats it, so the statements that stop here agree with
/// the ones that resolve in full that a dropped table is an unknown one.
pub(crate) async fn first_live_table(
    sources: &ParquetSources,
    tenant: &TenantHash,
    names: &BTreeSet<String>,
    accounting: &QueryAccounting,
) -> Result<Option<String>, ParquetQueryError> {
    let store = sources.resolve_store(accounting);
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
        if newest.is_some_and(|manifest| manifest.is_live()) {
            return Ok(Some(name.clone()));
        }
    }
    Ok(None)
}

/// The cost of a resolved Parquet statement that is known before any file is
/// read: the `resolve_requests` its resolve made, plus one GET for each file
/// its manifests name, since a scan opens every file. Like the segment cost
/// estimators in `cost.rs`, the request figure assumes a cold cache and a
/// full scan of every named file: a cached footer or column chunk costs no
/// GET, and a `LIMIT` that stops the scan before every file opens costs
/// fewer, so the actual count can land under this figure as well as over it
/// (an uncached footer and each column chunk still cost more).
/// `estimated_store_bytes` is the total size of the files, which bounds a
/// cold-cache full scan's bytes: a range read again after eviction, or a
/// footer re-read when the metadata cache is too small, reads bytes this
/// figure already counted once.
pub(crate) fn estimate_cost(resolution: &ParquetResolution, resolve_requests: u64) -> CostEstimate {
    let mut files = 0u64;
    let mut bytes = 0u64;
    for manifest in &resolution.manifests {
        files = files.saturating_add(manifest.files.len() as u64);
        for file in &manifest.files {
            bytes = bytes.saturating_add(file.size);
        }
    }
    CostEstimate::new(resolve_requests.saturating_add(files), bytes, 0, 0, 0)
}

/// Resolve the Parquet tables among `names`: the newest live manifest of each,
/// checked against the tenant's current grants. `None` when no name is a live
/// Parquet table of `tenant`.
///
/// Every file must lie inside a grant that exists now under the profile the
/// file is read through; the grant the manifest recorded is not consulted.
///
/// The caller has checked [`ParquetSources::is_configured`]; without profiles
/// [`build_tables`] refuses with [`ParquetQueryError::NotConfigured`].
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
    if manifests.is_empty() {
        return Ok(None);
    }
    Ok(Some(granted_resolution(&store, tenant, manifests).await?))
}

/// The manifests a Flight ticket pinned, read by their exact version (one GET
/// each, no LIST) and checked against the tenant's grants as they are now.
///
/// A pinned version that is gone is [`ParquetQueryError::PinnedManifestGone`]:
/// the newest version never stands in for it. A pinned version that is a drop
/// is refused the same way, as a defensive check only, since `GetFlightInfo`
/// pins live versions. So is a pin above [`MAX_MANIFEST_VERSION`], before any
/// read: `GetFlightInfo` resolves through `resolve::newest`, which never
/// returns one. The caller has checked that `pins` is not empty.
pub(crate) async fn resolve_pinned_tables(
    sources: &ParquetSources,
    tenant: &TenantHash,
    pins: &[ParquetPin],
    accounting: &QueryAccounting,
) -> Result<ParquetResolution, ParquetQueryError> {
    let store = sources.resolve_store(accounting);
    let mut manifests = Vec::with_capacity(pins.len());
    for pin in pins {
        let gone = || ParquetQueryError::PinnedManifestGone {
            table: pin.table.clone(),
            version: pin.version,
        };
        if pin.version > MAX_MANIFEST_VERSION {
            return Err(gone());
        }
        let manifest = resolve::read_version(&store, tenant, &pin.table, pin.version)
            .await
            .map_err(|source| ParquetQueryError::Resolve {
                table: pin.table.clone(),
                source,
            })?
            .filter(Manifest::is_live)
            .ok_or_else(gone)?;
        manifests.push(manifest);
    }
    granted_resolution(&store, tenant, manifests).await
}

/// `manifests` as a [`ParquetResolution`], once every file of each lies inside
/// a grant the tenant holds now, under the profile the file is read through.
async fn granted_resolution(
    store: &ResolveStore,
    tenant: &TenantHash,
    manifests: Vec<Manifest>,
) -> Result<ParquetResolution, ParquetQueryError> {
    let granted = grants::list(store, tenant)
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
    Ok(ParquetResolution { manifests })
}

/// Build one provider per resolved table, reading each table's first footer
/// through the reader (charged to Probe), and the store the session's
/// registry answers with. `parallel` is ADR-2040 D6's file grouping. Every
/// read the tables make, this footer read included, is admitted against
/// `limits`.
pub(crate) async fn build_tables(
    sources: &ParquetSources,
    tenant: TenantHash,
    resolution: &ParquetResolution,
    accounting: &PhaseAccounting,
    limits: &ReadLimits,
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
            limits.clone(),
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
    use ravel_pqtable::manifest::ManifestError;

    use super::*;

    /// A static-key S3 profile reaching `endpoint`, its keys read from files
    /// under `dir` when `readable`, from a missing file otherwise.
    fn s3_profile(
        name: &str,
        endpoint: Option<&str>,
        dir: &std::path::Path,
        readable: bool,
    ) -> ExternalProfile {
        s3_profile_in(name, endpoint, "us-east-1", true, dir, readable)
    }

    /// [`s3_profile`] in `region`, path-style or virtual-hosted.
    fn s3_profile_in(
        name: &str,
        endpoint: Option<&str>,
        region: &str,
        path_style: bool,
        dir: &std::path::Path,
        readable: bool,
    ) -> ExternalProfile {
        let key = dir.join(if readable { "key" } else { "missing" });
        if readable {
            std::fs::write(&key, "test-key\n").expect("write key");
        }
        let endpoint = endpoint.map_or("null".to_string(), |e| format!("{e:?}"));
        let json = format!(
            r#"[{{"name": {name:?}, "kind": "s3", "region": {region:?}, "endpoint": {endpoint},
                 "allow_http": true, "force_path_style": {path_style},
                 "credentials": {{"mode": "static",
                   "access_key_id": {{"from": "file", "path": {key:?}}},
                   "secret_access_key": {{"from": "file", "path": {key:?}}}}}}}]"#
        );
        ravel_object_store::external::load_profiles(&json)
            .expect("profile")
            .remove(0)
    }

    /// A Flight pin above the manifest version bound is refused as gone
    /// before Ravel's store is read at all.
    #[tokio::test]
    async fn a_pin_above_the_version_bound_is_gone_without_a_read() {
        use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
        use ravel_object_store::memory::MemoryStore;

        let ravel = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let sources = ParquetSources::new(
            ravel.clone(),
            None,
            Arc::new(GetLimiter::new(8).expect("limiter")),
            None,
            DEFAULT_PARQUET_METADATA_CACHE_BYTES,
        );
        let pins = [ParquetPin {
            table: "hits".to_string(),
            version: MAX_MANIFEST_VERSION + 1,
        }];
        let got = resolve_pinned_tables(
            &sources,
            &TenantHash([7; 16]),
            &pins,
            &QueryAccounting::new(),
        )
        .await;
        assert!(
            matches!(
                &got,
                Err(ParquetQueryError::PinnedManifestGone { table, version })
                    if table == "hits" && *version == MAX_MANIFEST_VERSION + 1
            ),
            "{:?}",
            got.err()
        );
        assert_eq!(ravel.metrics().snapshot().op(StoreOp::Get).calls, 0);
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
            region: "us-east-1".to_string(),
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
                bucket: "ravel-data".to_string(),
            },
        };
        assert_eq!(err.class(), ErrorClass::Unsupported);
        let message = err.client_message();
        assert!(message.contains("Ravel's own data bucket"), "{message}");
        assert!(!message.contains("ravel-data"), "{message}");
        assert!(!message.contains("lake"), "{message}");
    }

    /// Every Parquet store-error arm (a data read, directly and under a table
    /// build, a manifest resolve and a grants read) answers a checksum
    /// mismatch as corrupt (500) and a transient store fault as retryable
    /// (503), in its own `class()` and through the `SqlError` the server maps
    /// to a status.
    ///
    /// FLIP: drop the `ResolveError::Store { source: StoreError::Corrupted(_),
    /// .. }` pattern from the corrupt arm of `client_message()` and the
    /// resolve case fails with `left: "upstream storage temporarily
    /// unavailable"`, `right: "stored data failed integrity validation"`.
    #[test]
    fn a_store_checksum_mismatch_is_corrupt_and_a_transient_fault_is_retryable() {
        type Build = fn(StoreError) -> ParquetQueryError;
        let cases: [(&str, Build); 4] = [
            ("data read", |source| {
                ParquetQueryError::Read(ParquetReadError::Store {
                    key: "lake/part-0.parquet".to_string(),
                    source: Arc::new(source),
                })
            }),
            ("table build read", |source| {
                ParquetQueryError::Table(ParquetTableError::Read {
                    table: "t".to_string(),
                    source: ParquetReadError::Store {
                        key: "lake/part-0.parquet".to_string(),
                        source: Arc::new(source),
                    },
                })
            }),
            ("resolve", |source| ParquetQueryError::Resolve {
                table: "t".to_string(),
                source: ResolveError::Store {
                    key: "manifest-key".to_string(),
                    source,
                },
            }),
            ("grants", |source| {
                ParquetQueryError::Grants(GrantsError::Store {
                    key: "grants-key".to_string(),
                    source,
                })
            }),
        ];
        let corrupt = || StoreError::Corrupted("checksum mismatch on raw-store-detail".to_string());
        let transient = || StoreError::Transient("raw-store-detail".to_string());
        for (name, build) in cases {
            let err = build(corrupt());
            assert_eq!(err.client_message(), MSG_CORRUPT, "{name}");
            assert_eq!(err.class(), ErrorClass::Internal, "{name}");
            let err = crate::SqlError::from(build(corrupt()));
            assert_eq!(err.client_message(), MSG_CORRUPT, "{name}");
            assert_eq!(err.class(), ErrorClass::Internal, "{name}");

            let err = build(transient());
            assert_eq!(err.client_message(), MSG_UNAVAILABLE, "{name}");
            assert_eq!(err.class(), ErrorClass::Unavailable, "{name}");
            let err = crate::SqlError::from(build(transient()));
            assert_eq!(err.client_message(), MSG_UNAVAILABLE, "{name}");
            assert_eq!(err.class(), ErrorClass::Unavailable, "{name}");
        }
    }

    /// A manifest or grants record above the version ceiling this build reads
    /// is retryable (503), since a peer on a newer build can read it (ADR-0066
    /// decision 2); one below the floor, a corrupt data file and every other
    /// resolve or grants fault stays corrupt (500), as on the catalog surface.
    /// Each case is checked
    /// in its own `class()` and `client_message()` and through the `SqlError`
    /// the server maps to a status.
    ///
    /// FLIP: drop the `ParquetQueryError::Resolve { source, .. } if
    /// source.is_newer_format_version()` arm of `class()` and the manifest
    /// above-ceiling case fails with `left: (Internal, "upstream
    /// storage temporarily unavailable")`.
    #[test]
    fn a_version_above_the_ceiling_is_retryable_and_below_the_floor_is_corrupt() {
        fn resolve(source: ResolveError) -> ParquetQueryError {
            ParquetQueryError::Resolve {
                table: "t".to_string(),
                source,
            }
        }
        fn corrupt_read() -> ParquetReadError {
            ParquetReadError::Corrupt {
                key: "lake/part-0.parquet".to_string(),
                message: "raw-decoder-detail".to_string(),
            }
        }
        type Case = (
            &'static str,
            fn() -> ParquetQueryError,
            ErrorClass,
            &'static str,
        );
        let cases: Vec<Case> = vec![
            (
                "manifest above ceiling",
                || {
                    resolve(ResolveError::Manifest(ManifestError::UnsupportedVersion {
                        key: "manifest-key".to_string(),
                        got: 9,
                        ceiling: 1,
                    }))
                },
                ErrorClass::Unavailable,
                MSG_UNAVAILABLE,
            ),
            (
                "grants above ceiling",
                || {
                    ParquetQueryError::Grants(GrantsError::UnsupportedVersion {
                        key: "grants-key".to_string(),
                        got: 9,
                        ceiling: 1,
                    })
                },
                ErrorClass::Unavailable,
                MSG_UNAVAILABLE,
            ),
            (
                "manifest below floor",
                || {
                    resolve(ResolveError::Manifest(ManifestError::VersionBelowFloor {
                        key: "manifest-key".to_string(),
                        got: 0,
                        floor: 1,
                    }))
                },
                ErrorClass::Internal,
                MSG_CORRUPT,
            ),
            (
                "grants below floor",
                || {
                    ParquetQueryError::Grants(GrantsError::VersionBelowFloor {
                        key: "grants-key".to_string(),
                        got: 0,
                        floor: 1,
                    })
                },
                ErrorClass::Internal,
                MSG_CORRUPT,
            ),
            (
                "corrupt data read",
                || ParquetQueryError::Read(corrupt_read()),
                ErrorClass::Internal,
                MSG_CORRUPT,
            ),
            (
                "corrupt table build read",
                || {
                    ParquetQueryError::Table(ParquetTableError::Read {
                        table: "t".to_string(),
                        source: corrupt_read(),
                    })
                },
                ErrorClass::Internal,
                MSG_CORRUPT,
            ),
            (
                "resolve foreign key",
                || {
                    resolve(ResolveError::ForeignKey {
                        key: "foreign-key".to_string(),
                        prefix: "prefix".to_string(),
                        reason: "raw-key-detail".to_string(),
                    })
                },
                ErrorClass::Internal,
                MSG_CORRUPT,
            ),
            (
                "grants decode",
                || {
                    ParquetQueryError::Grants(GrantsError::Decode {
                        key: "grants-key".to_string(),
                        source: prost::encoding::decode_varint(&mut &[0x80u8][..])
                            .expect_err("a truncated varint"),
                    })
                },
                ErrorClass::Internal,
                MSG_CORRUPT,
            ),
        ];
        for (name, build, class, message) in cases {
            let err = build();
            assert_eq!(
                (err.class(), err.client_message().as_str()),
                (class, message),
                "{name}"
            );
            let err = crate::SqlError::from(build());
            assert_eq!(
                (err.class(), err.client_message().as_str()),
                (class, message),
                "{name}"
            );
        }
    }

    /// The service comparison: one AWS partition for no endpoint or any
    /// commercial S3 host, GCS's interoperability host for a GCS profile, and
    /// an Azure profile never matches an S3-configured bucket.
    #[tokio::test]
    async fn ravels_bucket_is_matched_by_service() {
        let dir = tempfile::tempdir().expect("temp dir");
        let aws = RavelBucket {
            endpoint: None,
            region: "us-east-1".to_string(),
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
            region: "us-east-1".to_string(),
            bucket: "ravel".to_string(),
        };
        assert!(gcs.is_reached_by(&profiles[0], "ravel"));
        assert!(!aws.is_reached_by(&profiles[0], "ravel"));
        assert!(!gcs.is_reached_by(&profiles[1], "ravel"));
        assert!(!aws.is_reached_by(&profiles[1], "ravel"));
    }

    /// A path-style profile endpoint carrying a path moves the bucket it
    /// names under the path: `http://ravel-host/ravel-bucket` with bucket `t`
    /// addresses Ravel's keys `t/...`. It is refused as Ravel's bucket when
    /// Ravel's is there, and refused at open when no Ravel bucket is
    /// configured or the host is another one.
    #[tokio::test]
    async fn a_profile_endpoint_with_a_path_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ravel = RavelBucket {
            endpoint: Some("http://ravel-host".to_string()),
            region: "us-east-1".to_string(),
            bucket: "ravel-bucket".to_string(),
        };
        let profiles = || {
            vec![
                s3_profile(
                    "under",
                    Some("http://ravel-host/ravel-bucket"),
                    dir.path(),
                    true,
                ),
                s3_profile("other", Some("http://127.0.0.1:9/prefix"), dir.path(), true),
            ]
        };
        let stores = ProfileStores::new(profiles()).refusing(ravel);
        assert!(
            matches!(
                stores.store("under", "t"),
                Err(ExternalStoreError::RavelBucket { .. })
            ),
            "the path puts bucket t inside Ravel's"
        );
        let unconfigured = ProfileStores::new(profiles());
        for (stores, profile) in [
            (&stores, "other"),
            (&unconfigured, "under"),
            (&unconfigured, "other"),
        ] {
            let err = stores.store(profile, "t").err().expect("refused");
            assert!(
                matches!(
                    &err,
                    ExternalStoreError::Open {
                        source: ProfileError::EndpointPath { .. },
                        ..
                    }
                ),
                "{profile}: {err:?}"
            );
        }
    }

    /// A virtual-hosted endpoint is used as written, so its bucket is the one
    /// its host names, not the bucket the profile is asked for; one that names
    /// no bucket sends each key verbatim, and a key beginning with Ravel's
    /// bucket reads Ravel's bucket.
    #[test]
    fn a_virtual_hosted_endpoint_is_compared_as_written() {
        let dir = tempfile::tempdir().expect("temp dir");
        let virtual_hosted = |endpoint: &str| {
            s3_profile_in("v", Some(endpoint), "us-east-1", false, dir.path(), true)
        };
        let local = RavelBucket {
            endpoint: Some("http://ravel-host".to_string()),
            region: "us-east-1".to_string(),
            bucket: "ravel-bucket".to_string(),
        };
        assert!(local.is_reached_by(&virtual_hosted("http://ravel-host"), "lake"));
        assert!(local.is_reached_by(&virtual_hosted("http://RAVEL-HOST:80/"), "lake"));
        assert!(!local.is_reached_by(&virtual_hosted("http://lake.ravel-host"), "ravel-bucket"));

        let aws = RavelBucket {
            endpoint: None,
            region: "us-east-1".to_string(),
            bucket: "ravel".to_string(),
        };
        assert!(aws.is_reached_by(
            &virtual_hosted("https://s3.us-east-1.amazonaws.com"),
            "lake"
        ));
        assert!(aws.is_reached_by(
            &virtual_hosted("https://ravel.s3.eu-west-1.amazonaws.com"),
            "lake"
        ));
        assert!(!aws.is_reached_by(
            &virtual_hosted("https://lake.s3.us-east-1.amazonaws.com"),
            "ravel"
        ));
        let regional = s3_profile_in("r", None, "eu-west-1", false, dir.path(), true);
        assert!(aws.is_reached_by(&regional, "ravel"));
        assert!(!aws.is_reached_by(&regional, "lake"));
    }

    /// A missing port is the scheme's default, so `https://h` and
    /// `https://h:443` are one endpoint and `https://h:9000` another.
    #[test]
    fn default_ports_are_normalised() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ravel = RavelBucket {
            endpoint: Some("https://minio.example".to_string()),
            region: "us-east-1".to_string(),
            bucket: "ravel".to_string(),
        };
        let at = |endpoint: &str| s3_profile("p", Some(endpoint), dir.path(), true);
        assert!(ravel.is_reached_by(&at("https://minio.example:443"), "ravel"));
        assert!(ravel.is_reached_by(&at("https://Minio.Example./"), "ravel"));
        assert!(!ravel.is_reached_by(&at("https://minio.example:9000"), "ravel"));
        assert!(!ravel.is_reached_by(&at("http://minio.example"), "ravel"));
        let explicit = RavelBucket {
            endpoint: Some("http://[::1]:80".to_string()),
            region: "us-east-1".to_string(),
            bucket: "ravel".to_string(),
        };
        assert!(explicit.is_reached_by(&at("http://[::1]"), "ravel"));
        assert!(!explicit.is_reached_by(&at("http://[::1]:9000"), "ravel"));
    }

    /// Bucket names are unique within an AWS partition, not across them: a
    /// GovCloud or China bucket of Ravel's bucket's name is another bucket.
    #[test]
    fn aws_partitions_are_compared_apart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let in_region = |region: &str| s3_profile_in("p", None, region, true, dir.path(), true);
        let at = |endpoint: &str| s3_profile("p", Some(endpoint), dir.path(), true);
        let commercial = RavelBucket {
            endpoint: None,
            region: "us-east-1".to_string(),
            bucket: "ravel".to_string(),
        };
        assert!(commercial.is_reached_by(&in_region("ap-south-1"), "ravel"));
        assert!(!commercial.is_reached_by(&in_region("us-gov-west-1"), "ravel"));
        assert!(
            !commercial.is_reached_by(&at("https://s3-fips.us-gov-west-1.amazonaws.com"), "ravel")
        );
        assert!(!commercial.is_reached_by(&at("https://s3.cn-north-1.amazonaws.com.cn"), "ravel"));
        let gov = RavelBucket {
            endpoint: None,
            region: "us-gov-west-1".to_string(),
            bucket: "ravel".to_string(),
        };
        assert!(gov.is_reached_by(&in_region("us-gov-east-1"), "ravel"));
        assert!(!gov.is_reached_by(&in_region("us-east-1"), "ravel"));
    }
}
