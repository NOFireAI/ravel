//! Read-only stores for buckets Ravel does not own (ADR-2040 decision 3).
//!
//! A Parquet table queried in place lives in someone else's bucket, reached
//! with someone else's credentials. Three things follow, and this module is
//! all three:
//!
//! - **A credential profile, not a URL.** An [`ExternalProfile`] names a
//!   credential set and the settings needed to reach the store it belongs to.
//!   Object identity is `(profile, bucket, key)`; a URL is a rendering of that
//!   triple, never the identity itself, because two profiles can reach the same
//!   URL with different rights and the same bytes under a different endpoint
//!   are not the same object.
//! - **No inline secrets.** A profile holds *where* a secret is
//!   ([`SecretSource`]: an environment variable or a file), never the secret.
//!   The [`Debug`] impls in this module render neither the secret nor the
//!   source, so a profile that reaches a log line or a panic message leaks
//!   nothing about the deployment's key material.
//! - **Read-only, enforced in the type.** [`ExternalStore`] implements
//!   [`ObjectStoreBackend`] with every mutating method refusing:
//!   [`StoreError::ReadOnly`] from `put`, `put_multipart` and `delete`. Ravel
//!   holds read credentials for a granted bucket and must not be able to write
//!   there even if a caller asks; the refusal is local, so a misconfigured
//!   grant cannot turn into a request.
//!
//! [`probe`] holds the two qualification checks a grant must pass before
//! anything reads through it: that the store evaluates read preconditions, and
//! that it is not Ravel's own bucket under another name.
//!
//! `ravel-server` opens one per (profile, bucket) a Parquet table query or a
//! `CREATE EXTERNAL TABLE` (#2054) reads, both through `ravel-sql`'s
//! `ProfileStores`; the DDL also runs [`probe`]'s checks on the location it
//! is about to write. `ravel-cli tenant parquet-grant add` opens one to probe
//! a grant.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as OsPath;
use object_store::{GetOptions as OsGetOptions, GetRange as OsGetRange};
use object_store::{ObjectStore as OsObjectStore, ObjectStoreExt as _};
use serde::Deserialize;

use crate::s3::{S3AuthMode, S3Config, S3Store};
use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PutOptions, PutOutcome, StoreError, Version,
};

pub mod probe;

/// Page size for a listing through a generic (GCS/Azure) external store.
/// Matches S3's default so a caller's paging loop behaves the same whichever
/// kind of store a grant points at.
const EXTERNAL_PAGE_SIZE: usize = 1000;

/// Where a secret lives. Never the secret itself.
///
/// [`Debug`] renders only which kind of source it is, not the variable name or
/// the path: an environment variable name and a key file's path are themselves
/// deployment facts worth keeping out of logs, and redacting the value while
/// printing its location would still hand an attacker with log access the
/// place to look.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum SecretSource {
    /// Read the secret from this environment variable.
    Env { name: String },
    /// Read the secret from this file. Trailing whitespace is trimmed, so a
    /// mounted secret with a trailing newline works unedited.
    File { path: PathBuf },
}

impl SecretSource {
    /// Which kind of source this is, for a log line that must say something.
    /// Never the variable name or the path.
    pub fn kind(&self) -> &'static str {
        match self {
            SecretSource::Env { .. } => "env",
            SecretSource::File { .. } => "file",
        }
    }

    /// Read the secret. The error names the kind of source and nothing else,
    /// so a failure is diagnosable without the message carrying the location.
    pub fn resolve(&self) -> Result<String, ProfileError> {
        match self {
            SecretSource::Env { name } => {
                std::env::var(name).map_err(|_| ProfileError::SecretUnavailable { kind: "env" })
            }
            SecretSource::File { path } => std::fs::read_to_string(path)
                .map(|contents| contents.trim_end().to_string())
                .map_err(|_| ProfileError::SecretUnavailable { kind: "file" }),
        }
    }
}

impl std::fmt::Debug for SecretSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretSource({}, redacted)", self.kind())
    }
}

/// How an S3-compatible external store authenticates. Mirrors [`S3Config`]'s
/// credential modes, with every secret behind a [`SecretSource`].
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum S3ProfileCredentials {
    /// An access key pair, optionally with a session token.
    Static {
        access_key_id: SecretSource,
        secret_access_key: SecretSource,
        #[serde(default)]
        session_token: Option<SecretSource>,
    },
    /// A JSON credentials file an external process rotates, passed straight to
    /// [`S3Config::credentials_file`].
    CredentialsFile { path: PathBuf },
    /// EC2 instance role credentials (IMDSv2).
    InstanceRole,
}

impl std::fmt::Debug for S3ProfileCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3ProfileCredentials::Static { .. } => f.write_str("S3Credentials(static, redacted)"),
            S3ProfileCredentials::CredentialsFile { .. } => {
                f.write_str("S3Credentials(credentials_file, redacted)")
            }
            S3ProfileCredentials::InstanceRole => f.write_str("S3Credentials(instance_role)"),
        }
    }
}

/// How a Google Cloud Storage external store authenticates.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum GcsProfileCredentials {
    /// A service-account JSON key file.
    ServiceAccount { path: PathBuf },
    /// Application Default Credentials, resolved by `object_store` from the
    /// ambient environment (workload identity, `gcloud` ADC file).
    ApplicationDefault,
}

impl std::fmt::Debug for GcsProfileCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcsProfileCredentials::ServiceAccount { .. } => {
                f.write_str("GcsCredentials(service_account, redacted)")
            }
            GcsProfileCredentials::ApplicationDefault => {
                f.write_str("GcsCredentials(application_default)")
            }
        }
    }
}

/// How an Azure Blob Storage external store authenticates.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum AzureProfileCredentials {
    /// A storage account access key.
    AccessKey { key: SecretSource },
    /// A shared access signature token, the read-scoped form of an Azure
    /// grant.
    SasToken { token: SecretSource },
}

impl std::fmt::Debug for AzureProfileCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AzureProfileCredentials::AccessKey { .. } => {
                f.write_str("AzureCredentials(access_key, redacted)")
            }
            AzureProfileCredentials::SasToken { .. } => {
                f.write_str("AzureCredentials(sas_token, redacted)")
            }
        }
    }
}

/// Which kind of store a profile reaches, and the per-kind connection
/// settings.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExternalKind {
    /// S3 or any S3-compatible endpoint. The connection settings are
    /// [`S3Config`]'s, minus the bucket (which [`ExternalStore::open`] takes
    /// per call, since one profile can reach several buckets).
    S3 {
        region: String,
        /// `None` uses AWS's regional endpoint; set it for an S3-compatible
        /// deployment. A scheme, host and port only: [`ExternalStore::open`]
        /// refuses an endpoint carrying a path.
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        force_path_style: bool,
        #[serde(default)]
        allow_http: bool,
        credentials: S3ProfileCredentials,
    },
    Gcs {
        credentials: GcsProfileCredentials,
    },
    Azure {
        /// Storage account name. Not a secret: it is half of the public
        /// endpoint hostname.
        account: String,
        credentials: AzureProfileCredentials,
    },
}

impl ExternalKind {
    /// Short tag for a log line or an error message.
    pub fn name(&self) -> &'static str {
        match self {
            ExternalKind::S3 { .. } => "s3",
            ExternalKind::Gcs { .. } => "gcs",
            ExternalKind::Azure { .. } => "azure",
        }
    }
}

/// One named credential profile: the first component of an external object's
/// identity `(profile, bucket, key)`.
///
/// `Debug` is derived, and safe: every field that could carry key material is
/// a [`SecretSource`] or a credential enum whose own `Debug` redacts.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ExternalProfile {
    pub name: String,
    #[serde(flatten)]
    pub kind: ExternalKind,
}

/// Failures of profile loading and of opening a store from one.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("external profile JSON is malformed: {0}")]
    Malformed(String),
    #[error("external profile name is empty")]
    EmptyName,
    #[error("duplicate external profile name {name:?}")]
    DuplicateName { name: String },
    /// The secret could not be read. Names the kind of source, never the
    /// variable name or the path.
    #[error("the {kind} source for this profile's credential could not be read")]
    SecretUnavailable { kind: &'static str },
    /// A backend builder rejected this profile's credentials.
    ///
    /// The message is fixed per store kind and carries only the kind and the
    /// profile name. The builder's own error is dropped rather than wrapped,
    /// because it quotes what it was given: the service-account file path on
    /// GCS, the SAS token or account key on Azure, the credentials-file path
    /// on S3. Neither `Display` nor the derived `Debug` can reach it, so the
    /// error is safe to log and to return to a caller.
    #[error("{kind} credentials could not be loaded for profile {profile}")]
    CredentialsRejected { kind: &'static str, profile: String },
    #[error("opening the external store failed: {0}")]
    Backend(#[from] StoreError),
    /// An S3 profile's endpoint carries a path, query or fragment. The client
    /// sends every request under the endpoint as written, with `/<bucket>`
    /// appended when path-style, so a path would move the bucket the profile
    /// names under another one.
    #[error(
        "the S3 endpoint of profile {profile} carries a path; an endpoint names a host and \
         port, and the bucket is named separately"
    )]
    EndpointPath { profile: String },
}

/// Parse a JSON array of profiles.
///
/// Names are the profile half of every external object's identity, so a
/// duplicate is rejected rather than resolved by last-one-wins: two profiles
/// sharing a name make `(profile, bucket, key)` ambiguous, and the cache key
/// built from it ([`ravel_cache::CacheKey::pinned`]) would then hash two
/// different credential sets to one value.
///
/// [`ravel_cache::CacheKey::pinned`]: https://docs.rs/ravel-cache
pub fn load_profiles(json: &str) -> Result<Vec<ExternalProfile>, ProfileError> {
    let profiles: Vec<ExternalProfile> =
        serde_json::from_str(json).map_err(|e| ProfileError::Malformed(e.to_string()))?;
    let mut seen = std::collections::BTreeSet::new();
    for profile in &profiles {
        if profile.name.is_empty() {
            return Err(ProfileError::EmptyName);
        }
        if !seen.insert(profile.name.as_str()) {
            return Err(ProfileError::DuplicateName {
                name: profile.name.clone(),
            });
        }
    }
    Ok(profiles)
}

/// A read-only view of one bucket reached through one [`ExternalProfile`].
///
/// Every read method delegates; every mutating method refuses with
/// [`StoreError::ReadOnly`] without touching the network.
pub struct ExternalStore {
    profile: String,
    bucket: String,
    backend: Backend,
    /// Whether the backend accepts a suffix range: `object_store`'s Azure
    /// client refuses one with `NotSupported` before sending the request.
    suffix_range: bool,
}

enum Backend {
    /// S3 and S3-compatible endpoints reuse the full [`S3Store`] adapter, so an
    /// external S3 read gets the same bounded whole-object reads, error
    /// mapping and attempt counting as a read of Ravel's own bucket.
    S3(S3Store),
    /// GCS and Azure: a narrow adapter over `object_store`, reads only.
    Generic(GenericStore),
}

impl ExternalStore {
    /// Open `bucket` through `profile`, read-only.
    ///
    /// Resolves every [`SecretSource`] the profile carries once, here, so an
    /// unreadable secret fails at open rather than on the first read. The GCS
    /// service-account key file is the one credential Ravel does not read
    /// itself: its path goes to `object_store`'s builder.
    pub fn open(
        profile: &ExternalProfile,
        bucket: &str,
    ) -> Result<Arc<dyn ObjectStoreBackend>, ProfileError> {
        tracing::debug!(
            profile = %profile.name,
            kind = profile.kind.name(),
            bucket = %bucket,
            "opening a read-only external store"
        );
        if let ExternalKind::S3 {
            endpoint: Some(endpoint),
            ..
        } = &profile.kind
            && endpoint_carries_a_path(endpoint)
        {
            return Err(ProfileError::EndpointPath {
                profile: profile.name.clone(),
            });
        }
        let backend = match &profile.kind {
            ExternalKind::S3 {
                region,
                endpoint,
                force_path_style,
                allow_http,
                credentials,
            } => Backend::S3(open_s3(
                &profile.name,
                bucket,
                region,
                endpoint.as_deref(),
                *force_path_style,
                *allow_http,
                credentials,
            )?),
            ExternalKind::Gcs { credentials } => {
                Backend::Generic(open_gcs(&profile.name, bucket, credentials)?)
            }
            ExternalKind::Azure {
                account,
                credentials,
            } => Backend::Generic(open_azure(&profile.name, bucket, account, credentials)?),
        };
        Ok(Arc::new(ExternalStore {
            profile: profile.name.clone(),
            bucket: bucket.to_string(),
            backend,
            suffix_range: !matches!(profile.kind, ExternalKind::Azure { .. }),
        }))
    }

    /// The profile name this store was opened with: the first component of
    /// every object identity it serves.
    pub fn profile(&self) -> &str {
        &self.profile
    }

    /// The bucket (GCS bucket, Azure container) this store reads.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    fn refuse<T>(&self, operation: &str) -> Result<T, StoreError> {
        Err(StoreError::ReadOnly {
            operation: format!("{operation} in {}", self.bucket),
            store: format!("external profile {}", self.profile),
        })
    }
}

/// Whether an S3 endpoint URL has anything after its authority other than
/// trailing `/`s, which the client trims: a path, a query or a fragment.
fn endpoint_carries_a_path(endpoint: &str) -> bool {
    let rest = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest);
    let after = rest.find(['/', '?', '#']).map_or("", |at| &rest[at..]);
    !after.trim_end_matches('/').is_empty()
}

fn open_s3(
    profile: &str,
    bucket: &str,
    region: &str,
    endpoint: Option<&str>,
    force_path_style: bool,
    allow_http: bool,
    credentials: &S3ProfileCredentials,
) -> Result<S3Store, ProfileError> {
    let (access_key_id, secret_access_key, session_token, credentials_file, auth) =
        match credentials {
            S3ProfileCredentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => (
                access_key_id.resolve()?,
                secret_access_key.resolve()?,
                session_token
                    .as_ref()
                    .map(SecretSource::resolve)
                    .transpose()?,
                None,
                S3AuthMode::Static,
            ),
            S3ProfileCredentials::CredentialsFile { path } => (
                String::new(),
                String::new(),
                None,
                Some(path.clone()),
                S3AuthMode::Static,
            ),
            S3ProfileCredentials::InstanceRole => (
                String::new(),
                String::new(),
                None,
                None,
                S3AuthMode::InstanceRole,
            ),
        };
    let config = S3Config {
        bucket: bucket.to_string(),
        region: region.to_string(),
        endpoint: endpoint.map(str::to_string),
        access_key_id,
        secret_access_key,
        allow_http,
        force_path_style,
        // Server-side encryption is a write-time choice and this store never
        // writes. Decryption of a read is server-side and needs nothing here.
        kms_key_id: None,
        session_token,
        credentials_file,
        auth,
        instance_metadata_endpoint: None,
    };
    S3Store::new(config).map_err(|_| ProfileError::CredentialsRejected {
        kind: "s3",
        profile: profile.to_string(),
    })
}

fn open_gcs(
    profile: &str,
    bucket: &str,
    credentials: &GcsProfileCredentials,
) -> Result<GenericStore, ProfileError> {
    let builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
    let builder = match credentials {
        GcsProfileCredentials::ServiceAccount { path } => {
            builder.with_service_account_path(path.to_string_lossy().into_owned())
        }
        // `object_store` reads Application Default Credentials exactly when no
        // explicit service account is configured, so this arm sets nothing.
        GcsProfileCredentials::ApplicationDefault => builder,
    };
    let store = builder
        .build()
        .map_err(|_| ProfileError::CredentialsRejected {
            kind: "gcs",
            profile: profile.to_string(),
        })?;
    Ok(GenericStore {
        store: Arc::new(store),
    })
}

fn open_azure(
    profile: &str,
    container: &str,
    account: &str,
    credentials: &AzureProfileCredentials,
) -> Result<GenericStore, ProfileError> {
    let builder = MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_container_name(container);
    let builder = match credentials {
        AzureProfileCredentials::AccessKey { key } => builder.with_access_key(key.resolve()?),
        AzureProfileCredentials::SasToken { token } => {
            builder.with_config(AzureConfigKey::SasKey, token.resolve()?)
        }
    };
    let store = builder
        .build()
        .map_err(|_| ProfileError::CredentialsRejected {
            kind: "azure",
            profile: profile.to_string(),
        })?;
    Ok(GenericStore {
        store: Arc::new(store),
    })
}

/// `object_store` metadata to [`ObjectMeta`], for the GCS/Azure path.
///
/// The ETag is passed through byte for byte, quotes and all: it is the pin a
/// later conditional read sends back as `If-Match`, and normalizing it here
/// would make that read compare a string the store never issued. `version` is
/// the store's own version or generation when it reports one; for a store that
/// does not version objects it falls back to the ETag, which is what
/// [`crate::s3::S3Store`] reports too. That fallback is a CAS token only:
/// [`crate::Pin::from_store`] drops a version equal to the ETag, so a pinned
/// read of an unversioned object carries the ETag precondition alone.
fn map_external_meta(meta: object_store::ObjectMeta) -> Result<ObjectMeta, StoreError> {
    let etag = meta.e_tag.clone().ok_or_else(|| {
        StoreError::Permanent(format!("the store returned no ETag for {}", meta.location))
    })?;
    let version = meta.version.clone().unwrap_or_else(|| etag.clone());
    Ok(ObjectMeta {
        key: meta.location.to_string(),
        size: meta.size,
        etag: Etag(etag),
        version: Version(version),
        last_modified_unix_ms: meta.last_modified.timestamp_millis(),
    })
}

/// Error mapping for the GCS/Azure path: [`crate::s3::map_error_common`],
/// except that a 404 or a list failure whose body names Azure's
/// `ContainerNotFound` is [`StoreError::Permanent`], because a missing
/// container is not a missing blob. GCS needs nothing here: its XML API answers
/// a missing bucket with S3's `NoSuchBucket`, which the common mapping already
/// reads as `Permanent`. A HEAD 404 has no body and stays `NotFound` on both.
fn map_external_error(e: object_store::Error) -> StoreError {
    missing_container(&e).unwrap_or_else(|| crate::s3::map_error_common(e))
}

/// [`map_external_error`] for a get, which also recognizes an unsatisfiable
/// range (see [`crate::s3::map_get_error`]).
fn map_external_get_error(e: object_store::Error) -> StoreError {
    missing_container(&e).unwrap_or_else(|| crate::s3::map_get_error(e))
}

/// `object_store` reports the get 404 as `NotFound` and the list 404 as
/// `Generic`; the body, and so the code, survives in the text of both. The body
/// is left out of the message: it can echo the request.
fn missing_container(e: &object_store::Error) -> Option<StoreError> {
    let (context, source) = match e {
        object_store::Error::NotFound { path, source } => (path.as_str(), source),
        object_store::Error::Generic { store, source } => (*store, source),
        _ => return None,
    };
    (crate::s3::s3_error_code(source.as_ref()).as_deref() == Some("ContainerNotFound")).then(|| {
        StoreError::Permanent(format!(
            "{context}: Azure container does not exist (ContainerNotFound)"
        ))
    })
}

/// The read half of `object_store`'s API, for the backends Ravel reaches only
/// through an external grant. Not an [`ObjectStoreBackend`] itself: it has no
/// write path to implement, and giving it one that refuses would put the
/// read-only decision in two places.
struct GenericStore {
    store: Arc<dyn OsObjectStore>,
}

impl GenericStore {
    async fn get(
        &self,
        key: &str,
        range: GetRange,
        pin: Option<&Pin>,
    ) -> Result<crate::PinnedRead, StoreError> {
        let path = crate::s3::path_of(key)?;
        let os_range = match range {
            GetRange::Full => None,
            GetRange::Range(start, end) => {
                if start >= end {
                    return Err(StoreError::InvalidRange(format!(
                        "empty or inverted range [{start}, {end})"
                    )));
                }
                Some(OsGetRange::Bounded(start..end))
            }
            GetRange::Suffix(0) => {
                return Err(StoreError::InvalidRange("zero-length suffix".into()));
            }
            GetRange::Suffix(n) => Some(OsGetRange::Suffix(n)),
        };
        let result = self
            .store
            .get_opts(
                &path,
                OsGetOptions {
                    range: os_range,
                    if_match: pin.map(|pin| pin.etag.clone()),
                    version: pin.and_then(|pin| pin.version.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(map_external_get_error)?;
        let etag = result.meta.e_tag.clone().ok_or_else(|| {
            StoreError::Permanent(format!("the store returned no ETag for {key}"))
        })?;
        let reported_version = result.meta.version.clone();
        let version = reported_version.clone().unwrap_or_else(|| etag.clone());
        let total_size = result.meta.size;
        let data = result.bytes().await.map_err(map_external_error)?;
        Ok(crate::PinnedRead {
            pin: Pin::from_store(etag.clone(), reported_version),
            outcome: GetOutcome {
                data,
                etag: Etag(etag),
                version: Version(version),
                total_size,
            },
        })
    }

    /// The pin for `key`, from a HEAD: the ETag as the precondition and the
    /// store's own version or generation as the selector when it reports one.
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        let raw = self
            .store
            .head(&crate::s3::path_of(key)?)
            .await
            .map_err(map_external_error)?;
        let reported_version = raw.version.clone();
        let meta = map_external_meta(raw)?;
        let pin = Pin::from_store(meta.etag.0.clone(), reported_version);
        Ok((meta, pin))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let meta = self
            .store
            .head(&crate::s3::path_of(key)?)
            .await
            .map_err(map_external_error)?;
        map_external_meta(meta)
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        use futures::StreamExt;

        let prefix_path = crate::s3::prefix_of(prefix);
        let offset = match (&page, start_after) {
            (Some(PageToken(after)), _) => Some(OsPath::from(after.as_str())),
            (None, Some(after)) => Some(OsPath::from(after)),
            (None, None) => None,
        };
        let mut stream = match &offset {
            Some(offset) => self.store.list_with_offset(prefix_path.as_ref(), offset),
            None => self.store.list(prefix_path.as_ref()),
        };
        let mut out = Vec::with_capacity(EXTERNAL_PAGE_SIZE.min(1024));
        while out.len() < EXTERNAL_PAGE_SIZE {
            match stream.next().await {
                Some(Ok(meta)) => out.push(map_external_meta(meta)?),
                Some(Err(e)) => return Err(map_external_error(e)),
                None => break,
            }
        }
        let next = if out.len() == EXTERNAL_PAGE_SIZE {
            out.last().map(|m| PageToken(m.key.clone()))
        } else {
            None
        };
        let (objects, unaddressable) = crate::classify_objects(prefix, out);
        Ok(ListPage {
            objects,
            next,
            unaddressable,
        })
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let prefix_path = crate::s3::prefix_of(prefix);
        let result = self
            .store
            .list_with_delimiter(prefix_path.as_ref())
            .await
            .map_err(map_external_error)?;
        let listed = result
            .objects
            .into_iter()
            .map(map_external_meta)
            .collect::<Result<Vec<_>, _>>()?;
        let listed_prefixes = result
            .common_prefixes
            .into_iter()
            .map(|p| format!("{p}/"))
            .collect();
        let (objects, unaddressable) = crate::classify_objects(prefix, listed);
        let (common_prefixes, unaddressable_prefixes) =
            crate::classify_prefixes(prefix, listed_prefixes);
        Ok(DelimitedList {
            objects,
            common_prefixes,
            unaddressable,
            unaddressable_prefixes,
        })
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for ExternalStore {
    async fn put(
        &self,
        key: &str,
        _data: Bytes,
        _opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        crate::s3::path_of(key)?;
        self.refuse(&format!("put of {key}"))
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        crate::s3::path_of(key)?;
        self.refuse(&format!("multipart upload of {key}"))
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        crate::s3::path_of(key)?;
        self.refuse(&format!("delete of {key}"))
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.get(key, range).await,
            Backend::Generic(store) => store.get(key, range, None).await.map(|read| read.outcome),
        }
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<crate::PinnedRead, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.get_pinned(key, range, pin).await,
            Backend::Generic(store) => store.get(key, range, Some(pin)).await,
        }
    }

    async fn get_with_pin(
        &self,
        key: &str,
        range: GetRange,
    ) -> Result<crate::PinnedRead, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.get_with_pin(key, range).await,
            Backend::Generic(store) => store.get(key, range, None).await,
        }
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        match &self.backend {
            Backend::S3(store) => store.pin_of(key).await,
            Backend::Generic(store) => store.pin_of(key).await,
        }
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.head(key).await,
            Backend::Generic(store) => store.head(key).await,
        }
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.list(prefix, page).await,
            Backend::Generic(store) => store.list(prefix, None, page).await,
        }
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.list_after(prefix, start_after, page).await,
            Backend::Generic(store) => store.list(prefix, start_after, page).await,
        }
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.list_delimited(prefix).await,
            Backend::Generic(store) => store.list_delimited(prefix).await,
        }
    }

    /// Reads only. The write-side flags are false because this store refuses
    /// every write, not because the underlying bucket lacks the capability:
    /// a caller choosing a write path by capability must not pick one here.
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            consistent_read: true,
            consistent_list: true,
            create_if_absent: false,
            cas_version: false,
            suffix_range: self.suffix_range,
            upload_checksum: false,
            prefix_list: true,
            multipart: false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A distinctive string planted in every place a profile could leak one:
    /// the environment variable's name, the secret file's path, and the file's
    /// contents.
    const MARKER: &str = "TOPSECRET-MARKER";

    fn secret_file(dir: &tempfile::TempDir) -> PathBuf {
        let path = dir.path().join(format!("{MARKER}.key"));
        std::fs::write(&path, format!("{MARKER}-contents\n")).expect("write the secret file");
        path
    }

    fn s3_profile(path: &std::path::Path) -> ExternalProfile {
        ExternalProfile {
            name: "lake".to_string(),
            kind: ExternalKind::S3 {
                region: "us-east-1".to_string(),
                endpoint: Some("http://127.0.0.1:1".to_string()),
                force_path_style: true,
                allow_http: true,
                credentials: S3ProfileCredentials::Static {
                    access_key_id: SecretSource::File {
                        path: path.to_path_buf(),
                    },
                    secret_access_key: SecretSource::File {
                        path: path.to_path_buf(),
                    },
                    session_token: None,
                },
            },
        }
    }

    fn assert_read_only(err: &StoreError, expect_in_operation: &str) {
        match err {
            StoreError::ReadOnly { operation, store } => {
                assert!(
                    operation.contains(expect_in_operation),
                    "operation {operation:?} does not name {expect_in_operation:?}"
                );
                assert!(
                    store.contains("lake"),
                    "store {store:?} does not name the profile"
                );
            }
            other => panic!("expected a read-only refusal, got {other:?}"),
        }
        assert!(
            !err.is_retryable(),
            "a read-only refusal must never be retried"
        );
    }

    /// Every mutating method refuses, and refuses locally: the endpoint these
    /// profiles name is a closed port, so a call that reached the network would
    /// surface as a transport error instead.
    #[tokio::test]
    async fn every_mutating_call_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = secret_file(&dir);
        let store = ExternalStore::open(&s3_profile(&path), "customer-lake").expect("open");

        let err = store
            .put("a/b", Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect_err("put must be refused");
        assert_read_only(&err, "put of a/b");

        let err = store
            .put(
                "a/b",
                Bytes::from_static(b"x"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect_err("a conditional put must be refused too");
        assert_read_only(&err, "put of a/b");

        let err = store
            .put_multipart("a/b")
            .await
            .err()
            .expect("multipart must be refused");
        assert_read_only(&err, "multipart upload of a/b");

        let err = store
            .delete("a/b")
            .await
            .expect_err("delete must be refused");
        assert_read_only(&err, "delete of a/b");
    }

    #[tokio::test]
    async fn the_capability_set_advertises_no_write_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = secret_file(&dir);
        let store = ExternalStore::open(&s3_profile(&path), "customer-lake").expect("open");
        let caps = store.capabilities();
        assert!(!caps.create_if_absent);
        assert!(!caps.cas_version);
        assert!(!caps.upload_checksum);
        assert!(!caps.multipart);
        assert!(caps.consistent_read && caps.prefix_list && caps.suffix_range);

        let sas = dir.path().join("azure.sas");
        std::fs::write(&sas, "sv=2024-11-04&sig=unused\n").expect("write the SAS file");
        let azure = ExternalProfile {
            name: "archive".to_string(),
            kind: ExternalKind::Azure {
                account: "contoso".to_string(),
                credentials: AzureProfileCredentials::SasToken {
                    token: SecretSource::File { path: sas },
                },
            },
        };
        let store = ExternalStore::open(&azure, "exports").expect("open");
        let caps = store.capabilities();
        assert!(caps.consistent_read && caps.prefix_list);
        assert!(
            !caps.suffix_range,
            "object_store's Azure client refuses a suffix range"
        );
    }

    /// An S3 endpoint with a path is refused before any secret is read (the
    /// secret file here does not exist), whatever the addressing style: a
    /// path-style `http://ravel-host/ravel-bucket` with bucket `t` would read
    /// Ravel's keys under `t/`. Trailing slashes, which the client trims, are
    /// not a path.
    #[test]
    fn an_s3_endpoint_with_a_path_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("missing");
        for endpoint in [
            "http://ravel-host/ravel-bucket",
            "http://ravel-host:9000/ravel-bucket/",
            "https://ravel-host/?x=1",
            "https://ravel-host#f",
            "ravel-host/ravel-bucket",
        ] {
            for force_path_style in [true, false] {
                let mut profile = s3_profile(&missing);
                if let ExternalKind::S3 {
                    endpoint: e,
                    force_path_style: style,
                    ..
                } = &mut profile.kind
                {
                    *e = Some(endpoint.to_string());
                    *style = force_path_style;
                }
                let err = ExternalStore::open(&profile, "t").err().expect("refused");
                assert!(
                    matches!(&err, ProfileError::EndpointPath { profile } if profile == "lake"),
                    "{endpoint} {force_path_style}: {err:?}"
                );
                assert!(!err.to_string().contains("ravel-bucket"), "{err}");
            }
        }
        for endpoint in [
            "http://ravel-host",
            "http://ravel-host/",
            "http://ravel-host:9000//",
        ] {
            assert!(!endpoint_carries_a_path(endpoint), "{endpoint}");
        }
        let path = secret_file(&dir);
        let mut profile = s3_profile(&path);
        if let ExternalKind::S3 { endpoint: e, .. } = &mut profile.kind {
            *e = Some("http://127.0.0.1:1/".to_string());
        }
        ExternalStore::open(&profile, "t").expect("a trailing slash opens");
    }

    #[test]
    fn profiles_parse_for_every_kind() {
        let json = r#"[
            {
              "name": "lake",
              "kind": "s3",
              "region": "eu-west-1",
              "endpoint": "https://minio.example:9000",
              "force_path_style": true,
              "allow_http": false,
              "credentials": {
                "mode": "static",
                "access_key_id": { "from": "env", "name": "LAKE_KEY_ID" },
                "secret_access_key": { "from": "file", "path": "/run/secrets/lake" }
              }
            },
            {
              "name": "warehouse",
              "kind": "gcs",
              "credentials": { "mode": "service_account", "path": "/run/secrets/sa.json" }
            },
            {
              "name": "archive",
              "kind": "azure",
              "account": "contoso",
              "credentials": {
                "mode": "sas_token",
                "token": { "from": "env", "name": "ARCHIVE_SAS" }
              }
            }
        ]"#;
        let profiles = load_profiles(json).expect("three well-formed profiles");
        assert_eq!(profiles.len(), 3);
        assert_eq!(
            profiles.iter().map(|p| p.kind.name()).collect::<Vec<_>>(),
            vec!["s3", "gcs", "azure"]
        );
        match &profiles[0].kind {
            ExternalKind::S3 {
                region,
                endpoint,
                force_path_style,
                allow_http,
                ..
            } => {
                assert_eq!(region, "eu-west-1");
                assert_eq!(endpoint.as_deref(), Some("https://minio.example:9000"));
                assert!(force_path_style);
                assert!(!allow_http);
            }
            other => panic!("expected an s3 profile, got {other:?}"),
        }
    }

    #[test]
    fn a_duplicate_profile_name_is_rejected() {
        let json = r#"[
            { "name": "lake", "kind": "gcs", "credentials": { "mode": "application_default" } },
            { "name": "lake", "kind": "gcs", "credentials": { "mode": "application_default" } }
        ]"#;
        match load_profiles(json) {
            Err(ProfileError::DuplicateName { name }) => assert_eq!(name, "lake"),
            other => panic!("expected a duplicate-name refusal, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_profile_name_is_rejected() {
        let json =
            r#"[{ "name": "", "kind": "gcs", "credentials": { "mode": "application_default" } }]"#;
        assert!(matches!(load_profiles(json), Err(ProfileError::EmptyName)));
    }

    /// A profile never renders a secret, the name of the variable holding one,
    /// or the path of the file holding one, in `Debug` or in any error it
    /// produces. Asserted on the formatted string rather than by inspection,
    /// because `Debug` is what a `tracing` field and a panic message call.
    #[test]
    fn debug_renders_no_secret_and_no_secret_location() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = secret_file(&dir);
        let profiles = [
            s3_profile(&path),
            ExternalProfile {
                name: "env-keyed".to_string(),
                kind: ExternalKind::S3 {
                    region: "us-east-1".to_string(),
                    endpoint: None,
                    force_path_style: false,
                    allow_http: false,
                    credentials: S3ProfileCredentials::Static {
                        access_key_id: SecretSource::Env {
                            name: format!("{MARKER}_ID"),
                        },
                        secret_access_key: SecretSource::Env {
                            name: format!("{MARKER}_SECRET"),
                        },
                        session_token: Some(SecretSource::Env {
                            name: format!("{MARKER}_TOKEN"),
                        }),
                    },
                },
            },
            ExternalProfile {
                name: "warehouse".to_string(),
                kind: ExternalKind::Gcs {
                    credentials: GcsProfileCredentials::ServiceAccount { path: path.clone() },
                },
            },
            ExternalProfile {
                name: "archive".to_string(),
                kind: ExternalKind::Azure {
                    account: "contoso".to_string(),
                    credentials: AzureProfileCredentials::SasToken {
                        token: SecretSource::File { path: path.clone() },
                    },
                },
            },
            ExternalProfile {
                name: "archive-key".to_string(),
                kind: ExternalKind::Azure {
                    account: "contoso".to_string(),
                    credentials: AzureProfileCredentials::AccessKey {
                        key: SecretSource::Env {
                            name: format!("{MARKER}_AZURE"),
                        },
                    },
                },
            },
        ];

        for profile in &profiles {
            let rendered = format!("{profile:?}");
            assert!(
                !rendered.contains(MARKER),
                "{} rendered a secret location: {rendered}",
                profile.name
            );
            assert!(
                rendered.contains("redacted"),
                "{} must say that something was redacted: {rendered}",
                profile.name
            );
            // The account name and the profile name are not secrets, and an
            // operator needs them to identify what was redacted.
            assert!(rendered.contains(&profile.name));
        }

        // `GcsProfileCredentials::ServiceAccount` carries a path and no
        // `SecretSource`, so its own `Debug` is the only thing between that
        // path and a log line.
        let gcs = GcsProfileCredentials::ServiceAccount { path: path.clone() };
        assert_eq!(
            format!("{gcs:?}"),
            "GcsCredentials(service_account, redacted)"
        );
    }

    /// Resolving a missing secret fails, and the error names the kind of source
    /// and nothing more: the message itself travels into logs.
    #[test]
    fn an_unreadable_secret_fails_without_naming_its_location() {
        let source = SecretSource::File {
            path: PathBuf::from(format!("/nonexistent/{MARKER}")),
        };
        let err = source.resolve().expect_err("the file does not exist");
        assert!(matches!(
            err,
            ProfileError::SecretUnavailable { kind: "file" }
        ));
        let rendered = format!("{err} / {err:?}");
        assert!(!rendered.contains(MARKER), "leaked the path: {rendered}");
    }

    /// The GCS service-account file is the one credential Ravel hands to
    /// `object_store` as a path instead of reading itself, and the builder
    /// reads it during `build`. Its error quotes the path it was given, so
    /// wrapping that error would put a filesystem path into every log line
    /// that reports a failed open. The message is fixed per store kind and
    /// the source error is dropped, which is what this asserts: neither
    /// `Display` nor `Debug` can reach the path.
    #[test]
    fn a_gcs_open_failure_names_the_kind_and_the_profile_and_nothing_else() {
        let profile = ExternalProfile {
            name: "lake".into(),
            kind: ExternalKind::Gcs {
                credentials: GcsProfileCredentials::ServiceAccount {
                    path: PathBuf::from(format!("/nonexistent/{MARKER}/service-account.json")),
                },
            },
        };

        let err = ExternalStore::open(&profile, "some-bucket")
            .err()
            .expect("the service account file does not exist");
        assert!(
            matches!(
                err,
                ProfileError::CredentialsRejected {
                    kind: "gcs",
                    ref profile
                } if profile == "lake"
            ),
            "got {err:?}"
        );
        assert_eq!(
            err.to_string(),
            "gcs credentials could not be loaded for profile lake"
        );
        let rendered = format!("{err} / {err:?}");
        assert!(!rendered.contains(MARKER), "leaked the path: {rendered}");
        assert!(
            !rendered.contains("service-account"),
            "leaked the file name: {rendered}"
        );
    }

    #[test]
    fn a_file_secret_is_read_with_its_trailing_newline_trimmed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("key");
        std::fs::write(&path, "abc123\n").expect("write");
        let source = SecretSource::File { path };
        assert_eq!(source.resolve().expect("resolve"), "abc123");
    }

    /// The ETag reaches [`ObjectMeta`] byte for byte, quotes included: it is
    /// the value a later `get_pinned` sends back as `If-Match`, and any
    /// normalization here would make that read compare a string the store never
    /// issued.
    #[test]
    fn external_metadata_passes_the_etag_through_verbatim() {
        let meta = map_external_meta(object_store::ObjectMeta {
            location: object_store::path::Path::from("a/b.parquet"),
            last_modified: Default::default(),
            size: 17,
            e_tag: Some("\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string()),
            version: Some("gen-42".to_string()),
        })
        .expect("an object with an ETag maps");
        assert_eq!(meta.etag.0, "\"9a0364b9e99bb480dd25e1f0284c8555\"");
        assert_eq!(meta.version.0, "gen-42");
        assert_eq!(meta.size, 17);
        assert_eq!(meta.key, "a/b.parquet");

        // No version reported: the fallback fills ObjectMeta's CAS version
        // token with the ETag. It does not reach a pin: `Pin::from_store`
        // drops a version equal to the ETag, so a pinned read of an
        // unversioned object is precondition-only.
        let meta = map_external_meta(object_store::ObjectMeta {
            location: object_store::path::Path::from("a/b.parquet"),
            last_modified: Default::default(),
            size: 17,
            e_tag: Some("\"abc\"".to_string()),
            version: None,
        })
        .expect("an unversioned object maps");
        assert_eq!(meta.version.0, "\"abc\"");

        // No ETag at all: there is no pin to record, so this is an error rather
        // than a silently unpinnable object.
        let err = map_external_meta(object_store::ObjectMeta {
            location: object_store::path::Path::from("a/b.parquet"),
            last_modified: Default::default(),
            size: 17,
            e_tag: None,
            version: None,
        })
        .expect_err("an object with no ETag cannot be pinned");
        assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");
    }

    /// Azure's 404 for a blob in a container that does not exist.
    const AZURE_NO_CONTAINER: &str = "\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?>\
        <Error><Code>ContainerNotFound</Code><Message>The specified container does not \
        exist.\nRequestId:0\nTime:2026-10-01T00:00:00.0000000Z</Message></Error>";
    /// Azure's 404 for a blob that does not exist in a container that does.
    const AZURE_NO_BLOB: &str = "\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?>\
        <Error><Code>BlobNotFound</Code><Message>The specified blob does not \
        exist.\nRequestId:0\nTime:2026-10-01T00:00:00.0000000Z</Message></Error>";
    /// The GCS XML API's 404 for a bucket that does not exist.
    const GCS_NO_BUCKET: &str = "<?xml version='1.0' encoding='UTF-8'?>\
        <Error><Code>NoSuchBucket</Code><Message>The specified bucket does not \
        exist.</Message></Error>";
    /// The GCS XML API's 404 for an object that does not exist.
    const GCS_NO_OBJECT: &str = "<?xml version='1.0' encoding='UTF-8'?>\
        <Error><Code>NoSuchKey</Code><Message>The specified key does not \
        exist.</Message></Error>";

    /// A loopback endpoint answering every request with a 404 carrying `body`,
    /// and the method of every request it saw.
    async fn fake_404(body: &'static str) -> (String, Arc<parking_lot::Mutex<Vec<String>>>) {
        fake_status(axum::http::StatusCode::NOT_FOUND, body, Duration::ZERO).await
    }

    /// A loopback endpoint answering every request with `status` and `body`
    /// after `delay`, and the method of every request it saw.
    async fn fake_status(
        status: axum::http::StatusCode,
        body: &'static str,
        delay: Duration,
    ) -> (String, Arc<parking_lot::Mutex<Vec<String>>>) {
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&seen);
        let app = axum::Router::new().fallback(move |method: axum::http::Method| {
            let recorded = Arc::clone(&recorded);
            async move {
                recorded.lock().push(method.to_string());
                tokio::time::sleep(delay).await;
                (
                    status,
                    [(axum::http::header::CONTENT_TYPE, "application/xml")],
                    body,
                )
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the fake endpoint must bind a loopback port");
        let addr = listener.local_addr().expect("a bound listener's address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), seen)
    }

    fn generic(store: impl OsObjectStore, suffix_range: bool) -> ExternalStore {
        ExternalStore {
            profile: "lake".to_string(),
            bucket: "exports".to_string(),
            backend: Backend::Generic(GenericStore {
                store: Arc::new(store),
            }),
            suffix_range,
        }
    }

    fn azure_at(endpoint: String) -> ExternalStore {
        let store = MicrosoftAzureBuilder::new()
            .with_account("devstoreaccount1")
            .with_container_name("exports")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_skip_signature(true)
            .build()
            .expect("an Azure store against the fake endpoint builds");
        generic(store, false)
    }

    fn gcs_at(endpoint: String) -> ExternalStore {
        let store = GoogleCloudStorageBuilder::new()
            .with_bucket_name("exports")
            .with_base_url(&endpoint)
            .with_client_options(object_store::ClientOptions::new().with_allow_http(true))
            .with_skip_signature(true)
            .build()
            .expect("a GCS store against the fake endpoint builds");
        generic(store, true)
    }

    fn assert_hard_error<T: std::fmt::Debug>(
        result: Result<T, StoreError>,
        operation: &str,
        code: &str,
    ) {
        match result {
            Err(StoreError::Permanent(message)) => assert!(
                message.contains(code),
                "{operation}: the error {message:?} does not name {code}"
            ),
            other => panic!("{operation}: expected Permanent naming {code}, got {other:?}"),
        }
    }

    fn assert_not_found<T: std::fmt::Debug>(result: Result<T, StoreError>, operation: &str) {
        assert!(
            matches!(result, Err(StoreError::NotFound)),
            "{operation}: expected NotFound, got {result:?}"
        );
    }

    /// Every operation that sends a GET against a missing container or bucket
    /// reads it as a hard error, through the real `object_store` client: the
    /// code survives in the body. A HEAD response has no body, so `head` and
    /// `pin_of` still read `NotFound`, with exactly one HEAD each.
    async fn assert_missing_bucket_is_hard(store: &ExternalStore, code: &str) {
        let pin = Pin::from_store("\"e\"", None);
        assert_hard_error(store.get("k", GetRange::Full).await, "get", code);
        assert_hard_error(store.get("k", GetRange::Range(0, 4)).await, "get", code);
        assert_hard_error(
            store.get_with_pin("k", GetRange::Full).await,
            "get_with_pin",
            code,
        );
        assert_hard_error(
            store.get_pinned("k", GetRange::Full, &pin).await,
            "get_pinned",
            code,
        );
        assert_hard_error(store.list("p/", None).await, "list", code);
        assert_hard_error(
            store.list_after("p/", Some("p/a"), None).await,
            "list_after",
            code,
        );
        assert_hard_error(store.list_delimited("p/").await, "list_delimited", code);
    }

    #[tokio::test]
    async fn a_missing_azure_container_is_a_hard_error() {
        let (endpoint, seen) = fake_404(AZURE_NO_CONTAINER).await;
        let store = azure_at(endpoint);
        assert_missing_bucket_is_hard(&store, "ContainerNotFound").await;
        assert!(seen.lock().iter().all(|method| method == "GET"), "{seen:?}");

        seen.lock().clear();
        assert_not_found(store.head("k").await, "head");
        assert_not_found(store.pin_of("k").await, "pin_of");
        assert_eq!(*seen.lock(), ["HEAD", "HEAD"]);
    }

    #[tokio::test]
    async fn a_missing_gcs_bucket_is_a_hard_error() {
        let (endpoint, seen) = fake_404(GCS_NO_BUCKET).await;
        let store = gcs_at(endpoint);
        assert_missing_bucket_is_hard(&store, "NoSuchBucket").await;
        assert!(seen.lock().iter().all(|method| method == "GET"), "{seen:?}");

        seen.lock().clear();
        assert_not_found(store.head("k").await, "head");
        assert_not_found(store.pin_of("k").await, "pin_of");
        assert_eq!(*seen.lock(), ["HEAD", "HEAD"]);
    }

    /// A missing blob or object is still `NotFound` on every read of a key.
    #[tokio::test]
    async fn a_missing_blob_or_object_is_still_not_found() {
        for (kind, body) in [("azure", AZURE_NO_BLOB), ("gcs", GCS_NO_OBJECT)] {
            let (endpoint, _) = fake_404(body).await;
            let store = match kind {
                "azure" => azure_at(endpoint),
                _ => gcs_at(endpoint),
            };
            let pin = Pin::from_store("\"e\"", None);
            assert_not_found(store.get("k", GetRange::Full).await, kind);
            assert_not_found(store.get("k", GetRange::Range(0, 4)).await, kind);
            assert_not_found(store.get_with_pin("k", GetRange::Full).await, kind);
            assert_not_found(store.get_pinned("k", GetRange::Full, &pin).await, kind);
            assert_not_found(store.head("k").await, kind);
            assert_not_found(store.pin_of("k").await, kind);
        }
    }

    /// A GCS store against `endpoint` that retries once after 1 ms and times a
    /// request out after `timeout`, so a throttle or a stall surfaces in
    /// milliseconds instead of after `object_store`'s default retry budget.
    fn gcs_fast_at(endpoint: String, timeout: Duration) -> ExternalStore {
        let retry = object_store::RetryConfig {
            backoff: object_store::BackoffConfig {
                init_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                base: 2.0,
            },
            max_retries: 1,
            retry_timeout: Duration::from_secs(30),
        };
        let store = GoogleCloudStorageBuilder::new()
            .with_bucket_name("exports")
            .with_base_url(&endpoint)
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout(timeout),
            )
            .with_retry(retry)
            .with_skip_signature(true)
            .build()
            .expect("a GCS store against the fake endpoint builds");
        generic(store, true)
    }

    /// Through the real `object_store` client, a 400 on a get and a 404 on a
    /// list whose request URL carries 503 and 429 are not throttles: a throttle
    /// is read from the status's reason phrase, not from digits in the key or
    /// the query.
    #[tokio::test]
    async fn digits_in_the_request_url_are_not_a_throttle() {
        let (endpoint, _) =
            fake_status(axum::http::StatusCode::BAD_REQUEST, "", Duration::ZERO).await;
        let store = gcs_fast_at(endpoint, Duration::from_secs(30));
        match store.get("k503/k429", GetRange::Full).await {
            Err(StoreError::Transient(message)) => {
                assert!(
                    message.contains("/exports/k503%2Fk429 in ")
                        && message.contains("Server returned non-2xx status code: 400 Bad Request"),
                    "the text must carry both the digits and the real status: {message:?}"
                );
            }
            other => panic!("a 400 must read Transient, got {other:?}"),
        }

        let (endpoint, _) = fake_404(GCS_NO_OBJECT).await;
        let store = gcs_fast_at(endpoint, Duration::from_secs(30));
        match store.list("p503/p429/", None).await {
            Err(StoreError::Transient(message)) => {
                assert!(
                    message.contains("p503%2Fp429%2F")
                        && message.contains("Server returned non-2xx status code: 404 Not Found"),
                    "the text must carry both the digits and the real status: {message:?}"
                );
            }
            other => panic!("a list 404 must read Transient, got {other:?}"),
        }
    }

    /// Through the real `object_store` client, an exhausted 429 or 503 reads
    /// Throttled even with the other code in its URL and the exhausted-retry
    /// suffix, whose `retry_timeout` carries "timeout", in the text; and a
    /// request that times out reads Timeout even with both codes in its URL.
    #[tokio::test]
    async fn a_real_throttle_and_a_real_timeout_keep_their_class() {
        for (status, key) in [
            (axum::http::StatusCode::TOO_MANY_REQUESTS, "k503"),
            (axum::http::StatusCode::SERVICE_UNAVAILABLE, "k429"),
        ] {
            let (endpoint, seen) = fake_status(status, "", Duration::ZERO).await;
            let store = gcs_fast_at(endpoint, Duration::from_secs(30));
            let result = store.get(key, GetRange::Full).await;
            assert!(
                matches!(
                    result,
                    Err(StoreError::Throttled {
                        retry_after_ms: 1000
                    })
                ),
                "{status}: got {result:?}"
            );
            assert_eq!(seen.lock().len(), 2, "{status}: one attempt and one retry");
        }

        let (endpoint, _) =
            fake_status(axum::http::StatusCode::OK, "", Duration::from_secs(30)).await;
        let store = gcs_fast_at(endpoint, Duration::from_millis(100));
        let result = store.get("k503/k429", GetRange::Full).await;
        assert!(
            matches!(result, Err(StoreError::Timeout)),
            "a stalled request must read Timeout, got {result:?}"
        );
    }

    /// Through the real `object_store` client, a 500 that exhausted its retry
    /// reads Transient: the exhausted-retry suffix's `retry_timeout` field name
    /// is not a timeout.
    #[tokio::test]
    async fn an_exhausted_server_error_reads_transient_not_timeout() {
        let (endpoint, seen) = fake_status(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "",
            Duration::ZERO,
        )
        .await;
        let store = gcs_fast_at(endpoint, Duration::from_secs(30));
        let result = store.get("k", GetRange::Full).await;
        match &result {
            Err(StoreError::Transient(message)) => assert!(
                message.contains("retry_timeout"),
                "the text must carry the exhausted-retry suffix: {message}"
            ),
            other => panic!("an exhausted 500 must read Transient, got {other:?}"),
        }
        assert_eq!(seen.lock().len(), 2, "one attempt and one retry");
    }

    /// Through the real `object_store` client, an exhausted 500 and a 400 on a
    /// key spelled with timeout and throttle words read Transient: the key
    /// appears in the request URI and in the wrapper text, and neither is a
    /// class signal.
    #[tokio::test]
    async fn class_words_in_the_key_are_not_a_class() {
        let key = "timeout/deadline/throttled/slowdown/slow down";
        for status in [
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::http::StatusCode::BAD_REQUEST,
        ] {
            let (endpoint, _) = fake_status(status, "", Duration::ZERO).await;
            let store = gcs_fast_at(endpoint, Duration::from_secs(30));
            match store.get(key, GetRange::Full).await {
                Err(StoreError::Transient(message)) => assert!(
                    message.contains("timeout%2Fdeadline%2Fthrottled%2Fslowdown"),
                    "{status}: the text must carry the key: {message}"
                ),
                other => panic!("{status}: must read Transient, got {other:?}"),
            }
        }
    }
}
