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
//! Nothing in a shipping binary constructs an [`ExternalStore`] yet. The
//! callers are the ravel-parquet reader (#2052) and the grant CLI and
//! `CREATE EXTERNAL TABLE` paths (#2051, #2054).

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
        /// deployment.
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
    #[error("opening the external store failed: {0}")]
    Backend(#[from] StoreError),
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
        let backend = match &profile.kind {
            ExternalKind::S3 {
                region,
                endpoint,
                force_path_style,
                allow_http,
                credentials,
            } => Backend::S3(open_s3(
                bucket,
                region,
                endpoint.as_deref(),
                *force_path_style,
                *allow_http,
                credentials,
            )?),
            ExternalKind::Gcs { credentials } => Backend::Generic(open_gcs(bucket, credentials)?),
            ExternalKind::Azure {
                account,
                credentials,
            } => Backend::Generic(open_azure(bucket, account, credentials)?),
        };
        Ok(Arc::new(ExternalStore {
            profile: profile.name.clone(),
            bucket: bucket.to_string(),
            backend,
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

fn open_s3(
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
    Ok(S3Store::new(config)?)
}

fn open_gcs(
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
        .map_err(|e| ProfileError::Backend(StoreError::Permanent(e.to_string())))?;
    Ok(GenericStore {
        store: Arc::new(store),
    })
}

fn open_azure(
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
        .map_err(|e| ProfileError::Backend(StoreError::Permanent(e.to_string())))?;
    Ok(GenericStore {
        store: Arc::new(store),
    })
}

/// `object_store` metadata to [`ObjectMeta`], for the GCS/Azure path.
///
/// The ETag is passed through byte for byte, quotes and all: it is the pin a
/// later conditional read sends back as `If-Match`, and normalizing it here
/// would make that read compare a string the store never issued. `version` is
/// the store's own version or generation when it reports one, so a pin can
/// carry both halves; it falls back to the ETag for a store that does not
/// version objects, which is what [`crate::s3::S3Store`] reports too.
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
    ) -> Result<GetOutcome, StoreError> {
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
                &crate::s3::path_of(key),
                OsGetOptions {
                    range: os_range,
                    if_match: pin.map(|pin| pin.etag.clone()),
                    version: pin.and_then(|pin| pin.version.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(crate::s3::map_get_error)?;
        let etag = result.meta.e_tag.clone().ok_or_else(|| {
            StoreError::Permanent(format!("the store returned no ETag for {key}"))
        })?;
        let version = result.meta.version.clone().unwrap_or_else(|| etag.clone());
        let total_size = result.meta.size;
        let data = result.bytes().await.map_err(crate::s3::map_error_common)?;
        Ok(GetOutcome {
            data,
            etag: Etag(etag),
            version: Version(version),
            total_size,
        })
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let meta = self
            .store
            .head(&crate::s3::path_of(key))
            .await
            .map_err(crate::s3::map_error_common)?;
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
                Some(Err(e)) => return Err(crate::s3::map_error_common(e)),
                None => break,
            }
        }
        let next = if out.len() == EXTERNAL_PAGE_SIZE {
            out.last().map(|m| PageToken(m.key.clone()))
        } else {
            None
        };
        Ok(ListPage { objects: out, next })
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let prefix_path = crate::s3::prefix_of(prefix);
        let result = self
            .store
            .list_with_delimiter(prefix_path.as_ref())
            .await
            .map_err(crate::s3::map_error_common)?;
        let objects = result
            .objects
            .into_iter()
            .map(map_external_meta)
            .collect::<Result<Vec<_>, _>>()?;
        let common_prefixes = result
            .common_prefixes
            .into_iter()
            .map(|p| format!("{p}/"))
            .collect();
        Ok(DelimitedList {
            objects,
            common_prefixes,
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
        self.refuse(&format!("put of {key}"))
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.refuse(&format!("multipart upload of {key}"))
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.refuse(&format!("delete of {key}"))
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.get(key, range).await,
            Backend::Generic(store) => store.get(key, range, None).await,
        }
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<GetOutcome, StoreError> {
        match &self.backend {
            Backend::S3(store) => store.get_pinned(key, range, pin).await,
            Backend::Generic(store) => store.get(key, range, Some(pin)).await,
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
            suffix_range: true,
            upload_checksum: false,
            prefix_list: true,
            multipart: false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
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

        // No version reported: the pin's version half falls back to the ETag,
        // so a pinned read still has both halves to send.
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
}
