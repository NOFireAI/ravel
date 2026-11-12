//! Azure Blob Storage backend (ADR-4158): [`AzureStore`], a read-write
//! [`ObjectStoreBackend`] over `object_store`'s `MicrosoftAzure` client, with
//! a Blob service REST client of its own for listing, blob properties and the
//! account check.
//!
//! # The version token is the ETag
//!
//! [`Version`] and [`Etag`] carry the same string on every path (`put`,
//! `get`, `head`, `pin_of` and listing entries): the blob's `ETag` exactly as
//! Get Blob Properties reports it, quotes included.
//!
//! - `CreateIfAbsent` sends `If-None-Match: *`. A 409 whose error code is
//!   `BlobAlreadyExists` is [`StoreError::AlreadyExists`]; a 409 with another
//!   code (an immutability policy, snapshots) is [`StoreError::Permanent`]. A
//!   409 whose code `object_store` did not keep is confirmed with one HEAD, as
//!   on S3: present is `AlreadyExists`, absent is [`StoreError::Transient`].
//! - `CasVersion(v)` sends `If-Match: v`. A 412 is
//!   [`StoreError::PreconditionFailed`], and so is a 404 (the key is gone, so
//!   the token no longer names it), except a 404 `ContainerNotFound`, which
//!   is `Permanent`.
//! - `Overwrite` sends no precondition.
//!
//! A leased blob answers a write with a 412 (or a 409) whose code starts with
//! `Lease`. That is `Permanent` in every put mode and on `delete`, never
//! `AlreadyExists` or `PreconditionFailed`: Ravel takes no leases, so no retry
//! clears it.
//!
//! # Reads
//!
//! `Full` is one GET and `Range` one ranged GET. Azure answers a suffix range
//! (`bytes=-n`) with 200 and the whole blob, so a suffix is never sent: a
//! `Suffix(n)` read is a HEAD for the length `L` and ETag `E`, then a GET of
//! `bytes=max(L-n,0)-(L-1)` with `If-Match: E`. A 412 on the GET means the
//! blob was replaced in between, and the read starts again from the HEAD, at
//! most [`SUFFIX_ROUNDS`] times before [`StoreError::Transient`]. A
//! zero-length blob, `Suffix(0)`, and an empty or inverted range are
//! [`StoreError::InvalidRange`].
//!
//! A pinned read sends the pin's ETag as `If-Match` and its version as the
//! `versionid` selector when it has one. A pinned suffix read sends both on
//! the HEAD and on the GET, and a 412 on either is
//! [`StoreError::PreconditionFailed`] at once.
//!
//! # Listing
//!
//! Through the `list` module's List Blobs client, not `object_store`, as are
//! `head`, `pin_of` and `delete` (`object_store` deletes through Blob Batch);
//! that
//! module's docs give the request shape. `list_after` sends `startFrom`, which
//! is inclusive and which a service may ignore, so every name not greater
//! than the resume point is dropped client-side whether or not it was
//! honoured. The [`PageToken`] is the raw last name. A listed `Etag` is quoted
//! to match the one Get Blob Properties reports.
//!
//! # Errors
//!
//! A 404 `ContainerNotFound` is [`StoreError::Permanent`] on every operation;
//! a 404 `BlobNotFound` is [`StoreError::NotFound`] (`delete` treats it as
//! success). 401 and 403 are [`StoreError::AccessDenied`], 416 is
//! [`StoreError::InvalidRange`], and 429, 503 and 5xx are retryable.
//!
//! # Instrumentation
//!
//! Both clients send through the S3 adapter's connector, observing only the
//! store's own host, so `ravel_store_attempts_total` counts every attempt and
//! [`ObjectStoreBackend::observed_store_time_ns`] reports the last `Date`.
//! A suffix read is one `get` call and two attempts. A token request to
//! Microsoft Entra ID or the metadata service is neither.
//!
//! # Multipart upload
//!
//! Put Block per part and one Put Block List at `complete`, through
//! `object_store`'s client and driven by the S3 adapter's handle, so the
//! part-sequence rules and poisoning are the S3 code. `abort` sends nothing:
//! Azure has no abort call and discards uncommitted blocks itself.
//! `complete` is unconditional.
//!
//! # Credentials
//!
//! Read from five environment variables only, never the whole `AZURE_*`
//! namespace ([`CredentialEnv`]): workload identity
//! (`AZURE_FEDERATED_TOKEN_FILE`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
//! `AZURE_AUTHORITY_HOST`) or a shared key (`AZURE_STORAGE_ACCOUNT_KEY`),
//! and otherwise `object_store`'s managed-identity fallback through the
//! instance metadata service. When both a shared key and workload identity
//! are set, the shared key is used, which is `object_store`'s order. No SAS
//! token and no Azure CLI credential.
//!
//! # Hierarchical namespace
//!
//! Construction sends Get Account Information through the container URL and,
//! when that answer carries no `x-ms-is-hns-enabled` (Azurite's does not),
//! again through the account URL. It refuses an account unless the header it
//! read is `false`: a `true` from either, a header absent from both, an
//! unreadable value and a failed request are all refusals.

use std::env::VarError;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use object_store::azure::{AzureCredential, MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::{
    ClientOptions, GetOptions as OsGetOptions, GetRange as OsGetRange, ObjectStore, ObjectStoreExt,
    PutMode as OsPutMode, PutOptions as OsPutOptions, PutPayload, RetryConfig,
    StaticCredentialProvider, UpdateVersion,
};

use crate::instrument::{StoreMetrics, StoreOp};
use crate::s3::connector::{self, ObservedStoreTime, S3HttpConnector};
use crate::s3::{
    LIST_PAGE_SIZE, S3HttpConfig, S3MultipartUpload, SigningClock, SystemSigningClock,
    list_http_client, map_error_common, map_get_error, path_of, preflight_checksum, s3_error_code,
};
use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutMode, PutOptions, PutOutcome, StoreError,
    Version,
};

#[cfg(any(test, feature = "test-support"))]
pub mod fake;

mod list;
use list::{AccountKey, AzureRestClient, RestClientParts};

/// Rounds a suffix read makes (a HEAD and a GET each) before it gives up on a
/// blob that keeps being replaced.
pub const SUFFIX_ROUNDS: usize = 3;

const ENV_FEDERATED_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";
const ENV_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const ENV_TENANT_ID: &str = "AZURE_TENANT_ID";
const ENV_AUTHORITY_HOST: &str = "AZURE_AUTHORITY_HOST";
const ENV_ACCOUNT_KEY: &str = "AZURE_STORAGE_ACCOUNT_KEY";

/// The only environment variables [`AzureStore`] reads.
pub const CREDENTIAL_ENV_KEYS: [&str; 5] = [
    ENV_FEDERATED_TOKEN_FILE,
    ENV_CLIENT_ID,
    ENV_TENANT_ID,
    ENV_AUTHORITY_HOST,
    ENV_ACCOUNT_KEY,
];

/// How to reach one Azure Blob Storage container.
#[derive(Debug, Clone)]
pub struct AzureConfig {
    /// The storage account: lowercase letters and digits.
    pub account: String,
    /// The container: lowercase letters, digits and `-`.
    pub container: String,
    /// Replaces `https://<account>.blob.core.windows.net`, path included (an
    /// emulator's is `http://host:port/<account>`). For a test endpoint or
    /// the emulator only.
    pub endpoint: Option<String>,
    /// Allow a plain-HTTP `endpoint`. Refused without `endpoint`.
    pub allow_http: bool,
    /// Look up no credentials and sign nothing. Refused without `endpoint`.
    /// Test builds only (the `test-support` feature).
    #[cfg(any(test, feature = "test-support"))]
    pub skip_signature: bool,
    /// A shared key that takes the place of `AZURE_STORAGE_ACCOUNT_KEY`, for
    /// the emulator and the fake. Test builds only.
    #[cfg(any(test, feature = "test-support"))]
    pub test_account_key: Option<TestAccountKey>,
    /// Keys per [`ObjectStoreBackend::list`] page, sent as `maxresults`.
    pub list_page_size: usize,
    /// Whole-request timeout, the response body included.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// `None` keeps `object_store`'s retry defaults.
    pub(crate) retry: Option<RetryConfig>,
}

/// A base64 shared key for a test account. Never printed.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone)]
pub struct TestAccountKey(pub String);

#[cfg(any(test, feature = "test-support"))]
impl fmt::Debug for TestAccountKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TestAccountKey(<redacted>)")
    }
}

impl AzureConfig {
    /// The container on Azure's public endpoint, with the S3 adapter's page
    /// size and timeouts (20 s per request, 3 s to connect).
    pub fn new(account: impl Into<String>, container: impl Into<String>) -> Self {
        let http = S3HttpConfig::default();
        AzureConfig {
            account: account.into(),
            container: container.into(),
            endpoint: None,
            allow_http: false,
            #[cfg(any(test, feature = "test-support"))]
            skip_signature: false,
            #[cfg(any(test, feature = "test-support"))]
            test_account_key: None,
            list_page_size: LIST_PAGE_SIZE,
            request_timeout: http.request_timeout,
            connect_timeout: http.connect_timeout,
            retry: None,
        }
    }

    /// The `backend_identity` a qualification record of this container
    /// carries (ADR-4158 D6).
    pub fn backend_identity(&self) -> String {
        crate::conformance::azure_backend_identity(
            &self.account,
            &self.container,
            self.endpoint.as_deref(),
        )
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.account.is_empty()
            || !self
                .account
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err(StoreError::Permanent(format!(
                "Azure storage account name {:?} must be non-empty lowercase letters and digits",
                self.account
            )));
        }
        if self.container.is_empty()
            || !self
                .container
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(StoreError::Permanent(format!(
                "Azure container name {:?} must be non-empty lowercase letters, digits and '-'",
                self.container
            )));
        }
        if self.endpoint.is_none() && self.allow_http {
            return Err(StoreError::Permanent(
                "Azure allow_http is only valid with an endpoint".into(),
            ));
        }
        if self.endpoint.is_none() && self.skip_signature() {
            return Err(StoreError::Permanent(
                "Azure skip_signature is only valid with an endpoint".into(),
            ));
        }
        if self.list_page_size == 0 {
            return Err(StoreError::Permanent(
                "Azure list page size must be at least 1".into(),
            ));
        }
        Ok(())
    }

    /// The service URL: the endpoint without a trailing `/`, or Azure's.
    fn account_url(&self) -> String {
        match &self.endpoint {
            Some(endpoint) => endpoint.trim_end_matches('/').to_string(),
            None => format!("https://{}.blob.core.windows.net", self.account),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    fn skip_signature(&self) -> bool {
        self.skip_signature
    }

    #[cfg(not(any(test, feature = "test-support")))]
    fn skip_signature(&self) -> bool {
        false
    }

    #[cfg(any(test, feature = "test-support"))]
    fn test_account_key(&self) -> Option<&str> {
        self.test_account_key.as_ref().map(|key| key.0.as_str())
    }

    #[cfg(not(any(test, feature = "test-support")))]
    fn test_account_key(&self) -> Option<&str> {
        None
    }
}

/// The credential settings [`AzureStore`] takes from the environment: these
/// five variables and no other. An empty value counts as unset.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct CredentialEnv {
    pub federated_token_file: Option<String>,
    pub client_id: Option<String>,
    pub tenant_id: Option<String>,
    pub authority_host: Option<String>,
    pub account_key: Option<String>,
}

impl fmt::Debug for CredentialEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialEnv")
            .field("federated_token_file", &self.federated_token_file)
            .field("client_id", &self.client_id)
            .field("tenant_id", &self.tenant_id)
            .field("authority_host", &self.authority_host)
            .field(
                "account_key",
                &self.account_key.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl CredentialEnv {
    /// Read [`CREDENTIAL_ENV_KEYS`] through `lookup` (`std::env::var` in
    /// production), asking for nothing else. A value that is not UTF-8 is
    /// refused, naming the variable and not its value.
    pub fn from_lookup(
        lookup: impl Fn(&str) -> Result<String, VarError>,
    ) -> Result<Self, StoreError> {
        let read = |name: &str| match lookup(name) {
            Ok(value) if value.is_empty() => Ok(None),
            Ok(value) => Ok(Some(value)),
            Err(VarError::NotPresent) => Ok(None),
            Err(VarError::NotUnicode(_)) => {
                Err(StoreError::Permanent(format!("{name} is not valid UTF-8")))
            }
        };
        Ok(CredentialEnv {
            federated_token_file: read(ENV_FEDERATED_TOKEN_FILE)?,
            client_id: read(ENV_CLIENT_ID)?,
            tenant_id: read(ENV_TENANT_ID)?,
            authority_host: read(ENV_AUTHORITY_HOST)?,
            account_key: read(ENV_ACCOUNT_KEY)?,
        })
    }

    /// Apply each setting with its explicit builder setter.
    fn apply(&self, mut builder: MicrosoftAzureBuilder) -> MicrosoftAzureBuilder {
        if let Some(path) = &self.federated_token_file {
            builder = builder.with_federated_token_file(path);
        }
        if let Some(client_id) = &self.client_id {
            builder = builder.with_client_id(client_id);
        }
        if let Some(tenant_id) = &self.tenant_id {
            builder = builder.with_tenant_id(tenant_id);
        }
        if let Some(host) = &self.authority_host {
            builder = builder.with_authority_host(host);
        }
        if let Some(key) = &self.account_key {
            builder = builder.with_access_key(key);
        }
        builder
    }
}

/// Azure Blob Storage backend implementing [`ObjectStoreBackend`]. See the
/// module docs.
#[derive(Debug)]
pub struct AzureStore {
    store: MicrosoftAzure,
    rest: AzureRestClient,
    store_time: Arc<ObservedStoreTime>,
    get_unverified: AtomicU64,
    /// Where `ravel_store_get_unverified_total` is also recorded.
    metrics: Arc<StoreMetrics>,
    backend_identity: String,
}

impl AzureStore {
    /// A store whose attempts are counted into a private [`StoreMetrics`].
    /// Sends Get Account Information and refuses a hierarchical-namespace
    /// account.
    pub async fn new(config: AzureConfig) -> Result<AzureStore, StoreError> {
        Self::with_metrics(config, Arc::default()).await
    }

    /// A store whose HTTP attempts, retries included, are counted into
    /// `metrics`. Credentials come from [`CredentialEnv`] read from the
    /// process environment.
    pub async fn with_metrics(
        config: AzureConfig,
        metrics: Arc<StoreMetrics>,
    ) -> Result<AzureStore, StoreError> {
        let env = CredentialEnv::from_lookup(|name| std::env::var(name))?;
        Self::build(config, metrics, env, Arc::new(SystemSigningClock)).await
    }

    pub(crate) async fn build(
        config: AzureConfig,
        metrics: Arc<StoreMetrics>,
        mut env: CredentialEnv,
        clock: Arc<dyn SigningClock>,
    ) -> Result<AzureStore, StoreError> {
        config.validate()?;
        if let Some(key) = config.test_account_key() {
            env.account_key = Some(key.to_string());
        }
        let store_time: Arc<ObservedStoreTime> = Arc::default();
        let authority = store_authority(&config.account, config.endpoint.as_deref())?;
        let account_url = config.account_url();
        let options = ClientOptions::new()
            .with_timeout(config.request_timeout)
            .with_connect_timeout(config.connect_timeout)
            .with_allow_http(config.allow_http);
        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(config.account.clone())
            .with_container_name(config.container.clone())
            .with_client_options(options)
            .with_http_connector(
                S3HttpConnector::new(Arc::clone(&metrics), Arc::clone(&store_time))
                    .only_for_authority(authority.clone()),
            );
        if config.endpoint.is_some() {
            builder = builder.with_endpoint(account_url.clone());
        }
        let skip_signature = config.skip_signature();
        let mut account_key = None;
        if skip_signature {
            // A fixed placeholder stands in for the managed-identity fallback,
            // which `skip_signature` never asks for a token.
            let placeholder = AzureCredential::BearerToken("unsigned".to_string());
            builder = builder
                .with_skip_signature(true)
                .with_credentials(Arc::new(StaticCredentialProvider::new(placeholder)));
        } else {
            if let Some(key) = &env.account_key {
                account_key = Some(AccountKey::decode(key).ok_or_else(|| {
                    StoreError::Permanent(format!("{ENV_ACCOUNT_KEY} is not base64"))
                })?);
            }
            builder = env.apply(builder);
        }
        if let Some(retry) = &config.retry {
            builder = builder.with_retry(retry.clone());
        }
        // The builder's error can name a credential setting; it is left out.
        let store = builder.build().map_err(|_| {
            StoreError::Permanent(format!(
                "Azure client for container {:?} in account {:?} could not be built: the \
                 configuration or the credential settings were rejected",
                config.container, config.account
            ))
        })?;
        let http = list_http_client(
            &S3HttpConfig {
                request_timeout: config.request_timeout,
                connect_timeout: config.connect_timeout,
                ..S3HttpConfig::default()
            },
            config.allow_http,
        )
        .map_err(|e| {
            StoreError::Permanent(format!("failed to build the Azure REST client: {e}"))
        })?;
        let rest = AzureRestClient::new(RestClientParts {
            http: S3HttpConnector::new(Arc::clone(&metrics), Arc::clone(&store_time))
                .only_for_authority(authority)
                .wrap(http),
            credentials: (!skip_signature).then(|| Arc::clone(store.credentials())),
            account_key,
            clock,
            retry: config.retry.clone().unwrap_or_default(),
            account: config.account.clone(),
            account_url,
            container: config.container.clone(),
            page_size: config.list_page_size,
        });
        rest.refuse_hierarchical_namespace().await?;
        Ok(AzureStore {
            store,
            rest,
            store_time,
            get_unverified: AtomicU64::new(0),
            metrics,
            backend_identity: config.backend_identity(),
        })
    }

    /// `az://<account>/<container>[@<endpoint>]` (ADR-4158 D6).
    pub fn backend_identity(&self) -> &str {
        &self.backend_identity
    }

    /// Full-object reads served without a stored-checksum check: every one,
    /// since this backend sends and reads no upload checksum.
    pub fn get_unverified(&self) -> u64 {
        self.get_unverified.load(Ordering::Relaxed)
    }

    async fn get_inner(
        &self,
        key: &str,
        range: GetRange,
        pin: Option<&Pin>,
    ) -> Result<PinnedRead, StoreError> {
        let path = path_of(key)?;
        match range {
            GetRange::Full => self.get_once(key, &path, None, pin, None).await,
            GetRange::Range(start, end) => {
                if start >= end {
                    return Err(StoreError::InvalidRange(format!(
                        "empty or inverted range [{start}, {end})"
                    )));
                }
                self.get_once(key, &path, Some(start..end), pin, None).await
            }
            GetRange::Suffix(0) => Err(StoreError::InvalidRange("zero-length suffix".into())),
            GetRange::Suffix(n) => self.get_suffix(key, &path, n, pin).await,
        }
    }

    /// A suffix read as a HEAD and a bounded GET conditioned on the HEAD's
    /// ETag (ADR-4158 D2). A pinned read conditions both on the pin.
    async fn get_suffix(
        &self,
        key: &str,
        path: &object_store::path::Path,
        n: u64,
        pin: Option<&Pin>,
    ) -> Result<PinnedRead, StoreError> {
        for _ in 0..SUFFIX_ROUNDS {
            let head = self.rest.head_blob(key, pin).await?;
            if head.size == 0 {
                return Err(StoreError::InvalidRange(format!(
                    "suffix of the zero-length blob {key}"
                )));
            }
            let start = head.size.saturating_sub(n);
            let if_match = pin.map_or(head.etag.as_str(), |pin| pin.etag.as_str());
            match self
                .get_once(key, path, Some(start..head.size), pin, Some(if_match))
                .await
            {
                Err(StoreError::PreconditionFailed) if pin.is_none() => continue,
                outcome => return outcome,
            }
        }
        Err(StoreError::Transient(format!(
            "suffix read of {key}: the blob was replaced during each of {SUFFIX_ROUNDS} rounds"
        )))
    }

    /// One GET. `if_match` overrides the pin's ETag as the precondition.
    async fn get_once(
        &self,
        key: &str,
        path: &object_store::path::Path,
        range: Option<std::ops::Range<u64>>,
        pin: Option<&Pin>,
        if_match: Option<&str>,
    ) -> Result<PinnedRead, StoreError> {
        let full = range.is_none();
        let result = self
            .store
            .get_opts(
                path,
                OsGetOptions {
                    range: range.map(OsGetRange::Bounded),
                    if_match: if_match
                        .map(str::to_string)
                        .or_else(|| pin.map(|pin| pin.etag.clone())),
                    version: pin.and_then(|pin| pin.version.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| map_azure_error(e, map_get_error))?;
        // `get_opts` resolves on the response headers; the body is read below.
        crate::get_latency::mark_first_byte();
        let etag = etag_of(key, result.meta.e_tag.clone())?;
        let version_id = result.meta.version.clone();
        let total_size = result.meta.size;
        let data = result
            .bytes()
            .await
            .map_err(|e| map_azure_error(e, map_error_common))?;
        if full {
            self.get_unverified.fetch_add(1, Ordering::Relaxed);
            self.metrics.record_get_unverified();
        }
        Ok(PinnedRead {
            outcome: GetOutcome {
                data,
                etag: Etag(etag.clone()),
                version: Version(etag.clone()),
                total_size,
            },
            pin: Pin::from_store(etag, version_id),
        })
    }

    async fn head_inner(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        crate::check_addressable(key)?;
        let head = self.rest.head_blob(key, None).await?;
        let pin = Pin::from_store(head.etag.clone(), head.version_id);
        Ok((
            ObjectMeta {
                key: key.to_string(),
                size: head.size,
                etag: Etag(head.etag.clone()),
                version: Version(head.etag),
                last_modified_unix_ms: head.last_modified_unix_ms,
            },
            pin,
        ))
    }

    async fn put_inner(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        let path = path_of(key)?;
        preflight_checksum(&data, opts.checksum)?;
        let os_mode = match &opts.mode {
            PutMode::Overwrite => OsPutMode::Overwrite,
            PutMode::CreateIfAbsent => OsPutMode::Create,
            PutMode::CasVersion(version) => OsPutMode::Update(UpdateVersion {
                e_tag: Some(version.0.clone()),
                version: None,
            }),
        };
        let result = self
            .store
            .put_opts(
                &path,
                PutPayload::from(data),
                OsPutOptions {
                    mode: os_mode,
                    ..Default::default()
                },
            )
            .await;
        match result {
            Ok(result) => put_outcome(key, result),
            Err(e) => match map_put_error(e, &opts.mode) {
                PutFailure::Mapped(error) => Err(error),
                PutFailure::ConfirmCreate => self.confirm_create_conflict(key).await,
            },
        }
    }

    /// Settle a create 409 whose error code `object_store` did not keep with
    /// one HEAD, as `S3Store` does: present is `AlreadyExists`, absent is a
    /// retryable conflict, and a HEAD that fails retryably determined nothing
    /// and is returned as it is. A HEAD that fails otherwise falls back to
    /// `AlreadyExists`.
    async fn confirm_create_conflict(&self, key: &str) -> Result<PutOutcome, StoreError> {
        match self.rest.head_blob(key, None).await {
            Ok(_) => Err(StoreError::AlreadyExists),
            Err(StoreError::NotFound) => Err(StoreError::Transient(format!(
                "create of {key}: 409 with no error code and the blob absent on HEAD"
            ))),
            Err(e) if e.is_retryable() => Err(e),
            Err(_) => Err(StoreError::AlreadyExists),
        }
    }
}

/// The `host[:port]` requests go to: the endpoint's as written, or Azure's
/// for the account. An endpoint carrying userinfo (`user:pass@`) is refused
/// without echoing it.
fn store_authority(account: &str, endpoint: Option<&str>) -> Result<String, StoreError> {
    let Some(endpoint) = endpoint else {
        return Ok(format!("{account}.blob.core.windows.net"));
    };
    let after_scheme = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if authority.contains('@') {
        return Err(StoreError::Permanent(
            "Azure endpoint carries userinfo (user:pass@); credentials come from the \
             environment only"
                .into(),
        ));
    }
    if authority.is_empty() {
        return Err(StoreError::Permanent(format!(
            "Azure endpoint {endpoint:?} names no host"
        )));
    }
    Ok(authority.to_string())
}

fn etag_of(key: &str, etag: Option<String>) -> Result<String, StoreError> {
    etag.ok_or_else(|| StoreError::Permanent(format!("Azure returned no ETag for {key}")))
}

fn put_outcome(key: &str, result: object_store::PutResult) -> Result<PutOutcome, StoreError> {
    let etag = etag_of(key, result.e_tag)?;
    Ok(PutOutcome {
        etag: Etag(etag.clone()),
        version: Version(etag),
    })
}

/// The `<Error><Code>` an `object_store` error kept, if any.
fn error_code_of(e: &object_store::Error) -> Option<String> {
    use object_store::Error as E;
    match e {
        E::Generic { source, .. }
        | E::NotFound { source, .. }
        | E::AlreadyExists { source, .. }
        | E::Precondition { source, .. }
        | E::NotModified { source, .. }
        | E::PermissionDenied { source, .. }
        | E::Unauthenticated { source, .. } => s3_error_code(source.as_ref()),
        _ => None,
    }
}

/// Whether `code` is a lease's: `LeaseIdMissing`, `LeaseAlreadyPresent` and
/// the rest of that family, sent with a 412 or a 409.
fn is_lease_code(code: Option<&str>) -> bool {
    code.is_some_and(|code| code.starts_with("Lease"))
}

/// A 412 or 409 with a lease's error code: a lease Ravel never takes, so no
/// retry and no precondition outcome can clear it (ADR-4158 D5).
fn lease_error(e: &object_store::Error, code: Option<&str>) -> Option<StoreError> {
    use object_store::Error as E;
    let status = match e {
        E::Precondition { .. } => 412,
        E::AlreadyExists { .. } => 409,
        _ => return None,
    };
    let code = code.filter(|code| is_lease_code(Some(code)))?;
    Some(StoreError::Permanent(format!(
        "Azure refused the request: HTTP {status} {code}: the blob is leased"
    )))
}

/// `ContainerNotFound` is `Permanent` on every operation, and so is a lease
/// conflict; a 409 outside a create is not `AlreadyExists` (ADR-4158 D5).
/// Anything else goes to `fallback`.
fn map_azure_error(
    e: object_store::Error,
    fallback: fn(object_store::Error) -> StoreError,
) -> StoreError {
    let code = error_code_of(&e);
    if code.as_deref() == Some("ContainerNotFound") {
        return StoreError::Permanent(
            "the Azure container does not exist (ContainerNotFound)".into(),
        );
    }
    if let Some(leased) = lease_error(&e, code.as_deref()) {
        return leased;
    }
    if let object_store::Error::AlreadyExists { .. } = e {
        return StoreError::Permanent(match code {
            Some(code) => format!("Azure refused the request: HTTP 409 {code}"),
            None => "Azure refused the request: HTTP 409".into(),
        });
    }
    fallback(e)
}

enum PutFailure {
    Mapped(StoreError),
    /// A create 409 with no error code: settle it with a HEAD.
    ConfirmCreate,
}

/// `put` errors by mode (ADR-4158 D1, D5).
fn map_put_error(e: object_store::Error, mode: &PutMode) -> PutFailure {
    use object_store::Error as E;
    let code = error_code_of(&e);
    if let Some(leased) = lease_error(&e, code.as_deref()) {
        return PutFailure::Mapped(leased);
    }
    if code.as_deref() == Some("ContainerNotFound") {
        return PutFailure::Mapped(map_azure_error(e, map_error_common));
    }
    match (&e, mode) {
        (E::AlreadyExists { .. }, PutMode::CreateIfAbsent) => match code.as_deref() {
            Some("BlobAlreadyExists") => PutFailure::Mapped(StoreError::AlreadyExists),
            None => PutFailure::ConfirmCreate,
            Some(_) => PutFailure::Mapped(map_azure_error(e, map_error_common)),
        },
        (E::Precondition { .. }, PutMode::CreateIfAbsent) => {
            PutFailure::Mapped(StoreError::AlreadyExists)
        }
        (E::Precondition { .. } | E::NotFound { .. }, PutMode::CasVersion(_)) => {
            PutFailure::Mapped(StoreError::PreconditionFailed)
        }
        _ => PutFailure::Mapped(map_azure_error(e, map_error_common)),
    }
}

/// The outcome of a completed Put Block List: its ETag is the version, as
/// for a single `put`.
fn multipart_outcome(key: &str, result: object_store::PutResult) -> Result<PutOutcome, StoreError> {
    put_outcome(key, result)
}

/// Put Block and Put Block List errors, with `ContainerNotFound` `Permanent`
/// as on every other operation.
fn map_multipart_error(e: object_store::Error) -> StoreError {
    map_azure_error(e, map_error_common)
}

#[async_trait::async_trait]
impl ObjectStoreBackend for AzureStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        connector::scope(StoreOp::Put, self.put_inner(key, data, opts)).await
    }

    /// Put Block per part and Put Block List at `complete`, driven by the S3
    /// adapter's handle. `object_store` sends nothing to open the upload.
    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        let upload = self
            .store
            .put_multipart(&path_of(key)?)
            .await
            .map_err(|e| map_azure_error(e, map_error_common))?;
        Ok(Box::new(
            S3MultipartUpload::new(key, upload, multipart_outcome)
                .with_error_map(map_multipart_error),
        ))
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        connector::scope(StoreOp::Get, self.get_inner(key, range, None))
            .await
            .map(|read| read.outcome)
    }

    /// The pin's ETag as `If-Match` and its version as `versionid`, on the
    /// HEAD and the GET of a suffix read as on a single GET.
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        connector::scope(StoreOp::Get, self.get_inner(key, range, Some(pin))).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        connector::scope(StoreOp::Get, self.get_inner(key, range, None)).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        connector::scope(StoreOp::Head, self.head_inner(key))
            .await
            .map(|(meta, _)| meta)
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        connector::scope(StoreOp::Head, self.head_inner(key)).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let after = page.as_ref().map(|PageToken(after)| after.as_str());
        connector::scope(StoreOp::List, self.rest.list_page(prefix, after)).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        // A page token is past `start_after`, so it takes precedence.
        let after = match &page {
            Some(PageToken(after)) => Some(after.as_str()),
            None => start_after,
        };
        connector::scope(StoreOp::List, self.rest.list_page(prefix, after)).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        connector::scope(
            StoreOp::ListDelimited,
            self.rest.list_delimited(prefix, crate::MAX_LIST_PAGES),
        )
        .await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        connector::scope(StoreOp::Delete, async move {
            path_of(key)?;
            match self.rest.delete_blob(key).await {
                // Idempotent per the contract: deleting a missing key succeeds.
                Ok(()) | Err(StoreError::NotFound) => Ok(()),
                Err(e) => Err(e),
            }
        })
        .await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            consistent_read: true,
            consistent_list: true,
            create_if_absent: true,
            cas_version: true,
            suffix_range: true,
            upload_checksum: false,
            prefix_list: true,
            multipart: true,
        }
    }

    /// The `Date` of the last response this store received, error responses
    /// included, in unix nanoseconds; `None` before the first one.
    fn observed_store_time_ns(&self) -> Option<i64> {
        self.store_time.latest()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests;
