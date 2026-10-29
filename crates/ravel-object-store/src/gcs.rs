//! Google Cloud Storage backend (ADR-3456): [`GcsStore`], a read-write
//! [`ObjectStoreBackend`] over `object_store`'s `GoogleCloudStorage` client,
//! which speaks the GCS XML API, with a JSON API listing client of its own.
//!
//! # The version token is the generation
//!
//! GCS assigns every write of an object name a new *generation*, a decimal
//! integer it does not reuse for that name. [`Version`] on every path
//! (`put`, `get`, `head`, `pin_of` and listing entries) is that generation as
//! a decimal string. The ETag is not a token here: for a single-request upload
//! it is the MD5 of the content, so an identical rewrite keeps the ETag while
//! moving the generation, and a CAS against the ETag would let a writer that
//! read the first state overwrite the second.
//!
//! - `CreateIfAbsent` is `object_store`'s `PutMode::Create`, which sends
//!   `x-goog-if-generation-match: 0`. A 412 is [`StoreError::AlreadyExists`].
//! - `CasVersion(v)` is `PutMode::Update` carrying `v` as its version, which
//!   sends `x-goog-if-generation-match: v`. A 412 is
//!   [`StoreError::PreconditionFailed`]. `v` must be a positive decimal with
//!   no leading zero and no sign, at most `i64::MAX`; anything else (`0`,
//!   `012`, `+1`, an empty, non-numeric or larger token) is refused with
//!   [`StoreError::Permanent`] before a request is sent. GCS issues no
//!   generation 0, and `x-goog-if-generation-match: 0` means
//!   create-if-absent, so a CAS on `0` would otherwise create an absent key.
//! - `Overwrite` sends no precondition.
//!
//! The tests below pin each header on the wire against the in-process fake
//! endpoint in [`fake`].
//!
//! # Pinned reads
//!
//! A [`Pin`] from this store carries the ETag and the generation. A pinned
//! read sends the ETag as `If-Match` and the generation as the `generation`
//! query parameter, which selects that generation (the [`Pin`] contract: the
//! version is a selector, not a precondition). GCS decides the selector
//! before the ETag: a generation the bucket no longer keeps, or a key that
//! does not exist, is [`StoreError::NotFound`] whatever the ETag; a live
//! generation with another ETag is [`StoreError::PreconditionFailed`].
//!
//! # Listing
//!
//! Listing does not go through `object_store`. The `list` module sends the
//! JSON API's `objects.list`, with `maxResults` set to
//! [`GcsConfig::list_page_size`] on every request. The prefix and the resume
//! point are sent as raw names (the resume as `startOffset`, which is
//! inclusive, so the start name itself is dropped). A call follows the
//! service's opaque `nextPageToken` until it holds a page of names, with one
//! exception: a response from which the start name was dropped and that
//! leaves at least one name ends the call, and its names, one short of the
//! page size when the response was full, are returned with a page token
//! instead of a second request filling the page. At a page size above 1 and
//! against responses that are each full, every call is therefore one request;
//! at page size 1 a resumed response holds only the start name, so a resumed
//! call is two. Up to 16 responses in a row that leave no name are followed
//! ([`StoreError::ListPageCeiling`] past that), and a repeated
//! `nextPageToken` fails the call ([`StoreError::ListRepeatedToken`]). The
//! [`PageToken`] handed back between calls is the raw last name, as on S3.
//! Listed names go to [`crate::classify_objects`] and
//! [`crate::classify_prefixes`] as received, so a name this store cannot
//! address is reported in [`ListPage::unaddressable`] rather than failing the
//! page. Each listed version is the item's `generation`; an item without a
//! positive decimal one fails the call. Each listed ETag is the quoted hex of
//! the item's `md5Hash`, which is the ETag `head` reports for such an object.
//! An object without an MD5 is a composite, which the item's `componentCount`
//! marks; `head` reports its ETag as a dash followed by the JSON API's `etag`,
//! quoted, and the listing reports the same value. An item with neither
//! `md5Hash` nor `componentCount` fails the call, naming the key, since the
//! ETag `head` would report for it is unknown. Responses carry only the
//! fields these need (the `fields` parameter).
//!
//! A listing of a bucket that does not exist is [`StoreError::Permanent`]; a
//! `head` is [`StoreError::NotFound`], as on S3.
//!
//! # Instrumentation
//!
//! The client sends through the S3 adapter's connector
//! ([`crate::s3`]'s `connector` module), installed with
//! `GoogleCloudStorageBuilder::with_http_connector`. It counts every HTTP
//! attempt, retries included, into the [`StoreMetrics`] this store was built
//! with, and records each response's `Date` header, which
//! [`ObjectStoreBackend::observed_store_time_ns`] reports. The listing client
//! sends through the same connector, around a reqwest client of its own that
//! follows no redirect. The data-plane client sends its credential requests
//! through the connector too; only requests to the store's own host (Google's
//! endpoint, or [`GcsConfig::base_url`]'s) are counted and read for a `Date`.
//!
//! # What this backend does not do
//!
//! No multipart upload (`multipart: false`; `put_multipart` refuses), no upload
//! checksum (`upload_checksum: false`; a caller's CRC32C is checked against the
//! buffer before the request and not sent), and no bucket-protection probes.
//! A full-object read is one GET, not split into ranges, so it has to finish
//! inside [`GcsConfig::request_timeout`]. Every full-object read is counted by
//! [`GcsStore::get_unverified`], since no stored checksum is checked.
//!
//! # Credentials
//!
//! Application Default Credentials only (ADR-3456 decision 2): the key file
//! `GOOGLE_APPLICATION_CREDENTIALS` names when it is set, otherwise
//! `object_store`'s own lookup (the gcloud well-known file, then the metadata
//! server). No credential passes through [`GcsConfig`]. The listing client
//! takes its bearer token from the data-plane client's credential provider
//! (`GoogleCloudStorage::credentials`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use object_store::gcp::{
    GcpCredential, GcpCredentialProvider, GoogleCloudStorage, GoogleCloudStorageBuilder,
};
use object_store::{
    ClientOptions, GetOptions as OsGetOptions, GetRange as OsGetRange, ObjectStore, ObjectStoreExt,
    PutMode as OsPutMode, PutOptions as OsPutOptions, PutPayload, RetryConfig,
    StaticCredentialProvider, UpdateVersion,
};

use crate::instrument::{StoreMetrics, StoreOp};
use crate::s3::connector::{self, ObservedStoreTime, S3HttpConnector};
use crate::s3::{
    LIST_PAGE_SIZE, S3HttpConfig, list_http_client, map_error_common, map_get_error, path_of,
    preflight_checksum,
};
use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutMode, PutOptions, PutOutcome, StoreError,
    Version,
};

#[cfg(any(test, feature = "test-support"))]
pub mod fake;

mod list;
use list::GcsListClient;

/// The environment variable naming an Application Default Credentials key
/// file. `object_store`'s `GoogleCloudStorageBuilder::new` does not read it,
/// so [`GcsStore`] passes it on.
const ADC_KEY_FILE_ENV: &str = "GOOGLE_APPLICATION_CREDENTIALS";

/// The authority of `object_store`'s default GCS base URL.
const GCS_AUTHORITY: &str = "storage.googleapis.com";

/// `object_store`'s default GCS base URL, which the listing client uses too.
const GCS_BASE_URL: &str = "https://storage.googleapis.com";

/// How to reach one GCS bucket.
#[derive(Debug, Clone)]
pub struct GcsConfig {
    pub bucket: String,
    /// Replaces `https://storage.googleapis.com`. For a test endpoint only.
    pub base_url: Option<String>,
    /// Allow a plain-HTTP `base_url`. Refused without `base_url`.
    pub allow_http: bool,
    /// Look up no credentials: data-plane requests carry a fixed placeholder
    /// bearer token and listing requests none. Refused without `base_url`.
    /// Test builds only (the `test-support` feature).
    #[cfg(any(test, feature = "test-support"))]
    pub skip_signature: bool,
    /// Keys per [`ObjectStoreBackend::list`] page.
    pub list_page_size: usize,
    /// Whole-request timeout, the response body included.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// `None` keeps `object_store`'s retry defaults.
    pub(crate) retry: Option<RetryConfig>,
}

impl GcsConfig {
    /// The bucket on Google's endpoint, with the S3 adapter's page size and
    /// timeouts (20 s per request, 3 s to connect).
    pub fn new(bucket: impl Into<String>) -> Self {
        let http = S3HttpConfig::default();
        GcsConfig {
            bucket: bucket.into(),
            base_url: None,
            allow_http: false,
            #[cfg(any(test, feature = "test-support"))]
            skip_signature: false,
            list_page_size: LIST_PAGE_SIZE,
            request_timeout: http.request_timeout,
            connect_timeout: http.connect_timeout,
            retry: None,
        }
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.bucket.is_empty() {
            return Err(StoreError::Permanent("GCS bucket name is empty".into()));
        }
        if self.base_url.is_none() && self.allow_http {
            return Err(StoreError::Permanent(
                "GCS allow_http is only valid with a base_url".into(),
            ));
        }
        if self.base_url.is_none() && self.skip_signature() {
            return Err(StoreError::Permanent(
                "GCS skip_signature is only valid with a base_url".into(),
            ));
        }
        if self.list_page_size == 0 {
            return Err(StoreError::Permanent(
                "GCS list page size must be at least 1".into(),
            ));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    fn skip_signature(&self) -> bool {
        self.skip_signature
    }

    #[cfg(not(any(test, feature = "test-support")))]
    fn skip_signature(&self) -> bool {
        false
    }
}

/// GCS backend implementing [`ObjectStoreBackend`]. See the module docs.
#[derive(Debug)]
pub struct GcsStore {
    store: GoogleCloudStorage,
    lister: GcsListClient,
    store_time: Arc<ObservedStoreTime>,
    get_unverified: AtomicU64,
}

impl GcsStore {
    /// A store whose attempts are counted into a private [`StoreMetrics`].
    pub fn new(config: GcsConfig) -> Result<Self, StoreError> {
        Self::with_metrics(config, Arc::default())
    }

    /// A store whose HTTP attempts, retries included, are counted into
    /// `metrics`, the registry `ravel_store_attempts_total` is rendered from.
    pub fn with_metrics(config: GcsConfig, metrics: Arc<StoreMetrics>) -> Result<Self, StoreError> {
        config.validate()?;
        let store_time: Arc<ObservedStoreTime> = Arc::default();
        let authority = store_authority(config.base_url.as_deref())?;
        let options = ClientOptions::new()
            .with_timeout(config.request_timeout)
            .with_connect_timeout(config.connect_timeout)
            .with_allow_http(config.allow_http);
        let mut builder = GoogleCloudStorageBuilder::new()
            .with_bucket_name(config.bucket.clone())
            .with_client_options(options)
            .with_http_connector(
                S3HttpConnector::new(Arc::clone(&metrics), Arc::clone(&store_time))
                    .only_for_authority(authority.clone()),
            );
        if let Some(base_url) = &config.base_url {
            builder = builder.with_base_url(base_url);
        }
        if config.skip_signature() {
            // `object_store` still fetches a credential for object requests
            // under `skip_signature`, so a fixed placeholder bearer stands in
            // for the Application Default Credentials lookup.
            let placeholder = GcpCredential {
                bearer: "unsigned".to_string(),
            };
            builder = builder
                .with_skip_signature(true)
                .with_credentials(Arc::new(StaticCredentialProvider::new(placeholder)));
        } else if let Some(path) = adc_key_file()? {
            builder = builder.with_application_credentials(path);
        }
        if let Some(retry) = &config.retry {
            builder = builder.with_retry(retry.clone());
        }
        // The builder's error can name a credential file; it is left out, as
        // the external store's GCS profile does.
        let store = builder.build().map_err(|_| {
            StoreError::Permanent(format!(
                "GCS client for bucket {:?} could not be built: the configuration or the \
                 Application Default Credentials were rejected",
                config.bucket
            ))
        })?;
        let lister = list_client(
            &config,
            metrics,
            Arc::clone(&store_time),
            authority,
            (!config.skip_signature()).then(|| Arc::clone(store.credentials())),
        )?;
        Ok(GcsStore {
            store,
            lister,
            store_time,
            get_unverified: AtomicU64::new(0),
        })
    }

    /// Store `data` under exactly `name` through the JSON API's media upload,
    /// with this store's credentials: a name the `object_store` adapter would
    /// rewrite, as another writer to the bucket could create.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn upload_foreign(&self, name: &str, data: Bytes) -> Result<(), StoreError> {
        self.lister.upload_media(name, data).await
    }

    /// Delete exactly `name` through the JSON API, with this store's
    /// credentials.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn delete_foreign(&self, name: &str) -> Result<(), StoreError> {
        self.lister.delete_object(name).await
    }

    /// Full-object reads served without a stored-checksum check: every one,
    /// since this backend sends and reads no upload checksum (ADR-3456
    /// decision 1).
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
        let full = os_range.is_none();
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
            .map_err(map_get_error)?;
        // `get_opts` resolves on the response headers; the body is read below.
        crate::get_latency::mark_first_byte();
        let etag = etag_of(key, result.meta.e_tag.clone())?;
        let generation = generation_of(key, result.meta.version.clone())?;
        let total_size = result.meta.size;
        let data = result.bytes().await.map_err(map_error_common)?;
        if full {
            self.get_unverified.fetch_add(1, Ordering::Relaxed);
        }
        Ok(PinnedRead {
            outcome: GetOutcome {
                data,
                etag: Etag(etag.clone()),
                version: Version(generation.clone()),
                total_size,
            },
            pin: Pin::from_store(etag, Some(generation)),
        })
    }

    async fn head_inner(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        let path = path_of(key)?;
        let meta = self.store.head(&path).await.map_err(map_error_common)?;
        let etag = etag_of(key, meta.e_tag)?;
        let generation = generation_of(key, meta.version)?;
        let pin = Pin::from_store(etag.clone(), Some(generation.clone()));
        Ok((
            ObjectMeta {
                key: meta.location.to_string(),
                size: meta.size,
                etag: Etag(etag),
                version: Version(generation),
                last_modified_unix_ms: meta.last_modified.timestamp_millis(),
            },
            pin,
        ))
    }
}

/// The key file `GOOGLE_APPLICATION_CREDENTIALS` names, or `None` when it is
/// unset or empty.
fn adc_key_file() -> Result<Option<String>, StoreError> {
    match std::env::var(ADC_KEY_FILE_ENV) {
        Ok(path) if path.is_empty() => Ok(None),
        Ok(path) => Ok(Some(path)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(StoreError::Permanent(format!(
            "{ADC_KEY_FILE_ENV} is not valid UTF-8"
        ))),
    }
}

/// The listing client: its own reqwest client behind the store's recording
/// connector, sending `credentials`' bearer token (none when `None`).
fn list_client(
    config: &GcsConfig,
    metrics: Arc<StoreMetrics>,
    store_time: Arc<ObservedStoreTime>,
    authority: String,
    credentials: Option<GcpCredentialProvider>,
) -> Result<GcsListClient, StoreError> {
    let http = list_http_client(
        &S3HttpConfig {
            request_timeout: config.request_timeout,
            connect_timeout: config.connect_timeout,
            ..S3HttpConfig::default()
        },
        config.allow_http,
    )
    .map_err(|e| StoreError::Permanent(format!("failed to build GCS list client: {e}")))?;
    Ok(GcsListClient::new(
        S3HttpConnector::new(metrics, store_time)
            .only_for_authority(authority)
            .wrap(http),
        credentials,
        config.retry.clone().unwrap_or_default(),
        config.base_url.as_deref().unwrap_or(GCS_BASE_URL),
        &config.bucket,
        config.list_page_size,
    ))
}

/// The `host[:port]` object requests go to: `base_url`'s as written, or
/// Google's endpoint. The connector observes only requests to it. A
/// `base_url` carrying userinfo (`user:pass@`) is refused without echoing it:
/// credentials come from Application Default Credentials only.
fn store_authority(base_url: Option<&str>) -> Result<String, StoreError> {
    let Some(base_url) = base_url else {
        return Ok(GCS_AUTHORITY.to_string());
    };
    let after_scheme = base_url
        .split_once("://")
        .map_or(base_url, |(_, rest)| rest);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if authority.contains('@') {
        return Err(StoreError::Permanent(
            "GCS base_url carries userinfo (user:pass@); credentials come from Application \
             Default Credentials only"
                .into(),
        ));
    }
    if authority.is_empty() {
        return Err(StoreError::Permanent(format!(
            "GCS base_url {base_url:?} names no host"
        )));
    }
    Ok(authority.to_string())
}

fn etag_of(key: &str, etag: Option<String>) -> Result<String, StoreError> {
    etag.ok_or_else(|| StoreError::Permanent(format!("GCS returned no ETag for {key}")))
}

/// The generation `object_store` read from `x-goog-generation`. Required: it
/// is the version token, and an ETag is not a substitute.
fn generation_of(key: &str, generation: Option<String>) -> Result<String, StoreError> {
    generation.ok_or_else(|| StoreError::Permanent(format!("GCS returned no generation for {key}")))
}

/// Whether `token` can be a GCS generation: a positive decimal integer with
/// no leading zero and no sign, at most `i64::MAX` (generations are int64).
fn is_generation(token: &str) -> bool {
    token.bytes().next().is_some_and(|b| b != b'0')
        && token.bytes().all(|b| b.is_ascii_digit())
        && token.parse::<i64>().is_ok()
}

/// `put` errors by mode: a refused precondition is `AlreadyExists` under
/// `CreateIfAbsent` and `PreconditionFailed` under `CasVersion`, whichever of
/// the two `object_store` reported it as. `Overwrite` sends no precondition and
/// is mapped as any other operation.
fn map_put_error(e: object_store::Error, mode: &PutMode) -> StoreError {
    use object_store::Error as E;
    match (&e, mode) {
        (E::AlreadyExists { .. } | E::Precondition { .. }, PutMode::CreateIfAbsent) => {
            StoreError::AlreadyExists
        }
        (E::AlreadyExists { .. } | E::Precondition { .. }, PutMode::CasVersion(_)) => {
            StoreError::PreconditionFailed
        }
        _ => map_error_common(e),
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for GcsStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        connector::scope(StoreOp::Put, async move {
            let path = path_of(key)?;
            preflight_checksum(&data, opts.checksum)?;
            let os_mode = match &opts.mode {
                PutMode::Overwrite => OsPutMode::Overwrite,
                PutMode::CreateIfAbsent => OsPutMode::Create,
                PutMode::CasVersion(version) => {
                    if !is_generation(&version.0) {
                        return Err(StoreError::Permanent(format!(
                            "GCS CAS token {:?} for {key} is not a generation: a positive \
                             decimal with no leading zero, at most i64::MAX",
                            version.0
                        )));
                    }
                    OsPutMode::Update(UpdateVersion {
                        e_tag: None,
                        version: Some(version.0.clone()),
                    })
                }
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
                .await
                .map_err(|e| map_put_error(e, &opts.mode))?;
            Ok(PutOutcome {
                etag: Etag(etag_of(key, result.e_tag)?),
                version: Version(generation_of(key, result.version)?),
            })
        })
        .await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        connector::scope(StoreOp::Get, self.get_inner(key, range, None))
            .await
            .map(|read| read.outcome)
    }

    /// One GET carrying the pin's ETag as `If-Match` and its generation as the
    /// `generation` selector.
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
        connector::scope(StoreOp::List, self.lister.list_page(prefix, after)).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        // Both are raw names to resume after. A page token is past
        // `start_after`, so it takes precedence.
        let after = match &page {
            Some(PageToken(after)) => Some(after.as_str()),
            None => start_after,
        };
        connector::scope(StoreOp::List, self.lister.list_page(prefix, after)).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        connector::scope(
            StoreOp::ListDelimited,
            self.lister.list_delimited(prefix, crate::MAX_LIST_PAGES),
        )
        .await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        connector::scope(StoreOp::Delete, async move {
            let path = path_of(key)?;
            match self.store.delete(&path).await.map_err(map_error_common) {
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
            multipart: false,
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
