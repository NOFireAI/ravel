//! S3 / MinIO adapter over the `object_store` crate's `AmazonS3` client
//! (ADR-0008). This module never leaks `object_store` types across the
//! [`ObjectStoreBackend`] boundary; every conversion happens here.
//!
//! ## Known divergences from the contract, forced by `object_store`
//!
//! - **Prefix listing is segment-based, not a raw string prefix.**
//!   `object_store`'s list machinery always appends the path delimiter (`/`)
//!   to a non-empty prefix before calling the backend (see
//!   `client::list::ListClientExt::list_paginated`), because S3's own
//!   `ListObjectsV2` `Prefix` parameter is conventionally a "directory"
//!   path. So `list("a", ..)` against this store only matches keys under
//!   `a/`, never a sibling key literally named `a` or `ab` --- unlike
//!   [`crate::memory::MemoryStore`], which does a raw `str::starts_with`.
//!   Callers (and the shared contract suite) MUST use segment-aligned
//!   prefixes (empty, or ending in `/`) for portable behavior.
//! - **`Version` is always the S3 ETag, never `object_store`'s own
//!   `PutResult::version`.** On a versioned bucket that field is an S3
//!   version-id, but `AmazonS3`'s conditional-put path
//!   (`aws::mod::PutMode::Update`) only ever reads `UpdateVersion::e_tag`
//!   for the `If-Match` precondition. If we round-tripped the version-id
//!   through our `Version` token, a later `CasVersion` put would send it as
//!   an `If-Match` value and fail forever. We still populate both `e_tag`
//!   and `version` on the outgoing `UpdateVersion` (harmless on AWS, and
//!   correct if a future backend behind this same adapter reads `version`),
//!   but our own `Version`/`Etag` types are always derived from the
//!   response `e_tag`.
//! - **Timeout / throttling detection is best-effort.** `object_store`'s
//!   public `Error` enum does not expose the HTTP status code or timeout
//!   classification for the common case (a retryable error that exhausted
//!   the crate's own internal retries surfaces as `Error::Generic` with an
//!   opaque, crate-private source type). We pattern-match the error's
//!   `Display` text for well-known signals (`"timed out"`, `"429"`, `"503"`,
//!   `"too many requests"`, ...). This is not as precise as inspecting a
//!   status code directly, but it is the only option available outside the
//!   `object_store` crate itself.

use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::{
    GetOptions as OsGetOptions, GetRange as OsGetRange, ObjectStore, ObjectStoreExt,
    PutMode as OsPutMode, PutOptions as OsPutOptions, PutPayload, UpdateVersion,
};

use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, ObjectMeta,
    ObjectStoreBackend, PageToken, PutMode, PutOptions, PutOutcome, StoreError, UploadChecksum,
    Version,
};

/// Default entries per `ListPage`, chosen to line up with S3's own
/// `ListObjectsV2` page size. Overridable per instance via
/// [`S3Store::with_page_size`].
const LIST_PAGE_SIZE: usize = 1000;

/// Explicit configuration for the S3 / MinIO adapter. No environment or
/// credential-chain magic: every value that changes behavior is a field
/// here so tests and production wiring are equally explicit.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Set for MinIO (or any other S3-compatible endpoint); left `None` to
    /// use AWS's regional endpoint.
    pub endpoint: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Allow plain HTTP; needed for a local MinIO without TLS.
    pub allow_http: bool,
    /// Path-style requests (`https://host/bucket/key`) instead of
    /// virtual-hosted style (`https://bucket.host/key`); MinIO deployments
    /// typically require this.
    pub force_path_style: bool,
}

/// S3 / MinIO backend implementing [`ObjectStoreBackend`] over
/// `object_store`'s `AmazonS3` client.
pub struct S3Store {
    store: AmazonS3,
    page_size: usize,
}

impl S3Store {
    pub fn new(config: S3Config) -> Result<Self, StoreError> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_access_key)
            .with_allow_http(config.allow_http)
            .with_virtual_hosted_style_request(!config.force_path_style);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint.clone());
        }
        let store = builder
            .build()
            .map_err(|e| StoreError::Permanent(format!("failed to build S3 client: {e}")))?;
        Ok(S3Store {
            store,
            page_size: LIST_PAGE_SIZE,
        })
    }

    /// Same backend, a smaller `list()` page size. Mirrors
    /// [`crate::memory::MemoryStore::with_page_size`] so the contract
    /// suite's manual-pagination assertion can force real multi-page
    /// continuation (via `list_with_offset`) against a real bucket without
    /// needing 1000+ objects.
    pub fn with_page_size(config: S3Config, page_size: usize) -> Result<Self, StoreError> {
        let mut store = Self::new(config)?;
        store.page_size = page_size.max(1);
        Ok(store)
    }
}

/// `key -> Path`. `object_store::path::Path` percent-encodes a small set of
/// reserved bytes per segment; plain ASCII keys (the only kind this crate's
/// tests and Ravel's own key scheme produce) round-trip exactly.
fn path_of(key: &str) -> Path {
    Path::from(key)
}

/// `prefix -> Option<Path>`, `None` for the empty (whole-bucket) prefix so
/// `object_store` does not append a stray delimiter.
fn prefix_of(prefix: &str) -> Option<Path> {
    if prefix.is_empty() {
        None
    } else {
        Some(Path::from(prefix))
    }
}

fn map_meta(meta: object_store::ObjectMeta) -> Result<ObjectMeta, StoreError> {
    let etag = meta.e_tag.clone().ok_or_else(|| {
        StoreError::Permanent(format!("S3 returned no ETag for {}", meta.location))
    })?;
    Ok(ObjectMeta {
        key: meta.location.to_string(),
        size: meta.size,
        etag: Etag(etag.clone()),
        version: Version(etag),
        last_modified_unix_ms: meta.last_modified.timestamp_millis(),
    })
}

/// Error mapping shared by every non-`put` operation. `put` has its own
/// mode-aware wrapper (see [`map_put_error`]) because conditional-write
/// failures must be interpreted differently depending on `PutMode`.
fn map_error_common(e: object_store::Error) -> StoreError {
    use object_store::Error as E;
    match e {
        E::NotFound { .. } => StoreError::NotFound,
        E::AlreadyExists { .. } => StoreError::AlreadyExists,
        E::Precondition { .. } => StoreError::PreconditionFailed,
        E::NotModified { path, source } => {
            StoreError::Transient(format!("not modified: {path}: {source}"))
        }
        E::PermissionDenied { path, source } => {
            StoreError::AccessDenied(format!("{path}: {source}"))
        }
        E::Unauthenticated { path, source } => {
            StoreError::AccessDenied(format!("{path}: {source}"))
        }
        E::InvalidPath { source } => StoreError::Permanent(format!("invalid path: {source}")),
        E::NotImplemented {
            operation,
            implementer,
        } => StoreError::Permanent(format!("{operation} not implemented by {implementer}")),
        E::UnknownConfigurationKey { store, key } => {
            StoreError::Permanent(format!("unknown configuration key '{key}' for {store}"))
        }
        E::Generic { store, source } => classify_generic(store, source.as_ref()),
        other => StoreError::Permanent(other.to_string()),
    }
}

/// `put`-specific mapping: conditional-write failures surface mode-aware
/// (contract §"Semantics adapters MUST honor" / ADR-0010 §12), regardless
/// of whether `object_store`/the backend classified the failure as
/// `AlreadyExists` (409, typically from `PutMode::Create`) or
/// `Precondition` (412, typically from `PutMode::Update`). Handling both
/// uniformly is deliberate: which status a given S3-compatible backend
/// actually returns for a given mode is not something this crate controls.
/// `Overwrite` has no precondition to fail, so it is never mode-remapped:
/// any error it produces goes through the common mapper unchanged.
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

/// `get`-specific mapping: additionally recognizes a range that the server
/// rejected as unsatisfiable (`start >= object length`), which
/// `object_store` cannot validate client-side without already knowing the
/// object's size.
fn map_get_error(e: object_store::Error) -> StoreError {
    if let object_store::Error::Generic { source, .. } = &e {
        let msg = source.to_string().to_lowercase();
        if msg.contains("range")
            && (msg.contains("satisfiable") || msg.contains("416") || msg.contains("too large"))
        {
            return StoreError::InvalidRange(source.to_string());
        }
    }
    map_error_common(e)
}

/// Best-effort classification of `Error::Generic`, the catch-all
/// `object_store` uses once its own retry loop gives up (or for errors with
/// no dedicated variant). See the module-level doc comment for why this is
/// necessarily string-based rather than status-code-based.
fn classify_generic(
    store: &'static str,
    source: &(dyn std::error::Error + Send + Sync),
) -> StoreError {
    let msg = source.to_string();
    let lower = msg.to_lowercase();
    if lower.contains("timed out") || lower.contains("timeout") || lower.contains("deadline") {
        return StoreError::Timeout;
    }
    if lower.contains("429")
        || lower.contains("too many requests")
        || lower.contains("slow down")
        || lower.contains("throttl")
        || lower.contains("503")
        || lower.contains("service unavailable")
    {
        return StoreError::Throttled {
            retry_after_ms: 1000,
        };
    }
    // `object_store` already retries its own retryable classes (5xx,
    // connection errors, timeouts) internally with backoff; anything that
    // still surfaces as Generic has exhausted those retries, so treat it as
    // transient rather than permanent, letting the caller apply its own
    // backoff per the contract's retry classification.
    StoreError::Transient(format!("{store}: {msg}"))
}

#[async_trait::async_trait]
impl ObjectStoreBackend for S3Store {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if let Some(UploadChecksum::Crc32c(expected)) = opts.checksum {
            let actual = crc32c::crc32c(&data);
            if actual != expected {
                return Err(StoreError::Corrupted(format!(
                    "upload checksum mismatch: expected {expected:08x}, computed {actual:08x}"
                )));
            }
        }
        let os_mode = match &opts.mode {
            PutMode::Overwrite => OsPutMode::Overwrite,
            PutMode::CreateIfAbsent => OsPutMode::Create,
            PutMode::CasVersion(version) => OsPutMode::Update(UpdateVersion {
                e_tag: Some(version.0.clone()),
                version: Some(version.0.clone()),
            }),
        };
        let path = path_of(key);
        let payload = PutPayload::from(data);
        let result = self
            .store
            .put_opts(
                &path,
                payload,
                OsPutOptions {
                    mode: os_mode,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| map_put_error(e, &opts.mode))?;
        let etag = result
            .e_tag
            .ok_or_else(|| StoreError::Permanent(format!("S3 returned no ETag for {key}")))?;
        Ok(PutOutcome {
            etag: Etag(etag.clone()),
            version: Version(etag),
        })
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
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
        let path = path_of(key);
        let result = self
            .store
            .get_opts(
                &path,
                OsGetOptions {
                    range: os_range,
                    ..Default::default()
                },
            )
            .await
            .map_err(map_get_error)?;
        let etag = result
            .meta
            .e_tag
            .clone()
            .ok_or_else(|| StoreError::Permanent(format!("S3 returned no ETag for {key}")))?;
        let total_size = result.meta.size;
        let data = result.bytes().await.map_err(map_error_common)?;
        Ok(GetOutcome {
            data,
            etag: Etag(etag.clone()),
            version: Version(etag),
            total_size,
        })
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let path = path_of(key);
        let meta = self.store.head(&path).await.map_err(map_error_common)?;
        map_meta(meta)
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let prefix_path = prefix_of(prefix);
        let mut stream = match &page {
            Some(PageToken(after)) => {
                let offset = Path::from(after.as_str());
                self.store.list_with_offset(prefix_path.as_ref(), &offset)
            }
            None => self.store.list(prefix_path.as_ref()),
        };
        let mut out = Vec::with_capacity(self.page_size.min(1024));
        while out.len() < self.page_size {
            match stream.next().await {
                Some(Ok(meta)) => out.push(map_meta(meta)?),
                Some(Err(e)) => return Err(map_error_common(e)),
                None => break,
            }
        }
        let next = if out.len() == self.page_size {
            out.last().map(|m| PageToken(m.key.clone()))
        } else {
            None
        };
        Ok(ListPage { objects: out, next })
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let prefix_path = prefix_of(prefix);
        let result = self
            .store
            .list_with_delimiter(prefix_path.as_ref())
            .await
            .map_err(map_error_common)?;
        let objects = result
            .objects
            .into_iter()
            .map(map_meta)
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

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let path = path_of(key);
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            // Idempotent per the contract: deleting a missing key succeeds.
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_error_common(e)),
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            consistent_read: true,
            consistent_list: true,
            create_if_absent: true,
            cas_version: true,
            suffix_range: true,
            upload_checksum: true,
            prefix_list: true,
            multipart: false,
        }
    }
}
