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
//! - **Timeout / throttling detection is partly typed, partly best-effort.**
//!   A retryable error that exhausted `object_store`'s own internal retries
//!   surfaces as `Error::Generic { source, .. }`, whose concrete `source`
//!   type is `object_store`'s crate-private `RetryError` (a `pub struct`
//!   inside `pub(crate) mod client::retry`, so not nameable, and not
//!   downcastable, from this crate). That type is where the HTTP status code
//!   (429, 503, ...) lives, so 429/503/throttle classification has no typed
//!   floor at this layer and stays a `Display`-text heuristic (`"429"`,
//!   `"503"`, `"too many requests"`, `"throttl"`, ...). Timeouts and
//!   connection failures are different: the `RetryError`'s own `source()`
//!   chain contains a publicly nameable [`object_store::client::HttpError`]
//!   whose [`object_store::client::HttpErrorKind`] is a typed
//!   transport-failure signal (`Timeout`, `Connect`, `Interrupted`, ...). So
//!   [`classify_generic`] recovers those by downcast first and only falls
//!   back to the `Display` heuristic when no `HttpError` is in the chain.
//!   See [`classify_generic`] for the single, well-documented classification
//!   path this crate uses for every `Error::Generic`.
//! - **`upload_checksum` is unsupported (`capabilities().upload_checksum ==
//!   false`), by contract design, not oversight.** `object_store` 0.14's
//!   `AmazonS3` client offers only a whole-client, crate-computed checksum
//!   algorithm (`AmazonS3Builder::with_checksum_algorithm`, SHA-256 or
//!   CRC64NVME only) with no per-request hook and no way to attach a
//!   caller-supplied precomputed value; `PutOptions::checksum`'s CRC32C has
//!   nowhere to go on the wire. `put()` still runs it as a local pre-flight
//!   against the input buffer, catching a caller/payload mismatch before we
//!   even talk to S3, but that check proves nothing about bytes actually
//!   received by the server. Because the limitation is permanent and affects
//!   every S3-compatible endpoint, `upload_checksum` is not part of
//!   [`Capabilities::mandatory`] and does not gate startup;
//!   read-time integrity comes from the segment crc32c hierarchy instead.
//!   See `capabilities()` below. The same gap applies per part: a multipart
//!   part's checksum is a local pre-flight too.
//! - **Multipart completion is unconditional.** `object_store` 0.14's
//!   `put_multipart_opts` takes a `PutMultipartOptions` carrying tags,
//!   attributes, and extensions --- no `PutMode` --- so no
//!   `If-None-Match`/`If-Match` precondition can ride on
//!   `CompleteMultipartUpload`. [`S3MultipartUpload::complete`] is therefore
//!   equivalent to a [`PutMode::Overwrite`] put, and `put()` only takes its
//!   multipart path (above [`MULTIPART_THRESHOLD`]) under `Overwrite`: a
//!   `CreateIfAbsent` or `CasVersion` put stays on the single-PUT path at
//!   every size rather than silently dropping the precondition the commit
//!   protocol depends on.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder, AwsCredentialProvider};
use object_store::path::Path;
use object_store::{
    GetOptions as OsGetOptions, GetRange as OsGetRange, MultipartUpload as OsMultipartUpload,
    ObjectStore, ObjectStoreExt, PutMode as OsPutMode, PutOptions as OsPutOptions, PutPayload,
    UpdateVersion,
};

use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PartSequence, PutMode, PutOptions, PutOutcome, StoreError,
    UploadChecksum, Version, multipart_finished, multipart_poisoned,
};

mod credentials;
use credentials::FileCredentialProvider;

/// Default entries per `ListPage`, chosen to line up with S3's own
/// `ListObjectsV2` page size. Overridable per instance via
/// [`S3Store::with_page_size`].
const LIST_PAGE_SIZE: usize = 1000;

/// Part size [`S3Store::put`] cuts an over-threshold payload into: 8 MiB.
///
/// Above S3's 5 MiB non-final-part minimum ([`crate::MULTIPART_MIN_PART_SIZE`]) with
/// margin, and a fixed size for every part but the last, which is what the
/// strictest S3-compatible backends require (R2 rejects mixed non-final part
/// sizes). 8 MiB also keeps the part count low enough that S3's 10 000-part
/// ceiling only binds at 80 GiB, far above any segment Ravel writes.
pub const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

/// Payload size above which [`S3Store::put`] switches from one PUT to a
/// multipart upload: 16 MiB, exactly two [`MULTIPART_PART_SIZE`] parts.
///
/// Chosen so the multipart path is never degenerate: a payload that takes it
/// always produces at least two parts, and every part but the last is exactly
/// 8 MiB. A lower threshold would produce single-part multipart uploads (three
/// round trips where one PUT would do, for no benefit); a much higher one
/// would leave large L1/L2 compaction outputs on the single-PUT path, whose
/// failure mode is re-sending the entire object.
pub const MULTIPART_THRESHOLD: usize = 2 * MULTIPART_PART_SIZE;

// The chunking constants satisfy S3's part rules by construction, checked at
// compile time rather than by a test: non-final parts at or above the 5 MiB
// minimum and all the same size, never fewer than two parts on the multipart
// path, and a part ceiling that only binds at 80 GiB, far above any object
// compaction produces (`max_l1_part_bytes` is measured in MiB).
const _: () = assert!(MULTIPART_PART_SIZE >= crate::MULTIPART_MIN_PART_SIZE);
const _: () = assert!(MULTIPART_THRESHOLD == 2 * MULTIPART_PART_SIZE);
const _: () = assert!(crate::MULTIPART_MAX_PARTS * MULTIPART_PART_SIZE >= 64 * 1024 * 1024 * 1024);

/// How many part uploads [`S3Store::put`] keeps in flight. Bounded because a
/// large object cut into 8 MiB parts can be hundreds of parts, and a
/// compactor writing several objects at once must not open an unbounded
/// number of connections per object.
const MULTIPART_UPLOAD_CONCURRENCY: usize = 4;

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
    /// Per-tenant SSE-KMS key id for bring-your-own-key encryption
    /// (ADR-0042 decision 1). `Some(key)` makes [`S3Store::new`] call
    /// `object_store`'s `with_sse_kms_encryption`, so S3 encrypts every PUT
    /// under this key inside AWS itself; Ravel adds no crypto code and does
    /// not manage keys --- BYOK means the tenant supplies (and can revoke)
    /// their own `kms_key_id`. `None` (every current caller) changes
    /// nothing: whatever bucket-default SSE the deployment has continues to
    /// apply, exactly as before. Single-layer SSE-KMS is the deliberate
    /// default; dual-layer (DSSE) is available from the same builder via
    /// `with_dsse_kms_encryption` if a future requirement needs it, but
    /// nothing here builds for that today.
    pub kms_key_id: Option<String>,
    /// Temporary AWS session token (ADR-0072 decision 1), paired with
    /// `access_key_id`/`secret_access_key` for STS-issued or IRSA-style
    /// credentials. Ignored when `credentials_file` is `Some`: the file
    /// wins. `None` (every current caller: no shipped binary sets this
    /// field yet, flags land with EE-T4/EE-T6) changes nothing.
    pub session_token: Option<String>,
    /// Path to a JSON file of `{access_key_id, secret_access_key,
    /// session_token}` (`session_token` optional), for credentials an
    /// external process rotates on disk -- a Kubernetes secret mount, an STS
    /// sidecar, IRSA-style token projection (ADR-0072 decision 1). Ravel
    /// itself never calls STS; this only makes an externally-minted rotating
    /// credential expressible.
    ///
    /// **Rotation contract.** [`S3Store::new`] reads and parses this file
    /// once at construction; an unreadable or malformed file fails
    /// construction with a typed [`StoreError`] (fail fast at startup, never
    /// a panic). After that, the file is re-read lazily, only on
    /// request-path credential access (inside `object_store`'s per-request
    /// `CredentialProvider::get_credential`), never from a background
    /// thread with its own lifecycle: unchanged mtime costs one `stat()` and
    /// returns the cached credential, changed mtime triggers a re-read and,
    /// on success, an atomic swap so every *subsequent* `get_credential`
    /// call sees the new credential while a request that already obtained
    /// the old one finishes on it unaffected. A parse failure while
    /// rotating (unlike at construction) never fails the request: the
    /// last-good credential is kept and a rate-limited warning is logged
    /// instead. When both `credentials_file` and inline
    /// `access_key_id`/`secret_access_key`/`session_token` are set, the file
    /// wins.
    pub credentials_file: Option<PathBuf>,
}

/// S3 / MinIO backend implementing [`ObjectStoreBackend`] over
/// `object_store`'s `AmazonS3` client.
pub struct S3Store {
    store: AmazonS3,
    page_size: usize,
    /// `Some` only when [`S3Config::credentials_file`] was set; kept
    /// alongside the `AmazonS3` client (which holds its own clone as an
    /// opaque `object_store::CredentialProvider` trait object) purely so
    /// [`S3Store::credential_rotation_failures`] has something to read.
    credential_provider: Option<Arc<FileCredentialProvider>>,
}

impl S3Store {
    pub fn new(config: S3Config) -> Result<Self, StoreError> {
        let (builder, credential_provider) = Self::builder(&config)?;
        let store = builder
            .build()
            .map_err(|e| StoreError::Permanent(format!("failed to build S3 client: {e}")))?;
        Ok(S3Store {
            store,
            page_size: LIST_PAGE_SIZE,
            credential_provider,
        })
    }

    /// Count of [`S3Config::credentials_file`] rotation attempts (a
    /// request-path mtime change) that failed to read or parse the rotated
    /// file and fell back to last-good credentials (ADR-0072 decision 1).
    /// Always `0` when `credentials_file` is unset.
    pub fn credential_rotation_failures(&self) -> u64 {
        self.credential_provider
            .as_deref()
            .map(FileCredentialProvider::rotation_failures)
            .unwrap_or(0)
    }

    /// Build the `AmazonS3Builder` from a [`S3Config`], with no network
    /// access (`build()` only validates local config shape). Split out from
    /// [`S3Store::new`] so a test can assert the fully-configured builder
    /// without a live endpoint. The `kms_key_id` branch is the only
    /// behavioral addition over the historical build path: when it is `None`
    /// this produces byte-for-byte the same builder as before ADR-0042.
    /// Fails (fail-fast, per [`S3Config::credentials_file`]'s rotation
    /// contract) only when `credentials_file` is `Some` and that file is
    /// unreadable or not valid JSON in the expected shape; every other
    /// config shape only sets local builder fields and cannot fail here.
    /// Returns the [`FileCredentialProvider`] alongside the builder (rather
    /// than only handing it to `with_credentials` as an opaque trait object)
    /// so [`S3Store::new`] can keep its own handle for observability.
    fn builder(
        config: &S3Config,
    ) -> Result<(AmazonS3Builder, Option<Arc<FileCredentialProvider>>), StoreError> {
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
        // Single-key SSE-KMS (ADR-0042 decision 1): the KMS call happens
        // inside S3 on every PUT, no crypto code here. Dual-layer DSSE is
        // reachable via `with_dsse_kms_encryption` on this same builder if a
        // future requirement needs it; single-layer is the sufficient default.
        if let Some(kms_key_id) = &config.kms_key_id {
            builder = builder.with_sse_kms_encryption(kms_key_id);
        }
        // Rotating file credentials win over inline ones (ADR-0072 decision
        // 1, S3Config::credentials_file doc comment): `with_credentials`
        // overrides the access_key_id/secret_access_key/token set above.
        // `FileCredentialProvider::load` does the fail-fast read+parse this
        // function's Result exists for.
        let mut credential_provider = None;
        if let Some(credentials_file) = &config.credentials_file {
            let provider = Arc::new(FileCredentialProvider::load(credentials_file.clone())?);
            builder = builder.with_credentials(Arc::clone(&provider) as AwsCredentialProvider);
            credential_provider = Some(provider);
        } else if let Some(session_token) = &config.session_token {
            builder = builder.with_token(session_token.clone());
        }
        Ok((builder, credential_provider))
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

/// Walk an error's [`std::error::Error::source`] chain looking for
/// `object_store`'s publicly nameable [`object_store::client::HttpError`],
/// returning its [`object_store::client::HttpErrorKind`] if present.
///
/// This is the one typed signal recoverable from an `Error::Generic` at this
/// layer. The `Generic` source is `object_store`'s crate-private `RetryError`
/// (not nameable, so not directly downcastable), but for transport failures
/// its `source()` chain carries an `HttpError`, whose `kind()` distinguishes a
/// timeout from a connection drop without any string matching. HTTP *status*
/// codes (429/503) are not reachable this way: they live in the crate-private
/// `RetryError`/`RequestError`, with no `HttpError` in the chain, so
/// [`classify_generic`] falls back to a `Display` heuristic for those.
fn typed_http_kind(
    source: &(dyn std::error::Error + Send + Sync + 'static),
) -> Option<object_store::client::HttpErrorKind> {
    if let Some(http) = source.downcast_ref::<object_store::client::HttpError>() {
        return Some(http.kind());
    }
    let mut current = source.source();
    while let Some(err) = current {
        if let Some(http) = err.downcast_ref::<object_store::client::HttpError>() {
            return Some(http.kind());
        }
        current = err.source();
    }
    None
}

/// The single classification path for `Error::Generic`, the catch-all
/// `object_store` uses once its own retry loop gives up (or for errors with no
/// dedicated typed variant). Every operation funnels its `Generic` errors here
/// (via [`map_error_common`], and [`map_put_error`]/[`map_get_error`] which
/// delegate to it), so this one function decides `Timeout` vs `Throttled` vs
/// `Transient` for the whole S3/MinIO adapter.
///
/// Two tiers, in order:
///
/// 1. **Typed transport kind (preferred).** [`typed_http_kind`] downcasts the
///    source chain to [`object_store::client::HttpError`]. A `Timeout` kind
///    maps to [`StoreError::Timeout`]; `Connect`/`Request`/`Interrupted` are
///    retryable transport failures and map to [`StoreError::Transient`]. This
///    is robust against `object_store` changing its error *text*: it reads the
///    typed kind, not a substring.
/// 2. **`Display`-text heuristic (fallback).** When no `HttpError` is in the
///    chain (notably the 429/503 throttle case, whose status is trapped in
///    `object_store`'s crate-private `RetryError`), match the lowercased
///    message for well-known signals: timeout words to [`StoreError::Timeout`],
///    429/503/throttle words to [`StoreError::Throttled`].
///
/// Anything unmatched is [`StoreError::Transient`], never `Permanent`:
/// `object_store` already retried its own retryable classes (5xx, connection
/// errors, timeouts) internally with backoff, so a `Generic` that still
/// surfaces has exhausted those retries; treating it as transient lets the
/// caller apply its own backoff per the contract's retry classification. The
/// outcome set ([`StoreError::Timeout`]/[`StoreError::Throttled`]/
/// [`StoreError::Transient`]) is unchanged from the historical string-only
/// version, so retry policy is unaffected; only the *precision* of the timeout
/// case improved.
fn classify_generic(
    store: &'static str,
    source: &(dyn std::error::Error + Send + Sync + 'static),
) -> StoreError {
    use object_store::client::HttpErrorKind as Kind;
    // Tier 1: typed transport-failure kind from the source chain.
    if let Some(kind) = typed_http_kind(source) {
        match kind {
            Kind::Timeout => return StoreError::Timeout,
            Kind::Connect | Kind::Request | Kind::Interrupted => {
                return StoreError::Transient(format!("{store}: {source}"));
            }
            // Decode/Unknown (and any future non_exhaustive kind) carry no
            // clear retry signal on their own; fall through to the heuristic.
            _ => {}
        }
    }

    // Tier 2: Display-text heuristic. This is the only floor for 429/503,
    // whose HTTP status is not reachable through any nameable type here.
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
    StoreError::Transient(format!("{store}: {msg}"))
}

/// Local CRC32C pre-flight, shared by [`S3Store::put`] and
/// [`S3MultipartUpload::put_part`]: recomputes the digest over the buffer we
/// are about to hand `object_store` and rejects a caller/payload mismatch
/// before any network call. It is never attached to the outgoing request and
/// proves nothing about bytes actually landing on S3/MinIO; see the doc
/// comment on [`S3Store::capabilities`] for why this crate cannot close that
/// gap with `object_store` 0.14's `AmazonS3` client.
fn preflight_checksum(data: &Bytes, checksum: Option<UploadChecksum>) -> Result<(), StoreError> {
    if let Some(UploadChecksum::Crc32c(expected)) = checksum {
        let actual = crc32c::crc32c(data);
        if actual != expected {
            return Err(StoreError::Corrupted(format!(
                "upload checksum mismatch: expected {expected:08x}, computed {actual:08x}"
            )));
        }
    }
    Ok(())
}

/// `PutResult -> PutOutcome`. Both our `Etag` and our `Version` come from the
/// response ETag, never `PutResult::version` (module doc, second divergence).
fn outcome_of(key: &str, result: object_store::PutResult) -> Result<PutOutcome, StoreError> {
    let etag = result
        .e_tag
        .ok_or_else(|| StoreError::Permanent(format!("S3 returned no ETag for {key}")))?;
    Ok(PutOutcome {
        etag: Etag(etag.clone()),
        version: Version(etag),
    })
}

/// One in-flight S3 multipart upload: `CreateMultipartUpload` already issued,
/// `UploadPart` per [`MultipartUpload::put_part`] call, and
/// `CompleteMultipartUpload` / `AbortMultipartUpload` at the end.
///
/// Part numbers follow the order `put_part` is called in, and the object
/// becomes visible only when `complete` succeeds --- S3's own multipart
/// guarantee, which is what makes an interrupted compaction upload wasted work
/// rather than a partial object. Nothing here retries: `object_store`'s client
/// already retries each request internally.
///
/// A part upload that still fails after those internal retries is
/// unrecoverable, so it *poisons* the handle rather than inviting
/// the caller to retry the part. `object_store`'s `S3MultiPartUpload`
/// increments its part index synchronously at `put_part` call time and
/// `complete` errors unless it holds exactly that many parts, so a failed part
/// leaves a permanent hole: a retried part lands at a *new* index and the hole
/// never fills. The first failure surfaces the classified `StoreError` a `put`
/// would (so a diagnostic keeps the original cause), but every later `put_part`
/// or `complete` returns a non-retryable [`multipart_poisoned`] error telling
/// the caller to abort and restart. A part-sequence violation poisons the same
/// way. `abort` stays callable on a poisoned handle.
pub struct S3MultipartUpload {
    key: String,
    upload: Box<dyn OsMultipartUpload>,
    sequence: PartSequence,
    /// Set by `complete`/`abort`; every later call on this handle fails
    /// instead of issuing a second request against a dead upload id.
    finished: bool,
    /// Set once a part upload fails at the backend or a part
    /// violates the sequence rules. Carries the original cause's
    /// text; every later `put_part`/`complete` fails with [`multipart_poisoned`]
    /// while `abort` stays callable. A checksum mismatch deliberately does not
    /// set this (it leaves the upload open for a re-send).
    poison: Option<String>,
}

#[async_trait::async_trait]
impl MultipartUpload for S3MultipartUpload {
    async fn put_part(
        &mut self,
        data: Bytes,
        checksum: Option<UploadChecksum>,
    ) -> Result<(), StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        if let Some(cause) = &self.poison {
            return Err(multipart_poisoned(&self.key, cause));
        }
        // The checksum pre-flight runs before anything touches upload state and
        // is the one recoverable rejection: a mismatch is not a part, the
        // upload stays open, and the caller can re-send the same bytes.
        preflight_checksum(&data, checksum)?;
        // A sequence-rule violation poisons the handle: once
        // `accept` counts a short non-final part or an empty part, the object
        // would be truncated, so no further part or completion may proceed.
        if let Err(e) = self.sequence.accept(&self.key, data.len()) {
            self.poison = Some(e.to_string());
            return Err(e);
        }
        // A backend part failure poisons the handle: the part
        // index is already spent, so a retry would land at a new index and
        // `complete` could never assemble a whole object. The first failure
        // surfaces the classified cause; later calls get the poison error.
        match self.upload.put_part(PutPayload::from(data)).await {
            Ok(()) => Ok(()),
            Err(e) => {
                let mapped = map_error_common(e);
                self.poison = Some(mapped.to_string());
                Err(mapped)
            }
        }
    }

    async fn complete(&mut self) -> Result<PutOutcome, StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        if let Some(cause) = &self.poison {
            return Err(multipart_poisoned(&self.key, cause));
        }
        self.sequence.finish(&self.key)?;
        let result = self.upload.complete().await.map_err(map_error_common)?;
        self.finished = true;
        outcome_of(&self.key, result)
    }

    async fn abort(&mut self) -> Result<(), StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        // Marked finished before the request: whether or not
        // `AbortMultipartUpload` reaches the server, this handle is spent, and
        // an upload S3 never heard the abort for is orphaned parts (billed
        // until a lifecycle rule reaps them), never a visible object.
        self.finished = true;
        self.upload.abort().await.map_err(map_error_common)
    }
}

impl S3Store {
    /// The [`MULTIPART_THRESHOLD`] path of [`ObjectStoreBackend::put`]: cut the
    /// buffer into [`MULTIPART_PART_SIZE`] parts, upload at most
    /// [`MULTIPART_UPLOAD_CONCURRENCY`] of them at a time, then complete. Any
    /// failure aborts the upload best-effort (so parts are not left billed)
    /// and surfaces the original error, never a partial object.
    async fn put_via_multipart(&self, key: &str, data: Bytes) -> Result<PutOutcome, StoreError> {
        // Only reachable for an in-memory buffer above 80 GiB, but refuse
        // before opening an upload S3 would reject at completion.
        let part_count = data.len().div_ceil(MULTIPART_PART_SIZE);
        if part_count > crate::MULTIPART_MAX_PARTS {
            return Err(StoreError::Permanent(format!(
                "put of {key}: {} bytes needs {part_count} parts of \
                 {MULTIPART_PART_SIZE} bytes, over the {}-part limit",
                data.len(),
                crate::MULTIPART_MAX_PARTS
            )));
        }
        let path = path_of(key);
        let mut upload = self
            .store
            .put_multipart(&path)
            .await
            .map_err(map_error_common)?;

        // Every part but the last is exactly MULTIPART_PART_SIZE, which is
        // both above S3's minimum and uniform, as R2-class backends require.
        // Slices are zero-copy views of the caller's buffer.
        let mut pending = Vec::with_capacity(data.len().div_ceil(MULTIPART_PART_SIZE));
        let mut offset = 0usize;
        while offset < data.len() {
            let end = (offset + MULTIPART_PART_SIZE).min(data.len());
            pending.push(upload.put_part(PutPayload::from(data.slice(offset..end))));
            offset = end;
        }

        // `UploadPart` futures are `'static`, so they can be driven with
        // bounded concurrency after all of them have been handed out; part
        // numbers were fixed by the `put_part` call order above, so completing
        // out of order does not reorder the object.
        let mut failure = None;
        {
            let mut inflight =
                futures::stream::iter(pending).buffer_unordered(MULTIPART_UPLOAD_CONCURRENCY);
            while let Some(result) = inflight.next().await {
                if let Err(e) = result {
                    failure = Some(map_error_common(e));
                    break;
                }
            }
        }
        if let Some(e) = failure {
            let _ = upload.abort().await;
            return Err(e);
        }

        match upload.complete().await {
            Ok(result) => outcome_of(key, result),
            Err(e) => {
                let _ = upload.abort().await;
                Err(map_error_common(e))
            }
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for S3Store {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        preflight_checksum(&data, opts.checksum)?;
        // Large payloads go out as a multipart upload, but only under
        // `Overwrite`: `object_store` 0.14 has no conditional
        // `CompleteMultipartUpload` (`PutMultipartOptions` carries tags and
        // attributes, no `PutMode`), so routing a conditional put through
        // multipart would silently drop the precondition the commit protocol
        // depends on. A `CreateIfAbsent`/`CasVersion` put therefore stays on
        // the single-PUT path at every size (S3's 5 GiB single-request limit
        // is the ceiling there). See docs/object-store-contract.md,
        // "Multipart upload".
        if matches!(opts.mode, PutMode::Overwrite) && data.len() > MULTIPART_THRESHOLD {
            return self.put_via_multipart(key, data).await;
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
        outcome_of(key, result)
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        let upload = self
            .store
            .put_multipart(&path_of(key))
            .await
            .map_err(map_error_common)?;
        Ok(Box::new(S3MultipartUpload {
            key: key.to_string(),
            upload,
            sequence: PartSequence::default(),
            finished: false,
            poison: None,
        }))
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
            // `false`, not a placeholder: `object_store` 0.14's `AmazonS3`
            // client has no per-request checksum hook at all. Its only
            // upload-integrity knob is `AmazonS3Builder::with_checksum_algorithm`,
            // which (a) is a whole-client setting, not something `put()` can
            // apply per-call from `PutOptions::checksum`, (b) only offers
            // `Checksum::SHA256` / `Checksum::CRC64NVME`, not CRC32C, and (c)
            // always has the crate compute the digest itself from the
            // payload it is about to send -- there is no way to hand it a
            // caller-supplied precomputed value to attach to the wire
            // request (see `Client::with_payload` in `object_store`'s
            // `aws/client.rs`). So `put()`'s CRC32C check above can only ever
            // be a local pre-flight against our own input buffer; it cannot
            // catch corruption introduced in transit, so this reports the
            // capability as unsupported rather than claiming integrity the
            // adapter does not provide. The flag is not startup-gating: it is
            // not in `Capabilities::mandatory()` precisely because no
            // S3-compatible endpoint can satisfy it through this client
            //, and read-time integrity comes from the
            // footer/section/page crc32c hierarchy instead
            // (docs/object-store-contract.md "Upload checksums").
            upload_checksum: false,
            prefix_list: true,
            // Real: `put_multipart` above drives
            // CreateMultipartUpload/UploadPart/CompleteMultipartUpload/
            // AbortMultipartUpload through `object_store`, and `put` itself
            // takes that path above `MULTIPART_THRESHOLD`. This is the flag
            // `required_capabilities(Mode::Maintain)` adds, so `--mode
            // maintain` starts against an S3-compatible backend.
            multipart: true,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::{PutResult, UploadPart};

    use super::*;

    /// A fake `object_store` multipart upload whose every `put_part` fails,
    /// modeling a backend part upload that already exhausted `object_store`'s
    /// internal retries. `complete` is wired to fail too, because the poison
    /// logic must guarantee it is never reached after a failed part. `abort`
    /// calls are counted so the test can prove `abort` still runs on a poisoned
    /// handle.
    #[derive(Debug)]
    struct FailingPartUpload {
        aborts: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl OsMultipartUpload for FailingPartUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(async {
                Err(object_store::Error::Generic {
                    store: "test",
                    source: "injected part upload failure".into(),
                })
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            Err(object_store::Error::Generic {
                store: "test",
                source: "complete must never be reached on a poisoned handle".into(),
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A backend `put_part` failure poisons the S3 handle: the
    /// first failure surfaces the classified (here retryable) cause, but every
    /// later `put_part`/`complete` returns a non-retryable poison error telling
    /// the caller to abort and restart, breaking the retry-forever live-lock.
    /// `abort` stays callable and reaches the backend.
    #[tokio::test]
    async fn backend_put_part_failure_poisons_handle() {
        let aborts = Arc::new(AtomicUsize::new(0));
        let mut handle = S3MultipartUpload {
            key: "poison".to_string(),
            upload: Box::new(FailingPartUpload {
                aborts: Arc::clone(&aborts),
            }),
            sequence: PartSequence::default(),
            finished: false,
            poison: None,
        };

        let part = Bytes::from(vec![0u8; crate::MULTIPART_MIN_PART_SIZE]);
        // First failure: the classified backend error (Transient here), which
        // on its own would invite a retry -- exactly the live-lock #296 fixes.
        let first = handle
            .put_part(part.clone(), None)
            .await
            .expect_err("the backend part upload must fail");
        assert!(matches!(first, StoreError::Transient(_)), "got {first:?}");

        // The handle is now poisoned: a retried part is refused non-retryably,
        // so the caller's is_retryable-driven loop stops instead of spinning.
        let retried = handle
            .put_part(part, None)
            .await
            .expect_err("a poisoned handle must refuse further parts");
        assert!(
            matches!(retried, StoreError::Permanent(_)),
            "got {retried:?}"
        );
        assert!(!retried.is_retryable());

        // complete is likewise poisoned: no truncated object may be published,
        // and the fake's own failing complete proves it was never reached.
        let completed = handle
            .complete()
            .await
            .expect_err("completing a poisoned upload must fail");
        assert!(matches!(completed, StoreError::Permanent(_)));

        // abort stays callable and actually reaches the backend.
        handle
            .abort()
            .await
            .expect("abort after poison must succeed");
        assert_eq!(aborts.load(Ordering::SeqCst), 1);

        // The handle is now spent: a later call fails as finished, not poisoned.
        let after_abort = handle
            .put_part(Bytes::from_static(b"late"), None)
            .await
            .expect_err("put_part after abort must fail");
        assert!(matches!(after_abort, StoreError::Permanent(_)));
    }

    fn test_config() -> S3Config {
        S3Config {
            bucket: "ravel-test".to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some("http://localhost:0".to_string()),
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            allow_http: true,
            force_path_style: true,
            kms_key_id: None,
            session_token: None,
            credentials_file: None,
        }
    }

    /// `S3Store` declares the capability `required_capabilities(Mode::
    /// Maintain)` demands, and it is not a claim about the endpoint: the
    /// adapter implements the create/upload-part/complete/abort sequence for
    /// every S3-compatible backend. `S3Store::new` only validates
    /// configuration, so no endpoint is needed here.
    #[test]
    fn capabilities_declare_multipart() {
        let store = S3Store::new(test_config()).expect("dummy config must build");
        assert!(store.capabilities().multipart);
    }

    /// A `kms_key_id: Some(..)` config builds successfully (ADR-0042
    /// decision 1). `AmazonS3Builder::build()` only validates local config
    /// shape --- no live AWS credentials, no reachable KMS key, no network
    /// --- so this is deterministic and needs no Docker/AWS (confirmed:
    /// `capabilities_declare_multipart` above already relies on the same
    /// no-network `new()` against an unreachable `localhost:0` endpoint).
    #[test]
    fn sse_kms_config_builds() {
        let mut config = test_config();
        config.kms_key_id = Some("arn:aws:kms:us-east-1:111122223333:key/abcd".to_string());
        S3Store::new(config).expect("SSE-KMS config must build without live credentials");
    }

    /// `kms_key_id: None` yields byte-for-byte the same builder configuration
    /// as the pre-ADR-0042 build path, so the unconfigured case (every
    /// current caller) has no accidental behavior change. Comparing the
    /// builder's `Debug` (it derives `Debug` but not `PartialEq`) proves the
    /// `None` branch touches no encryption field, and `Some` does.
    #[test]
    fn none_kms_key_leaves_builder_unchanged() {
        let config = test_config();
        assert!(config.kms_key_id.is_none());

        // The historical build path, reproduced verbatim without any KMS knob.
        let mut baseline = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_access_key)
            .with_allow_http(config.allow_http)
            .with_virtual_hosted_style_request(!config.force_path_style);
        if let Some(endpoint) = &config.endpoint {
            baseline = baseline.with_endpoint(endpoint.clone());
        }

        assert_eq!(
            format!(
                "{:?}",
                S3Store::builder(&config).expect("no credentials file").0
            ),
            format!("{baseline:?}"),
            "None kms_key_id must not change the builder"
        );

        // And a configured key must change it, or the test above is vacuous.
        let mut with_key = config.clone();
        with_key.kms_key_id = Some("arn:aws:kms:us-east-1:111122223333:key/abcd".to_string());
        assert_ne!(
            format!(
                "{:?}",
                S3Store::builder(&with_key).expect("no credentials file").0
            ),
            format!("{baseline:?}"),
            "Some kms_key_id must change the builder"
        );
    }

    #[test]
    fn session_token_reaches_the_builder() {
        let baseline = test_config();
        let mut with_token = baseline.clone();
        with_token.session_token = Some("FwoGZXIvYXdzEBc".to_string());
        assert_ne!(
            format!(
                "{:?}",
                S3Store::builder(&with_token)
                    .expect("no credentials file")
                    .0
            ),
            format!(
                "{:?}",
                S3Store::builder(&baseline).expect("no credentials file").0
            ),
            "a session token must change the builder"
        );
    }

    #[tokio::test]
    async fn credentials_file_wins_over_inline_credentials_and_token() {
        use object_store::CredentialProvider as _;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("creds.json");
        std::fs::write(
            &path,
            r#"{"access_key_id":"AKIA_FILE","secret_access_key":"file-secret"}"#,
        )
        .expect("write creds");

        let mut config = test_config();
        config.session_token = Some("inline-token".to_string());
        config.credentials_file = Some(path);
        let (_, provider) = S3Store::builder(&config).expect("valid credentials file");
        let provider = provider.expect("a credentials file must produce a provider");
        let credential = provider.get_credential().await.expect("file credentials");
        assert_eq!(
            credential.key_id, "AKIA_FILE",
            "the file's credentials must win over inline ones"
        );
        assert_eq!(
            credential.token, None,
            "a token comes from the file, never mixed in from inline config"
        );
    }

    // --- Classification of Error::Generic ---
    //
    // These pin the StoreError kind AND retryable() that the S3/MinIO
    // get/put/list error path produces for each representative error shape.
    // A future object_store bump that changes error text (tier 2) or the
    // typed HttpError API (tier 1) fails one of these loudly instead of
    // silently misclassifying a retryable error as permanent (or vice versa).

    use object_store::client::{HttpError, HttpErrorKind};

    /// A minimal opaque error usable as an `Error::Generic` source when the
    /// test only cares about the `Display` text (tier-2 heuristic path).
    #[derive(Debug)]
    struct TextError(&'static str);

    impl std::fmt::Display for TextError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for TextError {}

    /// An error whose `source()` yields the given boxed error, modeling
    /// `object_store`'s real nesting (`RetryError` -> `RequestError` ->
    /// `HttpError`) so the source-chain walk in [`typed_http_kind`] is
    /// exercised, not just a directly-embedded `HttpError`.
    #[derive(Debug)]
    struct WrapError {
        source: Box<dyn std::error::Error + Send + Sync>,
    }

    impl std::fmt::Display for WrapError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "outer wrapper: {}", self.source)
        }
    }

    impl std::error::Error for WrapError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.source.as_ref())
        }
    }

    fn generic(source: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: Box::new(source),
        }
    }

    fn http_error(kind: HttpErrorKind) -> HttpError {
        HttpError::new(kind, std::io::Error::other("injected transport error"))
    }

    /// Tier 1: a typed `HttpErrorKind::Timeout` in the source chain classifies
    /// as `Timeout` (retryable) without any string matching -- proven by giving
    /// the error Display text with no timeout words at all.
    #[test]
    fn typed_http_timeout_classifies_without_string_match() {
        let err = generic(http_error(HttpErrorKind::Timeout));
        let mapped = map_error_common(err);
        assert!(matches!(mapped, StoreError::Timeout), "got {mapped:?}");
        assert!(mapped.is_retryable());

        // And nested one level deeper, as object_store really wraps it.
        let nested = generic(WrapError {
            source: Box::new(http_error(HttpErrorKind::Timeout)),
        });
        let mapped = map_error_common(nested);
        assert!(
            matches!(mapped, StoreError::Timeout),
            "source-chain walk must find the nested HttpError, got {mapped:?}"
        );
    }

    /// Tier 1: typed connection-class transport kinds are retryable and map to
    /// `Transient` (unchanged outcome, now typed rather than defaulted).
    #[test]
    fn typed_http_connection_kinds_classify_as_transient() {
        for kind in [
            HttpErrorKind::Connect,
            HttpErrorKind::Request,
            HttpErrorKind::Interrupted,
        ] {
            let mapped = map_error_common(generic(http_error(kind)));
            assert!(
                matches!(mapped, StoreError::Transient(_)),
                "{kind:?} -> {mapped:?}"
            );
            assert!(mapped.is_retryable(), "{kind:?} must be retryable");
        }
    }

    /// Tier 2: with no `HttpError` in the chain, the `Display` heuristic is the
    /// floor. Pins the timeout / 429 / 503 / throttle / opaque shapes.
    #[test]
    fn display_heuristic_pins_kind_and_retryability() {
        struct Case {
            text: &'static str,
            expect_throttled: bool,
            expect_timeout: bool,
        }
        let cases = [
            Case {
                text: "connection timed out after 30s",
                expect_throttled: false,
                expect_timeout: true,
            },
            Case {
                text: "Server returned non-2xx status code: 429 Too Many Requests",
                expect_throttled: true,
                expect_timeout: false,
            },
            Case {
                text: "Server returned non-2xx status code: 503 Service Unavailable",
                expect_throttled: true,
                expect_timeout: false,
            },
            Case {
                text: "request was throttled by the backend",
                expect_throttled: true,
                expect_timeout: false,
            },
            // Opaque post-retry failure: retryable Transient, never Permanent.
            Case {
                text: "connection reset by peer",
                expect_throttled: false,
                expect_timeout: false,
            },
        ];
        for case in cases {
            let mapped = map_error_common(generic(TextError(case.text)));
            if case.expect_timeout {
                assert!(matches!(mapped, StoreError::Timeout), "{}", case.text);
            } else if case.expect_throttled {
                assert!(
                    matches!(mapped, StoreError::Throttled { .. }),
                    "{}",
                    case.text
                );
            } else {
                assert!(matches!(mapped, StoreError::Transient(_)), "{}", case.text);
            }
            // Every Generic classification outcome is retryable.
            assert!(mapped.is_retryable(), "{} must be retryable", case.text);
        }
    }

    /// The typed variants object_store already surfaces are mapped by variant,
    /// not by string, and their retryability matches the contract: NotFound /
    /// AlreadyExists / Precondition are terminal (not retryable).
    #[test]
    fn typed_variants_map_by_variant_and_are_not_retryable() {
        let not_found = map_error_common(object_store::Error::NotFound {
            path: "k".into(),
            source: Box::new(TextError("no such key")),
        });
        assert!(matches!(not_found, StoreError::NotFound));
        assert!(!not_found.is_retryable());

        let already = map_error_common(object_store::Error::AlreadyExists {
            path: "k".into(),
            source: Box::new(TextError("exists")),
        });
        assert!(matches!(already, StoreError::AlreadyExists));
        assert!(!already.is_retryable());

        let precondition = map_error_common(object_store::Error::Precondition {
            path: "k".into(),
            source: Box::new(TextError("if-match failed")),
        });
        assert!(matches!(precondition, StoreError::PreconditionFailed));
        assert!(!precondition.is_retryable());
    }

    /// `map_put_error` remaps a conditional-write precondition failure by mode,
    /// regardless of whether object_store reported it as AlreadyExists (409) or
    /// Precondition (412), and both outcomes are terminal.
    #[test]
    fn put_error_maps_conditional_failure_by_mode() {
        for reported in [
            object_store::Error::AlreadyExists {
                path: "k".into(),
                source: Box::new(TextError("409")),
            },
            object_store::Error::Precondition {
                path: "k".into(),
                source: Box::new(TextError("412")),
            },
        ] {
            let create = map_put_error(clone_err(&reported), &PutMode::CreateIfAbsent);
            assert!(matches!(create, StoreError::AlreadyExists), "{create:?}");
            assert!(!create.is_retryable());

            let cas = map_put_error(reported, &PutMode::CasVersion(crate::Version("v1".into())));
            assert!(matches!(cas, StoreError::PreconditionFailed), "{cas:?}");
            assert!(!cas.is_retryable());
        }
    }

    /// `map_get_error` recognizes an unsatisfiable range (416) as `InvalidRange`
    /// (a terminal caller error), before delegating anything else to the shared
    /// classifier.
    #[test]
    fn get_error_maps_unsatisfiable_range() {
        let err = generic(TextError(
            "the requested range is not satisfiable: 416 Range Not Satisfiable",
        ));
        let mapped = map_get_error(err);
        assert!(matches!(mapped, StoreError::InvalidRange(_)), "{mapped:?}");
        assert!(!mapped.is_retryable());

        // A get Generic with no range signal still flows through classify_generic.
        let timeout = map_get_error(generic(http_error(HttpErrorKind::Timeout)));
        assert!(matches!(timeout, StoreError::Timeout));
    }

    /// `object_store::Error` is not `Clone`; rebuild the two conditional-write
    /// shapes this test needs so each mode gets its own value.
    fn clone_err(err: &object_store::Error) -> object_store::Error {
        match err {
            object_store::Error::AlreadyExists { path, .. } => object_store::Error::AlreadyExists {
                path: path.clone(),
                source: Box::new(TextError("409")),
            },
            object_store::Error::Precondition { path, .. } => object_store::Error::Precondition {
                path: path.clone(),
                source: Box::new(TextError("412")),
            },
            _ => unreachable!("clone_err only used for the two conditional-write shapes"),
        }
    }
}
