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
use std::sync::atomic::{AtomicU64, Ordering};

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

mod instance_role;
use instance_role::{DEFAULT_IMDS_ENDPOINT, InstanceRoleCredentialProvider};

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

/// How [`S3Store`] sources AWS credentials (ADR-0106).
///
/// `Static` (the default) is every deployment today: inline keys, an optional
/// `session_token`, or a rotating `credentials_file`. `InstanceRole` fetches
/// short-lived credentials from the EC2 instance metadata service (IMDSv2) and
/// forbids any inline credential field being set at the same time. Selecting
/// the source is explicit, never inferred from the absence of keys, per
/// `S3Config`'s "no credential-chain magic" contract. Future sources (EKS
/// IRSA, ECS task roles) fit as additional variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum S3AuthMode {
    /// Inline `access_key_id`/`secret_access_key`, optionally with
    /// `session_token` or `credentials_file`. Behaves exactly as before
    /// ADR-0106.
    #[default]
    Static,
    /// Short-lived credentials from the EC2 IMDSv2 endpoint. Requires every
    /// inline credential field to be absent.
    InstanceRole,
}

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
    /// Which credential source [`S3Store`] uses (ADR-0106). `Static` (the
    /// default) is every caller today and preserves byte-identical behavior.
    /// `InstanceRole` fetches from EC2 IMDSv2 and requires `access_key_id`,
    /// `secret_access_key`, `session_token`, and `credentials_file` all to be
    /// absent; [`S3Store::new`] rejects the mix with a typed [`StoreError`].
    pub auth: S3AuthMode,
    /// Base URL of the EC2 instance metadata service, used only when
    /// `auth` is [`S3AuthMode::InstanceRole`]. `None` uses the AWS link-local
    /// address (`http://169.254.169.254`); a value redirects IMDS to a mock in
    /// tests or an unusual deployment. Ignored under [`S3AuthMode::Static`].
    pub instance_metadata_endpoint: Option<String>,
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
    /// `Some` only under [`S3AuthMode::InstanceRole`]; kept for the same reason
    /// as `credential_provider`, so [`S3Store::credential_refresh_failures`]
    /// can read its counter (ADR-0106).
    instance_role_provider: Option<Arc<InstanceRoleCredentialProvider>>,
    /// Best-effort aborts in [`S3Store::put_via_multipart`] whose
    /// `AbortMultipartUpload` request returned an error, so cleanup was NOT
    /// CONFIRMED from here (#864). Not proof of orphaning: S3 can apply
    /// `complete` or `abort` and then fail the response, so this can count an
    /// upload that is already a visible object. Reconcile against S3's own list
    /// of open uploads. Read via [`S3Store::multipart_abort_failures`].
    multipart_abort_failures: AtomicU64,
    /// Multipart uploads opened by [`S3Store::put_via_multipart`] that ended
    /// without a successful abort for any reason: a failed abort (also counted
    /// in `multipart_abort_failures`) or a future dropped mid-upload before
    /// either completion or abort ran (deadline cancellation, task teardown).
    /// The two are distinguishable because only the first also moves
    /// `multipart_abort_failures`; the difference isolates the dropped case.
    /// Read via [`S3Store::multipart_uploads_unreaped`] (#864).
    multipart_uploads_unreaped: AtomicU64,
}

impl S3Store {
    pub fn new(config: S3Config) -> Result<Self, StoreError> {
        let (builder, credential_provider, instance_role_provider) = Self::builder(&config)?;
        let store = builder
            .build()
            .map_err(|e| StoreError::Permanent(format!("failed to build S3 client: {e}")))?;
        Ok(S3Store {
            store,
            page_size: LIST_PAGE_SIZE,
            credential_provider,
            instance_role_provider,
            multipart_abort_failures: AtomicU64::new(0),
            multipart_uploads_unreaped: AtomicU64::new(0),
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

    /// Count of [`S3AuthMode::InstanceRole`] request-path refreshes that failed
    /// while the cached credential was already expired, so the S3 request had
    /// to fail (ADR-0106). A transient failure that still served an unexpired
    /// last-good credential does not count. Always `0` under
    /// [`S3AuthMode::Static`]. Mirrors [`S3Store::credential_rotation_failures`]
    /// so #545 can wire both into ravel-server observability.
    pub fn credential_refresh_failures(&self) -> u64 {
        self.instance_role_provider
            .as_deref()
            .map(InstanceRoleCredentialProvider::refresh_failures)
            .unwrap_or(0)
    }

    /// Count of best-effort aborts in [`S3Store::put_via_multipart`] whose
    /// `AbortMultipartUpload` request returned an error (#864).
    ///
    /// This is a **cleanup-not-confirmed** count, not a count of orphans. S3 can
    /// apply the operation and then fail the response, and an abort issued after
    /// an ambiguous `complete()` can fail precisely because the upload already
    /// completed — a visible object with nothing to reap, counted here anyway.
    /// So a non-zero value means "reconcile against S3's list of open uploads",
    /// and only the uploads that really are incomplete are billed until the
    /// `AbortIncompleteMultipartUpload` lifecycle rule
    /// (docs/object-store-contract.md, "Required bucket configuration") reaps
    /// them. The best-effort abort is deliberate: this counts the unconfirmed
    /// outcome rather than blocking or retrying it.
    pub fn multipart_abort_failures(&self) -> u64 {
        self.multipart_abort_failures.load(Ordering::Relaxed)
    }

    /// Count of multipart uploads opened by [`S3Store::put_via_multipart`] that
    /// ended without a successful abort for any reason (#864): either the abort
    /// itself failed (which also increments
    /// [`S3Store::multipart_abort_failures`]) or the upload future was dropped
    /// mid-flight before completion or abort ran, so no abort was even attempted
    /// (a deadline cancellation or task teardown). Subtracting
    /// `multipart_abort_failures` isolates the dropped case, which is otherwise
    /// invisible: a completed upload and a cleanly aborted one never count here.
    /// A hard process crash (SIGKILL) drops no futures and so increments
    /// nothing; that case is inferable only from S3's own list of open uploads.
    pub fn multipart_uploads_unreaped(&self) -> u64 {
        self.multipart_uploads_unreaped.load(Ordering::Relaxed)
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
    /// Returns the [`FileCredentialProvider`] and
    /// [`InstanceRoleCredentialProvider`] alongside the builder (rather than
    /// only handing whichever is active to `with_credentials` as an opaque
    /// trait object) so [`S3Store::new`] can keep its own handle for
    /// observability. At most one is ever `Some`.
    ///
    /// Under [`S3AuthMode::InstanceRole`] the inline key setters are skipped
    /// entirely and the eager IMDS fetch runs here (so a misconfigured
    /// instance fails at construction); mixing that mode with any inline
    /// credential field is rejected with a typed [`StoreError`] before any
    /// network call.
    #[allow(clippy::type_complexity)]
    fn builder(
        config: &S3Config,
    ) -> Result<
        (
            AmazonS3Builder,
            Option<Arc<FileCredentialProvider>>,
            Option<Arc<InstanceRoleCredentialProvider>>,
        ),
        StoreError,
    > {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region);
        // The inline key setters exist only under Static: `InstanceRole` never
        // signs with a static key, and setting one would be exactly the mix the
        // check below forbids.
        if matches!(config.auth, S3AuthMode::Static) {
            builder = builder
                .with_access_key_id(&config.access_key_id)
                .with_secret_access_key(&config.secret_access_key);
        }
        builder = builder
            .with_allow_http(config.allow_http)
            .with_virtual_hosted_style_request(!config.force_path_style);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint.clone());
        }
        // Single-key SSE-KMS (ADR-0042 decision 1): the KMS call happens
        // inside S3 on every PUT, no crypto code here. Dual-layer DSSE is
        // reachable via `with_dsse_kms_encryption` on this same builder if a
        // future requirement needs it; single-layer is the sufficient default.
        // Applies to both auth modes: an instance role needs
        // kms:GenerateDataKey/kms:Decrypt on this key (docs/object-store-contract.md).
        if let Some(kms_key_id) = &config.kms_key_id {
            builder = builder.with_sse_kms_encryption(kms_key_id);
        }

        let mut credential_provider = None;
        let mut instance_role_provider = None;
        match config.auth {
            S3AuthMode::Static => {
                // Rotating file credentials win over inline ones (ADR-0072
                // decision 1, S3Config::credentials_file doc comment):
                // `with_credentials` overrides the
                // access_key_id/secret_access_key/token set above.
                // `FileCredentialProvider::load` does the fail-fast read+parse
                // this function's Result exists for.
                if let Some(credentials_file) = &config.credentials_file {
                    let provider =
                        Arc::new(FileCredentialProvider::load(credentials_file.clone())?);
                    builder =
                        builder.with_credentials(Arc::clone(&provider) as AwsCredentialProvider);
                    credential_provider = Some(provider);
                } else if let Some(session_token) = &config.session_token {
                    builder = builder.with_token(session_token.clone());
                }
            }
            S3AuthMode::InstanceRole => {
                // Mixing an instance role with any inline credential is a
                // configuration error, not a precedence question: refuse it
                // outright (ADR-0106), before the eager IMDS fetch.
                if !config.access_key_id.is_empty()
                    || !config.secret_access_key.is_empty()
                    || config.session_token.is_some()
                    || config.credentials_file.is_some()
                {
                    return Err(StoreError::Permanent(
                        "auth=InstanceRole must not be combined with access_key_id, \
                         secret_access_key, session_token, or credentials_file"
                            .to_string(),
                    ));
                }
                let endpoint = config
                    .instance_metadata_endpoint
                    .clone()
                    .unwrap_or_else(|| DEFAULT_IMDS_ENDPOINT.to_string());
                // Eager fetch: a server misconfigured for EC2 fails here rather
                // than on its first S3 request. `with_credentials` installs the
                // provider as the client's credential source.
                let provider = Arc::new(InstanceRoleCredentialProvider::load(endpoint)?);
                builder = builder.with_credentials(Arc::clone(&provider) as AwsCredentialProvider);
                instance_role_provider = Some(provider);
            }
        }
        Ok((builder, credential_provider, instance_role_provider))
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
    // Throttle takes precedence over the timeout heuristic. `object_store`'s
    // `RetryError` Display appends ", ..., retry_timeout: {d} " on every
    // exhausted-retry message (whenever retries != 0), and the literal
    // substring "retry_timeout" contains "timeout": checking the bare
    // "timeout" substring first would misclassify every exhausted-retry
    // throttle (429/503/SlowDown) as Timeout. Genuine timeouts are matched by
    // "timed out"/"deadline", which that wrapper text does not carry, so the
    // real timeout case is preserved while the throttle case wins.
    if lower.contains("429")
        || lower.contains("too many requests")
        || lower.contains("slow down")
        || lower.contains("slowdown")
        || lower.contains("throttl")
        || lower.contains("503")
        || lower.contains("service unavailable")
    {
        return StoreError::Throttled {
            retry_after_ms: 1000,
        };
    }
    if lower.contains("timed out") || lower.contains("timeout") || lower.contains("deadline") {
        return StoreError::Timeout;
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

/// Increments the "ended without a successful abort" counter (#864) when
/// dropped, unless disarmed first. Armed right after a multipart upload is
/// opened and disarmed only on a clean complete or a successful abort, so a
/// future dropped mid-upload (deadline cancellation, task teardown) still
/// records the orphaned upload; without it, only a failed abort call would be
/// visible.
struct UnreapedGuard<'a> {
    counter: &'a AtomicU64,
    armed: bool,
}

impl<'a> UnreapedGuard<'a> {
    fn armed(counter: &'a AtomicU64) -> Self {
        Self {
            counter,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for UnreapedGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl S3Store {
    /// Abort a failed multipart upload best effort, observing the outcome
    /// rather than acting on it: the abort is deliberately never retried and
    /// never blocking (docs/object-store-contract.md, "Visibility and abort").
    /// A successful abort disarms `unreaped` because the parts were released.
    ///
    /// An abort that returns `Err` is recorded as **cleanup not confirmed**, not
    /// as proof that parts are orphaned, and the distinction is deliberate: a
    /// response can fail after S3 has already applied the operation. In
    /// particular, after an ambiguous `complete()` error the subsequent abort can
    /// fail *because the upload already completed*, in which case there is a
    /// visible object and nothing to reap. Both counters therefore mean "the
    /// outcome was not confirmed from here", and an operator reconciles against
    /// remote state (or the lifecycle rule does) rather than trusting them as a
    /// count of billable orphans. Treating `Err` as confirmed orphaning would
    /// over-report by exactly the ambiguous-completion case (#864).
    async fn abort_best_effort(
        &self,
        key: &str,
        upload: &mut Box<dyn OsMultipartUpload>,
        unreaped: &mut UnreapedGuard<'_>,
    ) {
        match upload.abort().await {
            Ok(()) => unreaped.disarm(),
            Err(e) => {
                self.multipart_abort_failures
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    phase = "multipart_abort",
                    key = %key,
                    error = %map_error_common(e),
                    "multipart abort not confirmed; if the upload did not \
                     already complete, its parts stay billable until the \
                     AbortIncompleteMultipartUpload lifecycle rule reaps them"
                );
            }
        }
    }

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

        // The upload now exists on the server: every early return or dropped
        // future from here until a clean complete or a successful abort leaves
        // parts billed. The guard records that as an unreaped upload (#864);
        // it is disarmed only on a clean resolution.
        let mut unreaped = UnreapedGuard::armed(&self.multipart_uploads_unreaped);

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
            self.abort_best_effort(key, &mut upload, &mut unreaped)
                .await;
            return Err(e);
        }

        match upload.complete().await {
            Ok(result) => {
                unreaped.disarm();
                outcome_of(key, result)
            }
            Err(e) => {
                self.abort_best_effort(key, &mut upload, &mut unreaped)
                    .await;
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

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let prefix_path = prefix_of(prefix);
        // A page token resumes strictly after the previous page's last key;
        // on the first page `start_after` plays the same role. Both map to
        // `object_store`'s `list_with_offset`, whose offset is exclusive
        // (ListObjectsV2 `start-after`), so keys equal to the offset are
        // skipped server-side and never transferred. A present page token is
        // always past `start_after`, so it takes precedence.
        let offset = match (&page, start_after) {
            (Some(PageToken(after)), _) => Some(Path::from(after.as_str())),
            (None, Some(after)) => Some(Path::from(after)),
            (None, None) => None,
        };
        let mut stream = match &offset {
            Some(offset) => self.store.list_with_offset(prefix_path.as_ref(), offset),
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
        // on its own would invite a retry -- exactly the live-lock the
        // handle-poisoning rule prevents.
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

    /// The unreaped-upload guard (#864) counts on drop only while armed, so a
    /// multipart upload future dropped mid-flight (deadline cancellation, task
    /// teardown) records the orphaned upload, while a clean complete or a
    /// successful abort disarms it and records nothing. This is the mechanism
    /// that makes the "process died mid-upload" case visible rather than only
    /// the "abort itself failed" one.
    #[test]
    fn unreaped_guard_counts_on_drop_only_while_armed() {
        let counter = AtomicU64::new(0);
        // Armed and dropped without a clean resolution: the dropped-mid-upload
        // case. It counts.
        drop(UnreapedGuard::armed(&counter));
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "an armed guard must count the orphaned upload on drop"
        );
        // Disarmed before drop: the clean-resolution case. It does not count.
        let mut guard = UnreapedGuard::armed(&counter);
        guard.disarm();
        drop(guard);
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "a disarmed guard must not count on drop"
        );
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
            auth: S3AuthMode::Static,
            instance_metadata_endpoint: None,
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

    /// Stand up a minimal always-succeeding mock IMDS on an ephemeral loopback
    /// port and return its `http://addr` base. Shared by the InstanceRole
    /// builder tests, whose only need is a working eager fetch.
    async fn spawn_ok_imds() -> String {
        use axum::Router;
        use axum::http::StatusCode;
        use axum::routing::{get, put};

        let app = Router::new()
            .route("/latest/api/token", put(|| async { "mock-token" }))
            .route(
                "/latest/meta-data/iam/security-credentials/",
                get(|| async { "ravel-role" }),
            )
            .route(
                "/latest/meta-data/iam/security-credentials/{role}",
                get(|| async {
                    (
                        StatusCode::OK,
                        r#"{"Code":"Success","AccessKeyId":"AKIA_IMDS",
                            "SecretAccessKey":"imds-secret","Token":"imds-token",
                            "Expiration":"2033-11-14T22:13:20Z"}"#,
                    )
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        endpoint
    }

    /// `auth=InstanceRole` combined with any inline credential is a
    /// construction-time typed error (ADR-0106). Non-vacuous by pointing at a
    /// *working* mock IMDS: the all-absent config builds cleanly against it, so
    /// each mixed config's failure can only be the mix guard, not a fetch
    /// failure or a blanket InstanceRole refusal. Each inline field is
    /// exercised on its own so a future field dropped from the guard is caught.
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_rejects_mixed_instance_role_and_inline_credentials() {
        let endpoint = spawn_ok_imds().await;
        let base = move || {
            let mut config = test_config();
            config.auth = S3AuthMode::InstanceRole;
            config.instance_metadata_endpoint = Some(endpoint.clone());
            config.access_key_id = String::new();
            config.secret_access_key = String::new();
            config
        };

        let mut with_key = base();
        with_key.access_key_id = "AKIA_INLINE".to_string();

        let mut with_secret = base();
        with_secret.secret_access_key = "inline-secret".to_string();

        let mut with_token = base();
        with_token.session_token = Some("inline-token".to_string());

        let mut with_file = base();
        with_file.credentials_file = Some(PathBuf::from("/nonexistent/creds.json"));

        let clean = base();

        // spawn_blocking: builder() blocks on the eager fetch, which must be
        // able to reach the mock task on this same runtime.
        tokio::task::spawn_blocking(move || {
            for (label, config) in [
                ("access_key_id", with_key),
                ("secret_access_key", with_secret),
                ("session_token", with_token),
                ("credentials_file", with_file),
            ] {
                let err = S3Store::builder(&config)
                    .expect_err(&format!("InstanceRole + {label} must be rejected"));
                assert!(
                    matches!(err, StoreError::Permanent(_)),
                    "{label}: got {err:?}"
                );
            }

            // All-absent against the same working endpoint builds cleanly: the
            // rejections above are the mix guard, not a blanket refusal.
            S3Store::builder(&clean)
                .expect("all-absent InstanceRole must build against a working IMDS");
        })
        .await
        .expect("join");
    }

    /// The `InstanceRole` builder installs a credential provider that a Static
    /// builder over the same non-credential config does not. Mirrors
    /// [`none_kms_key_leaves_builder_unchanged`]'s manual-baseline shape so the
    /// comparison isolates exactly the provider: the baseline reproduces every
    /// non-credential setter and omits only `with_credentials`, so removing the
    /// provider install would make the two Debug outputs equal and fail this.
    #[tokio::test(flavor = "multi_thread")]
    async fn instance_role_builder_differs_from_static_baseline() {
        let endpoint = spawn_ok_imds().await;

        let mut instance_role = test_config();
        instance_role.auth = S3AuthMode::InstanceRole;
        instance_role.access_key_id = String::new();
        instance_role.secret_access_key = String::new();
        instance_role.instance_metadata_endpoint = Some(endpoint);

        let baseline_config = instance_role.clone();

        let (instance_debug, baseline_debug) = tokio::task::spawn_blocking(move || {
            let instance_debug = format!(
                "{:?}",
                S3Store::builder(&instance_role)
                    .expect("instance-role builder must construct against the mock")
                    .0
            );
            // Every non-credential setter the InstanceRole path applies, with
            // no key setters and no provider: the one difference must be the
            // installed credential provider.
            let mut baseline = AmazonS3Builder::new()
                .with_bucket_name(&baseline_config.bucket)
                .with_region(&baseline_config.region)
                .with_allow_http(baseline_config.allow_http)
                .with_virtual_hosted_style_request(!baseline_config.force_path_style);
            if let Some(endpoint) = &baseline_config.endpoint {
                baseline = baseline.with_endpoint(endpoint.clone());
            }
            (instance_debug, format!("{baseline:?}"))
        })
        .await
        .expect("join");

        assert_ne!(
            instance_debug, baseline_debug,
            "InstanceRole must install a credential provider the plain builder lacks"
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
        let (_, provider, _) = S3Store::builder(&config).expect("valid credentials file");
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

    /// Regression for #1105: `object_store`'s real `RetryError` `Display`
    /// appends `", after {n} retries, max_retries: {m}, retry_timeout: {d}ms "`
    /// on every exhausted-retry message, and that literal `retry_timeout`
    /// substring contains `timeout`. A message that also carries a genuine
    /// throttle token (`429`/`SlowDown`) must classify as `Throttled`, not
    /// `Timeout`: the throttle branch is checked before the bare `timeout`
    /// heuristic. Uses the exact wrapper format object_store emits so this
    /// pins the real interaction, not a hand-written approximation.
    #[test]
    fn retry_timeout_wrapper_does_not_shadow_throttle() {
        for text in [
            "Server returned non-2xx status code: 429 Too Many Requests, \
             after 10 retries, max_retries: 10, retry_timeout: 180000ms ",
            "response error \"SlowDown\", after 10 retries, max_retries: 10, \
             retry_timeout: 180000ms ",
        ] {
            let mapped = map_error_common(generic(TextError(text)));
            assert!(
                matches!(mapped, StoreError::Throttled { .. }),
                "exhausted-retry throttle carrying `retry_timeout` must be \
                 Throttled, not Timeout, got {mapped:?} for {text:?}"
            );
            assert!(mapped.is_retryable(), "{text:?} must be retryable");
        }
    }

    /// A genuine timeout message that carries no throttle token still
    /// classifies as `Timeout`, even wrapped in the same exhausted-retry
    /// suffix: the throttle branch does not fire, so the timeout heuristic
    /// (`timed out`/`deadline`/bare `timeout`) still wins.
    #[test]
    fn genuine_timeout_still_classifies_as_timeout() {
        let text = "request timed out, after 10 retries, max_retries: 10, \
                    retry_timeout: 180000ms ";
        let mapped = map_error_common(generic(TextError(text)));
        assert!(
            matches!(mapped, StoreError::Timeout),
            "a genuine timeout without throttle language must stay Timeout, \
             got {mapped:?}"
        );
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
