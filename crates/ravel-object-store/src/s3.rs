//! S3 adapter over the `object_store` crate's `AmazonS3` client
//! (ADR-0008). This module never leaks `object_store` types across the
//! [`ObjectStoreBackend`] boundary; every conversion happens here.
//!
//! Listing does not go through `object_store`: `list`, `list_after` and
//! `list_delimited` send their own signed ListObjectsV2 requests with a raw
//! prefix and a raw `start-after` and return raw keys (`s3/list.rs`,
//! ADR-2637 decision 1), so a listing matches
//! [`crate::memory::MemoryStore`]'s raw `str::starts_with` prefix and resumes
//! on the exact key a page ended on.
//!
//! ## Known divergences from the contract, forced by `object_store`
//!
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
//!   floor at this layer and stays a `Display`-text heuristic (the reason
//!   phrases `"too many requests"` and `"service unavailable"` that always
//!   follow a 429 or 503 status, `"throttl"`, ...), read from the inner
//!   error text with the request URI left out. Timeouts and
//!   connection failures are different: the `RetryError`'s own `source()`
//!   chain contains a publicly nameable [`object_store::client::HttpError`]
//!   whose [`object_store::client::HttpErrorKind`] is a typed
//!   transport-failure signal (`Timeout`, `Connect`, `Interrupted`, ...). So
//!   [`classify_generic`] recovers those by downcast first and only falls
//!   back to the `Display` heuristic when no `HttpError` is in the chain.
//!   See [`classify_generic`] for the single, well-documented classification
//!   path this crate uses for every `Error::Generic`.
//! - **`upload_checksum` is configurable, off by default (#863).** The exact
//!   thing the contract's [`UploadChecksum`] names — the caller's own
//!   precomputed CRC32C attached per request as `x-amz-checksum-crc32c` — has
//!   nowhere to go: `object_store` 0.14's `AmazonS3` client has no per-request
//!   checksum hook and no way to attach a caller-supplied value
//!   (`PutRequest::with_payload` computes the digest itself). Its only
//!   upload-integrity knob is the whole-client
//!   [`AmazonS3Builder::with_checksum_algorithm`], SHA-256 or CRC64-NVME only,
//!   which `object_store` computes over the payload and sends as
//!   `x-amz-checksum-{sha256,crc64nvme}` for S3 to verify-or-reject. [`S3HttpConfig::upload_integrity`]
//!   selects it: `Off` (default) attaches nothing and reports
//!   `upload_checksum: false`; `Crc64Nvme`/`Sha256` attach the header and report
//!   `true`. When on, `put()`'s CRC32C pre-flight still runs over the same buffer
//!   `object_store` then digests, so the caller's bytes are covered caller ->
//!   buffer -> server; the wire algorithm is just not the caller's CRC32C value.
//!   A backend that rejects the header fails the PUT loudly; one that silently
//!   ignores it cannot be detected here (`PutResult` carries no response headers),
//!   so a non-`Off` mode is a deployment assertion that the endpoint honors it.
//!   `upload_checksum` is not in [`Capabilities::mandatory`] and gates no mode,
//!   so either setting starts. See [`UploadIntegrity`] and `capabilities()`
//!   below. When on, `put()` keeps every size on the single-PUT path: one
//!   billed request where multipart costs parts + 2, and one checksum over
//!   the whole object's bytes as sent. The algorithm is set for the whole
//!   client, so the explicit `put_multipart` path already sends checksums
//!   under integrity: `object_store`'s `create_multipart` sends
//!   `x-amz-checksum-algorithm` and each `put_part` goes through
//!   `PutRequest::with_payload`, which attaches the part's digest --- proven
//!   on the wire, part by part (not only the first), by
//!   `s3::tests::multipart_parts_carry_checksums_under_integrity` against a
//!   fake endpoint. Whether `CompleteMultipartUpload`'s body then carries a
//!   per-part checksum is not this client's choice to make: `object_store`
//!   reads each part's checksum off `UploadPart`'s *response* headers, not
//!   off what it sent, to build that part's serialized `PartId`, so the
//!   Complete body only carries one when the endpoint's `UploadPart`
//!   response echoes the same checksum header back. The same test records
//!   this against a fake endpoint that does echo it; an endpoint that does
//!   not would silently fall back to a bare e_tag there instead. It sends no
//!   `x-amz-checksum-type`, so what the endpoint records for the completed
//!   object is its default type for the algorithm (on AWS, a full-object
//!   checksum for CRC64-NVME and a composite one for SHA-256). Routing large
//!   overwrites through this path under integrity still waits on a
//!   real-endpoint check that it verifies-or-rejects those part checksums:
//!   sending them is now proven, server-side verification is not.
//! - **Read-side checksum verification is header-driven, and a whole-object
//!   read is only verifiable when one response carried the whole object**
//!   (ADR-1696 decisions 2 to 4). `object_store` 0.14's `GetResult` exposes no
//!   response headers, so the stored `x-amz-checksum-*` is read in the HTTP
//!   connector below the retry loop ([`connector`]) and handed back to
//!   [`S3Store::get_one`] through a per-request slot. Two consequences a reader
//!   should not have to rediscover. The request header that asks for the stored
//!   checksum (`x-amz-checksum-mode: ENABLED`) must be signed, and the
//!   connector runs after signing, so it rides on `ClientOptions`' default
//!   headers instead --- which means it is attached to every request, not only
//!   to GETs (see [`client_options`]), unless
//!   [`S3HttpConfig::request_stored_checksum`] turns it off. And an endpoint
//!   returns the stored checksum only on an *unranged* GET (MinIO and RustFS
//!   drop it whenever a `Range` header is present), so [`GetRange::Full`]
//!   starts with one unranged request whose body is read up to
//!   [`S3HttpConfig::max_request_body_bytes`] ([`S3Store::get_whole_object`]).
//!   An object that fits arrives in that one response with its checksum and
//!   is verified; a larger one is cut there, finished with ranged requests,
//!   and counted on [`S3Store::get_unverified`] rather than verified. Every
//!   commit-family record is orders of magnitude below that bound, so it is
//!   always one request and verifiable wherever the endpoint stored a
//!   checksum this adapter can recompute.
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
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder, AwsCredentialProvider, Checksum};
use object_store::client::HttpConnector;
use object_store::path::Path;
use object_store::{
    ClientOptions, GetOptions as OsGetOptions, GetRange as OsGetRange,
    MultipartUpload as OsMultipartUpload, ObjectStore, ObjectStoreExt, PutMode as OsPutMode,
    PutOptions as OsPutOptions, PutPayload, UpdateVersion,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PartSequence, Pin, PinnedRead, PutMode, PutOptions, PutOutcome,
    StoreError, UploadChecksum, Version, multipart_finished, multipart_poisoned,
};

mod bucket_config;
use bucket_config::{BucketControlPlaneClient, ReportNotes};

mod credentials;
use credentials::FileCredentialProvider;

mod instance_role;
use instance_role::{DEFAULT_IMDS_ENDPOINT, InstanceRoleCredentialProvider};

mod checksum;
use checksum::{CHECKSUM_MODE_ENABLED, CHECKSUM_MODE_HEADER, ObservedChecksum};

mod http_date;

mod list;
use list::ListClient;

mod connector;
use connector::{GetObservation, ObservedStoreTime, S3HttpConnector};

use crate::instrument::{StoreMetrics, StoreOp};

/// Default entries per `ListPage`, chosen to line up with S3's own
/// `ListObjectsV2` page size. Overridable per instance via
/// [`S3Store::with_page_size`]. Public so callers that must declare this
/// store's real page size elsewhere (`ravel-cli store qualify`'s
/// `--list-page-size` default, matching what `build_store` constructs) have
/// one definition to reference instead of a duplicated literal.
pub const LIST_PAGE_SIZE: usize = 1000;

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
/// S3's single-request PUT ceiling. With upload integrity enabled, `put`
/// stays on the single-PUT path (one billed request, one server-verified
/// checksum over the whole object) up to this size and refuses above it
/// rather than switching to multipart, whose per-part checksums have not
/// been checked against a real endpoint.
pub const SINGLE_PUT_MAX_BYTES: u64 = 5 * 1024 * 1024 * 1024;

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

/// The most requests [`S3Store::put`] can have in flight at once for a
/// `len`-byte payload under `mode`: up to [`MULTIPART_UPLOAD_CONCURRENCY`] part
/// uploads for an `Overwrite` above [`MULTIPART_THRESHOLD`], otherwise one.
/// This is the fan-out with upload integrity off; with it on every put is a
/// single PUT. A scheduled handle sizes its permits from this only for a store
/// that does not declare `upload_checksum` ([`crate::scheduling`], "Ops that
/// fan out").
pub(crate) fn put_fan_out(len: usize, mode: &PutMode) -> usize {
    if matches!(mode, PutMode::Overwrite) && len > MULTIPART_THRESHOLD {
        len.div_ceil(MULTIPART_PART_SIZE)
            .min(MULTIPART_UPLOAD_CONCURRENCY)
    } else {
        1
    }
}

/// How many of a bounded whole-object read's ranged GETs
/// ([`S3Store::get_whole_object`]) are in flight at once. Same reasoning and
/// same value as [`MULTIPART_UPLOAD_CONCURRENCY`] on the write side: enough
/// parallel connections that splitting a large read does not serialise its
/// round trips, few enough that a query fetching many objects at once does not
/// multiply its connection count by the chunk count of each. Under a scheduled
/// handle the read keeps no more in flight than the permits it holds.
const WHOLE_OBJECT_GET_CONCURRENCY: usize = 4;

/// Transfer rate the per-request body bound is sized against: 5 Mbps
/// (0.625 MB/s), roughly 2000x below this deployment's line rate (same-region
/// S3 from an r6a.4xlarge, up to 10 Gbps sustained).
///
/// This is a *floor*, not an estimate: it is the rate below which a request is
/// treated as failed rather than slow. Sizing the bound against it is what lets
/// [`S3HttpConfig::request_timeout`] stay a fixed number while the objects
/// Ravel reads do not — see that field for the arithmetic.
pub const FLOOR_TRANSFER_BYTES_PER_SEC: u64 = 625_000;

/// The share of [`S3HttpConfig::request_timeout`] reserved for everything that
/// is not body transfer: TCP connect, the TLS handshake, and S3's
/// time-to-first-byte on a degraded path. Deducted before
/// [`S3HttpConfig::max_request_body_bytes`] converts what is left into bytes at
/// [`FLOOR_TRANSFER_BYTES_PER_SEC`].
///
/// Twice the 3 s [`S3HttpConfig::connect_timeout`], so a connect that consumes
/// its whole budget still leaves room for a slow first byte.
const REQUEST_OVERHEAD_ALLOWANCE: Duration = Duration::from_secs(6);

/// Floor on [`S3HttpConfig::max_request_body_bytes`]. Below roughly this size
/// the per-request round trip dominates the transfer, so a whole-object read
/// split this finely costs more in requests than the bound buys back. A
/// `request_timeout` configured so tight that the floor binds (under
/// `REQUEST_OVERHEAD_ALLOWANCE + 1 MiB / FLOOR_TRANSFER_BYTES_PER_SEC`, about
/// 7.7 s) cannot satisfy the floor-bandwidth criterion at any non-degenerate
/// chunk size; the default satisfies it with margin, and the compile-time
/// assertion below pins that.
const MIN_REQUEST_BODY_BYTES: usize = 1024 * 1024;

/// The [`S3HttpConfig::request_timeout`] default, named so the compile-time
/// check below can state the criterion against it.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

// The criterion `request_timeout`'s doc comment states, checked at compile time
// rather than left to a reader re-deriving it: the largest body any single
// request carries (`MULTIPART_PART_SIZE`, the cap on both the multipart part
// size and the whole-object read chunk) transfers inside the default timeout at
// FLOOR_TRANSFER_BYTES_PER_SEC, with REQUEST_OVERHEAD_ALLOWANCE left over for
// connect, TLS, and time-to-first-byte. Lowering the default timeout or raising
// the part size without re-deriving the pair fails the build here.
const _: () = assert!(
    (MULTIPART_PART_SIZE as u64).div_ceil(FLOOR_TRANSFER_BYTES_PER_SEC)
        + REQUEST_OVERHEAD_ALLOWANCE.as_secs()
        <= DEFAULT_REQUEST_TIMEOUT.as_secs()
);

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

/// Write-time upload-integrity mode for the S3 adapter (#863).
///
/// This selects whether `put()` attaches a server-verified checksum to the
/// outgoing request so S3 verifies-or-rejects the bytes it received, rather
/// than corruption in transit being caught only at read time by the segment
/// crc32c hierarchy.
///
/// ## What `object_store` 0.14 lets us attach, and what it does not
///
/// The contract's [`UploadChecksum`] is a caller-supplied CRC32C, and issue
/// #863 asked for it on the wire as `x-amz-checksum-crc32c`. That exact
/// mechanism does not exist in `object_store` 0.14's `AmazonS3` client: there
/// is no per-request checksum hook and no way to hand it a precomputed digest
/// (`PutRequest::with_payload` in `object_store`'s `aws/client.rs` computes the
/// digest itself from the payload it is about to send). Its only upload-
/// integrity knob is the whole-client [`AmazonS3Builder::with_checksum_algorithm`],
/// which offers SHA-256 or CRC64-NVME only. Both are computed by `object_store`
/// over the exact `PutPayload` bytes it puts on the wire, sent as
/// `x-amz-checksum-{sha256,crc64nvme}`, and verified by S3 on receipt.
///
/// That still closes the gap this issue is about. `put()` runs
/// [`preflight_checksum`] over the same buffer first (caller/payload mismatch,
/// [`StoreError::Corrupted`] before any network call), and the immutable
/// [`Bytes`] handed to `object_store` is the very buffer the pre-flight
/// checked, so the caller's bytes are integrity-covered end to end: caller ->
/// our buffer (pre-flight) and our buffer -> S3 (the attached checksum). It is
/// a stronger transport check than CRC32C (64-bit vs 32-bit), just not the
/// caller's own digest value.
///
/// ## Compatibility, and why the default is [`UploadIntegrity::Off`]
///
/// Not every S3-compatible endpoint honors these headers. A backend that
/// *rejects* an unknown/unsupported checksum header fails the PUT loudly, which
/// surfaces as a [`StoreError`] the caller sees — no silent data loss. A backend
/// that *silently ignores* the header cannot be detected through this client:
/// `object_store`'s `PutResult` exposes only `e_tag`/`version`, never the
/// response headers S3 echoes a honored checksum in, so there is no in-adapter
/// way to observe "the server dropped it". The visible, configurable signal is
/// this switch plus [`Capabilities::upload_checksum`]: `Off` (the default)
/// keeps the historical behavior and reports `upload_checksum: false`; a
/// non-`Off` mode reports `true` and is a deployment-level assertion that the
/// configured endpoint honors the chosen algorithm. `upload_checksum` is not in
/// [`Capabilities::mandatory`] and gates no mode, so both settings start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UploadIntegrity {
    /// Attach no checksum. `capabilities().upload_checksum == false`; behavior
    /// is byte-for-byte the historical single-PUT/multipart path. The default,
    /// because a checksum header an endpoint does not support turns every write
    /// into an error.
    #[default]
    Off,
    /// Attach `x-amz-checksum-crc64nvme` (CRC64-NVME, computed by
    /// `object_store`). The cheaper of the two algorithms; supported by AWS S3,
    /// but not by every S3-compatible endpoint.
    Crc64Nvme,
    /// Attach `x-amz-checksum-sha256` (SHA-256, computed by `object_store`).
    /// The most broadly supported server-verified checksum, at the cost of a
    /// cryptographic hash over every payload.
    Sha256,
}

impl UploadIntegrity {
    /// The `object_store` [`Checksum`] algorithm to configure on the client, or
    /// `None` for [`UploadIntegrity::Off`].
    fn checksum_algorithm(self) -> Option<Checksum> {
        match self {
            UploadIntegrity::Off => None,
            UploadIntegrity::Crc64Nvme => Some(Checksum::CRC64NVME),
            UploadIntegrity::Sha256 => Some(Checksum::SHA256),
        }
    }

    /// Whether a write-time checksum is attached (i.e. not [`UploadIntegrity::Off`]).
    fn is_enabled(self) -> bool {
        self.checksum_algorithm().is_some()
    }
}

/// Explicit configuration for the S3 adapter. No environment or
/// credential-chain magic: every value that changes behavior is a field
/// here so tests and production wiring are equally explicit.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Set for RustFS (or any other S3-compatible endpoint); left `None` to
    /// use AWS's regional endpoint.
    pub endpoint: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Allow plain HTTP; needed for a local RustFS without TLS.
    pub allow_http: bool,
    /// Path-style requests (`https://host/bucket/key`) instead of
    /// virtual-hosted style (`https://bucket.host/key`); local S3-compatible
    /// deployments typically require this.
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

/// A plaintext `--s3-endpoint` pointing at a host the process cannot reach
/// over the loopback interface, refused by [`resolve_s3_allow_http`].
///
/// Typed rather than an `anyhow::Error` so every caller that must agree on
/// this rule renders one message from one decision instead of hand-written
/// strings that can drift. It lives beside [`S3Config::allow_http`], the field
/// the rule decides, because more than one shipping binary builds an
/// `S3Config`: `ravel-server` (`Cli::validate` and `build_store`) and
/// `ravel-cli`, which runs against the same bucket with the same credentials
/// and is invoked by the operator's store-qualification Job before any server
/// pod exists (issue #1707). The message names flags rather than field names
/// because both binaries spell them identically (`--s3-endpoint`,
/// `--s3-allow-http`, `RAVEL_S3_ALLOW_HTTP`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaintextS3Endpoint {
    /// The refused endpoint, verbatim as the operator wrote it.
    pub endpoint: String,
}

impl std::fmt::Display for PlaintextS3Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "--s3-endpoint '{}' uses plaintext http:// to a non-loopback host: every object \
             this process writes and reads, and the credentials signing those requests, cross \
             the network in the clear, so an on-path attacker can read telemetry and forge \
             writes for any tenant. Use https://, point at a loopback address for local \
             development, or pass --s3-allow-http (RAVEL_S3_ALLOW_HTTP) to accept plaintext \
             deliberately.",
            self.endpoint
        )
    }
}

impl std::error::Error for PlaintextS3Endpoint {}

/// An `--s3-endpoint` that begins with neither `https://` nor `http://`,
/// refused by [`resolve_s3_allow_http`] (issue #1911).
///
/// Such a value is not a usable URL. It was accepted at startup until this
/// refusal landed, and the process died later, inside `object_store`'s request
/// signing, on a message naming neither the endpoint nor the flag it came from
/// --- for a server pod, a crashloop whose cause is only in the pod log. Typed
/// and carried beside [`PlaintextS3Endpoint`] for the same reason that one is:
/// every binary that builds an [`S3Config`] renders one message from one
/// decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemelessS3Endpoint {
    /// The refused endpoint, verbatim as the operator wrote it.
    pub endpoint: String,
}

impl std::fmt::Display for SchemelessS3Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "--s3-endpoint '{}' is missing its URL scheme: an endpoint must begin with https:// \
             or http://. Write the scheme in: https:// for a TLS endpoint, or http:// for a \
             plaintext one, which is accepted unflagged only for a loopback host and otherwise \
             needs --s3-allow-http (RAVEL_S3_ALLOW_HTTP).",
            self.endpoint
        )
    }
}

impl std::error::Error for SchemelessS3Endpoint {}

/// Every way [`resolve_s3_allow_http`] refuses an endpoint.
///
/// One error type rather than two call sites' worth of `anyhow`, because each
/// case needs a *different* remedy in the message and the operator maps each to
/// its own `Degraded` reason: a schemeless endpoint is not a plaintext-exposure
/// problem at all, and [`PlaintextS3Endpoint`]'s wording would misdescribe it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3EndpointRefusal {
    /// Plaintext `http://` to a host that is not loopback, without the flag.
    Plaintext(PlaintextS3Endpoint),
    /// Neither `https://` nor `http://` at the front of the endpoint.
    Schemeless(SchemelessS3Endpoint),
}

impl S3EndpointRefusal {
    /// The refused endpoint, verbatim as the operator wrote it, whichever rule
    /// refused it.
    pub fn endpoint(&self) -> &str {
        match self {
            S3EndpointRefusal::Plaintext(refused) => &refused.endpoint,
            S3EndpointRefusal::Schemeless(refused) => &refused.endpoint,
        }
    }
}

impl std::fmt::Display for S3EndpointRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3EndpointRefusal::Plaintext(refused) => refused.fmt(f),
            S3EndpointRefusal::Schemeless(refused) => refused.fmt(f),
        }
    }
}

impl std::error::Error for S3EndpointRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            S3EndpointRefusal::Plaintext(refused) => Some(refused),
            S3EndpointRefusal::Schemeless(refused) => Some(refused),
        }
    }
}

/// Whether the S3 client may speak plaintext HTTP, decided by the endpoint's
/// own URL scheme rather than by whether an endpoint was set at all.
///
/// `https://` (and a real-AWS build, which sets no endpoint) is `false`: an
/// `allow_http` of `true` there would let a redirect or a misconfigured proxy
/// downgrade the connection silently. `http://` is `true`, because that is
/// what the operator asked for, but only after this refuses a non-loopback
/// host unless `allow_http_flag` is set. Loopback stays allowed unflagged:
/// a plaintext connection that never leaves the host has no on-path attacker,
/// and every local-development launcher in this repo depends on it.
///
/// An endpoint carrying neither scheme is refused outright (issue #1911), with
/// [`S3EndpointRefusal::Schemeless`] naming it and asking for a scheme. It is
/// not a usable URL: it used to be accepted here and kill the process later,
/// inside `object_store`'s request signing, on a message naming neither the
/// endpoint nor the flag.
///
/// The scheme is matched without regard to case (RFC 3986 section 3.1) and so
/// is the `localhost` host name, while either refusal still quotes the endpoint
/// exactly as it was written. Matching is anchored at the front rather than
/// anywhere in the string: a host named `my-http-proxy` carries no scheme, and
/// `HTTPS://` carries a perfectly good one.
pub fn resolve_s3_allow_http(
    endpoint: Option<&str>,
    allow_http_flag: bool,
) -> Result<bool, S3EndpointRefusal> {
    let Some(endpoint) = endpoint else {
        return Ok(false);
    };
    let lowercased = endpoint.to_ascii_lowercase();
    if lowercased.starts_with("https://") {
        return Ok(false);
    }
    if !lowercased.starts_with("http://") {
        return Err(S3EndpointRefusal::Schemeless(SchemelessS3Endpoint {
            endpoint: endpoint.to_string(),
        }));
    }
    let rest = &endpoint["http://".len()..];
    if allow_http_flag || is_loopback_authority(rest) {
        return Ok(true);
    }
    Err(S3EndpointRefusal::Plaintext(PlaintextS3Endpoint {
        endpoint: endpoint.to_string(),
    }))
}

/// Whether `endpoint` names this host: an `http://` or `https://` URL (the
/// scheme match is case-insensitive, same as [`resolve_s3_allow_http`]) whose
/// authority is `localhost`, a loopback IPv4 literal, or the loopback IPv6
/// literal (ADR-2014). A schemeless endpoint is `false`, not an error: unlike
/// [`resolve_s3_allow_http`] this has no flag to refuse toward, and a caller
/// deriving a default from locality treats "not a recognizable loopback URL"
/// the same as "not loopback".
///
/// Calls the same [`is_loopback_authority`] predicate `resolve_s3_allow_http`
/// uses, rather than a second copy of the host rule: a name that merely
/// resolves to loopback is not loopback here either, and the authority is cut
/// at the same `/`, `?`, `#` boundaries so `http://s3.example.com?x=@localhost`
/// is not loopback here either.
pub fn is_loopback_endpoint(endpoint: &str) -> bool {
    let lowercased = endpoint.to_ascii_lowercase();
    let rest = if lowercased.starts_with("https://") {
        &endpoint["https://".len()..]
    } else if lowercased.starts_with("http://") {
        &endpoint["http://".len()..]
    } else {
        return false;
    };
    is_loopback_authority(rest)
}

/// Whether the authority beginning `rest` (everything after `http://`) names
/// this host. Accepts `localhost`, a loopback IPv4 or IPv6 literal, and the
/// bracketed IPv6 form a URL authority requires; anything else, including a
/// name that merely resolves to loopback today, is treated as remote.
///
/// The authority ends at the first `/`, `?` or `#`. Ending it at `/` alone let
/// `http://s3.example.com?x=@localhost` read as loopback, which accepted
/// plaintext to a host on the network without the flag.
fn is_loopback_authority(rest: &str) -> bool {
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let host = match authority.strip_prefix('[') {
        // `[::1]:9000`: the bracketed literal is the host, and the port (if
        // any) follows the closing bracket.
        Some(bracketed) => bracketed.split(']').next().unwrap_or(""),
        None => authority.split(':').next().unwrap_or(""),
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Deliberate HTTP-client tuning for the S3 backend (#851).
///
/// `object_store` builds its `AmazonS3` client on inherited `ClientOptions`
/// defaults unless a `ClientOptions` is installed: a 30 s request timeout, a
/// 5 s connect timeout, and no pool-idle cap (reqwest's ~90 s). None of those
/// numbers were chosen by this repo, and the request timeout in particular is
/// a hole in the deadline model: on the read path Ravel runs (same-region S3,
/// r6a.4xlarge, up to 10 Gbps sustained, hundreds of concurrent fetches per
/// query) a single hung connection holds a concurrency slot for 30 s while the
/// query's own deadline is usually shorter, so the timeout never fires as a
/// safety net. Every value below is set on purpose, with the reasoning that
/// set it, so a future reader can retune from evidence rather than guess.
///
/// This is a separate struct rather than fields on [`S3Config`] because
/// `S3Config` is built by struct literal (no `..default`) in several crates
/// outside this one's edit scope; adding fields there would not compile.
/// [`S3Store::new`] applies [`S3HttpConfig::default`]; [`S3Store::with_http_config`]
/// overrides it. The default values are the deliberate choices; the fields
/// exist so tests and unusual deployments can set a non-default value that
/// reaches the client.
///
/// **Retry interaction / worst case.** `object_store` runs its own internal
/// retry loop per logical operation (`RetryConfig`, unchanged here: default
/// `max_retries = 10`, `retry_timeout = 180 s`, jittered exponential backoff),
/// and a request timeout is a *retryable* error. The loop checks its budget
/// *before* each retry, so no new attempt starts once 180 s have elapsed since
/// the first, but the final in-flight attempt still runs its full
/// `request_timeout`. The worst-case wall time for one logical operation is
/// therefore about `retry_timeout + request_timeout` = 180 s + 20 s ≈ 200 s of
/// internal retrying. In practice every caller passes a deadline and the trait
/// honors cancellation by drop (docs/object-store-contract.md, "Rules for
/// callers"), so the query deadline — typically well under 180 s — bounds one
/// operation first. Lowering `request_timeout` shortens each attempt but does
/// not change this 180 s ceiling; only `RetryConfig` would, and tuning it is
/// out of this change's scope.
#[derive(Debug, Clone)]
pub struct S3HttpConfig {
    /// Overall per-request timeout: connect through response-body-complete.
    ///
    /// Inherited default is 30 s. It must stay above the tail of the single
    /// largest request on the wire — an 8 MiB multipart part
    /// ([`MULTIPART_PART_SIZE`]) or a whole-object GET — even on a badly
    /// degraded connection.
    ///
    /// **The largest case is the read side, and it is bounded rather than
    /// assumed.** A whole-object read is the only request whose size is set by
    /// the data instead of by this crate: `max_l1_part_bytes` defaults to
    /// 256 MiB, 32x an 8 MiB part, so a fixed timeout sized for a part cannot
    /// also cover an unranged GET read to its end. [`S3Store::get`] therefore
    /// never reads one to its end: [`GetRange::Full`] reads at most
    /// [`S3HttpConfig::max_request_body_bytes`] from its one unranged request,
    /// dropping the rest, and serves any remainder as ranged requests of at
    /// most that size, which is derived from *this field* — so the criterion
    /// holds for whatever value is configured here, not only for the default.
    ///
    /// The arithmetic, for the largest request rather than the smallest: at the
    /// default 20 s, [`REQUEST_OVERHEAD_ALLOWANCE`] takes 6 s for connect, TLS,
    /// and time-to-first-byte, leaving 14 s of transfer. At a pathological
    /// ~5 Mbps ([`FLOOR_TRANSFER_BYTES_PER_SEC`], 0.625 MB/s, ~2000x below this
    /// box's line rate) that carries 8.75 MB, and the bound is capped at
    /// [`MULTIPART_PART_SIZE`] (8 MiB = 8.39 MB, ~13.4 s at the floor) so read
    /// and write share one largest-request-on-the-wire. A 256 MiB L1 compaction
    /// part is then 32 requests that each fit the timeout, not one ~410 s
    /// request against a 20 s ceiling that can only time out and burn the retry
    /// budget re-attempting a request that never fits. A compile-time assertion
    /// next to those constants pins the inequality.
    ///
    /// 20 s also cuts a hung connection's slot occupancy by a third versus the
    /// inherited 30 s. It is deliberately still conservative: there is no
    /// tail-latency measurement in this repo to justify going lower (toward
    /// ~10 s), and a timeout below the real tail turns a slow-but-succeeding
    /// request into a retry storm. Tighten only with a measurement — and note
    /// that tightening it also shrinks `max_request_body_bytes`, so a
    /// whole-object read pays it back in extra requests.
    pub request_timeout: Duration,
    /// TCP + TLS connect-phase timeout.
    ///
    /// Inherited default is 5 s. An in-region connect completes in single-digit
    /// to low-tens of milliseconds; the Linux initial SYN retransmit is ~1 s, so
    /// 3 s tolerates one lost SYN with margin while failing a black-holed path
    /// ~40% faster than the 5 s default, letting a retry re-dial a fresh 5-tuple
    /// within a typical query deadline. AWS's own latency-sensitive guidance
    /// (cited by `object_store`'s `ClientOptions::default`) recommends ~3.1 s.
    pub connect_timeout: Duration,
    /// How long an idle pooled connection is kept before it is recycled.
    ///
    /// Inherited default is unset (reqwest keeps idle connections ~90 s).
    /// Hundreds of concurrent fetches per query reuse warm TLS across back-to-
    /// back query phases (sub-second to few-second gaps), so we keep a generous
    /// window rather than tearing connections down after each wave — but we cap
    /// it below reqwest's ~90 s so a connection an intermediary or S3 silently
    /// half-closed during a longer idle gap is recycled locally before it is
    /// handed to a new request (reusing a dead socket costs a broken-pipe plus a
    /// retry). 30 s balances warm reuse against stale-socket risk; the exact
    /// value wants a keep-alive/idle-close measurement against the real endpoint.
    pub pool_idle_timeout: Duration,
    /// HTTP/2 keep-alive ping interval. See the type note on why this is inert
    /// under the HTTP/1.1 default we keep.
    pub http2_keep_alive_interval: Duration,
    /// HTTP/2 keep-alive ping acknowledgement timeout. See the type note.
    pub http2_keep_alive_timeout: Duration,
    /// Write-time upload-integrity mode (#863): whether `put()` attaches a
    /// server-verified checksum (`x-amz-checksum-*`) so S3 verifies-or-rejects
    /// the bytes it received. Default [`UploadIntegrity::Off`] (no checksum,
    /// historical behavior). See [`UploadIntegrity`] for what `object_store`
    /// 0.14 can and cannot attach and why this rides on the client-build config
    /// rather than [`S3Config`]. It is not, strictly, HTTP *tuning*, but this is
    /// the crate's overridable client-build config that already reaches
    /// [`S3Store::builder`], and [`S3Config`] cannot grow fields (struct-literal
    /// built out of this crate's edit scope).
    pub upload_integrity: UploadIntegrity,
    /// Whether every request carries `x-amz-checksum-mode: ENABLED`, which asks
    /// the endpoint to return the checksum it stored at upload so a full-object
    /// read can be verified against it (ADR-1696 decision 2 and its 2026-09-27
    /// amendment). Default `true`. `false` sends no such header: an endpoint
    /// then returns no stored checksum, and every full-object read is served
    /// and counted on [`S3Store::get_unverified`] instead of verified. It is
    /// the switch for an endpoint that rejects the header outright.
    pub request_stored_checksum: bool,
}

impl S3HttpConfig {
    /// The largest body a single request may carry and still finish inside
    /// [`S3HttpConfig::request_timeout`] at [`FLOOR_TRANSFER_BYTES_PER_SEC`],
    /// after [`REQUEST_OVERHEAD_ALLOWANCE`] is set aside for connect, TLS, and
    /// time-to-first-byte.
    ///
    /// This is what makes the timeout's criterion something the code satisfies
    /// rather than something the doc asserts: [`S3Store::get`] reads at most
    /// this many bytes from the unranged first request of a [`GetRange::Full`]
    /// read and splits the rest into ranged requests of at most this many
    /// bytes, so no request reads more than the timeout can carry. Capped
    /// at [`MULTIPART_PART_SIZE`] so the read and write paths share one largest
    /// request, and floored at [`MIN_REQUEST_BODY_BYTES`] so an extremely tight
    /// configured timeout degrades into more requests rather than into
    /// unboundedly many.
    ///
    /// A caller-supplied [`GetRange::Range`] is *not* bounded by this: the
    /// caller asked for exactly those bytes, and silently splitting a range the
    /// caller sized itself would hide the cost from the code that chose it.
    pub fn max_request_body_bytes(&self) -> usize {
        let transfer_budget = self
            .request_timeout
            .saturating_sub(REQUEST_OVERHEAD_ALLOWANCE);
        let millis = u64::try_from(transfer_budget.as_millis()).unwrap_or(u64::MAX);
        let bytes = millis.saturating_mul(FLOOR_TRANSFER_BYTES_PER_SEC) / 1000;
        usize::try_from(bytes)
            .unwrap_or(usize::MAX)
            .clamp(MIN_REQUEST_BODY_BYTES, MULTIPART_PART_SIZE)
    }
}

impl Default for S3HttpConfig {
    fn default() -> Self {
        S3HttpConfig {
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: Duration::from_secs(3),
            pool_idle_timeout: Duration::from_secs(30),
            http2_keep_alive_interval: Duration::from_secs(10),
            http2_keep_alive_timeout: Duration::from_secs(10),
            upload_integrity: UploadIntegrity::Off,
            request_stored_checksum: true,
        }
    }
}

/// Build the `object_store` [`ClientOptions`] from an [`S3HttpConfig`], setting
/// every value #851 cares about explicitly rather than inheriting it.
///
/// **HTTP/2 keep-alive is set but inert under the transport we use.**
/// `object_store`'s `ClientOptions` default is HTTP/1.1-only (arrow-rs#5194:
/// HTTP/2 is measurably slower for the bulk transfers S3 reads are), and we do
/// not force HTTP/2, so reqwest never negotiates it and the keep-alive ping
/// knobs below never fire. They are set anyway so the client is correct *if* a
/// future deployment enables HTTP/2: a black-holed connection is then detected
/// within ~interval+timeout (≈20 s) instead of only at `request_timeout`.
/// `with_http2_keep_alive_while_idle` extends that detection to connections
/// sitting idle in the pool, not only those with an active stream. Under the
/// HTTP/1.1 default that actually runs, connection liveness is covered by
/// `connect_timeout` (dead connect), the request timeout (hung request), and
/// `pool_idle_timeout` (stale pooled socket) instead.
///
/// `pool_max_idle_per_host` is deliberately left unset (no cap): hundreds of
/// concurrent per-query fetches want a large warm pool, and an idle cap would
/// force reconnect churn under exactly the load this deployment runs.
///
/// **`x-amz-checksum-mode: ENABLED` rides on the default headers** (ADR-1696
/// decision 2), which is the only hook in `object_store` 0.14 that lands a
/// header in the request *before* SigV4 signs it (`S3Client::request` and the
/// GET path both apply `ClientOptions::get_default_headers` ahead of
/// `with_aws_sigv4`). S3 requires every `x-amz-*` header to be signed, so the
/// counting HTTP connector below the retry loop --- which sees the response
/// headers and is where the verification itself lives --- cannot add it: a
/// header inserted there would be unsigned and every request would fail
/// `SignatureDoesNotMatch`. The hook is whole-client, so the header also rides
/// on PUT, DELETE and the multipart requests, where it is meaningless and
/// ignored; that is the cost of the only signed placement available. LIST is
/// the one path `object_store` does not sign it onto, and there it is not sent
/// at all: the connector builds its reqwest client without default headers
/// ([`connector`]), since reqwest would otherwise add it after signing.
///
/// [`S3HttpConfig::request_stored_checksum`] set to `false` leaves the header
/// out entirely.
fn client_options(http: &S3HttpConfig) -> ClientOptions {
    let mut default_headers = HeaderMap::new();
    if http.request_stored_checksum {
        default_headers.insert(
            HeaderName::from_static(CHECKSUM_MODE_HEADER),
            HeaderValue::from_static(CHECKSUM_MODE_ENABLED),
        );
    }
    ClientOptions::new()
        .with_timeout(http.request_timeout)
        .with_connect_timeout(http.connect_timeout)
        .with_pool_idle_timeout(http.pool_idle_timeout)
        .with_http2_keep_alive_interval(http.http2_keep_alive_interval)
        .with_http2_keep_alive_timeout(http.http2_keep_alive_timeout)
        .with_http2_keep_alive_while_idle()
        .with_default_headers(default_headers)
}

/// S3 backend implementing [`ObjectStoreBackend`] over
/// `object_store`'s `AmazonS3` client.
pub struct S3Store {
    store: AmazonS3,
    page_size: usize,
    /// Largest body [`S3Store::get_whole_object`] asks for in one request,
    /// resolved once from the [`S3HttpConfig`] this store was built with
    /// ([`S3HttpConfig::max_request_body_bytes`]) so the per-request derivation
    /// is not repeated on every read.
    max_get_chunk: usize,
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
    /// The write-time upload-integrity mode this store was built with (#863).
    /// Drives [`Capabilities::upload_checksum`]: a non-[`UploadIntegrity::Off`]
    /// mode configured `object_store` to attach a server-verified checksum in
    /// [`S3Store::builder`], so the capability reports `true` truthfully.
    upload_integrity: UploadIntegrity,
    /// Where this store records billed HTTP requests and
    /// `ravel_store_get_unverified_total` (ADR-1696 decision 3). Always present:
    /// the HTTP connector is always installed, because the read-side checksum
    /// verification lives in it and a store without it could not verify
    /// anything. A caller that supplied no handle gets a private one, readable
    /// through [`S3Store::get_unverified`] but shared with nothing, so a
    /// scrape path still sees only what it wired up.
    metrics: Arc<StoreMetrics>,
    /// The store's own clock as the last response this store received reported
    /// it (ADR-1685 decision 1), written by the HTTP connector below
    /// `object_store`'s retry loop and read by
    /// [`ObjectStoreBackend::observed_store_time_ns`]. Per store rather than
    /// per process: two stores pointed at different endpoints observe
    /// different clocks.
    store_time: Arc<ObservedStoreTime>,
    /// The read-only bucket-protection control plane (ADR-1727 decision 1). Signs
    /// its own SigV4 GETs with the same credential provider this store holds, so
    /// there is no second credential path. `ravel-cli store qualify`,
    /// `ravel-cli store verify-protection` and `ravel-server`'s
    /// `--require-bucket-protection` startup gate reach it through the concrete
    /// store; a caller holding only `dyn ObjectStoreBackend` does not.
    control_plane: Arc<BucketControlPlaneClient>,
    /// The raw-key ListObjectsV2 client behind `list`, `list_after` and
    /// `list_delimited` (ADR-2637 decision 1). Its requests go through the
    /// same [`S3HttpConnector`] as the data plane, so they are billed and
    /// timed the same way.
    lister: Arc<ListClient>,
}

impl S3Store {
    /// Build with the deliberate [`S3HttpConfig::default`] HTTP-client tuning
    /// (#851). It sets the request/connect/pool-idle timeouts and HTTP/2
    /// keep-alive explicitly rather than inheriting `object_store`'s defaults.
    /// `ravel-server` and `ravel-cli` build their primary stores through the
    /// `with_http_config*` constructors instead, to apply their checksum
    /// flags; the read-only external Parquet profile stores still use this
    /// one, since no upload checksum applies to them.
    pub fn new(config: S3Config) -> Result<Self, StoreError> {
        Self::with_http_config(config, S3HttpConfig::default())
    }

    /// Build with an explicit [`S3HttpConfig`], overriding the default HTTP
    /// client tuning (#851). Exists so an unusual deployment or a test can set
    /// a non-default timeout and have it reach the constructed client.
    pub fn with_http_config(config: S3Config, http: S3HttpConfig) -> Result<Self, StoreError> {
        Self::build(config, http, None)
    }

    /// Build recording billed HTTP requests (attempts, retries included) into
    /// the shared `metrics`, using the default HTTP tuning (issue #928).
    ///
    /// This installs a counting HTTP connector *below* `object_store`'s retry
    /// loop, so `metrics`'s `attempts` counter sees every retry a completed
    /// [`InstrumentedStore`](crate::InstrumentedStore) `calls` count hides. Pass
    /// the same `Arc` to
    /// [`InstrumentedStore::with_metrics`](crate::InstrumentedStore::with_metrics)
    /// so `attempts` and `calls` share one snapshot. `new`/`with_http_config`
    /// install the same connector over a *private* handle, so they record
    /// attempts nowhere a caller can read them beyond this store's own
    /// accessors; the connector itself is unconditional because the read-side
    /// checksum verification (ADR-1696 decision 2) lives in it.
    pub fn with_metrics(config: S3Config, metrics: Arc<StoreMetrics>) -> Result<Self, StoreError> {
        Self::build(config, S3HttpConfig::default(), Some(metrics))
    }

    /// Build with an explicit [`S3HttpConfig`] and an attempt-metrics sink.
    pub fn with_http_config_and_metrics(
        config: S3Config,
        http: S3HttpConfig,
        metrics: Arc<StoreMetrics>,
    ) -> Result<Self, StoreError> {
        Self::build(config, http, Some(metrics))
    }

    /// The shared build path. [`S3HttpConnector`] is always installed: it
    /// counts billed HTTP requests into `attempt_metrics` when a caller
    /// supplied a handle (#928), and it is the only layer that sees a GET
    /// response's `x-amz-checksum-*` header, which the read-side verification
    /// needs (ADR-1696 decision 2). With no caller handle the attempts land in
    /// a private one instead of nowhere.
    fn build(
        config: S3Config,
        http: S3HttpConfig,
        attempt_metrics: Option<Arc<StoreMetrics>>,
    ) -> Result<Self, StoreError> {
        let upload_integrity = http.upload_integrity;
        let (builder, credential_provider, instance_role_provider) = Self::builder(&config, &http)?;
        // The connector wraps the default reqwest client and delegates
        // unchanged, so this changes what is observed, never how a request
        // runs; `retry`/`RetryConfig` stay at `object_store`'s defaults.
        let metrics = attempt_metrics.unwrap_or_default();
        let store_time: Arc<ObservedStoreTime> = Arc::default();
        // The credential provider the bucket-protection control plane signs with:
        // the very one this store already uses (file, instance-role, or an inline
        // static provider built from the same S3Config fields), never a second
        // credential path (ADR-1727 decision 1, S3Config's "no credential-chain
        // magic" rule).
        let control_plane_credentials = if let Some(provider) = &instance_role_provider {
            Arc::clone(provider) as AwsCredentialProvider
        } else if let Some(provider) = &credential_provider {
            Arc::clone(provider) as AwsCredentialProvider
        } else {
            bucket_config::static_credential_provider(
                &config.access_key_id,
                &config.secret_access_key,
                config.session_token.as_deref(),
            )
        };
        // Same timeouts the data plane runs under: an unbounded control-plane GET
        // would hang the startup gate and the CLI on an endpoint that accepts the
        // connection and never answers. `Client::new` would also panic on a TLS
        // backend that fails to initialize; the builder reports it.
        let control_plane_client = bucket_config::control_plane_http_client(
            http.connect_timeout,
            http.request_timeout,
            http.pool_idle_timeout,
            config.allow_http,
        )
        .map_err(|e| {
            StoreError::Permanent(format!("failed to build bucket control-plane client: {e}"))
        })?;
        let list_http = S3HttpConnector::new(Arc::clone(&metrics), Arc::clone(&store_time))
            .connect(&client_options(&http).with_allow_http(config.allow_http))
            .map_err(|e| StoreError::Permanent(format!("failed to build S3 list client: {e}")))?;
        let lister = Arc::new(ListClient::new(
            list_http,
            Arc::clone(&control_plane_credentials),
            config.bucket.clone(),
            config.region.clone(),
            config.endpoint.clone(),
            config.force_path_style,
        ));
        let control_plane = Arc::new(BucketControlPlaneClient::new(
            control_plane_client,
            control_plane_credentials,
            Arc::clone(&metrics),
            config.bucket.clone(),
            config.region.clone(),
            config.endpoint.clone(),
            config.force_path_style,
        ));
        let store = builder
            .with_http_connector(S3HttpConnector::new(
                Arc::clone(&metrics),
                Arc::clone(&store_time),
            ))
            .build()
            .map_err(|e| StoreError::Permanent(format!("failed to build S3 client: {e}")))?;
        Ok(S3Store {
            store,
            page_size: LIST_PAGE_SIZE,
            max_get_chunk: http.max_request_body_bytes(),
            credential_provider,
            instance_role_provider,
            multipart_abort_failures: AtomicU64::new(0),
            multipart_uploads_unreaped: AtomicU64::new(0),
            upload_integrity,
            metrics,
            store_time,
            control_plane,
            lister,
        })
    }

    /// Value of `ravel_store_get_unverified_total` for this store: full-object
    /// reads served without checking the body against a stored checksum
    /// (ADR-1696 decision 3).
    ///
    /// A read counts here when the GET response carried no `x-amz-checksum-*`
    /// header (an endpoint that stores no checksum, or ignored
    /// `x-amz-checksum-mode`), when it carried a digest this adapter cannot
    /// recompute (SHA-256, or a composite multipart digest), or when the object
    /// was large enough that the whole-object read was split into bounded
    /// ranged requests, none of which is the whole object a whole-object
    /// checksum covers. Ranged reads the *caller* asked for never count: they
    /// are outside the check by decision 4, not a gap in it.
    ///
    /// Reads the same counter [`StoreMetrics::get_unverified`] exposes, so a
    /// store built with [`S3Store::with_metrics`] reports it both here and in
    /// that handle's snapshot.
    pub fn get_unverified(&self) -> u64 {
        self.metrics.get_unverified()
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
        http: &S3HttpConfig,
    ) -> Result<
        (
            AmazonS3Builder,
            Option<Arc<FileCredentialProvider>>,
            Option<Arc<InstanceRoleCredentialProvider>>,
        ),
        StoreError,
    > {
        // Install the explicit HTTP client tuning (#851) first: the per-knob
        // client setters below (`with_allow_http`) mutate the same
        // `ClientOptions`, whereas `with_client_options` replaces it wholesale,
        // so it has to come before them or it would clobber `allow_http`.
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_client_options(client_options(http));
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
        // Write-time upload integrity (#863): the only server-verified checksum
        // `object_store` 0.14 can attach is this whole-client algorithm, which it
        // computes itself over the payload and sends as `x-amz-checksum-*`. Off
        // by default; see `UploadIntegrity`. SigV4 signs the checksum header
        // inside `object_store`'s request path, so no signing code changes here.
        if let Some(algorithm) = http.upload_integrity.checksum_algorithm() {
            builder = builder.with_checksum_algorithm(algorithm);
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

    /// Same backend, a different `list()`/`list_after()` page size. Each
    /// ListObjectsV2 request asks for `max-keys` of the keys the page still
    /// needs, at most 1000, and a page larger than 1000 follows
    /// `NextContinuationToken` within the call until it is full. A small
    /// `page_size` therefore makes the backend itself serve a page boundary,
    /// which is what lets [`crate::conformance::run_conformance_suite`]'s
    /// cross-page probe exercise `start-after` against a real store.
    pub fn with_page_size(config: S3Config, page_size: usize) -> Result<Self, StoreError> {
        Self::with_http_config_and_page_size(config, S3HttpConfig::default(), page_size)
    }

    /// [`S3Store::with_page_size`] with an explicit [`S3HttpConfig`], so a
    /// caller that needs a non-default page size keeps its checksum and
    /// timeout settings rather than falling back to the library defaults.
    pub fn with_http_config_and_page_size(
        config: S3Config,
        http: S3HttpConfig,
        page_size: usize,
    ) -> Result<Self, StoreError> {
        let mut store = Self::with_http_config(config, http)?;
        store.page_size = page_size.max(1);
        Ok(store)
    }
}

// --- Bucket-protection control plane impls (ADR-1727 decision 2) ---
//
// `S3Store` answers all three probe seams affirmatively from its own read-only
// SigV4 GETs, while the `dyn ObjectStoreBackend` impls in `conformance.rs` stay
// as they are (every field `Unknown`). `ObjectStoreBackend` itself is unchanged.
// `ravel-cli store qualify`, `ravel-cli store verify-protection` and
// `ravel-server`'s startup gate reach these through the concrete store; the
// server keeps the base `S3Store` beside the wrapped handles for exactly this.

#[async_trait::async_trait]
impl crate::conformance::BucketControlPlane for S3Store {
    async fn bucket_protection_report(
        &self,
        params: &crate::conformance::BucketProtectionParams,
    ) -> crate::conformance::BucketProtectionReport {
        self.control_plane.report(params).await
    }
}

#[async_trait::async_trait]
impl crate::conformance::ObjectLockProbeSource for S3Store {
    async fn object_lock_status(&self) -> crate::conformance::ObjectLockProbe {
        let report = self
            .control_plane
            .report(&crate::conformance::BucketProtectionParams::default())
            .await;
        object_lock_probe(&report)
    }
}

#[async_trait::async_trait]
impl crate::conformance::BucketConfigProbeSource for S3Store {
    async fn bucket_config(&self) -> crate::conformance::BucketConfigProbe {
        let (report, notes) = self
            .control_plane
            .report_with_notes(&crate::conformance::BucketProtectionParams::default())
            .await;
        bucket_config_probe(&report, notes)
    }
}

#[async_trait::async_trait]
impl crate::conformance::BucketProbesSource for S3Store {
    /// Both probes from one report, so the bucket is read once.
    async fn bucket_probes(&self) -> crate::conformance::BucketProbes {
        let (report, notes) = self
            .control_plane
            .report_with_notes(&crate::conformance::BucketProtectionParams::default())
            .await;
        crate::conformance::BucketProbes {
            object_lock: object_lock_probe(&report),
            bucket_config: bucket_config_probe(&report, notes),
        }
    }
}

/// Map the report's `object-lock` condition onto an [`ObjectLockProbe`].
///
/// [`ObjectLockProbe`]: crate::conformance::ObjectLockProbe
fn object_lock_probe(
    report: &crate::conformance::BucketProtectionReport,
) -> crate::conformance::ObjectLockProbe {
    use crate::conformance::{ConditionState, ObjectLockProbe, ProtectionConditionId};
    match report.state(ProtectionConditionId::ObjectLock) {
        Some(ConditionState::Pass) => {
            ObjectLockProbe::enabled("Object Lock is enabled on the bucket (?object-lock)")
        }
        Some(ConditionState::Fail(detail)) => ObjectLockProbe::disabled(detail.clone()),
        Some(ConditionState::Unknown(detail)) => ObjectLockProbe::unknown(detail.clone()),
        None => ObjectLockProbe::unknown("object-lock condition missing from the report"),
    }
}

/// Map the report onto the older three-field [`BucketConfigProbe`]. A rule that
/// covers `t/` but fails its condition (an abort rule longer than 7 days,
/// covering noncurrent rules that disagree) is `NonCompliant` with the failure
/// as its reason; a failing condition with no covering rule is `Absent`.
///
/// [`BucketConfigProbe`]: crate::conformance::BucketConfigProbe
fn bucket_config_probe(
    report: &crate::conformance::BucketProtectionReport,
    notes: ReportNotes,
) -> crate::conformance::BucketConfigProbe {
    use crate::conformance::{
        BucketConfigProbe, ConditionState, LifecycleRuleStatus, ProtectionConditionId,
        VersioningStatus,
    };
    let versioning = match report.state(ProtectionConditionId::Versioning) {
        Some(ConditionState::Pass) => VersioningStatus::On,
        Some(ConditionState::Fail(_)) => VersioningStatus::Off,
        _ => VersioningStatus::Unknown,
    };
    let detail = "derived from the ADR-1727 bucket-protection control plane (?versioning, \
                  ?lifecycle over signed read-only GETs)"
        .to_string();
    let rule_status = |id: ProtectionConditionId, covers_data: bool| match report.state(id) {
        Some(ConditionState::Pass) => LifecycleRuleStatus::Present,
        Some(ConditionState::Fail(failure)) if covers_data => {
            LifecycleRuleStatus::NonCompliant(failure.clone())
        }
        Some(ConditionState::Fail(_)) => LifecycleRuleStatus::Absent,
        _ => LifecycleRuleStatus::Unknown,
    };
    let abort_incomplete_multipart_upload = rule_status(
        ProtectionConditionId::AbortMultipart,
        notes.abort_rule_covers_data,
    );
    let noncurrent_version_expiration = rule_status(
        ProtectionConditionId::NoncurrentExpiration,
        notes.noncurrent_rule_covers_data,
    );
    BucketConfigProbe {
        versioning,
        abort_incomplete_multipart_upload,
        noncurrent_version_expiration,
        detail,
    }
}

/// `key -> Path`, or [`StoreError::UnaddressableKey`] when the two differ.
///
/// `Path::from` drops empty segments and percent-encodes control characters,
/// non-ASCII bytes, `.`/`..` segments and reserved punctuation such as `*` and
/// `#`, so a request built from a key it rewrites would reach another key
/// (ADR-2637 decision 3). Every key operation of [`S3Store`] and
/// [`crate::external::ExternalStore`] builds its `Path` here, before any
/// request, so a refused key sends nothing.
pub(crate) fn path_of(key: &str) -> Result<Path, StoreError> {
    crate::check_addressable(key)?;
    Ok(Path::from(key))
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

/// The metadata and the pin for one object, from what S3 reported about it.
///
/// The two carry different things and neither can be derived from the other.
/// [`ObjectMeta::version`] is the compare-and-swap token, which on S3 is the
/// ETag, because that is what a conditional write compares. The pin's version
/// is the object version id, which is what a read selects with `versionId`. A
/// bucket without versioning reports no version id and the pin then carries
/// the ETag alone, so a read through it is a plain `If-Match`.
fn meta_to_pin(meta: object_store::ObjectMeta) -> Result<(ObjectMeta, Pin), StoreError> {
    let version = meta.version.clone();
    let mapped = map_meta(meta)?;
    let pin = Pin::from_store(mapped.etag.0.clone(), version);
    Ok((mapped, pin))
}

/// Error mapping shared by every non-`put` operation. `put` has its own
/// mode-aware wrapper (see [`map_put_error`]) because conditional-write
/// failures must be interpreted differently depending on `PutMode`.
///
/// A 404 is [`StoreError::NotFound`] unless its body names `NoSuchBucket`,
/// which is [`StoreError::Permanent`] on every operation: a missing bucket is
/// not a missing object. `object_store` reports the list 404 as `Generic`, so
/// that branch checks for the code too. A HEAD 404 has no body and so no code,
/// and stays `NotFound`.
pub(crate) fn map_error_common(e: object_store::Error) -> StoreError {
    use object_store::Error as E;
    match e {
        E::NotFound { path, source } if is_no_such_bucket(source.as_ref()) => no_such_bucket(&path),
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
        E::Generic { store, source } if is_no_such_bucket(source.as_ref()) => no_such_bucket(store),
        E::Generic { store, source } => classify_generic(store, source.as_ref()),
        other => StoreError::Permanent(other.to_string()),
    }
}

/// The `<Error><Code>` of the S3 error body an `object_store` error carries.
///
/// `object_store` keeps a failed response's body only inside its crate-private
/// `RetryError`, whose `Display` ends with the body, so the text is the one
/// place the code survives. `None` when the response had no XML error body.
/// Azure Blob Storage and the GCS XML API wrap their codes in the same
/// envelope, so the external store reads theirs through this too.
pub(crate) fn s3_error_code(
    source: &(dyn std::error::Error + Send + Sync + 'static),
) -> Option<String> {
    let text = source.to_string();
    let start = text.find("<Error>")?;
    bucket_config::parse_error_code(&text.as_bytes()[start..])
}

fn is_no_such_bucket(source: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    s3_error_code(source).as_deref() == Some("NoSuchBucket")
}

/// The body is left out: an S3 error body can echo the request it refuses.
fn no_such_bucket(context: &str) -> StoreError {
    StoreError::Permanent(format!("{context}: bucket does not exist (NoSuchBucket)"))
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
///
/// When the [`classified_text`] carries an S3 XML error body with a `Code`,
/// the body can echo the key in `Key` and `Resource`, so the range is read
/// from the code (`InvalidRange`) or the status line (a 416, or "range" with
/// "satisfiable" or "too large") only. Without a code, the range words are
/// matched over the whole classified text.
pub(crate) fn map_get_error(e: object_store::Error) -> StoreError {
    if let object_store::Error::Generic { source, .. } = &e {
        let text = classified_text(source.as_ref());
        let invalid_range = match status_line_and_code(&text) {
            (status_line, Some(code)) => {
                code == "InvalidRange"
                    || status_line.contains(" 416 ")
                    || has_range_words(&status_line)
            }
            (_, None) => has_range_words(&text.to_lowercase()),
        };
        if invalid_range {
            return StoreError::InvalidRange(source.to_string());
        }
    }
    map_error_common(e)
}

fn has_range_words(lower: &str) -> bool {
    lower.contains("range") && (lower.contains("satisfiable") || lower.contains("too large"))
}

/// Split [`classified_text`] output carrying an S3 XML error body into the
/// lowercased text before its `<Error>` element (the status line) and the
/// body's `Code`. Text with no body, or a body with no parseable `Code`, comes
/// back lowercased whole with no code.
fn status_line_and_code(text: &str) -> (String, Option<String>) {
    if let Some(start) = text.find("<Error>")
        && let Some(code) = bucket_config::parse_error_code(&text.as_bytes()[start..])
    {
        return (text[..start].to_lowercase(), Some(code));
    }
    (text.to_lowercase(), None)
}

/// `delete`-specific mapping. `object_store` sends every delete as a
/// `DeleteObjects` request unless `disable_bulk_delete` is set, which Ravel
/// never does, and S3 refuses a single key inside that request's
/// 200 response. `object_store` surfaces the refusal as `Error::Generic` over a
/// crate-private `DeleteFailed`, so its S3 error code is recoverable only from
/// the `Display` text. Each named code maps the way the single-request path
/// maps the HTTP status S3 documents for it: 403 to
/// [`StoreError::AccessDenied`], 404 to [`StoreError::NotFound`], 412 to
/// [`StoreError::PreconditionFailed`], 503 to [`StoreError::Throttled`]. Any
/// other code falls through to [`classify_generic`]. A response naming
/// `SlowDown` or `InternalError` never gets here: `object_store` retries the
/// whole request on either, and an exhausted retry is an ordinary `Generic`.
///
/// A whole-request 404 is different: `DeleteObjects` addresses the bucket, and
/// S3 reports a missing key per key, so a request-level 404 is the bucket
/// itself missing (`NoSuchBucket`). Only a `NoSuchKey` code reads as
/// [`StoreError::NotFound`] there; any other code, or none, is
/// [`StoreError::Permanent`], and so is a per-key `NoSuchBucket`.
fn map_delete_error(e: object_store::Error) -> StoreError {
    if let object_store::Error::NotFound { path, source } = &e {
        return match s3_error_code(source.as_ref()).as_deref() {
            Some("NoSuchKey") => StoreError::NotFound,
            Some(code) => {
                StoreError::Permanent(format!("{path}: DeleteObjects refused with 404 {code}"))
            }
            None => StoreError::Permanent(format!("{path}: DeleteObjects refused with 404")),
        };
    }
    if let object_store::Error::Generic { store, source } = &e
        && let Some(code) = delete_objects_key_code(source.as_ref())
    {
        match code.as_str() {
            "AccessDenied"
            | "AllAccessDisabled"
            | "AccountProblem"
            | "InvalidAccessKeyId"
            | "InvalidObjectState"
            | "SignatureDoesNotMatch" => {
                return StoreError::AccessDenied(format!("{store}: {source}"));
            }
            "NoSuchKey" => return StoreError::NotFound,
            "NoSuchBucket" => return no_such_bucket(&format!("{store}: {source}")),
            "PreconditionFailed" => return StoreError::PreconditionFailed,
            "ServiceUnavailable" => {
                return StoreError::Throttled {
                    retry_after_ms: 1000,
                };
            }
            _ => {}
        }
    }
    map_error_common(e)
}

/// The S3 error code of a per-key `DeleteObjects` refusal, parsed from
/// `object_store`'s `"DeleteObjects request failed for key {path}: {message}
/// (code: {code})"`. `None` for any other error.
fn delete_objects_key_code(
    source: &(dyn std::error::Error + Send + Sync + 'static),
) -> Option<String> {
    let msg = source.to_string();
    let rest = msg.strip_prefix("DeleteObjects request failed for key ")?;
    let (_, code) = rest.rsplit_once(" (code: ")?;
    code.strip_suffix(')').map(str::to_string)
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
/// `Transient` for the whole S3 adapter.
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
///    [`classified_text`], which leaves out the request URI and any wrapper
///    text naming a path, for well-known signals: timeout words to
///    [`StoreError::Timeout`],
///    throttle words, including the reason phrases `object_store` always
///    prints after a 429 or 503 status ("too many requests", "service
///    unavailable"), to [`StoreError::Throttled`]. A bare "429" or "503"
///    digit run is not a signal: a port, a key, an elapsed time or a request
///    id can carry one. When that text carries an S3 XML error body with a
///    `Code`, the words are matched over the status line only and the code
///    stands in for the body: a code [`is_throttle_code`] accepts to
///    [`StoreError::Throttled`], `RequestTimeout` to
///    [`StoreError::Timeout`], so a body echoing a key spelled with a class
///    word cannot pick the class. The throttle check runs before the timeout
///    check, and each reads the status line before the code, so a 503 whose
///    code is `RequestTimeout` reads [`StoreError::Throttled`].
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
    let text = classified_text(source);
    // With an S3 error code in the body, the code stands in for the rest of
    // the body, which can echo the request.
    let (lower, code) = status_line_and_code(&text);
    let code = code.as_deref();
    // `object_store`'s `RetryError` Display puts ", ..., retry_timeout: {d} "
    // in its prefix on every exhausted-retry message, and the field name
    // contains "timeout". `classified_text` already drops that prefix; the
    // strip below is a fallback for text that reaches here without the
    // `RetryError` shape, so an exhausted 500 still stays Transient.
    if lower.contains("too many requests")
        || lower.contains("slow down")
        || lower.contains("slowdown")
        || lower.contains("throttl")
        || lower.contains("service unavailable")
        || code.is_some_and(is_throttle_code)
    {
        return StoreError::Throttled {
            retry_after_ms: 1000,
        };
    }
    let without_retry_field = lower.replace("retry_timeout", " ");
    if without_retry_field.contains("timed out")
        || without_retry_field.contains("timeout")
        || without_retry_field.contains("deadline")
        || code == Some("RequestTimeout")
    {
        return StoreError::Timeout;
    }
    StoreError::Transient(format!("{store}: {msg}"))
}

/// S3 error codes that ask the client to slow down. Any code starting with
/// `SlowDown` counts, which covers MinIO's `SlowDownRead` and `SlowDownWrite`,
/// and so does any code containing `Throttl`, which covers spellings such as
/// `RequestThrottled` and `ThrottledException`.
fn is_throttle_code(code: &str) -> bool {
    code.starts_with("SlowDown")
        || code.contains("Throttl")
        || matches!(
            code,
            "Throttling" | "ThrottlingException" | "RequestLimitExceeded" | "TooManyRequests"
        )
}

/// The part of a `Generic` error's text that carries the failure itself, with
/// the request URI and any wrapper text naming a path left out, so a bucket,
/// endpoint or key spelled with a class word in the URI cannot pick the class.
/// The inner text still includes the response body, which can echo the
/// request; [`classify_generic`] reads only the S3 error code of a body that
/// carries one.
///
/// `RetryError` renders `"Error performing {method} {uri} in {elapsed:?}"`,
/// an optional exhausted-retry suffix, then `" - {inner}"`, where `inner` is
/// its `RequestError` source (status and body, error response body, or the
/// transport error, which `object_store` strips of its URL). Neither type is
/// nameable, so the inner error is found in the source chain by that shape,
/// and failing that (a chain the source does not expose) parsed out of the
/// text. A per-key `DeleteObjects` refusal names the key with no reliable end
/// marker, so only its code is used. Any other text carries no request URI
/// and is used whole.
fn classified_text(source: &(dyn std::error::Error + Send + Sync + 'static)) -> String {
    if let Some(code) = delete_objects_key_code(source) {
        return code;
    }
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(source);
    while let Some(err) = current {
        if let Some(inner) = err.source() {
            let outer = err.to_string();
            let inner_text = inner.to_string();
            if outer.starts_with("Error performing ")
                && outer
                    .strip_suffix(inner_text.as_str())
                    .is_some_and(|head| head.ends_with(" - "))
            {
                return inner_text;
            }
        }
        current = err.source();
    }
    let text = source.to_string();
    match retry_error_inner(&text) {
        Some(inner) => inner.to_string(),
        None => text,
    }
}

/// The `{inner}` of the last `RetryError`-shaped segment in `text`. The
/// method is an upper-case token and an `http::Uri` holds no space, so the
/// first `" - "` after `" in "` ends the prefix: neither the elapsed time nor
/// the exhausted-retry suffix contains one. Wrapper text such as `"Error
/// performing list request: "` does not match the shape and is skipped. The
/// last match is taken because a wrapper's raw path precedes the error it
/// wraps, so a path shaped like a `RetryError` prefix matches earlier.
fn retry_error_inner(text: &str) -> Option<&str> {
    const MARKER: &str = "Error performing ";
    let mut last = None;
    let mut from = 0;
    while let Some(found) = text[from..].find(MARKER) {
        let start = from + found + MARKER.len();
        from = start;
        let rest = &text[start..];
        let Some((method, rest)) = rest.split_once(' ') else {
            continue;
        };
        if method.is_empty() || !method.bytes().all(|b| b.is_ascii_uppercase()) {
            continue;
        }
        let Some((uri, rest)) = rest.split_once(' ') else {
            continue;
        };
        if uri.is_empty() || !rest.starts_with("in ") {
            continue;
        }
        if let Some((_, inner)) = rest.split_once(" - ") {
            last = Some(inner);
        }
    }
    last
}

/// Local CRC32C pre-flight, shared by [`S3Store::put`] and
/// [`S3MultipartUpload::put_part`]: recomputes the digest over the buffer we
/// are about to hand `object_store` and rejects a caller/payload mismatch
/// before any network call. This CRC32C value is never itself put on the wire
/// (`object_store` 0.14 has no hook for a caller-supplied digest); under a
/// non-`Off` [`S3HttpConfig::upload_integrity`] the on-wire, server-verified
/// checksum is a separate SHA-256/CRC64-NVME `object_store` computes over the
/// same buffer, so the two together cover the caller's bytes end to end. See
/// [`UploadIntegrity`] and the doc comment on [`S3Store::capabilities`].
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

    /// Disambiguate an `AlreadyExists` from a [`PutMode::CreateIfAbsent`] PUT
    /// with one HEAD (docs/object-store-contract.md, "Semantics adapters MUST
    /// honor").
    ///
    /// `object_store` 0.14 maps a raw 409 to `AlreadyExists` and enables
    /// conflict retry only for the update / etag-match modes, never for create.
    /// So a 409 `ConditionalRequestConflict` — which the AWS PutObject spec
    /// says the client MUST retry — reaches this crate looking exactly like a
    /// genuine already-exists. The HTTP status that would tell them apart lives
    /// in `object_store`'s crate-private error type (the same reason
    /// [`map_put_error`] is mode-aware rather than status-aware), so it is
    /// unreachable here; one HEAD is the disambiguation:
    ///
    /// - **key present** (`Ok`) → a real already-exists →
    ///   [`StoreError::AlreadyExists`], exactly as before this method existed.
    /// - **key absent** (`NotFound`) → the 409 could not have been a real
    ///   collision → a transient conditional-request conflict →
    ///   [`StoreError::Transient`], which [`StoreError::is_retryable`] routes
    ///   back into the caller's existing retry loop (the ingest flush loop, the
    ///   commit publish path).
    /// - **HEAD itself failed retryably** (`Throttled`/`Timeout`/`Transient`) →
    ///   the probe determined nothing, so the key's state is unknown. The
    ///   result is that same retryable error, surfaced verbatim. Returning a
    ///   terminal `AlreadyExists` here would be wrong, not merely
    ///   conservative: the PUT already failed so nothing was written, and
    ///   `AlreadyExists` is not retryable, so the caller would stop and treat a
    ///   race it did not lose as lost, sending the commit publish path into
    ///   `resolve_already_exists` to read back a winner that may not exist. A
    ///   retryable answer cannot lose a genuine already-exists: on the retry
    ///   the PUT conflicts again, and once a HEAD finally succeeds a present
    ///   key still yields `AlreadyExists`, so a real collision is delayed,
    ///   never downgraded. An inconclusive probe that keeps failing instead
    ///   exhausts the retry budget and surfaces a retryable error, the correct
    ///   report for a state nobody could determine.
    /// - **HEAD itself failed terminally** (`AccessDenied`, `Permanent`, and
    ///   the other non-retryable classes) → retrying cannot make the probe
    ///   conclusive, so the outcome cannot improve; fall back to the
    ///   conservative `AlreadyExists`.
    ///
    /// The split-brain guard on the commit path and the vanished-part guard on
    /// the compaction path are preserved by construction: a retryable result
    /// is returned only when the key is ABSENT or the probe was inconclusive,
    /// and a genuine collision requires the key PRESENT, so no real
    /// already-exists is ever downgraded to a retry.
    ///
    /// The arms enumerate every [`StoreError`] variant rather than leaning on a
    /// catch-all in either direction: a catch-all that mapped every non-absent
    /// HEAD error to `AlreadyExists` is what turned an inconclusive probe into
    /// a terminal verdict in the first place.
    async fn disambiguate_create_conflict(&self, key: &str) -> Result<PutOutcome, StoreError> {
        match self.head(key).await {
            Ok(_) => Err(StoreError::AlreadyExists),
            Err(StoreError::NotFound) => Err(StoreError::Transient(format!(
                "conditional-request conflict on create of {key}: 409 with the key \
                 absent on HEAD, retryable per the AWS PutObject specification"
            ))),
            Err(
                e @ (StoreError::Throttled { .. } | StoreError::Timeout | StoreError::Transient(_)),
            ) => Err(e),
            // The listing-drain variants are synthesized by `list_all`, never
            // returned by `head`, so they are unreachable here. Pass them
            // through unchanged rather than fold them into `AlreadyExists`:
            // that keeps an impossible value honest without fabricating a
            // terminal collision verdict. Named, not a wildcard, so a new
            // variant still fails to compile here.
            Err(
                e @ (StoreError::ListRepeatedToken { .. }
                | StoreError::ListPageCeiling { .. }
                | StoreError::ListOrderViolation { .. }),
            ) => Err(e),
            // Likewise impossible from this adapter's own `head`, which
            // implements the operation and never refuses a write it was not
            // asked to make. Passed through for the same reason.
            Err(e @ (StoreError::Unsupported { .. } | StoreError::ReadOnly { .. })) => Err(e),
            // The put that got here already passed `path_of` for this key, so
            // `head` cannot refuse it. Passed through for the same reason.
            Err(e @ StoreError::UnaddressableKey { .. }) => Err(e),
            Err(
                StoreError::AccessDenied(_)
                | StoreError::PreconditionFailed
                | StoreError::Corrupted(_)
                | StoreError::InvalidRange(_)
                | StoreError::Permanent(_)
                | StoreError::AlreadyExists,
            ) => Err(StoreError::AlreadyExists),
        }
    }

    /// The [`MULTIPART_THRESHOLD`] path of [`ObjectStoreBackend::put`]: cut the
    /// buffer into [`MULTIPART_PART_SIZE`] parts, upload at most
    /// [`MULTIPART_UPLOAD_CONCURRENCY`] of them at a time (fewer when a
    /// scheduled handle admitted the put with fewer permits), then complete. Any
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
        let path = path_of(key)?;
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
        let concurrency = crate::scheduling::request_budget()
            .map_or(MULTIPART_UPLOAD_CONCURRENCY, |budget| {
                budget.clamp(1, MULTIPART_UPLOAD_CONCURRENCY)
            });
        let mut failure = None;
        {
            let mut inflight = futures::stream::iter(pending).buffer_unordered(concurrency);
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

    /// The read path shared by [`ObjectStoreBackend::get`] and
    /// [`ObjectStoreBackend::get_pinned`]: identical request shaping and range
    /// validation, differing only in whether a caller-supplied [`Pin`] rides
    /// along as a precondition. Not wrapped in a [`connector::scope`]; the two
    /// callers own that, so one logical call is one scope either way.
    async fn get_inner(
        &self,
        key: &str,
        range: GetRange,
        pin: Option<&Pin>,
    ) -> Result<PinnedRead, StoreError> {
        let os_range = match range {
            // The one request whose size the caller does not choose, so the one
            // that has to be bounded here to stay inside
            // `S3HttpConfig::request_timeout`.
            GetRange::Full => return self.get_whole_object(key, pin).await,
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
        let chunk = self.get_one(key, os_range, pin, None).await?;
        Ok(chunk.into_pinned_read())
    }

    /// One GET, ranged or not, reduced to the three things this adapter needs
    /// from it. `pin` rides along as an `If-Match` precondition (plus a
    /// `versionId` selector when it carries one), used by
    /// [`S3Store::get_whole_object`] to pin every request of a split read to
    /// one version of the object and by
    /// [`ObjectStoreBackend::get_pinned`] to pin a read to the identity the
    /// catalog recorded. The server evaluates it; nothing here compares ETags
    /// after the bytes have been paid for.
    ///
    /// `body_limit` caps how many body bytes are read: once the response has
    /// delivered that many and more remain, the rest of the body is dropped
    /// unread and the returned chunk holds exactly `body_limit` bytes. `None`
    /// reads the whole body, which is only safe for a request whose size this
    /// adapter or its caller already bounded with a range.
    async fn get_one(
        &self,
        key: &str,
        range: Option<OsGetRange>,
        pin: Option<&Pin>,
        body_limit: Option<usize>,
    ) -> Result<GetChunk, StoreError> {
        // The observation slot is scoped to this one request, so the
        // concurrently-polled ranged GETs of a split whole-object read do not
        // overwrite each other's response headers.
        let path = path_of(key)?;
        let (result, observation) = connector::observe_get(self.store.get_opts(
            &path,
            OsGetOptions {
                range,
                if_match: pin.map(|pin| pin.etag.clone()),
                version: pin.and_then(|pin| pin.version.clone()),
                ..Default::default()
            },
        ))
        .await;
        let result = result.map_err(map_get_error)?;
        let etag = result
            .meta
            .e_tag
            .clone()
            .ok_or_else(|| StoreError::Permanent(format!("S3 returned no ETag for {key}")))?;
        // For a partial response this is the total object size parsed out of
        // `Content-Range`, not the length of the slice returned, which is what
        // makes one bounded request enough to learn how many more to issue.
        let total_size = result.meta.size;
        let version = result.meta.version.clone();
        let data = match body_limit {
            None => result.bytes().await.map_err(map_error_common)?,
            Some(limit) => read_body_capped(result, total_size, limit).await?,
        };
        Ok(GetChunk {
            data,
            etag,
            version,
            total_size,
            observation,
        })
    }

    /// Read-side verification for one full-object read (ADR-1696 decisions 2
    /// and 3): recompute the stored checksum over the bytes received, or count
    /// the read unverified when there is nothing to recompute.
    ///
    /// `chunk` must be a response that carried the *entire* object; a caller
    /// that assembled the object from several ranged responses has no
    /// whole-object body to check and passes `None`, which counts unverified.
    fn verify_full_read(&self, key: &str, chunk: Option<&GetChunk>) -> Result<(), StoreError> {
        if let Some(chunk) = chunk
            && let Some(observation) = chunk.observation.as_ref()
            && observation.whole_object
            && let ObservedChecksum::Verifiable(stored) = &observation.checksum
        {
            return stored.verify(key, &chunk.data);
        }
        // No checksum, one this adapter cannot recompute, or a body that is not
        // the whole object: serve and count, never refuse (decision 3).
        self.metrics.record_get_unverified();
        Ok(())
    }

    /// [`GetRange::Full`] as bounded requests: complete-object semantics for
    /// the caller, no single request reading more than
    /// [`S3HttpConfig::max_request_body_bytes`].
    ///
    /// An unranged GET is the only request this adapter issues whose size the
    /// data decides rather than this crate, so read to its end it is the one
    /// that can outgrow [`S3HttpConfig::request_timeout`] — a 256 MiB L1
    /// compaction part cannot finish inside 20 s at the floor rate the timeout
    /// is sized against, and retrying it only re-runs a request that never
    /// fits. Cutting it at the bound and splitting the rest makes every request
    /// one the timeout can carry.
    ///
    /// **The first request is unranged.** An endpoint returns the stored
    /// checksum only on a response to an unranged GET: MinIO, and RustFS which
    /// derives from it, drop `x-amz-checksum-*` whenever a `Range` header is
    /// present, whatever the range covers. So the first request asks for the
    /// whole object and its body is read through [`read_body_capped`], which
    /// stops after [`S3HttpConfig::max_request_body_bytes`] and drops the rest
    /// of the response unread. The bound on what one request moves and on what
    /// this call buffers from it is therefore the same as a ranged first
    /// request's; what differs is that an object that fits (every commit
    /// record, footer and index object) arrives in one response that carries
    /// its checksum, and is verified.
    ///
    /// **Cost.** An object at or below the chunk size is exactly one request,
    /// with no HEAD before it; above it, `ceil(size / chunk)` requests, the
    /// first being the truncated unranged one and the rest ranged, up to
    /// [`WHOLE_OBJECT_GET_CONCURRENCY`] of them in flight, and no more than the
    /// permits a scheduled handle's read holds once it has widened its budget
    /// with the free ones ([`crate::scheduling`], "Ops that fan out"); the
    /// truncated first request is always alone. Wire bytes are the
    /// object's size apart from one extra set of response headers per
    /// additional request and whatever the endpoint had already sent of the
    /// abandoned first body before the connection was dropped; neither figure
    /// counts `object_store`'s internal retries, which sit inside each request.
    /// Dropping an unread body closes that connection rather than returning it
    /// to the pool, which only an object above the chunk size pays.
    ///
    /// **One version, or an error.** Every request after the first carries the
    /// first's ETag as an `If-Match`, so an object overwritten mid-read fails
    /// the read instead of splicing two versions into one buffer. Data objects
    /// are immutable, so this is a guard on the mutable-pointer keys, and those
    /// are small enough to take the single-request path anyway.
    ///
    /// **Caller-supplied pin.** With `pin` set (the `get_pinned` path) the
    /// *first* request carries it too, and a refused precondition stays a
    /// [`StoreError::PreconditionFailed`] rather than being reported as the
    /// retryable mid-read overwrite above: the caller pinned a specific
    /// identity, so a fresh read would fail the same way.
    async fn get_whole_object(
        &self,
        key: &str,
        pin: Option<&Pin>,
    ) -> Result<PinnedRead, StoreError> {
        let chunk = self.max_get_chunk as u64;
        let first = self
            .get_one(key, None, pin, Some(self.max_get_chunk))
            .await?;

        let total_size = first.total_size;
        if first.data.len() as u64 > total_size {
            return Err(StoreError::Transient(format!(
                "get of {key}: response carried {} bytes of a {total_size}-byte object",
                first.data.len()
            )));
        }
        if first.data.len() as u64 == total_size {
            // One request carried the whole object, so the stored whole-object
            // checksum applies to exactly these bytes.
            self.verify_full_read(key, Some(&first))?;
            return Ok(first.into_pinned_read());
        }

        let mut ranges = Vec::new();
        let mut offset = first.data.len() as u64;
        while offset < total_size {
            let end = (offset + chunk).min(total_size);
            ranges.push(offset..end);
            offset = end;
        }

        let capacity = usize::try_from(total_size).map_err(|_| {
            StoreError::Permanent(format!(
                "get of {key}: object of {total_size} bytes does not fit in memory"
            ))
        })?;
        let mut data = BytesMut::with_capacity(capacity);
        data.extend_from_slice(&first.data);

        // `buffered`, not `buffer_unordered`: the pieces are concatenated in
        // issue order, so they must be yielded in issue order.
        let continuation = match pin {
            Some(pin) => pin.clone(),
            // ETag only. Carrying the first response's version id would send
            // `versionId` on Ravel's own (versioned, Object-Locked) bucket, which
            // needs `s3:GetObjectVersion`, a grant the shipped IAM templates do
            // not carry; and it would turn a mid-read overwrite into a silent
            // read of the old version instead of the failure documented above.
            None => Pin::etag(first.etag.clone()),
        };
        // A scheduled handle admitted this read with one permit; the fan-out is
        // known only now, so this is where it can take more.
        let fan_out = ranges.len().min(WHOLE_OBJECT_GET_CONCURRENCY);
        let concurrency = crate::scheduling::widen_request_budget(fan_out)
            .map_or(WHOLE_OBJECT_GET_CONCURRENCY, |budget| {
                budget.clamp(1, WHOLE_OBJECT_GET_CONCURRENCY)
            });
        {
            let mut inflight = futures::stream::iter(ranges.into_iter().map(|range| {
                self.get_one(
                    key,
                    Some(OsGetRange::Bounded(range)),
                    Some(&continuation),
                    None,
                )
            }))
            .buffered(concurrency);
            while let Some(piece) = inflight.next().await {
                let piece = piece.map_err(|e| match e {
                    // The `If-Match` failed: the object was overwritten between
                    // this read's first request and this one. Retryable,
                    // because a fresh read sees one consistent version. A
                    // caller-supplied pin is different: it names one identity,
                    // so the refusal is the answer and stays as it is.
                    StoreError::PreconditionFailed if pin.is_none() => {
                        StoreError::Transient(format!(
                            "get of {key}: object was overwritten during a bounded whole-object read"
                        ))
                    }
                    other => other,
                })?;
                data.extend_from_slice(&piece.data);
            }
        }

        if data.len() as u64 != total_size {
            return Err(StoreError::Transient(format!(
                "get of {key}: assembled {} bytes from bounded requests, expected {total_size}",
                data.len()
            )));
        }
        // Assembled from a truncated first response and ranged ones: no single
        // body covers the object the stored checksum was computed over, and the
        // ranged responses carry none, so this read is unverified and says so
        // (ADR-1696 decision 3 and its amendment). It is counted once for the
        // logical read, not once per chunk.
        self.verify_full_read(key, None)?;
        Ok(GetChunk {
            data: data.freeze(),
            etag: first.etag,
            version: first.version,
            total_size,
            observation: None,
        }
        .into_pinned_read())
    }
}

/// Read at most `limit` bytes of `result`'s body. An object larger than that is
/// cut at exactly `limit` bytes and the rest of the response is dropped unread,
/// so nothing past the bound is ever buffered here. A body that ends first is
/// returned whole.
async fn read_body_capped(
    result: object_store::GetResult,
    total_size: u64,
    limit: usize,
) -> Result<Bytes, StoreError> {
    let expected = usize::try_from(total_size).unwrap_or(usize::MAX);
    let mut data = BytesMut::with_capacity(expected.min(limit));
    let mut stream = result.into_stream();
    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(map_error_common)?;
        let room = limit.saturating_sub(data.len());
        if frame.len() > room {
            data.extend_from_slice(&frame[..room]);
            break;
        }
        data.extend_from_slice(&frame);
    }
    Ok(data.freeze())
}

/// One GET response reduced to what [`ObjectStoreBackend::get`] needs:
/// the bytes it returned, the object's ETag, the object's own version id when
/// the bucket has versioning on, and the object's *total* size (from
/// `Content-Range` on a partial response, so it is the whole object's size even
/// when the body is one chunk of it).
struct GetChunk {
    data: Bytes,
    etag: String,
    /// `x-amz-version-id` as `object_store` reports it: `None` on an
    /// unversioned bucket. This is the selector half of a [`Pin`], and the only
    /// place a read can learn it.
    version: Option<String>,
    total_size: u64,
    /// What the HTTP connector saw on this response: the stored checksum
    /// header, and whether the body is the whole object (ADR-1696). `None` when
    /// no response was observed at all, which counts as unverified.
    observation: Option<GetObservation>,
}

impl GetChunk {
    /// The bytes plus the identity to record for them.
    ///
    /// [`GetOutcome::version`] stays the CAS [`Version`], which on S3 is the
    /// ETag: commit-protocol callers spend it as a CAS token and must keep
    /// getting one. The object's version id is reported through
    /// [`PinnedRead::pin`] instead, where a caller recording a grant looks for
    /// it.
    fn into_pinned_read(self) -> PinnedRead {
        PinnedRead {
            pin: Pin::from_store(self.etag.clone(), self.version),
            outcome: GetOutcome {
                data: self.data,
                etag: Etag(self.etag.clone()),
                version: Version(self.etag),
                total_size: self.total_size,
            },
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
        connector::scope(StoreOp::Put, async move {
            let path = path_of(key)?;
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
            if matches!(opts.mode, PutMode::Overwrite)
                && data.len() > MULTIPART_THRESHOLD
                && !self.upload_integrity.is_enabled()
            {
                return self.put_via_multipart(key, data).await;
            }
            // With upload integrity enabled the multipart path is excluded.
            // Its part requests carry checksums on the wire, proven part by
            // part by s3::tests::multipart_parts_carry_checksums_under_integrity,
            // but no real endpoint has been checked to verify-or-reject them,
            // so `upload_checksum` rests on the single-PUT path alone: ONE
            // billed PUT where multipart costs parts + 2, and one checksum
            // over the whole object. That path covers every size up to S3's
            // 5 GiB per-request ceiling, and a payload above it is refused
            // loudly.
            if self.upload_integrity.is_enabled() && data.len() as u64 > SINGLE_PUT_MAX_BYTES {
                return Err(StoreError::Permanent(format!(
                    "put of {key}: {} bytes exceeds the {SINGLE_PUT_MAX_BYTES}-byte single-PUT \
                     ceiling, and with upload integrity on every put is a single PUT; disable \
                     upload integrity (UploadIntegrity::Off) to write objects this large",
                    data.len(),
                )));
            }
            let os_mode = match &opts.mode {
                PutMode::Overwrite => OsPutMode::Overwrite,
                PutMode::CreateIfAbsent => OsPutMode::Create,
                PutMode::CasVersion(version) => OsPutMode::Update(UpdateVersion {
                    e_tag: Some(version.0.clone()),
                    version: Some(version.0.clone()),
                }),
            };
            let payload = PutPayload::from(data);
            let result = match self
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
            {
                Ok(result) => result,
                Err(e) => {
                    let mapped = map_put_error(e, &opts.mode);
                    // A `CreateIfAbsent` PUT that surfaced `AlreadyExists` may be
                    // a genuine already-exists or a transient 409
                    // `ConditionalRequestConflict` the AWS PutObject spec says
                    // to retry; the HTTP status is unreachable here, so one HEAD
                    // decides. See `disambiguate_create_conflict`.
                    if matches!(mapped, StoreError::AlreadyExists)
                        && matches!(opts.mode, PutMode::CreateIfAbsent)
                    {
                        return self.disambiguate_create_conflict(key).await;
                    }
                    return Err(mapped);
                }
            };
            outcome_of(key, result)
        })
        .await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        let upload = self
            .store
            .put_multipart(&path_of(key)?)
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
        connector::scope(StoreOp::Get, self.get_inner(key, range, None))
            .await
            .map(|read| read.outcome)
    }

    /// The same request as [`Self::get`] with `If-Match` (and `versionId`, when
    /// the pin carries one) attached, so S3 decides the precondition and
    /// selects the version: a replaced object costs one refused request rather
    /// than a body, and a pinned version keeps serving its own bytes.
    ///
    /// Counted under [`StoreOp::Get`], like `get`: it is one GET on the wire.
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        connector::scope(StoreOp::Get, self.get_inner(key, range, Some(pin))).await
    }

    /// Same request as [`Self::get`], reporting the object's version id
    /// alongside the bytes so a caller can record a pin for exactly what it
    /// read.
    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        connector::scope(StoreOp::Get, self.get_inner(key, range, None)).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        connector::scope(StoreOp::Head, async move {
            let path = path_of(key)?;
            let meta = self.store.head(&path).await.map_err(map_error_common)?;
            map_meta(meta)
        })
        .await
    }

    /// One HEAD, reporting `x-amz-version-id` as the pin's selector.
    ///
    /// [`Self::head`] cannot: [`ObjectMeta::version`] is the CAS token, which
    /// on S3 is the ETag, so `object_store`'s own `version` is dropped there.
    /// This and the `PinnedRead` that `get_pinned` and `get_with_pin` return
    /// are the paths on which an S3 pin can carry a version (the ADR-2040
    /// pinning amendment, "S3 versions must be surfaced"). On a
    /// bucket without versioning `object_store` reports no version and the pin
    /// is ETag-only, which is the honest answer: there is nothing to select.
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        connector::scope(StoreOp::Head, async move {
            let path = path_of(key)?;
            let meta = self.store.head(&path).await.map_err(map_error_common)?;
            meta_to_pin(meta)
        })
        .await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let after = page.as_ref().map(|PageToken(after)| after.as_str());
        connector::scope(
            StoreOp::List,
            self.lister.list_page(prefix, after, self.page_size),
        )
        .await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        // Both resume as ListObjectsV2 `start-after`, which is exclusive, so a
        // key equal to it is skipped server-side. A present page token is
        // always past `start_after`, so it takes precedence.
        let after = match &page {
            Some(PageToken(after)) => Some(after.as_str()),
            None => start_after,
        };
        connector::scope(
            StoreOp::List,
            self.lister.list_page(prefix, after, self.page_size),
        )
        .await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        connector::scope(StoreOp::ListDelimited, self.lister.list_delimited(prefix)).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        connector::scope(StoreOp::Delete, async move {
            let path = path_of(key)?;
            match self.store.delete(&path).await.map_err(map_delete_error) {
                Ok(()) => Ok(()),
                // Idempotent per the contract: deleting a missing key succeeds.
                Err(StoreError::NotFound) => Ok(()),
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
            // True exactly when a non-`Off` `UploadIntegrity` was configured
            // (#863): that mode set `AmazonS3Builder::with_checksum_algorithm` in
            // `builder()`, so `object_store` attaches a server-verified
            // `x-amz-checksum-*` over the payload and S3 verifies-or-rejects the
            // write. `object_store` 0.14 offers no per-request hook and no way to
            // attach the caller's own precomputed CRC32C (see `UploadIntegrity`),
            // so the attached algorithm is SHA-256 / CRC64-NVME, not the
            // contract's CRC32C; combined with `put()`'s CRC32C pre-flight over
            // the same buffer this still covers the caller's bytes to the server.
            // `Off` (the default) attaches nothing and reports `false`, the
            // historical behavior for endpoints that do not honor the header. The
            // flag is not in `Capabilities::mandatory()` and gates no mode, so
            // either setting starts; read-time integrity still comes from the
            // footer/section/page crc32c hierarchy regardless
            // (docs/object-store-contract.md "Upload checksums").
            upload_checksum: self.upload_integrity.is_enabled(),
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

    /// The `Date` of the last response this store received, in unix
    /// nanoseconds, or `None` before the first one (ADR-1685 decision 1).
    ///
    /// Every S3 response carries a `Date`, including an error response, so any
    /// completed request seeds this, not only a successful one. The value is
    /// whatever the *latest* response said: never a running maximum, so an
    /// endpoint or proxy that answers one request with a wrong `Date` affects
    /// only the readings taken before the next response arrives.
    fn observed_store_time_ns(&self) -> Option<i64> {
        self.store_time.latest()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::{PutResult, UploadPart};

    use super::*;

    /// The probe for one lifecycle document, with versioning `Enabled`.
    fn probe_for_lifecycle(
        body: &[u8],
        params: &crate::conformance::BucketProtectionParams,
    ) -> crate::conformance::BucketConfigProbe {
        use bucket_config::FetchOutcome::{Present, Unknown};
        let versioning = bucket_config::parse_versioning(
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
        )
        .expect("parse");
        let lifecycle = bucket_config::parse_lifecycle(body).expect("parse");
        let (report, notes) = bucket_config::assemble_report(
            &Present(versioning),
            &Present(lifecycle),
            &Unknown("not asked".to_string()),
            &Unknown("not asked".to_string()),
            &bucket_config::RetentionSample::NotSampled,
            params,
        );
        bucket_config_probe(&report, notes)
    }

    /// An abort rule that covers `t/` but runs longer than 7 days is
    /// `NonCompliant` with the failure as its reason, and raises the abort
    /// NOTE naming that reason. With no covering noncurrent rule, that rule
    /// stays `Absent` and raises the versioning ALARM.
    #[test]
    fn bucket_config_probe_reports_an_out_of_range_abort_rule_as_non_compliant() {
        use crate::conformance::{LifecycleRuleStatus, VersioningStatus, bucket_config_alarms};
        let probe = probe_for_lifecycle(
            b"<LifecycleConfiguration><Rule><ID>ravel</ID><Status>Enabled</Status><Filter/>\
            <AbortIncompleteMultipartUpload><DaysAfterInitiation>30</DaysAfterInitiation>\
            </AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
            &crate::conformance::BucketProtectionParams::default(),
        );
        let reason = "rule \"ravel\": AbortIncompleteMultipartUpload is 30 days, more than 7";
        assert_eq!(probe.versioning, VersioningStatus::On);
        assert_eq!(
            probe.abort_incomplete_multipart_upload,
            LifecycleRuleStatus::NonCompliant(reason.to_string())
        );
        assert_eq!(
            probe.noncurrent_version_expiration,
            LifecycleRuleStatus::Absent
        );
        assert_eq!(
            probe.detail,
            "derived from the ADR-1727 bucket-protection control plane (?versioning, ?lifecycle \
             over signed read-only GETs)"
        );
        let alarms = bucket_config_alarms(&probe);
        assert_eq!(alarms.len(), 2, "{alarms:?}");
        assert_eq!(
            alarms[0],
            "ALARM: object versioning is enabled but no noncurrent-version expiration rule is \
             configured. This silently converts every Ravel delete (retention, sweep, and \
             ADR-0064 erasure) into a soft delete, inverting every deletion guarantee, and is an \
             unsupported configuration (ADR-0064 §7 point 1). Configure noncurrent-version \
             expiration plus expired-delete-marker cleanup on all t/ prefixes, or disable \
             versioning."
        );
        assert_eq!(
            alarms[1],
            format!(
                "NOTE: the REQUIRED AbortIncompleteMultipartUpload lifecycle rule (7 days or \
                 less) covers t/ but does not meet the contract: {reason} (ADR-0064 §7 point 3). \
                 Abandoned multipart uploads stay billable for longer than the contract allows. \
                 The NOTE prefix reflects the probe's limits, not an optional requirement."
            )
        );
    }

    /// A covering noncurrent rule whose `NoncurrentDays` disagrees with the
    /// expected value is `NonCompliant`, and on a versioned bucket raises the
    /// noncurrent ALARM naming the reason. The compliant abort rule beside it
    /// is `Present` and raises nothing.
    #[test]
    fn bucket_config_probe_reports_a_disagreeing_noncurrent_rule_as_non_compliant() {
        use crate::conformance::{LifecycleRuleStatus, bucket_config_alarms};
        let params = crate::conformance::BucketProtectionParams {
            expected_noncurrent_days: Some(30),
            ..Default::default()
        };
        let probe = probe_for_lifecycle(
            b"<LifecycleConfiguration><Rule><ID>ravel</ID><Status>Enabled</Status><Filter/>\
            <NoncurrentVersionExpiration><NoncurrentDays>90</NoncurrentDays>\
            </NoncurrentVersionExpiration>\
            <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation>\
            </AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
            &params,
        );
        let reason = "rule \"ravel\": NoncurrentDays is 90, expected 30";
        assert_eq!(
            probe.noncurrent_version_expiration,
            LifecycleRuleStatus::NonCompliant(reason.to_string())
        );
        assert_eq!(
            probe.abort_incomplete_multipart_upload,
            LifecycleRuleStatus::Present
        );
        assert_eq!(
            bucket_config_alarms(&probe),
            vec![format!(
                "ALARM: object versioning is enabled and a noncurrent-version expiration rule \
                 covers t/, but it does not meet the contract: {reason}. A Ravel delete \
                 (retention, sweep, and ADR-0064 erasure) then leaves prior versions recoverable \
                 for a window other than the one the deployment's deletion bounds assume, which \
                 is an unsupported configuration (ADR-0064 §7 point 1). Configure one \
                 noncurrent-version expiration rule on all t/ prefixes, or disable versioning."
            )]
        );
    }

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

    /// What the fake endpoint below recorded off real HTTP requests for one
    /// multipart upload: the checksum algorithm `CreateMultipartUpload`
    /// carried, each `UploadPart`'s checksum header value (in part order,
    /// `None` where absent), and the raw `CompleteMultipartUpload` request
    /// body.
    #[derive(Default)]
    struct MultipartCapture {
        create_checksum_algorithm: Option<String>,
        part_checksums: Vec<Option<String>>,
        complete_body: Option<String>,
    }

    #[derive(Clone)]
    struct MultipartCaptureState {
        capture: Arc<parking_lot::Mutex<MultipartCapture>>,
    }

    /// Answers the three explicit multipart requests `object_store` issues,
    /// distinguished by method and query string (S3 has no other signal: all
    /// three share one path). `UploadPart` echoes back whatever
    /// `x-amz-checksum-crc64nvme` it received as a response header, because
    /// `object_store` reads a part's checksum off the *response*, not off what
    /// it sent, to build `CompleteMultipartUpload`'s per-part checksum --- a
    /// fake that does not echo it would make every Complete-body assertion a
    /// false negative unrelated to whether the client sent anything.
    async fn multipart_capture_handler(
        axum::extract::State(state): axum::extract::State<MultipartCaptureState>,
        method: axum::http::Method,
        uri: axum::http::Uri,
        headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> axum::response::Response {
        let query = uri.query().unwrap_or("");
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };

        if method == axum::http::Method::PUT && query.contains("partNumber") {
            let part_checksum = header("x-amz-checksum-crc64nvme");
            state
                .capture
                .lock()
                .part_checksums
                .push(part_checksum.clone());
            let mut response = axum::response::Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("etag", "\"fake-part-etag\"");
            if let Some(sum) = &part_checksum {
                response = response.header("x-amz-checksum-crc64nvme", sum.as_str());
            }
            return response.body(axum::body::Body::empty()).expect("response");
        }

        if method == axum::http::Method::POST && query.contains("uploads") {
            state.capture.lock().create_checksum_algorithm = header("x-amz-checksum-algorithm");
            let xml = "<InitiateMultipartUploadResult><UploadId>fake-upload-id</UploadId>\
                       </InitiateMultipartUploadResult>";
            return axum::response::Response::builder()
                .status(axum::http::StatusCode::OK)
                .body(axum::body::Body::from(xml))
                .expect("response");
        }

        if method == axum::http::Method::POST && query.contains("uploadId") {
            state.capture.lock().complete_body = Some(String::from_utf8_lossy(&body).into_owned());
            let xml = "<CompleteMultipartUploadResult><ETag>\"fake-complete-etag\"</ETag>\
                       </CompleteMultipartUploadResult>";
            return axum::response::Response::builder()
                .status(axum::http::StatusCode::OK)
                .body(axum::body::Body::from(xml))
                .expect("response");
        }

        // AbortMultipartUpload (DELETE) or anything unrecognized: succeed
        // emptily, since neither test below drives an abort.
        axum::response::Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(axum::body::Body::empty())
            .expect("response")
    }

    /// Stand up the fake multipart endpoint, returning its base URL and the
    /// capture handle.
    async fn spawn_multipart_capture() -> (String, Arc<parking_lot::Mutex<MultipartCapture>>) {
        use axum::Router;
        use axum::routing::any;

        let capture = Arc::new(parking_lot::Mutex::new(MultipartCapture::default()));
        let state = MultipartCaptureState {
            capture: Arc::clone(&capture),
        };
        let app = Router::new()
            .route("/", any(multipart_capture_handler))
            .route("/{*rest}", any(multipart_capture_handler))
            .layer(axum::extract::DefaultBodyLimit::disable())
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (endpoint, capture)
    }

    /// With upload integrity on, the explicit `put_multipart` path sends a
    /// server-verified checksum on every part request, not only the first ---
    /// the wrong implementation this rules out is a harness assertion that
    /// only checks the first part, which would pass even if `object_store`
    /// sent a checksum on part 1 and silently dropped it afterward (e.g. by
    /// reusing a stale request builder). It also records whether
    /// `CompleteMultipartUpload`'s body carries a per-part checksum, which
    /// `object_store` only includes when `UploadPart`'s response echoes the
    /// checksum back (see `multipart_capture_handler`): with the echo in
    /// place, it does.
    #[tokio::test(flavor = "multi_thread")]
    async fn multipart_parts_carry_checksums_under_integrity() {
        let (endpoint, capture) = spawn_multipart_capture().await;
        let store = S3Store::with_http_config(
            S3Config {
                endpoint: Some(endpoint),
                ..test_config()
            },
            S3HttpConfig {
                upload_integrity: UploadIntegrity::Crc64Nvme,
                ..S3HttpConfig::default()
            },
        )
        .expect("store builds with integrity on");

        let mut upload = store
            .put_multipart("checksum-key")
            .await
            .expect("initiate multipart");
        let full_part = Bytes::from(vec![0u8; crate::MULTIPART_MIN_PART_SIZE]);
        upload
            .put_part(full_part.clone(), None)
            .await
            .expect("part 1");
        upload.put_part(full_part, None).await.expect("part 2");
        upload
            .put_part(Bytes::from_static(b"final small part"), None)
            .await
            .expect("part 3 (final, under the minimum)");
        upload.complete().await.expect("complete");

        let captured = capture.lock();
        assert_eq!(
            captured.create_checksum_algorithm.as_deref(),
            Some("CRC64NVME"),
            "CreateMultipartUpload must carry the checksum algorithm under integrity"
        );
        assert_eq!(
            captured.part_checksums.len(),
            3,
            "all 3 parts must have reached the fake endpoint"
        );
        for (index, checksum) in captured.part_checksums.iter().enumerate() {
            assert!(
                checksum.is_some(),
                "part {index} (0-based) must carry x-amz-checksum-crc64nvme, not only part 0"
            );
        }
        // Two parts carry the same bytes and the third different bytes, so
        // exactly two distinct digests arrive whatever order the parts landed
        // in. One digest resent on every part (a stale request builder) gives
        // one.
        let distinct: std::collections::BTreeSet<&Option<String>> =
            captured.part_checksums.iter().collect();
        assert_eq!(
            distinct.len(),
            2,
            "each part's checksum must be computed over that part's own bytes: {:?}",
            captured.part_checksums
        );
        assert!(
            captured
                .complete_body
                .as_deref()
                .expect("complete body captured")
                .contains("ChecksumCRC64NVME"),
            "CompleteMultipartUpload must carry a per-part checksum when UploadPart's \
             response echoed one back, got: {:?}",
            captured.complete_body
        );
    }

    /// With upload integrity off (the default), the explicit `put_multipart`
    /// path sends no checksum header on any request: the client-wide
    /// `object_store` checksum algorithm is simply unset.
    #[tokio::test(flavor = "multi_thread")]
    async fn multipart_parts_carry_no_checksums_with_integrity_off() {
        let (endpoint, capture) = spawn_multipart_capture().await;
        let store = S3Store::with_http_config(
            S3Config {
                endpoint: Some(endpoint),
                ..test_config()
            },
            S3HttpConfig::default(),
        )
        .expect("store builds with default (off) integrity");

        let mut upload = store
            .put_multipart("no-checksum-key")
            .await
            .expect("initiate multipart");
        let full_part = Bytes::from(vec![0u8; crate::MULTIPART_MIN_PART_SIZE]);
        upload
            .put_part(full_part.clone(), None)
            .await
            .expect("part 1");
        upload.put_part(full_part, None).await.expect("part 2");
        upload
            .put_part(Bytes::from_static(b"final small part"), None)
            .await
            .expect("part 3 (final, under the minimum)");
        upload.complete().await.expect("complete");

        let captured = capture.lock();
        assert_eq!(
            captured.create_checksum_algorithm, None,
            "CreateMultipartUpload must carry no checksum algorithm with integrity off"
        );
        assert_eq!(captured.part_checksums.len(), 3);
        for (index, checksum) in captured.part_checksums.iter().enumerate() {
            assert!(
                checksum.is_none(),
                "part {index} (0-based) must carry no checksum header with integrity off"
            );
        }
    }

    /// Issue #1911, at the one decision every binary routes through. An
    /// endpoint written with no scheme (`rustfs:9000`) was accepted here and
    /// killed the process later, inside `object_store`'s request signing, on a
    /// message naming neither the endpoint nor the flag. It is refused here
    /// now, and the refusal quotes the endpoint and asks for a scheme.
    ///
    /// The cases are chosen to fail two wrong implementations:
    ///
    /// - a substring test for `"http"` rather than a prefix match accepts
    ///   `my-http-proxy:9000` (schemeless, and its host merely contains the
    ///   word) and refuses `HTTPS://rustfs:9000` (a perfectly good URL);
    /// - a refusal written in one binary's own startup path instead of here
    ///   leaves this test failing outright, which is what makes the other
    ///   binaries' tables meaningful rather than three copies of one rule.
    ///
    /// The `http://` rows are the #1707 behaviour, asserted here so the new
    /// scheme check cannot regress them.
    #[test]
    fn an_endpoint_without_a_scheme_is_refused() {
        for endpoint in [
            "rustfs:9000",
            // WRONG-1: a substring test for "http" passes this one, whose host
            // name merely contains the word and which still has no scheme.
            "my-http-proxy:9000",
            "s3.example.com",
            // A scheme that is neither of the two, and the empty endpoint: both
            // are as unusable as a bare host:port.
            "ftp://rustfs:9000",
            "",
        ] {
            let refusal = resolve_s3_allow_http(Some(endpoint), false).expect_err(
                "an endpoint beginning with neither https:// nor http:// must be refused",
            );
            assert_eq!(
                refusal,
                S3EndpointRefusal::Schemeless(SchemelessS3Endpoint {
                    endpoint: endpoint.to_string(),
                }),
                "{endpoint} must be refused as schemeless, not as plaintext"
            );
            assert_eq!(refusal.endpoint(), endpoint);
            let rendered = refusal.to_string();
            assert!(
                rendered.contains(&format!("'{endpoint}'")),
                "the refusal must quote the endpoint verbatim, got: {rendered}"
            );
            assert!(
                rendered.contains("https://") && rendered.contains("http://"),
                "the refusal must state the fix (write a scheme), got: {rendered}"
            );
            // The flag accepts deliberate plaintext, never a missing scheme:
            // there is no usable URL to accept.
            assert!(
                resolve_s3_allow_http(Some(endpoint), true).is_err(),
                "{endpoint} must stay refused even with --s3-allow-http"
            );
        }

        // WRONG-1, the other half: an upper-case scheme is a scheme. RFC 3986
        // section 3.1 makes it case-insensitive, and refusing it would break a
        // working deployment.
        for endpoint in ["HTTPS://rustfs:9000", "Https://rustfs:9000"] {
            assert_eq!(
                resolve_s3_allow_http(Some(endpoint), false),
                Ok(false),
                "{endpoint} carries a scheme and must be accepted without plaintext"
            );
        }

        // Unchanged from #1707: https and real AWS accept, loopback plaintext
        // accepts unflagged, and plaintext to a host on the network is refused
        // as plaintext rather than as schemeless.
        assert_eq!(
            resolve_s3_allow_http(Some("https://s3.us-east-1.amazonaws.com"), false),
            Ok(false)
        );
        assert_eq!(resolve_s3_allow_http(None, false), Ok(false));
        for endpoint in [
            "http://127.0.0.1:9000",
            "http://localhost:9000",
            "http://[::1]:9000",
            "HTTP://localhost:9000",
        ] {
            assert_eq!(
                resolve_s3_allow_http(Some(endpoint), false),
                Ok(true),
                "{endpoint} is loopback plaintext and must enable allow_http unflagged"
            );
        }
        assert_eq!(
            resolve_s3_allow_http(Some("http://rustfs:9000"), false),
            Err(S3EndpointRefusal::Plaintext(PlaintextS3Endpoint {
                endpoint: "http://rustfs:9000".to_string(),
            })),
            "plaintext to a host on the network must keep its own refusal"
        );
        assert_eq!(
            resolve_s3_allow_http(Some("http://rustfs:9000"), true),
            Ok(true),
            "--s3-allow-http must still accept deliberate plaintext"
        );
    }

    /// `is_loopback_endpoint` is `resolve_s3_allow_http`'s own loopback
    /// branch, exposed standalone for a caller (the server's loopback
    /// fetch-cache share, ADR-2023) that has no `allow_http_flag` and must
    /// not refuse anything, only classify.
    #[test]
    fn is_loopback_endpoint_matches_the_shared_authority_predicate() {
        for endpoint in [
            "http://127.0.0.1:9000",
            "http://localhost:9000",
            "HTTP://LOCALHOST:9000",
            "http://[::1]:9000",
            "https://127.0.0.1:9000",
            // The whole 127.0.0.0/8 block is loopback, not just 127.0.0.1.
            "http://127.9.9.9:9000",
        ] {
            assert!(
                is_loopback_endpoint(endpoint),
                "{endpoint} must be classified as loopback"
            );
        }

        for endpoint in [
            "http://s3.example.com",
            // A host name that merely CONTAINS a loopback literal as a label
            // is not loopback: it resolves on the network, wherever that
            // resolution lands.
            "http://127.0.0.1.example.com:9000",
            // The authority ends at '?': matching to the first '@' or the end
            // of the string would read "localhost" here as the host.
            "http://s3.example.com?x=@localhost",
            // Schemeless: not a usable URL, so not loopback either.
            "127.0.0.1:9000",
            "https://s3.us-east-1.amazonaws.com",
        ] {
            assert!(
                !is_loopback_endpoint(endpoint),
                "{endpoint} must not be classified as loopback"
            );
        }
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

    /// A versioned bucket reports a version id, and it is the pin's selector.
    /// Reading it off `ObjectMeta::version` instead would read the ETag back,
    /// since that field is the compare-and-swap token on S3, and a pinned read
    /// would then send the ETag as a `versionId` and fail on an object that is
    /// still there.
    #[test]
    fn a_reported_version_id_becomes_the_pins_selector() {
        let (meta, pin) = meta_to_pin(object_store::ObjectMeta {
            location: Path::from("t/acme/seg/0001"),
            last_modified: Default::default(),
            size: 17,
            e_tag: Some("\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string()),
            version: Some("3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY".to_string()),
        })
        .expect("an object with an ETag maps");

        assert_eq!(pin.etag, "\"9a0364b9e99bb480dd25e1f0284c8555\"");
        assert_eq!(
            pin.version.as_deref(),
            Some("3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"),
            "the version id S3 reported is the pin's selector"
        );
        // The CAS token is unchanged: it is the ETag, and a conditional write
        // still compares it.
        assert_eq!(meta.version.0, "\"9a0364b9e99bb480dd25e1f0284c8555\"");
        assert_eq!(meta.etag.0, "\"9a0364b9e99bb480dd25e1f0284c8555\"");
    }

    /// An unversioned bucket reports no version id. The pin carries the ETag
    /// alone, so a read through it is a plain `If-Match` and never asks for a
    /// version the bucket cannot serve.
    #[test]
    fn an_unversioned_bucket_yields_a_pin_with_no_version() {
        let (meta, pin) = meta_to_pin(object_store::ObjectMeta {
            location: Path::from("t/acme/seg/0001"),
            last_modified: Default::default(),
            size: 17,
            e_tag: Some("\"abc\"".to_string()),
            version: None,
        })
        .expect("an object with an ETag maps");

        assert_eq!(pin.etag, "\"abc\"");
        assert_eq!(pin.version, None);
        assert_eq!(meta.version.0, "\"abc\"");
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

        // The historical build path, reproduced verbatim without any KMS knob,
        // plus the #851 client options every build now installs (in the same
        // position `builder` sets them, before the per-knob client setters).
        let mut baseline = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_client_options(client_options(&S3HttpConfig::default()))
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
                S3Store::builder(&config, &S3HttpConfig::default())
                    .expect("no credentials file")
                    .0
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
                S3Store::builder(&with_key, &S3HttpConfig::default())
                    .expect("no credentials file")
                    .0
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
                S3Store::builder(&with_token, &S3HttpConfig::default())
                    .expect("no credentials file")
                    .0
            ),
            format!(
                "{:?}",
                S3Store::builder(&baseline, &S3HttpConfig::default())
                    .expect("no credentials file")
                    .0
            ),
            "a session token must change the builder"
        );
    }

    /// Every HTTP-client value #851 sets is installed on the builder `.build()`
    /// consumes, and matches what [`client_options`] produced. The builder's
    /// `get_config_value` is the observable seam: the built `AmazonS3` client
    /// exposes no config readback, so a refactor that dropped
    /// `.with_client_options(..)` in [`S3Store::builder`] would make these read
    /// back `object_store`'s inherited defaults and fail here rather than
    /// silently reverting the timeouts. Flip: delete the `.with_client_options`
    /// call in `builder` and the `Timeout` case (and the inherited-vs-configured
    /// assertion below) fail, the latter because the builder then reads the
    /// inherited "30s".
    #[test]
    fn http_client_options_reach_the_builder() {
        use object_store::ClientConfigKey;
        use object_store::aws::AmazonS3ConfigKey;

        let http = S3HttpConfig::default();
        let reference = client_options(&http);
        let builder = S3Store::builder(&test_config(), &http)
            .expect("no credentials file")
            .0;

        for key in [
            ClientConfigKey::Timeout,
            ClientConfigKey::ConnectTimeout,
            ClientConfigKey::PoolIdleTimeout,
            ClientConfigKey::Http2KeepAliveInterval,
            ClientConfigKey::Http2KeepAliveTimeout,
            ClientConfigKey::Http2KeepAliveWhileIdle,
        ] {
            assert_eq!(
                builder.get_config_value(&AmazonS3ConfigKey::Client(key)),
                reference.get_config_value(&key),
                "builder must carry the configured {key:?}"
            );
        }

        // The request timeout specifically must be the deliberate value, not
        // object_store's inherited 30 s default -- the exact hole #851 closes.
        let inherited = ClientOptions::default().get_config_value(&ClientConfigKey::Timeout);
        let configured =
            builder.get_config_value(&AmazonS3ConfigKey::Client(ClientConfigKey::Timeout));
        assert_ne!(
            configured, inherited,
            "the request timeout must be deliberately set, not inherited (30s)"
        );
    }

    /// A non-default value set through the config mechanism ([`S3HttpConfig`],
    /// applied via [`S3Store::with_http_config`]/[`S3Store::builder`]) reaches
    /// the client, so the values are configurable and not just hard-coded.
    #[test]
    fn non_default_http_config_reaches_the_client() {
        use object_store::ClientConfigKey;
        use object_store::aws::AmazonS3ConfigKey;

        let http = S3HttpConfig {
            // Not the 20 s default, nor object_store's 30 s inherited default.
            request_timeout: Duration::from_secs(7),
            ..S3HttpConfig::default()
        };
        let builder = S3Store::builder(&test_config(), &http)
            .expect("no credentials file")
            .0;
        let got = builder.get_config_value(&AmazonS3ConfigKey::Client(ClientConfigKey::Timeout));
        assert_eq!(
            got,
            client_options(&http).get_config_value(&ClientConfigKey::Timeout),
            "a non-default request timeout set through config must reach the client"
        );
        assert_ne!(
            got,
            client_options(&S3HttpConfig::default()).get_config_value(&ClientConfigKey::Timeout),
            "the configured value must differ from the default, proving the path is live"
        );
        // The full construction path (build() included) accepts the override.
        S3Store::with_http_config(test_config(), http)
            .expect("a non-default http config must build");
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
                let err = S3Store::builder(&config, &S3HttpConfig::default())
                    .expect_err(&format!("InstanceRole + {label} must be rejected"));
                assert!(
                    matches!(err, StoreError::Permanent(_)),
                    "{label}: got {err:?}"
                );
            }

            // All-absent against the same working endpoint builds cleanly: the
            // rejections above are the mix guard, not a blanket refusal.
            S3Store::builder(&clean, &S3HttpConfig::default())
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
                S3Store::builder(&instance_role, &S3HttpConfig::default())
                    .expect("instance-role builder must construct against the mock")
                    .0
            );
            // Every non-credential setter the InstanceRole path applies (the
            // #851 client options included), with no key setters and no
            // provider: the one difference must be the installed credential
            // provider.
            let mut baseline = AmazonS3Builder::new()
                .with_bucket_name(&baseline_config.bucket)
                .with_region(&baseline_config.region)
                .with_client_options(client_options(&S3HttpConfig::default()))
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
        let (_, provider, _) =
            S3Store::builder(&config, &S3HttpConfig::default()).expect("valid credentials file");
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
    // These pin the StoreError kind AND retryable() that the S3
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
    /// `Timeout`: the throttle branch is checked before the timeout
    /// heuristic, which also skips the `retry_timeout` field. Uses the exact
    /// wrapper format object_store emits so this pins the real interaction,
    /// not a hand-written approximation.
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
    /// (`timed out`/`deadline`/`timeout` outside the `retry_timeout` field)
    /// still wins.
    #[test]
    fn genuine_timeout_still_classifies_as_timeout() {
        for text in [
            "request timed out, after 10 retries, max_retries: 10, \
             retry_timeout: 180000ms ",
            "Error performing GET http://h/b/k in 1s, after 1 retries, max_retries: 1, \
             retry_timeout: 30s  - Server returned non-2xx status code: 400 Bad Request: \
             <Error><Code>RequestTimeout</Code></Error>",
        ] {
            let mapped = map_error_common(generic(TextError(text)));
            assert!(
                matches!(mapped, StoreError::Timeout),
                "a genuine timeout without throttle language must stay Timeout, \
                 got {mapped:?} for {text:?}"
            );
        }
    }

    /// A 429 or 503 elsewhere in `object_store`'s `RetryError` text (a port, a
    /// key, a query, the elapsed time, a request id in the body) is not a
    /// throttle: only the status's reason phrase is. Each text is the exact shape
    /// `RetryError` renders: `"Error performing {method} {uri} in {elapsed:?}"`,
    /// the exhausted-retry suffix when there were retries, then
    /// `" - Server returned non-2xx status code: {status}: {body}"`.
    #[test]
    fn digits_outside_the_status_segment_are_not_a_throttle() {
        let transient = [
            "Error performing GET http://127.0.0.1:50321/b/k in 1.503ms - Server \
             returned non-2xx status code: 400 Bad Request: \
             <Error><Code>InvalidArgument</Code><RequestId>4291503</RequestId></Error>",
            "Error performing GET http://127.0.0.1:42900/b/k503 in 2.1ms - Server \
             returned non-2xx status code: 400 Bad Request: ",
            "Error performing GET http://127.0.0.1:9000/b?list-type=2&prefix=p%2F429%2F \
             in 3.429ms - Server returned non-2xx status code: 404 Not Found: ",
        ];
        for text in transient {
            let mapped = map_error_common(generic(TextError(text)));
            assert!(
                matches!(mapped, StoreError::Transient(_)),
                "a non-throttle status must not read Throttled from incidental \
                 digits, got {mapped:?} for {text:?}"
            );
        }

        // An exhausted 500 is Transient: neither the 503/429 digits nor the
        // suffix's `retry_timeout` field name is a class signal.
        let exhausted = "Error performing GET http://127.0.0.1:5030/b/k in 180.0503s, \
                         after 10 retries, max_retries: 10, retry_timeout: 180s  - Server \
                         returned non-2xx status code: 500 Internal Server Error: \
                         request id 503429";
        let mapped = map_error_common(generic(TextError(exhausted)));
        assert!(
            matches!(mapped, StoreError::Transient(_)),
            "an exhausted 500 must read Transient, not Throttled from incidental \
             digits or Timeout from `retry_timeout`, got {mapped:?}"
        );

        // No typed HttpError in the chain, so this is the text heuristic's
        // timeout, and the 503/429 digits in the port and elapsed time must not
        // turn it into a throttle.
        let timeout = "Error performing GET http://127.0.0.1:50329/b/k429 in 30.000503s \
                       - HTTP error: error sending request: operation timed out";
        let mapped = map_error_common(generic(TextError(timeout)));
        assert!(
            matches!(mapped, StoreError::Timeout),
            "a timeout whose URL and elapsed time carry 503/429 must stay Timeout, \
             got {mapped:?}"
        );
    }

    /// A real 429 or 503 status reads Throttled whatever other digits the text
    /// carries, with and without the exhausted-retry suffix.
    #[test]
    fn a_429_or_503_status_reads_throttled() {
        for text in [
            "Error performing GET http://127.0.0.1:5030/b/k in 1.4ms - Server \
             returned non-2xx status code: 429 Too Many Requests: ",
            "Error performing GET http://127.0.0.1:4290/b/k in 1.4s, after 10 retries, \
             max_retries: 10, retry_timeout: 180s  - Server returned non-2xx status \
             code: 503 Service Unavailable: <Error><Code>ServiceUnavailable</Code></Error>",
        ] {
            let mapped = map_error_common(generic(TextError(text)));
            assert!(
                matches!(
                    mapped,
                    StoreError::Throttled {
                        retry_after_ms: 1000
                    }
                ),
                "got {mapped:?} for {text:?}"
            );
        }
    }

    #[derive(Debug)]
    struct OwnedTextError(String);

    impl std::fmt::Display for OwnedTextError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for OwnedTextError {}

    /// An error that renders `"{prefix}{source}"` and exposes `source`, the
    /// shape of `RetryError` (prefix ending `" - "`), and of a wrapper naming
    /// a raw path (`"Error performing get request {path}: "`). object_store
    /// 0.14 dissolves its path-naming wrappers before they reach `Generic`, so
    /// the wrapper cases are defensive: they pin that such text, if it ever
    /// appears, still cannot pick the class.
    #[derive(Debug)]
    struct Wrapping {
        prefix: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    }

    impl std::fmt::Display for Wrapping {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}{}", self.prefix, self.source)
        }
    }

    impl std::error::Error for Wrapping {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.source.as_ref())
        }
    }

    const CLASS_WORDS: [&str; 4] = ["timeout", "deadline", "throttled", "slowdown"];

    /// An exhausted 500 and a 400 whose bucket, endpoint host or key carries a
    /// timeout or throttle word read Transient: only the inner `RequestError`
    /// text is a class signal, never the request URI. Covers the plain text,
    /// the text behind a wrapper that names the raw key (which can hold a
    /// space, so "slow down" too), the same pair as a source chain, and a
    /// per-key `DeleteObjects` refusal naming the key.
    #[test]
    fn class_words_in_the_request_uri_are_not_a_class() {
        let inners = [
            ", after 10 retries, max_retries: 10, retry_timeout: 180s  - Server returned \
             non-2xx status code: 500 Internal Server Error: "
                .to_string(),
            " - Server returned non-2xx status code: 400 Bad Request: \
             <Error><Code>InvalidArgument</Code></Error>"
                .to_string(),
        ];
        let mut texts = Vec::new();
        for word in CLASS_WORDS {
            for uri in [
                format!("http://127.0.0.1:9000/{word}-bucket/k"),
                format!("http://{word}.s3.example.com/b/k"),
                format!("http://127.0.0.1:9000/b/{word}/k"),
            ] {
                for inner in &inners {
                    texts.push(format!("Error performing GET {uri} in 1.2ms{inner}"));
                }
            }
        }
        for word in CLASS_WORDS.iter().chain(&["slow down"]) {
            for inner in &inners {
                texts.push(format!(
                    "Error performing get request {word}/k: Error performing GET \
                     http://127.0.0.1:9000/b/k in 1.2ms{inner}"
                ));
            }
        }
        for word in CLASS_WORDS.iter().chain(&["slow down"]) {
            texts.push(format!(
                "DeleteObjects request failed for key {word}/k: We encountered an \
                 internal error. (code: InternalError)"
            ));
        }
        for text in texts {
            let mapped = map_error_common(generic(OwnedTextError(text.clone())));
            assert!(
                matches!(mapped, StoreError::Transient(_)),
                "a class word in the URI or key must not pick the class, got \
                 {mapped:?} for {text:?}"
            );
        }

        for word in CLASS_WORDS.iter().chain(&["slow down"]) {
            for (status, retries) in [
                (
                    "500 Internal Server Error",
                    ", after 10 retries, max_retries: 10, retry_timeout: 180s ",
                ),
                ("400 Bad Request", ""),
            ] {
                let retry = Wrapping {
                    prefix: format!(
                        "Error performing GET http://{word}.example.com/{word}/{word} \
                         in 1.2ms{retries} - "
                    ),
                    source: Box::new(OwnedTextError(format!(
                        "Server returned non-2xx status code: {status}: "
                    ))),
                };
                let wrapped = Wrapping {
                    prefix: format!("Error performing get request {word}/k: "),
                    source: Box::new(retry),
                };
                let mapped = map_error_common(generic(wrapped));
                assert!(
                    matches!(mapped, StoreError::Transient(_)),
                    "a class word in the chain's URI or key must not pick the class, \
                     got {mapped:?} for {word:?} {status}"
                );
            }
        }
    }

    /// A raw key in a wrapper's text that itself looks like a `RetryError`
    /// prefix is not mistaken for one, whether the source chain is reachable
    /// (the inner text comes from the chain) or only the text is (the last
    /// matching segment is taken, not the first).
    #[test]
    fn a_key_shaped_like_a_retry_prefix_is_not_the_inner_text() {
        let text_only = OwnedTextError(
            "Error performing get request Error performing GET x in 1ms - timeout/k: \
             Error performing GET http://127.0.0.1:9000/b/k in 1.2ms - Server returned \
             non-2xx status code: 400 Bad Request: "
                .to_string(),
        );
        let mapped = map_error_common(generic(text_only));
        assert!(matches!(mapped, StoreError::Transient(_)), "got {mapped:?}");

        let wrapped = Wrapping {
            prefix: "Error performing get request Error performing GET x in 1ms - timeout/k: "
                .to_string(),
            source: Box::new(Wrapping {
                prefix: "Error performing GET http://127.0.0.1:9000/b/k in 1.2ms - ".to_string(),
                source: Box::new(OwnedTextError(
                    "Server returned non-2xx status code: 400 Bad Request: ".to_string(),
                )),
            }),
        };
        let mapped = map_error_common(generic(wrapped));
        assert!(matches!(mapped, StoreError::Transient(_)), "got {mapped:?}");
    }

    /// The genuine signals still classify through the URI split: the 429 and
    /// 503 reason phrases, S3 `SlowDown` and `RequestTimeout` codes in the body,
    /// a 504 or 408 status, a transport "operation timed out" or "deadline has
    /// elapsed", and a 2xx error response body (`RequestError::Response`), each
    /// behind a URI that carries no class word, as text and as a chain.
    #[test]
    fn genuine_signals_behind_the_request_uri_keep_their_class() {
        let throttled = [
            "Server returned non-2xx status code: 429 Too Many Requests: ",
            "Server returned non-2xx status code: 503 Service Unavailable: ",
            "Server returned non-2xx status code: 500 Internal Server Error: \
             <Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>",
            "Server returned error response: <Error><Code>SlowDown</Code></Error>",
        ];
        let timeout = [
            "Server returned non-2xx status code: 400 Bad Request: \
             <Error><Code>RequestTimeout</Code></Error>",
            "Server returned non-2xx status code: 504 Gateway Timeout: ",
            "Server returned non-2xx status code: 408 Request Timeout: ",
            "HTTP error: error sending request: operation timed out",
            "HTTP error: deadline has elapsed",
            "Server returned error response: <Error><Code>RequestTimeout</Code></Error>",
        ];
        let prefix = "Error performing PUT http://127.0.0.1:9000/b/k in 30.1s, after 10 \
                      retries, max_retries: 10, retry_timeout: 180s  - ";
        for (inner, want_throttled) in throttled
            .iter()
            .map(|t| (*t, true))
            .chain(timeout.iter().map(|t| (*t, false)))
        {
            let as_text = OwnedTextError(format!("{prefix}{inner}"));
            let as_chain = Wrapping {
                prefix: "Error performing list request: ".to_string(),
                source: Box::new(Wrapping {
                    prefix: prefix.to_string(),
                    source: Box::new(OwnedTextError(inner.to_string())),
                }),
            };
            for mapped in [
                map_error_common(generic(as_text)),
                map_error_common(generic(as_chain)),
            ] {
                if want_throttled {
                    assert!(
                        matches!(
                            mapped,
                            StoreError::Throttled {
                                retry_after_ms: 1000
                            }
                        ),
                        "got {mapped:?} for {inner:?}"
                    );
                } else {
                    assert!(
                        matches!(mapped, StoreError::Timeout),
                        "got {mapped:?} for {inner:?}"
                    );
                }
            }
        }
    }

    /// An exhausted 500 GET whose bucket or key carries both "range" and
    /// "satisfiable" reads Transient through `map_get_error`, as text and as a
    /// source chain: only the inner `RequestError` text is a range signal.
    #[test]
    fn range_words_in_the_request_uri_are_not_invalid_range() {
        let retries = ", after 10 retries, max_retries: 10, retry_timeout: 180s ";
        let inner = "Server returned non-2xx status code: 500 Internal Server Error: ";
        for uri in [
            "http://127.0.0.1:9000/range-not-satisfiable/k",
            "http://127.0.0.1:9000/b/range/not/satisfiable",
        ] {
            let as_text = OwnedTextError(format!(
                "Error performing GET {uri} in 1.2ms{retries} - {inner}"
            ));
            let as_chain = Wrapping {
                prefix: format!("Error performing GET {uri} in 1.2ms{retries} - "),
                source: Box::new(OwnedTextError(inner.to_string())),
            };
            for mapped in [
                map_get_error(generic(as_text)),
                map_get_error(generic(as_chain)),
            ] {
                assert!(
                    matches!(mapped, StoreError::Transient(_)),
                    "range words in the URI must not read InvalidRange, got {mapped:?} \
                     for {uri:?}"
                );
                assert!(mapped.is_retryable(), "{uri:?} must be retryable");
            }
        }
    }

    /// A body that echoes a key carrying class words reads Transient when its
    /// S3 error code is not a class, while a class code, or a 429/503/504/408
    /// status line, still classifies whatever the echoed key says.
    #[test]
    fn class_words_echoed_in_an_error_body_are_not_a_class() {
        let prefix = "Error performing GET http://127.0.0.1:9000/b/k in 1.2ms - ";
        let echo = |status: &str, code: &str| {
            format!(
                "Server returned non-2xx status code: {status}: <Error><Code>{code}</Code>\
                 <Message>m</Message><Key>slowdown/timeout/throttled/deadline</Key>\
                 <Resource>/b/slow down/timed out</Resource></Error>"
            )
        };
        let cases = [
            (echo("400 Bad Request", "InvalidArgument"), "transient"),
            (
                echo("500 Internal Server Error", "InternalError"),
                "transient",
            ),
            (
                "Server returned non-2xx status code: 400 Bad Request: <Error>\
                 <Code>InvalidArgument</Code><Key>timeout/deadline/timed out</Key></Error>"
                    .to_string(),
                "transient",
            ),
            (echo("503 Service Unavailable", "SlowDown"), "throttled"),
            (
                echo("503 Service Unavailable", "ServiceUnavailable"),
                "throttled",
            ),
            (
                echo("429 Too Many Requests", "InvalidArgument"),
                "throttled",
            ),
            (echo("400 Bad Request", "Throttling"), "throttled"),
            (echo("400 Bad Request", "ThrottlingException"), "throttled"),
            (
                echo("503 Service Unavailable", "RequestLimitExceeded"),
                "throttled",
            ),
            (echo("400 Bad Request", "TooManyRequests"), "throttled"),
            (echo("400 Bad Request", "RequestTimeout"), "timeout"),
            (echo("504 Gateway Timeout", "InvalidArgument"), "timeout"),
            (echo("408 Request Timeout", "InvalidArgument"), "timeout"),
        ];
        for (inner, want) in cases {
            let as_text = OwnedTextError(format!("{prefix}{inner}"));
            let as_chain = Wrapping {
                prefix: prefix.to_string(),
                source: Box::new(OwnedTextError(inner.clone())),
            };
            for mapped in [
                map_error_common(generic(as_text)),
                map_error_common(generic(as_chain)),
            ] {
                let got = match mapped {
                    StoreError::Transient(_) => "transient",
                    StoreError::Throttled {
                        retry_after_ms: 1000,
                    } => "throttled",
                    StoreError::Timeout => "timeout",
                    _ => "other",
                };
                assert_eq!(got, want, "got {mapped:?} for {inner:?}");
            }
        }
    }

    /// An S3 error body echoes the key in `Key` and `Resource`, so with a
    /// `Code` present a range class comes only from the code or the status
    /// line: an exhausted 500 or 503 `InternalError` on a key spelled with the
    /// range words reads Transient or Throttled by its status, while a 416 or
    /// an `InvalidRange` code still reads `InvalidRange`.
    #[test]
    fn range_words_echoed_in_an_error_body_are_not_invalid_range() {
        let prefix = "Error performing GET http://127.0.0.1:9000/b/k in 1.2ms, after 10 \
                      retries, max_retries: 10, retry_timeout: 180s  - ";
        let echo = |status: &str, code: &str| {
            format!(
                "Server returned non-2xx status code: {status}: <Error><Code>{code}</Code>\
                 <Message>m</Message><Key>range/not/satisfiable/too large</Key>\
                 <BucketName>b</BucketName>\
                 <Resource>/b/range/not/satisfiable/too large</Resource></Error>"
            )
        };
        let cases = [
            (
                echo("500 Internal Server Error", "InternalError"),
                "transient",
            ),
            (
                echo("503 Service Unavailable", "InternalError"),
                "throttled",
            ),
            (echo("416 Range Not Satisfiable", "InvalidRange"), "range"),
            (echo("416 Range Not Satisfiable", "InternalError"), "range"),
            (echo("400 Bad Request", "InvalidRange"), "range"),
        ];
        for (inner, want) in cases {
            let as_text = OwnedTextError(format!("{prefix}{inner}"));
            let as_chain = Wrapping {
                prefix: prefix.to_string(),
                source: Box::new(OwnedTextError(inner.clone())),
            };
            for mapped in [
                map_get_error(generic(as_text)),
                map_get_error(generic(as_chain)),
            ] {
                let got = match mapped {
                    StoreError::InvalidRange(_) => "range",
                    StoreError::Transient(_) => "transient",
                    StoreError::Throttled {
                        retry_after_ms: 1000,
                    } => "throttled",
                    _ => "other",
                };
                assert_eq!(got, want, "got {mapped:?} for {inner:?}");
            }
        }
    }

    /// MinIO's `SlowDownRead` and `SlowDownWrite` arrive on the error-response
    /// path, whose text has no status line, so only the code can read
    /// Throttled.
    #[test]
    fn slowdown_variants_on_the_error_response_path_are_throttled() {
        let prefix = "Error performing PUT http://127.0.0.1:9000/b/k in 1.2ms - ";
        for code in ["SlowDownWrite", "SlowDownRead"] {
            let inner = format!(
                "Server returned error response: <Error><Code>{code}</Code>\
                 <Message>Resource requested is unwritable, please reduce your request rate\
                 </Message><Key>k</Key></Error>"
            );
            let mapped = map_error_common(generic(OwnedTextError(format!("{prefix}{inner}"))));
            assert!(
                matches!(
                    mapped,
                    StoreError::Throttled {
                        retry_after_ms: 1000
                    }
                ),
                "got {mapped:?} for {code}"
            );
        }
    }

    /// Every class code, at a status line that carries no class signal of its
    /// own (a 400, and the error-response path with no status at all), so the
    /// code alone decides and dropping one from the list fails here.
    #[test]
    fn every_class_code_classifies_without_a_status_signal() {
        let prefix = "Error performing GET http://127.0.0.1:9000/b/k in 1.2ms - ";
        let cases = [
            ("SlowDown", "throttled"),
            ("Throttling", "throttled"),
            ("ThrottlingException", "throttled"),
            ("RequestLimitExceeded", "throttled"),
            ("TooManyRequests", "throttled"),
            ("RequestThrottled", "throttled"),
            ("RequestTimeout", "timeout"),
        ];
        for (code, want) in cases {
            for status_line in [
                "Server returned non-2xx status code: 400 Bad Request: ",
                "Server returned error response: ",
            ] {
                let inner = format!("{status_line}<Error><Code>{code}</Code><Key>k</Key></Error>");
                let mapped = map_error_common(generic(OwnedTextError(format!("{prefix}{inner}"))));
                let got = match mapped {
                    StoreError::Throttled {
                        retry_after_ms: 1000,
                    } => "throttled",
                    StoreError::Timeout => "timeout",
                    _ => "other",
                };
                assert_eq!(got, want, "got {mapped:?} for {inner:?}");
            }
        }
    }

    /// The throttle check runs before the timeout check: a 503 whose code is
    /// `RequestTimeout` reads Throttled.
    #[test]
    fn a_throttle_status_line_wins_over_a_timeout_code() {
        let text = "Error performing GET http://127.0.0.1:9000/b/k in 1.2ms - Server returned \
                    non-2xx status code: 503 Service Unavailable: \
                    <Error><Code>RequestTimeout</Code></Error>";
        let mapped = map_error_common(generic(OwnedTextError(text.to_string())));
        assert!(
            matches!(
                mapped,
                StoreError::Throttled {
                    retry_after_ms: 1000
                }
            ),
            "{mapped:?}"
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

    /// A 404 is classified by the S3 code at the end of `object_store`'s
    /// `RetryError` text: `NoSuchBucket` is `Permanent` on every path, a
    /// whole-request `DeleteObjects` 404 is `NotFound` only for `NoSuchKey`, and
    /// a bodiless 404 (a HEAD) is `NotFound` except on `DeleteObjects`.
    #[test]
    fn a_404_is_classified_by_its_s3_error_code() {
        const NO_BUCKET: &str = "Error performing GET http://h/b/k in 1ms - Server returned \
             non-2xx status code: 404 Not Found: <?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Error><Code>NoSuchBucket</Code><Message>m</Message></Error>";
        const NO_KEY: &str = "Error performing GET http://h/b/k in 1ms - Server returned \
             non-2xx status code: 404 Not Found: \
             <Error><Code>NoSuchKey</Code><Message>m</Message></Error>";
        const BODILESS: &str = "Error performing HEAD http://h/b/k in 1ms - Server returned \
             non-2xx status code: 404 Not Found: ";
        let not_found = |text: &'static str| object_store::Error::NotFound {
            path: "k".into(),
            source: Box::new(TextError(text)),
        };

        assert!(matches!(
            map_error_common(not_found(NO_BUCKET)),
            StoreError::Permanent(_)
        ));
        assert!(matches!(
            map_get_error(not_found(NO_BUCKET)),
            StoreError::Permanent(_)
        ));
        assert!(matches!(
            map_put_error(not_found(NO_BUCKET), &PutMode::CreateIfAbsent),
            StoreError::Permanent(_)
        ));
        assert!(matches!(
            map_error_common(generic(TextError(NO_BUCKET))),
            StoreError::Permanent(_)
        ));
        assert!(matches!(
            map_error_common(not_found(NO_KEY)),
            StoreError::NotFound
        ));
        assert!(matches!(
            map_error_common(not_found(BODILESS)),
            StoreError::NotFound
        ));

        assert!(matches!(
            map_delete_error(not_found(NO_BUCKET)),
            StoreError::Permanent(_)
        ));
        assert!(matches!(
            map_delete_error(not_found(NO_KEY)),
            StoreError::NotFound
        ));
        assert!(matches!(
            map_delete_error(not_found(BODILESS)),
            StoreError::Permanent(_)
        ));
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

        // A 416 elsewhere in the text (the port, the key, the request id) of a
        // response that also says "range" is not a range error: it stays
        // retryable instead of reading the terminal InvalidRange.
        for text in [
            "Error performing GET http://127.0.0.1:41600/b/range/k in 1.2ms - Server \
             returned non-2xx status code: 500 Internal Server Error: ",
            "Error performing GET http://127.0.0.1:9000/b/range/k416 in 1.2ms - Server \
             returned non-2xx status code: 500 Internal Server Error: ",
            "Error performing GET http://127.0.0.1:9000/b/k in 1.2ms - Server returned \
             non-2xx status code: 400 Bad Request: <Error><Code>InvalidArgument</Code>\
             <ArgumentName>Range</ArgumentName><RequestId>41641600</RequestId></Error>",
        ] {
            let mapped = map_get_error(generic(TextError(text)));
            assert!(
                matches!(mapped, StoreError::Transient(_)),
                "stray 416 digits must not read InvalidRange, got {mapped:?} for {text:?}"
            );
            assert!(mapped.is_retryable(), "{text:?} must be retryable");
        }

        // The text object_store renders for a real 416 still reads InvalidRange.
        let real = "Error performing GET http://127.0.0.1:9000/b/k in 1.2ms - Server \
                    returned non-2xx status code: 416 Range Not Satisfiable: \
                    <Error><Code>InvalidRange</Code></Error>";
        let mapped = map_get_error(generic(TextError(real)));
        assert!(matches!(mapped, StoreError::InvalidRange(_)), "{mapped:?}");

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
