//! Object store contract for Ravel (docs/object-store-contract.md, ADR-0008,
//! amended by ADR-0010 §12).
//!
//! Every durability argument in the system is made against
//! [`ObjectStoreBackend`], never against a vendor SDK. [`memory::MemoryStore`]
//! is the semantics oracle used by tests.

pub mod fault;
pub mod instrument;
pub mod memory;
pub mod s3;

use bytes::Bytes;

/// Per-operation counters, latency histogram, and byte totals for any backend
/// ([`instrument`]). Observability only: never correctness-bearing, and
/// wrapping a backend is a zero behavior change (results and
/// [`Capabilities`] pass through verbatim).
pub use instrument::{InstrumentedStore, StoreMetrics};

/// Content identity: used only for equality assertions between reads of the
/// same immutable object. Never used as a CAS precondition (that is
/// [`Version`]). The two coincide on S3 and differ on GCS/Azure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Etag(pub String);

/// Opaque precondition token for CAS puts: S3 etag, GCS generation, Azure
/// etag. Only the backend that issued it can interpret it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version(pub String);

/// Checksum the caller computed locally and the backend verifies on upload.
/// Transport-integrity only; blake3 identity lives in commit records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadChecksum {
    Crc32c(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutMode {
    /// Unconditional write. Safe only for keys unique by construction.
    Overwrite,
    /// Fail with [`StoreError::AlreadyExists`] if the key exists.
    CreateIfAbsent,
    /// Replace only if the current version matches, else
    /// [`StoreError::PreconditionFailed`].
    CasVersion(Version),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOptions {
    pub mode: PutMode,
    pub checksum: Option<UploadChecksum>,
}

impl Default for PutOptions {
    fn default() -> Self {
        PutOptions {
            mode: PutMode::Overwrite,
            checksum: None,
        }
    }
}

impl PutOptions {
    pub fn create_if_absent() -> Self {
        PutOptions {
            mode: PutMode::CreateIfAbsent,
            checksum: None,
        }
    }

    pub fn with_checksum(mut self, checksum: UploadChecksum) -> Self {
        self.checksum = Some(checksum);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetRange {
    Full,
    /// Half-open byte range `[start, end)`. Zero-length is `InvalidRange`.
    Range(u64, u64),
    /// Last `n` bytes, `n > 0` (`Suffix(0)` is `InvalidRange`).
    Suffix(u64),
}

#[derive(Debug, Clone)]
pub struct PutOutcome {
    pub etag: Etag,
    pub version: Version,
}

#[derive(Debug, Clone)]
pub struct GetOutcome {
    pub data: Bytes,
    pub etag: Etag,
    pub version: Version,
    /// Total object size, regardless of the range requested.
    pub total_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: Etag,
    pub version: Version,
    /// May have 1-second granularity on real backends. GC age checks only;
    /// never order commits by it.
    pub last_modified_unix_ms: i64,
}

/// Smallest legal size for any part but the last, in bytes (5 MiB).
///
/// S3 rejects a `CompleteMultipartUpload` whose non-final parts are smaller
/// than this with `EntityTooSmall`, so every backend enforces it locally and
/// fails the offending `put_part` call rather than letting the whole upload
/// die at `complete()`. The final part has no minimum (but may not be empty).
/// See docs/object-store-contract.md, "Multipart upload".
pub const MULTIPART_MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// Largest legal number of parts in one upload (S3's limit). Enforced
/// locally, like [`MULTIPART_MIN_PART_SIZE`].
pub const MULTIPART_MAX_PARTS: usize = 10_000;

/// An in-flight multipart upload: a sequence of parts that becomes one
/// object, atomically, at [`MultipartUpload::complete`].
///
/// Obtained from [`ObjectStoreBackend::put_multipart`], which only backends
/// reporting `Capabilities::multipart` provide. The contract (see
/// docs/object-store-contract.md, "Multipart upload"):
///
/// - Parts are ordered by the sequence of `put_part` calls, not by completion
///   order; an implementation may upload them concurrently.
/// - Every part except the last is at least [`MULTIPART_MIN_PART_SIZE`]
///   bytes, no part is empty, and there are at most
///   [`MULTIPART_MAX_PARTS`] of them.
/// - Nothing is readable at `key` until `complete` returns `Ok`. An
///   `abort`ed, dropped, or never-completed upload never becomes a visible
///   object, not even a truncated one.
/// - `complete` writes unconditionally, equivalent to
///   [`PutMode::Overwrite`]: there is no multipart form of `CreateIfAbsent`
///   or `CasVersion`, so callers needing create-once semantics must write
///   keys that are unique by construction (Ravel's data objects are).
/// - `complete` and `abort` consume the upload logically: any later call on
///   the same handle fails with [`StoreError::Permanent`] rather than
///   re-issuing a request.
#[async_trait::async_trait]
pub trait MultipartUpload: Send {
    /// Append one part. `checksum`, if given, is verified locally against
    /// `data` before anything is sent (the same pre-flight `put` runs, with
    /// the same limits: see [`Capabilities::mandatory`] on `upload_checksum`).
    /// A mismatch fails this call with [`StoreError::Corrupted`], does not
    /// count as a part, and leaves the upload usable and abortable.
    async fn put_part(
        &mut self,
        data: Bytes,
        checksum: Option<UploadChecksum>,
    ) -> Result<(), StoreError>;

    /// Assemble every part uploaded so far into the object, atomically.
    /// Fails without publishing anything if the part sequence is illegal
    /// (empty, or a non-final part below [`MULTIPART_MIN_PART_SIZE`]).
    async fn complete(&mut self) -> Result<PutOutcome, StoreError>;

    /// Discard the upload and its parts. The object must not exist
    /// afterwards. Idempotency is not promised: a second `abort` (or an
    /// `abort` after `complete`) fails with [`StoreError::Permanent`].
    async fn abort(&mut self) -> Result<(), StoreError>;
}

/// Part-sequence rule checker shared by every [`MultipartUpload`]
/// implementation, so the memory oracle and the S3 adapter reject exactly the
/// same illegal sequences with the same messages.
#[derive(Debug, Default)]
pub(crate) struct PartSequence {
    count: usize,
    /// Length of the most recent part. It is only constrained once a further
    /// part arrives and makes it non-final.
    last_len: Option<usize>,
}

impl PartSequence {
    /// Validate one more part of `len` bytes and count it.
    pub(crate) fn accept(&mut self, key: &str, len: usize) -> Result<(), StoreError> {
        if len == 0 {
            return Err(StoreError::Permanent(format!(
                "multipart upload of {key}: part {} is empty; no part may be zero-length",
                self.count + 1
            )));
        }
        if let Some(prev) = self.last_len
            && prev < MULTIPART_MIN_PART_SIZE
        {
            return Err(StoreError::Permanent(format!(
                "multipart upload of {key}: part {} is {prev} bytes, below the \
                 {MULTIPART_MIN_PART_SIZE}-byte minimum, and this part makes it non-final",
                self.count
            )));
        }
        if self.count >= MULTIPART_MAX_PARTS {
            return Err(StoreError::Permanent(format!(
                "multipart upload of {key}: more than {MULTIPART_MAX_PARTS} parts"
            )));
        }
        self.count += 1;
        self.last_len = Some(len);
        Ok(())
    }

    /// Validate the sequence as a whole, at `complete` time.
    pub(crate) fn finish(&self, key: &str) -> Result<(), StoreError> {
        if self.count == 0 {
            return Err(StoreError::Permanent(format!(
                "multipart upload of {key}: no parts were uploaded"
            )));
        }
        Ok(())
    }
}

/// The error every backend returns for a call on a handle whose `complete` or
/// `abort` already ran, and for `put_multipart` on a backend that does not
/// support it. Uniform text so a caller's logs read the same everywhere.
pub(crate) fn multipart_finished(key: &str) -> StoreError {
    StoreError::Permanent(format!(
        "multipart upload of {key}: already completed or aborted"
    ))
}

/// Opaque listing continuation token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageToken(pub String);

/// One page of listing results. Cross-page guarantee (see contract doc):
/// keys created before the first page request are always returned; keys
/// created during the scan may or may not appear; a key MAY appear more than
/// once across pages and callers MUST dedup by key.
#[derive(Debug, Clone)]
pub struct ListPage {
    pub objects: Vec<ObjectMeta>,
    pub next: Option<PageToken>,
}

/// One-level listing: objects directly under the prefix plus common
/// sub-prefixes (S3 delimiter semantics).
#[derive(Debug, Clone)]
pub struct DelimitedList {
    pub objects: Vec<ObjectMeta>,
    pub common_prefixes: Vec<String>,
}

/// Capability flags mirroring the capability tables in the contract doc.
/// Production startup fails if a flag [`Capabilities::mandatory`] requires is
/// false; the flags outside that set are mode-conditional (`multipart`) or
/// best-effort (`upload_checksum`), documented on `mandatory` below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    pub consistent_read: bool,
    pub consistent_list: bool,
    pub create_if_absent: bool,
    pub cas_version: bool,
    pub suffix_range: bool,
    pub upload_checksum: bool,
    pub prefix_list: bool,
    pub multipart: bool,
}

impl Capabilities {
    /// Everything Ravel's commit protocol and catalog require in production.
    ///
    /// Two flags are deliberately `false` here, for different reasons:
    ///
    /// - `multipart` is mode-conditional, not universally required: only
    ///   compaction writes multipart objects, so ravel-server's
    ///   `required_capabilities` adds it for `Mode::Maintain` alone.
    /// - `upload_checksum` is not required by any mode. It cannot be
    ///   satisfied by S3, the only durable backend Ravel ships: the
    ///   `object_store` 0.14 `AmazonS3` client has no per-request checksum
    ///   hook and no way to attach a caller-supplied precomputed digest to
    ///   the wire, so [`crate::s3::S3Store`] reports it as unsupported (see
    ///   that module's "Known divergences" doc). That is a permanent
    ///   client-library limitation, not a per-endpoint or per-mode gap:
    ///   requiring the flag made `--store s3` fail startup against every
    ///   S3-compatible endpoint unconditionally (issue #251), which blocks
    ///   the only durable backend instead of catching a real regression.
    ///   Upload checksums are a CRC32C-class transport-corruption check;
    ///   the actual backstop against corrupted data surviving is the
    ///   read-time footer/section/page crc32c hierarchy
    ///   (docs/segment-format.md), which is independent of them. Backends
    ///   that can honor the flag still do, `put()` still runs its local
    ///   pre-flight CRC32C check on every backend, and the contract suite
    ///   still asserts that behavior; it is simply not startup-gating.
    pub fn mandatory() -> Self {
        Capabilities {
            consistent_read: true,
            consistent_list: true,
            create_if_absent: true,
            cas_version: true,
            suffix_range: true,
            upload_checksum: false, // unsatisfiable on S3 (see doc above)
            prefix_list: true,
            multipart: false, // mandatory from Phase 2 (large L1/L2 segments)
        }
    }

    /// True when `self` (a backend's reported capabilities) provides every
    /// flag `required` demands. Call as `backend.satisfies(&mandatory())`:
    /// `self` is the backend, `required` is the contract. ravel-server's
    /// startup path uses exactly this to reject a backend that under-reports
    /// a mandatory flag, so the "startup fails" claim above is enforced, not
    /// decorative.
    pub fn satisfies(&self, required: &Capabilities) -> bool {
        (!required.consistent_read || self.consistent_read)
            && (!required.consistent_list || self.consistent_list)
            && (!required.create_if_absent || self.create_if_absent)
            && (!required.cas_version || self.cas_version)
            && (!required.suffix_range || self.suffix_range)
            && (!required.upload_checksum || self.upload_checksum)
            && (!required.prefix_list || self.prefix_list)
            && (!required.multipart || self.multipart)
    }
}

/// Error taxonomy sized for callers' retry decisions. `Throttled`, `Timeout`,
/// and `Transient` are retryable with jittered exponential backoff.
/// `AlreadyExists` under `CreateIfAbsent` is a protocol signal (ADR-0002).
/// Adapters MUST map conditional-put failures by mode: `AlreadyExists` under
/// `CreateIfAbsent`, `PreconditionFailed` under `CasVersion`.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object not found")]
    NotFound,
    #[error("object already exists")]
    AlreadyExists,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("throttled, retry after {retry_after_ms} ms")]
    Throttled { retry_after_ms: u64 },
    #[error("timeout")]
    Timeout,
    #[error("corrupted response: {0}")]
    Corrupted(String),
    #[error("invalid range: {0}")]
    InvalidRange(String),
    #[error("transient error: {0}")]
    Transient(String),
    #[error("permanent error: {0}")]
    Permanent(String),
}

impl StoreError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StoreError::Throttled { .. } | StoreError::Timeout | StoreError::Transient(_)
        )
    }
}

/// The contract. See docs/object-store-contract.md for caller rules:
/// visibility comes only from commit records, checksums are verified on all
/// format-bearing reads, and every caller bounds its work with deadlines.
#[async_trait::async_trait]
pub trait ObjectStoreBackend: Send + Sync + 'static {
    async fn put(&self, key: &str, data: Bytes, opts: PutOptions)
    -> Result<PutOutcome, StoreError>;

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError>;

    /// Begin a multipart upload of `key`. See [`MultipartUpload`] for the part
    /// sequence rules and the visibility guarantee.
    ///
    /// The default implementation refuses, which is the correct behavior for a
    /// backend reporting `Capabilities::multipart == false`: the flag and this
    /// method must agree, and the contract suite asserts they do in both
    /// directions. A backend reporting `multipart: true` MUST override it.
    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        Err(StoreError::Permanent(format!(
            "multipart upload of {key}: unsupported by this backend"
        )))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError>;

    /// Paginated recursive prefix listing in lexicographic key order.
    /// Pass `None` for the first page; follow `ListPage::next` until `None`.
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError>;

    /// One-level listing with delimiter `/`.
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError>;

    /// Idempotent: deleting a missing key succeeds.
    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    fn capabilities(&self) -> Capabilities;
}

/// Drain every page of a listing, deduplicating by key per the cross-page
/// guarantee. Convenience for callers with bounded prefixes.
pub async fn list_all(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> Result<Vec<ObjectMeta>, StoreError> {
    let mut out: Vec<ObjectMeta> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut page_token = None;
    loop {
        let page = store.list(prefix, page_token).await?;
        for meta in page.objects {
            if seen.insert(meta.key.clone()) {
                out.push(meta);
            }
        }
        match page.next {
            Some(next) => page_token = Some(next),
            None => break,
        }
    }
    Ok(out)
}
