//! Object store contract for Ravel (docs/object-store-contract.md, ADR-0008,
//! amended by ADR-0010 §12).
//!
//! Every durability argument in the system is made against
//! [`ObjectStoreBackend`], never against a vendor SDK. [`memory::MemoryStore`]
//! is the semantics oracle used by tests.

pub mod fault;
pub mod memory;
pub mod s3;

use bytes::Bytes;

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
