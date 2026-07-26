//! Object store contract for Ravel (docs/object-store-contract.md, ADR-0008).
//!
//! Every durability argument in the system is made against
//! [`ObjectStoreBackend`], never against a vendor SDK. [`memory::MemoryStore`]
//! is the semantics oracle used by tests.

pub mod memory;

use bytes::Bytes;

/// Opaque entity tag. Memory backend uses monotonic counters; S3 uses real
/// etags. Only equality and CAS preconditions are defined on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Etag(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutMode {
    /// Unconditional write. Safe only for keys unique by construction.
    Overwrite,
    /// Fail with [`StoreError::AlreadyExists`] if the key exists.
    CreateIfAbsent,
    /// Replace only if the current etag matches, else
    /// [`StoreError::PreconditionFailed`].
    CasEtag(Etag),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOptions {
    pub mode: PutMode,
}

impl Default for PutOptions {
    fn default() -> Self {
        PutOptions {
            mode: PutMode::Overwrite,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetRange {
    Full,
    /// Half-open byte range `[start, end)`.
    Range(u64, u64),
    /// Last `n` bytes.
    Suffix(u64),
}

#[derive(Debug, Clone)]
pub struct PutOutcome {
    pub etag: Etag,
}

#[derive(Debug, Clone)]
pub struct GetOutcome {
    pub data: Bytes,
    pub etag: Etag,
    /// Total object size, regardless of the range requested.
    pub total_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: Etag,
    pub last_modified_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    pub create_if_absent: bool,
    pub cas_etag: bool,
    pub suffix_range: bool,
    pub consistent_list: bool,
    pub multipart: bool,
}

impl Capabilities {
    /// Everything Ravel's commit protocol and catalog require in production.
    pub fn mandatory() -> Self {
        Capabilities {
            create_if_absent: true,
            cas_etag: true,
            suffix_range: true,
            consistent_list: true,
            multipart: false, // required from Phase 2 (large L1/L2 segments)
        }
    }

    pub fn satisfies(&self, required: &Capabilities) -> bool {
        (!required.create_if_absent || self.create_if_absent)
            && (!required.cas_etag || self.cas_etag)
            && (!required.suffix_range || self.suffix_range)
            && (!required.consistent_list || self.consistent_list)
            && (!required.multipart || self.multipart)
    }
}

/// Error taxonomy sized for callers' retry decisions. `Throttled`, `Timeout`,
/// and `Transient` are retryable with jittered exponential backoff.
/// `AlreadyExists` under `CreateIfAbsent` is a protocol signal (ADR-0002).
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object not found")]
    NotFound,
    #[error("object already exists")]
    AlreadyExists,
    #[error("precondition failed")]
    PreconditionFailed,
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

    /// Recursive prefix listing in lexicographic key order.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError>;

    /// Idempotent: deleting a missing key succeeds.
    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    fn capabilities(&self) -> Capabilities;
}
