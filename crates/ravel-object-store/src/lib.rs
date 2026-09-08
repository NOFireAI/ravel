//! Object store contract for Ravel (docs/object-store-contract.md, ADR-0008,
//! amended by ADR-0010 §12).
//!
//! Every durability argument in the system is made against
//! [`ObjectStoreBackend`], never against a vendor SDK. [`memory::MemoryStore`]
//! is the semantics oracle used by tests.

pub mod conformance;
pub mod fault;
pub mod instrument;
pub mod kms_routing;
pub mod memory;
pub mod s3;
pub mod scheduling;

use std::sync::Arc;

use bytes::Bytes;

/// Per-operation counters, latency histogram, and byte totals for any backend
/// ([`instrument`]). Observability only: never correctness-bearing, and
/// wrapping a backend is a zero behavior change (results and
/// [`Capabilities`] pass through verbatim).
pub use instrument::{
    InstrumentedStore, OpMetricsSnapshot, StoreMetrics, StoreMetricsSnapshot, StoreOp,
};

/// Per-tenant SSE-KMS key routing decorator (ADR-0062 decision 1a): routes
/// tenant writes to lazily-built, cached per-tenant [`s3::S3Store`]s while every
/// read and non-tenant key delegates to the default store.
pub use kms_routing::{KmsRoutingStore, routes_through_tenant_key};

/// Two-class request scheduling (ADR-0070 decision 1): [`ClassedStore`] hands
/// out foreground/background handles sharing one [`RequestScheduler`], with
/// strict-priority-with-floor admission. Off by default via
/// [`ClassedStore::passthrough`] (decision 2).
pub use scheduling::{ClassedStore, RequestClass, RequestScheduler, SchedulerConfig};

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
    /// May have 1-second granularity on real backends. Advisory age decisions
    /// only (GC age checks, claim expiry); never order commits by it.
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
/// - A `put_part` that fails at the backend, or a part that violates the
///   sequence rules (empty, or a non-final part below
///   [`MULTIPART_MIN_PART_SIZE`]), *poisons* the handle: every later `put_part`
///   and `complete` fails with a non-retryable [`StoreError::Permanent`], and
///   the caller must `abort` and restart the whole upload rather than retry the
///   part. This matches what `object_store`'s S3 upload actually permits: it
///   fixes each part's index at `put_part` call time and `complete` demands
///   exactly that many parts, so a retried part lands at a fresh index and the
///   hole a failed part left can never be filled. `abort` stays callable on a
///   poisoned handle. A checksum mismatch is the one recoverable rejection: it
///   does not poison, leaving the upload open so the caller can re-send the
///   same bytes with the correct checksum.
#[async_trait::async_trait]
pub trait MultipartUpload: Send {
    /// Append one part. `checksum`, if given, is verified locally against
    /// `data` before anything is sent (the same pre-flight `put` runs, with
    /// the same limits: see [`Capabilities::mandatory`] on `upload_checksum`).
    /// A checksum mismatch fails this call with [`StoreError::Corrupted`], does
    /// not count as a part, and leaves the upload usable and abortable. A
    /// sequence-rule violation or a backend part-upload failure instead poisons
    /// the handle (see the type-level docs): this and every later `put_part`
    /// and `complete` return a non-retryable [`StoreError::Permanent`], and the
    /// upload must be aborted and restarted.
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

/// The error a poisoned multipart handle returns from `put_part` and
/// `complete` after a part upload failed or a part-sequence rule
/// was violated. Unlike a checksum mismatch, which leaves the
/// upload open for a re-send, these are unrecoverable: `object_store`'s S3
/// upload fixes each part's index at `put_part` call time and `complete`
/// demands exactly that many parts, so a failed or rejected part leaves a
/// permanent hole a retried part (landing at a fresh index) can never fill.
/// The handle is dead; the caller must `abort` and restart the whole upload.
/// Always [`StoreError::Permanent`], so never retryable, and `abort` stays
/// callable on the poisoned handle.
pub(crate) fn multipart_poisoned(key: &str, cause: &str) -> StoreError {
    StoreError::Permanent(format!(
        "multipart upload of {key} is poisoned by an earlier failure and must be \
         aborted and restarted, not retried: {cause}"
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
    ///   `Mode::Maintain` gates on it, via ravel-server's
    ///   `required_capabilities`. That gate is forward-looking: no production
    ///   caller invokes `put_multipart` yet (ravel-maintain writes single-PUT
    ///   content-addressed compaction outputs today), so the flag
    ///   reserves the capability for when compaction streams large L1/L2
    ///   segments as multipart uploads rather than describing current traffic.
    /// - `upload_checksum` is not required by any mode. It cannot be
    ///   satisfied by S3, the only durable backend Ravel ships: the
    ///   `object_store` 0.14 `AmazonS3` client has no per-request checksum
    ///   hook and no way to attach a caller-supplied precomputed digest to
    ///   the wire, so [`crate::s3::S3Store`] reports it as unsupported (see
    ///   that module's "Known divergences" doc). That is a permanent
    ///   client-library limitation, not a per-endpoint or per-mode gap:
    ///   requiring the flag made `--store s3` fail startup against every
    ///   S3-compatible endpoint unconditionally, which blocks
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
    /// A paged listing drain saw the same continuation token twice: the backend
    /// reports "another page" while making no progress. Draining returns this
    /// rather than spinning forever. Never retryable.
    #[error("listing under {prefix:?} repeated its continuation token; refusing to spin")]
    ListRepeatedToken { prefix: String },
    /// A paged listing drain passed its page ceiling: a continuation token that
    /// keeps changing without ever ending. Never retryable.
    #[error("listing under {prefix:?} exceeded the {ceiling}-page listing ceiling")]
    ListPageCeiling { prefix: String, ceiling: usize },
    /// A paged listing delivered a key strictly below one already delivered,
    /// breaking the contract's lexicographic-order guarantee. Folding out of
    /// order is wrong, so draining returns this rather than silently
    /// reordering. Never retryable.
    #[error(
        "listing under {prefix:?} delivered {offending:?} after {previous:?}, \
         out of lexicographic order"
    )]
    ListOrderViolation {
        prefix: String,
        previous: String,
        offending: String,
    },
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

    /// Like [`list`](Self::list), but the listing begins strictly after
    /// `start_after` in key order (S3 `start-after` semantics): every returned
    /// key compares strictly greater than `start_after`, in the same
    /// lexicographic key order, with the same pagination and cross-page
    /// guarantee as `list`. `start_after == None` is identical to `list`.
    /// `start_after` need not name an existing key; it is typically a prefix
    /// string that sorts before the first key the caller wants, so a caller
    /// can skip a whole key sub-range server-side without paging through it.
    ///
    /// The default implementation lists from `prefix` and drops keys
    /// `<= start_after`; a backend whose store supports a native start-after
    /// (S3 `list_with_offset`, the in-memory ordered map) overrides it so the
    /// dropped keys are never transferred. Overriding is a performance
    /// property only: the visible result is identical either way.
    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let page = self.list(prefix, page).await?;
        match start_after {
            Some(after) => Ok(ListPage {
                objects: page
                    .objects
                    .into_iter()
                    .filter(|meta| meta.key.as_str() > after)
                    .collect(),
                next: page.next,
            }),
            None => Ok(page),
        }
    }

    /// One-level listing with delimiter `/`.
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError>;

    /// Idempotent: deleting a missing key succeeds.
    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    fn capabilities(&self) -> Capabilities;
}

/// A shared handle is itself a backend, forwarding every method to the pointee.
///
/// `?Sized` so this covers `Arc<dyn ObjectStoreBackend>` as well as
/// `Arc<ConcreteStore>`, which is what lets a decorator whose type parameter is
/// `S: ObjectStoreBackend` (for example [`InstrumentedStore`]) wrap an
/// already-type-erased `Arc<dyn ObjectStoreBackend>`: without this impl the
/// erased handle is not itself a backend and cannot be a decorator's `S`. Every
/// method delegates to the pointee, `put_multipart` and `capabilities`
/// included, so a `multipart: true` backend keeps that capability through the
/// `Arc` rather than falling back to the refusing default.
#[async_trait::async_trait]
impl<T: ObjectStoreBackend + ?Sized> ObjectStoreBackend for Arc<T> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        (**self).put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        (**self).get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        (**self).put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        (**self).head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        (**self).list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        (**self).list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        (**self).list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        (**self).delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        (**self).capabilities()
    }
}

/// Page ceiling for a single [`list_all`] drain.
///
/// At the contract's 1000-key page size (docs/object-store-contract.md,
/// "Listing"), 100 000 pages bounds one drain to 100 million keys, far above
/// any prefix a caller drains: the widest is a whole-tenant or whole-store
/// prefix during maintenance and benchmarks, orders of magnitude below this.
/// It only ever trips on a backend that never terminates. The repeated-token
/// guard below catches the common spin (a backend handing back the same
/// continuation token); this ceiling is the backstop for a token that keeps
/// changing without advancing.
const MAX_LIST_PAGES: usize = 100_000;

/// Drain every page of a listing, deduplicating by key per the cross-page
/// guarantee. Convenience for callers with bounded prefixes.
///
/// Two object-store contract guarantees (docs/object-store-contract.md,
/// "Listing") drive the loop: keys arrive in lexicographic order, and a key
/// MAY appear more than once so callers MUST dedup. Because a permitted repeat
/// is therefore always equal to the last key already delivered, the dedup holds
/// only that last key rather than a set of every key: an equal key is dropped,
/// a strictly smaller key breaks the order guarantee and becomes
/// [`StoreError::ListOrderViolation`], and a larger key is kept. That is
/// constant extra memory over the returned set.
///
/// The loop terminates on `page.next == None`. A backend that never terminates
/// is a typed error, never a spin: a repeated continuation token is
/// [`StoreError::ListRepeatedToken`], and a token that keeps changing without
/// ending trips [`MAX_LIST_PAGES`] as [`StoreError::ListPageCeiling`].
pub async fn list_all(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> Result<Vec<ObjectMeta>, StoreError> {
    drain_list(store, prefix, MAX_LIST_PAGES).await
}

/// [`list_all`] with an explicit page ceiling, so a test can exercise the
/// [`StoreError::ListPageCeiling`] path without draining 100 000 pages.
async fn drain_list(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
    max_pages: usize,
) -> Result<Vec<ObjectMeta>, StoreError> {
    let mut out: Vec<ObjectMeta> = Vec::new();
    let mut last_key: Option<String> = None;
    let mut page_token: Option<PageToken> = None;
    let mut prev_token: Option<PageToken> = None;
    let mut pages = 0usize;
    loop {
        if pages >= max_pages {
            return Err(StoreError::ListPageCeiling {
                prefix: prefix.to_string(),
                ceiling: max_pages,
            });
        }
        pages += 1;
        let page = store.list(prefix, page_token).await?;
        for meta in page.objects {
            match last_key.as_deref() {
                // Lexicographic order plus a permitted repeat means a key at or
                // below the last delivered one is either that same key again
                // (dropped) or a backend that broke ordering (a typed error).
                Some(last) if meta.key.as_str() < last => {
                    return Err(StoreError::ListOrderViolation {
                        prefix: prefix.to_string(),
                        previous: last.to_string(),
                        offending: meta.key,
                    });
                }
                Some(last) if meta.key.as_str() == last => {}
                _ => {
                    last_key = Some(meta.key.clone());
                    out.push(meta);
                }
            }
        }
        match page.next {
            Some(next) => {
                if prev_token.as_ref() == Some(&next) {
                    return Err(StoreError::ListRepeatedToken {
                        prefix: prefix.to_string(),
                    });
                }
                prev_token = Some(next.clone());
                page_token = Some(next);
            }
            None => break,
        }
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod list_all_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;

    use super::*;
    use crate::memory::MemoryStore;

    /// A backend that never signals a last page: it always reports another page.
    /// With `distinct` it hands back a fresh continuation token each call
    /// (exercising the page ceiling); otherwise it repeats one token (exercising
    /// the repeated-token guard). Every `list` call is counted.
    struct NeverEndingList {
        calls: AtomicUsize,
        distinct: bool,
    }

    impl NeverEndingList {
        fn new(distinct: bool) -> Self {
            NeverEndingList {
                calls: AtomicUsize::new(0),
                distinct,
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for NeverEndingList {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            unreachable!("NeverEndingList is list-only")
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            unreachable!("NeverEndingList is list-only")
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            unreachable!("NeverEndingList is list-only")
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let token = if self.distinct {
                PageToken(format!("tok-{n}"))
            } else {
                PageToken("stuck".to_string())
            };
            Ok(ListPage {
                objects: Vec::new(),
                next: Some(token),
            })
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            unreachable!("NeverEndingList is list-only")
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            unreachable!("NeverEndingList is list-only")
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// A backend that replays a fixed script of pages, so a test can place an
    /// exact key sequence across page boundaries: a contract-permitted repeat,
    /// or a contract-violating backward key. `MemoryStore` cannot repeat a key,
    /// so the dedup and order paths need a driver that can. Every `list` call is
    /// counted; the last scripted page ends the listing (`next == None`).
    struct ScriptedList {
        pages: Vec<Vec<String>>,
        calls: AtomicUsize,
    }

    impl ScriptedList {
        fn new(pages: &[&[&str]]) -> Self {
            ScriptedList {
                pages: pages
                    .iter()
                    .map(|page| page.iter().map(|k| k.to_string()).collect())
                    .collect(),
                calls: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn meta(key: &str) -> ObjectMeta {
            ObjectMeta {
                key: key.to_string(),
                size: 1,
                etag: Etag("e".to_string()),
                version: Version("v".to_string()),
                last_modified_unix_ms: 0,
            }
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for ScriptedList {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            unreachable!("ScriptedList is list-only")
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            unreachable!("ScriptedList is list-only")
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            unreachable!("ScriptedList is list-only")
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let objects = self.pages[n].iter().map(|k| Self::meta(k)).collect();
            let next = if n + 1 < self.pages.len() {
                Some(PageToken(format!("tok-{n}")))
            } else {
                None
            };
            Ok(ListPage { objects, next })
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            unreachable!("ScriptedList is list-only")
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            unreachable!("ScriptedList is list-only")
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// A backend that repeats one continuation token is a typed error on the
    /// second page, never an infinite loop.
    #[tokio::test]
    async fn a_repeated_continuation_token_is_a_typed_error_after_two_pages() {
        let store = NeverEndingList::new(false);
        let err = drain_list(&store, "p/", MAX_LIST_PAGES)
            .await
            .expect_err("a repeated token must not spin");
        assert!(
            matches!(err, StoreError::ListRepeatedToken { .. }),
            "got {err:?}"
        );
        assert_eq!(
            store.call_count(),
            2,
            "the repeat is detected on exactly the second page"
        );
    }

    /// A backend whose token keeps changing without ever ending trips the page
    /// ceiling at exactly `max_pages` pages.
    #[tokio::test]
    async fn an_ever_advancing_token_trips_the_page_ceiling_exactly() {
        let store = NeverEndingList::new(true);
        let err = drain_list(&store, "p/", 3)
            .await
            .expect_err("an unbounded listing must stop at the ceiling");
        assert!(
            matches!(err, StoreError::ListPageCeiling { ceiling: 3, .. }),
            "got {err:?}"
        );
        assert_eq!(
            store.call_count(),
            3,
            "exactly three pages are drained before the ceiling fires"
        );
    }

    /// The object-store contract permits a key to appear more than once across
    /// pages. When the last key of one page repeats as the first key of the
    /// next, the fold keeps it exactly once.
    #[tokio::test]
    async fn a_repeat_at_a_page_boundary_is_folded_once() {
        let store = ScriptedList::new(&[&["p/a", "p/b"], &["p/b", "p/c"]]);
        let keys: Vec<String> = drain_list(&store, "p/", MAX_LIST_PAGES)
            .await
            .expect("a permitted repeat must not error")
            .into_iter()
            .map(|m| m.key)
            .collect();
        assert_eq!(
            keys,
            vec!["p/a", "p/b", "p/c"],
            "the boundary repeat of p/b is folded once, not twice"
        );
        assert_eq!(store.call_count(), 2, "both scripted pages are drained");
    }

    /// A key strictly below the last delivered one breaks the contract's
    /// lexicographic-order guarantee. Folding out of order is wrong, so the
    /// drain returns a typed error rather than reordering.
    #[tokio::test]
    async fn a_backward_key_is_a_typed_order_violation() {
        let store = ScriptedList::new(&[&["p/a", "p/c"], &["p/b"]]);
        let err = drain_list(&store, "p/", MAX_LIST_PAGES)
            .await
            .expect_err("a backward key must be rejected");
        assert!(
            matches!(
                &err,
                StoreError::ListOrderViolation {
                    previous,
                    offending,
                    ..
                } if previous == "p/c" && offending == "p/b"
            ),
            "got {err:?}"
        );
        assert_eq!(
            store.call_count(),
            2,
            "the violation is detected on the second page, after both are fetched"
        );
    }

    /// A compliant backend drained over several real pages returns every key
    /// once, in lexicographic order, identical to the pre-bound behavior.
    #[tokio::test]
    async fn a_compliant_backend_returns_every_key_once_in_order() {
        // Page size 2 forces three pages over five keys.
        let store = MemoryStore::with_page_size(2);
        for key in ["p/a", "p/b", "p/c", "p/d", "p/e"] {
            store
                .put(
                    key,
                    Bytes::from_static(b"x"),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("seed key");
        }

        let keys: Vec<String> = list_all(&store, "p/")
            .await
            .expect("full drain")
            .into_iter()
            .map(|m| m.key)
            .collect();
        assert_eq!(
            keys,
            vec!["p/a", "p/b", "p/c", "p/d", "p/e"],
            "the drain returns every key once, in order"
        );
    }
}
