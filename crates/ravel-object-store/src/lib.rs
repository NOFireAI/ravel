//! Object store contract for Ravel (docs/object-store-contract.md, ADR-0008,
//! amended by ADR-0010 §12).
//!
//! Every durability argument in the system is made against
//! [`ObjectStoreBackend`], never against a vendor SDK. [`memory::MemoryStore`]
//! is the semantics oracle used by tests.

pub mod conformance;
pub mod external;
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

/// The recorded identity of an object Ravel did not write (ADR-2040
/// decision 1, as corrected by its pinning amendment): what the catalog
/// recorded about the object when the grant was created.
///
/// The two halves are asserted differently, and the difference is the whole
/// point of the type:
///
/// - `etag` is a **precondition**. It travels as `If-Match`, so an object
///   whose current identity differs is refused with
///   [`StoreError::PreconditionFailed`] rather than served.
/// - `version` is a **selector**, not a precondition. It travels as the
///   backend's own version parameter (S3 `versionId`, GCS `generation`, Azure
///   `versionid`), which asks for *that* version of the object. A newer
///   version existing does not make the read fail: it keeps returning the
///   pinned bytes. A version that no longer exists is
///   [`StoreError::NotFound`].
///
/// So a pin carrying a version reads the pinned bytes for as long as that
/// version is retained, and a pin without one (an unversioned bucket) fails
/// with `PreconditionFailed` as soon as the owner overwrites the key. Either
/// way, no read ever mixes bytes of two versions.
///
/// `version` is optional because not every store versions objects, and because
/// a store whose CAS [`Version`] is just the ETag again (S3 without
/// versioning) has no selector to send. [`Pin::from_store`] is the one place
/// that decision is made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pin {
    pub etag: String,
    pub version: Option<String>,
}

impl Pin {
    /// Pin on an ETag alone: a precondition and no selector.
    pub fn etag(etag: impl Into<String>) -> Self {
        Pin {
            etag: etag.into(),
            version: None,
        }
    }

    /// The one constructor for a pin built from what a store reported about an
    /// object ([`ObjectStoreBackend::pin_of`], the external-store adapters, and
    /// the probes all go through it).
    ///
    /// `etag` is taken verbatim, quotes included: it is sent back as `If-Match`
    /// and normalizing it here would compare a string the store never issued.
    /// `version` is recorded only when it is a selector the store can act on: a
    /// reported version equal to the ETag is *not* one (that is what
    /// [`ObjectMeta::version`] degrades to on an unversioned S3 bucket, where
    /// the CAS token is the ETag), and sending it as a `versionId` would ask
    /// for a version that does not exist.
    pub fn from_store(etag: impl Into<String>, version: Option<String>) -> Self {
        let etag = etag.into();
        let version = version.filter(|version| *version != etag);
        Pin { etag, version }
    }
}

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

/// A read plus the [`Pin`] that names exactly the bytes it returned.
///
/// [`GetOutcome`] cannot carry this itself: its `version` is the CAS
/// [`Version`], which on S3 is the ETag again, so it cannot express "this
/// object's `x-amz-version-id`". A caller recording a grant needs the
/// selector, not the CAS token, and needs it for the bytes it actually read
/// rather than for whatever a later HEAD reports.
#[derive(Debug, Clone)]
pub struct PinnedRead {
    pub outcome: GetOutcome,
    /// The identity to record so a later read returns these same bytes.
    pub pin: Pin,
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
    /// Keys the listing found that fail [`is_addressable_key`]: no operation
    /// can reach them, so they are reported here instead of in `objects`, and
    /// never fail the page (ADR-2637 decision 2).
    pub unaddressable: Vec<UnaddressableKey>,
}

/// One-level listing: objects directly under the prefix plus common
/// sub-prefixes (S3 delimiter semantics).
#[derive(Debug, Clone)]
pub struct DelimitedList {
    pub objects: Vec<ObjectMeta>,
    pub common_prefixes: Vec<String>,
    /// Keys directly under the prefix that fail [`is_addressable_key`], as on
    /// [`ListPage::unaddressable`].
    pub unaddressable: Vec<UnaddressableKey>,
    /// Raw common prefixes that fail [`is_addressable_prefix`]. They never
    /// appear in `common_prefixes`.
    pub unaddressable_prefixes: Vec<String>,
}

/// True when `key` reaches the store unchanged through the `object_store`
/// adapter: `Path::from(key)` is `key` itself (ADR-2637).
///
/// `Path::from` drops empty segments and percent-encodes control characters,
/// non-ASCII bytes, `.` and `..` segments, and a set of punctuation that
/// includes `*`, `#`, `%` and `\`, so a request for a key failing this would
/// reach a different key. A space is not encoded. The empty key is
/// addressable.
pub fn is_addressable_key(key: &str) -> bool {
    object_store::path::Path::from(key).as_ref() == key
}

/// True when a common prefix can be listed through the adapter: the empty
/// prefix, or a prefix whose stem (the prefix without its trailing `/`) is a
/// non-empty addressable key. `/` alone is not addressable: its stem is empty,
/// and a listing under it would reach the root instead.
pub fn is_addressable_prefix(prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    let stem = prefix.strip_suffix('/').unwrap_or(prefix);
    !stem.is_empty() && is_addressable_key(stem)
}

/// The key a request for `key` reaches through the adapter: `Path::from(key)`.
pub(crate) fn addressed_key(key: &str) -> String {
    object_store::path::Path::from(key).to_string()
}

/// `Ok` when `key` is addressable, else [`StoreError::UnaddressableKey`]. Every
/// key operation of a backend in this crate calls it before doing anything
/// else.
pub(crate) fn check_addressable(key: &str) -> Result<(), StoreError> {
    if is_addressable_key(key) {
        Ok(())
    } else {
        Err(StoreError::UnaddressableKey {
            key: key.to_string(),
            addresses: addressed_key(key),
        })
    }
}

/// A listed key that fails [`is_addressable_key`]. Reported by a listing,
/// never returned as an [`ObjectMeta`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnaddressableKey {
    /// The key exactly as the store listed it.
    pub key: String,
    /// The key a request for `key` would reach instead: `Path::from(key)`.
    pub addresses: String,
    pub size: u64,
    pub last_modified_unix_ms: i64,
}

impl UnaddressableKey {
    pub(crate) fn of(meta: ObjectMeta) -> Self {
        UnaddressableKey {
            addresses: addressed_key(&meta.key),
            key: meta.key,
            size: meta.size,
            last_modified_unix_ms: meta.last_modified_unix_ms,
        }
    }
}

/// How many [`UnaddressableKey`]s an [`Unaddressable`] keeps as a sample.
pub const UNADDRESSABLE_SAMPLE_MAX: usize = 16;

/// The unaddressable keys a whole drain skipped: the count of all of them and
/// the first [`UNADDRESSABLE_SAMPLE_MAX`] in listing order.
///
/// Deliberately not `#[must_use]`: a caller that ends its drain in `.await?;`
/// ignores it, and the adapter's metric and warning still report the keys.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Unaddressable {
    pub count: u64,
    pub sample: Vec<UnaddressableKey>,
}

impl Unaddressable {
    fn record(&mut self, key: UnaddressableKey) {
        self.count += 1;
        if self.sample.len() < UNADDRESSABLE_SAMPLE_MAX {
            self.sample.push(key);
        }
    }
}

/// A whole listing from [`list_all_reporting`]: every addressable key, and the
/// unaddressable keys the listing skipped.
#[derive(Debug, Clone)]
pub struct Listing {
    pub objects: Vec<ObjectMeta>,
    pub unaddressable: Unaddressable,
}

/// Split listed objects into the addressable ones and the
/// [`UnaddressableKey`]s, preserving listing order in both, and warn about the
/// unaddressable ones under `prefix`. Every base backend's listing goes
/// through this.
pub(crate) fn classify_objects(
    prefix: &str,
    listed: Vec<ObjectMeta>,
) -> (Vec<ObjectMeta>, Vec<UnaddressableKey>) {
    let (objects, refused): (Vec<_>, Vec<_>) = listed
        .into_iter()
        .partition(|meta| is_addressable_key(&meta.key));
    let unaddressable: Vec<UnaddressableKey> =
        refused.into_iter().map(UnaddressableKey::of).collect();
    warn_unaddressable(
        prefix,
        unaddressable
            .iter()
            .map(|skipped| (skipped.key.as_str(), skipped.addresses.as_str())),
    );
    (objects, unaddressable)
}

/// [`classify_objects`] for common prefixes: split by
/// [`is_addressable_prefix`], and warn about the refused ones.
pub(crate) fn classify_prefixes(prefix: &str, listed: Vec<String>) -> (Vec<String>, Vec<String>) {
    let (prefixes, refused): (Vec<_>, Vec<_>) = listed
        .into_iter()
        .partition(|common| is_addressable_prefix(common));
    let addresses: Vec<String> = refused.iter().map(|p| addressed_key(p)).collect();
    warn_unaddressable(
        prefix,
        refused
            .iter()
            .zip(&addresses)
            .map(|(raw, addressed)| (raw.as_str(), addressed.as_str())),
    );
    (prefixes, refused)
}

/// Cap on the distinct unaddressable keys a process remembers having warned
/// about. Matches `ravel-pqtable`'s `ABOVE_BOUND_TABLES_MAX`.
const UNADDRESSABLE_WARNED_MAX: usize = 4096;

/// Past [`UNADDRESSABLE_WARNED_MAX`], one listing in this many that skips a
/// key not already warned about warns. Matches `ABOVE_BOUND_WARN_EVERY`.
const UNADDRESSABLE_WARN_EVERY: u64 = 1024;

struct UnaddressableWarnState {
    warned: std::collections::BTreeSet<String>,
    /// Listings that skipped a key not warned about after the set filled.
    overflow: u64,
}

static UNADDRESSABLE_WARNED: std::sync::Mutex<UnaddressableWarnState> =
    std::sync::Mutex::new(UnaddressableWarnState {
        warned: std::collections::BTreeSet::new(),
        overflow: 0,
    });

/// Which of one listing's skipped keys to warn about, recording them in
/// `state`: every key not warned about before while the set has room, and once
/// it is full, the first new key of one listing in `warn_every`.
fn keys_to_warn<'k>(
    state: &mut UnaddressableWarnState,
    keys: impl Iterator<Item = (&'k str, &'k str)>,
    cap: usize,
    warn_every: u64,
) -> Vec<(&'k str, &'k str)> {
    let mut warn = Vec::new();
    let mut overflowed = None;
    for (key, addresses) in keys {
        if state.warned.contains(key) {
            continue;
        }
        if state.warned.len() < cap {
            state.warned.insert(key.to_string());
            warn.push((key, addresses));
        } else if overflowed.is_none() {
            overflowed = Some((key, addresses));
        }
    }
    if let Some(first) = overflowed {
        if state.overflow.is_multiple_of(warn_every) {
            warn.push(first);
        }
        state.overflow += 1;
    }
    warn
}

/// Warn once per distinct unaddressable key per process (ADR-2637 decision 2),
/// rate-limited past [`UNADDRESSABLE_WARNED_MAX`] keys.
fn warn_unaddressable<'k>(prefix: &str, keys: impl Iterator<Item = (&'k str, &'k str)>) {
    let mut keys = keys.peekable();
    if keys.peek().is_none() {
        return;
    }
    let warn = {
        let mut state = UNADDRESSABLE_WARNED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        keys_to_warn(
            &mut state,
            keys,
            UNADDRESSABLE_WARNED_MAX,
            UNADDRESSABLE_WARN_EVERY,
        )
    };
    for (key, addresses) in warn {
        tracing::warn!(
            "listing under {prefix:?} skipped key {key:?}: a request for it would reach \
             {addresses:?} instead, so no operation can read, overwrite or delete it; \
             delete it with the Maintain credential through an S3 tool"
        );
    }
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
    ///   `required_capabilities`. The explicit `put_multipart` path's
    ///   checksum behavior under [`crate::s3::UploadIntegrity`] is exercised
    ///   end-to-end against a fake endpoint by
    ///   `s3::tests::multipart_parts_carry_checksums_under_integrity`: every
    ///   part carries its own checksum header, and `CompleteMultipartUpload`
    ///   carries one too, when the endpoint echoes it back. `ravel-maintain`'s
    ///   own compaction writer still writes single-PUT content-addressed
    ///   outputs today (`crates/ravel-maintain/src/build.rs`); this flag
    ///   reserves the capability for a caller that streams large L1/L2
    ///   segments as multipart uploads rather than describing current
    ///   traffic.
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
    /// The backend does not implement this operation at all, so no retry and no
    /// alternative argument can make it succeed. Distinct from
    /// [`StoreError::Permanent`], which reports a request the backend
    /// understood and rejected. Never retryable.
    #[error("{operation}: unsupported by this backend")]
    Unsupported { operation: String },
    /// The store was opened read-only and the call would have mutated it. Every
    /// [`crate::external::ExternalStore`] refuses `put`, `put_multipart` and
    /// `delete` this way. Never retryable.
    #[error("{operation}: {store} is open read-only")]
    ReadOnly { operation: String, store: String },
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
    /// The key cannot be sent through the adapter unchanged: a request for it
    /// would reach `addresses` instead. See [`is_addressable_key`]. Never
    /// retryable.
    #[error("key {key:?} is not addressable: a request for it would reach {addresses:?}")]
    UnaddressableKey { key: String, addresses: String },
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

    /// Read `range` of the object `pin` names.
    ///
    /// This is the read path for objects Ravel did not write (ADR-2040
    /// decision 1 and its pinning amendment). The pin is evaluated by the
    /// backend, on the wire, not by comparing ETags after the fact: a backend
    /// that cannot evaluate it must not implement this method, because a local
    /// comparison would read and pay for the wrong bytes before noticing.
    ///
    /// The two halves of a [`Pin`] do different things, so the outcomes split
    /// by which half the pin carries. In the order they are decided:
    ///
    /// - No object at `key`: [`StoreError::NotFound`], never
    ///   `PreconditionFailed`. The two are distinct answers and callers act on
    ///   them differently (a missing grant target versus a changed one).
    /// - `pin.version` is `Some` and that version no longer exists (deleted, or
    ///   never existed): [`StoreError::NotFound`]. The version is a selector,
    ///   so an unknown one names no object rather than failing a precondition.
    ///   A version that does still exist is served even when the object has
    ///   since been overwritten: that is what a selector means.
    /// - The selected object's ETag differs from `pin.etag`:
    ///   [`StoreError::PreconditionFailed`], which is not retryable
    ///   ([`StoreError::is_retryable`]) because a retry reads the same changed
    ///   object. For a pin with no version this is the overwrite case.
    /// - Otherwise the bytes, in a [`PinnedRead`] whose `outcome.etag` is the
    ///   pinned one and whose `pin` names the version actually read.
    ///
    /// The default implementation refuses with [`StoreError::Unsupported`]
    /// rather than falling back to an unconditional `get`: silently dropping
    /// the pin would serve bytes from a replaced file. An unaddressable key is
    /// refused with [`StoreError::UnaddressableKey`] first, as every key
    /// operation refuses it.
    /// [`crate::external::probe::probe_preconditions`] is how a candidate store
    /// is qualified for this before any grant relies on it.
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        let _ = (range, pin);
        check_addressable(key)?;
        Err(StoreError::Unsupported {
            operation: format!("conditional get of {key}"),
        })
    }

    /// [`get`](Self::get), reporting the [`Pin`] for the bytes it returned.
    ///
    /// The read that records a grant (a Parquet footer read at
    /// `CREATE EXTERNAL TABLE` time) needs the object's identity *for the bytes
    /// it just read*, not for whatever a separate HEAD finds. A store that
    /// versions objects reports its version selector here, so the recorded pin
    /// survives the owner overwriting the key a moment later.
    ///
    /// The default implementation reads through `get` and reports an ETag-only
    /// pin, which is what a backend with no version selector to give can
    /// honestly say.
    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        let outcome = self.get(key, range).await?;
        let pin = Pin::etag(outcome.etag.0.clone());
        Ok(PinnedRead { outcome, pin })
    }

    /// The object's metadata and the [`Pin`] that names it, from one HEAD.
    ///
    /// [`ObjectMeta::version`] cannot answer this: it is the CAS [`Version`],
    /// which on S3 is the ETag again, so a caller building a pin out of it
    /// would record a `versionId` no bucket has. This method is the only
    /// supported way to learn a backend's real version selector for an object.
    ///
    /// The default implementation reports an ETag-only pin. A backend whose
    /// store versions objects overrides it ([`crate::s3::S3Store`] fills the
    /// `x-amz-version-id`, `None` on an unversioned bucket;
    /// [`crate::memory::MemoryStore`] reports its own version), and every
    /// decorator in this crate forwards it.
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        let meta = self.head(key).await?;
        let pin = Pin::etag(meta.etag.0.clone());
        Ok((meta, pin))
    }

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
                unaddressable: page
                    .unaddressable
                    .into_iter()
                    .filter(|skipped| skipped.key.as_str() > after)
                    .collect(),
            }),
            None => Ok(page),
        }
    }

    /// One-level listing with delimiter `/`.
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError>;

    /// Idempotent: deleting a missing key succeeds.
    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    fn capabilities(&self) -> Capabilities;

    /// The store's own clock, in unix nanoseconds, as this backend last
    /// observed it, or `None` when it has no observation (ADR-1685 decision 1).
    ///
    /// A writer stamps its ingest-hour bucket from its own clock and has no
    /// second time source to check that reading against. This is that source:
    /// a backend that talks to a remote store can report what the store said
    /// the time was, without an extra request or a new object.
    ///
    /// **For a store whose `Date` is correct it is a lower bound on the
    /// store's current time, never an estimate of it.** The store stamped the
    /// value before the response left it, and it is not advanced by elapsed
    /// time, so it only under-reports; a caller may use it to bound how far
    /// *behind* the store its own clock is, and must not use it to bound how
    /// far ahead. It can be arbitrarily stale in a process that has issued no
    /// requests, and is not monotonic: the latest observation wins, so a store
    /// (or a proxy) that answers one request with a wrong `Date` moves it,
    /// backwards or forwards, until the next response replaces it.
    ///
    /// The default is `None`, which is the honest answer for a backend with no
    /// remote store behind it, so a backend need not implement it. Every
    /// decorator in this crate delegates to the store it wraps; a decorator
    /// that did not would silently disable the caller's check.
    fn observed_store_time_ns(&self) -> Option<i64> {
        None
    }
}

/// A shared handle is itself a backend, forwarding every method to the pointee.
///
/// `?Sized` so this covers `Arc<dyn ObjectStoreBackend>` as well as
/// `Arc<ConcreteStore>`, which is what lets a decorator whose type parameter is
/// `S: ObjectStoreBackend` (for example [`InstrumentedStore`]) wrap an
/// already-type-erased `Arc<dyn ObjectStoreBackend>`: without this impl the
/// erased handle is not itself a backend and cannot be a decorator's `S`. Every
/// method delegates to the pointee, `put_multipart`, `get_pinned`,
/// `get_with_pin`, `pin_of` and `capabilities` included, so a `multipart: true`
/// backend keeps that capability through the `Arc`, and a backend that
/// evaluates pins and reports its version selector keeps that too, rather than
/// falling back to the refusing and ETag-only defaults.
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

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        (**self).get_pinned(key, range, pin).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        (**self).get_with_pin(key, range).await
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        (**self).pin_of(key).await
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

    fn observed_store_time_ns(&self) -> Option<i64> {
        (**self).observed_store_time_ns()
    }
}

/// Page ceiling for a single [`list_all`] or [`drain_pages`] drain.
///
/// At the contract's 1000-key page size (docs/object-store-contract.md,
/// "Listing"), 100 000 pages bounds one drain to 100 million keys, far above
/// any prefix a caller drains: the widest is a whole-tenant or whole-store
/// prefix during maintenance and benchmarks, orders of magnitude below this.
/// It only ever trips on a backend that never terminates. The repeated-token
/// guard below catches the common spin (a backend handing back the same
/// continuation token); this ceiling is the backstop for a token that keeps
/// changing without advancing.
pub const MAX_LIST_PAGES: usize = 100_000;

/// What a [`drain_pages`] key hook wants the drain to do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStep {
    /// Keep going: finish this page, then request the next one if the backend
    /// offers a continuation token.
    Continue,
    /// Stop the drain now, without visiting the rest of this page and without
    /// requesting another page. For a caller whose keys sort past the range it
    /// wants: the listing is ordered, so every later key is past it too.
    Stop,
}

/// Drain a paged listing under `prefix` (resuming after `start_after`, when
/// given), bounded, for every caller that pages a prefix.
///
/// The caller supplies the two hooks the drain has no business owning:
///
/// - `fetch` issues one page. It receives `start_after` and the continuation
///   token to use, and is where the per-page work lives: an in-flight permit
///   held across the request, request accounting, a caller's own request
///   ceiling, and the choice of [`ObjectStoreBackend::list`] or
///   [`ObjectStoreBackend::list_after`]. The permit is the reason the listing
///   call sits here rather than inside the drain: it is acquired and released
///   per page, so it has to wrap the request itself. It is a plain `FnMut`
///   returning a future, and takes `start_after` owned, rather than an async
///   closure over a borrow: both keep the future the drain builds free of
///   higher-ranked lifetimes, so a caller whose own future is `tokio::spawn`ed
///   stays `Send`. Anything the fetch hook must mutate across pages (a request
///   counter) goes through a shared cell the future can hold by reference, such
///   as an [`std::sync::atomic::AtomicU64`], since the future cannot borrow
///   from the closure and must itself stay `Send`.
/// - `keys` receives each key that survives the dedup, in delivery order. It
///   returns [`DrainStep::Stop`] to end the drain early, or a caller error (a
///   tenant-prefix isolation assertion, a malformed key).
///
/// What the drain owns is the bound and the contract's listing rules
/// (docs/object-store-contract.md, "Listing"):
///
/// - The raw delivery sequence is non-decreasing, and a repeat re-delivers the
///   last key already delivered, so the dedup holds only that last key rather
///   than a set of every key: an equal key is dropped, a strictly smaller key
///   breaks the order guarantee and becomes
///   [`StoreError::ListOrderViolation`], and a larger key is passed to `keys`.
///   That is constant extra memory whatever the prefix holds.
/// - A backend that never terminates is a typed error, never a spin: a
///   repeated continuation token is [`StoreError::ListRepeatedToken`], and a
///   token that keeps changing without ending trips `max_pages` as
///   [`StoreError::ListPageCeiling`]. Pass [`MAX_LIST_PAGES`] unless a test
///   needs a smaller ceiling.
///
/// It returns the [`ListPage::unaddressable`] keys of every page it fetched,
/// summed into an [`Unaddressable`], also when `keys` stops it early. The order
/// and dedup rules above see only `objects`; an unaddressable key equal to the
/// one recorded just before it is a repeat and is not counted twice.
pub async fn drain_pages<E, Fetch, Fut, Keys>(
    prefix: &str,
    start_after: Option<&str>,
    max_pages: usize,
    mut fetch: Fetch,
    mut keys: Keys,
) -> Result<Unaddressable, E>
where
    E: From<StoreError>,
    Fetch: FnMut(Option<String>, Option<PageToken>) -> Fut,
    Fut: std::future::Future<Output = Result<ListPage, E>>,
    Keys: FnMut(ObjectMeta) -> Result<DrainStep, E>,
{
    let start_after = start_after.map(str::to_string);
    let mut last_key: Option<String> = None;
    let mut page_token: Option<PageToken> = None;
    let mut prev_token: Option<PageToken> = None;
    let mut pages = 0usize;
    let mut unaddressable = Unaddressable::default();
    let mut last_unaddressable: Option<String> = None;
    loop {
        if pages >= max_pages {
            return Err(StoreError::ListPageCeiling {
                prefix: prefix.to_string(),
                ceiling: max_pages,
            }
            .into());
        }
        pages += 1;
        let page = fetch(start_after.clone(), page_token).await?;
        for skipped in page.unaddressable {
            if last_unaddressable.as_deref() != Some(skipped.key.as_str()) {
                last_unaddressable = Some(skipped.key.clone());
                unaddressable.record(skipped);
            }
        }
        for meta in page.objects {
            match last_key.as_deref() {
                // The raw delivery sequence never decreases and a repeat
                // re-delivers the last key, so a key at or below the last
                // delivered one is either that same key again (dropped) or a
                // backend that broke ordering (a typed error).
                Some(last) if meta.key.as_str() < last => {
                    return Err(StoreError::ListOrderViolation {
                        prefix: prefix.to_string(),
                        previous: last.to_string(),
                        offending: meta.key,
                    }
                    .into());
                }
                Some(last) if meta.key.as_str() == last => {}
                _ => {
                    last_key = Some(meta.key.clone());
                    if keys(meta)? == DrainStep::Stop {
                        return Ok(unaddressable);
                    }
                }
            }
        }
        match page.next {
            Some(next) => {
                if prev_token.as_ref() == Some(&next) {
                    return Err(StoreError::ListRepeatedToken {
                        prefix: prefix.to_string(),
                    }
                    .into());
                }
                prev_token = Some(next.clone());
                page_token = Some(next);
            }
            None => return Ok(unaddressable),
        }
    }
}

/// Drain every page of a [`ObjectStoreBackend::list`], deduplicating by key
/// per the cross-page guarantee. Convenience for callers with bounded prefixes
/// that need no per-page hook: [`drain_pages`] holds the bound and the listing
/// rules, and this adds only "collect every key".
pub async fn list_all(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> Result<Vec<ObjectMeta>, StoreError> {
    Ok(drain_list(store, prefix, MAX_LIST_PAGES).await?.objects)
}

/// [`list_all`], also returning the unaddressable keys the listing skipped,
/// for a caller that reports them.
pub async fn list_all_reporting(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> Result<Listing, StoreError> {
    drain_list(store, prefix, MAX_LIST_PAGES).await
}

/// [`list_all_reporting`] with an explicit page ceiling, so a test can
/// exercise the [`StoreError::ListPageCeiling`] path without draining 100 000
/// pages.
async fn drain_list(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
    max_pages: usize,
) -> Result<Listing, StoreError> {
    let mut objects: Vec<ObjectMeta> = Vec::new();
    let unaddressable = drain_pages(
        prefix,
        None,
        max_pages,
        |_start_after, token| async move { store.list(prefix, token).await },
        |meta| {
            objects.push(meta);
            Ok(DrainStep::Continue)
        },
    )
    .await?;
    Ok(Listing {
        objects,
        unaddressable,
    })
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
                unaddressable: Vec::new(),
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
    /// Scripted keys are classified like a real backend's, so an unaddressable
    /// one lands in `unaddressable`.
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
            prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let listed = self.pages[n].iter().map(|k| Self::meta(k)).collect();
            let (objects, unaddressable) = classify_objects(prefix, listed);
            let next = if n + 1 < self.pages.len() {
                Some(PageToken(format!("tok-{n}")))
            } else {
                None
            };
            Ok(ListPage {
                objects,
                next,
                unaddressable,
            })
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
            .objects
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

    /// A page that overlaps its predecessor by more than one key is where the
    /// two readings of the cross-page guarantee part. Under the contract's
    /// rule (the raw sequence never decreases, and a repeat re-delivers the
    /// last key delivered) the `p/b` after `p/c` is out of order and the drain
    /// says so; under a looser "any key may repeat" reading it would be
    /// dropped by a set dedup and the drain would return four keys. The
    /// conformance suite certifies the same rule, so no backend it qualifies
    /// reaches this error.
    #[tokio::test]
    async fn a_multi_key_page_overlap_is_a_typed_order_violation() {
        let store = ScriptedList::new(&[&["p/a", "p/b", "p/c"], &["p/b", "p/c", "p/d"]]);
        let err = drain_list(&store, "p/", MAX_LIST_PAGES)
            .await
            .expect_err("an overlap that re-delivers more than the last key must be rejected");
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

    /// Counts `list` calls while delegating everything to the pagination
    /// oracle, so a test can assert the exact number of pages a drain issued
    /// rather than claim it in a comment.
    struct CountingList {
        inner: MemoryStore,
        list_calls: AtomicUsize,
    }

    impl CountingList {
        fn with_page_size(page_size: usize) -> Self {
            CountingList {
                inner: MemoryStore::with_page_size(page_size),
                list_calls: AtomicUsize::new(0),
            }
        }

        fn list_call_count(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for CountingList {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// A compliant backend drained over several real pages returns every key
    /// once, in lexicographic order, identical to the pre-bound behavior. Page
    /// size 2 over five keys really is a multi-page drain: exactly three pages,
    /// counted at the store.
    #[tokio::test]
    async fn a_compliant_backend_returns_every_key_once_in_order() {
        let store = CountingList::with_page_size(2);
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
        assert_eq!(
            store.list_call_count(),
            3,
            "five keys at page size 2 is exactly three pages"
        );
    }

    /// The drain sums every page's unaddressable keys into one count, and keeps
    /// only the first `UNADDRESSABLE_SAMPLE_MAX` of them, in listing order, as
    /// the sample. Twenty foreign keys at page size 2 span many pages.
    #[tokio::test]
    async fn the_drain_sums_unaddressable_keys_and_caps_the_sample() {
        let store = MemoryStore::with_page_size(2);
        let mut foreign: Vec<String> = (0..20).map(|i| format!("p/{i:02}*")).collect();
        foreign.sort();
        for key in &foreign {
            store.insert_foreign(key, Bytes::from_static(b"x"));
        }
        for key in ["p/00", "p/10", "p/19"] {
            store
                .put(
                    key,
                    Bytes::from_static(b"x"),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("seed addressable key");
        }

        let listing = list_all_reporting(&store, "p/").await.expect("drain");
        let keys: Vec<&str> = listing.objects.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["p/00", "p/10", "p/19"]);
        assert_eq!(listing.unaddressable.count, 20);
        let sample: Vec<&str> = listing
            .unaddressable
            .sample
            .iter()
            .map(|skipped| skipped.key.as_str())
            .collect();
        let expected: Vec<&str> = foreign
            .iter()
            .take(UNADDRESSABLE_SAMPLE_MAX)
            .map(String::as_str)
            .collect();
        assert_eq!(UNADDRESSABLE_SAMPLE_MAX, 16);
        assert_eq!(sample, expected, "the first 16 in listing order");
        assert_eq!(listing.unaddressable.sample[0].addresses, "p/00%2A");
        assert_eq!(listing.unaddressable.sample[0].size, 1);
    }

    /// An unaddressable key re-delivered at a page boundary is the
    /// contract-permitted repeat, and is counted once.
    #[tokio::test]
    async fn an_unaddressable_repeat_at_a_page_boundary_is_counted_once() {
        let store = ScriptedList::new(&[&["p/a", "p/b*"], &["p/b*", "p/c"]]);
        let listing = drain_list(&store, "p/", MAX_LIST_PAGES)
            .await
            .expect("a permitted repeat must not error");
        let keys: Vec<&str> = listing.objects.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["p/a", "p/c"]);
        assert_eq!(listing.unaddressable.count, 1);
        assert_eq!(listing.unaddressable.sample[0].key, "p/b*");
    }

    /// A drain the key hook stops early still returns the unaddressable keys
    /// of the pages it fetched.
    #[tokio::test]
    async fn an_early_stop_still_returns_the_unaddressable_count() {
        let store = ScriptedList::new(&[&["p/a*", "p/b"], &["p/c"]]);
        let skipped = drain_pages::<StoreError, _, _, _>(
            "p/",
            None,
            MAX_LIST_PAGES,
            |_start_after, token| store.list("p/", token),
            |_meta| Ok(DrainStep::Stop),
        )
        .await
        .expect("drain");
        assert_eq!(skipped.count, 1);
        assert_eq!(store.call_count(), 1, "the stop ends the drain on page one");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod addressable_tests {
    use bytes::Bytes;
    use proptest::prelude::*;

    use super::*;
    use crate::memory::MemoryStore;

    #[test]
    fn keys_the_adapter_rewrites_are_not_addressable() {
        for key in ["", "a", "a/b", "a b/c+d", "t/0123/x.pqm"] {
            assert!(is_addressable_key(key), "{key:?}");
        }
        for key in [
            "a/b\u{1}c",
            "a/*b",
            "a//b",
            "/a",
            "a/",
            "a/./b",
            "a/../b",
            "a#b",
            "a%b",
            "é",
        ] {
            assert!(!is_addressable_key(key), "{key:?}");
        }
    }

    #[test]
    fn a_common_prefix_is_judged_by_its_stem() {
        for prefix in ["", "t/", "t/abc/", "a b/"] {
            assert!(is_addressable_prefix(prefix), "{prefix:?}");
        }
        for prefix in ["/", "t/abc\u{1}/", "t//", "t/*/", "./"] {
            assert!(!is_addressable_prefix(prefix), "{prefix:?}");
        }
    }

    #[test]
    fn the_error_escapes_the_key_and_is_not_retryable() {
        let err = check_addressable("a/b\u{1}c").expect_err("refused");
        assert_eq!(
            err.to_string(),
            r#"key "a/b\u{1}c" is not addressable: a request for it would reach "a/b%01c""#
        );
        assert!(!err.is_retryable());
    }

    /// Below the cap every new key warns once; at the cap, one listing in
    /// `warn_every` that skips a new key warns, naming its first new key.
    #[test]
    fn warnings_are_once_per_key_then_rate_limited_past_the_cap() {
        let mut state = UnaddressableWarnState {
            warned: std::collections::BTreeSet::new(),
            overflow: 0,
        };
        let warned = keys_to_warn(
            &mut state,
            [("a*", "a%2A"), ("b*", "b%2A")].into_iter(),
            2,
            3,
        );
        assert_eq!(warned, vec![("a*", "a%2A"), ("b*", "b%2A")]);
        let again = keys_to_warn(&mut state, [("a*", "a%2A")].into_iter(), 2, 3);
        assert!(again.is_empty(), "a key already warned about is silent");

        let mut warned_listings = 0;
        for _ in 0..6 {
            let listing = [("a*", "a%2A"), ("c*", "c%2A"), ("d*", "d%2A")];
            let warned = keys_to_warn(&mut state, listing.into_iter(), 2, 3);
            if !warned.is_empty() {
                assert_eq!(warned, vec![("c*", "c%2A")]);
                warned_listings += 1;
            }
        }
        assert_eq!(warned_listings, 2, "listings 1 and 4 of 6 past the cap");
        assert_eq!(state.warned.len(), 2, "the set stays at its cap");
        assert_eq!(state.overflow, 6);
    }

    fn key_strategy() -> impl Strategy<Value = String> {
        prop_oneof![any::<String>(), "[ab/. *#%+\\\\\u{1}é]{0,10}"]
    }

    proptest! {
        /// The predicate is exactly `Path::from` round-tripping, and the
        /// oracle refuses exactly the keys it rejects.
        #[test]
        fn memory_store_refuses_exactly_the_unaddressable_keys(key in key_strategy()) {
            let addressable = object_store::path::Path::from(key.as_str()).as_ref() == key;
            prop_assert_eq!(is_addressable_key(&key), addressable);

            let store = MemoryStore::new();
            let put = futures::executor::block_on(store.put(
                &key,
                Bytes::from_static(b"x"),
                PutOptions::create_if_absent(),
            ));
            let head = futures::executor::block_on(store.head(&key));
            let delete = futures::executor::block_on(store.delete(&key));
            for refused in [
                matches!(put, Err(StoreError::UnaddressableKey { .. })),
                matches!(head, Err(StoreError::UnaddressableKey { .. })),
                matches!(delete, Err(StoreError::UnaddressableKey { .. })),
            ] {
                prop_assert_eq!(refused, !addressable);
            }
            if addressable {
                prop_assert!(put.is_ok() && head.is_ok() && delete.is_ok());
            }
        }
    }
}

/// [`ObjectStoreBackend::observed_store_time_ns`] (ADR-1685 decision 1) at the
/// trait boundary: the default, the oracle's test-only setter, and the
/// `Arc<T>` forwarding impl. Each decorator's own delegation is asserted in its
/// own module, against the decorator's real constructor.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod observed_store_time_tests {
    use async_trait::async_trait;
    use bytes::Bytes;

    use super::*;
    use crate::memory::MemoryStore;

    /// A backend that implements only the required methods, so
    /// `observed_store_time_ns` is the trait's default. Nothing calls its
    /// operations; they exist because the trait requires them.
    struct NoObservationStore;

    #[async_trait]
    impl ObjectStoreBackend for NoObservationStore {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            Err(StoreError::Permanent("not used by this test".into()))
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            Err(StoreError::Permanent("not used by this test".into()))
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            Err(StoreError::Permanent("not used by this test".into()))
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            Err(StoreError::Permanent("not used by this test".into()))
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            Err(StoreError::Permanent("not used by this test".into()))
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            Err(StoreError::Permanent("not used by this test".into()))
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// A backend that does not implement the method observes nothing, which is
    /// what makes it a defaulted method rather than a contract change for a
    /// third-party implementation.
    #[test]
    fn the_default_is_no_observation() {
        assert_eq!(NoObservationStore.observed_store_time_ns(), None);
    }

    /// The oracle serves no responses, so it observes nothing until a test says
    /// otherwise, and `None` puts it back.
    #[test]
    fn the_oracle_reports_only_what_a_test_sets() {
        let store = MemoryStore::new();
        assert_eq!(store.observed_store_time_ns(), None);
        store.set_observed_store_time_ns(Some(1_700_000_000_000_000_000));
        assert_eq!(
            store.observed_store_time_ns(),
            Some(1_700_000_000_000_000_000)
        );
        store.set_observed_store_time_ns(None);
        assert_eq!(store.observed_store_time_ns(), None);
    }

    /// The `Arc<T>` impl forwards to the pointee, so an already-type-erased
    /// `Arc<dyn ObjectStoreBackend>` (what every decorator in `ravel-server`'s
    /// chain wraps) does not drop the observation on the floor.
    #[test]
    fn a_shared_handle_forwards_to_the_pointee() {
        let inner = MemoryStore::new();
        inner.set_observed_store_time_ns(Some(42));
        let shared: Arc<dyn ObjectStoreBackend> = Arc::new(inner);
        assert_eq!(shared.observed_store_time_ns(), Some(42));

        let erased_twice: Arc<dyn ObjectStoreBackend> = Arc::new(Arc::clone(&shared));
        assert_eq!(erased_twice.observed_store_time_ns(), Some(42));
    }
}
