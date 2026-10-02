//! Read-path GET accounting wrappers.
//!
//! Two counting wrappers, both delegating every call to an inner store while
//! tallying GET requests and the bytes those GETs actually transferred:
//!
//!   * [`CountingBackend`] wraps a [`ravel_object_store::ObjectStoreBackend`]
//!     (the RSEG read path fetches ranged bytes through this trait).
//!   * [`CountingObjectStore`] wraps an [`object_store::ObjectStore`] (the
//!     async Parquet reader fetches through this crate's own trait).
//!
//! These live in the bench crate, never in a library crate: they are a
//! measurement tool, not part of any durability argument. They mirror the
//! `FaultStore<S>` wrapper pattern from `ravel-object-store` (delegate,
//! observe, expose counters), but observe rather than inject.
//!
//! "GET count" is the number of GET operations issued to the inner store.
//! For [`CountingObjectStore`] every ranged and multi-ranged read funnels
//! through `get_opts` (the `object_store` trait routes `get`, `get_range`,
//! and `get_ranges` through it by default, coalescing adjacent ranges first),
//! so counting there captures the real, post-coalescing request count a live
//! S3 backend would see. A `head` request (size probe) is counted
//! separately and never as a GET. "bytes transferred" is the length of the
//! byte range each GET returned, summed.
//!
//! `expect`/`unwrap` are not used here; all methods return the inner store's
//! own `Result`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta as RavelObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};

use object_store::path::Path as OsPath;
use object_store::{
    CopyOptions, GetOptions as OsGetOptions, GetRange as OsGetRange, GetResult, ListResult,
    ObjectMeta as OsObjectMeta, ObjectStore, PutMultipartOptions, PutOptions as OsPutOptions,
    PutPayload, PutResult, Result as OsResult,
};

/// GET-request and transferred-byte counters shared by both wrappers.
///
/// `head_count` tracks size-probe HEAD requests (and `object_store`
/// head-flavored `get_opts` calls) separately: these move no bytes and are
/// not GETs, but a reader that pays a HEAD round trip per object should not
/// hide it.
///
/// `pinned_get_count`/`pinned_get_bytes` track `get_pinned`/`get_with_pin`
/// calls (the Parquet reader's conditional read path, issue #2391)
/// separately from an unconditional `get`, but a pinned read is still one GET
/// on the wire: it also bumps `get_count`/`get_bytes`, so those stay totals
/// across every read path rather than undercounting whichever path a caller
/// used. `pin_of_count` tracks `pin_of` calls and, for the same reason,
/// bumps `head_count` too: a `pin_of` is one HEAD on the wire.
#[derive(Debug, Default)]
pub struct Counters {
    get_count: AtomicU64,
    get_bytes: AtomicU64,
    head_count: AtomicU64,
    pinned_get_count: AtomicU64,
    pinned_get_bytes: AtomicU64,
    pin_of_count: AtomicU64,
}

impl Counters {
    fn record_get(&self, bytes: u64) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        self.get_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_head(&self) {
        self.head_count.fetch_add(1, Ordering::Relaxed);
    }

    /// A `get_pinned`/`get_with_pin` call: one GET on the wire, so this also
    /// bumps `get_count`/`get_bytes` via [`Self::record_get`].
    fn record_pinned_get(&self, bytes: u64) {
        self.pinned_get_count.fetch_add(1, Ordering::Relaxed);
        self.pinned_get_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.record_get(bytes);
    }

    /// A `pin_of` call: one HEAD on the wire, so this also bumps `head_count`
    /// via [`Self::record_head`].
    fn record_pin_of(&self) {
        self.pin_of_count.fetch_add(1, Ordering::Relaxed);
        self.record_head();
    }

    /// Number of GET requests issued to the inner store since the last
    /// [`reset`](Self::reset). Includes pinned reads
    /// ([`Self::pinned_get_count`]): this is the total GET count regardless
    /// of which read path a caller used.
    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    /// Total bytes returned across all GET requests, pinned reads included.
    pub fn get_bytes(&self) -> u64 {
        self.get_bytes.load(Ordering::Relaxed)
    }

    /// Number of HEAD (size-probe) requests issued to the inner store.
    /// Includes `pin_of` calls ([`Self::pin_of_count`]): this is the total
    /// HEAD count regardless of which call made it.
    pub fn head_count(&self) -> u64 {
        self.head_count.load(Ordering::Relaxed)
    }

    /// Number of `get_pinned`/`get_with_pin` calls issued to the inner store.
    pub fn pinned_get_count(&self) -> u64 {
        self.pinned_get_count.load(Ordering::Relaxed)
    }

    /// Total bytes returned across all `get_pinned`/`get_with_pin` calls.
    pub fn pinned_get_bytes(&self) -> u64 {
        self.pinned_get_bytes.load(Ordering::Relaxed)
    }

    /// Number of `pin_of` calls issued to the inner store.
    pub fn pin_of_count(&self) -> u64 {
        self.pin_of_count.load(Ordering::Relaxed)
    }

    /// A snapshot of the three original counters as `(gets, bytes, heads)`.
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (self.get_count(), self.get_bytes(), self.head_count())
    }

    /// Zero every counter. Call between measured access patterns so each is
    /// accounted independently.
    pub fn reset(&self) {
        self.get_count.store(0, Ordering::Relaxed);
        self.get_bytes.store(0, Ordering::Relaxed);
        self.head_count.store(0, Ordering::Relaxed);
        self.pinned_get_count.store(0, Ordering::Relaxed);
        self.pinned_get_bytes.store(0, Ordering::Relaxed);
        self.pin_of_count.store(0, Ordering::Relaxed);
    }
}

// ------------------------------------------------------ RSEG-side wrapper

/// Wraps a [`ravel_object_store::ObjectStoreBackend`], counting every `get`
/// and the bytes it returned. All other operations delegate untouched.
pub struct CountingBackend<S> {
    inner: S,
    counters: Arc<Counters>,
}

impl<S: ObjectStoreBackend> CountingBackend<S> {
    pub fn new(inner: S) -> Self {
        CountingBackend {
            inner,
            counters: Arc::new(Counters::default()),
        }
    }

    /// Shared handle to this wrapper's counters (clone-friendly; the wrapper
    /// keeps its own `Arc` too).
    pub fn counters(&self) -> Arc<Counters> {
        Arc::clone(&self.counters)
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        self.counters.snapshot()
    }

    pub fn reset(&self) {
        self.counters.reset();
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

#[async_trait]
impl<S: ObjectStoreBackend> ObjectStoreBackend for CountingBackend<S> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let outcome = self.inner.get(key, range).await?;
        self.counters.record_get(outcome.data.len() as u64);
        Ok(outcome)
    }

    /// Counted via [`Counters::record_pinned_get`]: a pinned read is still
    /// one GET on the wire, so it also bumps the plain GET totals.
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        let read = self.inner.get_pinned(key, range, pin).await?;
        self.counters
            .record_pinned_get(read.outcome.data.len() as u64);
        Ok(read)
    }

    /// Counted via [`Counters::record_pinned_get`], for the same reason as
    /// [`Self::get_pinned`].
    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        let read = self.inner.get_with_pin(key, range).await?;
        self.counters
            .record_pinned_get(read.outcome.data.len() as u64);
        Ok(read)
    }

    /// Counted via [`Counters::record_pin_of`]: a `pin_of` is still one HEAD
    /// on the wire, so it also bumps the plain HEAD total.
    async fn pin_of(&self, key: &str) -> Result<(RavelObjectMeta, Pin), StoreError> {
        let result = self.inner.pin_of(key).await?;
        self.counters.record_pin_of();
        Ok(result)
    }

    async fn head(&self, key: &str) -> Result<RavelObjectMeta, StoreError> {
        self.counters.record_head();
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // multipart: false so the flag matches the refusing default
        // `put_multipart` this double inherits.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

// -------------------------------------------------- object_store wrapper

/// Length of the byte range a `get_opts` call returned, derived from the
/// requested range and the object's total size (never from consuming the
/// returned payload stream).
fn returned_len(range: &Option<OsGetRange>, total: u64) -> u64 {
    match range {
        None => total,
        Some(OsGetRange::Bounded(r)) => r.end.min(total).saturating_sub(r.start.min(total)),
        Some(OsGetRange::Offset(o)) => total.saturating_sub(*o),
        Some(OsGetRange::Suffix(n)) => (*n).min(total),
    }
}

/// Wraps an [`object_store::ObjectStore`], counting every GET (`get_opts`,
/// which the trait routes `get`/`get_range`/`get_ranges` through) and the
/// bytes it returned. HEAD-flavored `get_opts` calls (size probes) count as
/// heads, not GETs. All other operations delegate untouched.
#[derive(Debug)]
pub struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<Counters>,
}

impl CountingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        CountingObjectStore {
            inner,
            counters: Arc::new(Counters::default()),
        }
    }

    pub fn counters(&self) -> Arc<Counters> {
        Arc::clone(&self.counters)
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        self.counters.snapshot()
    }

    pub fn reset(&self) {
        self.counters.reset();
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: OsPutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &OsPath, options: OsGetOptions) -> OsResult<GetResult> {
        let is_head = options.head;
        let requested = options.range.clone();
        let result = self.inner.get_opts(location, options).await?;
        if is_head {
            self.counters.record_head();
        } else {
            let total = result.meta.size;
            self.counters.record_get(returned_len(&requested, total));
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<OsPath>>,
    ) -> BoxStream<'static, OsResult<OsPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&OsPath>) -> BoxStream<'static, OsResult<OsObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&OsPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &OsPath, to: &OsPath, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    // `get`, `get_range`, `get_ranges`, `head`, and `delete` are left as the
    // trait/extension defaults: reads route through `get_opts` (get_ranges
    // after coalescing adjacent ranges; `head` as a head-flavored get_opts),
    // so counting in `get_opts` captures every GET (and every HEAD) exactly
    // once at the granularity the inner backend actually sees.
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    /// Issue #2391: `CountingBackend` must forward the pinned-read methods to
    /// its inner store, never falling back to the trait's refusing default.
    #[tokio::test]
    async fn counting_backend_forwards_pinned_reads() {
        let inner = MemoryStore::new();
        inner
            .put(
                "k",
                Bytes::from_static(b"hello"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("seed key");
        let store = CountingBackend::new(inner);

        let (meta, pin) = store.inner().pin_of("k").await.expect("pin_of on inner");
        let _ = meta;

        let with_pin = store
            .get_with_pin("k", GetRange::Full)
            .await
            .expect("get_with_pin");
        assert_eq!(with_pin.outcome.data.as_ref(), b"hello");

        let pinned = store
            .get_pinned("k", GetRange::Full, &pin)
            .await
            .expect("get_pinned with the right pin");
        assert_eq!(pinned.outcome.data.as_ref(), b"hello");

        let wrong_pin = Pin::etag("not-the-real-etag");
        let err = store
            .get_pinned("k", GetRange::Full, &wrong_pin)
            .await
            .expect_err("a wrong ETag must be refused, not served");
        assert!(
            matches!(err, StoreError::PreconditionFailed),
            "got {err:?}, want PreconditionFailed (never Unsupported)"
        );
    }

    /// Two pinned gets (`get_with_pin` then `get_pinned`) plus one `pin_of`
    /// must land in both the new, specific counters and the existing totals:
    /// a pinned get is still a GET, and `pin_of` is still a HEAD.
    #[tokio::test]
    async fn counting_backend_counts_pinned_gets_and_pin_of_into_totals() {
        let inner = MemoryStore::new();
        inner
            .put(
                "k",
                Bytes::from_static(b"hello"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("seed key");
        let store = CountingBackend::new(inner);

        let with_pin = store
            .get_with_pin("k", GetRange::Full)
            .await
            .expect("get_with_pin");
        store
            .get_pinned("k", GetRange::Full, &with_pin.pin)
            .await
            .expect("get_pinned with the pin get_with_pin reported");
        store.pin_of("k").await.expect("pin_of");

        let counters = store.counters();
        assert_eq!(counters.pinned_get_count(), 2);
        assert_eq!(counters.pin_of_count(), 1);
        assert_eq!(counters.get_count(), 2, "a pinned get is still a GET");
        assert_eq!(counters.head_count(), 1, "pin_of is still a HEAD");
        assert_eq!(
            counters.pinned_get_bytes(),
            2 * "hello".len() as u64,
            "two reads of a 5-byte object"
        );
    }
}
