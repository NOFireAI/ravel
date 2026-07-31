//! Read-path GET accounting wrappers (GitHub issue #97 phase b).
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
//! S3/MinIO backend would see. A `head` request (size probe) is counted
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
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
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
#[derive(Debug, Default)]
pub struct Counters {
    get_count: AtomicU64,
    get_bytes: AtomicU64,
    head_count: AtomicU64,
}

impl Counters {
    fn record_get(&self, bytes: u64) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        self.get_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_head(&self) {
        self.head_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of GET requests issued to the inner store since the last
    /// [`reset`](Self::reset).
    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    /// Total bytes returned across all GET requests.
    pub fn get_bytes(&self) -> u64 {
        self.get_bytes.load(Ordering::Relaxed)
    }

    /// Number of HEAD (size-probe) requests issued to the inner store.
    pub fn head_count(&self) -> u64 {
        self.head_count.load(Ordering::Relaxed)
    }

    /// A snapshot of the three counters as `(gets, bytes, heads)`.
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (self.get_count(), self.get_bytes(), self.head_count())
    }

    /// Zero every counter. Call between measured access patterns so each is
    /// accounted independently.
    pub fn reset(&self) {
        self.get_count.store(0, Ordering::Relaxed);
        self.get_bytes.store(0, Ordering::Relaxed);
        self.head_count.store(0, Ordering::Relaxed);
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

    async fn head(&self, key: &str) -> Result<RavelObjectMeta, StoreError> {
        self.counters.record_head();
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // multipart: false so the flag matches the refusing default
        // `put_multipart` this double inherits (issue #298).
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
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
