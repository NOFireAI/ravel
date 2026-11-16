//! A test store whose `sys/compat` GET fails with a chosen [`StoreError`],
//! for the activation policy's blind-view rule (ADR-4233 decision 3): only
//! a real [`StoreError::AccessDenied`] models a credential without the
//! `sys/compat` grant, and any other error must not read as one.

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use ravel_catalog::compat::COMPAT_KEY;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};

/// How a [`CompatGetStore`] answers a GET of `sys/compat`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompatGet {
    /// Served by the inner store.
    Passthrough,
    /// [`StoreError::AccessDenied`], as from a credential without the grant.
    AccessDenied,
    /// [`StoreError::Transient`] carrying a 503, which says nothing about the
    /// grant.
    ServerError,
}

/// A [`MemoryStore`] whose GETs of [`COMPAT_KEY`] answer as its
/// [`CompatGet`] says. Every other call, and every other key, goes to the
/// inner store.
pub(crate) struct CompatGetStore {
    inner: MemoryStore,
    get: CompatGet,
    failed: AtomicUsize,
}

impl CompatGetStore {
    pub(crate) fn new(get: CompatGet) -> Self {
        CompatGetStore {
            inner: MemoryStore::new(),
            get,
            failed: AtomicUsize::new(0),
        }
    }

    /// How many `sys/compat` GETs this store has failed.
    pub(crate) fn failed(&self) -> usize {
        self.failed.load(Ordering::SeqCst)
    }

    fn refusal(&self, key: &str) -> Option<StoreError> {
        if key != COMPAT_KEY {
            return None;
        }
        let err = match self.get {
            CompatGet::Passthrough => return None,
            CompatGet::AccessDenied => StoreError::AccessDenied("no s3:GetObject".to_string()),
            CompatGet::ServerError => StoreError::Transient("503 Service Unavailable".to_string()),
        };
        self.failed.fetch_add(1, Ordering::SeqCst);
        Some(err)
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for CompatGetStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if let Some(err) = self.refusal(key) {
            return Err(err);
        }
        self.inner.get(key, range).await
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        if let Some(err) = self.refusal(key) {
            return Err(err);
        }
        self.inner.get_pinned(key, range, pin).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        if let Some(err) = self.refusal(key) {
            return Err(err);
        }
        self.inner.get_with_pin(key, range).await
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.inner.pin_of(key).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
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
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}
