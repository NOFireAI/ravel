//! Shared test fixtures: tenants, manifests, a store wrapper that records
//! which writes landed and which prefixes were listed, and wrappers that
//! handle prefixes and keys the way the S3 adapter does.
#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;

use bytes::Bytes;
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutMode, PutOptions, PutOutcome, StoreError,
    UnaddressableKey,
};
use ravel_types::TenantHash;

use crate::clock::FixedClock;
use crate::manifest::{APPLY_NONCE_LEN, Manifest, ParquetFile};

pub const TENANT_A: TenantHash = TenantHash([0xa1; 16]);
pub const TENANT_B: TenantHash = TenantHash([0xb2; 16]);

/// An external Parquet file in the tenant's own bucket, distinct per `seed`.
pub fn file_for(seed: u8) -> ParquetFile {
    ParquetFile {
        profile: "prod".into(),
        bucket: "customer".into(),
        key: format!("data/part-{seed}.parquet").into_bytes(),
        size: 1 + u64::from(seed),
        etag: format!("etag-{seed}"),
        version: format!("gen-{seed}"),
        row_count: u64::from(seed),
        footer_len: 10,
    }
}

/// A live manifest over `s3://customer/data/` holding one file per seed.
pub fn live_manifest(table: &str, version: u64, seeds: &[u8]) -> Manifest {
    Manifest {
        table: table.to_string(),
        version,
        dropped: false,
        location: "s3://customer/data/".into(),
        grant: "s3://customer/data".into(),
        files: seeds.iter().map(|&s| file_for(s)).collect(),
        options: BTreeMap::new(),
        created_by: "test".into(),
        created_unix_ns: version as i64,
        statement: format!("v{version}"),
        apply_nonce: vec![version as u8; APPLY_NONCE_LEN],
    }
}

/// Wraps a backend and records, per key, how many `put` calls the inner
/// backend accepted, and every prefix that was listed. With
/// `replay_create_if_absent` set, a matching `CreateIfAbsent` put is sent to
/// the inner backend twice and the second result returned, which is what a
/// client that retries after a lost acknowledgement does. With
/// [`CountingStore::bump_clock_on_list`] set, each LIST advances an injected
/// clock, which is how a test ages a writer's resolve without a second task.
/// With `stall_then_land_late` set, the first matching `CreateIfAbsent` put
/// never returns and the object lands only after the next `get` of its key
/// has answered: a request the store received and applied after the writer
/// stopped waiting and checked for it. With
/// [`CountingStore::put_on_nth_list`] set, one object is written just before
/// a chosen LIST is answered, which is how a test puts another writer's
/// commit between a caller's put and its next resolve.
pub struct CountingStore<S> {
    pub inner: S,
    accepted_puts: Mutex<HashMap<String, usize>>,
    listed_prefixes: Mutex<Vec<String>>,
    list_bumps: Mutex<(Option<FixedClock>, VecDeque<i64>)>,
    list_write: Mutex<Option<(usize, String, Bytes)>>,
    list_deletes: Mutex<VecDeque<String>>,
    pub replay_create_if_absent: Option<String>,
    pub stall_then_land_late: Option<String>,
    stalled: Mutex<Option<(String, Bytes)>>,
    stalled_once: Mutex<bool>,
}

impl<S> CountingStore<S> {
    pub fn new(inner: S) -> Self {
        CountingStore {
            inner,
            accepted_puts: Mutex::new(HashMap::new()),
            listed_prefixes: Mutex::new(Vec::new()),
            list_bumps: Mutex::new((None, VecDeque::new())),
            list_write: Mutex::new(None),
            list_deletes: Mutex::new(VecDeque::new()),
            replay_create_if_absent: None,
            stall_then_land_late: None,
            stalled: Mutex::new(None),
            stalled_once: Mutex::new(false),
        }
    }

    /// Keys with at least one accepted `put`, and how many each had.
    pub fn accepted_puts(&self) -> HashMap<String, usize> {
        self.accepted_puts.lock().expect("lock").clone()
    }

    /// Every prefix passed to `list`, in call order, one entry per page.
    pub fn listed_prefixes(&self) -> Vec<String> {
        self.listed_prefixes.lock().expect("lock").clone()
    }

    /// How many `list` calls reached this store.
    pub fn list_count(&self) -> usize {
        self.listed_prefixes.lock().expect("lock").len()
    }

    /// Advance `clock` by each of `bumps` in turn, one per LIST call. A LIST
    /// past the end of the list advances nothing.
    pub fn bump_clock_on_list(&self, clock: FixedClock, bumps: impl IntoIterator<Item = i64>) {
        *self.list_bumps.lock().expect("lock") = (Some(clock), bumps.into_iter().collect());
    }

    /// Write `bytes` at `key` just before the `nth` `list` call (1-based) is
    /// answered, so the listing that follows already sees it.
    pub fn put_on_nth_list(&self, nth: usize, key: String, bytes: Bytes) {
        *self.list_write.lock().expect("lock") = Some((nth, key, bytes));
    }

    /// Delete one of `keys` after each `list` is answered, in order: an object
    /// that is listed and then gone before the caller can read it.
    pub fn delete_after_each_list(&self, keys: impl IntoIterator<Item = String>) {
        *self.list_deletes.lock().expect("lock") = keys.into_iter().collect();
    }

    fn record_put(&self, key: &str) {
        *self
            .accepted_puts
            .lock()
            .expect("lock")
            .entry(key.to_string())
            .or_insert(0) += 1;
    }

    /// Records the call and returns its 1-based index.
    fn record_list(&self, prefix: &str) -> usize {
        let nth = {
            let mut listed = self.listed_prefixes.lock().expect("lock");
            listed.push(prefix.to_string());
            listed.len()
        };
        let mut bumps = self.list_bumps.lock().expect("lock");
        if let (Some(clock), queue) = &mut *bumps
            && let Some(delta) = queue.pop_front()
        {
            clock.advance(delta);
        }
        nth
    }
}

impl<S: ObjectStoreBackend> CountingStore<S> {
    /// Applies the [`CountingStore::put_on_nth_list`] write when `nth` is its
    /// call.
    async fn write_before_list(&self, nth: usize) -> Result<(), StoreError> {
        let due = {
            let mut pending = self.list_write.lock().expect("lock");
            match &*pending {
                Some((n, _, _)) if *n == nth => pending.take(),
                _ => None,
            }
        };
        if let Some((_, key, data)) = due {
            self.inner
                .put(&key, data, PutOptions::create_if_absent())
                .await?;
            self.record_put(&key);
        }
        Ok(())
    }

    /// Applies the next [`CountingStore::delete_after_each_list`] deletion.
    async fn delete_after_list(&self) -> Result<(), StoreError> {
        let due = self.list_deletes.lock().expect("lock").pop_front();
        if let Some(key) = due {
            self.inner.delete(&key).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<S: ObjectStoreBackend> ObjectStoreBackend for CountingStore<S> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        let replay = opts.mode == PutMode::CreateIfAbsent
            && self
                .replay_create_if_absent
                .as_deref()
                .is_some_and(|p| key.contains(p));
        let stall = opts.mode == PutMode::CreateIfAbsent
            && self
                .stall_then_land_late
                .as_deref()
                .is_some_and(|p| key.contains(p))
            && !std::mem::replace(&mut *self.stalled_once.lock().expect("lock"), true);
        if stall {
            *self.stalled.lock().expect("lock") = Some((key.to_string(), data));
            return std::future::pending().await;
        }
        if replay
            && self
                .inner
                .put(key, data.clone(), opts.clone())
                .await
                .is_ok()
        {
            self.record_put(key);
        }
        let out = self.inner.put(key, data, opts).await?;
        self.record_put(key);
        Ok(out)
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let out = self.inner.get(key, range).await;
        let landing = {
            let mut stalled = self.stalled.lock().expect("lock");
            match &*stalled {
                Some((k, _)) if k == key => stalled.take(),
                _ => None,
            }
        };
        if let Some((k, data)) = landing {
            self.inner
                .put(&k, data, PutOptions::create_if_absent())
                .await?;
            self.record_put(&k);
        }
        out
    }

    /// Forwarded so the wrapped backend's pinned-read semantics are the ones
    /// under test; the trait default would refuse with `Unsupported`. The
    /// late-landing and accepted-put bookkeeping above is tied to `put` and
    /// `get`, so a pinned read is not counted.
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.inner.get_pinned(key, range, pin).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
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
        let nth = self.record_list(prefix);
        self.write_before_list(nth).await?;
        let out = self.inner.list(prefix, page).await;
        self.delete_after_list().await?;
        out
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let nth = self.record_list(prefix);
        self.write_before_list(nth).await?;
        let out = self.inner.list_delimited(prefix).await;
        self.delete_after_list().await?;
        out
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Lists the way the S3 adapter does: `object_store` appends the path
/// delimiter to every non-empty list prefix, so a prefix that ends mid
/// segment, such as a whole key, matches nothing.
pub struct SegmentAlignedStore {
    pub inner: InstrumentedStore<MemoryStore>,
    /// Every key passed to `delete`, in call order.
    deletes: Mutex<Vec<String>>,
}

impl SegmentAlignedStore {
    pub fn new(inner: MemoryStore) -> Self {
        SegmentAlignedStore {
            inner: InstrumentedStore::new(inner),
            deletes: Mutex::new(Vec::new()),
        }
    }

    pub fn deletes(&self) -> Vec<String> {
        self.deletes.lock().expect("lock").clone()
    }
}

fn segment_aligned(prefix: &str) -> String {
    let trimmed = prefix.trim_end_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for SegmentAlignedStore {
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
        self.inner.list(&segment_aligned(prefix), page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(&segment_aligned(prefix)).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.deletes.lock().expect("lock").push(key.to_string());
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Handles keys the way the S3 adapter does under ADR-2637. A request for a
/// key [`ravel_object_store::is_addressable_key`] refuses is answered with
/// [`StoreError::UnaddressableKey`] and never reaches `inner`, as the
/// adapter's `path_of` refuses it. A listing reads raw keys, so it never fails
/// on one: every listed key or common prefix that is not addressable moves to
/// the page's `unaddressable` or `unaddressable_prefixes`, whatever `inner`
/// reported it as. Prefixes and page tokens pass through.
pub struct S3KeyStore<S> {
    pub inner: S,
}

/// `key` when it is addressable, else the error the S3 adapter refuses it
/// with.
fn addressable(key: &str) -> Result<&str, StoreError> {
    if ravel_object_store::is_addressable_key(key) {
        Ok(key)
    } else {
        Err(StoreError::UnaddressableKey {
            key: key.to_string(),
            addresses: object_store::path::Path::from(key).to_string(),
        })
    }
}

/// Move every object in `objects` that is not addressable to `unaddressable`,
/// keeping listing order in both, and keep `unaddressable` sorted by key as a
/// listing delivers it.
fn classify(objects: &mut Vec<ObjectMeta>, unaddressable: &mut Vec<UnaddressableKey>) {
    let (kept, refused): (Vec<_>, Vec<_>) = std::mem::take(objects)
        .into_iter()
        .partition(|meta| ravel_object_store::is_addressable_key(&meta.key));
    *objects = kept;
    unaddressable.extend(refused.into_iter().map(|meta| UnaddressableKey {
        addresses: object_store::path::Path::from(meta.key.as_str()).to_string(),
        key: meta.key,
        size: meta.size,
        last_modified_unix_ms: meta.last_modified_unix_ms,
    }));
    unaddressable.sort_by(|a, b| a.key.cmp(&b.key));
}

#[async_trait::async_trait]
impl<S: ObjectStoreBackend> ObjectStoreBackend for S3KeyStore<S> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(addressable(key)?, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(addressable(key)?, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(addressable(key)?).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(addressable(key)?).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let mut page = self.inner.list(prefix, page).await?;
        classify(&mut page.objects, &mut page.unaddressable);
        Ok(page)
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let mut listed = self.inner.list_delimited(prefix).await?;
        classify(&mut listed.objects, &mut listed.unaddressable);
        let (kept, refused): (Vec<_>, Vec<_>) = std::mem::take(&mut listed.common_prefixes)
            .into_iter()
            .partition(|p| ravel_object_store::is_addressable_prefix(p));
        listed.common_prefixes = kept;
        listed.unaddressable_prefixes.extend(refused);
        listed.unaddressable_prefixes.sort();
        Ok(listed)
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(addressable(key)?).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}
