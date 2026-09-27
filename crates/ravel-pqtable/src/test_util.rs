//! Shared test fixtures: tenants, manifests, and a store wrapper that records
//! which writes landed and which prefixes were listed.
#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;

use bytes::Bytes;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutMode, PutOptions, PutOutcome, StoreError,
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
pub struct CountingStore<S> {
    pub inner: S,
    accepted_puts: Mutex<HashMap<String, usize>>,
    listed_prefixes: Mutex<Vec<String>>,
    list_bumps: Mutex<(Option<FixedClock>, VecDeque<i64>)>,
    pub replay_create_if_absent: Option<String>,
}

impl<S> CountingStore<S> {
    pub fn new(inner: S) -> Self {
        CountingStore {
            inner,
            accepted_puts: Mutex::new(HashMap::new()),
            listed_prefixes: Mutex::new(Vec::new()),
            list_bumps: Mutex::new((None, VecDeque::new())),
            replay_create_if_absent: None,
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

    fn record_put(&self, key: &str) {
        *self
            .accepted_puts
            .lock()
            .expect("lock")
            .entry(key.to_string())
            .or_insert(0) += 1;
    }

    fn record_list(&self, prefix: &str) {
        self.listed_prefixes
            .lock()
            .expect("lock")
            .push(prefix.to_string());
        let mut bumps = self.list_bumps.lock().expect("lock");
        let (Some(clock), queue) = &mut *bumps else {
            return;
        };
        if let Some(delta) = queue.pop_front() {
            clock.advance(delta);
        }
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
        self.record_list(prefix);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.record_list(prefix);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}
