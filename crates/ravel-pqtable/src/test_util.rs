//! Shared test fixtures: tenants, manifests, and a store wrapper that records
//! which writes actually landed.
#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutMode, PutOptions, PutOutcome, StoreError,
};
use ravel_types::TenantHash;

use crate::keys::dataset_object_key;
use crate::manifest::{Manifest, ParquetFile};

pub const TENANT_A: TenantHash = TenantHash([0xa1; 16]);
pub const TENANT_B: TenantHash = TenantHash([0xb2; 16]);

/// A data file entry for `dataset` whose content is the single byte `seed`.
pub fn file_for(tenant: &TenantHash, dataset: &str, seed: u8) -> ParquetFile {
    let blake3 = *blake3::hash(&[seed]).as_bytes();
    ParquetFile {
        key: dataset_object_key(tenant, dataset, &blake3).expect("key"),
        size: 1,
        blake3,
        row_count: u64::from(seed),
        footer_len: 10,
    }
}

/// A live manifest over dataset `table` holding one file per seed.
pub fn live_manifest(tenant: &TenantHash, table: &str, version: u64, seeds: &[u8]) -> Manifest {
    Manifest {
        table: table.to_string(),
        version,
        dropped: false,
        dataset: table.to_string(),
        files: seeds.iter().map(|&s| file_for(tenant, table, s)).collect(),
        options: BTreeMap::new(),
        created_by: "test".into(),
        created_unix_ns: version as i64,
        statement: format!("v{version}"),
    }
}

/// Wraps a backend and records, per key, how many `put` calls the inner
/// backend accepted, plus how many multipart uploads were started and
/// completed. With `replay_create_if_absent` set, a matching
/// `CreateIfAbsent` put is sent to the inner backend twice and the second
/// result returned, which is what a client that retries after a lost
/// acknowledgement does.
pub struct CountingStore<S> {
    pub inner: S,
    accepted_puts: Mutex<HashMap<String, usize>>,
    pub multipart_started: AtomicUsize,
    pub multipart_completed: AtomicUsize,
    pub replay_create_if_absent: Option<String>,
}

impl<S> CountingStore<S> {
    pub fn new(inner: S) -> Self {
        CountingStore {
            inner,
            accepted_puts: Mutex::new(HashMap::new()),
            multipart_started: AtomicUsize::new(0),
            multipart_completed: AtomicUsize::new(0),
            replay_create_if_absent: None,
        }
    }

    /// Keys with at least one accepted `put`, and how many each had.
    pub fn accepted_puts(&self) -> HashMap<String, usize> {
        self.accepted_puts.lock().expect("lock").clone()
    }

    fn record_put(&self, key: &str) {
        *self
            .accepted_puts
            .lock()
            .expect("lock")
            .entry(key.to_string())
            .or_insert(0) += 1;
    }
}

struct CountingUpload<'a> {
    inner: Box<dyn MultipartUpload + 'a>,
    completed: &'a AtomicUsize,
}

#[async_trait::async_trait]
impl MultipartUpload for CountingUpload<'_> {
    async fn put_part(
        &mut self,
        data: Bytes,
        checksum: Option<ravel_object_store::UploadChecksum>,
    ) -> Result<(), StoreError> {
        self.inner.put_part(data, checksum).await
    }

    async fn complete(&mut self) -> Result<PutOutcome, StoreError> {
        let out = self.inner.complete().await?;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(out)
    }

    async fn abort(&mut self) -> Result<(), StoreError> {
        self.inner.abort().await
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
        let inner = self.inner.put_multipart(key).await?;
        self.multipart_started.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(CountingUpload {
            inner,
            completed: &self.multipart_completed,
        }))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
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
        self.inner.capabilities()
    }
}
