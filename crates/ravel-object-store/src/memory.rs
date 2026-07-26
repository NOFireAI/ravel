//! Reference in-memory backend: the semantics oracle for the contract.
//! Strong consistency, atomic conditional writes, monotonic etags.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    Capabilities, Etag, GetOutcome, GetRange, ObjectMeta, ObjectStoreBackend, PutMode, PutOptions,
    PutOutcome, StoreError,
};

#[derive(Debug, Clone)]
struct Entry {
    data: Bytes,
    etag: Etag,
    last_modified_unix_ms: i64,
}

/// In-memory object store. Clock is injectable for deterministic tests.
#[derive(Default)]
pub struct MemoryStore {
    objects: RwLock<BTreeMap<String, Entry>>,
    etag_counter: AtomicU64,
    clock_ms: AtomicU64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the fake clock (tests exercising GC grace periods).
    pub fn set_clock_ms(&self, ms: u64) {
        self.clock_ms.store(ms, Ordering::SeqCst);
    }

    fn next_etag(&self) -> Etag {
        Etag(format!(
            "mem-{}",
            self.etag_counter.fetch_add(1, Ordering::SeqCst) + 1
        ))
    }

    fn now_ms(&self) -> i64 {
        self.clock_ms.load(Ordering::SeqCst) as i64
    }

    fn slice(data: &Bytes, range: GetRange) -> Result<Bytes, StoreError> {
        let len = data.len() as u64;
        match range {
            GetRange::Full => Ok(data.clone()),
            GetRange::Range(start, end) => {
                if start >= end || start >= len {
                    return Err(StoreError::InvalidRange(format!(
                        "[{start}, {end}) of {len}-byte object"
                    )));
                }
                let end = end.min(len);
                Ok(data.slice(start as usize..end as usize))
            }
            GetRange::Suffix(n) => {
                if n == 0 {
                    return Err(StoreError::InvalidRange("zero-length suffix".into()));
                }
                let n = n.min(len);
                Ok(data.slice((len - n) as usize..))
            }
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for MemoryStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        let mut objects = self.objects.write();
        match &opts.mode {
            PutMode::Overwrite => {}
            PutMode::CreateIfAbsent => {
                if objects.contains_key(key) {
                    return Err(StoreError::AlreadyExists);
                }
            }
            PutMode::CasEtag(expected) => match objects.get(key) {
                Some(entry) if &entry.etag == expected => {}
                Some(_) => return Err(StoreError::PreconditionFailed),
                None => return Err(StoreError::PreconditionFailed),
            },
        }
        let etag = self.next_etag();
        objects.insert(
            key.to_string(),
            Entry {
                data,
                etag: etag.clone(),
                last_modified_unix_ms: self.now_ms(),
            },
        );
        Ok(PutOutcome { etag })
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let objects = self.objects.read();
        let entry = objects.get(key).ok_or(StoreError::NotFound)?;
        Ok(GetOutcome {
            data: Self::slice(&entry.data, range)?,
            etag: entry.etag.clone(),
            total_size: entry.data.len() as u64,
        })
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let objects = self.objects.read();
        let entry = objects.get(key).ok_or(StoreError::NotFound)?;
        Ok(ObjectMeta {
            key: key.to_string(),
            size: entry.data.len() as u64,
            etag: entry.etag.clone(),
            last_modified_unix_ms: entry.last_modified_unix_ms,
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        let objects = self.objects.read();
        Ok(objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, entry)| ObjectMeta {
                key: k.clone(),
                size: entry.data.len() as u64,
                etag: entry.etag.clone(),
                last_modified_unix_ms: entry.last_modified_unix_ms,
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.objects.write().remove(key);
        Ok(())
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            create_if_absent: true,
            cas_etag: true,
            suffix_range: true,
            consistent_list: true,
            multipart: false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_if_absent_is_atomic() {
        let store = MemoryStore::new();
        let opts = PutOptions {
            mode: PutMode::CreateIfAbsent,
        };
        store
            .put("k", Bytes::from_static(b"a"), opts.clone())
            .await
            .expect("first create");
        let err = store.put("k", Bytes::from_static(b"b"), opts).await;
        assert!(matches!(err, Err(StoreError::AlreadyExists)));
        let got = store.get("k", GetRange::Full).await.expect("get");
        assert_eq!(&got.data[..], b"a");
    }

    #[tokio::test]
    async fn cas_requires_matching_etag() {
        let store = MemoryStore::new();
        let put = store
            .put("k", Bytes::from_static(b"v1"), PutOptions::default())
            .await
            .expect("put");
        let stale = Etag("mem-0".into());
        let err = store
            .put(
                "k",
                Bytes::from_static(b"v2"),
                PutOptions {
                    mode: PutMode::CasEtag(stale),
                },
            )
            .await;
        assert!(matches!(err, Err(StoreError::PreconditionFailed)));
        store
            .put(
                "k",
                Bytes::from_static(b"v2"),
                PutOptions {
                    mode: PutMode::CasEtag(put.etag),
                },
            )
            .await
            .expect("cas with fresh etag");
    }

    #[tokio::test]
    async fn suffix_and_range_reads() {
        let store = MemoryStore::new();
        store
            .put(
                "k",
                Bytes::from_static(b"0123456789"),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let suffix = store.get("k", GetRange::Suffix(4)).await.expect("suffix");
        assert_eq!(&suffix.data[..], b"6789");
        assert_eq!(suffix.total_size, 10);
        let range = store.get("k", GetRange::Range(2, 5)).await.expect("range");
        assert_eq!(&range.data[..], b"234");
        let oversize_suffix = store
            .get("k", GetRange::Suffix(100))
            .await
            .expect("clamped");
        assert_eq!(oversize_suffix.data.len(), 10);
        let zero_suffix = store.get("k", GetRange::Suffix(0)).await;
        assert!(matches!(zero_suffix, Err(StoreError::InvalidRange(_))));
    }

    #[tokio::test]
    async fn list_is_prefix_scoped_and_ordered() {
        let store = MemoryStore::new();
        for key in ["a/1", "a/2", "b/1"] {
            store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
        }
        let listed = store.list("a/").await.expect("list");
        let keys: Vec<_> = listed.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["a/1", "a/2"]);
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let store = MemoryStore::new();
        store.delete("missing").await.expect("idempotent delete");
    }
}
