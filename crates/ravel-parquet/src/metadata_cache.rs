use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use parquet::file::metadata::ParquetMetaData;
use ravel_cache::CacheKey;

/// Identity of one decoded footer: the tenant and the pinned identity's
/// content hash, the same two halves a pinned [`CacheKey`] carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetadataKey {
    tenant_hash: [u8; 16],
    content_hash: [u8; 32],
}

impl MetadataKey {
    /// The metadata key of the file a pinned byte-cache key belongs to; the
    /// key's range is not part of it.
    pub fn of(key: &CacheKey) -> Self {
        MetadataKey {
            tenant_hash: key.tenant_hash,
            content_hash: key.content_hash,
        }
    }
}

/// Decoded Parquet footers, bounded by [`ParquetMetaData::memory_size`] and
/// evicted least recently used first.
///
/// One instance lives for the process, beside the byte cache, so a footer is
/// decoded once per pinned file rather than once per statement.
#[derive(Debug)]
pub struct MetadataCache {
    max_bytes: u64,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<MetadataKey, Entry>,
    bytes: u64,
    clock: u64,
}

#[derive(Debug)]
struct Entry {
    metadata: Arc<ParquetMetaData>,
    bytes: u64,
    last_used: u64,
}

impl MetadataCache {
    pub fn new(max_bytes: u64) -> Self {
        MetadataCache {
            max_bytes,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn get(&self, key: &MetadataKey) -> Option<Arc<ParquetMetaData>> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.clock += 1;
        let now = inner.clock;
        let entry = inner.entries.get_mut(key)?;
        entry.last_used = now;
        Some(Arc::clone(&entry.metadata))
    }

    /// Admit `metadata` under `key`, evicting least recently used entries
    /// until the total fits. An entry larger than the whole bound is not
    /// admitted.
    pub fn insert(&self, key: MetadataKey, metadata: Arc<ParquetMetaData>) {
        let bytes = metadata.memory_size() as u64;
        if bytes > self.max_bytes {
            return;
        }
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.clock += 1;
        let last_used = inner.clock;
        if let Some(old) = inner.entries.insert(
            key,
            Entry {
                metadata,
                bytes,
                last_used,
            },
        ) {
            inner.bytes -= old.bytes;
        }
        inner.bytes += bytes;
        while inner.bytes > self.max_bytes {
            let Some(victim) = inner
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| *key)
            else {
                break;
            };
            if let Some(evicted) = inner.entries.remove(&victim) {
                inner.bytes -= evicted.bytes;
            }
        }
    }

    /// Bytes currently resident, as [`ParquetMetaData::memory_size`] counts
    /// them.
    pub fn resident_bytes(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .bytes
    }

    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
