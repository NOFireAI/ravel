use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use parquet::file::metadata::ParquetMetaData;
use ravel_cache::CacheKey;

/// Identity of one decoded footer: the tenant and the pinned identity's
/// content hash, the same two halves a pinned [`CacheKey`] carries, and the
/// footer length the manifest recorded. The reader decodes or refuses a
/// footer from the pinned bytes and that length together, so two manifests
/// recording different lengths for one file get separate entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetadataKey {
    tenant_hash: [u8; 16],
    content_hash: [u8; 32],
    footer_len: u32,
}

impl MetadataKey {
    /// The metadata key of the file a pinned byte-cache key belongs to, read
    /// with a `footer_len`-byte footer; the key's range is not part of it.
    pub fn of(key: &CacheKey, footer_len: u32) -> Self {
        MetadataKey {
            tenant_hash: key.tenant_hash,
            content_hash: key.content_hash,
            footer_len,
        }
    }
}

/// What the cache holds for one pinned file: its decoded footer, or the reason
/// the reader refused it as corrupt.
#[derive(Debug, Clone)]
pub enum CachedFooter {
    Decoded(Arc<ParquetMetaData>),
    /// The `Corrupt` message the first read produced. The bytes are pinned, so
    /// a second read of them would be refused the same way.
    Refused(Arc<str>),
}

impl CachedFooter {
    /// Bytes this entry is charged against the bound: the decoded footer's
    /// [`ParquetMetaData::memory_size`], or a refusal's message plus its key.
    fn bytes(&self) -> u64 {
        match self {
            CachedFooter::Decoded(metadata) => metadata.memory_size() as u64,
            CachedFooter::Refused(message) => {
                (message.len() + std::mem::size_of::<MetadataKey>()) as u64
            }
        }
    }
}

/// Decoded Parquet footers and refused ones, bounded in bytes and evicted least
/// recently used first.
///
/// One instance lives for the process, beside the byte cache, so a footer is
/// decoded, or refused, once per pinned file rather than once per statement.
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
    footer: CachedFooter,
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

    /// The entry for `key` and the bytes it is charged against the bound.
    pub fn get(&self, key: &MetadataKey) -> Option<(CachedFooter, u64)> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.clock += 1;
        let now = inner.clock;
        let entry = inner.entries.get_mut(key)?;
        entry.last_used = now;
        Some((entry.footer.clone(), entry.bytes))
    }

    /// Admit `metadata` under `key`, replacing whatever was there.
    pub fn insert(&self, key: MetadataKey, metadata: Arc<ParquetMetaData>) {
        self.admit(key, CachedFooter::Decoded(metadata));
    }

    /// Record that the footer under `key` was refused with `message`.
    pub fn insert_refused(&self, key: MetadataKey, message: &str) {
        self.admit(key, CachedFooter::Refused(Arc::from(message)));
    }

    /// Admit `footer`, evicting least recently used entries until the total
    /// fits. An entry larger than the whole bound is not admitted, and the
    /// entry it would have replaced is dropped.
    fn admit(&self, key: MetadataKey, footer: CachedFooter) {
        let bytes = footer.bytes();
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if bytes > self.max_bytes {
            if let Some(old) = inner.entries.remove(&key) {
                inner.bytes -= old.bytes;
            }
            return;
        }
        inner.clock += 1;
        let last_used = inner.clock;
        if let Some(old) = inner.entries.insert(
            key,
            Entry {
                footer,
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

    /// Bytes currently resident, as each entry is charged.
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn key(byte: u8) -> MetadataKey {
        MetadataKey {
            tenant_hash: [1; 16],
            content_hash: [byte; 32],
            footer_len: 8,
        }
    }

    #[test]
    fn refusals_are_bounded_and_evicted_like_footers() {
        let one = "x".repeat(10);
        let charged = (10 + std::mem::size_of::<MetadataKey>()) as u64;
        let cache = MetadataCache::new(2 * charged);
        cache.insert_refused(key(1), &one);
        cache.insert_refused(key(2), &one);
        assert_eq!(cache.resident_bytes(), 2 * charged);
        assert!(cache.get(&key(1)).is_some());
        cache.insert_refused(key(3), &one);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key(2)).is_none(), "the least recently used goes");
        match cache.get(&key(1)) {
            Some((CachedFooter::Refused(message), bytes)) => {
                assert_eq!(&*message, one.as_str());
                assert_eq!(bytes, charged);
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        cache.insert_refused(key(4), &"y".repeat(1024));
        assert!(cache.get(&key(4)).is_none(), "larger than the bound");
        assert_eq!(cache.resident_bytes(), 2 * charged);
    }
}
