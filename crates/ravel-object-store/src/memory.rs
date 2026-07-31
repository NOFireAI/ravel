//! Reference in-memory backend: the semantics oracle for the contract.
//! Strong consistency, atomic conditional writes, monotonic etags/versions.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PartSequence, PutMode, PutOptions, PutOutcome, StoreError,
    UploadChecksum, Version, multipart_finished,
};

#[derive(Debug, Clone)]
struct Entry {
    data: Bytes,
    etag: Etag,
    version: Version,
    last_modified_unix_ms: i64,
}

impl Entry {
    fn meta(&self, key: &str) -> ObjectMeta {
        ObjectMeta {
            key: key.to_string(),
            size: self.data.len() as u64,
            etag: self.etag.clone(),
            version: self.version.clone(),
            last_modified_unix_ms: self.last_modified_unix_ms,
        }
    }
}

/// In-memory object store. Clock is injectable for deterministic tests and
/// defaults to 0: GC-grace tests MUST set it explicitly.
#[derive(Default)]
pub struct MemoryStore {
    objects: RwLock<BTreeMap<String, Entry>>,
    counter: AtomicU64,
    clock_ms: AtomicU64,
    /// Page size for listings; tests can shrink it to exercise pagination.
    page_size: usize,
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            page_size: 1000,
            ..Default::default()
        }
    }

    /// Oracle with a tiny listing page size to force pagination in tests.
    pub fn with_page_size(page_size: usize) -> Self {
        MemoryStore {
            page_size: page_size.max(1),
            ..Default::default()
        }
    }

    /// Advance the fake clock (tests exercising GC grace periods).
    pub fn set_clock_ms(&self, ms: u64) {
        self.clock_ms.store(ms, Ordering::SeqCst);
    }

    fn next_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn now_ms(&self) -> i64 {
        self.clock_ms.load(Ordering::SeqCst) as i64
    }

    fn verify_checksum(data: &Bytes, checksum: Option<UploadChecksum>) -> Result<(), StoreError> {
        if let Some(UploadChecksum::Crc32c(expected)) = checksum {
            let actual = crc32c(data);
            if actual != expected {
                return Err(StoreError::Corrupted(format!(
                    "upload checksum mismatch: expected {expected:08x}, computed {actual:08x}"
                )));
            }
        }
        Ok(())
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

/// In-process multipart upload against [`MemoryStore`]: the oracle for the
/// multipart contract (docs/object-store-contract.md, "Multipart upload").
///
/// It deliberately does not chunk anything --- there is no transport to chunk
/// for --- but it accepts the same part sequence a real backend does, enforces
/// the same part-size and part-count rules ([`PartSequence`]), and produces
/// exactly the object a single [`MemoryStore::put`] of the concatenated parts
/// would have produced, with an etag and version drawn from the same counter.
/// Parts are buffered here and nowhere else: until `complete` succeeds the key
/// does not exist in the store, so an aborted or dropped upload cannot leak a
/// partial object.
pub struct MemoryMultipartUpload<'a> {
    store: &'a MemoryStore,
    key: String,
    parts: Vec<Bytes>,
    sequence: PartSequence,
    /// Set by `complete`/`abort`; every later call on this handle fails.
    finished: bool,
}

#[async_trait::async_trait]
impl MultipartUpload for MemoryMultipartUpload<'_> {
    async fn put_part(
        &mut self,
        data: Bytes,
        checksum: Option<UploadChecksum>,
    ) -> Result<(), StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        // Pre-flight before the sequence rules touch any state: a rejected
        // part is not a part, and the upload stays usable.
        MemoryStore::verify_checksum(&data, checksum)?;
        self.sequence.accept(&self.key, data.len())?;
        self.parts.push(data);
        Ok(())
    }

    async fn complete(&mut self) -> Result<PutOutcome, StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        self.sequence.finish(&self.key)?;
        let total: usize = self.parts.iter().map(Bytes::len).sum();
        let mut assembled = Vec::with_capacity(total);
        for part in &self.parts {
            assembled.extend_from_slice(part);
        }
        // Only now does the key become visible, and by the same code path a
        // single put would take, so the two are indistinguishable afterwards.
        let outcome = self
            .store
            .put(&self.key, Bytes::from(assembled), PutOptions::default())
            .await?;
        self.finished = true;
        self.parts.clear();
        Ok(outcome)
    }

    async fn abort(&mut self) -> Result<(), StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        self.finished = true;
        self.parts.clear();
        Ok(())
    }
}

/// Minimal CRC-32C (Castagnoli), bitwise; the oracle favors clarity over
/// speed. Production paths use the crc32c crate.
fn crc32c(data: &[u8]) -> u32 {
    const POLY: u32 = 0x82F6_3B78;
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLY & mask);
        }
    }
    !crc
}

#[async_trait::async_trait]
impl ObjectStoreBackend for MemoryStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        Self::verify_checksum(&data, opts.checksum)?;
        let mut objects = self.objects.write();
        match &opts.mode {
            PutMode::Overwrite => {}
            PutMode::CreateIfAbsent => {
                if objects.contains_key(key) {
                    return Err(StoreError::AlreadyExists);
                }
            }
            PutMode::CasVersion(expected) => match objects.get(key) {
                Some(entry) if &entry.version == expected => {}
                Some(_) => return Err(StoreError::PreconditionFailed),
                None => return Err(StoreError::PreconditionFailed),
            },
        }
        let id = self.next_id();
        let entry = Entry {
            data,
            etag: Etag(format!("mem-etag-{id}")),
            version: Version(format!("mem-v-{id}")),
            last_modified_unix_ms: self.now_ms(),
        };
        let outcome = PutOutcome {
            etag: entry.etag.clone(),
            version: entry.version.clone(),
        };
        objects.insert(key.to_string(), entry);
        Ok(outcome)
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let objects = self.objects.read();
        let entry = objects.get(key).ok_or(StoreError::NotFound)?;
        Ok(GetOutcome {
            data: Self::slice(&entry.data, range)?,
            etag: entry.etag.clone(),
            version: entry.version.clone(),
            total_size: entry.data.len() as u64,
        })
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        Ok(Box::new(MemoryMultipartUpload {
            store: self,
            key: key.to_string(),
            parts: Vec::new(),
            sequence: PartSequence::default(),
            finished: false,
        }))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let objects = self.objects.read();
        let entry = objects.get(key).ok_or(StoreError::NotFound)?;
        Ok(entry.meta(key))
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let objects = self.objects.read();
        // The token is the last key of the previous page; resume strictly
        // after it, mirroring S3 continuation semantics.
        let (start_key, skip_first_if_equal) = match &page {
            Some(PageToken(after)) => (after.clone(), true),
            None => (prefix.to_string(), false),
        };
        let mut out = Vec::with_capacity(self.page_size.min(64));
        for (key, entry) in objects.range(start_key.clone()..) {
            if skip_first_if_equal && key == &start_key {
                continue;
            }
            if !key.starts_with(prefix) {
                break;
            }
            out.push(entry.meta(key));
            if out.len() == self.page_size {
                break;
            }
        }
        let next = if out.len() == self.page_size {
            out.last().map(|m| PageToken(m.key.clone()))
        } else {
            None
        };
        Ok(ListPage { objects: out, next })
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let objects = self.objects.read();
        let mut direct = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        for (key, entry) in objects.range(prefix.to_string()..) {
            if !key.starts_with(prefix) {
                break;
            }
            let rest = &key[prefix.len()..];
            match rest.find('/') {
                Some(idx) => {
                    let common = format!("{prefix}{}", &rest[..=idx]);
                    if prefixes.last() != Some(&common) {
                        prefixes.push(common);
                    }
                }
                None => direct.push(entry.meta(key)),
            }
        }
        Ok(DelimitedList {
            objects: direct,
            common_prefixes: prefixes,
        })
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.objects.write().remove(key);
        Ok(())
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            consistent_read: true,
            consistent_list: true,
            create_if_absent: true,
            cas_version: true,
            suffix_range: true,
            upload_checksum: true,
            prefix_list: true,
            // Real: `put_multipart` above accepts the full part sequence,
            // enforces the same part-size and part-count rules S3 does, and
            // publishes the assembled object atomically at `complete`.
            multipart: true,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::list_all;

    #[tokio::test]
    async fn create_if_absent_is_atomic() {
        let store = MemoryStore::new();
        store
            .put(
                "k",
                Bytes::from_static(b"a"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("first create");
        let err = store
            .put(
                "k",
                Bytes::from_static(b"b"),
                PutOptions::create_if_absent(),
            )
            .await;
        assert!(matches!(err, Err(StoreError::AlreadyExists)));
        let got = store.get("k", GetRange::Full).await.expect("get");
        assert_eq!(&got.data[..], b"a");
    }

    #[tokio::test]
    async fn cas_requires_matching_version() {
        let store = MemoryStore::new();
        let put = store
            .put("k", Bytes::from_static(b"v1"), PutOptions::default())
            .await
            .expect("put");
        let stale = Version("mem-v-0".into());
        let err = store
            .put(
                "k",
                Bytes::from_static(b"v2"),
                PutOptions {
                    mode: PutMode::CasVersion(stale),
                    checksum: None,
                },
            )
            .await;
        assert!(matches!(err, Err(StoreError::PreconditionFailed)));
        store
            .put(
                "k",
                Bytes::from_static(b"v2"),
                PutOptions {
                    mode: PutMode::CasVersion(put.version),
                    checksum: None,
                },
            )
            .await
            .expect("cas with fresh version");
    }

    #[tokio::test]
    async fn upload_checksum_verified() {
        let store = MemoryStore::new();
        let data = Bytes::from_static(b"payload");
        let good = crc32c(&data);
        store
            .put(
                "k",
                data.clone(),
                PutOptions::default().with_checksum(UploadChecksum::Crc32c(good)),
            )
            .await
            .expect("checksum ok");
        let err = store
            .put(
                "k2",
                data,
                PutOptions::default().with_checksum(UploadChecksum::Crc32c(good ^ 1)),
            )
            .await;
        assert!(matches!(err, Err(StoreError::Corrupted(_))));
        assert!(matches!(store.head("k2").await, Err(StoreError::NotFound)));
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
        let oversize = store
            .get("k", GetRange::Suffix(100))
            .await
            .expect("clamped");
        assert_eq!(oversize.data.len(), 10);
        assert!(matches!(
            store.get("k", GetRange::Suffix(0)).await,
            Err(StoreError::InvalidRange(_))
        ));
        assert!(matches!(
            store.get("k", GetRange::Range(5, 5)).await,
            Err(StoreError::InvalidRange(_))
        ));
    }

    #[tokio::test]
    async fn paginated_list_is_prefix_scoped_ordered_and_complete() {
        let store = MemoryStore::with_page_size(2);
        for key in ["a/1", "a/2", "a/3", "a/4", "a/5", "b/1"] {
            store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
        }
        let first = store.list("a/", None).await.expect("page 1");
        assert_eq!(first.objects.len(), 2);
        assert!(first.next.is_some());
        let all = list_all(&store, "a/").await.expect("drain");
        let keys: Vec<_> = all.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["a/1", "a/2", "a/3", "a/4", "a/5"]);
    }

    #[tokio::test]
    async fn delimited_list_groups_prefixes() {
        let store = MemoryStore::new();
        for key in ["t/x/1", "t/x/2", "t/y/1", "t/z"] {
            store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
        }
        let listing = store.list_delimited("t/").await.expect("list");
        assert_eq!(
            listing.common_prefixes,
            vec!["t/x/".to_string(), "t/y/".to_string()]
        );
        let direct: Vec<_> = listing.objects.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(direct, vec!["t/z"]);
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let store = MemoryStore::new();
        store.delete("missing").await.expect("idempotent delete");
    }

    /// The oracle's multipart end state must be indistinguishable from the
    /// single put of the same bytes: same content, same size, and an
    /// etag/version from the same counter (issue #243).
    #[tokio::test]
    async fn multipart_matches_a_single_put_of_the_same_bytes() {
        let store = MemoryStore::new();
        let head = vec![0xABu8; crate::MULTIPART_MIN_PART_SIZE];
        let tail = b"tail".to_vec();
        let whole: Vec<u8> = head.iter().copied().chain(tail.iter().copied()).collect();

        let mut upload = store.put_multipart("multi").await.expect("start upload");
        upload
            .put_part(Bytes::from(head), None)
            .await
            .expect("part 1");
        assert!(
            matches!(store.head("multi").await, Err(StoreError::NotFound)),
            "an incomplete upload must not be visible"
        );
        upload
            .put_part(Bytes::from(tail), None)
            .await
            .expect("part 2");
        let multipart_outcome = upload.complete().await.expect("complete");

        let single_outcome = store
            .put("single", Bytes::from(whole.clone()), PutOptions::default())
            .await
            .expect("single put");

        let multi = store.get("multi", GetRange::Full).await.expect("get multi");
        let single = store
            .get("single", GetRange::Full)
            .await
            .expect("get single");
        assert!(multi.data == single.data, "same bytes both ways");
        assert_eq!(multi.total_size, whole.len() as u64);
        assert_eq!(multi.etag, multipart_outcome.etag);
        assert_eq!(multi.version, multipart_outcome.version);
        assert_ne!(
            multipart_outcome.etag, single_outcome.etag,
            "distinct writes still get distinct etags"
        );
    }

    /// An aborted upload leaves no object and no reusable handle. The bytes
    /// only ever lived in the handle, so there is nothing to leak.
    #[tokio::test]
    async fn aborted_multipart_leaves_no_object() {
        let store = MemoryStore::new();
        let mut upload = store.put_multipart("gone").await.expect("start upload");
        upload
            .put_part(Bytes::from(vec![1u8; crate::MULTIPART_MIN_PART_SIZE]), None)
            .await
            .expect("part 1");
        upload.abort().await.expect("abort");
        assert!(matches!(
            store.head("gone").await,
            Err(StoreError::NotFound)
        ));
        assert!(matches!(
            store.get("gone", GetRange::Full).await,
            Err(StoreError::NotFound)
        ));
        assert!(
            matches!(upload.complete().await, Err(StoreError::Permanent(_))),
            "an aborted upload cannot be completed"
        );
        assert!(matches!(
            store.head("gone").await,
            Err(StoreError::NotFound)
        ));
    }

    /// The oracle enforces S3's part rules locally, at the call that violates
    /// them, so a test backend never accepts a sequence a real bucket would
    /// reject at `CompleteMultipartUpload`.
    #[tokio::test]
    async fn multipart_part_rules_are_enforced() {
        let store = MemoryStore::new();

        let mut no_parts = store.put_multipart("no-parts").await.expect("start");
        assert!(matches!(
            no_parts.complete().await,
            Err(StoreError::Permanent(_))
        ));

        let mut empty_part = store.put_multipart("empty-part").await.expect("start");
        assert!(matches!(
            empty_part.put_part(Bytes::new(), None).await,
            Err(StoreError::Permanent(_))
        ));

        let mut short_non_final = store.put_multipart("short").await.expect("start");
        short_non_final
            .put_part(Bytes::from_static(b"short"), None)
            .await
            .expect("a short part is legal while it is the last one");
        assert!(
            matches!(
                short_non_final
                    .put_part(Bytes::from_static(b"more"), None)
                    .await,
                Err(StoreError::Permanent(_))
            ),
            "the part that makes a sub-minimum part non-final must be rejected"
        );

        for key in ["no-parts", "empty-part", "short"] {
            assert!(
                matches!(store.head(key).await, Err(StoreError::NotFound)),
                "{key} must not exist"
            );
        }
    }

    /// A per-part checksum is verified before the part is accepted, and a
    /// mismatch leaves the upload usable (the part simply never happened).
    #[tokio::test]
    async fn multipart_part_checksum_verified() {
        let store = MemoryStore::new();
        let part = Bytes::from_static(b"part-payload");
        let good = crc32c(&part);
        let mut upload = store.put_multipart("checked").await.expect("start");
        assert!(matches!(
            upload
                .put_part(part.clone(), Some(UploadChecksum::Crc32c(good ^ 1)))
                .await,
            Err(StoreError::Corrupted(_))
        ));
        upload
            .put_part(part.clone(), Some(UploadChecksum::Crc32c(good)))
            .await
            .expect("matching checksum");
        upload.complete().await.expect("complete");
        let got = store
            .get("checked", GetRange::Full)
            .await
            .expect("get checked");
        assert_eq!(
            got.data, part,
            "the rejected part must not be in the object"
        );
    }

    #[test]
    fn crc32c_known_vector() {
        // RFC 3720 B.4 test vector: 32 bytes of zeros.
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    }
}
