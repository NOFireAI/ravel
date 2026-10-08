//! Reference in-memory backend: the semantics oracle for the contract.
//! Strong consistency, atomic conditional writes, monotonic etags/versions.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PartSequence, PutMode, PutOptions, PutOutcome, StoreError,
    UploadChecksum, Version, check_addressable, classify_objects, classify_prefixes,
    multipart_finished, multipart_poisoned,
};

#[derive(Debug, Clone)]
struct Entry {
    data: Bytes,
    etag: Etag,
    version: Version,
    last_modified_unix_ms: i64,
    /// CRC-32C of `data` as it was written, the oracle's stand-in for the
    /// checksum S3 stores beside an object at upload (ADR-1696 decision 5).
    /// Recorded on every write, not only when the caller supplied a
    /// [`UploadChecksum`], because the real store computes one either way; a
    /// full-object [`ObjectStoreBackend::get`] checks the bytes against it.
    /// `MemoryStore::corrupt_stored_byte` (feature `test-support`) is the only
    /// thing that can make the two disagree.
    stored_checksum: u32,
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
    /// What [`ObjectStoreBackend::observed_store_time_ns`] reports (ADR-1685
    /// decision 1). `None` unless a test sets it through
    /// [`MemoryStore::set_observed_store_time_ns`]: the oracle is in-process
    /// and receives no responses, so it observes no store clock of its own,
    /// and inventing one from the host clock would hand callers an unasked-for
    /// second time source.
    observed_store_time_ns: RwLock<Option<i64>>,
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

    /// Set what [`ObjectStoreBackend::observed_store_time_ns`] reports, so a
    /// caller's store-clock check (ADR-1685 decision 2) can be driven against
    /// the oracle. `None` restores the default of having observed nothing.
    ///
    /// The oracle never sets this itself: it serves no HTTP responses, so it
    /// has no store clock to observe, and a test that wants one says so.
    ///
    /// Only compiled with the `test-support` feature, which no production
    /// build enables.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_observed_store_time_ns(&self, ns: Option<i64>) {
        *self.observed_store_time_ns.write() = ns;
    }

    /// Flip one bit of a stored object *without* touching the checksum recorded
    /// when it was written: bit rot at rest, the corruption ADR-1696's read-side
    /// verification exists to catch.
    ///
    /// This is the only way the oracle's stored bytes and stored checksum can
    /// disagree, so it is what makes that verification testable. Nothing else
    /// mutates an entry in place: a `put` rewrites the whole entry, checksum
    /// included, which is a new object rather than a corrupted one, and is why
    /// re-putting flipped bytes cannot stand in for this.
    ///
    /// `offset` is a byte index into the object and `bit` selects the bit
    /// within it; both are checked, so a test cannot silently corrupt nothing.
    /// Returns [`StoreError::NotFound`] for an absent key and
    /// [`StoreError::InvalidRange`] for an offset past the object's end or a
    /// bit index above 7.
    ///
    /// Only compiled with the `test-support` feature, which no production
    /// build enables.
    #[cfg(any(test, feature = "test-support"))]
    pub fn corrupt_stored_byte(
        &self,
        key: &str,
        offset: usize,
        bit: u32,
    ) -> Result<(), StoreError> {
        if bit > 7 {
            return Err(StoreError::InvalidRange(format!(
                "bit {bit} is not a bit index of a byte"
            )));
        }
        let mut objects = self.objects.write();
        let entry = objects.get_mut(key).ok_or(StoreError::NotFound)?;
        if offset >= entry.data.len() {
            return Err(StoreError::InvalidRange(format!(
                "offset {offset} of a {}-byte object",
                entry.data.len()
            )));
        }
        let mut bytes = entry.data.to_vec();
        bytes[offset] ^= 1u8 << bit;
        entry.data = Bytes::from(bytes);
        Ok(())
    }

    /// Store `data` at `key` unconditionally, without the
    /// [`crate::is_addressable_key`] check every operation applies: a writer
    /// outside Ravel, which is how a store comes to hold a key no operation can
    /// address (ADR-2637). Listings report such a key in `unaddressable`.
    ///
    /// Only compiled with the `test-support` feature, which no production
    /// build enables.
    #[cfg(any(test, feature = "test-support"))]
    pub fn insert_foreign(&self, key: &str, data: Bytes) {
        let id = self.next_id();
        let entry = Entry {
            stored_checksum: crc32c(&data),
            data,
            etag: Etag(format!("mem-etag-{id}")),
            version: Version(format!("mem-v-{id}")),
            last_modified_unix_ms: self.now_ms(),
        };
        self.objects.write().insert(key.to_string(), entry);
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

    /// Read-side verification (ADR-1696 decision 5), the oracle's form of what
    /// the S3 adapter does with the checksum S3 stored at upload: a full-object
    /// read is checked against the checksum recorded when the object was
    /// written, so bytes that changed at rest are refused with `Corrupted`
    /// instead of handed to a decoder. A ranged read is not checked, matching
    /// decision 4: the stored checksum covers the whole object and a slice
    /// cannot be compared against it.
    ///
    /// Every read path runs it, pinned or not: a pin decides *which* bytes are
    /// served, never whether they are checked.
    fn verify_full_read(key: &str, entry: &Entry, range: GetRange) -> Result<(), StoreError> {
        if range != GetRange::Full {
            return Ok(());
        }
        let actual = crc32c(&entry.data);
        if actual != entry.stored_checksum {
            return Err(StoreError::Corrupted(format!(
                "get of {key}: stored crc32c {:08x} does not match {actual:08x} computed over \
                 the {} stored bytes",
                entry.stored_checksum,
                entry.data.len()
            )));
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
    /// Set once a part violates the sequence rules: a rejected
    /// non-final or empty part would truncate the object, so every later
    /// `put_part`/`complete` fails with [`multipart_poisoned`] while `abort`
    /// stays callable. Mirrors [`crate::s3::S3MultipartUpload`], whose backend
    /// part failures poison the same field; the oracle has no
    /// transport to fail, so only the sequence path sets this here. A checksum
    /// mismatch does not poison (it leaves the upload open for a re-send).
    poison: Option<String>,
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
        if let Some(cause) = &self.poison {
            return Err(multipart_poisoned(&self.key, cause));
        }
        // Checksum pre-flight before the sequence rules touch any state: a
        // mismatch is the one recoverable rejection, so it is not a part and
        // the upload stays usable for a re-send (it must not poison).
        MemoryStore::verify_checksum(&data, checksum)?;
        // A sequence-rule violation poisons the handle so a later
        // `complete` cannot publish a truncated object; `abort` stays callable.
        if let Err(e) = self.sequence.accept(&self.key, data.len()) {
            self.poison = Some(e.to_string());
            return Err(e);
        }
        self.parts.push(data);
        Ok(())
    }

    async fn complete(&mut self) -> Result<PutOutcome, StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        if let Some(cause) = &self.poison {
            return Err(multipart_poisoned(&self.key, cause));
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
        check_addressable(key)?;
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
            stored_checksum: crc32c(&data),
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
        check_addressable(key)?;
        let objects = self.objects.read();
        let entry = objects.get(key).ok_or(StoreError::NotFound)?;
        Self::verify_full_read(key, entry, range)?;
        Ok(GetOutcome {
            data: Self::slice(&entry.data, range)?,
            etag: entry.etag.clone(),
            version: entry.version.clone(),
            total_size: entry.data.len() as u64,
        })
    }

    /// Evaluates the pin under the same lock that serves the bytes, so an
    /// overwrite cannot land between the check and the read.
    ///
    /// The order is the contract's, and the two halves answer differently
    /// (docs/object-store-contract.md, "Conditional reads"; the ADR-2040
    /// pinning amendment):
    ///
    /// - A missing key is `NotFound` whatever the pin says.
    /// - `pin.version` is a *selector*. This store keeps only the current
    ///   object, so any version but the current one has been replaced and is
    ///   gone: the answer is `NotFound`, the same answer a versioned store
    ///   gives for a version that has been deleted. It is never
    ///   `PreconditionFailed`, which would say the object is there and
    ///   different.
    /// - `pin.etag` is a *precondition*, evaluated on the object the selector
    ///   chose. A mismatch is `PreconditionFailed`, which for a pin with no
    ///   version is the overwrite case the pinning model rests on.
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &crate::Pin,
    ) -> Result<crate::PinnedRead, StoreError> {
        check_addressable(key)?;
        let objects = self.objects.read();
        let entry = objects.get(key).ok_or(StoreError::NotFound)?;
        if let Some(version) = pin.version.as_deref()
            && entry.version.0 != version
        {
            return Err(StoreError::NotFound);
        }
        if entry.etag.0 != pin.etag {
            return Err(StoreError::PreconditionFailed);
        }
        Self::verify_full_read(key, entry, range)?;
        Ok(crate::PinnedRead {
            outcome: GetOutcome {
                data: Self::slice(&entry.data, range)?,
                etag: entry.etag.clone(),
                version: entry.version.clone(),
                total_size: entry.data.len() as u64,
            },
            pin: crate::Pin::from_store(entry.etag.0.clone(), Some(entry.version.0.clone())),
        })
    }

    /// This store models a versioned one: its `version` is a distinct value per
    /// `put`, not the ETag again, so the pin it reports carries a selector and
    /// the selector case above is reachable from the conformance suite.
    async fn get_with_pin(
        &self,
        key: &str,
        range: GetRange,
    ) -> Result<crate::PinnedRead, StoreError> {
        check_addressable(key)?;
        let objects = self.objects.read();
        let entry = objects.get(key).ok_or(StoreError::NotFound)?;
        Self::verify_full_read(key, entry, range)?;
        Ok(crate::PinnedRead {
            outcome: GetOutcome {
                data: Self::slice(&entry.data, range)?,
                etag: entry.etag.clone(),
                version: entry.version.clone(),
                total_size: entry.data.len() as u64,
            },
            pin: crate::Pin::from_store(entry.etag.0.clone(), Some(entry.version.0.clone())),
        })
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, crate::Pin), StoreError> {
        check_addressable(key)?;
        let objects = self.objects.read();
        let entry = objects.get(key).ok_or(StoreError::NotFound)?;
        let pin = crate::Pin::from_store(entry.etag.0.clone(), Some(entry.version.0.clone()));
        Ok((entry.meta(key), pin))
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        check_addressable(key)?;
        Ok(Box::new(MemoryMultipartUpload {
            store: self,
            key: key.to_string(),
            parts: Vec::new(),
            sequence: PartSequence::default(),
            finished: false,
            poison: None,
        }))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        check_addressable(key)?;
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
        // Unaddressable keys count toward the page and can be its last key, as
        // on S3: classification happens after the page is cut.
        let next = if out.len() == self.page_size {
            out.last().map(|m| PageToken(m.key.clone()))
        } else {
            None
        };
        let (objects, unaddressable) = classify_objects(prefix, out);
        Ok(ListPage {
            objects,
            next,
            unaddressable,
        })
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let objects = self.objects.read();
        // Resume point: strictly after the page token when paging, else
        // strictly after `start_after` on the first page (skipping the
        // sub-range server-side rather than transferring and dropping it),
        // else the prefix itself. The larger of a page token and
        // `start_after` is always the page token (it is a key already past
        // `start_after`), so a present page token subsumes `start_after`.
        let (start_key, skip_first_if_equal) = match (&page, start_after) {
            (Some(PageToken(after)), _) => (after.clone(), true),
            // `start_after` below the prefix cannot exclude any key under it,
            // so fall back to listing from the prefix.
            (None, Some(after)) if after >= prefix => (after.to_string(), true),
            (None, _) => (prefix.to_string(), false),
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
        let (objects, unaddressable) = classify_objects(prefix, out);
        Ok(ListPage {
            objects,
            next,
            unaddressable,
        })
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
        let (objects, unaddressable) = classify_objects(prefix, direct);
        let (common_prefixes, unaddressable_prefixes) = classify_prefixes(prefix, prefixes);
        Ok(DelimitedList {
            objects,
            common_prefixes,
            unaddressable,
            unaddressable_prefixes,
        })
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        check_addressable(key)?;
        self.objects.write().remove(key);
        Ok(())
    }

    /// Whatever a test set (ADR-1685 decision 1), `None` otherwise.
    fn observed_store_time_ns(&self) -> Option<i64> {
        *self.observed_store_time_ns.read()
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

/// The conformance suite's foreign-key writer for a `MemoryStore` subject:
/// [`MemoryStore::insert_foreign`], and a removal that skips the key check the
/// same way.
#[cfg(any(test, feature = "test-support"))]
#[async_trait::async_trait]
impl crate::conformance::ForeignKeySeeder for MemoryStore {
    async fn seed(&self, key: &str, data: Bytes) -> Result<(), StoreError> {
        self.insert_foreign(key, data);
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<(), StoreError> {
        self.objects.write().remove(key);
        Ok(())
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
    /// etag/version from the same counter.
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

    /// A part that violates the sequence rules poisons the handle:
    /// completing afterward must error rather than publish a truncated
    /// object, the poison error is non-retryable, and `abort` stays callable.
    /// The sequence check happens before any backend call, so this in-process
    /// oracle covers the S3-shaped path too.
    #[tokio::test]
    async fn sequence_rejection_poisons_handle() {
        let store = MemoryStore::new();
        let mut upload = store.put_multipart("poisoned").await.expect("start");
        // A short part is legal while it is the last one...
        upload
            .put_part(Bytes::from_static(b"short"), None)
            .await
            .expect("a short part is legal while it is the last one");
        // ...but the next part makes it a sub-minimum non-final part: rejected.
        let rejected = upload
            .put_part(Bytes::from_static(b"more"), None)
            .await
            .expect_err("the part making a short part non-final must be rejected");
        assert!(matches!(rejected, StoreError::Permanent(_)));

        // The handle is now poisoned: further parts are refused non-retryably.
        let after = upload
            .put_part(Bytes::from(vec![0u8; crate::MULTIPART_MIN_PART_SIZE]), None)
            .await
            .expect_err("a poisoned handle must refuse further parts");
        assert!(matches!(after, StoreError::Permanent(_)));
        assert!(!after.is_retryable());

        // complete must error, never publish the truncated single short part.
        let completed = upload
            .complete()
            .await
            .expect_err("completing a poisoned upload must fail, not truncate");
        assert!(matches!(completed, StoreError::Permanent(_)));
        assert!(
            matches!(store.head("poisoned").await, Err(StoreError::NotFound)),
            "a poisoned upload must not have published a truncated object"
        );

        // abort stays callable on a poisoned handle and leaves no object.
        upload
            .abort()
            .await
            .expect("abort after poison must succeed");
        assert!(matches!(
            store.head("poisoned").await,
            Err(StoreError::NotFound)
        ));
    }

    fn assert_refused<T: std::fmt::Debug>(
        op: &str,
        result: Result<T, StoreError>,
        key: &str,
        addressed: &str,
    ) {
        match result {
            Err(StoreError::UnaddressableKey { key: k, addresses }) => {
                assert_eq!(k, key, "{op}");
                assert_eq!(addresses, addressed, "{op}");
            }
            other => panic!("{op} of {key:?}: expected UnaddressableKey, got {other:?}"),
        }
    }

    /// Every key operation refuses a key the adapter would rewrite, naming the
    /// key it would reach, even when an object is stored at the raw key.
    #[tokio::test]
    async fn operations_refuse_unaddressable_keys() {
        let store = MemoryStore::new();
        for (key, addressed) in [("a/b\u{1}c", "a/b%01c"), ("a/*b", "a/%2Ab")] {
            store.insert_foreign(key, Bytes::from_static(b"foreign"));
            let pin = crate::Pin::etag("e");
            assert_refused("get", store.get(key, GetRange::Full).await, key, addressed);
            assert_refused("head", store.head(key).await, key, addressed);
            assert_refused(
                "put",
                store
                    .put(key, Bytes::from_static(b"x"), PutOptions::default())
                    .await,
                key,
                addressed,
            );
            assert_refused("delete", store.delete(key).await, key, addressed);
            assert_refused("pin_of", store.pin_of(key).await, key, addressed);
            assert_refused(
                "get_pinned",
                store.get_pinned(key, GetRange::Full, &pin).await,
                key,
                addressed,
            );
            assert_refused(
                "get_with_pin",
                store.get_with_pin(key, GetRange::Full).await,
                key,
                addressed,
            );
            assert_refused(
                "put_multipart",
                store.put_multipart(key).await.map(|_| ()),
                key,
                addressed,
            );
        }
        let listing = crate::list_all_reporting(&store, "a/").await.expect("list");
        assert_eq!(
            listing.unaddressable.count, 2,
            "the refused delete left both foreign objects in place"
        );
    }

    /// Unaddressable keys seeded in the middle of a page and on each side of a
    /// page boundary: the listing returns every addressable key once and
    /// reports exactly the seeded keys, and raw keys still count toward the
    /// page size and end a page.
    #[tokio::test]
    async fn listing_reports_exactly_the_seeded_unaddressable_keys() {
        // Raw order: p/a p/b* p/c* p/d p/e p/f\x01 p/g. At page size 2 the
        // boundary falls between p/b* and p/c*; at page size 3, p/b* sits in
        // the middle of the first page.
        let foreign = ["p/b*", "p/c*", "p/f\u{1}"];
        let addressable = ["p/a", "p/d", "p/e", "p/g"];
        for page_size in [2, 3] {
            let store = MemoryStore::with_page_size(page_size);
            for key in foreign {
                store.insert_foreign(key, Bytes::from_static(b"f"));
            }
            for key in addressable {
                store
                    .put(key, Bytes::from_static(b"x"), PutOptions::default())
                    .await
                    .expect("put");
            }

            let first = store.list("p/", None).await.expect("page 1");
            let first_raw = first.objects.len() + first.unaddressable.len();
            assert_eq!(first_raw, page_size, "raw keys fill the page");
            if page_size == 2 {
                assert_eq!(
                    first.next,
                    Some(PageToken("p/b*".to_string())),
                    "the page ends on the raw unaddressable key"
                );
            }

            let listing = crate::list_all_reporting(&store, "p/")
                .await
                .expect("drain");
            let keys: Vec<&str> = listing.objects.iter().map(|m| m.key.as_str()).collect();
            assert_eq!(keys, addressable, "page size {page_size}");
            let skipped: Vec<&str> = listing
                .unaddressable
                .sample
                .iter()
                .map(|s| s.key.as_str())
                .collect();
            assert_eq!(skipped, foreign, "page size {page_size}");
            assert_eq!(listing.unaddressable.count, 3, "page size {page_size}");

            let after = store
                .list_after("p/", Some("p/b*"), None)
                .await
                .expect("list_after");
            let after_raw: Vec<&str> = after
                .unaddressable
                .iter()
                .map(|s| s.key.as_str())
                .chain(after.objects.iter().map(|m| m.key.as_str()))
                .collect();
            assert_eq!(after_raw.len(), page_size, "list_after pages raw keys too");
            assert!(!after_raw.contains(&"p/b*"), "start_after is exclusive");
        }
    }

    /// A common prefix that fails the prefix rule is reported apart from the
    /// addressable ones, and so is a direct key.
    #[tokio::test]
    async fn delimited_listing_classifies_prefixes_and_keys() {
        let store = MemoryStore::new();
        store.insert_foreign("t/abc\u{1}/x", Bytes::from_static(b"f"));
        store.insert_foreign("t/z\u{1}", Bytes::from_static(b"f"));
        for key in ["t/ok/1", "t/y"] {
            store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("put");
        }
        let listing = store.list_delimited("t/").await.expect("list");
        assert_eq!(listing.common_prefixes, vec!["t/ok/".to_string()]);
        assert_eq!(
            listing.unaddressable_prefixes,
            vec!["t/abc\u{1}/".to_string()]
        );
        let objects: Vec<&str> = listing.objects.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(objects, vec!["t/y"]);
        let skipped: Vec<&str> = listing
            .unaddressable
            .iter()
            .map(|s| s.key.as_str())
            .collect();
        assert_eq!(skipped, vec!["t/z\u{1}"]);
        assert_eq!(listing.unaddressable[0].addresses, "t/z%01");
    }

    #[test]
    fn crc32c_known_vector() {
        // RFC 3720 B.4 test vector: 32 bytes of zeros.
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    }
}
