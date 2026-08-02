//! Local-disk tier of ADR-0046's read cache (docs/adrs/0046-read-cache-tier.md,
//! decision 3's disk half, decision 4, decision 7, and rejected alternative
//! 6). Content-addressed raw-byte-range storage under a configured
//! directory, keyed by the existing [`CacheKey`].
//!
//! This is the first production file Ravel writes (ADR-0046's survey found
//! none outside test code and two startup/CLI conveniences). Object storage
//! stays the only durable backend: this tier is opt-in (no directory
//! configured, no disk tier -- callers that never construct a [`DiskCache`]
//! see byte-for-byte the same behavior as before this crate existed), and
//! every failure mode degrades to a cache miss rather than an error. There
//! is structurally no way to return an error from [`DiskCache::get`]: its
//! return type is `Option<Bytes>`, not a `Result`, so a corrupt, truncated,
//! foreign, or missing entry has no error variant available to surface
//! through even by a future bug. `insert` is `()`: the caller already has
//! its own copy of the bytes it is offering to cache, so admission failing
//! silently loses nothing durable.
//!
//! **Crash safety: write-to-temp-name-then-rename, not a checksum-only
//! read.** [`DiskCache::insert`] writes the full header-plus-payload buffer
//! to a scratch path in the same shard directory, then renames it onto the
//! final content-addressed path. `rename(2)` on the same filesystem is
//! atomic: any process that opens the final path either finds nothing (the
//! old miss it already tolerates) or finds every byte the writer produced,
//! never a byte range that stops partway through a write. A process killed
//! mid-write leaves an orphaned temp file that no reader ever looks at,
//! rather than a half-written file sitting at the path readers use. This is
//! the mechanism, not merely a belt on top of a checksum: the checksum below
//! exists for corruption *after* a successful rename (bit rot, a previous
//! release's incompatible format, a hand-edited or garbage file), not for
//! crash safety, which rename already provides on its own.
//!
//! **Read-time verification is crc32c, not blake3, and that is deliberate**
//! (decision 4). [`DiskCache::insert`] computes a blake3 hash of the exact
//! bytes being admitted, once, and stores it in the entry header -- this is
//! the "verified once, on admission" blake3 the ADR asks for: a
//! content-hash-based cache key does not by itself prove the payload bytes
//! are what the key claims (the crate has no way to check a byte range
//! against `CacheKey::content_hash`, which is the *whole object's* hash, not
//! a range's), and computing that fingerprint once at write time is the
//! honest thing to do with it. It is deliberately **not** recomputed on
//! every hit: recomputing blake3 over a multi-megabyte segment range on
//! every cache hit would put a full hash pass on the hot read path, which
//! ADR-0046 rejects outright (rejected alternative 4). Every hit instead
//! recomputes crc32c over the payload -- far cheaper, and exactly the
//! integrity primitive the rest of Ravel's read path already pays for at
//! every level of the footer/section/page/block/frame/window hierarchy, so
//! a corrupt cached range is caught the same way a corrupt store read would
//! be, just earlier.
//!
//! **Plaintext, no encryption inside Ravel** (decision 7). Cached bytes are
//! written to disk exactly as received, with no cipher. **With SSE-KMS
//! configured, cached bytes on local disk are not protected by that key.**
//! A deployment that needs encryption at rest for this directory supplies
//! it at the filesystem layer, the same precedent ADR-0042 already set for
//! object storage.
//!
//! **Eviction** bounds the directory by total resident bytes and entry
//! count (deliverable 7 of issue #442; rejected alternative 6 -- the tier
//! must stay optional, never load-bearing for a node to start). Entries
//! evict in FIFO order: oldest-inserted first. This tier does not reuse the
//! RAM tier's S3-FIFO scan resistance (decision 6 scopes that to the RAM
//! tier, whose caches sit under the compactor and the folder's continuous
//! cold scans in the same process); a disk tier miss costs a network round
//! trip, not a queue promotion, and plain FIFO is enough to keep the
//! directory bounded without that complexity.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::key::CacheKey;
use crate::limits::CacheLimits;
use crate::metrics::CacheMetrics;

const MAGIC: [u8; 4] = *b"RVCD";
const FORMAT_VERSION: u32 = 1;
const HEADER_LEN: usize = 4 + 4 + 16 + 32 + 8 + 8 + 4 + 32;
const ENTRY_EXTENSION: &str = "rvc";

struct Header {
    tenant_hash: [u8; 16],
    content_hash: [u8; 32],
    offset: u64,
    len: u64,
    crc32c: u32,
}

fn encode_header(key: &CacheKey, payload: &[u8]) -> [u8; HEADER_LEN] {
    let mut buf = [0u8; HEADER_LEN];
    let mut pos = 0usize;
    buf[pos..pos + 4].copy_from_slice(&MAGIC);
    pos += 4;
    buf[pos..pos + 4].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    pos += 4;
    buf[pos..pos + 16].copy_from_slice(&key.tenant_hash);
    pos += 16;
    buf[pos..pos + 32].copy_from_slice(&key.content_hash);
    pos += 32;
    buf[pos..pos + 8].copy_from_slice(&key.offset.to_le_bytes());
    pos += 8;
    buf[pos..pos + 8].copy_from_slice(&key.len.to_le_bytes());
    pos += 8;
    buf[pos..pos + 4].copy_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    pos += 4;
    // Recorded once, verified never again on the read path -- see the
    // module docs on why this crate does not decode this field back out.
    buf[pos..pos + 32].copy_from_slice(blake3::hash(payload).as_bytes());
    buf
}

/// Parses a header, or returns `None` for anything that is not exactly a
/// well-formed header this format produced: short reads, bad magic, and an
/// unknown version all collapse to the same "not a usable entry" outcome,
/// because the caller's only two responses are "use it" or "discard it".
fn decode_header(buf: &[u8; HEADER_LEN]) -> Option<Header> {
    let mut pos = 0usize;
    let magic: [u8; 4] = buf[pos..pos + 4].try_into().ok()?;
    pos += 4;
    if magic != MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
    pos += 4;
    if version != FORMAT_VERSION {
        return None;
    }
    let tenant_hash: [u8; 16] = buf[pos..pos + 16].try_into().ok()?;
    pos += 16;
    let content_hash: [u8; 32] = buf[pos..pos + 32].try_into().ok()?;
    pos += 32;
    let offset = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let len = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let crc32c = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
    Some(Header {
        tenant_hash,
        content_hash,
        offset,
        len,
        crc32c,
    })
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
}

/// Deterministic, collision-free path for `key` under `dir`: a two-hex-char
/// shard prefix taken from `content_hash` (directory fan-out, so one busy
/// tenant does not put thousands of files in a single directory), then a
/// filename built from every field of `key` verbatim, so distinct keys can
/// never map to the same path.
fn path_for(dir: &Path, key: &CacheKey) -> PathBuf {
    let mut shard = String::with_capacity(2);
    push_hex(&mut shard, &key.content_hash[..1]);
    let mut name = String::with_capacity(16 + 64 + 16 + 16 + 4);
    push_hex(&mut name, &key.tenant_hash);
    name.push('-');
    push_hex(&mut name, &key.content_hash);
    name.push('-');
    push_hex(&mut name, &key.offset.to_be_bytes());
    name.push('-');
    push_hex(&mut name, &key.len.to_be_bytes());
    dir.join(shard).join(name).with_extension(ENTRY_EXTENSION)
}

struct DiskState {
    order: VecDeque<CacheKey>,
    sizes: HashMap<CacheKey, u64>,
    total_bytes: u64,
}

impl DiskState {
    fn forget(&mut self, key: &CacheKey) {
        if let Some(size) = self.sizes.remove(key) {
            self.total_bytes -= size;
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                self.order.remove(pos);
            }
        }
    }
}

/// The disk tier of ADR-0046's read cache. See the [module docs](self) for
/// the crash-safety, verification, encryption, and eviction rationale.
pub struct DiskCache {
    dir: PathBuf,
    limits: CacheLimits,
    state: Mutex<DiskState>,
    metrics: Arc<CacheMetrics>,
    tmp_counter: AtomicU64,
}

impl DiskCache {
    /// Opens (or begins populating) a disk tier rooted at `dir`, which need
    /// not exist yet: a missing directory is created lazily on the first
    /// successful insert, and is never required for the cache to be
    /// constructed or read from (deliverable 5 -- a node whose cache
    /// directory is deleted mid-flight must go on answering queries
    /// correctly, and that starts with construction itself never failing).
    ///
    /// A best-effort scan seeds byte and entry accounting from whatever is
    /// already on disk (a previous process's entries surviving a restart).
    /// Anything the scan cannot parse as one of this format's own entries
    /// -- a previous release's incompatible layout, a partial file left by
    /// a crash, a foreign file -- is deleted on sight rather than counted:
    /// discard and rebuild, never repair, applies at startup exactly as it
    /// does on a live read.
    pub fn new(dir: PathBuf, limits: CacheLimits) -> Self {
        let (order, sizes, total_bytes) = Self::scan_existing(&dir);
        DiskCache {
            dir,
            limits,
            state: Mutex::new(DiskState {
                order,
                sizes,
                total_bytes,
            }),
            metrics: Arc::new(CacheMetrics::default()),
            tmp_counter: AtomicU64::new(0),
        }
    }

    fn scan_existing(dir: &Path) -> (VecDeque<CacheKey>, HashMap<CacheKey, u64>, u64) {
        let mut order = VecDeque::new();
        let mut sizes = HashMap::new();
        let mut total_bytes = 0u64;
        let Ok(shards) = fs::read_dir(dir) else {
            return (order, sizes, total_bytes);
        };
        for shard in shards.flatten() {
            let Ok(file_type) = shard.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let Ok(entries) = fs::read_dir(shard.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some(ENTRY_EXTENSION) {
                    continue;
                }
                match Self::scan_one(&path) {
                    Some((key, size)) => {
                        total_bytes += size;
                        sizes.insert(key, size);
                        order.push_back(key);
                    }
                    None => {
                        let _ = fs::remove_file(&path);
                    }
                }
            }
        }
        (order, sizes, total_bytes)
    }

    /// Reads just the header of a candidate file and cross-checks the
    /// declared payload length against the file's actual physical size, so
    /// a file truncated between a crash and the next startup is discarded
    /// here rather than accounted for and trusted until the next read.
    fn scan_one(path: &Path) -> Option<(CacheKey, u64)> {
        let metadata = fs::metadata(path).ok()?;
        let mut file = fs::File::open(path).ok()?;
        let mut header_buf = [0u8; HEADER_LEN];
        file.read_exact(&mut header_buf).ok()?;
        let header = decode_header(&header_buf)?;
        let physical_payload = metadata.len().checked_sub(HEADER_LEN as u64)?;
        if physical_payload != header.len {
            return None;
        }
        let key = CacheKey::new(
            header.tenant_hash,
            header.content_hash,
            header.offset,
            header.len,
        );
        Some((key, header.len))
    }

    /// A cloneable handle to this tier's counters, independent of the
    /// cache's own lifetime.
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        self.metrics.clone()
    }

    /// Current number of resident entries this process knows about.
    pub fn len(&self) -> usize {
        self.state.lock().sizes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current total bytes across every resident entry this process knows
    /// about.
    pub fn total_bytes(&self) -> u64 {
        self.state.lock().total_bytes
    }

    /// Look up `key`. Every failure -- the directory or file missing, a
    /// permission error, a short read, a bad header, a key mismatch, an
    /// oversized entry, or a crc32c mismatch -- returns `None`. There is no
    /// error variant this method can return: that is enforced by its
    /// signature, not by a convention callers must remember.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let path = path_for(&self.dir, key);
        match self.read_and_verify(key, &path) {
            Some(bytes) => {
                self.metrics.record_hit(bytes.len() as u64);
                Some(bytes)
            }
            None => {
                self.metrics.record_miss();
                None
            }
        }
    }

    fn read_and_verify(&self, key: &CacheKey, path: &Path) -> Option<Bytes> {
        let mut file = fs::File::open(path).ok()?;
        let mut header_buf = [0u8; HEADER_LEN];
        if file.read_exact(&mut header_buf).is_err() {
            // Shorter than even a header: cannot be a complete entry.
            self.discard(key, path);
            return None;
        }
        let Some(header) = decode_header(&header_buf) else {
            // Bad magic/version: a previous release's format, or a file
            // this cache never wrote at all.
            self.discard(key, path);
            return None;
        };
        if header.tenant_hash != key.tenant_hash
            || header.content_hash != key.content_hash
            || header.offset != key.offset
            || header.len != key.len
        {
            // Parses, but names a different key: never something a correct
            // writer of this format would produce at this path, since the
            // path is derived from the same fields.
            self.discard(key, path);
            return None;
        }
        if header.len > self.limits.max_entry_bytes {
            // Admitted under looser limits before a config change, or hand
            // -placed by a test: never served past the current maximum.
            self.discard(key, path);
            return None;
        }
        let mut payload = vec![0u8; header.len as usize];
        if file.read_exact(&mut payload).is_err() {
            // Header is intact but the payload is short: truncated, either
            // by a crash this format's rename discipline should have
            // prevented, or by damage after the rename.
            drop(file);
            self.discard(key, path);
            return None;
        }
        if crc32c::crc32c(&payload) != header.crc32c {
            drop(file);
            self.discard(key, path);
            return None;
        }
        Some(Bytes::from(payload))
    }

    fn discard(&self, key: &CacheKey, path: &Path) {
        let _ = fs::remove_file(path);
        self.state.lock().forget(key);
    }

    /// Admit `value` under `key`. Never an error and never partially
    /// visible: `value` is written to a scratch path and renamed onto the
    /// content-addressed final path, so a reader only ever sees nothing or
    /// everything (see the crash-safety discussion in the [module
    /// docs](self)). Any failure along the way -- the directory missing and
    /// uncreatable, read-only, out of space, or anything else -- leaves no
    /// trace and is not reported: the caller already has its own copy of
    /// `value`, so nothing is lost by declining to cache it.
    pub fn insert(&self, key: CacheKey, value: &[u8]) {
        let size = value.len() as u64;
        if size > self.limits.max_entry_bytes {
            self.metrics.record_rejected_size();
            return;
        }
        if self.state.lock().sizes.contains_key(&key) {
            // Content-addressed: an existing entry for this key is already
            // these exact bytes.
            return;
        }

        let path = path_for(&self.dir, &key);
        let Some(parent) = path.parent() else {
            return;
        };
        if fs::create_dir_all(parent).is_err() {
            return;
        }
        let tmp_path = self.tmp_path_for(parent);
        let mut buf = Vec::with_capacity(HEADER_LEN + value.len());
        buf.extend_from_slice(&encode_header(&key, value));
        buf.extend_from_slice(value);
        if fs::write(&tmp_path, &buf).is_err() {
            let _ = fs::remove_file(&tmp_path);
            return;
        }
        if fs::rename(&tmp_path, &path).is_err() {
            let _ = fs::remove_file(&tmp_path);
            return;
        }
        self.metrics.record_admission(size);

        let mut state = self.state.lock();
        if state.sizes.contains_key(&key) {
            // Lost a race with a concurrent insert of the same key: both
            // wrote identical bytes (content-addressed), so keep the
            // existing accounting rather than double-count.
            return;
        }
        state.order.push_back(key);
        state.sizes.insert(key, size);
        state.total_bytes += size;
        self.evict_to_bounds(&mut state);
    }

    fn tmp_path_for(&self, parent: &Path) -> PathBuf {
        let id = self.tmp_counter.fetch_add(1, Ordering::Relaxed);
        parent.join(format!(".tmp-{}-{id}", std::process::id()))
    }

    fn evict_to_bounds(&self, state: &mut DiskState) {
        while state.total_bytes > self.limits.max_bytes
            || state.order.len() > self.limits.max_entries
        {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            let Some(size) = state.sizes.remove(&oldest) else {
                continue;
            };
            state.total_bytes -= size;
            let path = path_for(&self.dir, &oldest);
            let _ = fs::remove_file(&path);
            self.metrics.record_eviction();
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::*;

    fn test_key(n: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([9u8; 16], content_hash, 0, 0)
    }

    fn test_key_with_len(n: u64, len: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([9u8; 16], content_hash, 0, len)
    }

    fn generous_limits() -> CacheLimits {
        CacheLimits::new(64 * 1024 * 1024, 10_000, 16 * 1024 * 1024)
    }

    #[cfg(unix)]
    fn make_readonly(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o555)).unwrap();
    }

    #[cfg(unix)]
    fn make_writable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn round_trip_identical_bytes_all_zero_and_zero_length() {
        let tmp = TempDir::new().unwrap();
        let cache = DiskCache::new(tmp.path().to_path_buf(), generous_limits());

        let key_normal = test_key_with_len(1, 5);
        cache.insert(key_normal, b"hello");
        assert_eq!(cache.get(&key_normal).as_deref(), Some(b"hello".as_slice()));

        let key_zero_len = test_key_with_len(2, 0);
        cache.insert(key_zero_len, b"");
        assert_eq!(cache.get(&key_zero_len).as_deref(), Some(b"".as_slice()));

        let all_zero = vec![0u8; 4096];
        let key_all_zero = test_key_with_len(3, all_zero.len() as u64);
        cache.insert(key_all_zero, &all_zero);
        assert_eq!(
            cache.get(&key_all_zero).as_deref(),
            Some(all_zero.as_slice())
        );
    }

    #[test]
    fn every_disk_failure_degrades_to_a_miss() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().to_path_buf();

        // Directory absent, and not creatable: the parent that would hold
        // it is read-only, so `create_dir_all` cannot make the configured
        // directory exist. Reads and inserts must both behave like an
        // ordinary miss, never panic or report an error.
        {
            let readonly_parent = base.join("readonly-parent");
            fs::create_dir_all(&readonly_parent).unwrap();
            let missing_dir = readonly_parent.join("cache-dir");
            #[cfg(unix)]
            {
                make_readonly(&readonly_parent);
                let cache = DiskCache::new(missing_dir, generous_limits());
                let payload: &[u8] = b"payload";
                let key = test_key_with_len(1, payload.len() as u64);
                assert!(cache.get(&key).is_none());
                cache.insert(key, payload); // must not panic
                assert!(cache.get(&key).is_none());
                make_writable(&readonly_parent);
            }
        }

        // Directory read-only: an entry written while writable stays
        // readable; a later insert while read-only is silently dropped
        // rather than erroring.
        {
            let dir = base.join("readonly-dir");
            fs::create_dir_all(&dir).unwrap();
            let cache = DiskCache::new(dir.clone(), generous_limits());
            let cached_before_payload: &[u8] = b"cached before lockdown";
            let already_cached = test_key_with_len(10, cached_before_payload.len() as u64);
            cache.insert(already_cached, cached_before_payload);
            assert!(cache.get(&already_cached).is_some());

            #[cfg(unix)]
            {
                make_readonly(&dir);
                // Any existing shard subdirectory it would need to write
                // into is also read-only, since it was created before lockdown.
                let never_cached_payload: &[u8] = b"should not persist";
                let never_cached = test_key_with_len(11, never_cached_payload.len() as u64);
                cache.insert(never_cached, never_cached_payload);
                assert!(cache.get(&never_cached).is_none());
                make_writable(&dir);
            }
        }

        // Entry truncated mid-file: valid header, payload cut short.
        {
            let dir = base.join("truncated");
            fs::create_dir_all(&dir).unwrap();
            let cache = DiskCache::new(dir.clone(), generous_limits());
            let key = test_key_with_len(20, 100);
            let payload = vec![7u8; 100];
            cache.insert(key, &payload);
            assert!(cache.get(&key).is_some());

            let path = path_for(&dir, &key);
            let full = fs::read(&path).unwrap();
            fs::write(&path, &full[..full.len() - 10]).unwrap();
            assert!(cache.get(&key).is_none());
        }

        // Entry whose crc32c (the per-hit integrity check the header
        // carries; see the module docs on why blake3 is not recomputed
        // here) no longer matches the payload: a single flipped byte after
        // a clean write, simulating bit rot or on-disk damage.
        {
            let dir = base.join("bitrot");
            fs::create_dir_all(&dir).unwrap();
            let cache = DiskCache::new(dir.clone(), generous_limits());
            let key = test_key_with_len(30, 64);
            let payload = vec![0xABu8; 64];
            cache.insert(key, &payload);
            assert!(cache.get(&key).is_some());

            let path = path_for(&dir, &key);
            let mut full = fs::read(&path).unwrap();
            let last = full.len() - 1;
            full[last] ^= 0xFF;
            fs::write(&path, &full).unwrap();
            assert!(cache.get(&key).is_none());
        }

        // Entry over the configured maximum: insert() itself declines to
        // write it, so a lookup right after is a plain miss.
        {
            let dir = base.join("oversized");
            fs::create_dir_all(&dir).unwrap();
            let limits = CacheLimits::new(1024 * 1024, 100, 16);
            let cache = DiskCache::new(dir, limits);
            let key = test_key_with_len(40, 17);
            cache.insert(key, &[0u8; 17]);
            assert!(cache.get(&key).is_none());
            assert_eq!(cache.metrics().snapshot().admissions_rejected_size, 1);
        }

        // Entry over the configured maximum, the other way: it was
        // admitted under looser limits (an earlier process, or a config
        // change), and is only too large under the limits this instance
        // was constructed with.
        {
            let dir = base.join("shrunk-limit");
            fs::create_dir_all(&dir).unwrap();
            let writer = DiskCache::new(dir.clone(), generous_limits());
            let key = test_key_with_len(41, 4096);
            writer.insert(key, &vec![1u8; 4096]);
            assert!(writer.get(&key).is_some());

            let stricter = DiskCache::new(dir, CacheLimits::new(1024 * 1024, 100, 1024));
            assert!(stricter.get(&key).is_none());
        }

        // A garbage file this cache never wrote, sitting exactly at the
        // path a real key would map to.
        {
            let dir = base.join("garbage");
            fs::create_dir_all(&dir).unwrap();
            let cache = DiskCache::new(dir.clone(), generous_limits());
            let key = test_key(50);
            let path = path_for(&dir, &key);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"not a cache entry, just noise from somewhere else").unwrap();
            assert!(cache.get(&key).is_none());
        }
    }

    #[test]
    fn partial_write_never_observed_as_complete() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let cache = DiskCache::new(dir.clone(), generous_limits());

        let key = test_key_with_len(1, 200);
        let payload = vec![0x42u8; 200];
        let full_header = encode_header(&key, &payload);
        let path = path_for(&dir, &key);
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        // Every truncation point short of the complete file: no bytes at
        // all, a partial header, exactly the header with no payload, and a
        // header with a partial payload. None of these is a real write
        // this cache ever performs (insert only ever exposes a complete
        // buffer via rename), but they are exactly what a crash mid-write
        // would leave behind if this cache wrote in place instead of via a
        // temp file, so the read path must refuse all of them regardless.
        let cutoffs = [0, 1, HEADER_LEN / 2, HEADER_LEN, HEADER_LEN + 50];
        for cutoff in cutoffs {
            let mut partial = Vec::with_capacity(cutoff);
            let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
            full.extend_from_slice(&full_header);
            full.extend_from_slice(&payload);
            partial.extend_from_slice(&full[..cutoff.min(full.len())]);
            fs::write(&path, &partial).unwrap();
            assert!(
                cache.get(&key).is_none(),
                "a {cutoff}-byte partial file must never be observed as complete"
            );
        }
    }

    #[test]
    fn deleting_cache_dir_while_live_keeps_reads_correct() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("cache");
        let cache = DiskCache::new(dir.clone(), generous_limits());

        let key = test_key_with_len(1, 5);
        cache.insert(key, b"hello");
        assert_eq!(cache.get(&key).as_deref(), Some(b"hello".as_slice()));

        fs::remove_dir_all(&dir).unwrap();

        // Every subsequent read is a correct miss, never a panic or an
        // error: the cache directory disappearing is exactly the scenario
        // ADR-0046 requires a query to survive, just more slowly (the
        // caller falls back to object storage, which this crate does not
        // model, but "does not panic and reports a clean miss" is the
        // contract this crate owns).
        assert!(cache.get(&key).is_none());
        let other_key = test_key_with_len(2, 5);
        assert!(cache.get(&other_key).is_none());

        // The directory comes back on the next insert, and the tier keeps
        // working correctly rather than staying wedged.
        cache.insert(other_key, b"world");
        assert_eq!(cache.get(&other_key).as_deref(), Some(b"world".as_slice()));
    }

    #[test]
    fn bounds_hold_under_insertion_pressure() {
        let tmp = TempDir::new().unwrap();
        let limits = CacheLimits::new(20 * 1024, 8, 4096);
        let cache = DiskCache::new(tmp.path().to_path_buf(), limits);

        for i in 0..200u64 {
            cache.insert(test_key_with_len(i, 512), &vec![0u8; 512]);
        }

        assert!(
            cache.len() <= 8,
            "entry-count bound violated: {} entries resident",
            cache.len()
        );
        assert!(
            cache.total_bytes() <= 20 * 1024,
            "byte bound violated: {} bytes resident",
            cache.total_bytes()
        );

        // The bound holds on disk too, not just in this process's
        // accounting: no more files are sitting in the directory than the
        // entry count allows.
        let mut on_disk = 0usize;
        for shard in fs::read_dir(tmp.path()).unwrap().flatten() {
            if shard.file_type().unwrap().is_dir() {
                on_disk += fs::read_dir(shard.path()).unwrap().count();
            }
        }
        assert!(
            on_disk <= 8,
            "{on_disk} files left on disk, entry bound is 8"
        );
    }

    #[test]
    fn scan_at_startup_seeds_accounting_and_discards_junk() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        {
            let cache = DiskCache::new(dir.clone(), generous_limits());
            cache.insert(test_key_with_len(1, 10), &[1u8; 10]);
            cache.insert(test_key_with_len(2, 20), &[2u8; 20]);
        }

        // A junk file from some other source, sitting in the same shard
        // layout a real entry would use.
        let junk_shard = dir.join("zz");
        fs::create_dir_all(&junk_shard).unwrap();
        let junk_path = junk_shard.join("not-a-real-entry.rvc");
        let mut junk = fs::File::create(&junk_path).unwrap();
        junk.write_all(b"whatever this is, it is not ours").unwrap();
        drop(junk);

        let reopened = DiskCache::new(dir, generous_limits());
        assert_eq!(reopened.len(), 2);
        assert_eq!(reopened.total_bytes(), 30);
        assert!(
            !junk_path.exists(),
            "a startup scan must discard a file it cannot parse as its own format"
        );
        assert_eq!(
            reopened.get(&test_key_with_len(1, 10)).as_deref(),
            Some([1u8; 10].as_slice())
        );
        assert_eq!(
            reopened.get(&test_key_with_len(2, 20)).as_deref(),
            Some([2u8; 20].as_slice())
        );
    }
}
