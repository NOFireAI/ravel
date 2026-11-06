//! Process-local scratch backend over a local directory: the transient run
//! store of ADR-3853 D2.
//!
//! [`ScratchStore`] is a cache and nothing else. It is not durable and not
//! shared: [`ScratchStore::open`] removes everything the directory held, so
//! nothing a previous process wrote is ever read back, and
//! [`ScratchStore::wipe`] empties it again at the end of a unit. No other
//! process reads it, nothing in it is published, and no durability or
//! recovery path may depend on it (CLAUDE.md, "Object storage is the source
//! of truth"). It must never back a tenant's data; it exists only so the
//! two-level merge can run `load_catalog_from_object` and `merge_catalogs`
//! over its runs unmodified.
//!
//! Layout. A key maps to a path of one file name per `/`-separated segment:
//! every segment but the last is a directory named `d` plus the escaped
//! segment, and the last is a file named `f` plus the escaped segment.
//! Escaping keeps ASCII letters, digits, `-` and `_` and writes every other
//! byte as `%XX`, so no escaped name is empty, `.`, `..`, or contains a
//! separator or NUL, and no key can name a path outside the directory. A
//! key whose escaped segment exceeds 255 bytes, the common file name limit,
//! is refused with [`StoreError::Permanent`]. Each file holds a 32-byte
//! header (version counter, last-modified time, CRC-32C of the bytes)
//! followed by the object's bytes. The header is private to this process and
//! is not a persistent format: nothing outlives the process that wrote it.
//!
//! Semantics mirror [`crate::memory::MemoryStore`] within one process, and
//! the store declares the same [`Capabilities`]; a local directory gives up
//! none of them here, because every guarantee is made inside the one process
//! that owns the directory:
//!
//! - A put writes a temporary file in the directory and renames it into
//!   place, so a reader opens either the previous object or the complete new
//!   one, never a partial write.
//! - Conditional puts and deletes are decided and applied under one lock, so
//!   `CreateIfAbsent` and `CasVersion` have exactly the oracle's outcomes;
//!   the filesystem is never asked to arbitrate a race.
//! - Etags and versions come from a counter that is never reset, so a
//!   recreated key never reuses a version.
//! - A full-object get is checked against the CRC-32C recorded at write, and
//!   a ranged get is not, as on the oracle.
//! - A multipart upload streams its parts into one temporary file and is
//!   published by the same rename at `complete`.
//!
//! Every [`ObjectStoreBackend`] operation runs its filesystem calls on
//! tokio's blocking pool; `open` and `wipe` block the calling thread.
//! Filesystem errors are [`StoreError::Permanent`].

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PartSequence, Pin, PinnedRead, PutMode, PutOptions, PutOutcome,
    StoreError, UploadChecksum, Version, check_addressable, classify_objects, classify_prefixes,
    multipart_finished, multipart_poisoned,
};

/// Name prefix of a directory standing for a non-final key segment.
const DIR_MARK: char = 'd';
/// Name prefix of a file holding an object, for the final key segment.
const OBJECT_MARK: char = 'f';
/// Name prefix of an in-flight write's temporary file, at the top level only.
const TEMP_MARK: char = 't';

/// Longest file name the store creates.
const MAX_NAME_BYTES: usize = 255;

const MAGIC: [u8; 8] = *b"RVSCRT01";
const HEADER_LEN: usize = 32;

/// Scratch [`ObjectStoreBackend`] over a local directory. See the module
/// documentation: process-local, non-durable, never a tenant's store.
pub struct ScratchStore {
    inner: Arc<Inner>,
}

struct Inner {
    root: PathBuf,
    page_size: usize,
    counter: AtomicU64,
    temp_counter: AtomicU64,
    clock_ms: AtomicU64,
    /// Held while a mutation decides and applies its outcome, and while a
    /// listing reads its snapshot.
    commit: Mutex<()>,
}

#[derive(Debug, Clone, Copy)]
struct Header {
    id: u64,
    last_modified_unix_ms: i64,
    checksum: u32,
}

impl Header {
    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[..8].copy_from_slice(&MAGIC);
        out[8..16].copy_from_slice(&self.id.to_le_bytes());
        out[16..24].copy_from_slice(&self.last_modified_unix_ms.to_le_bytes());
        out[24..28].copy_from_slice(&self.checksum.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8; HEADER_LEN], path: &Path) -> Result<Self, StoreError> {
        if bytes[..8] != MAGIC {
            return Err(StoreError::Corrupted(format!(
                "scratch object {} has no scratch header",
                path.display()
            )));
        }
        let mut id = [0u8; 8];
        id.copy_from_slice(&bytes[8..16]);
        let mut modified = [0u8; 8];
        modified.copy_from_slice(&bytes[16..24]);
        let mut checksum = [0u8; 4];
        checksum.copy_from_slice(&bytes[24..28]);
        Ok(Header {
            id: u64::from_le_bytes(id),
            last_modified_unix_ms: i64::from_le_bytes(modified),
            checksum: u32::from_le_bytes(checksum),
        })
    }

    fn etag(&self) -> Etag {
        Etag(format!("scratch-etag-{}", self.id))
    }

    fn version(&self) -> Version {
        Version(format!("scratch-v-{}", self.id))
    }

    fn pin(&self) -> Pin {
        Pin::from_store(self.etag().0, Some(self.version().0))
    }

    fn meta(&self, key: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            key: key.to_string(),
            size,
            etag: self.etag(),
            version: self.version(),
            last_modified_unix_ms: self.last_modified_unix_ms,
        }
    }
}

/// An object opened for reading: the file, its header, and the object's size.
struct Opened {
    file: File,
    header: Header,
    size: u64,
}

fn io_error(what: &str, path: &Path, err: io::Error) -> StoreError {
    StoreError::Permanent(format!("scratch store: {what} {}: {err}", path.display()))
}

fn escape_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn unescape_segment(escaped: &str) -> Option<String> {
    let bytes = escaped.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'%' {
            let hex = escaped.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            out.push(byte);
            i += 1;
        } else {
            return None;
        }
    }
    String::from_utf8(out).ok()
}

/// The file names `key` maps to, outermost first: a `d` name per non-final
/// segment and an `f` name for the final one.
fn key_components(key: &str) -> Vec<String> {
    let segments: Vec<&str> = key.split('/').collect();
    let last = segments.len() - 1;
    segments
        .iter()
        .enumerate()
        .map(|(i, segment)| {
            let mark = if i == last { OBJECT_MARK } else { DIR_MARK };
            format!("{mark}{}", escape_segment(segment))
        })
        .collect()
}

/// The inverse of [`key_components`]; `None` for names it never produces.
#[cfg(test)]
fn key_from_components(components: &[String]) -> Option<String> {
    let (last, dirs) = components.split_last()?;
    let mut key = String::new();
    for dir in dirs {
        key.push_str(&unescape_segment(dir.strip_prefix(DIR_MARK)?)?);
        key.push('/');
    }
    key.push_str(&unescape_segment(last.strip_prefix(OBJECT_MARK)?)?);
    Some(key)
}

fn range_bounds(range: GetRange, len: u64) -> Result<(u64, u64), StoreError> {
    match range {
        GetRange::Full => Ok((0, len)),
        GetRange::Range(start, end) => {
            if start >= end || start >= len {
                return Err(StoreError::InvalidRange(format!(
                    "[{start}, {end}) of {len}-byte object"
                )));
            }
            Ok((start, end.min(len)))
        }
        GetRange::Suffix(n) => {
            if n == 0 {
                return Err(StoreError::InvalidRange("zero-length suffix".into()));
            }
            Ok((len - n.min(len), len))
        }
    }
}

fn verify_upload_checksum(data: &[u8], checksum: Option<UploadChecksum>) -> Result<(), StoreError> {
    if let Some(UploadChecksum::Crc32c(expected)) = checksum {
        let actual = crc32c::crc32c(data);
        if actual != expected {
            return Err(StoreError::Corrupted(format!(
                "upload checksum mismatch: expected {expected:08x}, computed {actual:08x}"
            )));
        }
    }
    Ok(())
}

/// Removes every entry inside `dir`, leaving `dir` itself. A symlink is
/// removed, never followed.
fn clear_dir(dir: &Path) -> Result<(), StoreError> {
    let entries = fs::read_dir(dir).map_err(|e| io_error("read", dir, e))?;
    for entry in entries {
        let entry = entry.map_err(|e| io_error("read", dir, e))?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| io_error("stat", &path, e))?;
        if file_type.is_dir() {
            fs::remove_dir_all(&path).map_err(|e| io_error("remove", &path, e))?;
        } else {
            fs::remove_file(&path).map_err(|e| io_error("remove", &path, e))?;
        }
    }
    Ok(())
}

impl ScratchStore {
    /// Opens a scratch store over `dir` with listing pages of `page_size`
    /// keys (at least 1). Creates `dir` if it does not exist, then removes
    /// everything inside it, whatever wrote it, before returning.
    ///
    /// `dir` must be dedicated to this store: its contents are deleted
    /// without inspection. Blocks the calling thread for the filesystem work.
    pub fn open(dir: PathBuf, page_size: usize) -> Result<Self, StoreError> {
        fs::create_dir_all(&dir).map_err(|e| io_error("create", &dir, e))?;
        clear_dir(&dir)?;
        Ok(ScratchStore {
            inner: Arc::new(Inner {
                root: dir,
                page_size: page_size.max(1),
                counter: AtomicU64::new(0),
                temp_counter: AtomicU64::new(0),
                clock_ms: AtomicU64::new(0),
                commit: Mutex::new(()),
            }),
        })
    }

    /// Removes every object and every in-flight write's temporary file, for
    /// the unit-end cleanup of ADR-3853 D2. A put or multipart upload still
    /// in flight when this runs fails rather than publishing. Versions keep
    /// counting from where they were. Blocks the calling thread.
    pub fn wipe(&self) -> Result<(), StoreError> {
        let _guard = self.inner.commit.lock();
        clear_dir(&self.inner.root)
    }

    /// The directory this store owns.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Sets the clock recorded as each later write's last-modified time. It
    /// starts at 0, as [`crate::memory::MemoryStore`]'s does.
    pub fn set_clock_ms(&self, ms: u64) {
        self.inner.clock_ms.store(ms, Ordering::SeqCst);
    }

    async fn blocking<T, F>(&self, work: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Inner) -> Result<T, StoreError> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || work(&inner))
            .await
            .map_err(|e| StoreError::Permanent(format!("scratch store task failed: {e}")))?
    }
}

impl Inner {
    fn object_path(&self, key: &str) -> Result<PathBuf, StoreError> {
        let mut path = self.root.clone();
        for component in key_components(key) {
            if component.len() > MAX_NAME_BYTES {
                return Err(StoreError::Permanent(format!(
                    "scratch store: key {key:?} has a segment that maps to a {}-byte file \
                     name, over the {MAX_NAME_BYTES}-byte limit",
                    component.len()
                )));
            }
            path.push(component);
        }
        Ok(path)
    }

    fn now_ms(&self) -> i64 {
        self.clock_ms.load(Ordering::SeqCst) as i64
    }

    /// A fresh temporary file at the top level, with room for the header.
    fn create_temp(&self) -> Result<(File, PathBuf), StoreError> {
        let id = self.temp_counter.fetch_add(1, Ordering::SeqCst);
        let path = self.root.join(format!("{TEMP_MARK}{id}"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| io_error("create", &path, e))?;
        file.write_all(&[0u8; HEADER_LEN])
            .map_err(|e| io_error("write", &path, e))?;
        Ok((file, path))
    }

    fn open_object(&self, path: &Path) -> Result<Option<Opened>, StoreError> {
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error("open", path, e)),
        };
        let len = file
            .metadata()
            .map_err(|e| io_error("stat", path, e))?
            .len();
        let Some(size) = len.checked_sub(HEADER_LEN as u64) else {
            return Err(StoreError::Corrupted(format!(
                "scratch object {} is {len} bytes, shorter than its header",
                path.display()
            )));
        };
        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|e| io_error("read", path, e))?;
        let header = Header::decode(&header, path)?;
        Ok(Some(Opened { file, header, size }))
    }

    fn read_range(
        &self,
        key: &str,
        path: &Path,
        opened: &mut Opened,
        range: GetRange,
    ) -> Result<Bytes, StoreError> {
        let (start, end) = range_bounds(range, opened.size)?;
        let len = usize::try_from(end - start).map_err(|_| {
            StoreError::Permanent(format!("scratch store: get of {key}: range too large"))
        })?;
        let mut data = vec![0u8; len];
        opened
            .file
            .seek(SeekFrom::Start(HEADER_LEN as u64 + start))
            .map_err(|e| io_error("seek", path, e))?;
        opened
            .file
            .read_exact(&mut data)
            .map_err(|e| io_error("read", path, e))?;
        if range == GetRange::Full {
            let actual = crc32c::crc32c(&data);
            if actual != opened.header.checksum {
                return Err(StoreError::Corrupted(format!(
                    "get of {key}: stored crc32c {:08x} does not match {actual:08x} computed \
                     over the {} stored bytes",
                    opened.header.checksum,
                    data.len()
                )));
            }
        }
        Ok(Bytes::from(data))
    }

    fn get(&self, key: &str, range: GetRange, pin: Option<&Pin>) -> Result<PinnedRead, StoreError> {
        let path = self.object_path(key)?;
        let mut opened = self.open_object(&path)?.ok_or(StoreError::NotFound)?;
        let header = opened.header;
        if let Some(pin) = pin {
            if let Some(version) = pin.version.as_deref()
                && header.version().0 != version
            {
                return Err(StoreError::NotFound);
            }
            if header.etag().0 != pin.etag {
                return Err(StoreError::PreconditionFailed);
            }
        }
        let data = self.read_range(key, &path, &mut opened, range)?;
        Ok(PinnedRead {
            outcome: GetOutcome {
                data,
                etag: header.etag(),
                version: header.version(),
                total_size: opened.size,
            },
            pin: header.pin(),
        })
    }

    fn head(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        let path = self.object_path(key)?;
        let opened = self.open_object(&path)?.ok_or(StoreError::NotFound)?;
        Ok((opened.header.meta(key, opened.size), opened.header.pin()))
    }

    /// Decides `mode` against the current object at `path` and, if it
    /// allows the write, stamps `file`'s header and renames it into place,
    /// all under the commit lock.
    fn commit(
        &self,
        file: &mut File,
        temp: &Path,
        path: &Path,
        mode: &PutMode,
        checksum: u32,
    ) -> Result<PutOutcome, StoreError> {
        let _guard = self.commit.lock();
        match mode {
            PutMode::Overwrite => {}
            PutMode::CreateIfAbsent => {
                if self.open_object(path)?.is_some() {
                    return Err(StoreError::AlreadyExists);
                }
            }
            PutMode::CasVersion(expected) => match self.open_object(path)? {
                Some(current) if &current.header.version() == expected => {}
                _ => return Err(StoreError::PreconditionFailed),
            },
        }
        let header = Header {
            id: self.counter.fetch_add(1, Ordering::SeqCst) + 1,
            last_modified_unix_ms: self.now_ms(),
            checksum,
        };
        file.seek(SeekFrom::Start(0))
            .map_err(|e| io_error("seek", temp, e))?;
        file.write_all(&header.encode())
            .map_err(|e| io_error("write", temp, e))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_error("create", parent, e))?;
        }
        fs::rename(temp, path).map_err(|e| io_error("rename into", path, e))?;
        Ok(PutOutcome {
            etag: header.etag(),
            version: header.version(),
        })
    }

    fn put(&self, key: &str, data: &[u8], mode: &PutMode) -> Result<PutOutcome, StoreError> {
        let path = self.object_path(key)?;
        let (mut file, temp) = self.create_temp()?;
        let result = file
            .write_all(data)
            .map_err(|e| io_error("write", &temp, e))
            .and_then(|()| self.commit(&mut file, &temp, &path, mode, crc32c::crc32c(data)));
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        let path = self.object_path(key)?;
        let _guard = self.commit.lock();
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error("remove", &path, e)),
        }
    }

    /// Every key under `prefix`, in key order. Descends only into directories
    /// whose keys can start with `prefix`. The caller holds the commit lock.
    fn keys_under(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let mut keys = Vec::new();
        self.walk(&self.root, "", prefix, &mut keys)?;
        keys.sort_unstable();
        Ok(keys)
    }

    fn walk(
        &self,
        dir: &Path,
        key_prefix: &str,
        prefix: &str,
        out: &mut Vec<String>,
    ) -> Result<(), StoreError> {
        let top = key_prefix.is_empty();
        let entries = fs::read_dir(dir).map_err(|e| io_error("read", dir, e))?;
        for entry in entries {
            let entry = entry.map_err(|e| io_error("read", dir, e))?;
            let path = entry.path();
            let unexpected = || {
                StoreError::Corrupted(format!(
                    "scratch store holds {}, which no key maps to",
                    path.display()
                ))
            };
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(unexpected)?;
            if top && name.starts_with(TEMP_MARK) {
                continue;
            }
            let is_dir = entry
                .file_type()
                .map_err(|e| io_error("stat", &path, e))?
                .is_dir();
            if let (Some(escaped), true) = (name.strip_prefix(DIR_MARK), is_dir) {
                let segment = unescape_segment(escaped).ok_or_else(unexpected)?;
                let child = format!("{key_prefix}{segment}/");
                if child.starts_with(prefix) || prefix.starts_with(&child) {
                    self.walk(&path, &child, prefix, out)?;
                }
            } else if let (Some(escaped), false) = (name.strip_prefix(OBJECT_MARK), is_dir) {
                let segment = unescape_segment(escaped).ok_or_else(unexpected)?;
                let key = format!("{key_prefix}{segment}");
                if key.starts_with(prefix) {
                    out.push(key);
                }
            } else {
                return Err(unexpected());
            }
        }
        Ok(())
    }

    fn listed_meta(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let path = self.object_path(key)?;
        let opened = self.open_object(&path)?.ok_or_else(|| {
            StoreError::Corrupted(format!("listed key {key} vanished under the commit lock"))
        })?;
        Ok(opened.header.meta(key, opened.size))
    }

    /// One page of keys under `prefix` starting at `start`, strictly after it
    /// when `exclusive`, with the oracle's page and token rules.
    fn list_page(
        &self,
        prefix: &str,
        start: &str,
        exclusive: bool,
    ) -> Result<ListPage, StoreError> {
        let _guard = self.commit.lock();
        let mut out = Vec::with_capacity(self.page_size.min(64));
        for key in self.keys_under(prefix)? {
            let after = if exclusive {
                key.as_str() > start
            } else {
                key.as_str() >= start
            };
            if !after {
                continue;
            }
            out.push(self.listed_meta(&key)?);
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

    fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let _guard = self.commit.lock();
        let mut direct = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        for key in self.keys_under(prefix)? {
            let rest = &key[prefix.len()..];
            match rest.find('/') {
                Some(idx) => {
                    let common = format!("{prefix}{}", &rest[..=idx]);
                    if prefixes.last() != Some(&common) {
                        prefixes.push(common);
                    }
                }
                None => direct.push(self.listed_meta(&key)?),
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
}

/// Multipart upload against [`ScratchStore`]: parts are appended to one
/// temporary file, under the same part rules as every backend
/// ([`PartSequence`]), and `complete` publishes it by the rename a single
/// put uses. Until then the key does not exist; an aborted or dropped upload
/// removes its temporary file.
pub struct ScratchMultipartUpload {
    inner: Arc<Inner>,
    key: String,
    path: PathBuf,
    /// The temporary file and its path; `None` once completed, aborted, or
    /// lost to a failed write.
    temp: Option<(File, PathBuf)>,
    checksum: u32,
    sequence: PartSequence,
    finished: bool,
    poison: Option<String>,
}

impl ScratchMultipartUpload {
    fn discard_temp(&mut self) {
        if let Some((_, temp)) = self.temp.take() {
            let _ = fs::remove_file(temp);
        }
    }

    fn check_open(&self) -> Result<(), StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        if let Some(cause) = &self.poison {
            return Err(multipart_poisoned(&self.key, cause));
        }
        Ok(())
    }

    fn poisoned(&mut self, err: StoreError) -> StoreError {
        self.poison = Some(err.to_string());
        self.discard_temp();
        err
    }
}

impl Drop for ScratchMultipartUpload {
    fn drop(&mut self) {
        self.discard_temp();
    }
}

fn join_failed(e: tokio::task::JoinError) -> StoreError {
    StoreError::Permanent(format!("scratch store task failed: {e}"))
}

#[async_trait::async_trait]
impl MultipartUpload for ScratchMultipartUpload {
    async fn put_part(
        &mut self,
        data: Bytes,
        checksum: Option<UploadChecksum>,
    ) -> Result<(), StoreError> {
        self.check_open()?;
        verify_upload_checksum(&data, checksum)?;
        if let Err(e) = self.sequence.accept(&self.key, data.len()) {
            self.poison = Some(e.to_string());
            return Err(e);
        }
        let Some((mut file, temp)) = self.temp.take() else {
            return Err(multipart_poisoned(&self.key, "temporary file lost"));
        };
        let checksum = self.checksum;
        let written = tokio::task::spawn_blocking(move || {
            let result = file
                .write_all(&data)
                .map_err(|e| io_error("write", &temp, e))
                .map(|()| crc32c::crc32c_append(checksum, &data));
            (file, temp, result)
        })
        .await;
        match written {
            Ok((file, temp, Ok(checksum))) => {
                self.temp = Some((file, temp));
                self.checksum = checksum;
                Ok(())
            }
            Ok((file, temp, Err(e))) => {
                self.temp = Some((file, temp));
                Err(self.poisoned(e))
            }
            Err(e) => Err(self.poisoned(join_failed(e))),
        }
    }

    async fn complete(&mut self) -> Result<PutOutcome, StoreError> {
        self.check_open()?;
        self.sequence.finish(&self.key)?;
        let Some((mut file, temp)) = self.temp.take() else {
            return Err(multipart_poisoned(&self.key, "temporary file lost"));
        };
        let inner = Arc::clone(&self.inner);
        let path = self.path.clone();
        let checksum = self.checksum;
        let committed = tokio::task::spawn_blocking(move || {
            let result = inner.commit(&mut file, &temp, &path, &PutMode::Overwrite, checksum);
            if result.is_err() {
                let _ = fs::remove_file(&temp);
            }
            result
        })
        .await
        .map_err(join_failed)
        .and_then(|result| result);
        match committed {
            Ok(outcome) => {
                self.finished = true;
                Ok(outcome)
            }
            Err(e) => Err(self.poisoned(e)),
        }
    }

    async fn abort(&mut self) -> Result<(), StoreError> {
        if self.finished {
            return Err(multipart_finished(&self.key));
        }
        self.finished = true;
        self.discard_temp();
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for ScratchStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        check_addressable(key)?;
        verify_upload_checksum(&data, opts.checksum)?;
        let key = key.to_string();
        self.blocking(move |inner| inner.put(&key, &data, &opts.mode))
            .await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        check_addressable(key)?;
        let key = key.to_string();
        self.blocking(move |inner| inner.get(&key, range, None).map(|read| read.outcome))
            .await
    }

    /// The oracle's order: a missing key is `NotFound`, a version selector
    /// other than the current version is `NotFound` (only the current object
    /// is kept), and an etag mismatch on the selected object is
    /// `PreconditionFailed`. Decided on the same open file the bytes come
    /// from, so an overwrite cannot land between the check and the read.
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        check_addressable(key)?;
        let key = key.to_string();
        let pin = pin.clone();
        self.blocking(move |inner| inner.get(&key, range, Some(&pin)))
            .await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        check_addressable(key)?;
        let key = key.to_string();
        self.blocking(move |inner| inner.get(&key, range, None))
            .await
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        check_addressable(key)?;
        let key = key.to_string();
        self.blocking(move |inner| inner.head(&key)).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        check_addressable(key)?;
        let path = self.inner.object_path(key)?;
        let (file, temp) = self.blocking(|inner| inner.create_temp()).await?;
        Ok(Box::new(ScratchMultipartUpload {
            inner: Arc::clone(&self.inner),
            key: key.to_string(),
            path,
            temp: Some((file, temp)),
            checksum: 0,
            sequence: PartSequence::default(),
            finished: false,
            poison: None,
        }))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        check_addressable(key)?;
        let key = key.to_string();
        self.blocking(move |inner| inner.head(&key).map(|(meta, _)| meta))
            .await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let prefix = prefix.to_string();
        self.blocking(move |inner| match page {
            Some(PageToken(after)) => inner.list_page(&prefix, &after, true),
            None => inner.list_page(&prefix, &prefix, false),
        })
        .await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let prefix = prefix.to_string();
        let start_after = start_after.map(str::to_string);
        self.blocking(move |inner| match (page, start_after) {
            (Some(PageToken(after)), _) => inner.list_page(&prefix, &after, true),
            (None, Some(after)) if after >= prefix => inner.list_page(&prefix, &after, true),
            (None, _) => inner.list_page(&prefix, &prefix, false),
        })
        .await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let prefix = prefix.to_string();
        self.blocking(move |inner| inner.list_delimited(&prefix))
            .await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        check_addressable(key)?;
        let key = key.to_string();
        self.blocking(move |inner| inner.delete(&key)).await
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
            multipart: true,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;
    use crate::list_all;

    fn open_in(dir: &Path) -> ScratchStore {
        ScratchStore::open(dir.to_path_buf(), 1000).expect("open scratch store")
    }

    /// Every path under `dir`, relative to it, files and directories alike.
    fn tree(dir: &Path) -> BTreeSet<PathBuf> {
        fn visit(base: &Path, dir: &Path, out: &mut BTreeSet<PathBuf>) {
            for entry in fs::read_dir(dir).expect("read dir") {
                let path = entry.expect("dir entry").path();
                out.insert(path.strip_prefix(base).expect("under base").to_path_buf());
                if path.is_dir() {
                    visit(base, &path, out);
                }
            }
        }
        let mut out = BTreeSet::new();
        visit(dir, dir, &mut out);
        out
    }

    #[tokio::test]
    async fn open_wipes_whatever_the_directory_held() {
        let parent = tempfile::tempdir().expect("tempdir");
        let dir = parent.path().join("scratch");
        fs::create_dir_all(dir.join("nested/deeper")).expect("nested dirs");
        fs::write(dir.join("left-over.bin"), b"previous process").expect("file");
        fs::write(dir.join("nested/deeper/run-0.rlog"), b"old run").expect("nested file");
        fs::write(dir.join("fstale"), b"looks like an object").expect("object-shaped file");
        fs::write(dir.join("t7"), b"looks like a temp file").expect("temp-shaped file");
        assert_eq!(tree(&dir).len(), 6, "the fixture is in place");

        let store = open_in(&dir);

        assert!(dir.is_dir(), "the directory itself is kept");
        assert!(tree(&dir).is_empty(), "left: {:?}", tree(&dir));
        assert!(list_all(&store, "").await.expect("list").is_empty());
        assert!(matches!(
            store.get("stale", GetRange::Full).await,
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn open_creates_a_missing_directory() {
        let parent = tempfile::tempdir().expect("tempdir");
        let dir = parent.path().join("a/b/scratch");
        let store = ScratchStore::open(dir.clone(), 2).expect("open");
        assert!(dir.is_dir());
        assert_eq!(store.root(), dir.as_path());
    }

    #[tokio::test]
    async fn wipe_empties_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open_in(dir.path());
        let mut last_before_wipe = None;
        for key in ["runs/0/part-0", "runs/0/part-1", "runs/1/part-0", "top"] {
            last_before_wipe = Some(
                store
                    .put(key, Bytes::from_static(b"run bytes"), PutOptions::default())
                    .await
                    .expect("put"),
            );
        }
        let last_before_wipe = last_before_wipe.expect("four puts");
        let mut upload = store.put_multipart("runs/2/part-0").await.expect("start");
        upload
            .put_part(Bytes::from_static(b"in flight"), None)
            .await
            .expect("part");
        assert_eq!(list_all(&store, "").await.expect("list").len(), 4);

        store.wipe().expect("wipe");

        assert!(tree(dir.path()).is_empty(), "left: {:?}", tree(dir.path()));
        assert!(list_all(&store, "").await.expect("list").is_empty());
        assert!(matches!(
            store.head("runs/0/part-0").await,
            Err(StoreError::NotFound)
        ));
        assert!(
            upload.complete().await.is_err(),
            "an upload whose temporary file was wiped cannot publish"
        );
        assert!(matches!(
            store.head("runs/2/part-0").await,
            Err(StoreError::NotFound)
        ));

        let first = store
            .put("top", Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put after wipe");
        let id = |v: &Version| -> u64 {
            v.0.strip_prefix("scratch-v-")
                .and_then(|n| n.parse().ok())
                .expect("scratch version")
        };
        assert!(
            id(&first.version) > id(&last_before_wipe.version),
            "versions keep counting across a wipe: {:?} after {:?}",
            first.version,
            last_before_wipe.version
        );
    }

    #[tokio::test]
    async fn a_key_with_slashes_and_dots_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open_in(dir.path());
        store.set_clock_ms(1_234);
        let keys = [
            "t/acme/runs/hour=08/run.0001.rlog",
            "a..b/...c/x.y",
            "with space/plus+sign/.hidden",
            "a",
            "a/b",
        ];
        for (i, key) in keys.iter().enumerate() {
            store
                .put(
                    key,
                    Bytes::from(format!("object {i}")),
                    PutOptions::default(),
                )
                .await
                .expect("put");
        }
        for (i, key) in keys.iter().enumerate() {
            let got = store.get(key, GetRange::Full).await.expect("get");
            assert_eq!(got.data, Bytes::from(format!("object {i}")), "{key}");
            let meta = store.head(key).await.expect("head");
            assert_eq!(meta.key, *key);
            assert_eq!(meta.last_modified_unix_ms, 1_234);
        }
        let listed: Vec<String> = list_all(&store, "")
            .await
            .expect("list")
            .into_iter()
            .map(|m| m.key)
            .collect();
        let mut sorted: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        sorted.sort();
        assert_eq!(
            listed, sorted,
            "every key lists back verbatim, in key order"
        );
    }

    /// Non-ASCII keys are unaddressable (ADR-2637), so the store refuses them
    /// as every backend does; the file name mapping itself still round-trips
    /// them.
    #[tokio::test]
    async fn a_unicode_key_round_trips_through_the_mapping_and_is_refused_as_unaddressable() {
        let key = "résumé/日本/ключ.bin";
        let components = key_components(key);
        assert_eq!(key_from_components(&components).as_deref(), Some(key));
        assert!(components.iter().all(|c| c.is_ascii()));

        let dir = tempfile::tempdir().expect("tempdir");
        let store = open_in(dir.path());
        let oracle = crate::memory::MemoryStore::new();
        for result in [
            store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await,
            oracle
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await,
        ] {
            assert!(matches!(result, Err(StoreError::UnaddressableKey { .. })));
        }
        assert!(tree(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn traversal_keys_are_refused_and_never_write_outside_the_directory() {
        let parent = tempfile::tempdir().expect("tempdir");
        fs::write(parent.path().join("sentinel"), b"untouched").expect("sentinel");
        let dir = parent.path().join("scratch");
        let store = open_in(&dir);
        let before = tree(parent.path());

        for key in [
            "..",
            "../escape",
            "a/../../escape",
            "a/./b",
            "/absolute/path",
            "/",
            "a\0b",
            "../../../../../../../../tmp/ravel-scratch-escape",
        ] {
            let result = store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await;
            assert!(
                matches!(result, Err(StoreError::UnaddressableKey { .. })),
                "{key:?}: {result:?}"
            );
            assert!(store.put_multipart(key).await.is_err(), "{key:?}");
        }
        assert_eq!(tree(parent.path()), before, "nothing was written anywhere");

        // The mapping alone, without the addressability check in front of it,
        // keeps every such key inside the directory.
        for key in [
            "..",
            "../escape",
            "a/../../escape",
            "/absolute/path",
            "a\0b",
        ] {
            let path = store.inner.object_path(key).expect("mappable");
            let relative = path.strip_prefix(&dir).expect("inside the directory");
            assert!(
                relative
                    .components()
                    .all(|c| matches!(c, std::path::Component::Normal(_))),
                "{key:?} maps to {relative:?}"
            );
        }

        // Dots inside a segment are ordinary bytes.
        store
            .put("..a/b..", Bytes::from_static(b"in"), PutOptions::default())
            .await
            .expect("dotted key");
        let after: BTreeSet<PathBuf> = tree(parent.path())
            .into_iter()
            .filter(|p| !before.contains(p))
            .collect();
        assert!(!after.is_empty());
        assert!(
            after.iter().all(|p| p.starts_with("scratch")),
            "new paths: {after:?}"
        );
        assert_eq!(
            fs::read(parent.path().join("sentinel")).expect("sentinel"),
            b"untouched"
        );
    }

    #[tokio::test]
    async fn a_segment_too_long_for_a_file_name_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open_in(dir.path());
        let key = format!("runs/{}", "x".repeat(MAX_NAME_BYTES));
        let result = store
            .put(&key, Bytes::from_static(b"x"), PutOptions::default())
            .await;
        assert!(
            matches!(result, Err(StoreError::Permanent(_))),
            "{result:?}"
        );
        assert!(tree(dir.path()).is_empty());
        let fits = format!("runs/{}", "x".repeat(MAX_NAME_BYTES - 1));
        store
            .put(&fits, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("a 255-byte file name fits");
    }

    #[tokio::test]
    async fn an_open_multipart_upload_is_neither_listed_nor_readable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open_in(dir.path());
        let mut upload = store.put_multipart("m/object").await.expect("start");
        upload
            .put_part(Bytes::from_static(b"part"), None)
            .await
            .expect("part");
        assert_eq!(tree(dir.path()).len(), 1, "one temporary file");
        assert!(list_all(&store, "").await.expect("list").is_empty());
        assert!(matches!(
            store.head("m/object").await,
            Err(StoreError::NotFound)
        ));
        drop(upload);
        assert!(
            tree(dir.path()).is_empty(),
            "a dropped upload removes its temporary file"
        );
    }

    #[tokio::test]
    async fn a_recreated_key_never_reuses_a_version_or_etag() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open_in(dir.path());
        let first = store
            .put("k", Bytes::from_static(b"same"), PutOptions::default())
            .await
            .expect("put");
        store.delete("k").await.expect("delete");
        let second = store
            .put(
                "k",
                Bytes::from_static(b"same"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("create after delete");
        assert_ne!(second.version, first.version);
        assert_ne!(second.etag, first.etag);
        assert!(matches!(
            store
                .put(
                    "k",
                    Bytes::from_static(b"same"),
                    PutOptions {
                        mode: PutMode::CasVersion(first.version.clone()),
                        ..PutOptions::default()
                    }
                )
                .await,
            Err(StoreError::PreconditionFailed)
        ));
    }

    #[tokio::test]
    async fn a_full_get_refuses_bytes_that_changed_at_rest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open_in(dir.path());
        store
            .put(
                "k",
                Bytes::from_static(b"0123456789"),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let path = store.inner.object_path("k").expect("path");
        let mut bytes = fs::read(&path).expect("read");
        bytes[HEADER_LEN] ^= 1;
        fs::write(&path, bytes).expect("corrupt");
        assert!(matches!(
            store.get("k", GetRange::Full).await,
            Err(StoreError::Corrupted(_))
        ));
        let ranged = store.get("k", GetRange::Range(5, 10)).await.expect("range");
        assert_eq!(&ranged.data[..], b"56789", "a ranged get is not checked");
    }

    /// A writer overwrites one key with large objects of distinct fill bytes
    /// while a reader polls it: every read is absent or one whole object.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_get_never_observes_a_partially_written_object() {
        const SIZE: usize = 8 * 1024 * 1024;
        const WRITES: u8 = 6;
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(open_in(dir.path()));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let reader = {
            let store = Arc::clone(&store);
            let done = Arc::clone(&done);
            tokio::spawn(async move {
                let (mut absent, mut complete) = (0u32, 0u32);
                loop {
                    let finished = done.load(Ordering::SeqCst);
                    match store.get("big", GetRange::Full).await {
                        Err(StoreError::NotFound) => absent += 1,
                        Ok(got) => {
                            assert_eq!(got.data.len(), SIZE, "a partial object was read");
                            let fill = got.data[0];
                            assert!((1..=WRITES).contains(&fill));
                            assert!(
                                got.data.iter().all(|&b| b == fill),
                                "a read mixed two writes"
                            );
                            complete += 1;
                        }
                        Err(e) => panic!("unexpected get error: {e:?}"),
                    }
                    if finished {
                        return (absent, complete);
                    }
                }
            })
        };
        for fill in 1..=WRITES {
            store
                .put("big", Bytes::from(vec![fill; SIZE]), PutOptions::default())
                .await
                .expect("put");
        }
        done.store(true, Ordering::SeqCst);
        let (absent, complete) = reader.await.expect("reader");
        assert!(complete >= 1, "the reader saw the final object");
        assert!(absent + complete >= 2, "the reader polled while writes ran");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_if_absent_from_8_concurrent_tasks_has_exactly_one_winner() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(open_in(dir.path()));
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = Vec::new();
        for writer in 0u8..8 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let result = store
                    .put(
                        "contended",
                        Bytes::from(vec![writer; 4096]),
                        PutOptions::create_if_absent(),
                    )
                    .await;
                (writer, result)
            }));
        }
        let mut winners = Vec::new();
        for task in tasks {
            let (writer, result) = task.await.expect("task");
            match result {
                Ok(outcome) => winners.push((writer, outcome)),
                Err(StoreError::AlreadyExists) => {}
                Err(e) => panic!("writer {writer}: {e:?}"),
            }
        }
        assert_eq!(winners.len(), 1, "exactly one create succeeds");
        let (winner, outcome) = &winners[0];
        let got = store.get("contended", GetRange::Full).await.expect("get");
        assert_eq!(
            got.data,
            Bytes::from(vec![*winner; 4096]),
            "the winner's bytes"
        );
        assert_eq!(got.version, outcome.version);
        assert_eq!(
            tree(dir.path()).len(),
            1,
            "no losing temporary file is left"
        );
    }

    #[tokio::test]
    async fn paged_listing_and_start_after_match_the_oracle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ScratchStore::open(dir.path().to_path_buf(), 2).expect("open");
        let oracle = crate::memory::MemoryStore::with_page_size(2);
        let keys = ["a/1", "a/2", "a/3", "a/4/x", "a/5", "ab", "b/1"];
        for key in keys {
            for subject in [&store as &dyn ObjectStoreBackend, &oracle] {
                subject
                    .put(key, Bytes::from_static(b"x"), PutOptions::default())
                    .await
                    .expect("put");
            }
        }
        let names = |page: &ListPage| -> Vec<String> {
            page.objects.iter().map(|m| m.key.clone()).collect()
        };
        for (prefix, after) in [
            ("a/", None),
            ("a", Some("a/2")),
            ("", Some("a/4")),
            ("a/", Some("0")),
        ] {
            let mut ours = store.list_after(prefix, after, None).await.expect("ours");
            let mut theirs = oracle
                .list_after(prefix, after, None)
                .await
                .expect("oracle");
            loop {
                assert_eq!(names(&ours), names(&theirs), "{prefix:?} after {after:?}");
                assert_eq!(ours.next, theirs.next);
                let Some(token) = ours.next.clone() else {
                    break;
                };
                ours = store
                    .list_after(prefix, after, Some(token.clone()))
                    .await
                    .expect("ours");
                theirs = oracle
                    .list_after(prefix, after, Some(token))
                    .await
                    .expect("oracle");
            }
        }
        let ours = store.list_delimited("a/").await.expect("delimited");
        let theirs = oracle.list_delimited("a/").await.expect("delimited");
        assert_eq!(ours.common_prefixes, theirs.common_prefixes);
        let direct = |l: &DelimitedList| -> Vec<String> {
            l.objects.iter().map(|m| m.key.clone()).collect()
        };
        assert_eq!(direct(&ours), direct(&theirs));
    }

    fn key_strategy() -> impl Strategy<Value = String> {
        prop_oneof![any::<String>(), "[ab/. %~\\\\\u{0}\u{7f}é日-]{0,24}",]
    }

    proptest! {
        /// Escaping is reversible for every key, and each name it produces is
        /// one plain path component: a mark, then only letters, digits, `-`,
        /// `_` and `%`.
        #[test]
        fn escaping_round_trips_and_names_hold_no_separator(key in key_strategy()) {
            let components = key_components(&key);
            prop_assert_eq!(components.len(), key.split('/').count());
            let last = components.len() - 1;
            for (i, name) in components.iter().enumerate() {
                let mark = if i == last { OBJECT_MARK } else { DIR_MARK };
                prop_assert!(name.starts_with(mark), "{:?}", name);
                prop_assert!(
                    name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'%'),
                    "{:?}", name
                );
                prop_assert!(name != "." && name != "..");
            }
            prop_assert_eq!(key_from_components(&components), Some(key.clone()));
        }
    }
}
