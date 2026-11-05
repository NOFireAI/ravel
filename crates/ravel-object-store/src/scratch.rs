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
//! Every operation runs its filesystem calls on tokio's blocking pool.
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
        let file_type = entry
            .file_type()
            .map_err(|e| io_error("stat", &path, e))?;
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
    fn list_page(&self, prefix: &str, start: &str, exclusive: bool) -> Result<ListPage, StoreError> {
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
