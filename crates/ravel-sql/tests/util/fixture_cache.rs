//! A file cache for large, deterministic test fixtures, shared by every
//! process of one test binary.
//!
//! nextest runs each test in its own process, so a `OnceLock` caches a
//! fixture for one test only. [`cached_bytes`] keeps the fixture's bytes in
//! a file under a directory the caller names. The file name carries a key
//! from the test binary's path, size and modification time, so relinking
//! the binary, which changes its modification time, changes the name it
//! reads.
//!
//! The cache is an optimisation only: every failure on the cache path (the
//! directory, the lock, a read, a write, a rename, a file whose trailer does
//! not match its contents) falls back to building the bytes in the calling
//! process.

use std::collections::hash_map::DefaultHasher;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

/// Marks the end of a complete cache file.
const MAGIC: &[u8; 8] = b"RVLFXC01";

/// Magic, payload length and payload checksum.
const TRAILER_LEN: usize = 24;

/// The bytes `build` returns, read from `dir` when an earlier process of this
/// test binary stored them there, and otherwise built and stored.
///
/// `fixture` names the bytes: two fixtures must use two names, because the
/// cache checks a file's integrity, not which fixture wrote it.
pub fn cached_bytes(dir: &Path, fixture: &str, build: impl FnOnce() -> Vec<u8>) -> Vec<u8> {
    let entry = match Entry::new(dir, fixture) {
        Ok(entry) => entry,
        Err(err) => return fallback(fixture, "resolve the cache entry", &err, build),
    };
    if let Some(bytes) = entry.read() {
        return bytes;
    }
    let _lock = match entry.lock() {
        Ok(lock) => lock,
        Err(err) => return fallback(fixture, "lock the cache entry", &err, build),
    };
    if let Some(bytes) = entry.read() {
        return bytes;
    }
    let bytes = build();
    if let Err(err) = entry.store(&bytes) {
        eprintln!("fixture cache {fixture}: could not store the bytes: {err}");
    }
    entry.remove_older();
    bytes
}

/// The file `cached_bytes(dir, fixture, ..)` reads and writes for this test
/// binary.
pub fn data_path(dir: &Path, fixture: &str) -> io::Result<PathBuf> {
    Entry::new(dir, fixture).map(|entry| entry.data)
}

fn fallback(
    fixture: &str,
    step: &str,
    err: &io::Error,
    build: impl FnOnce() -> Vec<u8>,
) -> Vec<u8> {
    eprintln!("fixture cache {fixture}: could not {step}: {err}; building in process");
    build()
}

struct Entry {
    dir: PathBuf,
    /// `<fixture>-<binary path hash>-`, shared by every key of this binary.
    prefix: String,
    data: PathBuf,
    lock: PathBuf,
    tmp: PathBuf,
}

impl Entry {
    fn new(dir: &Path, fixture: &str) -> io::Result<Self> {
        let exe = std::env::current_exe()?;
        let meta = fs::metadata(&exe)?;
        let mtime = meta
            .modified()?
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?;
        let path_key = hash_of(&exe);
        let state_key = hash_of(&(meta.len(), mtime.as_nanos()));
        let prefix = format!("{fixture}-{path_key:016x}-");
        Ok(Entry {
            dir: dir.to_path_buf(),
            data: dir.join(format!("{prefix}{state_key:016x}.bin")),
            lock: dir.join(format!("{fixture}-{path_key:016x}.lock")),
            tmp: dir.join(format!(
                "{prefix}{state_key:016x}.tmp{}",
                std::process::id()
            )),
            prefix,
        })
    }

    /// The payload of a complete cache file, or `None` when the file is
    /// missing, unreadable, or its trailer does not match its contents.
    fn read(&self) -> Option<Vec<u8>> {
        let mut bytes = fs::read(&self.data).ok()?;
        let payload_len = verified_payload_len(&bytes)?;
        bytes.truncate(payload_len);
        Some(bytes)
    }

    fn lock(&self) -> io::Result<File> {
        fs::create_dir_all(&self.dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.lock)?;
        file.lock()?;
        Ok(file)
    }

    /// Writes `payload` and its trailer to a temporary file in the same
    /// directory and renames it into place, so a reader of the final name
    /// finds the previous file or the whole new one; [`Entry::read`]'s
    /// trailer check covers a file torn some other way.
    fn store(&self, payload: &[u8]) -> io::Result<()> {
        let written = (|| {
            let mut file = File::create(&self.tmp)?;
            file.write_all(payload)?;
            file.write_all(&trailer(payload))?;
            file.flush()?;
            drop(file);
            fs::rename(&self.tmp, &self.data)
        })();
        if written.is_err() {
            let _ = fs::remove_file(&self.tmp);
        }
        written
    }

    /// Removes this binary path's files for this fixture under any other
    /// key, and temporary files a dead writer left. Runs under the lock.
    fn remove_older(&self) {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let stale = path != self.data
                && entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(&self.prefix));
            if stale {
                let _ = fs::remove_file(&path);
            }
        }
    }
}

fn hash_of(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn trailer(payload: &[u8]) -> [u8; TRAILER_LEN] {
    let mut out = [0u8; TRAILER_LEN];
    out[..8].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    out[8..16].copy_from_slice(&checksum(payload).to_le_bytes());
    out[16..].copy_from_slice(MAGIC);
    out
}

/// The payload length of `file` when it ends in a trailer that matches the
/// bytes before it.
fn verified_payload_len(file: &[u8]) -> Option<usize> {
    let payload_len = file.len().checked_sub(TRAILER_LEN)?;
    let (payload, tail) = file.split_at(payload_len);
    (tail == trailer(payload)).then_some(payload_len)
}

/// FNV-1a over little-endian 64-bit words, then the remaining bytes.
fn checksum(bytes: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let (words, rest) = bytes.as_chunks::<8>();
    for word in words {
        hash = (hash ^ u64::from_le_bytes(*word)).wrapping_mul(PRIME);
    }
    for &byte in rest {
        hash = (hash ^ u64::from(byte)).wrapping_mul(PRIME);
    }
    hash
}
