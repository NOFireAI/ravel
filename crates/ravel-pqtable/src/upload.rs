//! Uploading a local Parquet file as a content-addressed data object
//! (ADR-2040 D1).
//!
//! The key is derived from the file's BLAKE3 digest, so the file is read
//! twice: once to hash it, once to upload it. The second pass re-hashes what
//! it sends and refuses to finish if the file changed in between. A file at
//! or below [`UploadLimits::single_put_max_bytes`] goes up as one
//! `CreateIfAbsent` put, read whole; a larger one is streamed through
//! `put_multipart` one part at a time, so the upload buffers at most the
//! larger of the single-PUT limit and the part size, never a large file.
//!
//! Uploading the same bytes twice leaves one object: the key already exists
//! and holds those bytes.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use ravel_object_store::{
    MULTIPART_MAX_PARTS, MULTIPART_MIN_PART_SIZE, MultipartUpload, ObjectStoreBackend, PutOptions,
    StoreError, s3,
};
use ravel_types::TenantHash;
use tokio::io::AsyncReadExt;

use crate::keys::{KeyError, dataset_object_key};
use crate::manifest::ParquetFile;
use crate::names::{NameError, validate_dataset};

/// Where the single-PUT path ends and the multipart path begins, and how big
/// each multipart part is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadLimits {
    /// Largest file sent as one put. Everything above goes multipart.
    pub single_put_max_bytes: u64,
    /// Size of every part but the last. Raised to [`MULTIPART_MIN_PART_SIZE`]
    /// if below it, and grown for files that would otherwise need more than
    /// [`MULTIPART_MAX_PARTS`] parts.
    pub part_size: usize,
}

impl Default for UploadLimits {
    /// The S3 backend's own multipart threshold and part size.
    fn default() -> Self {
        UploadLimits {
            single_put_max_bytes: s3::MULTIPART_THRESHOLD as u64,
            part_size: s3::MULTIPART_PART_SIZE,
        }
    }
}

impl UploadLimits {
    fn part_size_for(&self, size: u64) -> usize {
        let floor = self.part_size.max(MULTIPART_MIN_PART_SIZE);
        let needed = size.div_ceil(MULTIPART_MAX_PARTS as u64);
        usize::try_from(needed).map_or(usize::MAX, |n| n.max(floor))
    }
}

/// The uploaded object: what a manifest's `ParquetFile` needs besides the
/// footer facts only a Parquet reader knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFileRef {
    pub key: String,
    pub size: u64,
    pub blake3: [u8; 32],
}

impl ParquetFileRef {
    pub fn into_file(self, row_count: u64, footer_len: u32) -> ParquetFile {
        ParquetFile {
            key: self.key,
            size: self.size,
            blake3: self.blake3,
            row_count,
            footer_len,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("reading {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("object store error on {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("{path:?} is empty")]
    EmptyFile { path: PathBuf },
    /// The key already exists with a different size. Keys are content
    /// addressed, so the stored object is not these bytes.
    #[error("{key:?} exists with {stored} bytes, expected {expected}")]
    SizeMismatch {
        key: String,
        stored: u64,
        expected: u64,
    },
    #[error("{path:?} changed while it was being uploaded")]
    FileChanged { path: PathBuf },
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Name(#[from] NameError),
}

const HASH_CHUNK: usize = 1024 * 1024;

fn io_error(path: &Path, source: std::io::Error) -> UploadError {
    UploadError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn store_error(key: &str, source: StoreError) -> UploadError {
    UploadError::Store {
        key: key.to_string(),
        source,
    }
}

async fn open(path: &Path) -> Result<tokio::fs::File, UploadError> {
    tokio::fs::File::open(path)
        .await
        .map_err(|e| io_error(path, e))
}

/// Fill `buf` from `file` until it is full or the file ends; returns the
/// byte count read.
async fn read_full(
    file: &mut tokio::fs::File,
    path: &Path,
    buf: &mut [u8],
) -> Result<usize, UploadError> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = file
            .read(&mut buf[filled..])
            .await
            .map_err(|e| io_error(path, e))?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

async fn hash_file(path: &Path) -> Result<(u64, [u8; 32]), UploadError> {
    let mut file = open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; HASH_CHUNK];
    let mut size = 0u64;
    loop {
        let n = read_full(&mut file, path, &mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((size, *hasher.finalize().as_bytes()))
}

/// Size of `key` if it exists.
async fn stored_size(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<Option<u64>, UploadError> {
    match store.head(key).await {
        Ok(meta) => Ok(Some(meta.size)),
        Err(StoreError::NotFound) => Ok(None),
        Err(e) => Err(store_error(key, e)),
    }
}

/// Upload `path` into `dataset` with [`UploadLimits::default`].
pub async fn upload_file(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    dataset: &str,
    path: &Path,
) -> Result<ParquetFileRef, UploadError> {
    upload_file_with_limits(store, tenant, dataset, path, UploadLimits::default()).await
}

/// Upload `path` into `dataset`, choosing single put or multipart by `limits`.
pub async fn upload_file_with_limits(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    dataset: &str,
    path: &Path,
    limits: UploadLimits,
) -> Result<ParquetFileRef, UploadError> {
    validate_dataset(dataset)?;
    let (size, blake3) = hash_file(path).await?;
    if size == 0 {
        return Err(UploadError::EmptyFile {
            path: path.to_path_buf(),
        });
    }
    let key = dataset_object_key(tenant, dataset, &blake3)?;
    let out = ParquetFileRef {
        key: key.clone(),
        size,
        blake3,
    };
    let confirm_existing = |stored: u64| {
        if stored == size {
            Ok(out.clone())
        } else {
            Err(UploadError::SizeMismatch {
                key: key.clone(),
                stored,
                expected: size,
            })
        }
    };

    if size <= limits.single_put_max_bytes {
        let body = read_single(path, size, &blake3).await?;
        return match store.put(&key, body, PutOptions::create_if_absent()).await {
            Ok(_) => Ok(out),
            Err(StoreError::AlreadyExists) => match stored_size(store, &key).await? {
                Some(stored) => confirm_existing(stored),
                None => Err(store_error(&key, StoreError::AlreadyExists)),
            },
            Err(e) => Err(store_error(&key, e)),
        };
    }

    // Multipart completion overwrites unconditionally, so check for the
    // object first; content addressing makes an existing key these bytes.
    if let Some(stored) = stored_size(store, &key).await? {
        return confirm_existing(stored);
    }
    let mut upload = store
        .put_multipart(&key)
        .await
        .map_err(|e| store_error(&key, e))?;
    match send_parts(
        upload.as_mut(),
        path,
        &key,
        size,
        &blake3,
        limits.part_size_for(size),
    )
    .await
    {
        Ok(()) => Ok(out),
        Err(e) => {
            // The upload's own error is the one to report; after a failed
            // abort only the bucket's AbortIncompleteMultipartUpload rule
            // reaps the parts.
            let _ = upload.abort().await;
            Err(e)
        }
    }
}

/// Read a small file whole, refusing it if it no longer matches pass one.
async fn read_single(path: &Path, size: u64, blake3: &[u8; 32]) -> Result<Bytes, UploadError> {
    let data = tokio::fs::read(path).await.map_err(|e| io_error(path, e))?;
    if data.len() as u64 != size || blake3::hash(&data).as_bytes() != blake3 {
        return Err(UploadError::FileChanged {
            path: path.to_path_buf(),
        });
    }
    Ok(Bytes::from(data))
}

async fn send_parts(
    upload: &mut dyn MultipartUpload,
    path: &Path,
    key: &str,
    size: u64,
    blake3: &[u8; 32],
    part_size: usize,
) -> Result<(), UploadError> {
    let mut file = open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut sent = 0u64;
    loop {
        let mut buf = vec![0u8; part_size];
        let n = read_full(&mut file, path, &mut buf).await?;
        if n == 0 {
            break;
        }
        buf.truncate(n);
        hasher.update(&buf);
        sent += n as u64;
        if sent > size {
            break;
        }
        upload
            .put_part(Bytes::from(buf), None)
            .await
            .map_err(|e| store_error(key, e))?;
    }
    if sent != size || hasher.finalize().as_bytes() != blake3 {
        return Err(UploadError::FileChanged {
            path: path.to_path_buf(),
        });
    }
    upload.complete().await.map_err(|e| store_error(key, e))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::atomic::Ordering;

    use ravel_object_store::GetRange;
    use ravel_object_store::memory::MemoryStore;

    use super::*;
    use crate::resolve::list_dataset;
    use crate::test_util::{CountingStore, TENANT_A};

    const MIB: usize = 1024 * 1024;

    fn write_temp(dir: &tempfile::TempDir, name: &str, data: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, data).expect("write");
        path
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    const SMALL_LIMITS: UploadLimits = UploadLimits {
        single_put_max_bytes: (8 * MIB) as u64,
        part_size: 5 * MIB,
    };

    #[tokio::test]
    async fn a_file_above_the_single_put_limit_goes_through_multipart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = pattern(11 * MIB + 17);
        let path = write_temp(&dir, "big.parquet", &data);
        let store = CountingStore::new(MemoryStore::new());
        let got = upload_file_with_limits(&store, &TENANT_A, "hits", &path, SMALL_LIMITS)
            .await
            .expect("upload");
        let digest = *blake3::hash(&data).as_bytes();
        assert_eq!(
            got,
            ParquetFileRef {
                key: dataset_object_key(&TENANT_A, "hits", &digest).expect("key"),
                size: data.len() as u64,
                blake3: digest,
            }
        );
        assert_eq!(store.multipart_started.load(Ordering::SeqCst), 1);
        assert_eq!(store.multipart_completed.load(Ordering::SeqCst), 1);
        assert!(store.accepted_puts().is_empty(), "no single put ran");
        let stored = store.get(&got.key, GetRange::Full).await.expect("get");
        assert_eq!(stored.data.as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn a_file_at_the_limit_goes_up_as_one_conditional_put() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = pattern(8 * MIB);
        let path = write_temp(&dir, "small.parquet", &data);
        let store = CountingStore::new(MemoryStore::new());
        let got = upload_file_with_limits(&store, &TENANT_A, "hits", &path, SMALL_LIMITS)
            .await
            .expect("upload");
        assert_eq!(store.multipart_started.load(Ordering::SeqCst), 0);
        assert_eq!(store.accepted_puts().get(&got.key), Some(&1));
    }

    #[tokio::test]
    async fn the_same_bytes_twice_leave_one_object_on_both_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        for len in [100, 11 * MIB] {
            let data = pattern(len);
            let a = write_temp(&dir, "a.parquet", &data);
            let b = write_temp(&dir, "b.parquet", &data);
            let store = CountingStore::new(MemoryStore::new());
            let first = upload_file_with_limits(&store, &TENANT_A, "hits", &a, SMALL_LIMITS)
                .await
                .expect("first");
            let second = upload_file_with_limits(&store, &TENANT_A, "hits", &b, SMALL_LIMITS)
                .await
                .expect("second");
            assert_eq!(first, second);
            let keys: Vec<String> = list_dataset(&store, &TENANT_A, "hits")
                .await
                .expect("list")
                .into_iter()
                .map(|m| m.key)
                .collect();
            assert_eq!(keys, vec![first.key.clone()]);
            let multipart = store.multipart_completed.load(Ordering::SeqCst);
            let puts = store.accepted_puts().get(&first.key).copied().unwrap_or(0);
            assert_eq!(
                multipart + puts,
                1,
                "len {len}: one write reached the store"
            );
        }
    }

    #[tokio::test]
    async fn an_existing_key_with_another_size_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = pattern(100);
        let path = write_temp(&dir, "a.parquet", &data);
        let store = MemoryStore::new();
        let key =
            dataset_object_key(&TENANT_A, "hits", blake3::hash(&data).as_bytes()).expect("key");
        store
            .put(&key, Bytes::from_static(b"other"), PutOptions::default())
            .await
            .expect("put");
        let got = upload_file(&store, &TENANT_A, "hits", &path).await;
        assert!(
            matches!(
                got,
                Err(UploadError::SizeMismatch {
                    stored: 5,
                    expected: 100,
                    ..
                })
            ),
            "{got:?}"
        );
    }

    #[tokio::test]
    async fn an_empty_file_and_a_bad_dataset_are_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_temp(&dir, "empty.parquet", &[]);
        let store = MemoryStore::new();
        assert!(matches!(
            upload_file(&store, &TENANT_A, "hits", &path).await,
            Err(UploadError::EmptyFile { .. })
        ));
        assert!(matches!(
            upload_file(&store, &TENANT_A, "Hits", &path).await,
            Err(UploadError::Name(_))
        ));
    }

    /// A file that no longer matches its first-pass digest is refused on
    /// both paths, and the multipart upload is never completed.
    #[tokio::test]
    async fn a_file_that_changed_since_it_was_hashed_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let small = write_temp(&dir, "small.parquet", &pattern(100));
        let stale = [0x5a; 32];
        assert!(matches!(
            read_single(&small, 100, &stale).await,
            Err(UploadError::FileChanged { .. })
        ));
        assert!(matches!(
            read_single(&small, 99, blake3::hash(&pattern(100)).as_bytes()).await,
            Err(UploadError::FileChanged { .. })
        ));

        let len = 11 * MIB;
        let big = write_temp(&dir, "big.parquet", &pattern(len));
        let store = CountingStore::new(MemoryStore::new());
        let key = dataset_object_key(&TENANT_A, "hits", &stale).expect("key");
        let mut upload = store.put_multipart(&key).await.expect("start");
        let got = send_parts(upload.as_mut(), &big, &key, len as u64, &stale, 5 * MIB).await;
        assert!(
            matches!(got, Err(UploadError::FileChanged { .. })),
            "{got:?}"
        );
        assert_eq!(store.multipart_completed.load(Ordering::SeqCst), 0);
        upload.abort().await.expect("abort");
        assert!(matches!(store.head(&key).await, Err(StoreError::NotFound)));
    }

    #[test]
    fn the_part_size_grows_to_fit_the_part_limit() {
        let limits = UploadLimits {
            single_put_max_bytes: 0,
            part_size: 1,
        };
        assert_eq!(limits.part_size_for(1), MULTIPART_MIN_PART_SIZE);
        let huge = (MULTIPART_MIN_PART_SIZE as u64) * (MULTIPART_MAX_PARTS as u64) * 3;
        let part = limits.part_size_for(huge) as u64;
        assert!(huge.div_ceil(part) <= MULTIPART_MAX_PARTS as u64);
    }
}
