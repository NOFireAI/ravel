//! The file list a Parquet table's manifest pins, read from a granted
//! location (ADR-2040 decision D2).
//!
//! [`snapshot_location`] turns a `LOCATION` that already lies inside one of
//! the caller's grants into the [`ParquetFile`]s a manifest records and the
//! one schema they share. It reads every footer the way the scan will, so a
//! file the scan would refuse is refused here, and it records each file's
//! identity from the footer read's response, never from the listing.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use futures::StreamExt;
use futures::stream;
use ravel_object_store::{GetRange, ObjectStoreBackend, Pin, PinnedRead, StoreError};
use ravel_pqtable::grants::{Grant, KeyPrefix, contains_key};
use ravel_pqtable::manifest::{ParquetFile, key_is_addressable};
use ravel_query::{GetLimiter, PhaseAccounting, QueryPhase};
use ravel_types::accounting::AccountedOp;

use crate::provider::file_schema;
use crate::reader::{TRAILER_LEN, decode_footer, trailer_footer_len};

/// The most files one table may hold.
pub const MAX_TABLE_FILES: usize = 100_000;

/// The key suffix that makes a listed object a file of the table. It is
/// compared exactly: `.PARQUET` and `.parquet.tmp` are other suffixes.
pub const PARQUET_SUFFIX: &str = ".parquet";

/// Bytes the first footer read of each file takes from its end. A footer and
/// trailer that fit are one GET; a longer footer takes a second GET for the
/// bytes before these.
pub const FOOTER_PREFETCH: u64 = 64 * 1024;

/// A `LOCATION` and the grant that admits it, as
/// [`ravel_pqtable::grants::resolve_location`] returns them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantedLocation {
    pub grant: Grant,
    pub key: KeyPrefix,
}

impl GrantedLocation {
    /// The location as a URL, for messages. It carries no credential: a
    /// location never does.
    pub fn url(&self) -> String {
        let mut url = format!("{}://{}", self.grant.scheme, self.grant.bucket);
        if !self.key.key.is_empty() {
            url.push('/');
            url.push_str(&self.key.key);
            if self.key.directory {
                url.push('/');
            }
        }
        url
    }

    /// The prefix a directory location lists: every key strictly below it.
    fn list_prefix(&self) -> String {
        if self.key.key.is_empty() {
            String::new()
        } else {
            format!("{}/", self.key.key)
        }
    }
}

/// What a location holds, ready for a manifest.
#[derive(Debug, Clone)]
pub struct LocationSnapshot {
    /// One entry per `.parquet` object, in listing order.
    pub files: Vec<ParquetFile>,
    /// The Arrow schema every file shares, with the schema's and each
    /// top-level field's metadata cleared.
    pub schema: SchemaRef,
    /// Listed keys ending in `/`.
    pub skipped_directory_markers: u64,
    /// Listed keys ending in anything but `/` or [`PARQUET_SUFFIX`].
    pub skipped_other_suffixes: u64,
}

/// Why a location could not be snapshotted. Each names the location or the
/// object key it refused; none carries a credential.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("LOCATION {location} holds no {PARQUET_SUFFIX} file")]
    NoFiles { location: String },
    #[error("LOCATION {location} holds more than {limit} {PARQUET_SUFFIX} files")]
    TooManyFiles { location: String, limit: usize },
    #[error(
        "LOCATION {location}: object {key:?} cannot be addressed exactly by the object-store \
         client, so a table over it could not be read"
    )]
    Unaddressable { location: String, key: String },
    #[error("LOCATION {location}: object {key:?} lies outside the grant {grant}")]
    OutsideGrant {
        location: String,
        key: String,
        grant: String,
    },
    #[error("Parquet file {key:?} changed while its footer was read; run the statement again")]
    FileChanged { key: String },
    #[error("Parquet file {key:?} no longer exists")]
    FileMissing { key: String },
    #[error("Parquet file {key:?} is empty")]
    EmptyFile { key: String },
    /// The bytes are not a Parquet file the scan could read: truncated, a
    /// trailer or footer that does not decode, or a footer the reader's own
    /// checks refuse.
    #[error("Parquet file {key:?}: {message}")]
    Corrupt { key: String, message: String },
    #[error("Parquet file {key:?} has a different schema from {first:?}, the first file")]
    SchemaMismatch { key: String, first: String },
    #[error("listing LOCATION {location}: {source}")]
    List {
        location: String,
        #[source]
        source: StoreError,
    },
    #[error("reading {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("LOCATION {location} was not read within the {deadline:?} deadline")]
    Deadline {
        location: String,
        deadline: Duration,
    },
}

/// Snapshot `location` through `store`, the store of the grant's profile.
///
/// A location naming one object reads that object, whatever its suffix, and
/// lists nothing: one HEAD learns the ETag and size its footer read pins. A
/// directory location is listed once, recursively; every key ending in
/// exactly [`PARQUET_SUFFIX`] becomes a file, Hive-style subdirectories
/// included as plain files, and the other keys are counted and skipped. A
/// file key [`key_is_addressable`] refuses, more than [`MAX_TABLE_FILES`]
/// files, or none at all, refuses the snapshot.
///
/// Each file's footer read carries `If-Match` on the ETag the listing (or
/// the HEAD) reported, takes a `limiter` permit, and is charged to
/// [`QueryPhase::Probe`]; the LIST pages and the HEAD are charged to
/// [`QueryPhase::Resolve`]. The recorded ETag, version and size come from
/// that read's response. Up to `limiter.permits()` footer reads run at once.
/// A file changed or deleted after the listing, an empty file, and a file
/// whose trailer or footer the reader would refuse each refuse the whole
/// snapshot, naming the file, and so does a file whose schema differs from
/// the first file's. The whole snapshot runs under `deadline`.
pub async fn snapshot_location(
    store: &dyn ObjectStoreBackend,
    location: &GrantedLocation,
    limiter: &GetLimiter,
    deadline: Duration,
    accounting: &PhaseAccounting,
) -> Result<LocationSnapshot, SnapshotError> {
    snapshot_with_limit(
        store,
        location,
        limiter,
        deadline,
        accounting,
        MAX_TABLE_FILES,
    )
    .await
}

async fn snapshot_with_limit(
    store: &dyn ObjectStoreBackend,
    location: &GrantedLocation,
    limiter: &GetLimiter,
    deadline: Duration,
    accounting: &PhaseAccounting,
    limit: usize,
) -> Result<LocationSnapshot, SnapshotError> {
    let snapshot = async {
        let listed = if location.key.directory {
            list_files(store, location, accounting, limit).await?
        } else {
            head_file(store, location, accounting).await?
        };
        read_files(store, location, limiter, accounting, listed).await
    };
    tokio::time::timeout(deadline, snapshot)
        .await
        .map_err(|_| SnapshotError::Deadline {
            location: location.url(),
            deadline,
        })?
}

/// An object the listing (or the HEAD) reported, before its footer is read.
struct Candidate {
    key: String,
    etag: String,
    size: u64,
}

struct Listed {
    candidates: Vec<Candidate>,
    directory_markers: u64,
    other_suffixes: u64,
}

/// Refuse a file key the client cannot address or the grant does not admit.
fn check_file_key(location: &GrantedLocation, key: &str) -> Result<(), SnapshotError> {
    if !key_is_addressable(key.as_bytes()) {
        return Err(SnapshotError::Unaddressable {
            location: location.url(),
            key: key.to_string(),
        });
    }
    let grant = &location.grant;
    if !contains_key(grant, &grant.profile, &grant.bucket, key.as_bytes()) {
        return Err(SnapshotError::OutsideGrant {
            location: location.url(),
            key: key.to_string(),
            grant: grant.url(),
        });
    }
    Ok(())
}

async fn list_files(
    store: &dyn ObjectStoreBackend,
    location: &GrantedLocation,
    accounting: &PhaseAccounting,
    limit: usize,
) -> Result<Listed, SnapshotError> {
    let prefix = location.list_prefix();
    let resolve = accounting.phase(QueryPhase::Resolve);
    let mut listed = Listed {
        candidates: Vec::new(),
        directory_markers: 0,
        other_suffixes: 0,
    };
    // A key may appear on more than one page; each is counted once.
    let mut seen = HashSet::new();
    let mut page = None;
    loop {
        resolve.record_s3_request(AccountedOp::List);
        let result = store
            .list(&prefix, page)
            .await
            .map_err(|source| SnapshotError::List {
                location: location.url(),
                source,
            })?;
        for object in result.objects {
            if !seen.insert(object.key.clone()) {
                continue;
            }
            if object.key.ends_with('/') {
                listed.directory_markers += 1;
                continue;
            }
            if !object.key.ends_with(PARQUET_SUFFIX) {
                listed.other_suffixes += 1;
                continue;
            }
            check_file_key(location, &object.key)?;
            if listed.candidates.len() == limit {
                return Err(SnapshotError::TooManyFiles {
                    location: location.url(),
                    limit,
                });
            }
            listed.candidates.push(Candidate {
                key: object.key,
                etag: object.etag.0,
                size: object.size,
            });
        }
        page = result.next;
        if page.is_none() {
            return Ok(listed);
        }
    }
}

async fn head_file(
    store: &dyn ObjectStoreBackend,
    location: &GrantedLocation,
    accounting: &PhaseAccounting,
) -> Result<Listed, SnapshotError> {
    let key = location.key.key.clone();
    check_file_key(location, &key)?;
    accounting
        .phase(QueryPhase::Resolve)
        .record_s3_request(AccountedOp::Head);
    let meta = store
        .head(&key)
        .await
        .map_err(|source| read_error(&key, source))?;
    Ok(Listed {
        candidates: vec![Candidate {
            key,
            etag: meta.etag.0,
            size: meta.size,
        }],
        directory_markers: 0,
        other_suffixes: 0,
    })
}

async fn read_files(
    store: &dyn ObjectStoreBackend,
    location: &GrantedLocation,
    limiter: &GetLimiter,
    accounting: &PhaseAccounting,
    listed: Listed,
) -> Result<LocationSnapshot, SnapshotError> {
    let grant = &location.grant;
    let mut files = Vec::with_capacity(listed.candidates.len());
    let mut first: Option<(String, Schema)> = None;
    let mut reads = stream::iter(listed.candidates)
        .map(|candidate| read_file(store, grant, limiter, accounting, candidate))
        .buffered(limiter.permits());
    while let Some(read) = reads.next().await {
        let (file, schema) = read?;
        match &first {
            None => first = Some((key_of(&file), schema)),
            Some((first_key, first_schema)) => {
                if *first_schema != schema {
                    return Err(SnapshotError::SchemaMismatch {
                        key: key_of(&file),
                        first: first_key.clone(),
                    });
                }
            }
        }
        files.push(file);
    }
    let Some((_, schema)) = first else {
        return Err(SnapshotError::NoFiles {
            location: location.url(),
        });
    };
    Ok(LocationSnapshot {
        files,
        schema: Arc::new(schema),
        skipped_directory_markers: listed.directory_markers,
        skipped_other_suffixes: listed.other_suffixes,
    })
}

fn key_of(file: &ParquetFile) -> String {
    String::from_utf8_lossy(&file.key).into_owned()
}

fn read_error(key: &str, source: StoreError) -> SnapshotError {
    let key = key.to_string();
    match source {
        StoreError::PreconditionFailed => SnapshotError::FileChanged { key },
        StoreError::NotFound => SnapshotError::FileMissing { key },
        source => SnapshotError::Store { key, source },
    }
}

/// One pinned GET of bytes `start..end` of `key`, under a `limiter` permit,
/// charged to Probe.
async fn pinned_get(
    store: &dyn ObjectStoreBackend,
    limiter: &GetLimiter,
    accounting: &PhaseAccounting,
    key: &str,
    (start, end): (u64, u64),
    pin: &Pin,
) -> Result<PinnedRead, SnapshotError> {
    let _permit = limiter.acquire().await.map_err(|_| SnapshotError::Store {
        key: key.to_string(),
        source: StoreError::Transient("GetLimiter semaphore closed unexpectedly".into()),
    })?;
    let probe = accounting.phase(QueryPhase::Probe);
    probe.record_s3_request(AccountedOp::Get);
    let read = store
        .get_pinned(key, GetRange::Range(start, end), pin)
        .await
        .map_err(|source| read_error(key, source))?;
    probe.add_s3_bytes(AccountedOp::Get, read.outcome.data.len() as u64);
    Ok(read)
}

/// The last [`FOOTER_PREFETCH`] bytes of a `size`-byte object, or all of it.
fn tail_range(size: u64) -> (u64, u64) {
    (size.saturating_sub(FOOTER_PREFETCH), size)
}

/// Read and check one file's footer, and describe the file as the read's
/// response reported it.
async fn read_file(
    store: &dyn ObjectStoreBackend,
    grant: &Grant,
    limiter: &GetLimiter,
    accounting: &PhaseAccounting,
    candidate: Candidate,
) -> Result<(ParquetFile, Schema), SnapshotError> {
    let key = candidate.key;
    let corrupt = |message: String| SnapshotError::Corrupt {
        key: key.clone(),
        message,
    };
    if candidate.size == 0 {
        return Err(SnapshotError::EmptyFile { key: key.clone() });
    }
    let listed_pin = Pin::etag(candidate.etag);
    let mut tail = pinned_get(
        store,
        limiter,
        accounting,
        &key,
        tail_range(candidate.size),
        &listed_pin,
    )
    .await?;
    let size = tail.outcome.total_size;
    if size != candidate.size {
        // The listing misreported the size of the object its ETag names, so
        // the first read did not end at the end of the file: read the tail
        // the response's size places.
        if size == 0 {
            return Err(SnapshotError::EmptyFile { key: key.clone() });
        }
        tail = pinned_get(
            store,
            limiter,
            accounting,
            &key,
            tail_range(size),
            &listed_pin,
        )
        .await?;
        if tail.outcome.total_size != size {
            return Err(SnapshotError::FileChanged { key: key.clone() });
        }
    }
    let recorded = tail.pin.clone();
    let tail_start = tail_range(size).0;
    let data = tail.outcome.data;
    let fetched = data.len() as u64;
    if fetched != size - tail_start {
        return Err(corrupt(format!(
            "read of bytes {tail_start}..{size} returned {fetched} bytes"
        )));
    }
    if fetched < TRAILER_LEN {
        return Err(corrupt(format!(
            "the file is {size} bytes, shorter than the {TRAILER_LEN}-byte trailer"
        )));
    }
    let split = (fetched - TRAILER_LEN) as usize;
    let footer_len = trailer_footer_len(&data[split..]).map_err(&corrupt)?;
    let footer_and_trailer = footer_len + TRAILER_LEN;
    if footer_and_trailer > size {
        return Err(corrupt(format!(
            "the trailer records a {footer_len}-byte footer in a {size}-byte file"
        )));
    }
    let footer = if footer_and_trailer <= fetched {
        data.slice(split - footer_len as usize..split)
    } else {
        // Select the version the first read saw, so both reads are of one
        // object's bytes; If-Match stays on the listed ETag.
        let pin = Pin {
            etag: listed_pin.etag.clone(),
            version: recorded.version.clone(),
        };
        let head = pinned_get(
            store,
            limiter,
            accounting,
            &key,
            (size - footer_and_trailer, tail_start),
            &pin,
        )
        .await?;
        if head.outcome.total_size != size || head.pin != recorded {
            return Err(SnapshotError::FileChanged { key: key.clone() });
        }
        let head = head.outcome.data;
        if head.len() as u64 != tail_start - (size - footer_and_trailer) {
            return Err(corrupt(format!(
                "read of bytes {}..{tail_start} returned {} bytes",
                size - footer_and_trailer,
                head.len()
            )));
        }
        let mut footer = Vec::with_capacity(footer_len as usize);
        footer.extend_from_slice(&head);
        footer.extend_from_slice(&data[..split]);
        Bytes::from(footer)
    };
    let metadata = decode_footer(&footer, size - footer_and_trailer).map_err(&corrupt)?;
    let row_count = u64::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| corrupt("the footer records a negative row count".to_string()))?;
    let footer_len =
        u32::try_from(footer_len).map_err(|_| corrupt(format!("a {footer_len}-byte footer")))?;
    let schema = file_schema(&metadata).map_err(|message| corrupt(format!("schema: {message}")))?;
    let file = ParquetFile {
        profile: grant.profile.clone(),
        bucket: grant.bucket.clone(),
        key: key.into_bytes(),
        size,
        etag: recorded.etag,
        version: recorded.version.unwrap_or_default(),
        row_count,
        footer_len,
    };
    Ok((file, schema))
}
