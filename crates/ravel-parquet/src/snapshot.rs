//! The file list a Parquet table's manifest pins, read from a granted
//! location (ADR-2040 decision D2).
//!
//! [`snapshot_location`] turns a `LOCATION` that already lies inside one of
//! the caller's grants into the [`ParquetFile`]s a manifest records and the
//! one schema they share. It runs the reader's footer checks on every
//! footer, so a footer the scan would refuse is refused here, and it records
//! each file's identity from the footer read's response, never from the
//! listing.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use futures::StreamExt;
use futures::stream;
use ravel_memory::{MemoryBudget, Reservation};
use ravel_object_store::{
    DrainStep, GetRange, MAX_LIST_PAGES, ObjectStoreBackend, Pin, PinnedRead, StoreError,
    drain_pages,
};
use ravel_pqtable::grants::{Grant, KeyPrefix, contains_key};
use ravel_pqtable::manifest::{ParquetFile, key_is_addressable};
use ravel_query::{GetLimiter, PhaseAccounting, QueryPhase};
use ravel_types::accounting::AccountedOp;

use crate::provider::file_schema;
use crate::reader::{DecodeError, DecodedFooter, TRAILER_LEN, decode_footer, trailer_footer_len};

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
    /// One entry per file, in listing order.
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
/// object key it refused. The text built here names locations, keys and
/// grants, none of which carries a credential; `List` and `Store` append the
/// store error's own text.
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
    #[error(
        "Parquet file {key:?}: memory exhausted reading its footer: requested {requested} \
         bytes, {reserved} of {limit} byte budget already reserved"
    )]
    MemoryExhausted {
        key: String,
        requested: u64,
        reserved: u64,
        limit: u64,
    },
}

/// Snapshot `location` through `store`, the store of the grant's profile.
///
/// A location naming one object reads that object, whatever its suffix, and
/// lists nothing: one HEAD learns the ETag and size its footer read pins. A
/// directory location is listed once, recursively; every key ending in
/// exactly [`PARQUET_SUFFIX`] becomes a file, Hive-style subdirectories
/// included as plain files, and the other keys are counted and skipped. A
/// file key [`key_is_addressable`] refuses or the grant does not contain,
/// more than [`MAX_TABLE_FILES`] files, or none at all, refuses the
/// snapshot.
///
/// Each file's footer read reserves its bytes against `memory` before the
/// GET is issued and releases the reservation once the footer is decoded; a
/// refusal is a typed [`SnapshotError::MemoryExhausted`] naming the file,
/// with no GET issued for that read. Decoding the footer reserves the
/// estimate of what the decoder allocates the same way, before it decodes,
/// and holds it until the file's schema is built and the decoded footer
/// dropped. Each read carries `If-Match` on the
/// ETag the listing (or the HEAD) reported, takes a `limiter` permit, and is
/// charged to [`QueryPhase::Probe`]; the LIST pages and the HEAD are charged
/// to [`QueryPhase::Resolve`]. The recorded ETag, version and size come from
/// that read's response. Up to `limiter.permits()` footer reads run at once.
/// A file changed or deleted after the listing, an empty file, and a file
/// whose trailer or footer the reader would refuse each refuse the whole
/// snapshot, naming the file, and so does a file whose schema differs from
/// the first file's. The whole snapshot runs under `deadline`.
pub async fn snapshot_location(
    store: &dyn ObjectStoreBackend,
    location: &GrantedLocation,
    limiter: &GetLimiter,
    memory: &Arc<MemoryBudget>,
    deadline: Duration,
    accounting: &PhaseAccounting,
) -> Result<LocationSnapshot, SnapshotError> {
    snapshot_with_limit(
        store,
        location,
        limiter,
        memory,
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
    memory: &Arc<MemoryBudget>,
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
        read_files(store, location, limiter, memory, accounting, listed).await
    };
    tokio::time::timeout(deadline, snapshot)
        .await
        .map_err(|_| SnapshotError::Deadline {
            location: location.url(),
            deadline,
        })?
}

/// An object the listing (or the HEAD) reported, before its footer is read.
/// On a store with `suffix_range`, the first footer read is a suffix read
/// that is correct whatever the real size is, so `size` has no bearing on
/// it. On a store without it, `size` shapes where that first explicit-range
/// read lands; a stale `size` costs one retry rather than a misplaced read
/// (see [`read_file`]). A `size` of 0, or one that over-reports by
/// [`FOOTER_PREFETCH`] or more so the range starts at or past the object's
/// end, leaves no range to issue; that read recovers through one HEAD
/// instead (see
/// [`head_corrected_read`]).
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

/// Error out of the [`drain_pages`] hooks in [`list_files`], converted to a
/// [`SnapshotError`] once the drain finishes. `drain_pages` requires an
/// `E: From<StoreError>`; a blanket `From<StoreError> for SnapshotError`
/// would lose the location `SnapshotError::List` carries, so this carries it
/// through instead.
enum ListDrainError {
    Store(StoreError),
    Snapshot(SnapshotError),
}

impl From<StoreError> for ListDrainError {
    fn from(source: StoreError) -> Self {
        ListDrainError::Store(source)
    }
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
    drain_pages(
        &prefix,
        None,
        MAX_LIST_PAGES,
        |_start_after, page| async {
            resolve.record_s3_request(AccountedOp::List);
            Ok(store.list(&prefix, page).await?)
        },
        |object| {
            if object.key.ends_with('/') {
                listed.directory_markers += 1;
                return Ok(DrainStep::Continue);
            }
            if !object.key.ends_with(PARQUET_SUFFIX) {
                listed.other_suffixes += 1;
                return Ok(DrainStep::Continue);
            }
            check_file_key(location, &object.key).map_err(ListDrainError::Snapshot)?;
            if listed.candidates.len() == limit {
                return Err(ListDrainError::Snapshot(SnapshotError::TooManyFiles {
                    location: location.url(),
                    limit,
                }));
            }
            listed.candidates.push(Candidate {
                key: object.key,
                etag: object.etag.0,
                size: object.size,
            });
            Ok(DrainStep::Continue)
        },
    )
    .await
    .map_err(|err| match err {
        ListDrainError::Store(source) => SnapshotError::List {
            location: location.url(),
            source,
        },
        ListDrainError::Snapshot(err) => err,
    })?;
    Ok(listed)
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
    memory: &Arc<MemoryBudget>,
    accounting: &PhaseAccounting,
    listed: Listed,
) -> Result<LocationSnapshot, SnapshotError> {
    let grant = &location.grant;
    let mut files = Vec::with_capacity(listed.candidates.len());
    let mut first: Option<(String, Schema)> = None;
    let mut reads = stream::iter(listed.candidates)
        .map(|candidate| read_file(store, grant, limiter, memory, accounting, candidate))
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

/// `NotFound` means `FileMissing` here because most call sites are a read
/// that has not yet proved the object exists. Three exceptions have already
/// read (or, for the third, HEADed) this same object once, and each remaps
/// `FileMissing` to `FileChanged` at its own call site: the second GET of a
/// long footer, the stale-listed-size retry in [`first_footer_read`], and
/// the explicit-range read [`head_corrected_read`] issues after its HEAD.
fn read_error(key: &str, source: StoreError) -> SnapshotError {
    let key = key.to_string();
    match source {
        StoreError::PreconditionFailed => SnapshotError::FileChanged { key },
        StoreError::NotFound => SnapshotError::FileMissing { key },
        source => SnapshotError::Store { key, source },
    }
}

/// The two range shapes `pinned_get` issues for a footer read. Each carries
/// its own byte length to reserve, so a caller can never pass a length that
/// disagrees with the range it describes.
enum FooterRange {
    Suffix(u64),
    Range(u64, u64),
}

impl FooterRange {
    fn len(&self) -> u64 {
        match *self {
            FooterRange::Suffix(n) => n,
            FooterRange::Range(start, end) => end - start,
        }
    }

    fn as_get_range(&self) -> GetRange {
        match *self {
            FooterRange::Suffix(n) => GetRange::Suffix(n),
            FooterRange::Range(start, end) => GetRange::Range(start, end),
        }
    }
}

/// One pinned GET of `range` under a `limiter` permit, accounted to Probe.
/// Reserves `range`'s byte length against `memory` before the GET is issued;
/// a refusal is returned with no GET issued. The caller holds the returned
/// [`Reservation`] until the footer it reads is decoded.
async fn pinned_get(
    store: &dyn ObjectStoreBackend,
    limiter: &GetLimiter,
    memory: &Arc<MemoryBudget>,
    accounting: &PhaseAccounting,
    key: &str,
    range: FooterRange,
    pin: &Pin,
) -> Result<(PinnedRead, Reservation), SnapshotError> {
    let reservation =
        memory
            .reserve(range.len())
            .map_err(|exhausted| SnapshotError::MemoryExhausted {
                key: key.to_string(),
                requested: exhausted.requested,
                reserved: exhausted.reserved,
                limit: exhausted.limit,
            })?;
    let _permit = limiter.acquire().await.map_err(|_| SnapshotError::Store {
        key: key.to_string(),
        source: StoreError::Transient("GetLimiter semaphore closed unexpectedly".into()),
    })?;
    let probe = accounting.phase(QueryPhase::Probe);
    probe.record_s3_request(AccountedOp::Get);
    let read = store
        .get_pinned(key, range.as_get_range(), pin)
        .await
        .map_err(|source| read_error(key, source))?;
    probe.add_s3_bytes(AccountedOp::Get, read.outcome.data.len() as u64);
    Ok((read, reservation))
}

/// The object exists, at the pinned ETag, and has no bytes to read: an
/// endpoint that answers a footer read with `InvalidRange` (a 416) instead
/// of an empty body is reporting the same thing `size == 0` below reports
/// for one that answers it with an empty body.
fn empty_file_on_invalid_range(key: &str, err: SnapshotError) -> SnapshotError {
    match err {
        SnapshotError::Store {
            source: StoreError::InvalidRange(_),
            ..
        } => SnapshotError::EmptyFile {
            key: key.to_string(),
        },
        other => other,
    }
}

/// The last `min(FOOTER_PREFETCH, size)` bytes of an object reported to be
/// `size` bytes long.
fn tail_range(size: u64) -> FooterRange {
    FooterRange::Range(size - size.min(FOOTER_PREFETCH), size)
}

/// The first footer read: a suffix read where the store can serve one, or
/// an explicit range over the last `min(FOOTER_PREFETCH, size)` bytes of
/// `listed_size` (the listing's, or a single-object HEAD's, reported size)
/// otherwise. A store without `suffix_range` (Azure; see
/// `docs/object-store-contract.md`) refuses a suffix range before sending
/// it, so there the read has to be placed from a size learned ahead of
/// time, and `listed_size` is the only one available before this read's own
/// response. If that response's own reported size disagrees with
/// `listed_size`, the listing was stale: retry once, placed from the size
/// this read just reported, and refuse `FileChanged` if the retry's own
/// reported size disagrees too. A first response carrying more bytes than
/// its range asked for is refused as [`SnapshotError::Corrupt`] before its
/// reported size is compared, since a discarded response never reaches
/// [`read_file`]'s own length check.
///
/// A `listed_size` of 0 is not evidence the object is empty: it issues no
/// zero-length range at all. Nor is a `listed_size` that over-reports by
/// `FOOTER_PREFETCH` or more, which would place the range at or past the
/// object's real end; the store refuses that one client-side as
/// `InvalidRange`, before any size can be compared. Both recover through
/// [`head_corrected_read`].
async fn first_footer_read(
    store: &dyn ObjectStoreBackend,
    limiter: &GetLimiter,
    memory: &Arc<MemoryBudget>,
    accounting: &PhaseAccounting,
    key: &str,
    pin: &Pin,
    listed_size: u64,
) -> Result<(PinnedRead, Reservation), SnapshotError> {
    if store.capabilities().suffix_range {
        return pinned_get(
            store,
            limiter,
            memory,
            accounting,
            key,
            FooterRange::Suffix(FOOTER_PREFETCH),
            pin,
        )
        .await
        .map_err(|err| empty_file_on_invalid_range(key, err));
    }
    if listed_size == 0 {
        return head_corrected_read(store, limiter, memory, accounting, key, pin).await;
    }
    let requested = tail_range(listed_size);
    let requested_len = requested.len();
    let first = pinned_get(store, limiter, memory, accounting, key, requested, pin).await;
    let (tail, reservation) = match first {
        Ok(ok) => ok,
        Err(SnapshotError::Store {
            source: StoreError::InvalidRange(_),
            ..
        }) => return head_corrected_read(store, limiter, memory, accounting, key, pin).await,
        Err(err) => return Err(err),
    };
    // The retry below discards this response, so `read_file`'s length check
    // never sees it: a store that ignored the range must be refused here,
    // before the bytes outlive their smaller reservation.
    let fetched = tail.outcome.data.len() as u64;
    if fetched > requested_len {
        return Err(SnapshotError::Corrupt {
            key: key.to_string(),
            message: format!(
                "footer read of {requested_len} bytes returned {fetched} bytes, more than \
                 requested"
            ),
        });
    }
    if tail.outcome.total_size == listed_size {
        return Ok((tail, reservation));
    }
    let real_size = tail.outcome.total_size;
    let (retry, retry_reservation) = pinned_get(
        store,
        limiter,
        memory,
        accounting,
        key,
        tail_range(real_size),
        pin,
    )
    .await
    .map_err(|err| match err {
        // One of the exceptions `read_error`'s doc comment names: this GET
        // has already read the object once, at the first explicit-range
        // read above.
        SnapshotError::FileMissing { key } => SnapshotError::FileChanged { key },
        other => empty_file_on_invalid_range(key, other),
    })?;
    if retry.outcome.total_size != real_size {
        return Err(SnapshotError::FileChanged {
            key: key.to_string(),
        });
    }
    Ok((retry, retry_reservation))
}

/// Recover the first footer read on a store without `suffix_range` when the
/// explicit range computed from a stale listed size could not even be
/// issued: a `listed_size` of 0 (no range to request) or one that places
/// the range past the object's real end (refused client-side as
/// `InvalidRange`). One HEAD, charged to [`QueryPhase::Resolve`] like the
/// snapshot's other HEAD, learns the real size: an ETag that disagrees with
/// `pin` means the object changed since the listing, a reported size of 0
/// means it really is empty, and otherwise one explicit-range read placed
/// at that size is the first footer read, refusing `FileChanged` if its own
/// reported size still disagrees with the HEAD's.
async fn head_corrected_read(
    store: &dyn ObjectStoreBackend,
    limiter: &GetLimiter,
    memory: &Arc<MemoryBudget>,
    accounting: &PhaseAccounting,
    key: &str,
    pin: &Pin,
) -> Result<(PinnedRead, Reservation), SnapshotError> {
    accounting
        .phase(QueryPhase::Resolve)
        .record_s3_request(AccountedOp::Head);
    let meta = store
        .head(key)
        .await
        .map_err(|source| read_error(key, source))?;
    if meta.etag.0 != pin.etag {
        return Err(SnapshotError::FileChanged {
            key: key.to_string(),
        });
    }
    if meta.size == 0 {
        return Err(SnapshotError::EmptyFile {
            key: key.to_string(),
        });
    }
    let (read, reservation) = pinned_get(
        store,
        limiter,
        memory,
        accounting,
        key,
        tail_range(meta.size),
        pin,
    )
    .await
    .map_err(|err| match err {
        // The other exception `read_error`'s doc comment names: this GET
        // has already been proved to exist, by the HEAD just above.
        SnapshotError::FileMissing { key } => SnapshotError::FileChanged { key },
        other => other,
    })?;
    if read.outcome.total_size != meta.size {
        return Err(SnapshotError::FileChanged {
            key: key.to_string(),
        });
    }
    Ok((read, reservation))
}

/// Read and check one file's footer, and describe the file as the read's
/// response reported it. See [`first_footer_read`] for how the first read
/// is placed and self-corrected; the listing's reported size otherwise has
/// no bearing on where it lands, so a listing that under- or over-reports a
/// file's size, in either direction, cannot misplace a read on a store with
/// `suffix_range`, and costs at most one retry on one without -- or, when
/// the listed size left no valid range to read at all, one HEAD plus one
/// read (see [`head_corrected_read`]).
async fn read_file(
    store: &dyn ObjectStoreBackend,
    grant: &Grant,
    limiter: &GetLimiter,
    memory: &Arc<MemoryBudget>,
    accounting: &PhaseAccounting,
    candidate: Candidate,
) -> Result<(ParquetFile, Schema), SnapshotError> {
    let key = candidate.key;
    let corrupt = |message: String| SnapshotError::Corrupt {
        key: key.clone(),
        message,
    };
    let listed_pin = Pin::etag(candidate.etag);
    let (tail, _tail_reservation) = first_footer_read(
        store,
        limiter,
        memory,
        accounting,
        &key,
        &listed_pin,
        candidate.size,
    )
    .await?;
    let size = tail.outcome.total_size;
    if size == 0 {
        return Err(SnapshotError::EmptyFile { key: key.clone() });
    }
    let recorded = tail.pin.clone();
    let data = tail.outcome.data;
    let fetched = data.len() as u64;
    let expected = size.min(FOOTER_PREFETCH);
    if fetched != expected {
        return Err(corrupt(format!(
            "footer read of {FOOTER_PREFETCH} bytes from a {size}-byte file returned {fetched} \
             bytes"
        )));
    }
    let tail_start = size - fetched;
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
    let (footer, _extra_reservations): (Bytes, Vec<Reservation>) = if footer_and_trailer <= fetched
    {
        (data.slice(split - footer_len as usize..split), Vec::new())
    } else {
        // Select the version the first read saw, so both reads are of one
        // object's bytes; If-Match stays on the listed ETag.
        let pin = Pin {
            etag: listed_pin.etag.clone(),
            version: recorded.version.clone(),
        };
        let footer_start = size - footer_and_trailer;
        let (before, before_reservation) = pinned_get(
            store,
            limiter,
            memory,
            accounting,
            &key,
            FooterRange::Range(footer_start, tail_start),
            &pin,
        )
        .await
        .map_err(|err| match err {
            // The one exception `read_error`'s doc comment names.
            SnapshotError::FileMissing { key } => SnapshotError::FileChanged { key },
            other => other,
        })?;
        if before.outcome.total_size != size || before.pin != recorded {
            return Err(SnapshotError::FileChanged { key: key.clone() });
        }
        let before = before.outcome.data;
        if before.len() as u64 != tail_start - footer_start {
            return Err(corrupt(format!(
                "read of bytes {footer_start}..{tail_start} returned {} bytes",
                before.len()
            )));
        }
        // The concatenation is a third buffer, live alongside the tail
        // and before reads it copies, so it needs its own reservation:
        // the two GETs' reservations cover only their own bytes.
        let concat_reservation =
            memory
                .reserve(footer_len)
                .map_err(|exhausted| SnapshotError::MemoryExhausted {
                    key: key.clone(),
                    requested: exhausted.requested,
                    reserved: exhausted.reserved,
                    limit: exhausted.limit,
                })?;
        let mut footer = Vec::with_capacity(footer_len as usize);
        footer.extend_from_slice(&before);
        footer.extend_from_slice(&data[..split]);
        (
            Bytes::from(footer),
            vec![before_reservation, concat_reservation],
        )
    };
    let DecodedFooter {
        metadata,
        reservation: decoding,
        ..
    } = decode_footer(&footer, size - footer_and_trailer, |bytes| {
        memory
            .reserve(bytes)
            .map_err(|exhausted| SnapshotError::MemoryExhausted {
                key: key.clone(),
                requested: exhausted.requested,
                reserved: exhausted.reserved,
                limit: exhausted.limit,
            })
    })
    .map_err(|err| match err {
        DecodeError::Refused(message) => corrupt(message),
        DecodeError::Reserve(err) => err,
    })?;
    let row_count = u64::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| corrupt("the footer records a negative row count".to_string()))?;
    // `trailer_footer_len` widens the trailer's own 4-byte field (a u32) to
    // build `footer_len`, so this cannot truncate.
    let footer_len = footer_len as u32;
    let schema = file_schema(&metadata).map_err(|message| corrupt(format!("schema: {message}")))?;
    #[cfg(test)]
    tests::schema_built(memory);
    // The decode estimate covers the metadata and the conversions that built
    // the schema from it; it is released only once they are gone.
    drop(metadata);
    drop(decoding);
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Mutex, PoisonError};
    use std::task::Poll;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, DictionaryArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, Etag, GetOutcome, ListPage, ObjectMeta, PageToken, PutOptions,
        PutOutcome, Version,
    };
    use ravel_pqtable::grants::resolve_location;

    use super::*;
    use crate::test_support::{
        BUCKET, PROFILE, arrow_schema_panicking, binary_parquet_bytes, footer_len_of,
        int_parquet_bytes, parquet_bytes, write,
    };

    const DEADLINE: Duration = Duration::from_secs(60);

    type SchemaHook = Box<dyn Fn(&Arc<MemoryBudget>)>;

    thread_local! {
        /// Run by `read_file` on this thread once a file's schema is built
        /// and before the decoded footer is dropped, with the budget the
        /// file's reads and decode were reserved from.
        static SCHEMA_BUILT: std::cell::RefCell<Option<SchemaHook>> =
            const { std::cell::RefCell::new(None) };
    }

    pub(super) fn schema_built(memory: &Arc<MemoryBudget>) {
        SCHEMA_BUILT.with_borrow(|hook| {
            if let Some(hook) = hook {
                hook(memory);
            }
        });
    }

    /// Guards: the decode reservation `read_file` holds past `file_schema`.
    /// With a budget of exactly the footer read and the decode estimate, the
    /// budget is still full once the schema is built from the decoded
    /// footer, and a further byte is refused there.
    #[tokio::test]
    async fn the_decode_reservation_is_held_while_the_schema_is_built() {
        let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        let bytes = parquet_bytes(&[1], &["x"]);
        let estimate = decode_estimate(&bytes);
        put(store.inner(), "data/a.parquet", bytes).await;
        let limiter = GetLimiter::new(1).expect("permits");
        let memory = Arc::new(MemoryBudget::new(FOOTER_PREFETCH + estimate));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);
        SCHEMA_BUILT.set(Some(Box::new(move |memory| {
            let one_more = memory.reserve(1).is_ok();
            record
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((memory.reserved(), one_more));
        })));
        let got = snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
            &memory,
            DEADLINE,
            &PhaseAccounting::new(),
        )
        .await;
        SCHEMA_BUILT.set(None);
        assert_eq!(got.expect("snapshot").files.len(), 1);
        assert_eq!(
            *seen.lock().unwrap_or_else(PoisonError::into_inner),
            [(FOOTER_PREFETCH + estimate, false)]
        );
        assert_eq!(memory.reserved(), 0);
    }

    fn grant(prefix: &str) -> Grant {
        Grant {
            profile: PROFILE.to_string(),
            scheme: "s3".to_string(),
            bucket: BUCKET.to_string(),
            prefix: prefix.to_string(),
            created_unix_ns: 0,
            created_by: "test".to_string(),
        }
    }

    /// `url` resolved against a grant of `s3://lake/data`.
    fn location(url: &str) -> GrantedLocation {
        let (grant, key) = resolve_location(&[grant("data")], url).expect("granted");
        GrantedLocation { grant, key }
    }

    async fn snapshot(
        store: &dyn ObjectStoreBackend,
        url: &str,
    ) -> Result<LocationSnapshot, SnapshotError> {
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::unlimited());
        snapshot_location(
            store,
            &location(url),
            &limiter,
            &memory,
            DEADLINE,
            &PhaseAccounting::new(),
        )
        .await
    }

    async fn put(store: &dyn ObjectStoreBackend, key: &str, bytes: Bytes) {
        store
            .put(key, bytes, PutOptions::default())
            .await
            .expect("put");
    }

    fn keys(snapshot: &LocationSnapshot) -> Vec<String> {
        snapshot.files.iter().map(key_of).collect()
    }

    /// A store over a `MemoryStore` that records every LIST prefix, HEAD and
    /// pinned GET, and can serve a synthetic listing, report its listing
    /// differently from its reads, or repeat the last key of each page at the
    /// start of the next.
    #[derive(Default)]
    struct Scripted {
        inner: MemoryStore,
        /// List this many `.parquet` keys that hold no object.
        synthetic: Option<usize>,
        /// Report each listed object's ETag prefixed with `listed:`, its size
        /// one byte short, and a version of `listed-version`; strip the prefix
        /// from a pin's ETag before reading.
        misreport_listing: bool,
        /// Replace every listed object's reported `size` with this value,
        /// independent of `misreport_listing`, to prove the footer read
        /// never trusts the listing's size: it only ever informs a
        /// zero-size refusal or an out-of-range explicit GET if something
        /// still reads it.
        lie_listed_size: Option<u64>,
        repeat_page_boundary: bool,
        /// Replace the reported pin's version on the 1-based `get_pinned`
        /// call numbered here, to synthesize a long-footer second read that
        /// disagrees with the first without racing a real overwrite.
        lie_pin_version_on_call: Option<(usize, String)>,
        /// Answer the first `get_pinned` call with `StoreError::InvalidRange`
        /// instead of calling through, simulating an endpoint that rejects a
        /// suffix read against a 0-byte object with a 416 rather than
        /// answering it with an empty body.
        invalid_range_on_first_get: bool,
        /// Answer the 1-based `get_pinned` call numbered here with
        /// `StoreError::NotFound` instead of calling through, simulating the
        /// object vanishing between an earlier read of it and this one.
        not_found_on_call: Option<usize>,
        /// Report `suffix_range: false`, as the Azure external store does,
        /// instead of the inner `MemoryStore`'s `true`.
        force_no_suffix_range: bool,
        /// Answer every `get_pinned` with the whole object, whatever range
        /// was requested, as an endpoint that ignores `Range` does.
        ignore_range: bool,
        /// Serve `.csv` keys instead of `.parquet` ones from `synthetic`, so
        /// a synthetic listing can exercise the skip-and-count path instead
        /// of the candidate path.
        synthetic_non_parquet: bool,
        /// Serve these pages verbatim, in call order, ignoring the requested
        /// page token and the underlying store. The only way to script a raw
        /// delivery sequence a real backend would refuse to produce (a
        /// decrease): no listing built on a real store can reach one.
        scripted_pages: Mutex<Vec<ListPage>>,
        lists: Mutex<Vec<String>>,
        heads: Mutex<Vec<String>>,
        gets: Mutex<Vec<(String, GetRange, Pin)>>,
    }

    impl Scripted {
        fn lists(&self) -> Vec<String> {
            self.lists
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        fn heads(&self) -> Vec<String> {
            self.heads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        fn gets(&self) -> Vec<(String, GetRange, Pin)> {
            self.gets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        fn synthetic_page(count: usize, page: Option<PageToken>, suffix: &str) -> ListPage {
            let start = page.map_or(0, |PageToken(at)| at.parse().expect("token"));
            let end = count.min(start + 1000);
            let objects = (start..end)
                .map(|i| ObjectMeta {
                    key: format!("data/{i:06}{suffix}"),
                    size: 100,
                    etag: Etag(format!("e{i}")),
                    version: Version(format!("v{i}")),
                    last_modified_unix_ms: 0,
                })
                .collect();
            let next = (end < count).then(|| PageToken(end.to_string()));
            ListPage { objects, next }
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for Scripted {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &Pin,
        ) -> Result<PinnedRead, StoreError> {
            let call = {
                let mut gets = self.gets.lock().unwrap_or_else(PoisonError::into_inner);
                gets.push((key.to_string(), range, pin.clone()));
                gets.len()
            };
            if self.invalid_range_on_first_get && call == 1 {
                return Err(StoreError::InvalidRange(
                    "zero-length suffix not satisfiable".to_string(),
                ));
            }
            if self.not_found_on_call == Some(call) {
                return Err(StoreError::NotFound);
            }
            let mut sent_pin = pin.clone();
            if self.misreport_listing {
                sent_pin.etag = sent_pin.etag.trim_start_matches("listed:").to_string();
            }
            let sent_range = if self.ignore_range {
                GetRange::Full
            } else {
                range
            };
            let mut read = self.inner.get_pinned(key, sent_range, &sent_pin).await?;
            if let Some((n, ref version)) = self.lie_pin_version_on_call
                && n == call
            {
                read.pin.version = Some(version.clone());
            }
            Ok(read)
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.heads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(key.to_string());
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.lists
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(prefix.to_string());
            {
                let mut scripted = self
                    .scripted_pages
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if !scripted.is_empty() {
                    return Ok(scripted.remove(0));
                }
            }
            if let Some(count) = self.synthetic {
                let suffix = if self.synthetic_non_parquet {
                    ".csv"
                } else {
                    PARQUET_SUFFIX
                };
                return Ok(Self::synthetic_page(count, page, suffix));
            }
            let repeated = match (&page, self.repeat_page_boundary) {
                (Some(PageToken(after)), true) => Some(self.inner.head(after).await?),
                _ => None,
            };
            let mut listed = self.inner.list(prefix, page).await?;
            if let Some(repeated) = repeated {
                listed.objects.insert(0, repeated);
            }
            if self.misreport_listing {
                for object in &mut listed.objects {
                    object.etag = Etag(format!("listed:{}", object.etag.0));
                    object.size = object.size.saturating_sub(1);
                    object.version = Version("listed-version".to_string());
                }
            }
            if let Some(size) = self.lie_listed_size {
                for object in &mut listed.objects {
                    object.size = size;
                }
            }
            Ok(listed)
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            let mut caps = self.inner.capabilities();
            if self.force_no_suffix_range {
                caps.suffix_range = false;
            }
            caps
        }
    }

    /// A store holding `data/a.parquet` and `data/b.parquet`, both of schema
    /// `a: Int64, b: Utf8`, behind a `FaultStore`.
    async fn two_files() -> FaultStore<MemoryStore> {
        let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        put(store.inner(), "data/a.parquet", parquet_bytes(&[1], &["x"])).await;
        put(store.inner(), "data/b.parquet", parquet_bytes(&[2], &["y"])).await;
        store
    }

    /// Mutation that fails it: an unconditional footer read (`get_with_pin`
    /// in place of `get_pinned` on the listed ETag) snapshots the new bytes.
    #[tokio::test]
    async fn a_file_changed_between_list_and_footer_refuses_create() {
        let store = two_files().await;
        let listed = store.inner().head("data/b.parquet").await.expect("head");
        let gate = store.hold(Op::Get, Some("data/b.parquet".into()), Occurrence::Nth(1));
        let overwrite = async {
            gate.wait_until_held(1).await;
            put(
                store.inner(),
                "data/b.parquet",
                parquet_bytes(&[3, 4], &["z", "w"]),
            )
            .await;
            for id in gate.held() {
                gate.release(id);
            }
        };
        let (result, ()) = tokio::join!(snapshot(&store, "s3://lake/data/"), overwrite);
        match result {
            Err(SnapshotError::FileChanged { key }) => assert_eq!(key, "data/b.parquet"),
            other => panic!("expected FileChanged, got {other:?}"),
        }

        let current = store.inner().head("data/b.parquet").await.expect("head");
        assert_ne!(current.etag, listed.etag);
        let again = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(again.files[1].etag, current.etag.0);
        assert_eq!(again.files[1].row_count, 2);
    }

    /// Mutation that fails it: mapping `NotFound` from the footer read to
    /// `Store` instead of `FileMissing`.
    #[tokio::test]
    async fn a_file_deleted_between_list_and_footer_refuses_naming_it() {
        let store = two_files().await;
        let gate = store.hold(Op::Get, Some("data/b.parquet".into()), Occurrence::Nth(1));
        let delete = async {
            gate.wait_until_held(1).await;
            store
                .inner()
                .delete("data/b.parquet")
                .await
                .expect("delete");
            for id in gate.held() {
                gate.release(id);
            }
        };
        let (result, ()) = tokio::join!(snapshot(&store, "s3://lake/data/"), delete);
        match result {
            Err(SnapshotError::FileMissing { key }) => assert_eq!(key, "data/b.parquet"),
            other => panic!("expected FileMissing, got {other:?}"),
        }
    }

    /// A `Suffix(FOOTER_PREFETCH)` read of a zero-byte object succeeds with
    /// zero bytes, so the zero-size check runs on the GET response's
    /// `total_size`, not on a failed read. Mutation that fails it: dropping
    /// that check falls through to the `fetched < TRAILER_LEN` branch and
    /// refuses as `Corrupt` instead of `EmptyFile`.
    #[tokio::test]
    async fn a_zero_byte_file_refuses_naming_it() {
        let store = two_files().await;
        put(store.inner(), "data/c.parquet", Bytes::new()).await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::EmptyFile { key }) => assert_eq!(key, "data/c.parquet"),
            other => panic!("expected EmptyFile, got {other:?}"),
        }
    }

    /// An endpoint that answers a suffix read of a 0-byte object with a 416
    /// (`InvalidRange`) instead of an empty body: the object exists, at the
    /// pinned ETag, and simply has no bytes to read, so this is `EmptyFile`,
    /// not a generic `Store` error. Mutation that fails it: dropping the
    /// `InvalidRange` mapping on the first footer read reports `Store`
    /// instead.
    #[tokio::test]
    async fn an_invalid_range_on_the_first_footer_read_refuses_as_empty_file() {
        let store = Scripted {
            invalid_range_on_first_get: true,
            ..Scripted::default()
        };
        put(&store, "data/c.parquet", Bytes::new()).await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::EmptyFile { key }) => assert_eq!(key, "data/c.parquet"),
            other => panic!("expected EmptyFile, got {other:?}"),
        }
    }

    /// A store without `suffix_range` (Azure, in production) must never see
    /// a `GetRange::Suffix`: it refuses one before the request is even
    /// sent. Mutation that fails it: branching the first footer read on
    /// anything but `capabilities().suffix_range` still issues
    /// `GetRange::Suffix(FOOTER_PREFETCH)`, which this assertion catches
    /// against the code before this fix.
    #[tokio::test]
    async fn a_store_without_suffix_range_reads_an_explicit_footer_range() {
        let store = Scripted {
            force_no_suffix_range: true,
            ..Scripted::default()
        };
        assert!(!store.capabilities().suffix_range);
        put(
            &store,
            "data/a.parquet",
            parquet_bytes(&[4, 5], &["p", "q"]),
        )
        .await;
        let result = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(keys(&result), vec!["data/a.parquet"]);
        let ranges: Vec<GetRange> = store
            .gets()
            .into_iter()
            .map(|(_, range, _)| range)
            .collect();
        assert!(!ranges.is_empty());
        assert!(
            ranges
                .iter()
                .all(|range| !matches!(range, GetRange::Suffix(_))),
            "{ranges:?}"
        );
    }

    /// Without `suffix_range`, the first footer read is placed from the
    /// listing's reported size, which can be stale in either direction. One
    /// retry, placed from the size the first read's own response reports,
    /// corrects it. Mutation that fails it: trusting the listed size
    /// outright (no retry) either snapshots the wrong bytes (over-report:
    /// the read lands short of the real tail) or refuses a valid file as
    /// `Corrupt` (under-report: the read returns fewer bytes than the
    /// trailer records).
    #[tokio::test]
    async fn a_stale_listed_size_without_suffix_range_is_corrected_by_one_retry() {
        let bytes = parquet_bytes(&[4, 5], &["p", "q"]);
        let real_size = bytes.len() as u64;
        for lied_size in [real_size + 50, real_size.saturating_sub(10)] {
            let store = Scripted {
                force_no_suffix_range: true,
                lie_listed_size: Some(lied_size),
                ..Scripted::default()
            };
            put(&store, "data/a.parquet", bytes.clone()).await;
            let result = snapshot(&store, "s3://lake/data/")
                .await
                .unwrap_or_else(|err| panic!("lied_size={lied_size} real_size={real_size}: {err}"));
            assert_eq!(
                keys(&result),
                vec!["data/a.parquet"],
                "lied_size={lied_size}"
            );
            assert_eq!(result.files[0].row_count, 2, "lied_size={lied_size}");
            let ranges: Vec<GetRange> = store
                .gets()
                .into_iter()
                .map(|(_, range, _)| range)
                .collect();
            assert!(
                ranges
                    .iter()
                    .all(|range| !matches!(range, GetRange::Suffix(_))),
                "lied_size={lied_size}: {ranges:?}"
            );
            assert_eq!(
                ranges.len(),
                2,
                "a mismatched listed size costs exactly one retry: lied_size={lied_size}: \
                 {ranges:?}"
            );
        }
    }

    /// Without `suffix_range`, a listing that reports size 0 for a
    /// non-empty file leaves no range to request at all, and one that
    /// over-reports by `FOOTER_PREFETCH` or more places the range at or past
    /// the object's real end, which the store refuses client-side as
    /// `InvalidRange` before any size can be compared. Neither is evidence
    /// the object is empty: both now snapshot the file correctly, recovered
    /// by one HEAD plus one explicit-range read at the HEAD's reported
    /// size. A genuinely empty object still refuses `EmptyFile`, now
    /// reached the same way (its listed size is really 0). Mutation that
    /// fails it: mapping the first explicit read's `InvalidRange` straight
    /// to `EmptyFile` -- the code before this fix -- which refuses all
    /// three cases, wrongly for the first two: both lying cases return
    /// `EmptyFile` against that code instead of a snapshot.
    #[tokio::test]
    async fn a_listed_size_that_leaves_no_valid_range_is_corrected_by_a_head() {
        let bytes = parquet_bytes(&[4, 5], &["p", "q"]);
        let real_size = bytes.len() as u64;
        for lied_size in [0, real_size + FOOTER_PREFETCH + 1] {
            let store = Scripted {
                force_no_suffix_range: true,
                lie_listed_size: Some(lied_size),
                ..Scripted::default()
            };
            put(&store, "data/a.parquet", bytes.clone()).await;
            let result = snapshot(&store, "s3://lake/data/")
                .await
                .unwrap_or_else(|err| panic!("lied_size={lied_size} real_size={real_size}: {err}"));
            assert_eq!(
                keys(&result),
                vec!["data/a.parquet"],
                "lied_size={lied_size}"
            );
            assert_eq!(result.files[0].row_count, 2, "lied_size={lied_size}");
            assert_eq!(
                store.heads(),
                ["data/a.parquet"],
                "lied_size={lied_size}: recovery takes one HEAD"
            );
        }

        let store = Scripted {
            force_no_suffix_range: true,
            ..Scripted::default()
        };
        put(&store, "data/c.parquet", Bytes::new()).await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::EmptyFile { key }) => assert_eq!(key, "data/c.parquet"),
            other => panic!("expected EmptyFile, got {other:?}"),
        }
    }

    /// The size-mismatch retry (and the HEAD-driven re-read above it) have
    /// already read this object once, at the first explicit-range read; a
    /// `NotFound` there means the object changed since, not that it was
    /// never there. Mutation that fails it: reporting `FileMissing` for
    /// this retry's GET instead of remapping it to `FileChanged` (confirmed
    /// against the code before this fix: the same scenario reported
    /// `FileMissing`).
    #[tokio::test]
    async fn a_notfound_on_the_size_mismatch_retry_reports_the_file_changed() {
        let bytes = parquet_bytes(&[4, 5], &["p", "q"]);
        let real_size = bytes.len() as u64;
        let store = Scripted {
            force_no_suffix_range: true,
            lie_listed_size: Some(real_size + 50),
            not_found_on_call: Some(2),
            ..Scripted::default()
        };
        put(&store, "data/a.parquet", bytes).await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::FileChanged { key }) => assert_eq!(key, "data/a.parquet"),
            other => panic!("expected FileChanged, got {other:?}"),
        }
    }

    /// Without `suffix_range`, a stale listed size discards the first
    /// footer read's response for a retry, so `read_file`'s length check
    /// never sees it. An endpoint that ignores `Range` answers that read with
    /// the whole object, more bytes than the range's reservation covers: it
    /// is refused at the first read, with no retry issued. Mutation that
    /// fails it: dropping the length check in `first_footer_read`, after
    /// which the retry (also the whole object, which is then exactly the
    /// tail it asked for) snapshots the file with two GETs.
    #[tokio::test]
    async fn a_store_ignoring_range_on_a_stale_listed_size_refuses_before_the_retry() {
        let bytes = parquet_bytes(&[4, 5], &["p", "q"]);
        let real_size = bytes.len() as u64;
        let store = Scripted {
            force_no_suffix_range: true,
            ignore_range: true,
            lie_listed_size: Some(real_size - 10),
            ..Scripted::default()
        };
        put(&store, "data/a.parquet", bytes).await;
        assert_corrupt(
            snapshot(&store, "s3://lake/data/").await,
            "data/a.parquet",
            &format!(
                "footer read of {} bytes returned {real_size} bytes, more than requested",
                real_size - 10
            ),
        );
        let ranges: Vec<GetRange> = store
            .gets()
            .into_iter()
            .map(|(_, range, _)| range)
            .collect();
        assert_eq!(ranges, [GetRange::Range(0, real_size - 10)]);
    }

    fn assert_corrupt(result: Result<LocationSnapshot, SnapshotError>, key: &str, needle: &str) {
        match result {
            Err(SnapshotError::Corrupt { key: got, message }) => {
                assert_eq!(got, key);
                assert!(message.contains(needle), "{needle:?} not in {message:?}");
            }
            other => panic!("expected Corrupt with {needle:?}, got {other:?}"),
        }
    }

    /// Three truncations: the end cut off (no magic), the start cut off below
    /// the footer length the trailer records, and less than a trailer left.
    /// Mutation that fails it: dropping the footer-length-versus-size check
    /// (the second case then underflows computing where the footer starts).
    #[tokio::test]
    async fn a_truncated_file_refuses_naming_it() {
        let valid = parquet_bytes(&[1, 2, 3], &["x", "y", "z"]);
        let cases: [(Bytes, &str); 3] = [
            (valid.slice(..valid.len() / 2), "footer trailer"),
            (
                valid.slice(valid.len() - 20..),
                "-byte footer in a 20-byte file",
            ),
            (
                Bytes::from_static(b"PAR1"),
                "shorter than the 8-byte trailer",
            ),
        ];
        for (bytes, needle) in cases {
            let store = two_files().await;
            put(store.inner(), "data/c.parquet", bytes).await;
            assert_corrupt(
                snapshot(&store, "s3://lake/data/").await,
                "data/c.parquet",
                needle,
            );
        }
    }

    /// The reader's own footer checks run at create: a file with enough of
    /// its start cut off that its last column chunk ends past the footer's
    /// start (`check_chunks`), and a malformed embedded `ARROW:schema` that
    /// panics Arrow's decoder (the `catch_unwind` check). Mutation that fails
    /// it: decoding the footer with `ParquetMetaDataReader::decode_metadata`
    /// alone in place of `decode_footer`.
    #[tokio::test]
    async fn a_footer_the_scan_refuses_is_refused_at_create() {
        let valid = parquet_bytes(&[1, 2, 3], &["x", "y", "z"]);
        let footer_end = valid.len() - TRAILER_LEN as usize;
        let data_end = footer_end - footer_len_of(&valid) as usize;
        let metadata = parquet::file::metadata::ParquetMetaDataReader::decode_metadata(
            &valid[data_end..footer_end],
        )
        .expect("footer");
        let chunks_end = metadata
            .row_groups()
            .iter()
            .flat_map(|group| group.columns())
            .map(|chunk| {
                let (start, len) = chunk.byte_range();
                (start + len) as usize
            })
            .max()
            .expect("a column chunk");
        let cases = [
            (valid.slice(data_end - chunks_end + 1..), "outside the"),
            (
                Bytes::from(arrow_schema_panicking(&valid)),
                "embedded Arrow schema is malformed",
            ),
        ];
        for (bytes, needle) in cases {
            let store = two_files().await;
            put(store.inner(), "data/c.parquet", bytes).await;
            assert_corrupt(
                snapshot(&store, "s3://lake/data/").await,
                "data/c.parquet",
                needle,
            );
        }
    }

    /// Mutations that fail it: `ends_with("parquet")` in place of
    /// `ends_with(PARQUET_SUFFIX)` (`notparquet` becomes a file), and counting
    /// a directory marker as an other suffix.
    #[tokio::test]
    async fn directory_markers_and_other_suffixes_are_skipped_and_counted() {
        let store = MemoryStore::new();
        let valid = parquet_bytes(&[1], &["x"]);
        for key in ["data/", "data/year=2024/"] {
            put(&store, key, Bytes::new()).await;
        }
        for key in ["data/year=2024/a.parquet", "data/b.parquet"] {
            put(&store, key, valid.clone()).await;
        }
        for key in [
            "data/c.csv",
            "data/d.parquet.tmp",
            "data/E.PARQUET",
            "data/notparquet",
            "data/_SUCCESS",
        ] {
            put(&store, key, Bytes::from_static(b"not parquet")).await;
        }
        let got = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(keys(&got), ["data/b.parquet", "data/year=2024/a.parquet"]);
        assert_eq!(got.skipped_directory_markers, 2);
        assert_eq!(got.skipped_other_suffixes, 5);
        let names: Vec<&str> = got
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, ["a", "b"], "a Hive directory adds no column");
    }

    /// A store may list a key again at the start of the next page; here the
    /// repeated boundary key is a `.parquet` file on two different page
    /// boundaries (`data/a.parquet`, then `data/c.parquet`). Mutation that
    /// fails it: dropping the drain's last-key dedup lists each repeated
    /// file twice. `skipped_keys_repeated_across_a_page_boundary_are_counted_once`
    /// below covers a marker and a non-`.parquet` key repeating this way.
    #[tokio::test]
    async fn a_key_listed_on_two_pages_is_counted_once() {
        let store = Scripted {
            inner: MemoryStore::with_page_size(2),
            repeat_page_boundary: true,
            ..Scripted::default()
        };
        let valid = parquet_bytes(&[1], &["x"]);
        put(&store, "data/", Bytes::new()).await;
        put(&store, "data/a.parquet", valid.clone()).await;
        put(&store, "data/b.csv", Bytes::from_static(b"csv")).await;
        put(&store, "data/c.parquet", valid.clone()).await;
        put(&store, "data/d/", Bytes::new()).await;
        let got = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(store.lists().len(), 3, "three pages were listed");
        assert_eq!(keys(&got), ["data/a.parquet", "data/c.parquet"]);
        assert_eq!(got.skipped_directory_markers, 2);
        assert_eq!(got.skipped_other_suffixes, 1);
    }

    /// A directory marker and a non-`.parquet` key can repeat across a page
    /// boundary too; the drain's last-key dedup applies to every listed key
    /// before the suffix is even looked at, not only to `.parquet` files.
    /// With `page_size(1)` every key but the last repeats once. Mutation
    /// that fails it: deduping only `.parquet` keys, which counts the
    /// marker and the skipped key twice.
    #[tokio::test]
    async fn skipped_keys_repeated_across_a_page_boundary_are_counted_once() {
        let store = Scripted {
            inner: MemoryStore::with_page_size(1),
            repeat_page_boundary: true,
            ..Scripted::default()
        };
        let valid = parquet_bytes(&[1], &["x"]);
        put(&store, "data/", Bytes::new()).await;
        put(&store, "data/a.csv", Bytes::from_static(b"csv")).await;
        put(&store, "data/b.parquet", valid.clone()).await;
        put(&store, "data/c.parquet", valid).await;
        let got = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(keys(&got), ["data/b.parquet", "data/c.parquet"]);
        assert_eq!(got.skipped_directory_markers, 1);
        assert_eq!(got.skipped_other_suffixes, 1);
    }

    /// A raw delivery sequence a real backend would refuse to produce: the
    /// second page's only key sorts below the first page's, which is not the
    /// "repeat of the last delivered key" the contract allows across a page
    /// boundary. `list_files` now runs on `drain_pages`
    /// (`crates/ravel-object-store/src/lib.rs`), which refuses this as a
    /// typed `StoreError::ListOrderViolation` rather than reordering or
    /// silently admitting both keys (docs/object-store-contract.md,
    /// "Listing"). Confirmed against the code before this fix: the old
    /// hand-rolled loop deduped by a `HashSet` and had no ordering check at
    /// all, so it read both keys as two distinct files instead of refusing.
    #[tokio::test]
    async fn a_listing_that_decreases_across_pages_refuses_with_a_typed_error() {
        let store = Scripted::default();
        *store
            .scripted_pages
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = vec![
            ListPage {
                objects: vec![ObjectMeta {
                    key: "data/b.parquet".to_string(),
                    size: 1,
                    etag: Etag("eb".to_string()),
                    version: Version("vb".to_string()),
                    last_modified_unix_ms: 0,
                }],
                next: Some(PageToken("b".to_string())),
            },
            ListPage {
                objects: vec![ObjectMeta {
                    key: "data/a.parquet".to_string(),
                    size: 1,
                    etag: Etag("ea".to_string()),
                    version: Version("va".to_string()),
                    last_modified_unix_ms: 0,
                }],
                next: None,
            },
        ];
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::List {
                source:
                    StoreError::ListOrderViolation {
                        previous,
                        offending,
                        ..
                    },
                ..
            }) => {
                assert_eq!(previous, "data/b.parquet");
                assert_eq!(offending, "data/a.parquet");
            }
            other => panic!("expected List(ListOrderViolation), got {other:?}"),
        }
    }

    /// `MAX_TABLE_FILES` never bounds a listing's skipped keys, only its
    /// `.parquet` candidates, so a directory of skip-only keys far past that
    /// limit must still drain to completion. This does not observe memory
    /// directly; that is pinned instead by this module holding no `HashSet`
    /// (or other per-key set) and by the order-violation test above, which
    /// only a constant-memory last-key dedup can pass.
    #[tokio::test]
    async fn a_listing_of_many_skipped_keys_completes_with_the_right_counts() {
        let count = 2 * MAX_TABLE_FILES;
        let store = Scripted {
            synthetic: Some(count),
            synthetic_non_parquet: true,
            ..Scripted::default()
        };
        let listed = list_files(
            &store,
            &location("s3://lake/data/"),
            &PhaseAccounting::new(),
            MAX_TABLE_FILES,
        )
        .await
        .expect("listing");
        assert_eq!(listed.candidates.len(), 0);
        assert_eq!(listed.other_suffixes, count as u64);
        assert_eq!(listed.directory_markers, 0);
        assert_eq!(store.lists().len(), count / 1000);
    }

    /// Mutation that fails it: dropping the `key_is_addressable` check
    /// (`MemoryStore` reads the key back, so the snapshot succeeds).
    #[tokio::test]
    async fn an_unaddressable_listed_key_refuses_naming_it() {
        let store = Scripted::default();
        let valid = parquet_bytes(&[1], &["x"]);
        put(&store, "data/a.parquet", valid.clone()).await;
        put(&store, "data/x//y.parquet", valid).await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::Unaddressable { key, .. }) => assert_eq!(key, "data/x//y.parquet"),
            other => panic!("expected Unaddressable, got {other:?}"),
        }
        assert!(store.gets().is_empty(), "refused before any footer read");
    }

    /// A location handed in with a grant that does not admit it. Mutation
    /// that fails it: dropping the `contains_key` check.
    #[tokio::test]
    async fn a_listed_key_outside_the_grant_refuses_naming_it() {
        let store = two_files().await;
        let (_, key) = resolve_location(&[grant("data")], "s3://lake/data/").expect("granted");
        let location = GrantedLocation {
            grant: grant("other"),
            key,
        };
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::unlimited());
        let result = snapshot_location(
            &store,
            &location,
            &limiter,
            &memory,
            DEADLINE,
            &PhaseAccounting::new(),
        )
        .await;
        match result {
            Err(SnapshotError::OutsideGrant { key, grant, .. }) => {
                assert_eq!(key, "data/a.parquet");
                assert_eq!(grant, "s3://lake/other");
            }
            other => panic!("expected OutsideGrant, got {other:?}"),
        }
    }

    /// Mutation that fails it: returning an empty snapshot with an empty
    /// schema when no file was listed.
    #[tokio::test]
    async fn a_location_with_no_parquet_file_refuses() {
        let store = MemoryStore::new();
        put(&store, "other/a.parquet", parquet_bytes(&[1], &["x"])).await;
        for url in ["s3://lake/data/", "s3://lake/data/empty/"] {
            match snapshot(&store, url).await {
                Err(SnapshotError::NoFiles { location }) => assert_eq!(location, url),
                other => panic!("expected NoFiles, got {other:?}"),
            }
        }
        put(&store, "data/", Bytes::new()).await;
        put(&store, "data/c.csv", Bytes::from_static(b"csv")).await;
        assert!(matches!(
            snapshot(&store, "s3://lake/data/").await,
            Err(SnapshotError::NoFiles { .. })
        ));
    }

    /// Mutation that fails it: `MAX_TABLE_FILES` raised by one, or the check
    /// kept before the push but written `len > limit` instead of `== limit`
    /// (an off-by-one that lets exactly one extra file through silently;
    /// with only `MAX_TABLE_FILES + 1` synthetic files here it never fires
    /// in this loop at all, so the snapshot admits 100,001 files and goes on
    /// to read their footers).
    #[tokio::test]
    async fn more_than_the_file_limit_refuses_before_any_footer_read() {
        let store = Scripted {
            synthetic: Some(MAX_TABLE_FILES + 1),
            ..Scripted::default()
        };
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::TooManyFiles { limit, .. }) => assert_eq!(limit, 100_000),
            other => panic!("expected TooManyFiles, got {other:?}"),
        }
        assert_eq!(store.lists().len(), 101, "the whole listing was read");
        assert!(store.gets().is_empty());
    }

    /// The limit admits exactly `limit` files. Mutation that fails it:
    /// `len + 1 == limit` refuses a location holding exactly the limit.
    #[tokio::test]
    async fn exactly_the_file_limit_is_admitted() {
        let store = two_files().await;
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::unlimited());
        let run = |limit| {
            let (store, limiter, memory) = (&store, &limiter, &memory);
            async move {
                snapshot_with_limit(
                    store,
                    &location("s3://lake/data/"),
                    limiter,
                    memory,
                    DEADLINE,
                    &PhaseAccounting::new(),
                    limit,
                )
                .await
            }
        };
        assert_eq!(run(2).await.expect("two files").files.len(), 2);
        assert!(matches!(
            run(1).await,
            Err(SnapshotError::TooManyFiles { limit: 1, .. })
        ));
    }

    /// Mutation that fails it: skipping the comparison admits both differing
    /// files.
    #[tokio::test]
    async fn a_schema_mismatch_refuses_naming_the_first_file_that_differs() {
        let store = two_files().await;
        put(store.inner(), "data/c.parquet", int_parquet_bytes(&[1])).await;
        put(
            store.inner(),
            "data/d.parquet",
            binary_parquet_bytes(&[b"x"]),
        )
        .await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::SchemaMismatch { key, first }) => {
                assert_eq!(key, "data/c.parquet");
                assert_eq!(first, "data/a.parquet");
            }
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }

    /// Two files whose columns differ only in field metadata (a
    /// `PARQUET:field_id` on one) share a schema once that metadata is
    /// cleared, as the provider clears it. Mutation that fails it: comparing
    /// `parquet_to_arrow_schema`'s output uncleared.
    #[tokio::test]
    async fn files_differing_only_in_field_metadata_share_a_schema() {
        let store = MemoryStore::new();
        let column = || Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef;
        let plain = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let with_id = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "7".to_string(),
            )])),
        ]));
        put(&store, "data/a.parquet", write(plain, vec![column()])).await;
        put(&store, "data/b.parquet", write(with_id, vec![column()])).await;
        let got = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(got.files.len(), 2);
        assert!(got.schema.field(0).metadata().is_empty());
    }

    /// The physical Parquet schema the writer emits for `bytes`, rendered by
    /// `parquet`'s own schema printer: name, physical type, logical type and
    /// repetition for every column, nothing else (key-value metadata,
    /// including `ARROW:schema`, lives outside this tree).
    fn physical_schema(bytes: &[u8]) -> String {
        let footer_end = bytes.len() - TRAILER_LEN as usize;
        let data_end = footer_end - footer_len_of(bytes) as usize;
        let metadata = parquet::file::metadata::ParquetMetaDataReader::decode_metadata(
            &bytes[data_end..footer_end],
        )
        .expect("footer");
        let mut out = Vec::new();
        parquet::schema::printer::print_schema(&mut out, metadata.file_metadata().schema());
        String::from_utf8(out).expect("printer writes utf8")
    }

    /// Two files whose physical Parquet schema elements agree exactly --
    /// proven below by rendering both with `parquet`'s own schema printer --
    /// but whose embedded `ARROW:schema` hint resolves column `b`
    /// differently: plain `Utf8` on one file, `Dictionary(Int32, Utf8)` on
    /// the other. Parquet has no physical representation for an Arrow
    /// dictionary, so the writer encodes both columns identically and only
    /// the hint carries the difference. `file_schema` resolves the hint, so
    /// the two files still refuse as a mismatch. Mutation that fails it:
    /// comparing the raw physical schema instead of `file_schema`'s resolved
    /// `Schema`, which would let the two files through as sharing a schema.
    #[tokio::test]
    async fn an_arrow_schema_hint_divergence_with_identical_physical_schema_refuses() {
        let store = MemoryStore::new();
        let plain = Arc::new(Schema::new(vec![Field::new("b", DataType::Utf8, false)]));
        let dictionary = Arc::new(Schema::new(vec![Field::new(
            "b",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let plain_bytes = write(
            plain,
            vec![Arc::new(StringArray::from(vec!["x"])) as ArrayRef],
        );
        let dictionary_bytes = write(
            dictionary,
            vec![Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["x"])) as ArrayRef],
        );
        assert_eq!(
            physical_schema(&plain_bytes),
            physical_schema(&dictionary_bytes),
            "the two files' physical Parquet schemas must agree for this test to prove anything"
        );
        put(&store, "data/a.parquet", plain_bytes).await;
        put(&store, "data/b.parquet", dictionary_bytes).await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::SchemaMismatch { key, first }) => {
                assert_eq!(key, "data/b.parquet");
                assert_eq!(first, "data/a.parquet");
            }
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }

    /// The listing reports an ETag spelled differently, a size one byte
    /// short and a CAS version that is not the store's selector; the
    /// recorded file carries what the footer read's response reported.
    /// Mutations that fail it: taking the ETag or the size from the listing's
    /// `ObjectMeta`, and recording no version.
    #[tokio::test]
    async fn the_recorded_identity_comes_from_the_footer_read_not_the_listing() {
        let store = Scripted {
            misreport_listing: true,
            ..Scripted::default()
        };
        let bytes = parquet_bytes(&[1, 2, 3], &["x", "y", "z"]);
        put(&store, "data/a.parquet", bytes.clone()).await;
        let (meta, pin) = store.inner.pin_of("data/a.parquet").await.expect("pin");
        let version = pin.version.expect("MemoryStore reports a version");

        let got = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(
            got.files,
            [ParquetFile {
                profile: PROFILE.to_string(),
                bucket: BUCKET.to_string(),
                key: b"data/a.parquet".to_vec(),
                size: bytes.len() as u64,
                etag: meta.etag.0.clone(),
                version: version.clone(),
                row_count: 3,
                footer_len: footer_len_of(&bytes),
            }]
        );
        assert_ne!(version, "listed-version");
        let listed_pin = Pin::etag(format!("listed:{}", meta.etag.0));
        assert_eq!(
            store.gets(),
            [(
                "data/a.parquet".to_string(),
                GetRange::Suffix(FOOTER_PREFETCH),
                listed_pin
            )],
            "the one footer read is a suffix read pinned on the listed (misreported) ETag; \
             its response, not the listing, supplies the recorded size"
        );
    }

    /// A listing that reports size 0 for a non-empty file snapshots it
    /// correctly: the first read is a suffix read of the real object, not a
    /// zero-length range computed from the listing. Mutation that fails it:
    /// deriving the first read's range from `Candidate`'s (now-removed)
    /// listed size instead of always reading the last [`FOOTER_PREFETCH`]
    /// bytes.
    #[tokio::test]
    async fn a_listing_that_reports_zero_size_snapshots_the_file_correctly() {
        let store = Scripted {
            lie_listed_size: Some(0),
            ..Scripted::default()
        };
        let bytes = parquet_bytes(&[1, 2, 3], &["x", "y", "z"]);
        put(&store, "data/a.parquet", bytes.clone()).await;
        let got = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(got.files.len(), 1);
        assert_eq!(got.files[0].size, bytes.len() as u64);
        assert_eq!(got.files[0].row_count, 3);
    }

    /// A listing that over-reports a file's size by more than
    /// [`FOOTER_PREFETCH`] snapshots it correctly, never refusing with a
    /// generic `Store` error: the first read is a suffix read of the real
    /// object, independent of what the listing claimed. Mutation that fails
    /// it: computing the first read's start from the listed size, which
    /// would place it past the real object's end.
    #[tokio::test]
    async fn a_listing_that_overreports_size_snapshots_the_file_correctly() {
        let bytes = parquet_bytes(&[1, 2, 3], &["x", "y", "z"]);
        let store = Scripted {
            lie_listed_size: Some(bytes.len() as u64 + FOOTER_PREFETCH + 1),
            ..Scripted::default()
        };
        put(&store, "data/a.parquet", bytes.clone()).await;
        let got = snapshot(&store, "s3://lake/data/").await.expect("snapshot");
        assert_eq!(got.files.len(), 1);
        assert_eq!(got.files[0].size, bytes.len() as u64);
        assert_eq!(got.files[0].row_count, 3);
    }

    /// `width` Int64 columns of one row, which puts a footer over
    /// [`FOOTER_PREFETCH`] bytes.
    fn wide_parquet_bytes(width: usize) -> Bytes {
        let fields: Vec<Field> = (0..width)
            .map(|i| Field::new(format!("column_{i}"), DataType::Int64, false))
            .collect();
        let columns = (0..width)
            .map(|i| Arc::new(Int64Array::from(vec![i as i64])) as ArrayRef)
            .collect();
        write(Arc::new(Schema::new(fields)), columns)
    }

    /// Mutation that fails it: decoding only the prefetched bytes when the
    /// footer is longer than them.
    #[tokio::test]
    async fn a_footer_longer_than_the_prefetch_is_read_in_two_gets() {
        let store = Scripted::default();
        let bytes = wide_parquet_bytes(1500);
        let footer_len = footer_len_of(&bytes);
        assert!(u64::from(footer_len) + TRAILER_LEN > FOOTER_PREFETCH);
        put(&store, "data/wide.parquet", bytes.clone()).await;
        let (meta, pin) = store.inner.pin_of("data/wide.parquet").await.expect("pin");
        let version = pin.version.clone().expect("MemoryStore reports a version");
        let accounting = PhaseAccounting::new();
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::unlimited());
        let got = snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
            &memory,
            DEADLINE,
            &accounting,
        )
        .await
        .expect("snapshot");
        assert_eq!(got.files[0].footer_len, footer_len);
        assert_eq!(got.files[0].row_count, 1);
        assert_eq!(got.schema.fields().len(), 1500);

        let size = bytes.len() as u64;
        let tail_start = size - FOOTER_PREFETCH;
        let footer_start = size - u64::from(footer_len) - TRAILER_LEN;
        assert_eq!(
            store.gets(),
            [
                (
                    "data/wide.parquet".to_string(),
                    GetRange::Suffix(FOOTER_PREFETCH),
                    Pin::etag(meta.etag.0.clone())
                ),
                (
                    "data/wide.parquet".to_string(),
                    GetRange::Range(footer_start, tail_start),
                    Pin {
                        etag: meta.etag.0,
                        version: Some(version),
                    }
                ),
            ],
            "the second GET selects the version the first read saw and keeps \
             If-Match on the listed ETag"
        );
        let spent = accounting.snapshot();
        let get = AccountedOp::Get.index();
        let list = AccountedOp::List.index();
        assert_eq!(spent.probe.s3_requests[get], 2);
        assert_eq!(spent.probe.s3_bytes[get], size - footer_start);
        assert_eq!(spent.resolve.s3_requests[list], 1);
        assert_eq!(spent.resolve.s3_requests[get], 0);
        assert_eq!(spent.scan, Default::default());
    }

    /// A budget that holds exactly the first footer read's reservation plus
    /// one byte less than the second read needs refuses the second read:
    /// the first GET's `FOOTER_PREFETCH`-byte reservation is still held when
    /// the second is requested, not released as soon as its own GET
    /// returns. Mutation that fails it: dropping (or releasing early) the
    /// tail reservation once the first GET completes, which would leave the
    /// whole budget free for the second read's smaller request.
    #[tokio::test]
    async fn a_long_footers_second_get_is_refused_when_the_first_reservation_is_still_held() {
        let store = Scripted::default();
        let bytes = wide_parquet_bytes(1500);
        let footer_len = footer_len_of(&bytes);
        let size = bytes.len() as u64;
        let tail_start = size - FOOTER_PREFETCH;
        let footer_start = size - u64::from(footer_len) - TRAILER_LEN;
        let before_len = tail_start - footer_start;
        put(&store, "data/wide.parquet", bytes).await;
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::new(FOOTER_PREFETCH + before_len - 1));
        let accounting = PhaseAccounting::new();
        match snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
            &memory,
            DEADLINE,
            &accounting,
        )
        .await
        {
            Err(SnapshotError::MemoryExhausted { key, requested, .. }) => {
                assert_eq!(key, "data/wide.parquet");
                assert_eq!(requested, before_len);
            }
            other => panic!("expected MemoryExhausted, got {other:?}"),
        }
    }

    /// A budget that holds exactly both GETs' reservations, with nothing
    /// left over, refuses building the concatenated footer: that buffer is
    /// a third copy of the same bytes, live alongside the two GETs' own
    /// buffers, and needs its own reservation rather than riding on theirs.
    /// Mutation that fails it: dropping the concatenation's reservation
    /// lets this budget through, resident bytes briefly twice what is
    /// reserved while all three buffers are live.
    #[tokio::test]
    async fn a_long_footers_concatenation_is_refused_when_only_the_two_gets_are_reserved() {
        let store = Scripted::default();
        let bytes = wide_parquet_bytes(1500);
        let footer_len = footer_len_of(&bytes);
        let size = bytes.len() as u64;
        let tail_start = size - FOOTER_PREFETCH;
        let footer_start = size - u64::from(footer_len) - TRAILER_LEN;
        let before_len = tail_start - footer_start;
        put(&store, "data/wide.parquet", bytes).await;
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::new(FOOTER_PREFETCH + before_len));
        let accounting = PhaseAccounting::new();
        match snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
            &memory,
            DEADLINE,
            &accounting,
        )
        .await
        {
            Err(SnapshotError::MemoryExhausted { key, requested, .. }) => {
                assert_eq!(key, "data/wide.parquet");
                assert_eq!(requested, u64::from(footer_len));
            }
            other => panic!("expected MemoryExhausted, got {other:?}"),
        }
    }

    /// Guards: the `reserve` call in `decode_footer`, at the snapshot's
    /// decode site. A budget one byte short of the footer read and the
    /// decode estimate together refuses the file as memory exhausted, asking
    /// for the estimate; a budget holding both snapshots it.
    #[tokio::test]
    async fn decoding_a_footer_reserves_its_estimate() {
        let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        let bytes = parquet_bytes(&[1], &["x"]);
        let estimate = decode_estimate(&bytes);
        put(store.inner(), "data/a.parquet", bytes).await;
        let limiter = GetLimiter::new(1).expect("permits");
        let snapshot_within = |budget: u64| {
            let memory = Arc::new(MemoryBudget::new(budget));
            let (store, limiter) = (&store, &limiter);
            async move {
                snapshot_location(
                    store,
                    &location("s3://lake/data/"),
                    limiter,
                    &memory,
                    DEADLINE,
                    &PhaseAccounting::new(),
                )
                .await
            }
        };
        match snapshot_within(FOOTER_PREFETCH + estimate - 1).await {
            Err(SnapshotError::MemoryExhausted {
                key,
                requested,
                reserved,
                ..
            }) => {
                assert_eq!(key, "data/a.parquet");
                assert_eq!(requested, estimate);
                assert_eq!(reserved, FOOTER_PREFETCH);
            }
            other => panic!("expected MemoryExhausted, got {other:?}"),
        }
        let got = snapshot_within(FOOTER_PREFETCH + estimate)
            .await
            .expect("snapshot");
        assert_eq!(got.files.len(), 1);
    }

    /// Guards: the length check in `trailer_footer_len`, which refuses the
    /// footer from the trailer the first read returns, before the footer
    /// is read.
    #[tokio::test]
    async fn a_trailer_recording_more_than_the_limit_is_corrupt() {
        let store = MemoryStore::new();
        let mut bytes = b"PAR1".to_vec();
        let recorded = u32::try_from(crate::reader::MAX_FOOTER_BYTES + 1).expect("u32");
        bytes.extend_from_slice(&recorded.to_le_bytes());
        bytes.extend_from_slice(b"PAR1");
        put(&store, "data/one.parquet", Bytes::from(bytes)).await;
        assert_corrupt(
            snapshot(&store, "s3://lake/data/").await,
            "data/one.parquet",
            "the footer is 67108865 bytes, longer than the 67108864-byte limit",
        );
    }

    /// The long footer's second GET has already read this object once, at
    /// the tail read above; a `NotFound` there means the object changed
    /// since, not that it was never there. Mutation that fails it: reporting
    /// `FileMissing` for this GET instead of remapping it to `FileChanged`
    /// (confirmed against the code before this fix: the same scenario
    /// reported `FileMissing`).
    #[tokio::test]
    async fn a_notfound_on_the_long_footers_second_get_reports_the_file_changed() {
        let inner = MemoryStore::new();
        let bytes = wide_parquet_bytes(1500);
        put(&inner, "data/wide.parquet", bytes).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                .with_key_contains("wide.parquet".to_string())
                .with_occurrence(Occurrence::Nth(2)),
        );
        let store = FaultStore::new(inner, plan);
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::FileChanged { key }) => assert_eq!(key, "data/wide.parquet"),
            other => panic!("expected FileChanged, got {other:?}"),
        }
    }

    /// The long footer's second GET reports a pin whose version disagrees
    /// with the one the first read saw: the object changed between the two
    /// reads. Mutation that fails it: comparing only `total_size`, not
    /// `pin`, between the two reads.
    #[tokio::test]
    async fn a_second_footer_read_whose_reported_pin_disagrees_with_the_first_refuses_as_changed() {
        let store = Scripted {
            lie_pin_version_on_call: Some((2, "lied-version".to_string())),
            ..Scripted::default()
        };
        let bytes = wide_parquet_bytes(1500);
        put(&store, "data/wide.parquet", bytes).await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::FileChanged { key }) => assert_eq!(key, "data/wide.parquet"),
            other => panic!("expected FileChanged, got {other:?}"),
        }
    }

    /// Mutation that fails it: reporting the expiry as any other error.
    #[tokio::test]
    async fn a_snapshot_past_its_deadline_refuses_with_the_deadline_error() {
        let store = two_files().await;
        let _gate = store.hold(Op::Get, None, Occurrence::Always);
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::unlimited());
        let deadline = Duration::from_millis(20);
        let result = snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
            &memory,
            deadline,
            &PhaseAccounting::new(),
        )
        .await;
        match result {
            Err(SnapshotError::Deadline {
                location,
                deadline: got,
            }) => {
                assert_eq!(location, "s3://lake/data/");
                assert_eq!(got, deadline);
            }
            other => panic!("expected Deadline, got {other:?}"),
        }
    }

    /// One HEAD charged to Resolve and one footer GET charged to Probe, for a
    /// key with or without the `.parquet` suffix. Mutation that fails it:
    /// listing a single-object location as a prefix.
    #[tokio::test]
    async fn a_single_object_location_reads_that_object_and_lists_nothing() {
        for key in ["data/one.parquet", "data/export"] {
            let store = Scripted::default();
            let bytes = parquet_bytes(&[1], &["x"]);
            put(&store, key, bytes.clone()).await;
            put(&store, "data/two.parquet", parquet_bytes(&[2], &["y"])).await;
            let etag = store.inner.head(key).await.expect("head").etag.0;
            let accounting = PhaseAccounting::new();
            let limiter = GetLimiter::new(4).expect("permits");
            let memory = Arc::new(MemoryBudget::unlimited());
            let got = snapshot_location(
                &store,
                &location(&format!("s3://lake/{key}")),
                &limiter,
                &memory,
                DEADLINE,
                &accounting,
            )
            .await
            .expect("snapshot");
            assert_eq!(keys(&got), [key]);
            assert!(store.lists().is_empty());
            assert_eq!(store.heads(), [key]);
            let size = bytes.len() as u64;
            assert_eq!(
                store.gets(),
                [(
                    key.to_string(),
                    GetRange::Suffix(FOOTER_PREFETCH),
                    Pin::etag(etag)
                )]
            );
            assert_eq!(
                (got.skipped_directory_markers, got.skipped_other_suffixes),
                (0, 0)
            );
            let spent = accounting.snapshot();
            assert_eq!(spent.resolve.s3_requests[AccountedOp::Head.index()], 1);
            assert_eq!(spent.resolve.s3_requests[AccountedOp::List.index()], 0);
            assert_eq!(spent.probe.s3_requests[AccountedOp::Get.index()], 1);
            assert_eq!(spent.probe.s3_bytes[AccountedOp::Get.index()], size);
        }
    }

    /// A single-object LOCATION whose key the object-store client cannot
    /// address exactly. The key must still pass the location URL parser's
    /// own, narrower refusals (no `#`, `?`, `%`, or glob character) to reach
    /// `check_file_key` at all: `<` is outside that set but still a byte
    /// `key_is_addressable` refuses. Mutation that fails it: dropping the
    /// `key_is_addressable` check for the single-object path (`MemoryStore`
    /// reads the key back, so the snapshot would otherwise succeed).
    #[tokio::test]
    async fn an_unaddressable_single_object_key_refuses_naming_it() {
        let store = Scripted::default();
        put(&store, "data/a<b.parquet", parquet_bytes(&[1], &["x"])).await;
        match snapshot(&store, "s3://lake/data/a<b.parquet").await {
            Err(SnapshotError::Unaddressable { key, .. }) => assert_eq!(key, "data/a<b.parquet"),
            other => panic!("expected Unaddressable, got {other:?}"),
        }
        assert!(store.heads().is_empty(), "refused before any HEAD");
    }

    /// A single-object LOCATION handed in with a grant that does not admit
    /// it. Mutation that fails it: dropping the `contains_key` check for the
    /// single-object path.
    #[tokio::test]
    async fn a_single_object_key_outside_the_grant_refuses_naming_it() {
        let store = two_files().await;
        let (_, key) =
            resolve_location(&[grant("data")], "s3://lake/data/a.parquet").expect("granted");
        let location = GrantedLocation {
            grant: grant("other"),
            key,
        };
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::unlimited());
        let result = snapshot_location(
            &store,
            &location,
            &limiter,
            &memory,
            DEADLINE,
            &PhaseAccounting::new(),
        )
        .await;
        match result {
            Err(SnapshotError::OutsideGrant { key, grant, .. }) => {
                assert_eq!(key, "data/a.parquet");
                assert_eq!(grant, "s3://lake/other");
            }
            other => panic!("expected OutsideGrant, got {other:?}"),
        }
    }

    /// A single-object LOCATION naming a key that does not exist: the HEAD
    /// reports `NotFound`, which refuses as `FileMissing` with no footer
    /// read attempted. Mutation that fails it: treating a HEAD's `NotFound`
    /// as any other `Store` error instead of `FileMissing`.
    #[tokio::test]
    async fn a_single_object_locations_missing_head_refuses_as_file_missing() {
        let store = Scripted::default();
        match snapshot(&store, "s3://lake/data/missing.parquet").await {
            Err(SnapshotError::FileMissing { key }) => assert_eq!(key, "data/missing.parquet"),
            other => panic!("expected FileMissing, got {other:?}"),
        }
        assert_eq!(store.heads(), ["data/missing.parquet"]);
        assert!(store.gets().is_empty(), "refused before any footer read");
    }

    /// Mutation that fails it: reserving after the GET is issued, which would
    /// leave the GET recorded in `store.gets()` despite the refusal.
    #[tokio::test]
    async fn a_footer_read_past_the_memory_budget_is_refused_before_its_get() {
        let store = Scripted::default();
        put(&store, "data/wide.parquet", wide_parquet_bytes(1500)).await;
        let limiter = GetLimiter::new(4).expect("permits");
        let memory = Arc::new(MemoryBudget::new(FOOTER_PREFETCH - 1));
        let result = snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
            &memory,
            DEADLINE,
            &PhaseAccounting::new(),
        )
        .await;
        match result {
            Err(SnapshotError::MemoryExhausted {
                key,
                requested,
                reserved,
                limit,
            }) => {
                assert_eq!(key, "data/wide.parquet");
                assert_eq!(requested, FOOTER_PREFETCH);
                assert_eq!(reserved, 0);
                assert_eq!(limit, FOOTER_PREFETCH - 1);
            }
            other => panic!("expected MemoryExhausted, got {other:?}"),
        }
        assert!(store.gets().is_empty(), "refused before any footer read");
    }

    /// A store whose `get_pinned` ignores the requested range entirely and
    /// always answers with canned bytes and a canned total size, for the one
    /// invariant a listing-backed double cannot misreport: the relationship
    /// between the first footer read's declared length and what its response
    /// actually carries.
    struct LyingGet {
        returned_len: usize,
        total_size: u64,
    }

    #[async_trait]
    impl ObjectStoreBackend for LyingGet {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            unreachable!("not exercised by a footer read")
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            unreachable!("not exercised by a footer read")
        }

        async fn get_pinned(
            &self,
            _key: &str,
            _range: GetRange,
            pin: &Pin,
        ) -> Result<PinnedRead, StoreError> {
            Ok(PinnedRead {
                outcome: GetOutcome {
                    data: Bytes::from(vec![0u8; self.returned_len]),
                    etag: Etag("e".to_string()),
                    version: Version("v".to_string()),
                    total_size: self.total_size,
                },
                pin: pin.clone(),
            })
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            Ok(ObjectMeta {
                key: key.to_string(),
                size: self.total_size,
                etag: Etag("e".to_string()),
                version: Version("v".to_string()),
                last_modified_unix_ms: 0,
            })
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            unreachable!("a single-object location lists nothing")
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            unreachable!("not exercised by a footer read")
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            unreachable!("not exercised by a footer read")
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// An endpoint whose suffix read ignores `FOOTER_PREFETCH` and returns
    /// more bytes than were requested (an endpoint that does not honor
    /// `Range`/suffix semantics at all). Mutation that fails it: dropping the
    /// `fetched != expected` guard lets `split`/`trailer_footer_len` run on
    /// however many bytes came back instead of refusing up front.
    #[tokio::test]
    async fn a_footer_read_returning_more_bytes_than_requested_refuses_as_corrupt() {
        let store = LyingGet {
            returned_len: (FOOTER_PREFETCH + 10) as usize,
            total_size: 200_000,
        };
        assert_corrupt(
            snapshot(&store, "s3://lake/data/one.parquet").await,
            "data/one.parquet",
            "returned",
        );
    }

    /// An endpoint whose suffix read returns more bytes than the same
    /// response's own reported `total_size`. Mutation that fails it:
    /// dropping the `fetched != expected` guard computes
    /// `tail_start = size - fetched` with `fetched > size`, which underflows
    /// (panics in debug, wraps in release); confirmed against the code
    /// before this fix, which panics on exactly this input.
    #[tokio::test]
    async fn a_footer_read_returning_more_bytes_than_the_reported_size_refuses_as_corrupt() {
        let store = LyingGet {
            returned_len: 150,
            total_size: 100,
        };
        assert_corrupt(
            snapshot(&store, "s3://lake/data/one.parquet").await,
            "data/one.parquet",
            "returned",
        );
    }

    /// What decoding the footer of the whole file `bytes` reserves.
    fn decode_estimate(bytes: &[u8]) -> u64 {
        let end = bytes.len() - TRAILER_LEN as usize;
        let footer = &bytes[end - footer_len_of(bytes) as usize..end];
        crate::footer_shape::check_footer_shape(footer).expect("a writer footer")
    }

    /// A budget sized for exactly one footer read and decode at a time
    /// snapshots several files when a one-permit limiter serializes the
    /// reads. Mutation that fails it: never releasing the reservation (the
    /// second file's reserve then finds the first file's bytes still held
    /// and refuses).
    #[tokio::test]
    async fn a_budget_that_fits_one_footer_snapshots_several_files_when_reads_are_serialized() {
        let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        let bytes = parquet_bytes(&[1], &["x"]);
        for key in ["data/a.parquet", "data/b.parquet", "data/c.parquet"] {
            put(store.inner(), key, bytes.clone()).await;
        }
        let limiter = GetLimiter::new(1).expect("permits");
        let memory = Arc::new(MemoryBudget::new(FOOTER_PREFETCH + decode_estimate(&bytes)));
        let got = snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
            &memory,
            DEADLINE,
            &PhaseAccounting::new(),
        )
        .await
        .expect("snapshot");
        assert_eq!(got.files.len(), 3);
    }

    /// Drive a snapshot of four files whose every GET is held, releasing the
    /// held reads each round, and return the most held at once.
    async fn peak_concurrent_reads(limiter: &GetLimiter) -> usize {
        let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        for i in 0..4 {
            put(
                store.inner(),
                &format!("data/{i}.parquet"),
                parquet_bytes(&[i], &["x"]),
            )
            .await;
        }
        let gate = store.hold(Op::Get, None, Occurrence::Always);
        let location = location("s3://lake/data/");
        let accounting = PhaseAccounting::new();
        let memory = Arc::new(MemoryBudget::unlimited());
        let mut run = std::pin::pin!(snapshot_location(
            &store,
            &location,
            limiter,
            &memory,
            DEADLINE,
            &accounting,
        ));
        let mut peak = 0;
        loop {
            for _ in 0..16 {
                if let Poll::Ready(result) = futures::poll!(run.as_mut()) {
                    assert_eq!(result.expect("snapshot").files.len(), 4);
                    return peak;
                }
                tokio::task::yield_now().await;
            }
            peak = peak.max(gate.held_count());
            for id in gate.held() {
                gate.release(id);
            }
        }
    }

    /// Mutations that fail it: `buffered(1)` (one read at a time), and
    /// dropping the permit each read takes (the permit held outside no longer
    /// narrows the snapshot).
    #[tokio::test]
    async fn footer_reads_run_concurrently_up_to_the_limiter() {
        let limiter = GetLimiter::new(2).expect("permits");
        assert_eq!(peak_concurrent_reads(&limiter).await, 2);
        let _held_elsewhere = limiter.acquire().await.expect("permit");
        assert_eq!(peak_concurrent_reads(&limiter).await, 1);
    }
}
