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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Mutex, PoisonError};
    use std::task::Poll;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
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
        snapshot_location(
            store,
            &location(url),
            &limiter,
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
        repeat_page_boundary: bool,
        lists: Mutex<Vec<String>>,
        heads: Mutex<Vec<String>>,
        gets: Mutex<Vec<(String, GetRange)>>,
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

        fn gets(&self) -> Vec<(String, GetRange)> {
            self.gets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        fn synthetic_page(count: usize, page: Option<PageToken>) -> ListPage {
            let start = page.map_or(0, |PageToken(at)| at.parse().expect("token"));
            let end = count.min(start + 1000);
            let objects = (start..end)
                .map(|i| ObjectMeta {
                    key: format!("data/{i:06}.parquet"),
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
            self.gets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((key.to_string(), range));
            let mut pin = pin.clone();
            if self.misreport_listing {
                pin.etag = pin.etag.trim_start_matches("listed:").to_string();
            }
            self.inner.get_pinned(key, range, &pin).await
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
            if let Some(count) = self.synthetic {
                return Ok(Self::synthetic_page(count, page));
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
            Ok(listed)
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
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

    /// Mutation that fails it: dropping the zero-size check reads the empty
    /// object with a zero-length range, which the store refuses as a
    /// `Store` error rather than `EmptyFile`.
    #[tokio::test]
    async fn a_zero_byte_file_refuses_naming_it() {
        let store = two_files().await;
        put(store.inner(), "data/c.parquet", Bytes::new()).await;
        match snapshot(&store, "s3://lake/data/").await {
            Err(SnapshotError::EmptyFile { key }) => assert_eq!(key, "data/c.parquet"),
            other => panic!("expected EmptyFile, got {other:?}"),
        }
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

    /// A store may list a key again at the start of the next page. Mutation
    /// that fails it: dropping the `seen` check lists the repeated file twice
    /// and counts the repeated marker twice.
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
        let result = snapshot_location(
            &store,
            &location,
            &limiter,
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

    /// Mutation that fails it: `MAX_TABLE_FILES` raised by one, or the limit
    /// check moved after the push (`len > limit`), admits 100,001 files and
    /// goes on to read their footers.
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
        let run = |limit| {
            let (store, limiter) = (&store, &limiter);
            async move {
                snapshot_with_limit(
                    store,
                    &location("s3://lake/data/"),
                    limiter,
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

    /// Mutation that fails it: comparing each file with the one before it
    /// rather than with the first names `data/d.parquet` against
    /// `data/c.parquet`; skipping the comparison admits both.
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

    /// The listing reports an ETag spelled differently, a size one byte
    /// short and a CAS version that is not the store's selector; the
    /// recorded file carries what the footer read's response reported.
    /// Mutations that fail it: taking the ETag, the version or the size from
    /// the listing's `ObjectMeta`.
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
        let size = bytes.len() as u64;
        assert_eq!(
            store.gets(),
            [
                ("data/a.parquet".to_string(), GetRange::Range(0, size - 1)),
                ("data/a.parquet".to_string(), GetRange::Range(0, size)),
            ],
            "the misplaced first read is redone at the size the response reported"
        );
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
        let accounting = PhaseAccounting::new();
        let limiter = GetLimiter::new(4).expect("permits");
        let got = snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
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
                    GetRange::Range(tail_start, size)
                ),
                (
                    "data/wide.parquet".to_string(),
                    GetRange::Range(footer_start, tail_start)
                ),
            ]
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

    /// Mutation that fails it: dropping the `tokio::time::timeout` around the
    /// snapshot leaves it waiting on the held read forever.
    #[tokio::test]
    async fn a_snapshot_past_its_deadline_refuses_with_the_deadline_error() {
        let store = two_files().await;
        let _gate = store.hold(Op::Get, None, Occurrence::Always);
        let limiter = GetLimiter::new(4).expect("permits");
        let deadline = Duration::from_millis(20);
        let result = snapshot_location(
            &store,
            &location("s3://lake/data/"),
            &limiter,
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

    /// Mutation that fails it: listing a single-object location as a prefix.
    #[tokio::test]
    async fn a_single_object_location_reads_that_object_and_lists_nothing() {
        let store = Scripted::default();
        put(&store, "data/one.parquet", parquet_bytes(&[1], &["x"])).await;
        put(&store, "data/two.parquet", parquet_bytes(&[2], &["y"])).await;
        let got = snapshot(&store, "s3://lake/data/one.parquet")
            .await
            .expect("snapshot");
        assert_eq!(keys(&got), ["data/one.parquet"]);
        assert!(store.lists().is_empty());
        assert_eq!(store.heads(), ["data/one.parquet"]);
        assert!(
            store
                .gets()
                .iter()
                .all(|(key, _)| key == "data/one.parquet"),
            "{:?}",
            store.gets()
        );
        assert_eq!(
            (got.skipped_directory_markers, got.skipped_other_suffixes),
            (0, 0)
        );
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
        let mut run = std::pin::pin!(snapshot_location(
            &store,
            &location,
            limiter,
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
