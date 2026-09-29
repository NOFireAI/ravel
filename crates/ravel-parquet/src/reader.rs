//! The read path for one manifest file.
//!
//! Every byte a Parquet scan reads passes through [`PinnedParquetReader::read_range`]:
//! one pinned GET on the file's store, under one `GetLimiter` permit, through
//! the process `ReadCache` keyed by the pinned identity the manifest recorded.

use std::fmt;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource_parquet::ParquetFileReaderFactory;
use futures::future::{BoxFuture, FutureExt, try_join_all};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader};
use ravel_cache::{CacheKey, PinnedIdentity, SingleFlightError, Source};
use ravel_object_store::{GetRange, ObjectStoreBackend, Pin, StoreError};
use ravel_pqtable::manifest::ParquetFile;
use ravel_query::{CacheFetchError, GetLimiter, PhaseAccounting, QueryPhase, ReadCache};
use ravel_types::TenantHash;
use ravel_types::accounting::AccountedOp;

use crate::error::ParquetReadError;
use crate::metadata_cache::{MetadataCache, MetadataKey};
use crate::store::file_path;

/// Length of the Parquet trailer: a 4-byte footer length and the `PAR1` magic.
const TRAILER_LEN: u64 = 8;

/// One manifest file and the store it is read through.
pub struct PinnedFile {
    pub file: ParquetFile,
    pub store: Arc<dyn ObjectStoreBackend>,
}

impl PinnedFile {
    /// The object key as text; manifest keys are addressable ASCII.
    pub fn key_str(&self) -> String {
        String::from_utf8_lossy(&self.file.key).into_owned()
    }

    fn pin(&self) -> Pin {
        let version = &self.file.version;
        Pin::from_store(
            self.file.etag.clone(),
            (!version.is_empty()).then(|| version.clone()),
        )
    }

    fn identity(&self) -> PinnedIdentity<'_> {
        PinnedIdentity {
            profile: &self.file.profile,
            bucket: &self.file.bucket,
            key: &self.file.key,
            etag: &self.file.etag,
            version: (!self.file.version.is_empty()).then_some(self.file.version.as_str()),
            size: self.file.size,
        }
    }
}

impl fmt::Debug for PinnedFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedFile")
            .field("profile", &self.file.profile)
            .field("bucket", &self.file.bucket)
            .field("key", &self.key_str())
            .field("size", &self.file.size)
            .field("etag", &self.file.etag)
            .field("version", &self.file.version)
            .finish_non_exhaustive()
    }
}

/// The process-wide services every Parquet read shares with the rest of the
/// query path.
#[derive(Clone)]
pub struct ReadServices {
    pub limiter: Arc<GetLimiter>,
    pub cache: ReadCache,
    pub metadata: Arc<MetadataCache>,
}

impl fmt::Debug for ReadServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cache = match &self.cache {
            ReadCache::Ram(_) => "ram",
            ReadCache::Tiered(_) => "tiered",
        };
        f.debug_struct("ReadServices")
            .field("cache", &cache)
            .field("metadata_resident_bytes", &self.metadata.resident_bytes())
            .finish_non_exhaustive()
    }
}

/// An `AsyncFileReader` over one manifest file.
///
/// The footer is one ranged read of `footer_len + 8` bytes ending at the size
/// the manifest recorded, charged to [`QueryPhase::Probe`]; every other read
/// is charged to [`QueryPhase::Scan`].
#[derive(Debug, Clone)]
pub struct PinnedParquetReader {
    tenant: TenantHash,
    file: Arc<PinnedFile>,
    services: ReadServices,
    accounting: PhaseAccounting,
}

impl PinnedParquetReader {
    pub fn new(
        tenant: TenantHash,
        file: Arc<PinnedFile>,
        services: ReadServices,
        accounting: PhaseAccounting,
    ) -> Self {
        PinnedParquetReader {
            tenant,
            file,
            services,
            accounting,
        }
    }

    fn cache_key(&self, offset: u64, len: u64) -> CacheKey {
        CacheKey::pinned(self.tenant.0, &self.file.identity(), offset, len)
    }

    fn corrupt(&self, message: String) -> ParquetReadError {
        ParquetReadError::Corrupt {
            key: self.file.key_str(),
            message,
        }
    }

    /// Read `range` of the file, from the cache or with one pinned GET,
    /// charging it to `phase`.
    pub async fn read_range(
        &self,
        range: Range<u64>,
        phase: QueryPhase,
    ) -> Result<Bytes, ParquetReadError> {
        let size = self.file.file.size;
        if range.start > range.end || range.end > size {
            return Err(self.corrupt(format!(
                "read of bytes {}..{} is outside the {size} bytes the manifest recorded",
                range.start, range.end
            )));
        }
        let len = range.end - range.start;
        if len == 0 {
            return Ok(Bytes::new());
        }
        let key = self.cache_key(range.start, len);
        let accounting = self.accounting.phase(phase);

        let hit = match &self.services.cache {
            ReadCache::Ram(cache) => cache.get(&key),
            ReadCache::Tiered(_) => None,
        };
        if let Some(bytes) = hit {
            accounting.record_cache_hit();
            accounting.add_cache_bytes(bytes.len() as u64);
            return Ok(bytes);
        }

        let fetch = {
            let file = Arc::clone(&self.file);
            let limiter = Arc::clone(&self.services.limiter);
            let accounting = accounting.clone();
            let (start, end) = (range.start, range.end);
            move || async move {
                let _permit = limiter.acquire().await.map_err(|_| {
                    StoreError::Transient("GetLimiter semaphore closed unexpectedly".into())
                })?;
                accounting.record_s3_request(AccountedOp::Get);
                let read = file
                    .store
                    .get_pinned(&file.key_str(), GetRange::Range(start, end), &file.pin())
                    .await?;
                let data = read.outcome.data;
                accounting.add_s3_bytes(AccountedOp::Get, data.len() as u64);
                if data.len() as u64 != end - start {
                    return Err(CacheFetchError::Corrupt {
                        key: file.key_str(),
                        message: format!(
                            "read of bytes {start}..{end} returned {} bytes",
                            data.len()
                        ),
                    });
                }
                Ok(data)
            }
        };
        let fetched = match &self.services.cache {
            ReadCache::Ram(cache) => cache
                .get_or_fetch(key, fetch)
                .await
                .map(|bytes| (bytes, Source::Upstream)),
            ReadCache::Tiered(cache) => cache.get_or_fetch(key, fetch).await,
        };
        match fetched {
            Ok((bytes, Source::Cache)) => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                Ok(bytes)
            }
            Ok((bytes, Source::Upstream)) => {
                accounting.record_cache_miss();
                Ok(bytes)
            }
            Err(err) => Err(self.map_fetch_error(err)),
        }
    }

    fn map_fetch_error(&self, err: SingleFlightError<CacheFetchError>) -> ParquetReadError {
        let key = self.file.key_str();
        match err {
            SingleFlightError::LeaderLost => ParquetReadError::LeaderLost { key },
            SingleFlightError::Upstream(CacheFetchError::EtagChanged { .. }) => {
                ParquetReadError::FileChanged { key }
            }
            SingleFlightError::Upstream(CacheFetchError::Corrupt { message, .. }) => {
                ParquetReadError::Corrupt { key, message }
            }
            SingleFlightError::Upstream(CacheFetchError::Store(source)) => match *source {
                StoreError::PreconditionFailed => ParquetReadError::FileChanged { key },
                StoreError::NotFound => ParquetReadError::FileMissing { key },
                _ => ParquetReadError::Store { key, source },
            },
        }
    }

    /// The decoded footer, from the metadata cache or from one Probe read. A
    /// metadata cache hit counts as a Probe cache hit and adds no cache bytes:
    /// what it serves is a decoded footer, not a byte range.
    pub async fn metadata(&self) -> Result<Arc<ParquetMetaData>, ParquetReadError> {
        let cache_key = MetadataKey::of(&self.cache_key(0, 0));
        if let Some(metadata) = self.services.metadata.get(&cache_key) {
            self.accounting.phase(QueryPhase::Probe).record_cache_hit();
            return Ok(metadata);
        }
        let size = self.file.file.size;
        let footer_len = u64::from(self.file.file.footer_len);
        let tail_len = footer_len + TRAILER_LEN;
        if tail_len > size {
            return Err(self.corrupt(format!(
                "the manifest records a {footer_len}-byte footer in a {size}-byte file"
            )));
        }
        let tail = self
            .read_range(size - tail_len..size, QueryPhase::Probe)
            .await?;
        let split = tail.len() - TRAILER_LEN as usize;
        let mut trailer = [0u8; TRAILER_LEN as usize];
        trailer.copy_from_slice(&tail[split..]);
        let footer = FooterTail::try_from(trailer)
            .map_err(|err| self.corrupt(format!("footer trailer: {err}")))?;
        if footer.is_encrypted_footer() {
            return Err(self.corrupt("the footer is encrypted".to_string()));
        }
        if footer.metadata_length() as u64 != footer_len {
            return Err(self.corrupt(format!(
                "the trailer records a {}-byte footer, the manifest {footer_len}",
                footer.metadata_length()
            )));
        }
        let metadata = ParquetMetaDataReader::decode_metadata(&tail[..split])
            .map_err(|err| self.corrupt(format!("footer: {err}")))?;
        self.check_chunks(&metadata, size - tail_len)?;
        let metadata = Arc::new(metadata);
        self.check_arrow_schema(&metadata)?;
        self.services
            .metadata
            .insert(cache_key, Arc::clone(&metadata));
        Ok(metadata)
    }

    /// Refuse a footer placing any column chunk outside the `data_end` bytes
    /// before it. The footer decoder accepts a negative offset or length, and
    /// `ColumnChunkMetaData::byte_range`, which the scan calls, panics on one.
    fn check_chunks(
        &self,
        metadata: &ParquetMetaData,
        data_end: u64,
    ) -> Result<(), ParquetReadError> {
        for (row_group, group) in metadata.row_groups().iter().enumerate() {
            for (column, chunk) in group.columns().iter().enumerate() {
                let start = chunk
                    .dictionary_page_offset()
                    .unwrap_or_else(|| chunk.data_page_offset());
                let len = chunk.compressed_size();
                let end = u64::try_from(start)
                    .ok()
                    .zip(u64::try_from(len).ok())
                    .and_then(|(start, len)| start.checked_add(len));
                if !end.is_some_and(|end| end <= data_end) {
                    return Err(self.corrupt(format!(
                        "row group {row_group} column {column} is {len} bytes at offset {start}, \
                         outside the {data_end} bytes before the footer"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Refuse a footer whose schema the Arrow reader cannot convert. Arrow's
    /// IPC decoder panics on some malformed embedded `ARROW:schema` values
    /// rather than returning an error, so the conversion the Arrow reader
    /// makes from this metadata runs here first, under `catch_unwind`. The
    /// decode depends only on the metadata, so a footer that passes here does
    /// not panic when the scan converts it again.
    fn check_arrow_schema(&self, metadata: &Arc<ParquetMetaData>) -> Result<(), ParquetReadError> {
        let converted = std::panic::catch_unwind(AssertUnwindSafe(|| {
            ArrowReaderMetadata::try_new(Arc::clone(metadata), ArrowReaderOptions::new())
                .map(|_| ())
        }));
        match converted {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(self.corrupt(format!("footer schema: {err}"))),
            Err(_) => {
                Err(self.corrupt("the footer's embedded Arrow schema is malformed".to_string()))
            }
        }
    }
}

impl AsyncFileReader for PinnedParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.read_range(range, QueryPhase::Scan)
                .await
                .map_err(ParquetReadError::into_parquet)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            try_join_all(
                ranges
                    .into_iter()
                    .map(|range| self.read_range(range, QueryPhase::Scan)),
            )
            .await
            .map_err(ParquetReadError::into_parquet)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            self.metadata()
                .await
                .map_err(ParquetReadError::into_parquet)
        }
        .boxed()
    }
}

/// Builds a [`PinnedParquetReader`] for each file of one manifest version that
/// a scan opens, by the `<table>/<version>/f/<index>` path the scan names it
/// by.
#[derive(Debug)]
pub struct PinnedReaderFactory {
    tenant: TenantHash,
    table: String,
    version: u64,
    files: Arc<[Arc<PinnedFile>]>,
    services: ReadServices,
    accounting: PhaseAccounting,
}

impl PinnedReaderFactory {
    pub fn new(
        tenant: TenantHash,
        table: String,
        version: u64,
        files: Arc<[Arc<PinnedFile>]>,
        services: ReadServices,
        accounting: PhaseAccounting,
    ) -> Self {
        PinnedReaderFactory {
            tenant,
            table,
            version,
            files,
            services,
            accounting,
        }
    }

    /// The reader for file `index` of the manifest.
    pub fn reader(&self, index: usize) -> Option<PinnedParquetReader> {
        let file = self.files.get(index)?;
        Some(PinnedParquetReader::new(
            self.tenant,
            Arc::clone(file),
            self.services.clone(),
            self.accounting.clone(),
        ))
    }

    fn index_of(&self, location: &str) -> Option<usize> {
        let prefix = file_path(&self.table, self.version, 0);
        let prefix = prefix.strip_suffix('0')?;
        let index: usize = location.strip_prefix(prefix)?.parse().ok()?;
        (file_path(&self.table, self.version, index) == location).then_some(index)
    }
}

impl ParquetFileReaderFactory for PinnedReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        partitioned_file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DfResult<Box<dyn AsyncFileReader + Send>> {
        let location = partitioned_file.object_meta.location.as_ref();
        self.index_of(location)
            .and_then(|index| self.reader(index))
            .map(|reader| Box::new(reader) as Box<dyn AsyncFileReader + Send>)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{location} is not a file of Parquet table {} version {}",
                    self.table, self.version
                ))
            })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::test_support::{
        Fixture, RecordingStore, assert_file_changed, footer_len_of, parquet_bytes, read_all,
        read_error,
    };
    use proptest::prelude::*;
    use proptest::sample::Index;
    use ravel_object_store::memory::MemoryStore;

    const KEY: &str = "lake/t/bad.parquet";

    /// Store `bytes` at [`KEY`] described by `size` and `footer_len`, and read
    /// its footer through a reader over it.
    async fn footer_of(
        bytes: Vec<u8>,
        size: u64,
        footer_len: u32,
    ) -> Result<Arc<ParquetMetaData>, ParquetReadError> {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_raw(&store, KEY, Bytes::from(bytes), size, footer_len)
            .await;
        fixture.reader(file).metadata().await
    }

    /// The error must be `Corrupt` for [`KEY`] with `needle` in its message.
    fn assert_corrupt<T: fmt::Debug>(got: Result<T, ParquetReadError>, needle: &str) {
        match got {
            Err(ParquetReadError::Corrupt { key, message }) => {
                assert_eq!(key, KEY);
                assert!(message.contains(needle), "{needle:?} not in {message:?}");
            }
            other => panic!("expected Corrupt with {needle:?}, got {other:?}"),
        }
    }

    fn valid() -> Vec<u8> {
        parquet_bytes(&[4, 5, 6], &["four", "five", "sixx"]).to_vec()
    }

    #[tokio::test]
    async fn a_read_outside_the_recorded_size_is_refused() {
        let bytes = valid();
        let size = bytes.len() as u64;
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_raw(&store, KEY, Bytes::from(bytes), size, 0)
            .await;
        let reader = fixture.reader(file);
        assert_corrupt(
            reader
                .read_range(size - 4..size + 1, QueryPhase::Scan)
                .await,
            "outside the",
        );
        #[allow(clippy::reversed_empty_ranges)]
        let backwards = 5..4;
        assert_corrupt(
            reader.read_range(backwards, QueryPhase::Scan).await,
            "outside the",
        );
    }

    /// The manifest records three bytes more than the store holds, so the
    /// footer read comes back three bytes short.
    #[tokio::test]
    async fn a_short_read_is_corrupt() {
        let bytes = valid();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        assert_corrupt(
            footer_of(bytes, size + 3, footer_len).await,
            &format!("returned {} bytes", u64::from(footer_len) + TRAILER_LEN - 3),
        );
    }

    #[tokio::test]
    async fn a_footer_longer_than_the_file_is_corrupt() {
        let bytes = valid();
        let size = bytes.len() as u64;
        let footer_len = u32::try_from(size).expect("small file");
        assert_corrupt(
            footer_of(bytes, size, footer_len).await,
            &format!("records a {footer_len}-byte footer in a {size}-byte file"),
        );
    }

    #[tokio::test]
    async fn a_trailer_without_the_magic_is_corrupt() {
        let mut bytes = valid();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        let end = bytes.len();
        bytes[end - 4..].copy_from_slice(b"RAV1");
        assert_corrupt(footer_of(bytes, size, footer_len).await, "footer trailer: ");
    }

    #[tokio::test]
    async fn an_encrypted_footer_is_refused() {
        let mut bytes = valid();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        let end = bytes.len();
        bytes[end - 4..].copy_from_slice(b"PARE");
        assert_corrupt(
            footer_of(bytes, size, footer_len).await,
            "the footer is encrypted",
        );
    }

    #[tokio::test]
    async fn a_footer_length_the_trailer_disagrees_with_is_corrupt() {
        let bytes = valid();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        assert_corrupt(
            footer_of(bytes, size, footer_len - 1).await,
            &format!(
                "the trailer records a {footer_len}-byte footer, the manifest {}",
                footer_len - 1
            ),
        );
    }

    /// The trailer is intact and agrees with the manifest; the metadata it
    /// frames is not Thrift.
    #[tokio::test]
    async fn a_footer_that_does_not_decode_is_corrupt() {
        let mut bytes = valid();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        let end = bytes.len() - TRAILER_LEN as usize;
        bytes[end - footer_len as usize..end].fill(0xff);
        assert_corrupt(footer_of(bytes, size, footer_len).await, "footer: ");
    }

    /// A one-bit flip in the footer that still decodes but gives a column
    /// chunk a negative length: the reader refuses it rather than handing it
    /// to the scan, which panics on it, and the scan fails typed.
    #[tokio::test]
    async fn a_footer_placing_a_chunk_outside_the_file_is_corrupt() {
        let original = valid();
        let (size, footer_len) = (original.len() as u64, footer_len_of(&original));
        let end = original.len() - TRAILER_LEN as usize;
        let start = end - footer_len as usize;
        let negative = |bytes: &[u8]| {
            ParquetMetaDataReader::decode_metadata(&bytes[start..end])
                .ok()
                .is_some_and(|metadata| {
                    metadata
                        .row_groups()
                        .iter()
                        .flat_map(|group| group.columns())
                        .any(|chunk| chunk.compressed_size() < 0)
                })
        };
        let flipped = (start..end)
            .flat_map(|at| (0..8).map(move |bit| (at, 1u8 << bit)))
            .map(|(at, mask)| {
                let mut bytes = original.clone();
                bytes[at] ^= mask;
                bytes
            })
            .find(|bytes| negative(bytes))
            .expect("a one-bit flip that makes a chunk length negative");

        assert_corrupt(
            footer_of(flipped.clone(), size, footer_len).await,
            "outside the",
        );
        let err = tokio::task::spawn_blocking(move || scan_with_second_file(flipped))
            .await
            .expect("the scan does not panic")
            .expect_err("the scan fails");
        assert!(
            matches!(read_error(&err), Some(ParquetReadError::Corrupt { key, .. }) if key == KEY),
            "{err}"
        );
    }

    /// One base64 character of the embedded `ARROW:schema` replaced so that
    /// Arrow's IPC decoder panics on it: the reader refuses the footer, and the
    /// scan fails typed instead of panicking.
    #[tokio::test]
    async fn a_footer_whose_arrow_schema_panics_arrow_is_corrupt() {
        const BASE64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let original = valid();
        let (size, footer_len) = (original.len() as u64, footer_len_of(&original));
        let end = original.len() - TRAILER_LEN as usize;
        let start = end - footer_len as usize;
        let decode = |bytes: &[u8]| ParquetMetaDataReader::decode_metadata(&bytes[start..end]);
        let schema = decode(&original)
            .expect("footer")
            .file_metadata()
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|kv| kv.key == "ARROW:schema"))
            .and_then(|kv| kv.value.clone())
            .expect("ArrowWriter embeds its schema");
        let at = original[start..end]
            .windows(schema.len())
            .position(|window| window == schema.as_bytes())
            .expect("the schema's bytes are in the footer")
            + start;
        let panics = |bytes: &[u8]| {
            let Ok(metadata) = decode(bytes) else {
                return false;
            };
            let metadata = Arc::new(metadata);
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                ArrowReaderMetadata::try_new(metadata, ArrowReaderOptions::new()).map(|_| ())
            }))
            .is_err()
        };
        let broken = (at..at + schema.len())
            .flat_map(|at| BASE64.iter().map(move |&c| (at, c)))
            .filter(|&(at, c)| original[at] != c)
            .map(|(at, c)| {
                let mut bytes = original.clone();
                bytes[at] = c;
                bytes
            })
            .find(|bytes| panics(bytes))
            .expect("a one-character change that panics Arrow's schema decoder");

        assert_corrupt(
            footer_of(broken.clone(), size, footer_len).await,
            "embedded Arrow schema is malformed",
        );
        let err = tokio::task::spawn_blocking(move || scan_with_second_file(broken))
            .await
            .expect("the scan does not panic")
            .expect_err("the scan fails");
        assert!(
            matches!(read_error(&err), Some(ParquetReadError::Corrupt { key, .. }) if key == KEY),
            "{err}"
        );
    }

    /// A byte-level change to a stored file.
    #[derive(Debug, Clone)]
    enum Mutation {
        /// Keep the first `len` bytes, `len` below the original length.
        Truncate(Index),
        /// XOR one byte of the 8-byte trailer with a nonzero mask.
        FlipTrailer(Index, u8),
        /// XOR any byte with a nonzero mask.
        Flip(Index, u8),
        /// Remove `remove` bytes at the offset and insert `insert` there.
        Splice(Index, usize, Vec<u8>),
    }

    impl Mutation {
        fn apply(&self, original: &[u8]) -> Vec<u8> {
            let mut bytes = original.to_vec();
            match self {
                Mutation::Truncate(len) => bytes.truncate(len.index(original.len())),
                Mutation::FlipTrailer(at, mask) => {
                    let at = original.len() - TRAILER_LEN as usize + at.index(8);
                    bytes[at] ^= mask;
                }
                Mutation::Flip(at, mask) => bytes[at.index(original.len())] ^= mask,
                Mutation::Splice(at, remove, insert) => {
                    let at = at.index(original.len());
                    let end = (at + remove).min(original.len());
                    bytes.splice(at..end, insert.iter().copied());
                }
            }
            bytes
        }
    }

    fn detected_mutation() -> impl Strategy<Value = Mutation> {
        prop_oneof![
            any::<Index>().prop_map(Mutation::Truncate),
            (any::<Index>(), 1..=u8::MAX).prop_map(|(at, mask)| Mutation::FlipTrailer(at, mask)),
        ]
    }

    fn any_mutation() -> impl Strategy<Value = Mutation> {
        prop_oneof![
            detected_mutation(),
            (any::<Index>(), 1..=u8::MAX).prop_map(|(at, mask)| Mutation::Flip(at, mask)),
            (
                any::<Index>(),
                0..16_usize,
                proptest::collection::vec(any::<u8>(), 0..16)
            )
                .prop_map(|(at, remove, insert)| Mutation::Splice(at, remove, insert)),
        ]
    }

    /// Scan a two-file table whose second file is `mutated`, stored as is and
    /// described by its own length and by the footer length its trailer
    /// holds, so the reader gets past the manifest checks to the parser. The
    /// first file is intact and supplies the schema.
    fn scan_with_second_file(mutated: Vec<u8>) -> datafusion::error::Result<String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let store = Arc::new(MemoryStore::new());
            let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
            let good = fixture
                .put_file(
                    &store,
                    "lake/t/good.parquet",
                    parquet_bytes(&[1, 2, 3], &["one", "two", "six"]),
                    false,
                )
                .await;
            let size = mutated.len() as u64;
            let footer_len = if mutated.len() >= TRAILER_LEN as usize {
                footer_len_of(&mutated)
            } else {
                0
            };
            let bad = fixture
                .put_raw(&store, KEY, Bytes::from(mutated), size, footer_len)
                .await;
            let table = fixture.provider("t", 1, vec![good, bad], false).await;
            let ctx = fixture.session(&[("t", table)]);
            read_all(&ctx, "t", &["a", "b"]).await
        })
    }

    proptest! {
        /// A truncated file, or one whose trailer changed, fails the scan
        /// with a typed `Corrupt` error naming it, and returns no rows.
        #[test]
        fn a_truncated_file_or_a_changed_trailer_fails_the_scan_typed(
            mutation in detected_mutation(),
        ) {
            let got = scan_with_second_file(mutation.apply(&valid()));
            let err = got.expect_err("a corrupt file must fail the scan");
            let found = read_error(&err);
            prop_assert!(
                matches!(&found, Some(ParquetReadError::Corrupt { key, .. }) if key == KEY),
                "{err}"
            );
        }

        /// Any byte change, anywhere in the file, ends the scan with an error
        /// or with rows, never a panic. The reader cannot refuse every change:
        /// nothing it checks covers the page values, so a flipped value byte
        /// decodes as a different value. When the error carries a
        /// `ParquetReadError`, it is `Corrupt` and names the changed file.
        #[test]
        fn any_byte_change_is_an_error_or_rows_never_a_panic(
            mutation in any_mutation(),
        ) {
            let original = valid();
            let mutated = mutation.apply(&original);
            prop_assume!(mutated != original);
            if let Err(err) = scan_with_second_file(mutated)
                && let Some(found) = read_error(&err)
            {
                prop_assert!(
                    matches!(&found, ParquetReadError::Corrupt { key, .. } if key == KEY),
                    "{err}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_file_overwritten_after_create_fails_the_scan_and_is_never_mixed_with_cached_pages() {
        let store = Arc::new(MemoryStore::new());
        let v1 = parquet_bytes(&[1, 2, 3], &["one", "two", "six"]);
        let v2 = parquet_bytes(&[7, 8, 9], &["ten", "elf", "zwo"]);
        assert_eq!(v1.len(), v2.len(), "the overwrite must keep the size");

        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let first = fixture
            .put_file(&store, "lake/t/a.parquet", v1, false)
            .await;
        let table = fixture.provider("t", 1, vec![first.clone()], false).await;
        let ctx = fixture.session(&[("t", table)]);

        let warm = read_all(&ctx, "t", &["a"]).await;
        assert_eq!(warm.expect("v1 reads"), "1,2,3");

        let second = fixture
            .put_file(&store, "lake/t/a.parquet", v2, false)
            .await;
        assert_ne!(first.etag, second.etag);
        assert_eq!(first.size, second.size);

        let err = read_all(&ctx, "t", &["a", "b"])
            .await
            .expect_err("a file overwritten after create must fail the scan");
        assert_file_changed(&err, "lake/t/a.parquet");

        let replaced = fixture.provider("t", 2, vec![second], false).await;
        let ctx = fixture.session(&[("t", replaced)]);
        let rows = read_all(&ctx, "t", &["a", "b"]).await;
        assert_eq!(rows.expect("v2 reads"), "7|ten,8|elf,9|zwo");
    }

    #[tokio::test]
    async fn a_deleted_file_is_reported_missing() {
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(
                &store,
                "lake/t/gone.parquet",
                parquet_bytes(&[1], &["x"]),
                true,
            )
            .await;
        let reader = fixture.reader(file);
        store
            .delete("lake/t/gone.parquet")
            .await
            .expect("delete succeeds");
        let err = reader
            .metadata()
            .await
            .expect_err("a deleted file cannot be read");
        assert!(
            matches!(&err, ParquetReadError::FileMissing { key } if key == "lake/t/gone.parquet"),
            "{err}"
        );
        assert!(err.to_string().contains("CREATE OR REPLACE"), "{err}");
    }

    #[tokio::test]
    async fn the_footer_is_one_explicit_range_ending_at_the_pinned_size() {
        let memory = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&memory), false));
        assert!(!recording.capabilities().suffix_range);
        let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(
                &memory,
                "lake/t/a.parquet",
                parquet_bytes(&[4, 5], &["p", "q"]),
                true,
            )
            .await;
        let (size, footer_len) = (file.size, u64::from(file.footer_len));

        let table = fixture.provider("t", 1, vec![file], false).await;
        let ctx = fixture.session(&[("t", table)]);
        let rows = read_all(&ctx, "t", &["a", "b"]).await;
        assert_eq!(rows.expect("rows"), "4|p,5|q");

        let ranges = recording.ranges();
        assert!(
            ranges
                .iter()
                .all(|range| !matches!(range, GetRange::Suffix(_))),
            "{ranges:?}"
        );
        let footer = GetRange::Range(size - footer_len - TRAILER_LEN, size);
        assert_eq!(
            ranges.iter().filter(|range| **range == footer).count(),
            1,
            "{ranges:?}"
        );
        assert_eq!(ranges.first(), Some(&footer), "{ranges:?}");
    }
}
