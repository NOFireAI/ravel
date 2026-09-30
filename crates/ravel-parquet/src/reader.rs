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
use parquet::file::metadata::{
    FooterTail, ParquetMetaData, ParquetMetaDataBuilder, ParquetMetaDataReader,
};
use ravel_cache::{CacheKey, PinnedIdentity, SingleFlightError, Source};
use ravel_object_store::{GetRange, ObjectStoreBackend, Pin, StoreError};
use ravel_pqtable::manifest::ParquetFile;
use ravel_query::{CacheFetchError, GetLimiter, PhaseAccounting, QueryPhase, ReadCache};
use ravel_types::TenantHash;
use ravel_types::accounting::AccountedOp;

use crate::error::ParquetReadError;
use crate::metadata_cache::{CachedFooter, MetadataCache, MetadataKey};
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
/// query path. `cache` is `None` when the process runs with its read cache
/// off; every range is then one pinned GET.
#[derive(Clone)]
pub struct ReadServices {
    pub limiter: Arc<GetLimiter>,
    pub cache: Option<ReadCache>,
    pub metadata: Arc<MetadataCache>,
}

impl fmt::Debug for ReadServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cache = match &self.cache {
            Some(ReadCache::Ram(_)) => "ram",
            Some(ReadCache::Tiered(_)) => "tiered",
            None => "none",
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
///
/// The reader does not use the file's page index. The footer it hands out
/// carries no column index or offset index, whatever options the caller
/// passes, so the scan decodes every column chunk it reads by page header.
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
            Some(ReadCache::Ram(cache)) => cache.get(&key),
            Some(ReadCache::Tiered(_)) | None => None,
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
            Some(ReadCache::Ram(cache)) => cache
                .get_or_fetch(key, fetch)
                .await
                .map(|bytes| (bytes, Source::Upstream)),
            Some(ReadCache::Tiered(cache)) => cache.get_or_fetch(key, fetch).await,
            None => fetch()
                .await
                .map(|bytes| (bytes, Source::Upstream))
                .map_err(SingleFlightError::Upstream),
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

    /// The decoded footer, without its page index, from the metadata cache
    /// or from a Probe read.
    ///
    /// A metadata cache hit counts as a Probe cache hit of the entry's charged
    /// size, the convention the catalog caches use. The key is the pinned
    /// identity and the footer length the manifest recorded, the two inputs
    /// the footer is decoded or refused from. A footer refused as `Corrupt` is
    /// cached as refused under that key, so a later read fails the same way
    /// without reading it again. A read that failed is not cached, whatever
    /// it failed on: a store error or a read that came back short.
    pub async fn metadata(&self) -> Result<Arc<ParquetMetaData>, ParquetReadError> {
        let cache_key = MetadataKey::of(&self.cache_key(0, 0), self.file.file.footer_len);
        if let Some((footer, bytes)) = self.services.metadata.get(&cache_key) {
            let probe = self.accounting.phase(QueryPhase::Probe);
            probe.record_cache_hit();
            probe.add_cache_bytes(bytes);
            return match footer {
                CachedFooter::Decoded(metadata) => Ok(metadata),
                CachedFooter::Refused(message) => Err(self.corrupt(message.to_string())),
            };
        }
        match self.read_footer().await {
            Ok(metadata) => {
                self.services
                    .metadata
                    .insert(cache_key, Arc::clone(&metadata));
                Ok(metadata)
            }
            Err(FooterError::Refused(message)) => {
                self.services.metadata.insert_refused(cache_key, &message);
                Err(self.corrupt(message))
            }
            Err(FooterError::Read(err)) => Err(err),
        }
    }

    /// Read, decode and check the footer. The metadata it returns carries no
    /// page index.
    async fn read_footer(&self) -> Result<Arc<ParquetMetaData>, FooterError> {
        let size = self.file.file.size;
        let footer_len = u64::from(self.file.file.footer_len);
        let tail_len = footer_len + TRAILER_LEN;
        if tail_len > size {
            return Err(FooterError::Refused(format!(
                "the manifest records a {footer_len}-byte footer in a {size}-byte file"
            )));
        }
        let tail = self
            .read_range(size - tail_len..size, QueryPhase::Probe)
            .await
            .map_err(FooterError::Read)?;
        let split = tail.len() - TRAILER_LEN as usize;
        let mut trailer = [0u8; TRAILER_LEN as usize];
        trailer.copy_from_slice(&tail[split..]);
        let footer = FooterTail::try_from(trailer)
            .map_err(|err| FooterError::Refused(format!("footer trailer: {err}")))?;
        if footer.is_encrypted_footer() {
            return Err(FooterError::Refused("the footer is encrypted".to_string()));
        }
        if footer.metadata_length() as u64 != footer_len {
            return Err(FooterError::Refused(format!(
                "the trailer records a {}-byte footer, the manifest {footer_len}",
                footer.metadata_length()
            )));
        }
        let metadata = ParquetMetaDataReader::decode_metadata(&tail[..split])
            .map_err(|err| FooterError::Refused(format!("footer: {err}")))?;
        check_chunks(&metadata, size - tail_len).map_err(FooterError::Refused)?;
        let metadata = Arc::new(without_page_index(metadata));
        check_arrow_schema(&metadata).map_err(FooterError::Refused)?;
        Ok(metadata)
    }
}

/// Why [`PinnedParquetReader::read_footer`] could not hand a footer out.
enum FooterError {
    /// A read failed. The store may answer the next one, so it is not cached.
    Read(ParquetReadError),
    /// The pinned bytes were read and refused; a later read of the same bytes
    /// would be refused the same way.
    Refused(String),
}

/// `metadata` with no column index and no offset index. The parquet crate
/// reads each data page from the byte range its offset index location names
/// and never compares that with the page's own header, so a corrupt location
/// decodes the wrong rows without an error; without an offset index it walks
/// the column chunk by page header.
fn without_page_index(metadata: ParquetMetaData) -> ParquetMetaData {
    if metadata.column_index().is_none() && metadata.offset_index().is_none() {
        return metadata;
    }
    ParquetMetaDataBuilder::new_from_metadata(metadata)
        .set_column_index(None)
        .set_offset_index(None)
        .build()
}

/// Refuse a footer placing any column chunk outside the `data_end` bytes
/// before it. The footer decoder accepts a negative offset or length, and
/// `ColumnChunkMetaData::byte_range`, which the scan calls, panics on one.
fn check_chunks(metadata: &ParquetMetaData, data_end: u64) -> Result<(), String> {
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
                return Err(format!(
                    "row group {row_group} column {column} is {len} bytes at offset {start}, \
                     outside the {data_end} bytes before the footer"
                ));
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
fn check_arrow_schema(metadata: &Arc<ParquetMetaData>) -> Result<(), String> {
    let converted = std::panic::catch_unwind(AssertUnwindSafe(|| {
        ArrowReaderMetadata::try_new(Arc::clone(metadata), ArrowReaderOptions::new()).map(|_| ())
    }));
    match converted {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(format!("footer schema: {err}")),
        Err(_) => Err("the footer's embedded Arrow schema is malformed".to_string()),
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

    /// The footer without its page index, whatever page index policy
    /// `_options` asks for.
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
#[derive(Debug, Clone)]
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
            .map(|reader| {
                crate::boundary::record_opened(reader.file.key_str());
                Box::new(reader) as Box<dyn AsyncFileReader + Send>
            })
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
        read_error, read_where, render_rows,
    };
    use datafusion::logical_expr::{Expr, JoinType, col, ident, lit};
    use parquet::file::metadata::PageIndexPolicy;
    use parquet::file::page_index::offset_index::PageLocation;
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
        /// XOR one byte of the page index (column and offset indexes) with a
        /// nonzero mask.
        FlipPageIndex(Index, u8),
    }

    /// The bytes of `bytes`' page index: from the first column index to the
    /// end of the last offset index.
    fn page_index_region(bytes: &[u8]) -> Range<usize> {
        let end = bytes.len() - TRAILER_LEN as usize;
        let metadata = ParquetMetaDataReader::decode_metadata(
            &bytes[end - footer_len_of(bytes) as usize..end],
        )
        .expect("footer");
        let chunks: Vec<_> = metadata
            .row_groups()
            .iter()
            .flat_map(|group| group.columns())
            .collect();
        let start = chunks
            .iter()
            .filter_map(|chunk| chunk.column_index_offset())
            .min()
            .expect("ArrowWriter writes a column index");
        let end = chunks
            .iter()
            .filter_map(|chunk| {
                Some(chunk.offset_index_offset()? + i64::from(chunk.offset_index_length()?))
            })
            .max()
            .expect("ArrowWriter writes an offset index");
        start as usize..end as usize
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
                Mutation::FlipPageIndex(at, mask) => {
                    let region = page_index_region(original);
                    bytes[region.start + at.index(region.len())] ^= mask;
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
        scan_second_file_where(mutated, None)
    }

    /// `a > 4`: the intact file's row group (`a` in 1..=3) is pruned by its
    /// statistics, and the mutated file's (4..=6) is only partly matched, the
    /// case in which DataFusion would prune its pages with a page index.
    fn page_pruning_filter() -> Expr {
        ident("a").gt(lit(4_i64))
    }

    fn scan_second_file_where(
        mutated: Vec<u8>,
        filter: Option<Expr>,
    ) -> datafusion::error::Result<String> {
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
            read_where(&ctx, "t", &["a", "b"], filter).await
        })
    }

    /// The offset index of `bytes`, decoded the way the parquet crate decodes
    /// it for a scan, or `None` when it does not decode.
    fn page_locations_of(
        bytes: &[u8],
    ) -> Option<
        Vec<(
            i64,
            i64,
            Vec<parquet::file::page_index::offset_index::PageLocation>,
        )>,
    > {
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&Bytes::copy_from_slice(bytes))
            .ok()?;
        let offset_index = metadata.offset_index()?;
        let mut out = Vec::new();
        for (group, columns) in metadata.row_groups().iter().zip(offset_index) {
            for (chunk, index) in group.columns().iter().zip(columns) {
                let (start, len) = chunk.byte_range();
                out.push((
                    start as i64,
                    (start + len) as i64,
                    index.page_locations().clone(),
                ));
            }
        }
        Some(out)
    }

    /// [`valid`] with one bit of its offset index flipped, found by search, so
    /// that it still decodes but places a page outside its column chunk or
    /// gives the first page a row number other than 0. The footer is
    /// untouched.
    fn page_index_out_of_its_chunk() -> Vec<u8> {
        let original = valid();
        let metadata = ParquetMetaDataReader::decode_metadata(
            &original[original.len() - TRAILER_LEN as usize - footer_len_of(&original) as usize
                ..original.len() - TRAILER_LEN as usize],
        )
        .expect("footer");
        let regions: Vec<(usize, usize)> = metadata
            .row_groups()
            .iter()
            .flat_map(|group| group.columns())
            .filter_map(|chunk| {
                Some((
                    usize::try_from(chunk.offset_index_offset()?).ok()?,
                    usize::try_from(chunk.offset_index_length()?).ok()?,
                ))
            })
            .collect();
        assert!(!regions.is_empty(), "ArrowWriter writes an offset index");
        let malformed = |bytes: &[u8]| {
            page_locations_of(bytes).is_some_and(|chunks| {
                chunks.iter().any(|(start, end, pages)| {
                    pages.iter().any(|page| {
                        page.offset < *start
                            || page.offset + i64::from(page.compressed_page_size) > *end
                    }) || pages.first().is_some_and(|page| page.first_row_index != 0)
                })
            })
        };
        regions
            .iter()
            .flat_map(|&(at, len)| at..at + len)
            .flat_map(|at| (0..8).map(move |bit| (at, 1u8 << bit)))
            .map(|(at, mask)| {
                let mut bytes = original.clone();
                bytes[at] ^= mask;
                bytes
            })
            .find(|bytes| malformed(bytes))
            .expect("a one-bit flip that moves a page out of its chunk")
    }

    /// The parquet crate panics on some page locations outside their chunk
    /// when it prunes pages. The reader never hands the scan a page index, so
    /// an unfiltered, a filtered and another unfiltered scan over one metadata
    /// cache each return the file's exact rows, decoded by page header.
    #[tokio::test]
    async fn a_page_index_out_of_its_chunk_is_never_read_whatever_ran_before() {
        let flipped = page_index_out_of_its_chunk();
        let size = flipped.len() as u64;
        let footer_len = footer_len_of(&flipped);

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
        let bad = fixture
            .put_raw(&store, KEY, Bytes::from(flipped), size, footer_len)
            .await;
        let table = fixture.provider("t", 1, vec![good, bad], false).await;
        let ctx = fixture.session(&[("t", table)]);
        let outcome = |filter: Option<Expr>| {
            let ctx = &ctx;
            async move {
                match read_where(ctx, "t", &["a", "b"], filter).await {
                    Ok(rows) => format!("rows {rows}"),
                    Err(err) => format!("{err}"),
                }
            }
        };
        let all = "rows 1|one,2|two,3|six,4|four,5|five,6|sixx";
        assert_eq!(outcome(None).await, all);
        assert_eq!(
            outcome(Some(page_pruning_filter())).await,
            "rows 5|five,6|sixx"
        );
        assert_eq!(outcome(None).await, all);
    }

    /// One `a: Int64` column holding 1..=4, plain and uncompressed, in two
    /// data pages of two values each, with no dictionary page and no
    /// statistics, so the two pages are the same size.
    fn two_equal_pages() -> Vec<u8> {
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use parquet::file::properties::{EnabledStatistics, WriterProperties};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])) as ArrayRef],
        )
        .expect("batch");
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .build();
        let mut bytes = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        bytes
    }

    /// Append `value` to `out` as a compact-protocol zigzag varint.
    fn zigzag_varint(value: i64, out: &mut Vec<u8>) {
        let mut rest = ((value << 1) ^ (value >> 63)) as u64;
        while rest >= 0x80 {
            out.push((rest as u8 & 0x7f) | 0x80);
            rest >>= 7;
        }
        out.push(rest as u8);
    }

    /// [`two_equal_pages`] with the offset index's second page location
    /// moved onto the first page's offset, its size and first row unchanged.
    /// Every other byte, the footer included, is untouched.
    fn second_page_on_the_first() -> Vec<u8> {
        let mut bytes = two_equal_pages();
        let chunks = page_locations_of(&bytes).expect("an offset index");
        let [(_, _, pages)] = chunks.as_slice() else {
            panic!("one column chunk, got {}", chunks.len());
        };
        let [first, second] = pages.as_slice() else {
            panic!("two pages, got {pages:?}");
        };
        assert_eq!(first.compressed_page_size, second.compressed_page_size);
        assert_eq!(second.first_row_index, 2);
        // PageLocation's first two fields, as the offset index encodes them.
        let encode = |offset: i64| {
            let mut out = vec![0x16];
            zigzag_varint(offset, &mut out);
            out.push(0x15);
            zigzag_varint(i64::from(second.compressed_page_size), &mut out);
            out
        };
        let (was, now) = (encode(second.offset), encode(first.offset));
        assert_eq!(was.len(), now.len());
        let at: Vec<usize> = (0..bytes.len() - was.len())
            .filter(|&at| bytes[at..at + was.len()] == was[..])
            .collect();
        let [at] = at.as_slice() else {
            panic!("one encoding of the second location, found {at:?}");
        };
        bytes[*at..*at + now.len()].copy_from_slice(&now);
        let moved = page_locations_of(&bytes).expect("still decodes");
        assert_eq!(moved[0].2[1].offset, first.offset);
        assert_eq!(moved[0].2[1].first_row_index, 2);
        bytes
    }

    /// Two page locations naming the same bytes: the parquet crate reads every
    /// data page from its location, so with the offset index it would decode
    /// the first page twice and return 1, 2, 1, 2. The scan decodes by page
    /// header instead, and a scan beside an intact copy returns both files'
    /// real rows.
    #[tokio::test]
    async fn a_page_location_overlapping_the_one_before_changes_no_row() {
        let bytes = second_page_on_the_first();
        let got = scan_beside_intact(two_equal_pages(), bytes, None).await;
        assert_eq!(got.expect("rows"), "1,1,2,2,3,3,4,4");
    }

    /// Scan `a` over a table of two files, `intact` and `corrupt` at [`KEY`],
    /// the second described by its own length and trailer, with `filter`.
    async fn scan_beside_intact(
        intact: Vec<u8>,
        corrupt: Vec<u8>,
        filter: Option<Expr>,
    ) -> datafusion::error::Result<String> {
        let (size, footer_len) = (corrupt.len() as u64, footer_len_of(&corrupt));
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let intact = fixture
            .put_file(&store, "lake/t/intact.parquet", Bytes::from(intact), false)
            .await;
        let file = fixture
            .put_raw(&store, KEY, Bytes::from(corrupt), size, footer_len)
            .await;
        let table = fixture.provider("t", 1, vec![intact, file], false).await;
        read_where(&fixture.session(&[("t", table)]), "t", &["a"], filter).await
    }

    /// Scan `a` over a table whose one file is `bytes` at [`KEY`], described
    /// by its own length and trailer, with `filter`.
    async fn scan_alone(bytes: Vec<u8>, filter: Option<Expr>) -> datafusion::error::Result<String> {
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_raw(&store, KEY, Bytes::from(bytes), size, footer_len)
            .await;
        let table = fixture.provider("t", 1, vec![file], false).await;
        read_where(&fixture.session(&[("t", table)]), "t", &["a"], filter).await
    }

    /// The metadata of `bytes` with its page index, as the parquet crate loads
    /// it.
    fn metadata_with_page_index(bytes: &[u8]) -> ParquetMetaData {
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&Bytes::copy_from_slice(bytes))
            .expect("metadata with a page index")
    }

    /// One `a: Int64` column holding 1..=6, plain and uncompressed, in three
    /// data pages of two values each, with no dictionary page and with page
    /// statistics, so the file carries a column index and an offset index.
    fn three_pages() -> Vec<u8> {
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use parquet::file::properties::WriterProperties;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5, 6])) as ArrayRef],
        )
        .expect("batch");
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .build();
        let mut bytes = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        let metadata = metadata_with_page_index(&bytes);
        let pages = metadata.offset_index().expect("an offset index")[0][0].page_locations();
        let rows: Vec<i64> = pages.iter().map(|page| page.first_row_index).collect();
        assert_eq!(rows, [0, 2, 4]);
        assert!(metadata.column_index().is_some());
        bytes
    }

    /// `bytes`, a one-column file, with the offset index of its one column
    /// chunk replaced by `pages(&its locations)`. The new index is written in
    /// place and zero-padded to the old one's length, which the parquet crate
    /// ignores after the struct's end; every other byte, the footer included,
    /// is untouched.
    fn with_page_locations(
        bytes: &[u8],
        pages: impl Fn(&[PageLocation]) -> Vec<PageLocation>,
    ) -> Vec<u8> {
        let metadata = metadata_with_page_index(bytes);
        let chunk = metadata.row_group(0).column(0);
        let at =
            usize::try_from(chunk.offset_index_offset().expect("an offset index")).expect("offset");
        let len =
            usize::try_from(chunk.offset_index_length().expect("an offset index")).expect("length");
        let locations =
            pages(metadata.offset_index().expect("an offset index")[0][0].page_locations());
        let count = u8::try_from(locations.len()).expect("few pages");
        assert!(count < 15, "a short list header");
        // OffsetIndex { 1: list<PageLocation> } in the compact protocol.
        let mut index = vec![0x19, (count << 4) | 0x0c];
        for page in &locations {
            index.push(0x16);
            zigzag_varint(page.offset, &mut index);
            index.push(0x15);
            zigzag_varint(i64::from(page.compressed_page_size), &mut index);
            index.push(0x16);
            zigzag_varint(page.first_row_index, &mut index);
            index.push(0);
        }
        index.push(0);
        assert!(index.len() <= len, "{} bytes over {len}", index.len());
        index.resize(len, 0);
        let mut out = bytes.to_vec();
        out[at..at + len].copy_from_slice(&index);
        let decoded = page_locations_of(&out).expect("the new index decodes");
        assert_eq!(decoded[0].2, locations);
        out
    }

    /// [`three_pages`] whose offset index locates only the first and the last
    /// page. The parquet crate reads the pages the offset index names, so with
    /// it an unfiltered scan returns 1, 2, 5, 6 without an error.
    #[tokio::test]
    async fn a_missing_page_location_drops_no_row() {
        let bytes = with_page_locations(&three_pages(), |pages| {
            vec![pages[0].clone(), pages[2].clone()]
        });
        let got = scan_alone(bytes, None).await;
        assert_eq!(got.expect("rows"), "1,2,3,4,5,6");
    }

    /// [`three_pages`] whose offset index sizes its first location to cover
    /// the first two pages and has no location for the second. With it the
    /// second page's rows vanish without an error.
    #[tokio::test]
    async fn a_location_covering_two_pages_drops_no_row() {
        let bytes = with_page_locations(&three_pages(), |pages| {
            let mut first = pages[0].clone();
            first.compressed_page_size += pages[1].compressed_page_size;
            vec![first, pages[2].clone()]
        });
        let got = scan_alone(bytes, None).await;
        assert_eq!(got.expect("rows"), "1,2,3,4,5,6");
    }

    /// [`three_pages`] whose second location claims the page starts at row 3,
    /// not 2. With the offset index a filtered scan selects rows by those
    /// first rows and skips pages by them, so `a = 3` skips the row it
    /// selects and returns nothing.
    #[tokio::test]
    async fn a_wrong_first_row_index_leaves_a_filtered_scan_exact() {
        let bytes = with_page_locations(&three_pages(), |pages| {
            let mut pages = pages.to_vec();
            pages[1].first_row_index = 3;
            pages
        });
        for (value, want) in [(2, "2"), (3, "3"), (4, "4"), (5, "5")] {
            let got = scan_alone(bytes.clone(), Some(ident("a").eq(lit(value as i64)))).await;
            assert_eq!(got.expect("rows"), want, "a = {value}");
        }
        let got = scan_alone(bytes, Some(ident("a").gt(lit(2_i64)))).await;
        assert_eq!(got.expect("rows"), "3,4,5,6");
    }

    /// A dictionary-encoded file with several data pages per column scans to
    /// its rows.
    #[tokio::test]
    async fn a_dictionary_encoded_file_with_several_pages_scans_to_its_rows() {
        use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use parquet::file::properties::WriterProperties;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5, 6])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x", "y", "x", "y", "x", "y"])),
            ],
        )
        .expect("batch");
        let properties = WriterProperties::builder()
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .build();
        let mut dictionary = Vec::new();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut dictionary, schema, Some(properties))
                .expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let metadata = metadata_with_page_index(&dictionary);
        let offset_index = metadata.offset_index().expect("an offset index");
        for (chunk, index) in metadata.row_group(0).columns().iter().zip(&offset_index[0]) {
            assert_eq!(index.page_locations().len(), 3);
            assert!(chunk.dictionary_page_offset().is_some());
        }

        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(&store, KEY, Bytes::from(dictionary), false)
            .await;
        let table = fixture.provider("t", 1, vec![file], false).await;
        let rows = read_all(&fixture.session(&[("t", table)]), "t", &["a", "b"]).await;
        assert_eq!(rows.expect("rows"), "1|x,2|y,3|x,4|y,5|x,6|y");
    }

    /// `SELECT t.a, t.b FROM u JOIN t ON u.a = t.a`, where table `t`'s files
    /// are an intact one (`a` in 1..=3) and `second` (the fixture's `a` in
    /// 4..=6), and table `u` is one file holding `a = 5`; rendered as
    /// `v|v,v|v` in result order.
    fn join_with_second_file(second: Vec<u8>) -> datafusion::error::Result<String> {
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
            let size = second.len() as u64;
            let footer_len = footer_len_of(&second);
            let bad = fixture
                .put_raw(&store, KEY, Bytes::from(second), size, footer_len)
                .await;
            let probe = fixture
                .put_file(
                    &store,
                    "lake/u/five.parquet",
                    parquet_bytes(&[5], &["u"]),
                    false,
                )
                .await;
            let t = fixture.provider("t", 1, vec![good, bad], false).await;
            let u = fixture.provider("u", 1, vec![probe], false).await;
            let ctx = fixture.session(&[("t", t), ("u", u)]);
            let batches = ctx
                .table("u")
                .await?
                .alias("u")?
                .join_on(
                    ctx.table("t").await?.alias("t")?,
                    JoinType::Inner,
                    [col("u.a").eq(col("t.a"))],
                )?
                .select([col("t.a"), col("t.b")])?
                .collect()
                .await?;
            render_rows(&batches)
        })
    }

    /// The join's build side holds only `a = 5`, so DataFusion pushes a
    /// dynamic filter into `t`'s scan after planning: that scan gets a
    /// predicate although the statement has no WHERE clause and the
    /// provider's `scan()` received no filter. The corrupt file's row group
    /// (`a` in 4..=6) is only partly matched by it, the case in which
    /// DataFusion would prune that file's pages with a page index. The reader
    /// hands out none, so the join returns the corrupt file's real row rather
    /// than reaching the parquet crate's panic.
    #[tokio::test]
    async fn a_join_over_a_page_index_out_of_its_chunk_returns_exact_rows() {
        let intact = tokio::task::spawn_blocking(|| join_with_second_file(valid()))
            .await
            .expect("the join does not panic");
        assert_eq!(intact.expect("the intact join reads"), "5|five");

        let flipped = page_index_out_of_its_chunk();
        let got = tokio::task::spawn_blocking(move || join_with_second_file(flipped))
            .await
            .expect("the join does not panic");
        assert_eq!(got.expect("the join reads"), "5|five");
    }

    /// A footer refused as corrupt is cached as refused: the second read of the
    /// same pinned file fails with the same message, reads nothing, and counts
    /// as a Probe cache hit.
    #[tokio::test]
    async fn a_refused_footer_is_cached_and_never_read_again() {
        let mut bytes = valid();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        let end = bytes.len() - TRAILER_LEN as usize;
        bytes[end - footer_len as usize..end].fill(0xff);
        let memory = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&memory), false));
        let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_raw(&memory, KEY, Bytes::from(bytes), size, footer_len)
            .await;

        let first = fixture.reader(file.clone()).metadata().await;
        let message = match &first {
            Err(ParquetReadError::Corrupt { message, .. }) => message.clone(),
            other => panic!("expected Corrupt, got {other:?}"),
        };
        assert_eq!(recording.ranges().len(), 1, "one footer read");

        let accounting = PhaseAccounting::new();
        let reader = PinnedParquetReader::new(
            crate::test_support::TENANT,
            Arc::new(PinnedFile {
                file,
                store: Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>,
            }),
            fixture.services(),
            accounting.clone(),
        );
        match reader.metadata().await {
            Err(ParquetReadError::Corrupt {
                key,
                message: again,
            }) => {
                assert_eq!(key, KEY);
                assert_eq!(again, message);
            }
            other => panic!("expected the cached refusal, got {other:?}"),
        }
        assert_eq!(recording.ranges().len(), 1, "the refusal read nothing");
        let probe = accounting.snapshot();
        let probe = probe.phase(QueryPhase::Probe);
        assert_eq!(probe.cache_hits, 1);
        assert_eq!(probe.s3_requests(AccountedOp::Get), 0);
        assert_eq!(
            probe.cache_bytes,
            (message.len() + std::mem::size_of::<MetadataKey>()) as u64
        );
    }

    /// `SELECT a, b FROM t ORDER BY a LIMIT 1`, where table `t` is one group
    /// of two files in this order: one holding `a = 5`, then `second` (the
    /// fixture's `a` in 4..=6); rendered as `v|v`.
    fn top_one_with_second_file(second: Vec<u8>) -> datafusion::error::Result<String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let store = Arc::new(MemoryStore::new());
            let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
            let first = fixture
                .put_file(
                    &store,
                    "lake/t/five.parquet",
                    parquet_bytes(&[5], &["t"]),
                    false,
                )
                .await;
            let size = second.len() as u64;
            let footer_len = footer_len_of(&second);
            let bad = fixture
                .put_raw(&store, KEY, Bytes::from(second), size, footer_len)
                .await;
            let t = fixture.provider("t", 1, vec![first, bad], false).await;
            let ctx = fixture.session(&[("t", t)]);
            let batches = ctx
                .table("t")
                .await?
                .sort(vec![ident("a").sort(true, false)])?
                .limit(0, Some(1))?
                .select([col("a"), col("b")])?
                .collect()
                .await?;
            render_rows(&batches)
        })
    }

    /// The TopK over `t` pushes a dynamic filter into the scan once the first
    /// file has given it `a = 5`: `a < 5` only partly matches the corrupt
    /// file's row group, the case in which DataFusion would prune that file's
    /// pages with a page index. The statement has no WHERE clause; it returns
    /// the corrupt file's real smallest row.
    #[tokio::test]
    async fn a_top_k_over_a_page_index_out_of_its_chunk_returns_exact_rows() {
        let intact = tokio::task::spawn_blocking(|| top_one_with_second_file(valid()))
            .await
            .expect("the TopK does not panic");
        assert_eq!(intact.expect("the intact TopK reads"), "4|four");

        let flipped = page_index_out_of_its_chunk();
        let got = tokio::task::spawn_blocking(move || top_one_with_second_file(flipped))
            .await
            .expect("the TopK does not panic");
        assert_eq!(got.expect("the TopK reads"), "4|four");
    }

    /// The footer the reader hands out carries no column index and no offset
    /// index, whatever page index policy the caller asks for, although the
    /// file carries both; and a scan whose filter only partly matches the
    /// file's row group reads the footer and the column chunks, never a byte
    /// of the page index.
    #[tokio::test]
    async fn the_footer_carries_no_page_index_and_the_scan_never_reads_one() {
        let bytes = valid();
        let stored = metadata_with_page_index(&bytes);
        assert!(stored.column_index().is_some() && stored.offset_index().is_some());
        let stripped = without_page_index(stored);
        assert!(stripped.column_index().is_none() && stripped.offset_index().is_none());
        let page_index = page_index_region(&bytes);

        let memory = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&memory), false));
        let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(&memory, KEY, Bytes::from(bytes), false)
            .await;
        for policy in [
            PageIndexPolicy::Skip,
            PageIndexPolicy::Optional,
            PageIndexPolicy::Required,
        ] {
            let options = ArrowReaderOptions::new().with_page_index_policy(policy);
            let mut reader = fixture.reader(file.clone());
            let metadata = reader.get_metadata(Some(&options)).await.expect("footer");
            assert!(metadata.column_index().is_none(), "{policy:?}");
            assert!(metadata.offset_index().is_none(), "{policy:?}");
        }

        let chunks = fixture.column_chunk_bytes(&[&file]).await;
        let footer = u64::from(file.footer_len) + TRAILER_LEN;
        let before = recording.ranges().len();
        let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
        let accounting = PhaseAccounting::new();
        let table = fixture
            .provider_with("t", 1, vec![file], false, accounting.clone())
            .await;
        let ctx = fixture.session(&[("t", table)]);
        let rows = read_where(&ctx, "t", &["a", "b"], Some(page_pruning_filter())).await;
        assert_eq!(rows.expect("rows"), "5|five,6|sixx");
        let snapshot = accounting.snapshot();
        assert_eq!(
            snapshot.phase(QueryPhase::Probe).s3_bytes(AccountedOp::Get),
            footer
        );
        assert_eq!(
            snapshot.phase(QueryPhase::Scan).s3_bytes(AccountedOp::Get),
            chunks
        );
        let ranges = recording.ranges().split_off(before);
        assert_eq!(
            ranges.len(),
            3,
            "the footer and two column chunks: {ranges:?}"
        );
        assert!(
            ranges.iter().all(|range| match *range {
                GetRange::Range(start, end) =>
                    end <= page_index.start as u64 || start >= page_index.end as u64,
                _ => false,
            }),
            "{ranges:?} against the page index at {page_index:?}"
        );
    }

    /// A footer read that comes back short fails as `Corrupt` but is not
    /// cached as refused: it says nothing about the pinned bytes, and the next
    /// read of the same file succeeds.
    #[tokio::test]
    async fn a_short_read_is_not_cached_as_refused() {
        let memory = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&memory), false));
        let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(&memory, KEY, Bytes::from(valid()), false)
            .await;
        let tail = u64::from(file.footer_len) + TRAILER_LEN;

        recording.shorten_next_read();
        assert_corrupt(
            fixture.reader(file.clone()).metadata().await,
            &format!("returned {} bytes", tail - 1),
        );
        let metadata = fixture
            .reader(file)
            .metadata()
            .await
            .expect("the next read is not refused from the cache");
        assert_eq!(metadata.file_metadata().num_rows(), 3);
        assert_eq!(recording.ranges().len(), 2, "the footer twice");
    }

    /// Two tables of one tenant over the same pinned file, one recording its
    /// footer length and one recording a length the trailer disagrees with,
    /// each get the outcome they get alone, in either query order: the
    /// refusal describes the manifest, so it is not served to the table whose
    /// manifest is right, and the decoded footer is not served to the table
    /// whose manifest is wrong.
    #[tokio::test]
    async fn a_wrong_footer_length_is_cached_apart_from_the_right_one() {
        async fn outcomes(wrong_first: bool) -> Vec<String> {
            let memory = Arc::new(MemoryStore::new());
            let fixture = Fixture::new(Arc::clone(&memory) as Arc<dyn ObjectStoreBackend>);
            let right = fixture
                .put_file(&memory, KEY, Bytes::from(valid()), false)
                .await;
            let wrong = ParquetFile {
                footer_len: right.footer_len + 1,
                ..right.clone()
            };
            let mut files = [("right", right), ("wrong", wrong)];
            if wrong_first {
                files.reverse();
            }
            let mut out = Vec::new();
            for (name, file) in files {
                let outcome = match fixture
                    .provider_with_options(name, 1, vec![file], Default::default())
                    .await
                {
                    Ok(table) => {
                        let ctx = fixture.session(&[(name, Arc::new(table))]);
                        match read_all(&ctx, name, &["a", "b"]).await {
                            Ok(rows) => format!("{name}: rows {rows}"),
                            Err(err) => format!("{name}: {:?}", read_error(&err)),
                        }
                    }
                    Err(err) => format!("{name}: {err}"),
                };
                out.push(outcome);
            }
            out.sort();
            out
        }

        let right_first = outcomes(false).await;
        assert_eq!(right_first[0], "right: rows 4|four,5|five,6|sixx");
        assert!(
            right_first[1].starts_with("wrong: ") && right_first[1].contains("the trailer records"),
            "{right_first:?}"
        );
        assert_eq!(outcomes(true).await, right_first);
    }

    /// A one-byte change inside the page index, which is a small part of the
    /// file.
    fn page_index_mutation() -> impl Strategy<Value = Mutation> {
        (any::<Index>(), 1..=u8::MAX).prop_map(|(at, mask)| Mutation::FlipPageIndex(at, mask))
    }

    /// [`any_mutation`] with half the weight on the page index.
    fn filtered_scan_mutation() -> impl Strategy<Value = Mutation> {
        prop_oneof![any_mutation(), page_index_mutation()]
    }

    /// The error must be `Corrupt` for [`KEY`], reporting a decoder panic
    /// while table `t` was scanned.
    fn assert_decoder_panic(err: &DataFusionError) {
        match read_error(err) {
            Some(ParquetReadError::Corrupt { key, message }) => {
                assert_eq!(key, KEY, "{err}");
                assert!(
                    message.contains("panicked while scanning table t"),
                    "{message}"
                );
            }
            other => panic!("expected a Corrupt decoder panic, got {other:?} from {err}"),
        }
    }

    /// The case CI's seed for the filtered byte-change property shrinks to,
    /// `Splice(Index(847260924571603501), 14, [5, 22, 71, 25, 152])`: the
    /// splice leaves a data page header without its data page fields, which
    /// the parquet crate unwraps.
    #[test]
    fn the_filtered_scan_seed_ci_found_is_a_typed_corrupt_error() {
        let mut bytes = valid();
        // `Index::index`: a fixed-point multiply by the file's length.
        let at = ((bytes.len() as u128 * 847_260_924_571_603_501_u128) >> 64) as usize;
        let end = (at + 14).min(bytes.len());
        bytes.splice(at..end, [5, 22, 71, 25, 152]);
        let got = scan_second_file_where(bytes, Some(page_pruning_filter()));
        assert_decoder_panic(&got.expect_err("the spliced file must fail the scan"));
    }

    /// [`valid`] with column `b`'s one data page header retyped as a
    /// version 2 data page: the header keeps its version 1 fields and has no
    /// version 2 fields.
    fn data_page_retyped_as_v2() -> Vec<u8> {
        let mut bytes = valid();
        let end = bytes.len() - TRAILER_LEN as usize;
        let metadata = ParquetMetaDataReader::decode_metadata(
            &bytes[end - footer_len_of(&bytes) as usize..end],
        )
        .expect("footer");
        let at = metadata.row_group(0).column(1).data_page_offset();
        let at = usize::try_from(at).expect("offset");
        // Field 1 (`type`, an i32) of the compact-protocol PageHeader:
        // DATA_PAGE (0) as a zigzag varint becomes DATA_PAGE_V2 (3).
        assert_eq!(bytes[at..at + 2], [0x15, 0x00], "a data page header");
        bytes[at + 1] = 0x06;
        bytes
    }

    /// A page header the parquet crate panics on, under a filtered scan that
    /// skips a row of that page: the boundary's stream yields the intact
    /// file's rows, one typed `Corrupt` error naming the corrupt file and the
    /// table, and then ends, with no row of the corrupt file.
    #[tokio::test]
    async fn a_corrupt_page_header_under_a_filtered_scan_is_corrupt_and_yields_no_rows() {
        use datafusion::catalog::TableProvider;
        use futures::StreamExt;

        let bytes = data_page_retyped_as_v2();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
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
        let bad = fixture
            .put_raw(&store, KEY, Bytes::from(bytes), size, footer_len)
            .await;
        let table = fixture.provider("t", 1, vec![good, bad], false).await;
        let ctx = fixture.session(&[]);
        // Keeps 1 and 2 of the intact file, and 5 and 6 of the corrupt one,
        // whose 4 the reader skips by peeking at its page header.
        let filter = page_pruning_filter().or(ident("a").lt(lit(3_i64)));
        let plan = table
            .scan(&ctx.state(), None, &[filter], None)
            .await
            .expect("scan");
        let mut stream = plan.execute(0, ctx.task_ctx()).expect("execute");
        let mut rows = Vec::new();
        let err = loop {
            match stream.next().await {
                Some(Ok(batch)) => rows.push(render_rows(&[batch]).expect("render")),
                Some(Err(err)) => break err,
                None => panic!("the scan ended without an error: {rows:?}"),
            }
        };
        assert_decoder_panic(&err);
        assert!(stream.next().await.is_none(), "the stream ends after it");
        assert_eq!(rows.join(","), "1|one,2|two");
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

        /// A byte change inside the page index leaves a filtered scan's rows
        /// exactly the intact file's: the reader never reads those bytes.
        #[test]
        fn a_page_index_byte_change_leaves_a_filtered_scan_exact(
            mutation in page_index_mutation(),
        ) {
            let got = scan_second_file_where(mutation.apply(&valid()), Some(page_pruning_filter()));
            prop_assert_eq!(got.expect("rows"), "5|five,6|sixx");
        }

        /// [`any_byte_change_is_an_error_or_rows_never_a_panic`] for a
        /// filtered scan, with half the cases changing a byte of the page
        /// index.
        #[test]
        fn any_byte_change_under_a_filtered_scan_is_an_error_or_rows_never_a_panic(
            mutation in filtered_scan_mutation(),
        ) {
            let original = valid();
            let mutated = mutation.apply(&original);
            prop_assume!(mutated != original);
            if let Err(err) = scan_second_file_where(mutated, Some(page_pruning_filter()))
                && let Some(found) = read_error(&err)
            {
                prop_assert!(
                    matches!(&found, ParquetReadError::Corrupt { key, .. } if key == KEY),
                    "{err}"
                );
            }
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
