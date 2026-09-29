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
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::errors::ParquetError;
use parquet::file::metadata::{
    FooterTail, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
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
/// The reader always loads the page index with the footer (charged to Probe)
/// and checks it before handing the footer out, whatever the scan's
/// predicate. DataFusion prunes pages whenever the scan has a predicate when
/// it opens a file, and that includes dynamic filters (hash join, TopK)
/// pushed into the scan after planning; the parquet crate panics on some
/// malformed page locations rather than returning an error. DataFusion loads
/// a page index itself only when the footer it is handed lacks one, so the
/// index it prunes with is always one this reader checked.
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

    /// The decoded footer with its checked page index, from the metadata
    /// cache or from Probe reads.
    ///
    /// A metadata cache hit counts as a Probe cache hit of the entry's charged
    /// size, the convention the catalog caches use. The key is the pinned
    /// identity and the footer length the manifest recorded, the two inputs
    /// the footer is decoded or refused from. A footer refused as `Corrupt` is
    /// cached as refused under that key, so a later read fails the same way
    /// without reading it again. A read that failed is not cached, whatever
    /// it failed on: a store error, a read that came back short, or a page
    /// index range past the recorded size, which [`Self::read_range`] fails
    /// as `Corrupt` before any GET.
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

    /// Read, decode and check the footer, then load and check its page index.
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
        let metadata = Arc::new(metadata);
        check_arrow_schema(&metadata).map_err(FooterError::Refused)?;
        self.with_checked_page_index(metadata).await
    }

    /// `metadata` with its page index loaded through Probe reads and checked.
    /// A file that carries no page index, or whose offset index does not
    /// decode (dropped under the `Optional` policy), comes back without one;
    /// DataFusion's own load of the same pinned bytes would find none either.
    async fn with_checked_page_index(
        &self,
        metadata: Arc<ParquetMetaData>,
    ) -> Result<Arc<ParquetMetaData>, FooterError> {
        let owned = Arc::try_unwrap(metadata).unwrap_or_else(|shared| shared.as_ref().clone());
        let mut loader = ParquetMetaDataReader::new_with_metadata(owned)
            .with_page_index_policy(PageIndexPolicy::Optional);
        loader
            .load_page_index(ProbeFetch(self))
            .await
            .map_err(page_index_error)?;
        let metadata = loader.finish().map_err(page_index_error)?;
        check_page_index(&metadata).map_err(FooterError::Refused)?;
        Ok(Arc::new(metadata))
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

/// A page index load failure: the reader's own error when one of its reads
/// failed, a refusal when the bytes it read did not decode.
fn page_index_error(err: ParquetError) -> FooterError {
    if let ParquetError::External(source) = &err
        && let Some(read) = source.downcast_ref::<ParquetReadError>()
    {
        return FooterError::Read(read.clone());
    }
    FooterError::Refused(format!("page index: {err}"))
}

/// Refuse a page index whose page locations the scan could not use
/// safely: every data page must lie inside its column chunk's byte range,
/// the first at the chunk's `data_page_offset` and each later one at or past
/// the end of the one before; the first page must start at row 0, each page
/// must start at a later row than the one before and inside the row group;
/// and a column index, where there is one, must describe as many pages as
/// the offset index lists. The scan reads every data page from its location
/// and every byte before the first one as a dictionary page, so a location
/// naming another page's bytes, or a gap before the first, decodes the wrong
/// values without an error.
fn check_page_index(metadata: &ParquetMetaData) -> Result<(), String> {
    let Some(offset_index) = metadata.offset_index() else {
        return Ok(());
    };
    if offset_index.len() != metadata.num_row_groups() {
        return Err(format!(
            "the offset index covers {} row groups, the footer {}",
            offset_index.len(),
            metadata.num_row_groups()
        ));
    }
    for (row_group, (group, columns)) in metadata.row_groups().iter().zip(offset_index).enumerate()
    {
        if columns.len() != group.num_columns() {
            return Err(format!(
                "row group {row_group}: the offset index covers {} columns, the footer {}",
                columns.len(),
                group.num_columns()
            ));
        }
        let rows = group.num_rows();
        for (column, (chunk, index)) in group.columns().iter().zip(columns).enumerate() {
            let start = chunk
                .dictionary_page_offset()
                .unwrap_or_else(|| chunk.data_page_offset());
            let end = start.saturating_add(chunk.compressed_size());
            let mut previous_row: Option<i64> = None;
            let mut previous_end: Option<i64> = None;
            for (page, location) in index.page_locations().iter().enumerate() {
                let page_end = location
                    .offset
                    .checked_add(i64::from(location.compressed_page_size));
                let inside = location.offset >= start
                    && location.compressed_page_size > 0
                    && page_end.is_some_and(|page_end| page_end <= end);
                let row = location.first_row_index;
                let row_ok = match previous_row {
                    None => row == 0,
                    Some(previous) => row > previous,
                } && row < rows.max(1);
                if !inside || !row_ok {
                    return Err(format!(
                        "row group {row_group} column {column} page {page}: {} bytes at \
                         offset {} from row {row}, outside the chunk's bytes {start}..{end} \
                         or its {rows} rows",
                        location.compressed_page_size, location.offset
                    ));
                }
                match previous_end {
                    None if location.offset != chunk.data_page_offset() => {
                        return Err(format!(
                            "row group {row_group} column {column}: the first page is at offset \
                             {}, the chunk's first data page at {}",
                            location.offset,
                            chunk.data_page_offset()
                        ));
                    }
                    Some(previous_end) if location.offset < previous_end => {
                        return Err(format!(
                            "row group {row_group} column {column} page {page}: offset {} is \
                             before the previous page's end {previous_end}",
                            location.offset
                        ));
                    }
                    _ => {}
                }
                previous_row = Some(row);
                previous_end = page_end;
            }
            if let Some(column_index) = metadata
                .column_index()
                .and_then(|index| index.get(row_group))
                .and_then(|columns| columns.get(column))
                && !matches!(column_index, ColumnIndexMetaData::NONE)
                && column_index.num_pages() != index.page_locations().len() as u64
            {
                return Err(format!(
                    "row group {row_group} column {column}: the column index describes {} \
                     pages, the offset index {}",
                    column_index.num_pages(),
                    index.page_locations().len()
                ));
            }
        }
    }
    Ok(())
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

/// Page index reads for [`PinnedParquetReader::with_checked_page_index`],
/// charged to Probe like the footer they belong with.
struct ProbeFetch<'a>(&'a PinnedParquetReader);

impl MetadataFetch for ProbeFetch<'_> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let reader = self.0;
        async move {
            reader
                .read_range(range, QueryPhase::Probe)
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
        read_error, read_where, render_rows,
    };
    use datafusion::logical_expr::{Expr, JoinType, col, ident, lit};
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
    /// statistics, and the mutated file's (4..=6) is only partly matched, so
    /// DataFusion loads its page index to prune pages.
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

    /// The parquet crate panics on a page location outside its chunk when it
    /// prunes pages. The reader refuses such an index with the footer,
    /// whatever the scan's predicate, and the outcome does not depend on what
    /// ran before: an unfiltered, a filtered and another unfiltered scan over
    /// one metadata cache each fail with the same `Corrupt` error.
    #[tokio::test]
    async fn a_page_index_out_of_its_chunk_is_corrupt_whatever_ran_before() {
        let flipped = page_index_out_of_its_chunk();
        let size = flipped.len() as u64;
        let footer_len = footer_len_of(&flipped);

        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_raw(&store, KEY, Bytes::from(flipped.clone()), size, footer_len)
            .await;
        assert_corrupt(fixture.reader(file).metadata().await, "outside the chunk");

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
                    Err(err) => format!("{:?}", read_error(&err)),
                }
            }
        };
        let first = outcome(None).await;
        let filtered = outcome(Some(page_pruning_filter())).await;
        let again = outcome(None).await;
        assert!(
            first.starts_with("Some(Corrupt")
                && first.contains(KEY)
                && first.contains("outside the chunk"),
            "{first}"
        );
        assert_eq!(filtered, first);
        assert_eq!(again, first);
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

    /// Two page locations naming the same bytes pass every per-page check:
    /// each lies inside the chunk and starts at a later row. The scan reads
    /// every page from its location, so it would decode the first page twice
    /// and return 1, 2, 1, 2 with the right row count; the reader refuses the
    /// index instead, and a scan of it beside an intact file fails typed with
    /// no rows.
    #[tokio::test]
    async fn a_page_location_overlapping_the_one_before_is_corrupt() {
        let bytes = second_page_on_the_first();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let intact = fixture
            .put_file(
                &store,
                "lake/t/intact.parquet",
                Bytes::from(two_equal_pages()),
                false,
            )
            .await;
        let table = fixture.provider("t", 1, vec![intact.clone()], false).await;
        let rows = read_all(&fixture.session(&[("t", table)]), "t", &["a"]).await;
        assert_eq!(rows.expect("the intact file reads"), "1,2,3,4");

        let file = fixture
            .put_raw(&store, KEY, Bytes::from(bytes), size, footer_len)
            .await;
        let table = fixture.provider("t", 1, vec![intact, file], false).await;
        let got = read_all(&fixture.session(&[("t", table)]), "t", &["a"]).await;
        match got {
            Err(err) => match read_error(&err) {
                Some(ParquetReadError::Corrupt { key, message }) => {
                    assert_eq!(key, KEY);
                    assert!(
                        message.contains("before the previous page's end"),
                        "{message}"
                    );
                }
                other => panic!("expected Corrupt, got {other:?} from {err}"),
            },
            Ok(rows) => panic!("expected Corrupt, got rows {rows}"),
        }
    }

    /// The metadata of `bytes` with its page index, as the reader loads it.
    fn metadata_with_page_index(bytes: &[u8]) -> ParquetMetaData {
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&Bytes::copy_from_slice(bytes))
            .expect("metadata with a page index")
    }

    /// An offset index whose one location is the second data page: it lies
    /// inside the chunk and starts at row 0, but the scan would read the first
    /// data page's bytes, before it, as a dictionary page.
    #[test]
    fn a_first_page_location_past_the_first_data_page_is_refused() {
        let metadata = metadata_with_page_index(&two_equal_pages());
        let chunk = metadata.row_group(0).column(0);
        assert_eq!(chunk.dictionary_page_offset(), None);
        let second =
            metadata.offset_index().expect("an offset index")[0][0].page_locations()[1].clone();
        assert!(second.offset > chunk.data_page_offset());
        let mut index = parquet::file::metadata::OffsetIndexBuilder::new();
        index.append_offset_and_size(second.offset, second.compressed_page_size);
        index.append_row_count(4);
        let moved = parquet::file::metadata::ParquetMetaDataBuilder::new_from_metadata(metadata)
            .set_offset_index(Some(vec![vec![index.build()]]))
            .build();
        let err = check_page_index(&moved).expect_err("refused");
        assert!(err.contains("the chunk's first data page at"), "{err}");
    }

    /// ArrowWriter's own layout passes the page order checks: with a
    /// dictionary page, the first location is the chunk's `data_page_offset`
    /// and the dictionary page lies before it; without one, the first location
    /// is where the chunk starts. Both files scan to their rows.
    #[tokio::test]
    async fn arrow_writer_files_pass_the_page_order_checks() {
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
            let pages = index.page_locations();
            assert_eq!(pages.len(), 3, "{pages:?}");
            assert!(chunk.dictionary_page_offset().expect("a dictionary") < pages[0].offset);
            assert_eq!(pages[0].offset, chunk.data_page_offset());
        }
        check_page_index(&metadata).expect("the dictionary-encoded file passes");
        let plain = metadata_with_page_index(&two_equal_pages());
        check_page_index(&plain).expect("the plain file passes");

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
    /// (`a` in 4..=6) is only partly matched by it, so DataFusion prunes that
    /// file's pages with whatever page index the reader handed out. The reader
    /// always loads and checks the page index with the footer, so the join
    /// fails typed rather than reaching the parquet crate's panic.
    #[tokio::test]
    async fn a_join_over_a_page_index_out_of_its_chunk_is_corrupt() {
        let intact = tokio::task::spawn_blocking(|| join_with_second_file(valid()))
            .await
            .expect("the join does not panic");
        assert_eq!(intact.expect("the intact join reads"), "5|five");

        let flipped = page_index_out_of_its_chunk();
        let err = tokio::task::spawn_blocking(move || join_with_second_file(flipped))
            .await
            .expect("the join does not panic")
            .expect_err("the join fails");
        assert!(
            matches!(read_error(&err), Some(ParquetReadError::Corrupt { key, .. }) if key == KEY),
            "{err}"
        );
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
    /// file's row group, so DataFusion prunes that file's pages. The statement
    /// has no WHERE clause; it still fails typed.
    #[tokio::test]
    async fn a_top_k_over_a_page_index_out_of_its_chunk_is_corrupt() {
        let intact = tokio::task::spawn_blocking(|| top_one_with_second_file(valid()))
            .await
            .expect("the TopK does not panic");
        assert_eq!(intact.expect("the intact TopK reads"), "4|four");

        let flipped = page_index_out_of_its_chunk();
        let err = tokio::task::spawn_blocking(move || top_one_with_second_file(flipped))
            .await
            .expect("the TopK does not panic")
            .expect_err("the TopK fails");
        assert!(
            matches!(read_error(&err), Some(ParquetReadError::Corrupt { key, .. }) if key == KEY),
            "{err}"
        );
    }

    /// A file written with chunk statistics only carries an offset index and
    /// no column index. The loaded metadata records an explicit "no column
    /// index" for each chunk, which is what keeps DataFusion from loading the
    /// page index again, unchecked, through the Scan reads: a scan that prunes
    /// pages reads exactly the column chunks.
    #[tokio::test]
    async fn an_offset_index_without_a_column_index_is_not_loaded_again_by_the_scan() {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use parquet::file::properties::{EnabledStatistics, WriterProperties};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![4, 5, 6])) as ArrayRef,
                Arc::new(StringArray::from(vec!["four", "five", "sixx"])),
            ],
        )
        .expect("batch");
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let mut bytes = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let memory = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&memory) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(&memory, KEY, Bytes::from(bytes), false)
            .await;
        let metadata = fixture
            .reader(file.clone())
            .metadata()
            .await
            .expect("footer");
        assert!(metadata.offset_index().is_some());
        let column_index = metadata.column_index().expect("an explicit column index");
        assert!(
            column_index
                .iter()
                .flatten()
                .all(|index| matches!(index, ColumnIndexMetaData::NONE)),
            "{column_index:?}"
        );

        let chunks = fixture.column_chunk_bytes(&[&file]).await;
        let fixture = Fixture::new(Arc::clone(&memory) as Arc<dyn ObjectStoreBackend>);
        let accounting = PhaseAccounting::new();
        let table = fixture
            .provider_with("t", 1, vec![file], false, accounting.clone())
            .await;
        let ctx = fixture.session(&[("t", table)]);
        let rows = read_where(&ctx, "t", &["a", "b"], Some(page_pruning_filter())).await;
        assert_eq!(rows.expect("rows"), "5|five,6|sixx");
        let scan = accounting.snapshot();
        assert_eq!(
            scan.phase(QueryPhase::Scan).s3_bytes(AccountedOp::Get),
            chunks
        );
    }

    /// A footer read that comes back short fails as `Corrupt` but is not
    /// cached as refused: it says nothing about the pinned bytes, and the next
    /// read of the same file succeeds. The same holds for the page index read
    /// that follows a footer read.
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
        assert_eq!(recording.ranges().len(), 3, "footer twice, page index once");

        let memory = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&memory), false));
        let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
        let page_index = page_index_region(&valid()).len();
        let file = fixture
            .put_file(&memory, KEY, Bytes::from(valid()), false)
            .await;

        recording.shorten_read(2);
        assert_corrupt(
            fixture.reader(file.clone()).metadata().await,
            &format!("returned {} bytes", page_index - 1),
        );
        assert_eq!(recording.ranges().len(), 2, "footer, then the page index");
        let metadata = fixture
            .reader(file)
            .metadata()
            .await
            .expect("the next read is not refused from the cache");
        assert!(metadata.offset_index().is_some());
        assert_eq!(
            recording.ranges().len(),
            3,
            "the page index again, the footer from the byte cache"
        );
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

    /// [`any_mutation`] with half the weight on the page index, which a
    /// filtered scan prunes pages with and which is a small part of the file.
    fn filtered_scan_mutation() -> impl Strategy<Value = Mutation> {
        prop_oneof![
            any_mutation(),
            (any::<Index>(), 1..=u8::MAX).prop_map(|(at, mask)| Mutation::FlipPageIndex(at, mask)),
        ]
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

        /// [`any_byte_change_is_an_error_or_rows_never_a_panic`] for a
        /// filtered scan, which prunes pages with the page index, with half
        /// the cases changing a byte of that index.
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
