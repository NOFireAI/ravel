//! The read path for one manifest file.
//!
//! Every byte a Parquet scan reads passes through [`PinnedParquetReader::read_range`]:
//! one pinned GET on the file's store, under one `GetLimiter` permit, through
//! the process `ReadCache` keyed by the pinned identity the manifest recorded.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource_parquet::ParquetFileReaderFactory;
use futures::future::{BoxFuture, FutureExt, try_join_all};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
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
        let metadata = Arc::new(metadata);
        self.services
            .metadata
            .insert(cache_key, Arc::clone(&metadata));
        Ok(metadata)
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
        Fixture, RecordingStore, assert_file_changed, parquet_bytes, read_all,
    };
    use ravel_object_store::memory::MemoryStore;

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
