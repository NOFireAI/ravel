//! The read path for one manifest file.
//!
//! Every byte a Parquet scan reads passes through [`PinnedParquetReader::read_range`]:
//! one pinned GET on the file's store, under one `GetLimiter` permit, through
//! the process `ReadCache` keyed by the pinned identity the manifest recorded.

use std::fmt;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

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
use ravel_memory::Reservation;
use ravel_object_store::{GetRange, ObjectStoreBackend, Pin, StoreError};
use ravel_pqtable::manifest::ParquetFile;
use ravel_query::{CacheFetchError, GetLimiter, PhaseAccounting, QueryPhase, ReadCache};
use ravel_types::TenantHash;

use crate::error::ParquetReadError;
use crate::footer_shape::check_footer_shape;
use crate::limits::{ReadLimits, attach};
use crate::metadata_cache::{CachedFooter, MetadataCache, MetadataKey};
use crate::store::file_path;

/// Length of the Parquet trailer: a 4-byte footer length and the `PAR1` magic.
pub(crate) const TRAILER_LEN: u64 = 8;

/// Longest footer accepted, refused from the trailer or the manifest before
/// the footer is read. parquet 58.4.0's writer puts about 65 bytes per
/// column chunk in a footer without statistics and 120 to 230 with page
/// statistics (measured on Int64 columns, whose path the footer repeats per
/// chunk), so 64 MiB holds about 300,000 column chunks with statistics:
/// 1,000 columns in 300 row groups, or 10,000 columns in 30. At a writer's
/// default row group of a million rows, that is hundreds of millions of
/// rows in one file before the footer nears the limit.
pub(crate) const MAX_FOOTER_BYTES: u64 = 64 << 20;

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
    limits: ReadLimits,
    /// The footer the metadata cache did not admit, set by the first
    /// [`Self::metadata`] call that decoded it and shared by clones.
    uncached: Arc<Mutex<Option<UncachedFooter>>>,
}

/// A decoded footer the metadata cache could not hold, kept with the
/// reservation of its decode estimate so the metadata stays charged to the
/// memory budget until the last clone of the reader is dropped.
#[derive(Debug)]
struct UncachedFooter {
    metadata: Arc<ParquetMetaData>,
    _reservation: Reservation,
}

impl PinnedParquetReader {
    pub fn new(
        tenant: TenantHash,
        file: Arc<PinnedFile>,
        services: ReadServices,
        accounting: PhaseAccounting,
        limits: ReadLimits,
    ) -> Self {
        PinnedParquetReader {
            tenant,
            file,
            services,
            accounting,
            limits,
            uncached: Arc::new(Mutex::new(None)),
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
    ///
    /// The range's length is reserved against the process memory budget
    /// before the cache is consulted or anything is requested, and the
    /// reservation is released when the last clone of the returned `Bytes`
    /// drops, not when the GET returns.
    pub async fn read_range(
        &self,
        range: Range<u64>,
        phase: QueryPhase,
    ) -> Result<Bytes, ParquetReadError> {
        let len = self.checked_len(&range)?;
        let reservation = self.limits.reserve(len)?;
        self.read_reserved(range, phase, reservation).await
    }

    /// The length of `range`, which must lie inside the file.
    fn checked_len(&self, range: &Range<u64>) -> Result<u64, ParquetReadError> {
        let size = self.file.file.size;
        if range.start > range.end || range.end > size {
            return Err(self.corrupt(format!(
                "read of bytes {}..{} is outside the {size} bytes the manifest recorded",
                range.start, range.end
            )));
        }
        Ok(range.end - range.start)
    }

    /// [`Self::read_range`] for a range whose length `reservation` already
    /// holds.
    async fn read_reserved(
        &self,
        range: Range<u64>,
        phase: QueryPhase,
        mut reservation: Reservation,
    ) -> Result<Bytes, ParquetReadError> {
        let len = range.end - range.start;
        if len == 0 {
            return Ok(Bytes::new());
        }
        let key = self.cache_key(range.start, len);
        let accounting = self.accounting.phase(phase);
        // A buffer the read cache holds is under both the cache's bound and
        // this reservation, the overlap ADR-1170 decision 2 marks.
        let cached = self.services.cache.is_some();

        // A caller whose own closure never ran, because it followed a flight
        // whose leader was refused by ITS budget, tries again under its own.
        // Each attempt peeks the cache first, so a range another query cached
        // meanwhile is served from cache and the tiered cache's "the caller
        // already saw a miss" precondition holds. Only the first peek is
        // counted on the cache's tier metrics; a retry looks again uncounted,
        // so one logical read records one hit or miss there. The cap is 3
        // because the refused flight stays joinable briefly after its leader
        // publishes the refusal, so a first retry can follow the same flight.
        // Attempts can also be spent following distinct flights that are each
        // refused by their own leader's budget; a caller that follows refused
        // flights on every attempt gets a store-shaped error, never wrong
        // bytes. The cap trades that residual case against retrying without
        // bound.
        const MAX_ATTEMPTS: usize = 3;
        let mut attempt = 1;
        let (fetched, refused) = loop {
            if let Some(bytes) = self.peek(key, attempt == 1).await {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                reservation.mark_handed_off();
                return Ok(attach(bytes, reservation));
            }
            if attempt == 1 {
                self.limits.precheck(&self.accounting, len)?;
            }
            let (fetched, refused) = self.fetch_once(key, phase, range.start, range.end).await;
            let followed_a_refusal = matches!(
                fetched,
                Err(SingleFlightError::Upstream(CacheFetchError::BudgetRefused))
            ) && refused
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none();
            if followed_a_refusal && attempt < MAX_ATTEMPTS {
                attempt += 1;
                continue;
            }
            break (fetched, refused);
        };
        if let Some(err) = refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            return Err(err);
        }
        let bytes = match fetched {
            Ok((bytes, Source::Cache)) => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                bytes
            }
            Ok((bytes, Source::Upstream)) => {
                accounting.record_cache_miss();
                bytes
            }
            Err(err) => return Err(self.map_fetch_error(err)),
        };
        if cached {
            reservation.mark_handed_off();
        }
        Ok(attach(bytes, reservation))
    }

    /// Consult the read cache for `key` without fetching: both tiers of a
    /// tiered cache, the RAM cache otherwise, nothing without a cache. With
    /// `counted` false the lookup records no hit or miss on the cache's own
    /// metrics, for a retry whose first peek already recorded one.
    async fn peek(&self, key: CacheKey, counted: bool) -> Option<Bytes> {
        match (&self.services.cache, counted) {
            (Some(ReadCache::Ram(cache)), true) => cache.get(&key),
            (Some(ReadCache::Ram(cache)), false) => cache.peek_uncounted(&key),
            (Some(ReadCache::Tiered(cache)), true) => cache.get_off_worker(key).await,
            (Some(ReadCache::Tiered(cache)), false) => cache.peek_uncounted_off_worker(key).await,
            (None, _) => None,
        }
    }

    /// One attempt at resolving `key` through the read cache's single flight:
    /// a cache miss runs the fetch closure (if this caller becomes the
    /// flight's leader) or waits on whoever is already running it (if this
    /// caller joins as a follower). The returned `Arc` holds this attempt's
    /// own [`ParquetReadError`] if THIS caller's closure invocation refused
    /// the read against its budget; it stays empty for a follower, whose
    /// closure never ran. [`Self::read_reserved`] uses that distinction to
    /// retry a follower under its own budget rather than return it another
    /// caller's refusal.
    async fn fetch_once(
        &self,
        key: CacheKey,
        phase: QueryPhase,
        start: u64,
        end: u64,
    ) -> (
        Result<(Bytes, Source), SingleFlightError<CacheFetchError>>,
        Arc<Mutex<Option<ParquetReadError>>>,
    ) {
        let refused: Arc<Mutex<Option<ParquetReadError>>> = Arc::default();
        let fetch = {
            let file = Arc::clone(&self.file);
            let limiter = Arc::clone(&self.services.limiter);
            let limits = self.limits.clone();
            let phases = self.accounting.clone();
            let refused = Arc::clone(&refused);
            move || async move {
                let _permit = limiter.acquire().await.map_err(|_| {
                    StoreError::Transient("GetLimiter semaphore closed unexpectedly".into())
                })?;
                let admission = match limits.admit(&phases, phase, end - start) {
                    Ok(admission) => admission,
                    Err(err) => {
                        *refused.lock().unwrap_or_else(PoisonError::into_inner) = Some(err);
                        return Err(CacheFetchError::BudgetRefused);
                    }
                };
                let read = file
                    .store
                    .get_pinned(&file.key_str(), GetRange::Range(start, end), &file.pin())
                    .await?;
                let data = read.outcome.data;
                admission.complete(&phases, phase, data.len() as u64);
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
            // The peek above already consulted both tiers and confirmed a
            // miss (`get_off_worker`), so resolving through `get_or_fetch`
            // here would consult the disk tier a second time and record a
            // second, unaccounted miss. `resolve_peeked_miss` joins the same
            // single flight without consulting the disk tier again; its leader
            // rechecks only the RAM tier, uncounted, to reuse the bytes of a
            // flight that finished after the peek.
            Some(ReadCache::Tiered(cache)) => cache
                .resolve_peeked_miss(key, fetch)
                .await
                .map(|bytes| (bytes, Source::Upstream)),
            None => fetch()
                .await
                .map(|bytes| (bytes, Source::Upstream))
                .map_err(SingleFlightError::Upstream),
        };
        (fetched, refused)
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
            // Reached only when every attempt `read_reserved` makes followed
            // a refused flight: the attempts are capped, so the last
            // unconsulted refusal is reported the same shape a store error
            // would be, rather than retried again.
            SingleFlightError::Upstream(CacheFetchError::BudgetRefused) => {
                ParquetReadError::Store {
                    key,
                    source: Arc::new(StoreError::Transient(
                        "another query's request or byte budget refused the shared read this \
                         one joined"
                            .to_string(),
                    )),
                }
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
    /// the footer is decoded or refused from. A footer whose bytes are refused
    /// as `Corrupt` is cached as refused under that key, so a later read fails
    /// the same way without reading it again. A read that failed is not
    /// cached, whatever it failed on: a store error or a read that came back
    /// short.
    ///
    /// Decoding reserves its estimate from the memory budget. The cache
    /// charges the metadata the larger of its `memory_size` and that
    /// estimate; once it admits the metadata the reservation is released,
    /// and a caller holding the `Arc` after the cache evicts the entry keeps
    /// memory neither the budget nor the cache counts, at most that charge.
    /// A footer whose charge exceeds the cache's whole bound
    /// (`metadata_cache_bytes`) is read uncached: this reader keeps the
    /// metadata and its reservation until its last clone is dropped, so the
    /// metadata the scan holds stays charged to the budget, and a later
    /// call on the reader returns the same metadata without reading or
    /// reserving again.
    pub async fn metadata(&self) -> Result<Arc<ParquetMetaData>, ParquetReadError> {
        if let Some(held) = &*self.uncached.lock().unwrap_or_else(PoisonError::into_inner) {
            return Ok(Arc::clone(&held.metadata));
        }
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
            Ok(decoded) => {
                let admitted = self.services.metadata.insert(
                    cache_key,
                    Arc::clone(&decoded.metadata),
                    decoded.estimate,
                );
                if admitted {
                    drop(decoded.reservation);
                    return Ok(decoded.metadata);
                }
                // A concurrent call on a clone may have kept its own decode
                // first; this one's reservation is then released here.
                let mut held = self.uncached.lock().unwrap_or_else(PoisonError::into_inner);
                let held = held.get_or_insert_with(|| UncachedFooter {
                    metadata: decoded.metadata,
                    _reservation: decoded.reservation,
                });
                Ok(Arc::clone(&held.metadata))
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
    async fn read_footer(&self) -> Result<DecodedFooter, FooterError> {
        let size = self.file.file.size;
        let footer_len = u64::from(self.file.file.footer_len);
        check_footer_len(footer_len).map_err(FooterError::Refused)?;
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
        let recorded = trailer_footer_len(&tail[split..]).map_err(FooterError::Refused)?;
        if recorded != footer_len {
            return Err(FooterError::Refused(format!(
                "the trailer records a {recorded}-byte footer, the manifest {footer_len}"
            )));
        }
        decode_footer(&tail[..split], size - tail_len, |bytes| {
            self.limits.reserve(bytes)
        })
        .map_err(|err| match err {
            DecodeError::Refused(message) => FooterError::Refused(message),
            DecodeError::Reserve(err) => FooterError::Read(err),
        })
    }
}

/// The footer length an 8-byte Parquet trailer records, refusing a trailer
/// without the magic, an encrypted footer, and a footer longer than
/// [`MAX_FOOTER_BYTES`].
pub(crate) fn trailer_footer_len(trailer: &[u8]) -> Result<u64, String> {
    let trailer: [u8; TRAILER_LEN as usize] = trailer
        .try_into()
        .map_err(|_| format!("footer trailer: {} bytes, not {TRAILER_LEN}", trailer.len()))?;
    let footer = FooterTail::try_from(trailer).map_err(|err| format!("footer trailer: {err}"))?;
    if footer.is_encrypted_footer() {
        return Err("the footer is encrypted".to_string());
    }
    let footer_len = footer.metadata_length() as u64;
    check_footer_len(footer_len)?;
    Ok(footer_len)
}

fn check_footer_len(footer_len: u64) -> Result<(), String> {
    if footer_len > MAX_FOOTER_BYTES {
        return Err(format!(
            "the footer is {footer_len} bytes, longer than the {MAX_FOOTER_BYTES}-byte limit"
        ));
    }
    Ok(())
}

/// Why [`decode_footer`] did not hand a footer out.
pub(crate) enum DecodeError<E> {
    /// The footer is refused; the same bytes are refused the same way again.
    Refused(String),
    /// The memory its decoding needs could not be reserved.
    Reserve(E),
}

/// A footer [`decode_footer`] decoded and checked.
pub(crate) struct DecodedFooter {
    pub(crate) metadata: Arc<ParquetMetaData>,
    /// The estimate of what decoding the footer allocates.
    pub(crate) estimate: u64,
    /// The estimate, reserved. The metadata and what is derived from it
    /// outlive the decode, so the caller releases this only once whatever
    /// it keeps of them is charged elsewhere.
    pub(crate) reservation: Reservation,
}

/// Decode `footer`, the thrift footer of a file whose column chunks lie in
/// its first `data_end` bytes, and run every check the scan relies on:
/// [`check_chunks`], the page index removal, and [`check_arrow_schema`].
/// The footer's shape is checked first ([`check_footer_shape`]), because the
/// decoder sizes allocations from counts it does not bound, and the
/// estimate of what the decoder allocates that the check returns is taken
/// from `reserve` before decoding and handed back with the metadata.
///
/// The reader releases the reservation once the metadata cache has admitted
/// the metadata, charging it at least this estimate, so that bound is what
/// bounds the metadata after the release. A footer the cache cannot hold is
/// read uncached, and the reader holds its reservation for as long as the
/// scan holds the reader. The Arrow schema a scan converts from the metadata each time
/// it opens the file is within the estimate but is not reserved. The
/// snapshot holds the reservation until it has built the file's schema and
/// dropped the metadata.
pub(crate) fn decode_footer<E>(
    footer: &[u8],
    data_end: u64,
    reserve: impl FnOnce(u64) -> Result<Reservation, E>,
) -> Result<DecodedFooter, DecodeError<E>> {
    let estimate = check_footer_shape(footer).map_err(DecodeError::Refused)?;
    let reservation = reserve(estimate).map_err(DecodeError::Reserve)?;
    // A decoder panic on a malformed footer is refused like a decoder error.
    let metadata = match std::panic::catch_unwind(|| ParquetMetaDataReader::decode_metadata(footer))
    {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(err)) => return Err(DecodeError::Refused(format!("footer: {err}"))),
        Err(_) => {
            return Err(DecodeError::Refused(
                "the footer panicked the parquet decoder".to_string(),
            ));
        }
    };
    check_chunks(&metadata, data_end).map_err(DecodeError::Refused)?;
    let metadata = Arc::new(without_page_index(metadata));
    check_arrow_schema(&metadata).map_err(DecodeError::Refused)?;
    Ok(DecodedFooter {
        metadata,
        estimate,
        reservation,
    })
}

/// Why [`PinnedParquetReader::read_footer`] could not hand a footer out.
enum FooterError {
    /// A read failed, or the memory to read or decode the footer could not
    /// be reserved. A later attempt may succeed, so it is not cached.
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
            // Every range is reserved before the first is read, so a refusal
            // leaves the whole batch unrequested.
            let mut reserved = Vec::with_capacity(ranges.len());
            for range in ranges {
                let len = self
                    .checked_len(&range)
                    .map_err(ParquetReadError::into_parquet)?;
                let reservation = self
                    .limits
                    .reserve(len)
                    .map_err(ParquetReadError::into_parquet)?;
                reserved.push((range, reservation));
            }
            try_join_all(reserved.into_iter().map(|(range, reservation)| {
                self.read_reserved(range, QueryPhase::Scan, reservation)
            }))
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
    limits: ReadLimits,
}

impl PinnedReaderFactory {
    pub fn new(
        tenant: TenantHash,
        table: String,
        version: u64,
        files: Arc<[Arc<PinnedFile>]>,
        services: ReadServices,
        accounting: PhaseAccounting,
        limits: ReadLimits,
    ) -> Self {
        PinnedReaderFactory {
            tenant,
            table,
            version,
            files,
            services,
            accounting,
            limits,
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
            self.limits.clone(),
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
        Fixture, RecordingStore, TENANT, arrow_schema_panicking, assert_file_changed,
        footer_len_of, parquet_bytes, read_all, read_error, read_where, render_rows,
        retype_page_header,
    };
    use datafusion::logical_expr::{Expr, JoinType, col, ident, lit};
    use parquet::file::metadata::PageIndexPolicy;
    use parquet::file::page_index::offset_index::PageLocation;
    use proptest::prelude::*;
    use proptest::sample::Index;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::{ByteLimit, RequestLimit};
    use ravel_types::accounting::AccountedOp;
    use std::task::Poll;

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

    /// A 3-row file (`a`: 1,2,3; `b`: "one","two","three") with a bloom
    /// filter on column `b`, whose bitset bytes are zeroed: every membership
    /// probe against it reads as absent, the shape a corrupted (not merely
    /// absent) filter takes, since the on-disk bitset stays the length and
    /// format its own header describes.
    ///
    /// The header (the filter's algorithm, hash, compression and bitset
    /// size) is the Thrift-compact encoding of four i32 fields, each 1 to 5
    /// bytes, so at most 20 bytes total; parquet-59.3.0's own internal test
    /// `bloom_filter::mod::test_bloom_filter_header_size_assumption` uses
    /// the same bound. The bitset itself is always a whole number of
    /// 32-byte blocks (the SBBF block size), so `bloom_filter_length % 32`
    /// recovers the header's length without decoding it: the header is
    /// shorter than one block, and everything past it is whole blocks.
    fn bloom_filter_with_a_zeroed_bitset() -> Vec<u8> {
        use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use parquet::file::properties::WriterProperties;
        use parquet::schema::types::ColumnPath;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["one", "two", "three"])),
            ],
        )
        .expect("batch");
        let properties = WriterProperties::builder()
            .set_column_bloom_filter_enabled(ColumnPath::from("b"), true)
            .build();
        let mut bytes = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let end = bytes.len() - TRAILER_LEN as usize;
        let metadata = ParquetMetaDataReader::decode_metadata(
            &bytes[end - footer_len_of(&bytes) as usize..end],
        )
        .expect("footer");
        let column = metadata.row_group(0).column(1);
        let offset = usize::try_from(column.bloom_filter_offset().expect("b has a bloom filter"))
            .expect("offset");
        let length = usize::try_from(column.bloom_filter_length().expect("b has a bloom filter"))
            .expect("length");
        let header_len = length % 32;
        assert!(
            (1..=20).contains(&header_len),
            "a plausible bloom filter header length, got {header_len}"
        );
        for byte in &mut bytes[offset + header_len..offset + length] {
            *byte = 0;
        }
        bytes
    }

    /// A corrupt bloom filter bitset must not drop a row the scan should
    /// return: zeroing the bits for `b`'s bloom filter makes every probe
    /// against it read "not present", which a reader that trusted the
    /// filter would use to skip the row group holding `b = 'two'` before
    /// ever reading `b`'s page.
    ///
    /// This rules out setting `bloom_filter_on_read` on a
    /// `TableParquetOptions` copy that never reaches the real
    /// `ParquetSource`: that mistake would leave the real source's default
    /// (`true`) in effect, and this scan would drop the row.
    #[tokio::test]
    async fn a_corrupt_bloom_filter_cannot_drop_rows() {
        let bytes = bloom_filter_with_a_zeroed_bitset();
        let store = Arc::new(MemoryStore::new());
        let fixture = Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(&store, "lake/t/bloom.parquet", Bytes::from(bytes), false)
            .await;
        let table = fixture.provider("t", 1, vec![file], false).await;
        let ctx = fixture.session(&[("t", table)]);
        let got = read_where(&ctx, "t", &["a", "b"], Some(ident("b").eq(lit("two"))))
            .await
            .expect("scan");
        assert_eq!(got, "2|two");
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

    /// Guards: the `check_footer_len` call in `trailer_footer_len`.
    #[test]
    fn a_trailer_recording_more_than_the_limit_is_refused() {
        let trailer = |len: u64| {
            let mut out = u32::try_from(len).expect("u32").to_le_bytes().to_vec();
            out.extend_from_slice(b"PAR1");
            out
        };
        assert_eq!(
            trailer_footer_len(&trailer(MAX_FOOTER_BYTES)),
            Ok(MAX_FOOTER_BYTES)
        );
        assert_eq!(
            trailer_footer_len(&trailer(MAX_FOOTER_BYTES + 1)),
            Err("the footer is 67108865 bytes, longer than the 67108864-byte limit".to_string())
        );
    }

    /// Guards: the `check_footer_len` call in `read_footer`. A manifest
    /// recording a footer longer than the limit is refused before the
    /// footer is read.
    #[tokio::test]
    async fn a_footer_longer_than_the_limit_is_refused_before_its_read() {
        let (fixture, recording, mut file) = recorded_file(ReadLimits::unlimited()).await;
        file.footer_len = u32::try_from(MAX_FOOTER_BYTES + 1).expect("u32");
        file.size = MAX_FOOTER_BYTES + 1 + TRAILER_LEN;
        let got = fixture.reader(file).metadata().await;
        match got {
            Err(ParquetReadError::Corrupt { message, .. }) => assert_eq!(
                message,
                "the footer is 67108865 bytes, longer than the 67108864-byte limit"
            ),
            other => panic!("expected Corrupt, got {other:?}"),
        }
        assert!(recording.ranges().is_empty(), "{:?}", recording.ranges());
    }

    /// Guards: the `reserve` call in `decode_footer`, at the reader's
    /// decode site. The estimate the walk returns is reserved beside the
    /// footer's own bytes, so a budget one byte short of both refuses the
    /// footer as memory exhausted, and a budget holding both decodes it.
    #[tokio::test]
    async fn decoding_a_footer_reserves_its_estimate() {
        let bytes = parquet_bytes(&[4, 5, 6], &["four", "five", "sixx"]);
        let tail_len = u64::from(footer_len_of(&bytes)) + TRAILER_LEN;
        let end = bytes.len() - TRAILER_LEN as usize;
        let footer = &bytes[end - footer_len_of(&bytes) as usize..end];
        let estimate = check_footer_shape(footer).expect("a writer footer");

        let memory = Arc::new(ravel_memory::MemoryBudget::new(tail_len + estimate - 1));
        let limits = limits_of(&memory, ByteLimit::Unlimited, RequestLimit::Unlimited);
        let (fixture, _, file) = recorded_file(limits).await;
        let got = fixture.reader(file.clone()).metadata().await;
        match got {
            Err(ParquetReadError::MemoryExhausted {
                requested,
                reserved,
                ..
            }) => {
                assert_eq!(requested, estimate);
                assert_eq!(reserved, tail_len);
            }
            other => panic!("expected MemoryExhausted, got {other:?}"),
        }

        let memory = Arc::new(ravel_memory::MemoryBudget::new(tail_len + estimate));
        let limits = limits_of(&memory, ByteLimit::Unlimited, RequestLimit::Unlimited);
        let (fixture, _, file) = recorded_file(limits).await;
        let metadata = fixture.reader(file).metadata().await.expect("decodes");
        assert_eq!(metadata.file_metadata().num_rows(), 3);
    }

    /// Guards: the estimate `MetadataCache::insert` charges. A writer
    /// footer's decoded metadata is smaller than the decode estimate, and
    /// the cache that keeps it once the reservation is released charges it
    /// the estimate.
    #[tokio::test]
    async fn the_metadata_cache_charges_a_decoded_footer_its_estimate() {
        let bytes = parquet_bytes(&[4, 5, 6], &["four", "five", "sixx"]);
        let end = bytes.len() - TRAILER_LEN as usize;
        let footer = &bytes[end - footer_len_of(&bytes) as usize..end];
        let estimate = check_footer_shape(footer).expect("a writer footer");
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let limits = limits_of(&memory, ByteLimit::Unlimited, RequestLimit::Unlimited);
        let (fixture, _, file) = recorded_file(limits).await;
        let metadata = fixture.reader(file).metadata().await.expect("decodes");
        assert!((metadata.memory_size() as u64) < estimate);
        assert_eq!(fixture.services().metadata.resident_bytes(), estimate);
        drop(metadata);
        assert_eq!(memory.reserved(), 0);
    }

    /// The decode estimate of the footer [`recorded_file`] writes.
    fn recorded_footer_estimate() -> u64 {
        let bytes = parquet_bytes(&[4, 5, 6], &["four", "five", "sixx"]);
        let end = bytes.len() - TRAILER_LEN as usize;
        let footer = &bytes[end - footer_len_of(&bytes) as usize..end];
        check_footer_shape(footer).expect("a writer footer")
    }

    /// Guards: the uncached branch of `metadata`, where the reader keeps a
    /// footer the cache did not admit with its reservation. A footer charged
    /// one byte more than the cache's whole bound is handed out, not cached,
    /// and its estimate stays reserved on the budget while the reader lives;
    /// dropping the reader releases it.
    #[tokio::test]
    async fn a_footer_the_metadata_cache_cannot_hold_is_read_uncached_and_stays_reserved() {
        let estimate = recorded_footer_estimate();
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let limits = limits_of(&memory, ByteLimit::Unlimited, RequestLimit::Unlimited);
        let (fixture, recording, file) = recorded_file(limits).await;
        let fixture = fixture.with_metadata_cache_only(estimate - 1);

        let reader = fixture.reader(file);
        let metadata = reader.metadata().await.expect("read uncached");
        assert_eq!(metadata.file_metadata().num_rows(), 3);
        assert_eq!(recording.ranges().len(), 1);
        assert!(fixture.services().metadata.is_empty());
        assert_eq!(memory.reserved(), estimate, "held while the reader lives");

        drop(metadata);
        assert_eq!(memory.reserved(), estimate, "the reader still holds it");
        drop(reader);
        assert_eq!(memory.reserved(), 0);
    }

    /// Guards: the release of the reservation once `MetadataCache::insert`
    /// admits the footer. A bound equal to the charge caches the footer, a
    /// second reader's call is a cache hit, and nothing stays reserved while
    /// either reader lives.
    #[tokio::test]
    async fn a_footer_the_metadata_cache_holds_is_cached_and_released() {
        let estimate = recorded_footer_estimate();
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let limits = limits_of(&memory, ByteLimit::Unlimited, RequestLimit::Unlimited);
        let (fixture, recording, file) = recorded_file(limits).await;
        let fixture = fixture.with_metadata_cache_only(estimate);

        let first = fixture.reader(file.clone());
        let second = fixture.reader(file);
        for reader in [&first, &second] {
            let metadata = reader.metadata().await.expect("fits");
            assert_eq!(metadata.file_metadata().num_rows(), 3);
            assert_eq!(memory.reserved(), 0);
        }
        assert_eq!(
            recording.ranges().len(),
            1,
            "the second call is a cache hit"
        );
        assert_eq!(fixture.services().metadata.resident_bytes(), estimate);
    }

    /// Guards: the check of the reader's held footer at the top of
    /// `metadata`. A second call on the reader, or on a clone of it, returns
    /// the footer it holds: no second read and no second reservation.
    #[tokio::test]
    async fn an_uncached_footer_is_read_and_reserved_once_per_reader() {
        let estimate = recorded_footer_estimate();
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let limits = limits_of(&memory, ByteLimit::Unlimited, RequestLimit::Unlimited);
        let (fixture, recording, file) = recorded_file(limits).await;
        let fixture = fixture.with_metadata_cache_only(estimate - 1);

        let reader = fixture.reader(file);
        let first = reader.metadata().await.expect("read uncached");
        let clone = reader.clone();
        for again in [&reader, &clone] {
            let metadata = again.metadata().await.expect("held");
            assert!(Arc::ptr_eq(&metadata, &first));
            assert_eq!(memory.reserved(), estimate, "reserved once");
        }
        assert_eq!(recording.ranges().len(), 1, "the footer read once");
        drop(reader);
        assert_eq!(memory.reserved(), estimate, "the clone still holds it");
        drop(clone);
        assert_eq!(memory.reserved(), 0);
    }

    /// Guards: the `check_footer_shape` call in `decode_footer`. Without it
    /// the footer reaches the decoder, which reports its own error (or sizes
    /// an allocation from the declared count) instead of this refusal.
    #[tokio::test]
    async fn a_footer_declaring_more_elements_than_bytes_is_refused_before_decoding() {
        // FileMetaData field 5 (key_value_metadata) as a list of structs
        // declaring i32::MAX elements, with no element bytes after it.
        let mut footer = vec![0x59, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07, 0x00];
        let footer_len = u32::try_from(footer.len()).expect("small footer");
        let mut bytes = b"PAR1".to_vec();
        bytes.append(&mut footer);
        bytes.extend_from_slice(&footer_len.to_le_bytes());
        bytes.extend_from_slice(b"PAR1");
        let size = bytes.len() as u64;
        assert_corrupt(
            footer_of(bytes, size, footer_len).await,
            &format!("declares {} elements", i32::MAX),
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
    /// frames is well-formed Thrift that the decoder refuses: a struct that
    /// ends at its first byte, without the fields the decoder requires.
    #[tokio::test]
    async fn a_footer_that_does_not_decode_is_corrupt() {
        let mut bytes = valid();
        let (size, footer_len) = (bytes.len() as u64, footer_len_of(&bytes));
        let end = bytes.len() - TRAILER_LEN as usize;
        bytes[end - footer_len as usize..end].fill(0);
        assert_corrupt(
            footer_of(bytes, size, footer_len).await,
            "footer: Parquet error: Required field version is missing",
        );
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
        let original = valid();
        let (size, footer_len) = (original.len() as u64, footer_len_of(&original));
        let broken = arrow_schema_panicking(&original);

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
            ReadLimits::unlimited(),
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
        let bytes = valid();
        let end = bytes.len() - TRAILER_LEN as usize;
        let metadata = ParquetMetaDataReader::decode_metadata(
            &bytes[end - footer_len_of(&bytes) as usize..end],
        )
        .expect("footer");
        let at = metadata.row_group(0).column(1).data_page_offset();
        let at = usize::try_from(at).expect("offset");
        // Field 1 (`type`) of the compact-protocol PageHeader: DATA_PAGE (0)
        // becomes DATA_PAGE_V2 (3).
        retype_page_header(&bytes, at, 0, 3)
    }

    /// [`valid`] with column `a`'s one data page retyped from `PLAIN` to
    /// `RLE_DICTIONARY`: the file has no dictionary page (`write` disables
    /// dictionary encoding), so nothing ever registers a dictionary decoder
    /// for the column.
    ///
    /// The retyped byte sits inside the nested `DataPageHeader` the page's
    /// own header carries, 9 bytes past [`ParquetMetaData::data_page_offset`]
    /// confirmed by dumping that range: a one-byte `PageHeader.type` field
    /// (`[0x15, 0x00]`, `DATA_PAGE`), a one-byte `uncompressed_page_size`,
    /// a one-byte `compressed_page_size`, the `data_page_header` struct's own
    /// field header, then `DataPageHeader.num_values` before `.encoding`.
    fn data_page_retyped_as_dictionary_encoded() -> Vec<u8> {
        let bytes = valid();
        let end = bytes.len() - TRAILER_LEN as usize;
        let metadata = ParquetMetaDataReader::decode_metadata(
            &bytes[end - footer_len_of(&bytes) as usize..end],
        )
        .expect("footer");
        let at = metadata.row_group(0).column(0).data_page_offset();
        let at = usize::try_from(at).expect("offset") + 9;
        // PLAIN (0) as a zigzag varint becomes RLE_DICTIONARY (8).
        retype_page_header(&bytes, at, 0, 8)
    }

    /// A data page that claims `RLE_DICTIONARY` encoding in a file with no
    /// dictionary page fails the scan with a typed `Corrupt` error naming the
    /// file, not a generic operator error: the boundary's catch_unwind maps
    /// the panic (the parquet crate's column reader expects a dictionary
    /// decoder to already be set for this encoding, and none was) to
    /// `Corrupt` by itself, with no footer-level check needed for this shape
    /// (the footer cannot tell whether a chunk's first page is a dictionary
    /// page).
    ///
    /// Retyping an actual dictionary page's own top-level header as a data
    /// page does not reach this panic in parquet-59.3.0:
    /// `decode_page` has an explicit typed-error path for a mismatch between
    /// a `PageHeader`'s `type` field and the nested header struct it carries,
    /// which a type that no longer matches its own nested header always is.
    /// A data page's nested `DataPageHeader.encoding` field carries no such
    /// cross-check: a page stays a well-formed `DATA_PAGE` whatever value
    /// that field names, so `decode_page` passes it through, and the
    /// unguarded `.expect` in `column/reader/decoder.rs` is reached only once
    /// the column reader tries to use the decoder slot the claimed encoding
    /// names.
    #[test]
    fn a_data_page_claiming_dictionary_encoding_is_refused_as_corrupt() {
        let bytes = data_page_retyped_as_dictionary_encoded();
        let got = scan_second_file_where(bytes, Some(page_pruning_filter()));
        assert_decoder_panic(&got.expect_err("the retyped data page must fail the scan"));
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
    /// A fixture over one stored file, its recording store, and the file.
    async fn recorded_file(limits: ReadLimits) -> (Fixture, Arc<RecordingStore>, ParquetFile) {
        let memory = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&memory), false));
        let fixture =
            Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>).with_limits(limits);
        let file = fixture
            .put_file(
                &memory,
                "lake/t/a.parquet",
                parquet_bytes(&[4, 5, 6], &["four", "five", "sixx"]),
                false,
            )
            .await;
        (fixture, recording, file)
    }

    fn limits_of(
        memory: &Arc<ravel_memory::MemoryBudget>,
        max_bytes: ByteLimit,
        max_requests: RequestLimit,
    ) -> ReadLimits {
        ReadLimits::new(Arc::clone(memory), max_bytes, max_requests)
    }

    /// A range longer than the memory budget is refused with the budget's
    /// figures and no GET for it.
    ///
    /// FLIP: reserving after the GET returns instead of before it leaves one
    /// recorded read here.
    #[tokio::test]
    async fn a_read_past_the_memory_budget_is_refused_before_its_get() {
        let memory = Arc::new(ravel_memory::MemoryBudget::new(10));
        let (fixture, recording, file) = recorded_file(limits_of(
            &memory,
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        ))
        .await;
        let reader = fixture.reader(file);

        let err = reader
            .read_range(0..11, QueryPhase::Scan)
            .await
            .expect_err("eleven bytes over a ten-byte budget");
        assert!(
            matches!(
                err,
                ParquetReadError::MemoryExhausted {
                    requested: 11,
                    reserved: 0,
                    limit: 10
                }
            ),
            "{err:?}"
        );
        assert!(recording.ranges().is_empty(), "{:?}", recording.ranges());
        assert_eq!(memory.reserved(), 0);
    }

    /// The reservation is held until the last clone of the returned bytes
    /// drops, for a read that went to the store and for one the cache served,
    /// and the cached read is marked as handed to the cache's own ledger.
    ///
    /// FLIP: releasing the reservation when the read returns leaves
    /// `reserved()` at 0 while `bytes` is alive.
    #[tokio::test]
    async fn the_reservation_follows_the_returned_bytes() {
        let memory = Arc::new(ravel_memory::MemoryBudget::new(1 << 20));
        let (fixture, recording, file) = recorded_file(limits_of(
            &memory,
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        ))
        .await;
        let reader = fixture.reader(file);

        let fetched = reader
            .read_range(0..16, QueryPhase::Scan)
            .await
            .expect("fetched");
        assert_eq!(recording.ranges().len(), 1);
        assert_eq!(memory.reserved(), 16, "held while the bytes are alive");
        assert_eq!(
            memory.handoff_overlap(),
            16,
            "the cache holds the buffer too"
        );

        let hit = reader
            .read_range(0..16, QueryPhase::Scan)
            .await
            .expect("hit");
        assert_eq!(
            recording.ranges().len(),
            1,
            "the second read is a cache hit"
        );
        assert_eq!(memory.reserved(), 32, "a hit reserves its length too");

        let clone = fetched.clone();
        drop(fetched);
        assert_eq!(memory.reserved(), 32, "a clone keeps the reservation");
        drop(clone);
        assert_eq!(memory.reserved(), 16);
        drop(hit);
        assert_eq!(memory.reserved(), 0);
        assert_eq!(memory.handoff_overlap(), 0);
    }

    /// `get_byte_ranges` reserves every range before it reads any: one range
    /// that does not fit leaves the ranges before it unread.
    ///
    /// FLIP: reserving each range inside its own read future lets the first
    /// range's GET go out before the second is refused.
    #[tokio::test]
    async fn a_batch_with_a_range_over_the_budget_reads_nothing() {
        let memory = Arc::new(ravel_memory::MemoryBudget::new(20));
        let (fixture, recording, file) = recorded_file(limits_of(
            &memory,
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        ))
        .await;
        let mut reader = fixture.reader(file);

        let err = reader
            .get_byte_ranges(vec![0..8, 8..24])
            .await
            .expect_err("8 + 16 bytes over a 20-byte budget");
        assert!(err.to_string().contains("fetch memory exhausted"), "{err}");
        assert!(recording.ranges().is_empty(), "{:?}", recording.ranges());
        assert_eq!(
            memory.reserved(),
            0,
            "the first range's reservation is released"
        );

        let ok = reader
            .get_byte_ranges(vec![0..8, 8..16])
            .await
            .expect("16 bytes fit");
        assert_eq!(ok.iter().map(Bytes::len).sum::<usize>(), 16);
    }

    /// The request that would be the `max_s3_requests + 1`th is refused
    /// before it is issued: the store sees exactly `max` reads.
    ///
    /// FLIP: checking the budget after the GET, as the signal scan does per
    /// segment, leaves three recorded reads.
    #[tokio::test]
    async fn a_read_past_the_request_budget_is_refused_before_its_get() {
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let (fixture, recording, file) = recorded_file(limits_of(
            &memory,
            ByteLimit::Unlimited,
            RequestLimit::Bounded(2),
        ))
        .await;
        let reader = fixture.reader(file);

        reader
            .read_range(0..4, QueryPhase::Scan)
            .await
            .expect("one");
        reader
            .read_range(4..8, QueryPhase::Scan)
            .await
            .expect("two");
        let err = reader
            .read_range(8..12, QueryPhase::Scan)
            .await
            .expect_err("three");
        assert!(
            matches!(
                err,
                ParquetReadError::RequestBudgetExceeded {
                    requests: 3,
                    max: 2
                }
            ),
            "{err:?}"
        );
        assert_eq!(recording.ranges().len(), 2);
        reader
            .read_range(0..4, QueryPhase::Scan)
            .await
            .expect("a cache hit issues no request");
        assert_eq!(recording.ranges().len(), 2);
    }

    /// A query whose own budget would have admitted the read must not fail
    /// because ANOTHER query's leader, sharing its cache single flight, was
    /// refused by ITS budget: the follower retries once, under its own
    /// budget, and succeeds.
    ///
    /// The leader's own precheck passes (budget `Bounded(1)`, nothing
    /// consumed yet); while its closure is parked on the shared one-permit
    /// `GetLimiter`, a direct `admit` call simulates a second read from the
    /// leader's own query consuming the one request its budget allows, so
    /// the leader's real `admit` -- not its `precheck` -- is what refuses it,
    /// the path the single flight actually shares with followers.
    ///
    /// FLIP 1: returning `CacheFetchError::Store` from the admit-refusal
    /// branch (the pre-fix shape) instead of `BudgetRefused` makes the
    /// follower receive an un-typed store error and never retry: it ends in
    /// `ParquetReadError::Store` instead of succeeding.
    /// FLIP 2: mapping a follower's `BudgetRefused` straight to the
    /// follower's own typed budget error without an actual retried fetch
    /// leaves `recording.ranges()` empty instead of holding the one real GET
    /// the retry issues.
    #[tokio::test]
    async fn a_follower_of_a_refused_leader_retries_under_its_own_budget() {
        let store = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&store), false));
        let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(
                &store,
                "lake/t/a.parquet",
                parquet_bytes(&[1, 2, 3], &["one", "two", "tre"]),
                false,
            )
            .await;
        let pinned = Arc::new(PinnedFile {
            file,
            store: Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>,
        });
        let base = fixture.services();
        let limiter = Arc::new(GetLimiter::new(1).expect("1 permit is valid"));
        let services = ReadServices {
            limiter: Arc::clone(&limiter),
            cache: base.cache.clone(),
            metadata: Arc::clone(&base.metadata),
        };

        let leader_limits = ReadLimits::new(
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            ByteLimit::Unlimited,
            RequestLimit::Bounded(1),
        );
        let leader_accounting = PhaseAccounting::new();
        let leader = PinnedParquetReader::new(
            TENANT,
            Arc::clone(&pinned),
            services.clone(),
            leader_accounting.clone(),
            leader_limits.clone(),
        );

        let follower = PinnedParquetReader::new(
            TENANT,
            Arc::clone(&pinned),
            services.clone(),
            PhaseAccounting::new(),
            ReadLimits::unlimited(),
        );

        let permit = limiter.acquire().await.expect("semaphore is never closed");

        let mut leader_fut = Box::pin(leader.read_range(0..4, QueryPhase::Scan));
        let leader_parked = std::future::poll_fn(|cx| {
            Poll::Ready(std::future::Future::poll(leader_fut.as_mut(), cx))
        })
        .await;
        assert!(
            leader_parked.is_pending(),
            "the leader parks on the held GetLimiter permit, its flight already registered"
        );

        let mut follower_fut = Box::pin(follower.read_range(0..4, QueryPhase::Scan));
        let follower_parked = std::future::poll_fn(|cx| {
            Poll::Ready(std::future::Future::poll(follower_fut.as_mut(), cx))
        })
        .await;
        assert!(
            follower_parked.is_pending(),
            "the follower joins the leader's flight and waits on its result"
        );

        // A second read from the leader's own query, between its precheck
        // and its admit, consuming the one request its budget allows.
        leader_limits
            .admit(&leader_accounting, QueryPhase::Scan, 1)
            .expect("the first admission fits the bounded-1 budget")
            .complete(&leader_accounting, QueryPhase::Scan, 1);

        drop(permit);

        let leader_err = leader_fut
            .await
            .expect_err("the leader's own admit is now refused");
        assert!(
            matches!(
                leader_err,
                ParquetReadError::RequestBudgetExceeded {
                    requests: 2,
                    max: 1
                }
            ),
            "{leader_err:?}"
        );

        let follower_bytes = follower_fut
            .await
            .expect("the follower retries under its own, unexhausted budget");
        assert_eq!(&follower_bytes[..], b"PAR1", "the file's first 4 bytes");

        assert_eq!(
            recording.ranges().len(),
            1,
            "only the follower's retried GET reaches the store: the leader's \
             own admit failed before it issued one"
        );
    }

    /// A follower's attempts are capped at `MAX_ATTEMPTS` (3): if every
    /// attempt joins a refused leader's flight (its own budget never
    /// consulted), it reports the shape a store error takes rather than
    /// retrying forever.
    ///
    /// Four readers share one key: A, C and D each lead one flight in turn
    /// and are refused on their own admit (as above); B is the follower under
    /// test, and each of its three attempts joins one of those flights. B's
    /// own budget is left untouched throughout, so the capped read ends in an
    /// error with no GET ever issued and B's budget never consulted; a cap of
    /// 4 or more would instead make B lead a fourth attempt, admit clean
    /// against its own untouched budget, and complete the read.
    ///
    /// FLIP: raising `MAX_ATTEMPTS` to 4 (or removing the `attempt <
    /// MAX_ATTEMPTS` bound) makes B's `read_range` resolve `Ok` with one real
    /// GET recorded and one request on B's accounting, instead of the single
    /// `Store` error with none. A cap of 2 fails
    /// `a_follower_whose_retry_follows_a_refused_flight_is_attempted_again`.
    #[tokio::test]
    async fn a_follower_reports_its_refusal_once_every_attempt_follows_a_refused_flight() {
        let store = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&store), false));
        let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
        let file = fixture
            .put_file(
                &store,
                "lake/t/a.parquet",
                parquet_bytes(&[1, 2, 3], &["one", "two", "tre"]),
                false,
            )
            .await;
        let pinned = Arc::new(PinnedFile {
            file,
            store: Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>,
        });
        let base = fixture.services();
        let limiter = Arc::new(GetLimiter::new(1).expect("1 permit is valid"));
        let services = ReadServices {
            limiter: Arc::clone(&limiter),
            cache: base.cache.clone(),
            metadata: Arc::clone(&base.metadata),
        };

        let a_limits = ReadLimits::new(
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            ByteLimit::Unlimited,
            RequestLimit::Bounded(1),
        );
        let a_accounting = PhaseAccounting::new();
        let a = PinnedParquetReader::new(
            TENANT,
            Arc::clone(&pinned),
            services.clone(),
            a_accounting.clone(),
            a_limits.clone(),
        );

        let b_accounting = PhaseAccounting::new();
        let b = PinnedParquetReader::new(
            TENANT,
            Arc::clone(&pinned),
            services.clone(),
            b_accounting.clone(),
            ReadLimits::unlimited(),
        );

        let c_limits = ReadLimits::new(
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            ByteLimit::Unlimited,
            RequestLimit::Bounded(1),
        );
        let c_accounting = PhaseAccounting::new();
        let c = PinnedParquetReader::new(
            TENANT,
            Arc::clone(&pinned),
            services.clone(),
            c_accounting.clone(),
            c_limits.clone(),
        );

        let d_limits = ReadLimits::new(
            Arc::new(ravel_memory::MemoryBudget::unlimited()),
            ByteLimit::Unlimited,
            RequestLimit::Bounded(1),
        );
        let d_accounting = PhaseAccounting::new();
        let d = PinnedParquetReader::new(
            TENANT,
            Arc::clone(&pinned),
            services.clone(),
            d_accounting.clone(),
            d_limits.clone(),
        );

        // Flight 1: A leads, parked on the held permit.
        let permit = limiter.acquire().await.expect("semaphore is never closed");
        let mut a_fut = Box::pin(a.read_range(0..4, QueryPhase::Scan));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(a_fut.as_mut(), cx)))
                .await
                .is_pending(),
            "A parks on the held permit, its flight registered"
        );

        let mut b_fut = Box::pin(b.read_range(0..4, QueryPhase::Scan));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(b_fut.as_mut(), cx)))
                .await
                .is_pending(),
            "B joins A's flight as a follower"
        );

        a_limits
            .admit(&a_accounting, QueryPhase::Scan, 1)
            .expect("A's first admission fits its bounded-1 budget")
            .complete(&a_accounting, QueryPhase::Scan, 1);
        drop(permit);

        let a_err = a_fut.await.expect_err("A's own admit is now refused");
        assert!(
            matches!(
                a_err,
                ParquetReadError::RequestBudgetExceeded {
                    requests: 2,
                    max: 1
                }
            ),
            "{a_err:?}"
        );
        assert!(recording.ranges().is_empty(), "A's flight issued no GET");

        // Flight 2: C leads, parked on the permit A's flight just freed; B's
        // retry (driven by resuming its parked future below) joins it.
        let permit = limiter.acquire().await.expect("semaphore is never closed");
        let mut c_fut = Box::pin(c.read_range(0..4, QueryPhase::Scan));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(c_fut.as_mut(), cx)))
                .await
                .is_pending(),
            "C parks on the held permit, leading the second flight"
        );

        assert!(
            std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(b_fut.as_mut(), cx)))
                .await
                .is_pending(),
            "B's own retry joins C's flight as a follower again"
        );

        c_limits
            .admit(&c_accounting, QueryPhase::Scan, 1)
            .expect("C's first admission fits its bounded-1 budget")
            .complete(&c_accounting, QueryPhase::Scan, 1);
        drop(permit);

        let c_err = c_fut.await.expect_err("C's own admit is now refused");
        assert!(
            matches!(
                c_err,
                ParquetReadError::RequestBudgetExceeded {
                    requests: 2,
                    max: 1
                }
            ),
            "{c_err:?}"
        );

        // Flight 3: D leads before B is polled again, so B's second retry
        // (its third and last attempt) joins D's flight.
        let permit = limiter.acquire().await.expect("semaphore is never closed");
        let mut d_fut = Box::pin(d.read_range(0..4, QueryPhase::Scan));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(d_fut.as_mut(), cx)))
                .await
                .is_pending(),
            "D parks on the held permit, leading the third flight"
        );
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(b_fut.as_mut(), cx)))
                .await
                .is_pending(),
            "B's second retry joins D's flight as a follower once more"
        );
        d_limits
            .admit(&d_accounting, QueryPhase::Scan, 1)
            .expect("D's first admission fits its bounded-1 budget")
            .complete(&d_accounting, QueryPhase::Scan, 1);
        drop(permit);
        let d_err = d_fut.await.expect_err("D's own admit is now refused");
        assert!(
            matches!(
                d_err,
                ParquetReadError::RequestBudgetExceeded {
                    requests: 2,
                    max: 1
                }
            ),
            "{d_err:?}"
        );

        let b_err = b_fut.await.expect_err(
            "B's third attempt is its last: the unconsulted refusal is \
             reported, not retried again",
        );
        assert!(matches!(b_err, ParquetReadError::Store { .. }), "{b_err:?}");
        assert!(
            b_err.to_string().contains(
                "another query's request or byte budget refused the shared read this one joined"
            ),
            "the error blames the flight B joined, not B's own budget: {b_err}"
        );
        assert!(
            recording.ranges().is_empty(),
            "no flight, nor a fourth attempt under B's own untouched budget, \
             ever issued a GET"
        );
        assert_eq!(
            b_accounting
                .snapshot()
                .phase(QueryPhase::Scan)
                .s3_requests(AccountedOp::Get),
            0,
            "B's own budget was never consulted: it never led an attempt"
        );
    }

    /// Readers over one file, one cache and one one-permit `GetLimiter`, so a
    /// test holds the permit to park a leader's closure and drives the
    /// single-flight interleavings by hand.
    struct RefusalRig {
        recording: Arc<RecordingStore>,
        pinned: Arc<PinnedFile>,
        services: ReadServices,
    }

    /// A leader whose budget allows one request, parked in its closure.
    struct Leader {
        fut: std::pin::Pin<Box<dyn std::future::Future<Output = Result<Bytes, ParquetReadError>>>>,
        limits: ReadLimits,
        accounting: PhaseAccounting,
    }

    impl RefusalRig {
        async fn build(
            fixture: Fixture,
            store: &Arc<MemoryStore>,
            recording: Arc<RecordingStore>,
        ) -> Self {
            let file = fixture
                .put_file(
                    store,
                    "lake/t/a.parquet",
                    parquet_bytes(&[1, 2, 3], &["one", "two", "tre"]),
                    false,
                )
                .await;
            let pinned = Arc::new(PinnedFile {
                file,
                store: Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>,
            });
            let base = fixture.services();
            let services = ReadServices {
                limiter: Arc::new(GetLimiter::new(1).expect("1 permit is valid")),
                cache: base.cache.clone(),
                metadata: Arc::clone(&base.metadata),
            };
            RefusalRig {
                recording,
                pinned,
                services,
            }
        }

        async fn ram() -> Self {
            let store = Arc::new(MemoryStore::new());
            let recording = Arc::new(RecordingStore::new(Arc::clone(&store), false));
            let fixture = Fixture::new(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>);
            Self::build(fixture, &store, recording).await
        }

        async fn tiered(dir: &std::path::Path) -> Self {
            let store = Arc::new(MemoryStore::new());
            let recording = Arc::new(RecordingStore::new(Arc::clone(&store), false));
            let fixture =
                Fixture::new_tiered(Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>, dir);
            Self::build(fixture, &store, recording).await
        }

        fn reader(&self, limits: ReadLimits, accounting: &PhaseAccounting) -> PinnedParquetReader {
            PinnedParquetReader::new(
                TENANT,
                Arc::clone(&self.pinned),
                self.services.clone(),
                accounting.clone(),
                limits,
            )
        }

        fn bounded_one() -> ReadLimits {
            ReadLimits::new(
                Arc::new(ravel_memory::MemoryBudget::unlimited()),
                ByteLimit::Unlimited,
                RequestLimit::Bounded(1),
            )
        }

        fn tiered_cache(&self) -> &ravel_cache::TieredCache<CacheFetchError> {
            let Some(ReadCache::Tiered(tiered)) = &self.services.cache else {
                panic!("rig built with a Tiered cache");
            };
            tiered
        }

        /// Start a leader for bytes 0..4 and drive it until `parked` holds.
        async fn leader_until(&self, parked: impl Fn() -> bool) -> Leader {
            let limits = Self::bounded_one();
            let accounting = PhaseAccounting::new();
            let reader = self.reader(limits.clone(), &accounting);
            let mut fut: std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Bytes, ParquetReadError>>>,
            > = Box::pin(async move { reader.read_range(0..4, QueryPhase::Scan).await });
            drive_until(&mut fut, parked).await;
            Leader {
                fut,
                limits,
                accounting,
            }
        }
    }

    impl Leader {
        /// Spend the leader's one request elsewhere, release the permit it is
        /// parked on, and check its closure's own admit refused it.
        async fn refuse<P>(self, permit: P) {
            self.limits
                .admit(&self.accounting, QueryPhase::Scan, 1)
                .expect("the first admission fits the bounded-1 budget")
                .complete(&self.accounting, QueryPhase::Scan, 1);
            drop(permit);
            let err = self.fut.await.expect_err("the leader's own admit refuses");
            assert!(
                matches!(
                    err,
                    ParquetReadError::RequestBudgetExceeded {
                        requests: 2,
                        max: 1
                    }
                ),
                "{err:?}"
            );
        }
    }

    /// Poll `fut` until it has been pending and `parked` holds. A disk peek
    /// finishes on another thread, so a Tiered caller may need several polls
    /// to reach the flight; the timeout only turns a hang into a failure.
    async fn drive_until<F>(fut: &mut std::pin::Pin<Box<F>>, parked: impl Fn() -> bool)
    where
        F: std::future::Future + ?Sized,
    {
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                let polled = std::future::poll_fn(|cx| {
                    Poll::Ready(std::future::Future::poll(fut.as_mut(), cx))
                })
                .await;
                assert!(polled.is_pending(), "the caller parks, it does not finish");
                if parked() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the caller reaches the state the test parks it in");
    }

    /// A retry can join the very flight that just refused (the leader
    /// publishes its result before it clears its slot), and that flight's
    /// stored refusal must not use up the follower's last attempt. The single
    /// flight is private to the cache and cannot be held between publishing
    /// and clearing from here, so this drives the state the reader sees: B's
    /// first retry follows a second refused flight, and B must still get a
    /// further attempt under its own budget.
    ///
    /// A leads flight 1 and is refused; B follows it, retries, and follows
    /// C's flight 2, which is refused too; B's third attempt leads, admits
    /// under B's own unlimited budget, and issues the one GET. B's accounting
    /// holds one cache miss for the one logical read, not one per attempt.
    ///
    /// FLIP: capping the attempts at 2 (`MAX_ATTEMPTS = 2`, the earlier
    /// one-retry shape) ends B in `ParquetReadError::Store` with no GET.
    /// Counting a cache miss per attempt reads 3 misses instead of 1.
    #[tokio::test]
    async fn a_follower_whose_retry_follows_a_refused_flight_is_attempted_again() {
        let rig = RefusalRig::ram().await;
        let b_accounting = PhaseAccounting::new();
        let b = rig.reader(ReadLimits::unlimited(), &b_accounting);

        let permit = rig.services.limiter.acquire().await.expect("open");
        let a = rig.leader_until(|| true).await;
        let mut b_fut = Box::pin(b.read_range(0..4, QueryPhase::Scan));
        drive_until(&mut b_fut, || true).await;
        a.refuse(permit).await;

        let permit = rig.services.limiter.acquire().await.expect("open");
        let c = rig.leader_until(|| true).await;
        drive_until(&mut b_fut, || true).await;
        c.refuse(permit).await;

        let bytes = b_fut
            .await
            .expect("B's third attempt leads under its own budget");
        assert_eq!(&bytes[..], b"PAR1", "the file's first 4 bytes");
        assert_eq!(rig.recording.ranges().len(), 1, "one GET: B's own");
        let scan = b_accounting.snapshot();
        let scan = scan.phase(QueryPhase::Scan);
        assert_eq!(scan.cache_misses, 1, "one miss per read, not per attempt");
        assert_eq!(scan.cache_hits, 0);
    }

    /// A follower whose retry leads and is refused by its OWN budget returns
    /// that typed budget error, not the store-shaped follower error.
    ///
    /// A leads and is refused; B follows with a one-request budget. B's retry
    /// leads and parks on the permit; B's budget is then spent elsewhere, so
    /// its own closure's admit refuses it.
    ///
    /// FLIP: removing the `refused.take()` early return in `read_reserved`, so
    /// the mapped fetch error is reported instead of the retry's own typed
    /// error, yields `ParquetReadError::Store` instead of
    /// `RequestBudgetExceeded { requests: 2, max: 1 }`.
    #[tokio::test]
    async fn a_follower_that_leads_its_retry_returns_its_own_budget_error() {
        let rig = RefusalRig::ram().await;
        let b_limits = RefusalRig::bounded_one();
        let b_accounting = PhaseAccounting::new();
        let b = rig.reader(b_limits.clone(), &b_accounting);

        let permit = rig.services.limiter.acquire().await.expect("open");
        let a = rig.leader_until(|| true).await;
        let mut b_fut = Box::pin(b.read_range(0..4, QueryPhase::Scan));
        drive_until(&mut b_fut, || true).await;
        a.refuse(permit).await;

        let permit = rig.services.limiter.acquire().await.expect("open");
        drive_until(&mut b_fut, || true).await;
        b_limits
            .admit(&b_accounting, QueryPhase::Scan, 1)
            .expect("the first admission fits the bounded-1 budget")
            .complete(&b_accounting, QueryPhase::Scan, 1);
        drop(permit);

        let err = b_fut.await.expect_err("B's own admit refuses its retry");
        assert!(
            matches!(
                err,
                ParquetReadError::RequestBudgetExceeded {
                    requests: 2,
                    max: 1
                }
            ),
            "{err:?}"
        );
        assert!(rig.recording.ranges().is_empty());
    }

    /// Each retry peeks the cache again: a range another query put in the
    /// disk tier while the follower waited is served from cache, with no GET.
    ///
    /// A leads and parks; B peeks, misses and follows. The bytes land in the
    /// disk tier only, then A is refused. `resolve_peeked_miss` never
    /// consults the disk tier, so only a re-peek can find them.
    ///
    /// FLIP: retrying straight into `fetch_once` without the peek makes B
    /// lead, find nothing in the RAM recheck and issue a GET: the bytes read
    /// are `PAR1` and the recording holds one range.
    #[tokio::test]
    async fn a_retry_re_peeks_the_cache_and_is_served_without_a_get() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rig = RefusalRig::tiered(dir.path()).await;
        let tiered = rig.tiered_cache();
        let b_accounting = PhaseAccounting::new();
        let b = rig.reader(ReadLimits::unlimited(), &b_accounting);
        let key = b.cache_key(0, 4);

        let permit = rig.services.limiter.acquire().await.expect("open");
        let a = rig.leader_until(|| tiered.is_in_flight(&key)).await;
        let mut b_fut = Box::pin(b.read_range(0..4, QueryPhase::Scan));
        drive_until(&mut b_fut, || tiered.in_flight_waiters(&key) == 1).await;
        // Both first peeks (A's and B's) are counted by now; the retry's
        // re-peek must add nothing to either tier's own metrics.
        let ram_before = tiered.ram_metrics().snapshot();
        let disk_before = tiered.disk_metrics().snapshot();

        tiered.disk_for_test().insert(key, b"efgh");
        a.refuse(permit).await;

        let bytes = b_fut.await.expect("B's retry is served from the disk tier");
        assert_eq!(&bytes[..], b"efgh", "the bytes another query cached");
        assert!(rig.recording.ranges().is_empty(), "no GET for the retry");
        let scan = b_accounting.snapshot();
        let scan = scan.phase(QueryPhase::Scan);
        assert_eq!((scan.cache_hits, scan.cache_misses), (1, 0));
        let ram_after = tiered.ram_metrics().snapshot();
        let disk_after = tiered.disk_metrics().snapshot();
        assert_eq!(
            (ram_after.hits, ram_after.misses),
            (ram_before.hits, ram_before.misses),
            "the retry's re-peek records nothing on the RAM tier"
        );
        assert_eq!(
            (disk_after.hits, disk_after.misses),
            (disk_before.hits, disk_before.misses),
            "the retry's re-peek records nothing on the disk tier"
        );
    }

    /// A range whose body would take the wire bytes past `max_bytes_scanned`
    /// is refused before its GET; one that lands exactly on the budget is not.
    ///
    /// FLIP: comparing only the bytes already recorded (not the range about to
    /// be read) admits the 9-byte read and leaves two recorded reads.
    #[tokio::test]
    async fn a_read_past_the_byte_budget_is_refused_before_its_get() {
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let (fixture, recording, file) = recorded_file(limits_of(
            &memory,
            ByteLimit::Bounded(16),
            RequestLimit::Unlimited,
        ))
        .await;
        let reader = fixture.reader(file);

        reader.read_range(0..8, QueryPhase::Scan).await.expect("8");
        let err = reader
            .read_range(8..17, QueryPhase::Scan)
            .await
            .expect_err("8 + 9 bytes over a 16-byte budget");
        assert!(
            matches!(
                err,
                ParquetReadError::BytesBudgetExceeded {
                    scanned: 17,
                    max: 16
                }
            ),
            "{err:?}"
        );
        assert_eq!(recording.ranges().len(), 1);
        reader
            .read_range(8..16, QueryPhase::Scan)
            .await
            .expect("lands exactly on the budget");
        assert_eq!(recording.ranges().len(), 2);
    }

    /// A `Tiered` cache (production's `--cache-dir` configuration) serves a
    /// hit from either tier with zero new requests, even at the request
    /// budget: both tiers are consulted before `precheck`, not after.
    ///
    /// FLIP: consulting only the RAM tier before `precheck`, as
    /// `Some(ReadCache::Tiered(_)) | None => None` used to, runs `precheck`
    /// first for a Tiered cache and refuses both reads here.
    #[tokio::test]
    async fn a_tiered_cache_hit_is_admitted_at_the_request_budget() {
        let dir = tempfile::tempdir().expect("tempdir");
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let store = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&store), false));
        let fixture = Fixture::new_tiered(
            Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>,
            dir.path(),
        )
        .with_limits(limits_of(
            &memory,
            ByteLimit::Unlimited,
            RequestLimit::Bounded(0),
        ));
        let file = fixture
            .put_file(
                &store,
                "lake/t/a.parquet",
                parquet_bytes(&[4, 5, 6], &["four", "five", "sixx"]),
                false,
            )
            .await;
        let reader = fixture.reader(file);
        let Some(ReadCache::Tiered(tiered)) = &fixture.services().cache else {
            panic!("fixture built with a Tiered cache");
        };

        // RAM-tier hit.
        let ram_key = reader.cache_key(0, 4);
        tiered.insert(ram_key, Bytes::from_static(b"abcd"));
        reader
            .read_range(0..4, QueryPhase::Scan)
            .await
            .expect("a RAM-tier hit needs no request, even at a zero-request budget");
        assert!(recording.ranges().is_empty(), "{:?}", recording.ranges());

        // Disk-tier hit: the RAM tier misses, the disk tier alone holds the
        // bytes.
        let disk_key = reader.cache_key(4, 4);
        tiered.disk_for_test().insert(disk_key, b"efgh");
        reader
            .read_range(4..8, QueryPhase::Scan)
            .await
            .expect("a disk-tier hit needs no request, even at a zero-request budget");
        assert!(recording.ranges().is_empty(), "{:?}", recording.ranges());
    }

    /// A full miss on a `Tiered` cache consults the disk tier exactly once:
    /// the peek in `read_range` (`get_off_worker`) already consulted both
    /// tiers, so resolving the confirmed miss must not consult the disk tier
    /// again. The leader's one RAM recheck is uncounted and touches no disk.
    ///
    /// FLIP: resolving through `TieredCache::get_or_fetch` instead of
    /// `resolve_peeked_miss` (the pre-fix code) makes the leader consult the
    /// disk tier a second time, so the disk tier's own miss counter reads 2
    /// for this one logical read instead of 1.
    #[tokio::test]
    async fn a_tiered_full_miss_consults_disk_exactly_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let store = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&store), false));
        let fixture = Fixture::new_tiered(
            Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>,
            dir.path(),
        )
        .with_limits(limits_of(
            &memory,
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        ));
        let file = fixture
            .put_file(
                &store,
                "lake/t/a.parquet",
                parquet_bytes(&[1, 2, 3], &["one", "two", "tre"]),
                false,
            )
            .await;
        let reader = fixture.reader(file);
        let Some(ReadCache::Tiered(tiered)) = &fixture.services().cache else {
            panic!("fixture built with a Tiered cache");
        };
        let disk_metrics = tiered.disk_metrics();
        let before = disk_metrics.snapshot();

        reader
            .read_range(0..4, QueryPhase::Scan)
            .await
            .expect("a full miss resolves through the upstream fetch");

        let after = disk_metrics.snapshot();
        assert_eq!(
            after.misses,
            before.misses + 1,
            "the disk tier is consulted exactly once per logical miss: once \
             by the peek, not again while resolving it"
        );
        assert_eq!(
            recording.ranges().len(),
            1,
            "one GET for one logical full miss"
        );
    }

    /// 8 concurrent `read_range` misses on one range, each peeking both tiers
    /// (`get_off_worker`) while the leader's GET is in flight, collapse onto
    /// one upstream GET and every caller receives identical bytes.
    ///
    /// A `FaultStore` gate holds the leader's GET open until the leader and
    /// six parked followers have peeked and missed; those six are counted by
    /// `TieredCache::in_flight_waiters`. The eighth caller's disk peek is
    /// held open by `DiskCache::block_peek_for_test` (issue #2347) just long
    /// enough to prove it has parked there rather than already joined the
    /// flight, then released at once, so its miss is recorded the same as a
    /// merely slow real disk peek would, well before the leader's GET is
    /// released and bytes are admitted. Its future is then left unpolled
    /// until after the leader and the six followers have resolved and left
    /// the single-flight map, so the continuation past the peek --
    /// `resolve_peeked_miss`'s leader RAM recheck -- runs only once that
    /// flight is gone. This constructs deterministically the interleaving a
    /// slow `spawn_blocking` disk peek produces on a loaded machine -- which
    /// is how this test once saw two GETs -- instead of sampling it under
    /// real scheduling.
    ///
    /// FLIP: removing the leader's `self.ram.get_uncounted(&key)` check in
    /// `TieredCache::resolve_peeked_miss` lets the eighth caller lead a
    /// second flight with its own GET, so the GET count reads 2.
    #[tokio::test]
    async fn a_tiered_concurrent_miss_collapses_to_one_get() {
        let dir = tempfile::tempdir().expect("tempdir");
        let memory = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let store = Arc::new(MemoryStore::new());
        let recording = Arc::new(RecordingStore::new(Arc::clone(&store), false));
        let faults = Arc::new(FaultStore::new(Arc::clone(&recording), FaultPlan::empty()));
        let gate = faults.hold(Op::Get, None, Occurrence::Nth(1));
        let fixture = Fixture::new_tiered(
            Arc::clone(&faults) as Arc<dyn ObjectStoreBackend>,
            dir.path(),
        )
        .with_limits(limits_of(
            &memory,
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        ));
        let file = fixture
            .put_file(
                &store,
                "lake/t/a.parquet",
                parquet_bytes(&[7, 8, 9], &["sev", "eig", "nin"]),
                false,
            )
            .await;
        let reader = fixture.reader(file);
        let Some(ReadCache::Tiered(tiered)) = &fixture.services().cache else {
            panic!("fixture built with a Tiered cache");
        };
        let key = reader.cache_key(0, 4);
        let disk_metrics = tiered.disk_metrics();
        let before_misses = disk_metrics.snapshot().misses;

        const CALLERS: usize = 8;
        const PARKED: usize = CALLERS - 2;
        let leader = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read_range(0..4, QueryPhase::Scan).await })
        };
        gate.wait_until_held(1).await;

        // Gate the late caller's disk peek itself rather than sampling
        // whether it is still pending: the next `get`/`get_uncounted` of
        // `key` parks before touching disk until released below.
        let mut disk_peek = tiered.disk_for_test().block_peek_for_test(key);
        let mut late = Box::pin(reader.read_range(0..4, QueryPhase::Scan));
        let first =
            std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(late.as_mut(), cx)))
                .await;
        assert!(
            first.is_pending(),
            "the late caller waits on its gated disk peek"
        );
        tokio::time::timeout(std::time::Duration::from_secs(60), disk_peek.entered())
            .await
            .expect("the late caller's disk peek reaches its gate");
        assert_eq!(
            tiered.in_flight_waiters(&key),
            0,
            "the late caller is parked on its disk peek, not on any flight yet"
        );

        // Release the late caller's disk peek now, before the leader's GET
        // (and so before the leader ever admits bytes to disk): its read
        // finds nothing on disk and records a genuine miss, same as it would
        // on a real, merely slow disk. Its future is not polled again until
        // `late.await` far below, so the continuation past the peek (joining
        // or leading `resolve_peeked_miss`) waits for that poll regardless of
        // how fast the peek itself resolves.
        disk_peek.release();

        let parked: Vec<_> = (0..PARKED)
            .map(|_| {
                let reader = reader.clone();
                tokio::spawn(async move { reader.read_range(0..4, QueryPhase::Scan).await })
            })
            .collect();

        // Every caller's disk peek -- the leader's, the six parked
        // followers', and the late caller's released above -- misses and is
        // counted here. The timeout only turns a hang into a failure.
        let peeks = CALLERS as u64;
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            while disk_metrics.snapshot().misses - before_misses < peeks
                || tiered.in_flight_waiters(&key) < PARKED
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the leader, the six parked callers, and the late caller all peek and miss");
        assert_eq!(
            tiered.in_flight_waiters(&key),
            PARKED,
            "exactly the parked callers follow the leader; the late caller does not"
        );
        assert_eq!(gate.held_count(), 1, "only the leader's GET was issued");
        assert!(recording.ranges().is_empty(), "{:?}", recording.ranges());

        for id in gate.held() {
            gate.release(id);
        }
        let mut served = vec![
            leader
                .await
                .expect("leader task")
                .expect("the leader's read resolves"),
        ];
        for task in parked {
            served.push(
                task.await
                    .expect("parked task")
                    .expect("every parked caller resolves"),
            );
        }
        assert!(
            !tiered.is_in_flight(&key),
            "the leader's flight has finished and left the map before the late caller resumes"
        );

        // Only now is the late caller's future polled again: its disk peek
        // missed and recorded long ago, but the continuation past that peek
        // -- `resolve_peeked_miss`'s leader RAM recheck -- runs for the first
        // time here, after the leader's flight has already left the
        // single-flight map, deciding whether it reuses the finished
        // flight's bytes or leads a second one.
        served.push(late.await.expect("the late caller resolves"));

        assert_eq!(
            disk_metrics.snapshot().misses - before_misses,
            CALLERS as u64,
            "every one of the 8 callers' disk peeks missed"
        );
        assert_eq!(
            recording.ranges().len(),
            1,
            "8 concurrent misses on one range must produce exactly one GET"
        );
        assert_eq!(served.len(), CALLERS);
        assert_eq!(&served[0][..], b"PAR1", "the file's first 4 bytes");
        for bytes in &served {
            assert_eq!(bytes, &served[0], "every caller receives identical bytes");
        }
    }
}
